from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Tuple
import hashlib
import json
import uuid


class Clock:
    def __init__(self) -> None:
        self._now = datetime(2026, 4, 14, 12, 0, 0)

    def now(self) -> datetime:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += timedelta(seconds=seconds)


class VerdictCode(str, Enum):
    APPROVE = "APPROVE"
    REJECT = "REJECT"
    ESCALATE = "ESCALATE"
    FREEZE = "FREEZE"


class EventType(str, Enum):
    FREEZE_APPLIED = "FREEZE_APPLIED"
    INTENT_PROPOSED = "INTENT_PROPOSED"
    JURY_VERDICT = "JURY_VERDICT"
    STALE_STATE_REJECTED = "STALE_STATE_REJECTED"
    DUPLICATE_BLOCKED = "DUPLICATE_BLOCKED"
    EXECUTION_AUTHORISED = "EXECUTION_AUTHORISED"
    AUTH_CONSUMED = "AUTH_CONSUMED"
    EXECUTION_ABORTED = "EXECUTION_ABORTED"
    EXECUTION_CONFIRMED = "EXECUTION_CONFIRMED"


class RuleCode(str, Enum):
    ALL_CHECKS_PASSED = "ALL_CHECKS_PASSED"
    NO_RAW_STATE = "NO_RAW_STATE"
    FREEZE_BLOCKED = "FREEZE_BLOCKED"
    STALE_SENSORS = "STALE_SENSORS"
    DUPLICATE_BLOCKED = "DUPLICATE_BLOCKED"
    BATTERY_FLOOR = "BATTERY_FLOOR"
    HUMAN_PROXIMITY_ESCALATE = "HUMAN_PROXIMITY_ESCALATE"
    INVALID_AUTH_FIELDS = "INVALID_AUTH_FIELDS"
    AUTH_EXPIRED = "AUTH_EXPIRED"
    AUTH_ALREADY_CONSUMED = "AUTH_ALREADY_CONSUMED"
    CONTEXT_INVALIDATED = "CONTEXT_INVALIDATED"
    PROFILE_NOT_ALLOWED = "PROFILE_NOT_ALLOWED"
    PROFILE_TRANSITION_DELAY = "PROFILE_TRANSITION_DELAY"
    PROFILE_INVALIDATED = "PROFILE_INVALIDATED"
    BRIDGE_REJECTED = "BRIDGE_REJECTED"
    LEASE_TIMEOUT = "LEASE_TIMEOUT"
    FREEZE_CANCEL = "FREEZE_CANCEL"


class TerrainBucket(str, Enum):
    SMOOTH = "SMOOTH"
    MIXED = "MIXED"
    ROUGH = "ROUGH"


@dataclass(slots=True)
class GovernedIntent:
    robot_id: str
    action_type: str
    target_id: str
    idempotency_key: str
    intent_id: str
    mission_id: str
    proposed_at: datetime
    profile_id: Optional[str] = None
    requested_duration_s: float = 0.0


@dataclass(slots=True)
class RawStateSnapshot:
    robot_id: str
    battery_pct: float
    freeze_state: bool
    human_proximity_bucket: str
    sensor_freshness_ms: int
    mission_state_version: str
    reservation_state_version: str
    terrain_bucket: str = TerrainBucket.SMOOTH.value
    terrain_stable_for_s: float = 999.0
    source_timestamp: Optional[datetime] = None


@dataclass(slots=True)
class Verdict:
    code: VerdictCode
    rule_code: RuleCode


@dataclass(slots=True)
class ExecutionAuthorization:
    auth_id: str
    intent_id: str
    robot_id: str
    action_type: str
    target_id: str
    idempotency_key: str
    context_seq: int
    context_hash: str
    issued_at: datetime
    expires_at: datetime
    verdict: VerdictCode
    rule_code: RuleCode
    profile_id: Optional[str] = None
    fallback_profile_id: Optional[str] = None
    requested_duration_s: float = 0.0
    lease_check_interval_s: float = 3.0


@dataclass(slots=True)
class LedgerEvent:
    event_type: EventType
    intent_id: str
    robot_id: str
    rule_code: str
    context_seq: int
    context_hash: str
    timestamp: datetime


@dataclass(slots=True)
class ActiveExecution:
    auth: ExecutionAuthorization
    last_lease_check_at: datetime
    started_at: datetime


@dataclass(slots=True)
class ScenarioResult:
    name: str
    passed: bool
    terminal_event: str
    terminal_reason: str
    event_types: List[str]


class Ledger:
    def __init__(self, clock: Clock) -> None:
        self.clock = clock
        self.events: List[LedgerEvent] = []

    def record(self, event: LedgerEvent) -> None:
        terminal_exists = any(
            e.intent_id == event.intent_id
            and e.event_type in (EventType.EXECUTION_ABORTED, EventType.EXECUTION_CONFIRMED)
            for e in self.events
        )
        if terminal_exists and event.event_type in (EventType.EXECUTION_ABORTED, EventType.EXECUTION_CONFIRMED):
            return
        self.events.append(event)

    def make_event(
        self,
        event_type: EventType,
        intent_id: str,
        robot_id: str,
        rule_code: str = "",
        context_seq: int = 0,
        context_hash: str = "",
    ) -> LedgerEvent:
        return LedgerEvent(
            event_type=event_type,
            intent_id=intent_id,
            robot_id=robot_id,
            rule_code=rule_code,
            context_seq=context_seq,
            context_hash=context_hash,
            timestamp=self.clock.now(),
        )

    def event_types_for_intent(self, intent_id: str) -> List[str]:
        return [e.event_type.value for e in self.events if e.intent_id == intent_id]

    def last_terminal_for_intent(self, intent_id: str) -> Tuple[str, str]:
        for event in reversed(self.events):
            if event.intent_id == intent_id and event.event_type in (
                EventType.EXECUTION_ABORTED,
                EventType.EXECUTION_CONFIRMED,
            ):
                return event.event_type.value, event.rule_code
        return "-", "-"


class SurveyAdapter:
    def __init__(self, clock: Clock) -> None:
        self.clock = clock
        self.current_state_by_robot: Dict[str, RawStateSnapshot] = {}

    def update_state(self, state: RawStateSnapshot) -> None:
        if state.source_timestamp is None:
            state.source_timestamp = self.clock.now()
        self.current_state_by_robot[state.robot_id] = state

    def get_latest(self, robot_id: str) -> Optional[RawStateSnapshot]:
        return self.current_state_by_robot.get(robot_id)

    def simulate_fresh_state(self, robot_id: str = "R1") -> RawStateSnapshot:
        return RawStateSnapshot(
            robot_id=robot_id,
            battery_pct=0.82,
            freeze_state=False,
            human_proximity_bucket="CLEAR",
            sensor_freshness_ms=80,
            mission_state_version="m1",
            reservation_state_version="r1",
            terrain_bucket=TerrainBucket.SMOOTH.value,
            terrain_stable_for_s=30.0,
            source_timestamp=self.clock.now(),
        )


class PlannerAdapter:
    def __init__(self, clock: Clock) -> None:
        self.clock = clock
        self.counter = 0

    def _intent_id(self) -> str:
        self.counter += 1
        return f"intent_{self.counter:04d}_{uuid.uuid4().hex[:8]}"

    def create_enter_zone(self, target_id: str = "zone_pack_2", idempotency_key: str = "ik_enter_zone_001") -> GovernedIntent:
        return GovernedIntent(
            robot_id="R1",
            action_type="ENTER_ZONE",
            target_id=target_id,
            idempotency_key=idempotency_key,
            intent_id=self._intent_id(),
            mission_id="mission_42",
            proposed_at=self.clock.now(),
        )

    def create_run_profile(
        self,
        profile_id: str = "ALPHA",
        duration_s: float = 14.0,
        idempotency_key: str = "ik_profile_alpha_001",
    ) -> GovernedIntent:
        return GovernedIntent(
            robot_id="R1",
            action_type="RUN_PROFILE",
            target_id="profile_track",
            idempotency_key=idempotency_key,
            intent_id=self._intent_id(),
            mission_id="mission_42",
            proposed_at=self.clock.now(),
            profile_id=profile_id,
            requested_duration_s=duration_s,
        )


class Governor:
    def __init__(self, ledger: Ledger, clock: Clock) -> None:
        self.ledger = ledger
        self.clock = clock
        self.context_seq = 0
        self.latest_raw_by_robot: Dict[str, RawStateSnapshot] = {}
        self.consumed_idempotency: set[str] = set()
        self.active_auth_by_robot: Dict[str, ExecutionAuthorization] = {}
        self.profile_policy = {
            "ALPHA": {
                "allowed_terrain": {TerrainBucket.SMOOTH.value, TerrainBucket.MIXED.value},
                "fallback_profile_id": "SAFE_CRAWL",
                "transition_delay_s": 2.0,
                "lease_check_interval_s": 3.0,
            },
            "BETA": {
                "allowed_terrain": {TerrainBucket.SMOOTH.value, TerrainBucket.MIXED.value},
                "fallback_profile_id": "SAFE_CRAWL",
                "transition_delay_s": 2.0,
                "lease_check_interval_s": 3.0,
            },
            "DELTA": {
                "allowed_terrain": {TerrainBucket.MIXED.value, TerrainBucket.ROUGH.value},
                "fallback_profile_id": "SAFE_CRAWL",
                "transition_delay_s": 2.0,
                "lease_check_interval_s": 3.0,
            },
            "SAFE_CRAWL": {
                "allowed_terrain": {TerrainBucket.SMOOTH.value, TerrainBucket.MIXED.value, TerrainBucket.ROUGH.value},
                "fallback_profile_id": None,
                "transition_delay_s": 0.0,
                "lease_check_interval_s": 3.0,
            },
        }

    def update_raw_state(self, raw: RawStateSnapshot) -> None:
        if raw.source_timestamp is None:
            raw.source_timestamp = self.clock.now()
        self.latest_raw_by_robot[raw.robot_id] = raw

    def evaluate(self, intent: GovernedIntent) -> Tuple[Verdict, Optional[ExecutionAuthorization]]:
        raw = self.latest_raw_by_robot.get(intent.robot_id)
        if raw is None:
            verdict = Verdict(VerdictCode.REJECT, RuleCode.NO_RAW_STATE)
            self._record_verdict(intent, verdict, 0, "")
            return verdict, None

        self.context_seq += 1
        context_seq = self.context_seq
        context_hash = self._compute_context_hash(intent.robot_id, intent.target_id, intent.profile_id, raw)

        self.ledger.record(
            self.ledger.make_event(
                EventType.INTENT_PROPOSED,
                intent.intent_id,
                intent.robot_id,
                context_seq=context_seq,
                context_hash=context_hash,
            )
        )

        if raw.freeze_state:
            self.ledger.record(
                self.ledger.make_event(
                    EventType.FREEZE_APPLIED,
                    intent.intent_id,
                    intent.robot_id,
                    rule_code=RuleCode.FREEZE_BLOCKED.value,
                    context_seq=context_seq,
                    context_hash=context_hash,
                )
            )
            verdict = Verdict(VerdictCode.FREEZE, RuleCode.FREEZE_BLOCKED)
            self._record_verdict(intent, verdict, context_seq, context_hash)
            return verdict, None

        if raw.sensor_freshness_ms > 500:
            self.ledger.record(
                self.ledger.make_event(
                    EventType.STALE_STATE_REJECTED,
                    intent.intent_id,
                    intent.robot_id,
                    rule_code=RuleCode.STALE_SENSORS.value,
                    context_seq=context_seq,
                    context_hash=context_hash,
                )
            )
            verdict = Verdict(VerdictCode.REJECT, RuleCode.STALE_SENSORS)
            self._record_verdict(intent, verdict, context_seq, context_hash)
            return verdict, None

        if intent.idempotency_key in self.consumed_idempotency:
            self.ledger.record(
                self.ledger.make_event(
                    EventType.DUPLICATE_BLOCKED,
                    intent.intent_id,
                    intent.robot_id,
                    rule_code=RuleCode.DUPLICATE_BLOCKED.value,
                    context_seq=context_seq,
                    context_hash=context_hash,
                )
            )
            verdict = Verdict(VerdictCode.REJECT, RuleCode.DUPLICATE_BLOCKED)
            self._record_verdict(intent, verdict, context_seq, context_hash)
            return verdict, None

        if raw.battery_pct < 0.20:
            verdict = Verdict(VerdictCode.REJECT, RuleCode.BATTERY_FLOOR)
            self._record_verdict(intent, verdict, context_seq, context_hash)
            return verdict, None

        if intent.action_type == "RUN_PROFILE":
            allowed, reason = self._profile_allowed(intent.profile_id, raw)
            if not allowed:
                verdict = Verdict(VerdictCode.REJECT, reason)
                self._record_verdict(intent, verdict, context_seq, context_hash)
                return verdict, None

        verdict = Verdict(VerdictCode.APPROVE, RuleCode.ALL_CHECKS_PASSED)
        self._record_verdict(intent, verdict, context_seq, context_hash)

        fallback_profile_id = None
        lease_check_interval_s = 3.0
        if intent.profile_id:
            policy = self.profile_policy[intent.profile_id]
            fallback_profile_id = policy["fallback_profile_id"]
            lease_check_interval_s = float(policy["lease_check_interval_s"])

        auth = ExecutionAuthorization(
            auth_id=f"auth_{uuid.uuid4().hex}",
            intent_id=intent.intent_id,
            robot_id=intent.robot_id,
            action_type=intent.action_type,
            target_id=intent.target_id,
            idempotency_key=intent.idempotency_key,
            context_seq=context_seq,
            context_hash=context_hash,
            issued_at=self.clock.now(),
            expires_at=self.clock.now() + timedelta(seconds=30),
            verdict=VerdictCode.APPROVE,
            rule_code=RuleCode.ALL_CHECKS_PASSED,
            profile_id=intent.profile_id,
            fallback_profile_id=fallback_profile_id,
            requested_duration_s=intent.requested_duration_s,
            lease_check_interval_s=lease_check_interval_s,
        )

        self.ledger.record(
            self.ledger.make_event(
                EventType.EXECUTION_AUTHORISED,
                intent.intent_id,
                intent.robot_id,
                rule_code=RuleCode.ALL_CHECKS_PASSED.value,
                context_seq=context_seq,
                context_hash=context_hash,
            )
        )
        return verdict, auth

    def verify_authorization(self, auth: ExecutionAuthorization) -> Tuple[bool, RuleCode]:
        raw = self.latest_raw_by_robot.get(auth.robot_id)
        if raw is None:
            return False, RuleCode.NO_RAW_STATE

        if raw.freeze_state:
            return False, RuleCode.FREEZE_BLOCKED

        if raw.sensor_freshness_ms > 500:
            return False, RuleCode.STALE_SENSORS

        if raw.battery_pct < 0.20:
            return False, RuleCode.BATTERY_FLOOR

        if auth.profile_id:
            allowed, _ = self._profile_allowed(auth.profile_id, raw)
            if not allowed:
                return False, RuleCode.PROFILE_INVALIDATED

        current_hash = self._compute_context_hash(auth.robot_id, auth.target_id, auth.profile_id, raw)
        if current_hash != auth.context_hash:
            return False, RuleCode.CONTEXT_INVALIDATED

        return True, RuleCode.ALL_CHECKS_PASSED

    def on_auth_consumed(self, auth: ExecutionAuthorization) -> None:
        self.consumed_idempotency.add(auth.idempotency_key)
        self.active_auth_by_robot[auth.robot_id] = auth

    def on_execution_terminal(self, auth: ExecutionAuthorization) -> None:
        active = self.active_auth_by_robot.get(auth.robot_id)
        if active and active.auth_id == auth.auth_id:
            self.active_auth_by_robot.pop(auth.robot_id, None)

    def _profile_allowed(self, profile_id: Optional[str], raw: RawStateSnapshot) -> Tuple[bool, RuleCode]:
        if not profile_id:
            return True, RuleCode.ALL_CHECKS_PASSED
        policy = self.profile_policy.get(profile_id)
        if policy is None:
            return False, RuleCode.PROFILE_NOT_ALLOWED
        if raw.terrain_stable_for_s < float(policy["transition_delay_s"]):
            return False, RuleCode.PROFILE_TRANSITION_DELAY
        if raw.terrain_bucket not in policy["allowed_terrain"]:
            return False, RuleCode.PROFILE_NOT_ALLOWED
        return True, RuleCode.ALL_CHECKS_PASSED

    def _compute_context_hash(
        self,
        robot_id: str,
        target_id: str,
        profile_id: Optional[str],
        raw: RawStateSnapshot,
    ) -> str:
        battery_class = "LOW" if raw.battery_pct < 0.20 else "OK"
        payload = {
            "robot_id": robot_id,
            "target_id": target_id,
            "profile_id": profile_id or "",
            "freeze_state": raw.freeze_state,
            "human_proximity_bucket": raw.human_proximity_bucket,
            "freshness_bucket": "STALE" if raw.sensor_freshness_ms > 500 else "FRESH",
            "battery_class": battery_class,
            "mission_state_version": raw.mission_state_version,
            "reservation_state_version": raw.reservation_state_version,
            "terrain_bucket": raw.terrain_bucket,
        }
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()

    def _record_verdict(
        self,
        intent: GovernedIntent,
        verdict: Verdict,
        context_seq: int,
        context_hash: str,
    ) -> None:
        self.ledger.record(
            self.ledger.make_event(
                EventType.JURY_VERDICT,
                intent.intent_id,
                intent.robot_id,
                rule_code=verdict.rule_code.value,
                context_seq=context_seq,
                context_hash=context_hash,
            )
        )


class ExecutionGate:
    def __init__(self, ledger: Ledger, governor: Governor, clock: Clock) -> None:
        self.ledger = ledger
        self.governor = governor
        self.clock = clock
        self.consumed_auth_ids: set[str] = set()

    def verify(self, auth: ExecutionAuthorization) -> Tuple[bool, RuleCode]:
        if not auth.intent_id or not auth.robot_id or not auth.auth_id:
            self._abort(auth, RuleCode.INVALID_AUTH_FIELDS)
            return False, RuleCode.INVALID_AUTH_FIELDS

        if self.clock.now() > auth.expires_at:
            self._abort(auth, RuleCode.AUTH_EXPIRED)
            return False, RuleCode.AUTH_EXPIRED

        if auth.auth_id in self.consumed_auth_ids:
            active = self.governor.active_auth_by_robot.get(auth.robot_id)
            if active is None or active.auth_id != auth.auth_id:
                self._abort(auth, RuleCode.AUTH_ALREADY_CONSUMED)
                return False, RuleCode.AUTH_ALREADY_CONSUMED

        valid, rule_code = self.governor.verify_authorization(auth)
        if not valid:
            self._abort(auth, rule_code)
            return False, rule_code

        return True, RuleCode.ALL_CHECKS_PASSED

    def consume(self, auth: ExecutionAuthorization) -> None:
        self.consumed_auth_ids.add(auth.auth_id)
        self.ledger.record(
            self.ledger.make_event(
                EventType.AUTH_CONSUMED,
                auth.intent_id,
                auth.robot_id,
                rule_code=RuleCode.ALL_CHECKS_PASSED.value,
                context_seq=auth.context_seq,
                context_hash=auth.context_hash,
            )
        )

    def _abort(self, auth: ExecutionAuthorization, rule_code: RuleCode) -> None:
        self.ledger.record(
            self.ledger.make_event(
                EventType.EXECUTION_ABORTED,
                auth.intent_id,
                auth.robot_id,
                rule_code=rule_code.value,
                context_seq=auth.context_seq,
                context_hash=auth.context_hash,
            )
        )


class Nav2BridgeStub:
    def __init__(self, ledger: Ledger, gate: ExecutionGate, governor: Governor, clock: Clock) -> None:
        self.ledger = ledger
        self.gate = gate
        self.governor = governor
        self.clock = clock
        self.active_by_robot: Dict[str, ActiveExecution] = {}

    def submit_goal(self, auth: ExecutionAuthorization) -> bool:
        if auth.robot_id in self.active_by_robot:
            self.ledger.record(
                self.ledger.make_event(
                    EventType.EXECUTION_ABORTED,
                    auth.intent_id,
                    auth.robot_id,
                    rule_code=RuleCode.BRIDGE_REJECTED.value,
                    context_seq=auth.context_seq,
                    context_hash=auth.context_hash,
                )
            )
            return False

        accepted = True
        if not accepted:
            self.ledger.record(
                self.ledger.make_event(
                    EventType.EXECUTION_ABORTED,
                    auth.intent_id,
                    auth.robot_id,
                    rule_code=RuleCode.BRIDGE_REJECTED.value,
                    context_seq=auth.context_seq,
                    context_hash=auth.context_hash,
                )
            )
            return False

        self.gate.consume(auth)
        self.governor.on_auth_consumed(auth)
        self.active_by_robot[auth.robot_id] = ActiveExecution(
            auth=auth,
            last_lease_check_at=self.clock.now(),
            started_at=self.clock.now(),
        )

        if auth.action_type != "RUN_PROFILE":
            self.complete_goal(auth.robot_id, RuleCode.ALL_CHECKS_PASSED)
        return True

    def step_profile(self, robot_id: str, auto_check: bool = True) -> bool:
        active = self.active_by_robot.get(robot_id)
        if active is None:
            return False

        elapsed_since_last_check = (self.clock.now() - active.last_lease_check_at).total_seconds()

        if not auto_check:
            if elapsed_since_last_check > active.auth.lease_check_interval_s:
                self.abort_goal(robot_id, RuleCode.LEASE_TIMEOUT)
                return False
            return True

        valid, rule = self.gate.verify(active.auth)
        if not valid:
            mapped = RuleCode.FREEZE_CANCEL if rule == RuleCode.FREEZE_BLOCKED else rule
            self.abort_goal(robot_id, mapped)
            return False

        active.last_lease_check_at = self.clock.now()
        total_run = (self.clock.now() - active.started_at).total_seconds()
        if total_run >= active.auth.requested_duration_s:
            self.complete_goal(robot_id, RuleCode.ALL_CHECKS_PASSED)
            return True
        return True

    def complete_goal(self, robot_id: str, rule_code: RuleCode) -> None:
        active = self.active_by_robot.pop(robot_id, None)
        if active is None:
            return
        self.ledger.record(
            self.ledger.make_event(
                EventType.EXECUTION_CONFIRMED,
                active.auth.intent_id,
                robot_id,
                rule_code=rule_code.value,
                context_seq=active.auth.context_seq,
                context_hash=active.auth.context_hash,
            )
        )
        self.governor.on_execution_terminal(active.auth)

    def abort_goal(self, robot_id: str, rule_code: RuleCode) -> None:
        active = self.active_by_robot.pop(robot_id, None)
        if active is None:
            return
        self.ledger.record(
            self.ledger.make_event(
                EventType.EXECUTION_ABORTED,
                active.auth.intent_id,
                robot_id,
                rule_code=rule_code.value,
                context_seq=active.auth.context_seq,
                context_hash=active.auth.context_hash,
            )
        )
        self.governor.on_execution_terminal(active.auth)


class ScenarioRunner:
    def __init__(self) -> None:
        self.clock = Clock()
        self.ledger = Ledger(self.clock)
        self.survey = SurveyAdapter(self.clock)
        self.planner = PlannerAdapter(self.clock)
        self.governor = Governor(self.ledger, self.clock)
        self.gate = ExecutionGate(self.ledger, self.governor, self.clock)
        self.bridge = Nav2BridgeStub(self.ledger, self.gate, self.governor, self.clock)

    def _fresh_state(self) -> RawStateSnapshot:
        return self.survey.simulate_fresh_state("R1")

    def _result(self, name: str, intent_id: str, passed: bool) -> ScenarioResult:
        terminal_event, terminal_reason = self.ledger.last_terminal_for_intent(intent_id)
        return ScenarioResult(
            name=name,
            passed=passed,
            terminal_event=terminal_event,
            terminal_reason=terminal_reason,
            event_types=self.ledger.event_types_for_intent(intent_id),
        )

    def happy_path_enter_zone(self) -> ScenarioResult:
        raw = self._fresh_state()
        self.survey.update_state(raw)
        self.governor.update_raw_state(raw)

        intent = self.planner.create_enter_zone()
        verdict, auth = self.governor.evaluate(intent)

        ok = verdict.code == VerdictCode.APPROVE and auth is not None
        if ok:
            verified, _ = self.gate.verify(auth)
            ok = verified and self.bridge.submit_goal(auth)

        terminal_event, _ = self.ledger.last_terminal_for_intent(intent.intent_id)
        passed = ok and terminal_event == EventType.EXECUTION_CONFIRMED.value
        return self._result("happy_path_enter_zone", intent.intent_id, passed)

    def stale_state_reject(self) -> ScenarioResult:
        raw = self._fresh_state()
        raw.sensor_freshness_ms = 900
        self.survey.update_state(raw)
        self.governor.update_raw_state(raw)

        intent = self.planner.create_enter_zone(idempotency_key="ik_stale_001")
        verdict, auth = self.governor.evaluate(intent)

        passed = (
            verdict.code == VerdictCode.REJECT
            and verdict.rule_code == RuleCode.STALE_SENSORS
            and auth is None
        )
        return self._result("stale_state_reject", intent.intent_id, passed)

    def duplicate_block(self) -> ScenarioResult:
        raw = self._fresh_state()
        self.survey.update_state(raw)
        self.governor.update_raw_state(raw)

        intent_1 = self.planner.create_enter_zone(idempotency_key="ik_dup_001")
        verdict_1, auth_1 = self.governor.evaluate(intent_1)

        ok_first = verdict_1.code == VerdictCode.APPROVE and auth_1 is not None
        if ok_first:
            verified, _ = self.gate.verify(auth_1)
            ok_first = verified and self.bridge.submit_goal(auth_1)

        intent_2 = self.planner.create_enter_zone(idempotency_key="ik_dup_001")
        verdict_2, auth_2 = self.governor.evaluate(intent_2)

        passed = (
            ok_first
            and verdict_2.code == VerdictCode.REJECT
            and verdict_2.rule_code == RuleCode.DUPLICATE_BLOCKED
            and auth_2 is None
        )
        return self._result("duplicate_block", intent_2.intent_id, passed)

    def freeze_wall(self) -> ScenarioResult:
        raw = self._fresh_state()
        raw.freeze_state = True
        self.survey.update_state(raw)
        self.governor.update_raw_state(raw)

        intent = self.planner.create_enter_zone(idempotency_key="ik_freeze_001")
        verdict, auth = self.governor.evaluate(intent)

        passed = (
            verdict.code == VerdictCode.FREEZE
            and verdict.rule_code == RuleCode.FREEZE_BLOCKED
            and auth is None
        )
        return self._result("freeze_wall", intent.intent_id, passed)

    def profile_alpha_happy(self) -> ScenarioResult:
        raw = self._fresh_state()
        raw.terrain_bucket = TerrainBucket.SMOOTH.value
        raw.terrain_stable_for_s = 10.0
        self.survey.update_state(raw)
        self.governor.update_raw_state(raw)

        intent = self.planner.create_run_profile(
            profile_id="ALPHA",
            duration_s=6.0,
            idempotency_key="ik_alpha_happy_001",
        )
        verdict, auth = self.governor.evaluate(intent)

        ok = verdict.code == VerdictCode.APPROVE and auth is not None
        if ok:
            verified, _ = self.gate.verify(auth)
            ok = verified and self.bridge.submit_goal(auth)

        for _ in range(6):
            self.clock.advance(1.0)
            self.bridge.step_profile("R1", auto_check=True)

        terminal_event, _ = self.ledger.last_terminal_for_intent(intent.intent_id)
        passed = ok and terminal_event == EventType.EXECUTION_CONFIRMED.value
        return self._result("profile_alpha_happy", intent.intent_id, passed)

    def profile_invalidated_mid_run(self) -> ScenarioResult:
        raw = self._fresh_state()
        raw.terrain_bucket = TerrainBucket.SMOOTH.value
        raw.terrain_stable_for_s = 10.0
        self.survey.update_state(raw)
        self.governor.update_raw_state(raw)

        intent = self.planner.create_run_profile(
            profile_id="ALPHA",
            duration_s=14.0,
            idempotency_key="ik_alpha_break_001",
        )
        verdict, auth = self.governor.evaluate(intent)

        ok = verdict.code == VerdictCode.APPROVE and auth is not None
        if ok:
            verified, _ = self.gate.verify(auth)
            ok = verified and self.bridge.submit_goal(auth)

        self.clock.advance(1.0)
        self.bridge.step_profile("R1", auto_check=True)

        broken = self._fresh_state()
        broken.terrain_bucket = TerrainBucket.ROUGH.value
        broken.terrain_stable_for_s = 10.0
        self.survey.update_state(broken)
        self.governor.update_raw_state(broken)

        self.clock.advance(1.0)
        self.bridge.step_profile("R1", auto_check=True)

        terminal_event, terminal_reason = self.ledger.last_terminal_for_intent(intent.intent_id)
        passed = (
            terminal_event == EventType.EXECUTION_ABORTED.value
            and terminal_reason == RuleCode.PROFILE_INVALIDATED.value
        )
        return self._result("profile_invalidated_mid_run", intent.intent_id, passed)

    def lease_timeout(self) -> ScenarioResult:
        raw = self._fresh_state()
        raw.terrain_bucket = TerrainBucket.SMOOTH.value
        raw.terrain_stable_for_s = 10.0
        self.survey.update_state(raw)
        self.governor.update_raw_state(raw)

        intent = self.planner.create_run_profile(
            profile_id="ALPHA",
            duration_s=14.0,
            idempotency_key="ik_lease_001",
        )
        verdict, auth = self.governor.evaluate(intent)

        ok = verdict.code == VerdictCode.APPROVE and auth is not None
        if ok:
            verified, _ = self.gate.verify(auth)
            ok = verified and self.bridge.submit_goal(auth)

        self.clock.advance(4.0)
        self.bridge.step_profile("R1", auto_check=False)

        terminal_event, terminal_reason = self.ledger.last_terminal_for_intent(intent.intent_id)
        passed = (
            terminal_event == EventType.EXECUTION_ABORTED.value
            and terminal_reason == RuleCode.LEASE_TIMEOUT.value
        )
        return self._result("lease_timeout", intent.intent_id, passed)


def print_summary(results: List[ScenarioResult]) -> None:
    print("\n" + "=" * 72)
    print("SNAPSPACE ROBOTICS V0 LAB SUMMARY")
    print("=" * 72)

    passed = 0
    failed = 0

    for result in results:
        status = "PASS" if result.passed else "FAIL"
        if result.passed:
            passed += 1
        else:
            failed += 1

        print(
            f"{status:<4} | {result.name:<28} | "
            f"{result.terminal_event:<19} | {result.terminal_reason or '-'}"
        )
        print(f"      events: {', '.join(result.event_types) if result.event_types else '-'}")

    print("-" * 72)
    print(f"TOTAL: {len(results)}   PASSED: {passed}   FAILED: {failed}")
    print("=" * 72)


def main() -> None:
    runner = ScenarioRunner()
    results = [
        runner.happy_path_enter_zone(),
        runner.stale_state_reject(),
        runner.duplicate_block(),
        runner.freeze_wall(),
        runner.profile_alpha_happy(),
        runner.profile_invalidated_mid_run(),
        runner.lease_timeout(),
    ]
    print_summary(results)


if __name__ == "__main__":
    main()
