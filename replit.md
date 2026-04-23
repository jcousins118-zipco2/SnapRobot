# SnapRobot

## Project Overview
SnapRobot is a robot governance and execution control simulation system. It models the full lifecycle of robot action intents — from proposal through authorization, execution, and terminal confirmation or abort.

## Architecture

### Key Classes
- **Governor** — evaluates `GovernedIntent` objects against robot state, enforces safety rules (freeze, stale sensors, battery, terrain profiles), and issues `ExecutionAuthorization` tokens.
- **ExecutionGate** — verifies authorizations before execution: checks expiry, duplicate consumption, and live state.
- **Nav2BridgeStub** — simulates the ROS2 Nav2 bridge; accepts goals, runs profile lease checks, and drives completion/abort.
- **Ledger** — append-only event log for all governance events per intent.
- **SurveyAdapter** — manages live robot state snapshots.
- **PlannerAdapter** — creates `GovernedIntent` objects (ENTER_ZONE, RUN_PROFILE).
- **ScenarioRunner** — orchestrates end-to-end test scenarios.

### Enums & Data Models
- `VerdictCode`: APPROVE, REJECT, ESCALATE, FREEZE
- `RuleCode`: ALL_CHECKS_PASSED, FREEZE_BLOCKED, STALE_SENSORS, BATTERY_FLOOR, PROFILE_NOT_ALLOWED, CONTEXT_INVALIDATED, LEASE_TIMEOUT, etc.
- `TerrainBucket`: SMOOTH, MIXED, ROUGH
- `GovernedIntent`, `RawStateSnapshot`, `ExecutionAuthorization`, `LedgerEvent`, `ActiveExecution`

## Entry Point
`main.py` — runs `ScenarioRunner` with 7 test scenarios and prints a pass/fail summary table.

## Workflow
- **Start application** — runs `python main.py` (console output)

## GitHub Repository
https://github.com/jcousins118-zipco2/SnapRobot

## Scenarios Tested
1. `happy_path_enter_zone` — normal zone entry approval and execution
2. `stale_state_reject` — rejected due to stale sensor data
3. `duplicate_block` — idempotency key prevents replay
4. `freeze_wall` — freeze state blocks execution
5. `profile_alpha_happy` — ALPHA profile runs to completion
6. `profile_invalidated_mid_run` — terrain change aborts active profile
7. `lease_timeout` — lease check interval exceeded, execution aborted
