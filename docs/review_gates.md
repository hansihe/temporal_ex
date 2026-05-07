# Review Gates

This document records the core review gates required by `docs/implementation_slice.md`.

## Scope

Review date: May 7, 2026.

Reviewed implementation:

- `Temporalex.Core.Executor`
- `Temporalex.Core.TestHarness`
- `Temporalex.Workflow.API`
- `Temporalex.Workflow`
- `Temporalex.Activity`
- `Temporalex.Worker`
- `Temporalex.Server`
- `Temporalex.Backend`
- `Temporalex.Backend.Test`

Test evidence:

- `mix test`: 47 tests, 0 failures, 1 external test excluded.
- `mix test --only external`: 1 test, 0 failures.

## Slice 1 Review

Status: completed.

Findings:

- Executor state stays inside the deterministic core. Backend worker handles, task tokens, protobuf structs, and Rustler resources are absent from `Temporalex.Core.Executor.State`.
- Blocking calls generalize through `%Temporalex.Core.Pending{seq, thread_id, from, op}`. Held `GenServer.from()` values are keyed by command sequence and carry thread ids, so the same protocol supports root, branch, and handler workflow units.
- Command identity is matched in `append_command/2` through deterministic command fields. Activity, timer, terminal, update, query, cancel-timer, and search-attribute commands are compared without backend transport fields.
- Activation failure and workflow failure are distinct in code and tests. Nondeterminism and scheduler violations produce `%Temporalex.Core.Completion{status: {:failed, reason, opts}}`; application workflow failures produce `%Temporalex.Core.Command.FailWorkflow{}` inside successful completions.
- Runner lifecycle is executor-owned. Runners are linked to the executor, the executor traps exits, blocked runners remain alive, eviction tears down cached threads, and executor shutdown tears down linked runners.
- The test harness drives the same `%Temporalex.Core.Activation{}` and `%Temporalex.Core.Completion{}` structs used by the server/backend boundary.
- The implemented activation, job, command, and completion structs follow the documented Temporal Core mapping for the current alpha scope.

Outcome:

- No Slice 1 design mismatch was found that requires executor rework before or after adding structured concurrency.

## Slice 2 Review

Status: completed.

Findings:

- Scheduler rounds are explicit in executor state through `round`, `current_round`, `next_round`, `running`, and `in_round?`. Runnable units are snapshotted, sorted by stable thread id, and newly runnable units are deferred to the next round.
- Every workflow unit has a stable thread id and lifecycle state in `threads`. Root, parallel branches, phase dispatches, and async handlers all use hierarchical ids.
- Command sequence assignment remains executor-owned across root, branches, and handlers through the shared `next_seq` counter.
- `parallel` provides overlapping durable waits without command races. Branches are spawned by input order, command emission follows scheduler order, and parent completion waits for all branch results in input order.
- `phase` dispatch is driven by activation job order and phase queue order. Sync handlers serialize; async handlers are bound to the phase and must finish before phase return.
- Async handlers mutate phase state only through serialized `API.update_state/1`. Closures that attempt workflow APIs fail through the async handler response path.
- Query-only activations respond from published state and do not advance root, branch, or handler workflow units.
- Update validation respects `run_validator`; replay-style updates with `run_validator: false` skip validator execution and still emit accepted/completed responses.
- Replay mismatch tests cover root commands, branch command order, and handler command changes.
- Handler, branch, query, update, and workflow failures map to the documented failure surfaces covered by the core tests.
- Process teardown covers root runners, blocked calls, phase exit, async handlers, executor shutdown, and eviction for the implemented alpha scope.
- The server/backend integration reuses the same core structs, and backend conformance tests now exercise the stable boundary with `Temporalex.Backend.Test`.

Outcome:

- No scheduler, replay, phase, update, or query ambiguity is known that blocks alpha evaluation.
- Server/backend integration can proceed without changing executor scheduling semantics. The server implementation already routes, supervises, and submits without owning deterministic workflow state.

## Alpha Limits

These are known alpha limits, not review-gate blockers:

- `Temporalex.Backend.TemporalCore` is a placeholder until the native Temporal Core/Rustler bridge and protobuf codecs are implemented.
- The external Temporal CLI test starts and health-checks a local development server, but no end-to-end workflow is run against Temporal Server until the native backend exists.
- Client APIs are not implemented.
- Patch/version APIs, deterministic random helpers, UUID helpers, child workflows, local activities, and Nexus operations are outside the implemented alpha scope.
