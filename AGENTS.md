# @marianmeres/workflow — Agent Guide

## Quick Reference

- **Stack**: Deno/Node.js, PostgreSQL, TypeScript
- **Test**: `deno task test` (requires `TEST_PG_*` env; see [tests/_pg.ts](./tests/_pg.ts))
- **Build (npm)**: `deno task npm:build`
- **Entry point**: [src/mod.ts](./src/mod.ts)

## Package Overview

- **Name**: `@marianmeres/workflow`
- **Type**: Durable workflow / orchestration framework
- **Runtime**: Deno and Node.js (via npm build)
- **DB**: PostgreSQL only (uses `pg` driver)
- **License**: MIT

## Purpose

Long-lived, persisted FSM instances with durable wake-ups (timers) and external-signal correlation (inbox + matchers). Definitions are pure data, versioned per-instance. Effects run as steve jobs (transactional outbox). Built on top of `@marianmeres/fsm` (state graph), `@marianmeres/steve` (effect execution), `@marianmeres/cron` (scheduler & correlator ticks), `@marianmeres/migrate` (schema), `@marianmeres/clog` (logging).

**Ships no domain code.** Userland brings the definitions, handlers, and matchers.

## Architecture

```
┌─ User application ─────────────────────────────────────────────────┐
│  WorkflowDefinition (pure data) + Handlers + Matchers              │
│  steve.Jobs + cron.Cron — consumer-owned, consumer calls start/stop│
└────────────────────────────┬───────────────────────────────────────┘
                             ▼
┌─ @marianmeres/workflow ────────────────────────────────────────────┐
│  Workflow              WorkflowScheduler    WorkflowInboxCorrelator │
│  setHandler(advance,   register() attaches  register() attaches     │
│  effect.*) on Jobs     tick on Cron         tick on Cron            │
│    ▼                     ▼                    ▼                     │
│  __workflow_instances · __workflow_inbox · __workflow_history       │
└────────────────────────────┬───────────────────────────────────────┘
                             ▼
                          PostgreSQL
```

The framework owns no runtime. It attaches handlers to a `Jobs` instance and ticks to a `Cron` instance that the consumer constructs and starts. This means the consumer's other job types / cron jobs can coexist on the same shared runtime (one pollers per process instead of N).

```
src/
├── mod.ts                  # Public exports
├── types.ts                # Definition, Instance, NodeMeta, Handler, Matcher, ...
├── definition.ts           # Definition validator + defKey()
├── registry.ts             # WorkflowRegistry (validates on construction)
├── driver.ts               # runAdvance / runEffect — the core dispatch logic
├── workflow.ts             # Workflow class (attaches handlers to injected Jobs)
├── scheduler.ts            # WorkflowScheduler (attaches wake tick to injected Cron)
├── correlator.ts           # WorkflowInboxCorrelator (attaches match tick to injected Cron)
├── log.ts                  # createClog("workflow")
├── persistence/
│   ├── instances.ts        # CRUD + lock + seq bump + due/stale scans + find by correlation
│   ├── inbox.ts            # append, claim-unprocessed, lock-row, mark-processed
│   ├── history.ts          # append + read
│   └── tx.ts               # withTransaction helper
└── migrations/
    ├── 1_0_0.ts            # CREATE/DROP three tables
    ├── 1_1_0.ts            # rename project_id -> tenant_id (in place)
    ├── 1_2_0.ts            # add seq (fence) + stale-pending partial index
    └── index.ts            # createMigrate(pool) — @marianmeres/migrate setup
tests/
├── _pg.ts                  # env-driven pool + schema reset
├── fixtures/
│   ├── stock-replenishment.ts  # reference workflow used by tests only
│   └── weekly-digest.ts        # recurring-trigger reference workflow
├── correlator.test.ts          # deferral, dedupe, matcher throw/reject, delivery-in-advance
├── durability.test.ts          # fence, preconditions, expiry re-dispatch, stale-pending re-poke
├── migrations.test.ts          # schema rename/fence up/down + legacy payload drain
├── stock_replenishment.test.ts # happy/timeout/fail paths + validator
└── weekly_digest.test.ts       # recurring trigger via cron
```

## Conceptual Model — Three Layers

| Layer          | What it is                                                                                          | Lives in                               |
| -------------- | --------------------------------------------------------------------------------------------------- | -------------------------------------- |
| **Definition** | Static graph: nodes, transitions, handler ids (strings). Pure data, JSON-safe.                      | Code, registered in `WorkflowRegistry` |
| **Instance**   | Live state of one running occurrence. Cursor, context, execution_state, wake_at, correlation_token. | `__workflow_instances` row             |
| **Driver**     | Event-driven engine that advances instances. Owns no state.                                         | `workflow.advance` steve job           |

## Two Orthogonal States

Never conflate. Always two columns on `__workflow_instances`:

| Column            | Meaning                     | Source of truth for                                                      |
| ----------------- | --------------------------- | ------------------------------------------------------------------------ |
| `cursor`          | Which node am I at?         | The FSM's `state` (`getSnapshot().state`)                                |
| `execution_state` | What's my lifecycle status? | `pending` / `running` / `waiting` / `completed` / `failed` / `cancelled` |

An instance can be at node `await_reply` (cursor) and simultaneously `waiting` (execution_state).

## Node Taxonomy (`NodeMeta`)

Each FSM state's `meta` field carries a discriminated `NodeMeta`. The driver dispatches on `meta.kind`:

| Kind         | Driver behavior                                                                                                                                                                               | Required fields                           |
| ------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------- |
| `pure`       | Auto-fire `ENTER` event; transition by guards. Loop inline until terminal/effectful/suspending. Max 64 hops.                                                                                  | none                                      |
| `effectful`  | Enqueue `workflow.effect.<handler>` steve job, flip to `running`. On completion, an `advance` is enqueued with `{ outcome, outcome_data, correlation_token? }`.                               | `handler: string`                         |
| `suspending` | Set `execution_state='waiting'`, `wake_at` (if `timeoutSec`), keep the `correlation_token` (or the one the last handler returned). Wakes via scheduler (`TIMEOUT`) or correlator (`MATCHED`). | optional `matcher`, optional `timeoutSec` |
| `terminal`   | Set `execution_state='completed'`.                                                                                                                                                            | none                                      |

## The Fence and the Poke Model

Two invariants hold the durability story together. Break either one and the failure is silent.

**1. The advance is the only writer of `__workflow_instances`.** Ticks (scheduler, correlator) are read-only: they select what looks actionable and enqueue a `workflow.advance` for it. The advance re-checks the precondition under `SELECT ... FOR UPDATE` and does the write. Consequence: a crash anywhere in a tick costs nothing — whatever made the row look actionable is still true next tick, so the next tick pokes again. A stale or duplicate poke is a debug log, never a history row.

**2. Every settle-point write bumps `seq`; every job carries the `seq` it was issued against.** `expected_seq` on `AdvanceJobPayload`, `seq` on `EffectJobPayload`. On a mismatch the job is dropped:

| Where                              | Check                              | On mismatch                                                                                                                                              |
| ---------------------------------- | ---------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `runAdvance`                       | `row.seq !== payload.expected_seq` | debug log, no-op                                                                                                                                         |
| `runEffect`                        | `row.seq !== payload.seq`          | `row.seq > payload.seq` → skip (`{ skipped: "stale" }`); `row.seq < payload.seq` → **throw**, so steve retries: the dispatching tx has not committed yet |
| `failAdvanceJob` / `failEffectJob` | same                               | no-op — a dead job must not fail an instance that has moved on                                                                                           |

This is what makes at-least-once job delivery safe. Duplicate deliveries are _expected_, not exceptional.

**Per-`kind` preconditions** are the second half of the fence, and only apply when the payload states its `kind` (a legacy payload's kind is a guess):

| `kind`    | Requires                                                            |
| --------- | ------------------------------------------------------------------- |
| `start`   | `execution_state = 'pending'`                                       |
| `effect`  | `execution_state = 'running'`                                       |
| `signal`  | `execution_state = 'waiting'`                                       |
| `timeout` | `execution_state = 'waiting'` **and** `wake_at` set and in the past |

`timeout` re-checks due-ness because the tick is read-only — the timer is still live until this advance clears it, and applying `TIMEOUT` to a not-yet-due row would cut the wait short.

**Signal delivery commits with the transition.** A `kind: "signal"` advance carries `inbox_id`, not the payload: the driver locks the inbox row, reads the payload off it, transitions, and calls `markProcessed` — all in one transaction. A signal is therefore never consumed without being delivered. The correlator's own tick never marks a row processed except when it is _rejecting_ it (no live owner, or the matcher said no).

**Deferral.** The correlator leaves a row unprocessed (and re-examines it next tick) when the instance is live but not yet `waiting`, when the current node has no `MATCHED`/`*` edge, when the matcher throws, or when that instance was already poked this tick. Rationale: an early signal is early, not wrong; consuming it would lose it, and poking a node with no `MATCHED` edge would make the fsm reject and fail the instance. Known cost: a deterministic matcher throw retries (and logs) every tick, and a deferred backlog larger than `tickBatchSize` shadows newer rows (`ORDER BY received_at`).

## Public API (from `src/mod.ts`)

| Export                         | Type     | Purpose                                                                                                                                                                     |
| ------------------------------ | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Workflow`                     | class    | Main driver: attaches handlers to an injected `steve.Jobs`. Exposes `create/find/appendInbox/enqueueAdvance/enqueueEffect`. **No own start/stop** — consumer runs the Jobs. |
| `WorkflowScheduler`            | class    | Attaches a wake tick to an injected `cron.Cron`. Exposes `register/unregister/tickOnce`. **No own start/stop** — consumer runs the Cron.                                    |
| `WorkflowInboxCorrelator`      | class    | Attaches a match tick to an injected `cron.Cron`. Exposes `register/unregister/tickOnce`. **No own start/stop** — consumer runs the Cron.                                   |
| `WorkflowRegistry`             | class    | Immutable registry of definitions + handlers + matchers; validates on construction                                                                                          |
| `validateDefinition`           | function | Standalone validator usable outside `WorkflowRegistry`                                                                                                                      |
| `defKey(id, version)`          | function | `"id@version"` registry key helper                                                                                                                                          |
| `createMigrate(pool)`          | function | Builds `@marianmeres/migrate` instance pre-loaded with schema versions                                                                                                      |
| `getHistory(exec, id, limit?)` | function | Reads `__workflow_history` rows for an instance (observability)                                                                                                             |
| `effectJobType(name)`          | function | Steve job-type string for an effect handler: `workflow.effect.<name>`                                                                                                       |
| `JOB_TYPE_ADVANCE`             | const    | `"workflow.advance"`                                                                                                                                                        |
| `JOB_TYPE_EFFECT_PREFIX`       | const    | `"workflow.effect."`                                                                                                                                                        |
| `PURE_ENTER_EVENT`             | const    | `"ENTER"` — synthetic event fired by driver into pure nodes                                                                                                                 |
| `DEFAULT_TENANT_ID`            | const    | `"_default"`                                                                                                                                                                |
| `EXECUTION_STATE`              | const    | `{ PENDING, RUNNING, WAITING, COMPLETED, FAILED, CANCELLED }`                                                                                                               |
| `HISTORY_EVENT`                | const    | Audit event-type strings (`created`, `transition`, `effect_dispatched`, ...)                                                                                                |
| `NodeMeta`                     | type     | Discriminated union: `pure` \| `effectful` \| `suspending` \| `terminal`                                                                                                    |
| `AdvanceKind`                  | type     | `"start" \| "effect" \| "timeout" \| "signal"` — what produced an advance; drives its preconditions                                                                         |
| `SchedulerTickResult`          | type     | `{ woken, repoked }` — what one scheduler tick poked                                                                                                                        |
| `WorkflowDefinition`           | type     | `{ id, version, fsm: FSMConfig }`                                                                                                                                           |
| `Handler`                      | type     | `(args: HandlerArgs) => Promise<HandlerResult> \| HandlerResult`                                                                                                            |
| `Matcher`                      | type     | `(args: MatcherArgs) => boolean \| Promise<boolean>`                                                                                                                        |
| `WorkflowInstanceRow`          | type     | Schema-aligned row type                                                                                                                                                     |
| `InboxRow`                     | type     | Schema-aligned row type                                                                                                                                                     |
| `HistoryRow`                   | type     | Schema-aligned row type                                                                                                                                                     |
| `WorkflowContext`              | type     | `Record<string, unknown>` — accumulated payload                                                                                                                             |
| `WorkflowSnapshot`             | type     | `FSMSnapshot<string, WorkflowContext>`                                                                                                                                      |

See [API.md](./API.md) for full signatures, options, and examples.

## Outcome Convention

The driver passes outcome labels as fsm events. Three labels have special meaning:

| Label     | Source                    | Used by                                             |
| --------- | ------------------------- | --------------------------------------------------- |
| `ENTER`   | Driver (auto)             | Pure nodes — wire guarded transitions on this event |
| `MATCHED` | `WorkflowInboxCorrelator` | Suspending nodes whose matcher returns true         |
| `TIMEOUT` | `WorkflowScheduler`       | Suspending nodes whose `wake_at` fires              |

Effectful node outcomes are whatever the handler returns (e.g. `LOW`, `OK`, `CONFIRMED`).

## DB Schema (managed by `createMigrate`)

| Table                   | Purpose                                    | Indexes                                                                                                                                                                           |
| ----------------------- | ------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `__workflow_instances`  | Live instance rows (incl. the `seq` fence) | `(tenant_id, execution_state, wake_at)` partial, `(tenant_id, execution_state, correlation_token)` partial, `(tenant_id, updated_at)` partial WHERE `execution_state = 'pending'` |
| `__workflow_inbox`      | Append-only intake of external signals     | `(tenant_id, correlation_token)` partial WHERE unprocessed                                                                                                                        |
| `__workflow_history`    | Append-only audit log                      | `(instance_id, at)`                                                                                                                                                               |
| `__workflow_migrations` | Active migration version                   | —                                                                                                                                                                                 |

Steve and Cron own their own tables (`__job*`, `__cron*`); managed by their respective libraries.

**Schema versions** (registered in `src/migrations/index.ts`, applied via `createMigrate(pool).up("latest")`):

| Version | Change                                                                                                                                                                                                                    |
| ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `1.0.0` | Creates the three framework tables + indexes.                                                                                                                                                                             |
| `1.1.0` | Renames `project_id` -> `tenant_id` on all three tables (in place, data preserved, idempotent, reversible).                                                                                                               |
| `1.2.0` | Adds `seq integer NOT NULL DEFAULT 0` (the fence) to `__workflow_instances` + the partial index for the stale-`pending` scan. Additive and non-volatile — on PG >= 11 a metadata-only change, existing rows start at `0`. |

Migration files are an append-only ledger: `1_0_0.ts` still creates `project_id` and is deliberately left untouched — `1_1_0.ts` performs the rename, so fresh installs and upgrades converge on the same schema.

`driver.ts` carries two transitional shims for jobs enqueued by an older version and still sitting in the queue at upgrade time. Both are removable together, once no pre-2.1 job can be queued anywhere:

- `payloadTenantId()` falls back to a legacy `project_id` key (pre-1.1.0 payloads).
- `advanceKind()` infers `kind` from the payload shape when it is absent (pre-fence payloads). Such a payload also has no `expected_seq`, i.e. it is **unfenced**: it runs without the fence check, and without the per-`kind` preconditions — guessing the kind must not swallow the job.

## Runtime Ownership

The framework **does not start or stop steve.Jobs or cron.Cron.** The consumer constructs both, passes them in, and runs their lifecycle:

```ts
const jobs = new Jobs({ db: pool, autoCleanup: true });  // consumer-owned
const cron = new Cron({ db: pool });                     // consumer-owned

const wf         = new Workflow({ db: pool, jobs, ... });            // setHandler on jobs
const scheduler  = new WorkflowScheduler({ cron, workflow: wf });    // register tick on cron
const correlator = new WorkflowInboxCorrelator({ cron, workflow: wf });
await scheduler.register();
await correlator.register();

await jobs.start(4);   // consumer
await cron.start(2);   // consumer

// ... later ...
await cron.stop();     // consumer
await jobs.stop();     // consumer
```

`autoCleanup` is load-bearing, not a tuning knob: steve's reaper is what turns a crashed worker into an `expired` job, which is what the framework's re-dispatch reacts to. Default reap threshold is 5 minutes of `running`; the check interval defaults to 60s.

The consumer's other steve job types and cron jobs coexist on the same `Jobs` / `Cron` instances — handlers are keyed by `type` (steve) / `name` (cron), and the workflow's are namespaced (`workflow.advance`, `workflow.effect.<name>`, `workflow.scheduler.<tenantId>`, `workflow.correlator.<tenantId>`) so collisions are unlikely.

**Both runtimes noop what they cannot handle**, which makes a second unregistered runtime a silent data-loss bug rather than a loud one:

- A `Jobs` instance polling a `type` it doesn't know falls back to a noop handler that marks the job completed ([steve/jobs.ts](file:///Users/mm/projects/@marianmeres/steve/src/steve/jobs.ts)). What that costs depends on which job was eaten: a `timeout`/`signal` poke is re-poked next tick and a `start` advance is recovered by the stale-`pending` scan, but a noop'd **effect job or effect-completion advance strands the instance in `running` with nothing left to move it** — the reaper never sees it, because the job is `completed`.
- A `Cron` instance claims due rows **globally, regardless of tenant**, and runs a noop with a `warn` for a `name` it has no handler for ([cron/cron.ts](file:///Users/mm/projects/@marianmeres/cron/src/cron/cron.ts)) — then advances `next_run_at` as if the tick had run. A process that starts a `Cron` without calling `scheduler.register()` / `correlator.register()` therefore _steals_ ticks from the process that did.

Rule: one `Jobs` and one `Cron` per process, and every process that _starts_ them on these tables constructs the `Workflow` and registers the ticks. A producer-only process (`create`, `appendInbox`) still needs a `Jobs` to construct the `Workflow` — it just never calls `jobs.start()` / `cron.start()`.

## Critical Conventions

1. **Definitions are pure data.** Handlers and matchers are referenced by string name. Never store function pointers in a definition. Litmus test: `JSON.stringify(def)` must lose no meaning. Threading a handler's output into the context needs no `action` hook either — the handler returns `HandlerResult.context`, a shallow patch merged before the outcome transition.
2. **Version-pin instances.** Every instance carries `(definition_id, definition_version)`. Never edit an in-use definition in place — bump the version. Old versions stay registered until the last instance using them completes.
3. **Handlers must be idempotent.** The `seq` fence protects the instance row, not the outside world: a worker that finished the side effect and died before steve recorded the attempt gets retried, and the retry is _not_ stale (the instance never left that `seq`). Side effects must be safe to repeat.
4. **No fsm `onEnter`/`onExit` hooks for side effects.** Driver re-constructs FSMs via `FSM.fromSnapshot()`, which skips `onEnter`. Side effects belong in effectful handlers — they're the only way to get durable, retryable, observable execution.
5. **Cursor + execution_state are separate columns.** Don't fold one into the other.
6. **Outcome labels are fsm events.** The driver calls `fsm.transition(outcome, data)`. If fsm rejects (unknown event in current state), the instance is marked `failed` and history records `transition_rejected`.
7. **Correlation token is the index, matcher is the gate.** Token alone is not sufficient — the matcher does the semantic check.
8. **Only the advance writes instance rows.** Anything else that wants an instance moved enqueues an advance and lets it re-check under the lock. A new settle-point write must pass `{ bumpSeq: true }` to `updateInstance` and a new job payload must carry the fence, or duplicate delivery stops being safe.
9. **Fenced-out work is not an event.** Duplicate/stale jobs and pokes get a `clog.debug`, never a history row — with re-poking ticks they are routine, and history is an event log, not a trace.

## Before Making Changes

- [ ] Check existing patterns in [src/](./src/) — small files, plain SQL, explicit transactions
- [ ] Run `deno task test` (set `TEST_PG_*` env first; see `.env` example)
- [ ] If adding a new history event type, add it to `HISTORY_EVENT` in [src/types.ts](./src/types.ts) and emit it consistently
- [ ] If changing schema, write a new migration in [src/migrations/](./src/migrations/) — never edit `1_0_0.ts` in place

## Dependency Versions

| Package                | Min       | Why                                                           |
| ---------------------- | --------- | ------------------------------------------------------------- |
| `@marianmeres/fsm`     | `^3.1.0`  | Requires `state.meta` and `FSM.fromSnapshot` (added in 3.1.0) |
| `@marianmeres/steve`   | `^3.0.0`  | Job queue (advance + effect dispatch)                         |
| `@marianmeres/cron`    | `^3.2.0`  | Scheduler + correlator ticks                                  |
| `@marianmeres/migrate` | `^1.3.1`  | Schema versioning                                             |
| `@marianmeres/clog`    | `^3.21.0` | Logging                                                       |
| `pg`                   | `^8.23.0` | PostgreSQL client                                             |

## Failure Modes & Recovery

| Failure                                                                                         | Behavior                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| ----------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Handler throws                                                                                  | Steve retries the job `effectMaxAttempts` times. On terminal failure (`status: failed`), `Workflow`'s `onDone` listener marks the instance `failed` + appends `effect_failed` history.                                                                                                                                                                                                                                                                                                    |
| Advance job throws                                                                              | Steve retries the job `advanceMaxAttempts` times (default 10 — ~17min of exponential backoff, so a brief DB outage costs nothing). When they are exhausted, `onDone` marks the instance `failed` with the last attempt's `error_message`.                                                                                                                                                                                                                                                 |
| fsm rejects outcome                                                                             | Driver appends `transition_rejected` history, marks instance `failed`.                                                                                                                                                                                                                                                                                                                                                                                                                    |
| Pure node with no matching guard                                                                | Driver appends `transition_rejected` history, marks instance `failed`.                                                                                                                                                                                                                                                                                                                                                                                                                    |
| Pure-state cycle (>64 hops)                                                                     | Driver appends `failed` history with "exceeded MAX_PURE_HOPS" reason.                                                                                                                                                                                                                                                                                                                                                                                                                     |
| Definition missing on advance                                                                   | Driver marks instance `failed`. (Pin-and-freeze: keep old versions registered until drained.)                                                                                                                                                                                                                                                                                                                                                                                             |
| Crashed worker mid-job (advance or effect)                                                      | Steve's reaper marks the job `expired` — **terminal, never retried by steve**. `Workflow`'s `onDone` re-queues the identical payload instead, `redispatchLimit` dispatches in total (default 3), then fails the instance. Safe because the fence no-ops a re-dispatch whose original did commit. **Requires the reaper to run at all**: `new Jobs({ db, autoCleanup: true })`, or a periodic `jobs.cleanup()`. Without it the job sits in `running` forever and the instance never moves. |
| Lost advance after effect (the side effect happened, `jobs.create('workflow.advance')` did not) | A throwing enqueue → steve retries the effect job; a dead process → the job expires and is re-dispatched. Either way the fence lets the retry through — the instance never left that `seq` — so the handler runs again and re-enqueues the advance. Handlers must be idempotent.                                                                                                                                                                                                          |
| Crash between `create()`'s commit and steve's job insert                                        | The instance sits in `pending` with no job. The scheduler's second scan re-pokes it after `stalePendingSec` (default 300s; `0` disables).                                                                                                                                                                                                                                                                                                                                                 |
| Duplicate advance / effect job                                                                  | Fenced out by `seq`: debug-logged no-op, no history row. Duplicates are routine (every tick re-pokes what still looks due), so they are not events.                                                                                                                                                                                                                                                                                                                                       |
| Second `Jobs` / `Cron` without the workflow handlers                                            | Silent loss — see [Runtime Ownership](#runtime-ownership).                                                                                                                                                                                                                                                                                                                                                                                                                                |

## Testing

All tests run against a real PostgreSQL and skip gracefully when `TEST_PG_DATABASE` / `TEST_PG_USER` are unset — the driver is SQL-bound, so mocking it would test nothing. [stock_replenishment.test.ts](./tests/stock_replenishment.test.ts) covers happy/timeout/failure paths, [durability.test.ts](./tests/durability.test.ts) the fence and the recovery paths, [correlator.test.ts](./tests/correlator.test.ts) the signal semantics.

Per-tick helpers `WorkflowScheduler.tickOnce()` and `WorkflowInboxCorrelator.tickOnce()` are exposed for tests so they don't have to wait for cron expressions. Durability behavior is generally testable without a running `Jobs`: enqueue nothing, call `runAdvance` / `runEffect` directly with a hand-built payload, and assert on the row + history.

## Glossary

| Term              | Meaning                                                                                                              |
| ----------------- | -------------------------------------------------------------------------------------------------------------------- |
| Definition        | Static, versioned, declarative shape of a workflow. Pure data                                                        |
| Instance          | Single running occurrence of a definition; one DB row                                                                |
| Driver            | The engine (`workflow.advance` steve handler) that advances instances                                                |
| Cursor            | The instance's current node id                                                                                       |
| Execution state   | The instance's lifecycle status                                                                                      |
| Outcome           | The label a handler returns (or `MATCHED`/`TIMEOUT`/`ENTER`); fed to fsm as the event name                           |
| Effect port       | A typed interface to the outside world (email, AI, HTTP) — userland concern                                          |
| Inbox             | Append-only `__workflow_inbox` intake of external signals                                                            |
| Correlation token | Unique token that maps an external event back to a waiting instance                                                  |
| Scheduler         | Cron-driven loop that wakes time-suspended instances                                                                 |
| Correlator        | Cron-driven loop that matches inbox signals to waiting instances                                                     |
| Fence             | The `seq` column. Bumped by every settle-point write; carried by every job, so a stale one is recognized and dropped |
| Poke              | A `workflow.advance` job enqueued by something that will not write the row itself. The advance re-checks and decides |
| Settle point      | The end of an advance: terminal / effectful / suspending. Where the instance row is written and `seq` bumped         |
| Defer             | Correlator verdict: leave the inbox row unprocessed and re-examine it next tick                                      |
| Pin-and-freeze    | Versioning strategy: every instance runs on its birth-version forever                                                |
