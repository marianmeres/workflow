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
│   ├── instances.ts        # CRUD + lock + claim due wake-ups + find by correlation
│   ├── inbox.ts            # append, claim-unprocessed, mark-processed
│   ├── history.ts          # append + read
│   └── tx.ts               # withTransaction helper
└── migrations/
    ├── 1_0_0.ts            # CREATE/DROP three tables
    ├── 1_1_0.ts            # rename project_id -> tenant_id (in place)
    └── index.ts            # createMigrate(pool) — @marianmeres/migrate setup
tests/
├── _pg.ts                  # env-driven pool + schema reset
├── fixtures/
│   ├── stock-replenishment.ts  # reference workflow used by tests only
│   └── weekly-digest.ts        # recurring-trigger reference workflow
├── migrations.test.ts          # schema rename up/down + legacy payload drain
├── stock_replenishment.test.ts # happy/timeout/fail paths + validator
└── weekly_digest.test.ts       # recurring trigger via cron
```

## Conceptual Model — Three Layers

| Layer | What it is | Lives in |
|---|---|---|
| **Definition** | Static graph: nodes, transitions, handler ids (strings). Pure data, JSON-safe. | Code, registered in `WorkflowRegistry` |
| **Instance** | Live state of one running occurrence. Cursor, context, execution_state, wake_at, correlation_token. | `__workflow_instances` row |
| **Driver** | Event-driven engine that advances instances. Owns no state. | `workflow.advance` steve job |

## Two Orthogonal States

Never conflate. Always two columns on `__workflow_instances`:

| Column | Meaning | Source of truth for |
|---|---|---|
| `cursor` | Which node am I at? | The FSM's `state` (`getSnapshot().state`) |
| `execution_state` | What's my lifecycle status? | `pending` / `running` / `waiting` / `completed` / `failed` / `cancelled` |

An instance can be at node `await_reply` (cursor) and simultaneously `waiting` (execution_state).

## Node Taxonomy (`NodeMeta`)

Each FSM state's `meta` field carries a discriminated `NodeMeta`. The driver dispatches on `meta.kind`:

| Kind | Driver behavior | Required fields |
|---|---|---|
| `pure` | Auto-fire `ENTER` event; transition by guards. Loop inline until terminal/effectful/suspending. Max 64 hops. | none |
| `effectful` | Enqueue `workflow.effect.<handler>` steve job, flip to `running`. On completion, an `advance` is enqueued with `{ outcome, outcome_data }`. | `handler: string` |
| `suspending` | Set `execution_state='waiting'`, `wake_at` (if `timeoutSec`), keep `correlation_token`. Wakes via scheduler (`TIMEOUT`) or correlator (`MATCHED`). | optional `matcher`, optional `timeoutSec` |
| `terminal` | Set `execution_state='completed'`. | none |

## Public API (from `src/mod.ts`)

| Export | Type | Purpose |
|---|---|---|
| `Workflow` | class | Main driver: attaches handlers to an injected `steve.Jobs`. Exposes `create/find/appendInbox/enqueueAdvance/enqueueEffect`. **No own start/stop** — consumer runs the Jobs. |
| `WorkflowScheduler` | class | Attaches a wake tick to an injected `cron.Cron`. Exposes `register/unregister/tickOnce`. **No own start/stop** — consumer runs the Cron. |
| `WorkflowInboxCorrelator` | class | Attaches a match tick to an injected `cron.Cron`. Exposes `register/unregister/tickOnce`. **No own start/stop** — consumer runs the Cron. |
| `WorkflowRegistry` | class | Immutable registry of definitions + handlers + matchers; validates on construction |
| `validateDefinition` | function | Standalone validator usable outside `WorkflowRegistry` |
| `defKey(id, version)` | function | `"id@version"` registry key helper |
| `createMigrate(pool)` | function | Builds `@marianmeres/migrate` instance pre-loaded with schema versions |
| `getHistory(exec, id, limit?)` | function | Reads `__workflow_history` rows for an instance (observability) |
| `effectJobType(name)` | function | Steve job-type string for an effect handler: `workflow.effect.<name>` |
| `JOB_TYPE_ADVANCE` | const | `"workflow.advance"` |
| `JOB_TYPE_EFFECT_PREFIX` | const | `"workflow.effect."` |
| `PURE_ENTER_EVENT` | const | `"ENTER"` — synthetic event fired by driver into pure nodes |
| `DEFAULT_TENANT_ID` | const | `"_default"` |
| `EXECUTION_STATE` | const | `{ PENDING, RUNNING, WAITING, COMPLETED, FAILED, CANCELLED }` |
| `HISTORY_EVENT` | const | Audit event-type strings (`created`, `transition`, `effect_dispatched`, ...) |
| `NodeMeta` | type | Discriminated union: `pure` \| `effectful` \| `suspending` \| `terminal` |
| `WorkflowDefinition` | type | `{ id, version, fsm: FSMConfig }` |
| `Handler` | type | `(args: HandlerArgs) => Promise<HandlerResult> \| HandlerResult` |
| `Matcher` | type | `(args: MatcherArgs) => boolean \| Promise<boolean>` |
| `WorkflowInstanceRow` | type | Schema-aligned row type |
| `InboxRow` | type | Schema-aligned row type |
| `HistoryRow` | type | Schema-aligned row type |
| `WorkflowContext` | type | `Record<string, unknown>` — accumulated payload |
| `WorkflowSnapshot` | type | `FSMSnapshot<string, WorkflowContext>` |

See [API.md](./API.md) for full signatures, options, and examples.

## Outcome Convention

The driver passes outcome labels as fsm events. Three labels have special meaning:

| Label | Source | Used by |
|---|---|---|
| `ENTER` | Driver (auto) | Pure nodes — wire guarded transitions on this event |
| `MATCHED` | `WorkflowInboxCorrelator` | Suspending nodes whose matcher returns true |
| `TIMEOUT` | `WorkflowScheduler` | Suspending nodes whose `wake_at` fires |

Effectful node outcomes are whatever the handler returns (e.g. `LOW`, `OK`, `CONFIRMED`).

## DB Schema (managed by `createMigrate`)

| Table | Purpose | Indexes |
|---|---|---|
| `__workflow_instances` | Live instance rows | `(tenant_id, execution_state, wake_at)` partial, `(tenant_id, execution_state, correlation_token)` partial |
| `__workflow_inbox` | Append-only intake of external signals | `(tenant_id, correlation_token)` partial WHERE unprocessed |
| `__workflow_history` | Append-only audit log | `(instance_id, at)` |
| `__workflow_migrations` | Active migration version | — |

Steve and Cron own their own tables (`__job*`, `__cron*`); managed by their respective libraries.

**Schema versions** (registered in `src/migrations/index.ts`, applied via `createMigrate(pool).up("latest")`):

| Version | Change |
|---|---|
| `1.0.0` | Creates the three framework tables + indexes. |
| `1.1.0` | Renames `project_id` -> `tenant_id` on all three tables (in place, data preserved, idempotent, reversible). |

Migration files are an append-only ledger: `1_0_0.ts` still creates `project_id` and is deliberately left untouched — `1_1_0.ts` performs the rename, so fresh installs and upgrades converge on the same schema.

`driver.ts` reads the tenant off a job payload via `payloadTenantId()`, which falls back to a legacy `project_id` key so jobs enqueued before 1.1.0 still drain. Transitional — removable once no pre-1.1.0 job can be queued.

## Runtime Ownership

The framework **does not start or stop steve.Jobs or cron.Cron.** The consumer constructs both, passes them in, and runs their lifecycle:

```ts
const jobs = new Jobs({ db: pool });          // consumer-owned
const cron = new Cron({ db: pool });          // consumer-owned

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

The consumer's other steve job types and cron jobs coexist on the same `Jobs` / `Cron` instances — handlers are keyed by `type` (steve) / `name` (cron), and the workflow's are namespaced (`workflow.advance`, `workflow.effect.<name>`, `workflow.scheduler.<tenantId>`, `workflow.correlator.<tenantId>`) so collisions are unlikely. Running parallel `Jobs` instances on the same `__job` table is **unsafe**: an instance polling a type it doesn't know falls back to a noop handler that silently marks the job completed (see [steve/jobs.ts:499](file:///Users/mm/projects/@marianmeres/steve/src/steve/jobs.ts)). Always share.

## Critical Conventions

1. **Definitions are pure data.** Handlers and matchers are referenced by string name. Never store function pointers in a definition. Litmus test: `JSON.stringify(def)` must lose no meaning.
2. **Version-pin instances.** Every instance carries `(definition_id, definition_version)`. Never edit an in-use definition in place — bump the version. Old versions stay registered until the last instance using them completes.
3. **Handlers must be idempotent.** Steve may retry handlers if a worker crashes mid-execution. Side effects must be safe to repeat.
4. **No fsm `onEnter`/`onExit` hooks for side effects.** Driver re-constructs FSMs via `FSM.fromSnapshot()`, which skips `onEnter`. Side effects belong in effectful handlers — they're the only way to get durable, retryable, observable execution.
5. **Cursor + execution_state are separate columns.** Don't fold one into the other.
6. **Outcome labels are fsm events.** The driver calls `fsm.transition(outcome, data)`. If fsm rejects (unknown event in current state), the instance is marked `failed` and history records `transition_rejected`.
7. **Correlation token is the index, matcher is the gate.** Token alone is not sufficient — the matcher does the semantic check.

## Before Making Changes

- [ ] Check existing patterns in [src/](./src/) — small files, plain SQL, explicit transactions
- [ ] Run `deno task test` (set `TEST_PG_*` env first; see `.env` example)
- [ ] If adding a new history event type, add it to `HISTORY_EVENT` in [src/types.ts](./src/types.ts) and emit it consistently
- [ ] If changing schema, write a new migration in [src/migrations/](./src/migrations/) — never edit `1_0_0.ts` in place

## Dependency Versions

| Package | Min | Why |
|---|---|---|
| `@marianmeres/fsm` | `^3.1.0` | Requires `state.meta` and `FSM.fromSnapshot` (added in 3.1.0) |
| `@marianmeres/steve` | `^3.0.0` | Job queue (advance + effect dispatch) |
| `@marianmeres/cron` | `^3.2.0` | Scheduler + correlator ticks |
| `@marianmeres/migrate` | `^1.3.1` | Schema versioning |
| `@marianmeres/clog` | `^3.21.0` | Logging |
| `pg` | `^8.23.0` | PostgreSQL client |

## Failure Modes & Recovery

| Failure | Behavior |
|---|---|
| Handler throws | Steve retries per `effectMaxAttempts`. On terminal failure, `Workflow`'s `onDone` listener marks the instance `failed` + appends `effect_failed` history. |
| fsm rejects outcome | Driver appends `transition_rejected` history, marks instance `failed`. |
| Pure node with no matching guard | Driver appends `transition_rejected` history, marks instance `failed`. |
| Pure-state cycle (>64 hops) | Driver appends `failed` history with "exceeded MAX_PURE_HOPS" reason. |
| Definition missing on advance | Driver marks instance `failed`. (Pin-and-freeze: keep old versions registered until drained.) |
| Crashed worker mid-effect | Steve auto-cleanup reaps stuck `running` jobs. Steve retries per `max_attempts`. |
| Lost advance after effect (process dies between handler success and `jobs.create('workflow.advance')`) | Steve retries the effect job; handler runs again; advance enqueued. Handlers must be idempotent. |

## Testing

Integration tests in [tests/stock_replenishment.test.ts](./tests/stock_replenishment.test.ts) cover happy, timeout, and failure paths against a real PostgreSQL. Skip gracefully when `TEST_PG_DATABASE` / `TEST_PG_USER` are unset.

Per-tick helpers `WorkflowScheduler.tickOnce()` and `WorkflowInboxCorrelator.tickOnce()` are exposed for tests so they don't have to wait for cron expressions.

## Glossary

| Term | Meaning |
|---|---|
| Definition | Static, versioned, declarative shape of a workflow. Pure data |
| Instance | Single running occurrence of a definition; one DB row |
| Driver | The engine (`workflow.advance` steve handler) that advances instances |
| Cursor | The instance's current node id |
| Execution state | The instance's lifecycle status |
| Outcome | The label a handler returns (or `MATCHED`/`TIMEOUT`/`ENTER`); fed to fsm as the event name |
| Effect port | A typed interface to the outside world (email, AI, HTTP) — userland concern |
| Inbox | Append-only `__workflow_inbox` intake of external signals |
| Correlation token | Unique token that maps an external event back to a waiting instance |
| Scheduler | Cron-driven loop that wakes time-suspended instances |
| Correlator | Cron-driven loop that matches inbox signals to waiting instances |
| Pin-and-freeze | Versioning strategy: every instance runs on its birth-version forever |
