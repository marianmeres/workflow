# API Reference

Full public API for [`@marianmeres/workflow`](./README.md).

## Table of Contents

- [Classes](#classes)
  - [Workflow](#workflow)
  - [WorkflowScheduler](#workflowscheduler)
  - [WorkflowInboxCorrelator](#workflowinboxcorrelator)
  - [WorkflowRegistry](#workflowregistry)
- [Functions](#functions)
  - [createMigrate](#creatempool)
  - [validateDefinition](#validatedefinitiondef-available)
  - [defKey](#defkeyid-version)
  - [effectJobType](#effectjobtypename)
- [Types](#types)
  - [WorkflowDefinition](#workflowdefinition)
  - [NodeMeta](#nodemeta)
  - [Handler](#handler--handlerargs--handlerresult)
  - [Matcher](#matcher--matcherargs)
  - [WorkflowInstanceRow](#workflowinstancerow)
  - [InboxRow](#inboxrow)
  - [HistoryRow](#historyrow)
  - [Job payloads](#advancejobpayload--effectjobpayload)
- [Constants](#constants)

---

## Classes

### `Workflow`

The user-facing API for creating workflow instances and appending inbox signals. Attaches handlers (`workflow.advance` + `workflow.effect.<name>`) to an **externally-owned** `@marianmeres/steve` `Jobs` instance via `setHandler` at construction time. The consumer owns the Jobs lifecycle.

```typescript
import { Workflow } from "@marianmeres/workflow";
import { Jobs } from "@marianmeres/steve";
```

#### Constructor

```typescript
new Workflow(options: WorkflowOptions)
```

**`WorkflowOptions`:**

| Field                        | Type                      | Default      | Description                                                                                                                                                  |
| ---------------------------- | ------------------------- | ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `db`                         | `pg.Pool`                 | required     | PostgreSQL pool. Used for direct SQL (persistence helpers, transactions).                                                                                    |
| `jobs`                       | `Jobs`                    | required     | Externally-owned `steve.Jobs` instance. Workflow registers its handlers on it via `setHandler`. The consumer runs `jobs.start()` / `jobs.stop()`.            |
| `tenantId`                   | `string`                  | `"_default"` | Multi-tenant scope.                                                                                                                                          |
| `definitions`                | `WorkflowDefinition[]`    | required     | Definitions to register. Validated on construction.                                                                                                          |
| `handlers`                   | `Record<string, Handler>` | required     | Effect handlers keyed by name.                                                                                                                               |
| `matchers`                   | `Record<string, Matcher>` | `{}`         | Signal matchers keyed by name.                                                                                                                               |
| `effectMaxAttempts`          | `number`                  | `3`          | Max retries for effect handlers (steve `max_attempts`).                                                                                                      |
| `effectMaxAttemptDurationMs` | `number`                  | `0`          | Per-attempt timeout in ms (`0` = none).                                                                                                                      |
| `advanceMaxAttempts`         | `number`                  | `10`         | Max retries for `workflow.advance` jobs (steve `max_attempts`). Steve's exponential backoff over 10 attempts spans ~17 minutes of database-outage tolerance. |
| `redispatchLimit`            | `number`                  | `3`          | How many times one job payload may be dispatched, counting the original, before an expiry fails the instance.                                                |

Throws on construction if any definition references a handler/matcher not present in the maps, or if any other structural problem is found (unknown transition target, missing terminal state, etc.). See [`validateDefinition`](#validatedefinitiondef-available).

**One `Workflow` per `Jobs`.** Steve's `setHandler` is last-writer-wins, so a second `Workflow` on the same `Jobs` would take over `workflow.advance` and fail the first one's instances with "Unknown workflow definition". Constructing it throws instead; call [`detach()`](#detach-void) on the incumbent first, or give the second one its own `Jobs`.

The constructor also subscribes (`jobs.onDone`) to the terminal outcomes of `workflow.advance` and of each `workflow.effect.<name>` type:

- **`failed`** (steve exhausted the attempts) — the instance is marked `failed`, with `effect_failed` history for an effect job and `failed` history carrying the last attempt's error message for an advance.
- **`expired`** (the worker died mid-run; steve never retries those) — the identical payload is re-queued, up to `redispatchLimit` dispatches in total, and only then is the instance failed. A re-dispatch whose original did commit before the crash is fenced out by `seq`, so it costs nothing.

Note that jobs only ever reach `expired` if something reaps them: run the `Jobs` instance with `autoCleanup` or call `jobs.cleanup()` periodically. Without it a crashed worker leaves the instance `running` indefinitely.

#### Properties

| Name       | Type               | Description                                        |
| ---------- | ------------------ | -------------------------------------------------- |
| `db`       | `pg.Pool`          | The provided pool.                                 |
| `jobs`     | `Jobs`             | The injected `Jobs` instance.                      |
| `tenantId` | `string`           | Scope.                                             |
| `registry` | `WorkflowRegistry` | Read-only access to definitions/handlers/matchers. |

#### Methods

> **No `start()` or `stop()`.** The consumer manages the `Jobs` lifecycle.

##### `create(input): Promise<WorkflowInstanceRow>`

Creates a new workflow instance, appends a `created` history entry, and enqueues the first `workflow.advance` job.

| Field               | Type                         | Description                                                                                                                                                                                                                                             |
| ------------------- | ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `definitionId`      | `string`                     | Must be registered.                                                                                                                                                                                                                                     |
| `definitionVersion` | `string`                     | Must be registered.                                                                                                                                                                                                                                     |
| `context`           | `WorkflowContext` (optional) | Shallow-merged over the definition's `fsm.context` defaults (top-level keys replace). Default: the defaults alone, or `{}` if the definition declares none.                                                                                             |
| `correlationToken`  | `string \| null` (optional)  | Set up-front for signal-suspending nodes that need to be matchable before the workflow reaches them (e.g. outbound emails using UUID subaddressing). A handler can set or clear it later — see [`HandlerResult`](#handler--handlerargs--handlerresult). |

##### `find(id: string): Promise<WorkflowInstanceRow | null>`

Looks up an instance by id, scoped to this Workflow's `tenantId`. Returns `null` if not found or scoped to a different tenant.

##### `cancel(id: string, reason?: string): Promise<boolean>`

Operator escape hatch: aborts a live instance. The row goes `cancelled` (terminal), loses its `wake_at` and its correlation token, and gets a `cancelled` history row carrying `reason`. The `seq` bump makes every job already in flight for it stale — a queued effect job resolves as `{ skipped: "stale" }` without calling its handler, and a late completion is dropped.

Returns `false` when the instance does not exist, belongs to another tenant, or is already terminal.

##### `retry(id: string, opts?: { force?: boolean }): Promise<boolean>`

Operator escape hatch: resumes a `failed` instance from its current cursor instead of starting a new one and replaying every side effect from the top. The row goes back to `pending`, gets a `retried` history row carrying `from_state`, and a fresh `start` advance re-runs whatever the cursor node calls for — typically re-dispatching its effect.

`force: true` also allows a `running` instance, i.e. the operator asserting that its effect job is dead (a worker crashed and nothing reaps the job — see `autoCleanup`). Safe by construction: the `seq` bump makes the zombie stale, so a late completion is dropped.

Returns `false` when the instance does not exist, belongs to another tenant, or is in a state this does not cover. Retrying alone fixes nothing: an instance failed by a rejected transition or a deterministically throwing handler fails again unless the code changed.

##### `appendInbox(input): Promise<InboxRow>`

Appends an external signal to `__workflow_inbox`. The correlator's next tick matches it to a waiting instance, runs the matcher, and (on match) pokes an advance which delivers `MATCHED` with the row's payload and marks the row processed in one transaction. A signal that arrives before its wait point is deferred, not dropped — see [Per-tick behavior](#per-tick-behavior-1).

| Field              | Type                      | Description                                                    |
| ------------------ | ------------------------- | -------------------------------------------------------------- |
| `source`           | `string`                  | Free-form classifier, e.g. `"email"`, `"webhook"`.             |
| `correlationToken` | `string`                  | Index key into waiting instances.                              |
| `payload`          | `Record<string, unknown>` | Signal data, surfaced to the matcher and forwarded to the FSM. |

##### `detach(): void`

Removes everything the constructor registered on the `Jobs` instance — the `workflow.advance` and `workflow.effect.<name>` handlers plus the `onDone` subscriptions — and releases the one-Workflow-per-`Jobs` claim, so a replacement can be constructed on the same queue (tests, hot reload).

It does not stop the queue and does not touch running instances: jobs already enqueued stay in the table and fall back to steve's noop handler until something re-attaches. A no-op when this instance is not the attached one (already detached, or superseded).

##### `enqueueAdvance` / `enqueueEffect`

Internal `JobEnqueuer` interface methods used by the driver. Public so the scheduler and correlator can call them. You normally don't call these directly.

---

### `WorkflowScheduler`

Cron-driven scheduler that wakes time-suspended instances. Attaches its tick to an **externally-owned** `@marianmeres/cron` `Cron` instance when `register()` is called. Each tick is read-only: it selects due rows and pokes one fenced `TIMEOUT` advance per row, which does the actual write.

```typescript
import { WorkflowScheduler } from "@marianmeres/workflow";
import { Cron } from "@marianmeres/cron";
```

#### Constructor

```typescript
new WorkflowScheduler(options: WorkflowSchedulerOptions)
```

**`WorkflowSchedulerOptions`:**

| Field             | Type             | Default                         | Description                                                                                                                                                                                                               |
| ----------------- | ---------------- | ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cron`            | `Cron`           | required                        | Externally-owned `Cron` instance. Scheduler registers its tick on this via `cron.register`. The consumer runs `cron.start()` / `cron.stop()`.                                                                             |
| `workflow`        | `Workflow`       | required                        | The Workflow whose instances this scheduler wakes. `tenantId` and `db` are derived from it.                                                                                                                               |
| `tickExpression`  | `string`         | `"* * * * *"`                   | 5-field cron expression.                                                                                                                                                                                                  |
| `timezone`        | `string \| null` | host local                      | IANA timezone for the cron expression.                                                                                                                                                                                    |
| `tickBatchSize`   | `number`         | `100`                           | Max rows poked per tick, **per scan**.                                                                                                                                                                                    |
| `tickName`        | `string`         | `workflow.scheduler.<tenantId>` | Cron job name.                                                                                                                                                                                                            |
| `stalePendingSec` | `number`         | `300`                           | Age at which a `pending` instance is assumed stranded and re-poked with a fresh `start` advance. Covers the crash window between `create()`'s commit and steve's job insert (separate connection). `0` disables the scan. |

#### Methods

> **No `start()` or `stop()`.** The consumer manages the `Cron` lifecycle.

| Method                                     | Description                                                                                                                   |
| ------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------- |
| `register(): Promise<void>`                | Registers the tick on the injected `Cron`. Call once after construction. Idempotent (re-registering preserves `next_run_at`). |
| `unregister(): Promise<void>`              | Removes the tick from the injected `Cron`.                                                                                    |
| `tickOnce(): Promise<SchedulerTickResult>` | Runs one tick immediately. Exposed for tests and on-demand wakes.                                                             |

```typescript
interface SchedulerTickResult {
	woken: number; // due `waiting` rows poked with a `timeout` advance
	repoked: number; // stranded `pending` rows poked with a `start` advance
}
```

#### Per-tick behavior

Each tick is **read-only** against `__workflow_instances` and runs two scans:

1. `waiting` rows with `wake_at <= now()` → one `workflow.advance` poke each, `{ kind: 'timeout', outcome: 'TIMEOUT', expected_seq: row.seq }`.
2. If `stalePendingSec > 0`: `pending` rows older than that → `{ kind: 'start', expected_seq: row.seq }`.

The advance re-checks the precondition under the row lock (including that the timer really is due) and is what writes. So the counts are _pokes issued_, not instances moved, and a row poked but not yet advanced is simply poked again next tick — the fence turns the duplicate into a no-op. Nothing is stranded by a crash mid-tick.

---

### `WorkflowInboxCorrelator`

Cron-driven correlator that matches inbox signals to waiting instances. Attaches its tick to an externally-owned `Cron` instance when `register()` is called.

```typescript
import { WorkflowInboxCorrelator } from "@marianmeres/workflow";
import { Cron } from "@marianmeres/cron";
```

#### Constructor

```typescript
new WorkflowInboxCorrelator(options: WorkflowInboxCorrelatorOptions)
```

**`WorkflowInboxCorrelatorOptions`** takes the same fields as `WorkflowSchedulerOptions` minus `stalePendingSec` (cron / workflow / tickExpression / timezone / tickBatchSize / tickName), with `tickName` defaulting to `workflow.correlator.<tenantId>`.

#### Methods

> **No `start()` or `stop()`.** The consumer manages the `Cron` lifecycle.

| Method                        | Description                                                                                                                                                 |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `register(): Promise<void>`   | Registers the tick on the injected `Cron`. Call once after construction.                                                                                    |
| `unregister(): Promise<void>` | Removes the tick from the injected `Cron`.                                                                                                                  |
| `tickOnce(): Promise<number>` | Runs one tick immediately. Returns rows **resolved** — poked, rejected, or marked processed. Deferred rows are not counted; they are still there next tick. |

#### Per-tick behavior

Rows are claimed `ORDER BY received_at LIMIT tickBatchSize` with `FOR UPDATE SKIP LOCKED`. For each row, the correlator looks up the **live** (non-terminal) instance owning `correlation_token`:

| Instance                                            | Action                                                                                     |
| --------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| none, or terminal                                   | mark processed + warn log (plus `signal_rejected` history if a terminal instance is found) |
| live but not `waiting`                              | **defer**                                                                                  |
| `waiting` at a node with no `MATCHED` (or `*`) edge | **defer**                                                                                  |
| already poked earlier in this tick                  | **defer**                                                                                  |
| `waiting`, matcher throws                           | **defer** (error log)                                                                      |
| `waiting`, matcher returns `false`                  | `signal_rejected` history, mark processed                                                  |
| `waiting`, matcher returns `true` (or no matcher)   | poke an advance: `{ kind: 'signal', expected_seq, inbox_id }`                              |

**Defer** = leave the row unprocessed and look at it again next tick. An early signal — the instance is still running the step that emits the token, or sits at a timer-only node — is early, not wrong: consuming it would lose it, and delivering it to a node with no `MATCHED` edge would fail the instance.

The correlator **never writes the instance row** and never marks a delivered row processed. The advance does both: it locks the inbox row, reads `outcome_data` off it, transitions, appends `signal_received`, and marks it processed in one transaction. A crashed poke is simply re-poked next tick.

Pokes are enqueued _after_ the claiming transaction commits — the advance would otherwise block on the inbox lock this tick still holds.

Two known costs: a deterministically-throwing matcher retries and logs every tick, and a deferred backlog larger than `tickBatchSize` shadows newer rows.

---

### `WorkflowRegistry`

In-memory registry of definitions, handlers, and matchers. Built once at construction time; immutable after. You normally don't construct this directly — `Workflow` builds one internally.

```typescript
import { WorkflowRegistry } from "@marianmeres/workflow";
```

#### Constructor

```typescript
new WorkflowRegistry({
    definitions: WorkflowDefinition[],
    handlers: Record<string, Handler>,
    matchers?: Record<string, Matcher>,
})
```

Validates every definition against the registered handler/matcher names. Throws on the first structural problem.

#### Methods

| Method                           | Returns                           | Description                   |
| -------------------------------- | --------------------------------- | ----------------------------- |
| `getDefinition(id, version)`     | `WorkflowDefinition \| undefined` | Lookup.                       |
| `requireDefinition(id, version)` | `WorkflowDefinition`              | Throws if missing.            |
| `getHandler(name)`               | `Handler \| undefined`            | Lookup.                       |
| `requireHandler(name)`           | `Handler`                         | Throws if missing.            |
| `getMatcher(name)`               | `Matcher \| undefined`            | Lookup.                       |
| `requireMatcher(name)`           | `Matcher`                         | Throws if missing.            |
| `handlerNames()`                 | `string[]`                        | All registered handler names. |

---

## Functions

### `createMigrate(pool, options?)`

Builds a `@marianmeres/migrate` instance pre-loaded with the package's schema versions. The caller drives it (`.up('latest')`, `.down(...)`, `.status()`, etc.).

```typescript
import { createMigrate } from "@marianmeres/workflow";
```

**Parameters:**

- `pool` (`pg.Pool`) — required
- `options.logger` (`(...args: unknown[]) => void`, optional) — passed through to `Migrate`

**Returns:** `Migrate` — see [`@marianmeres/migrate`](https://jsr.io/@marianmeres/migrate)

**Example:**

```typescript
const migrate = createMigrate(pool);
await migrate.up("latest");
```

Active version is stored in `__workflow_migrations`. `1.0.0` creates `__workflow_instances`, `__workflow_inbox`, `__workflow_history` and their indexes; `1.1.0` renames `project_id` → `tenant_id`; `1.2.0` adds the `seq` fencing column and the partial index behind the scheduler's stale-`pending` scan. Upgrading an existing 2.0.x database needs `up("latest")` once — until then instances have `seq = 0` and every write bumps it from there.

---

### `validateDefinition(def, available)`

Standalone validator. Throws on the first structural problem.

**Parameters:**

- `def` (`WorkflowDefinition`)
- `available.handlers` (`ReadonlySet<string>`)
- `available.matchers` (`ReadonlySet<string>`)

Checks performed:

- `id`, `version` are non-empty strings.
- `fsm.initial` exists in `fsm.states`.
- Every state has a `meta` field with a known `kind`.
- Every `effectful` state names a handler in `available.handlers`.
- Every `suspending` state's matcher (if present) is in `available.matchers`.
- Every transition target resolves to a defined state.
- At least one `terminal` state exists.
- Every node has an edge for the events it is guaranteed to receive — each of these would otherwise be rejected by the FSM at runtime, failing the instance after any side effect at that node had already run:
  - `pure` → an `ENTER` (or `*`) transition.
  - `effectful` → at least one transition, or every handler outcome is rejected.
  - `suspending` → a `MATCHED` (or `*`) transition when `meta.matcher` is set; a `TIMEOUT` (or `*`) one when `meta.timeoutSec` is set; and at least one of the two, or the node can never wake. `timeoutSec`, when present, must be a finite number greater than `0`.
  - `terminal` → no transitions at all.

---

### `defKey(id, version)`

Composite key used in the definition registry.

**Returns:** `"id@version"` (string)

---

### `effectJobType(name)`

Builds the steve job-type string for an effect handler.

**Returns:** `"workflow.effect.<name>"` (string)

---

### `getHistory(exec, instanceId, limit?)`

Reads the append-only history rows for an instance in chronological order. Useful for `/admin/workflows`-style observability views.

**Parameters:**

- `exec` (`pg.Pool | pg.PoolClient | pg.Client`)
- `instanceId` (`string`)
- `limit` (`number`, optional) — Default: `200`

**Returns:** `Promise<HistoryRow[]>`

**Example:**

```typescript
import { getHistory } from "@marianmeres/workflow";
const events = await getHistory(pool, instance.id);
for (const e of events) {
	console.log(e.at.toISOString(), e.event_type, e.from_node, "→", e.to_node);
}
```

---

## Types

### `WorkflowDefinition`

```typescript
type WorkflowDefinition<TState extends string = string, TEvent extends string = string> =
	{
		id: string;
		version: string;
		fsm: FSMConfig<TState, TEvent, WorkflowContext>;
	};
```

The `fsm` field is a [`@marianmeres/fsm`](https://jsr.io/@marianmeres/fsm) config. Each state config carries `meta: NodeMeta` — the FSM passes it through, the workflow driver dispatches on it.

---

### `NodeMeta`

```typescript
type NodeMeta =
	| { kind: "pure" }
	| { kind: "effectful"; handler: string }
	| { kind: "suspending"; matcher?: string; timeoutSec?: number }
	| { kind: "terminal" };
```

Discriminated union of node kinds. The driver dispatches on `kind`:

| Kind         | Driver behavior                                                                         |
| ------------ | --------------------------------------------------------------------------------------- |
| `pure`       | Auto-fires `ENTER`. User wires guarded transitions. Inline-expands.                     |
| `effectful`  | Enqueues a steve job for `handler`. Waits for completion.                               |
| `suspending` | Parks the instance. Wakes on `wake_at` (`TIMEOUT`) or matched inbox signal (`MATCHED`). |
| `terminal`   | Marks instance `completed`.                                                             |

---

### `Handler` / `HandlerArgs` / `HandlerResult`

```typescript
type Handler = (args: HandlerArgs) => Promise<HandlerResult> | HandlerResult;

interface HandlerArgs {
	instanceId: string;
	tenantId: string;
	context: WorkflowContext;
	signal?: AbortSignal;
}

interface HandlerResult {
	outcome: string;
	data?: Record<string, unknown>;
	correlationToken?: string | null;
	context?: Partial<WorkflowContext>;
}
```

Handlers are async functions referenced by string name from `effectful` node metas. The driver passes the AbortSignal from steve — long-running handlers should observe it for cooperative cancellation.

The returned `outcome` is the FSM event name that drives the next transition. `data` is forwarded as the transition payload (available in guards/actions if the user wires them).

`context` is a shallow patch (top-level keys replace) merged into the instance context _before_ the outcome transition is applied, so guards and actions on the outcome edge already see it and it is persisted at the next settle point. `data` is never merged automatically — a handler that wants a value kept for later nodes returns it in `context`, instead of an fsm `action` that copies it out of the payload.

`correlationToken` sets the instance's token, written at the settle point this outcome leads to — so a wait point can key on something that only exists once the effect has run (an SMTP `Message-ID`, a payment-provider session id) and is signallable the moment it becomes `waiting`. `null` clears the token; omitting the field leaves whatever `create({ correlationToken })` set. The value is recorded in the `transition` history row's data.

**Idempotency:** handlers can run multiple times in failure scenarios. Design them accordingly.

---

### `Matcher` / `MatcherArgs`

```typescript
type Matcher = (args: MatcherArgs) => boolean | Promise<boolean>;

interface MatcherArgs {
	instanceId: string;
	tenantId: string;
	context: WorkflowContext;
	signal: InboxRow;
}
```

Matchers are predicate functions referenced from `suspending` node metas. The correlation token alone is just an index lookup — the matcher is the _semantic_ gate.

---

### `WorkflowInstanceRow`

```typescript
interface WorkflowInstanceRow {
	id: string;
	tenant_id: string;
	definition_id: string;
	definition_version: string;
	cursor: string; // current FSM state
	previous_cursor: string | null; // feeds FSM.fromSnapshot's snapshot.previous
	context: WorkflowContext;
	execution_state: ExecutionState;
	wake_at: Date | null;
	correlation_token: string | null;
	seq: number; // fencing token
	created_at: Date;
	updated_at: Date;
}
```

Schema-aligned. `cursor` and `execution_state` are deliberately separate columns — see [Two Orthogonal States](./AGENTS.md#two-orthogonal-states).

`seq` is the fencing token (schema 1.2.0). Every settle-point write bumps it; jobs carry the value they were issued against, so a duplicate or zombie job is recognized and dropped instead of being applied to a row that has moved on.

---

### `InboxRow`

```typescript
interface InboxRow {
	id: string;
	tenant_id: string;
	received_at: Date;
	source: string;
	correlation_token: string;
	payload: Record<string, unknown>;
	processed_at: Date | null;
}
```

---

### `HistoryRow`

```typescript
interface HistoryRow {
	id: number;
	tenant_id: string;
	instance_id: string;
	at: Date;
	event_type: HistoryEventType;
	from_node: string | null;
	to_node: string | null;
	data: Record<string, unknown>;
}
```

`event_type` is one of [`HISTORY_EVENT`](#history_event) values.

---

### `AdvanceJobPayload` / `EffectJobPayload`

```typescript
type AdvanceKind = "start" | "effect" | "timeout" | "signal";

interface AdvanceJobPayload {
	tenant_id: string;
	instance_id: string;
	kind?: AdvanceKind;
	expected_seq?: number;
	outcome?: string;
	outcome_data?: Record<string, unknown>;
	inbox_id?: string;
	handler?: string;
	correlation_token?: string | null;
	redispatch?: number;
}

interface EffectJobPayload {
	tenant_id: string;
	instance_id: string;
	handler: string;
	seq?: number;
	cursor?: string;
	redispatch?: number;
}
```

Steve job payloads. You don't normally construct these directly — `Workflow.create`, the driver and the scheduler/correlator do it for you.

| Field                  | Meaning                                                                                                                                                                                                                                                              |
| ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `kind`                 | What produced this advance. Selects the precondition the driver checks under the row lock: `start` → `pending`, `effect` → `running`, `signal` → `waiting`, `timeout` → `waiting` **and** a `wake_at` in the past.                                                   |
| `expected_seq` / `seq` | The fence: the instance `seq` the job was issued against. The driver drops the job if the locked row has moved past it. On an effect job a row that is _behind_ the fence means the dispatching transaction has not committed yet — the job throws so steve retries. |
| `inbox_id`             | `kind: "signal"` only. The advance reads the outcome data off that row and marks it processed in the same transaction as the transition.                                                                                                                             |
| `handler`              | `kind: "effect"` only; used for the `effect_completed` history entry.                                                                                                                                                                                                |
| `correlation_token`    | `kind: "effect"` only. The token the handler returned; written at this advance's settle point. `null` clears it, absent leaves the row's own value alone.                                                                                                            |
| `cursor`               | Node that dispatched the effect. Diagnostics only.                                                                                                                                                                                                                   |
| `redispatch`           | How many times this payload has been re-queued after its job expired. Bounded by `redispatchLimit`; absent on a first dispatch.                                                                                                                                      |

**Absent `kind` / `expected_seq` / `seq` means unfenced** — a job enqueued by 2.0.x and still queued at upgrade time. The driver infers the kind from the payload shape and skips both the fence and the preconditions rather than swallowing the job. A transitional shim; it will be removed together with the `project_id` fallback in a later major.

---

### Other re-exported types

| Type                  | Description                                                                                               |
| --------------------- | --------------------------------------------------------------------------------------------------------- |
| `WorkflowContext`     | `Record<string, unknown>` — accumulated payload carried in `__workflow_instances.context`                 |
| `WorkflowSnapshot`    | `FSMSnapshot<string, WorkflowContext>` — passthrough re-export for convenience                            |
| `ExecutionState`      | Union of execution-state strings                                                                          |
| `HistoryEventType`    | Union of history-event-type strings                                                                       |
| `AdvanceKind`         | `"start" \| "effect" \| "timeout" \| "signal"` — see [job payloads](#advancejobpayload--effectjobpayload) |
| `SchedulerTickResult` | `{ woken, repoked }` — what one `WorkflowScheduler` tick poked                                            |

---

## Constants

### `EXECUTION_STATE`

```typescript
const EXECUTION_STATE = {
	PENDING: "pending",
	RUNNING: "running",
	WAITING: "waiting",
	COMPLETED: "completed",
	FAILED: "failed",
	CANCELLED: "cancelled",
} as const;
```

### `HISTORY_EVENT`

```typescript
const HISTORY_EVENT = {
	CREATED: "created",
	TRANSITION: "transition",
	EFFECT_DISPATCHED: "effect_dispatched",
	EFFECT_COMPLETED: "effect_completed",
	EFFECT_FAILED: "effect_failed",
	SIGNAL_RECEIVED: "signal_received",
	SIGNAL_REJECTED: "signal_rejected",
	TIMEOUT: "timeout",
	TRANSITION_REJECTED: "transition_rejected",
	COMPLETED: "completed",
	FAILED: "failed",
	CANCELLED: "cancelled",
	RETRIED: "retried",
} as const;
```

### Misc

| Name                     | Value                | Notes                                                                                        |
| ------------------------ | -------------------- | -------------------------------------------------------------------------------------------- |
| `DEFAULT_TENANT_ID`      | `"_default"`         | Used when `tenantId` not supplied. Matches `@marianmeres/cron`'s default.                    |
| `JOB_TYPE_ADVANCE`       | `"workflow.advance"` | Steve job type for one driver step.                                                          |
| `JOB_TYPE_EFFECT_PREFIX` | `"workflow.effect."` | Full type: `workflow.effect.<handlerName>`.                                                  |
| `PURE_ENTER_EVENT`       | `"ENTER"`            | Synthetic event fired by the driver into pure nodes. Wire guarded transitions on this event. |

---

## See Also

- [README.md](./README.md) — overview, install, end-to-end example
- [AGENTS.md](./AGENTS.md) — architectural deep-dive for coding agents
- [`@marianmeres/fsm`](https://jsr.io/@marianmeres/fsm) — the underlying state-graph library
- [`@marianmeres/steve`](https://jsr.io/@marianmeres/steve) — the underlying job queue
- [`@marianmeres/cron`](https://jsr.io/@marianmeres/cron) — the underlying cron scheduler
- [`@marianmeres/migrate`](https://jsr.io/@marianmeres/migrate) — the underlying migration runner
