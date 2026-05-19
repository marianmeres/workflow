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

| Field | Type | Default | Description |
|---|---|---|---|
| `db` | `pg.Pool` | required | PostgreSQL pool. Used for direct SQL (persistence helpers, transactions). |
| `jobs` | `Jobs` | required | Externally-owned `steve.Jobs` instance. Workflow registers its handlers on it via `setHandler`. The consumer runs `jobs.start()` / `jobs.stop()`. |
| `projectId` | `string` | `"_default"` | Multi-tenant scope. |
| `definitions` | `WorkflowDefinition[]` | required | Definitions to register. Validated on construction. |
| `handlers` | `Record<string, Handler>` | required | Effect handlers keyed by name. |
| `matchers` | `Record<string, Matcher>` | `{}` | Signal matchers keyed by name. |
| `effectMaxAttempts` | `number` | `3` | Max retries for effect handlers (steve `max_attempts`). |
| `effectMaxAttemptDurationMs` | `number` | `0` | Per-attempt timeout in ms (`0` = none). |

Throws on construction if any definition references a handler/matcher not present in the maps, or if any other structural problem is found (unknown transition target, missing terminal state, etc.). See [`validateDefinition`](#validatedefinitiondef-available).

The constructor also subscribes (`jobs.onDone`) to terminal failures of each `workflow.effect.<name>` type and marks the corresponding workflow instance `failed` on `failed` / `expired`.

#### Properties

| Name | Type | Description |
|---|---|---|
| `db` | `pg.Pool` | The provided pool. |
| `jobs` | `Jobs` | The injected `Jobs` instance. |
| `projectId` | `string` | Scope. |
| `registry` | `WorkflowRegistry` | Read-only access to definitions/handlers/matchers. |

#### Methods

> **No `start()` or `stop()`.** The consumer manages the `Jobs` lifecycle.

##### `create(input): Promise<WorkflowInstanceRow>`

Creates a new workflow instance, appends a `created` history entry, and enqueues the first `workflow.advance` job.

| Field | Type | Description |
|---|---|---|
| `definitionId` | `string` | Must be registered. |
| `definitionVersion` | `string` | Must be registered. |
| `context` | `WorkflowContext` (optional) | Initial context. Default: `{}`. |
| `correlationToken` | `string \| null` (optional) | Set up-front for signal-suspending nodes that need to be matchable before the workflow reaches them (e.g. outbound emails using UUID subaddressing). |

##### `find(id: string): Promise<WorkflowInstanceRow | null>`

Looks up an instance by id, scoped to this Workflow's `projectId`. Returns `null` if not found or scoped to a different project.

##### `appendInbox(input): Promise<InboxRow>`

Appends an external signal to `__workflow_inbox`. The correlator's next tick matches it to a waiting instance, runs the matcher, and (on match) enqueues an advance with `outcome: 'MATCHED'` and `outcome_data: payload`.

| Field | Type | Description |
|---|---|---|
| `source` | `string` | Free-form classifier, e.g. `"email"`, `"webhook"`. |
| `correlationToken` | `string` | Index key into waiting instances. |
| `payload` | `Record<string, unknown>` | Signal data, surfaced to the matcher and forwarded to the FSM. |

##### `enqueueAdvance` / `enqueueEffect`

Internal `JobEnqueuer` interface methods used by the driver. Public so the scheduler and correlator can call them. You normally don't call these directly.

---

### `WorkflowScheduler`

Cron-driven scheduler that wakes time-suspended instances. Attaches its tick to an **externally-owned** `@marianmeres/cron` `Cron` instance when `register()` is called. Each tick claims due rows and enqueues advance jobs with `outcome: 'TIMEOUT'`.

```typescript
import { WorkflowScheduler } from "@marianmeres/workflow";
import { Cron } from "@marianmeres/cron";
```

#### Constructor

```typescript
new WorkflowScheduler(options: WorkflowSchedulerOptions)
```

**`WorkflowSchedulerOptions`:**

| Field | Type | Default | Description |
|---|---|---|---|
| `cron` | `Cron` | required | Externally-owned `Cron` instance. Scheduler registers its tick on this via `cron.register`. The consumer runs `cron.start()` / `cron.stop()`. |
| `workflow` | `Workflow` | required | The Workflow whose instances this scheduler wakes. `projectId` and `db` are derived from it. |
| `tickExpression` | `string` | `"* * * * *"` | 5-field cron expression. |
| `timezone` | `string \| null` | host local | IANA timezone for the cron expression. |
| `tickBatchSize` | `number` | `100` | Max rows claimed per tick. |
| `tickName` | `string` | `workflow.scheduler.<projectId>` | Cron job name. |

#### Methods

> **No `start()` or `stop()`.** The consumer manages the `Cron` lifecycle.

| Method | Description |
|---|---|
| `register(): Promise<void>` | Registers the tick on the injected `Cron`. Call once after construction. Idempotent (re-registering preserves `next_run_at`). |
| `unregister(): Promise<void>` | Removes the tick from the injected `Cron`. |
| `tickOnce(): Promise<number>` | Runs one tick immediately. Returns rows woken. Exposed for tests and on-demand wakes. |

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

**`WorkflowInboxCorrelatorOptions`** has the same shape as `WorkflowSchedulerOptions` (cron / workflow / tickExpression / timezone / tickBatchSize / tickName), with `tickName` defaulting to `workflow.correlator.<projectId>`.

#### Methods

> **No `start()` or `stop()`.** The consumer manages the `Cron` lifecycle.

| Method | Description |
|---|---|
| `register(): Promise<void>` | Registers the tick on the injected `Cron`. Call once after construction. |
| `unregister(): Promise<void>` | Removes the tick from the injected `Cron`. |
| `tickOnce(): Promise<number>` | Runs one tick immediately. Returns signals processed. Exposed for tests and on-demand correlation. |

#### Per-tick behavior

For each unprocessed inbox row (claimed via `FOR UPDATE SKIP LOCKED`):

1. Find a `waiting` instance with the same `correlation_token`. None → mark processed, done.
2. Resolve `meta.matcher` on the instance's current state. Missing matcher (string id) → unregistered matcher → `signal_rejected` history, mark processed.
3. Run the matcher with `{ instanceId, projectId, context, signal }`.
4. Matcher returns `false` → `signal_rejected` history, mark processed.
5. Matcher returns `true` → `signal_received` history, flip instance to `pending`, mark processed, enqueue advance with `outcome: 'MATCHED'` and `outcome_data: signal.payload`.

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

| Method | Returns | Description |
|---|---|---|
| `getDefinition(id, version)` | `WorkflowDefinition \| undefined` | Lookup. |
| `requireDefinition(id, version)` | `WorkflowDefinition` | Throws if missing. |
| `getHandler(name)` | `Handler \| undefined` | Lookup. |
| `requireHandler(name)` | `Handler` | Throws if missing. |
| `getMatcher(name)` | `Matcher \| undefined` | Lookup. |
| `requireMatcher(name)` | `Matcher` | Throws if missing. |
| `handlerNames()` | `string[]` | All registered handler names. |

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

Active version is stored in `__workflow_migrations`. The migration creates `__workflow_instances`, `__workflow_inbox`, `__workflow_history` and their indexes.

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
type WorkflowDefinition<TState extends string = string, TEvent extends string = string> = {
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

| Kind | Driver behavior |
|---|---|
| `pure` | Auto-fires `ENTER`. User wires guarded transitions. Inline-expands. |
| `effectful` | Enqueues a steve job for `handler`. Waits for completion. |
| `suspending` | Parks the instance. Wakes on `wake_at` (`TIMEOUT`) or matched inbox signal (`MATCHED`). |
| `terminal` | Marks instance `completed`. |

---

### `Handler` / `HandlerArgs` / `HandlerResult`

```typescript
type Handler = (args: HandlerArgs) => Promise<HandlerResult> | HandlerResult;

interface HandlerArgs {
    instanceId: string;
    projectId: string;
    context: WorkflowContext;
    signal?: AbortSignal;
}

interface HandlerResult {
    outcome: string;
    data?: Record<string, unknown>;
}
```

Handlers are async functions referenced by string name from `effectful` node metas. The driver passes the AbortSignal from steve — long-running handlers should observe it for cooperative cancellation.

The returned `outcome` is the FSM event name that drives the next transition. `data` is forwarded as the transition payload (available in guards/actions if the user wires them).

**Idempotency:** handlers can run multiple times in failure scenarios. Design them accordingly.

---

### `Matcher` / `MatcherArgs`

```typescript
type Matcher = (args: MatcherArgs) => boolean | Promise<boolean>;

interface MatcherArgs {
    instanceId: string;
    projectId: string;
    context: WorkflowContext;
    signal: InboxRow;
}
```

Matchers are predicate functions referenced from `suspending` node metas. The correlation token alone is just an index lookup — the matcher is the *semantic* gate.

---

### `WorkflowInstanceRow`

```typescript
interface WorkflowInstanceRow {
    id: string;
    project_id: string;
    definition_id: string;
    definition_version: string;
    cursor: string;                         // current FSM state
    previous_cursor: string | null;         // feeds FSM.fromSnapshot's snapshot.previous
    context: WorkflowContext;
    execution_state: ExecutionState;
    wake_at: Date | null;
    correlation_token: string | null;
    created_at: Date;
    updated_at: Date;
}
```

Schema-aligned. `cursor` and `execution_state` are deliberately separate columns — see [Two Orthogonal States](./AGENTS.md#two-orthogonal-states).

---

### `InboxRow`

```typescript
interface InboxRow {
    id: string;
    project_id: string;
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
    project_id: string;
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
interface AdvanceJobPayload {
    project_id: string;
    instance_id: string;
    outcome?: string;
    outcome_data?: Record<string, unknown>;
    timeout?: boolean;
}

interface EffectJobPayload {
    project_id: string;
    instance_id: string;
    handler: string;
}
```

Steve job payloads. You don't normally construct these directly — `Workflow.create` and the scheduler/correlator do it for you.

---

### Other re-exported types

| Type | Description |
|---|---|
| `WorkflowContext` | `Record<string, unknown>` — accumulated payload carried in `__workflow_instances.context` |
| `WorkflowSnapshot` | `FSMSnapshot<string, WorkflowContext>` — passthrough re-export for convenience |
| `ExecutionState` | Union of execution-state strings |
| `HistoryEventType` | Union of history-event-type strings |

---

## Constants

### `EXECUTION_STATE`

```typescript
const EXECUTION_STATE = {
    PENDING:   "pending",
    RUNNING:   "running",
    WAITING:   "waiting",
    COMPLETED: "completed",
    FAILED:    "failed",
    CANCELLED: "cancelled",
} as const;
```

### `HISTORY_EVENT`

```typescript
const HISTORY_EVENT = {
    CREATED:             "created",
    TRANSITION:          "transition",
    EFFECT_DISPATCHED:   "effect_dispatched",
    EFFECT_COMPLETED:    "effect_completed",
    EFFECT_FAILED:       "effect_failed",
    SIGNAL_RECEIVED:     "signal_received",
    SIGNAL_REJECTED:     "signal_rejected",
    TIMEOUT:             "timeout",
    TRANSITION_REJECTED: "transition_rejected",
    COMPLETED:           "completed",
    FAILED:              "failed",
    CANCELLED:           "cancelled",
} as const;
```

### Misc

| Name | Value | Notes |
|---|---|---|
| `DEFAULT_PROJECT_ID` | `"_default"` | Used when `projectId` not supplied. Matches `@marianmeres/cron`'s default. |
| `JOB_TYPE_ADVANCE` | `"workflow.advance"` | Steve job type for one driver step. |
| `JOB_TYPE_EFFECT_PREFIX` | `"workflow.effect."` | Full type: `workflow.effect.<handlerName>`. |
| `PURE_ENTER_EVENT` | `"ENTER"` | Synthetic event fired by the driver into pure nodes. Wire guarded transitions on this event. |

---

## See Also

- [README.md](./README.md) — overview, install, end-to-end example
- [AGENTS.md](./AGENTS.md) — architectural deep-dive for coding agents
- [`@marianmeres/fsm`](https://jsr.io/@marianmeres/fsm) — the underlying state-graph library
- [`@marianmeres/steve`](https://jsr.io/@marianmeres/steve) — the underlying job queue
- [`@marianmeres/cron`](https://jsr.io/@marianmeres/cron) — the underlying cron scheduler
- [`@marianmeres/migrate`](https://jsr.io/@marianmeres/migrate) — the underlying migration runner
