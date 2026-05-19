# @marianmeres/workflow

[![NPM version](https://img.shields.io/npm/v/@marianmeres/workflow.svg)](https://www.npmjs.com/package/@marianmeres/workflow)
[![JSR version](https://jsr.io/badges/@marianmeres/workflow)](https://jsr.io/@marianmeres/workflow)
[![License](https://img.shields.io/npm/l/@marianmeres/workflow)](LICENSE)

A durable workflow / orchestration framework for long-lived state machines. Built on PostgreSQL.

Instances persist across process restarts, sleep for days waiting for a signal or a timer, and resume from disk via [`@marianmeres/fsm`](https://jsr.io/@marianmeres/fsm)'s `fromSnapshot`. Effects run as [`@marianmeres/steve`](https://jsr.io/@marianmeres/steve) jobs (transactional outbox). The scheduler and inbox correlator are driven by [`@marianmeres/cron`](https://jsr.io/@marianmeres/cron). Schema is managed by [`@marianmeres/migrate`](https://jsr.io/@marianmeres/migrate).

**Ships no domain code.** Userland brings the definitions, handlers, and matchers. The framework provides the dispatch core and the durability story.

## Install

```sh
deno add jsr:@marianmeres/workflow
```

```sh
npm install @marianmeres/workflow
```

Required peer: a PostgreSQL connection (`pg.Pool`).

## Mental Model

A workflow is a **finite state machine with durable, long-lived instances**. Three concepts kept rigorously separate:

- **Definition** — Static, versioned, JSON-serializable graph of states. Pure data; handler/matcher ids are strings.
- **Instance** — One running occurrence of a definition. Lives as a row in `__workflow_instances`. Carries a *cursor* (current node) and a separate *execution_state* (lifecycle status).
- **Driver** — The advance engine. Stateless. Loads an instance, applies an outcome, dispatches the next step. Implemented as a steve job handler.

Each FSM state declares its **node kind** via `meta`:

| Kind | When the driver sees this state |
|---|---|
| `pure` | Fire `ENTER` to route via guards. Inline-expand. |
| `effectful` | Enqueue an effect handler job. Wait for completion. |
| `suspending` | Park the instance (`waiting`). Wake on `wake_at` (`TIMEOUT`) or matched inbox signal (`MATCHED`). |
| `terminal` | Mark instance `completed`. |

The user's handler returns `{ outcome, data }`. The driver calls `fsm.transition(outcome, data)`. The transition table picks the next state. There is no AI nondeterminism inside the FSM logic — return a label, look up the next node.

## Example

A supplier-replenishment workflow: detect low stock → send order email → wait up to 60s for a reply → classify with AI → write the order.

```typescript
import pg from "pg";
import { Jobs } from "@marianmeres/steve";
import { Cron } from "@marianmeres/cron";
import {
    createMigrate,
    Workflow,
    WorkflowInboxCorrelator,
    WorkflowScheduler,
    type WorkflowDefinition,
} from "@marianmeres/workflow";

// 1. Define the workflow (pure data — handler/matcher names are strings)
const stockReplenishment: WorkflowDefinition = {
    id: "stock_replenishment",
    version: "1.0.0",
    fsm: {
        initial: "detect_low_stock",
        states: {
            detect_low_stock: {
                meta: { kind: "effectful", handler: "checkInventory" },
                on: { LOW: "send_order", OK: "_end_ok" },
            },
            send_order: {
                meta: { kind: "effectful", handler: "sendOrderEmail" },
                on: { SENT: "await_reply", FAILED: "_end_failed" },
            },
            await_reply: {
                meta: { kind: "suspending", matcher: "matchEmailReply", timeoutSec: 60 },
                on: { MATCHED: "classify_reply", TIMEOUT: "_end_timeout" },
            },
            classify_reply: {
                meta: { kind: "effectful", handler: "aiClassifyReply" },
                on: { CONFIRMED: "write_order", DENIED: "_end_denied", UNKNOWN: "_end_unknown" },
            },
            write_order: {
                meta: { kind: "effectful", handler: "persistOrder" },
                on: { OK: "_end_ok" },
            },
            _end_ok:       { meta: { kind: "terminal" }, on: {} },
            _end_failed:   { meta: { kind: "terminal" }, on: {} },
            _end_timeout:  { meta: { kind: "terminal" }, on: {} },
            _end_denied:   { meta: { kind: "terminal" }, on: {} },
            _end_unknown:  { meta: { kind: "terminal" }, on: {} },
        },
    },
};

// 2. Connect + migrate
const pool = new pg.Pool({ /* ... */ });
await createMigrate(pool).up("latest");

// 3. Construct the runtime: steve.Jobs + cron.Cron are owned by *you*. The
// workflow framework attaches its handlers to them and the consumer owns
// the start/stop lifecycle. Share them with your app's other job/cron work
// if you want — they coexist by type/name on the same underlying tables.
const jobs = new Jobs({ db: pool });
const cron = new Cron({ db: pool });

// 4. Construct the Workflow. It registers `workflow.advance` + one
// `workflow.effect.<name>` handler per registered handler on the injected
// Jobs (via `jobs.setHandler`).
const wf = new Workflow({
    db: pool,
    jobs,
    projectId: "default",
    definitions: [stockReplenishment],
    handlers: {
        checkInventory:  async (args) => ({ outcome: "LOW",       data: { stock: 3 } }),
        sendOrderEmail:  async (args) => ({ outcome: "SENT",      data: { messageId: "..." } }),
        aiClassifyReply: async (args) => ({ outcome: "CONFIRMED", data: {} }),
        persistOrder:    async (args) => ({ outcome: "OK",        data: { orderId: "..." } }),
    },
    matchers: {
        matchEmailReply: ({ signal }) => signal.source === "email",
    },
});

// 5. Attach the scheduler + correlator ticks to the injected Cron.
const scheduler  = new WorkflowScheduler({ cron, workflow: wf });
const correlator = new WorkflowInboxCorrelator({ cron, workflow: wf });
await scheduler.register();
await correlator.register();

// 6. Start the runtimes. The consumer owns these.
await jobs.start(4);
await cron.start(2);

// 7. Create an instance
const instance = await wf.create({
    definitionId: "stock_replenishment",
    definitionVersion: "1.0.0",
    correlationToken: crypto.randomUUID(),
});

// 8. When an external signal arrives, append it to the inbox. The
// correlator's next tick matches it to the waiting instance and fires
// `MATCHED` on the FSM.
await wf.appendInbox({
    source: "email",
    correlationToken: instance.correlation_token!,
    payload: { subject: "Re: order", body: "yes please" },
});

// 9. Graceful shutdown — stop what you started.
await cron.stop();
await jobs.stop();
```

## How It Works

```
┌─ User application ─────────────────────────────────────────────────┐
│  WorkflowDefinition (pure data) + Handlers + Matchers              │
│  steve.Jobs + cron.Cron (consumer-owned; start/stop)               │
└────────────────────────────┬───────────────────────────────────────┘
                             ▼
┌─ @marianmeres/workflow ────────────────────────────────────────────┐
│  Workflow              WorkflowScheduler    WorkflowInboxCorrelator │
│    │ attaches to        │ attaches tick to   │ attaches tick to     │
│    │ injected Jobs       │ injected Cron      │ injected Cron        │
│    ▼                     ▼                    ▼                     │
│  __workflow_instances · __workflow_inbox · __workflow_history       │
└────────────────────────────┬───────────────────────────────────────┘
                             ▼
                          PostgreSQL
```

The framework owns no runtime processes. It registers job and cron handlers on the `Jobs` and `Cron` instances you pass in; you call `start()` / `stop()` on them. This lets the workflow's jobs/crons coexist with your application's own — they share `__job` / `__cron` tables and coexist by `type` / `name`. Sharing one runtime per process is cheaper than running parallel pollers.

A single steve job type — **`workflow.advance`** — encapsulates one step of the driver. Its payload is `{ project_id, instance_id, outcome?, outcome_data? }`. The handler:

1. Locks the instance row inside a transaction.
2. If an `outcome` was supplied (wake from effect / signal / timer), applies it via `fsm.transition(outcome, data)`. If the FSM rejects, the instance is marked `failed`.
3. Loops on the current state's `meta.kind`, settling at the first terminal / effectful / suspending state and persisting the new cursor + execution_state in the same transaction.
4. For effectful nodes, enqueues a **`workflow.effect.<handlerName>`** steve job. On its success, the effect handler enqueues a fresh advance carrying the outcome. On terminal failure, the instance is marked `failed`.

The **scheduler** runs as a registered cron job (default `* * * * *`). Each tick atomically flips `waiting` + `wake_at <= now()` rows to `pending`, clears `wake_at`, and enqueues a `workflow.advance` with `outcome: 'TIMEOUT'`.

The **correlator** runs as a registered cron job (default `* * * * *`). Each tick claims a batch of unprocessed `__workflow_inbox` rows, finds the waiting instance with the matching `correlation_token`, runs the user's `matcher` for the current state, and enqueues a `workflow.advance` with `outcome: 'MATCHED'` plus the signal payload.

## Versioning Definitions

Each instance row stores `definition_version`. The framework looks up the definition by exact `(id, version)` match — **pin-and-freeze**. Bump the version for any change to the graph, payload schema, or handler semantics. Keep old versions registered until the last instance using them drains. Git remembers; the DB stores only a pointer.

## Persistence Guarantees & Idempotency

- The advance step is a single PG transaction: cursor / execution_state / wake_at / history are all written together with the effect-job insert.
- Steve retries effect handlers per `effectMaxAttempts`. A worker crash mid-effect leaves the job in `running`; steve auto-cleanup reaps it after `~5min` and retries.
- **Handlers must be idempotent.** A handler can run multiple times for the same instance if a worker crashes after the side effect but before steve records completion. Use upserts, idempotency keys, or check the existing state.

## Failure Modes

| Failure | Behavior |
|---|---|
| Effect handler throws repeatedly | Instance marked `failed`; `effect_failed` history entry. |
| FSM rejects the outcome label | Instance marked `failed`; `transition_rejected` history entry. |
| Pure node with no guard matching | Instance marked `failed`; `transition_rejected` history entry. |
| Definition missing on advance | Instance marked `failed`. Keep old versions registered until drained. |

## Observability

Every transition, dispatch, signal, and failure is appended to `__workflow_history`. Read it via:

```typescript
import { getHistory } from "@marianmeres/workflow";
const events = await getHistory(pool, instanceId);
```

## Multi-tenancy

Pass `projectId` to `Workflow`, `WorkflowScheduler`, and `WorkflowInboxCorrelator`. All three tables (and Cron's internal tables) carry `project_id`. One process can scope workers to a tenant while another scopes to a different tenant; the `FOR UPDATE SKIP LOCKED` claiming keeps them disjoint.

## API

See [API.md](API.md) for the full reference. See [AGENTS.md](AGENTS.md) for the agent-oriented guide.

## License

[MIT](LICENSE)
