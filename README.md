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
- **Instance** — One running occurrence of a definition. Lives as a row in `__workflow_instances`. Carries a _cursor_ (current node) and a separate _execution_state_ (lifecycle status).
- **Driver** — The advance engine. Stateless. Loads an instance, applies an outcome, dispatches the next step. Implemented as a steve job handler.

Each FSM state declares its **node kind** via `meta`:

| Kind         | When the driver sees this state                                                                   |
| ------------ | ------------------------------------------------------------------------------------------------- |
| `pure`       | Fire `ENTER` to route via guards. Inline-expand.                                                  |
| `effectful`  | Enqueue an effect handler job. Wait for completion.                                               |
| `suspending` | Park the instance (`waiting`). Wake on `wake_at` (`TIMEOUT`) or matched inbox signal (`MATCHED`). |
| `terminal`   | Mark instance `completed`.                                                                        |

The user's handler returns `{ outcome, data }`. The driver calls `fsm.transition(outcome, data)`. The transition table picks the next state. `data` is the transition payload only; a handler that wants a value kept for later nodes returns it in `context` — a shallow patch merged before the transition. There is no AI nondeterminism inside the FSM logic — return a label, look up the next node.

## Example

A supplier-replenishment workflow: detect low stock → send order email → wait up to 60s for a reply → classify with AI → write the order.

```typescript
import pg from "pg";
import { Jobs } from "@marianmeres/steve";
import { Cron } from "@marianmeres/cron";
import {
	createMigrate,
	Workflow,
	type WorkflowDefinition,
	WorkflowInboxCorrelator,
	WorkflowScheduler,
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
				on: {
					CONFIRMED: "write_order",
					DENIED: "_end_denied",
					UNKNOWN: "_end_unknown",
				},
			},
			write_order: {
				meta: { kind: "effectful", handler: "persistOrder" },
				on: { OK: "_end_ok" },
			},
			_end_ok: { meta: { kind: "terminal" }, on: {} },
			_end_failed: { meta: { kind: "terminal" }, on: {} },
			_end_timeout: { meta: { kind: "terminal" }, on: {} },
			_end_denied: { meta: { kind: "terminal" }, on: {} },
			_end_unknown: { meta: { kind: "terminal" }, on: {} },
		},
	},
};

// 2. Connect + migrate
const pool = new pg.Pool({/* ... */});
await createMigrate(pool).up("latest");

// 3. Construct the runtime: steve.Jobs + cron.Cron are owned by *you*. The
// workflow framework attaches its handlers to them and the consumer owns
// the start/stop lifecycle. Share them with your app's other job/cron work
// if you want — they coexist by type/name on the same underlying tables.
//
// `autoCleanup` is not optional in practice: it runs steve's reaper, which
// is the only thing that notices a job whose worker died mid-run. Without
// it such a job sits in `running` forever and its instance never moves
// again. See "Persistence Guarantees" below.
const jobs = new Jobs({ db: pool, autoCleanup: true });
const cron = new Cron({ db: pool });

// 4. Construct the Workflow. It registers `workflow.advance` + one
// `workflow.effect.<name>` handler per registered handler on the injected
// Jobs (via `jobs.setHandler`).
const wf = new Workflow({
	db: pool,
	jobs,
	tenantId: "default",
	definitions: [stockReplenishment],
	handlers: {
		checkInventory: async (args) => ({ outcome: "LOW", data: { stock: 3 } }),
		sendOrderEmail: async (args) => ({ outcome: "SENT", data: { messageId: "..." } }),
		aiClassifyReply: async (args) => ({ outcome: "CONFIRMED", data: {} }),
		persistOrder: async (args) => ({ outcome: "OK", data: { orderId: "..." } }),
	},
	matchers: {
		matchEmailReply: ({ signal }) => signal.source === "email",
	},
});

// 5. Attach the scheduler + correlator ticks to the injected Cron.
const scheduler = new WorkflowScheduler({ cron, workflow: wf });
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

The framework owns no runtime processes. It registers job and cron handlers on the `Jobs` and `Cron` instances you pass in; you call `start()` / `stop()` on them. This lets the workflow's jobs/crons coexist with your application's own — they share `__job` / `__cron` tables and coexist by `type` / `name`. Sharing one runtime per process is cheaper than running parallel pollers — and safer: a `Jobs` or `Cron` that polls these tables without the workflow handlers attached silently noops the work it claims (see [Failure Modes](#failure-modes)).

A single steve job type — **`workflow.advance`** — encapsulates one step of the driver. Its payload is `{ tenant_id, instance_id, kind, expected_seq, outcome?, outcome_data?, inbox_id? }`. The handler:

1. Locks the instance row inside a transaction.
2. Drops the job if the instance is already terminal, if `expected_seq` no longer matches the row's `seq` (the fence — see below), or if the row is not in the state this `kind` of advance expects.
3. If an `outcome` was supplied (wake from effect / signal / timer), applies it via `fsm.transition(outcome, data)`. If the FSM rejects, the instance is marked `failed`.
4. Loops on the current state's `meta.kind`, settling at the first terminal / effectful / suspending state and persisting the new cursor + execution_state — and bumping `seq` — in the same transaction.
5. For effectful nodes, enqueues a **`workflow.effect.<handlerName>`** steve job carrying that new `seq`. On its success, the effect handler enqueues a fresh advance carrying the outcome. On terminal failure, the instance is marked `failed`.

**The advance is the only writer of instance rows** (bar the job-failure hooks and the `cancel` / `retry` admin calls, which settle a row directly and bump `seq` with it). Everything else — the scheduler, the correlator — only _pokes_: it reads what looks due and enqueues an advance for it. A poke that is lost to a crash costs nothing, because whatever made the row look due is still true next tick; a poke that arrives twice costs nothing, because the second one is fenced out.

The **fence** is the `seq` column. Every settle-point write bumps it, and every advance/effect job carries the value it was issued against. A job whose value no longer matches belongs to a step the instance has already left — a duplicate, or a zombie from a worker that hung — and is dropped with a debug log rather than applied to a row that moved on.

The **scheduler** runs as a registered cron job (default `* * * * *`). Each tick is read-only: it selects `waiting` rows whose `wake_at` has passed and pokes one `TIMEOUT` advance per row (the advance re-checks due-ness under the lock and clears `wake_at`). A second scan pokes instances stuck in `pending` for longer than `stalePendingSec` — the residue of a crash between `create()`'s commit and steve's job insert, which happens on its own connection.

The **correlator** runs as a registered cron job (default `* * * * *`). Each tick claims a batch of unprocessed `__workflow_inbox` rows, finds the live instance owning the `correlation_token`, runs the user's `matcher` for the current state, and pokes a `MATCHED` advance carrying the _inbox row id_. The advance re-reads that row under a lock and marks it processed in the same transaction as the transition, so a signal is never consumed without being delivered.

## Versioning Definitions

Each instance row stores `definition_version`. The framework looks up the definition by exact `(id, version)` match — **pin-and-freeze**. Bump the version for any change to the graph, payload schema, or handler semantics. Keep old versions registered until the last instance using them drains. Git remembers; the DB stores only a pointer.

## Persistence Guarantees & Idempotency

- **The instance write is one PG transaction.** Cursor, execution_state, wake_at, `seq` and history are written together, and a signal delivery marks its inbox row processed in that same transaction. The follow-up _job_ insert is not part of it: steve creates jobs on its own connection, so a job can exist for a transaction that ended up rolling back.
- **Job delivery is therefore at-least-once, and fenced.** A duplicate advance or effect job is recognized by `seq` and dropped. The fence is what makes the extra deliveries harmless, not their absence. `seq` arrives with schema 1.2.0 — run `createMigrate(pool).up("latest")` when upgrading.
- **Handlers must be idempotent.** The fence protects the _instance row_, not the outside world. A handler can still run twice — a worker that completed the side effect and then died before steve recorded the attempt gets retried. Use upserts, idempotency keys, or check the existing state.
- **Run `Jobs` with `autoCleanup: true`** (or call `jobs.cleanup()` periodically). Steve's reaper is the only thing that marks a job whose worker died as `expired`; without it that job stays `running` forever and its instance never moves again. With it, the framework re-queues the identical payload up to `redispatchLimit` times before giving up and failing the instance — the fence makes the re-queue a no-op if the dead worker's transaction did commit.
- **A `pending` instance is re-poked** after `stalePendingSec` (default 300s), which covers the crash window between `create()`'s commit and steve's job insert.

## Signals: early, late, deferred

`appendInbox` never blocks on the instance being ready for the signal. What the correlator does with a row depends on what it finds behind the token:

| Situation                                                                            | What happens                                                                                        |
| ------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------- |
| Instance is `waiting` at a node that accepts `MATCHED`, matcher says yes             | Advance poked; `signal_received` + the transition commit with the row's `processed_at`              |
| Matcher says no                                                                      | `signal_rejected` history; row marked processed                                                     |
| Matcher throws                                                                       | Row deferred — re-examined next tick, with an error log                                             |
| Instance is live but not `waiting` yet (still running the step that emits the token) | Row deferred                                                                                        |
| Instance is `waiting` at a node with no `MATCHED` edge (e.g. a timer-only delay)     | Row deferred                                                                                        |
| No live instance owns the token, or the one that did has failed                      | Row marked processed with a warn log (plus `signal_rejected` history if a terminal instance exists) |

"Deferred" means the row stays unprocessed and is looked at again on the next tick — an early signal is early, not wrong. Only one signal per instance is poked per tick; the rest defer, because a second one would be fenced out anyway.

Two consequences worth knowing: a deferred row is retried indefinitely (a permanently-throwing matcher logs on every tick), and rows are claimed `ORDER BY received_at LIMIT tickBatchSize`, so a deferred backlog larger than the batch would shadow newer rows.

## Failure Modes

| Failure                                                                             | Behavior                                                                                                                                                                                                                                                                                                                                 |
| ----------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Effect handler throws repeatedly                                                    | Steve retries the job `effectMaxAttempts` times, then the instance is marked `failed`; `effect_failed` history entry.                                                                                                                                                                                                                    |
| Advance job throws repeatedly                                                       | Steve retries the job `advanceMaxAttempts` times (default 10, ~17min of backoff), then the instance is marked `failed` with the last attempt's error message.                                                                                                                                                                            |
| Worker dies mid-job                                                                 | Steve's reaper (needs `autoCleanup`) marks the job `expired` — steve never retries those. The framework re-queues the identical payload, up to `redispatchLimit` dispatches in total, then fails the instance.                                                                                                                           |
| Lost advance: the side effect happened but the follow-up advance was never enqueued | The effect job is retried (if the enqueue threw) or re-dispatched after expiry (if the process died). Either way the handler runs again — the instance never left that `seq`, so the fence lets it through — and the advance is enqueued. **This is why handlers must be idempotent.**                                                   |
| FSM rejects the outcome label                                                       | Instance marked `failed`; `transition_rejected` history entry.                                                                                                                                                                                                                                                                           |
| Pure node with no guard matching                                                    | Instance marked `failed`; `transition_rejected` history entry.                                                                                                                                                                                                                                                                           |
| Definition missing on advance                                                       | Instance marked `failed`. Keep old versions registered until drained.                                                                                                                                                                                                                                                                    |
| A `failed` instance you want back                                                   | `wf.retry(id)` puts it back to `pending` at its current cursor and re-runs that node — no new instance, no replay of the effects it already did. `wf.retry(id, { force: true })` covers a `running` one whose worker died. Nothing is fixed by retrying alone: fix the cause first.                                                      |
| An instance that should not finish at all                                           | `wf.cancel(id, reason?)` marks it `cancelled` (terminal) and drops its timer and its correlation token. Jobs already in flight for it are fenced out — a queued effect job skips its handler rather than firing the side effect.                                                                                                         |
| A second `Jobs` or `Cron` in a process that never registered the workflow handlers  | **Silent data loss.** Both libraries fall back to a noop for an unknown `type` / `name`: steve marks the job completed, cron marks the tick run and advances `next_run_at`. Share one runtime per process, or make sure every process that starts a `Jobs`/`Cron` on these tables also constructs the `Workflow` and calls `register()`. |

## Observability

Every transition, dispatch, signal, and failure is appended to `__workflow_history`. Read it via:

```typescript
import { getHistory } from "@marianmeres/workflow";
const events = await getHistory(pool, instanceId);
```

## Multi-tenancy

Pass `tenantId` to `Workflow`, `WorkflowScheduler`, and `WorkflowInboxCorrelator`. All three tables (and Cron's internal tables) carry `tenant_id`, so instances, inbox rows, history, and the tick registrations are scoped: a `Workflow` only ever sees its own tenant's rows.

**The queues are not scoped, though.** Both steve and cron claim the next due row globally — no tenant filter, no type filter — and both fall back to a noop for work they have no handler for. So running one process per tenant against a shared database does _not_ isolate them: process A will claim process B's jobs and cron ticks, and silently noop the ones whose handler it does not have. That is safe only when every such process registers the _same_ definitions, handlers, and ticks (which makes the per-process scoping pointless anyway).

The supported shape today is one runtime per database, serving whichever tenants it has definitions for. Tenant scoping is for keeping data apart, not for partitioning workers.

## Breaking changes in 2.0

- **`project_id` renamed to `tenant_id` throughout**, aligning with the
  ecosystem's tenant-scoping convention (`@marianmeres/cron`,
  `@marianmeres/steve`). This covers the DB column on all three tables, the
  `tenant_id` field on `WorkflowInstanceRow` / `InboxRow` / `HistoryRow`, the
  `tenantId` option on `Workflow` / `WorkflowScheduler` /
  `WorkflowInboxCorrelator`, the `tenantId` key in `HandlerArgs` / `MatcherArgs`,
  the `tenant_id` key in `AdvanceJobPayload` / `EffectJobPayload`, and the
  `DEFAULT_TENANT_ID` export (was `DEFAULT_PROJECT_ID`).

  Run `createMigrate(pool).up("latest")` once on upgrade. Schema version 1.1.0
  renames the column in place on all three tables, preserving existing data. It
  is idempotent, and `down("1.0.0")` reverses it.

  Jobs enqueued by 1.0.x carry the old `project_id` key in their steve payload.
  The driver reads either key, so anything already queued at upgrade time drains
  normally. That fallback is a transitional shim and will be removed in a later
  major.

- **Requires `@marianmeres/cron` 3.x**, which performed the same rename on its
  own tables. If you are upgrading an existing database, run its migration once
  too — the cron tick registrations otherwise fail against the legacy column:

  ```typescript
  import { Cron } from "@marianmeres/cron";
  await Cron.migrate(pool);
  ```

  Fresh installs need no action. `@marianmeres/steve` 3.x self-heals its own
  schema on init, so it needs nothing either.

## API

See [API.md](API.md) for the full reference. See [AGENTS.md](AGENTS.md) for the agent-oriented guide.

## License

[MIT](LICENSE)
