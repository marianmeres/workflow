<!--
GENERATED ANALYSIS — @marianmeres/workflow (durability & idempotency)
Produced 2026-08-26 by an inline single-agent review: full read of src/, tests/, docs, and the
resolved sources of @marianmeres/fsm 3.1.0, steve 3.0.0, cron 3.2.0 → self-verification pass
re-opening every cited line. Claims verified against the codebase at commit 02a7635.
Planning artifact; no code was changed.
-->

# Durability & Idempotency — the driver, steve, and the crash windows

> This document is about the contract between the driver and `@marianmeres/steve`. Steve
> delivers jobs **at least once**: it re-runs a job whose handler already committed if the
> completion write fails (`_execute.ts:44–53` → `_handle-failure.ts:59–68`), and it never
> retries a job whose worker crashed — `cleanup()` marks such jobs `expired`, which is
> terminal by design (`_mark-expired.ts:6–8`), and only when the consumer turned
> `autoCleanup` on. The driver, however, is written as if delivery were exactly-once.

> The single most important takeaway: **there is no fencing token.** Nothing on an advance
> or effect job says which version of the instance row it was issued against, so a
> duplicate or stale job is not ignored — it is applied. A stale outcome is rejected by the
> FSM and marks a healthy instance `failed`; a replayed initial advance dispatches the same
> effect twice; an expired effect job from a step the instance already left marks the
> instance `failed`. Separately, the scheduler's claim-then-enqueue and `create()`'s
> insert-then-enqueue have windows that leave rows `pending` with no job that will ever
> move them, and `workflow.advance` failures are not observed at all.

> Headline recommendation: add a per-instance sequence number (`seq`) that every advance
> bumps and every job carries as `expected_seq`; make the scheduler and correlator ticks
> read-only "pokes" that the advance re-validates under the row lock; observe advance-job
> failures and re-dispatch `expired` jobs (bounded). With those three, every crash window
> collapses to "the next tick pokes again" and every duplicate collapses to a no-op. A
> `cancel()`/`retry()` pair then falls out almost for free.

> What is fine and needs no change: the per-advance transaction with `SELECT … FOR UPDATE`
> (`driver.ts:104–105`), the terminal-state guard (`:111–120`), the `fromSnapshot` resume
> (no `onEnter` on restore, `fsm.ts:1005–1048`), and `FOR UPDATE SKIP LOCKED` batch claiming.

## Summary of recommendations

| # | Recommendation                                                                               | Value | Effort | Risk                                       |
| - | -------------------------------------------------------------------------------------------- | ----- | ------ | ------------------------------------------ |
| 1 | Fencing token `seq` on the instance; `expected_seq` + `kind` on every advance; fenced effect | high  | M      | med — migration + payload shape (shimmed)  |
| 2 | Ticks become read-only pokes; stale-`pending` re-poke; `create()` inside a transaction       | high  | S–M    | low — behavior-preserving on healthy paths |
| 3 | Observe `workflow.advance` failures; bounded re-dispatch of `expired` jobs                   | high  | S      | low                                        |
| 4 | `cancel()` / `retry()` admin API                                                             | med   | S      | low — additive                             |

## Findings & recommendations (detailed)

### 1. No fencing token: duplicate or late jobs corrupt instances

- **Problem / observation.** Three concrete paths, all reachable in production, none
  reachable in the current test suite:

  **(a) The documented crash-after-enqueue scenario fails a healthy instance.**
  `AGENTS.md:232` describes it as recoverable: the effect handler succeeds, `runEffect`
  enqueues the completion advance (`driver.ts:375–380`), the process dies before steve
  writes the job's completion. On restart the advance runs and the instance moves on — say
  to `running` at `classify_reply`. Five minutes later `jobs.cleanup()` (via
  `autoCleanup`) marks the old effect job `expired` and publishes `onDone`
  (`jobs.ts:920–922`); the workflow's listener (`workflow.ts:131–144`) calls
  `failEffectJob` (`driver.ts:389–415`), which checks only that the row is non-terminal
  and marks it `failed` with `effect_failed { handler: "sendOrderEmail" }`. A stale job
  from a step the instance already left kills it.

  **(b) A re-run effect produces a second completion advance, which the FSM rejects.**
  If steve's completion write fails after the handler returned, `_executeJob` falls into
  its `catch` and `_handleJobFailure` puts the job back to `pending`
  (`_execute.ts:51–53`, `_handle-failure.ts:59–68`). The handler runs again (idempotency is
  documented, fine) and enqueues **a second advance with the same outcome**. `runAdvance`
  applies it at the _new_ cursor; `fsm.transition` returns `null`
  (`driver.ts:146–150`), so the instance gets `transition_rejected` and `failed`
  (`:151–165`). The same re-run of the _initial_ advance is worse: the row is already
  `running`, the dispatch loop lands on the effectful case again (`:211–233`) and enqueues
  a second effect job for the same node — the handler runs twice, both completions
  advance, the second is rejected.

  **(c) `runEffect` runs the handler unconditionally.** It reads the row without a lock and
  without looking at `execution_state` or the cursor (`driver.ts:349–373`). After a
  future `cancel()` (#4), or on any stale dispatch from (b), the side effect still fires.

  Also latent: an advance _without_ an outcome at a `waiting` cursor recomputes `wake_at`
  (`driver.ts:235–246`) — i.e. resets the timer. Today only a duplicate can trigger it; once
  ticks re-poke (#2) it becomes a real hazard unless advances carry a precondition.

  The comment at `workflow.ts:152–154` — "The advance is idempotent (cursor-aware) so an
  enqueue-but-no-commit race is recoverable" — is the assumption this finding refutes:
  nothing compares a job's origin against the row it lands on.

- **Evidence.** `driver.ts:389–415`:
  ```ts
  export async function failEffectJob(pool, payload, reason) {
  	await withTransaction(pool, async (client) => {
  		const row = await lockInstance(client, payload.instance_id);
  		if (!row) return;
  		if (row.execution_state === COMPLETED || FAILED || CANCELLED) return;
  		await updateInstance(client, row.id, { execution_state: EXECUTION_STATE.FAILED });
  		// ... history effect_failed
  ```
  `driver.ts:349–373` (`runEffect`): plain `SELECT … WHERE id = $1`, then `handler(...)` —
  no state check. `types.ts:151–169`: `AdvanceJobPayload` carries `outcome`, `outcome_data`,
  `timeout?: boolean`; `EffectJobPayload` carries `handler` — neither carries anything that
  identifies the row version they were issued against.

- **Proposed change.**
  1. Migration `1.2.0` (append-only ledger, `src/migrations/1_2_0.ts`):
     ```sql
     ALTER TABLE __workflow_instances ADD COLUMN IF NOT EXISTS seq integer NOT NULL DEFAULT 0;
     -- down: ALTER TABLE __workflow_instances DROP COLUMN IF EXISTS seq;
     ```
     (`DEFAULT 0` on a non-volatile default is a metadata-only change on PG ≥ 11.)
  2. `WorkflowInstanceRow.seq: number`. Every row write issued by `runAdvance`,
     `failInstance`, and (later) `cancel`/`retry` bumps `seq = seq + 1` in the same
     `UPDATE` as the state change — one settle-point write per advance, so a single
     `bumpSeq: true` flag on `updateInstance` (or a dedicated `advanceInstance` helper) is
     enough.
  3. Payload shapes (`types.ts`):
     ```ts
     export type AdvanceKind = "start" | "effect" | "timeout" | "signal";
     export interface AdvanceJobPayload {
     	tenant_id: string;
     	instance_id: string;
     	kind: AdvanceKind; // replaces `timeout?: boolean`
     	expected_seq: number; // fence
     	outcome?: string; // kind=effect (handler's label)
     	outcome_data?: Record<string, unknown>;
     	handler?: string; // kind=effect, for history
     	inbox_id?: string; // kind=signal (see 02 #1)
     	redispatch?: number; // see #3
     }
     export interface EffectJobPayload {
     	tenant_id: string;
     	instance_id: string;
     	handler: string;
     	seq: number; // fence
     	cursor: string; // for history/diagnostics
     	redispatch?: number;
     }
     ```
  4. `runAdvance`, inside the row lock, after the terminal check:
     ```ts
     if (payload.expected_seq !== undefined && row.seq !== payload.expected_seq) {
     	clog.debug?.(
     		`advance: stale (row.seq=${row.seq}, expected=${payload.expected_seq}); no-op`,
     	);
     	return;
     }
     // per-kind preconditions — violation is a debug no-op, never a failure
     switch (payload.kind) {
     	case "start":
     		if (row.execution_state !== PENDING) return;
     		break;
     	case "effect":
     		if (row.execution_state !== RUNNING) return;
     		break;
     	case "timeout":
     		if (
     			row.execution_state !== WAITING || !row.wake_at ||
     			row.wake_at > new Date()
     		) return;
     		break;
     	case "signal":
     		if (row.execution_state !== WAITING) return;
     		break;
     }
     ```
     A payload without `expected_seq`/`kind` is a job queued by 2.0.x: treat it as
     unfenced and infer `kind` (`timeout: true` → `timeout`; `outcome` present →
     `effect`; else `start`). Same transitional status as `payloadTenantId`
     (`driver.ts:34–38`); remove both together.
  5. `runEffect`: `if (row.seq !== payload.seq || row.execution_state !== RUNNING) return
     { skipped: "stale" };` — no handler call, no advance. Completion advance carries
     `{ kind: "effect", expected_seq: payload.seq, outcome, outcome_data, handler }`.
  6. `failEffectJob`: `if (row.seq !== payload.seq) return;` before anything else.
  7. `create()`: wrap insert + `created` history + enqueue in `withTransaction`. Steve's
     insert is autocommit on its own connection (`_create.ts:21,43`), so if our commit then
     fails the advance finds no row and no-ops (`driver.ts:106–109`); if `jobs.create`
     throws, our insert rolls back. The residual "insert committed, enqueue never happened"
     window is what #2's stale-`pending` re-poke covers.
  8. Emit the currently dead `HISTORY_EVENT.EFFECT_COMPLETED` (`types.ts:118`) for
     `kind: "effect"` advances, before the `transition` row, with `{ handler, outcome }`.

- **Done when** — `tests/durability.test.ts` (new) passes with: (1) enqueuing the initial
  advance payload twice yields exactly one `workflow.effect.checkInventory` row in `__job`;
  (2) enqueuing a second, identical effect-completion advance leaves the instance
  non-failed with the cursor advanced once; (3) `failEffectJob` with a stale `seq` leaves a
  moved-on instance untouched; and `tests/migrations.test.ts` asserts `seq` exists after
  `up("latest")` and is absent after `down("1.1.0")`.

- **Affected files.** `src/migrations/1_2_0.ts` (new), `src/migrations/index.ts`,
  `src/types.ts`, `src/persistence/instances.ts`, `src/driver.ts`, `src/workflow.ts`,
  `src/scheduler.ts` + `src/correlator.ts` (payload shape only — their redesign is #2 and
  02 #1), `tests/migrations.test.ts`, `tests/durability.test.ts` (new). Docs in 04 #1.

- **Effort / Value / Risk.** M / high / med. The migration is additive; the payload
  change is shimmed for jobs queued before the upgrade. Consumers calling
  `enqueueAdvance`/`enqueueEffect` directly (documented as internal, `API.md:102–104`)
  must now pass a fenced payload.

- **Implementation notes.** Keep the fence check _inside_ the row lock — it is only
  meaningful against the locked row. Log stale no-ops at debug and do not write a history
  row for them: once ticks re-poke (#2) they are routine, not events. While there, drop
  the ignored `_client` parameter from `JobEnqueuer` (`driver.ts:62–72`,
  `workflow.ts:148–168`): steve always inserts on its own connection, so the parameter
  only suggests an atomicity that does not exist (the README even claims it — 04 #1).
  Column name is bikeshed-able (`seq` / `step` / `revision`); `seq` avoids reading as
  "workflow step".

### 2. Ticks claim-then-enqueue and `create()` inserts-then-enqueues — crash windows leave rows `pending` forever

- **Problem / observation.** The scheduler tick (`scheduler.ts:87–108`) calls
  `claimDueWakeUps` — an autocommit `UPDATE … SET execution_state = 'pending', wake_at =
  NULL` on the pool (`instances.ts:137–161`) — and only _then_ enqueues one advance per
  row via `jobs.create` on steve's connection. Die between the two, or let `jobs.create`
  throw for row 3 of 5, and the affected rows are `pending` with `wake_at = NULL` and no
  job: `claimDueWakeUps` requires `waiting`, the correlator requires `waiting`, nothing
  will ever touch them again. `create()` (`workflow.ts:187–205`) is three autocommits;
  die after the first and the instance is `pending` forever. Steve cannot enlist in our
  transaction (`Jobs.create` → `_create` always uses `context.db`, `_create.ts:21,43`), so
  "enqueue inside the tx" is not an option — although `README.md:195` claims that is what
  happens (04 #1).

  > **Cut from the draft:** the correlator was listed here too. Its flip is inside the tick
  > transaction and the enqueue precedes the commit (`correlator.ts:198–210`), so its crash
  > windows are benign — a rollback either undoes everything or leaves a job that applies
  > `MATCHED` against a still-`waiting` row. Its real problems are semantic (02 #1).

- **Evidence.** `instances.ts:143–158`:
  ```sql
  UPDATE __workflow_instances SET execution_state = 'pending', wake_at = NULL, updated_at = now()
   WHERE id IN (SELECT id FROM __workflow_instances WHERE tenant_id = $1 AND execution_state = 'waiting'
                 AND wake_at IS NOT NULL AND wake_at <= now() ORDER BY wake_at LIMIT $4 FOR UPDATE SKIP LOCKED)
   RETURNING id, correlation_token
  ```
  followed at `scheduler.ts:97–105` by a `for` loop of `enqueueAdvance` calls.

- **Proposed change.** With #1's fence in place, make the tick a read-only poke and let the
  advance do every write under the lock:
  ```ts
  // scheduler tick — no UPDATE
  const due = await selectDueWakeUps(db, tenantId, batch); // SELECT id, seq ... WHERE waiting AND wake_at <= now() ORDER BY wake_at LIMIT n
  for (const r of due) {
  	await wf.enqueueAdvance({
  		tenant_id,
  		instance_id: r.id,
  		kind: "timeout",
  		expected_seq: r.seq,
  	});
  }

  const stale = await selectStalePending(db, tenantId, stalePendingSec, batch); // SELECT id, seq ... WHERE pending AND updated_at < now() - $sec
  for (const r of stale) {
  	await wf.enqueueAdvance({
  		tenant_id,
  		instance_id: r.id,
  		kind: "start",
  		expected_seq: r.seq,
  	});
  }
  ```
  The advance re-checks `waiting && wake_at <= now()` (the `timeout` precondition from #1),
  applies `TIMEOUT`, and clears `wake_at` itself. A row poked but not yet advanced is
  poked again next tick — a fenced no-op. Delete `claimDueWakeUps`. The stale-`pending`
  re-poke is safe _only_ because, after #1 and 02 #1, `create()` is the sole producer of
  `pending` (hence the T02 → T03 sequencing in `PROGRESS.md`). Add a partial index in
  migration `1.2.0` for the second scan:
  ```sql
  CREATE INDEX IF NOT EXISTS __workflow_instances_stale_pending_idx
  	ON __workflow_instances (tenant_id, updated_at) WHERE execution_state = 'pending';
  ```
  (`pending` rows are transient, so the index stays tiny.) New option
  `WorkflowSchedulerOptions.stalePendingSec` (default `300`; `0` disables).

- **Done when** — tests: "two `scheduler.tickOnce()` calls for one due row apply `TIMEOUT`
  once (exactly one `timeout` history row; cursor `_end_timeout`)"; "a `pending` row with
  no job and `updated_at` older than `stalePendingSec` is advanced by the next tick";
  `grep -rn claimDueWakeUps src/` returns nothing. The existing TIMEOUT test
  (`tests/stock_replenishment.test.ts:173–174`, `assertEquals(claimed, 1)`) is adjusted to
  the new return shape.

- **Affected files.** `src/scheduler.ts`, `src/persistence/instances.ts`, `src/driver.ts`
  (precondition from #1), `src/migrations/1_2_0.ts` (index), `tests/stock_replenishment.test.ts`,
  `tests/durability.test.ts`.

- **Effort / Value / Risk.** S–M / high / low. Healthy paths behave identically; only the
  moment `wake_at` is cleared moves from the tick to the advance.

- **Implementation notes.** `tickOnce()` can return `{ woken, repoked }` (or keep a number
  = sum). Keep `ORDER BY wake_at` and the batch size. An in-process "poked recently"
  `Map<id, ts>` to suppress duplicate pokes between ticks is a nice-to-have, not needed for
  correctness. The `timeout: true` flag becomes `kind: "timeout"`.

### 3. `workflow.advance` failures are unobserved; `expired` is terminal in steve and the docs say "retry"

- **Problem / observation.** `workflow.ts:131–144` subscribes `onDone` for effect types
  only. A `workflow.advance` job that throws — a guard or `action` throwing inside
  `fsm.transition` (fsm wraps and rethrows, `fsm.ts:414–449`), or any DB error — is
  retried by steve three times with 2 s / 4 s backoff (`_handle-failure.ts:56`) and then
  marked `failed`, silently. The instance stays `pending`/`running` with no job. After #2,
  a `pending` row is re-poked every `stalePendingSec` forever (a permanently-throwing guard
  becomes an infinite retry); a `running` row whose outcome died with the advance is
  stuck for good.

  Steve never retries a crashed job: `cleanup()` flips `running` rows older than 5 min to
  `expired`, terminal by design (`_mark-expired.ts:6–8`), and publishes `onDone`
  (`jobs.ts:920–922`). That reaper runs only with `autoCleanup: true`
  (`jobs.ts:451–454`, `:697–700`) or a manual `cleanup()`; the default is off. So a worker
  crash mid-effect means: `autoCleanup` on → instance `failed` after ~5 min via
  `failEffectJob`; off → instance `running` forever. `AGENTS.md:231` and `README.md:196`
  say steve "retries" — it does not. (04 #1 fixes the text; this finding fixes the
  behavior.)

- **Evidence.** `workflow.ts:131–144`:
  ```ts
  for (const name of handlerNames) {
  	const type = effectJobType(name);
  	this.jobs.onDone(type, (job) => {
  		if (job.status === JOB_STATUS.FAILED || job.status === JOB_STATUS.EXPIRED) {
  			failEffectJob(this.db, payload, `steve: ${job.status}`).catch(...)
  ```
  No subscription for `JOB_TYPE_ADVANCE`. `_mark-expired.ts:6–8`: "we don't auto-retry
  because the work may now be stale".

- **Proposed change.**
  1. `jobs.onDone(JOB_TYPE_ADVANCE, cb)`: `failed` → `failInstance` with the last attempt's
     `error_message` (`jobs.find(job.uid, true)` → `attempts.at(-1)?.error_message`,
     `jobs.ts:806–824`), history `failed { reason, job_uid }`; `expired` → re-enqueue the
     identical payload with `redispatch: (payload.redispatch ?? 0) + 1` while
     `< redispatchLimit`, else fail. If the original advance did commit before the crash,
     the re-dispatch is a fenced no-op (#1).
  2. Effect types: `expired` → same bounded re-dispatch of the identical effect payload
     (`seq` intact — a zombie handler that later completes produces a fenced duplicate;
     handler idempotency is already required); `failed` → `failEffectJob` as today (fenced).
  3. New options: `advanceMaxAttempts` (default `10`: exponential backoff sums to ≈17 min
     of DB-outage tolerance before an instance is failed; today's implicit `3` gives ≈6 s)
     passed as `max_attempts` where advance jobs are created (`workflow.ts:155`,
     `:202–205`); `redispatchLimit` (default `3`).

- **Done when** — tests: "an advance whose transition `action` throws every time ends with
  the instance `failed` and a `failed` history row whose `reason` contains the error
  message"; "an effect job marked `expired` via `jobs.cleanup(0)` is re-dispatched and the
  workflow completes"; "after `redispatchLimit` expiries the instance is `failed` with
  `effect_failed`".

- **Affected files.** `src/workflow.ts`, `src/driver.ts` (reason plumbing into
  `failInstance`), `src/types.ts` (`redispatch`), `API.md` (options).

- **Effort / Value / Risk.** S / high / low.

- **Implementation notes.** `jobs.cleanup(0)` reaps immediately in tests
  (`jobs.ts:904–916`; `Math.max(0, …)` at `_mark-expired.ts:20`). Steve wraps `onDone`
  callbacks in try/catch and only logs (`jobs.ts:1099–1105`) — keep the
  `.catch(clog.error)` pattern. Document that `autoCleanup: true` (or a periodic
  `cleanup()`) is what makes crash recovery happen at all; the README example should set
  it (04 #1).

### 4. No way to cancel or retry an instance

- **Problem / observation.** `EXECUTION_STATE.CANCELLED` and `HISTORY_EVENT.CANCELLED`
  exist (`types.ts:16`, `:126`) and the driver treats `cancelled` as terminal
  (`driver.ts:111–120`, `:397–403`), but nothing sets it: `Workflow` exposes
  `create`/`find`/`appendInbox`/`enqueueAdvance`/`enqueueEffect` only
  (`workflow.ts:177–232`). An instance waiting three days for a reply cannot be aborted
  except by SQL, and a `failed` instance (an API outage exhausted `effectMaxAttempts`)
  cannot be resumed — the only recovery is a new instance, which re-runs every side effect
  from the start.

- **Evidence.** `grep -rn "CANCELLED" src/` → only the two terminal checks in `driver.ts`
  and the constant definitions; no writer.

- **Proposed change.** On `Workflow`, tenant-scoped like `find`:
  ```ts
  /** Non-terminal → cancelled. Clears wake_at/correlation_token, bumps seq (in-flight jobs become stale). */
  async cancel(id: string, reason?: string): Promise<boolean>;
  /** failed → pending at the current cursor, bumps seq, enqueues { kind: "start" }. `force` also allows `running`. */
  async retry(id: string, opts?: { force?: boolean }): Promise<boolean>;
  ```
  Both return `false` when the state does not allow the operation. History: `cancelled
  { reason }`, and a new `HISTORY_EVENT.RETRIED { from_state }`. `force: true` is the
  operator asserting the effect is dead — the `seq` bump (#1) makes any zombie effect job
  stale, so this is safe by construction.

- **Done when** — tests: "`cancel()` on a `waiting` instance → `cancelled`; a later
  matching signal is marked processed without delivery; `scheduler.tickOnce()` ignores
  it"; "`cancel()` while an effect job is queued → the job completes with
  `{ skipped: "stale" }` and the handler is never called"; "`retry()` on a `failed`
  instance re-dispatches the cursor node's effect and the workflow completes".

- **Affected files.** `src/workflow.ts`, `src/persistence/instances.ts`, `src/types.ts`,
  `API.md`, `README.md` (Failure Modes).

- **Effort / Value / Risk.** S / med / low. Depends on #1.

- **Implementation notes.** `retry` from a `transition_rejected` failure re-dispatches the
  cursor node's effect and will fail again unless the handler was fixed — acceptable for a
  manual tool. If a "force an outcome" escape hatch is ever wanted, `enqueueAdvance` with
  `{ kind: "effect", expected_seq: row.seq, outcome }` is it; today's unfenced
  `enqueueAdvance` is that hatch by accident, and #1 closes it deliberately.

## Open questions / decisions needed

All resolved in the owner interview of 2026-08-26 (see `PROGRESS.md` → Decisions log):
column name `seq`; `advanceMaxAttempts = 10` and `redispatchLimit = 3`; `expired` →
bounded re-dispatch of the identical payload (advance and effect), then fail;
stale-`pending` re-poke on by default at `300` s (`0` disables).
