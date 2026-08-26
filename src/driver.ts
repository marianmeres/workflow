import { FSM } from "@marianmeres/fsm";
import type pg from "pg";
import { clog } from "./log.ts";
import { appendHistory } from "./persistence/history.ts";
import { lockInboxRow, markProcessed } from "./persistence/inbox.ts";
import {
	lockInstance,
	updateInstance,
} from "./persistence/instances.ts";
import { withTransaction } from "./persistence/tx.ts";
import type { WorkflowRegistry } from "./registry.ts";
import {
	type AdvanceJobPayload,
	type AdvanceKind,
	type EffectJobPayload,
	EXECUTION_STATE,
	HISTORY_EVENT,
	type InboxRow,
	JOB_TYPE_ADVANCE,
	JOB_TYPE_EFFECT_PREFIX,
	type NodeMeta,
	type WorkflowContext,
	type WorkflowInstanceRow,
} from "./types.ts";

/**
 * Reads the tenant scope off a job payload, tolerating the legacy `project_id`
 * key used before the 1.1.0 rename.
 *
 * Jobs already sitting in the steve queue when a deployment upgrades still
 * carry the old key. Without this fallback their `tenant_id` would read as
 * `undefined`, the history insert would violate its NOT NULL constraint, and
 * the job would retry until it exhausted its attempts.
 *
 * Safe to delete once no pre-1.1.0 jobs can remain in any deployed queue.
 */
function payloadTenantId(
	payload: AdvanceJobPayload | EffectJobPayload,
): string {
	return (payload.tenant_id ?? payload.project_id) as string;
}

/**
 * What produced this advance. Jobs queued before the fence existed carry no
 * `kind`, so it is inferred from the legacy payload shape: `timeout: true` was
 * the scheduler, an `outcome` without it was an effect completion or a matched
 * signal, and neither was the initial dispatch.
 *
 * Same transitional status as {@link payloadTenantId} — delete both together.
 */
function advanceKind(payload: AdvanceJobPayload): AdvanceKind {
	if (payload.kind) return payload.kind;
	if (payload.timeout === true) return "timeout";
	return payload.outcome !== undefined ? "effect" : "start";
}

/**
 * Synthetic event fired by the driver when entering a pure (decision) state,
 * so the user's guarded transitions can route by inspecting context.
 *
 * Pure nodes typically look like:
 *
 *     foo: {
 *       meta: { kind: "pure" },
 *       on: {
 *         ENTER: [
 *           { target: "go_left",  guard: (ctx) => ctx.x > 5 },
 *           { target: "go_right" },
 *         ],
 *       },
 *     }
 */
export const PURE_ENTER_EVENT = "ENTER";

/**
 * Event fired at a suspending node when a correlated inbox signal is delivered.
 * A node that does not accept it is not a delivery target at all — the
 * correlator defers the signal instead of poking (see `WorkflowInboxCorrelator`).
 */
export const SIGNAL_MATCHED_EVENT = "MATCHED";

/**
 * Function the driver uses to enqueue follow-up steve jobs. Supplied by the
 * `Workflow` class — the driver itself doesn't reach back into steve.Jobs.
 */
export interface JobEnqueuer {
	enqueueAdvance(
		client: pg.PoolClient | pg.Client | pg.Pool,
		payload: AdvanceJobPayload,
	): Promise<void>;
	enqueueEffect(
		client: pg.PoolClient | pg.Client | pg.Pool,
		handler: string,
		payload: EffectJobPayload,
	): Promise<void>;
}

/**
 * Maximum number of inline pure-state hops the driver will take within a
 * single advance before bailing out (loop guard against pathological
 * pure-state cycles).
 */
const MAX_PURE_HOPS = 64;

/**
 * The `workflow.advance` job body.
 *
 * Wrapped in a single PG transaction:
 * 1. Lock the instance row.
 * 2. If in a terminal execution_state, no-op.
 * 2b. If the payload's fence (`expected_seq`) does not match the locked row, or
 *    the row is not in the state this `kind` of advance expects, no-op — the job
 *    is a duplicate, or a zombie from a step the instance already left.
 * 2c. `kind: "signal"` → lock the inbox row; it supplies the outcome data and is
 *    marked processed in this same transaction.
 * 3. If `outcome` was supplied, apply it via fsm.transition(outcome, data).
 *    - Reject → mark failed, append history.
 * 4. Loop on the current state's meta.kind:
 *    - terminal   → mark completed, return.
 *    - pure       → fire `ENTER` synthetic event (with guards) and loop.
 *    - effectful  → enqueue an effect job + mark running, return.
 *    - suspending → mark waiting, set wake_at/correlation_token, return.
 */
export async function runAdvance(
	pool: pg.Pool,
	registry: WorkflowRegistry,
	enqueuer: JobEnqueuer,
	payload: AdvanceJobPayload,
): Promise<void> {
	const { instance_id } = payload;
	const tenant_id = payloadTenantId(payload);
	const kind = advanceKind(payload);

	await withTransaction(pool, async (client) => {
		const row = await lockInstance(client, instance_id);
		if (!row) {
			// Also the window where the creating transaction has not committed yet
			// (steve's job insert is autocommit on its own connection, so the job
			// can be claimed first). Throw rather than drop the only job the
			// instance will ever get — steve retries.
			throw new Error(`advance: instance ${instance_id} not found`);
		}

		if (
			row.execution_state === EXECUTION_STATE.COMPLETED ||
			row.execution_state === EXECUTION_STATE.FAILED ||
			row.execution_state === EXECUTION_STATE.CANCELLED
		) {
			clog.debug?.(
				`advance: instance ${instance_id} already terminal (${row.execution_state}); no-op`,
			);
			return;
		}

		// The fence. Only meaningful against the locked row, hence its position.
		// A mismatch means the instance has settled at least once since this job
		// was issued: the job is a duplicate or a zombie. Debug-log it and drop
		// it — with re-poking ticks these are routine, not events, so no history.
		if (payload.expected_seq !== undefined && row.seq !== payload.expected_seq) {
			clog.debug?.(
				`advance: stale (row.seq=${row.seq}, expected=${payload.expected_seq}); no-op`,
			);
			return;
		}

		// Per-kind preconditions. Applied only to payloads that state their kind:
		// on a legacy payload the kind is a guess, and guessing wrong must not
		// swallow the job. `timeout` gets its own once the scheduler tick stops
		// pre-flipping the row to `pending` (01-durability #2).
		if (
			(payload.kind === "start" &&
				row.execution_state !== EXECUTION_STATE.PENDING) ||
			(payload.kind === "effect" &&
				row.execution_state !== EXECUTION_STATE.RUNNING) ||
			(payload.kind === "signal" &&
				row.execution_state !== EXECUTION_STATE.WAITING)
		) {
			clog.debug?.(
				`advance: ${payload.kind} precondition not met (${row.execution_state}); no-op`,
			);
			return;
		}

		// A signal advance carries the inbox row id, not its payload: the row is
		// the source of truth, and marking it processed happens in this very
		// transaction, so delivery and transition commit together or not at all.
		let outcome = payload.outcome;
		let outcome_data = payload.outcome_data;
		let inbox: InboxRow | null = null;
		if (payload.kind === "signal" && payload.inbox_id) {
			inbox = await lockInboxRow(client, payload.inbox_id);
			if (!inbox || inbox.processed_at) {
				clog.debug?.(
					`advance: inbox row ${payload.inbox_id} already delivered; no-op`,
				);
				return;
			}
			outcome = SIGNAL_MATCHED_EVENT;
			outcome_data = inbox.payload;
		}

		let def;
		try {
			def = registry.requireDefinition(row.definition_id, row.definition_version);
		} catch (e) {
			await failInstance(
				client,
				row,
				`Unknown definition ${row.definition_id}@${row.definition_version}: ${String(e)}`,
			);
			return;
		}

		// Construct the FSM at the saved cursor. fromSnapshot skips onEnter
		// (a resume is not entry) and preserves previous.
		const positioned = FSM.fromSnapshot(def.fsm, {
			state: row.cursor,
			previous: row.previous_cursor,
			context: row.context,
		});

		// If an outcome was supplied, apply it. This is the wake-up after an effect
		// completion, a matched signal, or a timer expiry.
		if (outcome !== undefined) {
			const before = positioned.state;
			const result = positioned.transition(
				outcome,
				outcome_data ?? undefined,
				false,
			);
			if (result === null) {
				await appendHistory(client, {
					tenant_id,
					instance_id,
					event_type: HISTORY_EVENT.TRANSITION_REJECTED,
					from_node: before,
					data: { outcome, outcome_data, reason: "fsm rejected transition" },
				});
				await failInstance(
					client,
					row,
					`fsm rejected outcome "${outcome}" at state "${before}"`,
				);
				return;
			}
			// Optionally merge outcome_data into context here. Convention: handlers
			// return `{ outcome, data }` and the data is the payload — fsm action
			// hooks merge it if the user wires them. We don't auto-merge to keep
			// state shape under the user's control; the data is available via the
			// payload arg to actions/guards.
			if (kind === "effect" && payload.handler) {
				await appendHistory(client, {
					tenant_id,
					instance_id,
					event_type: HISTORY_EVENT.EFFECT_COMPLETED,
					from_node: before,
					data: { handler: payload.handler, outcome },
				});
			}
			if (inbox) {
				await appendHistory(client, {
					tenant_id,
					instance_id,
					event_type: HISTORY_EVENT.SIGNAL_RECEIVED,
					from_node: before,
					data: { inbox_id: inbox.id, source: inbox.source },
				});
				await markProcessed(client, inbox.id);
			}
			await appendHistory(client, {
				tenant_id,
				instance_id,
				event_type: kind === "timeout"
					? HISTORY_EVENT.TIMEOUT
					: HISTORY_EVENT.TRANSITION,
				from_node: before,
				to_node: positioned.state,
				data: { outcome, outcome_data },
			});
		}

		// Drive forward through pure states, settling at terminal/effectful/suspending.
		for (let hop = 0; hop < MAX_PURE_HOPS; hop++) {
			const meta = positioned.getCurrentMeta<NodeMeta>();
			if (!meta) {
				await failInstance(
					client,
					row,
					`state "${positioned.state}" has no meta`,
				);
				return;
			}

			switch (meta.kind) {
				case "terminal": {
					await updateInstance(client, instance_id, {
						cursor: positioned.state,
						previous_cursor: positioned.previous,
						context: positioned.context as WorkflowContext,
						execution_state: EXECUTION_STATE.COMPLETED,
						wake_at: null,
						correlation_token: null,
					}, { bumpSeq: true });
					await appendHistory(client, {
						tenant_id,
						instance_id,
						event_type: HISTORY_EVENT.COMPLETED,
						from_node: positioned.state,
					});
					return;
				}
				case "effectful": {
					// Persist context (any guards/actions may have updated it) + flip to running.
					const settled = await updateInstance(client, instance_id, {
						cursor: positioned.state,
						previous_cursor: positioned.previous,
						context: positioned.context as WorkflowContext,
						execution_state: EXECUTION_STATE.RUNNING,
						wake_at: null,
					}, { bumpSeq: true });
					await enqueuer.enqueueEffect(client, meta.handler, {
						tenant_id,
						instance_id,
						handler: meta.handler,
						seq: settled.seq,
						cursor: settled.cursor,
					});
					await appendHistory(client, {
						tenant_id,
						instance_id,
						event_type: HISTORY_EVENT.EFFECT_DISPATCHED,
						from_node: positioned.state,
						data: { handler: meta.handler },
					});
					return;
				}
				case "suspending": {
					const wake_at = meta.timeoutSec
						? new Date(Date.now() + meta.timeoutSec * 1000)
						: null;
					// correlation_token already on the row (set at create-time or by the user
					// via context). We don't auto-generate one here.
					await updateInstance(client, instance_id, {
						cursor: positioned.state,
						previous_cursor: positioned.previous,
						context: positioned.context as WorkflowContext,
						execution_state: EXECUTION_STATE.WAITING,
						wake_at,
					}, { bumpSeq: true });
					await appendHistory(client, {
						tenant_id,
						instance_id,
						event_type: HISTORY_EVENT.TRANSITION,
						from_node: positioned.state,
						data: { suspended: true, wake_at, matcher: meta.matcher ?? null },
					});
					return;
				}
				case "pure": {
					const before = positioned.state;
					const result = positioned.transition(PURE_ENTER_EVENT, undefined, false);
					if (result === null) {
						await appendHistory(client, {
							tenant_id,
							instance_id,
							event_type: HISTORY_EVENT.TRANSITION_REJECTED,
							from_node: before,
							data: { event: PURE_ENTER_EVENT, reason: "no guarded transition matched" },
						});
						await failInstance(
							client,
							row,
							`pure state "${before}" did not route on ENTER`,
						);
						return;
					}
					await appendHistory(client, {
						tenant_id,
						instance_id,
						event_type: HISTORY_EVENT.TRANSITION,
						from_node: before,
						to_node: positioned.state,
						data: { event: PURE_ENTER_EVENT },
					});
					// loop
					break;
				}
				default: {
					await failInstance(
						client,
						row,
						`unknown meta.kind "${String((meta as { kind?: string }).kind)}" at state "${positioned.state}"`,
					);
					return;
				}
			}
		}

		await failInstance(
			client,
			row,
			`exceeded ${MAX_PURE_HOPS} pure-state hops (likely cycle)`,
		);
	});
}

async function failInstance(
	client: pg.PoolClient | pg.Client,
	row: WorkflowInstanceRow,
	reason: string,
): Promise<void> {
	clog.error?.(`workflow instance ${row.id} failed: ${reason}`);
	await updateInstance(client, row.id, {
		execution_state: EXECUTION_STATE.FAILED,
	}, { bumpSeq: true });
	await appendHistory(client, {
		tenant_id: row.tenant_id,
		instance_id: row.id,
		event_type: HISTORY_EVENT.FAILED,
		from_node: row.cursor,
		data: { reason },
	});
}

/**
 * The `workflow.effect.<handlerName>` job body.
 *
 * 1. Looks up the user handler in the registry by `payload.handler`.
 * 2. Loads the instance to grab the current context (no lock — handlers run
 *    outside the advance tx).
 * 3. Checks the fence: the handler runs only if the row is still at the `seq`
 *    that dispatched it. A row that moved on means this job is a duplicate or a
 *    zombie — skip, so the side effect does not fire twice.
 * 4. Runs the handler.
 * 5. On success: enqueues a `workflow.advance` job with the outcome + data.
 * 6. On throw: re-throws so steve records the attempt as failed (steve retries
 *    per the job's `max_attempts`). When steve gives up, the on-failed hook in
 *    Workflow marks the instance failed.
 *
 * Handlers must be idempotent — steve may retry the handler if a worker crashes
 * mid-execution, and even successful handlers can run twice if completion-write
 * fails after the advance has been enqueued.
 */
export async function runEffect(
	pool: pg.Pool,
	registry: WorkflowRegistry,
	enqueuer: JobEnqueuer,
	payload: EffectJobPayload,
	signal?: AbortSignal,
): Promise<
	{ outcome: string; data?: Record<string, unknown> } | { skipped: "stale" }
> {
	const { instance_id, handler: handlerName } = payload;
	const tenant_id = payloadTenantId(payload);
	const handler = registry.requireHandler(handlerName);

	const client = await pool.connect();
	let row: WorkflowInstanceRow | null;
	try {
		const r = await client.query<WorkflowInstanceRow>(
			`SELECT id, tenant_id, definition_id, definition_version, cursor,
			        previous_cursor, context, execution_state, wake_at,
			        correlation_token, seq, created_at, updated_at
			   FROM __workflow_instances WHERE id = $1`,
			[instance_id],
		);
		row = r.rows[0] ?? null;
	} finally {
		client.release();
	}

	if (!row) {
		throw new Error(`effect: instance ${instance_id} not found`);
	}

	if (payload.seq !== undefined && row.seq !== payload.seq) {
		if (row.seq < payload.seq) {
			// The dispatching advance has not committed yet — steve's job insert
			// is autocommit on its own connection, so the job can be claimed
			// before the transaction that enqueued it lands. Throw to retry.
			throw new Error(
				`effect: instance ${instance_id} not yet at seq ${payload.seq} (row.seq=${row.seq})`,
			);
		}
		clog.debug?.(
			`effect: stale ${handlerName} (row.seq=${row.seq}, job seq=${payload.seq}); skipped`,
		);
		return { skipped: "stale" };
	}
	if (row.execution_state !== EXECUTION_STATE.RUNNING) {
		clog.debug?.(
			`effect: ${handlerName} on a non-running instance (${row.execution_state}); skipped`,
		);
		return { skipped: "stale" };
	}

	const result = await handler({
		instanceId: instance_id,
		tenantId: tenant_id,
		context: row.context,
		signal,
	});

	await enqueuer.enqueueAdvance(pool, {
		tenant_id,
		instance_id,
		kind: "effect",
		expected_seq: payload.seq,
		outcome: result.outcome,
		outcome_data: result.data,
		handler: handlerName,
	});

	return { outcome: result.outcome, data: result.data };
}

/**
 * Called by the Workflow layer when steve gives up on an effect job (all
 * attempts exhausted or expired). Marks the workflow instance as failed —
 * unless the job is fenced out, i.e. it belongs to a step the instance has
 * already left, in which case a dead job must not kill a healthy instance.
 */
export async function failEffectJob(
	pool: pg.Pool,
	payload: EffectJobPayload,
	reason: string,
): Promise<void> {
	await withTransaction(pool, async (client) => {
		const row = await lockInstance(client, payload.instance_id);
		if (!row) return;
		if (payload.seq !== undefined && row.seq !== payload.seq) {
			clog.debug?.(
				`failEffectJob: stale (row.seq=${row.seq}, job seq=${payload.seq}); no-op`,
			);
			return;
		}
		if (
			row.execution_state === EXECUTION_STATE.COMPLETED ||
			row.execution_state === EXECUTION_STATE.FAILED ||
			row.execution_state === EXECUTION_STATE.CANCELLED
		) {
			return;
		}
		await updateInstance(client, row.id, {
			execution_state: EXECUTION_STATE.FAILED,
		}, { bumpSeq: true });
		await appendHistory(client, {
			tenant_id: row.tenant_id,
			instance_id: row.id,
			event_type: HISTORY_EVENT.EFFECT_FAILED,
			from_node: row.cursor,
			data: { handler: payload.handler, reason },
		});
	});
}

/** Composite steve job-type string for an effect handler. */
export function effectJobType(handlerName: string): string {
	return `${JOB_TYPE_EFFECT_PREFIX}${handlerName}`;
}

export { JOB_TYPE_ADVANCE };
