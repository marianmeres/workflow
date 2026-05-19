import { FSM } from "@marianmeres/fsm";
import type pg from "pg";
import { clog } from "./log.ts";
import { appendHistory } from "./persistence/history.ts";
import {
	lockInstance,
	updateInstance,
} from "./persistence/instances.ts";
import { withTransaction } from "./persistence/tx.ts";
import type { WorkflowRegistry } from "./registry.ts";
import {
	type AdvanceJobPayload,
	type EffectJobPayload,
	EXECUTION_STATE,
	HISTORY_EVENT,
	JOB_TYPE_ADVANCE,
	JOB_TYPE_EFFECT_PREFIX,
	type NodeMeta,
	type WorkflowContext,
	type WorkflowInstanceRow,
} from "./types.ts";

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
	const { instance_id, project_id, outcome, outcome_data, timeout } = payload;

	await withTransaction(pool, async (client) => {
		const row = await lockInstance(client, instance_id);
		if (!row) {
			clog.warn?.(`advance: instance ${instance_id} not found`);
			return;
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
					project_id,
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
			await appendHistory(client, {
				project_id,
				instance_id,
				event_type: timeout ? HISTORY_EVENT.TIMEOUT : HISTORY_EVENT.TRANSITION,
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
					});
					await appendHistory(client, {
						project_id,
						instance_id,
						event_type: HISTORY_EVENT.COMPLETED,
						from_node: positioned.state,
					});
					return;
				}
				case "effectful": {
					// Persist context (any guards/actions may have updated it) + flip to running.
					await updateInstance(client, instance_id, {
						cursor: positioned.state,
						previous_cursor: positioned.previous,
						context: positioned.context as WorkflowContext,
						execution_state: EXECUTION_STATE.RUNNING,
						wake_at: null,
					});
					await enqueuer.enqueueEffect(client, meta.handler, {
						project_id,
						instance_id,
						handler: meta.handler,
					});
					await appendHistory(client, {
						project_id,
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
					});
					await appendHistory(client, {
						project_id,
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
							project_id,
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
						project_id,
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
	});
	await appendHistory(client, {
		project_id: row.project_id,
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
 * 3. Runs the handler.
 * 4. On success: enqueues a `workflow.advance` job with the outcome + data.
 * 5. On throw: re-throws so steve records the attempt as failed (steve retries
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
): Promise<{ outcome: string; data?: Record<string, unknown> }> {
	const { instance_id, project_id, handler: handlerName } = payload;
	const handler = registry.requireHandler(handlerName);

	const client = await pool.connect();
	let row: WorkflowInstanceRow | null;
	try {
		const r = await client.query<WorkflowInstanceRow>(
			`SELECT id, project_id, definition_id, definition_version, cursor,
			        previous_cursor, context, execution_state, wake_at,
			        correlation_token, created_at, updated_at
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

	const result = await handler({
		instanceId: instance_id,
		projectId: project_id,
		context: row.context,
		signal,
	});

	await enqueuer.enqueueAdvance(pool, {
		project_id,
		instance_id,
		outcome: result.outcome,
		outcome_data: result.data,
	});

	return { outcome: result.outcome, data: result.data };
}

/**
 * Called by the Workflow layer when steve gives up on an effect job (all
 * attempts exhausted or expired). Marks the workflow instance as failed.
 */
export async function failEffectJob(
	pool: pg.Pool,
	payload: EffectJobPayload,
	reason: string,
): Promise<void> {
	await withTransaction(pool, async (client) => {
		const row = await lockInstance(client, payload.instance_id);
		if (!row) return;
		if (
			row.execution_state === EXECUTION_STATE.COMPLETED ||
			row.execution_state === EXECUTION_STATE.FAILED ||
			row.execution_state === EXECUTION_STATE.CANCELLED
		) {
			return;
		}
		await updateInstance(client, row.id, {
			execution_state: EXECUTION_STATE.FAILED,
		});
		await appendHistory(client, {
			project_id: row.project_id,
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
