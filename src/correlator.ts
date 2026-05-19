import type { Cron } from "@marianmeres/cron";
import type { FSMConfig } from "@marianmeres/fsm";
import type pg from "pg";
import { clog } from "./log.ts";
import { appendHistory } from "./persistence/history.ts";
import { claimUnprocessed, markProcessed } from "./persistence/inbox.ts";
import { findWaitingByCorrelation } from "./persistence/instances.ts";
import { withTransaction } from "./persistence/tx.ts";
import {
	EXECUTION_STATE,
	HISTORY_EVENT,
	type InboxRow,
	type NodeMeta,
	type WorkflowContext,
} from "./types.ts";
import type { Workflow } from "./workflow.ts";

/** Options accepted by {@link WorkflowInboxCorrelator}. */
export interface WorkflowInboxCorrelatorOptions {
	/**
	 * Externally-owned `@marianmeres/cron` `Cron` instance. The correlator
	 * registers its tick handler on this instance when `register()` is called.
	 * The consumer owns the lifecycle: `await cron.start(N)` / `await cron.stop()`.
	 */
	cron: Cron;
	/** The Workflow whose waiting instances this correlator delivers signals to. */
	workflow: Workflow;
	/** Cron expression for the tick. Default: every minute. */
	tickExpression?: string;
	/** IANA timezone for the cron expression. */
	timezone?: string | null;
	/** Maximum number of inbox rows claimed per tick. Default: 100. */
	tickBatchSize?: number;
	/** Custom name for the registered cron job. Default: `workflow.correlator.<projectId>`. */
	tickName?: string;
}

const DEFAULT_TICK_EXPRESSION = "* * * * *";

/**
 * Cron-driven correlator that matches `__workflow_inbox` rows to waiting
 * workflow instances. On each tick:
 *
 * 1. Claim a batch of unprocessed inbox rows (`FOR UPDATE SKIP LOCKED`).
 * 2. For each row, find a waiting instance with the same correlation_token.
 * 3. Resolve the instance's current-state `meta.matcher` (if any), call it
 *    against the signal. The matcher is the semantic filter — the token is
 *    just the index lookup.
 * 4. On match: enqueue an advance with `outcome: 'MATCHED'` carrying the
 *    signal payload, mark the inbox row processed, history entry.
 * 5. On no-match (or no waiting instance): mark inbox row processed with
 *    `signal_rejected` history entry against the instance (if there was one).
 *
 * Like {@link WorkflowScheduler}, the correlator does not own its `Cron`. The
 * caller constructs a `Cron`, passes it in, runs its lifecycle. Call
 * `register()` once after construction to attach the tick.
 */
export class WorkflowInboxCorrelator {
	readonly cron: Cron;
	readonly projectId: string;
	readonly tickExpression: string;
	readonly tickName: string;

	readonly #workflow: Workflow;
	readonly #tickBatchSize: number;
	readonly #timezone: string | null | undefined;

	constructor(options: WorkflowInboxCorrelatorOptions) {
		this.cron = options.cron;
		this.#workflow = options.workflow;
		this.projectId = options.workflow.projectId;
		this.tickExpression = options.tickExpression ?? DEFAULT_TICK_EXPRESSION;
		this.tickName = options.tickName ?? `workflow.correlator.${this.projectId}`;
		this.#tickBatchSize = options.tickBatchSize ?? 100;
		this.#timezone = options.timezone;
	}

	/**
	 * Registers the tick on the injected `Cron`. Idempotent — re-registering an
	 * existing name updates the expression/options but preserves `next_run_at`.
	 */
	async register(): Promise<void> {
		await this.cron.register(
			this.tickName,
			this.tickExpression,
			async () => {
				await this.#tick();
			},
			{
				timezone: this.#timezone,
			},
		);
	}

	/** Removes the tick from the injected `Cron`. */
	async unregister(): Promise<void> {
		await this.cron.unregister(this.tickName);
	}

	/** Runs one tick immediately. Exposed for testing and on-demand matches. */
	async tickOnce(): Promise<number> {
		return await this.#tick();
	}

	async #tick(): Promise<number> {
		let processed = 0;

		await withTransaction(this.#workflow.db, async (client) => {
			const rows = await claimUnprocessed(
				client,
				this.projectId,
				this.#tickBatchSize,
			);
			for (const row of rows) {
				await this.#processOne(client, row);
				processed++;
			}
		});

		if (processed > 0) clog.debug?.(`correlator: processed ${processed} signals`);
		return processed;
	}

	async #processOne(client: pg.PoolClient, row: InboxRow): Promise<void> {
		const instance = await findWaitingByCorrelation(
			client,
			this.projectId,
			row.correlation_token,
		);
		if (!instance) {
			await markProcessed(client, row.id);
			return;
		}

		const def = this.#workflow.registry.getDefinition(
			instance.definition_id,
			instance.definition_version,
		);
		const stateCfg = def?.fsm.states[instance.cursor as keyof typeof def.fsm.states] as
			| (FSMConfig<string, string, WorkflowContext>["states"][string])
			| undefined;
		const meta = stateCfg?.meta as NodeMeta | undefined;

		// Run matcher if any. No matcher = correlation_token alone is the gate.
		let matched = true;
		if (meta && meta.kind === "suspending" && meta.matcher) {
			const matcherFn = this.#workflow.registry.getMatcher(meta.matcher);
			if (!matcherFn) {
				clog.error?.(
					`correlator: matcher "${meta.matcher}" not registered for instance ${instance.id}`,
				);
				await appendHistory(client, {
					project_id: this.projectId,
					instance_id: instance.id,
					event_type: HISTORY_EVENT.SIGNAL_REJECTED,
					from_node: instance.cursor,
					data: { reason: `unregistered matcher "${meta.matcher}"` },
				});
				await markProcessed(client, row.id);
				return;
			}
			try {
				matched = await matcherFn({
					instanceId: instance.id,
					projectId: this.projectId,
					context: instance.context,
					signal: row,
				});
			} catch (e) {
				clog.error?.(`correlator: matcher "${meta.matcher}" threw: ${e}`);
				matched = false;
			}
		}

		if (!matched) {
			await appendHistory(client, {
				project_id: this.projectId,
				instance_id: instance.id,
				event_type: HISTORY_EVENT.SIGNAL_REJECTED,
				from_node: instance.cursor,
				data: { source: row.source, inbox_id: row.id },
			});
			await markProcessed(client, row.id);
			return;
		}

		await appendHistory(client, {
			project_id: this.projectId,
			instance_id: instance.id,
			event_type: HISTORY_EVENT.SIGNAL_RECEIVED,
			from_node: instance.cursor,
			data: { source: row.source, inbox_id: row.id },
		});
		await markProcessed(client, row.id);

		// Flip to pending so the scheduler / a re-tick won't double-fire. The
		// advance handler will load and route based on cursor/meta.
		await client.query(
			`UPDATE __workflow_instances
			    SET execution_state = $2, wake_at = NULL, updated_at = now()
			  WHERE id = $1`,
			[instance.id, EXECUTION_STATE.PENDING],
		);

		await this.#workflow.enqueueAdvance(this.#workflow.db, {
			project_id: this.projectId,
			instance_id: instance.id,
			outcome: "MATCHED",
			outcome_data: row.payload,
		});
	}
}
