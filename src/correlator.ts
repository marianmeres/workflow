import type { Cron } from "@marianmeres/cron";
import type { FSMConfig } from "@marianmeres/fsm";
import type pg from "pg";
import { SIGNAL_MATCHED_EVENT } from "./driver.ts";
import { clog } from "./log.ts";
import { appendHistory } from "./persistence/history.ts";
import { claimUnprocessed, markProcessed } from "./persistence/inbox.ts";
import { findByCorrelation, findTerminalByCorrelation } from "./persistence/instances.ts";
import { withTransaction } from "./persistence/tx.ts";
import {
	type AdvanceJobPayload,
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
	/** Custom name for the registered cron job. Default: `workflow.correlator.<tenantId>`. */
	tickName?: string;
}

const DEFAULT_TICK_EXPRESSION = "* * * * *";

/**
 * Cron-driven correlator that matches `__workflow_inbox` rows to workflow
 * instances. On each tick it claims a batch of unprocessed rows
 * (`FOR UPDATE SKIP LOCKED`) and, per row, finds the live instance owning the
 * correlation token:
 *
 * | Instance                                        | Action                                              |
 * | ----------------------------------------------- | --------------------------------------------------- |
 * | none / terminal                                 | mark processed (+ `signal_rejected` if terminal)    |
 * | live but not `waiting`                          | defer                                               |
 * | `waiting` at a node that has no `MATCHED` edge  | defer                                               |
 * | `waiting`, matcher says no                      | `signal_rejected`, mark processed                   |
 * | `waiting`, matcher says yes                     | poke an advance; the advance does the rest          |
 *
 * "Defer" means: leave the row unprocessed and look at it again next tick. A
 * signal that arrives before its wait point — the instance is still running the
 * step that emits the token, or sits at a timer-only node — is early, not
 * wrong; consuming it would lose it, and delivering it to a node with no
 * `MATCHED` edge would fail the instance.
 *
 * The correlator only pokes: it never writes the instance row. The advance
 * transitions the instance, records `signal_received` and marks the inbox row
 * processed in one transaction, so a crashed poke is simply re-poked next tick.
 *
 * Starvation note: rows are claimed `ORDER BY received_at LIMIT tickBatchSize`,
 * so a backlog of deferred rows larger than the batch would shadow newer ones.
 *
 * Like {@link WorkflowScheduler}, the correlator does not own its `Cron`. The
 * caller constructs a `Cron`, passes it in, runs its lifecycle. Call
 * `register()` once after construction to attach the tick.
 */
export class WorkflowInboxCorrelator {
	readonly cron: Cron;
	readonly tenantId: string;
	readonly tickExpression: string;
	readonly tickName: string;

	readonly #workflow: Workflow;
	readonly #tickBatchSize: number;
	readonly #timezone: string | null | undefined;

	constructor(options: WorkflowInboxCorrelatorOptions) {
		this.cron = options.cron;
		this.#workflow = options.workflow;
		this.tenantId = options.workflow.tenantId;
		this.tickExpression = options.tickExpression ?? DEFAULT_TICK_EXPRESSION;
		this.tickName = options.tickName ?? `workflow.correlator.${this.tenantId}`;
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

	/**
	 * Runs one tick immediately. Exposed for testing and on-demand matches.
	 * Returns the number of rows resolved — poked, rejected or marked processed.
	 * Deferred rows are not counted; they are still there next tick.
	 */
	async tickOnce(): Promise<number> {
		return await this.#tick();
	}

	async #tick(): Promise<number> {
		const pokes: AdvanceJobPayload[] = [];

		const resolved = await withTransaction(this.#workflow.db, async (client) => {
			const rows = await claimUnprocessed(
				client,
				this.tenantId,
				this.#tickBatchSize,
			);
			// One poke per instance per tick: a second signal for the same
			// instance would be fenced out by the first anyway, and deferring it
			// keeps it for the instance's next wait point.
			const poked = new Set<string>();
			let n = 0;
			for (const row of rows) {
				if (await this.#processOne(client, row, poked, pokes)) n++;
			}
			return n;
		});

		// Deliberately after the commit: the advance blocks on the inbox row's
		// lock, which this tick holds until then, and steve's insert needs a pool
		// connection of its own — enqueuing under the claim risks starving the
		// pool. Nothing is lost if the process dies here; the row is still
		// unprocessed, so the next tick pokes again.
		for (const payload of pokes) {
			await this.#workflow.enqueueAdvance(this.#workflow.db, payload);
		}

		if (resolved > 0) clog.debug?.(`correlator: resolved ${resolved} signals`);
		return resolved;
	}

	/** Leaves the row unprocessed for a later tick. Always returns `false`. */
	#defer(row: InboxRow, reason: string): boolean {
		clog.debug?.(`correlator: deferring inbox row ${row.id} (${reason})`);
		return false;
	}

	/**
	 * True if the row was resolved, false if it was deferred. A delivery is
	 * resolved by appending to `pokes` — the tick enqueues those after it commits.
	 */
	async #processOne(
		client: pg.PoolClient,
		row: InboxRow,
		poked: Set<string>,
		pokes: AdvanceJobPayload[],
	): Promise<boolean> {
		const instance = await findByCorrelation(
			client,
			this.tenantId,
			row.correlation_token,
		);

		// A token no live instance owns is an upstream bug, not something to wait
		// for. (A completed instance has its token cleared, so it lands here too —
		// only failed/cancelled ones are still findable.)
		if (!instance) {
			clog.warn?.(
				`correlator: no live instance for token "${row.correlation_token}" (inbox row ${row.id})`,
			);
			const terminal = await findTerminalByCorrelation(
				client,
				this.tenantId,
				row.correlation_token,
			);
			if (terminal) {
				await appendHistory(client, {
					tenant_id: this.tenantId,
					instance_id: terminal.id,
					event_type: HISTORY_EVENT.SIGNAL_REJECTED,
					from_node: terminal.cursor,
					data: {
						source: row.source,
						inbox_id: row.id,
						reason: `instance is ${terminal.execution_state}`,
					},
				});
			}
			await markProcessed(client, row.id);
			return true;
		}

		if (instance.execution_state !== EXECUTION_STATE.WAITING) {
			return this.#defer(row, `instance ${instance.id} is not waiting yet`);
		}
		if (poked.has(instance.id)) {
			return this.#defer(row, `instance ${instance.id} already poked this tick`);
		}

		const def = this.#workflow.registry.getDefinition(
			instance.definition_id,
			instance.definition_version,
		);
		const stateCfg = def?.fsm
			.states[instance.cursor as keyof typeof def.fsm.states] as
				| (FSMConfig<string, string, WorkflowContext>["states"][string])
				| undefined;

		// A node with no MATCHED (or wildcard) edge is not a delivery target:
		// poking it would make the fsm reject the transition and fail the
		// instance. Typically a timer-only delay node on the way to the real wait.
		const on = stateCfg?.on ?? {};
		if (!(SIGNAL_MATCHED_EVENT in on) && !("*" in on)) {
			return this.#defer(row, `node "${instance.cursor}" does not accept MATCHED`);
		}

		// Run matcher if any. No matcher = correlation_token alone is the gate.
		const meta = stateCfg?.meta as NodeMeta | undefined;
		if (meta && meta.kind === "suspending" && meta.matcher) {
			let matched: boolean;
			try {
				const matcherFn = this.#workflow.registry.requireMatcher(meta.matcher);
				matched = await matcherFn({
					instanceId: instance.id,
					tenantId: this.tenantId,
					context: instance.context,
					signal: row,
				});
			} catch (e) {
				// Deferring keeps a transient failure (a lookup, an HTTP call) from
				// consuming the signal for good. A deterministic throw retries every
				// tick with an error line — noisy, but not silent.
				clog.error?.(`correlator: matcher "${meta.matcher}" threw: ${e}`);
				return this.#defer(row, "matcher threw");
			}
			if (!matched) {
				await appendHistory(client, {
					tenant_id: this.tenantId,
					instance_id: instance.id,
					event_type: HISTORY_EVENT.SIGNAL_REJECTED,
					from_node: instance.cursor,
					data: { source: row.source, inbox_id: row.id },
				});
				await markProcessed(client, row.id);
				return true;
			}
		}

		// Poke only — no write to the instance row. The advance re-reads the inbox
		// row under a lock, transitions, and marks it processed in one transaction;
		// the fence makes a duplicate poke a no-op.
		poked.add(instance.id);
		pokes.push({
			tenant_id: this.tenantId,
			instance_id: instance.id,
			kind: "signal",
			expected_seq: instance.seq,
			inbox_id: row.id,
		});
		return true;
	}
}
