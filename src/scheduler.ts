import type { Cron } from "@marianmeres/cron";
import { clog } from "./log.ts";
import {
	type InstancePoke,
	selectDueWakeUps,
	selectStalePending,
} from "./persistence/instances.ts";
import type { Workflow } from "./workflow.ts";
import { type AdvanceJobPayload, TIMEOUT_EVENT } from "./types.ts";

/** Options accepted by {@link WorkflowScheduler}. */
export interface WorkflowSchedulerOptions {
	/**
	 * Externally-owned `@marianmeres/cron` `Cron` instance. The scheduler
	 * registers its tick handler on this instance when `register()` is called.
	 * The consumer owns the lifecycle: `await cron.start(N)` / `await cron.stop()`.
	 */
	cron: Cron;
	/** The Workflow whose instances this scheduler wakes (used for `db`, `tenantId`, `enqueueAdvance`). */
	workflow: Workflow;
	/** Cron expression for the tick. Default: every minute. */
	tickExpression?: string;
	/** IANA timezone for the cron expression. */
	timezone?: string | null;
	/** Maximum number of rows poked per tick, per scan. Default: 100. */
	tickBatchSize?: number;
	/** Custom name for the registered cron job. Default: `workflow.scheduler.<tenantId>`. */
	tickName?: string;
	/**
	 * Age in seconds at which a `pending` instance is assumed stranded and
	 * re-poked with a fresh `start` advance. Covers the window between
	 * `create()`'s commit and steve's job insert, which happens on its own
	 * connection and can be lost to a crash. Default: 300. `0` disables the scan.
	 */
	stalePendingSec?: number;
}

/** What one scheduler tick poked. */
export interface SchedulerTickResult {
	/** Due `waiting` rows poked with a `timeout` advance. */
	woken: number;
	/** Stranded `pending` rows re-poked with a `start` advance. */
	repoked: number;
}

const DEFAULT_TICK_EXPRESSION = "* * * * *";
const DEFAULT_STALE_PENDING_SEC = 300;

/**
 * Cron-driven scheduler that wakes time-suspended workflow instances.
 *
 * Each tick is read-only against `__workflow_instances`: it selects due rows and
 * enqueues one `workflow.advance` poke per row, carrying the row's `seq` as the
 * fence. The advance re-checks the precondition under the row lock and is the
 * only thing that writes — so a crash anywhere in a tick costs nothing but a
 * repeated poke next tick, and the repeat is fenced out.
 *
 * Two scans per tick: due timers (`waiting` with an expired `wake_at`) and, at
 * `stalePendingSec`, instances stuck in `pending` because the job that should
 * have started them never made it into the queue.
 *
 * The scheduler does not own its `Cron`. The caller constructs a `Cron`, passes
 * it in, and runs its lifecycle (`start` / `stop`). Call `register()` once
 * after construction to attach the tick; `unregister()` to detach.
 */
export class WorkflowScheduler {
	readonly cron: Cron;
	readonly tenantId: string;
	readonly tickExpression: string;
	readonly tickName: string;
	readonly stalePendingSec: number;

	readonly #workflow: Workflow;
	readonly #tickBatchSize: number;
	readonly #timezone: string | null | undefined;

	constructor(options: WorkflowSchedulerOptions) {
		this.cron = options.cron;
		this.#workflow = options.workflow;
		this.tenantId = options.workflow.tenantId;
		this.tickExpression = options.tickExpression ?? DEFAULT_TICK_EXPRESSION;
		this.tickName = options.tickName ?? `workflow.scheduler.${this.tenantId}`;
		this.#tickBatchSize = options.tickBatchSize ?? 100;
		this.#timezone = options.timezone;
		this.stalePendingSec = options.stalePendingSec ?? DEFAULT_STALE_PENDING_SEC;
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
	 * Runs one tick immediately. Exposed for testing and on-demand wakes. The
	 * counts are pokes issued, not instances moved — the advance decides that.
	 */
	async tickOnce(): Promise<SchedulerTickResult> {
		return await this.#tick();
	}

	async #tick(): Promise<SchedulerTickResult> {
		const due = await selectDueWakeUps(
			this.#workflow.db,
			this.tenantId,
			this.#tickBatchSize,
		);
		for (const row of due) {
			await this.#poke(row, { kind: "timeout", outcome: TIMEOUT_EVENT });
		}
		if (due.length) clog.debug?.(`scheduler: poked ${due.length} due wake-ups`);

		let stale: InstancePoke[] = [];
		if (this.stalePendingSec > 0) {
			stale = await selectStalePending(
				this.#workflow.db,
				this.tenantId,
				this.stalePendingSec,
				this.#tickBatchSize,
			);
			for (const row of stale) {
				await this.#poke(row, { kind: "start" });
			}
			if (stale.length) {
				clog.debug?.(`scheduler: re-poked ${stale.length} pending instances`);
			}
		}

		return { woken: due.length, repoked: stale.length };
	}

	#poke(
		row: InstancePoke,
		what: Pick<AdvanceJobPayload, "kind" | "outcome">,
	): Promise<void> {
		return this.#workflow.enqueueAdvance(this.#workflow.db, {
			tenant_id: this.tenantId,
			instance_id: row.id,
			expected_seq: row.seq,
			...what,
		});
	}
}
