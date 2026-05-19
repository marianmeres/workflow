import type { Cron } from "@marianmeres/cron";
import { clog } from "./log.ts";
import { claimDueWakeUps } from "./persistence/instances.ts";
import type { Workflow } from "./workflow.ts";
import type { AdvanceJobPayload } from "./types.ts";

/** Options accepted by {@link WorkflowScheduler}. */
export interface WorkflowSchedulerOptions {
	/**
	 * Externally-owned `@marianmeres/cron` `Cron` instance. The scheduler
	 * registers its tick handler on this instance when `register()` is called.
	 * The consumer owns the lifecycle: `await cron.start(N)` / `await cron.stop()`.
	 */
	cron: Cron;
	/** The Workflow whose instances this scheduler wakes (used for `db`, `projectId`, `enqueueAdvance`). */
	workflow: Workflow;
	/** Cron expression for the tick. Default: every minute. */
	tickExpression?: string;
	/** IANA timezone for the cron expression. */
	timezone?: string | null;
	/** Maximum number of due rows claimed per tick. Default: 100. */
	tickBatchSize?: number;
	/** Custom name for the registered cron job. Default: `workflow.scheduler.<projectId>`. */
	tickName?: string;
}

const DEFAULT_TICK_EXPRESSION = "* * * * *";

/**
 * Cron-driven scheduler that wakes time-suspended workflow instances.
 *
 * On each tick it atomically flips waiting+due rows to `pending` (clearing
 * their `wake_at`) and dispatches a `workflow.advance` job for each, carrying
 * `outcome: 'TIMEOUT'`.
 *
 * The scheduler does not own its `Cron`. The caller constructs a `Cron`, passes
 * it in, and runs its lifecycle (`start` / `stop`). Call `register()` once
 * after construction to attach the tick; `unregister()` to detach.
 */
export class WorkflowScheduler {
	readonly cron: Cron;
	readonly projectId: string;
	readonly tickExpression: string;
	readonly tickName: string;

	readonly #workflow: Workflow;
	readonly #tickBatchSize: number;
	readonly #timezone: string | null | undefined;

	constructor(options: WorkflowSchedulerOptions) {
		this.cron = options.cron;
		this.#workflow = options.workflow;
		this.projectId = options.workflow.projectId;
		this.tickExpression = options.tickExpression ?? DEFAULT_TICK_EXPRESSION;
		this.tickName = options.tickName ?? `workflow.scheduler.${this.projectId}`;
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

	/** Runs one tick immediately. Exposed for testing and on-demand wakes. */
	async tickOnce(): Promise<number> {
		return await this.#tick();
	}

	async #tick(): Promise<number> {
		const claimed = await claimDueWakeUps(
			this.#workflow.db,
			this.projectId,
			this.#tickBatchSize,
		);
		if (claimed.length === 0) return 0;

		clog.debug?.(`scheduler: claimed ${claimed.length} due wake-ups`);

		for (const row of claimed) {
			const payload: AdvanceJobPayload = {
				project_id: this.projectId,
				instance_id: row.id,
				timeout: true,
				outcome: "TIMEOUT",
			};
			await this.#workflow.enqueueAdvance(this.#workflow.db, payload);
		}

		return claimed.length;
	}
}
