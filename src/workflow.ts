import {
	type Job,
	JOB_STATUS,
	type Jobs,
} from "@marianmeres/steve";
import type pg from "pg";
import {
	effectJobType,
	failEffectJob,
	type JobEnqueuer,
	JOB_TYPE_ADVANCE,
	runAdvance,
	runEffect,
} from "./driver.ts";
import { clog } from "./log.ts";
import { appendHistory } from "./persistence/history.ts";
import { appendInbox } from "./persistence/inbox.ts";
import {
	createInstance,
	findInstance,
} from "./persistence/instances.ts";
import { WorkflowRegistry } from "./registry.ts";
import {
	type AdvanceJobPayload,
	DEFAULT_PROJECT_ID,
	type EffectJobPayload,
	type Handler,
	HISTORY_EVENT,
	type InboxRow,
	type Matcher,
	type WorkflowContext,
	type WorkflowDefinition,
	type WorkflowInstanceRow,
} from "./types.ts";

/** Options accepted by {@link Workflow}. */
export interface WorkflowOptions {
	/** PostgreSQL pool. Used for direct SQL (persistence helpers, transactions). */
	db: pg.Pool;
	/**
	 * Externally-owned `@marianmeres/steve` `Jobs` instance. The framework
	 * registers its handlers (`workflow.advance` + `workflow.effect.<name>`) on
	 * this instance via `setHandler` at construction time. The consumer owns
	 * the lifecycle: `await jobs.start(N)` / `await jobs.stop()`.
	 *
	 * Sharing this `Jobs` with the app's own job types is fine — they coexist
	 * by `type` on the same `__job` table.
	 */
	jobs: Jobs;
	/** Project scope (multi-tenancy). Defaults to `_default`. */
	projectId?: string;
	/** Workflow definitions to register. Each is keyed by `(id, version)`. */
	definitions: WorkflowDefinition[];
	/** Handlers keyed by name (referenced from `state.meta.handler`). */
	handlers: Record<string, Handler>;
	/** Matchers keyed by name (referenced from `state.meta.matcher`). */
	matchers?: Record<string, Matcher>;
	/** Per-effect-job retry count. Default: 3 (steve default). */
	effectMaxAttempts?: number;
	/** Per-effect-job timeout in ms. Default: 0 (no limit). */
	effectMaxAttemptDurationMs?: number;
}

/**
 * The user-facing API for creating workflow instances and appending inbox
 * signals.
 *
 * Attaches two kinds of handlers to the injected `Jobs` instance:
 * - `workflow.advance` — the durable driver step.
 * - `workflow.effect.<handlerName>` — one per registered effect handler.
 *
 * The consumer constructs and owns the `Jobs` lifecycle (`start` / `stop`).
 * Use {@link WorkflowScheduler} and {@link WorkflowInboxCorrelator} alongside
 * this class to wake suspended instances and correlate inbox signals; both
 * accept an externally-owned `Cron` instance the same way.
 */
export class Workflow implements JobEnqueuer {
	readonly db: pg.Pool;
	readonly jobs: Jobs;
	readonly projectId: string;
	readonly registry: WorkflowRegistry;

	readonly #effectMaxAttempts: number;
	readonly #effectMaxAttemptDurationMs: number;

	constructor(options: WorkflowOptions) {
		this.db = options.db;
		this.jobs = options.jobs;
		this.projectId = options.projectId ?? DEFAULT_PROJECT_ID;
		this.registry = new WorkflowRegistry({
			definitions: options.definitions,
			handlers: options.handlers,
			matchers: options.matchers,
		});
		this.#effectMaxAttempts = options.effectMaxAttempts ?? 3;
		this.#effectMaxAttemptDurationMs = options.effectMaxAttemptDurationMs ?? 0;

		// Register the advance handler.
		this.jobs.setHandler(
			JOB_TYPE_ADVANCE,
			async (job: Job, _signal?: AbortSignal) => {
				await runAdvance(
					this.db,
					this.registry,
					this,
					job.payload as AdvanceJobPayload,
				);
				return { ok: true };
			},
		);

		// Register one job-type handler per registered effect handler.
		const handlerNames = this.registry.handlerNames();
		for (const name of handlerNames) {
			this.jobs.setHandler(
				effectJobType(name),
				async (job: Job, signal?: AbortSignal) => {
					return await runEffect(
						this.db,
						this.registry,
						this,
						job.payload as EffectJobPayload,
						signal,
					);
				},
			);
		}

		// Watch effect-job terminal failures to mark the workflow instance failed.
		// onDone fires on completed / failed / expired — we react only to non-success.
		for (const name of handlerNames) {
			const type = effectJobType(name);
			this.jobs.onDone(type, (job: Job) => {
				if (
					job.status === JOB_STATUS.FAILED ||
					job.status === JOB_STATUS.EXPIRED
				) {
					const payload = job.payload as EffectJobPayload;
					failEffectJob(this.db, payload, `steve: ${job.status}`).catch(
						(e) => clog.error?.(`failEffectJob: ${e}`),
					);
				}
			});
		}
	}

	/** Internal — used by the driver to enqueue an advance job. */
	async enqueueAdvance(
		_client: pg.PoolClient | pg.Client | pg.Pool,
		payload: AdvanceJobPayload,
	): Promise<void> {
		// Steve creates within its own connection/tx; passing the locked client
		// would not gain us atomicity vs steve's bookkeeping. The advance is
		// idempotent (cursor-aware) so an enqueue-but-no-commit race is recoverable.
		await this.jobs.create(JOB_TYPE_ADVANCE, payload);
	}

	/** Internal — used by the driver to enqueue an effect job. */
	async enqueueEffect(
		_client: pg.PoolClient | pg.Client | pg.Pool,
		handler: string,
		payload: EffectJobPayload,
	): Promise<void> {
		await this.jobs.create(effectJobType(handler), payload, {
			max_attempts: this.#effectMaxAttempts,
			max_attempt_duration_ms: this.#effectMaxAttemptDurationMs,
		});
	}

	/**
	 * Creates a new workflow instance and enqueues its first advance.
	 *
	 * @param input.correlationToken - optional, set up-front for signal-suspending
	 *   nodes that need to be matchable from the moment the instance is born
	 *   (e.g., outbound emails using UUID subaddressing).
	 */
	async create(input: {
		definitionId: string;
		definitionVersion: string;
		context?: WorkflowContext;
		correlationToken?: string | null;
	}): Promise<WorkflowInstanceRow> {
		const def = this.registry.requireDefinition(
			input.definitionId,
			input.definitionVersion,
		);
		const row = await createInstance(this.db, {
			project_id: this.projectId,
			definition_id: input.definitionId,
			definition_version: input.definitionVersion,
			cursor: def.fsm.initial,
			context: input.context ?? {},
			correlation_token: input.correlationToken ?? null,
		});
		await appendHistory(this.db, {
			project_id: this.projectId,
			instance_id: row.id,
			event_type: HISTORY_EVENT.CREATED,
			to_node: row.cursor,
			data: { definitionId: input.definitionId, definitionVersion: input.definitionVersion },
		});
		await this.jobs.create(JOB_TYPE_ADVANCE, {
			project_id: this.projectId,
			instance_id: row.id,
		} as AdvanceJobPayload);
		return row;
	}

	/** Looks up an instance by id, scoped to this Workflow's project. */
	async find(id: string): Promise<WorkflowInstanceRow | null> {
		const row = await findInstance(this.db, id);
		if (row && row.project_id !== this.projectId) return null;
		return row;
	}

	/**
	 * Appends an external signal to the inbox. The {@link WorkflowInboxCorrelator}
	 * will pick it up on its next tick and dispatch an advance if a matching
	 * waiting instance is found.
	 */
	async appendInbox(input: {
		source: string;
		correlationToken: string;
		payload: Record<string, unknown>;
	}): Promise<InboxRow> {
		return await appendInbox(this.db, {
			project_id: this.projectId,
			source: input.source,
			correlation_token: input.correlationToken,
			payload: input.payload,
		});
	}
}
