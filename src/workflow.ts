import { type Job, JOB_STATUS, type Jobs } from "@marianmeres/steve";
import type pg from "pg";
import {
	effectJobType,
	failAdvanceJob,
	failEffectJob,
	JOB_TYPE_ADVANCE,
	type JobEnqueuer,
	runAdvance,
	runEffect,
} from "./driver.ts";
import { clog } from "./log.ts";
import { appendHistory } from "./persistence/history.ts";
import { appendInbox } from "./persistence/inbox.ts";
import { createInstance, findInstance } from "./persistence/instances.ts";
import { withTransaction } from "./persistence/tx.ts";
import { WorkflowRegistry } from "./registry.ts";
import {
	type AdvanceJobPayload,
	DEFAULT_TENANT_ID,
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
	/** Tenant scope (multi-tenancy). Defaults to `_default`. */
	tenantId?: string;
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
	/**
	 * Per-advance-job retry count. Default: 10 — an advance only fails on a
	 * throwing guard/action or a database error, and steve's exponential backoff
	 * over 10 attempts spans ~17 minutes, so a brief outage does not cost an
	 * instance. When they are exhausted the instance is marked `failed` with the
	 * last attempt's error message.
	 */
	advanceMaxAttempts?: number;
	/**
	 * How many times a single job payload may be dispatched, counting the
	 * original. Default: 3.
	 *
	 * An `expired` job is one whose worker died mid-run; steve never retries
	 * those, so the framework re-queues the identical payload instead. The fence
	 * (`seq`) makes the re-dispatch a no-op if the dead worker's transaction did
	 * commit, and handlers are required to be idempotent, so a crash becomes a
	 * recovery. Once the budget is spent the instance is marked `failed`.
	 *
	 * Note that expiry only happens at all when the `Jobs` instance runs with
	 * `autoCleanup` (or something calls `jobs.cleanup()` periodically).
	 */
	redispatchLimit?: number;
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
	readonly tenantId: string;
	readonly registry: WorkflowRegistry;

	readonly #effectMaxAttempts: number;
	readonly #effectMaxAttemptDurationMs: number;
	readonly #advanceMaxAttempts: number;
	readonly #redispatchLimit: number;

	constructor(options: WorkflowOptions) {
		this.db = options.db;
		this.jobs = options.jobs;
		this.tenantId = options.tenantId ?? DEFAULT_TENANT_ID;
		this.registry = new WorkflowRegistry({
			definitions: options.definitions,
			handlers: options.handlers,
			matchers: options.matchers,
		});
		this.#effectMaxAttempts = options.effectMaxAttempts ?? 3;
		this.#effectMaxAttemptDurationMs = options.effectMaxAttemptDurationMs ?? 0;
		this.#advanceMaxAttempts = options.advanceMaxAttempts ?? 10;
		this.#redispatchLimit = options.redispatchLimit ?? 3;

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

		// Watch job outcomes. onDone fires on completed / failed / expired — we
		// react only to non-success. `failed` means steve exhausted the attempts,
		// `expired` means the worker died mid-run and steve will not retry it, so
		// the two get different treatment: give up vs. re-dispatch.
		for (const name of handlerNames) {
			this.jobs.onDone(effectJobType(name), (job: Job) => {
				const payload = job.payload as EffectJobPayload;
				if (job.status === JOB_STATUS.EXPIRED) {
					this.#redispatchEffect(job, payload).catch(
						(e) => clog.error?.(`redispatchEffect: ${e}`),
					);
				} else if (job.status === JOB_STATUS.FAILED) {
					failEffectJob(this.db, payload, `steve: ${job.status}`).catch(
						(e) => clog.error?.(`failEffectJob: ${e}`),
					);
				}
			});
		}

		this.jobs.onDone(JOB_TYPE_ADVANCE, (job: Job) => {
			const payload = job.payload as AdvanceJobPayload;
			if (job.status === JOB_STATUS.EXPIRED) {
				this.#redispatchAdvance(job, payload).catch(
					(e) => clog.error?.(`redispatchAdvance: ${e}`),
				);
			} else if (job.status === JOB_STATUS.FAILED) {
				this.#failAdvance(job, payload).catch(
					(e) => clog.error?.(`failAdvanceJob: ${e}`),
				);
			}
		});
	}

	/** `redispatch` value for the next dispatch, or `null` if the budget is spent. */
	#nextRedispatch(current: number | undefined): number | null {
		const next = (current ?? 0) + 1;
		return next < this.#redispatchLimit ? next : null;
	}

	async #redispatchEffect(job: Job, payload: EffectJobPayload): Promise<void> {
		const next = this.#nextRedispatch(payload.redispatch);
		if (next === null) {
			await failEffectJob(
				this.db,
				payload,
				`steve: expired (redispatch limit ${this.#redispatchLimit} reached)`,
			);
			return;
		}
		clog.debug?.(
			`effect ${payload.handler} expired; re-dispatch #${next} (job ${job.uid})`,
		);
		await this.enqueueEffect(this.db, payload.handler, {
			...payload,
			redispatch: next,
		});
	}

	async #redispatchAdvance(job: Job, payload: AdvanceJobPayload): Promise<void> {
		const next = this.#nextRedispatch(payload.redispatch);
		if (next === null) {
			await failAdvanceJob(
				this.db,
				payload,
				`advance job expired (redispatch limit ${this.#redispatchLimit} reached)`,
				job.uid,
			);
			return;
		}
		clog.debug?.(`advance expired; re-dispatch #${next} (job ${job.uid})`);
		await this.enqueueAdvance(this.db, { ...payload, redispatch: next });
	}

	async #failAdvance(job: Job, payload: AdvanceJobPayload): Promise<void> {
		const { attempts } = await this.jobs.find(job.uid, true);
		const last = attempts?.at(-1)?.error_message;
		await failAdvanceJob(
			this.db,
			payload,
			`advance job failed: ${last ?? `steve: ${job.status}`}`,
			job.uid,
		);
	}

	/** Internal — used by the driver to enqueue an advance job. */
	async enqueueAdvance(
		_client: pg.PoolClient | pg.Client | pg.Pool,
		payload: AdvanceJobPayload,
	): Promise<void> {
		// Steve creates within its own connection/tx; passing the locked client
		// would not gain us atomicity vs steve's bookkeeping. The advance is
		// fenced (`expected_seq`), so an enqueue-but-no-commit race costs at most
		// a duplicate job that no-ops.
		await this.jobs.create(JOB_TYPE_ADVANCE, payload, {
			max_attempts: this.#advanceMaxAttempts,
		});
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
	 * @param input.context - shallow-merged over the definition's own
	 *   `fsm.context` defaults (top-level keys replace, they do not deep-merge).
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
		// The driver always resumes via `FSM.fromSnapshot`, which takes the
		// persisted context verbatim — so the definition's `context` defaults are
		// only ever applied here. Same value/factory handling as fsm's own init.
		const defaults = typeof def.fsm.context === "function"
			? def.fsm.context()
			: structuredClone(def.fsm.context ?? {});
		// One transaction: a throwing enqueue rolls the instance back rather than
		// leaving a row nothing will ever advance. Steve inserts on its own
		// connection, so the job can be claimed before this commits — the advance
		// throws on the not-yet-visible row and steve retries it.
		return await withTransaction(this.db, async (client) => {
			const row = await createInstance(client, {
				tenant_id: this.tenantId,
				definition_id: input.definitionId,
				definition_version: input.definitionVersion,
				cursor: def.fsm.initial,
				context: { ...defaults, ...input.context },
				correlation_token: input.correlationToken ?? null,
			});
			await appendHistory(client, {
				tenant_id: this.tenantId,
				instance_id: row.id,
				event_type: HISTORY_EVENT.CREATED,
				to_node: row.cursor,
				data: {
					definitionId: input.definitionId,
					definitionVersion: input.definitionVersion,
				},
			});
			await this.jobs.create(JOB_TYPE_ADVANCE, {
				tenant_id: this.tenantId,
				instance_id: row.id,
				kind: "start",
				expected_seq: row.seq,
			} as AdvanceJobPayload, { max_attempts: this.#advanceMaxAttempts });
			return row;
		});
	}

	/** Looks up an instance by id, scoped to this Workflow's tenant. */
	async find(id: string): Promise<WorkflowInstanceRow | null> {
		const row = await findInstance(this.db, id);
		if (row && row.tenant_id !== this.tenantId) return null;
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
			tenant_id: this.tenantId,
			source: input.source,
			correlation_token: input.correlationToken,
			payload: input.payload,
		});
	}
}
