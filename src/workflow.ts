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
import {
	createInstance,
	findInstance,
	lockInstance,
	updateInstance,
} from "./persistence/instances.ts";
import { withTransaction } from "./persistence/tx.ts";
import { WorkflowRegistry } from "./registry.ts";
import {
	type AdvanceJobPayload,
	DEFAULT_TENANT_ID,
	type EffectJobPayload,
	EXECUTION_STATE,
	type Handler,
	HISTORY_EVENT,
	type InboxRow,
	type Matcher,
	type WorkflowContext,
	type WorkflowDefinition,
	type WorkflowInstanceRow,
} from "./types.ts";

/**
 * Steve's `setHandler` is last-writer-wins, so a second `Workflow` on the same
 * `Jobs` would silently route every `workflow.advance` through its own registry
 * and fail the first one's instances with "Unknown workflow definition". The
 * constructor refuses that; {@link Workflow.detach} releases the claim.
 */
const attachedWorkflows = new WeakMap<Jobs, Workflow>();

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

	readonly #jobTypes: string[] = [];
	readonly #unsubscribers: ReturnType<Jobs["onDone"]>[] = [];

	constructor(options: WorkflowOptions) {
		if (attachedWorkflows.has(options.jobs)) {
			throw new Error(
				"Workflow: a Workflow is already attached to this Jobs instance. " +
					"Call detach() on it first, or pass a separate Jobs instance.",
			);
		}
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
		this.#jobTypes.push(JOB_TYPE_ADVANCE);
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
			this.#jobTypes.push(effectJobType(name));
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
			const unsub = this.jobs.onDone(effectJobType(name), (job: Job) => {
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
			this.#unsubscribers.push(unsub);
		}

		const unsubAdvance = this.jobs.onDone(JOB_TYPE_ADVANCE, (job: Job) => {
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
		this.#unsubscribers.push(unsubAdvance);

		attachedWorkflows.set(this.jobs, this);
	}

	/**
	 * Removes everything the constructor registered on the `Jobs` instance — the
	 * `workflow.advance` and `workflow.effect.<name>` handlers plus the `onDone`
	 * subscriptions — and releases the one-Workflow-per-Jobs claim, so a
	 * replacement can be constructed on the same queue (tests, hot reload).
	 *
	 * Detaching does not stop the queue and does not touch running instances:
	 * jobs enqueued for this `Workflow` stay in the table and fall back to
	 * steve's noop handler until something re-attaches.
	 *
	 * A no-op when this instance is not the attached one (already detached, or
	 * superseded by a later `Workflow`).
	 */
	detach(): void {
		if (attachedWorkflows.get(this.jobs) !== this) return;
		for (const type of this.#jobTypes) this.jobs.setHandler(type, null);
		for (const unsub of this.#unsubscribers) unsub();
		attachedWorkflows.delete(this.jobs);
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

	/**
	 * Operator escape hatch: aborts a live instance. The row goes `cancelled`
	 * (terminal), loses its `wake_at` and its correlation token — so no timer, no
	 * signal and no queued job can move it again: the `seq` bump makes every job
	 * already in flight for it stale, and a stale effect job skips its handler
	 * instead of running the side effect.
	 *
	 * Returns `false` when the instance does not exist, belongs to another tenant,
	 * or is already terminal.
	 *
	 * @param reason - free-form, recorded on the `cancelled` history row.
	 */
	async cancel(id: string, reason?: string): Promise<boolean> {
		return await withTransaction(this.db, async (client) => {
			const row = await lockInstance(client, id);
			if (!row || row.tenant_id !== this.tenantId) return false;
			if (
				row.execution_state === EXECUTION_STATE.COMPLETED ||
				row.execution_state === EXECUTION_STATE.FAILED ||
				row.execution_state === EXECUTION_STATE.CANCELLED
			) {
				return false;
			}
			await updateInstance(client, row.id, {
				execution_state: EXECUTION_STATE.CANCELLED,
				wake_at: null,
				correlation_token: null,
			}, { bumpSeq: true });
			await appendHistory(client, {
				tenant_id: this.tenantId,
				instance_id: row.id,
				event_type: HISTORY_EVENT.CANCELLED,
				from_node: row.cursor,
				data: { reason: reason ?? null },
			});
			return true;
		});
	}

	/**
	 * Operator escape hatch: resumes a `failed` instance from its current cursor,
	 * rather than starting a new one and replaying every side effect from the top.
	 * The row goes back to `pending` and a fresh `start` advance re-runs whatever
	 * the cursor node calls for — typically re-dispatching its effect.
	 *
	 * Nothing is fixed by retrying alone: an instance failed by a rejected
	 * transition or a deterministically throwing handler will fail again unless
	 * the code changed.
	 *
	 * Returns `false` when the instance does not exist, belongs to another tenant,
	 * or is in a state this does not cover.
	 *
	 * @param opts.force - also allow a `running` instance, i.e. the operator
	 *   asserting that its effect job is dead (a worker crashed with no reaper to
	 *   expire the job). Safe by construction: the `seq` bump makes the zombie
	 *   stale, so a late completion is dropped.
	 */
	async retry(id: string, opts: { force?: boolean } = {}): Promise<boolean> {
		const settled = await withTransaction(this.db, async (client) => {
			const row = await lockInstance(client, id);
			if (!row || row.tenant_id !== this.tenantId) return null;
			const allowed = row.execution_state === EXECUTION_STATE.FAILED ||
				(opts.force === true &&
					row.execution_state === EXECUTION_STATE.RUNNING);
			if (!allowed) return null;
			const updated = await updateInstance(client, row.id, {
				execution_state: EXECUTION_STATE.PENDING,
				wake_at: null,
			}, { bumpSeq: true });
			await appendHistory(client, {
				tenant_id: this.tenantId,
				instance_id: row.id,
				event_type: HISTORY_EVENT.RETRIED,
				from_node: row.cursor,
				data: { from_state: row.execution_state },
			});
			return updated;
		});
		if (!settled) return false;
		// After the commit, not inside it: an advance claimed before the commit
		// landed would see the row still `failed` at the old `seq` and drop itself
		// as stale. A crash in this gap leaves a `pending` row, which the
		// scheduler's stale-pending scan re-pokes.
		await this.enqueueAdvance(this.db, {
			tenant_id: this.tenantId,
			instance_id: settled.id,
			kind: "start",
			expected_seq: settled.seq,
		});
		return true;
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
