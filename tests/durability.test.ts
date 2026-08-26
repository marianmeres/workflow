import { Cron } from "@marianmeres/cron";
import { Jobs } from "@marianmeres/steve";
import { assert, assertEquals } from "@std/assert";
import type pg from "pg";
import {
	type AdvanceJobPayload,
	createMigrate,
	type EffectJobPayload,
	EXECUTION_STATE,
	getHistory,
	JOB_TYPE_ADVANCE,
	Workflow,
	type WorkflowDefinition,
	type WorkflowInstanceRow,
	WorkflowRegistry,
	WorkflowScheduler,
} from "../src/mod.ts";
import { failEffectJob, type JobEnqueuer, runAdvance } from "../src/driver.ts";
import { createPg, pgConfigured, resetSchema } from "./_pg.ts";
import { makeHandlers, stockReplenishmentV1 } from "./fixtures/stock-replenishment.ts";

const PG = pgConfigured();

async function waitUntil<T>(
	predicate: () => Promise<T | null | undefined | false>,
	{ timeoutMs = 10_000, intervalMs = 50 } = {},
): Promise<T> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		const v = await predicate();
		if (v) return v as T;
		await new Promise((r) => setTimeout(r, intervalMs));
	}
	throw new Error(`waitUntil: timed out after ${timeoutMs}ms`);
}

/** Records what the driver would enqueue instead of touching steve. */
function recordingEnqueuer() {
	const advances: AdvanceJobPayload[] = [];
	const effects: EffectJobPayload[] = [];
	const enqueuer: JobEnqueuer = {
		enqueueAdvance: (_c, payload) => {
			advances.push(payload);
			return Promise.resolve();
		},
		enqueueEffect: (_c, _handler, payload) => {
			effects.push(payload);
			return Promise.resolve();
		},
	};
	return { advances, effects, enqueuer };
}

const registry = () =>
	new WorkflowRegistry({ definitions: [stockReplenishmentV1], ...makeHandlers() });

/** Inserts a fresh `pending` instance at the definition's initial node. */
async function seedInstance(
	pool: pg.Pool,
	tenantId: string,
): Promise<WorkflowInstanceRow> {
	const { rows } = await pool.query<WorkflowInstanceRow>(
		`INSERT INTO __workflow_instances
			(tenant_id, definition_id, definition_version, cursor, context, execution_state)
		 VALUES ($1, 'stock_replenishment', '1.0.0', 'detect_low_stock', '{}'::jsonb, 'pending')
		 RETURNING id, tenant_id, cursor, execution_state, seq`,
		[tenantId],
	);
	return rows[0];
}

const reread = async (pool: pg.Pool, id: string) => {
	const { rows } = await pool.query<WorkflowInstanceRow>(
		`SELECT cursor, execution_state, seq FROM __workflow_instances WHERE id = $1`,
		[id],
	);
	return rows[0];
};

Deno.test({
	name: "fence: a replayed initial advance dispatches its effect only once",
	ignore: !PG,
	async fn() {
		const pool = createPg();
		await resetSchema(pool);
		await createMigrate(pool).up("latest");

		const jobs = new Jobs({ db: pool, pollTimeoutMs: 50 });
		const { handlers, matchers } = makeHandlers();
		const workflow = new Workflow({
			db: pool,
			jobs,
			tenantId: "fence-start",
			definitions: [stockReplenishmentV1],
			handlers,
			matchers,
		});

		try {
			const inst = await workflow.create({
				definitionId: "stock_replenishment",
				definitionVersion: "1.0.0",
				correlationToken: crypto.randomUUID(),
			});

			// The duplicate steve delivers when a completion write fails: the very
			// same payload, queued twice.
			await workflow.enqueueAdvance(pool, {
				tenant_id: "fence-start",
				instance_id: inst.id,
				kind: "start",
				expected_seq: inst.seq,
			});

			await jobs.start(2);

			await waitUntil(async () => {
				const row = await workflow.find(inst.id);
				return row?.execution_state === EXECUTION_STATE.WAITING ? row : null;
			});

			const { rows } = await pool.query<{ n: string }>(
				`SELECT count(*) AS n FROM __job WHERE type = 'workflow.effect.checkInventory'`,
			);
			assertEquals(rows[0].n, "1");
		} finally {
			await jobs.stop();
			await pool.end();
		}
	},
});

Deno.test({
	name: "fence: a duplicate effect-completion advance is a no-op, not a failure",
	ignore: !PG,
	async fn() {
		const pool = createPg();
		await resetSchema(pool);
		await createMigrate(pool).up("latest");

		try {
			const inst = await seedInstance(pool, "fence-effect");
			const { advances, effects, enqueuer } = recordingEnqueuer();
			const reg = registry();

			await runAdvance(pool, reg, enqueuer, {
				tenant_id: "fence-effect",
				instance_id: inst.id,
				kind: "start",
				expected_seq: inst.seq,
			});
			assertEquals(effects.length, 1);
			assertEquals(effects[0].handler, "checkInventory");

			// checkInventory completed; steve re-ran it, so the same completion
			// advance arrives twice.
			const completion: AdvanceJobPayload = {
				tenant_id: "fence-effect",
				instance_id: inst.id,
				kind: "effect",
				expected_seq: effects[0].seq,
				outcome: "LOW",
				outcome_data: { stock: 3 },
				handler: "checkInventory",
			};
			await runAdvance(pool, reg, enqueuer, completion);
			await runAdvance(pool, reg, enqueuer, completion);

			const row = await reread(pool, inst.id);
			assertEquals(row.execution_state, EXECUTION_STATE.RUNNING);
			assertEquals(row.cursor, "send_order");
			// One dispatch per node — the second completion moved nothing.
			assertEquals(effects.map((e) => e.handler), [
				"checkInventory",
				"sendOrderEmail",
			]);
			assertEquals(advances.length, 0);

			const events = (await getHistory(pool, inst.id)).map((h) => h.event_type);
			assert(
				!events.includes("transition_rejected"),
				`unexpected rejection: ${events.join(", ")}`,
			);
			assertEquals(events.filter((e) => e === "effect_completed").length, 1);
		} finally {
			await pool.end();
		}
	},
});

Deno.test({
	name: "fence: a stale effect job's failure does not kill a moved-on instance",
	ignore: !PG,
	async fn() {
		const pool = createPg();
		await resetSchema(pool);
		await createMigrate(pool).up("latest");

		try {
			const inst = await seedInstance(pool, "fence-fail");
			const { effects, enqueuer } = recordingEnqueuer();
			const reg = registry();

			await runAdvance(pool, reg, enqueuer, {
				tenant_id: "fence-fail",
				instance_id: inst.id,
				kind: "start",
				expected_seq: inst.seq,
			});
			const staleEffect = effects[0];

			await runAdvance(pool, reg, enqueuer, {
				tenant_id: "fence-fail",
				instance_id: inst.id,
				kind: "effect",
				expected_seq: staleEffect.seq,
				outcome: "LOW",
				handler: "checkInventory",
			});
			const moved = await reread(pool, inst.id);

			// The reaper marks the long-abandoned checkInventory job expired.
			await failEffectJob(pool, staleEffect, "steve: expired");

			const after = await reread(pool, inst.id);
			assertEquals(after.execution_state, moved.execution_state);
			assertEquals(after.cursor, moved.cursor);
			assertEquals(after.seq, moved.seq);
			assert(
				!(await getHistory(pool, inst.id)).some((h) =>
					h.event_type === "effect_failed"
				),
			);

			// Same call, current fence: now it must fail the instance.
			await failEffectJob(pool, effects[1], "steve: expired");
			assertEquals(
				(await reread(pool, inst.id)).execution_state,
				EXECUTION_STATE.FAILED,
			);
		} finally {
			await pool.end();
		}
	},
});

/** A Workflow wired to a real (but not yet started) Jobs, plus its scheduler. */
function setupScheduler(
	pool: pg.Pool,
	tenantId: string,
	stalePendingSec?: number,
) {
	const jobs = new Jobs({ db: pool, pollTimeoutMs: 50 });
	const { handlers, matchers } = makeHandlers();
	const workflow = new Workflow({
		db: pool,
		jobs,
		tenantId,
		definitions: [stockReplenishmentV1],
		handlers,
		matchers,
	});
	const cron = new Cron({ db: pool, pollTimeoutMs: 50 });
	const scheduler = new WorkflowScheduler({ cron, workflow, stalePendingSec });
	return { cron, jobs, workflow, scheduler };
}

Deno.test({
	name: "poke: two scheduler ticks over one due row apply TIMEOUT once",
	ignore: !PG,
	async fn() {
		const pool = createPg();
		await resetSchema(pool);
		await createMigrate(pool).up("latest");

		const { jobs, workflow, scheduler } = setupScheduler(pool, "poke-timeout");

		try {
			// Drive the instance to `await_reply` with the worker still stopped, so
			// both ticks below see the very same, un-advanced row.
			const inst = await seedInstance(pool, "poke-timeout");
			const { effects, enqueuer } = recordingEnqueuer();
			const reg = registry();
			const base = { tenant_id: "poke-timeout", instance_id: inst.id };
			await runAdvance(pool, reg, enqueuer, {
				...base,
				kind: "start",
				expected_seq: inst.seq,
			});
			await runAdvance(pool, reg, enqueuer, {
				...base,
				kind: "effect",
				expected_seq: effects[0].seq,
				outcome: "LOW",
				handler: "checkInventory",
			});
			await runAdvance(pool, reg, enqueuer, {
				...base,
				kind: "effect",
				expected_seq: effects[1].seq,
				outcome: "SENT",
				handler: "sendOrderEmail",
			});
			const waiting = await reread(pool, inst.id);
			assertEquals(waiting.execution_state, EXECUTION_STATE.WAITING);
			assertEquals(waiting.cursor, "await_reply");

			await pool.query(
				`UPDATE __workflow_instances SET wake_at = now() - interval '1 minute' WHERE id = $1`,
				[inst.id],
			);

			// The tick no longer claims the row, so it is still due on the second
			// pass and gets poked again — with the same, now duplicated, fence.
			assertEquals(await scheduler.tickOnce(), { woken: 1, repoked: 0 });
			assertEquals(await scheduler.tickOnce(), { woken: 1, repoked: 0 });
			const { rows: queued } = await pool.query<{ n: string }>(
				`SELECT count(*) AS n FROM __job WHERE type = $1`,
				[JOB_TYPE_ADVANCE],
			);
			assertEquals(queued[0].n, "2");

			await jobs.start(2);
			const done = await waitUntil(async () => {
				const row = await workflow.find(inst.id);
				return row?.execution_state === EXECUTION_STATE.COMPLETED ? row : null;
			});
			assertEquals(done.cursor, "_end_timeout");

			const events = (await getHistory(pool, inst.id)).map((h) => h.event_type);
			assertEquals(events.filter((e) => e === "timeout").length, 1);
			assert(
				!events.includes("transition_rejected"),
				`unexpected rejection: ${events.join(", ")}`,
			);
		} finally {
			await jobs.stop();
			await pool.end();
		}
	},
});

Deno.test({
	name: "re-poke: a stale pending row with no job is advanced by the next tick",
	ignore: !PG,
	async fn() {
		const pool = createPg();
		await resetSchema(pool);
		await createMigrate(pool).up("latest");

		const { cron, jobs, workflow, scheduler } = setupScheduler(pool, "repoke", 60);
		const disabled = new WorkflowScheduler({
			cron,
			workflow,
			stalePendingSec: 0,
		});

		try {
			// What a crash between create()'s commit and steve's job insert leaves
			// behind: a pending row nothing will ever pick up.
			const inst = await seedInstance(pool, "repoke");

			// Too fresh to be stranded — it may still be someone's in-flight job.
			assertEquals(await scheduler.tickOnce(), { woken: 0, repoked: 0 });

			await pool.query(
				`UPDATE __workflow_instances SET updated_at = now() - interval '10 minutes' WHERE id = $1`,
				[inst.id],
			);
			assertEquals(await disabled.tickOnce(), { woken: 0, repoked: 0 });
			assertEquals(await scheduler.tickOnce(), { woken: 0, repoked: 1 });

			await jobs.start(2);
			const row = await waitUntil(async () => {
				const r = await workflow.find(inst.id);
				return r?.execution_state === EXECUTION_STATE.WAITING ? r : null;
			});
			assertEquals(row.cursor, "await_reply");
		} finally {
			await jobs.stop();
			await pool.end();
		}
	},
});

/** Its first hop throws inside the transition action, on every attempt. */
const throwingActionV1: WorkflowDefinition = {
	id: "throwing_action",
	version: "1.0.0",
	fsm: {
		initial: "route",
		context: () => ({}),
		states: {
			route: {
				meta: { kind: "pure" },
				on: {
					ENTER: [{
						target: "_end_ok",
						action: () => {
							throw new Error("action exploded");
						},
					}],
				},
			},
			_end_ok: { meta: { kind: "terminal" }, on: {} },
		},
	},
};

Deno.test({
	name: "advance failure: a throwing action fails the instance with its message",
	ignore: !PG,
	async fn() {
		const pool = createPg();
		await resetSchema(pool);
		await createMigrate(pool).up("latest");

		const jobs = new Jobs({ db: pool, pollTimeoutMs: 50 });
		const workflow = new Workflow({
			db: pool,
			jobs,
			tenantId: "advance-fail",
			definitions: [throwingActionV1],
			handlers: {},
			advanceMaxAttempts: 1,
		});

		try {
			const inst = await workflow.create({
				definitionId: "throwing_action",
				definitionVersion: "1.0.0",
			});
			// Only now — an attempt racing create()'s commit would fail on the
			// not-yet-visible row instead of on the action, and one attempt is all
			// this job gets.
			await jobs.start(1);

			await waitUntil(async () => {
				const r = await workflow.find(inst.id);
				return r?.execution_state === EXECUTION_STATE.FAILED ? r : null;
			});

			const failed = (await getHistory(pool, inst.id)).find(
				(h) => h.event_type === "failed",
			);
			assert(failed, "expected a `failed` history row");
			assert(
				String(failed.data.reason).includes("action exploded"),
				`reason does not carry the error: ${failed.data.reason}`,
			);
			assert(failed.data.job_uid, "expected the failing job's uid on the row");
		} finally {
			await jobs.stop();
			await pool.end();
		}
	},
});

/** A Workflow whose one effect (`checkInventory` → OK) completes the instance. */
function setupOneEffect(
	pool: pg.Pool,
	tenantId: string,
	redispatchLimit?: number,
) {
	const jobs = new Jobs({ db: pool, pollTimeoutMs: 50 });
	const { handlers, matchers } = makeHandlers({ inventory: "OK" });
	const workflow = new Workflow({
		db: pool,
		jobs,
		tenantId,
		definitions: [stockReplenishmentV1],
		handlers,
		matchers,
		redispatchLimit,
	});
	const reg = new WorkflowRegistry({
		definitions: [stockReplenishmentV1],
		handlers,
		matchers,
	});
	return { jobs, workflow, reg };
}

/**
 * What a worker dying mid-job leaves behind: a job stuck in `running` that
 * steve's reaper flips to `expired` — terminal, never retried by steve itself.
 */
async function crashRunningJob(
	pool: pg.Pool,
	jobs: Jobs,
	type: string,
): Promise<number> {
	const r = await pool.query(
		`UPDATE __job SET status = 'running', started_at = now() - interval '1 minute'
		  WHERE type = $1 AND status = 'pending'`,
		[type],
	);
	assertEquals(r.rowCount, 1, `expected exactly one pending ${type} job`);
	return await jobs.cleanup(0);
}

const countJobs = async (pool: pg.Pool, type: string) => {
	const { rows } = await pool.query<{ n: string }>(
		`SELECT count(*) AS n FROM __job WHERE type = $1`,
		[type],
	);
	return Number(rows[0].n);
};

const EFFECT_TYPE = "workflow.effect.checkInventory";

Deno.test({
	name: "expired: a crashed effect job is re-dispatched and the workflow completes",
	ignore: !PG,
	async fn() {
		const pool = createPg();
		await resetSchema(pool);
		await createMigrate(pool).up("latest");

		const { jobs, workflow, reg } = setupOneEffect(pool, "expire-redispatch");

		try {
			// Dispatch the effect with no worker running, so the job below is
			// guaranteed to be the one we crash.
			const inst = await seedInstance(pool, "expire-redispatch");
			await runAdvance(pool, reg, workflow, {
				tenant_id: "expire-redispatch",
				instance_id: inst.id,
				kind: "start",
				expected_seq: inst.seq,
			});
			assertEquals(await countJobs(pool, EFFECT_TYPE), 1);

			assertEquals(await crashRunningJob(pool, jobs, EFFECT_TYPE), 1);
			await waitUntil(() => countJobs(pool, EFFECT_TYPE).then((n) => n === 2));

			const { rows } = await pool.query<{ redispatch: string }>(
				`SELECT payload->>'redispatch' AS redispatch FROM __job
				  WHERE type = $1 AND status = 'pending'`,
				[EFFECT_TYPE],
			);
			assertEquals(rows.map((r) => r.redispatch), ["1"]);

			await jobs.start(2);
			const done = await waitUntil(async () => {
				const r = await workflow.find(inst.id);
				return r?.execution_state === EXECUTION_STATE.COMPLETED ? r : null;
			});
			assertEquals(done.cursor, "_end_ok");

			const events = (await getHistory(pool, inst.id)).map((h) => h.event_type);
			assert(
				!events.includes("effect_failed"),
				`unexpected failure: ${events.join(", ")}`,
			);
		} finally {
			await jobs.stop();
			await pool.end();
		}
	},
});

Deno.test({
	name: "expired: the instance fails once the redispatch budget is spent",
	ignore: !PG,
	async fn() {
		const pool = createPg();
		await resetSchema(pool);
		await createMigrate(pool).up("latest");

		// Budget of 2 = the original dispatch plus one re-dispatch.
		const { jobs, workflow, reg } = setupOneEffect(pool, "expire-limit", 2);

		try {
			const inst = await seedInstance(pool, "expire-limit");
			await runAdvance(pool, reg, workflow, {
				tenant_id: "expire-limit",
				instance_id: inst.id,
				kind: "start",
				expected_seq: inst.seq,
			});

			assertEquals(await crashRunningJob(pool, jobs, EFFECT_TYPE), 1);
			await waitUntil(() => countJobs(pool, EFFECT_TYPE).then((n) => n === 2));

			assertEquals(await crashRunningJob(pool, jobs, EFFECT_TYPE), 1);
			const failed = await waitUntil(async () => {
				const r = await workflow.find(inst.id);
				return r?.execution_state === EXECUTION_STATE.FAILED ? r : null;
			});
			assertEquals(failed.cursor, "detect_low_stock");
			// Nothing re-queued past the budget.
			assertEquals(await countJobs(pool, EFFECT_TYPE), 2);

			const event = (await getHistory(pool, inst.id)).find(
				(h) => h.event_type === "effect_failed",
			);
			assert(event, "expected an `effect_failed` history row");
			assertEquals(event.data.handler, "checkInventory");
			assert(
				String(event.data.reason).includes("redispatch limit"),
				`reason: ${event.data.reason}`,
			);
		} finally {
			await jobs.stop();
			await pool.end();
		}
	},
});
