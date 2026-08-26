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
	Workflow,
	WorkflowInboxCorrelator,
	type WorkflowInstanceRow,
	WorkflowRegistry,
	WorkflowScheduler,
} from "../src/mod.ts";
import { type JobEnqueuer, runAdvance, runEffect } from "../src/driver.ts";
import { createPg, pgConfigured, resetSchema } from "./_pg.ts";
import { waitUntil } from "./_util.ts";
import { makeHandlers, stockReplenishmentV1 } from "./fixtures/stock-replenishment.ts";

const PG = pgConfigured();

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
		`SELECT cursor, execution_state, wake_at, correlation_token, seq
		   FROM __workflow_instances WHERE id = $1`,
		[id],
	);
	return rows[0];
};

/** Workflow + registry over the reference definition. One attempt per effect job. */
function setup(
	pool: pg.Pool,
	tenantId: string,
	{ handlers, matchers }: ReturnType<typeof makeHandlers>,
) {
	const jobs = new Jobs({ db: pool, pollTimeoutMs: 50 });
	const workflow = new Workflow({
		db: pool,
		jobs,
		tenantId,
		definitions: [stockReplenishmentV1],
		handlers,
		matchers,
		effectMaxAttempts: 1,
	});
	const reg = new WorkflowRegistry({
		definitions: [stockReplenishmentV1],
		handlers,
		matchers,
	});
	return { jobs, workflow, reg };
}

Deno.test({
	name: "cancel: a waiting instance stops taking signals and timer wake-ups",
	ignore: !PG,
	async fn() {
		const pool = createPg();
		await resetSchema(pool);
		await createMigrate(pool).up("latest");

		const { jobs, workflow } = setup(pool, "cancel-waiting", makeHandlers());
		const cron = new Cron({ db: pool, pollTimeoutMs: 50 });
		const scheduler = new WorkflowScheduler({ cron, workflow });
		const correlator = new WorkflowInboxCorrelator({ cron, workflow });

		try {
			const token = crypto.randomUUID();
			const inst = await workflow.create({
				definitionId: "stock_replenishment",
				definitionVersion: "1.0.0",
				correlationToken: token,
			});
			await jobs.start(2);
			await waitUntil(async () => {
				const r = await workflow.find(inst.id);
				return r?.execution_state === EXECUTION_STATE.WAITING ? r : null;
			});
			// Stop the worker: from here on nothing but the explicit calls below
			// touches the instance.
			await jobs.stop();

			// Due, so a tick would wake it if cancelling did not take it out of scope.
			await pool.query(
				`UPDATE __workflow_instances SET wake_at = now() - interval '1 minute' WHERE id = $1`,
				[inst.id],
			);

			assertEquals(
				await workflow.cancel(inst.id, "operator changed their mind"),
				true,
			);
			const row = await reread(pool, inst.id);
			assertEquals(row.execution_state, EXECUTION_STATE.CANCELLED);
			assertEquals(row.cursor, "await_reply");
			assertEquals(row.wake_at, null);
			assertEquals(row.correlation_token, null);
			assert(row.seq > inst.seq, `seq not bumped (${row.seq} vs ${inst.seq})`);

			const cancelled = (await getHistory(pool, inst.id)).find(
				(h) => h.event_type === "cancelled",
			);
			assert(cancelled, "expected a `cancelled` history row");
			assertEquals(cancelled.data.reason, "operator changed their mind");

			// The reply the instance was waiting for arrives anyway.
			await workflow.appendInbox({
				source: "email",
				correlationToken: token,
				payload: { body: "yes" },
			});
			assertEquals(await correlator.tickOnce(), 1);
			const { rows: inbox } = await pool.query<{ processed_at: Date | null }>(
				`SELECT processed_at FROM __workflow_inbox WHERE correlation_token = $1`,
				[token],
			);
			assert(inbox[0].processed_at, "inbox row should be consumed, not deferred");

			assertEquals(await scheduler.tickOnce(), { woken: 0, repoked: 0 });

			const events = (await getHistory(pool, inst.id)).map((h) => h.event_type);
			assert(
				!events.includes("signal_received") && !events.includes("timeout"),
				`cancelled instance still moved: ${events.join(", ")}`,
			);

			// Terminal — a second cancel changes nothing.
			assertEquals(await workflow.cancel(inst.id), false);
		} finally {
			await jobs.stop();
			await pool.end();
		}
	},
});

Deno.test({
	name: "cancel: a queued effect job skips its handler instead of running it",
	ignore: !PG,
	async fn() {
		const pool = createPg();
		await resetSchema(pool);
		await createMigrate(pool).up("latest");

		let calls = 0;
		const fixture = makeHandlers();
		fixture.handlers.checkInventory = () => {
			calls++;
			return { outcome: "LOW", data: {} };
		};
		const { jobs, workflow, reg } = setup(pool, "cancel-effect", fixture);

		try {
			// Dispatch the effect with the worker stopped, so the job is still queued
			// when the cancel lands.
			const inst = await seedInstance(pool, "cancel-effect");
			const { advances, effects, enqueuer } = recordingEnqueuer();
			await runAdvance(pool, reg, enqueuer, {
				tenant_id: "cancel-effect",
				instance_id: inst.id,
				kind: "start",
				expected_seq: inst.seq,
			});
			assertEquals(effects.length, 1);

			assertEquals(await workflow.cancel(inst.id), true);

			assertEquals(await runEffect(pool, reg, enqueuer, effects[0]), {
				skipped: "stale",
			});
			assertEquals(calls, 0);
			assertEquals(advances.length, 0);
			assertEquals(
				(await reread(pool, inst.id)).execution_state,
				EXECUTION_STATE.CANCELLED,
			);
		} finally {
			await jobs.stop();
			await pool.end();
		}
	},
});

Deno.test({
	name: "retry: a failed instance resumes from its cursor and completes",
	ignore: !PG,
	async fn() {
		const pool = createPg();
		await resetSchema(pool);
		await createMigrate(pool).up("latest");

		let broken = true;
		const fixture = makeHandlers();
		fixture.handlers.checkInventory = () => {
			if (broken) throw new Error("inventory service down");
			return { outcome: "OK", data: { stock: 42 } };
		};
		const { jobs, workflow } = setup(pool, "retry-failed", fixture);

		try {
			const inst = await workflow.create({
				definitionId: "stock_replenishment",
				definitionVersion: "1.0.0",
			});
			await jobs.start(2);
			const failed = await waitUntil(async () => {
				const r = await workflow.find(inst.id);
				return r?.execution_state === EXECUTION_STATE.FAILED ? r : null;
			});
			assertEquals(failed.cursor, "detect_low_stock");

			// The outage is over.
			broken = false;
			assertEquals(await workflow.retry(inst.id), true);

			const done = await waitUntil(async () => {
				const r = await workflow.find(inst.id);
				return r?.execution_state === EXECUTION_STATE.COMPLETED ? r : null;
			});
			assertEquals(done.cursor, "_end_ok");

			const retried = (await getHistory(pool, inst.id)).find(
				(h) => h.event_type === "retried",
			);
			assert(retried, "expected a `retried` history row");
			assertEquals(retried.data.from_state, EXECUTION_STATE.FAILED);
			assertEquals(retried.from_node, "detect_low_stock");

			// Terminal again — and this time there is nothing to retry.
			assertEquals(await workflow.retry(inst.id), false);
		} finally {
			await jobs.stop();
			await pool.end();
		}
	},
});

Deno.test({
	name: "retry: refuses states it does not own; `force` covers running",
	ignore: !PG,
	async fn() {
		const pool = createPg();
		await resetSchema(pool);
		await createMigrate(pool).up("latest");

		const { jobs, workflow, reg } = setup(pool, "retry-guard", makeHandlers());
		const other = setup(pool, "someone-else", makeHandlers());

		try {
			assertEquals(await workflow.retry(crypto.randomUUID()), false);
			assertEquals(await workflow.cancel(crypto.randomUUID()), false);

			const inst = await seedInstance(pool, "retry-guard");
			// `pending` is not a failure — the start advance is either queued or
			// about to be re-poked.
			assertEquals(await workflow.retry(inst.id), false);
			assertEquals(await workflow.retry(inst.id, { force: true }), false);

			const { effects, enqueuer } = recordingEnqueuer();
			await runAdvance(pool, reg, enqueuer, {
				tenant_id: "retry-guard",
				instance_id: inst.id,
				kind: "start",
				expected_seq: inst.seq,
			});
			const running = await reread(pool, inst.id);
			assertEquals(running.execution_state, EXECUTION_STATE.RUNNING);

			// Another tenant's Workflow cannot reach it.
			assertEquals(await other.workflow.retry(inst.id, { force: true }), false);
			assertEquals(await other.workflow.cancel(inst.id), false);

			// A live effect job is presumed alive unless the operator says otherwise.
			assertEquals(await workflow.retry(inst.id), false);
			assertEquals(await workflow.retry(inst.id, { force: true }), true);

			const retried = await reread(pool, inst.id);
			assertEquals(retried.execution_state, EXECUTION_STATE.PENDING);
			assertEquals(retried.cursor, "detect_low_stock");
			assert(retried.seq > running.seq, "seq not bumped");

			const row = (await getHistory(pool, inst.id)).find(
				(h) => h.event_type === "retried",
			);
			assertEquals(row?.data.from_state, EXECUTION_STATE.RUNNING);

			// ...which is exactly what makes the orphaned effect job harmless.
			assertEquals(await runEffect(pool, reg, enqueuer, effects[0]), {
				skipped: "stale",
			});
		} finally {
			await jobs.stop();
			await other.jobs.stop();
			await pool.end();
		}
	},
});
