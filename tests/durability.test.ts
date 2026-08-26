import { Jobs } from "@marianmeres/steve";
import { assert, assertEquals } from "@std/assert";
import type pg from "pg";
import {
	createMigrate,
	EXECUTION_STATE,
	getHistory,
	Workflow,
	WorkflowRegistry,
	type AdvanceJobPayload,
	type EffectJobPayload,
	type WorkflowInstanceRow,
} from "../src/mod.ts";
import { failEffectJob, type JobEnqueuer, runAdvance } from "../src/driver.ts";
import { createPg, pgConfigured, resetSchema } from "./_pg.ts";
import {
	makeHandlers,
	stockReplenishmentV1,
} from "./fixtures/stock-replenishment.ts";

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
