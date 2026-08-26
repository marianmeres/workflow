/**
 * Driver branches the reference-workflow tests never reach: the pure-node hop
 * loop and its guard, the FSM-rejection path, and an instance pinned to a
 * definition this process does not have.
 *
 * These drive `runAdvance` directly against a recording enqueuer — no steve, no
 * cron. The behaviors under test are decided inside the advance transaction, so
 * a queue would only add latency and flakiness.
 */
import { assert, assertEquals } from "@std/assert";
import type pg from "pg";
import {
	type AdvanceJobPayload,
	createMigrate,
	type EffectJobPayload,
	EXECUTION_STATE,
	getHistory,
	HISTORY_EVENT,
	type WorkflowContext,
	type WorkflowInstanceRow,
	WorkflowRegistry,
} from "../src/mod.ts";
import { type JobEnqueuer, runAdvance } from "../src/driver.ts";
import { createPg, pgConfigured, resetSchema } from "./_pg.ts";
import { makeRoutingHandlers, routingV1 } from "./fixtures/routing.ts";

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

const registry = () =>
	new WorkflowRegistry({
		definitions: [routingV1],
		handlers: makeRoutingHandlers(),
	});

/** Inserts an instance parked at `cursor` in the given execution state. */
async function seedInstance(
	pool: pg.Pool,
	tenantId: string,
	input: {
		cursor: string;
		context?: WorkflowContext;
		executionState?: string;
		definitionVersion?: string;
	},
): Promise<WorkflowInstanceRow> {
	const { rows } = await pool.query<WorkflowInstanceRow>(
		`INSERT INTO __workflow_instances
			(tenant_id, definition_id, definition_version, cursor, context, execution_state)
		 VALUES ($1, 'routing', $2, $3, $4::jsonb, $5)
		 RETURNING id, tenant_id, cursor, execution_state, seq`,
		[
			tenantId,
			input.definitionVersion ?? "1.0.0",
			input.cursor,
			JSON.stringify(input.context ?? {}),
			input.executionState ?? EXECUTION_STATE.PENDING,
		],
	);
	return rows[0];
}

const reread = async (pool: pg.Pool, id: string) => {
	const { rows } = await pool.query<WorkflowInstanceRow>(
		`SELECT cursor, previous_cursor, execution_state, seq
		   FROM __workflow_instances WHERE id = $1`,
		[id],
	);
	return rows[0];
};

/** Boots a clean schema and hands back the pool. */
async function freshPool(): Promise<pg.Pool> {
	const pool = createPg();
	await resetSchema(pool);
	await createMigrate(pool).up("latest");
	return pool;
}

Deno.test({
	name: "pure: ENTER routes by guard, left and right, in one advance each",
	ignore: !PG,
	async fn() {
		const pool = await freshPool();
		try {
			const reg = registry();

			for (
				const [route, gate, handler] of [
					["left", "do_left", "leftEffect"],
					["right", "do_right", "rightEffect"],
				]
			) {
				const inst = await seedInstance(pool, "pure-routing", {
					cursor: "classify",
					context: { route },
				});
				const { effects, enqueuer } = recordingEnqueuer();

				await runAdvance(pool, reg, enqueuer, {
					tenant_id: "pure-routing",
					instance_id: inst.id,
					kind: "start",
					expected_seq: inst.seq,
				});

				const row = await reread(pool, inst.id);
				assertEquals(row.cursor, gate, `route "${route}" landed wrong`);
				assertEquals(row.execution_state, EXECUTION_STATE.RUNNING);
				assertEquals(effects.map((e) => e.handler), [handler]);
			}
		} finally {
			await pool.end();
		}
	},
});

Deno.test({
	name: "pure: two hops then an effect dispatch, all within one advance",
	ignore: !PG,
	async fn() {
		const pool = await freshPool();
		try {
			const inst = await seedInstance(pool, "pure-hops", {
				cursor: "classify",
				context: { route: "left" },
			});
			const { advances, effects, enqueuer } = recordingEnqueuer();

			await runAdvance(pool, registry(), enqueuer, {
				tenant_id: "pure-hops",
				instance_id: inst.id,
				kind: "start",
				expected_seq: inst.seq,
			});

			// The whole pure chain is inlined: no advance job is queued to walk it.
			assertEquals(advances.length, 0);
			assertEquals(effects.length, 1);
			assertEquals(effects[0].handler, "leftEffect");
			assertEquals(effects[0].cursor, "do_left");

			const row = await reread(pool, inst.id);
			assertEquals(row.cursor, "do_left");
			assertEquals(row.previous_cursor, "left_gate");

			const history = await getHistory(pool, inst.id);
			assertEquals(
				history.map((h) => `${h.event_type}:${h.from_node}>${h.to_node ?? "_"}`),
				[
					"transition:classify>left_gate",
					"transition:left_gate>do_left",
					"effect_dispatched:do_left>_",
				],
			);
		} finally {
			await pool.end();
		}
	},
});

Deno.test({
	name: "pure: a self-cycling node is stopped by the hop guard, not looped forever",
	ignore: !PG,
	async fn() {
		const pool = await freshPool();
		try {
			const inst = await seedInstance(pool, "pure-cycle", { cursor: "spin" });
			const { effects, enqueuer } = recordingEnqueuer();

			await runAdvance(pool, registry(), enqueuer, {
				tenant_id: "pure-cycle",
				instance_id: inst.id,
				kind: "start",
				expected_seq: inst.seq,
			});

			assertEquals(effects.length, 0);
			const row = await reread(pool, inst.id);
			assertEquals(row.execution_state, EXECUTION_STATE.FAILED);

			const history = await getHistory(pool, inst.id);
			const failed = history.at(-1);
			assertEquals(failed?.event_type, HISTORY_EVENT.FAILED);
			const reason = String(failed?.data.reason);
			assert(
				/exceeded \d+ pure-state hops/.test(reason),
				`unexpected reason: ${reason}`,
			);
			// One `transition` per hop taken, and the guard bailed out.
			const hops = history.filter((h) => h.event_type === HISTORY_EVENT.TRANSITION);
			assert(hops.length > 1, `expected several hops, got ${hops.length}`);
		} finally {
			await pool.end();
		}
	},
});

Deno.test({
	name: "rejection: an outcome the node does not accept fails the instance",
	ignore: !PG,
	async fn() {
		const pool = await freshPool();
		try {
			const inst = await seedInstance(pool, "reject", {
				cursor: "do_left",
				executionState: EXECUTION_STATE.RUNNING,
			});
			const { effects, enqueuer } = recordingEnqueuer();

			// `do_left` accepts OK and nothing else.
			await runAdvance(pool, registry(), enqueuer, {
				tenant_id: "reject",
				instance_id: inst.id,
				kind: "effect",
				expected_seq: inst.seq,
				outcome: "NOPE",
				handler: "leftEffect",
			});

			assertEquals(effects.length, 0);
			const row = await reread(pool, inst.id);
			assertEquals(row.execution_state, EXECUTION_STATE.FAILED);
			assertEquals(row.cursor, "do_left");

			const events = (await getHistory(pool, inst.id)).map((h) => h.event_type);
			assertEquals(events, [
				HISTORY_EVENT.TRANSITION_REJECTED,
				HISTORY_EVENT.FAILED,
			]);
		} finally {
			await pool.end();
		}
	},
});

Deno.test({
	name: "unknown definition: an unregistered version fails the instance",
	ignore: !PG,
	async fn() {
		const pool = await freshPool();
		try {
			const inst = await seedInstance(pool, "unknown-def", {
				cursor: "classify",
				definitionVersion: "9.9.9",
			});
			const { effects, enqueuer } = recordingEnqueuer();

			await runAdvance(pool, registry(), enqueuer, {
				tenant_id: "unknown-def",
				instance_id: inst.id,
				kind: "start",
				expected_seq: inst.seq,
			});

			assertEquals(effects.length, 0);
			const row = await reread(pool, inst.id);
			assertEquals(row.execution_state, EXECUTION_STATE.FAILED);

			const history = await getHistory(pool, inst.id);
			assertEquals(history.length, 1);
			assertEquals(history[0].event_type, HISTORY_EVENT.FAILED);
			assert(
				String(history[0].data.reason).includes("routing@9.9.9"),
				`unexpected reason: ${history[0].data.reason}`,
			);
		} finally {
			await pool.end();
		}
	},
});
