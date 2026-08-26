import { Cron } from "@marianmeres/cron";
import { Jobs } from "@marianmeres/steve";
import { assertEquals, assertNotEquals } from "@std/assert";
import type pg from "pg";
import {
	createMigrate,
	EXECUTION_STATE,
	getHistory,
	Workflow,
	WorkflowInboxCorrelator,
	WorkflowScheduler,
} from "../src/mod.ts";
import { createPg, pgConfigured, resetSchema } from "./_pg.ts";
import { makeHandlers, stockReplenishmentV1 } from "./fixtures/stock-replenishment.ts";

const PG = pgConfigured();

/** Polls `predicate` every `intervalMs` until truthy or `timeoutMs` elapses. */
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

/** Sets up a fresh Workflow + Jobs + Cron triple ready for a test. */
function setupRuntime(input: {
	pool: pg.Pool;
	tenantId: string;
	scenarios?: Parameters<typeof makeHandlers>[0];
	effectMaxAttempts?: number;
}) {
	const { handlers, matchers } = makeHandlers(input.scenarios);
	const jobs = new Jobs({ db: input.pool, pollTimeoutMs: 50 });
	const cron = new Cron({ db: input.pool, pollTimeoutMs: 50 });
	const workflow = new Workflow({
		db: input.pool,
		jobs,
		tenantId: input.tenantId,
		definitions: [stockReplenishmentV1],
		handlers,
		matchers,
		effectMaxAttempts: input.effectMaxAttempts,
	});
	return { jobs, cron, workflow };
}

Deno.test({
	name: "happy path: LOW → SENT → MATCHED → CONFIRMED → _end_ok",
	ignore: !PG,
	async fn() {
		const pool = createPg();
		await resetSchema(pool);
		await createMigrate(pool).up("latest");

		const { jobs, cron, workflow } = setupRuntime({
			pool,
			tenantId: "test",
		});
		await jobs.start(2);

		const correlator = new WorkflowInboxCorrelator({ cron, workflow });

		try {
			const correlationToken = crypto.randomUUID();
			const inst = await workflow.create({
				definitionId: "stock_replenishment",
				definitionVersion: "1.0.0",
				correlationToken,
			});

			// Wait for the workflow to advance past detect_low_stock + send_order
			// and land in await_reply (execution_state = waiting).
			await waitUntil(async () => {
				const row = await workflow.find(inst.id);
				return row && row.execution_state === EXECUTION_STATE.WAITING
					? row
					: null;
			});

			const reread = await workflow.find(inst.id);
			assertEquals(reread?.cursor, "await_reply");

			// Drop an inbox signal & let the correlator process it (using tickOnce —
			// no need to start cron for this test).
			await workflow.appendInbox({
				source: "email",
				correlationToken,
				payload: { subject: "Re: order", body: "yes please" },
			});

			const processed = await correlator.tickOnce();
			assertEquals(processed, 1);

			const finalRow = await waitUntil(async () => {
				const row = await workflow.find(inst.id);
				return row && row.execution_state === EXECUTION_STATE.COMPLETED
					? row
					: null;
			});
			assertEquals(finalRow.cursor, "_end_ok");

			const history = await getHistory(pool, inst.id);
			const joined = history
				.map((h) => `${h.event_type}:${h.to_node ?? h.from_node ?? "_"}`)
				.join(" | ");
			for (
				const needle of [
					"created:detect_low_stock",
					"effect_dispatched:detect_low_stock",
					"transition:send_order",
					"effect_dispatched:send_order",
					"transition:await_reply",
					"signal_received:await_reply",
					"transition:classify_reply",
					"effect_dispatched:classify_reply",
					"transition:write_order",
					"effect_dispatched:write_order",
					"transition:_end_ok",
					"completed:_end_ok",
				]
			) {
				if (!joined.includes(needle)) {
					throw new Error(`history missing "${needle}"\nfull: ${joined}`);
				}
			}
		} finally {
			await jobs.stop();
			await pool.end();
		}
	},
});

Deno.test({
	name: "TIMEOUT path: scheduler wakes a suspended instance after wake_at",
	ignore: !PG,
	async fn() {
		const pool = createPg();
		await resetSchema(pool);
		await createMigrate(pool).up("latest");

		const { jobs, cron, workflow } = setupRuntime({
			pool,
			tenantId: "test-timeout",
		});
		await jobs.start(2);

		const scheduler = new WorkflowScheduler({ cron, workflow });

		try {
			const inst = await workflow.create({
				definitionId: "stock_replenishment",
				definitionVersion: "1.0.0",
				correlationToken: crypto.randomUUID(),
			});

			await waitUntil(async () => {
				const row = await workflow.find(inst.id);
				return row && row.execution_state === EXECUTION_STATE.WAITING
					? row
					: null;
			});

			// Force wake_at into the past so the scheduler's next tick pokes it.
			await pool.query(
				`UPDATE __workflow_instances SET wake_at = now() - interval '1 minute' WHERE id = $1`,
				[inst.id],
			);

			assertEquals(await scheduler.tickOnce(), { woken: 1, repoked: 0 });

			const finalRow = await waitUntil(async () => {
				const row = await workflow.find(inst.id);
				return row && row.execution_state === EXECUTION_STATE.COMPLETED
					? row
					: null;
			});
			assertEquals(finalRow.cursor, "_end_timeout");
		} finally {
			await jobs.stop();
			await pool.end();
		}
	},
});

Deno.test({
	name: "FAIL path: handler throws repeatedly → instance marked failed",
	ignore: !PG,
	async fn() {
		const pool = createPg();
		await resetSchema(pool);
		await createMigrate(pool).up("latest");

		const { jobs, workflow } = setupRuntime({
			pool,
			tenantId: "test-fail",
			scenarios: { throwOn: "checkInventory" },
			effectMaxAttempts: 1,
		});
		await jobs.start(2);

		try {
			const inst = await workflow.create({
				definitionId: "stock_replenishment",
				definitionVersion: "1.0.0",
			});

			const finalRow = await waitUntil(async () => {
				const row = await workflow.find(inst.id);
				return row && row.execution_state === EXECUTION_STATE.FAILED ? row : null;
			});
			assertNotEquals(finalRow.cursor, "_end_ok");
		} finally {
			await jobs.stop();
			await pool.end();
		}
	},
});

Deno.test({
	name: "validator rejects definitions referencing unregistered handlers",
	fn() {
		const bad = {
			id: "bad",
			version: "1.0.0",
			fsm: {
				initial: "s",
				states: {
					s: {
						meta: { kind: "effectful" as const, handler: "doesNotExist" },
						on: { OK: "_end" },
					},
					_end: { meta: { kind: "terminal" as const }, on: {} },
				},
			},
		};

		let threw = false;
		try {
			new Workflow({
				db: undefined as never,
				jobs: undefined as never,
				definitions: [bad],
				handlers: {},
			});
		} catch (e) {
			threw = true;
			if (!String(e).includes("unregistered handler")) {
				throw new Error(`unexpected error: ${e}`);
			}
		}
		if (!threw) throw new Error("expected validator to throw");
	},
});
