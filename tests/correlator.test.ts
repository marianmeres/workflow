import { Cron } from "@marianmeres/cron";
import { Jobs } from "@marianmeres/steve";
import { assert, assertEquals, assertNotEquals } from "@std/assert";
import type pg from "pg";
import {
	type AdvanceJobPayload,
	createMigrate,
	EXECUTION_STATE,
	getHistory,
	type Handler,
	type InboxRow,
	type Matcher,
	Workflow,
	WorkflowInboxCorrelator,
	type WorkflowInstanceRow,
} from "../src/mod.ts";
import { type JobEnqueuer, runAdvance } from "../src/driver.ts";
import { createPg, pgConfigured, resetSchema } from "./_pg.ts";
import { waitUntil } from "./_util.ts";
import {
	makeHandlers,
	stockReplenishmentCooldownV1,
	stockReplenishmentV1,
} from "./fixtures/stock-replenishment.ts";

const PG = pgConfigured();

/** Records what the driver would enqueue instead of touching steve. */
function recordingEnqueuer() {
	const effects: Array<{ handler: string; seq?: number }> = [];
	const enqueuer: JobEnqueuer = {
		enqueueAdvance: () => Promise.resolve(),
		enqueueEffect: (_c, _handler, payload) => {
			effects.push(payload);
			return Promise.resolve();
		},
	};
	return { effects, enqueuer };
}

/** Intercepts the pokes the correlator would hand to steve. */
function interceptPokes(workflow: Workflow): AdvanceJobPayload[] {
	const pokes: AdvanceJobPayload[] = [];
	workflow.enqueueAdvance = (_c, payload) => {
		pokes.push(payload);
		return Promise.resolve();
	};
	return pokes;
}

const instanceRow = async (pool: pg.Pool, id: string) => {
	const { rows } = await pool.query<WorkflowInstanceRow>(
		`SELECT * FROM __workflow_instances WHERE id = $1`,
		[id],
	);
	return rows[0];
};

const inboxRow = async (pool: pg.Pool, id: string) => {
	const { rows } = await pool.query<InboxRow>(
		`SELECT * FROM __workflow_inbox WHERE id = $1`,
		[id],
	);
	return rows[0];
};

function setup(input: {
	pool: pg.Pool;
	tenantId: string;
	handlers?: Record<string, Handler>;
	matchers?: Record<string, Matcher>;
}) {
	const base = makeHandlers();
	const jobs = new Jobs({ db: input.pool, pollTimeoutMs: 50 });
	const cron = new Cron({ db: input.pool, pollTimeoutMs: 50 });
	const workflow = new Workflow({
		db: input.pool,
		jobs,
		tenantId: input.tenantId,
		definitions: [stockReplenishmentV1, stockReplenishmentCooldownV1],
		handlers: input.handlers ?? base.handlers,
		matchers: input.matchers ?? base.matchers,
	});
	return {
		jobs,
		cron,
		workflow,
		correlator: new WorkflowInboxCorrelator({ cron, workflow }),
	};
}

/**
 * Seeds an instance and drives it — without steve, one `runAdvance` at a time —
 * up to its first suspending node. Returns that parked row plus a `run()` that
 * applies further advance payloads the same way.
 */
async function park(
	pool: pg.Pool,
	workflow: Workflow,
	input: { definitionId: string; correlationToken: string | null },
) {
	const tenant_id = workflow.tenantId;
	const { effects, enqueuer } = recordingEnqueuer();
	const { rows } = await pool.query<WorkflowInstanceRow>(
		`INSERT INTO __workflow_instances
			(tenant_id, definition_id, definition_version, cursor, context,
			 execution_state, correlation_token)
		 VALUES ($1, $2, '1.0.0', 'detect_low_stock', '{}'::jsonb, 'pending', $3)
		 RETURNING id, seq`,
		[tenant_id, input.definitionId, input.correlationToken],
	);
	const id = rows[0].id;

	const run = (patch: Omit<AdvanceJobPayload, "tenant_id" | "instance_id">) =>
		runAdvance(pool, workflow.registry, enqueuer, {
			tenant_id,
			instance_id: id,
			...patch,
		});
	const last = () => effects[effects.length - 1];

	await run({ kind: "start", expected_seq: rows[0].seq });
	await run({
		kind: "effect",
		expected_seq: last().seq,
		outcome: "LOW",
		handler: last().handler,
	});
	await run({
		kind: "effect",
		expected_seq: last().seq,
		outcome: "SENT",
		handler: last().handler,
	});

	return { id, run, last, row: () => instanceRow(pool, id) };
}

Deno.test({
	name: "early signal: deferred while the instance runs, delivered once it waits",
	ignore: !PG,
	async fn() {
		const pool = createPg();
		await resetSchema(pool);
		await createMigrate(pool).up("latest");

		let release = () => {};
		const gate = new Promise<void>((r) => (release = r));
		const { handlers } = makeHandlers();
		const { jobs, workflow, correlator } = setup({
			pool,
			tenantId: "early",
			handlers: {
				...handlers,
				sendOrderEmail: async (args) => {
					await gate;
					return await handlers.sendOrderEmail(args);
				},
			},
		});
		await jobs.start(2);

		try {
			const correlationToken = crypto.randomUUID();
			const inst = await workflow.create({
				definitionId: "stock_replenishment",
				definitionVersion: "1.0.0",
				correlationToken,
			});

			// The reply lands while the send step is still in flight.
			await waitUntil(async () => {
				const row = await workflow.find(inst.id);
				return row?.cursor === "send_order" &&
						row.execution_state === EXECUTION_STATE.RUNNING
					? row
					: null;
			});
			const signal = await workflow.appendInbox({
				source: "email",
				correlationToken,
				payload: { body: "yes please" },
			});

			assertEquals(await correlator.tickOnce(), 0);
			assertEquals((await inboxRow(pool, signal.id)).processed_at, null);
			assertEquals(
				(await workflow.find(inst.id))?.execution_state,
				EXECUTION_STATE.RUNNING,
			);

			release();
			await waitUntil(async () => {
				const row = await workflow.find(inst.id);
				return row?.execution_state === EXECUTION_STATE.WAITING ? row : null;
			});

			assertEquals(await correlator.tickOnce(), 1);

			const final = await waitUntil(async () => {
				const row = await workflow.find(inst.id);
				return row?.execution_state === EXECUTION_STATE.COMPLETED ? row : null;
			});
			assertEquals(final.cursor, "_end_ok");
			assertNotEquals((await inboxRow(pool, signal.id)).processed_at, null);
		} finally {
			release();
			await jobs.stop();
			await pool.end();
		}
	},
});

Deno.test({
	name: "timer-only node: a signal during the cooldown is deferred, not delivered",
	ignore: !PG,
	async fn() {
		const pool = createPg();
		await resetSchema(pool);
		await createMigrate(pool).up("latest");

		try {
			const { workflow, correlator } = setup({ pool, tenantId: "cooldown" });
			const pokes = interceptPokes(workflow);
			const correlationToken = crypto.randomUUID();
			const inst = await park(pool, workflow, {
				definitionId: "stock_replenishment_cooldown",
				correlationToken,
			});

			const parked = await inst.row();
			assertEquals(parked.cursor, "cooldown");
			assertEquals(parked.execution_state, EXECUTION_STATE.WAITING);

			const signal = await workflow.appendInbox({
				source: "email",
				correlationToken,
				payload: { body: "yes please" },
			});

			// `cooldown` has no MATCHED edge — delivering here would fail the instance.
			assertEquals(await correlator.tickOnce(), 0);
			assertEquals(pokes.length, 0);
			assertEquals((await inboxRow(pool, signal.id)).processed_at, null);
			const survived = await inst.row();
			assertEquals(survived.execution_state, EXECUTION_STATE.WAITING);
			assertEquals(survived.cursor, "cooldown");

			// The cooldown expires; now the instance is at the real wait point.
			// The timer has to be genuinely due: the scheduler tick only pokes, so
			// the advance is what re-checks `wake_at` before applying TIMEOUT.
			await pool.query(
				`UPDATE __workflow_instances SET wake_at = now() - interval '1 second' WHERE id = $1`,
				[inst.id],
			);
			await inst.run({
				kind: "timeout",
				expected_seq: survived.seq,
				outcome: "TIMEOUT",
			});
			assertEquals((await inst.row()).cursor, "await_reply");

			assertEquals(await correlator.tickOnce(), 1);
			assertEquals(pokes.length, 1);
			await inst.run(pokes[0]);

			assertEquals((await inst.row()).cursor, "classify_reply");
			assertNotEquals((await inboxRow(pool, signal.id)).processed_at, null);
		} finally {
			await pool.end();
		}
	},
});

Deno.test({
	name: "delivery: the correlator only pokes; the advance settles it atomically",
	ignore: !PG,
	async fn() {
		const pool = createPg();
		await resetSchema(pool);
		await createMigrate(pool).up("latest");

		try {
			const { workflow, correlator } = setup({ pool, tenantId: "deliver" });
			const pokes = interceptPokes(workflow);
			const correlationToken = crypto.randomUUID();
			const inst = await park(pool, workflow, {
				definitionId: "stock_replenishment",
				correlationToken,
			});
			const waiting = await inst.row();
			assertEquals(waiting.cursor, "await_reply");

			const signal = await workflow.appendInbox({
				source: "email",
				correlationToken,
				payload: { body: "yes please" },
			});

			// Two ticks before the poke is consumed: the row is still unprocessed,
			// so the second tick re-pokes it.
			assertEquals(await correlator.tickOnce(), 1);
			assertEquals(await correlator.tickOnce(), 1);
			assertEquals(pokes.length, 2);
			assertEquals(pokes[0].kind, "signal");
			assertEquals(pokes[0].inbox_id, signal.id);
			assertEquals(pokes[0].expected_seq, waiting.seq);
			assertEquals(pokes[0].outcome, undefined);

			// The correlator wrote nothing: no history, no instance row change.
			assertEquals((await inboxRow(pool, signal.id)).processed_at, null);
			const untouched = await inst.row();
			assertEquals(untouched.execution_state, EXECUTION_STATE.WAITING);
			assertEquals(untouched.seq, waiting.seq);

			await inst.run(pokes[0]);

			const delivered = await inst.row();
			assertEquals(delivered.cursor, "classify_reply");
			assertEquals(delivered.seq, waiting.seq + 1);
			assertNotEquals((await inboxRow(pool, signal.id)).processed_at, null);

			const received = (await getHistory(pool, inst.id)).find(
				(h) => h.event_type === "signal_received",
			);
			assert(received, "expected a signal_received history row");
			assertEquals(received.from_node, "await_reply");
			assertEquals(received.data.inbox_id, signal.id);
			assertEquals(received.data.source, "email");

			// The duplicate poke is fenced out — no second delivery.
			await inst.run(pokes[1]);
			const after = await inst.row();
			assertEquals(after.seq, delivered.seq);
			assertEquals(
				(await getHistory(pool, inst.id)).filter(
					(h) => h.event_type === "signal_received",
				).length,
				1,
			);
		} finally {
			await pool.end();
		}
	},
});

Deno.test({
	name: "two signals for one token: the second waits its turn instead of being dropped",
	ignore: !PG,
	async fn() {
		const pool = createPg();
		await resetSchema(pool);
		await createMigrate(pool).up("latest");

		try {
			const { workflow, correlator } = setup({ pool, tenantId: "duplicate" });
			const pokes = interceptPokes(workflow);
			const correlationToken = crypto.randomUUID();
			const inst = await park(pool, workflow, {
				definitionId: "stock_replenishment",
				correlationToken,
			});

			const first = await workflow.appendInbox({
				source: "email",
				correlationToken,
				payload: { body: "yes please" },
			});
			const second = await workflow.appendInbox({
				source: "email",
				correlationToken,
				payload: { body: "...and one more thing" },
			});

			// One poke per instance per tick.
			assertEquals(await correlator.tickOnce(), 1);
			assertEquals(pokes.length, 1);
			assertEquals(pokes[0].inbox_id, first.id);
			assertEquals((await inboxRow(pool, second.id)).processed_at, null);

			await inst.run(pokes[0]);
			assertNotEquals((await inboxRow(pool, first.id)).processed_at, null);

			// Instance is running the classify step — the second signal keeps waiting.
			assertEquals(await correlator.tickOnce(), 0);
			assertEquals((await inboxRow(pool, second.id)).processed_at, null);

			await inst.run({
				kind: "effect",
				expected_seq: inst.last().seq,
				outcome: "CONFIRMED",
				handler: inst.last().handler,
			});
			await inst.run({
				kind: "effect",
				expected_seq: inst.last().seq,
				outcome: "OK",
				handler: inst.last().handler,
			});
			assertEquals((await inst.row()).execution_state, EXECUTION_STATE.COMPLETED);

			// No live instance owns the token any more: resolved, not retried forever.
			assertEquals(await correlator.tickOnce(), 1);
			assertNotEquals((await inboxRow(pool, second.id)).processed_at, null);
			assertEquals(pokes.length, 1);
		} finally {
			await pool.end();
		}
	},
});

Deno.test({
	name: "matcher that throws is retried on the next tick",
	ignore: !PG,
	async fn() {
		const pool = createPg();
		await resetSchema(pool);
		await createMigrate(pool).up("latest");

		try {
			let calls = 0;
			const { workflow, correlator } = setup({
				pool,
				tenantId: "matcher-throw",
				matchers: {
					matchEmailReply: () => {
						if (calls++ === 0) {
							throw new Error("simulated transient lookup failure");
						}
						return true;
					},
				},
			});
			const pokes = interceptPokes(workflow);
			const correlationToken = crypto.randomUUID();
			const inst = await park(pool, workflow, {
				definitionId: "stock_replenishment",
				correlationToken,
			});

			const signal = await workflow.appendInbox({
				source: "email",
				correlationToken,
				payload: { body: "yes please" },
			});

			assertEquals(await correlator.tickOnce(), 0);
			assertEquals(pokes.length, 0);
			assertEquals((await inboxRow(pool, signal.id)).processed_at, null);

			assertEquals(await correlator.tickOnce(), 1);
			assertEquals(pokes.length, 1);
			assertEquals(pokes[0].inbox_id, signal.id);

			await inst.run(pokes[0]);
			assertEquals((await inst.row()).cursor, "classify_reply");
		} finally {
			await pool.end();
		}
	},
});

Deno.test({
	name: "handler-set correlation token: applied at the settle point, null clears it",
	ignore: !PG,
	async fn() {
		const pool = createPg();
		await resetSchema(pool);
		await createMigrate(pool).up("latest");

		let release = () => {};
		const gate = new Promise<void>((r) => (release = r));
		const { handlers } = makeHandlers();
		const { jobs, workflow, correlator } = setup({
			pool,
			tenantId: "handler-token",
			handlers: {
				...handlers,
				// The Message-ID only exists once the mail has actually been sent.
				sendOrderEmail: () => ({
					outcome: "SENT",
					data: { messageId: "msg-42" },
					correlationToken: "msg-42",
				}),
				aiClassifyReply: () => ({ outcome: "CONFIRMED", correlationToken: null }),
				persistOrder: async (args) => {
					await gate;
					return await handlers.persistOrder(args);
				},
			},
		});
		await jobs.start(2);

		try {
			const inst = await workflow.create({
				definitionId: "stock_replenishment",
				definitionVersion: "1.0.0",
			});
			assertEquals(inst.correlation_token, null);

			const waiting = await waitUntil(async () => {
				const row = await workflow.find(inst.id);
				return row?.execution_state === EXECUTION_STATE.WAITING ? row : null;
			});
			assertEquals(waiting.cursor, "await_reply");
			assertEquals(waiting.correlation_token, "msg-42");

			const transition = (await getHistory(pool, inst.id)).find(
				(h) => h.event_type === "transition" && h.data.outcome === "SENT",
			);
			assert(transition, "expected a transition history row for the SENT outcome");
			assertEquals(transition.data.correlation_token, "msg-42");

			// The wait point is now reachable by the token the handler produced.
			const signal = await workflow.appendInbox({
				source: "email",
				correlationToken: "msg-42",
				payload: { body: "yes please" },
			});
			assertEquals(await correlator.tickOnce(), 1);

			const cleared = await waitUntil(async () => {
				const row = await workflow.find(inst.id);
				return row?.cursor === "write_order" ? row : null;
			});
			assertEquals(cleared.execution_state, EXECUTION_STATE.RUNNING);
			assertEquals(cleared.correlation_token, null);

			release();
			const final = await waitUntil(async () => {
				const row = await workflow.find(inst.id);
				return row?.execution_state === EXECUTION_STATE.COMPLETED ? row : null;
			});
			assertEquals(final.cursor, "_end_ok");
			assertNotEquals((await inboxRow(pool, signal.id)).processed_at, null);
		} finally {
			release();
			await jobs.stop();
			await pool.end();
		}
	},
});

Deno.test({
	name: "no live owner: the row is marked processed on the first tick",
	ignore: !PG,
	async fn() {
		const pool = createPg();
		await resetSchema(pool);
		await createMigrate(pool).up("latest");

		try {
			const { workflow, correlator } = setup({ pool, tenantId: "no-owner" });
			const pokes = interceptPokes(workflow);

			const orphan = await workflow.appendInbox({
				source: "email",
				correlationToken: crypto.randomUUID(),
				payload: { body: "who?" },
			});

			const correlationToken = crypto.randomUUID();
			const inst = await park(pool, workflow, {
				definitionId: "stock_replenishment",
				correlationToken,
			});
			await pool.query(
				`UPDATE __workflow_instances SET execution_state = 'failed' WHERE id = $1`,
				[inst.id],
			);
			const late = await workflow.appendInbox({
				source: "email",
				correlationToken,
				payload: { body: "too late" },
			});

			assertEquals(await correlator.tickOnce(), 2);
			assertEquals(pokes.length, 0);
			assertNotEquals((await inboxRow(pool, orphan.id)).processed_at, null);
			assertNotEquals((await inboxRow(pool, late.id)).processed_at, null);

			const rejected = (await getHistory(pool, inst.id)).find(
				(h) => h.event_type === "signal_rejected",
			);
			assert(rejected, "expected a signal_rejected history row");
			assertEquals(rejected.data.inbox_id, late.id);
			assertEquals(rejected.data.reason, "instance is failed");
		} finally {
			await pool.end();
		}
	},
});
