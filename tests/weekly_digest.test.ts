/**
 * Tests demonstrating the "recurring trigger" pattern: a cron job fires every
 * Monday morning and creates a fresh workflow instance.
 *
 * Key idea: the framework does NOT own the recurring trigger. The consumer
 * registers a normal `cron.register(...)` job on the shared `Cron` instance,
 * and the handler body calls `workflow.create(...)`. Each tick spawns one
 * fresh, independent instance.
 *
 * The tests trigger manually via `workflow.create()` rather than waiting for
 * the actual cron to fire — the cron part is just illustrated for the reader.
 */
import { Cron } from "@marianmeres/cron";
import { Jobs } from "@marianmeres/steve";
import { assertEquals } from "@std/assert";
import {
	createMigrate,
	EXECUTION_STATE,
	getHistory,
	Workflow,
	type WorkflowInstanceRow,
} from "../src/mod.ts";
import { createPg, pgConfigured, resetSchema } from "./_pg.ts";
import {
	makeDigestCapture,
	makeDigestHandlers,
	weeklyDigestV1,
} from "./fixtures/weekly-digest.ts";

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

Deno.test({
	name: "weekly digest happy path: fetch → summarize → send → _end_ok",
	ignore: !PG,
	async fn() {
		const pool = createPg();
		await resetSchema(pool);
		await createMigrate(pool).up("latest");

		const capture = makeDigestCapture();
		const handlers = makeDigestHandlers(capture);

		const jobs = new Jobs({ db: pool, pollTimeoutMs: 50 });
		const cron = new Cron({ db: pool, pollTimeoutMs: 50 });

		const wf = new Workflow({
			db: pool,
			jobs,
			tenantId: "test-digest",
			definitions: [weeklyDigestV1],
			handlers,
		});

		// THE RECURRING TRIGGER PATTERN.
		// In production this is how you'd schedule "every Monday at 9am":
		await cron.register(
			"weekly-digest-trigger",
			"0 9 * * 1", // Mon 09:00 host time (or pass timezone option)
			async () => {
				await wf.create({
					definitionId: "weekly_digest",
					definitionVersion: "1.0.0",
				});
			},
		);

		// For the test we just call the trigger body directly — waiting for an
		// actual Monday is silly. In real life: `await cron.start(N)` and let it
		// fire on schedule.
		await jobs.start(2);

		try {
			const inst = await wf.create({
				definitionId: "weekly_digest",
				definitionVersion: "1.0.0",
			});

			const finalRow = await waitUntil<WorkflowInstanceRow>(async () => {
				const row = await wf.find(inst.id);
				return row && row.execution_state === EXECUTION_STATE.COMPLETED ? row : null;
			});

			assertEquals(finalRow.cursor, "_end_ok");

			// Each handler ran exactly once
			assertEquals(capture.fetchCalls, 1);
			assertEquals(capture.aiCalls, 1);
			assertEquals(capture.sentEmails.length, 1);

			// Demonstrates context propagation: the email body contains the AI
			// summary, which in turn contains the fetched content. The chain
			// only works because the fsm `action` hooks on FETCHED / DONE copy
			// the handler payload into context.
			assertEquals(
				capture.sentEmails[0].body,
				"Summary of: Hello from xyz.com! Today's top story: ...",
			);

			// Final persisted context is also threaded
			assertEquals(finalRow.context.content, "Hello from xyz.com! Today's top story: ...");
			assertEquals(finalRow.context.summary, capture.sentEmails[0].body);
		} finally {
			await cron.stop();
			await jobs.stop();
			await pool.end();
		}
	},
});

Deno.test({
	name: "weekly digest AI failure: workflow stops at _end_ai_failed, no email sent",
	ignore: !PG,
	async fn() {
		const pool = createPg();
		await resetSchema(pool);
		await createMigrate(pool).up("latest");

		const capture = makeDigestCapture();
		const handlers = makeDigestHandlers(capture, { ai: "FAILED" });

		const jobs = new Jobs({ db: pool, pollTimeoutMs: 50 });

		const wf = new Workflow({
			db: pool,
			jobs,
			tenantId: "test-digest-fail",
			definitions: [weeklyDigestV1],
			handlers,
		});

		await jobs.start(2);

		try {
			const inst = await wf.create({
				definitionId: "weekly_digest",
				definitionVersion: "1.0.0",
			});

			const finalRow = await waitUntil<WorkflowInstanceRow>(async () => {
				const row = await wf.find(inst.id);
				return row && row.execution_state === EXECUTION_STATE.COMPLETED ? row : null;
			});

			// AI returned FAILED → routed to _end_ai_failed. The workflow lifecycle
			// still ends as `completed` (it reached a terminal node successfully) —
			// the distinction "happy vs failure path" is encoded in WHICH terminal
			// it landed on, not in execution_state. Use execution_state='failed'
			// only for unrecoverable errors (handler exceptions, fsm rejections, etc.).
			assertEquals(finalRow.cursor, "_end_ai_failed");

			// Fetch ran, AI ran (and returned FAILED), email never ran
			assertEquals(capture.fetchCalls, 1);
			assertEquals(capture.aiCalls, 1);
			assertEquals(capture.sentEmails.length, 0);

			// History shows the path taken
			const history = await getHistory(pool, inst.id);
			const path = history
				.map((h) => `${h.event_type}:${h.to_node ?? h.from_node ?? "_"}`)
				.join(" | ");
			for (
				const needle of [
					"created:fetch_content",
					"effect_dispatched:fetch_content",
					"transition:summarize",
					"effect_dispatched:summarize",
					"transition:_end_ai_failed",
					"completed:_end_ai_failed",
				]
			) {
				if (!path.includes(needle)) {
					throw new Error(`history missing "${needle}"\nfull: ${path}`);
				}
			}
		} finally {
			await jobs.stop();
			await pool.end();
		}
	},
});

Deno.test({
	name: "weekly digest: cron actually fires the trigger (one tick = one instance)",
	ignore: !PG,
	async fn() {
		// This test demonstrates the cron firing for real. We use a `* * * * *`
		// expression and `next_run_at` is computed by cron — to avoid waiting up
		// to a minute, we register, then nudge the DB row to be due immediately.
		const pool = createPg();
		await resetSchema(pool);
		await createMigrate(pool).up("latest");

		const capture = makeDigestCapture();
		const handlers = makeDigestHandlers(capture);

		const jobs = new Jobs({ db: pool, pollTimeoutMs: 50 });
		const cron = new Cron({ db: pool, pollTimeoutMs: 50 });

		const wf = new Workflow({
			db: pool,
			jobs,
			tenantId: "test-digest-cron",
			definitions: [weeklyDigestV1],
			handlers,
		});

		await cron.register(
			"weekly-digest-trigger",
			"* * * * *", // every minute — for the test we'll force next_run_at
			async () => {
				await wf.create({
					definitionId: "weekly_digest",
					definitionVersion: "1.0.0",
				});
			},
		);

		// Make the cron row due NOW so the next cron processor poll claims it
		// without waiting for the minute boundary.
		await pool.query(
			`UPDATE __cron SET next_run_at = now() - interval '1 second'
			  WHERE name = 'weekly-digest-trigger'`,
		);

		await jobs.start(2);
		await cron.start(1);

		try {
			// Wait for cron to fire, workflow.create() to be called, instance to
			// complete. We don't know the id up-front since the cron created it;
			// poll the DB for any completed instance in our tenant.
			const finalRow = await waitUntil<WorkflowInstanceRow>(async () => {
				const r = await pool.query<WorkflowInstanceRow>(
					`SELECT * FROM __workflow_instances
					  WHERE tenant_id = 'test-digest-cron'
					    AND execution_state = 'completed'
					    AND cursor = '_end_ok'
					  LIMIT 1`,
				);
				return r.rows[0] ?? null;
			}, { timeoutMs: 5_000 });

			assertEquals(finalRow.cursor, "_end_ok");
			assertEquals(capture.sentEmails.length, 1);
		} finally {
			await cron.stop();
			await jobs.stop();
			await pool.end();
		}
	},
});
