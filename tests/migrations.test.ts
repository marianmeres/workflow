import { Jobs } from "@marianmeres/steve";
import { assert, assertEquals } from "@std/assert";
import { createMigrate, JOB_TYPE_ADVANCE, Workflow } from "../src/mod.ts";
import {
	makeDigestCapture,
	makeDigestHandlers,
	weeklyDigestV1,
} from "./fixtures/weekly-digest.ts";
import { createPg, pgConfigured, resetSchema } from "./_pg.ts";
import type pg from "pg";

const columns = async (pool: pg.Pool, table: string): Promise<string[]> => {
	const { rows } = await pool.query<{ column_name: string }>(
		`SELECT column_name FROM information_schema.columns WHERE table_name = $1`,
		[table],
	);
	return rows.map((r) => r.column_name);
};

const TABLES = ["__workflow_instances", "__workflow_inbox", "__workflow_history"];

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
	name: "migration 1.1.0: renames project_id -> tenant_id, preserving data",
	ignore: !pgConfigured(),
	async fn() {
		const pool = createPg();
		try {
			await resetSchema(pool);

			// Stop at 1.0.0 — the pre-rename schema.
			await createMigrate(pool).up("1.0.0");
			for (const t of TABLES) {
				const cols = await columns(pool, t);
				assert(cols.includes("project_id"), `${t} should start with project_id`);
				assert(!cols.includes("tenant_id"), `${t} should not yet have tenant_id`);
			}

			// Seed legacy data.
			const { rows: [seeded] } = await pool.query<{ id: string }>(
				`INSERT INTO __workflow_instances
					(project_id, definition_id, definition_version, cursor, execution_state)
				 VALUES ('acme', 'def', '1.0.0', 'start', 'pending')
				 RETURNING id`,
			);
			await pool.query(
				`INSERT INTO __workflow_history (project_id, instance_id, event_type)
				 VALUES ('acme', $1, 'created')`,
				[seeded.id],
			);
			await pool.query(
				`INSERT INTO __workflow_inbox (project_id, source, correlation_token, payload)
				 VALUES ('acme', 'email', 'tok-1', '{"a":1}'::jsonb)`,
			);

			// Upgrade.
			await createMigrate(pool).up("latest");
			for (const t of TABLES) {
				const cols = await columns(pool, t);
				assert(
					cols.includes("tenant_id"),
					`${t} should have tenant_id after upgrade`,
				);
				assert(
					!cols.includes("project_id"),
					`${t} should have dropped project_id`,
				);
			}

			// Data survived the rename, under the new column.
			const inst = await pool.query(
				`SELECT tenant_id, definition_id FROM __workflow_instances WHERE id = $1`,
				[seeded.id],
			);
			assertEquals(inst.rows[0].tenant_id, "acme");
			assertEquals(inst.rows[0].definition_id, "def");

			const hist = await pool.query(
				`SELECT tenant_id FROM __workflow_history WHERE instance_id = $1`,
				[seeded.id],
			);
			assertEquals(hist.rows[0].tenant_id, "acme");

			const inbox = await pool.query(
				`SELECT tenant_id, correlation_token FROM __workflow_inbox`,
			);
			assertEquals(inbox.rows[0].tenant_id, "acme");
			assertEquals(inbox.rows[0].correlation_token, "tok-1");
		} finally {
			await pool.end();
		}
	},
});

Deno.test({
	name: "migration 1.1.0: idempotent, and reversible via down()",
	ignore: !pgConfigured(),
	async fn() {
		const pool = createPg();
		try {
			await resetSchema(pool);
			await createMigrate(pool).up("latest");

			// Re-running the rename against an already-renamed schema is a no-op.
			await createMigrate(pool).up("latest");
			for (const t of TABLES) {
				assert((await columns(pool, t)).includes("tenant_id"));
			}

			await pool.query(
				`INSERT INTO __workflow_instances
					(tenant_id, definition_id, definition_version, cursor, execution_state)
				 VALUES ('acme', 'def', '1.0.0', 'start', 'pending')`,
			);

			// down() rolls the rename back without losing data.
			await createMigrate(pool).down("1.0.0");
			for (const t of TABLES) {
				const cols = await columns(pool, t);
				assert(cols.includes("project_id"), `${t} should be back to project_id`);
				assert(
					!cols.includes("tenant_id"),
					`${t} should no longer have tenant_id`,
				);
			}
			const { rows } = await pool.query(
				`SELECT project_id FROM __workflow_instances`,
			);
			assertEquals(rows[0].project_id, "acme");
		} finally {
			await pool.end();
		}
	},
});

Deno.test({
	name: "migration 1.2.0: adds the seq fence, reversible via down('1.1.0')",
	ignore: !pgConfigured(),
	async fn() {
		const pool = createPg();
		try {
			await resetSchema(pool);

			await createMigrate(pool).up("1.1.0");
			assert(!(await columns(pool, "__workflow_instances")).includes("seq"));

			await createMigrate(pool).up("latest");
			assert((await columns(pool, "__workflow_instances")).includes("seq"));

			// Pre-existing rows migrate in at the fence's zero point.
			const { rows } = await pool.query<{ seq: number }>(
				`INSERT INTO __workflow_instances
					(tenant_id, definition_id, definition_version, cursor, execution_state)
				 VALUES ('acme', 'def', '1.0.0', 'start', 'pending')
				 RETURNING seq`,
			);
			assertEquals(rows[0].seq, 0);

			await createMigrate(pool).down("1.1.0");
			assert(!(await columns(pool, "__workflow_instances")).includes("seq"));
			assert((await columns(pool, "__workflow_instances")).includes("tenant_id"));
		} finally {
			await pool.end();
		}
	},
});

Deno.test({
	name:
		"upgrade path: an in-flight job with a legacy project_id payload still advances",
	ignore: !pgConfigured(),
	async fn() {
		const pool = createPg();
		await resetSchema(pool);
		await createMigrate(pool).up("latest");

		const capture = makeDigestCapture();
		const jobs = new Jobs({ db: pool, pollTimeoutMs: 50 });
		// Constructing the Workflow is the point here: it registers the
		// `workflow.advance` + `workflow.effect.*` handlers on `jobs`. The test
		// then drives it purely through the queue, never touching the instance.
		new Workflow({
			db: pool,
			jobs,
			tenantId: "legacy-payload",
			definitions: [weeklyDigestV1],
			handlers: makeDigestHandlers(capture),
		});

		// Seed the instance directly so the *first* advance is the legacy-shaped
		// one — mimicking a job enqueued by 1.0.x and still queued at upgrade.
		const { rows: [inst] } = await pool.query<{ id: string }>(
			`INSERT INTO __workflow_instances
				(tenant_id, definition_id, definition_version, cursor, context, execution_state)
			 VALUES ('legacy-payload', 'weekly_digest', '1.0.0', 'fetch_content', '{}'::jsonb, 'pending')
			 RETURNING id`,
		);

		// Note the `project_id` key — what a pre-rename enqueue wrote.
		await jobs.create(JOB_TYPE_ADVANCE, {
			project_id: "legacy-payload",
			instance_id: inst.id,
		});

		await jobs.start(2);
		try {
			const done = await waitUntil(async () => {
				const { rows } = await pool.query<{ execution_state: string }>(
					`SELECT execution_state FROM __workflow_instances WHERE id = $1`,
					[inst.id],
				);
				return rows[0]?.execution_state === "completed" ? rows[0] : null;
			});
			assertEquals(done.execution_state, "completed");
			assertEquals(capture.sentEmails.length, 1);

			// History must carry the tenant resolved from the legacy key — without
			// the fallback this insert would violate NOT NULL and the job would
			// retry until exhausted.
			const { rows } = await pool.query<{ tenant_id: string }>(
				`SELECT DISTINCT tenant_id FROM __workflow_history WHERE instance_id = $1`,
				[inst.id],
			);
			assertEquals(rows.map((r) => r.tenant_id), ["legacy-payload"]);
		} finally {
			await jobs.stop();
			await pool.end();
		}
	},
});
