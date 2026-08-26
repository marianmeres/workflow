import type pg from "pg";

interface Ctx {
	pool: pg.Pool;
}

/**
 * Builds an idempotent `RENAME COLUMN` guarded on the source column existing
 * and the target not existing — so it is a no-op on an already-migrated schema.
 * Postgres has no `IF EXISTS` for `RENAME COLUMN`, hence the DO block.
 *
 * The guard is scoped to `current_schema()` because the `ALTER TABLE` it guards
 * is unqualified and so resolves through `search_path`: without the predicate a
 * same-named table in *another* schema decides whether this schema gets renamed.
 *
 * Index definitions follow a renamed column automatically, and none of this
 * package's index *names* embed the column name, so nothing else to adjust.
 */
function renameColumn(table: string, from: string, to: string): string {
	const col = (c: string) =>
		`EXISTS (SELECT 1 FROM information_schema.columns
		          WHERE table_schema = current_schema()
		            AND table_name = '${table}' AND column_name = '${c}')`;
	return `
		DO $$
		BEGIN
			IF ${col(from)} AND NOT ${col(to)} THEN
				EXECUTE 'ALTER TABLE ${table} RENAME COLUMN ${from} TO ${to}';
			END IF;
		END $$;
	`;
}

const TABLES = [
	"__workflow_instances",
	"__workflow_inbox",
	"__workflow_history",
];

const SQL_UP = TABLES.map((t) => renameColumn(t, "project_id", "tenant_id")).join("\n");

const SQL_DOWN = TABLES.map((t) => renameColumn(t, "tenant_id", "project_id")).join("\n");

/**
 * Forward migration for schema version 1.1.0 — renames `project_id` to
 * `tenant_id` on all three framework tables, aligning with the ecosystem's
 * tenant-scoping convention (`@marianmeres/cron`, `@marianmeres/steve`).
 *
 * The rename is in place: existing data is preserved. Idempotent — safe to run
 * against a schema that already uses `tenant_id`.
 */
export async function up(ctx?: Record<string, unknown>): Promise<void> {
	const { pool } = ctx as unknown as Ctx;
	const client = await pool.connect();
	try {
		await client.query(SQL_UP);
	} finally {
		client.release();
	}
}

/** Reverse migration — renames `tenant_id` back to `project_id`. Non-destructive. */
export async function down(ctx?: Record<string, unknown>): Promise<void> {
	const { pool } = ctx as unknown as Ctx;
	const client = await pool.connect();
	try {
		await client.query(SQL_DOWN);
	} finally {
		client.release();
	}
}
