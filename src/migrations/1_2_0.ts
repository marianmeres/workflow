import type pg from "pg";

interface Ctx {
	pool: pg.Pool;
}

const SQL_UP = `
ALTER TABLE __workflow_instances ADD COLUMN IF NOT EXISTS seq integer NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS __workflow_instances_stale_pending_idx
	ON __workflow_instances (tenant_id, updated_at) WHERE execution_state = 'pending';
`;

const SQL_DOWN = `
DROP INDEX IF EXISTS __workflow_instances_stale_pending_idx;
ALTER TABLE __workflow_instances DROP COLUMN IF EXISTS seq;
`;

/**
 * Forward migration for schema version 1.2.0 — adds the `seq` fencing token to
 * `__workflow_instances`, plus the partial index the scheduler's stale-`pending`
 * scan runs on.
 *
 * Every settle-point write bumps `seq`, and every advance/effect job carries the
 * `seq` it was issued against, so a duplicate or zombie job lands as a no-op
 * instead of being applied to a row that has moved on.
 *
 * Additive and non-volatile, so on PG >= 11 the column is a metadata-only
 * change: existing rows are not rewritten and start at `0`. The index is partial
 * on a transient state, so it stays tiny.
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

/** Reverse migration — drops `seq`. Destructive only of the fence itself. */
export async function down(ctx?: Record<string, unknown>): Promise<void> {
	const { pool } = ctx as unknown as Ctx;
	const client = await pool.connect();
	try {
		await client.query(SQL_DOWN);
	} finally {
		client.release();
	}
}
