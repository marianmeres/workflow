import type pg from "pg";

interface Ctx {
	pool: pg.Pool;
}

const SQL_UP = `
ALTER TABLE __workflow_instances ADD COLUMN IF NOT EXISTS seq integer NOT NULL DEFAULT 0;
`;

const SQL_DOWN = `
ALTER TABLE __workflow_instances DROP COLUMN IF EXISTS seq;
`;

/**
 * Forward migration for schema version 1.2.0 — adds the `seq` fencing token to
 * `__workflow_instances`.
 *
 * Every settle-point write bumps it, and every advance/effect job carries the
 * `seq` it was issued against, so a duplicate or zombie job lands as a no-op
 * instead of being applied to a row that has moved on.
 *
 * Additive and non-volatile, so on PG >= 11 this is a metadata-only change:
 * existing rows are not rewritten and start at `0`.
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
