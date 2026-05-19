import type pg from "pg";

interface Ctx {
	pool: pg.Pool;
}

const SQL_UP = `
-- __workflow_instances: live state of a single running occurrence of a definition
CREATE TABLE IF NOT EXISTS __workflow_instances (
	id                  uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
	project_id          text        NOT NULL,
	definition_id       text        NOT NULL,
	definition_version  text        NOT NULL,
	cursor              text        NOT NULL,
	previous_cursor     text        NULL,
	context             jsonb       NOT NULL DEFAULT '{}'::jsonb,
	execution_state     text        NOT NULL,
	wake_at             timestamptz NULL,
	correlation_token   text        NULL,
	created_at          timestamptz NOT NULL DEFAULT now(),
	updated_at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS __workflow_instances_scheduler_idx
	ON __workflow_instances (project_id, execution_state, wake_at)
	WHERE wake_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS __workflow_instances_correlator_idx
	ON __workflow_instances (project_id, execution_state, correlation_token)
	WHERE correlation_token IS NOT NULL;

-- __workflow_inbox: append-only intake of external signals awaiting correlation
CREATE TABLE IF NOT EXISTS __workflow_inbox (
	id                  uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
	project_id          text        NOT NULL,
	received_at         timestamptz NOT NULL DEFAULT now(),
	source              text        NOT NULL,
	correlation_token   text        NOT NULL,
	payload             jsonb       NOT NULL,
	processed_at        timestamptz NULL
);

CREATE INDEX IF NOT EXISTS __workflow_inbox_unprocessed_idx
	ON __workflow_inbox (project_id, correlation_token)
	WHERE processed_at IS NULL;

-- __workflow_history: append-only per-instance audit log
CREATE TABLE IF NOT EXISTS __workflow_history (
	id          bigserial   PRIMARY KEY,
	project_id  text        NOT NULL,
	instance_id uuid        NOT NULL,
	at          timestamptz NOT NULL DEFAULT now(),
	event_type  text        NOT NULL,
	from_node   text        NULL,
	to_node     text        NULL,
	data        jsonb       NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS __workflow_history_instance_idx
	ON __workflow_history (instance_id, at);
`;

const SQL_DOWN = `
DROP TABLE IF EXISTS __workflow_history;
DROP TABLE IF EXISTS __workflow_inbox;
DROP TABLE IF EXISTS __workflow_instances;
`;

/**
 * Forward migration for schema version 1.0.0 — creates the three framework
 * tables and their indexes. Expects `ctx.pool` to be a `pg.Pool` (the shape
 * created by {@link createMigrate}).
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

/** Reverse migration — drops the three framework tables. Destructive. */
export async function down(ctx?: Record<string, unknown>): Promise<void> {
	const { pool } = ctx as unknown as Ctx;
	const client = await pool.connect();
	try {
		await client.query(SQL_DOWN);
	} finally {
		client.release();
	}
}
