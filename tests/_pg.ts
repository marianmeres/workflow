import pg from "pg";

const env = {
	host: Deno.env.get("TEST_PG_HOST") || "localhost",
	database: Deno.env.get("TEST_PG_DATABASE"),
	user: Deno.env.get("TEST_PG_USER"),
	password: Deno.env.get("TEST_PG_PASSWORD"),
	port: parseInt(Deno.env.get("TEST_PG_PORT") || "5432"),
};

/** True iff the minimum env to connect to a test PG is provided. */
export function pgConfigured(): boolean {
	return Boolean(env.database && env.user);
}

export function createPg(): pg.Pool {
	if (!pgConfigured()) {
		throw new Error(
			"PG test env not configured (need TEST_PG_DATABASE, TEST_PG_USER, optionally TEST_PG_HOST/PORT/PASSWORD)",
		);
	}
	return new pg.Pool(env);
}

/**
 * Drops all framework + dependency tables we touch so each test starts clean.
 * Safe to call repeatedly; nothing happens if tables don't exist.
 */
export async function resetSchema(pool: pg.Pool): Promise<void> {
	const client = await pool.connect();
	try {
		await client.query(`
			DROP TABLE IF EXISTS __workflow_history;
			DROP TABLE IF EXISTS __workflow_inbox;
			DROP TABLE IF EXISTS __workflow_instances;
			DROP TABLE IF EXISTS __workflow_migrations;
			DROP TABLE IF EXISTS __job_attempt_log;
			DROP TABLE IF EXISTS __job;
			DROP TABLE IF EXISTS __cron_run_log;
			DROP TABLE IF EXISTS __cron;
		`);
	} finally {
		client.release();
	}
}
