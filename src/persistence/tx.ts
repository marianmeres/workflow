import type pg from "pg";

/**
 * Runs `fn` inside a single PG transaction on a checked-out client.
 * Commits on resolve, rolls back on throw. Always releases the client.
 */
export async function withTransaction<T>(
	pool: pg.Pool,
	fn: (client: pg.PoolClient) => Promise<T>,
): Promise<T> {
	const client = await pool.connect();
	try {
		await client.query("BEGIN");
		const result = await fn(client);
		await client.query("COMMIT");
		return result;
	} catch (err) {
		try {
			await client.query("ROLLBACK");
		} catch {
			// rollback best-effort; surface the original error
		}
		throw err;
	} finally {
		client.release();
	}
}
