import type pg from "pg";
import type { InboxRow } from "../types.ts";

type Executor = pg.Pool | pg.PoolClient | pg.Client;

const SELECT_COLUMNS = `
	id, project_id, received_at, source, correlation_token, payload, processed_at
`;

/**
 * Inserts an external signal into `__workflow_inbox`. The correlator will
 * pick it up on its next tick and attempt to match it to a waiting instance.
 */
export async function appendInbox(
	exec: Executor,
	input: {
		project_id: string;
		source: string;
		correlation_token: string;
		payload: Record<string, unknown>;
	},
): Promise<InboxRow> {
	const r = await exec.query<InboxRow>(
		`INSERT INTO __workflow_inbox (project_id, source, correlation_token, payload)
		 VALUES ($1, $2, $3, $4::jsonb)
		 RETURNING ${SELECT_COLUMNS}`,
		[
			input.project_id,
			input.source,
			input.correlation_token,
			JSON.stringify(input.payload),
		],
	);
	return r.rows[0];
}

/**
 * Claims a batch of unprocessed inbox rows for the project. Uses
 * `FOR UPDATE SKIP LOCKED` so two concurrent correlator workers don't grab
 * the same row. Caller MUST be inside a transaction and either mark them
 * processed or roll back.
 */
export async function claimUnprocessed(
	client: pg.PoolClient | pg.Client,
	project_id: string,
	limit: number = 100,
): Promise<InboxRow[]> {
	const r = await client.query<InboxRow>(
		`SELECT ${SELECT_COLUMNS} FROM __workflow_inbox
		  WHERE project_id = $1
		    AND processed_at IS NULL
		  ORDER BY received_at
		  LIMIT $2
		  FOR UPDATE SKIP LOCKED`,
		[project_id, limit],
	);
	return r.rows;
}

/** Marks an inbox row as processed (sets `processed_at = now()`). */
export async function markProcessed(
	exec: Executor,
	id: string,
): Promise<void> {
	await exec.query(
		`UPDATE __workflow_inbox SET processed_at = now() WHERE id = $1`,
		[id],
	);
}
