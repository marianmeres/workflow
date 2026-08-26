import type pg from "pg";
import type { HistoryEventType, HistoryRow } from "../types.ts";

type Executor = pg.Pool | pg.PoolClient | pg.Client;

const SELECT_COLUMNS = `id, tenant_id, instance_id, at, event_type, from_node, to_node, data`;

/**
 * Appends a single audit row to `__workflow_history`. Accepts a pool, a
 * client, or a transaction-bound client — call this inside the same
 * transaction as the state change it describes.
 */
export async function appendHistory(
	exec: Executor,
	input: {
		tenant_id: string;
		instance_id: string;
		event_type: HistoryEventType;
		from_node?: string | null;
		to_node?: string | null;
		data?: Record<string, unknown>;
	},
): Promise<void> {
	await exec.query(
		`INSERT INTO __workflow_history
			(tenant_id, instance_id, event_type, from_node, to_node, data)
		 VALUES ($1, $2, $3, $4, $5, $6::jsonb)`,
		[
			input.tenant_id,
			input.instance_id,
			input.event_type,
			input.from_node ?? null,
			input.to_node ?? null,
			JSON.stringify(input.data ?? {}),
		],
	);
}

/**
 * Reads the audit log for an instance in chronological order (oldest first).
 * Useful for `/admin/workflows`-style observability surfaces and tests.
 *
 * @param exec - pool, client, or transaction-bound client
 * @param instance_id - id of the instance whose history to read
 * @param limit - max rows to return; default `200`
 */
export async function getHistory(
	exec: Executor,
	instance_id: string,
	limit: number = 200,
): Promise<HistoryRow[]> {
	const r = await exec.query<HistoryRow>(
		`SELECT ${SELECT_COLUMNS} FROM __workflow_history
		  WHERE instance_id = $1
		  ORDER BY at, id
		  LIMIT $2`,
		[instance_id, limit],
	);
	return r.rows;
}
