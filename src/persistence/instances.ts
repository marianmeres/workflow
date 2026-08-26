import type pg from "pg";
import {
	EXECUTION_STATE,
	type ExecutionState,
	type WorkflowContext,
	type WorkflowInstanceRow,
} from "../types.ts";

type Executor = pg.Pool | pg.PoolClient | pg.Client;

const SELECT_COLUMNS = `
	id, tenant_id, definition_id, definition_version,
	cursor, previous_cursor, context, execution_state,
	wake_at, correlation_token, created_at, updated_at
`;

/**
 * Inserts a new workflow instance row in `execution_state='pending'` at the
 * provided cursor (typically `def.fsm.initial`). Caller is responsible for
 * enqueuing the first `workflow.advance` job.
 */
export async function createInstance(
	exec: Executor,
	input: {
		tenant_id: string;
		definition_id: string;
		definition_version: string;
		cursor: string;
		context: WorkflowContext;
		correlation_token?: string | null;
	},
): Promise<WorkflowInstanceRow> {
	const r = await exec.query<WorkflowInstanceRow>(
		`INSERT INTO __workflow_instances
			(tenant_id, definition_id, definition_version, cursor, context, execution_state, correlation_token)
		 VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7)
		 RETURNING ${SELECT_COLUMNS}`,
		[
			input.tenant_id,
			input.definition_id,
			input.definition_version,
			input.cursor,
			JSON.stringify(input.context),
			EXECUTION_STATE.PENDING,
			input.correlation_token ?? null,
		],
	);
	return r.rows[0];
}

/** Reads a workflow instance by id, or `null` if not found. No lock. */
export async function findInstance(
	exec: Executor,
	id: string,
): Promise<WorkflowInstanceRow | null> {
	const r = await exec.query<WorkflowInstanceRow>(
		`SELECT ${SELECT_COLUMNS} FROM __workflow_instances WHERE id = $1`,
		[id],
	);
	return r.rows[0] ?? null;
}

/** Loads with a row-level lock — caller MUST be inside a transaction. */
export async function lockInstance(
	client: pg.PoolClient | pg.Client,
	id: string,
): Promise<WorkflowInstanceRow | null> {
	const r = await client.query<WorkflowInstanceRow>(
		`SELECT ${SELECT_COLUMNS} FROM __workflow_instances WHERE id = $1 FOR UPDATE`,
		[id],
	);
	return r.rows[0] ?? null;
}

/**
 * Partial-update of a workflow instance row. Only fields present in `patch`
 * are written; `updated_at` is bumped automatically.
 */
export async function updateInstance(
	exec: Executor,
	id: string,
	patch: {
		cursor?: string;
		previous_cursor?: string | null;
		context?: WorkflowContext;
		execution_state?: ExecutionState;
		wake_at?: Date | null;
		correlation_token?: string | null;
	},
): Promise<WorkflowInstanceRow> {
	const sets: string[] = [];
	const values: unknown[] = [];
	let i = 1;

	if (patch.cursor !== undefined) {
		sets.push(`cursor = $${i++}`);
		values.push(patch.cursor);
	}
	if (patch.previous_cursor !== undefined) {
		sets.push(`previous_cursor = $${i++}`);
		values.push(patch.previous_cursor);
	}
	if (patch.context !== undefined) {
		sets.push(`context = $${i++}::jsonb`);
		values.push(JSON.stringify(patch.context));
	}
	if (patch.execution_state !== undefined) {
		sets.push(`execution_state = $${i++}`);
		values.push(patch.execution_state);
	}
	if (patch.wake_at !== undefined) {
		sets.push(`wake_at = $${i++}`);
		values.push(patch.wake_at);
	}
	if (patch.correlation_token !== undefined) {
		sets.push(`correlation_token = $${i++}`);
		values.push(patch.correlation_token);
	}
	sets.push(`updated_at = now()`);
	values.push(id);

	const r = await exec.query<WorkflowInstanceRow>(
		`UPDATE __workflow_instances SET ${sets.join(", ")} WHERE id = $${i}
		 RETURNING ${SELECT_COLUMNS}`,
		values,
	);
	return r.rows[0];
}

/**
 * Atomically transitions waiting+due rows to `pending` and clears their
 * `wake_at`. Returns the ids of rows that flipped — caller dispatches advance
 * jobs for each.
 *
 * Atomic on the row so two concurrent scheduler ticks can't double-dispatch.
 */
export async function claimDueWakeUps(
	exec: Executor,
	tenant_id: string,
	limit: number = 100,
): Promise<Array<{ id: string; correlation_token: string | null }>> {
	const r = await exec.query<{ id: string; correlation_token: string | null }>(
		`UPDATE __workflow_instances
		    SET execution_state = $2,
		        wake_at = NULL,
		        updated_at = now()
		  WHERE id IN (
		      SELECT id FROM __workflow_instances
		       WHERE tenant_id = $1
		         AND execution_state = $3
		         AND wake_at IS NOT NULL
		         AND wake_at <= now()
		       ORDER BY wake_at
		       LIMIT $4
		       FOR UPDATE SKIP LOCKED
		  )
		  RETURNING id, correlation_token`,
		[tenant_id, EXECUTION_STATE.PENDING, EXECUTION_STATE.WAITING, limit],
	);
	return r.rows;
}

/**
 * Finds the single waiting instance for this tenant + correlation token, if any.
 * Used by the inbox correlator. Does not lock (correlator processes one signal
 * at a time inside its own tx).
 */
export async function findWaitingByCorrelation(
	exec: Executor,
	tenant_id: string,
	correlation_token: string,
): Promise<WorkflowInstanceRow | null> {
	const r = await exec.query<WorkflowInstanceRow>(
		`SELECT ${SELECT_COLUMNS} FROM __workflow_instances
		  WHERE tenant_id = $1
		    AND execution_state = $2
		    AND correlation_token = $3
		  LIMIT 1`,
		[tenant_id, EXECUTION_STATE.WAITING, correlation_token],
	);
	return r.rows[0] ?? null;
}
