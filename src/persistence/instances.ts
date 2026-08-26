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
	wake_at, correlation_token, seq, created_at, updated_at
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
 *
 * `options.bumpSeq` advances the fencing token in the same `UPDATE` — set it on
 * settle-point writes, so every job issued against the pre-write row becomes
 * recognizably stale.
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
	options: { bumpSeq?: boolean } = {},
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
	if (options.bumpSeq) sets.push(`seq = seq + 1`);
	sets.push(`updated_at = now()`);
	values.push(id);

	const r = await exec.query<WorkflowInstanceRow>(
		`UPDATE __workflow_instances SET ${sets.join(", ")} WHERE id = $${i}
		 RETURNING ${SELECT_COLUMNS}`,
		values,
	);
	return r.rows[0];
}

/** The minimum a scheduler poke needs: who to poke, and the fence to poke with. */
export interface InstancePoke {
	id: string;
	seq: number;
}

/**
 * Instances whose timer has expired. Read-only on purpose: the scheduler only
 * pokes an advance per row, and the advance — which re-checks the due-ness under
 * the row lock — is what writes the state and clears `wake_at`. A row poked but
 * not yet advanced stays due and is poked again next tick; the fence turns the
 * duplicate into a no-op. Nothing is left stranded by a crash mid-tick.
 */
export async function selectDueWakeUps(
	exec: Executor,
	tenant_id: string,
	limit: number = 100,
): Promise<InstancePoke[]> {
	const r = await exec.query<InstancePoke>(
		`SELECT id, seq FROM __workflow_instances
		  WHERE tenant_id = $1
		    AND execution_state = $2
		    AND wake_at IS NOT NULL
		    AND wake_at <= now()
		  ORDER BY wake_at
		  LIMIT $3`,
		[tenant_id, EXECUTION_STATE.WAITING, limit],
	);
	return r.rows;
}

/**
 * Instances stuck in `pending` for longer than `older_than_sec` — the residue of
 * a crash between `create()`'s commit and steve's (separate-connection) job
 * insert, i.e. rows no job will ever pick up.
 *
 * Only meaningful because `create()` is the sole producer of `pending`: every
 * other write path settles the row into a state this scan ignores. A row that is
 * merely waiting on a slow queue is re-poked too, harmlessly — the second
 * advance to reach it is fenced out.
 */
export async function selectStalePending(
	exec: Executor,
	tenant_id: string,
	older_than_sec: number,
	limit: number = 100,
): Promise<InstancePoke[]> {
	const r = await exec.query<InstancePoke>(
		`SELECT id, seq FROM __workflow_instances
		  WHERE tenant_id = $1
		    AND execution_state = $2
		    AND updated_at < now() - make_interval(secs => $3::float8)
		  ORDER BY updated_at
		  LIMIT $4`,
		[tenant_id, EXECUTION_STATE.PENDING, older_than_sec, limit],
	);
	return r.rows;
}

const TERMINAL_STATES = [
	EXECUTION_STATE.COMPLETED,
	EXECUTION_STATE.FAILED,
	EXECUTION_STATE.CANCELLED,
];

/**
 * Finds the live (non-terminal) instance owning this correlation token, if any.
 * Used by the inbox correlator — the instance may be in any live execution
 * state, not just `waiting`: a signal that arrives early is deferred, not
 * dropped.
 *
 * The contract is one live instance per token; `ORDER BY created_at` only makes
 * a violation deterministic (oldest wins) rather than arbitrary. Does not lock —
 * the advance re-reads under a lock and is fenced by `seq`.
 */
export async function findByCorrelation(
	exec: Executor,
	tenant_id: string,
	correlation_token: string,
): Promise<WorkflowInstanceRow | null> {
	const r = await exec.query<WorkflowInstanceRow>(
		`SELECT ${SELECT_COLUMNS} FROM __workflow_instances
		  WHERE tenant_id = $1
		    AND correlation_token = $2
		    AND NOT (execution_state = ANY($3))
		  ORDER BY created_at
		  LIMIT 1`,
		[tenant_id, correlation_token, TERMINAL_STATES],
	);
	return r.rows[0] ?? null;
}

/**
 * Finds the most recent terminal instance owning this correlation token, if any.
 * Only consulted when {@link findByCorrelation} misses, to tell "the instance is
 * over" apart from "nobody ever owned this token".
 *
 * Note that a *completed* instance has its token cleared, so in practice this
 * finds the failed/cancelled ones.
 */
export async function findTerminalByCorrelation(
	exec: Executor,
	tenant_id: string,
	correlation_token: string,
): Promise<WorkflowInstanceRow | null> {
	const r = await exec.query<WorkflowInstanceRow>(
		`SELECT ${SELECT_COLUMNS} FROM __workflow_instances
		  WHERE tenant_id = $1
		    AND correlation_token = $2
		    AND execution_state = ANY($3)
		  ORDER BY created_at DESC
		  LIMIT 1`,
		[tenant_id, correlation_token, TERMINAL_STATES],
	);
	return r.rows[0] ?? null;
}
