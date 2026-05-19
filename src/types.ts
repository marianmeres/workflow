import type { FSMConfig, FSMSnapshot } from "@marianmeres/fsm";

/**
 * Default project id used when none is supplied. Mirrors the convention from
 * `@marianmeres/cron`.
 */
export const DEFAULT_PROJECT_ID = "_default";

/** Execution-lifecycle states. Independent of the FSM cursor. */
export const EXECUTION_STATE = {
	PENDING: "pending",
	RUNNING: "running",
	WAITING: "waiting",
	COMPLETED: "completed",
	FAILED: "failed",
	CANCELLED: "cancelled",
} as const;

/** Union of all valid {@link EXECUTION_STATE} string values. */
export type ExecutionState = (typeof EXECUTION_STATE)[keyof typeof EXECUTION_STATE];

/** Per-node metadata stored on each FSM state's `meta` field. Discriminated by `kind`. */
export type NodeMeta =
	| { kind: "pure" }
	| { kind: "effectful"; handler: string }
	| { kind: "suspending"; matcher?: string; timeoutSec?: number }
	| { kind: "terminal" };

/**
 * Context carried by a workflow instance. Plain JSON. Accumulates across nodes.
 */
export type WorkflowContext = Record<string, unknown>;

/**
 * A workflow definition: pure, JSON-serializable data. The fsm config carries
 * per-state {@link NodeMeta} on its `meta` field; the driver dispatches on
 * `meta.kind`. Outcome labels are fsm events.
 */
export interface WorkflowDefinition<
	TState extends string = string,
	TEvent extends string = string,
> {
	id: string;
	version: string;
	fsm: FSMConfig<TState, TEvent, WorkflowContext>;
}

/** Result returned by an effectful node's handler. */
export interface HandlerResult {
	outcome: string;
	data?: Record<string, unknown>;
}

/** Arguments passed to an effectful handler. */
export interface HandlerArgs {
	instanceId: string;
	projectId: string;
	context: WorkflowContext;
	signal?: AbortSignal;
}

/**
 * Userland effect-handler signature. Referenced from a node's
 * `meta.handler` by string id. Must return `{ outcome, data? }` — the
 * `outcome` label drives the next FSM transition. Handlers must be
 * idempotent (steve may retry them after worker crashes).
 */
export type Handler = (args: HandlerArgs) => Promise<HandlerResult> | HandlerResult;

/** Arguments passed to a matcher predicate. */
export interface MatcherArgs {
	instanceId: string;
	projectId: string;
	context: WorkflowContext;
	signal: InboxRow;
}

/**
 * Userland signal-matcher signature. Referenced from a suspending node's
 * `meta.matcher` by string id. Returns true if the incoming inbox signal
 * belongs to this waiting instance. The correlation token is just the
 * index — the matcher is the semantic gate.
 */
export type Matcher = (args: MatcherArgs) => boolean | Promise<boolean>;

/** A single row from `__workflow_instances`. Matches the schema 1:1. */
export interface WorkflowInstanceRow {
	id: string;
	project_id: string;
	definition_id: string;
	definition_version: string;
	cursor: string;
	previous_cursor: string | null;
	context: WorkflowContext;
	execution_state: ExecutionState;
	wake_at: Date | null;
	correlation_token: string | null;
	created_at: Date;
	updated_at: Date;
}

/** A single row from `__workflow_inbox`. */
export interface InboxRow {
	id: string;
	project_id: string;
	received_at: Date;
	source: string;
	correlation_token: string;
	payload: Record<string, unknown>;
	processed_at: Date | null;
}

/** Event types recorded in `__workflow_history`. */
export const HISTORY_EVENT = {
	CREATED: "created",
	TRANSITION: "transition",
	EFFECT_DISPATCHED: "effect_dispatched",
	EFFECT_COMPLETED: "effect_completed",
	EFFECT_FAILED: "effect_failed",
	SIGNAL_RECEIVED: "signal_received",
	SIGNAL_REJECTED: "signal_rejected",
	TIMEOUT: "timeout",
	TRANSITION_REJECTED: "transition_rejected",
	COMPLETED: "completed",
	FAILED: "failed",
	CANCELLED: "cancelled",
} as const;

/** Union of all valid {@link HISTORY_EVENT} string values. */
export type HistoryEventType = (typeof HISTORY_EVENT)[keyof typeof HISTORY_EVENT];

/** A single row from `__workflow_history`. */
export interface HistoryRow {
	id: number;
	project_id: string;
	instance_id: string;
	at: Date;
	event_type: HistoryEventType;
	from_node: string | null;
	to_node: string | null;
	data: Record<string, unknown>;
}

/** Steve job type for "advance this instance one step". */
export const JOB_TYPE_ADVANCE = "workflow.advance";

/** Steve job-type prefix for effect-handler jobs. Full type is `workflow.effect.<handlerName>`. */
export const JOB_TYPE_EFFECT_PREFIX = "workflow.effect.";

/** Payload of a `workflow.advance` job. */
export interface AdvanceJobPayload {
	project_id: string;
	instance_id: string;
	/** Optional outcome label to apply before dispatching. Set when an effect/signal completes. */
	outcome?: string;
	/** Optional payload to merge into context via the outcome's `data` field. */
	outcome_data?: Record<string, unknown>;
	/** Marks this advance as a TIMEOUT wake-up (so the driver emits a TIMEOUT outcome). */
	timeout?: boolean;
	[key: string]: unknown;
}

/** Payload of a `workflow.effect.<handlerName>` job. */
export interface EffectJobPayload {
	project_id: string;
	instance_id: string;
	handler: string;
	[key: string]: unknown;
}

/** Convenience re-export — the FSM snapshot shape the driver passes around. */
export type WorkflowSnapshot = FSMSnapshot<string, WorkflowContext>;
