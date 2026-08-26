/**
 * @module @marianmeres/workflow
 *
 * Durable workflow / orchestration framework for long-lived state machines.
 * Built on PostgreSQL. Instances persist across process restarts, sleep for
 * days waiting for a signal or a timer, and resume from disk via
 * {@link https://jsr.io/@marianmeres/fsm | @marianmeres/fsm}'s `fromSnapshot`.
 * Effects run as {@link https://jsr.io/@marianmeres/steve | @marianmeres/steve}
 * jobs (transactional outbox). The scheduler and inbox correlator are driven
 * by {@link https://jsr.io/@marianmeres/cron | @marianmeres/cron}. Schema is
 * managed by {@link https://jsr.io/@marianmeres/migrate | @marianmeres/migrate}.
 *
 * Ships no domain code — userland brings the definitions, handlers, and
 * matchers. The framework provides the dispatch core and the durability story.
 *
 * @example
 * ```typescript
 * import pg from "pg";
 * import { Jobs } from "@marianmeres/steve";
 * import { Cron } from "@marianmeres/cron";
 * import { createMigrate, Workflow } from "@marianmeres/workflow";
 *
 * const pool = new pg.Pool({ /* ... *\/ });
 * await createMigrate(pool).up("latest");
 *
 * const jobs = new Jobs({ db: pool });
 * const wf = new Workflow({
 *     db: pool,
 *     jobs,
 *     definitions: [/* ... *\/],
 *     handlers: {/* ... *\/},
 * });
 *
 * await jobs.start(4);
 * await wf.create({ definitionId: "...", definitionVersion: "1.0.0" });
 * ```
 */
export { Workflow, type WorkflowOptions } from "./workflow.ts";
export {
	type SchedulerTickResult,
	WorkflowScheduler,
	type WorkflowSchedulerOptions,
} from "./scheduler.ts";
export {
	WorkflowInboxCorrelator,
	type WorkflowInboxCorrelatorOptions,
} from "./correlator.ts";
export { WorkflowRegistry } from "./registry.ts";
export { validateDefinition, defKey } from "./definition.ts";
export { createMigrate } from "./migrations/index.ts";
export { getHistory } from "./persistence/history.ts";
export {
	effectJobType,
	JOB_TYPE_ADVANCE,
	PURE_ENTER_EVENT,
} from "./driver.ts";
export {
	DEFAULT_TENANT_ID,
	EXECUTION_STATE,
	HISTORY_EVENT,
	JOB_TYPE_EFFECT_PREFIX,
	type AdvanceJobPayload,
	type AdvanceKind,
	type EffectJobPayload,
	type ExecutionState,
	type Handler,
	type HandlerArgs,
	type HandlerResult,
	type HistoryEventType,
	type HistoryRow,
	type InboxRow,
	type Matcher,
	type MatcherArgs,
	type NodeMeta,
	type WorkflowContext,
	type WorkflowDefinition,
	type WorkflowInstanceRow,
	type WorkflowSnapshot,
} from "./types.ts";
