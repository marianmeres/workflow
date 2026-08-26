/**
 * The userland half of the example: one definition, its handlers, its matcher.
 *
 * This is the part the framework does not ship. `@marianmeres/workflow` brings the
 * dispatch core and the durability story; everything below — the graph, what each
 * effect actually does, what counts as a matching signal — is domain code.
 *
 * The flow is an expense approval:
 *
 *   triage (pure)
 *     ├── amount ≤ autoApproveBelow ────────────────────────────→ book
 *     └── notify_approver (effectful, sendApprovalRequest)
 *           ├── UNDELIVERABLE → _undeliverable
 *           └── SENT → await_decision (suspending, isDecision, 20s timeout)
 *                        ├── MATCHED → decide (pure)
 *                        │              ├── approve → book (effectful, bookExpense) → _approved
 *                        │              └── reject  → _rejected
 *                        └── TIMEOUT → escalate (pure)
 *                                       ├── notified < maxNotify → notify_approver (again)
 *                                       └── _expired
 *
 * All four node kinds are in there, plus the two things that make a workflow engine
 * necessary in the first place: a wait that outlives the process (`await_decision`) and
 * a signal that arrives from outside it (the inbox row the UI appends).
 *
 * Two deliberate teaching points:
 *
 *   1. **An expected failure is an outcome, not a throw.** `sendApprovalRequest` with no
 *      approver returns `UNDELIVERABLE`, the graph routes it, and the instance completes
 *      at a terminal node that says so. Nothing failed — the workflow simply ended
 *      somewhere else.
 *   2. **An unexpected failure is a throw.** `bookExpense` can be armed to throw once
 *      (a stand-in for a transient database blip). The effect job fails, the instance
 *      goes `failed`, and it takes a `retry()` to move again — from its current cursor,
 *      without replaying the effects it already did.
 *
 * Every handler sleeps a little on purpose: without it the `running` state would flash
 * past between two polls and the UI would look like it skipped a node.
 */
import type {
	Handler,
	Matcher,
	WorkflowContext,
	WorkflowDefinition,
} from "@marianmeres/workflow";

export const DEFINITION_ID = "expense_approval";
export const DEFINITION_VERSION = "1.0.0";

/**
 * How long `await_decision` waits before firing `TIMEOUT`.
 *
 * It lives in the **definition**, not on the instance: `wake_at` is computed from
 * `meta.timeoutSec` when the driver parks the instance, and there is no per-instance
 * override. A deadline that varies per instance would be a different definition version.
 * 20 seconds because this is a demo; a real approval would say `timeoutSec: 3 * 86400`
 * and mean it — the instance would sleep for three days across any number of restarts.
 */
export const DECISION_TIMEOUT_SEC = 20;

/** The shape this workflow keeps in `__workflow_instances.context`. */
export interface ExpenseContext extends WorkflowContext {
	title: string;
	amount: number;
	currency: string;
	/** Empty string = nobody to send to, which the graph handles as an outcome. */
	approver: string;
	/** Below this, `triage` skips the whole approval branch. */
	autoApproveBelow: number;
	/** How many times `sendApprovalRequest` may run, counting the first. */
	maxNotify: number;
	/** Bumped by the handler itself, via `HandlerResult.context`. */
	notified: number;
	decision?: "approve" | "reject";
	decidedBy?: string;
	decidedAt?: string;
	bookedAt?: string;
	reference?: string;
}

/** Guards and actions see the generic `WorkflowContext`; this names it. */
const ctx = (c: Readonly<WorkflowContext>): ExpenseContext => c as ExpenseContext;

/**
 * The definition. Pure data: node kinds, transition labels, and handler/matcher ids as
 * strings. The only functions in here are fsm guards and one fsm action — they belong to
 * the graph, not to the framework, and they never touch the database.
 */
export const expenseApprovalV1: WorkflowDefinition = {
	id: DEFINITION_ID,
	version: DEFINITION_VERSION,
	fsm: {
		initial: "triage",
		context: () => ({ notified: 0 }),
		states: {
			// A pure node costs no job and no row transition of its own: the driver
			// fires ENTER, follows the first guard that passes, and keeps going
			// inside the same advance. You never see an instance sitting here.
			triage: {
				meta: { kind: "pure" },
				on: {
					ENTER: [
						{
							target: "book",
							guard: (c) => ctx(c).amount <= ctx(c).autoApproveBelow,
						},
						{ target: "notify_approver" },
					],
				},
			},

			// An effectful node enqueues `workflow.effect.sendApprovalRequest` and
			// flips the instance to `running`. The handler's returned outcome is
			// what picks the edge below.
			notify_approver: {
				meta: { kind: "effectful", handler: "sendApprovalRequest" },
				on: { SENT: "await_decision", UNDELIVERABLE: "_undeliverable" },
			},

			// The whole reason this package exists. The instance is parked
			// (`waiting`), `wake_at` is set, and nothing in the process holds it —
			// it is a row. The scheduler wakes it on the timer; the correlator
			// wakes it when a matching inbox signal shows up.
			await_decision: {
				meta: {
					kind: "suspending",
					matcher: "isDecision",
					timeoutSec: DECISION_TIMEOUT_SEC,
				},
				on: {
					MATCHED: {
						target: "decide",
						// The driver never merges a signal's payload into the
						// context — `outcome_data` is the fsm event payload and
						// stops there. An action is how it becomes state.
						action: (context, payload) => {
							const p = (payload ?? {}) as Record<string, unknown>;
							const c = ctx(context);
							c.decision = p.decision === "reject" ? "reject" : "approve";
							c.decidedBy = String(p.by ?? "someone");
							c.decidedAt = new Date().toISOString();
						},
					},
					TIMEOUT: "escalate",
				},
			},

			decide: {
				meta: { kind: "pure" },
				on: {
					ENTER: [
						{ target: "book", guard: (c) => ctx(c).decision === "approve" },
						{ target: "_rejected" },
					],
				},
			},

			// Loops back into an effectful node, which parks at the wait again with
			// a fresh `wake_at`. The guard is what keeps a cycle from being
			// infinite — the driver's 64-hop ceiling only covers *pure* hops.
			escalate: {
				meta: { kind: "pure" },
				on: {
					ENTER: [
						{
							target: "notify_approver",
							guard: (c) => ctx(c).notified < ctx(c).maxNotify,
						},
						{ target: "_expired" },
					],
				},
			},

			book: {
				meta: { kind: "effectful", handler: "bookExpense" },
				on: { BOOKED: "_approved" },
			},

			_approved: { meta: { kind: "terminal" }, on: {} },
			_rejected: { meta: { kind: "terminal" }, on: {} },
			_expired: { meta: { kind: "terminal" }, on: {} },
			_undeliverable: { meta: { kind: "terminal" }, on: {} },
		},
	},
};

/* ---- the transient failure ------------------------------------------------- */

/**
 * Instances whose next `bookExpense` should throw, once.
 *
 * In memory on purpose: it stands in for the kind of fault `retry()` is for — a
 * connection reset, a provider hiccup — something that is gone by the time an operator
 * looks at it. Not a `failBooking` flag in the instance context, because a flag like
 * that would still be there after the retry and the instance would just fail again.
 */
const armed = new Set<string>();

/** Arms the next `bookExpense` for this instance to throw. */
export function armBookingFailure(instanceId: string): void {
	armed.add(instanceId);
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/* ---- handlers -------------------------------------------------------------- */

/**
 * Effect handlers, keyed by the id the definition's `meta.handler` refers to.
 *
 * Two rules the framework holds them to:
 *
 * - **Idempotent.** steve delivers at least once. A duplicate delivery is fenced out by
 *   `seq` before the handler runs, but a handler that already ran and then had its
 *   worker die is re-dispatched with the same payload — so "did I already do this?" has
 *   to be a question the side effect itself answers.
 * - **They return a label, not a decision.** The outcome string is looked up in the
 *   transition table. A handler cannot pick the next node, only name what happened.
 */
export const handlers: Record<string, Handler> = {
	/**
	 * Pretends to send the approval request.
	 *
	 * `context` on the result is a shallow patch merged *before* the outcome
	 * transition, so the `escalate` guard downstream reads the count this send just
	 * bumped. `data` would not do: that is the fsm payload and never reaches the
	 * persisted context on its own.
	 */
	sendApprovalRequest: async ({ context }) => {
		const c = context as ExpenseContext;
		await sleep(500);
		if (!c.approver) {
			// An expected, domain-level failure. Not a throw — the graph has an
			// edge for it, so this is a route, not a fault.
			return { outcome: "UNDELIVERABLE", data: { reason: "no approver" } };
		}
		const attempt = (c.notified ?? 0) + 1;
		return {
			outcome: "SENT",
			data: { to: c.approver, attempt },
			context: { notified: attempt },
			// A handler may also hand back the correlation token its side effect
			// produced (an SMTP Message-ID, a provider session id) — it is written
			// at the settle point, so it is on the row the moment the instance
			// becomes signallable. This example sets the token at create() time
			// instead, so a signal can arrive before the wait even exists:
			//   correlationToken: `msg-${instanceId}-${attempt}`,
		};
	},

	/**
	 * Pretends to write the expense to the ledger — and throws instead if this
	 * instance was armed for it.
	 */
	bookExpense: async ({ instanceId, context }) => {
		await sleep(600);
		if (armed.has(instanceId)) {
			armed.delete(instanceId);
			throw new Error("ledger unavailable (simulated transient failure)");
		}
		const c = context as ExpenseContext;
		return {
			outcome: "BOOKED",
			data: { amount: c.amount },
			context: {
				bookedAt: new Date().toISOString(),
				reference: `LEDGER-${instanceId.slice(0, 8)}`,
			},
		};
	},
};

/* ---- matchers -------------------------------------------------------------- */

/**
 * Signal matchers, keyed by the id a suspending node's `meta.matcher` refers to.
 *
 * The correlation token is the index — it is what finds the instance. The matcher is the
 * semantic gate: given that this row is addressed to this instance, is it the thing the
 * instance is waiting for? A `false` here is recorded as `signal_rejected` and the inbox
 * row is consumed; the instance keeps waiting.
 */
export const matchers: Record<string, Matcher> = {
	isDecision: ({ signal }) => {
		const decision = (signal.payload as { decision?: unknown }).decision;
		return decision === "approve" || decision === "reject";
	},
};
