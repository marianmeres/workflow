/**
 * Reference workflow for the "weekly digest" use case used by integration tests.
 *
 * Pipeline: fetch_content → summarize → send_email → _end_ok
 *
 * Each effectful node has a dedicated failure terminal so a failed AI call
 * doesn't accidentally trigger an email send.
 *
 * Data threading: handlers return `{ outcome, data }` but the driver doesn't
 * auto-merge `data` into context. We use fsm `action` hooks on transitions to
 * copy the handler payload into `ctx` so downstream nodes can read it via
 * `args.context`. The driver persists `ctx` after each transition.
 *
 * {@link weeklyDigestNoActionsV1} is the same pipeline threaded the other way —
 * via `HandlerResult.context`, with no `action`s at all.
 */
import type { Handler, WorkflowContext, WorkflowDefinition } from "../../src/mod.ts";

interface DigestContext extends WorkflowContext {
	url?: string;
	content?: string;
	summary?: string;
}

export const weeklyDigestV1: WorkflowDefinition = {
	id: "weekly_digest",
	version: "1.0.0",
	fsm: {
		initial: "fetch_content",
		states: {
			fetch_content: {
				meta: { kind: "effectful", handler: "fetchWebsite" },
				on: {
					FETCHED: {
						target: "summarize",
						action: (ctx: DigestContext, payload) => {
							const p = payload as { content?: string } | undefined;
							ctx.content = p?.content;
						},
					},
					FAILED: "_end_fetch_failed",
				},
			},
			summarize: {
				meta: { kind: "effectful", handler: "summarizeWithAi" },
				on: {
					DONE: {
						target: "send_email",
						action: (ctx: DigestContext, payload) => {
							const p = payload as { summary?: string } | undefined;
							ctx.summary = p?.summary;
						},
					},
					FAILED: "_end_ai_failed",
				},
			},
			send_email: {
				meta: { kind: "effectful", handler: "sendSummaryEmail" },
				on: {
					SENT: "_end_ok",
					FAILED: "_end_email_failed",
				},
			},
			_end_ok: { meta: { kind: "terminal" }, on: {} },
			_end_fetch_failed: { meta: { kind: "terminal" }, on: {} },
			_end_ai_failed: { meta: { kind: "terminal" }, on: {} },
			_end_email_failed: { meta: { kind: "terminal" }, on: {} },
		},
	},
};

/**
 * Same pipeline as {@link weeklyDigestV1}, but with no `action` hooks: the
 * handlers hand back `HandlerResult.context` and the driver merges it before
 * applying the outcome. The guard on the `DONE` edge reads `ctx.summary` — a
 * key that exists at guard time only because of that merge; without it the run
 * routes to `_end_unmerged` instead of `_end_ok`.
 */
export const weeklyDigestNoActionsV1: WorkflowDefinition = {
	id: "weekly_digest_no_actions",
	version: "1.0.0",
	fsm: {
		initial: "fetch_content",
		states: {
			fetch_content: {
				meta: { kind: "effectful", handler: "fetchWebsite" },
				on: { FETCHED: "summarize", FAILED: "_end_fetch_failed" },
			},
			summarize: {
				meta: { kind: "effectful", handler: "summarizeWithAi" },
				on: {
					DONE: [
						{
							target: "send_email",
							guard: (ctx: DigestContext) => !!ctx.summary,
						},
						{ target: "_end_unmerged" },
					],
					FAILED: "_end_ai_failed",
				},
			},
			send_email: {
				meta: { kind: "effectful", handler: "sendSummaryEmail" },
				on: { SENT: "_end_ok", FAILED: "_end_email_failed" },
			},
			_end_ok: { meta: { kind: "terminal" }, on: {} },
			_end_unmerged: { meta: { kind: "terminal" }, on: {} },
			_end_fetch_failed: { meta: { kind: "terminal" }, on: {} },
			_end_ai_failed: { meta: { kind: "terminal" }, on: {} },
			_end_email_failed: { meta: { kind: "terminal" }, on: {} },
		},
	},
};

/** External-world capture for assertions. Created fresh per test. */
export interface DigestCapture {
	fetchCalls: number;
	aiCalls: number;
	sentEmails: Array<{ to: string; subject: string; body: string }>;
}

export function makeDigestCapture(): DigestCapture {
	return { fetchCalls: 0, aiCalls: 0, sentEmails: [] };
}

/**
 * Builds handlers parameterised by which outcome each step should produce, so a
 * single test can drive the workflow down any path. All handlers are pure mocks
 * — no real HTTP, AI, or SMTP.
 */
export function makeDigestHandlers(
	capture: DigestCapture,
	scenarios: {
		fetch?: "FETCHED" | "FAILED";
		ai?: "DONE" | "FAILED";
		email?: "SENT" | "FAILED";
	} = {},
): Record<string, Handler> {
	return {
		fetchWebsite: () => {
			capture.fetchCalls++;
			if (scenarios.fetch === "FAILED") {
				return { outcome: "FAILED", data: { reason: "404" } };
			}
			return {
				outcome: "FETCHED",
				data: { content: "Hello from xyz.com! Today's top story: ..." },
			};
		},
		summarizeWithAi: ({ context }) => {
			capture.aiCalls++;
			const ctx = context as DigestContext;
			if (scenarios.ai === "FAILED") {
				return { outcome: "FAILED", data: { reason: "rate limited" } };
			}
			return {
				outcome: "DONE",
				data: { summary: `Summary of: ${ctx.content ?? "(empty)"}` },
			};
		},
		sendSummaryEmail: ({ context }) => {
			const ctx = context as DigestContext;
			if (scenarios.email === "FAILED") {
				return { outcome: "FAILED", data: { reason: "SMTP down" } };
			}
			capture.sentEmails.push({
				to: "me@example.com",
				subject: "Weekly digest",
				body: ctx.summary ?? "(no summary)",
			});
			return { outcome: "SENT" };
		},
	};
}

/**
 * Handlers for {@link weeklyDigestNoActionsV1}: they thread their output
 * through `context` instead of `data`, and produce the same final context as
 * the happy path of {@link makeDigestHandlers}.
 */
export function makeDigestHandlersViaContext(
	capture: DigestCapture,
): Record<string, Handler> {
	return {
		fetchWebsite: () => {
			capture.fetchCalls++;
			return {
				outcome: "FETCHED",
				context: { content: "Hello from xyz.com! Today's top story: ..." },
			};
		},
		summarizeWithAi: ({ context }) => {
			capture.aiCalls++;
			const ctx = context as DigestContext;
			return {
				outcome: "DONE",
				context: { summary: `Summary of: ${ctx.content ?? "(empty)"}` },
			};
		},
		sendSummaryEmail: ({ context }) => {
			const ctx = context as DigestContext;
			capture.sentEmails.push({
				to: "me@example.com",
				subject: "Weekly digest",
				body: ctx.summary ?? "(no summary)",
			});
			return { outcome: "SENT" };
		},
	};
}
