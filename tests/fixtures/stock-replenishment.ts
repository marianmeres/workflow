/**
 * Reference workflow used by integration tests.
 *
 * The replenishment flow:
 *
 *   detect_low_stock (effectful, checkInventory)
 *     ├── LOW → send_order (effectful, sendOrderEmail)
 *     │           ├── SENT → await_reply (suspending, matchEmailReply, 3-day timeout)
 *     │           │           ├── MATCHED → classify_reply (effectful, aiClassifyReply)
 *     │           │           │              ├── CONFIRMED → write_order (effectful) → _end_ok
 *     │           │           │              ├── DENIED → _end_denied
 *     │           │           │              └── UNKNOWN → _end_unknown
 *     │           │           └── TIMEOUT → _end_timeout
 *     │           └── FAILED → _end_failed
 *     └── OK → _end_ok
 */
import type { Handler, Matcher, WorkflowDefinition } from "../../src/mod.ts";

export const stockReplenishmentV1: WorkflowDefinition = {
	id: "stock_replenishment",
	version: "1.0.0",
	fsm: {
		initial: "detect_low_stock",
		context: () => ({}),
		states: {
			detect_low_stock: {
				meta: { kind: "effectful", handler: "checkInventory" },
				on: { LOW: "send_order", OK: "_end_ok" },
			},
			send_order: {
				meta: { kind: "effectful", handler: "sendOrderEmail" },
				on: { SENT: "await_reply", FAILED: "_end_failed" },
			},
			await_reply: {
				meta: { kind: "suspending", matcher: "matchEmailReply", timeoutSec: 60 },
				on: { MATCHED: "classify_reply", TIMEOUT: "_end_timeout" },
			},
			classify_reply: {
				meta: { kind: "effectful", handler: "aiClassifyReply" },
				on: {
					CONFIRMED: "write_order",
					DENIED: "_end_denied",
					UNKNOWN: "_end_unknown",
				},
			},
			write_order: {
				meta: { kind: "effectful", handler: "persistOrder" },
				on: { OK: "_end_ok" },
			},
			_end_ok: { meta: { kind: "terminal" }, on: {} },
			_end_failed: { meta: { kind: "terminal" }, on: {} },
			_end_timeout: { meta: { kind: "terminal" }, on: {} },
			_end_denied: { meta: { kind: "terminal" }, on: {} },
			_end_unknown: { meta: { kind: "terminal" }, on: {} },
		},
	},
};

/**
 * Same flow with a timer-only `cooldown` node inserted between `send_order` and
 * `await_reply`. The cooldown has no `MATCHED` edge, so a reply arriving during
 * it is early: the correlator must defer it rather than deliver it (which would
 * fail the instance) or drop it.
 */
export const stockReplenishmentCooldownV1: WorkflowDefinition = {
	...stockReplenishmentV1,
	id: "stock_replenishment_cooldown",
	fsm: {
		...stockReplenishmentV1.fsm,
		states: {
			...stockReplenishmentV1.fsm.states,
			send_order: {
				meta: { kind: "effectful", handler: "sendOrderEmail" },
				on: { SENT: "cooldown", FAILED: "_end_failed" },
			},
			cooldown: {
				meta: { kind: "suspending", timeoutSec: 3600 },
				on: { TIMEOUT: "await_reply" },
			},
		},
	},
};

/**
 * Builds a set of handlers/matchers parameterised by the desired outcomes,
 * so a single test can drive the workflow through different paths.
 */
export function makeHandlers(scenarios: {
	inventory?: "LOW" | "OK";
	send?: "SENT" | "FAILED";
	classify?: "CONFIRMED" | "DENIED" | "UNKNOWN";
	throwOn?: string;
} = {}): { handlers: Record<string, Handler>; matchers: Record<string, Matcher> } {
	const handlers: Record<string, Handler> = {
		checkInventory: async () => {
			if (scenarios.throwOn === "checkInventory") {
				throw new Error("simulated checkInventory failure");
			}
			return { outcome: scenarios.inventory ?? "LOW", data: { stock: 3 } };
		},
		sendOrderEmail: async () => {
			if (scenarios.throwOn === "sendOrderEmail") {
				throw new Error("simulated sendOrderEmail failure");
			}
			return { outcome: scenarios.send ?? "SENT", data: { messageId: "msg-1" } };
		},
		aiClassifyReply: async () => {
			return { outcome: scenarios.classify ?? "CONFIRMED", data: {} };
		},
		persistOrder: async () => {
			return { outcome: "OK", data: { orderId: "o-1" } };
		},
	};

	const matchers: Record<string, Matcher> = {
		matchEmailReply: ({ signal }) => {
			return signal.source === "email";
		},
	};

	return { handlers, matchers };
}
