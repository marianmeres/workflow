/**
 * Minimal fixture for the driver's pure-node machinery, which the two reference
 * workflows never exercise — neither of them has a `pure` state at all.
 *
 *   classify (pure)
 *     ├── ENTER [ctx.route === "left"] → left_gate (pure) → do_left  (effectful) → _end_ok
 *     └── ENTER                        → right_gate (pure) → do_right (effectful) → _end_ok
 *
 *   spin (pure) — ENTER targets itself, so an advance that lands here can only
 *   end at the driver's hop guard. Unreachable from `classify` on purpose:
 *   tests seed the cursor there directly.
 */
import type { Handler, WorkflowContext, WorkflowDefinition } from "../../src/mod.ts";

interface RoutingContext extends WorkflowContext {
	route?: string;
}

export const routingV1: WorkflowDefinition = {
	id: "routing",
	version: "1.0.0",
	fsm: {
		initial: "classify",
		states: {
			classify: {
				meta: { kind: "pure" },
				on: {
					ENTER: [
						{
							target: "left_gate",
							guard: (ctx: RoutingContext) => ctx.route === "left",
						},
						{ target: "right_gate" },
					],
				},
			},
			left_gate: {
				meta: { kind: "pure" },
				on: { ENTER: "do_left" },
			},
			right_gate: {
				meta: { kind: "pure" },
				on: { ENTER: "do_right" },
			},
			do_left: {
				meta: { kind: "effectful", handler: "leftEffect" },
				on: { OK: "_end_ok" },
			},
			do_right: {
				meta: { kind: "effectful", handler: "rightEffect" },
				on: { OK: "_end_ok" },
			},
			spin: {
				meta: { kind: "pure" },
				on: { ENTER: "spin" },
			},
			_end_ok: { meta: { kind: "terminal" }, on: {} },
		},
	},
};

/**
 * The effects are never actually run by these tests — the driver only enqueues
 * them — but the registry refuses a definition naming a handler it doesn't know.
 */
export function makeRoutingHandlers(): Record<string, Handler> {
	return {
		leftEffect: () => ({ outcome: "OK", data: { side: "left" } }),
		rightEffect: () => ({ outcome: "OK", data: { side: "right" } }),
	};
}
