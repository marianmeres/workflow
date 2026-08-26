/**
 * `validateDefinition`'s runtime-failure rules: a node missing an edge for an
 * event it is guaranteed to receive is rejected at construction, not at 3am
 * with a half-run side effect behind it.
 *
 * Pure unit tests — no database, no queue.
 */
import { assertThrows } from "@std/assert";
import {
	validateDefinition,
	type WorkflowDefinition,
	WorkflowRegistry,
} from "../src/mod.ts";
import { makeRoutingHandlers, routingV1 } from "./fixtures/routing.ts";
import {
	makeHandlers,
	stockReplenishmentCooldownV1,
	stockReplenishmentV1,
} from "./fixtures/stock-replenishment.ts";
import {
	makeDigestCapture,
	makeDigestHandlers,
	weeklyDigestV1,
} from "./fixtures/weekly-digest.ts";

type States = WorkflowDefinition["fsm"]["states"];

const available = {
	handlers: new Set(["doEffect"]),
	matchers: new Set(["matchIt"]),
};

const END: States[string] = { meta: { kind: "terminal" }, on: {} };

/** Builds a definition entering at the first state given. */
function def(states: States): WorkflowDefinition {
	return {
		id: "t",
		version: "1.0.0",
		fsm: { initial: Object.keys(states)[0], states },
	};
}

function assertRejects(states: States, message: string): void {
	assertThrows(() => validateDefinition(def(states), available), Error, message);
}

Deno.test("validator: pure state without ENTER (or *) is rejected", () => {
	assertRejects(
		{ p: { meta: { kind: "pure" }, on: { OTHER: "_end" } }, _end: END },
		`pure state "p" has no "ENTER" (or "*") transition`,
	);
});

Deno.test("validator: pure state routing on the wildcard is accepted", () => {
	validateDefinition(
		def({ p: { meta: { kind: "pure" }, on: { "*": "_end" } }, _end: END }),
		available,
	);
});

Deno.test("validator: effectful state with an empty `on` is rejected", () => {
	assertRejects(
		{
			e: { meta: { kind: "effectful", handler: "doEffect" }, on: {} },
			_end: END,
		},
		`effectful state "e" has no transitions`,
	);
});

Deno.test("validator: suspending state with a matcher but no MATCHED is rejected", () => {
	assertRejects(
		{
			s: {
				meta: { kind: "suspending", matcher: "matchIt", timeoutSec: 60 },
				on: { TIMEOUT: "_end" },
			},
			_end: END,
		},
		`suspending state "s" has a matcher but no "MATCHED"`,
	);
});

Deno.test("validator: suspending state with a timeoutSec but no TIMEOUT is rejected", () => {
	assertRejects(
		{
			s: {
				meta: { kind: "suspending", matcher: "matchIt", timeoutSec: 60 },
				on: { MATCHED: "_end" },
			},
			_end: END,
		},
		`suspending state "s" has a timeoutSec but no "TIMEOUT"`,
	);
});

Deno.test("validator: non-positive or non-finite timeoutSec is rejected", () => {
	for (const timeoutSec of [0, -1, Number.NaN, Number.POSITIVE_INFINITY]) {
		assertRejects(
			{
				s: { meta: { kind: "suspending", timeoutSec }, on: { TIMEOUT: "_end" } },
				_end: END,
			},
			`suspending state "s" timeoutSec must be a finite number greater than 0`,
		);
	}
});

Deno.test("validator: suspending state that can never wake is rejected", () => {
	assertRejects(
		{
			s: { meta: { kind: "suspending" }, on: { SOMETHING: "_end" } },
			_end: END,
		},
		`suspending state "s" can never wake`,
	);
});

Deno.test("validator: a wildcard edge is a valid wake path", () => {
	validateDefinition(
		def({
			s: { meta: { kind: "suspending", matcher: "matchIt" }, on: { "*": "_end" } },
			_end: END,
		}),
		available,
	);
});

Deno.test("validator: terminal state with transitions is rejected", () => {
	assertRejects(
		{ _end: { meta: { kind: "terminal" }, on: { AGAIN: "_end" } } },
		`terminal state "_end" must have no transitions`,
	);
});

Deno.test("validator: a misspelled meta.kind is caught at compile time too", () => {
	const states: States = {
		e: {
			// @ts-expect-error — "efectful" is not a NodeMeta kind
			meta: { kind: "efectful", handler: "doEffect" },
			on: { OK: "_end" },
		},
		_end: END,
	};
	assertRejects(states, `state "e" has unknown meta.kind "efectful"`);
});

Deno.test("validator: a state without meta is caught at compile time too", () => {
	const states: States = {
		// @ts-expect-error — `meta` is required on WorkflowStateConfig
		e: { on: { OK: "_end" } },
		_end: END,
	};
	assertRejects(states, `state "e" is missing meta`);
});

Deno.test("validator: the shipped fixtures still validate", () => {
	const { handlers, matchers } = makeHandlers();
	new WorkflowRegistry({
		definitions: [stockReplenishmentV1, stockReplenishmentCooldownV1],
		handlers,
		matchers,
	});
	new WorkflowRegistry({ definitions: [routingV1], handlers: makeRoutingHandlers() });
	new WorkflowRegistry({
		definitions: [weeklyDigestV1],
		handlers: makeDigestHandlers(makeDigestCapture()),
	});
});
