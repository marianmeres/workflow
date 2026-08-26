/**
 * `create()` seeds the instance context from the definition's own `fsm.context`
 * (value or factory), with the caller's `context` shallow-merged over it. The
 * driver only ever resumes via `fromSnapshot`, so this is the one and only place
 * those defaults are ever read.
 *
 * The queue is constructed but never started — nothing here needs the instance
 * to advance, only the row `create()` writes.
 */
import { Jobs } from "@marianmeres/steve";
import { assertEquals } from "@std/assert";
import type pg from "pg";
import {
	createMigrate,
	Workflow,
	type WorkflowContext,
	type WorkflowDefinition,
} from "../src/mod.ts";
import { createPg, pgConfigured, resetSchema } from "./_pg.ts";
import { makeRoutingHandlers, routingV1 } from "./fixtures/routing.ts";

const PG = pgConfigured();

/** `start` → `_end`, no handlers needed; only the context shape matters here. */
function seeded(
	version: string,
	context?: WorkflowContext | (() => WorkflowContext),
): WorkflowDefinition {
	return {
		id: "seeded",
		version,
		fsm: {
			initial: "start",
			context,
			states: {
				start: { meta: { kind: "pure" }, on: { ENTER: "_end" } },
				_end: { meta: { kind: "terminal" }, on: {} },
			},
		},
	};
}

const factoryDefaults = seeded("1.0.0", () => ({ n: 1, s: "x" }));
const literalDefaults = seeded("2.0.0", { n: 1, s: "x" });

async function freshPool(): Promise<pg.Pool> {
	const pool = createPg();
	await resetSchema(pool);
	await createMigrate(pool).up("latest");
	return pool;
}

Deno.test({
	name: "create: input context is shallow-merged over the definition's defaults",
	ignore: !PG,
	async fn() {
		const pool = await freshPool();
		const jobs = new Jobs({ db: pool });
		try {
			const wf = new Workflow({
				db: pool,
				jobs,
				tenantId: "create-ctx",
				definitions: [factoryDefaults, literalDefaults, routingV1],
				handlers: makeRoutingHandlers(),
			});

			// Both the factory and the value form of `fsm.context` seed the row.
			for (const def of [factoryDefaults, literalDefaults]) {
				const overlaid = await wf.create({
					definitionId: def.id,
					definitionVersion: def.version,
					context: { s: "y" },
				});
				assertEquals(
					overlaid.context,
					{ n: 1, s: "y" },
					`overlay ${def.version}`,
				);

				const asDeclared = await wf.create({
					definitionId: def.id,
					definitionVersion: def.version,
				});
				assertEquals(asDeclared.context, { n: 1, s: "x" }, `bare ${def.version}`);
			}

			// A definition without `context` behaves exactly as before.
			const noDefaults = await wf.create({
				definitionId: "routing",
				definitionVersion: "1.0.0",
			});
			assertEquals(noDefaults.context, {});

			const withInput = await wf.create({
				definitionId: "routing",
				definitionVersion: "1.0.0",
				context: { route: "left" },
			});
			assertEquals(withInput.context, { route: "left" });
		} finally {
			await jobs.stop();
			await pool.end();
		}
	},
});
