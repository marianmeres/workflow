/**
 * One `Workflow` per `Jobs`. Steve's `setHandler` is last-writer-wins, so a
 * second attach used to hijack `workflow.advance` silently; now it throws, and
 * `detach()` is the way to hand the queue over.
 *
 * Nothing here touches the database — the pool is only what `Jobs` was built
 * with, and no job is ever created or claimed.
 */
import { Jobs } from "@marianmeres/steve";
import { assert, assertEquals, assertThrows } from "@std/assert";
import { effectJobType, JOB_TYPE_ADVANCE, Workflow } from "../src/mod.ts";
import { createPg, pgConfigured } from "./_pg.ts";
import { makeRoutingHandlers, routingV1 } from "./fixtures/routing.ts";

const PG = pgConfigured();

const ADVANCE = JOB_TYPE_ADVANCE;
const LEFT = effectJobType("leftEffect");

Deno.test({
	name: "attach: a second Workflow on the same Jobs throws until detach()",
	ignore: !PG,
	async fn() {
		const pool = createPg();
		const jobs = new Jobs({ db: pool });
		const options = {
			db: pool,
			jobs,
			definitions: [routingV1],
			handlers: makeRoutingHandlers(),
		};
		try {
			const first = new Workflow({ ...options, tenantId: "first" });
			assert(jobs.hasHandler(ADVANCE));
			assert(jobs.hasHandler(LEFT));

			const err = assertThrows(() =>
				new Workflow({ ...options, tenantId: "second" })
			);
			assert(
				String(err).includes("already attached to this Jobs instance"),
				`unexpected error: ${err}`,
			);
			// The refused constructor must not have overwritten anything.
			assertEquals(first.tenantId, "first");
			assert(jobs.hasHandler(ADVANCE));

			// A different queue is a different claim.
			const otherJobs = new Jobs({ db: pool });
			new Workflow({ ...options, jobs: otherJobs, tenantId: "other" });

			first.detach();
			assertEquals(jobs.hasHandler(ADVANCE), false);
			assertEquals(jobs.hasHandler(LEFT), false);

			const second = new Workflow({ ...options, tenantId: "second" });
			assert(jobs.hasHandler(ADVANCE));

			// The detached one no longer owns the queue: re-detaching it (double
			// teardown, hot reload) must not strip its successor's handlers.
			first.detach();
			assert(jobs.hasHandler(ADVANCE));
			assertEquals(second.tenantId, "second");
		} finally {
			await pool.end();
		}
	},
});
