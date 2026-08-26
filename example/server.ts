/**
 * The example app's server: the runtime, and a small HTTP API over it.
 *
 * Why a server at all — nothing in this package can run in a browser. It is PostgreSQL,
 * a job queue and a cron ticker, and the whole point of it is that a workflow instance
 * outlives the process that started it, never mind the page. So the browser holds the
 * buttons and everything real happens here.
 *
 * The wiring below is the wiring from the README, verbatim in shape:
 *
 *   pool → createMigrate(pool).up("latest")
 *        → Jobs (yours) + Cron (yours)
 *        → Workflow attaches its handlers to the Jobs
 *        → WorkflowScheduler + WorkflowInboxCorrelator attach their ticks to the Cron
 *        → you start and stop both runtimes
 *
 * with exactly one demo-shaped deviation, and it is a loud one: **the ticks are also
 * driven by hand.** A cron expression's finest granularity is one minute, so a demo that
 * waited for `* * * * *` would show you a spinner for up to sixty seconds before a
 * timeout fired. `tickOnce()` is public API for precisely this ("testing and on-demand
 * wakes"), so the server calls it on a 500 ms interval — or not at all, if you switch
 * the tick mode to manual and press the buttons yourself. The registered cron jobs are
 * still there, still ticking every minute, doing the same thing more slowly.
 *
 * Routes:
 *
 *   GET  /                        the app (index.html + bundle + the two stylesheets)
 *   GET  /api/runtime             what this server is running, and the tick mode
 *   POST /api/runtime             { autoTick }
 *   GET  /api/definition          the graph, serialized out of the registered definition
 *   GET  /api/instances           recent instances
 *   POST /api/instances           create one
 *   GET  /api/instance/:id        the poll — row, history, inbox
 *   POST /api/instance/:id/signal { kind: approve | reject | junk | stray }
 *   POST /api/instance/:id/cancel
 *   POST /api/instance/:id/retry
 *   POST /api/tick                { what: scheduler | correlator | both }
 *
 * Run with: `deno task example` (then open http://127.0.0.1:8000).
 */

import pg from "pg";
import { extname, fromFileUrl, join, normalize } from "@std/path";
import { Jobs } from "@marianmeres/steve";
import { Cron } from "@marianmeres/cron";
import {
	createMigrate,
	EXECUTION_STATE,
	getHistory,
	type HistoryRow,
	type InboxRow,
	Workflow,
	WorkflowInboxCorrelator,
	type WorkflowInstanceRow,
	WorkflowScheduler,
} from "@marianmeres/workflow";
import {
	armBookingFailure,
	DECISION_TIMEOUT_SEC,
	DEFINITION_ID,
	DEFINITION_VERSION,
	expenseApprovalV1,
	handlers,
	matchers,
} from "./workflow.ts";

/* ---- config ---------------------------------------------------------------- */

const PORT = Number(Deno.env.get("PORT") ?? 8000);
/** `127.0.0.1` by default; a deployment sets `EXAMPLE_HOST=0.0.0.0` deliberately. */
const HOSTNAME = Deno.env.get("EXAMPLE_HOST") ?? "127.0.0.1";
const STATIC_ROOT = fromFileUrl(new URL("./", import.meta.url));

const TENANT_ID = "example";

/** How often the demo drives the ticks by hand while the mode is `auto`. */
const TICK_MS = 500;

/** Ceilings, so a browser cannot ask this box for something silly. */
const CAPS = {
	/** Non-terminal instances at once. */
	live: 25,
	amount: 1_000_000,
	maxNotify: 4,
	/** History rows read per poll. One instance never has many. */
	history: 200,
	/** Inbox rows shown, newest first, across the whole tenant. */
	inbox: 20,
	/** Instances listed in the sidebar. */
	instances: 20,
} as const;

const HTML = "text/html; charset=utf-8";
const MIME: Record<string, string> = {
	".html": HTML,
	".css": "text/css; charset=utf-8",
	".js": "text/javascript; charset=utf-8",
	".json": "application/json; charset=utf-8",
	".svg": "image/svg+xml",
};

/* ---- the runtime ----------------------------------------------------------- */

const db = new pg.Pool({
	host: Deno.env.get("EXAMPLE_PG_HOST") ?? "localhost",
	database: Deno.env.get("EXAMPLE_PG_DATABASE") ?? "example_workflow",
	user: Deno.env.get("EXAMPLE_PG_USER") || undefined,
	password: Deno.env.get("EXAMPLE_PG_PASSWORD") || undefined,
	port: Number(Deno.env.get("EXAMPLE_PG_PORT") ?? 5432),
	max: 10,
});

// The three tables. Idempotent, so it runs on every boot.
await createMigrate(db).up("latest");

/**
 * Both runtimes are **yours**, not the framework's — that is the shape the package
 * insists on, so that your own jobs and cron ticks can share one poller per process
 * instead of N.
 *
 * `autoCleanup` is not decoration: it runs steve's reaper, and the reaper is the only
 * thing that notices a job whose worker died mid-run. Without it such a job sits in
 * `running` forever and its instance never moves again.
 *
 * The 250 ms poll is a demo value. The default is a second, which is right for
 * production and feels sticky when you are watching a graph.
 */
const jobs = new Jobs({ db, pollTimeoutMs: 250, autoCleanup: true });
const cron = new Cron({ db, pollTimeoutMs: 250 });

const wf = new Workflow({
	db,
	jobs,
	tenantId: TENANT_ID,
	definitions: [expenseApprovalV1],
	handlers,
	matchers,
	/**
	 * One attempt, so a throwing handler surfaces immediately as a `failed`
	 * instance you can press Retry on.
	 *
	 * The default is 3, and it is the right default: steve would retry
	 * `bookExpense` with exponential backoff and the transient failure this demo
	 * simulates would heal on attempt two, with the instance never leaving
	 * `running`. That is the better production behavior and the worse
	 * demonstration — there would be nothing to see.
	 */
	effectMaxAttempts: 1,
});

const scheduler = new WorkflowScheduler({
	cron,
	workflow: wf,
	// The default is every minute; the demo ticker below is what actually keeps
	// this responsive. Left registered because this is the production wiring and
	// a demo that quietly dropped it would teach the wrong thing.
	tickExpression: "* * * * *",
	// Default 300. Shorter here so the "instance stranded in pending" recovery is
	// something you can witness rather than read about.
	stalePendingSec: 30,
});

const correlator = new WorkflowInboxCorrelator({
	cron,
	workflow: wf,
	tickExpression: "* * * * *",
});

await scheduler.register();
await correlator.register();

await jobs.start(2);
await cron.start(2);

/* ---- the demo ticker ------------------------------------------------------- */

const ticks = {
	auto: true,
	count: 0,
	woken: 0,
	repoked: 0,
	resolved: 0,
};

/**
 * One scheduler tick and one correlator tick.
 *
 * Both are read-only against `__workflow_instances`: they select what looks actionable
 * and enqueue a `workflow.advance` for it. Nothing here writes an instance row — the
 * advance does, under a lock, after re-checking the precondition. Which is why calling
 * this by hand, twice, out of order, or while the cron is doing the same thing, is
 * uninteresting rather than dangerous.
 */
async function tickOnce(what: "scheduler" | "correlator" | "both") {
	const out = { woken: 0, repoked: 0, resolved: 0 };
	if (what !== "correlator") {
		const r = await scheduler.tickOnce();
		out.woken = r.woken;
		out.repoked = r.repoked;
	}
	if (what !== "scheduler") {
		out.resolved = await correlator.tickOnce();
	}
	ticks.count++;
	ticks.woken += out.woken;
	ticks.repoked += out.repoked;
	ticks.resolved += out.resolved;
	return out;
}

let ticking = false;
setInterval(() => {
	// no overlap: a slow tick must not stack up behind itself
	if (!ticks.auto || ticking) return;
	ticking = true;
	tickOnce("both")
		.catch((e) => console.error(`tick: ${e}`))
		.finally(() => (ticking = false));
}, TICK_MS);

/* ---- reading it back ------------------------------------------------------- */

/**
 * Listing instances is raw SQL because the package has no list API: it exposes
 * `find(id)` and `getHistory(id)`, which is what a workflow *runtime* needs. An admin
 * surface is a different job, and this is what one looks like at its smallest.
 */
async function listInstances(): Promise<WorkflowInstanceRow[]> {
	const r = await db.query<WorkflowInstanceRow>(
		`SELECT id, tenant_id, definition_id, definition_version, cursor,
		        previous_cursor, context, execution_state, wake_at,
		        correlation_token, seq, created_at, updated_at
		   FROM __workflow_instances
		  WHERE tenant_id = $1
		  ORDER BY created_at DESC
		  LIMIT $2`,
		[TENANT_ID, CAPS.instances],
	);
	return r.rows;
}

async function countLive(): Promise<number> {
	const r = await db.query<{ n: string }>(
		`SELECT count(*) AS n FROM __workflow_instances
		  WHERE tenant_id = $1 AND execution_state IN ($2, $3, $4)`,
		[
			TENANT_ID,
			EXECUTION_STATE.PENDING,
			EXECUTION_STATE.RUNNING,
			EXECUTION_STATE.WAITING,
		],
	);
	return Number(r.rows[0]?.n ?? 0);
}

/**
 * The inbox, newest first, across the whole tenant — not just this instance's token.
 *
 * That is deliberate: the inbox is a shared, unaddressed queue, and the interesting
 * states are the ones where a row is *not* yet anyone's. A row with `processed_at: null`
 * is either waiting for the next correlator tick or being deferred by it, and telling
 * those apart is the whole lesson of "a signal that arrives early is early, not wrong".
 */
async function listInbox(): Promise<InboxRow[]> {
	const r = await db.query<InboxRow>(
		`SELECT id, tenant_id, received_at, source, correlation_token, payload, processed_at
		   FROM __workflow_inbox
		  WHERE tenant_id = $1
		  ORDER BY received_at DESC, id DESC
		  LIMIT $2`,
		[TENANT_ID, CAPS.inbox],
	);
	return r.rows;
}

/** Row + history + inbox, plus the server's clock so the countdown is honest. */
async function snapshot(id: string): Promise<Record<string, unknown>> {
	const instance = await wf.find(id);
	if (!instance) throw new HttpError(404, `No such instance: ${id}`);
	const [history, inbox] = await Promise.all([
		getHistory(db, id, CAPS.history) as Promise<HistoryRow[]>,
		listInbox(),
	]);
	// A terminal instance has had its correlation token cleared, so "is this row
	// mine?" cannot be answered by the token alone once it finishes. The history
	// remembers: every delivered or rejected signal recorded its `inbox_id`.
	const seen = new Set<string>();
	for (const row of history) {
		const inboxId = row.data?.inbox_id;
		if (typeof inboxId === "string") seen.add(inboxId);
	}

	return {
		instance,
		history,
		inbox: inbox.map((row) => ({
			...row,
			mine: seen.has(row.id) ||
				(!!instance.correlation_token &&
					row.correlation_token === instance.correlation_token),
		})),
		now: new Date().toISOString(),
		terminal: isTerminal(instance),
	};
}

const isTerminal = (row: WorkflowInstanceRow): boolean =>
	row.execution_state === EXECUTION_STATE.COMPLETED ||
	row.execution_state === EXECUTION_STATE.FAILED ||
	row.execution_state === EXECUTION_STATE.CANCELLED;

/* ---- the graph, as data ---------------------------------------------------- */

interface GraphEdge {
	event: string;
	target: string;
	/** The edge only fires when a guard passes; the guard itself is code, not data. */
	guarded: boolean;
}

interface GraphNode {
	name: string;
	kind: string;
	handler?: string;
	matcher?: string;
	timeoutSec?: number;
	initial: boolean;
	edges: GraphEdge[];
}

/**
 * The definition, flattened for the browser.
 *
 * Sent rather than hardcoded in the client so the picture cannot drift from the graph
 * the driver is actually walking. Guards and actions do not survive the trip — they are
 * functions — so a guarded edge is sent as a flag and drawn as one.
 */
function serializeGraph(): GraphNode[] {
	const fsm = expenseApprovalV1.fsm;
	return Object.entries(fsm.states).map(([name, state]) => {
		const meta = state.meta as Record<string, unknown>;
		const edges: GraphEdge[] = [];
		for (const [event, def] of Object.entries(state.on ?? {})) {
			for (const t of Array.isArray(def) ? def : [def]) {
				if (typeof t === "string") {
					edges.push({ event, target: t, guarded: false });
				} else if (t && typeof t === "object" && "target" in t) {
					edges.push({
						event,
						target: String(t.target),
						guarded: typeof t.guard === "function",
					});
				}
			}
		}
		return {
			name,
			kind: String(meta.kind),
			handler: meta.handler as string | undefined,
			matcher: meta.matcher as string | undefined,
			timeoutSec: meta.timeoutSec as number | undefined,
			initial: name === fsm.initial,
			edges,
		};
	});
}

/* ---- routes ---------------------------------------------------------------- */

class HttpError extends Error {
	constructor(readonly status: number, message: string) {
		super(message);
	}
}

const json = (data: unknown, status = 200): Response =>
	new Response(JSON.stringify(data), {
		status,
		headers: { "content-type": MIME[".json"] },
	});

const num = (v: unknown, fallback: number): number => {
	const n = Number(v);
	return Number.isFinite(n) ? n : fallback;
};

interface CreateBody {
	title?: string;
	amount?: number;
	autoApproveBelow?: number;
	approver?: string;
	maxNotify?: number;
	failBooking?: boolean;
	/** Append the approval *before* the instance reaches its wait point. */
	approveNow?: boolean;
}

/**
 * Creates one instance.
 *
 * The `correlationToken` is set here, at birth, rather than returned later by a handler —
 * so the instance is addressable from the moment it exists, and a signal that arrives
 * while it is still `pending` or `running` has somewhere to land. That is what makes the
 * "approve it before it waits" checkbox demonstrate deferral instead of a lost signal:
 * the correlator finds a live instance for the token, sees it is not `waiting` yet, and
 * leaves the row alone for a later tick.
 */
async function create(body: CreateBody): Promise<Record<string, unknown>> {
	if (await countLive() >= CAPS.live) {
		throw new HttpError(
			429,
			`${CAPS.live} instances are already live. Cancel a few first — ` +
				`they are rows, they do not go away on their own.`,
		);
	}

	const amount = Math.min(
		CAPS.amount,
		Math.max(0, Math.round(num(body.amount, 250))),
	);
	const autoApproveBelow = Math.min(
		CAPS.amount,
		Math.max(0, Math.round(num(body.autoApproveBelow, 100))),
	);
	const maxNotify = Math.min(
		CAPS.maxNotify,
		Math.max(1, Math.round(num(body.maxNotify, 2))),
	);

	const instance = await wf.create({
		definitionId: DEFINITION_ID,
		definitionVersion: DEFINITION_VERSION,
		correlationToken: `expense-${crypto.randomUUID().slice(0, 8)}`,
		context: {
			title: String(body.title ?? "").trim().slice(0, 120) || "Team offsite",
			amount,
			currency: "EUR",
			approver: String(body.approver ?? "").trim().slice(0, 120),
			autoApproveBelow,
			maxNotify,
		},
	});

	if (body.failBooking) armBookingFailure(instance.id);

	if (body.approveNow) {
		await wf.appendInbox({
			source: "example-ui",
			correlationToken: instance.correlation_token!,
			payload: { decision: "approve", by: "you (early)" },
		});
	}

	return { instance };
}

/** The four kinds of signal this demo can send, and what each one is for. */
async function signal(
	instance: WorkflowInstanceRow,
	kind: string,
): Promise<Record<string, unknown>> {
	if (kind === "stray") {
		// Nobody owns this token. The correlator warns, marks the row processed,
		// and moves on — an unroutable signal is an upstream bug, not something
		// to hold on to.
		const row = await wf.appendInbox({
			source: "example-ui",
			correlationToken: `nobody-${crypto.randomUUID().slice(0, 8)}`,
			payload: { decision: "approve", by: "a stranger" },
		});
		return { inbox: row };
	}

	if (!instance.correlation_token) {
		throw new HttpError(
			409,
			"This instance has no correlation token any more — a terminal instance " +
				"drops it, so nothing can be delivered to it.",
		);
	}

	// `junk` passes the token (so it finds the instance) and fails the matcher (so
	// the instance keeps waiting and the row is recorded as `signal_rejected`).
	const payload = kind === "junk"
		? { note: "not a decision at all" }
		: { decision: kind === "reject" ? "reject" : "approve", by: "you" };

	const row = await wf.appendInbox({
		source: "example-ui",
		correlationToken: instance.correlation_token,
		payload,
	});
	return { inbox: row };
}

async function api(req: Request, url: URL): Promise<Response> {
	const path = url.pathname;

	if (path === "/api/runtime") {
		if (req.method === "POST") {
			const body = await req.json().catch(() => ({}));
			ticks.auto = (body as { autoTick?: boolean }).autoTick !== false;
		}
		return json({
			tenantId: TENANT_ID,
			definitionId: DEFINITION_ID,
			definitionVersion: DEFINITION_VERSION,
			decisionTimeoutSec: DECISION_TIMEOUT_SEC,
			stalePendingSec: scheduler.stalePendingSec,
			schedulerTick: scheduler.tickExpression,
			correlatorTick: correlator.tickExpression,
			tickMs: TICK_MS,
			caps: CAPS,
			ticks,
		});
	}

	if (path === "/api/definition" && req.method === "GET") {
		return json({
			id: DEFINITION_ID,
			version: DEFINITION_VERSION,
			initial: expenseApprovalV1.fsm.initial,
			nodes: serializeGraph(),
		});
	}

	if (path === "/api/instances") {
		if (req.method === "POST") {
			const body = await req.json().catch(() => ({}));
			return json(await create(body as CreateBody));
		}
		return json({ instances: await listInstances() });
	}

	if (path === "/api/tick" && req.method === "POST") {
		const body = await req.json().catch(() => ({}));
		const what = (body as { what?: string }).what;
		const result = await tickOnce(
			what === "scheduler" || what === "correlator" ? what : "both",
		);
		return json({ ...result, ticks });
	}

	// /api/instance/:id[/signal|/cancel|/retry]
	const parts = path.split("/").filter(Boolean); // ["api","instance",id,tail?]
	if (parts[0] === "api" && parts[1] === "instance" && parts.length >= 3) {
		const id = parts[2];
		const tail = parts[3];

		if (!tail && req.method === "GET") return json(await snapshot(id));

		const instance = await wf.find(id);
		if (!instance) throw new HttpError(404, `No such instance: ${id}`);

		if (tail === "signal" && req.method === "POST") {
			const body = await req.json().catch(() => ({}));
			return json(await signal(instance, String((body as { kind?: string }).kind)));
		}

		if (tail === "cancel" && req.method === "POST") {
			const ok = await wf.cancel(id, "cancelled from the example UI");
			// `false` means it was already terminal — not an error, just late.
			return json({ cancelled: ok });
		}

		if (tail === "retry" && req.method === "POST") {
			const body = await req.json().catch(() => ({}));
			const force = (body as { force?: boolean }).force === true;
			const ok = await wf.retry(id, { force });
			return json({ retried: ok });
		}
	}

	throw new HttpError(404, `No route: ${req.method} ${path}`);
}

/* ---- static ---------------------------------------------------------------- */

async function serveStatic(url: URL): Promise<Response> {
	const rel = url.pathname === "/" ? "index.html" : url.pathname.slice(1);
	// normalize first, then confirm the result is still inside the example directory
	const file = join(STATIC_ROOT, normalize(rel));
	if (!file.startsWith(STATIC_ROOT)) return new Response("Nope", { status: 403 });
	try {
		const body = await Deno.readFile(file);
		return new Response(body, {
			headers: {
				"content-type": MIME[extname(file)] ?? "application/octet-stream",
				"cache-control": "no-cache",
			},
		});
	} catch {
		return new Response("Not found", { status: 404 });
	}
}

/* ---- boot ------------------------------------------------------------------ */

/**
 * BOTH signals, not just SIGINT. steve and cron install their own SIGTERM handlers that
 * stop the pollers and deliberately do not exit — that is the consumer's job, i.e. this.
 *
 * Nothing is lost either way, which is the point worth trying: kill this process while
 * an instance is waiting, start it again, and the instance is exactly where it was. The
 * timer is a `wake_at` column, not a `setTimeout`.
 */
let shuttingDown = false;
const shutdown = async () => {
	if (shuttingDown) return;
	shuttingDown = true;
	console.log("\nstopping — every live instance keeps its place in PG");
	await cron.stop().catch(() => {});
	await jobs.stop().catch(() => {});
	await db.end().catch(() => {});
	Deno.exit(0);
};

for (const sig of ["SIGINT", "SIGTERM"] as const) {
	Deno.addSignalListener(sig, () => void shutdown());
}

console.log(`workflow example → http://${HOSTNAME}:${PORT}`);

Deno.serve({ port: PORT, hostname: HOSTNAME }, async (req) => {
	const url = new URL(req.url);
	try {
		if (url.pathname.startsWith("/api/")) return await api(req, url);
		return await serveStatic(url);
	} catch (error) {
		const status = error instanceof HttpError ? error.status : 500;
		if (status === 500) console.error(error);
		return json({ error: (error as Error).message ?? String(error) }, status);
	}
});
