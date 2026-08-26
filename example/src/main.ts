/// <reference no-default-lib="true" />
/// <reference lib="dom" />
/// <reference lib="dom.iterable" />
/// <reference lib="esnext" />
/**
 * Example app for `@marianmeres/workflow`.
 *
 * One instance at a time, shown three ways: where its cursor sits in the graph, what its
 * execution state is beside it, and every row the engine has written about it since it
 * was born. Buttons send it signals, cancel it, retry it, and drive the scheduler and
 * correlator ticks by hand.
 *
 * The engine runs in `example/server.ts` — none of it can run here. What this file does
 * is poll `/api/instance/:id`, and every field it draws was read back out of PostgreSQL
 * rather than remembered in a variable. That is the property worth seeing: kill the
 * server while an instance is waiting, start it again, and this page carries on as if
 * nothing happened, timer included.
 *
 * Built with `@marianmeres/vanilla`: explicit reactive state (`observable`), markup in
 * `<template>`s (`fromTemplate` / `refs`), one delegated listener tree (`delegate`).
 *
 * This is browser code: the triple-slash lib references above type it against the DOM
 * (the repo's `deno.json` targets the Deno runtime for the library itself).
 *
 * Bundle with: `deno task example:build` (→ `example/dist/bundle.js`).
 */
import { createView, delegate, fromTemplate, refs } from "@marianmeres/vanilla";
import { VERSION } from "./version.generated.ts";

/* ---- config --------------------------------------------------------------- */

/** Must match the literal in the anti-FOUC inline script in index.html. */
const THEME_KEY = "workflow-example-theme";

/** Poll cadence while something is moving, and while nothing is. */
const POLL_BUSY_MS = 600;
const POLL_IDLE_MS = 2000;

/** How often the `wake_at` countdown repaints between polls. */
const CLOCK_MS = 250;

/* ---- the wire ------------------------------------------------------------- */

interface InstanceRow {
	id: string;
	definition_id: string;
	definition_version: string;
	cursor: string;
	previous_cursor: string | null;
	context: Record<string, unknown>;
	execution_state: string;
	wake_at: string | null;
	correlation_token: string | null;
	seq: number;
	created_at: string;
	updated_at: string;
}

interface HistoryRow {
	id: number;
	at: string;
	event_type: string;
	from_node: string | null;
	to_node: string | null;
	data: Record<string, unknown>;
}

interface InboxRow {
	id: string;
	received_at: string;
	source: string;
	correlation_token: string;
	payload: Record<string, unknown>;
	processed_at: string | null;
	mine: boolean;
}

interface Snapshot {
	instance: InstanceRow;
	history: HistoryRow[];
	inbox: InboxRow[];
	/** The server's clock, so the countdown does not trust the browser's. */
	now: string;
	terminal: boolean;
	error?: string;
}

interface GraphNode {
	name: string;
	kind: string;
	handler?: string;
	matcher?: string;
	timeoutSec?: number;
	initial: boolean;
	edges: { event: string; target: string; guarded: boolean }[];
}

interface Runtime {
	tenantId: string;
	definitionId: string;
	definitionVersion: string;
	decisionTimeoutSec: number;
	stalePendingSec: number;
	schedulerTick: string;
	correlatorTick: string;
	tickMs: number;
	ticks: {
		auto: boolean;
		count: number;
		woken: number;
		repoked: number;
		resolved: number;
	};
}

/* ---- theme (page-level, class-based: matches the design-tokens `.dark`) ----
 * The class is set pre-paint by the inline script in index.html; this keeps it
 * and the browser chrome color (<meta name="theme-color">) in sync afterwards. */

const prefersDark = (): boolean =>
	globalThis.matchMedia?.("(prefers-color-scheme: dark)").matches ?? false;

const applyTheme = (dark: boolean): void => {
	const root = document.documentElement;
	root.classList.toggle("dark", dark);
	const bg = getComputedStyle(root).getPropertyValue("--stuic-color-background").trim();
	if (bg) {
		document.querySelector('meta[name="theme-color"]')?.setAttribute("content", bg);
	}
};

let isDark = (() => {
	const stored = localStorage.getItem(THEME_KEY);
	return stored ? stored === "dark" : prefersDark();
})();
applyTheme(isDark);

/* ---- utils ---------------------------------------------------------------- */

const TERMINAL = new Set(["completed", "failed", "cancelled"]);

/** The one mapping that matters here: lifecycle state → how alarming it looks. */
const stateKind = (state: string): string =>
	state === "completed"
		? "ok"
		: state === "failed"
		? "bad"
		: state === "running"
		? "run"
		: state === "waiting"
		? "wait"
		: "";

const EVENT_KIND: Record<string, string> = {
	completed: "ok",
	effect_completed: "ok",
	signal_received: "ok",
	failed: "bad",
	effect_failed: "bad",
	transition_rejected: "bad",
	signal_rejected: "bad",
	cancelled: "bad",
	timeout: "warn",
	retried: "warn",
};

const time = (iso: string): string =>
	new Date(iso).toLocaleTimeString(undefined, { hour12: false });

const clip = (s: string, n: number): string => (s.length > n ? s.slice(0, n) + "…" : s);

const short = (id: string): string => id.slice(0, 8);

const json = (v: unknown, max = 120): string => {
	if (v === null || v === undefined) return "";
	const s = JSON.stringify(v);
	return s === "{}" ? "" : clip(s, max);
};

/** A `<dt>/<dd>` pair appended to a `dl.kv`. An empty value is a fact, so it is drawn. */
function kv(list: HTMLElement, k: string, v: unknown): void {
	const dt = document.createElement("dt");
	dt.textContent = k;
	const dd = document.createElement("dd");
	dd.textContent = v === null || v === undefined || v === "" ? "—" : String(v);
	list.append(dt, dd);
}

async function api<T>(path: string, init?: RequestInit): Promise<T> {
	const res = await fetch(path, init);
	const data = await res.json().catch(() => ({}));
	if (!res.ok) {
		throw new Error((data as { error?: string }).error ?? `HTTP ${res.status}`);
	}
	return data as T;
}

const post = <T>(path: string, body?: unknown): Promise<T> =>
	api<T>(path, {
		method: "POST",
		headers: { "content-type": "application/json" },
		body: JSON.stringify(body ?? {}),
	});

/* ---- view ----------------------------------------------------------------- */

const app = createView((track) => {
	const el = fromTemplate("tpl-app");
	const r = refs(el);

	let runtime: Runtime | null = null;
	let graph: GraphNode[] = [];
	/** The instance on screen. `null` before anything is selected. */
	let current: Snapshot | null = null;
	let selectedId: string | null = null;
	/** `Date.now()` minus the server's clock, so the countdown is the server's. */
	let skew = 0;
	let timer: ReturnType<typeof setTimeout> | undefined;
	let busy = false;

	/* -- the form -- */

	const val = (name: string): string => (r[name] as HTMLInputElement).value.trim();
	const int = (name: string, fallback: number): number => {
		const n = Number(val(name));
		return Number.isFinite(n) ? n : fallback;
	};
	const on = (name: string): boolean => (r[name] as HTMLInputElement).checked;

	const setDefaults = (): void => {
		(r.title as HTMLInputElement).value = "Team offsite";
		(r.amount as HTMLInputElement).value = "250";
		(r.autoApproveBelow as HTMLInputElement).value = "100";
		(r.approver as HTMLInputElement).value = "alice@example.com";
	};

	/* -- errors -- */

	const showError = (message: string): void => {
		r.errorBox.hidden = false;
		r.errorBox.textContent = message;
	};
	const clearError = (): void => {
		r.errorBox.hidden = true;
		r.errorBox.textContent = "";
	};

	/* -- the graph -- */

	/**
	 * Redraws every node, marking the one the cursor is on and the ones the history
	 * says have been visited.
	 *
	 * The nodes come from the server, serialized out of the registered definition, so
	 * this picture cannot drift from the graph the driver is actually walking.
	 */
	const renderGraph = (): void => {
		const cursor = current?.instance.cursor;
		const visited = new Set<string>();
		for (const h of current?.history ?? []) {
			if (h.from_node) visited.add(h.from_node);
			if (h.to_node) visited.add(h.to_node);
		}

		const frag = document.createDocumentFragment();
		for (const node of graph) {
			const item = fromTemplate("tpl-node");
			const q = refs(item);
			if (node.name === cursor) item.classList.add("is-current");
			else if (visited.has(node.name)) item.classList.add("is-visited");

			q.name.textContent = node.name;
			q.kind.textContent = node.kind;

			// What each kind actually costs at runtime, in one line.
			q.detail.textContent = node.kind === "effectful"
				? `handler: ${node.handler} — one steve job, instance goes running`
				: node.kind === "suspending"
				? `${node.matcher ? `matcher: ${node.matcher} · ` : ""}${
					node.timeoutSec ? `timeout: ${node.timeoutSec}s · ` : ""
				}instance goes waiting`
				: node.kind === "pure"
				? "routed inline on ENTER — no job, no pause"
				: "instance goes completed";

			if (node.name === cursor && current) {
				const state = current.instance.execution_state;
				q.here.hidden = false;
				q.here.textContent = state;
				q.here.className = `badge badge-${stateKind(state)}`;
			}

			q.edges.replaceChildren();
			for (const edge of node.edges) {
				const span = document.createElement("span");
				const em = document.createElement("em");
				em.textContent = edge.event;
				span.append(em, ` → ${edge.target}${edge.guarded ? " (guarded)" : ""}`);
				q.edges.appendChild(span);
			}
			if (node.initial) {
				const span = document.createElement("span");
				span.textContent = "· initial";
				q.edges.appendChild(span);
			}
			frag.appendChild(item);
		}
		r.graph.replaceChildren(frag);
	};

	/* -- the instance -- */

	const renderInstance = (): void => {
		const snap = current;
		const inst = snap?.instance;

		const state = inst?.execution_state ?? "no instance";
		r.stateBadge.textContent = state;
		r.stateBadge.className = `badge badge-${inst ? stateKind(state) : ""}`;
		r.cursorLabel.textContent = inst ? `cursor: ${inst.cursor}` : "";

		const terminal = !inst || TERMINAL.has(inst.execution_state);
		const signallable = !!inst && !terminal && !!inst.correlation_token;
		(r.approveBtn as HTMLButtonElement).disabled = !signallable;
		(r.rejectBtn as HTMLButtonElement).disabled = !signallable;
		(r.junkBtn as HTMLButtonElement).disabled = !signallable;
		(r.strayBtn as HTMLButtonElement).disabled = false;
		(r.cancelBtn as HTMLButtonElement).disabled = !inst || terminal;
		// `retry()` covers a failed instance, and a running one only under
		// `{ force: true }` — the operator asserting its worker is dead.
		(r.retryBtn as HTMLButtonElement).disabled = !inst ||
			(inst.execution_state !== "failed" && inst.execution_state !== "running");
		(r.retryBtn as HTMLButtonElement).title = inst?.execution_state === "running"
			? "retry(id, { force: true }) — for an instance whose worker died"
			: "retry(id) — resume a failed instance from its current cursor";

		r.kv.replaceChildren();
		r.instanceEmpty.hidden = !!inst;
		(r.kv as HTMLElement).hidden = !inst;
		if (!inst) {
			(r.context as HTMLElement).hidden = true;
			return;
		}
		kv(r.kv, "id", inst.id);
		kv(r.kv, "definition", `${inst.definition_id}@${inst.definition_version}`);
		kv(r.kv, "cursor", inst.cursor);
		kv(r.kv, "previous", inst.previous_cursor);
		kv(r.kv, "execution_state", inst.execution_state);
		kv(r.kv, "seq (fence)", inst.seq);
		kv(r.kv, "correlation_token", inst.correlation_token);
		kv(
			r.kv,
			"wake_at",
			inst.wake_at ? new Date(inst.wake_at).toLocaleString() : null,
		);
		kv(r.kv, "updated_at", new Date(inst.updated_at).toLocaleString());

		(r.context as HTMLElement).hidden = false;
		r.context.textContent = JSON.stringify(inst.context, null, 2);
	};

	/**
	 * The countdown, repainted between polls off the server's clock.
	 *
	 * `wake_at` is a column, not a `setTimeout` — nothing in any process is holding
	 * this instance. When it passes, the next scheduler tick sees a due row and pokes
	 * an advance for it, which is why the number can go slightly negative before
	 * anything happens: in manual tick mode it will sit there until you press the
	 * button.
	 */
	const renderCountdown = (): void => {
		const inst = current?.instance;
		if (!inst?.wake_at || inst.execution_state !== "waiting") {
			r.countdown.textContent = "";
			return;
		}
		const left = (Date.parse(inst.wake_at) - (Date.now() - skew)) / 1000;
		r.countdown.textContent = left > 0
			? `wakes in ${left.toFixed(1)}s`
			: `due ${Math.abs(left).toFixed(1)}s ago — waiting for a scheduler tick`;
	};

	/* -- history + inbox -- */

	const renderHistory = (rows: HistoryRow[]): void => {
		r.cHistory.textContent = String(rows.length);
		r.historyEmpty.hidden = rows.length > 0;
		const frag = document.createDocumentFragment();
		// newest first: the last thing that happened is the thing you are watching for
		for (const h of [...rows].reverse()) {
			const node = fromTemplate("tpl-history-row");
			const q = refs(node);
			q.at.textContent = time(h.at);
			q.event.textContent = h.event_type;
			q.event.className = `ev${
				EVENT_KIND[h.event_type] ? ` ev-${EVENT_KIND[h.event_type]}` : ""
			}`;
			q.nodes.textContent = h.to_node
				? `${h.from_node ?? "—"} → ${h.to_node}`
				: (h.from_node ?? "");
			q.data.textContent = json(h.data, 160);
			frag.appendChild(node);
		}
		r.historyBody.replaceChildren(frag);
	};

	const renderInbox = (rows: InboxRow[]): void => {
		r.cInbox.textContent = String(rows.length);
		r.inboxEmpty.hidden = rows.length > 0;
		const frag = document.createDocumentFragment();
		for (const row of rows) {
			const node = fromTemplate("tpl-inbox-row");
			const q = refs(node);
			if (!row.processed_at) node.classList.add("is-unprocessed");
			if (row.mine) node.classList.add("is-mine");
			q.at.textContent = time(row.received_at);
			q.token.textContent = row.correlation_token;
			q.payload.textContent = json(row.payload, 90);
			q.processed.textContent = row.processed_at
				? time(row.processed_at)
				: "unprocessed";
			frag.appendChild(node);
		}
		r.inboxBody.replaceChildren(frag);
	};

	/* -- the sidebar list -- */

	const renderInstances = (rows: InstanceRow[]): void => {
		r.instancesEmpty.hidden = rows.length > 0;
		const frag = document.createDocumentFragment();
		for (const row of rows) {
			const node = fromTemplate("tpl-instance-row");
			const q = refs(node);
			const btn = node.querySelector("button") as HTMLButtonElement;
			btn.dataset.id = row.id;
			btn.setAttribute("aria-current", String(row.id === selectedId));
			q.state.textContent = row.execution_state;
			q.state.className = `badge badge-${stateKind(row.execution_state)}`;
			q.title.textContent = String(row.context.title ?? short(row.id));
			q.meta.textContent = [
				row.cursor,
				`${row.context.amount ?? "?"} ${row.context.currency ?? ""}`.trim(),
				`seq ${row.seq}`,
			].join(" · ");
			frag.appendChild(node);
		}
		r.instances.replaceChildren(frag);
	};

	/* -- ticks -- */

	const renderRuntime = (rt: Runtime): void => {
		runtime = rt;
		r.cTicks.textContent = String(rt.ticks.count);
		r.cWoken.textContent = String(rt.ticks.woken);
		r.cRepoked.textContent = String(rt.ticks.repoked);
		r.cResolved.textContent = String(rt.ticks.resolved);
		(r.tickSchedulerBtn as HTMLButtonElement).disabled = rt.ticks.auto;
		(r.tickCorrelatorBtn as HTMLButtonElement).disabled = rt.ticks.auto;
		(r.tickAuto as HTMLInputElement).checked = rt.ticks.auto;
		(r.tickManual as HTMLInputElement).checked = !rt.ticks.auto;
	};

	/* -- polling -- */

	const cadence = (): number =>
		current && !TERMINAL.has(current.instance.execution_state)
			? POLL_BUSY_MS
			: POLL_IDLE_MS;

	const poll = async (): Promise<void> => {
		try {
			const [rt, list] = await Promise.all([
				api<Runtime>("/api/runtime"),
				api<{ instances: InstanceRow[] }>("/api/instances"),
			]);
			renderRuntime(rt);
			renderInstances(list.instances);

			if (selectedId) {
				const snap = await api<Snapshot>(`/api/instance/${selectedId}`);
				skew = Date.now() - Date.parse(snap.now);
				current = snap;
				renderInstance();
				renderCountdown();
				renderGraph();
				renderHistory(snap.history);
				renderInbox(snap.inbox);
			}
		} catch (e) {
			showError(`Polling: ${e instanceof Error ? e.message : e}`);
		} finally {
			timer = setTimeout(() => void poll(), cadence());
		}
	};

	/** Skip the wait: something just changed, so look now rather than in 600 ms. */
	const refresh = (): void => {
		clearTimeout(timer);
		void poll();
	};

	/* -- actions -- */

	const select = (id: string): void => {
		selectedId = id;
		current = null;
		clearError();
		refresh();
	};

	const create = async (): Promise<void> => {
		if (busy) return;
		busy = true;
		(r.createBtn as HTMLButtonElement).disabled = true;
		clearError();
		try {
			const data = await post<{ instance: InstanceRow }>("/api/instances", {
				title: val("title"),
				amount: int("amount", 250),
				autoApproveBelow: int("autoApproveBelow", 100),
				approver: val("approver"),
				maxNotify: int("maxNotify", 2),
				failBooking: on("failBooking"),
				approveNow: on("approveNow"),
			});
			select(data.instance.id);
		} catch (e) {
			showError(String(e instanceof Error ? e.message : e));
		} finally {
			busy = false;
			(r.createBtn as HTMLButtonElement).disabled = false;
		}
	};

	const act = async (fn: () => Promise<unknown>): Promise<void> => {
		clearError();
		try {
			await fn();
		} catch (e) {
			showError(String(e instanceof Error ? e.message : e));
		}
		refresh();
	};

	const selectTab = (name: string): void => {
		for (const btn of (r.tabs as HTMLElement).querySelectorAll("button")) {
			btn.setAttribute("aria-selected", String(btn.dataset.tab === name));
		}
		r.paneHistory.hidden = name !== "history";
		r.paneInbox.hidden = name !== "inbox";
	};

	const setTickMode = (auto: boolean): void =>
		void act(() => post("/api/runtime", { autoTick: auto }));

	/* -- wiring -- */

	// One delegated listener tree for the whole view (events bubble to `el`).
	track(delegate(el, {
		submit: (e) => {
			e.preventDefault();
			void create();
		},
		toggleTheme: () => {
			isDark = !isDark;
			applyTheme(isDark);
			localStorage.setItem(THEME_KEY, isDark ? "dark" : "light");
		},
		tab: (_e, target) => selectTab(target.dataset.tab!),
		selectInstance: (_e, target) => select(target.dataset.id!),
		signal: (_e, target) => {
			if (!selectedId) return;
			void act(() =>
				post(`/api/instance/${selectedId}/signal`, {
					kind: target.dataset.kind,
				})
			);
		},
		cancel: () => {
			if (selectedId) void act(() => post(`/api/instance/${selectedId}/cancel`));
		},
		retry: () => {
			if (!selectedId) return;
			const force = current?.instance.execution_state === "running";
			void act(() => post(`/api/instance/${selectedId}/retry`, { force }));
		},
		tickScheduler: () => void act(() => post("/api/tick", { what: "scheduler" })),
		tickCorrelator: () => void act(() => post("/api/tick", { what: "correlator" })),
	}));

	// the radios have nothing to delegate to, so they get plain listeners
	const autoChanged = () => setTickMode((r.tickAuto as HTMLInputElement).checked);
	for (const node of [r.tickAuto, r.tickManual]) {
		node.addEventListener("change", autoChanged);
		track(() => node.removeEventListener("change", autoChanged));
	}

	const clock = setInterval(renderCountdown, CLOCK_MS);
	track(() => clearInterval(clock));
	track(() => clearTimeout(timer));

	/* -- boot -- */

	(r.form as HTMLFormElement).setAttribute("novalidate", "");
	setDefaults();
	selectTab("history");
	renderInstance();
	r.version.textContent = `· v${VERSION}`;

	void (async () => {
		try {
			const def = await api<{ id: string; version: string; nodes: GraphNode[] }>(
				"/api/definition",
			);
			graph = def.nodes;
			r.graphTitle.textContent = `${def.id}@${def.version}`;
			renderGraph();
		} catch (e) {
			showError(`Could not load the definition: ${e}`);
		}
		const rt = runtime ?? await api<Runtime>("/api/runtime").catch(() => null);
		if (rt) {
			r.tickHint.textContent =
				`A cron expression cannot go below a minute, so the ticks here are also driven ` +
				`by hand every ${rt.tickMs} ms — tickOnce() is public API for exactly that. ` +
				`The registered cron jobs (${rt.schedulerTick}) are still running too, and a ` +
				`pending instance stranded for ${rt.stalePendingSec}s gets re-poked. Switch to ` +
				`manual and nothing moves until you press a button.`;
			r.graphHint.textContent =
				`The highlighted node is the cursor; the badge on it is the execution state. ` +
				`An instance can be at "await_decision" and "waiting" at once — one says where, ` +
				`the other says what it is doing there. Guarded edges are picked by code in ` +
				`example/workflow.ts, which is why the guard itself is not drawn.`;
		}
		void poll();
	})();

	return { el };
});

document.getElementById("app")!.appendChild(app.el!);
