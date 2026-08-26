# The interactive example

One expense-approval workflow, one instance at a time, and every button needed to push it
down each of its branches: approve it, reject it, let its timer run out, hand it a signal
its matcher declines, break the effect that books it, cancel it, retry it. The page draws
the graph with the cursor on it, the execution state beside the cursor, the countdown to
`wake_at`, and every row the engine wrote about the instance since it was born.

```bash
createdb example_workflow    # or point EXAMPLE_PG_* at a database you already have
deno task example            # → http://127.0.0.1:8000
```

The bundle is committed, so that is all you need. If you change `example/src/main.ts`:

```bash
deno task example:build            # one-shot bundle → example/dist/bundle.js
deno task example:dev              # the same, in watch mode (run the server in another shell)
deno task example:theme mauveTeal  # regenerate theme.css from another bundled palette
```

`EXAMPLE_PG_*` in `.env` points at the database (see [`.env.example`](../.env.example)).
Give it one of its own: the server runs `createMigrate(pool).up("latest")` on boot and
creates the three `__workflow_*` tables, plus steve's `__job*` and cron's `__cron*` — the
workflow tables have fixed names, so there is no prefix to keep them out of the way.

## Why a server

Nothing in this package can run in a browser. It is PostgreSQL, a job queue and a cron
ticker, and the whole premise is that an instance outlives the process that started it,
never mind the page. So the browser holds the buttons and
[`server.ts`](./server.ts) does everything real.

Which makes the most instructive thing you can do here a two-line experiment: with an
instance sitting in `waiting`, press <kbd>Ctrl-C</kbd> on the server and start it again.
The page reconnects and nothing has changed — same cursor, same countdown, still counting
down. The timer was never a `setTimeout`. It is a `wake_at` column, and the thing that
fires it is a query.

## What you are looking at

**The highlighted node is the cursor. The badge on it is the execution state.** Those are
two different questions and the engine is built on never confusing them: an instance is at
`await_decision` (cursor) _and_ `waiting` (execution state), and it would be at
`await_decision` and `running` if something were mid-effect there. The sidebar list shows
the same pair per instance.

The graph is not drawn from a copy of the definition — the server serializes the
registered `WorkflowDefinition` and sends it, so the picture cannot drift from the graph
the driver is actually walking. Guards do not survive that trip (they are functions), so a
guarded edge is drawn as one and the guard itself lives in
[`workflow.ts`](./workflow.ts).

## Things to try

Each of these is one mechanism, and the History tab is where you watch it land.

| Do this                                           | What it demonstrates                                                                                                                                                                                                             |
| ------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Amount **below** the auto-approve limit           | A `pure` node routing inline. `triage` never appears as a state anyone sees — no job, no pause, no row of its own; the first advance walks it and settles at `book`.                                                             |
| Amount above it, then **Approve**                 | The full loop: effect job → `waiting` → an inbox row → the correlator's matcher → `MATCHED` → a pure decision node → the next effect.                                                                                            |
| **Reject**                                        | Same signal, different payload. The graph branches in `decide`, on context the fsm action wrote out of the signal — the driver never merges a payload into the context by itself.                                                |
| Do nothing for 20 seconds                         | The scheduler wakes it: `TIMEOUT` → `escalate` → a second notification → a fresh `wake_at`. Watch `notified` climb in the context, and the guard end the loop when it hits `maxNotify`.                                          |
| **Signal the matcher declines**                   | The token found the instance; the matcher said no. `signal_rejected` in the history, the inbox row consumed, the instance still waiting. The token is the index; the matcher is the gate.                                        |
| **Approve it immediately** (the checkbox)         | Deferral. The signal is appended while the instance is still `pending`, so the correlator finds a live instance that is not `waiting` yet and leaves the row alone — highlighted in the Inbox tab — until the wait point exists. |
| **Signal for an unknown token**                   | No live instance owns it. The correlator warns, marks the row processed and moves on: an unroutable signal is an upstream bug, not something to hold on to.                                                                      |
| **Make `bookExpense` throw once**, then **Retry** | A throw is not an outcome. The effect job fails, the instance goes `failed` at `book`, and `retry()` resumes it from that cursor — re-running that node only, with nothing replayed.                                             |
| Empty **approver**                                | The other kind of failure: `sendApprovalRequest` returns `UNDELIVERABLE`, the graph routes it to a terminal node, and the instance is `completed`. Nothing failed. See below.                                                    |
| **Cancel** something that is waiting              | The row goes `cancelled` and loses both its `wake_at` and its correlation token, so no timer and no signal can reach it again — and the `seq` bump makes every job already in flight for it stale.                               |
| Switch ticks to **manual**                        | Nothing moves until you press a button, which is the poke model made visible: send an approval and watch the inbox row sit there unprocessed until you tick the correlator.                                                      |
| Tick **twice** in a row, fast                     | Nothing happens the second time. A poke that arrives twice is fenced out by `seq`; a poke lost to a crash costs nothing, because whatever made the row look actionable is still true next tick.                                  |

## Two kinds of failure, on purpose

The demo workflow can fail in two ways, and they are not the same thing:

- **An expected failure is an outcome.** No approver → the handler returns
  `UNDELIVERABLE`, the transition table has an edge for it, and the instance finishes at
  `_undeliverable` with `execution_state: completed`. The workflow ended somewhere else,
  that is all.
- **An unexpected failure is a throw.** The armed `bookExpense` throws, steve's attempts
  run out, and the instance is marked `failed` with the error on the `effect_failed`
  history row. It needs an operator — the Retry button — to move again.

Getting that distinction right in your own graphs is most of what makes them readable
later. If a case is worth a branch, give it an outcome label.

## The one demo-shaped lie

The scheduler and correlator are registered on a real `Cron` at `* * * * *`, exactly as
the README wires them, and those ticks really are running. But a cron expression cannot go
below a minute, and a demo where a timeout takes up to sixty seconds to be noticed teaches
nothing. So the server **also** calls `scheduler.tickOnce()` and `correlator.tickOnce()`
on a 500 ms interval — public API, documented for "testing and on-demand wakes" — and the
Ticks panel lets you switch that off and drive them by hand.

That is safe for the same reason duplicate pokes are safe: a tick is read-only against
`__workflow_instances`. It selects what looks actionable and enqueues an advance; the
advance re-checks the precondition under a row lock and is the only thing that writes.

Two other values are demo-shaped, and both are commented where they are set:

- `effectMaxAttempts: 1` — so a throwing handler surfaces as a `failed` instance
  immediately. The default of 3 is the better production setting, and it would have
  retried the simulated transient fault into success with nothing to see.
- `stalePendingSec: 30` (default 300) — the scan that re-pokes instances stranded in
  `pending`, short enough to witness.

## What the example does that the package does not offer

Worth knowing, because it is the seam between "framework" and "admin surface":

- **Listing instances is raw SQL.** The package exposes `find(id)` and `getHistory(id)` —
  what a runtime needs. Anything that lists, filters or counts is a query you write, and
  [`server.ts`](./server.ts) has the smallest version of one.
- **The inbox is read with raw SQL too**, for the same reason.
- **The 20-second deadline is in the definition, not the instance.** `wake_at` is computed
  from `meta.timeoutSec`, and there is no per-instance override — a deadline that varies
  per expense would be a different definition version, or a different node.

## Routes

| Route                           | What it does                                                     |
| ------------------------------- | ---------------------------------------------------------------- |
| `GET /`                         | the app (`index.html` + `dist/bundle.js` + the two stylesheets)  |
| `GET /api/runtime`              | what is running, tick counters, the tick mode                    |
| `POST /api/runtime`             | `{ autoTick }` — flip the 500 ms ticker off and on               |
| `GET /api/definition`           | the graph, serialized out of the registered definition           |
| `GET /api/instances`            | recent instances                                                 |
| `POST /api/instances`           | create one (every number clamped server-side)                    |
| `GET /api/instance/:id`         | the poll — the row, its history, the inbox, and the server clock |
| `POST /api/instance/:id/signal` | `{ kind: approve \| reject \| junk \| stray }` → `appendInbox`   |
| `POST /api/instance/:id/cancel` | `wf.cancel(id, reason)`                                          |
| `POST /api/instance/:id/retry`  | `wf.retry(id, { force })`                                        |
| `POST /api/tick`                | `{ what: scheduler \| correlator \| both }` → one `tickOnce()`   |

The poll re-sends the whole history every time rather than paging it: one instance's
history is bounded by its own graph, and a cursor would be more code than the thing it
saves.

> ⚠️ It binds `127.0.0.1` unless you set `EXAMPLE_HOST`. It touches nothing but its own
> database, but it will happily create instances for anyone who can reach it.

## How it is built

- [`@marianmeres/vanilla`](https://jsr.io/@marianmeres/vanilla) — markup in `<template>`s
  (`fromTemplate` / `refs`), one delegated listener tree (`delegate`).
- [`@marianmeres/design-tokens`](https://jsr.io/@marianmeres/design-tokens) — `theme.css`
  is generated (`deno task example:theme`) with the Bootstrap Reboot bridge that
  `reboot.css` consumes. Light/dark follows `:root.dark`.
- [`@marianmeres/deno-build`](https://jsr.io/@marianmeres/deno-build) — bundles
  `src/main.ts` into `dist/bundle.js`; no node_modules, no build config.

`src/version.generated.ts` is generated too (gitignored) — `deno task example:build`
writes it before bundling.

## The files

| File                           | What is in it                                                                                     |
| ------------------------------ | ------------------------------------------------------------------------------------------------- |
| [`workflow.ts`](./workflow.ts) | the userland half: one definition, two handlers, one matcher. Read this one first.                |
| [`server.ts`](./server.ts)     | the runtime wiring (pool → migrate → Jobs + Cron → Workflow → scheduler + correlator) and the API |
| [`src/main.ts`](./src/main.ts) | the page: polls, draws, and holds no state the database does not                                  |
| [`index.html`](./index.html)   | the shell, the styles, and the `<template>`s                                                      |
