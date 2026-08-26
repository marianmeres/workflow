# Implementation Progress — workflow analysis roadmap

<!-- tracker: v1 -->

Living tracker for acting on [`00-overview-and-roadmap.md`](./00-overview-and-roadmap.md).
A fresh conversation should read this file first, then the relevant `NN-*.md` section.

**Status legend:** ⬜ ready · 🚧 in progress · ⏸️ blocked/awaiting decision · 🔒 human-only · ✅ done · ⏭️ deferred

> Convention: one branch per sprint, one commit per task. Each task resolves its source doc's
> "Open questions" first (record in the Decisions log), then implement → verify → tick here.
> The tasks in the first sprint change durability semantics — every one lands with its test
> (the source section's **Done when** names it).

## First sprint (durability + signal delivery, before first production use)

Branch: `analysis/first-sprint`
Options: --max-tasks 20 --budget 40 --model claude-opus-5 --effort xhigh
Verify: deno task test
Verify: deno check src/mod.ts tests/*.ts

| Status | ID  | Deps            | Task                                                                                                                                      | Source                           | Commit |
| ------ | --- | --------------- | ----------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------- | ------ |
| ✅     | T01 | —               | Fencing token: migration 1.2.0 `seq`; `kind` + `expected_seq` on advances, `seq` on effects; fenced advance/effect/fail; `create()` in tx | [01](./01-durability.md) #1      | c7e5b22 |
| ✅     | T02 | T01             | Correlator: defer early signals, deliver only to `MATCHED`-accepting waits, processed-in-advance, per-tick dedupe, matcher-throw retry    | [02](./02-correlation.md) #1, #2 | 27c0f60 |
| ✅     | T03 | T01 T02         | Scheduler: read-only TIMEOUT pokes + stale-pending re-poke; delete `claimDueWakeUps`; pending partial index                               | [01](./01-durability.md) #2      | 53a3323 |
| ✅     | T04 | T01             | Observe `workflow.advance` failures; bounded re-dispatch of `expired` advance/effect jobs; `advanceMaxAttempts`, `redispatchLimit`        | [01](./01-durability.md) #3      | 6de8396 |
| ✅     | T05 | T01 T02 T03 T04 | Docs pass: correct durability claims, document `autoCleanup`, fence/poke model, signal semantics, noop-handler hazards                    | [04](./04-docs-tests-ops.md) #1  | a7f96c4 |

`Verify:` deliberately omitted `deno lint` / `deno fmt --check` — both were red at the
baseline (T15). The second sprint adds them, with T15 as its first row.

Before starting the driver: commit `docs/analysis/` on `master` (the driver refuses a
dirty tree and only switches to the sprint branch from a default branch), then run
`sprint docs/analysis` from the repository root. `deno task test` needs the `TEST_PG_*`
env from `.env`.

## Second sprint (hardening, API surface, release)

Branch: `analysis/first-sprint`
Options: --max-tasks 12 --budget 90 --task-budget 12 --max-turns 120 --run-deadline 21600 --model claude-opus-5 --effort xhigh
Verify: deno lint
Verify: deno fmt --check

| Status | ID  | Deps                                    | Task                                                                                               | Source                           | Commit |
| ------ | --- | --------------------------------------- | -------------------------------------------------------------------------------------------------- | -------------------------------- | ------ |
| ✅     | T15 | —                                       | `deno fmt` + `deno lint` clean; `deno task check`; `fmt.exclude` for this tracker                   | [04](./04-docs-tests-ops.md) #4  | —      |
| ✅     | T13 | —                                       | `tests/driver.test.ts`: pure routing, hop guard, rejections, unknown definition; `tests/_util.ts`   | [04](./04-docs-tests-ops.md) #3  | —      |
| ✅     | T12 | —                                       | Migration 1.1.0 guard: `table_schema = current_schema()`                                           | [04](./04-docs-tests-ops.md) #2  | —      |
| ✅     | T06 | —                                       | Validator: reject definitions that must fail at runtime (ENTER/TIMEOUT/MATCHED/empty `on`)         | [03](./03-definition-and-api.md) #1 | —   |
| ✅     | T09 | —                                       | `create()` seeds context from `def.fsm.context`, input overlays                                    | [03](./03-definition-and-api.md) #2 | —   |
| ⬜     | T10 | —                                       | Guard: one `Workflow` per `Jobs` (WeakMap + throw) and `detach()`                                  | [03](./03-definition-and-api.md) #3 | —   |
| ⬜     | T08 | T01                                     | `HandlerResult.correlationToken` applied at the settle-point write                                 | [02](./02-correlation.md) #3     | —      |
| ⬜     | T17 | T08                                     | Opt-in `HandlerResult.context` shallow patch before the outcome transition                         | [03](./03-definition-and-api.md) #4 | —   |
| ⬜     | T07 | T01                                     | `cancel(id, reason?)` / `retry(id, { force? })` on `Workflow`; `RETRIED` history event             | [01](./01-durability.md) #4      | —      |
| ⬜     | T14 | —                                       | Typed `meta`: `WorkflowStateConfig` / `WorkflowFSMConfig`; drop the `as NodeMeta` casts            | [03](./03-definition-and-api.md) #5 | —   |
| 🔒     | T16 | T06 T07 T08 T09 T10 T12 T13 T14 T15 T17 | Release 2.1.0 to JSR + npm (`deno task rpm`); smoke-run the npm build under Node first             | [04](./04-docs-tests-ops.md) #1  | —      |

The whole backlog, promoted in **run order rather than rank order**: T15 first because it makes
the two new `Verify:` commands green, T13 second because every later test file wants
`tests/_util.ts` and a driver fixture, T14 last because it retypes what the fixtures written
before it compile against. The rest keep their relative rank.

`Verify:` accumulates, so the two lines above join the first sprint's `deno task test` and
`deno check` — **four commands after every task**, T15 included (which is why it runs first).

T16 sits in the sprint table rather than the backlog on purpose: a run that finishes every
runnable row then exits `2` — "nothing the driver can run, 1 🔒 — yours to do" — instead of `0`,
so "sprint complete" and "the release is waiting on you" do not look the same
(`sprint/SPEC.md` §3.3). Flip it to ✅ after publishing and the next run exits `0`.

Run it with `sprint docs/analysis` from the repository root. The declared branch is already
checked out, so the driver switches nothing. `deno task test` needs the `TEST_PG_*` env from
`.env`.

## Backlog (ranked, post-sprint)

| Status | ID  | Rank | Deps | Task                                                                                          | Source                              |
| ------ | --- | ---- | ---- | --------------------------------------------------------------------------------------------- | ----------------------------------- |
| ⏭️     | T11 | 13   | T10  | Tenant-per-call runtime: `tenantId` on `create`/`appendInbox`/`find`, `"*"` ticks              | [03](./03-definition-and-api.md) #3 |

T11 is the only row left here — deferred by decision, guard only (T10) for now; the revisit
trigger is in the Decisions log. Everything else was promoted into the second sprint above.

## Decisions log

- **2026-08-26** — Backlog promoted into a second sprint (owner interview):
  - **Scope:** all ten ⬜ backlog rows into one section, on the same branch
    `analysis/first-sprint` — no merge to `master` in between. T16 moves into the sprint
    table as 🔒 so a completed run exits `2` ("the release is yours"), not `0`. T11 stays
    the only backlog row.
  - **Run order is not rank order.** T15 first (it makes the two new `Verify:` commands
    green), T13 second (`tests/_util.ts` and a driver fixture for every later test), T14
    last (it retypes what earlier fixtures compile against). The rest keep their rank.
  - **`Verify:` gains `deno lint` and `deno fmt --check`** — approved now that T15 leads.
    They accumulate onto the first sprint's two commands: four run after every task.
  - **T15 additionally excludes `docs/analysis/PROGRESS.md` from `deno fmt`.** —
    _Rationale:_ `setCommitCell` writes a 7-character hash into a `Commit` cell sized for
    `—` without re-padding the header, so the driver's own end-of-run bookkeeping commit
    leaves this file failing `deno fmt --check` — it does today, at `253032b`. Without the
    exclusion the first task of any _later_ run fails a verification that has nothing to do
    with it. This does **not** reverse the "no `fmt.exclude`" decision for
    `README`/`API`/`AGENTS`; those are still reflowed.
  - **T14 ships as specced, under 2.1.0.** `meta` stays required on `WorkflowStateConfig`.
    Only a definition that already threw in `validateDefinition` loses compilation, so no
    working consumer breaks and a minor bump is honest.
  - **T12 covers 1.1.0 only.** The 1.2.0 migration T01 shipped uses
    `ADD COLUMN IF NOT EXISTS` / `CREATE INDEX IF NOT EXISTS` and reads no
    `information_schema`, so it has no schema-blind guard to fix.
  - **T13 case (f) is already covered.** `tests/correlator.test.ts` (T02) asserts the
    `signal_rejected` path. T13 is cases (a)–(e) plus moving `waitUntil` — now duplicated in
    **five** test files, not the three the source doc counted — into `tests/_util.ts`.
  - **Ceilings raised from sprint 1's measurements:** the five tasks cost $3.66–$5.85
    (mean $4.71, $23.43 total) and ran 8–13 min each, and T05 used 75 of its 80 turns. Hence
    `--budget 90 --task-budget 12 --max-turns 120 --run-deadline 21600`: ten tasks ≈ $47 and
    ≈ 2 h, with the turn cap no longer one refusal away and the whole-run deadline able to
    fit the tail.

- **2026-08-26** — Owner interview: every open question closed; the sprint is cleared for
  unattended EXECUTE via the sprint driver.
  - **T11 tenant-per-call runtime → ⏭️ deferred.** Guard only (T10) for now. —
    _Rationale:_ same-process multi-tenant is not a current deployment, and T11 is purely
    additive later. _Revisit trigger:_ a real need to serve several `tenant_id`s from one
    `Workflow`/`Jobs` in one process.
  - **T17 opt-in `HandlerResult.context` patch → ⬜ approved.** Shallow-merged into the
    instance context before the outcome transition; `data` stays the fsm payload; no
    automatic merge of `data`. — _Rationale:_ removes the per-edge `action` boilerplate
    while keeping the persisted shape an explicit per-handler choice.
  - **T04 `expired` policy: bounded re-dispatch** of the identical payload, for advance and
    effect jobs alike, `redispatchLimit = 3`, then fail the instance. — _Rationale:_ the
    fence makes a zombie's late completion a no-op and handlers are already required to be
    idempotent, so re-dispatch turns a crash into recovery.
  - **T15 → ⬜ approved: let `deno fmt` reflow the markdown** (README, API, AGENTS); no
    `fmt.exclude`.
  - **Defaults accepted:** fence column `seq` (`integer NOT NULL DEFAULT 0`);
    `advanceMaxAttempts = 10`; `redispatchLimit = 3`; `stalePendingSec = 300` with the
    re-poke on by default (`0` disables). All but the column name are runtime options.
  - **Inbox rows whose token no live instance owns** (unknown token, or the instance is
    terminal) **are marked processed on the first tick** with a warn log — plus a
    `signal_rejected` history row when a terminal instance exists. No grace period. —
    _Rationale:_ a token no live instance owns is an upstream bug, not something to wait on.
  - **Validator (T06): a `terminal` state with a non-empty `on` throws.**
  - **Execution: unattended via the sprint driver.** One commit per task on
    `analysis/first-sprint`, pre-authorized; the driver records the hashes. The owner
    commits `docs/analysis/` first and runs `sprint docs/analysis` from the repo root.
- **2026-08-26** — Plan-author defaults confirmed as written in the interview above:
  an advance payload without `expected_seq` is treated as unfenced (2.0.x jobs still queued
  at upgrade) — a transitional shim removed together with `payloadTenantId` (_Rationale:_
  additive migration, zero downtime, same policy as the 1.1.0 rename); ticks are read-only
  pokes and the advance is the only writer of instance rows, stale pokes debug-logged and
  never written to history (_Rationale:_ every crash window becomes "next tick pokes
  again"; history stays an event log); T03 is sequenced after T02 (_Rationale:_ the
  stale-`pending` re-poke is only safe once `create()` is the sole producer of `pending`);
  early signals — instance live but not `waiting`, or node without a `MATCHED` edge — stay
  unprocessed and are retried each tick, and a throwing matcher leaves its row for the next
  tick (_Rationale:_ losing a signal is worse than re-examining a row); `failed` advance
  jobs fail the instance with the last attempt's error message; `Verify:` excludes lint/fmt
  until T15 lands (_Rationale:_ a red baseline would refuse the sprint, `sprint/SPEC.md`
  §3.2).

## How to resume (for a fresh conversation)

1. Read this file + `00-overview-and-roadmap.md`.
2. Pick the first ⬜ task whose `Deps` are all ✅ (or the `T##` you were given); open its
   source doc section for the detail and its **Done when** criterion. Check the finding
   still holds against the current code — the plan was verified at `02a7635`.
3. Every "Open questions" item is already resolved in the Decisions log — do not re-ask. If
   the task's premise no longer holds against the current code, return `blocked` rather
   than building something plausible in its place.
4. Implement (with the test the **Done when** names) → run the `Verify:` commands → flip
   the row to ✅ here → one commit per task on the sprint branch (pre-authorized; leave the
   `Commit` cell as `—`, the driver fills it in).
