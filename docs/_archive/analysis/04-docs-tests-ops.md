<!--
GENERATED ANALYSIS — @marianmeres/workflow (docs accuracy, tests, ops hygiene)
Produced 2026-08-26 by an inline single-agent review: full read of src/, tests/, docs, and the
resolved sources of @marianmeres/fsm 3.1.0, steve 3.0.0, cron 3.2.0 → self-verification pass
re-opening every cited line. Claims verified against the codebase at commit 02a7635.
Planning artifact; no code was changed.
-->

# Docs Accuracy, Test Coverage & Repo Hygiene

> The documentation is unusually complete for a v2 package — `AGENTS.md`'s conventions,
> failure-mode table and glossary are exactly what a consumer (or an agent) needs. Three
> sentences in it, though, promise durability the code does not deliver, and they are
> precisely the sentences a consumer reads before deciding _not_ to add their own safety
> nets. Tests cover the happy, timeout and handler-failure paths and the schema migration,
> but not the driver's pure-node loop, any rejection path, or any duplicate-delivery path.
> `deno lint` and `deno fmt --check` are red. One migration guard is schema-blind.

> The single most important takeaway: fix the three over-claims _together with_ the
> behavior changes in `01-durability.md` — the docs should describe what the driver
> actually guarantees after the sprint, and `autoCleanup`, which today is never mentioned,
> is the switch that makes crash recovery exist at all.

> Headline recommendation: one docs pass at the end of the first sprint (README, AGENTS,
> API); a `tests/driver.test.ts` for the untested driver branches; the one-line
> `table_schema` fix in migration 1.1.0; and a `deno task check` that makes lint/fmt part
> of the routine.

## Summary of recommendations

| # | Recommendation                                                                                      | Value | Effort | Risk |
| - | --------------------------------------------------------------------------------------------------- | ----- | ------ | ---- |
| 1 | Correct the three durability over-claims; document `autoCleanup`; document the noop-handler hazards | high  | S      | low  |
| 2 | Migration 1.1.0 column guard ignores `table_schema`                                                 | med   | S      | low  |
| 3 | Driver test coverage: pure routing, hop guard, rejection paths, matcher rejection                   | med   | M      | low  |
| 4 | `deno fmt` / `deno lint` clean, plus a `check` task                                                 | low   | S      | low  |

## Findings & recommendations (detailed)

### 1. Three durability claims are wrong, and the switch that enables recovery is undocumented

- **Problem / observation.**
  - `README.md:195` — "The advance step is a single PG transaction: cursor /
    execution_state / wake_at / history are all written together with the effect-job
    insert." The effect-job insert goes through `jobs.create` on steve's own connection
    (`workflow.ts:159–168` ignores its `_client` argument; `_create.ts:21,43`), outside the
    advance transaction. Enqueue precedes commit (`driver.ts:220–224`), so the practical
    consequence is bounded — a job can exist for a transaction that rolled back — but the
    sentence is false as written, and the fix in 01 #1 makes the true story simple to
    state ("at-least-once, fenced").
  - `README.md:196` — "steve auto-cleanup reaps it after ~5min and retries" — and
    `AGENTS.md:231` — "Steve retries per `max_attempts`". Steve's reaper marks jobs
    `expired`, terminal, no retry (`_mark-expired.ts:6–8`); the workflow then marks the
    instance `failed` (`workflow.ts:133–143`). And the reaper only runs with
    `autoCleanup: true` (`jobs.ts:451–454`): `grep -rn autoCleanup README.md AGENTS.md
    API.md` → nothing. The README example constructs `new Jobs({ db: pool })`
    (`README.md:104`) — a reader who copies it gets a system where a worker crash strands
    the instance in `running` forever.
  - `AGENTS.md:232` — "Lost advance after effect … handler runs again; advance enqueued.
    Handlers must be idempotent." Incomplete: with the current driver the _second_ advance
    is rejected by the FSM and fails the instance (01 #1 (b)).
  - Missing: the cron-side noop hazard. A `Cron` started in a process that never called
    `scheduler.register()` still claims the persisted `workflow.scheduler.<tenant>` row,
    runs a noop with a warning (`cron.ts:472`, `:487–492`) and advances `next_run_at`.
    `AGENTS.md:192` covers the equivalent steve hazard; the README mentions neither.

- **Evidence.** `README.md:193–197`:
  ```
  - The advance step is a single PG transaction: cursor / execution_state / wake_at / history are all written together with the effect-job insert.
  - Steve retries effect handlers per `effectMaxAttempts`. A worker crash mid-effect leaves the job in `running`; steve auto-cleanup reaps it after `~5min` and retries.
  ```

- **Proposed change.** After T01–T04 land, rewrite: README "Persistence Guarantees &
  Idempotency" and "Failure Modes"; AGENTS "Failure Modes & Recovery" and "Runtime
  Ownership"; API `AdvanceJobPayload` / `EffectJobPayload`, the new options
  (`advanceMaxAttempts`, `redispatchLimit`, `stalePendingSec`), `cancel`/`retry` when they
  land, and the correlator's "Per-tick behavior" (`API.md:173–181`) for the deferral
  semantics of 02 #1. Add `autoCleanup: true` to the README example with one sentence on
  why. Add a short "Signals: early, late, deferred" section. Follow the
  `HUMAN_DOCUMENTATION_GUIDE.md` / `AGENT_DOCUMENTATION_GUIDE.md` conventions from
  mm-local-docs.

- **Done when** — `grep -n "written together with the effect-job insert\|reaps it after
  ~5min and retries\|Steve retries per" README.md AGENTS.md` returns nothing;
  `grep -c autoCleanup README.md AGENTS.md` ≥ 1 in each; `API.md` documents
  `expected_seq`, `kind`, `seq` and every new option shipped in the sprint.

- **Affected files.** `README.md`, `AGENTS.md`, `API.md`, `src/mod.ts` (module doc
  example).

- **Effort / Value / Risk.** S / high / low. Sequenced last in the sprint so it describes
  shipped behavior, not intent.

### 2. Migration 1.1.0's column guard is schema-blind

- **Problem / observation.** `renameColumn()` guards the `RENAME COLUMN` on
  `information_schema.columns WHERE table_name = '…' AND column_name = '…'`
  (`1_1_0.ts:16–18`) with no `table_schema` predicate. In a database with several schemas
  holding the same tables — per-tenant or per-app schemas selected via `search_path`, a
  common Postgres layout — a `project_id` found in _another_ schema's `__workflow_instances`,
  or a `tenant_id` already present there, flips the guard: the rename is skipped in the
  current schema (every query then fails on `tenant_id`), or attempted where it already
  happened. The test suite runs in one schema and cannot observe it.

- **Evidence.** `src/migrations/1_1_0.ts:16–18`:
  ```ts
  const col = (c: string) =>
  	`EXISTS (SELECT 1 FROM information_schema.columns
  	          WHERE table_name = '${table}' AND column_name = '${c}')`;
  ```

- **Proposed change.** `AND table_schema = current_schema()` in the `EXISTS` (or
  `= ANY(current_schemas(false))`). The rest of the DDL is unqualified and follows
  `search_path` consistently — that is the right behavior; only the guard is
  inconsistent with it. Apply the same discipline to the `1.2.0` migration from 01 #1.

- **Done when** — `renameColumn()`'s SQL contains `table_schema = current_schema()`;
  `tests/migrations.test.ts` passes; optionally a test creates
  `other.__workflow_instances (tenant_id text)` first and asserts the rename in `public`
  still happens.

- **Affected files.** `src/migrations/1_1_0.ts`.

- **Effort / Value / Risk.** S / med / low.

### 3. Driver test coverage gaps

- **Problem / observation.** Both fixtures define only `effectful`, `suspending` and
  `terminal` nodes (`tests/fixtures/stock-replenishment.ts:26–55`,
  `tests/fixtures/weekly-digest.ts:32–69`). Untested: the `pure` branch
  (`driver.ts:256–284`), `MAX_PURE_HOPS` (`:79`, `:296–300`), `transition_rejected`
  (`:151–165`), the unknown-definition path (`:122–132`), and the correlator's
  `signal_rejected` path (`correlator.ts:175–185`). The suite is PG-only
  (`ignore: !pgConfigured()`), which is right for a driver that is SQL-bound — but each
  behavior change in the first sprint must land with its test, and the branches above have
  none to extend. Also, `waitUntil` is copy-pasted three times
  (`tests/stock_replenishment.test.ts:22–33`, `tests/weekly_digest.test.ts:32–43`,
  `tests/migrations.test.ts:22–33`).

- **Proposed change.** `tests/driver.test.ts` with a small dedicated fixture:
  (a) `pure` routing on `ENTER` with two guards (left/right by a context flag);
  (b) `pure → pure → effectful` in one advance (history shows both `transition` rows and one
  `effect_dispatched`);
  (c) a `pure` self-cycle hits `MAX_PURE_HOPS` → `failed` with the "exceeded" reason;
  (d) a handler returning an outcome the node does not accept → `transition_rejected` +
  `failed`;
  (e) an instance whose `definition_version` is not registered → `failed`;
  (f) matcher returns `false` → `signal_rejected`, inbox row processed, instance still
  `waiting`.
  Move `waitUntil` to `tests/_util.ts`.

- **Done when** — `deno task test` runs the six cases green; `grep -c "async function
  waitUntil" tests/*.test.ts` is `0`.

- **Affected files.** `tests/driver.test.ts` (new), `tests/fixtures/` (new fixture),
  `tests/_util.ts` (new), the three existing test files.

- **Effort / Value / Risk.** M / med / low.

### 4. Lint and format are red; there is no aggregate check task

- **Problem / observation.** `deno lint` → 5 × `require-await`
  (`tests/fixtures/stock-replenishment.ts:70,76,82,85`,
  `tests/stock_replenishment.test.ts:224`). `deno fmt --check` → 12 files: `src/correlator.ts`,
  `src/driver.ts`, `src/mod.ts`, `src/persistence/history.ts`, `src/workflow.ts`, three
  test files, and `README.md` / `API.md` / `AGENTS.md` / `deno.json`. The markdown diffs
  are table re-alignment under the repo's own `fmt` config (`deno.json:29–34`); the
  TypeScript diffs are import/wrapping trivia. Nothing here is a bug — but a red baseline
  means a driver (`sprint/SPEC.md` §2) cannot use these as `Verify:` commands, and nobody
  notices new drift.

- **Proposed change.** Drop the `async` keywords on the sync fixtures (`Handler` allows a
  plain `HandlerResult`, `types.ts:68`); `deno fmt`; add
  `"check": "deno fmt --check && deno lint && deno check src/mod.ts tests/*.ts"` to
  `deno.json` tasks.

- **Done when** — `deno task check` exits `0`.

- **Affected files.** `deno.json`, the 12 listed files.

- **Effort / Value / Risk.** S / low / low. Kept only because it is trivially cheap.

## Open questions / decisions needed

Resolved 2026-08-26 (see `PROGRESS.md` → Decisions log): **#4** — let `deno fmt` reflow
the markdown tables (`README.md`, `API.md`, `AGENTS.md`); no `fmt.exclude` for them (T15
approved). Amended when the backlog was promoted: `fmt.exclude` gains
`docs/analysis/PROGRESS.md` alone, because the sprint driver's own bookkeeping commit
rewrites a `Commit` cell without re-padding the table and would otherwise leave
`deno fmt --check` — now a `Verify:` command — red through no task's fault.
**#1** has no open question, but it must wait for T01–T04 so it documents shipped behavior.
