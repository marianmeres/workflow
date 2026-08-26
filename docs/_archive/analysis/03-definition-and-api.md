<!--
GENERATED ANALYSIS — @marianmeres/workflow (definitions, registry, public API)
Produced 2026-08-26 by an inline single-agent review: full read of src/, tests/, docs, and the
resolved sources of @marianmeres/fsm 3.1.0, steve 3.0.0, cron 3.2.0 → self-verification pass
re-opening every cited line. Claims verified against the codebase at commit 02a7635.
Planning artifact; no code was changed.
-->

# Definitions, Registry & Public API

> This is the part of the package that is genuinely well done and needs no rethinking:
> pure-data definitions with string handler references, construction-time validation
> (`definition.ts:16–139`), pin-and-freeze versioning keyed by `id@version`, `meta`
> passthrough via fsm 3.1's `getCurrentMeta` (`fsm.ts:847–858`), and a correct resume via
> `fromSnapshot` that skips `onEnter` (`fsm.ts:1005–1048`). No findings against the design.

> The single most important takeaway: the gaps are edges a first real workflow will hit
> within a day. The validator accepts definitions that are _guaranteed_ to fail at runtime
> (a `pure` node with no `ENTER` edge, a `timeoutSec` with no `TIMEOUT` edge). `create()`
> ignores the definition's own `context` defaults. Two `Workflow`s on one `Jobs` overwrite
> each other's `workflow.advance` handler without a word. And `meta` is `unknown` while you
> type it.

> Headline recommendation: four small additive changes (validator rules, context seeding, a
> double-attach guard, typed `meta`) and two decisions for the owner — whether one runtime
> should serve many tenants in one process, and whether handlers may patch context without
> an fsm `action` per edge.

## Summary of recommendations

| # | Recommendation                                                                  | Value | Effort | Risk                       |
| - | ------------------------------------------------------------------------------- | ----- | ------ | -------------------------- |
| 1 | Validator: reject definitions that must fail at runtime                         | med   | S      | low                        |
| 2 | `create()` seeds context from `def.fsm.context`                                 | med   | S      | low                        |
| 3 | Guard against two `Workflow`s on one `Jobs`; (decision) tenant-per-call runtime | med   | S (+M) | low (+med for the runtime) |
| 4 | Opt-in `HandlerResult.context` patch (decision)                                 | med   | S      | low                        |
| 5 | Typed `meta` in `WorkflowDefinition`                                            | low   | S      | low — trivially cheap      |

## Findings & recommendations (detailed)

### 1. The validator misses guaranteed runtime failures

- **Problem / observation.** `validateDefinition` checks handler/matcher names and that
  every transition _target_ exists, but never that the _events_ a node will receive have
  an edge. `case "pure": break;` (`definition.ts:62–63`); suspending checks the matcher
  name only (`:80–97`); the transition loop validates targets (`:108–130`). Each of these
  fails deterministically at runtime, after side effects may already have run:

  | Definition                                                  | Runtime                                                                             |
  | ----------------------------------------------------------- | ----------------------------------------------------------------------------------- |
  | `pure` node without `on.ENTER` (or `*`)                     | `transition_rejected` + `failed` on entry (`driver.ts:258–273`)                     |
  | `suspending` with `timeoutSec`, no `TIMEOUT`                | scheduler fires `TIMEOUT` → rejected → `failed` (`driver.ts:151–165`)               |
  | `suspending` with `matcher`, no `MATCHED`                   | today: `failed` on the first signal; after 02 #1: deferred forever — silent instead |
  | `suspending` with neither `timeoutSec` nor a `MATCHED` edge | can never wake — sleeps forever                                                     |
  | `effectful` with `on: {}`                                   | every outcome rejected → `failed`                                                   |

- **Evidence.** `definition.ts:60–63`:
  ```ts
  const kind = (meta as { kind?: string }).kind;
  switch (kind) {
  	case "pure":
  		break;
  ```

- **Proposed change.** Add, in the existing message style (`"${def.id}@${def.version}":
  state "${stateName}" …`):
  - `pure` → `on.ENTER` or `on["*"]` must exist.
  - `effectful` → `on` must have at least one event.
  - `suspending` → `timeoutSec` present ⇒ `TIMEOUT` or `*`; `matcher` present ⇒ `MATCHED`
    or `*`; at least one of (`timeoutSec`, a `MATCHED`/`*` edge) must exist;
    `timeoutSec`, when present, must be a finite number `> 0`.
  - `terminal` → `on` must be empty (a terminal with edges is a contradiction; the fixtures
    already write `on: {}`).

- **Done when** — `tests/validator.test.ts` (new) has one failing definition per rule above
  and asserts the thrown message; the existing fixtures still validate.

- **Affected files.** `src/definition.ts`, `tests/validator.test.ts` (new), `API.md`
  (`validateDefinition` → "Checks performed").

- **Effort / Value / Risk.** S / med / low. Can only reject definitions that would have
  failed at runtime.

### 2. `create()` ignores the definition's `context` defaults

- **Problem / observation.** `create()` seeds `context: input.context ?? {}`
  (`workflow.ts:192`). fsm supports `context?: TContext | (() => TContext)` (`fsm.ts:69`)
  and builds it in `#initContext` (`:354–362`) — but the driver only ever constructs
  machines via `fromSnapshot`, which takes the snapshot's context verbatim (`fsm.ts:1039`).
  `def.fsm.context` is never read anywhere in `src/`. A definition declaring
  `context: () => ({ attempts: 0 })` starts with `{}`, and a guard reading
  `ctx.attempts < 3` compares `undefined`.

- **Evidence.** `workflow.ts:187–194`:
  ```ts
  const row = await createInstance(this.db, {
  	...
  	cursor: def.fsm.initial,
  	context: input.context ?? {},
  ```

- **Proposed change.** In `create()`:
  ```ts
  const base = typeof def.fsm.context === "function"
  	? def.fsm.context()
  	: structuredClone(def.fsm.context ?? {});
  const context = { ...base, ...(input.context ?? {}) }; // shallow; document it
  ```

- **Done when** — test: a definition with `context: () => ({ n: 1, s: "x" })` and
  `create({ context: { s: "y" } })` → `row.context` deep-equals `{ n: 1, s: "y" }`; a
  definition without `context` behaves as before.

- **Affected files.** `src/workflow.ts`, `tests/` (a case in the driver test file from
  04 #3 or its own), `API.md` (`create` table: `context` "merged over the definition's
  defaults").

- **Effort / Value / Risk.** S / med / low. Behavior changes only for definitions that
  declare `context` — which are silently ignored today.

### 3. Two `Workflow`s on one `Jobs` overwrite each other; one tenant per runtime

- **Problem / observation.** The constructor does `jobs.setHandler(JOB_TYPE_ADVANCE, …)`
  (`workflow.ts:99–110`); steve's `setHandler` assigns into a map — last writer wins
  (`jobs.ts:625–632`) — while `onDone` subscriptions accumulate (`jobs.ts:1045–1051`). A
  second `Workflow` on the same `Jobs` (another tenant, or another set of definitions)
  routes _every_ `workflow.advance` job through the second registry; the first's instances
  fail one by one with `Unknown definition …` (`driver.ts:122–132`). Nothing warns.
  `README.md:219` steers toward one process per tenant, but the in-process case is the
  natural shape for a small multi-tenant app — and this package's ecosystem is built
  around `tenant_id` everywhere.

- **Evidence.** `jobs.ts:625–632`:
  ```ts
  setHandler(type, handler) {
  	if (typeof handler === "function") this.#jobHandlers[type] = handler;
  ```

- **Proposed change.**
  **(a) Guard (small, do it):** a module-level `WeakMap<Jobs, Workflow>`; the constructor
  throws `Workflow: a Workflow is already attached to this Jobs instance` on a second
  attach. Add `detach()` — `setHandler(type, null)` for `workflow.advance` and every effect
  type, unsubscribe the `onDone` listeners (steve returns unsubscribers), clear the map
  entry — so tests and hot-reload setups can re-attach.
  **(b) Tenant-per-call runtime (decision):** make the tenant a per-call concern so one
  `Workflow` serves many tenants — `create({ tenantId? })`, `appendInbox({ tenantId? })`,
  `find(id, { tenantId? })`, `cancel`/`retry` likewise, defaulting to the constructor's
  `tenantId`; `WorkflowScheduler`/`WorkflowInboxCorrelator` gain
  `tenantId?: string | "*"` (`"*"` = all tenants; tick name `workflow.scheduler`). The job
  payloads already carry `tenant_id`, the driver is tenant-agnostic, the registry is
  shared, the tables are tenant-keyed. Purely additive.

- **Done when** — (a) test: a second `new Workflow({ jobs, … })` throws; after
  `wf.detach()` it succeeds. (b) test: one `Workflow`, `create({ tenantId: "a" })` and
  `create({ tenantId: "b" })`, a scheduler with `tenantId: "*"` wakes both; `find` with
  the wrong tenant returns `null`.

- **Affected files.** (a) `src/workflow.ts`, `API.md`. (b) additionally
  `src/scheduler.ts`, `src/correlator.ts`, `src/persistence/instances.ts`,
  `src/persistence/inbox.ts`, `README.md` (Multi-tenancy).

- **Effort / Value / Risk.** (a) S / med / low. (b) M / med / med — an API-surface
  decision; only worth it if same-process multi-tenant is a target deployment.

### 4. Threading handler data into context requires an fsm `action` per edge (decision)

- **Problem / observation.** The driver deliberately does not merge `data` into context
  (`driver.ts:166–170`: "We don't auto-merge to keep state shape under the user's
  control"). The consequence is visible in the package's own fixture: every effectful edge
  in `tests/fixtures/weekly-digest.ts` carries an `action` whose only job is to copy
  `payload.content` / `payload.summary` into `ctx` (`:35–41`, `:48–54`; the file's header
  comment at `:9–12` explains the workaround). Those functions are the only non-data parts
  of an otherwise JSON-safe definition, and every real workflow will replicate them.

- **Evidence.** `tests/fixtures/weekly-digest.ts:35–41`:
  ```ts
  FETCHED: {
  	target: "summarize",
  	action: (ctx: DigestContext, payload) => {
  		const p = payload as { content?: string } | undefined;
  		ctx.content = p?.content;
  	},
  },
  ```

- **Proposed change.** Opt-in, explicit per handler — keeps the shape under the author's
  control without the boilerplate: `HandlerResult.context?: Partial<WorkflowContext>`,
  shallow-merged into the instance context in `runAdvance` _before_ `fsm.transition(outcome)`,
  so guards on the outcome edge see it. `data` is unchanged (still the fsm payload).

- **Done when** — a weekly-digest fixture variant with no `action`s reaches `_end_ok` with
  the same final context; a guard on the outcome edge observes a merged key.

- **Affected files.** `src/types.ts`, `src/driver.ts`, `README.md` / `API.md` /
  `AGENTS.md` (convention #1 wording).

- **Effort / Value / Risk.** S / med / low. **Decision needed:** the current stance is a
  considered choice, not an oversight; this changes it.

### 5. `meta` is `unknown` while authoring a definition

- **Problem / observation.** `WorkflowDefinition.fsm` is
  `FSMConfig<TState, TEvent, WorkflowContext>` (`types.ts:45`), whose per-state `meta` is
  `unknown` (`fsm.ts:33`). `{ kind: "efectful", handler: "x" }` type-checks; it is caught at
  construction by `definition.ts:101–105`, so this is editor feedback and autocomplete,
  not safety.

- **Proposed change.**
  ```ts
  export type WorkflowStateConfig<TState extends string, TEvent extends string> =
  	& FSMStatesConfigValue<TState, TEvent, WorkflowContext>
  	& { meta: NodeMeta };
  export type WorkflowFSMConfig<
  	TState extends string = string,
  	TEvent extends string = string,
  > =
  	& Omit<FSMConfig<TState, TEvent, WorkflowContext>, "states">
  	& { states: Record<TState, WorkflowStateConfig<TState, TEvent>> };
  export interface WorkflowDefinition<
  	TState extends string = string,
  	TEvent extends string = string,
  > {
  	id: string;
  	version: string;
  	fsm: WorkflowFSMConfig<TState, TEvent>;
  }
  ```
  `WorkflowFSMConfig` narrows `meta` from `unknown`, so it stays assignable to `FSMConfig`
  and `FSM.fromSnapshot(def.fsm, …)` keeps type-checking. `definition.ts` and
  `correlator.ts:139–142` can drop their `as NodeMeta` casts.

- **Done when** — a fixture line with a misspelled `kind` under `// @ts-expect-error`
  passes `deno check`; the existing fixtures compile unchanged.

- **Affected files.** `src/types.ts`, `src/definition.ts`, `src/correlator.ts`, `API.md`.

- **Effort / Value / Risk.** S / low / low. A consumer definition with a state that has no
  `meta` stops compiling — it already failed validation.

## Open questions / decisions needed

Resolved 2026-08-26 (see `PROGRESS.md` → Decisions log):

- **#3 (b):** guard only (T10). The tenant-per-call runtime (T11) is deferred; revisit when
  a real need to serve several tenants from one `Workflow`/`Jobs` in one process appears.
- **#4:** add the opt-in `HandlerResult.context` patch (T17 approved) — shallow merge before
  the outcome transition; `data` stays the fsm payload; no automatic merge of `data`.
- **#1:** a `terminal` state with a non-empty `on` throws at validation.
