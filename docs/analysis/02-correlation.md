<!--
GENERATED ANALYSIS — @marianmeres/workflow (inbox correlation)
Produced 2026-08-26 by an inline single-agent review: full read of src/, tests/, docs, and the
resolved sources of @marianmeres/fsm 3.1.0, steve 3.0.0, cron 3.2.0 → self-verification pass
re-opening every cited line. Claims verified against the codebase at commit 02a7635.
Planning artifact; no code was changed.
-->

# Inbox Correlation — signal delivery semantics

> The inbox and correlator are how the outside world reaches a sleeping instance. For the
> motivating use case — supplier email replies — this is the main event, not an edge. The
> split "token is the index, matcher is the gate" (`AGENTS.md:202`) is right, the
> append-only inbox decoupled from matching is right, and the `FOR UPDATE SKIP LOCKED`
> batch claim is right. The delivery _rules_ are where the problems are.

> The single most important takeaway: **the correlator only looks for instances that are
> already `waiting`, and it consumes every inbox row it looks at.** A reply that lands
> while the instance is still `running` its send step — or sitting in a timer-only delay
> node — is marked processed and silently thrown away; the instance then waits out its
> full timeout for a reply that already arrived. Worse, at a timer-only suspending node the
> signal _is_ delivered as `MATCHED`, which the node has no transition for, and the
> instance is marked `failed`. Two replies inside one tick lose the second; a matcher that
> throws once consumes the signal permanently.

> Headline recommendation: deliver only when the instance is `waiting` at a node that
> accepts `MATCHED` and the matcher agrees; otherwise leave the row **unprocessed** (early
> signal — try again next tick) and mark it processed only when no live instance owns the
> token. Move "mark processed" into the advance transaction so delivery and transition are
> atomic, and let effect handlers set the correlation token so a wait point can key on
> something that only exists after the effect ran (a `Message-ID`, a provider session id).

## Summary of recommendations

| # | Recommendation                                                                                                     | Value | Effort | Risk |
| - | ------------------------------------------------------------------------------------------------------------------ | ----- | ------ | ---- |
| 1 | Defer early/late signals instead of dropping them; deliver only to `MATCHED`-accepting waits; processed-in-advance | high  | M      | low  |
| 2 | Duplicate / concurrent signals and matcher exceptions                                                              | med   | S      | low  |
| 3 | Handler-settable correlation token (per wait point)                                                                | med   | S      | low  |

## Findings & recommendations (detailed)

### 1. Signals that arrive before the wait point are dropped; signals at timer-only waits fail the instance

- **Problem / observation.** `#processOne` looks the token up with
  `findWaitingByCorrelation`, which filters `execution_state = 'waiting'`
  (`instances.ts:175–177`), and on a miss marks the inbox row processed and returns
  (`correlator.ts:130–133`). Timeline for the README flow: `sendOrderEmail` is `running`
  (or the following advance has not yet flipped the row to `waiting`); the supplier's
  auto-reply, or a fast human, is appended to the inbox by a webhook that has no idea where
  the instance is; the correlator tick runs first → row processed, nothing delivered → the
  instance reaches `await_reply` and waits three days for a reply that already came. The
  window is the whole duration of whatever sits between "token goes out" and "instance is
  waiting": a slow AI step, a retry with backoff, any timer-only node.

  The timer-only case is not a drop but a failure: a `suspending` node without `matcher`
  takes the `matched = true` default (`correlator.ts:144–146`), an advance with
  `outcome: "MATCHED"` is enqueued (`:205–210`), `positioned.transition("MATCHED")`
  returns `null` because the node's `on` has neither `MATCHED` nor `*`
  (`fsm.ts:536–546`), and the driver writes `transition_rejected` and marks the instance
  `failed` (`driver.ts:151–165`). Example: a "wait 1 h, then send a reminder" delay node
  between `send_order` and `await_reply` — a reply during the hour kills the instance.
  The validator cannot catch this and should not try (03 #1 covers `matcher` without
  `MATCHED`): a delay node legitimately has no `MATCHED` edge.

- **Evidence.** `correlator.ts:124–133`:
  ```ts
  async #processOne(client, row) {
  	const instance = await findWaitingByCorrelation(client, this.tenantId, row.correlation_token);
  	if (!instance) {
  		await markProcessed(client, row.id);   // <-- early signal is consumed here
  		return;
  	}
  ```
  `correlator.ts:144–146`: `let matched = true; if (meta && meta.kind === "suspending" &&
  meta.matcher) { … }` — no check that the node accepts `MATCHED`.

- **Proposed change.** Replace the lookup and the decision table; move the processed-mark
  into the advance.
  1. `findByCorrelation(exec, tenant_id, token)` → the live (non-terminal) instance,
     `ORDER BY created_at LIMIT 1`.
  2. Decision table in the tick (inside the existing batch transaction):

     | Instance                                        | Action                                                                                                          |
     | ----------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
     | none (unknown token) / terminal                 | mark processed; `clog.warn` (unknown) or `signal_rejected { reason }`                                           |
     | live but not `waiting`                          | **defer** — leave unprocessed, debug log                                                                        |
     | `waiting`, node lacks `MATCHED` and `*`         | **defer**                                                                                                       |
     | `waiting`, accepts `MATCHED`, matcher → `false` | `signal_rejected` history; mark processed                                                                       |
     | `waiting`, accepts `MATCHED`, matcher → `true`  | enqueue `{ kind: "signal", inbox_id, expected_seq: instance.seq }` — do **not** flip, do **not** mark processed |

     "Accepts `MATCHED`" = `"MATCHED" in stateCfg.on || "*" in stateCfg.on` (resolution
     order per `fsm.ts:510–534`).
  3. `runAdvance`, `kind: "signal"` (01 #1 gives the fence and the `waiting` precondition):
     ```ts
     const inbox = await lockInboxRow(client, payload.inbox_id!); // SELECT ... FOR UPDATE
     if (!inbox || inbox.processed_at) return; // delivered by a duplicate poke
     const result = positioned.transition("MATCHED", inbox.payload, false);
     // ... rejected → transition_rejected + failInstance (unchanged)
     await appendHistory(client, {
     	event_type: SIGNAL_RECEIVED,
     	from_node,
     	data: { inbox_id, source },
     });
     await markProcessed(client, inbox.id); // same transaction as the transition
     ```
  4. Deferred rows are re-examined every tick. Starvation note: `claimUnprocessed` orders
     by `received_at` with a `LIMIT` (`inbox.ts:49–55`); a backlog of deferred rows ≥
     `tickBatchSize` would shadow newer tokens. At the stated scale that is a non-issue;
     document it, and keep an `inboxMaxAgeSec` knob ("rows older than N seconds are marked
     processed with `signal_expired`") as a later addition if it ever bites.

- **Done when** — `tests/correlator.test.ts` (new): (a) append the signal _before_ the
  instance reaches `await_reply` (gate the `sendOrderEmail` handler on a promise), tick →
  the row is still unprocessed and the instance is not failed; release the handler, wait
  for `waiting`, tick → delivered, cursor `_end_ok`; (b) a fixture variant with a
  timer-only `cooldown` node before `await_reply`: signal during cooldown → deferred,
  instance survives, later delivered; (c) after delivery, the `signal_received` history
  row and `processed_at` were written together (both present; the instance's `seq`
  advanced by exactly one for the signal step).

- **Affected files.** `src/correlator.ts`, `src/persistence/instances.ts`
  (`findByCorrelation`), `src/persistence/inbox.ts` (`lockInboxRow`), `src/driver.ts`
  (`kind: "signal"` branch), `src/types.ts`, `tests/correlator.test.ts` (new),
  `tests/fixtures/stock-replenishment.ts` (cooldown variant), `API.md` "Per-tick behavior"
  (via 04 #1).

- **Effort / Value / Risk.** M / high / low–med. The semantic change is that early
  signals now wait instead of vanishing; a signal for an instance that never waits again
  lingers unprocessed until that instance terminates — bounded by the instance's lifetime.

- **Implementation notes.** Keep the matcher call inside the tick transaction as today
  (`correlator.ts:105–118`): the `SKIP LOCKED` claim is what stops two correlator workers
  from double-poking the same row within a tick, and matchers are cheap by contract.
  Depends on 01 #1 for `expected_seq` and the `waiting` precondition. Delete the
  instance `UPDATE` at `correlator.ts:198–203` — with the fence it is no longer needed as
  a "claimed" marker (see #2 for why it looked useful).

### 2. Concurrent / duplicate signals and matcher exceptions

- **Problem / observation.**
  **(a) Same tick, same token, two rows.** The first match flips the instance to
  `pending` (`correlator.ts:198–203`); the second row then finds no `waiting` instance and
  is marked processed — dropped. Under #1 the flip is gone, so the second row would be
  poked too; its advance is a fenced no-op (01 #1) and, because processed-marking now
  happens in the advance, the row stays unprocessed and is deferred to the next wait point
  — correct, _provided_ the tick does not poke the same instance twice in one batch.
  **(b) Matcher throws.** `catch → matched = false` (`correlator.ts:169–172`) leads to
  `signal_rejected` + mark processed: a transient failure in an async matcher (a lookup, an
  HTTP call) permanently consumes the signal.
  **(c) `LIMIT 1` without `ORDER BY`** (`instances.ts:174–178`): if two live instances
  share a token (not enforced anywhere), delivery is arbitrary.

- **Evidence.** `correlator.ts:169–172`:
  ```ts
  } catch (e) {
  	clog.error?.(`correlator: matcher "${meta.matcher}" threw: ${e}`);
  	matched = false;
  }
  ```

- **Proposed change.** (a) A per-tick `Set<instance_id>`; a second row for an instance
  already poked in this tick is deferred. (b) On throw: log at error, leave the row
  unprocessed, continue with the next row. A deterministic throw retries every tick with
  an error line — visible rather than silent, bounded by the instance's lifetime. (c)
  `ORDER BY created_at` in `findByCorrelation`; document "one live instance per token" as
  the contract. No unique index: the terminal path clears the token (`driver.ts:201`) and a
  partial unique index on `waiting` rows would turn a definition mistake into a failed
  advance rather than a matcher decision.

- **Done when** — tests: "two signals for one token appended before a tick: the first is
  delivered, the second remains unprocessed and is delivered at the next wait point (or
  marked processed once the instance terminates)"; "a matcher that throws on its first
  call and succeeds on the second delivers the signal on the second tick".

- **Affected files.** `src/correlator.ts`, `src/persistence/instances.ts`,
  `tests/correlator.test.ts`. Part of the same task as #1.

- **Effort / Value / Risk.** S / med / low.

### 3. The correlation token cannot be set after creation

- **Problem / observation.** The token lives on the instance row and is settable only via
  `create({ correlationToken })` (`workflow.ts:177–194`). `updateInstance` is not exported
  (`mod.ts:38–77`), `HandlerResult` is `{ outcome, data? }` (`types.ts:49–52`), and the
  suspending branch deliberately leaves the token alone (`driver.ts:238–239`). So: one
  token per instance, chosen before any effect runs. That fits plus-addressing
  (`README.md:140`) and nothing else — an SMTP `Message-ID` for `In-Reply-To` threading, a
  payment-provider session id, a ticket number returned by an API all exist only _after_
  the effect. Two sequential wait points must share one token and rely on the matcher to
  tell their signals apart.

- **Evidence.** `types.ts:49–52`:
  ```ts
  export interface HandlerResult {
  	outcome: string;
  	data?: Record<string, unknown>;
  }
  ```
  `driver.ts:238–239`: "correlation_token already on the row (set at create-time …). We
  don't auto-generate one here."

- **Proposed change.** `HandlerResult.correlationToken?: string | null`, carried on the
  effect-completion advance payload (01 #1 already reshapes it) and applied in
  `runAdvance` at the settle-point write, so the token is in place the moment the row
  becomes `waiting`. `null` clears it. Record it in the `transition` history row's data.
  `create({ correlationToken })` stays as the up-front option.

- **Done when** — test: `sendOrderEmail` returns `{ outcome: "SENT", correlationToken:
  "msg-42" }` on an instance created without a token → `find()` shows `msg-42` → a signal
  with that token is delivered and the workflow reaches `_end_ok`.

- **Affected files.** `src/types.ts`, `src/driver.ts`, `src/workflow.ts` (payload),
  `API.md`.

- **Effort / Value / Risk.** S / med / low. Depends on 01 #1 (payload shape).

- **Implementation notes.** An instance with `correlation_token IS NULL` entering a
  `MATCHED`-accepting wait can never be signalled; the validator cannot see it (tokens are
  runtime), so `clog.warn` at that moment in the suspending branch.

## Open questions / decisions needed

Resolved 2026-08-26 (see `PROGRESS.md` → Decisions log): inbox rows whose token no live
instance owns — unknown token, or the instance is terminal — are marked processed on the
first tick with a warn log, plus a `signal_rejected` history row when a terminal instance
exists. No grace period. The deferral semantics for early signals stand as proposed in #1
and #2.
