# Analysis & Roadmap — `@marianmeres/workflow`

This directory holds a **code-verified** review of the package produced on **2026-08-26**
(codebase at commit `02a7635`, before any production use). It is a **planning artifact** —
no source code was changed. Every technical claim cites `file:line` in this repo or in the
resolved sources of `@marianmeres/fsm` 3.1.0, `@marianmeres/steve` 3.0.0 and
`@marianmeres/cron` 3.2.0, and was re-checked against the code before shipping;
suggestions that duplicated existing behavior or were low-value for a solo maintainer were
cut.

**Start here:** [`00-overview-and-roadmap.md`](./00-overview-and-roadmap.md) — the
verdict, the ranked master table, the recommended first sprint, and the sequencing graph.
Then [`PROGRESS.md`](./PROGRESS.md) to act on it.

## Documents

| #  | Doc                                                  | Scope                                      | Headline finding                                                                                                          |
| -- | ---------------------------------------------------- | ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------- |
| 00 | [overview-and-roadmap](./00-overview-and-roadmap.md) | Synthesis + ranked roadmap                 | Architecture is right; the driver assumes exactly-once delivery from an at-least-once queue — add a fence                 |
| 01 | [durability](./01-durability.md)                     | Driver ↔ steve contract, crash windows     | No fencing token: stale/duplicate jobs fail healthy instances; claim-then-enqueue strands rows; `expired` ≠ retry         |
| 02 | [correlation](./02-correlation.md)                   | Inbox → instance signal delivery           | Early signals are dropped; a signal at a timer-only wait fails the instance; token is create-time only                    |
| 03 | [definition-and-api](./03-definition-and-api.md)     | Validator, registry, public API, DX        | Validator admits definitions that must fail; `create()` ignores `context` defaults; two `Workflow`s on one `Jobs` collide |
| 04 | [docs-tests-ops](./04-docs-tests-ops.md)             | Docs accuracy, test coverage, repo hygiene | Three durability claims are false and `autoCleanup` is undocumented; pure nodes and rejections are untested               |

## How it was produced

A single-agent inline review: complete read of `src/`, `tests/`, `README.md`, `API.md`,
`AGENTS.md` and the working notes in `tmp/`, plus the relevant dependency sources
(`fsm.ts`, steve's `jobs.ts` and `job/*`, cron's `cron.ts` and `cron/*`), followed by a
verification pass that re-opened every cited line and ran the baseline (`deno task test`:
10 passed; `deno check`: clean; `deno lint`: 5 problems; `deno fmt --check`: 12 files).
Editorial notes such as "_Cut from the draft:_" inside the docs record what the
verification pass removed and why.

> Every open question was closed in an owner interview on 2026-08-26. The calls and their
> rationale are in the `PROGRESS.md` decisions log; each doc's **"Open questions /
> decisions needed"** section records its own resolutions. The first sprint is cleared for
> unattended EXECUTE via the sprint driver.
