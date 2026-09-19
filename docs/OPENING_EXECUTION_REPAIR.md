# Decision publication and opening paper execution

## Observed failure

On 2026-09-17, Webull run 35237622247 reached the executor at 15:03:53 UTC (11:03:53 New York), reported connected=True, cash=$0 and zero tickets, and finished successfully. The source installed dependencies after waiting for the bell. Both `open_0930.yml` and `webull_paper.yml` could submit, plus sleeve-completion reruns. Its clock treated the entire 09:30–16:00 period as the open window. Its balance parser silently returned zero for unknown schemas. Failed connection, stale decisions and order errors could also return process exit code zero.

Dashboard production was usually tied to whole-workflow completion. A readiness report showing that inputs were present did not itself run the ranker and publish new decisions. GITHUB_TOKEN-created commits do not launch ordinary push workflows; another push trigger alone cannot repair this path.

The later 09:30-wait model then assumed paper MARKET orders could not be accepted before the bell, so publication and send stayed on separate clocks. That forced a second waiter (hosted cron and/or ECS timer) and created a miss when decisions were ready but the waiter was late.

Evidence: https://github.com/SRoyaltyy/fullscan/actions/runs/35237622247
GitHub scheduling constraints: https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule
Workflow-token event limitations: https://docs.github.com/en/actions/how-tos/writing-workflows/choosing-when-your-workflow-runs/triggering-a-workflow

## Empirical proof: standing paper MARKET/CORE/DAY before 09:30 ET

Webull paper accepts a MARKET + CORE + DAY order before the RTH open. It stays `SUBMITTED` with `filled_qty=0` until the open, then fills at the open print. Do not re-litigate this. Paper only. Never `--env real`.

| Field | Value |
| --- | --- |
| `client_order_id` | `STANDTEST-20260918-1789726292` |
| `order_id` | `TKG43FLL3OL4D6DSK7VE06ONS9` |
| Pre-open 06:20 ET | `SUBMITTED` / filled 0 |
| Post-open | `FILLED` at 09:30:00.251 ET @ 337.61 |
| Cash | 1M → ~999662; AAPL×1 |

Therefore a ready decision can be sent immediately. A second delayed cron is not required to "wait for the bell."

## New path

1. `safe_git_push.sh` explicitly dispatches the decision publisher when a relevant current-session input lands. It evaluates the combined main tree, not the producer's incomplete local checkout. Publication outputs cannot recursively trigger this hook. Workflow-completion and scheduled checks remain recovery paths.
2. `decision_ready` validates the declared stock-book inputs plus ranked join, peers and historical factor panel. Existing book/dashboard outputs are not prerequisites. The publisher immediately rebuilds the ranker and inexpensive live strategy tickets; optional LLM/research completion and the large historical mining grid are not on this path.
3. Ticket generation hashes the required input set before and after building. A change during generation invalidates readiness. Full and slim publications retain the readiness proof and market-regime score. Empty valid selections remain empty; the broker does not silently backfill an alternative historical list.
4. On ready publish success, **the same** `publish_strategy_tickets.yml` job submits the paper hot4 batch immediately: `python -m src.paper_open --submit --ready --owner actions`. It reads the workspace tickets (not a lagged raw.githubusercontent copy), sizes leftover cash via `webull_exec.plan_hot4_for_broker`, and places one sandbox batch through `webull_exec.order_body` (MARKET / CORE / DAY). Standing placement is allowed 04:00–16:00 ET on a session day. WEBULL_APP_KEY / WEBULL_APP_SECRET / WEBULL_ACCOUNT_ID are wired only into that step. No new cron is added.
5. `paper_open` writes a durable per-session journal (`data/paper_open/{date}_submit.json`) before the send. A re-fire of the same workflow, the hosted fallback, or a restart sees the journal (local or the copy landed on `main`) and does not resend. Stable `client_order_id` values (`fs{YYYYMMDD}{B|S}{TICKER}`) are the broker-side duplicate lock. Fill status stays `not_observed` until separately reconciled. An ambiguous acknowledgment is never blindly retried.
6. Single-owner lock: `00_grounding/paper_open_owner.json` is `owner=actions`. `paper_open.owner_enabled` is the only automatic gate. The ECS unit `fullscan-paper-open.service` still starts with `--owner ecs` and therefore **no-ops**. `install_paper_open.yml` must not flip this record back to `ecs`. `webull_paper.yml` stays warm/fallback with `--owner actions`; when owner≠actions it skips. Do not add a delayed cron. Do not enable live money.

The live factor-mine strip refreshes every ten seconds. Other live strips select the newest session/timestamp across raw and deployed JSON; an old deployed cache cannot overwrite new decisions. Publication replaces only the factor page's live snippet, preserving the historical research pack. Fresh completion timestamps and matching outer input hashes prevent an old ready file from hiding a failed rebuild. Upstream writer workflows explicitly provide the dispatch token and Actions write permission.

## Clock installation and activation

`webull_paper.yml` still warms a hosted runner at 12:07/13:07 UTC (both DST seasons). It is fallback only: if ready-publish already journaled a send, it prints `session already attempted; no resend`. If ready-publish never placed, the 09:30 wait path may still release inside the two-second bell window. GitHub queues can start late, in which case that fallback fails rather than buying at 11:00 and calling it an opening entry.

The ECS timer `fullscan-paper-open.timer` remains installed at 08:15 America/New_York as a dormant spare. Because the committed owner is `actions` and the service passes `--owner ecs`, `owner_enabled` skips before any broker call. Re-run the installer after executor updates to refresh the isolated checkout; the installer now **keeps** `owner=actions` and refuses to claim the send. Journals on the host stay in `/home/gha/fullscan-persist/paper-open` and are independent of the Actions workspace journal landed on `main`.

```sh
systemctl status fullscan-paper-open.timer
journalctl -u fullscan-paper-open.service --since today
```

Do not run ad-hoc legacy `webull_exec --submit` alongside the automatic owner. Do not delete an attempted-session journal to retry an uncertain submission: reconcile the broker's client IDs first. Paper only.

## Acceptance criteria and limits

The next real session must show: required inputs validated; a published dated decision; a paper journal from the same ready-publish job (standing submit before 09:30 is OK); MARKET/CORE/DAY acknowledgments; and no second place from ECS or `webull_paper.yml`. A broker fill at exactly 09:30:00 is not guaranteed by a network client, systemd or a market order — STANDTEST filled at 09:30:00.251 ET after sitting SUBMITTED. This repair targets timely submission on ready and reports misses honestly.

Regression tests cover ready→submit before the bell, owner-gate skip when `--owner` ≠ the committed record, journal/remote-journal no-resend, fallback no-op after a ready send, late-ready standing submit, input-change invalidation, publication-loop prevention, pre-armed 09:30 release, session/clock guards, duplicate journals, unknown outcomes, empty selections, unknown/zero balances and HTTP-200 business rejections. Tests do not call the live broker. An old Pages test hardcoded to the live 2026-09-16 snapshot is excluded because it depends on mutable external repository artifacts, not an isolated fixture. YAML, Python and shell syntax are checked.

This patch is independent of research-validation PR #268; it does not certify any strategy or authorize real-money trading. The automated executor hard-restricts itself to the Webull sandbox host.
