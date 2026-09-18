# Decision publication and opening paper execution

## Observed failure

On 2026-09-17, Webull run 35237622247 reached the executor at 15:03:53 UTC (11:03:53 New York), reported connected=True, cash=$0 and zero tickets, and finished successfully. The source installed dependencies after waiting for the bell. Both `open_0930.yml` and `webull_paper.yml` could submit, plus sleeve-completion reruns. Its clock treated the entire 09:30–16:00 period as the open window. Its balance parser silently returned zero for unknown schemas. Failed connection, stale decisions and order errors could also return process exit code zero.

Dashboard production was usually tied to whole-workflow completion. A readiness report showing that inputs were present did not itself run the ranker and publish new decisions. GITHUB_TOKEN-created commits do not launch ordinary push workflows; another push trigger alone cannot repair this path.

Evidence: https://github.com/SRoyaltyy/fullscan/actions/runs/35237622247
GitHub scheduling constraints: https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule
Workflow-token event limitations: https://docs.github.com/en/actions/how-tos/writing-workflows/choosing-when-your-workflow-runs/triggering-a-workflow

## New path

1. `safe_git_push.sh` explicitly dispatches the decision publisher when a relevant current-session input lands. It evaluates the combined main tree, not the producer's incomplete local checkout. Publication outputs cannot recursively trigger this hook. Workflow-completion and scheduled checks remain recovery paths.
2. `decision_ready` validates the declared stock-book inputs plus ranked join, peers and historical factor panel. Existing book/dashboard outputs are not prerequisites. The publisher immediately rebuilds the ranker and inexpensive live strategy tickets; optional LLM/research completion and the large historical mining grid are not on this path.
3. Ticket generation hashes the required input set before and after building. A change during generation invalidates readiness. Full and slim publications retain the readiness proof and market-regime score. Empty valid selections remain empty; the broker does not silently backfill an alternative historical list.
4. `paper_open` connects and polls the current dated full decision artifact before the bell, builds the sandbox plan and stops preparation five seconds before 09:30. The last broker preflight must be at most 90 seconds old. Missing/incomplete decisions, unknown regime, missing price or insufficient cash block execution.
5. At 09:30 it writes a durable intent and sends the prepared orders in one broker batch. Early releases and starts more than two seconds late are rejected. Broker acknowledgment and actual fill are distinct: fill status remains `not_observed` until separately reconciled. A timeout/ambiguous acknowledgment is never blindly retried. Stable client IDs provide additional broker-side duplicate protection.
6. Only the owner named in `00_grounding/paper_open_owner.json` may run the automatic sender. The old open-pack paper job and sleeve-completion submission triggers are removed. Code pushes do not submit paper orders.

The live factor-mine strip refreshes every ten seconds. Other live strips select the newest session/timestamp across raw and deployed JSON; an old deployed cache cannot overwrite new decisions. Publication replaces only the factor page's live snippet, preserving the historical research pack. Fresh completion timestamps and matching outer input hashes prevent an old ready file from hiding a failed rebuild. Upstream writer workflows explicitly provide the dispatch token and Actions write permission.

## Clock installation and activation

`webull_paper.yml` warms a hosted runner at 12:07/13:07 UTC, covering both DST seasons. It is a fallback: GitHub queues can still start late, in which case the run fails rather than buying at 11:00 and calling it an opening entry.

The persistent primary option is `fullscan-paper-open.timer`, starting at 08:15 America/New_York on the ECS host. Its dedicated checkout and persistent journal are separate from the preopen producer's resetting checkout. Dependencies are installed during setup, never at the bell. `Persistent=false` prevents catch-up trading after a machine resumes late.

On merge, `install_paper_open.yml` runs on the existing `[self-hosted, ecs]` runner. It installs a separate credential file from the repository's sandbox secrets, verifies the systemd timer and makes a read-only broker connection/balance check. Only after a positive spendable-cash check succeeds does it switch the committed owner to `ecs`. Ownership changes during 08:00–09:31 ET are refused to avoid competing already-armed workers. If the runner is offline, sudo is unavailable, credentials fail, or the account truly has no cash, installation must remain visibly incomplete; code cannot create brokerage funds or make an offline host run.

The scheduled service checks the exchange-session calendar and refuses late starts. It journals in `/home/gha/fullscan-persist/paper-open`. Inspect with:

```sh
systemctl status fullscan-paper-open.timer
journalctl -u fullscan-paper-open.service --since today
```

Re-run the installer workflow after executor updates. Do not run ad-hoc legacy `webull_exec --submit` alongside the automatic owner. Do not delete an attempted-session journal to retry an uncertain submission: reconcile the broker's client IDs first.

## Acceptance criteria and limits

The next real session must show: required inputs validated before the bell; a published dated decision; `armed` before 09:30; the batch's submission timestamp/lateness; and individual broker acknowledgments. A broker fill at exactly 09:30:00 is not guaranteed by a network client, systemd or a market order. This repair targets timely submission and reports misses honestly. It cannot submit at 09:30 using information that arrives afterward.

Sixty focused regression tests pass, including input-change invalidation, publication-loop prevention, pre-armed release with a simulated clock, session/clock guards, duplicate journals, unknown outcomes, empty selections, unknown/zero balances and HTTP-200 business rejections. Existing tests were updated to require a nonzero exit for connection/staleness failures. An old Pages test hardcoded to the live 2026-09-16 snapshot is excluded because it depends on mutable external repository artifacts, not an isolated fixture. YAML, Python and shell syntax are checked. Live broker acceptance and next-session punctuality remain operational acceptance checks, not claims made from mocks.

This patch is independent of research-validation PR #268; it does not certify any strategy or authorize real-money trading. The automated executor hard-restricts itself to the Webull sandbox host.
