# Why the morning Grok write fails

Written 2026-10-15. This is the record of why general predicts stopped being Grok essays, and what we changed so the next morning does not sit there until GitHub kills the job.

## The short version

Think of the OpenClaw gateway as a single cashier. A tiny question ("what is 17+25?") gets an answer in about ten seconds. The morning essay asks that same cashier to go search the web and X, and on 2026-10-14 that trip did not come back.

The program waiting at the counter gave up after 4 minutes. The cashier did not. The gateway's own timer is 3 hours, so the unfinished search stayed in the process. The next customer waited behind it, also for 4 minutes. After two of those, a circuit breaker flipped and every later Grok call failed instantly.

The backup writer said it had taken over, then walked back to the same cashier and got nothing. The news judge and the predict both refused to save an empty page. At the end, a board step sat for another 8 minutes. GitHub cancelled the whole step at 50 minutes and, as it always does on cancel, tore up the job's password. The last git push then failed with "Invalid username or token". Earlier pushes in that same step had worked, so the password was fine until the cancel.

Grok itself is not broken as a writer. It has written the general predict on many days (the trace says `xai/grok-4.6`). What broke on the 14th is the long search turn, plus a backup path that did not actually call the free model, plus one computer doing the night job and the morning job.

## Who wrote the predict

`grok_ok` in `01_daily/<date>_preopen_status.json` is the hostile review of the packet. It is not "Grok wrote the essay". On 2026-10-13 `grok_ok` is true and the trace says `glm-5.3-flash`.

A real Grok essay looks like this in `01_daily/general/<date>_predict_trace.md`:

`Provider: OpenClaw gateway, backend model xai/grok-4.6`

A DeepSeek or GLM essay says `Model: deepseek-chat` or `Model: glm-5.3-flash`.

Days whose general trace names `xai/grok-4.6` (Grok did finish a big prompt, including ones over 600,000 characters): 2026-08-23, 08-24, 08-28, 09-01, 09-02, 09-03, 09-16, 09-17, 09-18, 09-21, 09-22, 09-23, 09-25, 09-28, 09-29, 10-02, 10-05.

Days whose trace names `deepseek-chat`: most of early August, and 08-31, 09-04, 09-08, 09-09, 09-10, 09-11, 09-14, 09-15, 09-24, 10-01, 10-08.

2026-10-09 and 2026-10-12 have no `*_predict.md`. Those days stay missing. We do not backfill them.

2026-10-13 was written by `glm-5.3-flash` (commit `b54a990ca`). Paid DeepSeek was already out of credit by then (HTTP 402 Insufficient Balance).

2026-10-14 has a predict file (landed 10:13 UTC in run `38534415018`) and no predict trace next to it. The status file for that run says `grok_ok: false`, `empty_review_reply`, and `events` missing. Do not treat that file as a Grok essay.

## What the logs show for 2026-10-14

Three Pre-Open runs that day:

| Run | What happened |
| --- | --- |
| `38534415018` | Morning ECS Pre-Open. Status written 09:13 ET. `grok_ok` false. |
| `38569102482` | Force run, cancelled. It did not cause the morning hang. |
| `38578583308` | Afternoon dispatch `#171` on `ecs-openclaw`, head `a83e7a4`. Failed. |

On `38578583308` the gateway port `127.0.0.1:18789` was up. The bearer in the live config is 48 characters; the GitHub `OPENCLAW_TOKEN` secret is 64 and gets a 401, so the job realigns to the file on the box. That part worked.

Then the events stage (native web/X search, `tools=True`):

- 14:38:56Z read timeout after 240s
- the log said the GLM fallback was engaged
- the next call was still an OpenClaw POST (this run is before #597) and timed out again at 14:42:57Z
- breaker file `openclaw_breaker_38578583308.txt` opened after 2 timeouts
- every later Grok call failed immediately
- news judge FAIL-CLOSED `too_small(28)`, predict FAIL-CLOSED, `chars=0`
- `live_boards` started 15:05:31Z and hit its 480s cap at 15:15:55Z
- safe-push had succeeded at 15:03–15:05Z with the same token
- at 15:16:12Z, during `The operation was canceled`, fetch failed `Invalid username or token`

The morning run `38534415018` has the same two-timeout shape at 08:46Z and 08:50Z, before the afternoon cancel. So leftover processes from the cancel are not the morning cause.

OAuth on the same box, same day:

- Run `38512052164` at 04:34Z. `models auth list` said the token was already expired (`expires 2026-10-13T22:28:31Z`). One real `17+25` completion returned 42 in 8.7s and the new expiry was `2026-10-14T10:35:12Z`. Unattended refresh works when the refresh token is still alive.
- Canary `38546117276` at 10:00Z: "HEAL PROBE OK: Grok returned heal in 11.0s".
- Guard run `38582373791` was created 14:38:49Z, while the hung Pre-Open still owned the only ECS runner. The job did not start until 15:16:39Z. After the hung job let go, a short completion worked again (9.8s) and the token was fresh until `2026-10-14T20:10:29Z`.

The hang is the long search turn, not "the gateway process is dead" and not "the token was already expired at 08:42" (it had about two hours left then).

## Why the backup wrote nothing

`GROK_ONLY=1` means: try Grok, and do not call paid DeepSeek. On the 14th the fallback code printed `FALLBACK provider engaged -> model=glm-5.3` and then posted to the gateway again. The free model was never asked. That is why the essay was 0 characters.

#597 (merged, `e89f204`) lets `glm-5.3-flash` answer when Grok is empty, and still blocks `api.deepseek.com`. This incident's fixes sit on top of that. They do not turn paid DeepSeek or paid `glm-5.3` back on. A probe the same day (`38602435068`) saw Zhipu `glm-5.3` HTTP 429 (no balance). That model stays off.

#598 is open on purpose. It would refuse to seal a plan unless `grok_ok` is true and the trace is Grok. Cyrus has not accepted that gate. Do not merge it from this work.

## Why the token looks dead in the morning

xAI OAuth lasts about six hours. A real completion refreshes it. If the refresh token is dead, the guard exits 2 and someone has to open the device-login page and press Allow. That is the only step that cannot be done from here.

The systemd timer fires at 00:10, 05:10, 10:10, 15:10, 20:10 ET, plus 05:30 ET on weekdays. The status file stayed on the box (`/home/gha/fullscan-logs/openclaw_guard_status.json`) and was not committed, so a stale "token fresh" line could sit there after the timer had stopped running.

The GitHub backup cron was `40 9 * * 1-5` (05:40 ET). That is the same minute as the Finviz scrape, and it queued on the only ECS runner fifteen minutes before Pre-Open. On the 14th the guard created during the hang did not start until the Pre-Open released the runner.

Night post-close can hold that same runner for hours (`timeout-minutes: 1080`, systemd `TimeoutStartSec=18h`). A renew scheduled at 00:10 ET never starts while that job owns the machine.

## What we changed

These are durable. A one-off rerun would hit the same cashier.

1. Client read cap is 75s and the breaker trips on the first timeout. The gateway's own 10800s timer stays. The runtime door treats anything shorter as a failure and would put it back.
2. The first hung read restarts the gateway once, so the 3-hour zombie dies. A port check is not the health check. Before the essays, a `17+25` probe runs, and a failed probe restarts once.
3. After any hang, the general predict gets one Grok try with search off (the documents are already in the prompt). If that is empty, `glm-5.3-flash` writes. A failed Grok rewrite does not delete a quality-ok essay that is already on disk.
4. Post-close on the ECS box stops between 04:00 and 10:30 ET so reauth and Pre-Open can start. The GitHub job does the same. The 1080 minute job limit is unchanged.
5. OAuth: systemd also fires at 04:10 ET. GitHub fires at 21:10 ET and 04:40 ET, not at 05:40. The run copies the status JSON into `01_daily/_openclaw_oauth_status.json` with no tokens and pushes it.
6. The Pre-Open step sets a 40 minute deadline, under the 50 minute kill, and the tail steps (live boards, sleeve, paper) shrink or skip. `live_boards` no longer gets a flat 480s at the end.
7. `safe_git_push.sh` prefers `FULLSCAN_PUSH_TOKEN` when that secret exists, and says plainly when a cancel has revoked `GITHUB_TOKEN`. Until that secret is set, the deadline is what keeps the push alive.

Paid providers stay off. Engine files pinned by `ENGINE_SHA256` were not edited. No past day's buy/sell record was rewritten. #598 was not merged.

## What only Cyrus can do

If the 04:10 renew says `invalid_grant` or "interactive device login needed", open the verification URL the guard prints and press Allow on the xAI account that owns this gateway. A script cannot click that.

If mornings should still push after GitHub has cancelled a step, add a long-lived `FULLSCAN_PUSH_TOKEN` secret (a PAT with contents write on this repo). Do not put a paid DeepSeek key or a paid GLM key back in to "fix" a hang.
