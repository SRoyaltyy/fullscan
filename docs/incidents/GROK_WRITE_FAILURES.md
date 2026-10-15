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

## Every distinct failure mode in the logs

Surveyed 2026-10-15 from the failed and cancelled runs of Pre-Open, Post-Close, xAI device reauth, the OAuth guard, OpenClaw enable / ping / probe, map-heat research, the Grok auth canary, news judge, and sector predict. Cancelled runs are included because a cancel is often the failure (the runner was busy, or GitHub killed the step). Each mode below is something the log actually said. A red run that is only a repeat of a mode already listed is not a new mode.

`apply_jev_repo_draw.yml` fails on every push with zero jobs. That is a broken workflow file, not a Grok or ECS failure, and it is left out of the list below.

### The Grok call itself

1. **Native-search read timeout, then the breaker.** The events call (`tools=True`) does not return inside the client cap. After two of those the breaker opens and later calls are skipped. Seen on ECS Pre-Open `38367704408` (2026-10-13, then the 50-minute step kill), `38534415018` (2026-10-14 morning, timeouts at 08:46Z and 08:50Z), `38548455006` (2026-10-14 10:16Z, then QC `degraded=11` and the 50-minute kill), and `38578583308` (2026-10-14 afternoon, timeouts at 14:38:56Z and 14:42:57Z). #599 cuts the client cap to 75s and trips the breaker on the first timeout.

2. **Each sector burns its own ~3 minute timeout, with no breaker.** On `36688284790` (2026-09-30), `37435156916` (2026-10-06), and `37592783327` (2026-10-07) the general predict had already landed, then every sector logged `timeout after 164–181s` and `DeepSeek blocked (supergrok_remaining=unknown)`. The job was then cancelled (`37592783327` started 08:22Z and completed 09:51Z; the log says `The operation was canceled`, and the Pages upload then got `403 Forbidden: job is completed`). #599's breaker stops this pile-up after the first hang.

3. **The gateway keeps the turn after the client gives up.** The process timer stays 10800s. The next call waits behind that unfinished turn. A listening port is not proof the cashier is free. #599 restarts the gateway once on the first hung read.

4. **PONG is not a health check.** Reauth `33597448920` (2026-09-02) wrote `pid=417604 pong ok` and `classroom live` in the same minute it committed `xAI device login unfinished`. The classroom greeting is a different model (`openclaw/default`), not a finished xAI OAuth.

5. **HTTP 500 marks the gateway DOWN for the rest of the run.** Map-heat `33362410296` (2026-08-31) had `CLASSROOM_OPEN=yes`, then `gateway marked DOWN for this run: HTTP 500 ... internal error`. The step then failed because captain coverage was short (`124/142 < 128`), which is a research QC miss on top of the 500.

6. **A 127-character body, repeated for every sector at once.** ECS Pre-Open `34215042304` (2026-09-08) returned events fine (`chars=38517`), then general predict and all 11 sectors failed `too_small(127)` within one timestamp (12:09:31Z). That is a fixed short error string, not eleven separate hangs. The same job logged `ConfigMutationConflictError: config changed since last load` while the timeout patcher rewrote `openclaw.json`.

7. **The model answered, and the format check threw it out.** `33399857749` (2026-08-31) and `34994648372` (2026-09-15) failed sectors for missing `MEMORY_CONFIRM` / `SECTOR_SCORES_*` / `HIT_GRID_BEGIN`, or `too_small(2247)`. The general predict on those runs was written. This is a contract miss, not a dead gateway.

8. **The backup said it took over, then posted to OpenClaw again.** `38578583308`, before #597. `chars=0`. News judge FAIL-CLOSED `too_small(28)`. Predict FAIL-CLOSED. #597 sends an empty Grok result to free `glm-5.3-flash`.

9. **After the breaker, sectors stay DEGRADED.** `supergrok_remaining=unknown` blocks the paid fallback, so the log says `DEGRADED provider=openclaw` (`38548455006`, `38367704408`). A later line `sector grok retry backoff 79s health_ping=ok` still leaves the breaker open. A successful ping does not reopen the gateway for that run.

10. **`grok_ok` is the reviewer, not the writer.** `empty_review_reply` on 2026-10-14 is the hostile review refusing the packet. A GLM essay can still be `grok_ok: true` (2026-10-13). A predict with no trace (2026-10-14, run `38534415018`) is not a Grok essay.

### Login, tokens, and empty paid accounts

11. **Device login never finished.** Schedule reauth failed all day 2026-08-28 through 2026-09-02 (30 failures in the window; `33597448920` printed `user_code=QXGD-P3AP` and `https://accounts.x.ai/oauth2/device?user_code=QXGD-P3AP`, then `xAI device login unfinished`, exit 1). Pressing Allow is Cyrus's step.

12. **The device waiter could not reach the user bus.** Same run: `systemd-run rc=1 Failed to connect to bus: $DBUS_SESSION_BUS_ADDRESS and $XDG_RUNTIME_DIR not defined`.

13. **A good 17+25 did not move the expiry.** Guard `38268223011` (2026-10-12) got HTTP 200, answer `42`, in 11.1s, and still exited 2: `FAIL: token not extended ... expires` stayed `2026-10-12T14:34:42Z` (`left_min` about -45). Later guards (2026-10-14 04:35Z and 23:08 ET) treat a completion that actually extends the expiry as success. A completion that returns 42 and does not move `expires` is still a dead refresh token, which ends at device login.

14. **The token lasts about six hours, and the systemd renew does not publish.** The status on main from 2026-10-14 23:08 ET has `expires_after` `2026-10-15T09:08:45Z`, which is 05:08 ET, before the 05:55 ET Pre-Open. The 00:10 ET timer did not update `01_daily/_openclaw_oauth_status.json`. The GitHub publisher is the guard workflow, not the systemd unit. #599 moved that publisher to 21:10 ET and 04:40 ET and added a 04:10 ET systemd slot. If the 04:40 run says `invalid_grant`, that is mode 11.

15. **Two bearers, and the long one is rejected.** Map-heat `33399713686` (2026-08-31): the 48-character file token got `403 "You have run out of credits or need a Grok subscription"` while the log still said `chat_http=401`. The 64-character GitHub `OPENCLAW_TOKEN` got `Unauthorized` as bearer and as `x-api-key`. Jobs that realign to the on-box file get past the 64-character 401. A credits 403 is not fixed by realigning.

16. **The gateway process was not up.** Enable-chat `33040037378` (2026-08-27): `Connection refused` on `127.0.0.1:18789`, `HTTP 000`, `CLASSROOM_OPEN=no`. The next enable, `33045368002`, got HTTP 200 on `/` and `chat_http=401` five times, and the journal said `Gateway start blocked: existing config is missing gateway.mode`.

17. **Paid DeepSeek is empty.** HTTP 402 Insufficient Balance. News judge `37917987325` (2026-10-09, ubuntu). ECS Pre-Open `34206618217` (2026-09-08): OpenClaw events `chars=0` and every DeepSeek repair was 402, then `zero_events`. `34099583763` (2026-09-07): sector essays `empty` after the same 402. This is why 2026-10-09 and 2026-10-12 have no predict. Those days stay missing. Do not put a paid key back.

18. **Paid Zhipu `glm-5.3` is empty.** Probe `38602435068`: HTTP 429, code 1113, no balance. It stays off. Free `glm-5.3-flash` is the backup #597 already uses.

19. **The free-backup host can hang too.** News judge `38259716482` (2026-10-12, ubuntu, not the gateway): `tokenhub.tencentmaas.com` read timeout 120s, then `too_small(28)` and `missing:NEWS_PARSE_BEGIN`.

### One computer for every ECS job

20. **One self-hosted runner.** Guard `38582373791` was created 14:38:49Z while `38578583308` owned `ecs-openclaw`, and the guard did not start until 15:16:39Z. Canaries `38538308810` and `38575838805`, ping `38245435878` (cancelled during checkout), probe `35100767419`, and guard `38566070014` were cancelled with no Grok error line. They were waiting on a busy box, or a newer dispatch replaced them.

21. **systemd and GitHub share the lock, and backing off is exit 1.** Map-heat `33036683298` and `33032497583` (2026-08-27) failed the step `Back off if ECS systemd research owns the box`. Pre-Open scripts print `ECS systemd job holds the lock — backing off`. That is contention, and the workflow still goes red.

22. **Night post-close can sit across the renew and the morning.** Job `timeout-minutes: 1080`, systemd `TimeoutStartSec=18h`. #599 makes a new post-close on the ECS box exit before 04:00–10:30 ET. A process that already started does not pick that up until it restarts.

23. **The shared workdir is left sparse, or the fetch never finishes.** Post-close `37525922950` (2026-10-06): `git sparse-checkout disable` took from 20:22 to 20:33, then `Could not open requirements file: requirements.txt`. Reauth `38562269047` (2026-10-14): `git fetch` hit its 90s cap, then `ERROR: workdir missing scripts after sync` and `runtime_status.py` Errno 2. Reauth `38562600140`: `could not read Username for 'https://github.com'` and `/home/gha/fullscan` had no `xai_device_reauth.py`. Reauth `38563036955`: `Authentication failed` on the clone, then the 180s clone cap, exit 1. Pre-Open `34099583763`: `git fetch origin main` timed out after 60s, twice.

24. **Two writers touch `openclaw.json` at once.** Mode 6. `ensure_openclaw_timeouts.sh` patches the file and prints `Restart the gateway to apply` (`34994648372`). A second writer in the same minute raises `ConfigMutationConflictError`.

### The job dies after the model has already answered

25. **The 50-minute step kill.** `38367704408`, `38392029548`, `38548455006`. GitHub cancels the step. On `38578583308` the python step is the one marked cancelled and the run is `failure`. #599 puts a 40-minute deadline inside the step so the tail work is skipped before GitHub kills it.

26. **`live_boards` sat for 480s at the end.** `38578583308`, 15:05:31Z to 15:15:55Z. Also `34955668851` (2026-09-15): `timed out after 180s` on `collectors.finviz_financials` and on `publish_live_boards`, and the same run skipped essays with `skip LLM essays (DeepSeek credits)`. #599 bounds those tail steps by the time left.

27. **Cancel revokes `GITHUB_TOKEN`.** `38578583308` at 15:16:12Z: `Invalid username or token`, after pushes at 15:03–15:05Z had worked. `34955668851` shows the same `Authentication failed` under `The operation was canceled`. `FULLSCAN_PUSH_TOKEN` is still unset. The deadline in #599 is what keeps the push on the happy path. A cancel that still happens will keep failing the last fetch until that secret exists.

28. **Pages upload after the job is already dead.** `37592783327`: `Failed to CreateArtifact: (403) Forbidden: job is completed`. `38548455006` cancelled the Pages step after the 50-minute kill.

### Wrong machine, or too late in the day

29. **Ubuntu cannot call the loopback gateway.** Pre-Open `38392029548` (2026-10-13, `ubuntu-latest`) never logged a breaker. It died in the DeepSeek tool loop: SearXNG `404` on `/search`, DuckDuckGo `html.duckduckgo.com` retries exceeded, then the 50-minute kill, with sector predicts and the finviz digest missing. The scheduled GitHub Pre-Open cron is ubuntu. The ECS path is `workflow_dispatch` with `runner=ecs`, and the systemd timer.

30. **Finviz blocks the ECS address.** The scrape job is named `Finviz scrape (ubuntu — ECS 403s)` for that reason. A red scrape is not automatically a 403. `36345968854` (2026-09-27 19:53Z) logged Elite login HTTP 200, then `[finviz_digest] past 09:25 ET — not writing a late copy` and `refusing map_heat: now 15:55 EDT is past 09:25`. That exit 1 is the cutoff guard. The same family: `37592783327` logged `[peer_rs] FAIL no finviz export for 2026-10-07 (refusing to relabel an older export)`.

31. **An evening push used to start a packet.** Merging #599 at 23:07 ET started Pre-Open `38674627338` because the clock gate only blocked pushes before 03:55 ET. The essay step skipped (the 14th was already on disk). The book job did rewrite the human-readable 2026-10-14 stock book. #600 blocks a push-triggered packet before 03:55 ET and after 09:25 ET. Schedule and `workflow_dispatch` still run.

32. **A quality-ok packet skips the Grok rewrite.** If `preopen_report` is `all_ok`, later writes are skipped even when `grok_ok` is false. A GLM essay that lands before 05:55 would be kept. That is why a night dispatch must not write the next day's predict.

33. **A required file can fail the run after Grok already wrote.** `33618162369` (2026-09-02): general predict present, sector QC 9/11, `grok_ok=True`, `qc_all_ok=True`, then `FAIL ... trash or missing required artifacts ['map_heat_research']`.

34. **Upstream data, separate from the model.** `34215042304` also logged `FRED DGS30 API failed: 502`. The predict failure on that run is mode 6, not the 502.

### Red runs that are not the gateway

35. **A Python syntax error in map-heat.** `33159529821` (2026-08-28): classroom was open (`chat_http=200`), then `SyntaxError: f-string expression part cannot include a backslash`. The twin `33159175641` failed the same step.

36. **Invalid workflow, zero jobs.** Pushes `35971371921` and `35971415244` (2026-09-24, "Wire SuperGrok...") failed before any step. Empty failed logs.

37. **The run is red and every recorded job is green.** `34946431321`, `34944936288`, and `34943180893` (2026-09-15) have clock, scrape, and land-book success, no ECS `preopen` job in the run, and no error line. That is a hole in the run record, not a timeout we can quote.

38. **Force-cancel and queue twins.** `38569102482` was the force-cancelled afternoon Pre-Open. It did not cause the morning hang on `38534415018`. The 2026-10-12 Pre-Open list is full of cancelled dispatches that never reached a Grok line.

### What is already changed, and what is not

#597, #599, and #600 already change modes 1, 2, 3, 8, 22, 25, 26, and 31. Mode 14 is changed only for the GitHub publisher and the 04:10 ET timer; a dead refresh token is still mode 11.

Still owner-only: mode 11 (open the device URL and press Allow), mode 15 if the message is the credits/subscription 403, and mode 27 until a `FULLSCAN_PUSH_TOKEN` secret exists.

Left as they are on purpose: paid DeepSeek and paid `glm-5.3` stay off (17, 18); the gateway timer stays 10800s; missed days stay missing; #598 stays unmerged.
