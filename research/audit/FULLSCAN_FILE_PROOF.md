# Fullscan per-file pre-open proof

Sessions 2026-08-13 through 2026-09-25 (31 days). A file is PROVEN only when an Actions job log shows a run pushed that commit to main (`[safe-push] pushed <sha>` or `old..new -> main`) at or before 13:30 UTC that day, the blob contains that morning's date, and headline inputs are not on the #331 quarantine list. Committer timestamps are not proof. A commit in no run's range is not proven. `PROVEN_BUT_CHANGED` still counts: only the pre-open blob counts, and `late_rows` lists what landed later.

Log lines read: 1887. Distinct commits timed: 1890. Unresolved short SHAs or lines with no timestamp: 0.

The day's full candidate list (ohlc_hot, probable, hot_score) cannot be rebuilt from these per-day files on any session. Those buckets read `data/prices/ohlc.parquet`, which is not a per-day file. `candidate_from_per_day_files` is `no` on every row.

S is the predict.md score (`Prediction: … total score`). When predict.md was never committed, S falls back to `weather.json` `general_score`. Hard-red is that score at or below -3. `data/hard_red_exceptions/latest.json` is not a per-day file and is not given a fake day proof. Sector `*trace*` files are omitted.

Headline inputs (actions, baseline, catalyst, digest, events, judge, map_heat, market_digest, parsed, research) have `stale_content=yes` on the 18 quarantine sessions (7 stale-dated, 11 undated Finviz). Those rows count only when `before_0930=yes` and `stale_content=no`.

Excel rows use the owner table `research/audit/excel_preopen_proof.csv`: session N reads `suggestions.csv` rows whose `signal_date` is the prior trading day, and the `excel_bot.yml` run must have finished before 13:30 UTC. A same-day filename is not the session key.

| input | proven days | total |
|---|---:|---:|
| ab_checklist | 18 | 31 |
| ab_enriched | 18 | 31 |
| predict | 27 | 31 |
| actions | 12 | 31 |
| judge | 6 | 31 |
| map_heat | 4 | 31 |
| catalyst | 2 | 31 |
| digest | 7 | 31 |
| parsed | 12 | 31 |
| market_digest | 1 | 31 |
| export | 20 | 31 |
| join | 0 | 31 |
| baseline | 1 | 31 |
| weather | 24 | 31 |
| events | 5 | 31 |
| research | 3 | 31 |
| stock_book | 19 | 31 |
| stock_suggestions | 11 | 31 |
| green | 0 | 31 |
| peers | 0 | 31 |
| universe_membership | 0 | 31 |
| segment_stats | 0 | 31 |
| quote_colors | 9 | 31 |
| excel_daily | 17 | 31 |
| excel_suggestions | 17 | 31 |
| S | 27 | 31 |
| hard_red | 27 | 31 |
| sector_predict | 21 | 31 |
| sector_outcome | 0 | 31 |
| sector_reflect | 0 | 31 |
| sector_board | 20 | 31 |
| sector_qc | 21 | 31 |

Join, peers, universe membership, and segment stats have ranked rows and a dated filename, and the body does not contain the session date. `green.json` is the same: a pre-open copy on some days, with no session date in the json. Coverage fails for those, so they are not PROVEN.

Excel, owner proof, these 31 sessions: 14 PROVEN (08-31, 09-03, 09-08, 09-10, 09-14 through 09-18, 09-21 through 09-25) and 3 PROVEN_BUT_CHANGED, where only the pre-open rows count (09-02 drop CMII, 09-04 drop AUBN, 09-11 drop SVCC). Not proven: 08-13 through 08-28, 09-01, and 09-09. GitHub API spot-check agreed. Run 33322096008 finished 2026-08-30T16:22:25Z and its job log says `[safe-push] pushed 2cc2571` (commit `2cc2571f494e`), before the 08-31 open. Run 34490029194 finished 2026-09-10T15:22:39Z and pushed `4dc97fcb`, before the 09-11 open; SVCC arrived in run 34610907074, which finished 2026-09-11T15:22:17Z, after that open. Run 34240092081 (09-08) was cancelled and left no 09-09 signals.

Theme Radar landed [research/lever_panel/server_time_proof.csv](https://github.com/SRoyaltyy/theme-radar/blob/19973230e4d80c74565e1246ada911503a808fb7/research/lever_panel/server_time_proof.csv) at `19973230e4d80c74565e1246ada911503a808fb7` (2026-09-26T03:56:51Z). All 234 rows are `PROVEN` from that repo's Actions logs. There is no separate "not proven frozen" file next to the lever panel. That table is Theme Radar's Finviz lever panel, not this repo's `panel.json`.

`code_selected_sha_committer_clock_not_proof` is the commit `materialize()` would pick with committer time. It is not a server time. `code_selected_matches=yes` means that blob is the log-proven pre-open blob.

## Panel rows before the open

The sequential walk reads `data/factor_mine/snapshots/{date}.json`, not `panel.json`. Every snapshot file for 09-09 through 09-24, and `data/factor_mine/retro_prices/ohlc.parquet`, share one commit, `5f13a4415ea0` (merge #336). That commit is not in any harvested run's push range. The first Actions run on that head started 2026-09-25T15:49:10Z (run 36156731817), which is a checkout of the merge. `snapshots/2026-09-25.json` first appears in `77db2793d6a2`, job log 2026-09-25T22:30:02Z. **0 of 31** sessions have a pre-open panel row. `PANEL_ROW_PREOPEN` is no on every session.

`panel.json` does have Factor strategy mine log lines for 16 of 22 commits. None of those pushes is at or before 13:30 UTC on the session whose rows it adds. Checked again for every morning 09-09 through 09-25, under Excel PR #353's clock (latest `main` commit with an Actions `run_started_at` strictly before 13:30 UTC) and under the job-log clock. Both trees have zero rows for that morning. The 09-09, 09-10, and 09-11 pins match the API: `c1167245934f` at 2026-09-09T13:29:07Z ends on 09-08; `a714ba9c0622` at 2026-09-10T13:11:49Z ends on 09-09; `ae97009391d2` at 2026-09-11T11:49:49Z is the same panel blob and has no 09-11 row. The closest later publish is 09-25: run 36141227990 started at 13:28:59Z on a tree that still ended on 09-24, and the log line pushed the 67 rows for 09-25 at 13:41:52Z. The day-by-day times are in `research/audit/PANEL_ROW_PREOPEN.csv`. Six `panel.json` commits have no log line, including the first appearance `f5b46d20eec2` (merge #172). A push-triggered Pages run whose `head_sha` is that commit is not a push range, so it is not proof.

HOT4 and holdup, with every day whose candidate list first appeared after 09:30 ET left out: no session, with GLND kept or removed.

## Rebuild match and HOT4 / holdup

Rebuild match is unchanged from `INPUT_PROVENANCE_336.md`. Today's Part A does not reproduce the server-proven pre-open A1-A15 files (0/3 days: 09-21, 09-22, 09-24). Price-list outputs and price features have no pre-open artifact of their own. Snapshot comparisons are not a license to rebuild earlier days.

HOT4 (`union_hot_n4_h1`) and holdup (`union_hot_n4_holdup`) need yday_gainer, yday_mover, ohlc_hot, overnight, probable, earn_react, price features, alarm, flatten, and mover_buy together. Those outputs are not proven per-day files. The price store is not a per-day file. Alarm, flatten, and mover_buy have no pre-open list. Headline inputs on the 18 quarantine sessions do not count. Every session's candidate list first appears after 09:30 ET (`PANEL_ROW_PREOPEN` no, snapshot absent from the pre-open tree). **No session** remains after that exclusion, with GLND kept or removed, so there is no return.

