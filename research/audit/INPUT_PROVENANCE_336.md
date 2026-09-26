# Input provenance for the #336 sequential Factor Mine rebuild

Read-only audit. No ledger, snapshot, panel, or state file was written.

## Plain summary

Sessions checked: 31 (2026-08-13 through 2026-09-25).
Proven-frozen: 0. Not proven: 31.
Proven-frozen days: none.
Not proven: 2026-08-13, 2026-08-14, 2026-08-17, 2026-08-18, 2026-08-19, 2026-08-20, 2026-08-21, 2026-08-24, 2026-08-25, 2026-08-26, 2026-08-27, 2026-08-28, 2026-08-31, 2026-09-01, 2026-09-02, 2026-09-03, 2026-09-04, 2026-09-08, 2026-09-09, 2026-09-10, 2026-09-11, 2026-09-14, 2026-09-15, 2026-09-16, 2026-09-17, 2026-09-18, 2026-09-21, 2026-09-22, 2026-09-23, 2026-09-24, 2026-09-25.

HOT4 is `union_hot_n4_h1`. Holdup is `union_hot_n4_holdup`. The replay runs only on proven-frozen days, in that order, starting from $10,000. A day that is not proven is left out, not scored as a flat day.

**HOT4 and holdup, GLND kept.**
No proven-frozen sessions, so there is no return and no ending equity.

**HOT4 and holdup, GLND removed.**
No proven-frozen sessions, so there is no return and no ending equity.

The fill tape the scorer opens is `data/factor_mine/retro_prices/ohlc.parquet`, commit `5f13a4415ea0`, server time 2026-09-25 15:49:10Z via actions_push. That is one file for every session. It is pre-open only for a session whose 09:30 ET is after that server time.

## What the #336 path opens

`src/factor_mine_sequential.run_books` steps each session in `factor_mine_retro.SESSIONS` (2026-08-13 through 2026-09-24). 2026-09-25 is on disk as a later live snapshot and is included in the trace; that walk's date list does not score it.

For session D the walk calls `rows_for_day(D)`, which opens only `data/factor_mine/snapshots/D.json` and keeps rows dated D. Fills come from `load_bar_store`, which reads `data/factor_mine/retro_prices/ohlc.parquet` and then drops bars after D. The prior session is `data/factor_mine/state/<recipe>/<prior>.json` when a file is already there. This audit does not rewrite those files. The published snapshots and the parquet first appear in merge `5f13a4415ea0cfe460155afb3b7677070a2cd188` (the #336 merge).

Those snapshot rows were built earlier in the same rebuild by `factor_mine_retro.materialize` plus `build_panel`. `materialize` copies each packet's last commit whose **committer** time is at or before 09:30 ET. The snapshot `sources` map is that pick for the twelve named inputs. `build_index` then opens the AB checklist (slim, else enriched, else the plain checklist). Sector files are copied into the overlay when their committer time is early enough; `build_panel` does not open the sector folder. `pin_excel_signals` records `excel_bot/suggestions/suggestions.csv` and each final daily note, again by committer time. `build_panel` does not open them. The live price pin `data/factor_mine/prices/D.json` is not what the sequential walk reads. `data/factor_mine/panel.json` is not opened by `rows_for_day` or by `build_panel`; the candidate rows below are the snapshot rows compared with whatever rows for D were in the last pre-open `panel.json`.

Committer timestamps are shown only as the clock the code used. They do not prove the file was on GitHub.

## Server clock

Actions runs were listed per day from 2026-08-01 through 2026-09-26 (4260 distinct head SHAs). The repo events API returned 300 events, from 2026-09-25T06:14:02Z to 2026-09-26T04:07:51Z. Those PushEvent payloads omit the commits array; the push head is present, and that matched 253 SHAs. The retained window starts on 2026-09-25, so it cannot prove an August or earlier-September blob. Those days use the Actions run.

A blob is **proven before the open** when a server time is at or before 09:30 ET. A push-event Actions run (or a PushEvent) after 09:30 means the commit was pushed after the open. Any other run that starts after 09:30 does not prove the commit was late, and it also does not prove it was early, so the blob stays **not proven**.

## Days

| date | proven-frozen | candidate rows vs pre-open panel | inputs that miss |
| --- | --- | --- | --- |
| 2026-08-13 | no | both empty (snapshot scored no names, and the pre-open panel had none for this date) | fill tape pushed_after; snapshot data/factor_mine/snapshots/2026-08-13.json no pre-open copy |
| 2026-08-14 | no | both empty (snapshot scored no names, and the pre-open panel had none for this date) | fill tape pushed_after; snapshot data/factor_mine/snapshots/2026-08-14.json no pre-open copy |
| 2026-08-17 | no | both empty (snapshot scored no names, and the pre-open panel had none for this date) | fill tape pushed_after; snapshot data/factor_mine/snapshots/2026-08-17.json no pre-open copy |
| 2026-08-18 | no | both empty (snapshot scored no names, and the pre-open panel had none for this date) | fill tape pushed_after; snapshot data/factor_mine/snapshots/2026-08-18.json no pre-open copy |
| 2026-08-19 | no | both empty (snapshot scored no names, and the pre-open panel had none for this date) | fill tape pushed_after; snapshot data/factor_mine/snapshots/2026-08-19.json no pre-open copy; predict 01_daily/general/2026-08-19_predict.md no pre-open copy; news 01_daily/news/2026-08-19_actions.json no pre-open copy |
| 2026-08-20 | no | both empty (snapshot scored no names, and the pre-open panel had none for this date) | fill tape pushed_after; snapshot data/factor_mine/snapshots/2026-08-20.json no pre-open copy |
| 2026-08-21 | no | both empty (snapshot scored no names, and the pre-open panel had none for this date) | fill tape pushed_after; snapshot data/factor_mine/snapshots/2026-08-21.json no pre-open copy; news 01_daily/news/2026-08-21_actions.json no pre-open copy |
| 2026-08-24 | no | added 78 (pre-open panel had no row for this date) | fill tape pushed_after; snapshot data/factor_mine/snapshots/2026-08-24.json no pre-open copy; packet data/exports/finviz_2026-08-24.csv no pre-open copy; join data/join/2026-08-24_ranked.csv no pre-open copy (+4 more) |
| 2026-08-25 | no | both empty (snapshot scored no names, and the pre-open panel had none for this date) | fill tape pushed_after; snapshot data/factor_mine/snapshots/2026-08-25.json no pre-open copy; packet data/exports/finviz_2026-08-25.csv no pre-open copy; join data/join/2026-08-25_ranked.csv no pre-open copy (+1 more) |
| 2026-08-26 | no | both empty (snapshot scored no names, and the pre-open panel had none for this date) | fill tape pushed_after; snapshot data/factor_mine/snapshots/2026-08-26.json no pre-open copy; packet 01_daily/map_heat/2026-08-26_map_heat.json different blob |
| 2026-08-27 | no | both empty (snapshot scored no names, and the pre-open panel had none for this date) | fill tape pushed_after; snapshot data/factor_mine/snapshots/2026-08-27.json no pre-open copy; packet 01_daily/map_heat/2026-08-27_map_heat.json no pre-open copy; packet data/exports/finviz_2026-08-27.csv no pre-open copy (+14 more) |
| 2026-08-28 | no | added 98 (pre-open panel had no row for this date) | fill tape pushed_after; snapshot data/factor_mine/snapshots/2026-08-28.json no pre-open copy; news 01_daily/news/2026-08-28_finviz_digest.json no pre-open copy; packet 01_daily/map_heat/2026-08-28_map_heat.json different blob (+1 more) |
| 2026-08-31 | no | added 72 (pre-open panel had no row for this date) | fill tape pushed_after; snapshot data/factor_mine/snapshots/2026-08-31.json no pre-open copy; packet 01_daily/map_heat/2026-08-31_map_heat.json different blob; packet data/exports/finviz_2026-08-31.csv different blob (+15 more) |
| 2026-09-01 | no | both empty (snapshot scored no names, and the pre-open panel had none for this date) | fill tape pushed_after; snapshot data/factor_mine/snapshots/2026-09-01.json no pre-open copy; packet data/exports/finviz_2026-09-01.csv different blob; join data/join/2026-09-01_ranked.csv no pre-open copy (+2 more) |
| 2026-09-02 | no | both empty (snapshot scored no names, and the pre-open panel had none for this date) | fill tape pushed_after; snapshot data/factor_mine/snapshots/2026-09-02.json no pre-open copy; packet 01_daily/map_heat/2026-09-02_map_heat.json different blob; packet 01_daily/catalyst/2026-09-02_dossiers.json no pre-open copy (+18 more) |
| 2026-09-03 | no | both empty (snapshot scored no names, and the pre-open panel had none for this date) | fill tape pushed_after; snapshot data/factor_mine/snapshots/2026-09-03.json no pre-open copy; news 01_daily/news/2026-09-03_finviz_digest.json no pre-open copy; packet 01_daily/map_heat/2026-09-03_map_heat.json different blob (+21 more) |
| 2026-09-04 | no | added 77 (pre-open panel had no row for this date) | fill tape pushed_after; snapshot data/factor_mine/snapshots/2026-09-04.json no pre-open copy; news 01_daily/news/2026-09-04_finviz_digest.json no pre-open copy; packet 01_daily/map_heat/2026-09-04_map_heat.json different blob (+2 more) |
| 2026-09-08 | no | both empty (snapshot scored no names, and the pre-open panel had none for this date) | fill tape pushed_after; snapshot data/factor_mine/snapshots/2026-09-08.json no pre-open copy; packet 01_daily/map_heat/2026-09-08_map_heat.json different blob; packet data/exports/finviz_2026-09-08.csv different blob (+3 more) |
| 2026-09-09 | no | both empty (snapshot scored no names, and the pre-open panel had none for this date) | fill tape pushed_after; snapshot data/factor_mine/snapshots/2026-09-09.json no pre-open copy; news 01_daily/news/2026-09-09_finviz_digest.json no pre-open copy; packet 01_daily/map_heat/2026-09-09_map_heat.json no pre-open copy (+22 more) |
| 2026-09-10 | no | added 82 (pre-open panel had no row for this date) | fill tape pushed_after; snapshot data/factor_mine/snapshots/2026-09-10.json no pre-open copy; news 01_daily/news/2026-09-10_finviz_digest.json no pre-open copy; packet 01_daily/map_heat/2026-09-10_map_heat.json no pre-open copy (+24 more) |
| 2026-09-11 | no | added 74 (pre-open panel had no row for this date) | fill tape pushed_after; snapshot data/factor_mine/snapshots/2026-09-11.json no pre-open copy; news 01_daily/news/2026-09-11_finviz_digest.json no pre-open copy; packet 01_daily/map_heat/2026-09-11_map_heat.json no pre-open copy (+22 more) |
| 2026-09-14 | no | added 62 (pre-open panel had no row for this date) | fill tape pushed_after; snapshot data/factor_mine/snapshots/2026-09-14.json no pre-open copy; news 01_daily/news/2026-09-14_finviz_digest.json no pre-open copy; packet 01_daily/map_heat/2026-09-14_map_heat.json no pre-open copy (+22 more) |
| 2026-09-15 | no | added 64 (pre-open panel had no row for this date) | fill tape pushed_after; snapshot data/factor_mine/snapshots/2026-09-15.json no pre-open copy; news 01_daily/news/2026-09-15_finviz_digest.json different blob; packet 01_daily/map_heat/2026-09-15_map_heat.json no pre-open copy (+26 more) |
| 2026-09-16 | no | added 65 (pre-open panel had no row for this date) | fill tape pushed_after; snapshot data/factor_mine/snapshots/2026-09-16.json no pre-open copy; news 01_daily/news/2026-09-16_finviz_digest.json no pre-open copy; packet 01_daily/map_heat/2026-09-16_map_heat.json no pre-open copy (+26 more) |
| 2026-09-17 | no | added 60 (pre-open panel had no row for this date) | fill tape pushed_after; snapshot data/factor_mine/snapshots/2026-09-17.json no pre-open copy; news 01_daily/news/2026-09-17_finviz_digest.json no pre-open copy; packet 01_daily/map_heat/2026-09-17_map_heat.json no pre-open copy (+22 more) |
| 2026-09-18 | no | added 55 (pre-open panel had no row for this date) | fill tape pushed_after; snapshot data/factor_mine/snapshots/2026-09-18.json no pre-open copy; news 01_daily/news/2026-09-18_finviz_digest.json different blob; packet 01_daily/map_heat/2026-09-18_map_heat.json different blob (+5 more) |
| 2026-09-21 | no | added 62 (pre-open panel had no row for this date) | fill tape pushed_after; snapshot data/factor_mine/snapshots/2026-09-21.json no pre-open copy; packet 01_daily/map_heat/2026-09-21_map_heat.json different blob; packet data/exports/finviz_2026-09-21.csv no pre-open copy (+3 more) |
| 2026-09-22 | no | added 72 (pre-open panel had no row for this date) | fill tape pushed_after; snapshot data/factor_mine/snapshots/2026-09-22.json no pre-open copy; packet 01_daily/map_heat/2026-09-22_map_heat.json different blob; packet data/exports/finviz_2026-09-22.csv no pre-open copy (+5 more) |
| 2026-09-23 | no | added 76 (pre-open panel had no row for this date) | fill tape pushed_after; snapshot data/factor_mine/snapshots/2026-09-23.json no pre-open copy; news 01_daily/news/2026-09-23_finviz_digest.json different blob; packet 01_daily/map_heat/2026-09-23_map_heat.json different blob (+6 more) |
| 2026-09-24 | no | added 74 (pre-open panel had no row for this date) | fill tape pushed_after; snapshot data/factor_mine/snapshots/2026-09-24.json no pre-open copy; news 01_daily/news/2026-09-24_finviz_digest.json different blob; packet 01_daily/map_heat/2026-09-24_map_heat.json different blob (+4 more) |
| 2026-09-25 | no | added 62 (pre-open panel had no row for this date) | fill tape pushed_after; snapshot data/factor_mine/snapshots/2026-09-25.json no pre-open copy; news 01_daily/news/2026-09-25_finviz_digest.json different blob; excel excel_bot/suggestions/suggestions.csv different blob (+1 more) |

## Inputs by day

Sector files are one summary row. A sector day matches only when every non-trace file in that folder matches. `used` is the blob the code selected. `pre-open` is the latest blob with a server time at or before 09:30 ET.

### 2026-08-13

Snapshot label `incomplete_pit`, rows 0, code sha the snapshot records `693b0b078bba`.

| group | path | used | server | verdict | pre-open | match |
| --- | --- | --- | --- | --- | --- | --- |
| snapshot | `data/factor_mine/snapshots/2026-08-13.json` | `5f13a4415ea0` | 2026-09-25 15:49:10Z actions_push | pushed_after | `—` | no pre-open copy |
| panel | `data/factor_mine/panel.json` | `—` | — absent | absent | `—` | both absent |
| news | `01_daily/news/2026-08-13_finviz_digest.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/map_heat/2026-08-13_map_heat.json` | `—` | — absent | absent | `—` | both absent |
| packet | `data/exports/finviz_2026-08-13.csv` | `a5e59afbe710` | 2026-08-13 06:59:47Z actions_head | proven_before | `a5e59afbe710` | same |
| join | `data/join/2026-08-13_ranked.csv` | `bd650e582ff7` | 2026-08-13 10:19:22Z actions_head | proven_before | `bd650e582ff7` | same |
| packet | `01_daily/catalyst/2026-08-13_dossiers.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/map_heat/2026-08-13_research_baseline.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/weather/2026-08-13_weather.json` | `bd650e582ff7` | 2026-08-13 10:19:22Z actions_head | proven_before | `bd650e582ff7` | same |
| predict | `01_daily/general/2026-08-13_predict.md` | `e3bc9af22701` | 2026-08-13 12:28:22Z actions_head | proven_before | `e3bc9af22701` | same |
| news | `01_daily/news/2026-08-13_actions.json` | `7db8368fa607` | 2026-08-13 12:08:54Z actions_head | proven_before | `7db8368fa607` | same |
| news | `01_daily/news/2026-08-13_judge.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/events/2026-08-13_events.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/map_heat/2026-08-13_research.json` | `—` | — absent | absent | `—` | both absent |
| ab | `data/ab_checklist/2026-08-13_ab_checklist.csv` | `—` | — absent | absent | `—` | both absent |
| excel | `excel_bot/suggestions/suggestions.csv` | `—` | — absent | absent | `—` | both absent |
| excel | `excel_bot/daily/2026-08-13_excel_bot.md` | `—` | — absent | absent | `—` | both absent |
| price | `data/factor_mine/prices/2026-08-13.json` | `—` | — absent | absent | `—` | both absent |
| sector | `01_daily/sectors/2026-08-13/` (35 files) | | | ok | | 35 match, 0 miss |

Candidate rows the walk scored (snapshot) versus rows dated this session in the last `panel.json` proven before the open: **same** same=0 added=0 removed=0 changed=0. Fields: —.

### 2026-08-14

Snapshot label `incomplete_pit`, rows 0, code sha the snapshot records `693b0b078bba`.

| group | path | used | server | verdict | pre-open | match |
| --- | --- | --- | --- | --- | --- | --- |
| snapshot | `data/factor_mine/snapshots/2026-08-14.json` | `5f13a4415ea0` | 2026-09-25 15:49:10Z actions_push | pushed_after | `—` | no pre-open copy |
| panel | `data/factor_mine/panel.json` | `—` | — absent | absent | `—` | both absent |
| news | `01_daily/news/2026-08-14_finviz_digest.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/map_heat/2026-08-14_map_heat.json` | `—` | — absent | absent | `—` | both absent |
| packet | `data/exports/finviz_2026-08-14.csv` | `aff60dbd3827` | 2026-08-14 10:55:13Z actions_head | proven_before | `aff60dbd3827` | same |
| join | `data/join/2026-08-14_ranked.csv` | `aff60dbd3827` | 2026-08-14 10:55:13Z actions_head | proven_before | `aff60dbd3827` | same |
| packet | `01_daily/catalyst/2026-08-14_dossiers.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/map_heat/2026-08-14_research_baseline.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/weather/2026-08-14_weather.json` | `aff60dbd3827` | 2026-08-14 10:55:13Z actions_head | proven_before | `aff60dbd3827` | same |
| predict | `01_daily/general/2026-08-14_predict.md` | `6b74bb2d501c` | 2026-08-14 12:24:18Z actions_head | proven_before | `6b74bb2d501c` | same |
| news | `01_daily/news/2026-08-14_actions.json` | `80c34e912385` | 2026-08-14 12:07:06Z actions_head | proven_before | `80c34e912385` | same |
| news | `01_daily/news/2026-08-14_judge.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/events/2026-08-14_events.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/map_heat/2026-08-14_research.json` | `—` | — absent | absent | `—` | both absent |
| ab | `data/ab_checklist/2026-08-14_ab_checklist.csv` | `—` | — absent | absent | `—` | both absent |
| excel | `excel_bot/suggestions/suggestions.csv` | `—` | — absent | absent | `—` | both absent |
| excel | `excel_bot/daily/2026-08-14_excel_bot.md` | `—` | — absent | absent | `—` | both absent |
| price | `data/factor_mine/prices/2026-08-14.json` | `—` | — absent | absent | `—` | both absent |
| sector | `01_daily/sectors/2026-08-14/` (35 files) | | | ok | | 35 match, 0 miss |

Candidate rows the walk scored (snapshot) versus rows dated this session in the last `panel.json` proven before the open: **same** same=0 added=0 removed=0 changed=0. Fields: —.

### 2026-08-17

Snapshot label `incomplete_pit`, rows 0, code sha the snapshot records `693b0b078bba`.

| group | path | used | server | verdict | pre-open | match |
| --- | --- | --- | --- | --- | --- | --- |
| snapshot | `data/factor_mine/snapshots/2026-08-17.json` | `5f13a4415ea0` | 2026-09-25 15:49:10Z actions_push | pushed_after | `—` | no pre-open copy |
| panel | `data/factor_mine/panel.json` | `—` | — absent | absent | `—` | both absent |
| news | `01_daily/news/2026-08-17_finviz_digest.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/map_heat/2026-08-17_map_heat.json` | `—` | — absent | absent | `—` | both absent |
| packet | `data/exports/finviz_2026-08-17.csv` | `b5db5f33fb35` | 2026-08-17 13:24:36Z actions_head | proven_before | `b5db5f33fb35` | same |
| join | `data/join/2026-08-17_ranked.csv` | `b5db5f33fb35` | 2026-08-17 13:24:36Z actions_head | proven_before | `b5db5f33fb35` | same |
| packet | `01_daily/catalyst/2026-08-17_dossiers.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/map_heat/2026-08-17_research_baseline.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/weather/2026-08-17_weather.json` | `b5db5f33fb35` | 2026-08-17 13:24:36Z actions_head | proven_before | `b5db5f33fb35` | same |
| predict | `01_daily/general/2026-08-17_predict.md` | `682d3731365e` | 2026-08-17 11:53:16Z actions_head | proven_before | `682d3731365e` | same |
| news | `01_daily/news/2026-08-17_actions.json` | `b74e206dc8e1` | 2026-08-17 11:49:57Z actions_head | proven_before | `b74e206dc8e1` | same |
| news | `01_daily/news/2026-08-17_judge.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/events/2026-08-17_events.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/map_heat/2026-08-17_research.json` | `—` | — absent | absent | `—` | both absent |
| ab | `data/ab_checklist/2026-08-17_ab_checklist.csv` | `—` | — absent | absent | `—` | both absent |
| excel | `excel_bot/suggestions/suggestions.csv` | `—` | — absent | absent | `—` | both absent |
| excel | `excel_bot/daily/2026-08-17_excel_bot.md` | `—` | — absent | absent | `—` | both absent |
| price | `data/factor_mine/prices/2026-08-17.json` | `—` | — absent | absent | `—` | both absent |
| sector | `01_daily/sectors/2026-08-17/` (35 files) | | | ok | | 35 match, 0 miss |

Candidate rows the walk scored (snapshot) versus rows dated this session in the last `panel.json` proven before the open: **same** same=0 added=0 removed=0 changed=0. Fields: —.

### 2026-08-18

Snapshot label `incomplete_pit`, rows 0, code sha the snapshot records `693b0b078bba`.

| group | path | used | server | verdict | pre-open | match |
| --- | --- | --- | --- | --- | --- | --- |
| snapshot | `data/factor_mine/snapshots/2026-08-18.json` | `5f13a4415ea0` | 2026-09-25 15:49:10Z actions_push | pushed_after | `—` | no pre-open copy |
| panel | `data/factor_mine/panel.json` | `—` | — absent | absent | `—` | both absent |
| news | `01_daily/news/2026-08-18_finviz_digest.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/map_heat/2026-08-18_map_heat.json` | `—` | — absent | absent | `—` | both absent |
| packet | `data/exports/finviz_2026-08-18.csv` | `—` | — absent | absent | `—` | both absent |
| join | `data/join/2026-08-18_ranked.csv` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/catalyst/2026-08-18_dossiers.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/map_heat/2026-08-18_research_baseline.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/weather/2026-08-18_weather.json` | `—` | — absent | absent | `—` | both absent |
| predict | `01_daily/general/2026-08-18_predict.md` | `c74bb5b7b46e` | 2026-08-18 12:19:33Z actions_head | proven_before | `c74bb5b7b46e` | same |
| news | `01_daily/news/2026-08-18_actions.json` | `c9d2d79cb94d` | 2026-08-18 11:50:50Z actions_head | proven_before | `c9d2d79cb94d` | same |
| news | `01_daily/news/2026-08-18_judge.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/events/2026-08-18_events.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/map_heat/2026-08-18_research.json` | `—` | — absent | absent | `—` | both absent |
| ab | `data/ab_checklist/2026-08-18_ab_checklist.csv` | `—` | — absent | absent | `—` | both absent |
| excel | `excel_bot/suggestions/suggestions.csv` | `—` | — absent | absent | `—` | both absent |
| excel | `excel_bot/daily/2026-08-18_excel_bot.md` | `—` | — absent | absent | `—` | both absent |
| price | `data/factor_mine/prices/2026-08-18.json` | `—` | — absent | absent | `—` | both absent |
| sector | `01_daily/sectors/2026-08-18/` (35 files) | | | ok | | 35 match, 0 miss |

Candidate rows the walk scored (snapshot) versus rows dated this session in the last `panel.json` proven before the open: **same** same=0 added=0 removed=0 changed=0. Fields: —.

### 2026-08-19

Snapshot label `incomplete_pit`, rows 0, code sha the snapshot records `693b0b078bba`.

| group | path | used | server | verdict | pre-open | match |
| --- | --- | --- | --- | --- | --- | --- |
| snapshot | `data/factor_mine/snapshots/2026-08-19.json` | `5f13a4415ea0` | 2026-09-25 15:49:10Z actions_push | pushed_after | `—` | no pre-open copy |
| panel | `data/factor_mine/panel.json` | `—` | — absent | absent | `—` | both absent |
| news | `01_daily/news/2026-08-19_finviz_digest.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/map_heat/2026-08-19_map_heat.json` | `—` | — absent | absent | `—` | both absent |
| packet | `data/exports/finviz_2026-08-19.csv` | `—` | — absent | absent | `—` | both absent |
| join | `data/join/2026-08-19_ranked.csv` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/catalyst/2026-08-19_dossiers.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/map_heat/2026-08-19_research_baseline.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/weather/2026-08-19_weather.json` | `—` | — absent | absent | `—` | both absent |
| predict | `01_daily/general/2026-08-19_predict.md` | `b6fc915b0413` | — none | not_proven | `—` | no pre-open copy |
| news | `01_daily/news/2026-08-19_actions.json` | `b7c6bb6d6c8e` | — none | not_proven | `—` | no pre-open copy |
| news | `01_daily/news/2026-08-19_judge.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/events/2026-08-19_events.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/map_heat/2026-08-19_research.json` | `—` | — absent | absent | `—` | both absent |
| ab | `data/ab_checklist/2026-08-19_ab_checklist_enriched.csv` | `—` | — absent | absent | `—` | both absent |
| excel | `excel_bot/suggestions/suggestions.csv` | `—` | — absent | absent | `—` | both absent |
| excel | `excel_bot/daily/2026-08-19_excel_bot.md` | `—` | — absent | absent | `—` | both absent |
| price | `data/factor_mine/prices/2026-08-19.json` | `—` | — absent | absent | `—` | both absent |
| sector | `01_daily/sectors/2026-08-19/` (2 files) | | | ok | | 2 match, 0 miss |

Row diff, blob the code selected versus the proven pre-open blob:

- `01_daily/general/2026-08-19_predict.md`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/news/2026-08-19_actions.json`: added same=0 added=1 removed=0 changed=0 fields=file

Candidate rows the walk scored (snapshot) versus rows dated this session in the last `panel.json` proven before the open: **same** same=0 added=0 removed=0 changed=0. Fields: —.

### 2026-08-20

Snapshot label `incomplete_pit`, rows 0, code sha the snapshot records `693b0b078bba`.

| group | path | used | server | verdict | pre-open | match |
| --- | --- | --- | --- | --- | --- | --- |
| snapshot | `data/factor_mine/snapshots/2026-08-20.json` | `5f13a4415ea0` | 2026-09-25 15:49:10Z actions_push | pushed_after | `—` | no pre-open copy |
| panel | `data/factor_mine/panel.json` | `—` | — absent | absent | `—` | both absent |
| news | `01_daily/news/2026-08-20_finviz_digest.json` | `a30cdcd8fef0` | 2026-08-20 13:08:06Z actions_head | proven_before | `a30cdcd8fef0` | same |
| packet | `01_daily/map_heat/2026-08-20_map_heat.json` | `—` | — absent | absent | `—` | both absent |
| packet | `data/exports/finviz_2026-08-20.csv` | `—` | — absent | absent | `—` | both absent |
| join | `data/join/2026-08-20_ranked.csv` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/catalyst/2026-08-20_dossiers.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/map_heat/2026-08-20_research_baseline.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/weather/2026-08-20_weather.json` | `—` | — absent | absent | `—` | both absent |
| predict | `01_daily/general/2026-08-20_predict.md` | `db5f47399361` | 2026-08-20 12:16:22Z actions_head | proven_before | `db5f47399361` | same |
| news | `01_daily/news/2026-08-20_actions.json` | `c37da9cb4cf6` | 2026-08-20 11:53:13Z actions_head | proven_before | `c37da9cb4cf6` | same |
| news | `01_daily/news/2026-08-20_judge.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/events/2026-08-20_events.json` | `d5d75fb31ecc` | 2026-08-20 11:14:58Z actions_head | proven_before | `d5d75fb31ecc` | same |
| packet | `01_daily/map_heat/2026-08-20_research.json` | `—` | — absent | absent | `—` | both absent |
| ab | `data/ab_checklist/2026-08-20_ab_checklist_enriched.csv` | `—` | — absent | absent | `—` | both absent |
| excel | `excel_bot/suggestions/suggestions.csv` | `—` | — absent | absent | `—` | both absent |
| excel | `excel_bot/daily/2026-08-20_excel_bot.md` | `—` | — absent | absent | `—` | both absent |
| price | `data/factor_mine/prices/2026-08-20.json` | `—` | — absent | absent | `—` | both absent |
| sector | `01_daily/sectors/2026-08-20/` (2 files) | | | ok | | 2 match, 0 miss |

Candidate rows the walk scored (snapshot) versus rows dated this session in the last `panel.json` proven before the open: **same** same=0 added=0 removed=0 changed=0. Fields: —.

### 2026-08-21

Snapshot label `incomplete_pit`, rows 0, code sha the snapshot records `693b0b078bba`.

| group | path | used | server | verdict | pre-open | match |
| --- | --- | --- | --- | --- | --- | --- |
| snapshot | `data/factor_mine/snapshots/2026-08-21.json` | `5f13a4415ea0` | 2026-09-25 15:49:10Z actions_push | pushed_after | `—` | no pre-open copy |
| panel | `data/factor_mine/panel.json` | `—` | — absent | absent | `—` | both absent |
| news | `01_daily/news/2026-08-21_finviz_digest.json` | `e80327d4701e` | 2026-08-21 13:07:26Z actions_head | proven_before | `e80327d4701e` | same |
| packet | `01_daily/map_heat/2026-08-21_map_heat.json` | `—` | — absent | absent | `—` | both absent |
| packet | `data/exports/finviz_2026-08-21.csv` | `—` | — absent | absent | `—` | both absent |
| join | `data/join/2026-08-21_ranked.csv` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/catalyst/2026-08-21_dossiers.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/map_heat/2026-08-21_research_baseline.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/weather/2026-08-21_weather.json` | `—` | — absent | absent | `—` | both absent |
| predict | `01_daily/general/2026-08-21_predict.md` | `5be5f4bbbbc5` | 2026-08-21 12:15:23Z actions_head | proven_before | `5be5f4bbbbc5` | same |
| news | `01_daily/news/2026-08-21_actions.json` | `9c659d7d6037` | — none | not_proven | `—` | no pre-open copy |
| news | `01_daily/news/2026-08-21_judge.json` | `1f9fb2a751d5` | 2026-08-21 13:27:47Z actions_head | proven_before | `1f9fb2a751d5` | same |
| packet | `01_daily/events/2026-08-21_events.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/map_heat/2026-08-21_research.json` | `—` | — absent | absent | `—` | both absent |
| ab | `data/ab_checklist/2026-08-21_ab_checklist_enriched.csv` | `—` | — absent | absent | `—` | both absent |
| excel | `excel_bot/suggestions/suggestions.csv` | `—` | — absent | absent | `—` | both absent |
| excel | `excel_bot/daily/2026-08-21_excel_bot.md` | `—` | — absent | absent | `—` | both absent |
| price | `data/factor_mine/prices/2026-08-21.json` | `—` | — absent | absent | `—` | both absent |
| sector | `01_daily/sectors/2026-08-21/` (35 files) | | | ok | | 35 match, 0 miss |

Row diff, blob the code selected versus the proven pre-open blob:

- `01_daily/news/2026-08-21_actions.json`: added same=0 added=1 removed=0 changed=0 fields=file

Candidate rows the walk scored (snapshot) versus rows dated this session in the last `panel.json` proven before the open: **same** same=0 added=0 removed=0 changed=0. Fields: —.

### 2026-08-24

Snapshot label `pit_rebuilt`, rows 78, code sha the snapshot records `693b0b078bba`.

| group | path | used | server | verdict | pre-open | match |
| --- | --- | --- | --- | --- | --- | --- |
| snapshot | `data/factor_mine/snapshots/2026-08-24.json` | `5f13a4415ea0` | 2026-09-25 15:49:10Z actions_push | pushed_after | `—` | no pre-open copy |
| panel | `data/factor_mine/panel.json` | `—` | — absent | absent | `—` | both absent |
| news | `01_daily/news/2026-08-24_finviz_digest.json` | `dd5f072ec8d1` | 2026-08-24 13:10:23Z actions_head | proven_before | `dd5f072ec8d1` | same |
| packet | `01_daily/map_heat/2026-08-24_map_heat.json` | `—` | — absent | absent | `—` | both absent |
| packet | `data/exports/finviz_2026-08-24.csv` | `645a7b394963` | — none | not_proven | `—` | no pre-open copy |
| join | `data/join/2026-08-24_ranked.csv` | `645a7b394963` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/catalyst/2026-08-24_dossiers.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/map_heat/2026-08-24_research_baseline.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/weather/2026-08-24_weather.json` | `645a7b394963` | — none | not_proven | `—` | no pre-open copy |
| predict | `01_daily/general/2026-08-24_predict.md` | `e2ab857c4bd9` | — none | not_proven | `—` | no pre-open copy |
| news | `01_daily/news/2026-08-24_actions.json` | `dcc42f86c755` | 2026-08-24 11:54:07Z actions_head | proven_before | `dcc42f86c755` | same |
| news | `01_daily/news/2026-08-24_judge.json` | `260c443f1c3b` | 2026-08-24 11:06:53Z actions_head | proven_before | `260c443f1c3b` | same |
| packet | `01_daily/events/2026-08-24_events.json` | `6a7cd4fe157b` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/map_heat/2026-08-24_research.json` | `—` | — absent | absent | `—` | both absent |
| ab | `data/ab_checklist/2026-08-24_ab_checklist_enriched.csv` | `b146f8000479` | 2026-08-24 12:30:01Z actions_head | proven_before | `b146f8000479` | same |
| excel | `excel_bot/suggestions/suggestions.csv` | `—` | — absent | absent | `—` | both absent |
| excel | `excel_bot/daily/2026-08-24_excel_bot.md` | `—` | — absent | absent | `—` | both absent |
| price | `data/factor_mine/prices/2026-08-24.json` | `—` | — absent | absent | `—` | both absent |
| sector | `01_daily/sectors/2026-08-24/` (13 files) | | | ok | | 13 match, 0 miss |

Row diff, blob the code selected versus the proven pre-open blob:

- `data/exports/finviz_2026-08-24.csv`: added same=0 added=11618 removed=0 changed=0 fields=—
- `data/join/2026-08-24_ranked.csv`: added same=0 added=5904 removed=0 changed=0 fields=—
- `01_daily/weather/2026-08-24_weather.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/general/2026-08-24_predict.md`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/events/2026-08-24_events.json`: added same=0 added=1 removed=0 changed=0 fields=file

Candidate rows the walk scored (snapshot) versus rows dated this session in the last `panel.json` proven before the open: **added** same=0 added=78 removed=0 changed=0. Fields: —.
Added tickers (up to 12): ABAT, ABUS, AEM, ALM, ALOY, AMTX, ARCT, ASST, AVAH, BJ, BKKT, BMO.

### 2026-08-25

Snapshot label `incomplete_pit`, rows 0, code sha the snapshot records `693b0b078bba`.

| group | path | used | server | verdict | pre-open | match |
| --- | --- | --- | --- | --- | --- | --- |
| snapshot | `data/factor_mine/snapshots/2026-08-25.json` | `5f13a4415ea0` | 2026-09-25 15:49:10Z actions_push | pushed_after | `—` | no pre-open copy |
| panel | `data/factor_mine/panel.json` | `—` | — absent | absent | `—` | both absent |
| news | `01_daily/news/2026-08-25_finviz_digest.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/map_heat/2026-08-25_map_heat.json` | `—` | — absent | absent | `—` | both absent |
| packet | `data/exports/finviz_2026-08-25.csv` | `8b562adec212` | — none | not_proven | `—` | no pre-open copy |
| join | `data/join/2026-08-25_ranked.csv` | `8b562adec212` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/catalyst/2026-08-25_dossiers.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/map_heat/2026-08-25_research_baseline.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/weather/2026-08-25_weather.json` | `8b562adec212` | — none | not_proven | `—` | no pre-open copy |
| predict | `01_daily/general/2026-08-25_predict.md` | `—` | — absent | absent | `—` | both absent |
| news | `01_daily/news/2026-08-25_actions.json` | `—` | — absent | absent | `—` | both absent |
| news | `01_daily/news/2026-08-25_judge.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/events/2026-08-25_events.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/map_heat/2026-08-25_research.json` | `—` | — absent | absent | `—` | both absent |
| ab | `data/ab_checklist/2026-08-25_ab_checklist_enriched.csv` | `3e0ce83b16f0` | 2026-08-25 13:08:12Z actions_head | proven_before | `3e0ce83b16f0` | same |
| excel | `excel_bot/suggestions/suggestions.csv` | `—` | — absent | absent | `—` | both absent |
| excel | `excel_bot/daily/2026-08-25_excel_bot.md` | `—` | — absent | absent | `—` | both absent |
| price | `data/factor_mine/prices/2026-08-25.json` | `—` | — absent | absent | `—` | both absent |
| sector | `01_daily/sectors/2026-08-25/` (33 files) | | | ok | | 33 match, 0 miss |

Row diff, blob the code selected versus the proven pre-open blob:

- `data/exports/finviz_2026-08-25.csv`: added same=0 added=11625 removed=0 changed=0 fields=—
- `data/join/2026-08-25_ranked.csv`: added same=0 added=5907 removed=0 changed=0 fields=—
- `01_daily/weather/2026-08-25_weather.json`: added same=0 added=1 removed=0 changed=0 fields=file

Candidate rows the walk scored (snapshot) versus rows dated this session in the last `panel.json` proven before the open: **same** same=0 added=0 removed=0 changed=0. Fields: —.

### 2026-08-26

Snapshot label `incomplete_pit`, rows 0, code sha the snapshot records `693b0b078bba`.

| group | path | used | server | verdict | pre-open | match |
| --- | --- | --- | --- | --- | --- | --- |
| snapshot | `data/factor_mine/snapshots/2026-08-26.json` | `5f13a4415ea0` | 2026-09-25 15:49:10Z actions_push | pushed_after | `—` | no pre-open copy |
| panel | `data/factor_mine/panel.json` | `—` | — absent | absent | `—` | both absent |
| news | `01_daily/news/2026-08-26_finviz_digest.json` | `f90f7fdc1991` | 2026-08-26 11:41:05Z actions_head | proven_before | `f90f7fdc1991` | same |
| packet | `01_daily/map_heat/2026-08-26_map_heat.json` | `bbda6b1f07fb` | — none | not_proven | `f90f7fdc1991` | different blob |
| packet | `data/exports/finviz_2026-08-26.csv` | `—` | — absent | absent | `—` | both absent |
| join | `data/join/2026-08-26_ranked.csv` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/catalyst/2026-08-26_dossiers.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/map_heat/2026-08-26_research_baseline.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/weather/2026-08-26_weather.json` | `—` | — absent | absent | `—` | both absent |
| predict | `01_daily/general/2026-08-26_predict.md` | `f90f7fdc1991` | 2026-08-26 11:41:05Z actions_head | proven_before | `f90f7fdc1991` | same |
| news | `01_daily/news/2026-08-26_actions.json` | `f90f7fdc1991` | 2026-08-26 11:41:05Z actions_head | proven_before | `f90f7fdc1991` | same |
| news | `01_daily/news/2026-08-26_judge.json` | `f90f7fdc1991` | 2026-08-26 11:41:05Z actions_head | proven_before | `f90f7fdc1991` | same |
| packet | `01_daily/events/2026-08-26_events.json` | `f90f7fdc1991` | 2026-08-26 11:41:05Z actions_head | proven_before | `f90f7fdc1991` | same |
| packet | `01_daily/map_heat/2026-08-26_research.json` | `f90f7fdc1991` | 2026-08-26 11:41:05Z actions_head | proven_before | `f90f7fdc1991` | same |
| ab | `data/ab_checklist/2026-08-26_ab_checklist.csv` | `—` | — absent | absent | `—` | both absent |
| excel | `excel_bot/suggestions/suggestions.csv` | `—` | — absent | absent | `—` | both absent |
| excel | `excel_bot/daily/2026-08-26_excel_bot.md` | `—` | — absent | absent | `—` | both absent |
| price | `data/factor_mine/prices/2026-08-26.json` | `—` | — absent | absent | `—` | both absent |
| sector | `01_daily/sectors/2026-08-26/` (23 files) | | | ok | | 23 match, 0 miss |

Row diff, blob the code selected versus the proven pre-open blob:

- `01_daily/map_heat/2026-08-26_map_heat.json`: changed same=0 added=0 removed=0 changed=1 fields=file

Candidate rows the walk scored (snapshot) versus rows dated this session in the last `panel.json` proven before the open: **same** same=0 added=0 removed=0 changed=0. Fields: —.

### 2026-08-27

Snapshot label `incomplete_pit`, rows 0, code sha the snapshot records `693b0b078bba`.

| group | path | used | server | verdict | pre-open | match |
| --- | --- | --- | --- | --- | --- | --- |
| snapshot | `data/factor_mine/snapshots/2026-08-27.json` | `5f13a4415ea0` | 2026-09-25 15:49:10Z actions_push | pushed_after | `—` | no pre-open copy |
| panel | `data/factor_mine/panel.json` | `—` | — absent | absent | `—` | both absent |
| news | `01_daily/news/2026-08-27_finviz_digest.json` | `b40850c0842e` | 2026-08-27 06:29:26Z actions_head | proven_before | `b40850c0842e` | same |
| packet | `01_daily/map_heat/2026-08-27_map_heat.json` | `56b4dcbcc8ae` | — none | not_proven | `—` | no pre-open copy |
| packet | `data/exports/finviz_2026-08-27.csv` | `a05223b49749` | — none | not_proven | `—` | no pre-open copy |
| join | `data/join/2026-08-27_ranked.csv` | `b40850c0842e` | 2026-08-27 06:29:26Z actions_head | proven_before | `b40850c0842e` | same |
| packet | `01_daily/catalyst/2026-08-27_dossiers.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/map_heat/2026-08-27_research_baseline.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/weather/2026-08-27_weather.json` | `b40850c0842e` | 2026-08-27 06:29:26Z actions_head | proven_before | `b40850c0842e` | same |
| predict | `01_daily/general/2026-08-27_predict.md` | `—` | — absent | absent | `—` | both absent |
| news | `01_daily/news/2026-08-27_actions.json` | `b40850c0842e` | 2026-08-27 06:29:26Z actions_head | proven_before | `b40850c0842e` | same |
| news | `01_daily/news/2026-08-27_judge.json` | `b40850c0842e` | 2026-08-27 06:29:26Z actions_head | proven_before | `b40850c0842e` | same |
| packet | `01_daily/events/2026-08-27_events.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/map_heat/2026-08-27_research.json` | `—` | — absent | absent | `—` | both absent |
| ab | `data/ab_checklist/2026-08-27_ab_checklist_enriched.csv` | `543979ab7363` | — none | not_proven | `—` | no pre-open copy |
| excel | `excel_bot/suggestions/suggestions.csv` | `—` | — absent | absent | `—` | both absent |
| excel | `excel_bot/daily/2026-08-27_excel_bot.md` | `—` | — absent | absent | `—` | both absent |
| price | `data/factor_mine/prices/2026-08-27.json` | `—` | — absent | absent | `—` | both absent |
| sector | `01_daily/sectors/2026-08-27/` (33 files) | | | fail | | 20 match, 13 miss |

Sector misses:

- `01_daily/sectors/2026-08-27/_BOARD.md` used `69314679d827` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-08-27/_board.json` used `69314679d827` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-08-27/_qc.json` used `69314679d827` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-08-27/basic_materials_predict.md` used `69314679d827` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-08-27/communication_services_predict.md` used `69314679d827` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-08-27/consumer_cyclical_predict.md` used `69314679d827` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-08-27/consumer_defensive_predict.md` used `69314679d827` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-08-27/energy_predict.md` used `69314679d827` not_proven pre-open `—` no pre-open copy
- and 5 more

Row diff, blob the code selected versus the proven pre-open blob:

- `01_daily/map_heat/2026-08-27_map_heat.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `data/exports/finviz_2026-08-27.csv`: added same=0 added=11638 removed=0 changed=0 fields=—
- `data/ab_checklist/2026-08-27_ab_checklist_enriched.csv`: added same=0 added=2698 removed=0 changed=0 fields=—

Candidate rows the walk scored (snapshot) versus rows dated this session in the last `panel.json` proven before the open: **same** same=0 added=0 removed=0 changed=0. Fields: —.

### 2026-08-28

Snapshot label `pit_rebuilt`, rows 98, code sha the snapshot records `693b0b078bba`.

| group | path | used | server | verdict | pre-open | match |
| --- | --- | --- | --- | --- | --- | --- |
| snapshot | `data/factor_mine/snapshots/2026-08-28.json` | `5f13a4415ea0` | 2026-09-25 15:49:10Z actions_push | pushed_after | `—` | no pre-open copy |
| panel | `data/factor_mine/panel.json` | `—` | — absent | absent | `—` | both absent |
| news | `01_daily/news/2026-08-28_finviz_digest.json` | `5d90542b69ff` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/map_heat/2026-08-28_map_heat.json` | `5d90542b69ff` | — none | not_proven | `ca27b701fc87` | different blob |
| packet | `data/exports/finviz_2026-08-28.csv` | `a149893ef5be` | 2026-08-28 05:59:44Z actions_head | proven_before | `a149893ef5be` | same |
| join | `data/join/2026-08-28_ranked.csv` | `a149893ef5be` | 2026-08-28 05:59:44Z actions_head | proven_before | `a149893ef5be` | same |
| packet | `01_daily/catalyst/2026-08-28_dossiers.json` | `411132e8e5ad` | 2026-08-28 08:56:04Z actions_head | proven_before | `411132e8e5ad` | same |
| packet | `01_daily/map_heat/2026-08-28_research_baseline.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/weather/2026-08-28_weather.json` | `a149893ef5be` | 2026-08-28 05:59:44Z actions_head | proven_before | `a149893ef5be` | same |
| predict | `01_daily/general/2026-08-28_predict.md` | `411132e8e5ad` | 2026-08-28 08:56:04Z actions_head | proven_before | `411132e8e5ad` | same |
| news | `01_daily/news/2026-08-28_actions.json` | `411132e8e5ad` | 2026-08-28 08:56:04Z actions_head | proven_before | `411132e8e5ad` | same |
| news | `01_daily/news/2026-08-28_judge.json` | `411132e8e5ad` | 2026-08-28 08:56:04Z actions_head | proven_before | `411132e8e5ad` | same |
| packet | `01_daily/events/2026-08-28_events.json` | `411132e8e5ad` | 2026-08-28 08:56:04Z actions_head | proven_before | `411132e8e5ad` | same |
| packet | `01_daily/map_heat/2026-08-28_research.json` | `411132e8e5ad` | 2026-08-28 08:56:04Z actions_head | proven_before | `411132e8e5ad` | same |
| ab | `data/ab_checklist/2026-08-28_ab_checklist_enriched.csv` | `4794d85793b5` | 2026-08-28 05:49:26Z actions_head | proven_before | `4794d85793b5` | same |
| excel | `excel_bot/suggestions/suggestions.csv` | `—` | — absent | absent | `—` | both absent |
| excel | `excel_bot/daily/2026-08-28_excel_bot.md` | `—` | — absent | absent | `—` | both absent |
| price | `data/factor_mine/prices/2026-08-28.json` | `—` | — absent | absent | `—` | both absent |
| sector | `01_daily/sectors/2026-08-28/` (36 files) | | | ok | | 36 match, 0 miss |

Row diff, blob the code selected versus the proven pre-open blob:

- `01_daily/news/2026-08-28_finviz_digest.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/map_heat/2026-08-28_map_heat.json`: changed same=0 added=0 removed=0 changed=1 fields=file

Candidate rows the walk scored (snapshot) versus rows dated this session in the last `panel.json` proven before the open: **added** same=0 added=98 removed=0 changed=0. Fields: —.
Added tickers (up to 12): ABAT, ABTC, ADCT, ADSK, AFRM, ANF, AVT, BBAR, BBWI, BHVN, BRZE, BTSG.

### 2026-08-31

Snapshot label `pit_rebuilt`, rows 72, code sha the snapshot records `693b0b078bba`.

| group | path | used | server | verdict | pre-open | match |
| --- | --- | --- | --- | --- | --- | --- |
| snapshot | `data/factor_mine/snapshots/2026-08-31.json` | `5f13a4415ea0` | 2026-09-25 15:49:10Z actions_push | pushed_after | `—` | no pre-open copy |
| panel | `data/factor_mine/panel.json` | `—` | — absent | absent | `—` | both absent |
| news | `01_daily/news/2026-08-31_finviz_digest.json` | `4e814505cc45` | 2026-08-31 07:56:13Z actions_head | proven_before | `4e814505cc45` | same |
| packet | `01_daily/map_heat/2026-08-31_map_heat.json` | `6ea2b2a43d45` | — none | not_proven | `7233a38cd5ce` | different blob |
| packet | `data/exports/finviz_2026-08-31.csv` | `454c8e0a5be0` | — none | not_proven | `4e814505cc45` | different blob |
| join | `data/join/2026-08-31_ranked.csv` | `0286e463fc5f` | 2026-08-31 13:04:18Z actions_head | proven_before | `0286e463fc5f` | same |
| packet | `01_daily/catalyst/2026-08-31_dossiers.json` | `0286e463fc5f` | 2026-08-31 13:04:18Z actions_head | proven_before | `0286e463fc5f` | same |
| packet | `01_daily/map_heat/2026-08-31_research_baseline.json` | `4b6eae716771` | 2026-08-28 11:37:52Z actions_head | proven_before | `4b6eae716771` | same |
| packet | `01_daily/weather/2026-08-31_weather.json` | `0286e463fc5f` | 2026-08-31 13:04:18Z actions_head | proven_before | `0286e463fc5f` | same |
| predict | `01_daily/general/2026-08-31_predict.md` | `203ee9f0c3ca` | — none | not_proven | `—` | no pre-open copy |
| news | `01_daily/news/2026-08-31_actions.json` | `203ee9f0c3ca` | — none | not_proven | `—` | no pre-open copy |
| news | `01_daily/news/2026-08-31_judge.json` | `203ee9f0c3ca` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/events/2026-08-31_events.json` | `203ee9f0c3ca` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/map_heat/2026-08-31_research.json` | `—` | — absent | absent | `—` | both absent |
| ab | `data/ab_checklist/2026-08-31_ab_checklist_enriched.csv` | `c48c306a8e1d` | 2026-08-31 11:26:20Z actions_head | proven_before | `c48c306a8e1d` | same |
| excel | `excel_bot/suggestions/suggestions.csv` | `2cc2571f494e` | — none | not_proven | `—` | no pre-open copy |
| excel | `excel_bot/daily/2026-08-31_excel_bot.md` | `—` | — absent | absent | `—` | both absent |
| price | `data/factor_mine/prices/2026-08-31.json` | `—` | — absent | absent | `—` | both absent |
| sector | `01_daily/sectors/2026-08-31/` (11 files) | | | fail | | 2 match, 9 miss |

Sector misses:

- `01_daily/sectors/2026-08-31/_BOARD.md` used `203ee9f0c3ca` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-08-31/_board.json` used `203ee9f0c3ca` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-08-31/consumer_defensive_predict.md` used `203ee9f0c3ca` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-08-31/energy_predict.md` used `203ee9f0c3ca` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-08-31/financial_predict.md` used `203ee9f0c3ca` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-08-31/healthcare_predict.md` used `203ee9f0c3ca` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-08-31/real_estate_predict.md` used `203ee9f0c3ca` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-08-31/technology_predict.md` used `203ee9f0c3ca` not_proven pre-open `—` no pre-open copy
- and 1 more

Row diff, blob the code selected versus the proven pre-open blob:

- `01_daily/map_heat/2026-08-31_map_heat.json`: changed same=0 added=0 removed=0 changed=1 fields=file
- `data/exports/finviz_2026-08-31.csv`: changed same=0 added=9 removed=5 changed=11623 fields=20-Day Simple Moving Average,200-Day Simple Moving Average,50-Day High,50-Day Low,50-Day Simple Moving Average,52-Week High,52-Week Low,52-Week Range,After-Hours Change,After-Hours Close
- `01_daily/general/2026-08-31_predict.md`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/news/2026-08-31_actions.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/news/2026-08-31_judge.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/events/2026-08-31_events.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `excel_bot/suggestions/suggestions.csv`: added same=0 added=16 removed=0 changed=0 fields=—

Candidate rows the walk scored (snapshot) versus rows dated this session in the last `panel.json` proven before the open: **added** same=0 added=72 removed=0 changed=0. Fields: —.
Added tickers (up to 12): ACDC, ACIW, APPN, ARCT, AVPT, BB, BRUN, BRZE, CAN, CDNS, CHKP, CMRC.

### 2026-09-01

Snapshot label `incomplete_pit`, rows 0, code sha the snapshot records `693b0b078bba`.

| group | path | used | server | verdict | pre-open | match |
| --- | --- | --- | --- | --- | --- | --- |
| snapshot | `data/factor_mine/snapshots/2026-09-01.json` | `5f13a4415ea0` | 2026-09-25 15:49:10Z actions_push | pushed_after | `—` | no pre-open copy |
| panel | `data/factor_mine/panel.json` | `—` | — absent | absent | `—` | both absent |
| news | `01_daily/news/2026-09-01_finviz_digest.json` | `e08744600d83` | 2026-09-01 07:36:12Z actions_head | proven_before | `e08744600d83` | same |
| packet | `01_daily/map_heat/2026-09-01_map_heat.json` | `324f0dacf57b` | 2026-09-01 13:25:57Z actions_head | proven_before | `324f0dacf57b` | same |
| packet | `data/exports/finviz_2026-09-01.csv` | `dcc176cdb086` | — none | not_proven | `e08744600d83` | different blob |
| join | `data/join/2026-09-01_ranked.csv` | `dcc176cdb086` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/catalyst/2026-09-01_dossiers.json` | `324f0dacf57b` | 2026-09-01 13:25:57Z actions_head | proven_before | `324f0dacf57b` | same |
| packet | `01_daily/map_heat/2026-09-01_research_baseline.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/weather/2026-09-01_weather.json` | `dcc176cdb086` | — none | not_proven | `—` | no pre-open copy |
| predict | `01_daily/general/2026-09-01_predict.md` | `324f0dacf57b` | 2026-09-01 13:25:57Z actions_head | proven_before | `324f0dacf57b` | same |
| news | `01_daily/news/2026-09-01_actions.json` | `324f0dacf57b` | 2026-09-01 13:25:57Z actions_head | proven_before | `324f0dacf57b` | same |
| news | `01_daily/news/2026-09-01_judge.json` | `324f0dacf57b` | 2026-09-01 13:25:57Z actions_head | proven_before | `324f0dacf57b` | same |
| packet | `01_daily/events/2026-09-01_events.json` | `324f0dacf57b` | 2026-09-01 13:25:57Z actions_head | proven_before | `324f0dacf57b` | same |
| packet | `01_daily/map_heat/2026-09-01_research.json` | `324f0dacf57b` | 2026-09-01 13:25:57Z actions_head | proven_before | `324f0dacf57b` | same |
| ab | `data/ab_checklist/2026-09-01_ab_checklist_enriched.csv` | `5e0ffec92dc3` | 2026-09-01 10:38:27Z actions_head | proven_before | `5e0ffec92dc3` | same |
| excel | `excel_bot/suggestions/suggestions.csv` | `2cc2571f494e` | — none | not_proven | `—` | no pre-open copy |
| excel | `excel_bot/daily/2026-09-01_excel_bot.md` | `—` | — absent | absent | `—` | both absent |
| price | `data/factor_mine/prices/2026-09-01.json` | `—` | — absent | absent | `—` | both absent |
| sector | `01_daily/sectors/2026-09-01/` (14 files) | | | ok | | 14 match, 0 miss |

Row diff, blob the code selected versus the proven pre-open blob:

- `data/exports/finviz_2026-09-01.csv`: changed same=0 added=6 removed=2 changed=11630 fields=20-Day Simple Moving Average,200-Day Simple Moving Average,50-Day High,50-Day Low,50-Day Simple Moving Average,52-Week High,52-Week Low,52-Week Range,After-Hours Change,After-Hours Close
- `data/join/2026-09-01_ranked.csv`: added same=0 added=5913 removed=0 changed=0 fields=—
- `01_daily/weather/2026-09-01_weather.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `excel_bot/suggestions/suggestions.csv`: added same=0 added=16 removed=0 changed=0 fields=—

Candidate rows the walk scored (snapshot) versus rows dated this session in the last `panel.json` proven before the open: **same** same=0 added=0 removed=0 changed=0. Fields: —.

### 2026-09-02

Snapshot label `incomplete_pit`, rows 0, code sha the snapshot records `693b0b078bba`.

| group | path | used | server | verdict | pre-open | match |
| --- | --- | --- | --- | --- | --- | --- |
| snapshot | `data/factor_mine/snapshots/2026-09-02.json` | `5f13a4415ea0` | 2026-09-25 15:49:10Z actions_push | pushed_after | `—` | no pre-open copy |
| panel | `data/factor_mine/panel.json` | `—` | — absent | absent | `—` | both absent |
| news | `01_daily/news/2026-09-02_finviz_digest.json` | `3b576cbaf1be` | 2026-09-02 06:06:45Z actions_head | proven_before | `3b576cbaf1be` | same |
| packet | `01_daily/map_heat/2026-09-02_map_heat.json` | `ae7ae50ae314` | — none | not_proven | `c88b2deb9835` | different blob |
| packet | `data/exports/finviz_2026-09-02.csv` | `3b576cbaf1be` | 2026-09-02 06:06:45Z actions_head | proven_before | `3b576cbaf1be` | same |
| join | `data/join/2026-09-02_ranked.csv` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/catalyst/2026-09-02_dossiers.json` | `5231bc1e5499` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/map_heat/2026-09-02_research_baseline.json` | `9d9e9bfe7473` | — none | not_proven | `30ac803f097b` | different blob |
| packet | `01_daily/weather/2026-09-02_weather.json` | `—` | — absent | absent | `—` | both absent |
| predict | `01_daily/general/2026-09-02_predict.md` | `5231bc1e5499` | — none | not_proven | `—` | no pre-open copy |
| news | `01_daily/news/2026-09-02_actions.json` | `5231bc1e5499` | — none | not_proven | `—` | no pre-open copy |
| news | `01_daily/news/2026-09-02_judge.json` | `5231bc1e5499` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/events/2026-09-02_events.json` | `5231bc1e5499` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/map_heat/2026-09-02_research.json` | `—` | — absent | absent | `—` | both absent |
| ab | `data/ab_checklist/2026-09-02_ab_checklist_enriched.csv` | `—` | — absent | absent | `—` | both absent |
| excel | `excel_bot/suggestions/suggestions.csv` | `5fc8152510be` | — none | not_proven | `—` | no pre-open copy |
| excel | `excel_bot/daily/2026-09-02_excel_bot.md` | `—` | — absent | absent | `—` | both absent |
| price | `data/factor_mine/prices/2026-09-02.json` | `—` | — absent | absent | `—` | both absent |
| sector | `01_daily/sectors/2026-09-02/` (12 files) | | | fail | | 0 match, 12 miss |

Sector misses:

- `01_daily/sectors/2026-09-02/_BOARD.md` used `5231bc1e5499` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-02/_board.json` used `5231bc1e5499` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-02/_qc.json` used `5231bc1e5499` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-02/basic_materials_predict.md` used `5231bc1e5499` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-02/communication_services_predict.md` used `5231bc1e5499` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-02/consumer_cyclical_predict.md` used `5231bc1e5499` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-02/consumer_defensive_predict.md` used `5231bc1e5499` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-02/energy_predict.md` used `5231bc1e5499` not_proven pre-open `—` no pre-open copy
- and 4 more

Row diff, blob the code selected versus the proven pre-open blob:

- `01_daily/map_heat/2026-09-02_map_heat.json`: changed same=0 added=0 removed=0 changed=1 fields=file
- `01_daily/catalyst/2026-09-02_dossiers.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/general/2026-09-02_predict.md`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/news/2026-09-02_actions.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/news/2026-09-02_judge.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/events/2026-09-02_events.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `excel_bot/suggestions/suggestions.csv`: added same=0 added=17 removed=0 changed=0 fields=—

Candidate rows the walk scored (snapshot) versus rows dated this session in the last `panel.json` proven before the open: **same** same=0 added=0 removed=0 changed=0. Fields: —.

### 2026-09-03

Snapshot label `incomplete_pit`, rows 0, code sha the snapshot records `693b0b078bba`.

| group | path | used | server | verdict | pre-open | match |
| --- | --- | --- | --- | --- | --- | --- |
| snapshot | `data/factor_mine/snapshots/2026-09-03.json` | `5f13a4415ea0` | 2026-09-25 15:49:10Z actions_push | pushed_after | `—` | no pre-open copy |
| panel | `data/factor_mine/panel.json` | `—` | — absent | absent | `—` | both absent |
| news | `01_daily/news/2026-09-03_finviz_digest.json` | `27c279c34a54` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/map_heat/2026-09-03_map_heat.json` | `8de70dd7daa9` | — none | not_proven | `87ebeb9045a1` | different blob |
| packet | `data/exports/finviz_2026-09-03.csv` | `27c279c34a54` | — none | not_proven | `—` | no pre-open copy |
| join | `data/join/2026-09-03_ranked.csv` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/catalyst/2026-09-03_dossiers.json` | `70da5272a6b1` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/map_heat/2026-09-03_research_baseline.json` | `87ebeb9045a1` | 2026-09-02 09:47:14Z actions_head | proven_before | `87ebeb9045a1` | same |
| packet | `01_daily/weather/2026-09-03_weather.json` | `—` | — absent | absent | `—` | both absent |
| predict | `01_daily/general/2026-09-03_predict.md` | `70da5272a6b1` | — none | not_proven | `—` | no pre-open copy |
| news | `01_daily/news/2026-09-03_actions.json` | `70da5272a6b1` | — none | not_proven | `—` | no pre-open copy |
| news | `01_daily/news/2026-09-03_judge.json` | `70da5272a6b1` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/events/2026-09-03_events.json` | `70da5272a6b1` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/map_heat/2026-09-03_research.json` | `70da5272a6b1` | — none | not_proven | `—` | no pre-open copy |
| ab | `data/ab_checklist/2026-09-03_ab_checklist_enriched.csv` | `2423ad84eda3` | 2026-09-03 12:47:21Z actions_head | proven_before | `2423ad84eda3` | same |
| excel | `excel_bot/suggestions/suggestions.csv` | `a9ed6a403daa` | 2026-09-02 15:20:56Z actions_head | proven_before | `a9ed6a403daa` | same |
| excel | `excel_bot/daily/2026-09-03_excel_bot.md` | `—` | — absent | absent | `—` | both absent |
| price | `data/factor_mine/prices/2026-09-03.json` | `—` | — absent | absent | `—` | both absent |
| sector | `01_daily/sectors/2026-09-03/` (36 files) | | | fail | | 22 match, 14 miss |

Sector misses:

- `01_daily/sectors/2026-09-03/_BOARD.md` used `70da5272a6b1` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-03/_board.json` used `70da5272a6b1` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-03/_qc.json` used `70da5272a6b1` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-03/basic_materials_predict.md` used `70da5272a6b1` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-03/communication_services_predict.md` used `70da5272a6b1` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-03/consumer_cyclical_predict.md` used `70da5272a6b1` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-03/consumer_defensive_predict.md` used `70da5272a6b1` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-03/energy_predict.md` used `70da5272a6b1` not_proven pre-open `—` no pre-open copy
- and 6 more

Row diff, blob the code selected versus the proven pre-open blob:

- `01_daily/news/2026-09-03_finviz_digest.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/map_heat/2026-09-03_map_heat.json`: changed same=0 added=0 removed=0 changed=1 fields=file
- `data/exports/finviz_2026-09-03.csv`: added same=0 added=11641 removed=0 changed=0 fields=—
- `01_daily/catalyst/2026-09-03_dossiers.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/general/2026-09-03_predict.md`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/news/2026-09-03_actions.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/news/2026-09-03_judge.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/events/2026-09-03_events.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/map_heat/2026-09-03_research.json`: added same=0 added=1 removed=0 changed=0 fields=file

Candidate rows the walk scored (snapshot) versus rows dated this session in the last `panel.json` proven before the open: **same** same=0 added=0 removed=0 changed=0. Fields: —.

### 2026-09-04

Snapshot label `pit_rebuilt`, rows 77, code sha the snapshot records `693b0b078bba`.

| group | path | used | server | verdict | pre-open | match |
| --- | --- | --- | --- | --- | --- | --- |
| snapshot | `data/factor_mine/snapshots/2026-09-04.json` | `5f13a4415ea0` | 2026-09-25 15:49:10Z actions_push | pushed_after | `—` | no pre-open copy |
| panel | `data/factor_mine/panel.json` | `—` | — absent | absent | `—` | both absent |
| news | `01_daily/news/2026-09-04_finviz_digest.json` | `57a60f57e39d` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/map_heat/2026-09-04_map_heat.json` | `1efe10d78b0d` | — none | not_proven | `267235b8cde2` | different blob |
| packet | `data/exports/finviz_2026-09-04.csv` | `57a60f57e39d` | — none | not_proven | `—` | no pre-open copy |
| join | `data/join/2026-09-04_ranked.csv` | `05140caed454` | 2026-09-04 12:54:44Z actions_head | proven_before | `05140caed454` | same |
| packet | `01_daily/catalyst/2026-09-04_dossiers.json` | `5d71ebebd603` | 2026-09-04 12:22:04Z actions_head | proven_before | `5d71ebebd603` | same |
| packet | `01_daily/map_heat/2026-09-04_research_baseline.json` | `267235b8cde2` | 2026-09-04 08:51:18Z actions_head | proven_before | `267235b8cde2` | same |
| packet | `01_daily/weather/2026-09-04_weather.json` | `05140caed454` | 2026-09-04 12:54:44Z actions_head | proven_before | `05140caed454` | same |
| predict | `01_daily/general/2026-09-04_predict.md` | `5d71ebebd603` | 2026-09-04 12:22:04Z actions_head | proven_before | `5d71ebebd603` | same |
| news | `01_daily/news/2026-09-04_actions.json` | `5d71ebebd603` | 2026-09-04 12:22:04Z actions_head | proven_before | `5d71ebebd603` | same |
| news | `01_daily/news/2026-09-04_judge.json` | `5d71ebebd603` | 2026-09-04 12:22:04Z actions_head | proven_before | `5d71ebebd603` | same |
| packet | `01_daily/events/2026-09-04_events.json` | `5d71ebebd603` | 2026-09-04 12:22:04Z actions_head | proven_before | `5d71ebebd603` | same |
| packet | `01_daily/map_heat/2026-09-04_research.json` | `5d71ebebd603` | 2026-09-04 12:22:04Z actions_head | proven_before | `5d71ebebd603` | same |
| ab | `data/ab_checklist/2026-09-04_ab_checklist_enriched.csv` | `5d71ebebd603` | 2026-09-04 12:22:04Z actions_head | proven_before | `5d71ebebd603` | same |
| excel | `excel_bot/suggestions/suggestions.csv` | `1c8354e489fa` | 2026-09-03 16:41:44Z actions_head | proven_before | `1c8354e489fa` | same |
| excel | `excel_bot/daily/2026-09-04_excel_bot.md` | `—` | — absent | absent | `—` | both absent |
| price | `data/factor_mine/prices/2026-09-04.json` | `—` | — absent | absent | `—` | both absent |
| sector | `01_daily/sectors/2026-09-04/` (36 files) | | | ok | | 36 match, 0 miss |

Row diff, blob the code selected versus the proven pre-open blob:

- `01_daily/news/2026-09-04_finviz_digest.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/map_heat/2026-09-04_map_heat.json`: changed same=0 added=0 removed=0 changed=1 fields=file
- `data/exports/finviz_2026-09-04.csv`: added same=0 added=11644 removed=0 changed=0 fields=—

Candidate rows the walk scored (snapshot) versus rows dated this session in the last `panel.json` proven before the open: **added** same=0 added=77 removed=0 changed=0. Fields: —.
Added tickers (up to 12): ABM, ADCT, AHCO, AIIO, ALEC, AMBA, ASAN, ASST, ASTS, ATRC, BAK, BE.

### 2026-09-08

Snapshot label `incomplete_pit`, rows 0, code sha the snapshot records `693b0b078bba`.

| group | path | used | server | verdict | pre-open | match |
| --- | --- | --- | --- | --- | --- | --- |
| snapshot | `data/factor_mine/snapshots/2026-09-08.json` | `5f13a4415ea0` | 2026-09-25 15:49:10Z actions_push | pushed_after | `—` | no pre-open copy |
| panel | `data/factor_mine/panel.json` | `—` | — absent | absent | `—` | both absent |
| news | `01_daily/news/2026-09-08_finviz_digest.json` | `289b087a3715` | 2026-09-08 08:17:51Z actions_head | proven_before | `289b087a3715` | same |
| packet | `01_daily/map_heat/2026-09-08_map_heat.json` | `998aa0d70a1d` | — none | not_proven | `289b087a3715` | different blob |
| packet | `data/exports/finviz_2026-09-08.csv` | `998aa0d70a1d` | — none | not_proven | `289b087a3715` | different blob |
| join | `data/join/2026-09-08_ranked.csv` | `b3f0275dc902` | — none | not_proven | `e42c89e5371e` | different blob |
| packet | `01_daily/catalyst/2026-09-08_dossiers.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/map_heat/2026-09-08_research_baseline.json` | `c09a5a17c20a` | 2026-09-04 22:19:22Z actions_head | proven_before | `c09a5a17c20a` | same |
| packet | `01_daily/weather/2026-09-08_weather.json` | `a30d8019ef1e` | 2026-09-08 12:34:10Z actions_head | proven_before | `a30d8019ef1e` | same |
| predict | `01_daily/general/2026-09-08_predict.md` | `—` | — absent | absent | `—` | both absent |
| news | `01_daily/news/2026-09-08_actions.json` | `—` | — absent | absent | `—` | both absent |
| news | `01_daily/news/2026-09-08_judge.json` | `c72c653dde23` | 2026-09-08 10:13:17Z actions_head | proven_before | `c72c653dde23` | same |
| packet | `01_daily/events/2026-09-08_events.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/map_heat/2026-09-08_research.json` | `—` | — absent | absent | `—` | both absent |
| ab | `data/ab_checklist/2026-09-08_ab_checklist_enriched.csv` | `b658cbb942ef` | — none | not_proven | `—` | no pre-open copy |
| excel | `excel_bot/suggestions/suggestions.csv` | `284a7ad025f9` | — none | not_proven | `1c8354e489fa` | different blob |
| excel | `excel_bot/daily/2026-09-08_excel_bot.md` | `—` | — absent | absent | `—` | both absent |
| price | `data/factor_mine/prices/2026-09-08.json` | `—` | — absent | absent | `—` | both absent |
| sector | `01_daily/sectors/2026-09-08/` (32 files) | | | ok | | 32 match, 0 miss |

Row diff, blob the code selected versus the proven pre-open blob:

- `01_daily/map_heat/2026-09-08_map_heat.json`: changed same=0 added=0 removed=0 changed=1 fields=file
- `data/exports/finviz_2026-09-08.csv`: changed same=0 added=4 removed=1 changed=11605 fields=20-Day Simple Moving Average,200-Day Simple Moving Average,50-Day High,50-Day Low,50-Day Simple Moving Average,52-Week High,52-Week Low,52-Week Range,After-Hours Change,After-Hours Close
- `data/ab_checklist/2026-09-08_ab_checklist_enriched.csv`: added same=0 added=2666 removed=0 changed=0 fields=—
- `excel_bot/suggestions/suggestions.csv`: changed same=0 added=1 removed=0 changed=19 fields=current_price,days_held,first_open,ref_close,ret_vs_close,ret_vs_open,run_date,signal_colors,ticker

Candidate rows the walk scored (snapshot) versus rows dated this session in the last `panel.json` proven before the open: **same** same=0 added=0 removed=0 changed=0. Fields: —.

### 2026-09-09

Snapshot label `incomplete_pit`, rows 0, code sha the snapshot records `693b0b078bba`.

| group | path | used | server | verdict | pre-open | match |
| --- | --- | --- | --- | --- | --- | --- |
| snapshot | `data/factor_mine/snapshots/2026-09-09.json` | `5f13a4415ea0` | 2026-09-25 15:49:10Z actions_push | pushed_after | `—` | no pre-open copy |
| panel | `data/factor_mine/panel.json` | `f5b46d20eec2` | 2026-09-09 07:00:04Z actions_push | proven_before | `f5b46d20eec2` | same |
| news | `01_daily/news/2026-09-09_finviz_digest.json` | `6d998c8b070b` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/map_heat/2026-09-09_map_heat.json` | `fdac38ead3ff` | — none | not_proven | `—` | no pre-open copy |
| packet | `data/exports/finviz_2026-09-09.csv` | `ae4c8b78b1ce` | — none | not_proven | `—` | no pre-open copy |
| join | `data/join/2026-09-09_ranked.csv` | `bb0843209aa8` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/catalyst/2026-09-09_dossiers.json` | `—` | — absent | absent | `—` | both absent |
| packet | `01_daily/map_heat/2026-09-09_research_baseline.json` | `61d36cc223a5` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/weather/2026-09-09_weather.json` | `ae4c8b78b1ce` | — none | not_proven | `3fd1e32455bc` | different blob |
| predict | `01_daily/general/2026-09-09_predict.md` | `—` | — absent | absent | `—` | both absent |
| news | `01_daily/news/2026-09-09_actions.json` | `d97220676334` | — none | not_proven | `—` | no pre-open copy |
| news | `01_daily/news/2026-09-09_judge.json` | `81e0780268d6` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/events/2026-09-09_events.json` | `74557653591d` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/map_heat/2026-09-09_research.json` | `1e1c7a111fee` | — none | not_proven | `—` | no pre-open copy |
| ab | `data/ab_checklist/2026-09-09_ab_checklist_enriched.csv` | `fe69d9de7217` | — none | not_proven | `—` | no pre-open copy |
| excel | `excel_bot/suggestions/suggestions.csv` | `284a7ad025f9` | — none | not_proven | `1c8354e489fa` | different blob |
| excel | `excel_bot/daily/2026-09-09_excel_bot.md` | `—` | — absent | absent | `—` | both absent |
| price | `data/factor_mine/prices/2026-09-09.json` | `—` | — absent | absent | `—` | both absent |
| sector | `01_daily/sectors/2026-09-09/` (36 files) | | | fail | | 24 match, 12 miss |

Sector misses:

- `01_daily/sectors/2026-09-09/_board.json` used `8350d61bfb38` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-09/_qc.json` used `5dac616c43ac` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-09/basic_materials_predict.md` used `5dac616c43ac` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-09/communication_services_predict.md` used `5dac616c43ac` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-09/consumer_cyclical_predict.md` used `5dac616c43ac` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-09/consumer_defensive_predict.md` used `5dac616c43ac` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-09/energy_predict.md` used `5dac616c43ac` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-09/financial_predict.md` used `5dac616c43ac` not_proven pre-open `—` no pre-open copy
- and 4 more

Row diff, blob the code selected versus the proven pre-open blob:

- `01_daily/news/2026-09-09_finviz_digest.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/map_heat/2026-09-09_map_heat.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `data/exports/finviz_2026-09-09.csv`: added same=0 added=11614 removed=0 changed=0 fields=—
- `data/join/2026-09-09_ranked.csv`: added same=0 added=5902 removed=0 changed=0 fields=—
- `01_daily/map_heat/2026-09-09_research_baseline.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/weather/2026-09-09_weather.json`: changed same=0 added=0 removed=0 changed=1 fields=file
- `01_daily/news/2026-09-09_actions.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/news/2026-09-09_judge.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/events/2026-09-09_events.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/map_heat/2026-09-09_research.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `data/ab_checklist/2026-09-09_ab_checklist_enriched.csv`: added same=0 added=2660 removed=0 changed=0 fields=—
- `excel_bot/suggestions/suggestions.csv`: changed same=0 added=1 removed=0 changed=19 fields=current_price,days_held,first_open,ref_close,ret_vs_close,ret_vs_open,run_date,signal_colors,ticker

Candidate rows the walk scored (snapshot) versus rows dated this session in the last `panel.json` proven before the open: **same** same=0 added=0 removed=0 changed=0. Fields: —.

### 2026-09-10

Snapshot label `pit_rebuilt`, rows 82, code sha the snapshot records `693b0b078bba`.

| group | path | used | server | verdict | pre-open | match |
| --- | --- | --- | --- | --- | --- | --- |
| snapshot | `data/factor_mine/snapshots/2026-09-10.json` | `5f13a4415ea0` | 2026-09-25 15:49:10Z actions_push | pushed_after | `—` | no pre-open copy |
| panel | `data/factor_mine/panel.json` | `957fc58e6215` | — none | not_proven | `f5b46d20eec2` | different blob |
| news | `01_daily/news/2026-09-10_finviz_digest.json` | `3368fcb08b37` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/map_heat/2026-09-10_map_heat.json` | `8df95cb6100f` | — none | not_proven | `—` | no pre-open copy |
| packet | `data/exports/finviz_2026-09-10.csv` | `d131ab85595c` | — none | not_proven | `—` | no pre-open copy |
| join | `data/join/2026-09-10_ranked.csv` | `ae85c42c4de4` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/catalyst/2026-09-10_dossiers.json` | `e494580eb95e` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/map_heat/2026-09-10_research_baseline.json` | `2f673806eb18` | 2026-09-09 23:13:55Z actions_head | proven_before | `2f673806eb18` | same |
| packet | `01_daily/weather/2026-09-10_weather.json` | `b98ebfab8872` | — none | not_proven | `—` | no pre-open copy |
| predict | `01_daily/general/2026-09-10_predict.md` | `267017fec350` | — none | not_proven | `—` | no pre-open copy |
| news | `01_daily/news/2026-09-10_actions.json` | `4f0cb97f8f55` | 2026-09-10 11:07:43Z actions_head | proven_before | `4f0cb97f8f55` | same |
| news | `01_daily/news/2026-09-10_judge.json` | `2f07f9d7ba4d` | — none | not_proven | `93b06d05545e` | different blob |
| packet | `01_daily/events/2026-09-10_events.json` | `ce39a72e2cce` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/map_heat/2026-09-10_research.json` | `96e3383d7441` | — none | not_proven | `—` | no pre-open copy |
| ab | `data/ab_checklist/2026-09-10_ab_checklist_enriched.csv` | `a71a6ea8cce4` | — none | not_proven | `—` | no pre-open copy |
| excel | `excel_bot/suggestions/suggestions.csv` | `ee2dc5369963` | — none | not_proven | `1c8354e489fa` | different blob |
| excel | `excel_bot/daily/2026-09-10_excel_bot.md` | `—` | — absent | absent | `—` | both absent |
| price | `data/factor_mine/prices/2026-09-10.json` | `—` | — absent | absent | `—` | both absent |
| sector | `01_daily/sectors/2026-09-10/` (36 files) | | | fail | | 23 match, 13 miss |

Sector misses:

- `01_daily/sectors/2026-09-10/_board.json` used `e89b37d7b4cc` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-10/_qc.json` used `ba62d39f10a6` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-10/basic_materials_predict.md` used `ba62d39f10a6` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-10/communication_services_predict.md` used `ba62d39f10a6` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-10/consumer_cyclical_predict.md` used `ba62d39f10a6` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-10/consumer_defensive_predict.md` used `ba62d39f10a6` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-10/energy_predict.md` used `ba62d39f10a6` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-10/financial_predict.md` used `ba62d39f10a6` not_proven pre-open `—` no pre-open copy
- and 5 more

Row diff, blob the code selected versus the proven pre-open blob:

- `01_daily/news/2026-09-10_finviz_digest.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/map_heat/2026-09-10_map_heat.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `data/exports/finviz_2026-09-10.csv`: added same=0 added=11609 removed=0 changed=0 fields=—
- `data/join/2026-09-10_ranked.csv`: added same=0 added=5902 removed=0 changed=0 fields=—
- `01_daily/catalyst/2026-09-10_dossiers.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/weather/2026-09-10_weather.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/general/2026-09-10_predict.md`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/news/2026-09-10_judge.json`: changed same=0 added=0 removed=0 changed=1 fields=file
- `01_daily/events/2026-09-10_events.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/map_heat/2026-09-10_research.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `data/ab_checklist/2026-09-10_ab_checklist_enriched.csv`: added same=0 added=11613 removed=0 changed=0 fields=—
- `excel_bot/suggestions/suggestions.csv`: changed same=0 added=2 removed=0 changed=19 fields=current_price,days_held,first_open,ref_close,ret_vs_close,ret_vs_open,run_date,signal_colors,ticker

Candidate rows the walk scored (snapshot) versus rows dated this session in the last `panel.json` proven before the open: **added** same=0 added=82 removed=0 changed=0. Fields: —.
Added tickers (up to 12): ADBE, AEO, AESI, AGRO, AIAI, AMBA, AMBQ, AMD, ARBE, ASO, ASX, AVAV.

### 2026-09-11

Snapshot label `pit_rebuilt`, rows 74, code sha the snapshot records `693b0b078bba`.

| group | path | used | server | verdict | pre-open | match |
| --- | --- | --- | --- | --- | --- | --- |
| snapshot | `data/factor_mine/snapshots/2026-09-11.json` | `5f13a4415ea0` | 2026-09-25 15:49:10Z actions_push | pushed_after | `—` | no pre-open copy |
| panel | `data/factor_mine/panel.json` | `957fc58e6215` | — none | not_proven | `f5b46d20eec2` | different blob |
| news | `01_daily/news/2026-09-11_finviz_digest.json` | `1d10caf974ae` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/map_heat/2026-09-11_map_heat.json` | `5aade49f3568` | — none | not_proven | `—` | no pre-open copy |
| packet | `data/exports/finviz_2026-09-11.csv` | `16a0ad086c5e` | — none | not_proven | `—` | no pre-open copy |
| join | `data/join/2026-09-11_ranked.csv` | `8de8a7fb82d1` | — none | not_proven | `dd5b9a33db4a` | different blob |
| packet | `01_daily/catalyst/2026-09-11_dossiers.json` | `8b1eb3ef0beb` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/map_heat/2026-09-11_research_baseline.json` | `855ad2c966d6` | 2026-09-10 23:09:44Z actions_head | proven_before | `855ad2c966d6` | same |
| packet | `01_daily/weather/2026-09-11_weather.json` | `a12960635d30` | — none | not_proven | `dd5b9a33db4a` | different blob |
| predict | `01_daily/general/2026-09-11_predict.md` | `8dd42563c909` | 2026-09-11 09:14:39Z actions_head | proven_before | `8dd42563c909` | same |
| news | `01_daily/news/2026-09-11_actions.json` | `dd5b9a33db4a` | 2026-09-11 09:56:19Z actions_head | proven_before | `dd5b9a33db4a` | same |
| news | `01_daily/news/2026-09-11_judge.json` | `f97dce68295a` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/events/2026-09-11_events.json` | `ae2d4d3887af` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/map_heat/2026-09-11_research.json` | `da1c763dfa5a` | — none | not_proven | `—` | no pre-open copy |
| ab | `data/ab_checklist/2026-09-11_ab_checklist_enriched.csv` | `55b922d43226` | — none | not_proven | `—` | no pre-open copy |
| excel | `excel_bot/suggestions/suggestions.csv` | `4dc97fcbd5d4` | 2026-09-10 16:41:36Z actions_head | proven_before | `4dc97fcbd5d4` | same |
| excel | `excel_bot/daily/2026-09-11_excel_bot.md` | `—` | — absent | absent | `—` | both absent |
| price | `data/factor_mine/prices/2026-09-11.json` | `—` | — absent | absent | `—` | both absent |
| sector | `01_daily/sectors/2026-09-11/` (36 files) | | | fail | | 23 match, 13 miss |

Sector misses:

- `01_daily/sectors/2026-09-11/_board.json` used `ba4e7fcffe9b` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-11/_qc.json` used `d6f9998941be` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-11/basic_materials_predict.md` used `d6f9998941be` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-11/communication_services_predict.md` used `d6f9998941be` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-11/consumer_cyclical_predict.md` used `d6f9998941be` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-11/consumer_defensive_predict.md` used `d6f9998941be` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-11/energy_predict.md` used `d6f9998941be` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-11/financial_predict.md` used `d6f9998941be` not_proven pre-open `—` no pre-open copy
- and 5 more

Row diff, blob the code selected versus the proven pre-open blob:

- `01_daily/news/2026-09-11_finviz_digest.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/map_heat/2026-09-11_map_heat.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `data/exports/finviz_2026-09-11.csv`: added same=0 added=11633 removed=0 changed=0 fields=—
- `data/join/2026-09-11_ranked.csv`: changed same=64 added=1 removed=1 changed=5839 fields=analyst,bears,beta,bulls,detail,earnsurp,ext,families_known,flags,liq
- `01_daily/catalyst/2026-09-11_dossiers.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/weather/2026-09-11_weather.json`: changed same=0 added=0 removed=0 changed=1 fields=file
- `01_daily/news/2026-09-11_judge.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/events/2026-09-11_events.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/map_heat/2026-09-11_research.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `data/ab_checklist/2026-09-11_ab_checklist_enriched.csv`: added same=0 added=2653 removed=0 changed=0 fields=—

Candidate rows the walk scored (snapshot) versus rows dated this session in the last `panel.json` proven before the open: **added** same=0 added=74 removed=0 changed=0. Fields: —.
Added tickers (up to 12): ADBE, AEO, AMTX, ANGX, APH, APPS, ASO, AUPH, AXGN, BAK, BAND, BHVN.

### 2026-09-14

Snapshot label `pit_rebuilt`, rows 62, code sha the snapshot records `693b0b078bba`.

| group | path | used | server | verdict | pre-open | match |
| --- | --- | --- | --- | --- | --- | --- |
| snapshot | `data/factor_mine/snapshots/2026-09-14.json` | `5f13a4415ea0` | 2026-09-25 15:49:10Z actions_push | pushed_after | `—` | no pre-open copy |
| panel | `data/factor_mine/panel.json` | `bd2dffab3d11` | — none | not_proven | `1c7a450b893d` | different blob |
| news | `01_daily/news/2026-09-14_finviz_digest.json` | `7e1874926dd7` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/map_heat/2026-09-14_map_heat.json` | `cfb391108362` | — none | not_proven | `—` | no pre-open copy |
| packet | `data/exports/finviz_2026-09-14.csv` | `f041ce3d9e76` | — none | not_proven | `53363b00c489` | different blob |
| join | `data/join/2026-09-14_ranked.csv` | `15328cf4af4a` | — none | not_proven | `3314a3ed821d` | different blob |
| packet | `01_daily/catalyst/2026-09-14_dossiers.json` | `148656b241ef` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/map_heat/2026-09-14_research_baseline.json` | `ccfb19040671` | 2026-09-11 23:15:05Z actions_head | proven_before | `ccfb19040671` | same |
| packet | `01_daily/weather/2026-09-14_weather.json` | `6322beefa478` | — none | not_proven | `3314a3ed821d` | different blob |
| predict | `01_daily/general/2026-09-14_predict.md` | `200687ab14dc` | — none | not_proven | `—` | no pre-open copy |
| news | `01_daily/news/2026-09-14_actions.json` | `3314a3ed821d` | 2026-09-14 09:40:46Z actions_head | proven_before | `3314a3ed821d` | same |
| news | `01_daily/news/2026-09-14_judge.json` | `40b4b69042ae` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/events/2026-09-14_events.json` | `d3dcaf6f159a` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/map_heat/2026-09-14_research.json` | `f70db3d610b7` | — none | not_proven | `—` | no pre-open copy |
| ab | `data/ab_checklist/2026-09-14_ab_checklist_enriched.csv` | `feadc5890619` | — none | not_proven | `—` | no pre-open copy |
| excel | `excel_bot/suggestions/suggestions.csv` | `081bbcb8067b` | 2026-09-12 15:03:28Z actions_head | proven_before | `081bbcb8067b` | same |
| excel | `excel_bot/daily/2026-09-14_excel_bot.md` | `—` | — absent | absent | `—` | both absent |
| price | `data/factor_mine/prices/2026-09-14.json` | `—` | — absent | absent | `—` | both absent |
| sector | `01_daily/sectors/2026-09-14/` (33 files) | | | fail | | 21 match, 12 miss |

Sector misses:

- `01_daily/sectors/2026-09-14/_board.json` used `93ca17982ac6` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-14/_qc.json` used `782723f046b5` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-14/basic_materials_predict.md` used `782723f046b5` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-14/consumer_cyclical_predict.md` used `782723f046b5` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-14/consumer_defensive_predict.md` used `782723f046b5` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-14/energy_predict.md` used `782723f046b5` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-14/financial_predict.md` used `782723f046b5` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-14/healthcare_predict.md` used `782723f046b5` not_proven pre-open `—` no pre-open copy
- and 4 more

Row diff, blob the code selected versus the proven pre-open blob:

- `01_daily/news/2026-09-14_finviz_digest.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/map_heat/2026-09-14_map_heat.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `data/exports/finviz_2026-09-14.csv`: changed same=9843 added=0 removed=0 changed=1774 fields=20-Day Simple Moving Average,200-Day Simple Moving Average,50-Day High,50-Day Low,50-Day Simple Moving Average,52-Week High,52-Week Low,All-Time High,All-Time Low,Average True Range
- `data/join/2026-09-14_ranked.csv`: changed same=77 added=0 removed=0 changed=5818 fields=analyst,bears,beta,bulls,detail,ext,families_known,flags,liq,mom
- `01_daily/catalyst/2026-09-14_dossiers.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/weather/2026-09-14_weather.json`: changed same=0 added=0 removed=0 changed=1 fields=file
- `01_daily/general/2026-09-14_predict.md`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/news/2026-09-14_judge.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/events/2026-09-14_events.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/map_heat/2026-09-14_research.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `data/ab_checklist/2026-09-14_ab_checklist_enriched.csv`: added same=0 added=2651 removed=0 changed=0 fields=—

Candidate rows the walk scored (snapshot) versus rows dated this session in the last `panel.json` proven before the open: **added** same=0 added=62 removed=0 changed=0. Fields: —.
Added tickers (up to 12): ACVA, ANAB, APH, ARW, ATEC, AVT, AXSM, BAND, BG, BNC, BW, CAN.

### 2026-09-15

Snapshot label `pit_rebuilt`, rows 64, code sha the snapshot records `693b0b078bba`.

| group | path | used | server | verdict | pre-open | match |
| --- | --- | --- | --- | --- | --- | --- |
| snapshot | `data/factor_mine/snapshots/2026-09-15.json` | `5f13a4415ea0` | 2026-09-25 15:49:10Z actions_push | pushed_after | `—` | no pre-open copy |
| panel | `data/factor_mine/panel.json` | `16b65eec717b` | 2026-09-14 21:24:52Z actions_head | proven_before | `16b65eec717b` | same |
| news | `01_daily/news/2026-09-15_finviz_digest.json` | `023274dc0e0d` | — none | not_proven | `b7944cc64aaf` | different blob |
| packet | `01_daily/map_heat/2026-09-15_map_heat.json` | `61b40dd305dc` | — none | not_proven | `—` | no pre-open copy |
| packet | `data/exports/finviz_2026-09-15.csv` | `023274dc0e0d` | — none | not_proven | `143d9c2835b8` | different blob |
| join | `data/join/2026-09-15_ranked.csv` | `8b55350627b6` | — none | not_proven | `6854fab2429d` | different blob |
| packet | `01_daily/catalyst/2026-09-15_dossiers.json` | `6f21490c8479` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/map_heat/2026-09-15_research_baseline.json` | `449e7338dc07` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/weather/2026-09-15_weather.json` | `a9226f000d77` | — none | not_proven | `520a7e36075e` | different blob |
| predict | `01_daily/general/2026-09-15_predict.md` | `677dffc6cee4` | — none | not_proven | `—` | no pre-open copy |
| news | `01_daily/news/2026-09-15_actions.json` | `173141d96113` | — none | not_proven | `520a7e36075e` | different blob |
| news | `01_daily/news/2026-09-15_judge.json` | `27d38f3077e3` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/events/2026-09-15_events.json` | `4b32f64b91df` | — none | not_proven | `3a2cd95daf6f` | different blob |
| packet | `01_daily/map_heat/2026-09-15_research.json` | `c1e614c2b568` | 2026-09-15 13:30:03Z actions_head | not_proven | `—` | no pre-open copy |
| ab | `data/ab_checklist/2026-09-15_ab_checklist_enriched.csv` | `a3d6cd1626f5` | — none | not_proven | `—` | no pre-open copy |
| excel | `excel_bot/suggestions/suggestions.csv` | `b4f6731e73ef` | — none | not_proven | `081bbcb8067b` | different blob |
| excel | `excel_bot/daily/2026-09-15_excel_bot.md` | `—` | — absent | absent | `—` | both absent |
| price | `data/factor_mine/prices/2026-09-15.json` | `—` | — absent | absent | `—` | both absent |
| sector | `01_daily/sectors/2026-09-15/` (34 files) | | | fail | | 21 match, 13 miss |

Sector misses:

- `01_daily/sectors/2026-09-15/_BOARD.md` used `5757baca51d4` not_proven pre-open `520a7e36075e` different blob
- `01_daily/sectors/2026-09-15/_board.json` used `90f9fa79585d` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-15/_qc.json` used `5757baca51d4` not_proven pre-open `520a7e36075e` different blob
- `01_daily/sectors/2026-09-15/basic_materials_predict.md` used `5757baca51d4` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-15/communication_services_predict.md` used `5757baca51d4` not_proven pre-open `520a7e36075e` different blob
- `01_daily/sectors/2026-09-15/consumer_cyclical_predict.md` used `5757baca51d4` not_proven pre-open `520a7e36075e` different blob
- `01_daily/sectors/2026-09-15/energy_predict.md` used `5757baca51d4` not_proven pre-open `520a7e36075e` different blob
- `01_daily/sectors/2026-09-15/financial_predict.md` used `5757baca51d4` not_proven pre-open `520a7e36075e` different blob
- and 5 more

Row diff, blob the code selected versus the proven pre-open blob:

- `01_daily/news/2026-09-15_finviz_digest.json`: changed same=0 added=0 removed=0 changed=1 fields=file
- `01_daily/map_heat/2026-09-15_map_heat.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `data/exports/finviz_2026-09-15.csv`: changed same=0 added=18 removed=1 changed=11616 fields=20-Day Simple Moving Average,200-Day Simple Moving Average,50-Day High,50-Day Low,50-Day Simple Moving Average,52-Week High,52-Week Low,All-Time High,All-Time Low,Analyst Recom
- `data/join/2026-09-15_ranked.csv`: changed same=157 added=0 removed=0 changed=5740 fields=bears,bulls,detail,earn,earnsurp,ext,families_known,flags,liq,mom
- `01_daily/catalyst/2026-09-15_dossiers.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/map_heat/2026-09-15_research_baseline.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/weather/2026-09-15_weather.json`: changed same=0 added=0 removed=0 changed=1 fields=file
- `01_daily/general/2026-09-15_predict.md`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/news/2026-09-15_actions.json`: changed same=0 added=0 removed=0 changed=1 fields=file
- `01_daily/news/2026-09-15_judge.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/events/2026-09-15_events.json`: changed same=0 added=0 removed=0 changed=1 fields=file
- `01_daily/map_heat/2026-09-15_research.json`: added same=0 added=1 removed=0 changed=0 fields=file
- and 2 more files

Candidate rows the walk scored (snapshot) versus rows dated this session in the last `panel.json` proven before the open: **added** same=0 added=64 removed=0 changed=0. Fields: —.
Added tickers (up to 12): APH, ARQQ, ATEC, ATRC, BAND, BBY, CLNE, CNTB, CRWD, CYPH, DBI, ECO.

### 2026-09-16

Snapshot label `pit_rebuilt`, rows 65, code sha the snapshot records `693b0b078bba`.

| group | path | used | server | verdict | pre-open | match |
| --- | --- | --- | --- | --- | --- | --- |
| snapshot | `data/factor_mine/snapshots/2026-09-16.json` | `5f13a4415ea0` | 2026-09-25 15:49:10Z actions_push | pushed_after | `—` | no pre-open copy |
| panel | `data/factor_mine/panel.json` | `284ca0e3a187` | 2026-09-15 21:22:24Z actions_head | proven_before | `284ca0e3a187` | same |
| news | `01_daily/news/2026-09-16_finviz_digest.json` | `d513744be8ea` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/map_heat/2026-09-16_map_heat.json` | `eaddc995cb5e` | — none | not_proven | `—` | no pre-open copy |
| packet | `data/exports/finviz_2026-09-16.csv` | `37e737b0168f` | — none | not_proven | `—` | no pre-open copy |
| join | `data/join/2026-09-16_ranked.csv` | `5cdd89790cba` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/catalyst/2026-09-16_dossiers.json` | `c81e9078e808` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/map_heat/2026-09-16_research_baseline.json` | `375bd544711b` | 2026-09-16 00:11:33Z actions_head | proven_before | `375bd544711b` | same |
| packet | `01_daily/weather/2026-09-16_weather.json` | `3b12ea1f9509` | — none | not_proven | `—` | no pre-open copy |
| predict | `01_daily/general/2026-09-16_predict.md` | `f3ae3982ed23` | — none | not_proven | `—` | no pre-open copy |
| news | `01_daily/news/2026-09-16_actions.json` | `f9def0f7908b` | — none | not_proven | `—` | no pre-open copy |
| news | `01_daily/news/2026-09-16_judge.json` | `914adc1e8ec4` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/events/2026-09-16_events.json` | `38cef4949f15` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/map_heat/2026-09-16_research.json` | `1c1a1c7c27e9` | — none | not_proven | `—` | no pre-open copy |
| ab | `data/ab_checklist/2026-09-16_ab_checklist_enriched.csv` | `00b87d331715` | — none | not_proven | `—` | no pre-open copy |
| excel | `excel_bot/suggestions/suggestions.csv` | `4c7aaead87af` | — none | not_proven | `081bbcb8067b` | different blob |
| excel | `excel_bot/daily/2026-09-16_excel_bot.md` | `—` | — absent | absent | `—` | both absent |
| price | `data/factor_mine/prices/2026-09-16.json` | `—` | — absent | absent | `—` | both absent |
| sector | `01_daily/sectors/2026-09-16/` (36 files) | | | fail | | 22 match, 14 miss |

Sector misses:

- `01_daily/sectors/2026-09-16/_BOARD.md` used `8f43ae21ce6c` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-16/_board.json` used `8f43ae21ce6c` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-16/_qc.json` used `8f43ae21ce6c` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-16/basic_materials_predict.md` used `8f43ae21ce6c` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-16/communication_services_predict.md` used `8f43ae21ce6c` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-16/consumer_cyclical_predict.md` used `8f43ae21ce6c` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-16/consumer_defensive_predict.md` used `8f43ae21ce6c` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-16/energy_predict.md` used `8f43ae21ce6c` not_proven pre-open `—` no pre-open copy
- and 6 more

Row diff, blob the code selected versus the proven pre-open blob:

- `01_daily/news/2026-09-16_finviz_digest.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/map_heat/2026-09-16_map_heat.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `data/exports/finviz_2026-09-16.csv`: added same=0 added=11641 removed=0 changed=0 fields=—
- `data/join/2026-09-16_ranked.csv`: added same=0 added=5900 removed=0 changed=0 fields=—
- `01_daily/catalyst/2026-09-16_dossiers.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/weather/2026-09-16_weather.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/general/2026-09-16_predict.md`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/news/2026-09-16_actions.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/news/2026-09-16_judge.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/events/2026-09-16_events.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/map_heat/2026-09-16_research.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `data/ab_checklist/2026-09-16_ab_checklist_enriched.csv`: added same=0 added=2657 removed=0 changed=0 fields=—
- and 1 more files

Candidate rows the walk scored (snapshot) versus rows dated this session in the last `panel.json` proven before the open: **added** same=0 added=65 removed=0 changed=0. Fields: —.
Added tickers (up to 12): ADPT, ALHC, ALMU, APH, ARQQ, AVAH, BBNX, BLFS, CAI, CRBP, CRUS, CRWD.

### 2026-09-17

Snapshot label `pit_rebuilt`, rows 60, code sha the snapshot records `693b0b078bba`.

| group | path | used | server | verdict | pre-open | match |
| --- | --- | --- | --- | --- | --- | --- |
| snapshot | `data/factor_mine/snapshots/2026-09-17.json` | `5f13a4415ea0` | 2026-09-25 15:49:10Z actions_push | pushed_after | `—` | no pre-open copy |
| panel | `data/factor_mine/panel.json` | `5391bbe581c0` | 2026-09-16 23:37:11Z actions_head | proven_before | `5391bbe581c0` | same |
| news | `01_daily/news/2026-09-17_finviz_digest.json` | `b557150e6bd2` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/map_heat/2026-09-17_map_heat.json` | `aae4cc02aa85` | — none | not_proven | `—` | no pre-open copy |
| packet | `data/exports/finviz_2026-09-17.csv` | `56c2362d7f1b` | — none | not_proven | `—` | no pre-open copy |
| join | `data/join/2026-09-17_ranked.csv` | `8d00641d9f5b` | — none | not_proven | `a36bbb19872b` | different blob |
| packet | `01_daily/catalyst/2026-09-17_dossiers.json` | `d15894cb6f37` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/map_heat/2026-09-17_research_baseline.json` | `f2bf504610c0` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/weather/2026-09-17_weather.json` | `9d7cc4a7a6d3` | — none | not_proven | `a36bbb19872b` | different blob |
| predict | `01_daily/general/2026-09-17_predict.md` | `e7939a81e64f` | — none | not_proven | `—` | no pre-open copy |
| news | `01_daily/news/2026-09-17_actions.json` | `a36bbb19872b` | 2026-09-17 09:08:38Z actions_head | proven_before | `a36bbb19872b` | same |
| news | `01_daily/news/2026-09-17_judge.json` | `94b445bf2f78` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/events/2026-09-17_events.json` | `72a4f7a0b422` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/map_heat/2026-09-17_research.json` | `f395bcc05198` | — none | not_proven | `—` | no pre-open copy |
| ab | `data/ab_checklist/2026-09-17_ab_checklist_enriched.csv` | `f649d669211b` | — none | not_proven | `—` | no pre-open copy |
| excel | `excel_bot/suggestions/suggestions.csv` | `a20ec7070f8f` | — none | not_proven | `e8a64ba8debd` | different blob |
| excel | `excel_bot/daily/2026-09-17_excel_bot.md` | `—` | — absent | absent | `—` | both absent |
| price | `data/factor_mine/prices/2026-09-17.json` | `—` | — absent | absent | `—` | both absent |
| sector | `01_daily/sectors/2026-09-17/` (36 files) | | | fail | | 26 match, 10 miss |

Sector misses:

- `01_daily/sectors/2026-09-17/_qc.json` used `97356efc8f88` not_proven pre-open `3aff2b803bfc` different blob
- `01_daily/sectors/2026-09-17/communication_services_predict.md` used `bce9a035b9cd` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-17/consumer_cyclical_predict.md` used `d0fe9a9c1f50` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-17/consumer_defensive_predict.md` used `cb94ce4d1064` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-17/energy_predict.md` used `ad33fdc11f71` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-17/healthcare_predict.md` used `4c1626077c8e` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-17/industrials_predict.md` used `e0ab4f2d597b` not_proven pre-open `—` no pre-open copy
- `01_daily/sectors/2026-09-17/real_estate_predict.md` used `2bebda8af2fe` not_proven pre-open `—` no pre-open copy
- and 2 more

Row diff, blob the code selected versus the proven pre-open blob:

- `01_daily/news/2026-09-17_finviz_digest.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/map_heat/2026-09-17_map_heat.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `data/exports/finviz_2026-09-17.csv`: added same=0 added=11651 removed=0 changed=0 fields=—
- `data/join/2026-09-17_ranked.csv`: changed same=1 added=1 removed=1 changed=5899 fields=bears,beta,bulls,detail,earn,earnsurp,ext,families_known,flags,liq
- `01_daily/catalyst/2026-09-17_dossiers.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/map_heat/2026-09-17_research_baseline.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/weather/2026-09-17_weather.json`: changed same=0 added=0 removed=0 changed=1 fields=file
- `01_daily/general/2026-09-17_predict.md`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/news/2026-09-17_judge.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/events/2026-09-17_events.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/map_heat/2026-09-17_research.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `data/ab_checklist/2026-09-17_ab_checklist_enriched.csv`: added same=0 added=2653 removed=0 changed=0 fields=—
- and 1 more files

Candidate rows the walk scored (snapshot) versus rows dated this session in the last `panel.json` proven before the open: **added** same=0 added=60 removed=0 changed=0. Fields: —.
Added tickers (up to 12): ADPT, AIB, ALHC, ALMU, AMN, AMRX, ANGI, APH, ARQT, ATRC, AXTI, BAK.

### 2026-09-18

Snapshot label `pit_rebuilt`, rows 55, code sha the snapshot records `693b0b078bba`.

| group | path | used | server | verdict | pre-open | match |
| --- | --- | --- | --- | --- | --- | --- |
| snapshot | `data/factor_mine/snapshots/2026-09-18.json` | `5f13a4415ea0` | 2026-09-25 15:49:10Z actions_push | pushed_after | `—` | no pre-open copy |
| panel | `data/factor_mine/panel.json` | `29ee926301d3` | 2026-09-17 23:23:36Z actions_head | proven_before | `29ee926301d3` | same |
| news | `01_daily/news/2026-09-18_finviz_digest.json` | `f399c5edf74d` | — none | not_proven | `03a5d480c0d6` | different blob |
| packet | `01_daily/map_heat/2026-09-18_map_heat.json` | `fa212be29150` | — none | not_proven | `b7051e94c4ce` | different blob |
| packet | `data/exports/finviz_2026-09-18.csv` | `71eb086c26a8` | — none | not_proven | `—` | no pre-open copy |
| join | `data/join/2026-09-18_ranked.csv` | `0e58d6e318d7` | 2026-09-18 10:43:19Z actions_head | proven_before | `0e58d6e318d7` | same |
| packet | `01_daily/catalyst/2026-09-18_dossiers.json` | `b049997bc96b` | 2026-09-18 11:23:00Z actions_head | proven_before | `b049997bc96b` | same |
| packet | `01_daily/map_heat/2026-09-18_research_baseline.json` | `96a854bc9abd` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/weather/2026-09-18_weather.json` | `b049997bc96b` | 2026-09-18 11:23:00Z actions_head | proven_before | `b049997bc96b` | same |
| predict | `01_daily/general/2026-09-18_predict.md` | `ccdfe58999c9` | 2026-09-18 08:37:28Z actions_head | proven_before | `ccdfe58999c9` | same |
| news | `01_daily/news/2026-09-18_actions.json` | `a86c8424bbe7` | 2026-09-18 09:05:15Z actions_head | proven_before | `a86c8424bbe7` | same |
| news | `01_daily/news/2026-09-18_judge.json` | `2f64e7f95d17` | 2026-09-18 08:29:47Z actions_head | proven_before | `2f64e7f95d17` | same |
| packet | `01_daily/events/2026-09-18_events.json` | `eb517b379f75` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/map_heat/2026-09-18_research.json` | `474afd15eb68` | 2026-09-18 08:55:25Z actions_head | proven_before | `474afd15eb68` | same |
| ab | `data/ab_checklist/2026-09-18_ab_checklist_enriched.csv` | `3c5050c91abb` | 2026-09-18 08:17:16Z actions_head | proven_before | `3c5050c91abb` | same |
| excel | `excel_bot/suggestions/suggestions.csv` | `46ac3337a72d` | — none | not_proven | `c9c7022d6ae4` | different blob |
| excel | `excel_bot/daily/2026-09-18_excel_bot.md` | `—` | — absent | absent | `—` | both absent |
| price | `data/factor_mine/prices/2026-09-18.json` | `—` | — absent | absent | `—` | both absent |
| sector | `01_daily/sectors/2026-09-18/` (36 files) | | | ok | | 36 match, 0 miss |

Row diff, blob the code selected versus the proven pre-open blob:

- `01_daily/news/2026-09-18_finviz_digest.json`: changed same=0 added=0 removed=0 changed=1 fields=file
- `01_daily/map_heat/2026-09-18_map_heat.json`: changed same=0 added=0 removed=0 changed=1 fields=file
- `data/exports/finviz_2026-09-18.csv`: added same=0 added=11651 removed=0 changed=0 fields=—
- `01_daily/map_heat/2026-09-18_research_baseline.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/events/2026-09-18_events.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `excel_bot/suggestions/suggestions.csv`: changed same=2 added=0 removed=0 changed=25 fields=current_price,ret_vs_close,ret_vs_open

Candidate rows the walk scored (snapshot) versus rows dated this session in the last `panel.json` proven before the open: **added** same=0 added=55 removed=0 changed=0. Fields: —.
Added tickers (up to 12): ABSI, ALMU, AMD, ARE, ARQT, ATRC, BHVN, BNC, BRKR, CERT, CROX, CRWD.

### 2026-09-21

Snapshot label `pit_rebuilt`, rows 62, code sha the snapshot records `693b0b078bba`.

| group | path | used | server | verdict | pre-open | match |
| --- | --- | --- | --- | --- | --- | --- |
| snapshot | `data/factor_mine/snapshots/2026-09-21.json` | `5f13a4415ea0` | 2026-09-25 15:49:10Z actions_push | pushed_after | `—` | no pre-open copy |
| panel | `data/factor_mine/panel.json` | `6e140da069f2` | 2026-09-19 18:26:01Z actions_head | proven_before | `6e140da069f2` | same |
| news | `01_daily/news/2026-09-21_finviz_digest.json` | `00b9379e4d6f` | 2026-09-21 10:53:52Z actions_head | proven_before | `00b9379e4d6f` | same |
| packet | `01_daily/map_heat/2026-09-21_map_heat.json` | `b86861a46d73` | — none | not_proven | `d482c38afdad` | different blob |
| packet | `data/exports/finviz_2026-09-21.csv` | `be0497cd71d5` | — none | not_proven | `—` | no pre-open copy |
| join | `data/join/2026-09-21_ranked.csv` | `90c7febae1ba` | 2026-09-21 10:53:02Z actions_head | proven_before | `90c7febae1ba` | same |
| packet | `01_daily/catalyst/2026-09-21_dossiers.json` | `3e0975466168` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/map_heat/2026-09-21_research_baseline.json` | `7f1aa17cd113` | 2026-09-18 23:12:05Z actions_head | proven_before | `7f1aa17cd113` | same |
| packet | `01_daily/weather/2026-09-21_weather.json` | `80a2abec3bc6` | 2026-09-21 10:52:57Z actions_head | proven_before | `80a2abec3bc6` | same |
| predict | `01_daily/general/2026-09-21_predict.md` | `35e6e34f8d34` | 2026-09-21 08:40:33Z actions_head | proven_before | `35e6e34f8d34` | same |
| news | `01_daily/news/2026-09-21_actions.json` | `b114756f9fb6` | 2026-09-21 09:05:31Z actions_head | proven_before | `b114756f9fb6` | same |
| news | `01_daily/news/2026-09-21_judge.json` | `ce2d2ae8d0be` | 2026-09-21 08:35:36Z actions_head | proven_before | `ce2d2ae8d0be` | same |
| packet | `01_daily/events/2026-09-21_events.json` | `c9efd3b55058` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/map_heat/2026-09-21_research.json` | `cee2feabb91a` | 2026-09-21 08:58:22Z actions_head | proven_before | `cee2feabb91a` | same |
| ab | `data/ab_checklist/2026-09-21_ab_checklist_enriched.csv` | `cf81555c0ccc` | 2026-09-21 10:53:40Z actions_head | proven_before | `cf81555c0ccc` | same |
| excel | `excel_bot/suggestions/suggestions.csv` | `f416f7c78ee4` | 2026-09-19 15:26:02Z actions_head | proven_before | `f416f7c78ee4` | same |
| excel | `excel_bot/daily/2026-09-21_excel_bot.md` | `—` | — absent | absent | `—` | both absent |
| price | `data/factor_mine/prices/2026-09-21.json` | `—` | — absent | absent | `—` | both absent |
| sector | `01_daily/sectors/2026-09-21/` (36 files) | | | ok | | 36 match, 0 miss |

Row diff, blob the code selected versus the proven pre-open blob:

- `01_daily/map_heat/2026-09-21_map_heat.json`: changed same=0 added=0 removed=0 changed=1 fields=file
- `data/exports/finviz_2026-09-21.csv`: added same=0 added=11643 removed=0 changed=0 fields=—
- `01_daily/catalyst/2026-09-21_dossiers.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/events/2026-09-21_events.json`: added same=0 added=1 removed=0 changed=0 fields=file

Candidate rows the walk scored (snapshot) versus rows dated this session in the last `panel.json` proven before the open: **added** same=0 added=62 removed=0 changed=0. Fields: —.
Added tickers (up to 12): A, ABSI, ABTC, ABVX, AEHL, ALMU, AMD, AMTX, ASST, BKKT, BTBT, BTDR.

### 2026-09-22

Snapshot label `pit_rebuilt`, rows 72, code sha the snapshot records `693b0b078bba`.

| group | path | used | server | verdict | pre-open | match |
| --- | --- | --- | --- | --- | --- | --- |
| snapshot | `data/factor_mine/snapshots/2026-09-22.json` | `5f13a4415ea0` | 2026-09-25 15:49:10Z actions_push | pushed_after | `—` | no pre-open copy |
| panel | `data/factor_mine/panel.json` | `087d5fda408d` | 2026-09-21 21:51:31Z actions_head | proven_before | `087d5fda408d` | same |
| news | `01_daily/news/2026-09-22_finviz_digest.json` | `4d822a3cb658` | 2026-09-22 11:10:03Z actions_head | proven_before | `4d822a3cb658` | same |
| packet | `01_daily/map_heat/2026-09-22_map_heat.json` | `b1294429389c` | — none | not_proven | `37f0e3a1fd7e` | different blob |
| packet | `data/exports/finviz_2026-09-22.csv` | `ece79fbb5809` | — none | not_proven | `—` | no pre-open copy |
| join | `data/join/2026-09-22_ranked.csv` | `97cddc5b867e` | 2026-09-22 11:05:15Z actions_head | proven_before | `97cddc5b867e` | same |
| packet | `01_daily/catalyst/2026-09-22_dossiers.json` | `fb889f32d698` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/map_heat/2026-09-22_research_baseline.json` | `d828e88cdcc9` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/weather/2026-09-22_weather.json` | `ada7b22335d5` | 2026-09-22 11:45:19Z actions_head | proven_before | `ada7b22335d5` | same |
| predict | `01_daily/general/2026-09-22_predict.md` | `4e3d46749663` | 2026-09-22 08:39:44Z actions_head | proven_before | `4e3d46749663` | same |
| news | `01_daily/news/2026-09-22_actions.json` | `b4b6c72f853a` | 2026-09-22 09:05:41Z actions_head | proven_before | `b4b6c72f853a` | same |
| news | `01_daily/news/2026-09-22_judge.json` | `77eb08342a6e` | 2026-09-22 08:35:32Z actions_head | proven_before | `77eb08342a6e` | same |
| packet | `01_daily/events/2026-09-22_events.json` | `bba7b0d1a5ac` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/map_heat/2026-09-22_research.json` | `1515bba5c623` | 2026-09-22 08:57:30Z actions_head | proven_before | `1515bba5c623` | same |
| ab | `data/ab_checklist/2026-09-22_ab_checklist_enriched.csv` | `cb8398c279ab` | 2026-09-22 12:25:39Z actions_head | proven_before | `cb8398c279ab` | same |
| excel | `excel_bot/suggestions/suggestions.csv` | `abcfeaf8bed7` | — none | not_proven | `f416f7c78ee4` | different blob |
| excel | `excel_bot/daily/2026-09-22_excel_bot.md` | `—` | — absent | absent | `—` | both absent |
| price | `data/factor_mine/prices/2026-09-22.json` | `—` | — absent | absent | `—` | both absent |
| sector | `01_daily/sectors/2026-09-22/` (36 files) | | | ok | | 36 match, 0 miss |

Row diff, blob the code selected versus the proven pre-open blob:

- `01_daily/map_heat/2026-09-22_map_heat.json`: changed same=0 added=0 removed=0 changed=1 fields=file
- `data/exports/finviz_2026-09-22.csv`: added same=0 added=11648 removed=0 changed=0 fields=—
- `01_daily/catalyst/2026-09-22_dossiers.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/map_heat/2026-09-22_research_baseline.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/events/2026-09-22_events.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `excel_bot/suggestions/suggestions.csv`: changed same=0 added=1 removed=0 changed=28 fields=current_price,days_held,first_open,ret_vs_close,ret_vs_open

Candidate rows the walk scored (snapshot) versus rows dated this session in the last `panel.json` proven before the open: **added** same=0 added=72 removed=0 changed=0. Fields: —.
Added tickers (up to 12): ABVX, AEHL, AIBZ, AIP, ALOY, AMC, AMRX, ANAB, APPS, ARHS, ARM, ARQQ.

### 2026-09-23

Snapshot label `pit_rebuilt`, rows 76, code sha the snapshot records `693b0b078bba`.

| group | path | used | server | verdict | pre-open | match |
| --- | --- | --- | --- | --- | --- | --- |
| snapshot | `data/factor_mine/snapshots/2026-09-23.json` | `5f13a4415ea0` | 2026-09-25 15:49:10Z actions_push | pushed_after | `—` | no pre-open copy |
| panel | `data/factor_mine/panel.json` | `41bcc47fe169` | 2026-09-23 00:04:54Z actions_head | proven_before | `41bcc47fe169` | same |
| news | `01_daily/news/2026-09-23_finviz_digest.json` | `eb5503bb8195` | — none | not_proven | `97ff2ee87801` | different blob |
| packet | `01_daily/map_heat/2026-09-23_map_heat.json` | `e6ddedb894a3` | — none | not_proven | `1d3d25224e4e` | different blob |
| packet | `data/exports/finviz_2026-09-23.csv` | `aa8492b5edf2` | — none | not_proven | `—` | no pre-open copy |
| join | `data/join/2026-09-23_ranked.csv` | `473dbaa064b7` | 2026-09-23 09:40:26Z actions_head | proven_before | `473dbaa064b7` | same |
| packet | `01_daily/catalyst/2026-09-23_dossiers.json` | `82a2aa5041e2` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/map_heat/2026-09-23_research_baseline.json` | `8792882925c8` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/weather/2026-09-23_weather.json` | `945da3d9fe92` | 2026-09-23 09:40:20Z actions_head | proven_before | `945da3d9fe92` | same |
| predict | `01_daily/general/2026-09-23_predict.md` | `564b959cf12b` | 2026-09-23 08:39:59Z actions_head | proven_before | `564b959cf12b` | same |
| news | `01_daily/news/2026-09-23_actions.json` | `a52cd9fa14c3` | 2026-09-23 09:08:43Z actions_head | proven_before | `a52cd9fa14c3` | same |
| news | `01_daily/news/2026-09-23_judge.json` | `37c4cf4140d4` | 2026-09-23 08:35:16Z actions_head | proven_before | `37c4cf4140d4` | same |
| packet | `01_daily/events/2026-09-23_events.json` | `a68222ffb20b` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/map_heat/2026-09-23_research.json` | `da5901b8f840` | 2026-09-23 08:57:52Z actions_head | proven_before | `da5901b8f840` | same |
| ab | `data/ab_checklist/2026-09-23_ab_checklist_enriched.csv` | `9f6d4b8ed72d` | 2026-09-23 08:25:24Z actions_head | proven_before | `9f6d4b8ed72d` | same |
| excel | `excel_bot/suggestions/suggestions.csv` | `feadeca24e82` | — none | not_proven | `5f33c82127c7` | different blob |
| excel | `excel_bot/daily/2026-09-23_excel_bot.md` | `—` | — absent | absent | `—` | both absent |
| price | `data/factor_mine/prices/2026-09-23.json` | `—` | — absent | absent | `—` | both absent |
| sector | `01_daily/sectors/2026-09-23/` (36 files) | | | ok | | 36 match, 0 miss |

Row diff, blob the code selected versus the proven pre-open blob:

- `01_daily/news/2026-09-23_finviz_digest.json`: changed same=0 added=0 removed=0 changed=1 fields=file
- `01_daily/map_heat/2026-09-23_map_heat.json`: changed same=0 added=0 removed=0 changed=1 fields=file
- `data/exports/finviz_2026-09-23.csv`: added same=0 added=11648 removed=0 changed=0 fields=—
- `01_daily/catalyst/2026-09-23_dossiers.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/map_heat/2026-09-23_research_baseline.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/events/2026-09-23_events.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `excel_bot/suggestions/suggestions.csv`: changed same=3 added=0 removed=0 changed=27 fields=current_price,exit_rule,ref_close,ret_vs_close,ret_vs_open,signal_colors,strategy,ticker

Candidate rows the walk scored (snapshot) versus rows dated this session in the last `panel.json` proven before the open: **added** same=0 added=76 removed=0 changed=0. Fields: —.
Added tickers (up to 12): A, ADMA, AEHL, AMKR, ARQT, BAND, BB, BFLY, BLLN, CBRL, CLPT, CMPX.

### 2026-09-24

Snapshot label `pit_rebuilt`, rows 74, code sha the snapshot records `693b0b078bba`.

| group | path | used | server | verdict | pre-open | match |
| --- | --- | --- | --- | --- | --- | --- |
| snapshot | `data/factor_mine/snapshots/2026-09-24.json` | `5f13a4415ea0` | 2026-09-25 15:49:10Z actions_push | pushed_after | `—` | no pre-open copy |
| panel | `data/factor_mine/panel.json` | `0d3fdf111048` | 2026-09-23 21:16:24Z actions_head | proven_before | `0d3fdf111048` | same |
| news | `01_daily/news/2026-09-24_finviz_digest.json` | `1f46d9620c39` | — none | not_proven | `6da7cab54fd5` | different blob |
| packet | `01_daily/map_heat/2026-09-24_map_heat.json` | `d28f206ce048` | — none | not_proven | `30e84ea0c435` | different blob |
| packet | `data/exports/finviz_2026-09-24.csv` | `c4199bbdd456` | — none | not_proven | `8c2495a3cc31` | different blob |
| join | `data/join/2026-09-24_ranked.csv` | `e87a13bdd792` | 2026-09-24 10:53:30Z actions_head | proven_before | `e87a13bdd792` | same |
| packet | `01_daily/catalyst/2026-09-24_dossiers.json` | `ec7498fc74f1` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/map_heat/2026-09-24_research_baseline.json` | `ef0140cf67ce` | 2026-09-23 21:19:02Z actions_head | proven_before | `ef0140cf67ce` | same |
| packet | `01_daily/weather/2026-09-24_weather.json` | `055df1a548fe` | 2026-09-24 10:53:24Z actions_head | proven_before | `055df1a548fe` | same |
| predict | `01_daily/general/2026-09-24_predict.md` | `f3f6b05d19ee` | 2026-09-24 10:08:35Z actions_head | proven_before | `f3f6b05d19ee` | same |
| news | `01_daily/news/2026-09-24_actions.json` | `15938bbb783e` | 2026-09-24 11:46:08Z actions_head | proven_before | `15938bbb783e` | same |
| news | `01_daily/news/2026-09-24_judge.json` | `0e50bcbcd6b3` | 2026-09-24 10:03:15Z actions_head | proven_before | `0e50bcbcd6b3` | same |
| packet | `01_daily/events/2026-09-24_events.json` | `e3fbc0ba9c5c` | — none | not_proven | `—` | no pre-open copy |
| packet | `01_daily/map_heat/2026-09-24_research.json` | `ea39db54ab22` | 2026-09-24 10:50:55Z actions_head | proven_before | `ea39db54ab22` | same |
| ab | `data/ab_checklist/2026-09-24_ab_checklist_enriched.csv` | `54dbd0263570` | 2026-09-24 09:59:28Z actions_head | proven_before | `54dbd0263570` | same |
| excel | `excel_bot/suggestions/suggestions.csv` | `284929eff966` | 2026-09-23 21:32:31Z actions_head | proven_before | `284929eff966` | same |
| excel | `excel_bot/daily/2026-09-24_excel_bot.md` | `—` | — absent | absent | `—` | both absent |
| price | `data/factor_mine/prices/2026-09-24.json` | `—` | — absent | absent | `—` | both absent |
| sector | `01_daily/sectors/2026-09-24/` (33 files) | | | ok | | 33 match, 0 miss |

Row diff, blob the code selected versus the proven pre-open blob:

- `01_daily/news/2026-09-24_finviz_digest.json`: changed same=0 added=0 removed=0 changed=1 fields=file
- `01_daily/map_heat/2026-09-24_map_heat.json`: changed same=0 added=0 removed=0 changed=1 fields=file
- `data/exports/finviz_2026-09-24.csv`: changed same=0 added=3 removed=28 changed=11645 fields=20-Day Simple Moving Average,200-Day Simple Moving Average,50-Day High,50-Day Low,50-Day Simple Moving Average,52-Week High,52-Week Low,52-Week Range,Active/Passive,All-Time High
- `01_daily/catalyst/2026-09-24_dossiers.json`: added same=0 added=1 removed=0 changed=0 fields=file
- `01_daily/events/2026-09-24_events.json`: added same=0 added=1 removed=0 changed=0 fields=file

Candidate rows the walk scored (snapshot) versus rows dated this session in the last `panel.json` proven before the open: **added** same=0 added=74 removed=0 changed=0. Fields: —.
Added tickers (up to 12): AAON, ACMR, ADCT, AEHL, AIB, AIRS, AKAM, ALKT, AMPL, ASPN, AVT, BAH.

### 2026-09-25

Snapshot label `None`, rows 62, code sha the snapshot records `43f96067d26b`.

| group | path | used | server | verdict | pre-open | match |
| --- | --- | --- | --- | --- | --- | --- |
| snapshot | `data/factor_mine/snapshots/2026-09-25.json` | `77db2793d6a2` | 2026-09-25 22:30:02Z events_push | pushed_after | `—` | no pre-open copy |
| panel | `data/factor_mine/panel.json` | `0eab5298326f` | 2026-09-24 21:44:55Z actions_head | proven_before | `0eab5298326f` | same |
| news | `01_daily/news/2026-09-25_finviz_digest.json` | `a1f0eb34e518` | — none | not_proven | `1c7c7ceb7109` | different blob |
| packet | `01_daily/map_heat/2026-09-25_map_heat.json` | `c1f094c13ccd` | 2026-09-25 08:20:46Z events_push | proven_before | `c1f094c13ccd` | same |
| packet | `data/exports/finviz_2026-09-25.csv` | `dd5ca1549fbd` | 2026-09-25 09:03:31Z events_push | proven_before | `dd5ca1549fbd` | same |
| join | `data/join/2026-09-25_ranked.csv` | `f23ffda95e97` | 2026-09-25 10:10:32Z events_push | proven_before | `f23ffda95e97` | same |
| packet | `01_daily/catalyst/2026-09-25_dossiers.json` | `472cebabac93` | 2026-09-25 11:02:24Z actions_head | proven_before | `472cebabac93` | same |
| packet | `01_daily/map_heat/2026-09-25_research_baseline.json` | `3a1ce612b47f` | 2026-09-24 23:27:37Z actions_head | proven_before | `3a1ce612b47f` | same |
| packet | `01_daily/weather/2026-09-25_weather.json` | `52e709b864df` | 2026-09-25 10:10:27Z events_push | proven_before | `52e709b864df` | same |
| predict | `01_daily/general/2026-09-25_predict.md` | `eccc6f4bd24b` | 2026-09-25 08:44:08Z actions_head | proven_before | `eccc6f4bd24b` | same |
| news | `01_daily/news/2026-09-25_actions.json` | `4c2c586ee840` | 2026-09-25 09:10:14Z events_push | proven_before | `4c2c586ee840` | same |
| news | `01_daily/news/2026-09-25_judge.json` | `7ad127bb151f` | 2026-09-25 08:39:17Z events_push | proven_before | `7ad127bb151f` | same |
| packet | `01_daily/events/2026-09-25_events.json` | `08c2d51226c4` | 2026-09-25 08:36:33Z events_push | proven_before | `08c2d51226c4` | same |
| packet | `01_daily/map_heat/2026-09-25_research.json` | `5f58b8703411` | 2026-09-25 09:01:56Z events_push | proven_before | `5f58b8703411` | same |
| ab | `data/ab_checklist/2026-09-25_ab_checklist_enriched.csv` | `93e67f9234e8` | 2026-09-25 11:04:23Z events_push | proven_before | `93e67f9234e8` | same |
| excel | `excel_bot/suggestions/suggestions.csv` | `7fc0b0a8b408` | — none | not_proven | `af4175b84815` | different blob |
| excel | `excel_bot/daily/2026-09-25_excel_bot.md` | `—` | — absent | absent | `—` | both absent |
| price | `data/factor_mine/prices/2026-09-25.json` | `—` | — absent | absent | `—` | both absent |
| sector | `01_daily/sectors/2026-09-25/` (36 files) | | | ok | | 36 match, 0 miss |

Row diff, blob the code selected versus the proven pre-open blob:

- `01_daily/news/2026-09-25_finviz_digest.json`: changed same=0 added=0 removed=0 changed=1 fields=file
- `excel_bot/suggestions/suggestions.csv`: changed same=2 added=0 removed=0 changed=30 fields=current_price,ref_close,ret_vs_close,ret_vs_open,signal_colors,strategy,ticker

Candidate rows the walk scored (snapshot) versus rows dated this session in the last `panel.json` proven before the open: **added** same=0 added=62 removed=0 changed=0. Fields: —.
Added tickers (up to 12): A, ACAD, ADPT, AEHL, AMD, BLFS, BLLN, BRVE, CBRL, CDNA, CDNS, CLS.

## Panel rewrites

`data/factor_mine/panel.json` commits on this branch, oldest first. Server time is the proof above, not the committer clock.

| commit | committer (not proof) | server | proof | sessions in the file |
| --- | --- | --- | --- | --- |
| `f5b46d20eec2` | 2026-09-09 07:00:01Z | 2026-09-09 07:00:04Z | actions_push | 2026-08-13 .. 2026-09-08 (18) |
| `957fc58e6215` | 2026-09-10 06:08:36Z | — | none | 2026-08-13 .. 2026-09-09 (19) |
| `1c7a450b893d` | 2026-09-12 03:04:47Z | 2026-09-12 03:04:50Z | actions_push | 2026-08-13 .. 2026-09-11 (21) |
| `bd2dffab3d11` | 2026-09-14 04:39:02Z | — | none | 2026-08-13 .. 2026-09-11 (21) |
| `eef0854ca59c` | 2026-09-14 20:46:53Z | 2026-09-14 20:47:12Z | actions_head | 2026-08-13 .. 2026-09-14 (22) |
| `16b65eec717b` | 2026-09-14 21:24:38Z | 2026-09-14 21:24:52Z | actions_head | 2026-08-13 .. 2026-09-14 (22) |
| `284ca0e3a187` | 2026-09-15 21:22:06Z | 2026-09-15 21:22:24Z | actions_head | 2026-08-13 .. 2026-09-15 (23) |
| `5ae9d19c83e5` | 2026-09-16 17:55:52Z | 2026-09-16 17:55:55Z | actions_push | 2026-08-13 .. 2026-09-16 (24) |
| `5391bbe581c0` | 2026-09-16 23:36:53Z | 2026-09-16 23:37:11Z | actions_head | 2026-08-13 .. 2026-09-16 (24) |
| `2208918b2fac` | 2026-09-17 13:59:02Z | 2026-09-17 13:59:19Z | actions_head | 2026-08-13 .. 2026-09-17 (25) |
| `29ee926301d3` | 2026-09-17 23:23:16Z | 2026-09-17 23:23:36Z | actions_head | 2026-08-13 .. 2026-09-17 (25) |
| `77a6a92c5857` | 2026-09-18 13:55:41Z | 2026-09-18 13:55:53Z | actions_head | 2026-08-13 .. 2026-09-18 (26) |
| `cb7df662fa44` | 2026-09-19 05:24:35Z | 2026-09-19 05:24:43Z | actions_head | 2026-08-13 .. 2026-09-18 (26) |
| `f997f7e6a698` | 2026-09-19 10:56:53Z | 2026-09-19 10:57:10Z | actions_pull_request | 2026-08-13 .. 2026-09-18 (26) |
| `75ff1cd1b5b0` | 2026-09-19 11:27:30Z | 2026-09-19 11:27:36Z | actions_head | 2026-08-13 .. 2026-09-18 (26) |
| `b3ac5d29b79c` | 2026-09-19 17:34:18Z | — | none | 2026-08-13 .. 2026-09-18 (26) |
| `6e140da069f2` | 2026-09-19 18:25:54Z | 2026-09-19 18:26:01Z | actions_head | 2026-08-13 .. 2026-09-18 (26) |
| `087d5fda408d` | 2026-09-21 21:51:23Z | 2026-09-21 21:51:31Z | actions_head | 2026-08-13 .. 2026-09-21 (27) |
| `41bcc47fe169` | 2026-09-23 00:04:46Z | 2026-09-23 00:04:54Z | actions_head | 2026-08-13 .. 2026-09-22 (28) |
| `0d3fdf111048` | 2026-09-23 21:16:14Z | 2026-09-23 21:16:24Z | actions_head | 2026-08-13 .. 2026-09-23 (29) |
| `0eab5298326f` | 2026-09-24 21:44:47Z | 2026-09-24 21:44:55Z | actions_head | 2026-08-13 .. 2026-09-24 (30) |
| `b5946b1d71bf` | 2026-09-25 13:41:47Z | 2026-09-25 13:41:52Z | events_push | 2026-08-13 .. 2026-09-25 (31) |
| `77db2793d6a2` | 2026-09-25 22:29:54Z | 2026-09-25 22:30:02Z | events_push | 2026-08-13 .. 2026-09-25 (31) |

Commits that change the file and are reachable from HEAD: 23. The first is the commit that added it (`f5b46d20eec2`, server push 2026-09-09 07:00:04Z). `git log -- data/factor_mine/panel.json` simplifies that list to 21. The two it drops, `75ff1cd1b5b0` and `6e140da069f2`, still changed the file and were the default-branch head on 2026-09-19 (a workflow_dispatch run started on each).

## Recompute

Implemented as `recompute` in `research/audit/input_provenance_336.py`. It calls `factor_mine_sequential.walk` with `persist=False` and the two recipes, then again with `exclude='GLND'`. An empty proven-frozen list returns no return. Before and after the run the script compares size and mtime of the ledger directory, the state directory, snapshots, price pins, `panel.json`, the freeze manifest, and the retro parquet.

Fingerprint unchanged: True.

