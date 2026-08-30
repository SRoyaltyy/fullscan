# Excel Replication Engine — Architecture & Caveats

Goal: replicate "Simple View--Calculation.xlsx" (Sheet1, A1:JO364, 35,769
formulas) in Python so the model can be backtested across many tickers.

## Pipeline
1. `extract_model.py` — parses the xlsx into `model.json`: all formulas
   (incl. array-formula text + spill refs), Excel's cached values (ground
   truth), conditional formatting rules (raw XML, dxf fills resolved through
   the theme palette), external workbook cache ([1]Change!), static fills.
   NOTE: the Claw-made JSON extracts in inputs/ are NOT used — they lost all
   array-formula text (incl. the STOCKHISTORY anchors).
2. `xlparse.py` — tokenizer + Pratt parser -> AST tuples.
3. `xlrt.py` — Excel value semantics: errors as values, blank/empty coercion
   (formula returning empty ref displays 0), Excel ordering (num<text<bool>),
   1x1-array broadcasting, COUNTIF criteria, General 15-sig-digit text.
4. `evaluator.py` — lazy memoized grid eval, spill/anchor coverage, external
   cache resolution, INDEX():INDEX() range-as-reference resolution, ~45
   functions incl. LET/LAMBDA/SCAN/FILTER/TAKE/DROP/VSTACK/UNIQUE/XMATCH
   (search mode), IFS/IFERROR laziness, AND/OR error propagation (no
   short-circuit), TODAY() injected, STOCKHISTORY intercepted.
5. `colors.py` — CF engine: priority-ordered cellIs/expression/containsText/
   colorScale rules; expression refs re-anchored per cell via AST shifting.
6. `stockhistory.py` — STOCKHISTORY-equivalent arrays from the Yahoo Finance
   plugin datasource (daily + Mon-start weekly aggregation).
7. `run.py` — end-to-end runner -> outputs/grid_*.html + .json (per-day
   values + fill colors for columns A..O, rows 1..145).

## Validation (vs Excel cached values, file's own NNE data 2026-07-27)
- 32,729 / 32,730 formula cells with ground truth match exactly (99.997%).
- Sole mismatch EB150: stale Excel cache (every input cell in EB1:EB148
  matches cache; Excel could not produce #VALUE! from this state).
- Run: `python engine/validate.py --full --save`

## Caveats
- Ticker in the file is NNE (Nano Nuclear); the Yahoo datasource currently
  returns EMPTY_DATA for it ("no timezone found"). AAPL works fine. If NNE
  is needed, find another data source and plug it into stockhistory.py.
- Yahoo raw OHLC is split-adjusted, not dividend-adjusted — same convention
  as STOCKHISTORY, but data vendor differences (Refinitiv vs Yahoo) can cause
  small price/volume deltas vs Excel.
- TODAY() is injectable for reproducible backtests (serial date).
- External workbook [1]Change! is a static cache snapshot from the file;
  only Q1/R1 (and 141 similar lookup formulas) use it.

## Usage
- Replica of current Excel state: `python engine/run.py --from-cache`
- Live run: `python engine/run.py --ticker AAPL --date 2026-07-24`
- Outputs land in outputs/ (HTML visual grid + JSON of values/colors).

## Backtesting (added 2026-07-27)
- `universe.py` — 5,266 cleaned US tickers (ex-ETF/warrant/unit/SPAC/preferred)
  from NASDAQ Trader symdir files in data/.
- `backtest.py` — resumable grid builder; per ticker saves grids/<T>.json
  (~139 days of OHLCV + 15 fills). `--sample N --workers 6 --time-budget S`.
  Throughput ≈ 4s/ticker with 6 workers. Failures tracked in grids/_failed.json
  (skipped after 2 attempts). Prints COMPLETE when universe is done.
- `signals.py` / `sweep.py` — cluster definition matrix + three return flavors:
  ret (hindsight open->close), ret_c (entry-lagged), ret_rt (fully real-time,
  enter at confirmation close, exit at flip-confirmation close). Leaderboard ->
  outputs/sweep_leaderboard.csv, sorted by RT tstat.
- Pilot (28 tickers, ~5 months): hindsight 67-76% win vs real-time best
  ~46%/+0.94% (hyst_core_score_e3_x2, t=1.23, not yet significant). The
  hindsight-real-time gap is the signal's lag cost — the key thing to watch.
- Full-universe build runs as cron Automation
  `automation_47fe792e-e8f0-4180-a9c4-c88c874c9a90` (every 10 min, ~150
  tickers/chunk, self-disables + runs sweep when COMPLETE).
- Phase 2 (not built yet): deeper history per definition-winner by stitching
  multiple 2y Yahoo calls (600d weekly lookback eats most of one 2y call).

## Exit-rule shootout (2026-07-27, engine/exit_rules.py)
Question: is hold-to-flip the right exit for confirmed green clusters? No.
Entry = confirmation close, def tol2_core_score_ml3, 762 grids.
- ALL (n=5133): flip -0.14% (t=-0.7); hold1 +0.35% (t=2.9); hold2/3 +0.5% (t~3.3);
  tp3-8 ~+0.2% (t~2). MFE +11.4% (hindsight ceiling, not tradable).
- mid(1-10B) (n=1565): flip +0.43%; hold2/3 +0.39% (t=3.1); tp ~+0.24-0.30%.
- mid:beta>1.5 (n=319): flip +1.41%; short holds/tight TP NEGATIVE; tp15 +0.24%.
- mid:BBAI-like (n=130): flip +5.27% (t=2.4); hold8 +1.29%; tp15 +1.43%;
  tp3 -0.69%. MFE +24.9%.
Conclusion: exit rule must match stock type. Low-beta: quick exits harvest small
pops. High-beta hype: continuation IS the edge - tight exits cap the runners.
BBAI per-cluster: tp5/tp8/tp10 filled in all 3 green clusters (+5/+8/+10 each)
while flip-hold lost on 2 of 3.
Caveats: overlapping trades inflate t; universe ~15% built; BBAI n=3 anecdotal.

## Timing verdict (rulebook section 5, engine/timing_test.py, 2026-07-27)
Perturbation test on BBAI (5 sample days, OHLCV x0.4-4.0):
- Start-of-day knowable columns: A, B, C, G, J, K, L, M, O (0/5 same-day changes)
- CLOSE-knowable columns: D, E, F, H, I, N (use day-t OHLC)
- core_score = columns A..J INCLUDES close-knowable D,E,F,H,I
  -> all core_score / row_score definitions are CLOSE-knowable.
  -> entry at confirmation CLOSE (current practice) is the leak-free choice.
  -> entry at confirmation open would be a LOOK-FORWARD LEAK. Do not use.
  -> only column-A-keyed definitions (a_score) may legitimately enter at open.
Owner's belief "colors use start-of-day data" is true for 9 of 15 columns only.

## Causality test + discovery mining (2026-07-27 late)
- engine/causality_test.py: corrupt-future test, 3229 pre-cutoff decisions
  bit-identical across 30 grids x 6 definitions; negative control sensitive.
  PASS. Mining code path certified leak-free per rulebook section 5.
- engine/mine.py: rulebook miner. Discovery set only (holdout_split.json),
  33 defs x 13 exits x 2 sides x finviz cohorts, NET of costs (0.1%/0.3%).
- Dry run on 1628 partial discovery grids (mine_results_dryrun.json):
  18480 cells n>=100. Top: hyst_core_score_e3_x2 short_red hold1 ALL
  +0.66% net, t=13.3, n=11263, 55% win. Long side alive in mid-caps:
  tol3_A_ml3 hold2 mid(1-10B) +0.91% t=6.3; tol2_core hold2 mid +0.62% t=6.1.
  Caveats: discovery only, one regime (6.5mo), overlapping trades inflate t,
  short-borrow fees NOT in cost model (matters for short_red strategies).
- Cron automation updated: on COMPLETE runs `python engine/mine.py --min-n 300`
  then self-disables.

## Extended cohorts (2026-07-27, cohort_analysis.py)
Added for full-universe run: sector (GICS), index membership, volatility-month
(>8%/<3%), dollar-volume liquidity (finviz AvgVol is in THOUSANDS of shares;
buckets >$500M/$50-500M/$5-50M/<$5M per day), institutional ownership,
gross margin, ROE, insider ownership, optionable. mine.py picks these up
automatically via cohorts_of(). Cell count grows ~25 tags; discovery/holdout
protocol unchanged.

## Strategy card generator + first formal holdout PASS (2026-07-28)
- engine/cards.py: emits strategies/<name>/{card.json,trades.csv,README.md}.
  trades.csv: exact dates/prices/exit reason + entry/exit day colors in
  HUMAN-READABLE names (rulebook section 2) for all 15 columns.
- Smoke test on partial grids (3241 built): tol2_core_score_ml3 long_green
  hold2 mid(1-10B):all -> discovery +0.64% t=7.1 (n=3583) | holdout +0.54%
  t=5.4 (n=2448). Same sign, holdout t>=2 -> PASS (preliminary, one regime).
- Roadmap note (owner, 2026-07-28): finish Excel pipeline first; then Grok
  macro event-study harness (saved history allows retroactive eval), Deepseek
  verdict logging (Layer 3 sniper on cluster candidates), THEN GitHub repo /
  Supabase data-lake audit (news, filings, web parses; point-in-time check).

## FULL BACKTEST COMPLETE on partial universe (2026-07-28)
- Cron DISABLED (owner quota). Universe frozen at 3,603 grids + 1,114 dead
  (68% of 5,266; ~550 backfillable later).
- mine.py parallelized (--workers). Full mining: 2,137 discovery grids,
  47,320 cells n>=300. holdout.py: 25/25 top candidates PASSED (same sign,
  holdout t>=2). Verdicts in engine/holdout_verdicts.json.
- 7 strategy cards in strategies/ + strategies/README.md dossier index.
  Headline: L1 long tp8 low-vol holdout +1.22%/trade t=10.0; S1 short_red
  1-day holdout +0.50%/trade t=10.5 (966 tickers). L4 BBAI-like +1.94% t=2.4
  but only 36 tickers (below 50-ticker bar -> exploratory).
- New discovery: LOW-volatility (<3%/month) names are the strongest LONG
  cohort (was untested until extended cohorts added).
- Remaining: deep 5y history for regime validation (deferred, quota).

## 2026-07-28 — Local daily pipeline (replaces Kimi cron)
- `engine/daily_run.py` — full local daily run: Yahoo fetch (same method as cron) → Excel-replica engine → strategy evaluation vs latest day → append to single suggestions file → track old suggestions (open vs current price). Flags: `--limit N` (test), `--signals-only` (no fetch).
- `daily_run.cmd` — Task Scheduler wrapper; logs to `logs/daily_run.log`.
- Scheduled task **TradingBotDaily**: daily 18:30 local, Enabled, first real run 2026-07-28. PC must be awake.
- Tests: `--limit 60` → 52 ok / 8 err in 157 s (extrapolates ~2.6 h for full 3,603). `--signals-only` scan of all 3,603 grids in 14 s → 97 suggestions (dated 2026-07-24, in `suggestions/suggestions.csv`).
- Bugs fixed in test: cohort filters now applied (via cohort_analysis.load_finviz/cohorts_of); signal_date = grid last day, not run date.
- `DAILY_PIPELINE.md` — operator's guide (files, column meanings, manual commands, caveats).

## 2026-07-28 (pm) — Daily pipeline speed overhaul (3h -> ~40min)
- Root causes found: (1) every fetch spawned the yahoo_finance plugin subprocess
  via agent-gw gateway (~12-15s/ticker); (2) engine re-parsed all ~34k formulas
  for every trading day (xlparse.parse = 70% of CPU).
- Fixes:
  - `engine/fastfetch.py` — direct Yahoo v8 chart API over HTTPS, 0.1-0.25s/ticker.
    Verified value-identical to plugin path (only fresher: includes latest close).
    Permanent errors (404/500, dead tickers) skip retries + skip plugin fallback.
  - `engine/xlparse.py` — module-level parse memo + disk cache `.parse_cache.pkl`
    (read-only per process; regenerate with `python engine/warm_parse_cache.py`
    if model.json ever changes).
  - `engine/daily_run.py` — multiprocessing Pool (default 8 workers, chunksize 32);
    fast fetch inside workers, plugin path as fallback for transient errors only.
- Correctness: ANET grid rebuilt after changes = byte-identical fills (137/137 days).
- Timing: 300 tickers in 210s => full 3,603 in ~40 min (was ~2.6h extrapolated).
- Tracking verified: first_open/ret_vs_open now filling (AEP 7/24 signal: open
  135.18 -> cur 133.59 = -1.18%). Only 6/101 filled so far because test runs
  used --limit; full run tonight refreshes all.

## 2026-07-29 — Incident: locked suggestions file + task battery settings
- 28/7 18:30 scheduled run: OK (229 rows). 29/7 18:30: task SKIPPED because
  default settings had "No Start On Batteries". Manual 19:19 run: fetch+engine
  OK (260 signals for 7/28) but CRASHED writing suggestions.csv
  (PermissionError: file was open in Excel). User then saved the CSV from
  Excel (20:33), converting ISO dates to locale 28/7/2026 -> would have broken
  dedupe + tracking on all future runs.
- Fixes in daily_run.py: parse_date() accepts ISO + locale formats;
  load_suggestions() normalizes all dates back to ISO on every load; write is
  now temp-file + os.replace with 6x20s retries and a dated fallback file
  (never loses data, exits 2 with clear instructions).
- Task updated via PowerShell: DisallowStartIfOnBatteries=False,
  StopIfGoingOnBatteries=False, StartWhenAvailable=True, 3h time limit.
- Recovery: fallback file suggestions_fallback_2026-07-29.csv holds the full
  correct dataset (489 rows = 229 + 260 new for 7/28, all first_open filled).
  Pending: user closes Excel -> rerun --signals-only -> delete fallback + .tmp.
