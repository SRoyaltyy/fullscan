# Buy/sell day audit

The workflow `.github/workflows/buy_sell_audit.yml` fingerprints each sealed day's buys and sells for live fullscan strategies and compares them to the first seal in `data/buy_sell_audit/baselines.jsonl`.

A fingerprint is only the session date, side, ticker, and size. Equity marks, sit-out notes, prices, and would-have-bought lines are not in the hash. A baseline line is written once. `--seed` appends new days and refuses to change an old hash.

## What a red run means

The job prints `RED` and exits non-zero when any line starts with `DRIFT` or `NEW`.

- `DRIFT <family> <name> <date>: buy/sell fingerprint moved` — that strategy's sealed buys or sells are not the first seal. The parenthetical shows the first-seal prefix and the current prefix. Do not edit the baseline to match the new list, and do not rewrite the sealed record.
- `DRIFT strategy_tickets <date>` or `DRIFT webull_sim ...` — the sealed file no longer matches `data/past_day_lock`.
- `DRIFT page ...` — only when the audit is run with `--pages`. An in-repo dashboard or `data/strategy_status/<date>.json` buy/sell list does not match the sealed file. Live GitHub Pages fetches are a follow-up; the daily job does not call them.
- `NEW <family> <name> <date>: no first-seal baseline` — a day sealed after the manifest was last appended. Run `python3 -m src.buy_sell_audit --seed` and commit only the new lines.
- `DRIFT theme_radar_research_shorts: FAIL ...` — `SRoyaltyy/theme-radar` `audit --json -` exited non-zero, the checkout was missing, or the JSON `strategy` was not `theme_radar_research_shorts`. Do not edit `data/buy_sell_audit/baselines.jsonl` to clear it. The seal lives in theme-radar.

`MISSING` means that day has no sealed buy/sell (the gap stays empty; nothing is backfilled). `SKIP` lines are gaps in this repo, such as a log file that is not on disk. They do not fail the job.

Theme Radar research shorts are strategy `theme_radar_research_shorts`. The workflow checks out `SRoyaltyy/theme-radar` `main` under the runner temp directory (the tree is not committed in fullscan) and runs `python research/shadow_log/shadow_log_lock.py audit --json -` there. Exit 0 with that strategy and `"ok": true` prints `PASS theme_radar_research_shorts`. A non-zero exit, a missing checkout, unreadable JSON, or any other `strategy` prints `DRIFT theme_radar_research_shorts: FAIL ...` and fails the job. Theme Radar's audit needs the full git history so it can read pre-open commit times; a shallow clone is a failure. This command does not seal and does not rewrite theme-radar rows. A trading day with no plan file stays a gap inside that audit and is not a fullscan failure.

Checked sources, the same ones the strategy-status board reads: dated strategy tickets (factor mine, both pre-open sleeves, paper, stock book, flatten_robust), h1 and holdup plan rows, the excel prior-session lock, and webull sim rows that `past_day_lock` has sealed (from 2026-10-06). Ticket days on or before the past-day-lock watermark are not fingerprinted.
