# updown_rel1_hv_v1: paper shadow

> **paper only — failed multiple-testing haircut (t 2.02 vs 3.37).** This is a shadow record with no orders and no broker
> wiring. It is never promoted automatically.

Dashboard: https://sroyaltyy.github.io/fullscan/dashboard/updown-shadow/ · related risk column (updown_expmove_v1):
https://sroyaltyy.github.io/fullscan/dashboard/updown/

## What it trades (fixed; any change = a new strategy name)
This is "D8" from round-3 research (`/workspace/updown/r3`, ROUND3_REPORT.md).

- **Universe.** The top 1,000 US names by 20-day mean dollar volume, among names whose *actual* (unadjusted) prior close is at least $5. The prior close is unadjusted using Yahoo split factors, and the base filters are at least 60 prior bars and at least $1M dollar volume.
- **Two frozen LightGBM models.** Both use the same 77 pre-open features (list in `models/frozen_meta.json`).
  - `d1_rel_oo1` is a 1-day ranker. Its target is the open→next-open return minus the industry median (sector-relative).
  - `m1_abs1` predicts the move size |open→next open|.
- **Picks.**
  1. Keep the half of the 1,000 with M1 at or above that day's median (the "high-move half").
  2. Within that half, go long D1 rank percentile > 0.9 and short ≤ 0.1, equal weight. That is about 50 long and 50 short; long weights sum to +1 and short weights to −1.
- **Fill.** Enter at the 09:30 ET official open of day N and exit at the 09:30 open of the next session (Yahoo split-adjusted). Rebalance daily.
- **Fees.** Bp/side × turnover, where turnover = Σ|w_N − w_{N−1}|. Results are shown at 5, 10 and 15 bp/side. The Futubull per-order schedule is not applied because the shadow has no notional size.
- **Beta-hedged version.** Gross minus the ex-ante portfolio beta × the equal-weight open→next-open return of the frozen 1,000-name universe. The beta is Σ w·beta60 (NaN → 1), frozen in picks.json. beta60 is measured against that equal-weight market, as in research `diag_beta.py`. An SPY-hedged net (10bp) is shown too. Hedge trading costs are not charged.
- The ex-ante beta swings a lot (research sd ~0.7–1.0). On the 2026-10-06 dry run it was −2.0: the shorts are high-beta names.

## Inputs (knowable before 09:30 ET on N; no same-day open)
- Yahoo daily bars dated before N, split-adjusted. Returns use adjusted prices; the price filter uses the actual price.
- SPY and ^VIX closes dated before N.
- Nasdaq earnings calendar (api.nasdaq.com). Each event counts as known only from the session after its date, as in research. "Upcoming" (0–5 sessions ahead) uses pre-announced dates.
  - Older dates come from `static/earn_seed.csv.gz` (fetched after the fact, through 2026-10-06).
  - Recent and future dates are fetched live. If a live fetch fails, the run falls back to the seed. If both fail, the day still freezes with fewer events, and the counts are in the manifest.
- Static 2026 Finviz industry map (`static/industry_map_finviz_2026-04-26.csv`), as in research.
- Candidate tickers: `static/tickers.txt`, the 6,307-name research list. Names listed after it are not covered.

The live feature builder (`ushadow.build_features`) reproduces the research panel exactly on historical days. On 2025-10-30 and 2026-06-16, all 77 features and all 100 picks were identical.

## Model rule (fixed)
**Frozen.** Both models were trained once on L1000 rows dated 2018-03-29..2026-10-01, all before the 2026-10-07 start, and are never retrained. The params are LightGBM n_estimators=300, lr 0.05, num_leaves 63, min_child_samples 1000, subsample 0.7, colsample 0.7, reg_lambda 10, 1.5M-row sample, deterministic.

Model sha256 values are in `models/frozen_meta.json`, and every run asserts them. Retraining or any change to features, universe, fees or fill makes a new strategy with a new name.

## Daily timeline
All times US/Eastern; HKT = ET + 12h while US daylight time lasts (until 2026-11-01), then ET + 13h.

| step | when |
|---|---|
| Freeze day N (picks + expmove rank) | first run from **17:15 ET on the previous session**, which needs that session's final bars. Morning runs are backstops. |
| Writer cutoff | refuses to write N at/after **09:20 ET** on N |
| Push cutoff | an unpushed freeze commit is dropped at/after **09:24 ET** |
| Lock deadline (checked forever) | every lock line must be stamped before **09:25 ET** on its day |
| Quiet window | nothing is written 09:20–09:45 ET on a session day |
| Realized | after the close of the exit session (N+1 ≥ 16:30 ET), written to `realized/` (a separate ledger) |
| Missed | a session with no lock by 09:25 ET gets an append-only line in `MISSED.jsonl` after 09:45 ET. It is never back-filled and earns nothing. |

## Records (IRONCLAD)
- `days/<N>/` holds:
  - `picks.json`
  - `scores.csv` (all 1,000 scored names, the high-move flag and side)
  - `features_L1000.csv.gz` (the frozen float32 feature matrix that was scored)
  - `earnings_used.csv.gz`
  - `manifest.json` (input hashes, dropped tickers, readiness, code sha256)
- `LOCKS.jsonl` is append-only. Each line holds the day's file sha256 values, the freeze time, and `prev_sha256`, the hash of the ledger before that line (a hash chain). Fingerprints start on the first live day; nothing is fingerprinted retroactively.
- `python ushadow.py check [--base-ref REF]` fails if any of these happen:
  - a locked file changed or is missing
  - a lock line was edited, reordered or removed (hash chain + prefix vs the base ref)
  - a day folder exists with no lock line
  - a lock was stamped at/after 09:25 ET
  - a lock is dated before the start
  - a missed day is also locked

  It runs at the start and end of every run, before every push (vs the then-current origin/main), and on every PR/push touching these paths (base vs head).
- `python ushadow.py verify` re-scores every locked day from its frozen feature file with the frozen models and requires identical picks (the restart check).
- `realized/daily.csv` and `realized/summary.json` are recomputed from the frozen picks on every run. Pick files are never edited. They contain:
  - gross, turnover, and net at 5/10/15 bp/side
  - beta-hedged gross/net
  - SPY and IWM open→next-open
  - RANDOM4 (seed 20260813, 1000 random books per day of the same size from the frozen high-move half, same fee model): median, p95, and the strategy's percentile
  - net 10bp without the single best stock
  - missing fills (counted as 0)
  - >3x unexplained price jumps (flagged and zeroed)

## Kill / promote rule (pre-set 2026-10-07, before the first live day)
- **KILL** if, after ≥ 60 realized locked days, the cumulative net return at **10bp/side** is negative.
- **Early KILL** if, after ≥ 20 realized days, the Newey-West t (lag 5) of daily net at 10bp/side is ≤ −2.5.
- A killed strategy stops freezing new days. Its record stays untouched.
- **PROMOTION REVIEW** is a decision by Cyrus and is never automatic; it stays paper until he decides. All of these must hold:
  - ≥ 120 realized locked days
  - live Newey-West t of daily net at 10bp/side ≥ **3.37** (the cumulative multiple-testing bar)
  - cumulative net beats the RANDOM4 median and IWM over the same days
  - it is still positive without its single best stock
- Missed days count as days without returns and are listed.

The status is computed by `ushadow.summarize` and shown on the dashboard.

## Workflow
`.github/workflows/updown_shadow.yml` ("updown shadow freeze (paper, rel1_hv_v1 + expmove_v1)"):

- Concurrency group `updown-shadow`, ubuntu-latest.
- Writes only `research/updown_rel1_hv_v1/`, `dashboard/updown/data/expmove/` and `dashboard/updown-shadow/`.
- Rebase-retry push with GITHUB_TOKEN.
- Triggers:
  - cron (late on this repo, so treated as a backstop)
  - workflow_run after "h1 post-close fill", "Post-Close ALL", "Finviz pre-open scrape", "Pre-Open ALL" and "h1 append-only forward"
  - dispatch with `dry_run=true` (full freeze into a temp dir, nothing committed)

Every trigger re-gates and exits in seconds when nothing is due. The workflow is not part of h1, Factor Mine, tickets or any deploy path.
