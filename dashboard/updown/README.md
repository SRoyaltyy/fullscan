# dashboard/updown: every-stock up/down read (research)

Static research page for `updown_o2c_v2`. It shows p(up) for today's official open → close, for every stock on every day
from 2020-01-02 to 2026-10-05. Past days show only walk-forward out-of-sample predictions. The final holdout starts 2026-07-06.
**Research page, not a trading signal.**

v2 replaces v1 after a leak audit (`LEAK_AUDIT.md`). v1 used the split-adjusted price level, which leaks future reverse splits.
The open→close edge depends on today's official open, which cannot be traded at the printed open. With no same-day open
information, there is no after-fee edge. Details are in the audit.

- `index.html`: self-contained vanilla JS with no external requests. Deep links: `#d=YYYY-MM-DD&t=TICKER`.
- `data/index.json`: tickers, days, per-day stats, summary.
- `data/y/YYYY.bin.gz`: one gzip-compressed binary per year (format in index.json `format`), decoded in the browser.
- `LEAK_AUDIT.md`: the leak audit, with its verdict on the first line.

Additive only. Nothing in the h1 / Factor Mine daily pipeline reads or writes this folder; only `data/expmove/` is appended to by the separate shadow workflow (below). It is regenerated offline. It is not part of
h1, Factor Mine, or any ledger.

## Expected-move rank column (`updown_expmove_v1`), added 2026-10-07
- **What it is.** A descriptive per-stock risk column with no trading claim. It shows the predicted size of the move from today's 09:30 open to the next session's 09:30 open (|open→next open|), ranked within the top 1,000 names by 20-day dollar volume (actual prior close ≥ $5). 100 = largest expected move.
- **Inputs.** Pre-open only: bars dated before the day, SPY/VIX closes before the day, and the Nasdaq earnings calendar.
- **Baseline.** It beats plain 20-day Parkinson volatility only slightly: mean daily rank IC vs realized |open→next open| is about 0.42 vs 0.38 (confirm window 2025-01..2026-07; 0.40 vs 0.37 over 2020-24).
- **Past days.** 2020-01-02..2026-10-02 hold only walk-forward out-of-sample predictions (round-3 M1 model; each window was predicted by a model trained on earlier dates). 2026-10-05 and 2026-10-06 are not scored.
- **Forward days.** From 2026-10-07, each day is frozen before 09:25 ET by `.github/workflows/updown_shadow.yml` with the frozen model in `research/updown_rel1_hv_v1/models/m1_abs1.txt`. Days are append-only and fingerprinted.
- **Files**, all added alongside the existing data:
  - `data/expmove/index.json` + `data/expmove/y/YYYY.bin.gz`: the backfill. The format is in index.json.
  - `data/expmove/days/YYYY-MM-DD.json` + `data/expmove/LOCKS.jsonl` (+ `MISSED.jsonl`): forward frozen days and their lock ledger.
  - `data/expmove/forward_realized.json`: realized |open→next open| and rank IC for forward days, computed after the exit session.
- **What stays unchanged.** The existing v1/v2 up/down data (`data/index.json`, `data/y/`) and the LEAK_AUDIT.md verdict are untouched. The forward files are written by the shadow workflow, not by the daily h1/Factor Mine pipeline.
