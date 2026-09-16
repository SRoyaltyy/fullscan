# Ticket lesson filter — backtest

Window **2026-08-13 → 2026-09-15** · 23 sessions · fit `2026-09-04` / OOS `2026-09-05`→latest.

Universe = unique proposed NEW entries `(date, ticker, side)` from dated `strategy_tickets` when present, else factor-mine `pick_day` + stock-book files. Features are prior-bar only. Marks are 09:30 open → horizon close (short flips the sign). No invented fills.

Proposed rows 45149 · unique 2764.

## Contract

- RWT 9/11 short blocked
- IS blocked-sleeve mean -1.964% ≤ 0 (n_marked=4)
- OOS blocked-sleeve mean -0.852% ≤ 0 (n_marked=1)
- full blocked-sleeve mean -1.742% ≤ 0 (n_marked=5)
- OOS traded +0.073% vs baseline +0.070%

**Contract PASS** · shipped `oversold_crash_pause`

Iteration: first seed treated any wide bar as an air-pocket, so QMLS-class *up* days and F2 melt-up longs made the blocked sleeve positive in-sample. Crash now requires prior 1d ≤ −8% or a down gap. F2/F3 dropped. Live filter is F1 only.

## Sleeves (unique tickets)

| Split | A take-all | B filtered | Blocked |
|---|---|---|---|
| Full | n=2764 marked=2279 meanH=-0.017% hit=48.6% d1=-0.0297 d5=-1.0823 | n=2759 marked=2274 meanH=-0.013% hit=48.6% d1=-0.0301 d5=-1.0793 | n=5 marked=5 meanH=-1.742% hit=20.0% d1=0.1574 d5=-2.5445 |
| IS 08-13→09-04 | n=2008 marked=1938 meanH=-0.032% hit=49.4% d1=-0.0221 d5=-1.0823 | n=2004 marked=1934 meanH=-0.028% hit=49.5% d1=-0.023 d5=-1.0793 | n=4 marked=4 meanH=-1.964% hit=25.0% d1=0.4098 d5=-2.5445 |
| OOS 09-05→latest | n=756 marked=341 meanH=+0.070% hit=43.7% d1=-0.0683 d5=None | n=755 marked=340 meanH=+0.073% hit=43.8% d1=-0.0663 d5=None | n=1 marked=1 meanH=-0.852% hit=0.0% d1=-0.8523 d5=None |

## RWT 2026-09-11 short

- present=True blocked=True rsi=18.36 1d=-14.788734286201677 rvol=12.573968312854408 crash=True ret_h=-0.8523 ret_1d=-0.8523 ret_5d=None
- audit: `RWT SHORT 2026-09-11 BLOCKED by oversold_crash_pause (rsi=18.36, 1d=-14.788734286201677, rule=rsi<=25 & crash)`

## Blocked blotter

| Date | Ticker | Side | RSI | 1d | H% | Lesson |
|---|---|---|---:|---:|---:|---|
| 2026-08-17 | KLC | short | 19.93 | -46.16977338091569 | 2.2901 | oversold_crash_pause |
| 2026-08-18 | INV | short | 24.02 | -14.241487165034815 | -0.0 | oversold_crash_pause |
| 2026-08-19 | BIDU | short | 23.05 | -12.72570077840779 | -1.2097 | oversold_crash_pause |
| 2026-09-03 | AIIO | short | 24.86 | -17.0731664767653 | -8.9386 | oversold_crash_pause |
| 2026-09-11 | RWT | short | 18.36 | -14.788734286201677 | -0.8523 | oversold_crash_pause |

## Filters shipped vs dropped

- shipped: ['oversold_crash_pause']
- dropped: [{'id': 'overbought_meltup_pause', 'why': 'IS blocked-sleeve mean > 0 (HTFL/FDMT/OABI melt-up longs kept paying). Symmetric seed failed the falsifier — do not ship.'}, {'id': 'hard_red_rsi30_short', 'lesson_id': 'hard_red_rsi30_short', 'side': 'short', 'why': 'F3 n is tiny and overlaps F1. Dropped — do not fish a third predicate. (n_extra=6 mean_h=-0.9994 — n tiny / do not fish a third predicate)', 'n_extra_blocked': 6, 'blocked_mean_h': -0.9994}]
- F3 eval: {'n_extra': 6, 'mean_h': -0.9994, 'dropped': True}

## Coverage holes

- dated strategy_tickets missing 2026-08-13→2026-09-09 — reconstructed from panel pick_day + stock-book files
- rsi missing 29/2764; ret_1 missing 18/2764; news/camera missing 961/2764
- hold-horizon unmarked 485/2764 (exit not in archive)
- macro / tape_anchor / channel1 not attached on ticket rows — unused in v1
- data/prices/ohlc.parquet asof 2026-09-11 — 09-14/09-15 official opens/closes are missing, so RWT hold>1 and OOS 5d marks are unmarked. Do not invent the 3.83/3.94 bounce.
- 09-12/09-13 are weekend — not sessions

- dated tickets: ['2026-09-10', '2026-09-11', '2026-09-14', '2026-09-15']
- reconstructed: ['2026-08-13', '2026-08-14', '2026-08-17', '2026-08-18', '2026-08-19', '2026-08-20', '2026-08-21', '2026-08-24', '2026-08-25', '2026-08-26', '2026-08-27', '2026-08-28', '2026-08-31', '2026-09-01', '2026-09-02', '2026-09-03', '2026-09-04', '2026-09-08', '2026-09-09']

North star remains ~2%/day after fees. These numbers are archive marks, not a live claim.

