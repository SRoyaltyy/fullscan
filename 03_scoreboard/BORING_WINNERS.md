# Boring winners — book × mine overlay

Starts from the **same 1d BUY list** the Top Gainer As-Of walk uses, then overlays the fixed FEATURE_MINE stacks. Equal-weight, close-to-close, clip ±30 on the book line. Per-name 1d/2d/3d/1w are raw.

**2026-09-03 is this morning's live book.** 1d/2d are blank until later tapes. Buys/sells vs yesterday are the eval.

## How a seat is won

1. **Keep** a 1d BUY name that is not fade. Prefer a named mine stack.
2. **Drop** a 1d BUY name that printed fade / first_crack.
3. **Swap** a stack-less book name for a gated extra with a named stack.
4. **Add** at most 5 extras the book missed — only if they pass the BUY floor (`mcap ≥ $400M`, `ADV ≥ 500k`, not micro) and print a named stack. Extras also honor the book's 4/sector, 3/industry, 4 large/mega caps.
5. **Paint** every seat with the 12 as-of cameras, 6 coaches, 🔵🚨⚪ marks, Cond, and hall-pass — same board as Top Gainer As-Of.
6. **Short** is book SELL ∩ fade, not the raw fade dump.

Mine-only 25-seat fill is kept as a comparison column. It is not the live book.

## Edges the overlay prefers

| priority | stack | mine-board 1d | role |
|---:|---|---|---|
| 1 | `hot+ab+peer` | 70.6% hit · +3.14 mean · n=51 | scalp, if the book or a gated extra has it |
| 2 | `steady+blue` | 52.0% hit · +9.54 mean · n=1394 | core swing |
| 3 | `blue+white` | 49.4% hit · +10.48 mean · n=1246 | white only with blue |
| 4 | `blue` | 57.7% hit · +4.46 mean · n=3387 | baseline |
| 5 | `ab AND peer` | ~65% hit · ~+1 mean | modest fill |
| 6 | `alarm AND NOT white` | 47.7% hit · +2.27 mean | rebound |
| 7 | `rsi=oversold` / `gap=down` | low hit · huge mean | extras only if mine_score ≥ 3 |
| short | book SELL ∩ `fade` | 38.2% hit · −0.72 mean | short only |

Never seated as a long: white alone, fade, `ab OR peer` dump, `join AND Band` alphabet dump, micro / sub-$400M extras.
Thin 1d BUY mornings stay thin — we do not force 25 junk seats. A cameras print from **2026-08-20**. Panel yfinance 1d is settled through **2026-08-20**; later days use the next Finviz tape (`Price`/`Price`, else `Change%`). The parquet is not rebuilt.

Per-day files: `03_scoreboard/boring_winners/<date>.md` · today also at `01_daily/<date>_boring_winners.md` and `latest_boring_winners.md`.

## Daily book returns

| date | stacks | n | keep | add | drop | overlay 1d | book BUY 1d | mine-only 1d | uni med | 2d | W | L |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `rsi_oversold+gap_down` | 25 | 25 | 0 | 0 | +2.00 | +2.00 | +3.37 | +0.00 | +3.47 | 9 | 8 |
| 2026-08-14 | `steady_blue+blue_white` | 25 | 25 | 0 | 0 | +0.52 | +0.52 | +1.58 | -0.23 | -0.86 | 10 | 7 |
| 2026-08-17 | `steady_blue+blue_white` | 23 | 23 | 0 | 2 | -1.56 | -1.59 | -2.15 | -0.59 | +0.48 | 10 | 13 |
| 2026-08-18 | `alarm_rebound` | 22 | 17 | 5 | 8 | +2.32 | +2.07 | -0.74 | +0.88 | +1.68 | 20 | 2 |
| 2026-08-19 | `steady_blue+blue+alarm_rebound` | 25 | 24 | 1 | 1 | -0.64 | -0.78 | -0.99 | -0.97 | -0.42 | 6 | 18 |
| 2026-08-20 | `steady_blue+blue_white+blue+hot_ab_peer` | 17 | 15 | 2 | 0 | +0.93 | +1.21 | +1.16 | +0.72 | +0.29 | 9 | 8 |
| 2026-08-21 | `hot_ab_peer+steady_blue+blue_white+blue+ab_and_peer` | 24 | 22 | 2 | 3 | -0.41 | -0.74 | +0.69 | -0.36 | -0.04 | 11 | 12 |
| 2026-08-27 | `steady_blue+blue_white+ab_and_peer+hot_ab_peer` | 19 | 16 | 3 | 9 | -0.13 | -0.27 | +1.13 | -0.13 | -1.15 | 8 | 11 |
| 2026-08-30 | `steady_blue+ab_and_peer+alarm_rebound+hot_ab_peer` | 25 | 20 | 5 | 5 | +0.14 | +0.06 | -0.10 | +0.00 | -0.62 | 7 | 6 |
| 2026-08-31 | `hot_ab_peer+steady_blue+blue` | 12 | 10 | 2 | 0 | -0.33 | -0.48 | -0.08 | -0.80 | -1.27 | 5 | 7 |
| 2026-09-01 | `steady_blue+blue+ab_and_peer` | 11 | 10 | 1 | 0 | -1.01 | -1.01 | -0.64 | -1.04 | -0.60 | 2 | 9 |
| 2026-09-02 | `hot_ab_peer+blue` | 13 | 10 | 3 | 0 | +0.08 | +0.41 | -0.29 | +0.75 | — | 7 | 5 |
| 2026-09-03 | `hot_ab_peer+blue_white+blue` | 12 | 8 | 4 | 0 | — | — | — | — | — | 0 | 0 |

Overlay 1d: 12 priced days · p(loss day)=50.0% · mean=+0.16 · cum=+1.91.
Stock-book BUY 1d (same panel): 12 priced days · p(loss day)=50.0% · mean=+0.12 · cum=+1.40.
Mine-only 25 1d (comparison): 12 priced days · p(loss day)=58.3% · mean=+0.24 · cum=+2.93.
Overlay names 1d: n=225 · p_win=46.2% · p_loss=47.1% · avg_win=+2.42 · avg_loss=-2.04 · mean=+0.15 · clip30=+0.15 · payoff=1.18.
Overlay names 2d: n=212 · p_win=50.5% · p_loss=49.5% · avg_win=+3.24 · avg_loss=-3.05 · mean=+0.13 · clip30=+0.13 · payoff=1.06.

## Daily short overlay (book SELL ∩ fade, −1 × clipped name return)

| date | n | 1d | 2d | new | covered |
|---|---:|---:|---:|---:|---:|
| 2026-08-13 | 0 | — | — | 0 | 0 |
| 2026-08-14 | 0 | — | — | 0 | 0 |
| 2026-08-17 | 0 | — | — | 0 | 0 |
| 2026-08-18 | 0 | — | — | 0 | 0 |
| 2026-08-19 | 0 | — | — | 0 | 0 |
| 2026-08-20 | 0 | — | — | 0 | 0 |
| 2026-08-21 | 0 | — | — | 0 | 0 |
| 2026-08-27 | 0 | — | — | 0 | 0 |
| 2026-08-30 | 0 | — | — | 0 | 0 |
| 2026-08-31 | 8 | +2.93 | +3.93 | 8 | 0 |
| 2026-09-01 | 7 | +2.44 | +0.55 | 7 | 8 |
| 2026-09-02 | 4 | -0.15 | — | 4 | 7 |
| 2026-09-03 | 0 | — | — | 0 | 4 |

Short overlay 1d: 3 priced days · p(loss day)=33.3% · mean=+1.74 · cum=+5.22.

## Each day's stocks

`keep` = on 1d BUY. `add` = gated extra. `buy` / `hold` this morning. `sell` = dropped overnight. Cameras and coaches are the Top Gainer As-Of 09:30 ET paint.

### 2026-08-13 · `rsi_oversold+gap_down` · n=25 (keep 25 / add 0 / drop 0)

Market: — · tone `good`

1d BUY walk × mined stacks: keep / drop fade / swap stack-less / add gated extras → lottery `rsi=oversold` (low hit, huge mean) → lottery `gap=down`

Overlay 1d +2.00 · 2d +3.47 · 3d +4.66 · 1w +5.07 · W/L 9/8 · stock-book BUY 1d +2.00 · mine-only 1d +3.37 · universe med +0.00.

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| 1 | buy | `MBRX` | book | `rsi_oversold` | 🔵 — ⚪ | 🟢 4/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw⬛ | +7.18 | +6.30 | +6.83 | +3.50 |
| 2 | buy | `ABEO` | book | `gap_down` | 🔵 — ⚪ | 🟢 4/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw⬛ | -5.67 | -6.46 | -2.99 | -6.14 |
| 3 | buy | `TKVA` | book | `none` | 🔵 — ⚪ | 🟢 4/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw⬛ | — | — | — | — |
| 4 | buy | `APNAU` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | — | — | — | — |
| 5 | buy | `BCACU` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | — | — | — | — |
| 6 | buy | `IDIAU` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | — | — | — | — |
| 7 | buy | `LEDRU` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | — | — | — | — |
| 8 | buy | `PHAXU` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | — | — | — | — |
| 9 | buy | `SCATU` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | — | — | — | — |
| 10 | buy | `SKAIU` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | — | — | — | — |
| 11 | buy | `TNDM` | book | `none` | 🔵 — ⚪ | 🟢 4/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw⬛ | -1.77 | -3.78 | +2.59 | -2.42 |
| 12 | buy | `ACHV` | book | `none` | 🔵 — ⚪ | 🟢 4/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw⬛ | +4.69 | +3.45 | +3.72 | +10.62 |
| 13 | buy | `AGEN` | book | `none` | 🔵 — ⚪ | 🟢 4/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw⬛ | +4.57 | +2.14 | +7.57 | +7.00 |
| 14 | buy | `VOR` | book | `none` | 🔵 — ⚪ | 🟢 4/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw⬛ | -1.12 | -1.20 | -0.04 | -0.97 |
| 15 | buy | `SGRY` | book | `none` | 🔵 — ⚪ | 🟢 4/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw⬛ | -1.05 | -5.46 | -6.25 | -8.23 |
| 16 | buy | `WW` | book | `none` | 🔵 — ⚪ | 🟢 4/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw⬛ | -2.70 | -6.02 | -3.88 | +3.81 |
| 17 | buy | `FDMT` | book | `none` | 🔵 — ⚪ | 🟢 4/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw⬛ | +4.63 | +24.85 | +26.35 | +38.20 |
| 18 | buy | `FTRE` | book | `none` | 🔵 — ⚪ | 🟢 4/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw⬛ | -2.30 | +1.57 | -1.24 | +2.08 |
| 19 | buy | `IMNN` | book | `none` | 🔵 — ⚪ | 🟢 4/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw⬛ | +4.43 | +1.27 | +0.00 | +0.00 |
| 20 | buy | `NRXP` | book | `none` | 🔵 — ⚪ | 🟢 4/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw⬛ | -2.10 | +2.69 | +2.99 | +5.09 |
| 21 | buy | `PROK` | book | `none` | 🔵 — ⚪ | 🟢 4/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw⬛ | -5.34 | -0.36 | -1.78 | +1.07 |
| 22 | buy | `SPRB` | book | `none` | 🔵 — ⚪ | 🟢 4/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw⬛ | +5.77 | +6.33 | +6.18 | +4.45 |
| 23 | buy | `TGTX` | book | `none` | 🔵 — ⚪ | 🟢 4/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw⬛ | +1.67 | +2.79 | +4.84 | +5.46 |
| 24 | buy | `UNCY` | book | `none` | 🔵 — ⚪ | 🟢 4/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw⬛ | +2.04 | +3.15 | +4.26 | +0.93 |
| 25 | buy | `INO` | book | `none` | 🔵 — ⚪ | 🟢 4/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw⬛ | +21.11 | +27.78 | +33.33 | +34.44 |

Seats 1d n=17 · p_win=52.9% · p_loss=47.1% · avg_win=+6.23 · avg_loss=-2.76 · mean=+2.00 · clip30=+2.00 · payoff=2.26.

### 2026-08-14 · `steady_blue+blue_white` · n=25 (keep 25 / add 0 / drop 0)

Market: — · tone `good`

1d BUY walk × mined stacks: keep / drop fade / swap stack-less / add gated extras → core `steady+blue` (52% hit / +9.54 mean on the mine board) → swing `blue+white` (white only with blue)

Overlay 1d +0.52 · 2d -0.86 · 3d -0.45 · 1w -0.89 · W/L 10/7 · stock-book BUY 1d +0.52 · mine-only 1d +1.58 · universe med -0.23.

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| 1 | buy | `TLN` | book | `steady_blue` | 🔵 — — | 🟢 4/1/1 | ⬜ | join🟢 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | -1.60 | -12.43 | -11.13 | -13.31 |
| 2 | buy | `VST` | book | `steady_blue` | 🔵 — — | 🟢 4/1/1 | ⬜ | join🟢 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | -1.36 | -5.14 | -3.67 | -8.05 |
| 3 | buy | `DVN` | book | `steady_blue` | 🔵 — — | 🟢 4/1/1 | ⬜ | join🟢 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +3.75 | +4.32 | +5.10 | +7.09 |
| 4 | buy | `EOG` | book | `steady_blue` | 🔵 — — | 🟢 4/1/1 | ⬜ | join🟢 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +2.48 | +4.27 | +4.82 | +7.32 |
| 5 | buy | `FANG` | book | `steady_blue` | 🔵 — — | 🟢 4/1/1 | ⬜ | join🟢 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +1.89 | +3.73 | +3.00 | +4.08 |
| 6 | buy | `OXY` | book | `steady_blue` | 🔵 — — | 🟢 3/2/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +1.17 | +2.47 | +2.96 | +5.04 |
| 7 | buy | `COP` | book | `steady_blue` | 🔵 — — | 🟢 3/2/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +0.61 | +2.32 | +3.00 | +6.38 |
| 8 | buy | `NRG` | book | `steady_blue` | 🔵 — — | 🟢 4/1/1 | ⬜ | join🟢 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | -3.07 | -8.46 | -4.48 | -10.40 |
| 9 | buy | `CEG` | book | `steady_blue` | 🔵 — — | 🟢 4/1/1 | ⬜ | join🟢 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | -1.67 | -5.55 | -2.95 | -3.40 |
| 10 | buy | `XOM` | book | `steady_blue` | 🔵 — — | 🟢 3/2/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +0.85 | +3.41 | +2.92 | +3.13 |
| 11 | buy | `CVX` | book | `steady_blue` | 🔵 — — | 🟢 3/2/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +0.47 | +1.98 | +2.88 | +2.63 |
| 12 | buy | `NEE` | book | `steady_blue` | 🔵 — — | 🟢 4/1/1 | ⬜ | join🟢 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +0.04 | +0.04 | -0.33 | -2.95 |
| 13 | buy | `APA` | book | `blue_white` | 🔵 — — | 🟢 3/2/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +2.74 | +4.70 | +7.36 | +7.21 |
| 14 | hold | `APNAU` | book | `none` | — — ⚪ | 🟢 4/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟢 yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🟡 | — | — | — | — |
| 15 | hold | `BCACU` | book | `none` | — — ⚪ | 🟢 4/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟢 yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🟡 | — | — | — | — |
| 16 | buy | `BEMD` | book | `none` | 🔵 — ⚪ | 🟢 3/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy🟡 yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | -0.52 | -0.80 | -0.36 | -0.76 |
| 17 | hold | `IDIAU` | book | `none` | — — ⚪ | 🟢 4/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟢 yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🟡 | — | — | — | — |
| 18 | hold | `LEDRU` | book | `none` | — — ⚪ | 🟢 4/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟢 yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🟡 | — | — | — | — |
| 19 | hold | `PHAXU` | book | `none` | — — ⚪ | 🟢 4/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟢 yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🟡 | — | — | — | — |
| 20 | hold | `SCATU` | book | `none` | — — ⚪ | 🟢 4/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟢 yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🟡 | — | — | — | — |
| 21 | hold | `SKAIU` | book | `none` | — — ⚪ | 🟢 4/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟢 yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🟡 | — | — | — | — |
| 22 | buy | `CLRS` | book | `none` | — — ⚪ | 🟢 4/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟢 yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🟡 | — | — | — | — |
| 23 | buy | `PGY` | book | `none` | — — — | 🟢 3/2/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟡 AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟡 set⬜ flw🔴 | -2.92 | -5.75 | -5.06 | +0.56 |
| 24 | buy | `TBCH` | book | `none` | — — — | 🟢 3/2/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟡 AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟡 set⬜ flw🔴 | -4.00 | -2.75 | -3.22 | -0.71 |
| 25 | buy | `WOLF` | book | `none` | — — ⚪ | 🟡 3/3/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟡 AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟡 set⬜ flw🟡 | +9.97 | -1.04 | -8.49 | -18.97 |

Sold overnight:

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| — | sell | `ABEO` | book | `gap_down` | 🔵 — ⚪ | 🟢 4/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw⬛ | -5.67 | -6.46 | -2.99 | -6.14 |
| — | sell | `ACHV` | book | `none` | 🔵 — ⚪ | 🟢 4/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw⬛ | +4.69 | +3.45 | +3.72 | +10.62 |
| — | sell | `AGEN` | book | `none` | 🔵 — ⚪ | 🟢 4/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw⬛ | +4.57 | +2.14 | +7.57 | +7.00 |
| — | sell | `FDMT` | book | `none` | 🔵 — ⚪ | 🟢 4/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw⬛ | +4.63 | +24.85 | +26.35 | +38.20 |
| — | sell | `FTRE` | book | `none` | 🔵 — ⚪ | 🟢 4/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw⬛ | -2.30 | +1.57 | -1.24 | +2.08 |
| — | sell | `IMNN` | book | `none` | 🔵 — ⚪ | 🟢 4/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw⬛ | +4.43 | +1.27 | +0.00 | +0.00 |
| — | sell | `INO` | book | `none` | 🔵 — ⚪ | 🟢 4/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw⬛ | +21.11 | +27.78 | +33.33 | +34.44 |
| — | sell | `MBRX` | book | `rsi_oversold` | 🔵 — ⚪ | 🟢 4/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw⬛ | +7.18 | +6.30 | +6.83 | +3.50 |
| — | sell | `NRXP` | book | `none` | 🔵 — ⚪ | 🟢 4/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw⬛ | -2.10 | +2.69 | +2.99 | +5.09 |
| — | sell | `PROK` | book | `none` | 🔵 — ⚪ | 🟢 4/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw⬛ | -5.34 | -0.36 | -1.78 | +1.07 |
| — | sell | `SGRY` | book | `none` | 🔵 — ⚪ | 🟢 4/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw⬛ | -1.05 | -5.46 | -6.25 | -8.23 |
| — | sell | `SPRB` | book | `none` | 🔵 — ⚪ | 🟢 4/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw⬛ | +5.77 | +6.33 | +6.18 | +4.45 |
| — | sell | `TGTX` | book | `none` | 🔵 — ⚪ | 🟢 4/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw⬛ | +1.67 | +2.79 | +4.84 | +5.46 |
| — | sell | `TKVA` | book | `none` | 🔵 — ⚪ | 🟢 4/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw⬛ | — | — | — | — |
| — | sell | `TNDM` | book | `none` | 🔵 — ⚪ | 🟢 4/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw⬛ | -1.77 | -3.78 | +2.59 | -2.42 |
| — | sell | `UNCY` | book | `none` | 🔵 — ⚪ | 🟢 4/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw⬛ | +2.04 | +3.15 | +4.26 | +0.93 |
| — | sell | `VOR` | book | `none` | 🔵 — ⚪ | 🟢 4/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw⬛ | -1.12 | -1.20 | -0.04 | -0.97 |
| — | sell | `WW` | book | `none` | 🔵 — ⚪ | 🟢 4/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw⬛ | -2.70 | -6.02 | -3.88 | +3.81 |

Seats 1d n=17 · p_win=58.8% · p_loss=41.2% · avg_win=+2.40 · avg_loss=-2.16 · mean=+0.52 · clip30=+0.52 · payoff=1.11.

### 2026-08-17 · `steady_blue+blue_white` · n=23 (keep 23 / add 0 / drop 2)

Market: — · tone `good`

1d BUY walk × mined stacks: keep / drop fade / swap stack-less / add gated extras → core `steady+blue` (52% hit / +9.54 mean on the mine board) → swing `blue+white` (white only with blue)

Overlay 1d -1.56 · 2d +0.48 · 3d +1.24 · 1w +2.36 · W/L 10/13 · stock-book BUY 1d -1.59 · mine-only 1d -2.15 · universe med -0.59.

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| 1 | buy | `TMC` | book | `steady_blue` | 🔵 — — | 🟢 4/1/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🔴 AB⬜ peer⬜ heat⬜ vol🟢 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🔴 set⬜ flw🟢 | +3.98 | +5.30 | +5.30 | +20.79 |
| 2 | buy | `TGB` | book | `steady_blue` | 🔵 — — | 🟢 3/2/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🔴 AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🔴 set⬜ flw🟡 | -4.67 | -3.42 | -0.91 | +6.24 |
| 3 | buy | `ERO` | book | `steady_blue` | — — — | 🟢 3/1/2 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🔴 AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🔴 set⬜ flw🔴 | -5.56 | -2.38 | +3.18 | +9.34 |
| 4 | buy | `MOS` | book | `steady_blue` | 🔵 — — | 🟢 3/1/2 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🔴 AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🔴 set⬜ flw🔴 | -0.42 | +4.71 | +9.99 | +14.29 |
| 5 | buy | `INTC` | book | `steady_blue` | — 🚨 ⚪ | 🟡 3/4/0 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg🟡 AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🟡 | -6.57 | -10.33 | -10.98 | -16.81 |
| 6 | buy | `AMD` | book | `steady_blue` | — 🚨 ⚪ | 🟡 3/4/0 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg🟡 AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🟡 | -4.27 | -7.82 | -7.22 | -10.47 |
| 7 | buy | `NVDA` | book | `steady_blue` | — 🚨 — | 🟡 3/3/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg🟡 AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | -2.34 | -3.31 | -3.63 | -7.38 |
| 8 | buy | `MU` | book | `blue_white` | — 🚨 ⚪ | 🟡 3/4/0 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg🟡 AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🟡 | -7.02 | -7.38 | -3.70 | -11.77 |
| 9 | hold | `DVN` | book | `none` | — — — | 🟢 5/0/1 | ⬜ | join🟢 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +0.55 | +1.30 | +3.64 | +2.77 |
| 10 | hold | `EOG` | book | `none` | — — — | 🟢 5/0/1 | ⬜ | join🟢 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +1.75 | +2.28 | +4.13 | +4.22 |
| 11 | hold | `FANG` | book | `none` | — — — | 🟢 5/0/1 | ⬜ | join🟢 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +1.81 | +1.10 | +2.29 | +1.03 |
| 12 | hold | `OXY` | book | `none` | — — — | 🟢 4/1/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +1.29 | +1.78 | +4.20 | +2.44 |
| 13 | hold | `APA` | book | `none` | — — — | 🟢 4/1/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +1.90 | +4.50 | +6.76 | +3.57 |
| 14 | hold | `COP` | book | `none` | — — — | 🟢 4/1/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +1.69 | +2.37 | +5.75 | +6.14 |
| 15 | hold | `XOM` | book | `none` | — — — | 🟢 4/1/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +2.54 | +2.05 | +2.90 | +1.31 |
| 16 | hold | `CVX` | book | `none` | 🔵 — ⚪ | 🟢 4/2/0 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🟡 | +1.50 | +2.40 | +2.40 | +0.57 |
| 17 | hold | `TLN` | book | `none` | — 🚨 — | 🟢 4/1/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | -11.00 | -9.69 | -11.19 | -14.24 |
| 18 | hold | `CEG` | book | `none` | — 🚨 — | 🟢 4/1/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | -3.94 | -1.30 | -1.75 | -1.17 |
| 19 | hold | `NRG` | book | `none` | — 🚨 — | 🟢 4/1/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | -5.57 | -1.46 | -5.73 | -8.61 |
| 20 | buy | `ELF` | book | `none` | — — — | 🟢 3/1/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🔴 | -1.23 | +6.39 | +5.12 | +11.78 |
| 21 | buy | `DNN` | book | `none` | — — ⚪ | 🟢 3/2/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🟡 | -1.25 | +0.94 | -1.57 | +8.59 |
| 22 | buy | `HNST` | book | `none` | — 🚨 ⚪ | 🟡 2/3/0 | ⬜ | join🟡 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🟡 | +1.06 | +6.81 | +5.53 | +9.66 |
| 23 | buy | `EL` | book | `none` | — — — | 🟢 3/1/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🔴 | -0.08 | +16.21 | +14.00 | +21.92 |

Dropped from 1d BUY:

| Ticker | why | stack | 1d |
|---|---|---|---:|
| `VST` | fade | `fade` | -3.83 |
| `NEE` | fade | `fade` | +0.00 |

Sold overnight:

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| — | sell | `APNAU` | book | `none` | — — ⚪ | 🟢 4/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟢 yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🟡 | — | — | — | — |
| — | sell | `BCACU` | book | `none` | — — ⚪ | 🟢 4/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟢 yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🟡 | — | — | — | — |
| — | sell | `BEMD` | book | `none` | 🔵 — ⚪ | 🟢 3/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy🟡 yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | -0.52 | -0.80 | -0.36 | -0.76 |
| — | sell | `CLRS` | book | `none` | — — ⚪ | 🟢 4/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟢 yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🟡 | — | — | — | — |
| — | sell | `IDIAU` | book | `none` | — — ⚪ | 🟢 4/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟢 yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🟡 | — | — | — | — |
| — | sell | `LEDRU` | book | `none` | — — ⚪ | 🟢 4/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟢 yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🟡 | — | — | — | — |
| — | sell | `NEE` | book | `steady_blue` | 🔵 — — | 🟢 4/1/1 | ⬜ | join🟢 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +0.04 | +0.04 | -0.33 | -2.95 |
| — | sell | `PGY` | book | `none` | — — — | 🟢 3/2/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟡 AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟡 set⬜ flw🔴 | -2.92 | -5.75 | -5.06 | +0.56 |
| — | sell | `PHAXU` | book | `none` | — — ⚪ | 🟢 4/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟢 yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🟡 | — | — | — | — |
| — | sell | `SCATU` | book | `none` | — — ⚪ | 🟢 4/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟢 yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🟡 | — | — | — | — |
| — | sell | `SKAIU` | book | `none` | — — ⚪ | 🟢 4/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟢 yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🟡 | — | — | — | — |
| — | sell | `TBCH` | book | `none` | — — — | 🟢 3/2/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟡 AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟡 set⬜ flw🔴 | -4.00 | -2.75 | -3.22 | -0.71 |
| — | sell | `VST` | book | `steady_blue` | 🔵 — — | 🟢 4/1/1 | ⬜ | join🟢 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | -1.36 | -5.14 | -3.67 | -8.05 |
| — | sell | `WOLF` | book | `none` | — — ⚪ | 🟡 3/3/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟡 AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟡 set⬜ flw🟡 | +9.97 | -1.04 | -8.49 | -18.97 |

Seats 1d n=23 · p_win=43.5% · p_loss=56.5% · avg_win=+1.81 · avg_loss=-4.15 · mean=-1.56 · clip30=-1.56 · payoff=0.43.

### 2026-08-18 · `alarm_rebound` · n=22 (keep 17 / add 5 / drop 8)

Market: — · tone `bad`

1d BUY walk × mined stacks: keep / drop fade / swap stack-less / add gated extras → rebound `alarm AND NOT white`

Overlay 1d +2.32 · 2d +1.68 · 3d +1.92 · 1w +1.13 · W/L 20/2 · stock-book BUY 1d +2.07 · mine-only 1d -0.74 · universe med +0.88.

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| 1 | buy | `MUR` | book | `alarm_rebound` | — — — | 🟡 2/2/1 | ⬜ | join🟡 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🟡 cat⬜ buy🟢 yΔ🔴 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🟡 | +4.80 | +8.81 | +6.34 | +1.74 |
| 2 | buy | `ABEV` | extra | `alarm_rebound` | — — — | 🟡 2/2/1 | ⬜ | join🟢 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🟡 | +1.08 | +1.80 | +3.60 | +3.93 |
| 3 | buy | `DHT` | extra | `alarm_rebound` | — 🚨 — | 🟡 1/3/1 | ⬜ | join🟡 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🟡 | +3.52 | +2.54 | +2.64 | +2.36 |
| 4 | buy | `ENB` | extra | `alarm_rebound` | — 🚨 — | 🟡 1/2/2 | ⬜ | join🟡 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🔴 | -2.22 | -0.78 | -1.81 | -2.36 |
| 5 | buy | `CIG` | extra | `alarm_rebound` | — — — | 🟡 2/2/1 | ⬜ | join🟡 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🟢 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🟢 | +1.58 | -1.05 | +1.05 | +2.62 |
| 6 | buy | `SFD` | extra | `alarm_rebound` | — — — | 🟡 1/3/1 | ⬜ | join🟡 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🟡 | +0.09 | -1.11 | -1.24 | -1.72 |
| 7 | buy | `STE` | book | `none` | — — — | 🟢 3/1/2 | ⬜ | join🟢 sect🟢 gen🔴 news⬛ dig⬜ jdg🟢 AB⬜ peer⬛ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +1.95 | +1.44 | +2.38 | +2.78 |
| 8 | buy | `DHR` | book | `none` | — — — | 🟢 3/1/2 | ⬜ | join🟢 sect🟢 gen🔴 news⬛ dig⬜ jdg🟢 AB⬜ peer⬛ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +5.97 | +8.18 | +9.65 | +7.54 |
| 9 | buy | `SYK` | book | `none` | — — — | 🟢 3/1/2 | ⬜ | join🟢 sect🟢 gen🔴 news⬛ dig⬜ jdg🟢 AB⬜ peer⬛ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +2.60 | -1.11 | -0.58 | -2.55 |
| 10 | buy | `TMO` | book | `none` | — — — | 🟢 3/1/2 | ⬜ | join🟢 sect🟢 gen🔴 news⬛ dig⬜ jdg🟢 AB⬜ peer⬛ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +4.16 | +6.54 | +6.83 | +7.15 |
| 11 | buy | `EW` | book | `none` | — — — | 🟢 3/1/2 | ⬜ | join🟢 sect🟢 gen🔴 news⬛ dig⬜ jdg🟢 AB⬜ peer⬛ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +1.43 | -1.26 | -1.31 | -2.36 |
| 12 | buy | `UTHR` | book | `none` | — — — | 🟢 3/1/2 | ⬜ | join🟢 sect🟢 gen🔴 news⬛ dig⬜ jdg🟢 AB⬜ peer⬛ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +2.10 | +0.53 | -0.99 | -0.47 |
| 13 | buy | `COO` | book | `none` | — — — | 🟢 3/1/2 | ⬜ | join🟢 sect🟢 gen🔴 news⬛ dig⬜ jdg🟢 AB⬜ peer⬛ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +1.28 | +0.53 | +0.61 | -0.98 |
| 14 | buy | `FAST` | book | `none` | — — — | 🟡 2/1/2 | ⬜ | join🟢 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🔴 | +0.33 | -1.19 | -0.02 | -0.45 |
| 15 | buy | `MDT` | book | `none` | — — — | 🟢 3/1/2 | ⬜ | join🟢 sect🟢 gen🔴 news⬛ dig⬜ jdg🟢 AB⬜ peer⬛ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +2.21 | +0.23 | +1.37 | +0.57 |
| 16 | buy | `IQV` | book | `none` | — — — | 🟡 2/3/1 | ⬜ | join🟡 sect🟢 gen🔴 news⬛ dig⬜ jdg🟢 AB⬜ peer⬛ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd⬜ co🟢 set⬜ flw🟡 | +3.85 | +6.67 | +8.24 | +5.18 |
| 17 | buy | `RMD` | book | `none` | — — — | 🟢 3/1/2 | ⬜ | join🟢 sect🟢 gen🔴 news⬛ dig⬜ jdg🟢 AB⬜ peer⬛ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +4.85 | +4.14 | +5.55 | +3.66 |
| 18 | buy | `ES` | book | `none` | — — — | 🟡 2/1/2 | ⬜ | join🟢 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🔴 | -0.32 | -0.10 | -2.78 | -1.61 |
| 19 | buy | `AOS` | book | `none` | — — — | 🟡 2/1/2 | ⬜ | join🟢 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🔴 | +4.83 | +2.58 | +3.65 | +2.75 |
| 20 | buy | `GGG` | book | `none` | — — — | 🟡 2/1/2 | ⬜ | join🟢 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🔴 | +2.07 | +0.86 | -0.46 | -1.44 |
| 21 | buy | `MLYS` | book | `none` | — — — | 🟡 2/2/2 | ⬜ | join🟡 sect🟢 gen🔴 news⬛ dig⬜ jdg🟢 AB⬜ peer⬛ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +1.28 | -3.23 | -3.61 | -4.25 |
| 22 | buy | `PFE` | book | `none` | — — — | 🟢 3/2/1 | ⬜ | join🟢 sect🟢 gen🔴 news⬛ dig⬜ jdg🟢 AB⬜ peer⬛ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd⬜ co🟢 set⬜ flw🟡 | +3.63 | +1.98 | +3.01 | +2.75 |

Dropped from 1d BUY:

| Ticker | why | stack | 1d |
|---|---|---|---:|
| `OXY` | fade | `fade` | +0.48 |
| `COP` | fade | `fade` | +0.66 |
| `APA` | fade | `fade` | +2.55 |
| `DVN` | fade | `fade` | +0.75 |
| `EOG` | fade | `fade` | +0.53 |
| `FANG` | fade | `fade` | -0.70 |
| `XOM` | fade | `fade` | -0.48 |
| `CVX` | fade | `fade` | +0.88 |

Sold overnight:

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| — | sell | `AMD` | book | `steady_blue` | — 🚨 ⚪ | 🟡 3/4/0 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg🟡 AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🟡 | -4.27 | -7.82 | -7.22 | -10.47 |
| — | sell | `APA` | book | `none` | — — — | 🟢 4/1/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +1.90 | +4.50 | +6.76 | +3.57 |
| — | sell | `CEG` | book | `none` | — 🚨 — | 🟢 4/1/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | -3.94 | -1.30 | -1.75 | -1.17 |
| — | sell | `COP` | book | `none` | — — — | 🟢 4/1/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +1.69 | +2.37 | +5.75 | +6.14 |
| — | sell | `CVX` | book | `none` | 🔵 — ⚪ | 🟢 4/2/0 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🟡 | +1.50 | +2.40 | +2.40 | +0.57 |
| — | sell | `DNN` | book | `none` | — — ⚪ | 🟢 3/2/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🟡 | -1.25 | +0.94 | -1.57 | +8.59 |
| — | sell | `DVN` | book | `none` | — — — | 🟢 5/0/1 | ⬜ | join🟢 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +0.55 | +1.30 | +3.64 | +2.77 |
| — | sell | `EL` | book | `none` | — — — | 🟢 3/1/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🔴 | -0.08 | +16.21 | +14.00 | +21.92 |
| — | sell | `ELF` | book | `none` | — — — | 🟢 3/1/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🔴 | -1.23 | +6.39 | +5.12 | +11.78 |
| — | sell | `EOG` | book | `none` | — — — | 🟢 5/0/1 | ⬜ | join🟢 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +1.75 | +2.28 | +4.13 | +4.22 |
| — | sell | `ERO` | book | `steady_blue` | — — — | 🟢 3/1/2 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🔴 AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🔴 set⬜ flw🔴 | -5.56 | -2.38 | +3.18 | +9.34 |
| — | sell | `FANG` | book | `none` | — — — | 🟢 5/0/1 | ⬜ | join🟢 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +1.81 | +1.10 | +2.29 | +1.03 |
| — | sell | `HNST` | book | `none` | — 🚨 ⚪ | 🟡 2/3/0 | ⬜ | join🟡 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🟡 | +1.06 | +6.81 | +5.53 | +9.66 |
| — | sell | `INTC` | book | `steady_blue` | — 🚨 ⚪ | 🟡 3/4/0 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg🟡 AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🟡 | -6.57 | -10.33 | -10.98 | -16.81 |
| — | sell | `MOS` | book | `steady_blue` | 🔵 — — | 🟢 3/1/2 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🔴 AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🔴 set⬜ flw🔴 | -0.42 | +4.71 | +9.99 | +14.29 |
| — | sell | `MU` | book | `blue_white` | — 🚨 ⚪ | 🟡 3/4/0 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg🟡 AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🟡 | -7.02 | -7.38 | -3.70 | -11.77 |
| — | sell | `NRG` | book | `none` | — 🚨 — | 🟢 4/1/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | -5.57 | -1.46 | -5.73 | -8.61 |
| — | sell | `NVDA` | book | `steady_blue` | — 🚨 — | 🟡 3/3/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg🟡 AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | -2.34 | -3.31 | -3.63 | -7.38 |
| — | sell | `OXY` | book | `none` | — — — | 🟢 4/1/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +1.29 | +1.78 | +4.20 | +2.44 |
| — | sell | `TGB` | book | `steady_blue` | 🔵 — — | 🟢 3/2/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🔴 AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🔴 set⬜ flw🟡 | -4.67 | -3.42 | -0.91 | +6.24 |
| — | sell | `TLN` | book | `none` | — 🚨 — | 🟢 4/1/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | -11.00 | -9.69 | -11.19 | -14.24 |
| — | sell | `TMC` | book | `steady_blue` | 🔵 — — | 🟢 4/1/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🔴 AB⬜ peer⬜ heat⬜ vol🟢 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🔴 set⬜ flw🟢 | +3.98 | +5.30 | +5.30 | +20.79 |
| — | sell | `XOM` | book | `none` | — — — | 🟢 4/1/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +2.54 | +2.05 | +2.90 | +1.31 |

Seats 1d n=22 · p_win=90.9% · p_loss=9.1% · avg_win=+2.68 · avg_loss=-1.27 · mean=+2.32 · clip30=+2.32 · payoff=2.11.

### 2026-08-19 · `steady_blue+blue+alarm_rebound` · n=25 (keep 24 / add 1 / drop 1)

Market: — · tone `bad`

1d BUY walk × mined stacks: keep / drop fade / swap stack-less / add gated extras → core `steady+blue` (52% hit / +9.54 mean on the mine board) → baseline `blue` → rebound `alarm AND NOT white`

Overlay 1d -0.64 · 2d -0.42 · 3d -0.13 · 1w — · W/L 6/18 · stock-book BUY 1d -0.78 · mine-only 1d -0.99 · universe med -0.97.

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| 1 | buy | `NEE` | book | `steady_blue` | 🔵 🚨 — | 🟡 2/4/1 | ⬜ | join🟢 sect🟡 gen🔴 news🟢 dig⬜ jdg⬛ AB⬛ peer🟡 heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🔴 par🟡 chd⬜ co🟢 set⬛ flw🟡 | -1.02 | -2.63 | -2.56 | — |
| 2 | buy | `NRG` | book | `blue` | 🔵 🚨 — | 🔴 1/2/4 | ⬜ | join🔴 sect🟡 gen🔴 news🟢 dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟡 chd⬜ co🟢 set⬛ flw🔴 | -4.33 | -6.20 | -5.21 | — |
| 3 | hold | `MUR` | book | `alarm_rebound` | — 🚨 — | 🔴 1/2/3 | ⬜ | join🟡 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | +3.82 | +1.47 | -1.22 | — |
| 4 | hold | `IQV` | book | `alarm_rebound` | — 🚨 — | 🔴 1/2/3 | ⬜ | join🟡 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | +2.72 | +4.23 | +4.41 | — |
| 5 | buy | `SKM` | extra | `blue` | 🔵 — — | 🟡 2/3/1 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🟢 heat⬜ vol🟡 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🟡 | -1.11 | +4.00 | +4.58 | — |
| 6 | hold | `STE` | book | `none` | — 🚨 — | 🔴 2/1/3 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -0.49 | +0.43 | +0.44 | — |
| 7 | hold | `DHR` | book | `none` | — 🚨 — | 🔴 2/1/3 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🔴 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | +2.08 | +3.48 | +2.49 | — |
| 8 | hold | `SYK` | book | `none` | — — — | 🟡 2/2/2 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🟡 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🟡 | -3.61 | -3.10 | -2.83 | — |
| 9 | hold | `UTHR` | book | `none` | — 🚨 — | 🔴 2/1/3 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -1.54 | -3.03 | -2.41 | — |
| 10 | buy | `CEG` | book | `none` | — 🚨 — | 🟡 1/3/3 | ⬜ | join🟡 sect🟡 gen🔴 news🟢 dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟡 chd⬜ co🟢 set⬛ flw🔴 | -0.46 | -0.47 | +0.17 | — |
| 11 | hold | `EW` | book | `none` | — 🚨 — | 🔴 2/1/3 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -2.65 | -2.70 | -2.06 | — |
| 12 | hold | `FAST` | book | `none` | — 🚨 — | 🔴 2/1/3 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -1.52 | -0.35 | +0.12 | — |
| 13 | hold | `COO` | book | `none` | — 🚨 — | 🟡 1/3/2 | ⬜ | join🟡 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🟡 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -0.74 | -0.67 | -1.52 | — |
| 14 | hold | `TMO` | book | `none` | — 🚨 — | 🔴 1/2/3 | ⬜ | join🟡 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | +2.28 | +2.56 | +0.66 | — |
| 15 | buy | `VST` | book | `none` | — 🚨 — | 🟡 1/3/3 | ⬜ | join🟡 sect🟡 gen🔴 news🟢 dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟡 chd⬜ co🟢 set⬛ flw🔴 | -2.63 | -4.55 | -3.09 | — |
| 16 | hold | `MDT` | book | `none` | — 🚨 — | 🟢 3/1/2 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🟢 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -1.94 | -0.83 | -0.36 | — |
| 17 | buy | `WTW` | book | `none` | 🔵 — — | 🔴 2/1/3 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | +1.70 | +1.95 | +4.19 | — |
| 18 | hold | `GGG` | book | `none` | — 🚨 — | 🟡 2/2/2 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🟡 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🔴 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -1.19 | -2.48 | -1.27 | — |
| 19 | hold | `RMD` | book | `none` | — 🚨 — | 🔴 2/1/3 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -0.67 | +0.67 | +2.40 | — |
| 20 | hold | `ES` | book | `none` | — 🚨 — | 🟢 3/1/2 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🟢 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | +0.22 | -2.47 | -1.51 | — |
| 21 | buy | `ITW` | book | `none` | — 🚨 — | 🟡 0/4/2 | ⬜ | join🟡 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🟡 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -0.53 | -0.69 | -1.38 | — |
| 22 | hold | `AOS` | book | `none` | — 🚨 — | 🔴 1/2/3 | ⬜ | join🟡 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🔴 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -2.15 | -1.13 | -1.30 | — |
| 23 | buy | `ELV` | book | `none` | — 🚨 — | 🔴 2/1/3 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -0.55 | +0.53 | +0.29 | — |
| 24 | buy | `HLN` | book | `none` | — — — | 🟡 2/2/2 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🟡 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🟡 | +0.00 | +1.32 | +3.46 | — |
| 25 | buy | `ZBH` | book | `none` | — 🚨 — | 🟢 3/1/2 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🟢 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -1.76 | +0.24 | +0.35 | — |

Dropped from 1d BUY:

| Ticker | why | stack | 1d |
|---|---|---|---:|
| `MLYS` | swapped_for_mine_extra | `none` | -4.45 |

Sold overnight:

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| — | sell | `ABEV` | extra | `alarm_rebound` | — — — | 🟡 2/2/1 | ⬜ | join🟢 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🟡 | +1.08 | +1.80 | +3.60 | +3.93 |
| — | sell | `CIG` | extra | `alarm_rebound` | — — — | 🟡 2/2/1 | ⬜ | join🟡 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🟢 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🟢 | +1.58 | -1.05 | +1.05 | +2.62 |
| — | sell | `DHT` | extra | `alarm_rebound` | — 🚨 — | 🟡 1/3/1 | ⬜ | join🟡 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🟡 | +3.52 | +2.54 | +2.64 | +2.36 |
| — | sell | `ENB` | extra | `alarm_rebound` | — 🚨 — | 🟡 1/2/2 | ⬜ | join🟡 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🔴 | -2.22 | -0.78 | -1.81 | -2.36 |
| — | sell | `MLYS` | book | `none` | — — — | 🟡 2/2/2 | ⬜ | join🟡 sect🟢 gen🔴 news⬛ dig⬜ jdg🟢 AB⬜ peer⬛ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +1.28 | -3.23 | -3.61 | -4.25 |
| — | sell | `PFE` | book | `none` | — — — | 🟢 3/2/1 | ⬜ | join🟢 sect🟢 gen🔴 news⬛ dig⬜ jdg🟢 AB⬜ peer⬛ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd⬜ co🟢 set⬜ flw🟡 | +3.63 | +1.98 | +3.01 | +2.75 |
| — | sell | `SFD` | extra | `alarm_rebound` | — — — | 🟡 1/3/1 | ⬜ | join🟡 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🟡 | +0.09 | -1.11 | -1.24 | -1.72 |

Seats 1d n=25 · p_win=24.0% · p_loss=72.0% · avg_win=+2.14 · avg_loss=-1.60 · mean=-0.64 · clip30=-0.64 · payoff=1.33.

### 2026-08-20 · `steady_blue+blue_white+blue+hot_ab_peer` · n=17 (keep 15 / add 2 / drop 0)

Market: — · tone `good`

1d BUY walk × mined stacks: keep / drop fade / swap stack-less / add gated extras → core `steady+blue` (52% hit / +9.54 mean on the mine board) → swing `blue+white` (white only with blue) → baseline `blue` → scalp `hot+ab+peer` (70.6% hit, small n)

Overlay 1d +0.93 · 2d +0.29 · 3d +0.52 · 1w -0.85 · W/L 9/8 · stock-book BUY 1d +1.21 · mine-only 1d +1.16 · universe med +0.72.

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| 1 | buy | `GSHD` | book | `steady_blue` | 🔵 — — | 🟢 5/2/1 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟢 set🟢 flw🔴 | +2.74 | +4.98 | +3.78 | +1.38 |
| 2 | buy | `ELF` | book | `steady_blue` | 🔵 — — | 🟢 4/3/1 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟡 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟡 set🟢 flw🔴 | +3.53 | +4.52 | +6.20 | +7.39 |
| 3 | buy | `MOS` | book | `steady_blue` | 🔵 — — | 🟢 4/3/2 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟢 jdg🟢 AB🔴 peer🔴 heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟢 set🔴 flw🟡 | +4.54 | +4.39 | +3.35 | +5.13 |
| 4 | buy | `CE` | book | `steady_blue` | 🔵 — — | 🟢 6/2/1 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟢 jdg🟢 AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟢 set🟢 flw🔴 | -0.38 | -2.60 | -2.79 | -4.14 |
| 5 | buy | `IRTC` | book | `steady_blue` | 🔵 — — | 🟡 3/3/2 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟢 jdg⬛ AB🟡 peer🔴 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟢 set🟡 flw🔴 | +0.42 | -0.78 | -2.67 | -6.86 |
| 6 | buy | `CALX` | book | `steady_blue` | 🔵 — — | 🟢 5/2/2 | ⬜ | join🟡 sect🟢 gen🟢 news⬛ dig🟢 jdg🔴 AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟡 set🟢 flw🔴 | -0.64 | +0.20 | -1.36 | -4.93 |
| 7 | buy | `OGS` | book | `steady_blue` | 🔵 — — | 🟢 4/3/1 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟡 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟡 set🟢 flw🔴 | -3.15 | -3.61 | -2.39 | -1.48 |
| 8 | buy | `HLNE` | book | `steady_blue` | 🔵 — — | 🟢 5/2/1 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟢 set🟢 flw🔴 | +1.24 | +1.82 | +1.14 | +1.06 |
| 9 | buy | `NHI` | book | `steady_blue` | 🔵 — — | 🟢 4/1/2 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig⬛ jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🔴 chd⬜ co⬛ set🟢 flw🔴 | -0.74 | -1.48 | -1.93 | -1.50 |
| 10 | buy | `AUPH` | book | `blue_white` | 🔵 — — | 🟢 4/3/1 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🔴 heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟢 set🟢 flw🟡 | -3.59 | -2.87 | -2.22 | -3.22 |
| 11 | buy | `OCUL` | book | `blue_white` | 🔵 — ⚪ | 🟢 5/3/0 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟢 set🟢 flw🟡 | -0.09 | -7.09 | -4.46 | -5.77 |
| 12 | buy | `WRBY` | book | `blue_white` | 🔵 — — | 🟢 5/2/1 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟢 set🟢 flw🔴 | -0.07 | -4.73 | -2.58 | -7.24 |
| 13 | buy | `CELH` | book | `blue_white` | 🔵 — ⚪ | 🟡 4/4/0 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟡 jdg⬛ AB🟢 peer🟢 heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟡 set🟢 flw🟡 | +2.52 | +7.11 | +8.84 | +8.84 |
| 14 | buy | `FIGR` | book | `blue_white` | 🔵 — ⚪ | 🟢 4/3/0 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer⬛ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟢 set🟢 flw🟡 | +8.43 | +6.98 | +7.39 | +2.29 |
| 15 | buy | `EPAM` | book | `blue` | 🔵 — — | 🟢 5/2/2 | ⬜ | join🔴 sect🟢 gen🟢 news⬛ dig🟢 jdg🔴 AB🟢 peer🟢 heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟡 set🟢 flw🟡 | +3.35 | +3.32 | +2.97 | +1.13 |
| 16 | buy | `KC` | extra | `hot_ab_peer` | 🔵 — — | 🟢 6/1/2 | ⬜ | join🔴 sect🟢 gen🟢 news⬛ dig🟢 jdg🔴 AB🟢 peer🟢 heat⬜ vol🟢 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟡 set🟢 flw🟢 | -2.45 | -6.40 | -5.78 | -9.07 |
| 17 | buy | `UBS` | extra | `hot_ab_peer` | 🔵 — ⚪ | 🟢 6/2/0 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🟢 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟢 set🟢 flw🟢 | +0.17 | +1.18 | +1.30 | +2.50 |

Sold overnight:

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| — | sell | `AOS` | book | `none` | — 🚨 — | 🔴 1/2/3 | ⬜ | join🟡 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🔴 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -2.15 | -1.13 | -1.30 | — |
| — | sell | `CEG` | book | `none` | — 🚨 — | 🟡 1/3/3 | ⬜ | join🟡 sect🟡 gen🔴 news🟢 dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟡 chd⬜ co🟢 set⬛ flw🔴 | -0.46 | -0.47 | +0.17 | — |
| — | sell | `COO` | book | `none` | — 🚨 — | 🟡 1/3/2 | ⬜ | join🟡 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🟡 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -0.74 | -0.67 | -1.52 | — |
| — | sell | `DHR` | book | `none` | — 🚨 — | 🔴 2/1/3 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🔴 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | +2.08 | +3.48 | +2.49 | — |
| — | sell | `ELV` | book | `none` | — 🚨 — | 🔴 2/1/3 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -0.55 | +0.53 | +0.29 | — |
| — | sell | `ES` | book | `none` | — 🚨 — | 🟢 3/1/2 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🟢 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | +0.22 | -2.47 | -1.51 | — |
| — | sell | `EW` | book | `none` | — 🚨 — | 🔴 2/1/3 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -2.65 | -2.70 | -2.06 | — |
| — | sell | `FAST` | book | `none` | — 🚨 — | 🔴 2/1/3 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -1.52 | -0.35 | +0.12 | — |
| — | sell | `GGG` | book | `none` | — 🚨 — | 🟡 2/2/2 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🟡 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🔴 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -1.19 | -2.48 | -1.27 | — |
| — | sell | `HLN` | book | `none` | — — — | 🟡 2/2/2 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🟡 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🟡 | +0.00 | +1.32 | +3.46 | — |
| — | sell | `IQV` | book | `alarm_rebound` | — 🚨 — | 🔴 1/2/3 | ⬜ | join🟡 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | +2.72 | +4.23 | +4.41 | — |
| — | sell | `ITW` | book | `none` | — 🚨 — | 🟡 0/4/2 | ⬜ | join🟡 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🟡 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -0.53 | -0.69 | -1.38 | — |
| — | sell | `MDT` | book | `none` | — 🚨 — | 🟢 3/1/2 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🟢 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -1.94 | -0.83 | -0.36 | — |
| — | sell | `MUR` | book | `alarm_rebound` | — 🚨 — | 🔴 1/2/3 | ⬜ | join🟡 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | +3.82 | +1.47 | -1.22 | — |
| — | sell | `NEE` | book | `steady_blue` | 🔵 🚨 — | 🟡 2/4/1 | ⬜ | join🟢 sect🟡 gen🔴 news🟢 dig⬜ jdg⬛ AB⬛ peer🟡 heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🔴 par🟡 chd⬜ co🟢 set⬛ flw🟡 | -1.02 | -2.63 | -2.56 | — |
| — | sell | `NRG` | book | `blue` | 🔵 🚨 — | 🔴 1/2/4 | ⬜ | join🔴 sect🟡 gen🔴 news🟢 dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟡 chd⬜ co🟢 set⬛ flw🔴 | -4.33 | -6.20 | -5.21 | — |
| — | sell | `RMD` | book | `none` | — 🚨 — | 🔴 2/1/3 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -0.67 | +0.67 | +2.40 | — |
| — | sell | `SKM` | extra | `blue` | 🔵 — — | 🟡 2/3/1 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🟢 heat⬜ vol🟡 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🟡 | -1.11 | +4.00 | +4.58 | — |
| — | sell | `STE` | book | `none` | — 🚨 — | 🔴 2/1/3 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -0.49 | +0.43 | +0.44 | — |
| — | sell | `SYK` | book | `none` | — — — | 🟡 2/2/2 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🟡 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🟡 | -3.61 | -3.10 | -2.83 | — |
| — | sell | `TMO` | book | `none` | — 🚨 — | 🔴 1/2/3 | ⬜ | join🟡 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | +2.28 | +2.56 | +0.66 | — |
| — | sell | `UTHR` | book | `none` | — 🚨 — | 🔴 2/1/3 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -1.54 | -3.03 | -2.41 | — |
| — | sell | `VST` | book | `none` | — 🚨 — | 🟡 1/3/3 | ⬜ | join🟡 sect🟡 gen🔴 news🟢 dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟡 chd⬜ co🟢 set⬛ flw🔴 | -2.63 | -4.55 | -3.09 | — |
| — | sell | `WTW` | book | `none` | 🔵 — — | 🔴 2/1/3 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | +1.70 | +1.95 | +4.19 | — |
| — | sell | `ZBH` | book | `none` | — 🚨 — | 🟢 3/1/2 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🟢 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -1.76 | +0.24 | +0.35 | — |

Seats 1d n=17 · p_win=52.9% · p_loss=47.1% · avg_win=+3.00 · avg_loss=-1.39 · mean=+0.93 · clip30=+0.93 · payoff=2.15.

### 2026-08-21 · `hot_ab_peer+steady_blue+blue_white+blue+ab_and_peer` · n=24 (keep 22 / add 2 / drop 3)

Market: — · tone `good`

1d BUY walk × mined stacks: keep / drop fade / swap stack-less / add gated extras → scalp `hot+ab+peer` (70.6% hit, small n) → core `steady+blue` (52% hit / +9.54 mean on the mine board) → swing `blue+white` (white only with blue) → baseline `blue` → scalp `ab AND peer` (high hit, modest mean)

Overlay 1d -0.41 · 2d -0.04 · 3d — · 1w -0.68 · W/L 11/12 · stock-book BUY 1d -0.74 · mine-only 1d +0.69 · universe med -0.36.

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| 1 | buy | `CRSP` | book | `hot_ab_peer` | 🔵 — ⚪ | 🟢 7/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🟢 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🟢 | -8.15 | -7.96 | — | -4.18 |
| 2 | buy | `OPCH` | book | `steady_blue` | 🔵 — — | 🟢 5/1/2 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🔴 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🔴 | -0.50 | +0.96 | — | +0.04 |
| 3 | buy | `XP` | book | `steady_blue` | 🔵 — ⚪ | 🟢 6/2/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🟡 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🟡 | +3.49 | +1.01 | — | +4.68 |
| 4 | buy | `FBRT` | book | `steady_blue` | — — — | 🟢 4/1/2 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬛ jdg⬛ AB🟢 peer🔴 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co⬛ set🟢 flw🔴 | +0.60 | +0.96 | — | +0.60 |
| 5 | hold | `MOS` | book | `steady_blue` | 🔵 — ⚪ | 🟢 7/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟡 peer🟢 heat⬜ vol🟢 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set🟡 flw🟢 | -2.56 | -3.53 | — | -3.49 |
| 6 | hold | `GSHD` | book | `steady_blue` | 🔵 — — | 🟢 7/0/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🔴 | +2.17 | +1.01 | — | -2.52 |
| 7 | buy | `WGS` | book | `steady_blue` | — — — | 🟢 5/1/2 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🔴 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🔴 | +2.18 | +1.77 | — | +6.22 |
| 8 | hold | `HLNE` | book | `steady_blue` | 🔵 — — | 🟢 7/0/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🔴 | +0.96 | +0.29 | — | +1.73 |
| 9 | hold | `NHI` | book | `steady_blue` | 🔵 — — | 🟢 6/0/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬛ jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co⬛ set🟢 flw🔴 | -0.85 | -1.31 | — | -2.00 |
| 10 | hold | `CE` | book | `steady_blue` | 🔵 — — | 🟢 7/0/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🔴 | -4.44 | -4.63 | — | -5.60 |
| 11 | buy | `NMRK` | book | `steady_blue` | — — — | 🟢 5/1/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬛ jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co⬛ set🟢 flw🔴 | -0.25 | +1.57 | — | -2.51 |
| 12 | buy | `Z` | book | `steady_blue` | — — — | 🟡 3/2/3 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟡 jdg⬛ AB🟢 peer🔴 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🔴 chd⬜ co🟡 set🟢 flw🔴 | +1.16 | +4.37 | — | -2.10 |
| 13 | buy | `DNN` | book | `steady_blue` | 🔵 — — | 🟢 4/2/3 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟢 jdg🟢 AB🔴 peer🔴 heat⬜ vol🟡 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🔴 chd⬜ co🟢 set🔴 flw🟡 | +1.14 | +2.57 | — | +4.86 |
| 14 | hold | `OCUL` | book | `blue_white` | — — — | 🟢 7/0/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🔴 | -6.18 | -3.53 | — | -4.42 |
| 15 | buy | `ABR` | book | `blue_white` | — — — | 🟢 4/1/2 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬛ jdg⬛ AB🟢 peer🔴 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co⬛ set🟢 flw🔴 | +0.00 | +0.38 | — | -4.01 |
| 16 | hold | `FIGR` | book | `blue_white` | 🔵 — ⚪ | 🟢 6/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer⬛ heat⬜ vol🟡 cat⬜ buy🟢 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🟡 | -0.49 | -0.10 | — | -2.44 |
| 17 | buy | `GPK` | book | `blue` | — — — | 🔴 3/1/4 | ⬜ | join🔴 sect🔴 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🔴 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🔴 chd⬜ co🟢 set🟢 flw🔴 | -4.87 | -4.54 | — | -6.44 |
| 18 | hold | `EPAM` | book | `ab_and_peer` | — 🚨 — | 🟢 6/0/3 | ⬜ | join🔴 sect🟢 gen🟢 news⬛ dig🟢 jdg🔴 AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟡 set🟢 flw🔴 | -0.01 | -0.35 | — | +0.92 |
| 19 | buy | `ZG` | book | `ab_and_peer` | — — — | 🟢 4/2/2 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟡 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🔴 chd⬜ co🟡 set🟢 flw🔴 | +1.21 | +3.07 | — | -2.02 |
| 20 | buy | `VNT` | book | `ab_and_peer` | — — — | 🟢 5/1/3 | ⬜ | join🔴 sect🟢 gen🟢 news⬛ dig🟢 jdg🔴 AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟡 set🟢 flw🔴 | +0.24 | +0.48 | — | +0.09 |
| 21 | buy | `NFG` | book | `ab_and_peer` | 🔵 — — | 🟢 6/1/2 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟢 jdg🟢 AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🔴 chd⬜ co🟢 set🟢 flw🔴 | -1.46 | -0.88 | — | -0.39 |
| 22 | buy | `BFAM` | book | `ab_and_peer` | — 🚨 — | 🟢 4/1/3 | ⬜ | join🔴 sect🔴 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🔴 chd⬜ co🟢 set🟢 flw🔴 | +3.16 | +1.48 | — | +1.82 |
| 23 | buy | `MT` | extra | `hot_ab_peer` | 🔵 — ⚪ | 🟢 7/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🟢 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🟢 | -0.20 | +0.71 | — | +2.09 |
| 24 | buy | `SUZ` | extra | `hot_ab_peer` | 🔵 — ⚪ | 🟢 7/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🟢 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🟢 | +3.78 | +5.16 | — | +2.63 |

Dropped from 1d BUY:

| Ticker | why | stack | 1d |
|---|---|---|---:|
| `DUOL` | fade | `fade` | -0.46 |
| `CALX` | fade | `fade` | +0.37 |
| `BZ` | fade | `fade` | -4.89 |

Sold overnight:

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| — | sell | `AUPH` | book | `blue_white` | 🔵 — — | 🟢 4/3/1 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🔴 heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟢 set🟢 flw🟡 | -3.59 | -2.87 | -2.22 | -3.22 |
| — | sell | `CALX` | book | `steady_blue` | 🔵 — — | 🟢 5/2/2 | ⬜ | join🟡 sect🟢 gen🟢 news⬛ dig🟢 jdg🔴 AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟡 set🟢 flw🔴 | -0.64 | +0.20 | -1.36 | -4.93 |
| — | sell | `CELH` | book | `blue_white` | 🔵 — ⚪ | 🟡 4/4/0 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟡 jdg⬛ AB🟢 peer🟢 heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟡 set🟢 flw🟡 | +2.52 | +7.11 | +8.84 | +8.84 |
| — | sell | `ELF` | book | `steady_blue` | 🔵 — — | 🟢 4/3/1 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟡 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟡 set🟢 flw🔴 | +3.53 | +4.52 | +6.20 | +7.39 |
| — | sell | `IRTC` | book | `steady_blue` | 🔵 — — | 🟡 3/3/2 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟢 jdg⬛ AB🟡 peer🔴 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟢 set🟡 flw🔴 | +0.42 | -0.78 | -2.67 | -6.86 |
| — | sell | `KC` | extra | `hot_ab_peer` | 🔵 — — | 🟢 6/1/2 | ⬜ | join🔴 sect🟢 gen🟢 news⬛ dig🟢 jdg🔴 AB🟢 peer🟢 heat⬜ vol🟢 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟡 set🟢 flw🟢 | -2.45 | -6.40 | -5.78 | -9.07 |
| — | sell | `OGS` | book | `steady_blue` | 🔵 — — | 🟢 4/3/1 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟡 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟡 set🟢 flw🔴 | -3.15 | -3.61 | -2.39 | -1.48 |
| — | sell | `UBS` | extra | `hot_ab_peer` | 🔵 — ⚪ | 🟢 6/2/0 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🟢 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟢 set🟢 flw🟢 | +0.17 | +1.18 | +1.30 | +2.50 |
| — | sell | `WRBY` | book | `blue_white` | 🔵 — — | 🟢 5/2/1 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟢 set🟢 flw🔴 | -0.07 | -4.73 | -2.58 | -7.24 |

Seats 1d n=24 · p_win=45.8% · p_loss=50.0% · avg_win=+1.83 · avg_loss=-2.50 · mean=-0.41 · clip30=-0.41 · payoff=0.73.

### 2026-08-27 · `steady_blue+blue_white+ab_and_peer+hot_ab_peer` · n=19 (keep 16 / add 3 / drop 9)

Market: — · tone `neutral`

1d BUY walk × mined stacks: keep / drop fade / swap stack-less / add gated extras → core `steady+blue` (52% hit / +9.54 mean on the mine board) → swing `blue+white` (white only with blue) → scalp `ab AND peer` (high hit, modest mean) → scalp `hot+ab+peer` (70.6% hit, small n)

Overlay 1d -0.13 · 2d -1.15 · 3d -1.15 · 1w -2.42 · W/L 8/11 · stock-book BUY 1d -0.27 · mine-only 1d +1.13 · universe med -0.13.

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| 1 | buy | `ACMR` | book | `steady_blue` | 🔵 — — | 🟢 8/2/1 | ⬜ | join🟢 sect🟢 gen🟢 news🟢 dig🟢 jdg🟡 AB🟢 peer🟢 heat🟢 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🟢 chd🟢 co🟢 set🟢 flw🔴 | +1.74 | -5.93 | -5.93 | -9.14 |
| 2 | hold | `VNT` | book | `steady_blue` | 🔵 — — | 🟢 7/1/2 | ⬜ | join🔴 sect🟢 gen🟢 news⬛ dig🟢 jdg🟡 AB🟢 peer🟢 heat🟢 vol🔴 cat⬜ buy🟢 yΔ⬛ | mkt🟡 par🟢 chd🟢 co🟢 set🟢 flw🔴 | -1.69 | -2.76 | -2.76 | -4.39 |
| 3 | buy | `MNDY` | book | `steady_blue` | 🔵 — — | 🟢 6/2/2 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg🟡 AB🟢 peer🟢 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🟢 chd🔴 co🟢 set🟢 flw🔴 | +7.51 | +8.82 | +8.82 | +7.17 |
| 4 | buy | `CRK` | book | `steady_blue` | — — — | 🟡 4/1/4 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🔴 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🔴 chd🔴 co🟢 set🟢 flw🔴 | +0.83 | -1.45 | -1.45 | +2.76 |
| 5 | buy | `CXT` | book | `steady_blue` | — — — | 🟢 4/2/3 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟡 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🔴 chd🔴 co🟢 set🟢 flw🔴 | -1.84 | -1.01 | -1.01 | -3.44 |
| 6 | hold | `BFAM` | book | `steady_blue` | — — — | 🟢 6/0/3 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat🔴 vol🔴 cat⬜ buy🟢 yΔ⬛ | mkt🟡 par🔴 chd🔴 co🟢 set🟢 flw🔴 | -0.17 | +0.47 | +0.47 | -0.78 |
| 7 | buy | `SFM` | book | `steady_blue` | — 🚨 — | 🟡 3/3/3 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟡 jdg⬛ AB🟢 peer🔴 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🟡 chd🔴 co🟡 set🟢 flw🔴 | -6.67 | -6.18 | -6.18 | -5.56 |
| 8 | buy | `DEC` | book | `steady_blue` | — — — | 🔴 4/1/5 | ⬜ | join🔴 sect🔴 gen🟢 news🟢 dig🟢 jdg⬛ AB🟢 peer🔴 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🔴 chd🔴 co🟢 set🟢 flw🔴 | +0.94 | -1.21 | -1.21 | +0.34 |
| 9 | buy | `KBR` | book | `steady_blue` | — — — | 🟢 5/1/3 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🔴 chd🔴 co🟢 set🟢 flw🔴 | +0.69 | +0.26 | +0.26 | +3.85 |
| 10 | buy | `OSK` | book | `steady_blue` | — — — | 🟢 5/1/3 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🔴 chd🔴 co🟢 set🟢 flw🔴 | -0.49 | -2.23 | -2.23 | -2.14 |
| 11 | buy | `RRC` | book | `steady_blue` | — — — | 🟢 5/1/4 | ⬜ | join🟢 sect🔴 gen🟢 news🟢 dig🟢 jdg⬛ AB🟢 peer🔴 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🔴 chd🔴 co🟢 set🟢 flw🔴 | +0.22 | -0.22 | -0.22 | -0.55 |
| 12 | hold | `EPAM` | book | `blue_white` | 🔵 — — | 🟢 7/1/2 | ⬜ | join🔴 sect🟢 gen🟢 news⬛ dig🟢 jdg🟡 AB🟢 peer🟢 heat🟢 vol🔴 cat⬜ buy🟢 yΔ⬛ | mkt🟡 par🟢 chd🟢 co🟢 set🟢 flw🔴 | +3.12 | +4.97 | +4.97 | +5.98 |
| 13 | buy | `ICL` | book | `ab_and_peer` | — — — | 🟢 6/1/3 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg🟢 AB🟢 peer🔴 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🟢 chd🔴 co🟢 set🟢 flw🔴 | -0.18 | -1.41 | -1.41 | -0.35 |
| 14 | buy | `ALHC` | book | `ab_and_peer` | — — — | 🔴 3/2/5 | ⬜ | join🔴 sect🟡 gen🟢 news⬛ dig🟢 jdg🔴 AB🟢 peer🔴 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🟡 chd🔴 co🟡 set🟢 flw🔴 | -2.45 | -2.01 | -2.01 | -3.60 |
| 15 | hold | `Z` | book | `ab_and_peer` | — — — | 🟢 6/2/1 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟡 jdg⬛ AB🟢 peer🟢 heat🟢 vol🔴 cat⬜ buy🟢 yΔ⬛ | mkt🟡 par🟡 chd🟢 co🟡 set🟢 flw🔴 | +1.37 | +2.03 | +2.03 | -1.69 |
| 16 | buy | `ROST` | extra | `hot_ab_peer` | — — — | 🟢 4/2/3 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟡 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🔴 chd🔴 co🟢 set🟢 flw🔴 | -2.71 | -3.27 | -3.27 | -3.50 |
| 17 | buy | `TRON` | extra | `hot_ab_peer` | — — — | 🟢 4/2/3 | ⬜ | join🟡 sect🔴 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🔴 chd🔴 co🟢 set🟢 flw🔴 | -0.47 | -3.26 | -3.26 | -20.93 |
| 18 | buy | `RZLT` | extra | `hot_ab_peer` | — — — | 🟢 5/2/3 | ⬜ | join🔴 sect🟡 gen🟢 news⬛ dig🟢 jdg🔴 AB🟢 peer🟢 heat🟢 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🟡 chd🟢 co🟡 set🟢 flw🔴 | -1.19 | -8.33 | -8.33 | -7.74 |
| 19 | hold | `GPK` | book | `none` | — — — | 🟢 6/0/3 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat🔴 vol🔴 cat⬜ buy🟢 yΔ⬛ | mkt🟡 par🔴 chd🔴 co🟢 set🟢 flw🔴 | -1.05 | +0.96 | +0.96 | -2.18 |

Dropped from 1d BUY:

| Ticker | why | stack | 1d |
|---|---|---|---:|
| `MOS` | fade | `fade` | -1.66 |
| `ELF` | fade | `fade` | -0.75 |
| `XP` | fade | `fade` | -0.73 |
| `SLI` | fade | `fade` | +1.15 |
| `LW` | fade | `fade` | -0.83 |
| `WT` | fade | `fade` | +1.13 |
| `TIGR` | fade | `fade` | -7.33 |
| `BXSL` | fade | `fade` | -0.76 |
| `DNN` | fade | `fade` | +1.10 |

Sold overnight:

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| — | sell | `ABR` | book | `blue_white` | — — — | 🟢 4/1/2 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬛ jdg⬛ AB🟢 peer🔴 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co⬛ set🟢 flw🔴 | +0.00 | +0.38 | — | -4.01 |
| — | sell | `CE` | book | `steady_blue` | 🔵 — — | 🟢 7/0/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🔴 | -4.44 | -4.63 | — | -5.60 |
| — | sell | `CRSP` | book | `hot_ab_peer` | 🔵 — ⚪ | 🟢 7/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🟢 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🟢 | -8.15 | -7.96 | — | -4.18 |
| — | sell | `DNN` | book | `steady_blue` | 🔵 — — | 🟢 4/2/3 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟢 jdg🟢 AB🔴 peer🔴 heat⬜ vol🟡 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🔴 chd⬜ co🟢 set🔴 flw🟡 | +1.14 | +2.57 | — | +4.86 |
| — | sell | `FBRT` | book | `steady_blue` | — — — | 🟢 4/1/2 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬛ jdg⬛ AB🟢 peer🔴 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co⬛ set🟢 flw🔴 | +0.60 | +0.96 | — | +0.60 |
| — | sell | `FIGR` | book | `blue_white` | 🔵 — ⚪ | 🟢 6/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer⬛ heat⬜ vol🟡 cat⬜ buy🟢 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🟡 | -0.49 | -0.10 | — | -2.44 |
| — | sell | `GSHD` | book | `steady_blue` | 🔵 — — | 🟢 7/0/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🔴 | +2.17 | +1.01 | — | -2.52 |
| — | sell | `HLNE` | book | `steady_blue` | 🔵 — — | 🟢 7/0/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🔴 | +0.96 | +0.29 | — | +1.73 |
| — | sell | `MOS` | book | `steady_blue` | 🔵 — ⚪ | 🟢 7/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟡 peer🟢 heat⬜ vol🟢 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set🟡 flw🟢 | -2.56 | -3.53 | — | -3.49 |
| — | sell | `MT` | extra | `hot_ab_peer` | 🔵 — ⚪ | 🟢 7/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🟢 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🟢 | -0.20 | +0.71 | — | +2.09 |
| — | sell | `NFG` | book | `ab_and_peer` | 🔵 — — | 🟢 6/1/2 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟢 jdg🟢 AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🔴 chd⬜ co🟢 set🟢 flw🔴 | -1.46 | -0.88 | — | -0.39 |
| — | sell | `NHI` | book | `steady_blue` | 🔵 — — | 🟢 6/0/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬛ jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co⬛ set🟢 flw🔴 | -0.85 | -1.31 | — | -2.00 |
| — | sell | `NMRK` | book | `steady_blue` | — — — | 🟢 5/1/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬛ jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co⬛ set🟢 flw🔴 | -0.25 | +1.57 | — | -2.51 |
| — | sell | `OCUL` | book | `blue_white` | — — — | 🟢 7/0/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🔴 | -6.18 | -3.53 | — | -4.42 |
| — | sell | `OPCH` | book | `steady_blue` | 🔵 — — | 🟢 5/1/2 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🔴 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🔴 | -0.50 | +0.96 | — | +0.04 |
| — | sell | `SUZ` | extra | `hot_ab_peer` | 🔵 — ⚪ | 🟢 7/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🟢 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🟢 | +3.78 | +5.16 | — | +2.63 |
| — | sell | `WGS` | book | `steady_blue` | — — — | 🟢 5/1/2 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🔴 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🔴 | +2.18 | +1.77 | — | +6.22 |
| — | sell | `XP` | book | `steady_blue` | 🔵 — ⚪ | 🟢 6/2/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🟡 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🟡 | +3.49 | +1.01 | — | +4.68 |
| — | sell | `ZG` | book | `ab_and_peer` | — — — | 🟢 4/2/2 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟡 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🔴 chd⬜ co🟡 set🟢 flw🔴 | +1.21 | +3.07 | — | -2.02 |

Seats 1d n=19 · p_win=42.1% · p_loss=57.9% · avg_win=+2.05 · avg_loss=-1.72 · mean=-0.13 · clip30=-0.13 · payoff=1.19.

### 2026-08-30 · `steady_blue+ab_and_peer+alarm_rebound+hot_ab_peer` · n=25 (keep 20 / add 5 / drop 5)

Market: — · tone `neutral`

1d BUY walk × mined stacks: keep / drop fade / swap stack-less / add gated extras → core `steady+blue` (52% hit / +9.54 mean on the mine board) → scalp `ab AND peer` (high hit, modest mean) → rebound `alarm AND NOT white` → scalp `hot+ab+peer` (70.6% hit, small n)

Overlay 1d +0.14 · 2d -0.62 · 3d -1.48 · 1w — · W/L 7/6 · stock-book BUY 1d +0.06 · mine-only 1d -0.10 · universe med +0.00.

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| 1 | buy | `DUOL` | book | `steady_blue` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | -0.21 | +6.00 | +8.02 | — |
| 2 | buy | `WAY` | book | `steady_blue` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +0.00 | -0.23 | -1.83 | — |
| 3 | buy | `ATHM` | book | `steady_blue` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +0.00 | -5.12 | -3.20 | — |
| 4 | hold | `BFAM` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +0.00 | -1.24 | -2.33 | — |
| 5 | buy | `NCNO` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | -0.48 | -1.70 | -3.00 | — |
| 6 | hold | `MNDY` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | -0.58 | -1.51 | -4.17 | — |
| 7 | buy | `KD` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | -0.30 | +1.89 | -0.61 | — |
| 8 | hold | `CRK` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +1.54 | +4.27 | +12.11 | — |
| 9 | buy | `ZG` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +0.00 | -2.24 | -6.15 | — |
| 10 | buy | `COUR` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +0.15 | -0.62 | -4.48 | — |
| 11 | buy | `LW` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +0.00 | -2.75 | -4.33 | — |
| 12 | buy | `KT` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +0.00 | +0.26 | +0.20 | — |
| 13 | buy | `ELF` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | -1.01 | +4.23 | +1.38 | — |
| 14 | hold | `Z` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | -0.06 | -3.65 | -6.20 | — |
| 15 | hold | `ALHC` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +0.00 | -1.61 | -0.22 | — |
| 16 | buy | `CVI` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +0.00 | +2.63 | +2.66 | — |
| 17 | buy | `TFPM` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +0.00 | -2.73 | -3.97 | — |
| 18 | buy | `PBH` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +0.00 | -1.26 | +0.32 | — |
| 19 | buy | `FIGR` | book | `alarm_rebound` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +1.00 | -0.97 | -6.52 | — |
| 20 | buy | `DTM` | extra | `hot_ab_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +0.78 | +1.15 | +0.35 | — |
| 21 | buy | `ANF` | extra | `hot_ab_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +0.58 | -3.65 | -5.21 | — |
| 22 | buy | `MATV` | extra | `hot_ab_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +0.00 | -1.98 | -0.66 | — |
| 23 | buy | `URBN` | extra | `hot_ab_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +0.00 | -0.49 | -2.22 | — |
| 24 | buy | `CRML` | extra | `hot_ab_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +1.26 | -0.70 | -2.81 | — |
| 25 | buy | `LEU` | book | `none` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +0.68 | -3.52 | -4.25 | — |

Dropped from 1d BUY:

| Ticker | why | stack | 1d |
|---|---|---|---:|
| `RRC` | fade | `fade` | +0.77 |
| `XP` | fade | `fade` | +0.00 |
| `HLNE` | fade | `fade` | +0.00 |
| `SEZL` | fade | `fade` | -0.10 |
| `MH` | swapped_for_mine_extra | `none` | +0.00 |

Sold overnight:

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| — | sell | `ACMR` | book | `steady_blue` | 🔵 — — | 🟢 8/2/1 | ⬜ | join🟢 sect🟢 gen🟢 news🟢 dig🟢 jdg🟡 AB🟢 peer🟢 heat🟢 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🟢 chd🟢 co🟢 set🟢 flw🔴 | +1.74 | -5.93 | -5.93 | -9.14 |
| — | sell | `CXT` | book | `steady_blue` | — — — | 🟢 4/2/3 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟡 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🔴 chd🔴 co🟢 set🟢 flw🔴 | -1.84 | -1.01 | -1.01 | -3.44 |
| — | sell | `DEC` | book | `steady_blue` | — — — | 🔴 4/1/5 | ⬜ | join🔴 sect🔴 gen🟢 news🟢 dig🟢 jdg⬛ AB🟢 peer🔴 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🔴 chd🔴 co🟢 set🟢 flw🔴 | +0.94 | -1.21 | -1.21 | +0.34 |
| — | sell | `EPAM` | book | `blue_white` | 🔵 — — | 🟢 7/1/2 | ⬜ | join🔴 sect🟢 gen🟢 news⬛ dig🟢 jdg🟡 AB🟢 peer🟢 heat🟢 vol🔴 cat⬜ buy🟢 yΔ⬛ | mkt🟡 par🟢 chd🟢 co🟢 set🟢 flw🔴 | +3.12 | +4.97 | +4.97 | +5.98 |
| — | sell | `GPK` | book | `none` | — — — | 🟢 6/0/3 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat🔴 vol🔴 cat⬜ buy🟢 yΔ⬛ | mkt🟡 par🔴 chd🔴 co🟢 set🟢 flw🔴 | -1.05 | +0.96 | +0.96 | -2.18 |
| — | sell | `ICL` | book | `ab_and_peer` | — — — | 🟢 6/1/3 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg🟢 AB🟢 peer🔴 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🟢 chd🔴 co🟢 set🟢 flw🔴 | -0.18 | -1.41 | -1.41 | -0.35 |
| — | sell | `KBR` | book | `steady_blue` | — — — | 🟢 5/1/3 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🔴 chd🔴 co🟢 set🟢 flw🔴 | +0.69 | +0.26 | +0.26 | +3.85 |
| — | sell | `OSK` | book | `steady_blue` | — — — | 🟢 5/1/3 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🔴 chd🔴 co🟢 set🟢 flw🔴 | -0.49 | -2.23 | -2.23 | -2.14 |
| — | sell | `ROST` | extra | `hot_ab_peer` | — — — | 🟢 4/2/3 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟡 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🔴 chd🔴 co🟢 set🟢 flw🔴 | -2.71 | -3.27 | -3.27 | -3.50 |
| — | sell | `RRC` | book | `steady_blue` | — — — | 🟢 5/1/4 | ⬜ | join🟢 sect🔴 gen🟢 news🟢 dig🟢 jdg⬛ AB🟢 peer🔴 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🔴 chd🔴 co🟢 set🟢 flw🔴 | +0.22 | -0.22 | -0.22 | -0.55 |
| — | sell | `RZLT` | extra | `hot_ab_peer` | — — — | 🟢 5/2/3 | ⬜ | join🔴 sect🟡 gen🟢 news⬛ dig🟢 jdg🔴 AB🟢 peer🟢 heat🟢 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🟡 chd🟢 co🟡 set🟢 flw🔴 | -1.19 | -8.33 | -8.33 | -7.74 |
| — | sell | `SFM` | book | `steady_blue` | — 🚨 — | 🟡 3/3/3 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟡 jdg⬛ AB🟢 peer🔴 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🟡 chd🔴 co🟡 set🟢 flw🔴 | -6.67 | -6.18 | -6.18 | -5.56 |
| — | sell | `TRON` | extra | `hot_ab_peer` | — — — | 🟢 4/2/3 | ⬜ | join🟡 sect🔴 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🔴 chd🔴 co🟢 set🟢 flw🔴 | -0.47 | -3.26 | -3.26 | -20.93 |
| — | sell | `VNT` | book | `steady_blue` | 🔵 — — | 🟢 7/1/2 | ⬜ | join🔴 sect🟢 gen🟢 news⬛ dig🟢 jdg🟡 AB🟢 peer🟢 heat🟢 vol🔴 cat⬜ buy🟢 yΔ⬛ | mkt🟡 par🟢 chd🟢 co🟢 set🟢 flw🔴 | -1.69 | -2.76 | -2.76 | -4.39 |

Seats 1d n=25 · p_win=28.0% · p_loss=24.0% · avg_win=+0.86 · avg_loss=-0.44 · mean=+0.14 · clip30=+0.14 · payoff=1.95.

### 2026-08-31 · `hot_ab_peer+steady_blue+blue` · n=12 (keep 10 / add 2 / drop 0)

Market: hard_red · tone `bad`

1d BUY walk × mined stacks: keep / drop fade / swap stack-less / add gated extras → scalp `hot+ab+peer` (70.6% hit, small n) → core `steady+blue` (52% hit / +9.54 mean on the mine board) → baseline `blue`

Overlay 1d -0.33 · 2d -1.27 · 3d -0.87 · 1w — · W/L 5/7 · stock-book BUY 1d -0.48 · mine-only 1d -0.08 · universe med -0.80.

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| 1 | buy | `CRM` | book | `hot_ab_peer` | 🔵 — — | 🟢 7/2/2 | probable | join🔴 sect🟢 gen🔴 news🟢 dig🟢 jdg🟡 AB🟢 peer🟢 heat🟢 vol🟢 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🔴 chd🟢 co🟢 set🟢 flw🟡 | +0.62 | +1.65 | +1.19 | — |
| 2 | buy | `CVX` | book | `steady_blue` | 🔵 — — | 🟢 6/3/2 | probable | join🟢 sect🟡 gen🔴 news🟢 dig🟢 jdg🟢 AB🟢 peer🔴 heat🟢 vol🟡 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd🟡 co🟡 set🟢 flw🟢 | +1.09 | +2.51 | +2.87 | — |
| 3 | buy | `BMO` | book | `steady_blue` | 🔵 — — | 🟢 6/2/2 | probable | join🟢 sect🟢 gen🔴 news🟢 dig🟢 jdg⬛ AB🟢 peer🔴 heat🟢 vol🟡 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | +6.36 | -3.10 | -0.50 | — |
| 4 | buy | `CVE` | book | `steady_blue` | 🔵 — — | 🟢 5/2/4 | probable | join🟢 sect🟡 gen🔴 news🟢 dig🟢 jdg🟢 AB🔴 peer🔴 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd🟡 co🟡 set🟢 flw🟢 | -0.67 | +1.25 | +0.28 | — |
| 5 | buy | `AON` | book | `steady_blue` | 🔵 — — | 🟢 4/2/3 | probable | join🟢 sect🟢 gen🔴 news⬛ dig🟡 jdg⬛ AB🟢 peer🔴 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd🟢 co🟢 set🟢 flw🟡 | -8.67 | -8.15 | -6.86 | — |
| 6 | buy | `LIN` | book | `blue` | 🔵 — — | 🟢 7/2/2 | probable | join🟢 sect🔴 gen🔴 news🟢 dig🟢 jdg🟢 AB🟢 peer🟢 heat🟢 vol🟡 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🔴 chd🟢 co🟡 set🟢 flw🟢 | -0.07 | -0.60 | -0.46 | — |
| 7 | buy | `AMZN` | book | `blue` | 🔵 — — | 🟢 4/3/3 | probable | join🟡 sect🔴 gen🔴 news🟢 dig🟢 jdg⬛ AB🟢 peer🔴 heat🟢 vol🟡 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🔴 chd🟢 co🟡 set🟢 flw🟡 | -2.83 | -3.83 | -3.80 | — |
| 8 | buy | `MPC` | book | `blue` | 🔵 — — | 🟢 7/3/1 | probable | join🟢 sect🟡 gen🔴 news🟢 dig🟢 jdg🟢 AB🟢 peer🟢 heat🟢 vol🟡 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | +0.91 | +2.24 | +3.31 | — |
| 9 | buy | `CM` | book | `blue` | 🔵 — — | 🟢 8/1/1 | probable | join🟢 sect🟢 gen🔴 news🟢 dig🟢 jdg⬛ AB🟢 peer🟢 heat🟢 vol🟢 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟡 | -1.38 | -1.67 | +1.16 | — |
| 10 | buy | `AMGN` | book | `blue` | 🔵 — — | 🔴 3/2/6 | probable | join🟢 sect🔴 gen🔴 news🔴 dig🔴 jdg🟢 AB🟢 peer🟡 heat🔴 vol🔴 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🔴 chd🔴 co🟢 set🟢 flw🔴 | -0.22 | +1.64 | +2.73 | — |
| 11 | buy | `TENB` | extra | `hot_ab_peer` | — — — | 🟢 5/3/2 | probable | join🔴 sect🟢 gen🔴 news⬛ dig🟢 jdg🟡 AB🟢 peer🟡 heat🟢 vol🟢 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🔴 chd🟢 co🟡 set🟢 flw🟢 | -0.64 | -5.41 | -8.69 | — |
| 12 | buy | `ESI` | extra | `hot_ab_peer` | — — — | 🟢 6/2/2 | blocked | join🟢 sect🔴 gen🔴 news⬛ dig🟢 jdg🟢 AB🟢 peer🟢 heat🟢 vol🟡 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🔴 chd🟢 co🟡 set🟢 flw🟢 | +1.54 | -1.83 | -1.63 | — |

Sold overnight:

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| — | sell | `ALHC` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +0.00 | -1.61 | -0.22 | — |
| — | sell | `ANF` | extra | `hot_ab_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +0.58 | -3.65 | -5.21 | — |
| — | sell | `ATHM` | book | `steady_blue` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +0.00 | -5.12 | -3.20 | — |
| — | sell | `BFAM` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +0.00 | -1.24 | -2.33 | — |
| — | sell | `COUR` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +0.15 | -0.62 | -4.48 | — |
| — | sell | `CRK` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +1.54 | +4.27 | +12.11 | — |
| — | sell | `CRML` | extra | `hot_ab_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +1.26 | -0.70 | -2.81 | — |
| — | sell | `CVI` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +0.00 | +2.63 | +2.66 | — |
| — | sell | `DTM` | extra | `hot_ab_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +0.78 | +1.15 | +0.35 | — |
| — | sell | `DUOL` | book | `steady_blue` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | -0.21 | +6.00 | +8.02 | — |
| — | sell | `ELF` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | -1.01 | +4.23 | +1.38 | — |
| — | sell | `FIGR` | book | `alarm_rebound` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +1.00 | -0.97 | -6.52 | — |
| — | sell | `KD` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | -0.30 | +1.89 | -0.61 | — |
| — | sell | `KT` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +0.00 | +0.26 | +0.20 | — |
| — | sell | `LEU` | book | `none` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +0.68 | -3.52 | -4.25 | — |
| — | sell | `LW` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +0.00 | -2.75 | -4.33 | — |
| — | sell | `MATV` | extra | `hot_ab_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +0.00 | -1.98 | -0.66 | — |
| — | sell | `MNDY` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | -0.58 | -1.51 | -4.17 | — |
| — | sell | `NCNO` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | -0.48 | -1.70 | -3.00 | — |
| — | sell | `PBH` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +0.00 | -1.26 | +0.32 | — |
| — | sell | `TFPM` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +0.00 | -2.73 | -3.97 | — |
| — | sell | `URBN` | extra | `hot_ab_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +0.00 | -0.49 | -2.22 | — |
| — | sell | `WAY` | book | `steady_blue` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +0.00 | -0.23 | -1.83 | — |
| — | sell | `Z` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | -0.06 | -3.65 | -6.20 | — |
| — | sell | `ZG` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | +0.00 | -2.24 | -6.15 | — |

Seats 1d n=12 · p_win=41.7% · p_loss=58.3% · avg_win=+2.10 · avg_loss=-2.07 · mean=-0.33 · clip30=-0.33 · payoff=1.02.

### 2026-09-01 · `steady_blue+blue+ab_and_peer` · n=11 (keep 10 / add 1 / drop 0)

Market: hard_red · tone `bad`

1d BUY walk × mined stacks: keep / drop fade / swap stack-less / add gated extras → core `steady+blue` (52% hit / +9.54 mean on the mine board) → baseline `blue` → scalp `ab AND peer` (high hit, modest mean)

Overlay 1d -1.01 · 2d -0.60 · 3d — · 1w — · W/L 2/9 · stock-book BUY 1d -1.01 · mine-only 1d -0.64 · universe med -1.04.

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| 1 | buy | `DHT` | book | `steady_blue` | 🔵 — — | 🟢 6/1/2 | probable | join🟢 sect🟢 gen🔴 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | -1.05 | +0.85 | — | — |
| 2 | buy | `CNR` | book | `steady_blue` | 🔵 — — | 🟢 5/1/3 | probable | join🟢 sect🟢 gen🔴 news⬛ dig🟢 jdg⬛ AB🟢 peer🔴 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | -1.04 | +0.35 | — | — |
| 3 | buy | `KMI` | book | `steady_blue` | 🔵 — — | 🟢 4/2/3 | probable | join🟢 sect🟢 gen🔴 news⬛ dig🟡 jdg⬛ AB🟢 peer🔴 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | -1.69 | -2.08 | — | — |
| 4 | buy | `FTI` | book | `steady_blue` | 🔵 — — | 🟢 5/1/3 | probable | join🟢 sect🟢 gen🔴 news⬛ dig🟢 jdg⬛ AB🟢 peer🔴 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟡 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | -1.53 | +0.18 | — | — |
| 5 | buy | `LNG` | book | `steady_blue` | 🔵 — — | 🟢 5/1/3 | probable | join🟢 sect🟢 gen🔴 news⬛ dig🟢 jdg⬛ AB🟢 peer🔴 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | -0.12 | +0.47 | — | — |
| 6 | buy | `DK` | book | `steady_blue` | 🔵 — — | 🟢 5/1/3 | probable | join🟢 sect🟢 gen🔴 news⬛ dig🟢 jdg⬛ AB🟢 peer🔴 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟡 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | -2.72 | -5.23 | — | — |
| 7 | buy | `OXY` | book | `steady_blue` | 🔵 — — | 🟢 7/1/2 | probable | join🟢 sect🟢 gen🔴 news🟢 dig🟢 jdg⬛ AB🟢 peer🟢 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd🟡 co🟡 set🟢 flw🟢 | +0.03 | -0.03 | — | — |
| 8 | buy | `INVX` | book | `steady_blue` | 🔵 — — | 🟢 5/1/3 | probable | join🟢 sect🟢 gen🔴 news⬛ dig🟢 jdg⬛ AB🟢 peer🔴 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟡 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | -1.37 | +1.56 | — | — |
| 9 | buy | `DOCS` | book | `blue` | 🔵 — — | 🟢 6/1/3 | probable | join🟢 sect🔴 gen🔴 news⬛ dig🟢 jdg🟢 AB🟢 peer🟢 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🔴 chd🟢 co🟡 set🟢 flw🟢 | -1.61 | -1.72 | — | — |
| 10 | hold | `CRM` | book | `ab_and_peer` | — — — | 🟢 7/1/3 | probable | join🔴 sect🟢 gen🔴 news🟢 dig🟢 jdg🟡 AB🟢 peer🟢 heat🟢 vol🔴 cat⬛ buy🟢 yΔ🔴 | mkt🔴 par🟡 chd🟡 co🟢 set🟢 flw🟡 | +1.02 | +0.56 | — | — |
| 11 | buy | `G` | extra | `steady_blue` | 🔵 — — | 🟢 5/2/3 | probable | join🟢 sect🟢 gen🔴 news⬛ dig🟢 jdg🟡 AB🟢 peer🟢 heat🔴 vol🔴 cat⬛ buy🟡 yΔ🟡 | mkt🔴 par🟡 chd🟡 co🟡 set🟢 flw🟢 | -1.02 | -1.55 | — | — |

Sold overnight:

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| — | sell | `AMGN` | book | `blue` | 🔵 — — | 🔴 3/2/6 | probable | join🟢 sect🔴 gen🔴 news🔴 dig🔴 jdg🟢 AB🟢 peer🟡 heat🔴 vol🔴 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🔴 chd🔴 co🟢 set🟢 flw🔴 | -0.22 | +1.64 | +2.73 | — |
| — | sell | `AMZN` | book | `blue` | 🔵 — — | 🟢 4/3/3 | probable | join🟡 sect🔴 gen🔴 news🟢 dig🟢 jdg⬛ AB🟢 peer🔴 heat🟢 vol🟡 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🔴 chd🟢 co🟡 set🟢 flw🟡 | -2.83 | -3.83 | -3.80 | — |
| — | sell | `AON` | book | `steady_blue` | 🔵 — — | 🟢 4/2/3 | probable | join🟢 sect🟢 gen🔴 news⬛ dig🟡 jdg⬛ AB🟢 peer🔴 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd🟢 co🟢 set🟢 flw🟡 | -8.67 | -8.15 | -6.86 | — |
| — | sell | `BMO` | book | `steady_blue` | 🔵 — — | 🟢 6/2/2 | probable | join🟢 sect🟢 gen🔴 news🟢 dig🟢 jdg⬛ AB🟢 peer🔴 heat🟢 vol🟡 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | +6.36 | -3.10 | -0.50 | — |
| — | sell | `CM` | book | `blue` | 🔵 — — | 🟢 8/1/1 | probable | join🟢 sect🟢 gen🔴 news🟢 dig🟢 jdg⬛ AB🟢 peer🟢 heat🟢 vol🟢 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟡 | -1.38 | -1.67 | +1.16 | — |
| — | sell | `CVE` | book | `steady_blue` | 🔵 — — | 🟢 5/2/4 | probable | join🟢 sect🟡 gen🔴 news🟢 dig🟢 jdg🟢 AB🔴 peer🔴 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd🟡 co🟡 set🟢 flw🟢 | -0.67 | +1.25 | +0.28 | — |
| — | sell | `CVX` | book | `steady_blue` | 🔵 — — | 🟢 6/3/2 | probable | join🟢 sect🟡 gen🔴 news🟢 dig🟢 jdg🟢 AB🟢 peer🔴 heat🟢 vol🟡 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd🟡 co🟡 set🟢 flw🟢 | +1.09 | +2.51 | +2.87 | — |
| — | sell | `ESI` | extra | `hot_ab_peer` | — — — | 🟢 6/2/2 | blocked | join🟢 sect🔴 gen🔴 news⬛ dig🟢 jdg🟢 AB🟢 peer🟢 heat🟢 vol🟡 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🔴 chd🟢 co🟡 set🟢 flw🟢 | +1.54 | -1.83 | -1.63 | — |
| — | sell | `LIN` | book | `blue` | 🔵 — — | 🟢 7/2/2 | probable | join🟢 sect🔴 gen🔴 news🟢 dig🟢 jdg🟢 AB🟢 peer🟢 heat🟢 vol🟡 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🔴 chd🟢 co🟡 set🟢 flw🟢 | -0.07 | -0.60 | -0.46 | — |
| — | sell | `MPC` | book | `blue` | 🔵 — — | 🟢 7/3/1 | probable | join🟢 sect🟡 gen🔴 news🟢 dig🟢 jdg🟢 AB🟢 peer🟢 heat🟢 vol🟡 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | +0.91 | +2.24 | +3.31 | — |
| — | sell | `TENB` | extra | `hot_ab_peer` | — — — | 🟢 5/3/2 | probable | join🔴 sect🟢 gen🔴 news⬛ dig🟢 jdg🟡 AB🟢 peer🟡 heat🟢 vol🟢 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🔴 chd🟢 co🟡 set🟢 flw🟢 | -0.64 | -5.41 | -8.69 | — |

Seats 1d n=11 · p_win=18.2% · p_loss=81.8% · avg_win=+0.53 · avg_loss=-1.35 · mean=-1.01 · clip30=-1.01 · payoff=0.39.

### 2026-09-02 · `hot_ab_peer+blue` · n=13 (keep 10 / add 3 / drop 0)

Market: hard_red · tone `bad`

1d BUY walk × mined stacks: keep / drop fade / swap stack-less / add gated extras → scalp `hot+ab+peer` (70.6% hit, small n) → baseline `blue`

Overlay 1d +0.08 · 2d — · 3d — · 1w — · W/L 7/5 · stock-book BUY 1d +0.41 · mine-only 1d -0.29 · universe med +0.75.

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| 1 | buy | `PBF` | book | `hot_ab_peer` | 🔵 — — | 🟢 5/2/2 | probable | join🟢 sect🟡 gen🔴 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | +0.65 | — | — | — |
| 2 | buy | `BG` | book | `blue` | 🔵 — — | 🟢 6/1/2 | probable | join🟢 sect🟢 gen🔴 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | +1.50 | — | — | — |
| 3 | buy | `CVS` | book | `blue` | 🔵 — — | 🟢 6/2/3 | probable | join🟢 sect🟡 gen🔴 news🟢 dig🟢 jdg🟢 AB🟢 peer🟢 heat🔴 vol🔴 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🟡 chd🟢 co🟡 set🟢 flw🟢 | -0.38 | — | — | — |
| 4 | buy | `ADM` | book | `blue` | 🔵 — — | 🟢 6/1/2 | probable | join🟢 sect🟢 gen🔴 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | +0.98 | — | — | — |
| 5 | buy | `COR` | book | `blue` | 🔵 — — | 🟢 4/3/3 | probable | join🟢 sect🟡 gen🔴 news⬛ dig🟡 jdg🟢 AB🟢 peer🔴 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🟡 chd🟢 co🟡 set🟢 flw🟢 | +1.63 | — | — | — |
| 6 | buy | `CVE` | book | `blue` | 🔵 — — | 🟢 6/2/2 | probable | join🟢 sect🟡 gen🔴 news🟢 dig🟢 jdg⬛ AB🟢 peer🟢 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | -0.97 | — | — | — |
| 7 | buy | `CVX` | book | `blue` | 🔵 — — | 🟢 6/2/2 | probable | join🟢 sect🟡 gen🔴 news🟢 dig🟢 jdg⬛ AB🟢 peer🟢 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | +0.35 | — | — | — |
| 8 | buy | `EOG` | book | `blue` | 🔵 — — | 🟢 5/2/3 | probable | join🟢 sect🟡 gen🔴 news🟢 dig🟢 jdg⬛ AB🟢 peer🔴 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟡 | +0.41 | — | — | — |
| 9 | buy | `CNQ` | book | `blue` | 🔵 — — | 🟢 5/2/3 | probable | join🟢 sect🟡 gen🔴 news🟢 dig🟢 jdg⬛ AB🟢 peer🔴 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | +0.00 | — | — | — |
| 10 | buy | `BZ` | extra | `hot_ab_peer` | 🔵 — — | 🟡 3/3/3 | blocked | join🟢 sect🟡 gen🔴 news⬛ dig🟡 jdg⬛ AB🟢 peer🟢 heat🔴 vol🔴 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🔴 chd🔴 co🟡 set🟢 flw🟢 | -1.43 | — | — | — |
| 11 | buy | `CHEF` | extra | `hot_ab_peer` | 🔵 — — | 🟢 6/1/2 | probable | join🟢 sect🟢 gen🔴 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟡 | mkt🔴 par🟢 chd🟡 co🟡 set🟢 flw🟢 | -1.81 | — | — | — |
| 12 | buy | `EDU` | extra | `hot_ab_peer` | 🔵 — — | 🟢 6/1/2 | probable | join🟢 sect🟢 gen🔴 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd🟡 co🟡 set🟢 flw🟢 | +0.15 | — | — | — |
| 13 | hold | `OXY` | book | `none` | 🔵 — — | 🟢 7/1/2 | probable | join🟢 sect🟡 gen🔴 news🟢 dig🟢 jdg⬛ AB🟢 peer🟢 heat🟢 vol🔴 cat⬛ buy🟢 yΔ🟢 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | -0.07 | — | — | — |

Sold overnight:

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| — | sell | `CNR` | book | `steady_blue` | 🔵 — — | 🟢 5/1/3 | probable | join🟢 sect🟢 gen🔴 news⬛ dig🟢 jdg⬛ AB🟢 peer🔴 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | -1.04 | +0.35 | — | — |
| — | sell | `CRM` | book | `ab_and_peer` | — — — | 🟢 7/1/3 | probable | join🔴 sect🟢 gen🔴 news🟢 dig🟢 jdg🟡 AB🟢 peer🟢 heat🟢 vol🔴 cat⬛ buy🟢 yΔ🔴 | mkt🔴 par🟡 chd🟡 co🟢 set🟢 flw🟡 | +1.02 | +0.56 | — | — |
| — | sell | `DHT` | book | `steady_blue` | 🔵 — — | 🟢 6/1/2 | probable | join🟢 sect🟢 gen🔴 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | -1.05 | +0.85 | — | — |
| — | sell | `DK` | book | `steady_blue` | 🔵 — — | 🟢 5/1/3 | probable | join🟢 sect🟢 gen🔴 news⬛ dig🟢 jdg⬛ AB🟢 peer🔴 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟡 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | -2.72 | -5.23 | — | — |
| — | sell | `DOCS` | book | `blue` | 🔵 — — | 🟢 6/1/3 | probable | join🟢 sect🔴 gen🔴 news⬛ dig🟢 jdg🟢 AB🟢 peer🟢 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🔴 chd🟢 co🟡 set🟢 flw🟢 | -1.61 | -1.72 | — | — |
| — | sell | `FTI` | book | `steady_blue` | 🔵 — — | 🟢 5/1/3 | probable | join🟢 sect🟢 gen🔴 news⬛ dig🟢 jdg⬛ AB🟢 peer🔴 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟡 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | -1.53 | +0.18 | — | — |
| — | sell | `G` | extra | `steady_blue` | 🔵 — — | 🟢 5/2/3 | probable | join🟢 sect🟢 gen🔴 news⬛ dig🟢 jdg🟡 AB🟢 peer🟢 heat🔴 vol🔴 cat⬛ buy🟡 yΔ🟡 | mkt🔴 par🟡 chd🟡 co🟡 set🟢 flw🟢 | -1.02 | -1.55 | — | — |
| — | sell | `INVX` | book | `steady_blue` | 🔵 — — | 🟢 5/1/3 | probable | join🟢 sect🟢 gen🔴 news⬛ dig🟢 jdg⬛ AB🟢 peer🔴 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟡 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | -1.37 | +1.56 | — | — |
| — | sell | `KMI` | book | `steady_blue` | 🔵 — — | 🟢 4/2/3 | probable | join🟢 sect🟢 gen🔴 news⬛ dig🟡 jdg⬛ AB🟢 peer🔴 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | -1.69 | -2.08 | — | — |
| — | sell | `LNG` | book | `steady_blue` | 🔵 — — | 🟢 5/1/3 | probable | join🟢 sect🟢 gen🔴 news⬛ dig🟢 jdg⬛ AB🟢 peer🔴 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | -0.12 | +0.47 | — | — |

Seats 1d n=13 · p_win=53.8% · p_loss=38.5% · avg_win=+0.81 · avg_loss=-0.93 · mean=+0.08 · clip30=+0.08 · payoff=0.87.

### 2026-09-03 · `hot_ab_peer+blue_white+blue` · n=12 (keep 8 / add 4 / drop 0)

Market: yellow · tone `neutral`

1d BUY walk × mined stacks: keep / drop fade / swap stack-less / add gated extras → scalp `hot+ab+peer` (70.6% hit, small n) → swing `blue+white` (white only with blue) → baseline `blue`

Overlay 1d — · 2d — · 3d — · 1w — · W/L 0/0 · stock-book BUY 1d — · mine-only 1d — · universe med —.

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| 1 | buy | `VAL` | book | `hot_ab_peer` | 🔵 — ⚪ | 🟡 3/4/2 | group leader | join🟡 sect🔴 gen🟡 news⬛ dig🟢 jdg⬛ AB🟢 peer🔴 heat🟢 vol🟡 cat⬛ buy🟡 yΔ🟢 | mkt🟡 par🟢 chd🟢 co🟡 set🟢 flw🟢 | — | — | — | — |
| 2 | buy | `AVGO` | book | `hot_ab_peer` | 🔵 — ⚪ | 🟢 7/3/1 | catalyst | join🔴 sect🟢 gen🟡 news🟢 dig🟢 jdg🟢 AB🟢 peer🟢 heat🟢 vol🟡 cat⬛ buy🟡 yΔ🔴 | mkt🟡 par🟡 chd🟢 co🟢 set🟡 flw🟢 | — | — | — | — |
| 3 | buy | `WAY` | book | `blue_white` | 🔵 — ⚪ | 🟢 6/4/0 | group leader | join🟢 sect🟡 gen🟡 news⬛ dig🟢 jdg🟢 AB🟢 peer🟢 heat🟢 vol🟡 cat⬛ buy🟡 yΔ🔴 | mkt🟡 par🟡 chd🟢 co🟡 set🟢 flw🟢 | — | — | — | — |
| 4 | buy | `VFF` | book | `blue_white` | 🔵 — ⚪ | 🟢 5/3/1 | group leader | join🟢 sect🟢 gen🟡 news⬛ dig🟡 jdg⬛ AB🟢 peer🟢 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🔴 | mkt🟡 par🟢 chd🟢 co🟡 set🟢 flw🟢 | — | — | — | — |
| 5 | buy | `CEG` | book | `blue_white` | 🔵 — ⚪ | 🟢 6/4/0 | group leader | join🟢 sect🟡 gen🟡 news🟢 dig🟢 jdg⬛ AB🟢 peer🟢 heat🟢 vol🟡 cat⬛ buy🟡 yΔ🟢 | mkt🟡 par🟡 chd🟢 co🟡 set🟢 flw🟢 | — | — | — | — |
| 6 | buy | `VEEV` | book | `blue_white` | 🔵 — ⚪ | 🟢 6/3/1 | group leader | join🟢 sect🟡 gen🟡 news⬛ dig🟢 jdg🟢 AB🟢 peer🟢 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🔴 | mkt🟡 par🟡 chd🟢 co🟡 set🟢 flw🟢 | — | — | — | — |
| 7 | hold | `ADM` | book | `blue_white` | 🔵 — ⚪ | 🟢 5/4/1 | group leader | join🟢 sect🟢 gen🟡 news⬛ dig🟡 jdg⬛ AB🟢 peer🟢 heat🔴 vol🟡 cat🟡 buy🟢 yΔ🟢 | mkt🟡 par🟢 chd🟢 co🟡 set🟢 flw🟢 | — | — | — | — |
| 8 | buy | `HPE` | book | `blue` | 🔵 — ⚪ | 🟢 6/4/1 | catalyst | join🟢 sect🟢 gen🟡 news🟢 dig🟢 jdg🟡 AB🟢 peer🔴 heat🟢 vol🟡 cat⬛ buy🟡 yΔ🔴 | mkt🟡 par🟡 chd🟡 co🟢 set🟢 flw🟢 | — | — | — | — |
| 9 | buy | `CABA` | extra | `hot_ab_peer` | 🔵 — ⚪ | 🟢 6/4/0 | standard | join🟢 sect🟡 gen🟡 news⬛ dig🟢 jdg🟢 AB🟢 peer🟢 heat🟢 vol🟡 cat⬛ buy🟡 yΔ🟢 | mkt🟡 par🟡 chd🟡 co🟡 set🟢 flw🟢 | — | — | — | — |
| 10 | buy | `MGNI` | extra | `hot_ab_peer` | 🔵 — ⚪ | 🟡 3/4/2 | standard | join🟢 sect🟡 gen🟡 news⬛ dig🟡 jdg⬛ AB🟢 peer🔴 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🔴 | mkt🟡 par🟡 chd🟢 co🟡 set🟢 flw🟢 | — | — | — | — |
| 11 | buy | `VSTM` | extra | `hot_ab_peer` | 🔵 — ⚪ | 🟢 7/3/0 | standard | join🟢 sect🟡 gen🟡 news⬛ dig🟢 jdg🟢 AB🟢 peer🟢 heat🟢 vol🟡 cat⬛ buy🟢 yΔ🟢 | mkt🟡 par🟡 chd🟡 co🟡 set🟢 flw🟢 | — | — | — | — |
| 12 | buy | `TDS` | extra | `hot_ab_peer` | 🔵 — ⚪ | 🟡 3/5/1 | standard | join🟢 sect🟡 gen🟡 news⬛ dig🟡 jdg⬛ AB🟢 peer🔴 heat🟢 vol🟡 cat⬛ buy🟡 yΔ🔴 | mkt🟡 par🟡 chd🟢 co🟡 set🟢 flw🟢 | — | — | — | — |

Sold overnight:

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| — | sell | `BG` | book | `blue` | 🔵 — — | 🟢 6/1/2 | probable | join🟢 sect🟢 gen🔴 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | +1.50 | — | — | — |
| — | sell | `BZ` | extra | `hot_ab_peer` | 🔵 — — | 🟡 3/3/3 | blocked | join🟢 sect🟡 gen🔴 news⬛ dig🟡 jdg⬛ AB🟢 peer🟢 heat🔴 vol🔴 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🔴 chd🔴 co🟡 set🟢 flw🟢 | -1.43 | — | — | — |
| — | sell | `CHEF` | extra | `hot_ab_peer` | 🔵 — — | 🟢 6/1/2 | probable | join🟢 sect🟢 gen🔴 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟡 | mkt🔴 par🟢 chd🟡 co🟡 set🟢 flw🟢 | -1.81 | — | — | — |
| — | sell | `CNQ` | book | `blue` | 🔵 — — | 🟢 5/2/3 | probable | join🟢 sect🟡 gen🔴 news🟢 dig🟢 jdg⬛ AB🟢 peer🔴 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | +0.00 | — | — | — |
| — | sell | `COR` | book | `blue` | 🔵 — — | 🟢 4/3/3 | probable | join🟢 sect🟡 gen🔴 news⬛ dig🟡 jdg🟢 AB🟢 peer🔴 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🟡 chd🟢 co🟡 set🟢 flw🟢 | +1.63 | — | — | — |
| — | sell | `CVE` | book | `blue` | 🔵 — — | 🟢 6/2/2 | probable | join🟢 sect🟡 gen🔴 news🟢 dig🟢 jdg⬛ AB🟢 peer🟢 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | -0.97 | — | — | — |
| — | sell | `CVS` | book | `blue` | 🔵 — — | 🟢 6/2/3 | probable | join🟢 sect🟡 gen🔴 news🟢 dig🟢 jdg🟢 AB🟢 peer🟢 heat🔴 vol🔴 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🟡 chd🟢 co🟡 set🟢 flw🟢 | -0.38 | — | — | — |
| — | sell | `CVX` | book | `blue` | 🔵 — — | 🟢 6/2/2 | probable | join🟢 sect🟡 gen🔴 news🟢 dig🟢 jdg⬛ AB🟢 peer🟢 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | +0.35 | — | — | — |
| — | sell | `EDU` | extra | `hot_ab_peer` | 🔵 — — | 🟢 6/1/2 | probable | join🟢 sect🟢 gen🔴 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd🟡 co🟡 set🟢 flw🟢 | +0.15 | — | — | — |
| — | sell | `EOG` | book | `blue` | 🔵 — — | 🟢 5/2/3 | probable | join🟢 sect🟡 gen🔴 news🟢 dig🟢 jdg⬛ AB🟢 peer🔴 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟡 | +0.41 | — | — | — |
| — | sell | `OXY` | book | `none` | 🔵 — — | 🟢 7/1/2 | probable | join🟢 sect🟡 gen🔴 news🟢 dig🟢 jdg⬛ AB🟢 peer🟢 heat🟢 vol🔴 cat⬛ buy🟢 yΔ🟢 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | -0.07 | — | — | — |
| — | sell | `PBF` | book | `hot_ab_peer` | 🔵 — — | 🟢 5/2/2 | probable | join🟢 sect🟡 gen🔴 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | +0.65 | — | — | — |

1d not settled — names only.

## Notes

1. The point is the combination: book quality gates + mined stacks + as-of cameras. Mine-only lost to stock-book BUY on the priced window. Overlay should track the book and beat it when a stack adds or a fade drops.
2. `blue` board mean +4.46 is squeeze-contaminated. Book lines clip ±30.
3. HARD_RED / thin BUY mornings stay thin. We do not backfill 25 lottery names.
4. A cameras (`ab` / `peer`) only print from **2026-08-20**. Before that, extras cannot fire `hot+ab+peer` / `ab+peer`.
5. 1d after 8/20 comes from the next Finviz tape we already parse every morning. Same close-to-close idea as the panel; not a parquet rebuild.

