# Boring winners — book × mine overlay

Starts from the **same 1d BUY list** the Top Gainer As-Of walk uses, then overlays the fixed FEATURE_MINE stacks. Equal-weight, close-to-close, clip ±30 on the book line. Per-name 1d/2d/3d/1w are raw.

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
Thin 1d BUY mornings stay thin — we do not force 25 junk seats. A cameras print from **2026-08-20**. Settled 1d through **2026-08-20**.

Per-day files: `03_scoreboard/boring_winners/<date>.md` · today also at `01_daily/<date>_boring_winners.md` and `latest_boring_winners.md`.

## Daily book returns

| date | stacks | n | keep | add | drop | overlay 1d | book BUY 1d | mine-only 1d | uni med | 2d | W | L |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `rsi_oversold+gap_down` | 25 | 25 | 0 | 0 | +2.00 | +2.00 | +2.26 | +0.00 | +3.47 | 9 | 8 |
| 2026-08-14 | `steady_blue+blue_white` | 25 | 25 | 0 | 0 | +0.52 | +0.52 | +1.08 | -0.24 | -0.86 | 10 | 7 |
| 2026-08-17 | `steady_blue+blue_white` | 23 | 23 | 0 | 2 | -1.56 | -1.59 | -2.15 | -0.60 | +0.48 | 10 | 13 |
| 2026-08-18 | `alarm_rebound` | 22 | 17 | 5 | 8 | +2.32 | +2.07 | -0.74 | +0.88 | +1.68 | 20 | 2 |
| 2026-08-19 | `steady_blue+blue+alarm_rebound` | 25 | 24 | 1 | 1 | -0.64 | -0.78 | -0.99 | -0.98 | -0.42 | 6 | 18 |
| 2026-08-20 | `steady_blue+blue_white+blue+hot_ab_peer` | 17 | 15 | 2 | 0 | +0.82 | +1.10 | +1.21 | +0.72 | — | 8 | 8 |
| 2026-08-21 | `hot_ab_peer+steady_blue+blue_white+blue+ab_and_peer` | 24 | 22 | 2 | 3 | — | — | — | — | — | 0 | 0 |
| 2026-08-27 | `steady_blue+blue_white+ab_and_peer+hot_ab_peer` | 19 | 16 | 3 | 9 | — | — | — | — | — | 0 | 0 |
| 2026-08-30 | `steady_blue+ab_and_peer+alarm_rebound+hot_ab_peer` | 25 | 20 | 5 | 5 | — | — | — | — | — | 0 | 0 |
| 2026-08-31 | `hot_ab_peer+steady_blue+blue` | 12 | 10 | 2 | 0 | — | — | — | — | — | 0 | 0 |
| 2026-09-01 | `steady_blue+blue+ab_and_peer` | 11 | 10 | 1 | 0 | — | — | — | — | — | 0 | 0 |

Overlay 1d: 6 priced days · p(loss day)=33.3% · mean=+0.58 · cum=+3.46.
Stock-book BUY 1d (same panel): 6 priced days · p(loss day)=33.3% · mean=+0.55 · cum=+3.33.
Mine-only 25 1d (comparison): 6 priced days · p(loss day)=50.0% · mean=+0.11 · cum=+0.67.
Overlay names 1d: n=120 · p_win=52.5% · p_loss=46.7% · avg_win=+3.00 · avg_loss=-2.39 · mean=+0.46 · clip30=+0.46 · payoff=1.25.
Overlay names 2d: n=104 · p_win=56.7% · p_loss=43.3% · avg_win=+3.99 · avg_loss=-3.41 · mean=+0.79 · clip30=+0.79 · payoff=1.17.

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
| 2026-08-31 | 8 | — | — | 8 | 0 |
| 2026-09-01 | 7 | — | — | 7 | 8 |

## Each day's stocks

`keep` = on 1d BUY. `add` = gated extra. `buy` / `hold` this morning. `sell` = dropped overnight. Cameras and coaches are the Top Gainer As-Of 09:30 ET paint.

### 2026-08-13 · `rsi_oversold+gap_down` · n=25 (keep 25 / add 0 / drop 0)

Market: — · tone `good`

1d BUY walk × mined stacks: keep / drop fade / swap stack-less / add gated extras → lottery `rsi=oversold` (low hit, huge mean) → lottery `gap=down`

Overlay 1d +2.00 · 2d +3.47 · 3d +4.66 · 1w +5.07 · W/L 9/8 · stock-book BUY 1d +2.00 · mine-only 1d +2.26 · universe med +0.00.

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| 1 | buy | `MBRX` | book | `rsi_oversold` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | +7.18 | +6.30 | +6.83 | +3.50 |
| 2 | buy | `ABEO` | book | `gap_down` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | -5.67 | -6.46 | -2.99 | -6.14 |
| 3 | buy | `TKVA` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | — | — | — | — |
| 4 | buy | `APNAU` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | — | — | — | — |
| 5 | buy | `BCACU` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | — | — | — | — |
| 6 | buy | `IDIAU` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | — | — | — | — |
| 7 | buy | `LEDRU` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | — | — | — | — |
| 8 | buy | `PHAXU` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | — | — | — | — |
| 9 | buy | `SCATU` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | — | — | — | — |
| 10 | buy | `SKAIU` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | — | — | — | — |
| 11 | buy | `TNDM` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | -1.77 | -3.78 | +2.59 | -2.42 |
| 12 | buy | `ACHV` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | +4.69 | +3.45 | +3.72 | +10.62 |
| 13 | buy | `AGEN` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | +4.57 | +2.14 | +7.57 | +7.00 |
| 14 | buy | `VOR` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | -1.12 | -1.20 | -0.04 | -0.97 |
| 15 | buy | `SGRY` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | -1.05 | -5.46 | -6.25 | -8.23 |
| 16 | buy | `WW` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | -2.70 | -6.02 | -3.88 | +3.81 |
| 17 | buy | `FDMT` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | +4.63 | +24.85 | +26.35 | +38.20 |
| 18 | buy | `FTRE` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | -2.30 | +1.57 | -1.24 | +2.08 |
| 19 | buy | `IMNN` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | +4.43 | +1.27 | +0.00 | +0.00 |
| 20 | buy | `NRXP` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | -2.10 | +2.69 | +2.99 | +5.09 |
| 21 | buy | `PROK` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | -5.34 | -0.36 | -1.78 | +1.07 |
| 22 | buy | `SPRB` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | +5.77 | +6.33 | +6.18 | +4.45 |
| 23 | buy | `TGTX` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | +1.67 | +2.79 | +4.84 | +5.46 |
| 24 | buy | `UNCY` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | +2.04 | +3.15 | +4.26 | +0.93 |
| 25 | buy | `INO` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | +21.11 | +27.78 | +33.33 | +34.44 |

Seats 1d n=17 · p_win=52.9% · p_loss=47.1% · avg_win=+6.23 · avg_loss=-2.76 · mean=+2.00 · clip30=+2.00 · payoff=2.26.

### 2026-08-14 · `steady_blue+blue_white` · n=25 (keep 25 / add 0 / drop 0)

Market: — · tone `good`

1d BUY walk × mined stacks: keep / drop fade / swap stack-less / add gated extras → core `steady+blue` (52% hit / +9.54 mean on the mine board) → swing `blue+white` (white only with blue)

Overlay 1d +0.52 · 2d -0.86 · 3d -0.45 · 1w -0.89 · W/L 10/7 · stock-book BUY 1d +0.52 · mine-only 1d +1.08 · universe med -0.24.

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
| 23 | buy | `PGY` | book | `none` | — — — | 🟢 4/1/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | -2.92 | -5.75 | -5.06 | +0.56 |
| 24 | buy | `TBCH` | book | `none` | — — — | 🟢 4/1/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | -4.00 | -2.75 | -3.22 | -0.71 |
| 25 | buy | `WOLF` | book | `none` | — — ⚪ | 🟢 4/2/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🟡 | +9.97 | -1.04 | -8.49 | -18.97 |

Sold overnight:

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| — | sell | `ABEO` | book | `gap_down` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | -5.67 | -6.46 | -2.99 | -6.14 |
| — | sell | `ACHV` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | +4.69 | +3.45 | +3.72 | +10.62 |
| — | sell | `AGEN` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | +4.57 | +2.14 | +7.57 | +7.00 |
| — | sell | `FDMT` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | +4.63 | +24.85 | +26.35 | +38.20 |
| — | sell | `FTRE` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | -2.30 | +1.57 | -1.24 | +2.08 |
| — | sell | `IMNN` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | +4.43 | +1.27 | +0.00 | +0.00 |
| — | sell | `INO` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | +21.11 | +27.78 | +33.33 | +34.44 |
| — | sell | `MBRX` | book | `rsi_oversold` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | +7.18 | +6.30 | +6.83 | +3.50 |
| — | sell | `NRXP` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | -2.10 | +2.69 | +2.99 | +5.09 |
| — | sell | `PROK` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | -5.34 | -0.36 | -1.78 | +1.07 |
| — | sell | `SGRY` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | -1.05 | -5.46 | -6.25 | -8.23 |
| — | sell | `SPRB` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | +5.77 | +6.33 | +6.18 | +4.45 |
| — | sell | `TGTX` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | +1.67 | +2.79 | +4.84 | +5.46 |
| — | sell | `TKVA` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | — | — | — | — |
| — | sell | `TNDM` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | -1.77 | -3.78 | +2.59 | -2.42 |
| — | sell | `UNCY` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | +2.04 | +3.15 | +4.26 | +0.93 |
| — | sell | `VOR` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | -1.12 | -1.20 | -0.04 | -0.97 |
| — | sell | `WW` | book | `none` | 🔵 — ⚪ | 🟢 3/0/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw⬛ | -2.70 | -6.02 | -3.88 | +3.81 |

Seats 1d n=17 · p_win=58.8% · p_loss=41.2% · avg_win=+2.40 · avg_loss=-2.16 · mean=+0.52 · clip30=+0.52 · payoff=1.11.

### 2026-08-17 · `steady_blue+blue_white` · n=23 (keep 23 / add 0 / drop 2)

Market: — · tone `good`

1d BUY walk × mined stacks: keep / drop fade / swap stack-less / add gated extras → core `steady+blue` (52% hit / +9.54 mean on the mine board) → swing `blue+white` (white only with blue)

Overlay 1d -1.56 · 2d +0.48 · 3d +1.24 · 1w — · W/L 10/13 · stock-book BUY 1d -1.59 · mine-only 1d -2.15 · universe med -0.60.

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| 1 | buy | `TMC` | book | `steady_blue` | 🔵 — ⚪ | 🟢 4/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🟢 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🟢 | +3.98 | +5.30 | +5.30 | — |
| 2 | buy | `TGB` | book | `steady_blue` | 🔵 — ⚪ | 🟢 3/2/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🟡 | -4.67 | -3.42 | -0.91 | — |
| 3 | buy | `ERO` | book | `steady_blue` | — — — | 🟢 3/1/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🔴 | -5.56 | -2.38 | +3.18 | — |
| 4 | buy | `MOS` | book | `steady_blue` | 🔵 — — | 🟢 3/1/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🔴 | -0.42 | +4.71 | +9.99 | — |
| 5 | buy | `INTC` | book | `steady_blue` | — 🚨 ⚪ | 🟢 4/3/0 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🟡 | -6.57 | -10.33 | -10.98 | — |
| 6 | buy | `AMD` | book | `steady_blue` | — 🚨 ⚪ | 🟢 4/3/0 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🟡 | -4.27 | -7.82 | -7.22 | — |
| 7 | buy | `NVDA` | book | `steady_blue` | — 🚨 — | 🟢 4/2/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | -2.34 | -3.31 | -3.63 | — |
| 8 | buy | `MU` | book | `blue_white` | — 🚨 ⚪ | 🟢 4/3/0 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🟡 | -7.02 | -7.38 | -3.70 | — |
| 9 | hold | `DVN` | book | `none` | — — — | 🟢 5/0/1 | ⬜ | join🟢 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +0.55 | +1.30 | +3.64 | — |
| 10 | hold | `EOG` | book | `none` | — — — | 🟢 5/0/1 | ⬜ | join🟢 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +1.75 | +2.28 | +4.13 | — |
| 11 | hold | `FANG` | book | `none` | — — — | 🟢 5/0/1 | ⬜ | join🟢 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +1.81 | +1.10 | +2.29 | — |
| 12 | hold | `OXY` | book | `none` | — — — | 🟢 4/1/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +1.29 | +1.78 | +4.20 | — |
| 13 | hold | `APA` | book | `none` | — — — | 🟢 4/1/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +1.90 | +4.50 | +6.76 | — |
| 14 | hold | `COP` | book | `none` | — — — | 🟢 4/1/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +1.69 | +2.37 | +5.75 | — |
| 15 | hold | `XOM` | book | `none` | — — — | 🟢 4/1/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +2.54 | +2.05 | +2.90 | — |
| 16 | hold | `CVX` | book | `none` | 🔵 — ⚪ | 🟢 4/2/0 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🟡 | +1.50 | +2.40 | +2.40 | — |
| 17 | hold | `TLN` | book | `none` | — 🚨 — | 🟢 4/1/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | -11.00 | -9.69 | -11.19 | — |
| 18 | hold | `CEG` | book | `none` | — 🚨 — | 🟢 4/1/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | -3.94 | -1.30 | -1.75 | — |
| 19 | hold | `NRG` | book | `none` | — 🚨 — | 🟢 4/1/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | -5.57 | -1.46 | -5.73 | — |
| 20 | buy | `ELF` | book | `none` | — — — | 🟢 3/1/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🔴 | -1.23 | +6.39 | +5.12 | — |
| 21 | buy | `DNN` | book | `none` | — — ⚪ | 🟢 3/2/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🟡 | -1.25 | +0.94 | -1.57 | — |
| 22 | buy | `HNST` | book | `none` | — 🚨 ⚪ | 🟡 2/3/0 | ⬜ | join🟡 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🟡 | +1.06 | +6.81 | +5.53 | — |
| 23 | buy | `EL` | book | `none` | — — — | 🟢 3/1/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🔴 | -0.08 | +16.21 | +14.00 | — |

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
| — | sell | `PGY` | book | `none` | — — — | 🟢 4/1/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | -2.92 | -5.75 | -5.06 | +0.56 |
| — | sell | `PHAXU` | book | `none` | — — ⚪ | 🟢 4/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟢 yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🟡 | — | — | — | — |
| — | sell | `SCATU` | book | `none` | — — ⚪ | 🟢 4/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟢 yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🟡 | — | — | — | — |
| — | sell | `SKAIU` | book | `none` | — — ⚪ | 🟢 4/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟢 yΔ⬛ | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🟡 | — | — | — | — |
| — | sell | `TBCH` | book | `none` | — — — | 🟢 4/1/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | -4.00 | -2.75 | -3.22 | -0.71 |
| — | sell | `VST` | book | `steady_blue` | 🔵 — — | 🟢 4/1/1 | ⬜ | join🟢 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | -1.36 | -5.14 | -3.67 | -8.05 |
| — | sell | `WOLF` | book | `none` | — — ⚪ | 🟢 4/2/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🟡 | +9.97 | -1.04 | -8.49 | -18.97 |

Seats 1d n=23 · p_win=43.5% · p_loss=56.5% · avg_win=+1.81 · avg_loss=-4.15 · mean=-1.56 · clip30=-1.56 · payoff=0.43.

### 2026-08-18 · `alarm_rebound` · n=22 (keep 17 / add 5 / drop 8)

Market: — · tone `bad`

1d BUY walk × mined stacks: keep / drop fade / swap stack-less / add gated extras → rebound `alarm AND NOT white`

Overlay 1d +2.32 · 2d +1.68 · 3d +1.92 · 1w — · W/L 20/2 · stock-book BUY 1d +2.07 · mine-only 1d -0.74 · universe med +0.88.

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| 1 | buy | `MUR` | book | `alarm_rebound` | — — — | 🟡 2/2/1 | ⬜ | join🟡 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🟡 cat⬜ buy🟢 yΔ🔴 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🟡 | +4.80 | +8.81 | +6.34 | — |
| 2 | buy | `ABEV` | extra | `alarm_rebound` | — — — | 🟡 2/2/1 | ⬜ | join🟢 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🟡 | +1.08 | +1.80 | +3.60 | — |
| 3 | buy | `DHT` | extra | `alarm_rebound` | — 🚨 — | 🟡 1/3/1 | ⬜ | join🟡 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🟡 | +3.52 | +2.54 | +2.64 | — |
| 4 | buy | `ENB` | extra | `alarm_rebound` | — 🚨 — | 🟡 1/2/2 | ⬜ | join🟡 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🔴 | -2.22 | -0.78 | -1.81 | — |
| 5 | buy | `CIG` | extra | `alarm_rebound` | — — — | 🟡 2/2/1 | ⬜ | join🟡 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🟢 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🟢 | +1.58 | -1.05 | +1.05 | — |
| 6 | buy | `SFD` | extra | `alarm_rebound` | — — — | 🟡 1/3/1 | ⬜ | join🟡 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🟡 | +0.09 | -1.11 | -1.24 | — |
| 7 | buy | `STE` | book | `none` | — — — | 🟡 2/1/2 | ⬜ | join🟢 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🔴 | +1.95 | +1.44 | +2.38 | — |
| 8 | buy | `DHR` | book | `none` | — — — | 🟡 2/1/2 | ⬜ | join🟢 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🔴 | +5.97 | +8.18 | +9.65 | — |
| 9 | buy | `SYK` | book | `none` | — — — | 🟡 2/1/2 | ⬜ | join🟢 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🔴 | +2.60 | -1.11 | -0.58 | — |
| 10 | buy | `TMO` | book | `none` | — — — | 🟡 2/1/2 | ⬜ | join🟢 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🔴 | +4.16 | +6.54 | +6.83 | — |
| 11 | buy | `EW` | book | `none` | — — — | 🟡 2/1/2 | ⬜ | join🟢 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🔴 | +1.43 | -1.26 | -1.31 | — |
| 12 | buy | `UTHR` | book | `none` | — — — | 🟡 2/1/2 | ⬜ | join🟢 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🔴 | +2.10 | +0.53 | -0.99 | — |
| 13 | buy | `COO` | book | `none` | — — — | 🟡 2/1/2 | ⬜ | join🟢 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🔴 | +1.28 | +0.53 | +0.61 | — |
| 14 | buy | `FAST` | book | `none` | — — — | 🟡 2/1/2 | ⬜ | join🟢 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🔴 | +0.33 | -1.19 | -0.02 | — |
| 15 | buy | `MDT` | book | `none` | — — — | 🟡 2/1/2 | ⬜ | join🟢 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🔴 | +2.21 | +0.23 | +1.37 | — |
| 16 | buy | `IQV` | book | `none` | — — — | 🟡 1/3/1 | ⬜ | join🟡 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🟡 | +3.85 | +6.67 | +8.24 | — |
| 17 | buy | `RMD` | book | `none` | — — — | 🟡 2/1/2 | ⬜ | join🟢 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🔴 | +4.85 | +4.14 | +5.55 | — |
| 18 | buy | `ES` | book | `none` | — — — | 🟡 2/1/2 | ⬜ | join🟢 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🔴 | -0.32 | -0.10 | -2.78 | — |
| 19 | buy | `AOS` | book | `none` | — — — | 🟡 2/1/2 | ⬜ | join🟢 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🔴 | +4.83 | +2.58 | +3.65 | — |
| 20 | buy | `GGG` | book | `none` | — — — | 🟡 2/1/2 | ⬜ | join🟢 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🔴 | +2.07 | +0.86 | -0.46 | — |
| 21 | buy | `MLYS` | book | `none` | — — — | 🟡 1/2/2 | ⬜ | join🟡 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🔴 | +1.28 | -3.23 | -3.61 | — |
| 22 | buy | `PFE` | book | `none` | — — — | 🟡 2/2/1 | ⬜ | join🟢 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🟡 | +3.63 | +1.98 | +3.01 | — |

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
| — | sell | `AMD` | book | `steady_blue` | — 🚨 ⚪ | 🟢 4/3/0 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🟡 | -4.27 | -7.82 | -7.22 | — |
| — | sell | `APA` | book | `none` | — — — | 🟢 4/1/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +1.90 | +4.50 | +6.76 | — |
| — | sell | `CEG` | book | `none` | — 🚨 — | 🟢 4/1/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | -3.94 | -1.30 | -1.75 | — |
| — | sell | `COP` | book | `none` | — — — | 🟢 4/1/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +1.69 | +2.37 | +5.75 | — |
| — | sell | `CVX` | book | `none` | 🔵 — ⚪ | 🟢 4/2/0 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🟡 | +1.50 | +2.40 | +2.40 | — |
| — | sell | `DNN` | book | `none` | — — ⚪ | 🟢 3/2/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🟡 | -1.25 | +0.94 | -1.57 | — |
| — | sell | `DVN` | book | `none` | — — — | 🟢 5/0/1 | ⬜ | join🟢 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +0.55 | +1.30 | +3.64 | — |
| — | sell | `EL` | book | `none` | — — — | 🟢 3/1/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🔴 | -0.08 | +16.21 | +14.00 | — |
| — | sell | `ELF` | book | `none` | — — — | 🟢 3/1/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🔴 | -1.23 | +6.39 | +5.12 | — |
| — | sell | `EOG` | book | `none` | — — — | 🟢 5/0/1 | ⬜ | join🟢 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +1.75 | +2.28 | +4.13 | — |
| — | sell | `ERO` | book | `steady_blue` | — — — | 🟢 3/1/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🔴 | -5.56 | -2.38 | +3.18 | — |
| — | sell | `FANG` | book | `none` | — — — | 🟢 5/0/1 | ⬜ | join🟢 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +1.81 | +1.10 | +2.29 | — |
| — | sell | `HNST` | book | `none` | — 🚨 ⚪ | 🟡 2/3/0 | ⬜ | join🟡 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🟡 | +1.06 | +6.81 | +5.53 | — |
| — | sell | `INTC` | book | `steady_blue` | — 🚨 ⚪ | 🟢 4/3/0 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🟡 | -6.57 | -10.33 | -10.98 | — |
| — | sell | `MOS` | book | `steady_blue` | 🔵 — — | 🟢 3/1/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🔴 | -0.42 | +4.71 | +9.99 | — |
| — | sell | `MU` | book | `blue_white` | — 🚨 ⚪ | 🟢 4/3/0 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🟡 | -7.02 | -7.38 | -3.70 | — |
| — | sell | `NRG` | book | `none` | — 🚨 — | 🟢 4/1/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | -5.57 | -1.46 | -5.73 | — |
| — | sell | `NVDA` | book | `steady_blue` | — 🚨 — | 🟢 4/2/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg🟢 AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | -2.34 | -3.31 | -3.63 | — |
| — | sell | `OXY` | book | `none` | — — — | 🟢 4/1/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +1.29 | +1.78 | +4.20 | — |
| — | sell | `TGB` | book | `steady_blue` | 🔵 — ⚪ | 🟢 3/2/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🟡 | -4.67 | -3.42 | -0.91 | — |
| — | sell | `TLN` | book | `none` | — 🚨 — | 🟢 4/1/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | -11.00 | -9.69 | -11.19 | — |
| — | sell | `TMC` | book | `steady_blue` | 🔵 — ⚪ | 🟢 4/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🟢 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co⬛ set⬜ flw🟢 | +3.98 | +5.30 | +5.30 | — |
| — | sell | `XOM` | book | `none` | — — — | 🟢 4/1/1 | ⬜ | join🟡 sect🟢 gen🟢 news🟢 dig⬜ jdg⬜ AB⬜ peer⬜ heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set⬜ flw🔴 | +2.54 | +2.05 | +2.90 | — |

Seats 1d n=22 · p_win=90.9% · p_loss=9.1% · avg_win=+2.68 · avg_loss=-1.27 · mean=+2.32 · clip30=+2.32 · payoff=2.11.

### 2026-08-19 · `steady_blue+blue+alarm_rebound` · n=25 (keep 24 / add 1 / drop 1)

Market: — · tone `bad`

1d BUY walk × mined stacks: keep / drop fade / swap stack-less / add gated extras → core `steady+blue` (52% hit / +9.54 mean on the mine board) → baseline `blue` → rebound `alarm AND NOT white`

Overlay 1d -0.64 · 2d -0.42 · 3d — · 1w — · W/L 6/18 · stock-book BUY 1d -0.78 · mine-only 1d -0.99 · universe med -0.98.

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| 1 | buy | `NEE` | book | `steady_blue` | 🔵 🚨 — | 🟡 2/4/1 | ⬜ | join🟢 sect🟡 gen🔴 news🟢 dig⬜ jdg⬛ AB⬛ peer🟡 heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🔴 par🟡 chd⬜ co🟢 set⬛ flw🟡 | -1.02 | -2.63 | — | — |
| 2 | buy | `NRG` | book | `blue` | 🔵 🚨 — | 🔴 1/2/4 | ⬜ | join🔴 sect🟡 gen🔴 news🟢 dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟡 chd⬜ co🟢 set⬛ flw🔴 | -4.33 | -6.20 | — | — |
| 3 | hold | `MUR` | book | `alarm_rebound` | — 🚨 — | 🔴 1/2/3 | ⬜ | join🟡 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | +3.82 | +1.47 | — | — |
| 4 | hold | `IQV` | book | `alarm_rebound` | — 🚨 — | 🔴 1/2/3 | ⬜ | join🟡 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | +2.72 | +4.23 | — | — |
| 5 | buy | `SKM` | extra | `blue` | 🔵 — — | 🟡 2/3/1 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🟢 heat⬜ vol🟡 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🟡 | -1.11 | +4.00 | — | — |
| 6 | hold | `STE` | book | `none` | — 🚨 — | 🔴 2/1/3 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -0.49 | +0.43 | — | — |
| 7 | hold | `DHR` | book | `none` | — 🚨 — | 🔴 2/1/3 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🔴 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | +2.08 | +3.48 | — | — |
| 8 | hold | `SYK` | book | `none` | — — — | 🟡 2/2/2 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🟡 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🟡 | -3.61 | -3.10 | — | — |
| 9 | hold | `UTHR` | book | `none` | — 🚨 — | 🔴 2/1/3 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -1.54 | -3.03 | — | — |
| 10 | buy | `CEG` | book | `none` | — 🚨 — | 🟡 1/3/3 | ⬜ | join🟡 sect🟡 gen🔴 news🟢 dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟡 chd⬜ co🟢 set⬛ flw🔴 | -0.46 | -0.47 | — | — |
| 11 | hold | `EW` | book | `none` | — 🚨 — | 🔴 2/1/3 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -2.65 | -2.70 | — | — |
| 12 | hold | `FAST` | book | `none` | — 🚨 — | 🔴 2/1/3 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -1.52 | -0.35 | — | — |
| 13 | hold | `COO` | book | `none` | — 🚨 — | 🟡 1/3/2 | ⬜ | join🟡 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🟡 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -0.74 | -0.67 | — | — |
| 14 | hold | `TMO` | book | `none` | — 🚨 — | 🔴 1/2/3 | ⬜ | join🟡 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | +2.28 | +2.56 | — | — |
| 15 | buy | `VST` | book | `none` | — 🚨 — | 🟡 1/3/3 | ⬜ | join🟡 sect🟡 gen🔴 news🟢 dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟡 chd⬜ co🟢 set⬛ flw🔴 | -2.63 | -4.55 | — | — |
| 16 | hold | `MDT` | book | `none` | — 🚨 — | 🟢 3/1/2 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🟢 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -1.94 | -0.83 | — | — |
| 17 | buy | `WTW` | book | `none` | 🔵 — — | 🔴 2/1/3 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | +1.70 | +1.95 | — | — |
| 18 | hold | `GGG` | book | `none` | — 🚨 — | 🟡 2/2/2 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🟡 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🔴 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -1.19 | -2.48 | — | — |
| 19 | hold | `RMD` | book | `none` | — 🚨 — | 🔴 2/1/3 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -0.67 | +0.67 | — | — |
| 20 | hold | `ES` | book | `none` | — 🚨 — | 🟢 3/1/2 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🟢 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | +0.22 | -2.47 | — | — |
| 21 | buy | `ITW` | book | `none` | — 🚨 — | 🟡 0/4/2 | ⬜ | join🟡 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🟡 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -0.53 | -0.69 | — | — |
| 22 | hold | `AOS` | book | `none` | — 🚨 — | 🔴 1/2/3 | ⬜ | join🟡 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🔴 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -2.15 | -1.13 | — | — |
| 23 | buy | `ELV` | book | `none` | — 🚨 — | 🔴 2/1/3 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -0.55 | +0.53 | — | — |
| 24 | buy | `HLN` | book | `none` | — — — | 🟡 2/2/2 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🟡 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🟡 | +0.00 | +1.32 | — | — |
| 25 | buy | `ZBH` | book | `none` | — 🚨 — | 🟢 3/1/2 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🟢 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -1.76 | +0.24 | — | — |

Dropped from 1d BUY:

| Ticker | why | stack | 1d |
|---|---|---|---:|
| `MLYS` | swapped_for_mine_extra | `none` | -4.45 |

Sold overnight:

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| — | sell | `ABEV` | extra | `alarm_rebound` | — — — | 🟡 2/2/1 | ⬜ | join🟢 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🟡 | +1.08 | +1.80 | +3.60 | — |
| — | sell | `CIG` | extra | `alarm_rebound` | — — — | 🟡 2/2/1 | ⬜ | join🟡 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🟢 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🟢 | +1.58 | -1.05 | +1.05 | — |
| — | sell | `DHT` | extra | `alarm_rebound` | — 🚨 — | 🟡 1/3/1 | ⬜ | join🟡 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🟡 | +3.52 | +2.54 | +2.64 | — |
| — | sell | `ENB` | extra | `alarm_rebound` | — 🚨 — | 🟡 1/2/2 | ⬜ | join🟡 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🔴 | -2.22 | -0.78 | -1.81 | — |
| — | sell | `MLYS` | book | `none` | — — — | 🟡 1/2/2 | ⬜ | join🟡 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🔴 | +1.28 | -3.23 | -3.61 | — |
| — | sell | `PFE` | book | `none` | — — — | 🟡 2/2/1 | ⬜ | join🟢 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🟡 | +3.63 | +1.98 | +3.01 | — |
| — | sell | `SFD` | extra | `alarm_rebound` | — — — | 🟡 1/3/1 | ⬜ | join🟡 sect🟢 gen🔴 news⬛ dig⬜ jdg⬜ AB⬜ peer⬛ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd⬜ co⬛ set⬜ flw🟡 | +0.09 | -1.11 | -1.24 | — |

Seats 1d n=25 · p_win=24.0% · p_loss=72.0% · avg_win=+2.14 · avg_loss=-1.60 · mean=-0.64 · clip30=-0.64 · payoff=1.33.

### 2026-08-20 · `steady_blue+blue_white+blue+hot_ab_peer` · n=17 (keep 15 / add 2 / drop 0)

Market: — · tone `good`

1d BUY walk × mined stacks: keep / drop fade / swap stack-less / add gated extras → core `steady+blue` (52% hit / +9.54 mean on the mine board) → swing `blue+white` (white only with blue) → baseline `blue` → scalp `hot+ab+peer` (70.6% hit, small n)

Overlay 1d +0.82 · 2d — · 3d — · 1w — · W/L 8/8 · stock-book BUY 1d +1.10 · mine-only 1d +1.21 · universe med +0.72.

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| 1 | buy | `GSHD` | book | `steady_blue` | 🔵 — — | 🟢 5/2/1 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟢 set🟢 flw🔴 | — | — | — | — |
| 2 | buy | `ELF` | book | `steady_blue` | 🔵 — — | 🟢 4/3/1 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟡 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟡 set🟢 flw🔴 | +3.53 | — | — | — |
| 3 | buy | `MOS` | book | `steady_blue` | 🔵 — — | 🟢 4/3/2 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟢 jdg🟢 AB🔴 peer🔴 heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟢 set🔴 flw🟡 | +4.54 | — | — | — |
| 4 | buy | `CE` | book | `steady_blue` | 🔵 — — | 🟢 6/2/1 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟢 jdg🟢 AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟢 set🟢 flw🔴 | -0.38 | — | — | — |
| 5 | buy | `IRTC` | book | `steady_blue` | 🔵 — — | 🟡 3/3/2 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟢 jdg⬛ AB🟡 peer🔴 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟢 set🟡 flw🔴 | +0.42 | — | — | — |
| 6 | buy | `CALX` | book | `steady_blue` | 🔵 — — | 🟢 5/2/2 | ⬜ | join🟡 sect🟢 gen🟢 news⬛ dig🟢 jdg🔴 AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟡 set🟢 flw🔴 | -0.64 | — | — | — |
| 7 | buy | `OGS` | book | `steady_blue` | 🔵 — — | 🟢 4/3/1 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟡 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟡 set🟢 flw🔴 | -3.15 | — | — | — |
| 8 | buy | `HLNE` | book | `steady_blue` | 🔵 — — | 🟢 5/2/1 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟢 set🟢 flw🔴 | +1.24 | — | — | — |
| 9 | buy | `NHI` | book | `steady_blue` | 🔵 — — | 🟢 4/1/2 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig⬛ jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🔴 chd⬜ co⬛ set🟢 flw🔴 | -0.74 | — | — | — |
| 10 | buy | `AUPH` | book | `blue_white` | 🔵 — — | 🟢 4/3/1 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🔴 heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟢 set🟢 flw🟡 | -3.59 | — | — | — |
| 11 | buy | `OCUL` | book | `blue_white` | 🔵 — ⚪ | 🟢 5/3/0 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟢 set🟢 flw🟡 | -0.09 | — | — | — |
| 12 | buy | `WRBY` | book | `blue_white` | 🔵 — — | 🟢 5/2/1 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟢 set🟢 flw🔴 | -0.07 | — | — | — |
| 13 | buy | `CELH` | book | `blue_white` | 🔵 — ⚪ | 🟡 4/4/0 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟡 jdg⬛ AB🟢 peer🟢 heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟡 set🟢 flw🟡 | +2.52 | — | — | — |
| 14 | buy | `FIGR` | book | `blue_white` | 🔵 — ⚪ | 🟢 4/3/0 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer⬛ heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟢 set🟢 flw🟡 | +8.43 | — | — | — |
| 15 | buy | `EPAM` | book | `blue` | 🔵 — — | 🟢 5/2/2 | ⬜ | join🔴 sect🟢 gen🟢 news⬛ dig🟢 jdg🔴 AB🟢 peer🟢 heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟡 set🟢 flw🟡 | +3.35 | — | — | — |
| 16 | buy | `KC` | extra | `hot_ab_peer` | 🔵 — — | 🟢 6/1/2 | ⬜ | join🔴 sect🟢 gen🟢 news⬛ dig🟢 jdg🔴 AB🟢 peer🟢 heat⬜ vol🟢 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟡 set🟢 flw🟢 | -2.45 | — | — | — |
| 17 | buy | `UBS` | extra | `hot_ab_peer` | 🔵 — ⚪ | 🟢 6/2/0 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🟢 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟢 set🟢 flw🟢 | +0.17 | — | — | — |

Sold overnight:

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| — | sell | `AOS` | book | `none` | — 🚨 — | 🔴 1/2/3 | ⬜ | join🟡 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🔴 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -2.15 | -1.13 | — | — |
| — | sell | `CEG` | book | `none` | — 🚨 — | 🟡 1/3/3 | ⬜ | join🟡 sect🟡 gen🔴 news🟢 dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟡 chd⬜ co🟢 set⬛ flw🔴 | -0.46 | -0.47 | — | — |
| — | sell | `COO` | book | `none` | — 🚨 — | 🟡 1/3/2 | ⬜ | join🟡 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🟡 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -0.74 | -0.67 | — | — |
| — | sell | `DHR` | book | `none` | — 🚨 — | 🔴 2/1/3 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🔴 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | +2.08 | +3.48 | — | — |
| — | sell | `ELV` | book | `none` | — 🚨 — | 🔴 2/1/3 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -0.55 | +0.53 | — | — |
| — | sell | `ES` | book | `none` | — 🚨 — | 🟢 3/1/2 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🟢 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | +0.22 | -2.47 | — | — |
| — | sell | `EW` | book | `none` | — 🚨 — | 🔴 2/1/3 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -2.65 | -2.70 | — | — |
| — | sell | `FAST` | book | `none` | — 🚨 — | 🔴 2/1/3 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -1.52 | -0.35 | — | — |
| — | sell | `GGG` | book | `none` | — 🚨 — | 🟡 2/2/2 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🟡 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🔴 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -1.19 | -2.48 | — | — |
| — | sell | `HLN` | book | `none` | — — — | 🟡 2/2/2 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🟡 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🟡 | +0.00 | +1.32 | — | — |
| — | sell | `IQV` | book | `alarm_rebound` | — 🚨 — | 🔴 1/2/3 | ⬜ | join🟡 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | +2.72 | +4.23 | — | — |
| — | sell | `ITW` | book | `none` | — 🚨 — | 🟡 0/4/2 | ⬜ | join🟡 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🟡 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -0.53 | -0.69 | — | — |
| — | sell | `MDT` | book | `none` | — 🚨 — | 🟢 3/1/2 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🟢 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -1.94 | -0.83 | — | — |
| — | sell | `MUR` | book | `alarm_rebound` | — 🚨 — | 🔴 1/2/3 | ⬜ | join🟡 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | +3.82 | +1.47 | — | — |
| — | sell | `NEE` | book | `steady_blue` | 🔵 🚨 — | 🟡 2/4/1 | ⬜ | join🟢 sect🟡 gen🔴 news🟢 dig⬜ jdg⬛ AB⬛ peer🟡 heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🔴 par🟡 chd⬜ co🟢 set⬛ flw🟡 | -1.02 | -2.63 | — | — |
| — | sell | `NRG` | book | `blue` | 🔵 🚨 — | 🔴 1/2/4 | ⬜ | join🔴 sect🟡 gen🔴 news🟢 dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟡 chd⬜ co🟢 set⬛ flw🔴 | -4.33 | -6.20 | — | — |
| — | sell | `RMD` | book | `none` | — 🚨 — | 🔴 2/1/3 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -0.67 | +0.67 | — | — |
| — | sell | `SKM` | extra | `blue` | 🔵 — — | 🟡 2/3/1 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🟢 heat⬜ vol🟡 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🟡 | -1.11 | +4.00 | — | — |
| — | sell | `STE` | book | `none` | — 🚨 — | 🔴 2/1/3 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -0.49 | +0.43 | — | — |
| — | sell | `SYK` | book | `none` | — — — | 🟡 2/2/2 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🟡 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🟡 | -3.61 | -3.10 | — | — |
| — | sell | `TMO` | book | `none` | — 🚨 — | 🔴 1/2/3 | ⬜ | join🟡 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | +2.28 | +2.56 | — | — |
| — | sell | `UTHR` | book | `none` | — 🚨 — | 🔴 2/1/3 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -1.54 | -3.03 | — | — |
| — | sell | `VST` | book | `none` | — 🚨 — | 🟡 1/3/3 | ⬜ | join🟡 sect🟡 gen🔴 news🟢 dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🔴 par🟡 chd⬜ co🟢 set⬛ flw🔴 | -2.63 | -4.55 | — | — |
| — | sell | `WTW` | book | `none` | 🔵 — — | 🔴 2/1/3 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🔴 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | +1.70 | +1.95 | — | — |
| — | sell | `ZBH` | book | `none` | — 🚨 — | 🟢 3/1/2 | ⬜ | join🟢 sect🟡 gen🔴 news⬛ dig⬜ jdg⬛ AB⬛ peer🟢 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🔴 par🟡 chd⬜ co⬛ set⬛ flw🔴 | -1.76 | +0.24 | — | — |

Seats 1d n=16 · p_win=50.0% · p_loss=50.0% · avg_win=+3.03 · avg_loss=-1.39 · mean=+0.82 · clip30=+0.82 · payoff=2.18.

### 2026-08-21 · `hot_ab_peer+steady_blue+blue_white+blue+ab_and_peer` · n=24 (keep 22 / add 2 / drop 3)

Market: — · tone `good`

1d BUY walk × mined stacks: keep / drop fade / swap stack-less / add gated extras → scalp `hot+ab+peer` (70.6% hit, small n) → core `steady+blue` (52% hit / +9.54 mean on the mine board) → swing `blue+white` (white only with blue) → baseline `blue` → scalp `ab AND peer` (high hit, modest mean)

Overlay 1d — · 2d — · 3d — · 1w — · W/L 0/0 · stock-book BUY 1d — · mine-only 1d — · universe med —.

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| 1 | buy | `CRSP` | book | `hot_ab_peer` | 🔵 — ⚪ | 🟢 7/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🟢 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🟢 | — | — | — | — |
| 2 | buy | `OPCH` | book | `steady_blue` | 🔵 — — | 🟢 5/1/2 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🔴 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🔴 | — | — | — | — |
| 3 | buy | `XP` | book | `steady_blue` | 🔵 — ⚪ | 🟢 6/2/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🟡 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🟡 | — | — | — | — |
| 4 | buy | `FBRT` | book | `steady_blue` | — — — | 🟢 4/1/2 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬛ jdg⬛ AB🟢 peer🔴 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co⬛ set🟢 flw🔴 | — | — | — | — |
| 5 | hold | `MOS` | book | `steady_blue` | 🔵 — ⚪ | 🟢 7/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟡 peer🟢 heat⬜ vol🟢 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set🟡 flw🟢 | — | — | — | — |
| 6 | hold | `GSHD` | book | `steady_blue` | 🔵 — — | 🟢 7/0/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🔴 | — | — | — | — |
| 7 | buy | `WGS` | book | `steady_blue` | — — — | 🟢 5/1/2 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🔴 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🔴 | — | — | — | — |
| 8 | hold | `HLNE` | book | `steady_blue` | 🔵 — — | 🟢 7/0/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🔴 | — | — | — | — |
| 9 | hold | `NHI` | book | `steady_blue` | 🔵 — — | 🟢 6/0/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬛ jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co⬛ set🟢 flw🔴 | — | — | — | — |
| 10 | hold | `CE` | book | `steady_blue` | 🔵 — — | 🟢 7/0/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🔴 | — | — | — | — |
| 11 | buy | `NMRK` | book | `steady_blue` | — — — | 🟢 5/1/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬛ jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co⬛ set🟢 flw🔴 | — | — | — | — |
| 12 | buy | `Z` | book | `steady_blue` | — — — | 🟡 3/2/3 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟡 jdg⬛ AB🟢 peer🔴 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🔴 chd⬜ co🟡 set🟢 flw🔴 | — | — | — | — |
| 13 | buy | `DNN` | book | `steady_blue` | 🔵 — — | 🟢 4/2/3 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟢 jdg🟢 AB🔴 peer🔴 heat⬜ vol🟡 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🔴 chd⬜ co🟢 set🔴 flw🟡 | — | — | — | — |
| 14 | hold | `OCUL` | book | `blue_white` | — — — | 🟢 7/0/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🔴 | — | — | — | — |
| 15 | buy | `ABR` | book | `blue_white` | — — — | 🟢 4/1/2 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬛ jdg⬛ AB🟢 peer🔴 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co⬛ set🟢 flw🔴 | — | — | — | — |
| 16 | hold | `FIGR` | book | `blue_white` | 🔵 — ⚪ | 🟢 6/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer⬛ heat⬜ vol🟡 cat⬜ buy🟢 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🟡 | — | — | — | — |
| 17 | buy | `GPK` | book | `blue` | — — — | 🔴 3/1/4 | ⬜ | join🔴 sect🔴 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🔴 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🔴 chd⬜ co🟢 set🟢 flw🔴 | — | — | — | — |
| 18 | hold | `EPAM` | book | `ab_and_peer` | — 🚨 — | 🟢 6/0/3 | ⬜ | join🔴 sect🟢 gen🟢 news⬛ dig🟢 jdg🔴 AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟡 set🟢 flw🔴 | — | — | — | — |
| 19 | buy | `ZG` | book | `ab_and_peer` | — — — | 🟢 4/2/2 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟡 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🔴 chd⬜ co🟡 set🟢 flw🔴 | — | — | — | — |
| 20 | buy | `VNT` | book | `ab_and_peer` | — — — | 🟢 5/1/3 | ⬜ | join🔴 sect🟢 gen🟢 news⬛ dig🟢 jdg🔴 AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟡 set🟢 flw🔴 | — | — | — | — |
| 21 | buy | `NFG` | book | `ab_and_peer` | 🔵 — — | 🟢 6/1/2 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟢 jdg🟢 AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🔴 chd⬜ co🟢 set🟢 flw🔴 | — | — | — | — |
| 22 | buy | `BFAM` | book | `ab_and_peer` | — 🚨 — | 🟢 4/1/3 | ⬜ | join🔴 sect🔴 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🔴 chd⬜ co🟢 set🟢 flw🔴 | — | — | — | — |
| 23 | buy | `MT` | extra | `hot_ab_peer` | 🔵 — ⚪ | 🟢 7/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🟢 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🟢 | — | — | — | — |
| 24 | buy | `SUZ` | extra | `hot_ab_peer` | 🔵 — ⚪ | 🟢 7/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🟢 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🟢 | — | — | — | — |

Dropped from 1d BUY:

| Ticker | why | stack | 1d |
|---|---|---|---:|
| `DUOL` | fade | `fade` | — |
| `CALX` | fade | `fade` | — |
| `BZ` | fade | `fade` | — |

Sold overnight:

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| — | sell | `AUPH` | book | `blue_white` | 🔵 — — | 🟢 4/3/1 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🔴 heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟢 set🟢 flw🟡 | -3.59 | — | — | — |
| — | sell | `CALX` | book | `steady_blue` | 🔵 — — | 🟢 5/2/2 | ⬜ | join🟡 sect🟢 gen🟢 news⬛ dig🟢 jdg🔴 AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟡 set🟢 flw🔴 | -0.64 | — | — | — |
| — | sell | `CELH` | book | `blue_white` | 🔵 — ⚪ | 🟡 4/4/0 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟡 jdg⬛ AB🟢 peer🟢 heat⬜ vol🟡 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟡 set🟢 flw🟡 | +2.52 | — | — | — |
| — | sell | `ELF` | book | `steady_blue` | 🔵 — — | 🟢 4/3/1 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟡 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟡 set🟢 flw🔴 | +3.53 | — | — | — |
| — | sell | `IRTC` | book | `steady_blue` | 🔵 — — | 🟡 3/3/2 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟢 jdg⬛ AB🟡 peer🔴 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟢 set🟡 flw🔴 | +0.42 | — | — | — |
| — | sell | `KC` | extra | `hot_ab_peer` | 🔵 — — | 🟢 6/1/2 | ⬜ | join🔴 sect🟢 gen🟢 news⬛ dig🟢 jdg🔴 AB🟢 peer🟢 heat⬜ vol🟢 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟡 set🟢 flw🟢 | -2.45 | — | — | — |
| — | sell | `OGS` | book | `steady_blue` | 🔵 — — | 🟢 4/3/1 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟡 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟡 set🟢 flw🔴 | -3.15 | — | — | — |
| — | sell | `UBS` | extra | `hot_ab_peer` | 🔵 — ⚪ | 🟢 6/2/0 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🟢 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟢 set🟢 flw🟢 | +0.17 | — | — | — |
| — | sell | `WRBY` | book | `blue_white` | 🔵 — — | 🟢 5/2/1 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟡 chd⬜ co🟢 set🟢 flw🔴 | -0.07 | — | — | — |

1d not settled — names only.

### 2026-08-27 · `steady_blue+blue_white+ab_and_peer+hot_ab_peer` · n=19 (keep 16 / add 3 / drop 9)

Market: — · tone `neutral`

1d BUY walk × mined stacks: keep / drop fade / swap stack-less / add gated extras → core `steady+blue` (52% hit / +9.54 mean on the mine board) → swing `blue+white` (white only with blue) → scalp `ab AND peer` (high hit, modest mean) → scalp `hot+ab+peer` (70.6% hit, small n)

Overlay 1d — · 2d — · 3d — · 1w — · W/L 0/0 · stock-book BUY 1d — · mine-only 1d — · universe med —.

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| 1 | buy | `ACMR` | book | `steady_blue` | 🔵 — — | 🟢 8/2/1 | ⬜ | join🟢 sect🟢 gen🟢 news🟢 dig🟢 jdg🟡 AB🟢 peer🟢 heat🟢 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🟢 chd🟢 co🟢 set🟢 flw🔴 | — | — | — | — |
| 2 | hold | `VNT` | book | `steady_blue` | 🔵 — — | 🟢 7/1/2 | ⬜ | join🔴 sect🟢 gen🟢 news⬛ dig🟢 jdg🟡 AB🟢 peer🟢 heat🟢 vol🔴 cat⬜ buy🟢 yΔ⬛ | mkt🟡 par🟢 chd🟢 co🟢 set🟢 flw🔴 | — | — | — | — |
| 3 | buy | `MNDY` | book | `steady_blue` | 🔵 — — | 🟢 6/2/2 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg🟡 AB🟢 peer🟢 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🟢 chd🔴 co🟢 set🟢 flw🔴 | — | — | — | — |
| 4 | buy | `CRK` | book | `steady_blue` | — — — | 🟡 4/1/4 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🔴 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🔴 chd🔴 co🟢 set🟢 flw🔴 | — | — | — | — |
| 5 | buy | `CXT` | book | `steady_blue` | — — — | 🟢 4/2/3 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟡 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🔴 chd🔴 co🟢 set🟢 flw🔴 | — | — | — | — |
| 6 | hold | `BFAM` | book | `steady_blue` | — — — | 🟢 6/0/3 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat🔴 vol🔴 cat⬜ buy🟢 yΔ⬛ | mkt🟡 par🔴 chd🔴 co🟢 set🟢 flw🔴 | — | — | — | — |
| 7 | buy | `SFM` | book | `steady_blue` | — 🚨 — | 🟡 3/3/3 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟡 jdg⬛ AB🟢 peer🔴 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🟡 chd🔴 co🟡 set🟢 flw🔴 | — | — | — | — |
| 8 | buy | `DEC` | book | `steady_blue` | — — — | 🔴 4/1/5 | ⬜ | join🔴 sect🔴 gen🟢 news🟢 dig🟢 jdg⬛ AB🟢 peer🔴 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🔴 chd🔴 co🟢 set🟢 flw🔴 | — | — | — | — |
| 9 | buy | `KBR` | book | `steady_blue` | — — — | 🟢 5/1/3 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🔴 chd🔴 co🟢 set🟢 flw🔴 | — | — | — | — |
| 10 | buy | `OSK` | book | `steady_blue` | — — — | 🟢 5/1/3 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🔴 chd🔴 co🟢 set🟢 flw🔴 | — | — | — | — |
| 11 | buy | `RRC` | book | `steady_blue` | — — — | 🟢 5/1/4 | ⬜ | join🟢 sect🔴 gen🟢 news🟢 dig🟢 jdg⬛ AB🟢 peer🔴 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🔴 chd🔴 co🟢 set🟢 flw🔴 | — | — | — | — |
| 12 | hold | `EPAM` | book | `blue_white` | 🔵 — — | 🟢 7/1/2 | ⬜ | join🔴 sect🟢 gen🟢 news⬛ dig🟢 jdg🟡 AB🟢 peer🟢 heat🟢 vol🔴 cat⬜ buy🟢 yΔ⬛ | mkt🟡 par🟢 chd🟢 co🟢 set🟢 flw🔴 | — | — | — | — |
| 13 | buy | `ICL` | book | `ab_and_peer` | — — — | 🟢 6/1/3 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg🟢 AB🟢 peer🔴 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🟢 chd🔴 co🟢 set🟢 flw🔴 | — | — | — | — |
| 14 | buy | `ALHC` | book | `ab_and_peer` | — — — | 🔴 3/2/5 | ⬜ | join🔴 sect🟡 gen🟢 news⬛ dig🟢 jdg🔴 AB🟢 peer🔴 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🟡 chd🔴 co🟡 set🟢 flw🔴 | — | — | — | — |
| 15 | hold | `Z` | book | `ab_and_peer` | — — — | 🟢 6/2/1 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟡 jdg⬛ AB🟢 peer🟢 heat🟢 vol🔴 cat⬜ buy🟢 yΔ⬛ | mkt🟡 par🟡 chd🟢 co🟡 set🟢 flw🔴 | — | — | — | — |
| 16 | buy | `ROST` | extra | `hot_ab_peer` | — — — | 🟢 4/2/3 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟡 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🔴 chd🔴 co🟢 set🟢 flw🔴 | — | — | — | — |
| 17 | buy | `TRON` | extra | `hot_ab_peer` | — — — | 🟢 4/2/3 | ⬜ | join🟡 sect🔴 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🔴 chd🔴 co🟢 set🟢 flw🔴 | — | — | — | — |
| 18 | buy | `RZLT` | extra | `hot_ab_peer` | — — — | 🟢 5/2/3 | ⬜ | join🔴 sect🟡 gen🟢 news⬛ dig🟢 jdg🔴 AB🟢 peer🟢 heat🟢 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🟡 chd🟢 co🟡 set🟢 flw🔴 | — | — | — | — |
| 19 | hold | `GPK` | book | `none` | — — — | 🟢 6/0/3 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat🔴 vol🔴 cat⬜ buy🟢 yΔ⬛ | mkt🟡 par🔴 chd🔴 co🟢 set🟢 flw🔴 | — | — | — | — |

Dropped from 1d BUY:

| Ticker | why | stack | 1d |
|---|---|---|---:|
| `MOS` | fade | `fade` | — |
| `ELF` | fade | `fade` | — |
| `XP` | fade | `fade` | — |
| `SLI` | fade | `fade` | — |
| `LW` | fade | `fade` | — |
| `WT` | fade | `fade` | — |
| `TIGR` | fade | `fade` | — |
| `BXSL` | fade | `fade` | — |
| `DNN` | fade | `fade` | — |

Sold overnight:

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| — | sell | `ABR` | book | `blue_white` | — — — | 🟢 4/1/2 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬛ jdg⬛ AB🟢 peer🔴 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co⬛ set🟢 flw🔴 | — | — | — | — |
| — | sell | `CE` | book | `steady_blue` | 🔵 — — | 🟢 7/0/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🔴 | — | — | — | — |
| — | sell | `CRSP` | book | `hot_ab_peer` | 🔵 — ⚪ | 🟢 7/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🟢 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🟢 | — | — | — | — |
| — | sell | `DNN` | book | `steady_blue` | 🔵 — — | 🟢 4/2/3 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟢 jdg🟢 AB🔴 peer🔴 heat⬜ vol🟡 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🔴 chd⬜ co🟢 set🔴 flw🟡 | — | — | — | — |
| — | sell | `FBRT` | book | `steady_blue` | — — — | 🟢 4/1/2 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬛ jdg⬛ AB🟢 peer🔴 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co⬛ set🟢 flw🔴 | — | — | — | — |
| — | sell | `FIGR` | book | `blue_white` | 🔵 — ⚪ | 🟢 6/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer⬛ heat⬜ vol🟡 cat⬜ buy🟢 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🟡 | — | — | — | — |
| — | sell | `GSHD` | book | `steady_blue` | 🔵 — — | 🟢 7/0/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🔴 | — | — | — | — |
| — | sell | `HLNE` | book | `steady_blue` | 🔵 — — | 🟢 7/0/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🔴 | — | — | — | — |
| — | sell | `MOS` | book | `steady_blue` | 🔵 — ⚪ | 🟢 7/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟡 peer🟢 heat⬜ vol🟢 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set🟡 flw🟢 | — | — | — | — |
| — | sell | `MT` | extra | `hot_ab_peer` | 🔵 — ⚪ | 🟢 7/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🟢 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🟢 | — | — | — | — |
| — | sell | `NFG` | book | `ab_and_peer` | 🔵 — — | 🟢 6/1/2 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟢 jdg🟢 AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🔴 chd⬜ co🟢 set🟢 flw🔴 | — | — | — | — |
| — | sell | `NHI` | book | `steady_blue` | 🔵 — — | 🟢 6/0/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬛ jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🟢 | mkt🟢 par🟢 chd⬜ co⬛ set🟢 flw🔴 | — | — | — | — |
| — | sell | `NMRK` | book | `steady_blue` | — — — | 🟢 5/1/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig⬛ jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co⬛ set🟢 flw🔴 | — | — | — | — |
| — | sell | `OCUL` | book | `blue_white` | — — — | 🟢 7/0/1 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟢 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🔴 | — | — | — | — |
| — | sell | `OPCH` | book | `steady_blue` | 🔵 — — | 🟢 5/1/2 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🔴 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🔴 | — | — | — | — |
| — | sell | `SUZ` | extra | `hot_ab_peer` | 🔵 — ⚪ | 🟢 7/1/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🟢 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🟢 | — | — | — | — |
| — | sell | `WGS` | book | `steady_blue` | — — — | 🟢 5/1/2 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🔴 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🟢 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🔴 | — | — | — | — |
| — | sell | `XP` | book | `steady_blue` | 🔵 — ⚪ | 🟢 6/2/0 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat⬜ vol🟡 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🟢 chd⬜ co🟢 set🟢 flw🟡 | — | — | — | — |
| — | sell | `ZG` | book | `ab_and_peer` | — — — | 🟢 4/2/2 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟡 jdg⬛ AB🟢 peer🟢 heat⬜ vol🔴 cat⬜ buy🟡 yΔ🔴 | mkt🟢 par🔴 chd⬜ co🟡 set🟢 flw🔴 | — | — | — | — |

1d not settled — names only.

### 2026-08-30 · `steady_blue+ab_and_peer+alarm_rebound+hot_ab_peer` · n=25 (keep 20 / add 5 / drop 5)

Market: — · tone `neutral`

1d BUY walk × mined stacks: keep / drop fade / swap stack-less / add gated extras → core `steady+blue` (52% hit / +9.54 mean on the mine board) → scalp `ab AND peer` (high hit, modest mean) → rebound `alarm AND NOT white` → scalp `hot+ab+peer` (70.6% hit, small n)

Overlay 1d — · 2d — · 3d — · 1w — · W/L 0/0 · stock-book BUY 1d — · mine-only 1d — · universe med —.

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| 1 | buy | `DUOL` | book | `steady_blue` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| 2 | buy | `WAY` | book | `steady_blue` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| 3 | buy | `ATHM` | book | `steady_blue` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| 4 | hold | `BFAM` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| 5 | buy | `NCNO` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| 6 | hold | `MNDY` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| 7 | buy | `KD` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| 8 | hold | `CRK` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| 9 | buy | `ZG` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| 10 | buy | `COUR` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| 11 | buy | `LW` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| 12 | buy | `KT` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| 13 | buy | `ELF` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| 14 | hold | `Z` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| 15 | hold | `ALHC` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| 16 | buy | `CVI` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| 17 | buy | `TFPM` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| 18 | buy | `PBH` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| 19 | buy | `FIGR` | book | `alarm_rebound` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| 20 | buy | `DTM` | extra | `hot_ab_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| 21 | buy | `ANF` | extra | `hot_ab_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| 22 | buy | `MATV` | extra | `hot_ab_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| 23 | buy | `URBN` | extra | `hot_ab_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| 24 | buy | `CRML` | extra | `hot_ab_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| 25 | buy | `LEU` | book | `none` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |

Dropped from 1d BUY:

| Ticker | why | stack | 1d |
|---|---|---|---:|
| `RRC` | fade | `fade` | — |
| `XP` | fade | `fade` | — |
| `HLNE` | fade | `fade` | — |
| `SEZL` | fade | `fade` | — |
| `MH` | swapped_for_mine_extra | `none` | — |

Sold overnight:

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| — | sell | `ACMR` | book | `steady_blue` | 🔵 — — | 🟢 8/2/1 | ⬜ | join🟢 sect🟢 gen🟢 news🟢 dig🟢 jdg🟡 AB🟢 peer🟢 heat🟢 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🟢 chd🟢 co🟢 set🟢 flw🔴 | — | — | — | — |
| — | sell | `CXT` | book | `steady_blue` | — — — | 🟢 4/2/3 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟡 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🔴 chd🔴 co🟢 set🟢 flw🔴 | — | — | — | — |
| — | sell | `DEC` | book | `steady_blue` | — — — | 🔴 4/1/5 | ⬜ | join🔴 sect🔴 gen🟢 news🟢 dig🟢 jdg⬛ AB🟢 peer🔴 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🔴 chd🔴 co🟢 set🟢 flw🔴 | — | — | — | — |
| — | sell | `EPAM` | book | `blue_white` | 🔵 — — | 🟢 7/1/2 | ⬜ | join🔴 sect🟢 gen🟢 news⬛ dig🟢 jdg🟡 AB🟢 peer🟢 heat🟢 vol🔴 cat⬜ buy🟢 yΔ⬛ | mkt🟡 par🟢 chd🟢 co🟢 set🟢 flw🔴 | — | — | — | — |
| — | sell | `GPK` | book | `none` | — — — | 🟢 6/0/3 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat🔴 vol🔴 cat⬜ buy🟢 yΔ⬛ | mkt🟡 par🔴 chd🔴 co🟢 set🟢 flw🔴 | — | — | — | — |
| — | sell | `ICL` | book | `ab_and_peer` | — — — | 🟢 6/1/3 | ⬜ | join🟢 sect🟢 gen🟢 news⬛ dig🟢 jdg🟢 AB🟢 peer🔴 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🟢 chd🔴 co🟢 set🟢 flw🔴 | — | — | — | — |
| — | sell | `KBR` | book | `steady_blue` | — — — | 🟢 5/1/3 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🔴 chd🔴 co🟢 set🟢 flw🔴 | — | — | — | — |
| — | sell | `OSK` | book | `steady_blue` | — — — | 🟢 5/1/3 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🔴 chd🔴 co🟢 set🟢 flw🔴 | — | — | — | — |
| — | sell | `ROST` | extra | `hot_ab_peer` | — — — | 🟢 4/2/3 | ⬜ | join🟢 sect🔴 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟡 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🔴 chd🔴 co🟢 set🟢 flw🔴 | — | — | — | — |
| — | sell | `RRC` | book | `steady_blue` | — — — | 🟢 5/1/4 | ⬜ | join🟢 sect🔴 gen🟢 news🟢 dig🟢 jdg⬛ AB🟢 peer🔴 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🔴 chd🔴 co🟢 set🟢 flw🔴 | — | — | — | — |
| — | sell | `RZLT` | extra | `hot_ab_peer` | — — — | 🟢 5/2/3 | ⬜ | join🔴 sect🟡 gen🟢 news⬛ dig🟢 jdg🔴 AB🟢 peer🟢 heat🟢 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🟡 chd🟢 co🟡 set🟢 flw🔴 | — | — | — | — |
| — | sell | `SFM` | book | `steady_blue` | — 🚨 — | 🟡 3/3/3 | ⬜ | join🟢 sect🟡 gen🟢 news⬛ dig🟡 jdg⬛ AB🟢 peer🔴 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🟡 chd🔴 co🟡 set🟢 flw🔴 | — | — | — | — |
| — | sell | `TRON` | extra | `hot_ab_peer` | — — — | 🟢 4/2/3 | ⬜ | join🟡 sect🔴 gen🟢 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat🔴 vol🔴 cat⬜ buy🟡 yΔ⬛ | mkt🟡 par🔴 chd🔴 co🟢 set🟢 flw🔴 | — | — | — | — |
| — | sell | `VNT` | book | `steady_blue` | 🔵 — — | 🟢 7/1/2 | ⬜ | join🔴 sect🟢 gen🟢 news⬛ dig🟢 jdg🟡 AB🟢 peer🟢 heat🟢 vol🔴 cat⬜ buy🟢 yΔ⬛ | mkt🟡 par🟢 chd🟢 co🟢 set🟢 flw🔴 | — | — | — | — |

1d not settled — names only.

### 2026-08-31 · `hot_ab_peer+steady_blue+blue` · n=12 (keep 10 / add 2 / drop 0)

Market: hard_red · tone `bad`

1d BUY walk × mined stacks: keep / drop fade / swap stack-less / add gated extras → scalp `hot+ab+peer` (70.6% hit, small n) → core `steady+blue` (52% hit / +9.54 mean on the mine board) → baseline `blue`

Overlay 1d — · 2d — · 3d — · 1w — · W/L 0/0 · stock-book BUY 1d — · mine-only 1d — · universe med —.

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| 1 | buy | `CRM` | book | `hot_ab_peer` | 🔵 — — | 🟢 7/2/2 | probable | join🔴 sect🟢 gen🔴 news🟢 dig🟢 jdg🟡 AB🟢 peer🟢 heat🟢 vol🟢 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🔴 chd🟢 co🟢 set🟢 flw🟡 | — | — | — | — |
| 2 | buy | `CVX` | book | `steady_blue` | 🔵 — — | 🟢 6/3/2 | probable | join🟢 sect🟡 gen🔴 news🟢 dig🟢 jdg🟢 AB🟢 peer🔴 heat🟢 vol🟡 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd🟡 co🟡 set🟢 flw🟢 | — | — | — | — |
| 3 | buy | `BMO` | book | `steady_blue` | 🔵 — — | 🟢 6/2/2 | probable | join🟢 sect🟢 gen🔴 news🟢 dig🟢 jdg⬛ AB🟢 peer🔴 heat🟢 vol🟡 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | — | — | — | — |
| 4 | buy | `CVE` | book | `steady_blue` | 🔵 — — | 🟢 5/2/4 | probable | join🟢 sect🟡 gen🔴 news🟢 dig🟢 jdg🟢 AB🔴 peer🔴 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd🟡 co🟡 set🟢 flw🟢 | — | — | — | — |
| 5 | buy | `AON` | book | `steady_blue` | 🔵 — — | 🟢 4/2/3 | probable | join🟢 sect🟢 gen🔴 news⬛ dig🟡 jdg⬛ AB🟢 peer🔴 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd🟢 co🟢 set🟢 flw🟡 | — | — | — | — |
| 6 | buy | `LIN` | book | `blue` | 🔵 — — | 🟢 7/2/2 | probable | join🟢 sect🔴 gen🔴 news🟢 dig🟢 jdg🟢 AB🟢 peer🟢 heat🟢 vol🟡 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🔴 chd🟢 co🟡 set🟢 flw🟢 | — | — | — | — |
| 7 | buy | `AMZN` | book | `blue` | 🔵 — — | 🟢 4/3/3 | probable | join🟡 sect🔴 gen🔴 news🟢 dig🟢 jdg⬛ AB🟢 peer🔴 heat🟢 vol🟡 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🔴 chd🟢 co🟡 set🟢 flw🟡 | — | — | — | — |
| 8 | buy | `MPC` | book | `blue` | 🔵 — — | 🟢 7/3/1 | probable | join🟢 sect🟡 gen🔴 news🟢 dig🟢 jdg🟢 AB🟢 peer🟢 heat🟢 vol🟡 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | — | — | — | — |
| 9 | buy | `CM` | book | `blue` | 🔵 — — | 🟢 8/1/1 | probable | join🟢 sect🟢 gen🔴 news🟢 dig🟢 jdg⬛ AB🟢 peer🟢 heat🟢 vol🟢 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟡 | — | — | — | — |
| 10 | buy | `AMGN` | book | `blue` | 🔵 — — | 🔴 3/2/6 | probable | join🟢 sect🔴 gen🔴 news🔴 dig🔴 jdg🟢 AB🟢 peer🟡 heat🔴 vol🔴 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🔴 chd🔴 co🟢 set🟢 flw🔴 | — | — | — | — |
| 11 | buy | `TENB` | extra | `hot_ab_peer` | — — — | 🟢 5/3/2 | probable | join🔴 sect🟢 gen🔴 news⬛ dig🟢 jdg🟡 AB🟢 peer🟡 heat🟢 vol🟢 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🔴 chd🟢 co🟡 set🟢 flw🟢 | — | — | — | — |
| 12 | buy | `ESI` | extra | `hot_ab_peer` | — — — | 🟢 6/2/2 | blocked | join🟢 sect🔴 gen🔴 news⬛ dig🟢 jdg🟢 AB🟢 peer🟢 heat🟢 vol🟡 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🔴 chd🟢 co🟡 set🟢 flw🟢 | — | — | — | — |

Sold overnight:

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| — | sell | `ALHC` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| — | sell | `ANF` | extra | `hot_ab_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| — | sell | `ATHM` | book | `steady_blue` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| — | sell | `BFAM` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| — | sell | `COUR` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| — | sell | `CRK` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| — | sell | `CRML` | extra | `hot_ab_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| — | sell | `CVI` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| — | sell | `DTM` | extra | `hot_ab_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| — | sell | `DUOL` | book | `steady_blue` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| — | sell | `ELF` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| — | sell | `FIGR` | book | `alarm_rebound` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| — | sell | `KD` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| — | sell | `KT` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| — | sell | `LEU` | book | `none` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| — | sell | `LW` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| — | sell | `MATV` | extra | `hot_ab_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| — | sell | `MNDY` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| — | sell | `NCNO` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| — | sell | `PBH` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| — | sell | `TFPM` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| — | sell | `URBN` | extra | `hot_ab_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| — | sell | `WAY` | book | `steady_blue` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| — | sell | `Z` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |
| — | sell | `ZG` | book | `ab_and_peer` | ⬜ ⬜ ⬜ | ⬜ | ⬜ | join⬛ sect⬛ gen⬛ news⬛ dig⬛ jdg⬛ AB⬛ peer⬛ heat⬛ vol⬛ cat⬜ buy⬛ yΔ⬛ | mkt🟡 par⬛ chd⬛ co⬛ set⬛ flw⬛ | — | — | — | — |

1d not settled — names only.

### 2026-09-01 · `steady_blue+blue+ab_and_peer` · n=11 (keep 10 / add 1 / drop 0)

Market: hard_red · tone `bad`

1d BUY walk × mined stacks: keep / drop fade / swap stack-less / add gated extras → core `steady+blue` (52% hit / +9.54 mean on the mine board) → baseline `blue` → scalp `ab AND peer` (high hit, modest mean)

Overlay 1d — · 2d — · 3d — · 1w — · W/L 0/0 · stock-book BUY 1d — · mine-only 1d — · universe med —.

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| 1 | buy | `DHT` | book | `steady_blue` | 🔵 — — | 🟢 6/1/2 | probable | join🟢 sect🟢 gen🔴 news⬛ dig🟢 jdg⬛ AB🟢 peer🟢 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | — | — | — | — |
| 2 | buy | `CNR` | book | `steady_blue` | 🔵 — — | 🟢 5/1/3 | probable | join🟢 sect🟢 gen🔴 news⬛ dig🟢 jdg⬛ AB🟢 peer🔴 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | — | — | — | — |
| 3 | buy | `KMI` | book | `steady_blue` | 🔵 — — | 🟢 4/2/3 | probable | join🟢 sect🟢 gen🔴 news⬛ dig🟡 jdg⬛ AB🟢 peer🔴 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | — | — | — | — |
| 4 | buy | `FTI` | book | `steady_blue` | 🔵 — — | 🟢 5/1/3 | probable | join🟢 sect🟢 gen🔴 news⬛ dig🟢 jdg⬛ AB🟢 peer🔴 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟡 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | — | — | — | — |
| 5 | buy | `LNG` | book | `steady_blue` | 🔵 — — | 🟢 5/1/3 | probable | join🟢 sect🟢 gen🔴 news⬛ dig🟢 jdg⬛ AB🟢 peer🔴 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | — | — | — | — |
| 6 | buy | `DK` | book | `steady_blue` | 🔵 — — | 🟢 5/1/3 | probable | join🟢 sect🟢 gen🔴 news⬛ dig🟢 jdg⬛ AB🟢 peer🔴 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟡 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | — | — | — | — |
| 7 | buy | `OXY` | book | `steady_blue` | 🔵 — — | 🟢 7/1/2 | probable | join🟢 sect🟢 gen🔴 news🟢 dig🟢 jdg⬛ AB🟢 peer🟢 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd🟡 co🟡 set🟢 flw🟢 | — | — | — | — |
| 8 | buy | `INVX` | book | `steady_blue` | 🔵 — — | 🟢 5/1/3 | probable | join🟢 sect🟢 gen🔴 news⬛ dig🟢 jdg⬛ AB🟢 peer🔴 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟡 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | — | — | — | — |
| 9 | buy | `DOCS` | book | `blue` | 🔵 — — | 🟢 6/1/3 | probable | join🟢 sect🔴 gen🔴 news⬛ dig🟢 jdg🟢 AB🟢 peer🟢 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🔴 chd🟢 co🟡 set🟢 flw🟢 | — | — | — | — |
| 10 | hold | `CRM` | book | `ab_and_peer` | — — — | 🟢 6/1/4 | probable | join🔴 sect🟢 gen🔴 news🟢 dig🟢 jdg🟡 AB🟢 peer🟢 heat🔴 vol🔴 cat⬛ buy🟢 yΔ🔴 | mkt🔴 par🟡 chd🟡 co🟢 set🟢 flw🟡 | — | — | — | — |
| 11 | buy | `G` | extra | `steady_blue` | 🔵 — — | 🟢 5/2/3 | probable | join🟢 sect🟢 gen🔴 news⬛ dig🟢 jdg🟡 AB🟢 peer🟢 heat🔴 vol🔴 cat⬛ buy🟡 yΔ🟡 | mkt🔴 par🟡 chd🟡 co🟡 set🟢 flw🟢 | — | — | — | — |

Sold overnight:

| # | action | Ticker | src | stack | Marks | Cond | Hall-pass | cameras | coaches | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| — | sell | `AMGN` | book | `blue` | 🔵 — — | 🔴 3/2/6 | probable | join🟢 sect🔴 gen🔴 news🔴 dig🔴 jdg🟢 AB🟢 peer🟡 heat🔴 vol🔴 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🔴 chd🔴 co🟢 set🟢 flw🔴 | — | — | — | — |
| — | sell | `AMZN` | book | `blue` | 🔵 — — | 🟢 4/3/3 | probable | join🟡 sect🔴 gen🔴 news🟢 dig🟢 jdg⬛ AB🟢 peer🔴 heat🟢 vol🟡 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🔴 chd🟢 co🟡 set🟢 flw🟡 | — | — | — | — |
| — | sell | `AON` | book | `steady_blue` | 🔵 — — | 🟢 4/2/3 | probable | join🟢 sect🟢 gen🔴 news⬛ dig🟡 jdg⬛ AB🟢 peer🔴 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd🟢 co🟢 set🟢 flw🟡 | — | — | — | — |
| — | sell | `BMO` | book | `steady_blue` | 🔵 — — | 🟢 6/2/2 | probable | join🟢 sect🟢 gen🔴 news🟢 dig🟢 jdg⬛ AB🟢 peer🔴 heat🟢 vol🟡 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | — | — | — | — |
| — | sell | `CM` | book | `blue` | 🔵 — — | 🟢 8/1/1 | probable | join🟢 sect🟢 gen🔴 news🟢 dig🟢 jdg⬛ AB🟢 peer🟢 heat🟢 vol🟢 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟡 | — | — | — | — |
| — | sell | `CVE` | book | `steady_blue` | 🔵 — — | 🟢 5/2/4 | probable | join🟢 sect🟡 gen🔴 news🟢 dig🟢 jdg🟢 AB🔴 peer🔴 heat🟢 vol🔴 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd🟡 co🟡 set🟢 flw🟢 | — | — | — | — |
| — | sell | `CVX` | book | `steady_blue` | 🔵 — — | 🟢 6/3/2 | probable | join🟢 sect🟡 gen🔴 news🟢 dig🟢 jdg🟢 AB🟢 peer🔴 heat🟢 vol🟡 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🟢 chd🟡 co🟡 set🟢 flw🟢 | — | — | — | — |
| — | sell | `ESI` | extra | `hot_ab_peer` | — — — | 🟢 6/2/2 | blocked | join🟢 sect🔴 gen🔴 news⬛ dig🟢 jdg🟢 AB🟢 peer🟢 heat🟢 vol🟡 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🔴 chd🟢 co🟡 set🟢 flw🟢 | — | — | — | — |
| — | sell | `LIN` | book | `blue` | 🔵 — — | 🟢 7/2/2 | probable | join🟢 sect🔴 gen🔴 news🟢 dig🟢 jdg🟢 AB🟢 peer🟢 heat🟢 vol🟡 cat⬛ buy🟡 yΔ🔴 | mkt🔴 par🔴 chd🟢 co🟡 set🟢 flw🟢 | — | — | — | — |
| — | sell | `MPC` | book | `blue` | 🔵 — — | 🟢 7/3/1 | probable | join🟢 sect🟡 gen🔴 news🟢 dig🟢 jdg🟢 AB🟢 peer🟢 heat🟢 vol🟡 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🟢 chd🟢 co🟡 set🟢 flw🟢 | — | — | — | — |
| — | sell | `TENB` | extra | `hot_ab_peer` | — — — | 🟢 5/3/2 | probable | join🔴 sect🟢 gen🔴 news⬛ dig🟢 jdg🟡 AB🟢 peer🟡 heat🟢 vol🟢 cat⬛ buy🟡 yΔ🟢 | mkt🔴 par🔴 chd🟢 co🟡 set🟢 flw🟢 | — | — | — | — |

1d not settled — names only.

## Notes

1. The point is the combination: book quality gates + mined stacks + as-of cameras. Mine-only lost to stock-book BUY on the priced window. Overlay should track the book and beat it when a stack adds or a fade drops.
2. `blue` board mean +4.46 is squeeze-contaminated. Book lines clip ±30.
3. HARD_RED / thin BUY mornings stay thin. We do not backfill 25 lottery names.
4. A cameras (`ab` / `peer`) only print from **2026-08-20**. Before that, extras cannot fire `hot+ab+peer` / `ab+peer`.

