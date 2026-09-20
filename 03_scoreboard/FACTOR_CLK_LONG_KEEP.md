# Factor-mine Clock-B long KEEP — fee-aware prove

status=DONE verdict=**FAIL** TIME-SPLIT cutoff=2026-09-10 aisle_days=25 name-days=8253 KEEP=0 thin-n=1 FAIL=2

Research only. Live `flatten_robust` is not imported and is not written.
Cash Book% on the remine board is **not** KEEP.

## Headline

FAIL vs goal (b). 0 of 3 Clock-B longs cleared the Cyrus fee-KEEP bar on holdout (cutoff 2026-09-10). 2 FAIL, 1 thin-n. Best near-miss: `union_clk_mom_break_peer_opp_h1` prove n=54 after-fee WR 50.0%. Aisle baseline prove n=2537 after-fee WR 38.7%. Cash Book% is not KEEP. Lift-only is not KEEP. Touch-rate already improved +3.3pp on Taskforce (goal a partial).

## KEEP bar

Cyrus KEEP: **≥30 prove fires** and **after-fee H win rate > 55%**. After-fee H = open-to-close minus 15 bp Futubull (`FEE_RT=0.0015`). A fire is an aisle name-day (multi-src panel ∪ Clock-B oppset) where the #279 recipe gate is true at the 09:30 open. **Lift-only is never KEEP.** Thin n that prints >55% is **thin-n** (not KEEP). Discovery cannot KEEP. Cash Book% cannot KEEP. After-fee H is the equal-weight day-mean minus 15 bp Futubull, not dollar-weighted.

## Goal

- (a) Touch-rate: already improved +3.3pp on Taskforce — partial, out of scope here.
- (b) Tangible factor-mine selection improvement: a Clock-B long that clears this fee-KEEP bar. **0 KEEP = FAIL vs goal (b).**

## Clock lock

- Gate: `OPEN_SAME_ROW_LABELS + CLOCK_MAP`
- Same-row leak abort: `DF, BB, BQ`
- Leak check: **PASS**
- Features: `#279` `clock_b_tells` atoms on `join_morning` (T) + `finviz_asof` (T−1) / panel prior tape. Open is the fill, not a feature. Same-day Gap / Change / RelVol / minute Performance* are never Clock B and never flags. H/I are labels only.
- Split: TIME-SPLIT last 30% of aisle session dates (cutoff `2026-09-10`). Discovery feature date is strictly before cutoff.
- Aisle: restored multi-src morning panel (`lookback=full_session_cal`) ∪ Theme Radar Clock-B flagged oppset (`theme-radar a782cc2b research/oppset_clock_b`). Recipes that require opp fire only on oppset membership. Oppset gap/RelVol flags are T−1 membership only (VOL/CROWD aisle, not direction atoms). Flatten-only / starved days stay out unless the oppset covers that morning.
- Live: `flatten_robust` not imported, not written. The miner module is not loaded (avoids live lookback).
- Remine input: GitHub Actions `35458265920` (FULLSCAN_OPPSET_UNION=1, from=2026-08-13, rebuild_panel=true).

## Aisle

Panel `2026-09-18` n_rows=2542 lookback=`full_session_cal`. Oppset `theme-radar a782cc2b research/oppset_clock_b` flagged=7370. Aisle mix: panel=2533 overlap=1396 oppset_only=5974 scored=8253. Oppset-only labels use same-day Finviz Open→Price (n=5722); not features. Aisle days: 2026-08-14, 2026-08-17, 2026-08-18, 2026-08-19, 2026-08-20, 2026-08-21, 2026-08-24, 2026-08-25, 2026-08-26, 2026-08-27, 2026-08-28, 2026-08-31, 2026-09-01, 2026-09-02, 2026-09-03, 2026-09-04, 2026-09-08, 2026-09-09, 2026-09-10, 2026-09-11, 2026-09-14, 2026-09-15, 2026-09-16, 2026-09-17, 2026-09-18.

Excluded days (not the restored aisle):

- `2026-08-13` n=9 sources=['flatten'] — flatten-only / no Finviz-OHLC aux / no Clock-B oppset

Holdout baseline (every aisle name-day, long): n=2537 after-fee WR 38.7% mean_net=-0.0050.

## Cash Book% (not KEEP)

These printed on the remine cash/recipe board. They are leftover top-8 books with hard-red sit — a different metric. They cannot KEEP a name-day fee prove.

| recipe | remine Book% | starts | fee-KEEP |
|---|---:|---|---|
| `union_clk_mom_break_peer_opp_h1` | +13.82% | 19/26 | **FAIL** |
| `union_clk_nr7_mom_h1` | +12.48% | 26/26 | **thin-n** |
| `union_clk_hold_vs_sector_opp_h1` | +8.41% | 26/26 | **FAIL** |

## Per-recipe prove

| # | recipe | Clock-B | opp | prove n | after-fee WR | verdict | notes |
|--:|---|---|---|---:|---:|---|---|
| 1 | `union_clk_mom_break_peer_opp_h1` | `clk_mom_break_peer` | yes | 54 | 50.0% | **FAIL** | n=54, after-fee WR 50.0% ≤ 55% |
| 2 | `union_clk_nr7_mom_h1` | `clk_nr7_mom` | no | 15 | 60.0% | **thin-n** | thin n=15 (bar ≥30), after-fee WR 60.0% |
| 3 | `union_clk_hold_vs_sector_opp_h1` | `clk_hold_vs_sector` | yes | 102 | 49.0% | **FAIL** | n=102, after-fee WR 49.0% ≤ 55% |

## Cards

### 1. `union_clk_mom_break_peer_opp_h1`

**FAIL** — n=54, after-fee WR 50.0% ≤ 55%

- Thesis: continuation on flagged opportunity-set names. Side `long`. Hold 1.
- Gate: `clk_mom_break_peer=True`, `oppset=True`; forbid `clk_ext_veto` + `alarm`.
- HAVE / calculable: panel ohlc_ret_5 / last_green / macd_up / ohlc_break_10 / candle_capture / boxes.peer|sector / rs_week; Theme Radar T−1 oppset membership
- Remine cash Book% +13.82% (starts 19/26) — not KEEP.
- Discovery n=96 after-fee WR 47.9% mean_net=+0.0016 (not KEEP).
- Prove n=54 after-fee WR 50.0% mean_net=+0.0034 n_pos=27.
- Cash Book% +13.82% on remine 35458265920 is not KEEP. Longs also forbid clk_ext_veto and alarm (#279 wire).

### 2. `union_clk_nr7_mom_h1`

**thin-n** — thin n=15 (bar ≥30), after-fee WR 60.0%

- Thesis: coil then continuation. Side `long`. Hold 1.
- Gate: `clk_nr7_mom=True`; forbid `clk_ext_veto` + `alarm`.
- HAVE / calculable: panel ohlc_nr7 / ohlc_ret_5 / last_green / macd_up
- Remine cash Book% +12.48% (starts 26/26) — not KEEP.
- Discovery n=29 after-fee WR 41.4% mean_net=+0.0064 (not KEEP).
- Prove n=15 after-fee WR 60.0% mean_net=+0.0066 n_pos=9.
- Cash Book% +12.48% with 26/26 starts is not KEEP. May be thin on holdout fires. Does not require oppset.

### 3. `union_clk_hold_vs_sector_opp_h1`

**FAIL** — n=102, after-fee WR 49.0% ≤ 55%

- Thesis: stock holds while sector camera is red, on oppset. Side `long`. Hold 1.
- Gate: `clk_hold_vs_sector=True`, `oppset=True`; forbid `clk_ext_veto` + `alarm`.
- HAVE / calculable: boxes.sector bad + ohlc_ret_1 / last_green; Theme Radar T−1 oppset membership
- Remine cash Book% +8.41% (starts 26/26) — not KEEP.
- Discovery n=134 after-fee WR 40.3% mean_net=-0.0071 (not KEEP).
- Prove n=102 after-fee WR 49.0% mean_net=+0.0023 n_pos=50.
- Cash Book% +8.41% on remine 35458265920 is not KEEP.

## Discovery (not KEEP)

Discovery ranks honesty only. A discovery >55% print is not a call.

| recipe | disc n | disc after-fee WR |
|---|---:|---:|
| `union_clk_mom_break_peer_opp_h1` | 96 | 47.9% |
| `union_clk_nr7_mom_h1` | 29 | 41.4% |
| `union_clk_hold_vs_sector_opp_h1` | 134 | 40.3% |

## Walk-forward (discovery folds, not KEEP)

| recipe | fold1 n / WR | fold2 n / WR | fold3 n / WR |
|---|---|---|---|
| `union_clk_mom_break_peer_opp_h1` | 29 / 58.6% | 27 / 37.0% | 40 / 47.5% |
| `union_clk_nr7_mom_h1` | 3 / 33.3% | 16 / 37.5% | 10 / 50.0% |
| `union_clk_hold_vs_sector_opp_h1` | 29 / 41.4% | 48 / 41.7% | 57 / 38.6% |

## Explicit verdict

**KEEP:** none
**FAIL:** `union_clk_mom_break_peer_opp_h1`, `union_clk_hold_vs_sector_opp_h1`
**thin-n:** `union_clk_nr7_mom_h1`

**Goal (b):** FAIL. No long cleared ≥30 prove fires and >55% after-fee H.

## Explicitly not live

No recipe is wired into `flatten_robust` or cash/paper. A KEEP here would be a research card, not a ship. Do not train ML on flatten-only starved days.

## Source

`src/clock_b_tells.py` (#279 recipe wire) · `excel_clock_gate.py` / `CLOCK_MAP.md` · `j_winrate.py` (`WIN_BAR`, `MIN_FIRES`, `FEE_RT=0.0015`) · Clock-B oppset `theme-radar a782cc2b research/oppset_clock_b` · oppset-union remine `35458265920` · restored `data/factor_mine/panel.json`. Research only.
