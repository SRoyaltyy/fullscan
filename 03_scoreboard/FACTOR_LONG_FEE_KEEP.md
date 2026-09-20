# Factor-mine long fee KEEP — cash leaders + leftover Clock-B

status=DONE verdict=**KEEP** TIME-SPLIT cutoff=2026-09-10 aisle_days=25 name-days=8253 KEEP=1 thin-n=1 FAIL=4

Research only. Live `flatten_robust` is not imported and is not written.
Cash Book% on the remine / FACTOR_MINE_ACTION board is **not** KEEP.
Prior #281 Clock-B opp longs: 0 KEEP.

## Headline

KEEP vs goal (b). 1 of 5 longs cleared ≥30 prove fires and >55% after-fee H (cutoff 2026-09-10): `union_e_fresh_h3` n=34, after-fee WR 55.9% (>55%). Cash Book% is not KEEP.

## KEEP bar

Cyrus KEEP: **≥30 prove fires** and **after-fee H win rate > 55%**. After-fee H = open-to-close minus 15 bp Futubull (`FEE_RT=0.0015`). Shorts pay the same 15 bp (they do not collect it). A fire is an aisle name-day (multi-src panel ∪ Clock-B oppset) where the recipe gate is true at the 09:30 open. **Lift-only is never KEEP.** Thin n that prints >55% is **thin-n** (not KEEP). Discovery cannot KEEP. Cash Book% cannot KEEP. Goal (b) needs a **long** KEEP — a mix KEEP on `combo_se_5050_skip` does not count. After-fee H is the equal-weight day-mean minus 15 bp Futubull, not dollar-weighted.

## Goal

- (a) Touch-rate: already improved +3.3pp on Taskforce — partial, out of scope here.
- (b) Tangible factor-mine selection improvement: ≥1 **long** with prove n≥30 and after-fee H WR >55%. A long that clears this bar is the call; 0 long KEEP is FAIL.

## Clock lock

- Gate: `OPEN_SAME_ROW_LABELS + CLOCK_MAP`
- Same-row leak abort: `DF, BB, BQ`
- Leak check: **PASS**
- Features: cash-leader ERD / camera atoms and `#279` `clock_b_tells` on `join_morning` (T) + `finviz_asof` (T−1) / panel prior tape. Open is the fill, not a feature. Same-day Gap / Change / RelVol / minute Performance* are never Clock B and never flags. H/I are labels only.
- Split: TIME-SPLIT last 30% of aisle session dates (cutoff `2026-09-10`). Discovery feature date is strictly before cutoff.
- Aisle: restored multi-src morning panel (`lookback=full_session_cal`) ∪ Theme Radar Clock-B flagged oppset (`theme-radar a782cc2b research/oppset_clock_b`). Oppset membership is required **only** where the recipe requires opp (`union_clk_nr7_mom_opp_h1`). Oppset gap/RelVol flags are T−1 membership only (VOL/CROWD aisle, not direction atoms). Flatten-only / starved days stay out unless the oppset covers that morning.
- Live: `flatten_robust` not imported, not written. The miner module is not loaded (avoids live lookback).
- Remine input: GitHub Actions `35458265920` (FULLSCAN_OPPSET_UNION=1, from=2026-08-13, rebuild_panel=true).
- Prior prove: #281 FACTOR_CLK_LONG_KEEP (0 KEEP).

## Aisle

Panel `2026-09-18` n_rows=2542 lookback=`full_session_cal`. Oppset `theme-radar a782cc2b research/oppset_clock_b` flagged=7370. Aisle mix: panel=2533 overlap=1396 oppset_only=5974 scored=8253. Oppset-only labels use same-day Finviz Open→Price (n=5722); not features. Aisle days: 2026-08-14, 2026-08-17, 2026-08-18, 2026-08-19, 2026-08-20, 2026-08-21, 2026-08-24, 2026-08-25, 2026-08-26, 2026-08-27, 2026-08-28, 2026-08-31, 2026-09-01, 2026-09-02, 2026-09-03, 2026-09-04, 2026-09-08, 2026-09-09, 2026-09-10, 2026-09-11, 2026-09-14, 2026-09-15, 2026-09-16, 2026-09-17, 2026-09-18.

Excluded days (not the restored aisle):

- `2026-08-13` n=9 sources=['flatten'] — flatten-only / no Finviz-OHLC aux / no Clock-B oppset

Holdout baseline (every aisle name-day, long): n=2537 after-fee WR 38.7% mean_net=-0.0050.

## Cash Book% (not KEEP)

These printed on FACTOR_MINE_ACTION / remine cash books. They are leftover top-8 books with hard-red sit — a different metric. They cannot KEEP a name-day fee prove.

| # | recipe | remine Book% | starts | side | fee-KEEP |
|--:|---|---:|---|---|---|
| 1 | `union_e_fresh_h3` | +38.46% | 25/26 | long | **KEEP** |
| 2 | `combo_se_5050_skip` | +38.42% | 26/26 | mix | **FAIL** |
| 3 | `combo_ej_5050_shared` | +36.17% | 26/26 | long | **FAIL** |
| 4 | `union_clk_mom_break_peer_h1` | +3.05% | 26/26 | long | **FAIL** |
| 5 | `union_clk_hold_vs_sector_h1` | +4.71% | 18/26 | long | **FAIL** |
| 6 | `union_clk_nr7_mom_opp_h1` | -0.74% | 1/26 | long | **thin-n** |

## Per-recipe prove

| # | recipe | group | side | opp | prove n | after-fee WR | verdict | notes |
|--:|---|---|---|---|---:|---:|---|---|
| 1 | `union_e_fresh_h3` | A | long | no | 34 | 55.9% | **KEEP** | n=34, after-fee WR 55.9% (>55%) |
| 2 | `combo_se_5050_skip` | A | mix | no | 64 | 46.9% | **FAIL** | n=64, after-fee WR 46.9% ≤ 55% |
| 3 | `combo_ej_5050_shared` | A | long | no | 134 | 50.0% | **FAIL** | n=134, after-fee WR 50.0% ≤ 55% |
| 4 | `union_clk_mom_break_peer_h1` | B | long | no | 127 | 46.5% | **FAIL** | n=127, after-fee WR 46.5% ≤ 55% |
| 5 | `union_clk_hold_vs_sector_h1` | B | long | no | 156 | 51.3% | **FAIL** | n=156, after-fee WR 51.3% ≤ 55% |
| 6 | `union_clk_nr7_mom_opp_h1` | B | long | yes | 7 | 42.9% | **thin-n** | thin n=7 (bar ≥30), after-fee WR 42.9% |

## Cards

### 1. `union_e_fresh_h3`

**KEEP** — n=34, after-fee WR 55.9% (>55%)

- Thesis: knowable earnings printed within 1 session. Side `long`. Hold 3.
- Gate: `days_since_E≤1` + `flag_E≥0`; forbid `alarm`.
- HAVE / calculable: erd_days_since_E ≤ 1 and erd_flag_E ≥ 0; forbid alarm
- FACTOR_MINE_ACTION cash Book% +38.46% (starts 25/26) — not KEEP.
- Discovery n=216 after-fee WR 45.8% mean_net=-0.0087 (not KEEP).
- Prove n=34 after-fee WR 55.9% mean_net=+0.0112 n_pos=19.
- Goal (b) eligible: yes.
- Cash Book% +38.46% on FACTOR_MINE_ACTION is not KEEP. Hold 3 is a cash-book timer; name-day H is same-session.

### 2. `combo_se_5050_skip`

**FAIL** — n=64, after-fee WR 46.9% ≤ 55%

- Thesis: long-led cash mix; name-day prove scores both sides. Side `mix`. Hold mix.
- Gate: members `short_news_r_h3`, `union_e_fresh_h3`, net=`skip`.
- HAVE / calculable: short: boxes.news=bad; long: e_fresh (days_since_E≤1, flag_E≥0, no alarm). Skip when both claim one ticker.
- FACTOR_MINE_ACTION cash Book% +38.42% (starts 26/26) — not KEEP.
- Discovery n=282 after-fee WR 48.2% mean_net=-0.0058 (not KEEP).
- Prove n=64 after-fee WR 46.9% mean_net=-0.0021 n_pos=30 (long 33 / short 31).
- Goal (b) eligible: no (mix).
- Mixed recipe. A mix KEEP is not a long KEEP for goal (b). Shorts pay FEE_RT (they do not collect it).

### 3. `combo_ej_5050_shared`

**FAIL** — n=134, after-fee WR 50.0% ≤ 55%

- Thesis: two long rifles, one name-day lot via claim order. Side `long`. Hold mix.
- Gate: members `union_e_fresh_h3`, `union_join_vol_green_h1`, net=`priority`.
- HAVE / calculable: e_fresh as #1; join🟢 + vol🟢 + last_green, forbid alarm and news🔴. Same-side ties go to e_fresh.
- FACTOR_MINE_ACTION cash Book% +36.17% (starts 26/26) — not KEEP.
- Discovery n=377 after-fee WR 45.6% mean_net=-0.0032 (not KEEP).
- Prove n=134 after-fee WR 50.0% mean_net=+0.0046 n_pos=67.
- Goal (b) eligible: yes.
- Both members are longs. Cash Book% +36.17% is not KEEP.

### 4. `union_clk_mom_break_peer_h1`

**FAIL** — n=127, after-fee WR 46.5% ≤ 55%

- Thesis: continuation without Theme Radar membership. Side `long`. Hold 1.
- Gate: `clk_mom_break_peer=True`; forbid `clk_ext_veto` + `alarm`.
- HAVE / calculable: panel ohlc_ret_5 / last_green / macd_up / ohlc_break_10 / candle_capture / boxes.peer|sector / rs_week
- FACTOR_MINE_ACTION cash Book% +3.05% (starts 26/26) — not KEEP.
- Discovery n=233 after-fee WR 45.1% mean_net=-0.0002 (not KEEP).
- Prove n=127 after-fee WR 46.5% mean_net=+0.0003 n_pos=59.
- Goal (b) eligible: yes.
- #281 proved the opp splice (FAIL 50.0% n=54). This is the plain union recipe. Cash Book% +3.05% is not KEEP.

### 5. `union_clk_hold_vs_sector_h1`

**FAIL** — n=156, after-fee WR 51.3% ≤ 55%

- Thesis: stock-specific resilience, no opp filter. Side `long`. Hold 1.
- Gate: `clk_hold_vs_sector=True`; forbid `clk_ext_veto` + `alarm`.
- HAVE / calculable: boxes.sector bad + ohlc_ret_1 / last_green
- FACTOR_MINE_ACTION cash Book% +4.71% (starts 18/26) — not KEEP.
- Discovery n=240 after-fee WR 42.9% mean_net=-0.0065 (not KEEP).
- Prove n=156 after-fee WR 51.3% mean_net=+0.0063 n_pos=80.
- Goal (b) eligible: yes.
- #281 proved the opp splice (FAIL 49.0% n=102). This is the plain union recipe. Cash Book% +4.71% is not KEEP.

### 6. `union_clk_nr7_mom_opp_h1`

**thin-n** — thin n=7 (bar ≥30), after-fee WR 42.9%

- Thesis: coil then continuation on flagged oppset names. Side `long`. Hold 1.
- Gate: `clk_nr7_mom=True`, `oppset=True`; forbid `clk_ext_veto` + `alarm`.
- HAVE / calculable: panel ohlc_nr7 / ohlc_ret_5 / last_green / macd_up; Theme Radar T−1 oppset membership
- FACTOR_MINE_ACTION cash Book% -0.74% (starts 1/26) — not KEEP.
- Discovery n=7 after-fee WR 42.9% mean_net=+0.0043 (not KEEP).
- Prove n=7 after-fee WR 42.9% mean_net=-0.0034 n_pos=3.
- Goal (b) eligible: yes.
- #281 union_clk_nr7_mom_h1 (no opp) was thin-n n=15 WR 60%. Opp splice is thinner. Mark thin-n if prove n < 30. Cash Book% −0.74% (1/26 starts) is not KEEP.

## Discovery (not KEEP)

Discovery ranks honesty only. A discovery >55% print is not a call.

| recipe | disc n | disc after-fee WR |
|---|---:|---:|
| `union_e_fresh_h3` | 216 | 45.8% |
| `combo_se_5050_skip` | 282 | 48.2% |
| `combo_ej_5050_shared` | 377 | 45.6% |
| `union_clk_mom_break_peer_h1` | 233 | 45.1% |
| `union_clk_hold_vs_sector_h1` | 240 | 42.9% |
| `union_clk_nr7_mom_opp_h1` | 7 | 42.9% |

## Walk-forward (discovery folds, not KEEP)

| recipe | fold1 n / WR | fold2 n / WR | fold3 n / WR |
|---|---|---|---|
| `union_e_fresh_h3` | 95 / 46.3% | 64 / 48.4% | 57 / 42.1% |
| `combo_se_5050_skip` | 122 / 50.8% | 88 / 51.1% | 72 / 40.3% |
| `combo_ej_5050_shared` | 177 / 46.9% | 106 / 48.1% | 94 / 40.4% |
| `union_clk_mom_break_peer_h1` | 63 / 54.0% | 70 / 34.3% | 100 / 47.0% |
| `union_clk_hold_vs_sector_h1` | 41 / 48.8% | 81 / 38.3% | 118 / 44.1% |
| `union_clk_nr7_mom_opp_h1` | 1 / 0.0% | 5 / 60.0% | 1 / 0.0% |

## Explicit verdict

**KEEP:** `union_e_fresh_h3`
**FAIL:** `combo_se_5050_skip`, `combo_ej_5050_shared`, `union_clk_mom_break_peer_h1`, `union_clk_hold_vs_sector_h1`
**thin-n:** `union_clk_nr7_mom_opp_h1`

**Goal (b):** KEEP. Long(s) that cleared the fee bar: `union_e_fresh_h3`.

## Explicitly not live

No recipe is wired into `flatten_robust` or cash/paper. A KEEP here would be a research card, not a ship. Do not train ML on flatten-only starved days.

## Source

`src/clock_b_tells.py` (#279 recipe wire) · cash-leader gates restated (e_fresh / join_vol_green / short_news_r; miner not imported) · `excel_clock_gate.py` / `CLOCK_MAP.md` · `j_winrate.py` (`WIN_BAR`, `MIN_FIRES`, `FEE_RT=0.0015`) · Clock-B oppset `theme-radar a782cc2b research/oppset_clock_b` · oppset-union remine `35458265920` · prior #281 · restored `data/factor_mine/panel.json`. Research only.
