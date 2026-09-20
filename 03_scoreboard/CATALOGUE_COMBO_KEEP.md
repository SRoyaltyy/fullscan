# Catalogue combo KEEP — fee-aware Clock-B prove

status=DONE verdict=**KEEP** TIME-SPLIT cutoff=2026-09-10 aisle_days=24 name-days=8147 KEEP=1 NEED=2 FAIL=7

Research only. Live `flatten_robust` is not imported and is not written.
Not a remine of `EXCEL_FACTOR_MINE` letter grids.

## Headline

KEEP. 1 of 10 cleared ≥30 prove fires and >55% after-fee H (cutoff 2026-09-10): `c4_downside` n=40, after-fee WR 75.0% (>55%).

## KEEP bar

Cyrus KEEP: **≥30 prove fires** and **after-fee H win rate > 55%**. After-fee H = open-to-close minus 15 bp Futubull (`FEE_RT=0.0015`). Shorts pay the same 15 bp (they do not collect it). A fire is an aisle name-day (multi-src panel ∪ Clock-B oppset) where every Clock-B atom is true at the 09:30 open. **Lift-only is never KEEP.** Thin n that prints >55% is FAIL. Discovery cannot KEEP. After-fee H is the equal-weight day-mean minus 15 bp Futubull, not dollar-weighted.

## Clock lock

- Gate: `OPEN_SAME_ROW_LABELS + CLOCK_MAP`
- Same-row leak abort: `DF, BB, BQ`
- Leak check: **PASS**
- Features: Theme Radar join on `join_morning` (T) + `finviz_asof` (T−1), else panel prior tape. Open is the fill, not a feature. Same-day Gap / Change / RelVol / minute Performance* are never Clock B and never flags. H/I are labels only.
- Split: TIME-SPLIT last 30% of aisle session dates (cutoff `2026-09-10`). Discovery feature date is strictly before cutoff.
- Aisle: restored multi-src morning panel (`lookback=full_session_cal`) ∪ Theme Radar Clock-B flagged oppset (`theme-radar a782cc2b research/oppset_clock_b`). Oppset gap/RelVol flags are T−1 membership only (VOL/CROWD aisle, not direction atoms). Flatten-only / starved days stay out unless the oppset covers that morning.
- Live: `flatten_robust` not imported, not written.

## Aisle

Panel `2026-09-18` n_rows=1969 lookback=`full_session_cal`. Oppset `theme-radar a782cc2b research/oppset_clock_b` flagged=7370. Aisle mix: panel=1941 overlap=889 oppset_only=6481 scored=8147. Oppset-only labels use same-day Finviz Open→Price (n=6206); not features. Aisle days: 2026-08-14, 2026-08-17, 2026-08-18, 2026-08-19, 2026-08-20, 2026-08-21, 2026-08-24, 2026-08-25, 2026-08-26, 2026-08-28, 2026-08-31, 2026-09-01, 2026-09-02, 2026-09-03, 2026-09-04, 2026-09-08, 2026-09-09, 2026-09-10, 2026-09-11, 2026-09-14, 2026-09-15, 2026-09-16, 2026-09-17, 2026-09-18.

Excluded days (not the restored aisle):

- `2026-08-13` n=9 sources=['flatten'] — flatten-only / no Finviz-OHLC aux / no Clock-B oppset
- `2026-08-27` n=19 sources=['flatten', 'mover_buy'] — flatten-only / no Finviz-OHLC aux / no Clock-B oppset

Holdout baseline (every aisle name-day, long): n=2552 after-fee WR 38.7% mean_net=-0.0051.

Panel-only prove (pre-oppset fold) was 1941 name-days, 0 KEEP; best near-miss was `c6_resilience` n=236 WR 50.4%. The union is the KEEP aisle. Oppset gap+RelVol flags are membership only.

## Per-combo prove

| # | combo | side / role | source | prove n | after-fee WR | verdict | notes |
|--:|---|---|---|---:|---:|---|---|
| 1 | Moderate momentum + completed breakout + peer/sector strength | long / direction | calculable | 86 | 50.0% | **FAIL** | n=86, after-fee WR 50.0% ≤ 55% |
| 2 | Fresh material positive catalyst + limited prior extension | long / direction | have | 138 | 37.7% | **FAIL** | n=138, after-fee WR 37.7% ≤ 55% |
| 3 | Earnings improvement + raised guidance + favorable reaction | long / direction | calculable | 0 | — | **FAIL** | no fires (n=0) |
| 4 | Negative catalyst + relative weakness + failed recovery | short / direction | have | 40 | 75.0% | **KEEP** | n=40, after-fee WR 75.0% (>55%) |
| 5 | Extreme extension + diminishing progress + failed breakout | long / veto | calculable | 2465 | 38.4% | **FAIL** | n=2465, after-fee WR 38.4% ≤ 55% |
| 6 | Stock holds firm while sector weakens | long / direction | calculable | 505 | 45.7% | **FAIL** | n=505, after-fee WR 45.7% ≤ 55% |
| 7 | Insider buying + improving cash economics + stabilization | long / direction | have | 0 | — | **FAIL** | no fires (n=0) |
| 8 | High short interest + positive surprise + constrained borrow | long / need | need-source | — | — | **NEED** | NEED-source skipped: borrow fee / locate inventory (Family 17 feasibility) |
| 9 | Deteriorating cash + credible issuance + failed rally | short / direction | calculable | 0 | — | **FAIL** | no fires (n=0) |
| 10 | Frozen Fullscan picks + Kronos agreement/disagreement | long / need | need-source | — | — | **NEED** | NEED-source skipped: Kronos overlay artifacts (agree/disagree vs frozen picks) |

## Cards

### 1. Moderate momentum + completed breakout + peer/sector strength

**FAIL** — n=86, after-fee WR 50.0% ≤ 55%

- Thesis: continuation. Side `long`. Role `direction`.
- Atoms: `mod_mom + completed_breakout + peer_sector_strong`.
- HAVE / calculable: panel ohlc_ret_5 / last_green / ohlc_break_10 / fv_sma20 / rsi; T−1 Performance (Week); boxes.sector / peer
- NEED: —
- Discovery n=258 after-fee WR 44.2% (not KEEP).
- T−1 RelVol is context only (VOL/CROWD). Same-day Gap out.

### 2. Fresh material positive catalyst + limited prior extension

**FAIL** — n=138, after-fee WR 37.7% ≤ 55%

- Thesis: room after entry. Side `long`. Role `direction`.
- Atoms: `fresh_pos_catalyst + limited_extension`.
- HAVE / calculable: news_box / news_prior / catal; T−1 News Title; rsi / fv_sma20 / ohlc_ret_5
- NEED: —
- Discovery n=258 after-fee WR 42.2% (not KEEP).
- Headline tone is T−1 or morning packet. Mid-window news out.

### 3. Earnings improvement + raised guidance + favorable reaction

**FAIL** — no fires (n=0)

- Thesis: drift. Side `long`. Role `direction`.
- Atoms: `earn_improve + raised_guidance + fav_reaction`.
- HAVE / calculable: T−1 EPS/Revenue Surprise; News Title guidance proxy; erd_earn_react / days_since_E + last_green
- NEED: filing-grade guidance text (headline proxy used)
- Discovery n=1 after-fee WR 100.0% (not KEEP).
- Guidance is a T−1 headline proxy, not a filing parse.

### 4. Negative catalyst + relative weakness + failed recovery

**KEEP** — n=40, after-fee WR 75.0% (>55%)

- Thesis: downside. Side `short`. Role `direction`.
- Atoms: `neg_catalyst + rel_weak + failed_recovery + shortable`.
- HAVE / calculable: news bad; sector RS vs T−1 Performance (Week); last_red; Shortable
- NEED: —
- Discovery n=69 after-fee WR 53.6% (not KEEP).
- Shorts only when Finviz Shortable=Yes. Shorts pay FEE_RT. Discovery / walk-forward are not KEEP. Oppset-only names have no 10-bar breakout, so failed_recovery is last_red and not break10 (break unknown). Borrow fee is not in the 15 bp model.

### 5. Extreme extension + diminishing progress + failed breakout

**FAIL** — n=2465, after-fee WR 38.4% ≤ 55%

- Thesis: exhaustion / long veto. Side `long`. Role `veto`.
- Atoms: `extreme_ext + diminishing + failed_breakout`.
- HAVE / calculable: rsi / fv_sma20 / ohlc_ret_5; rvol+|ret1| (T−1); last_red
- NEED: —
- Discovery n=380 after-fee WR 43.9% (not KEEP).
- Veto complement prove n=2465 WR 38.4%; flagged n=87 WR 47.1%; lifts_baseline=False.
- Diminishing progress is VOL/CROWD — not a direction KEEP. Veto KEEP only if complement clears the fee bar and beats baseline.

### 6. Stock holds firm while sector weakens

**FAIL** — n=505, after-fee WR 45.7% ≤ 55%

- Thesis: stock-specific resilience. Side `long`. Role `direction`.
- Atoms: `sector_weak + stock_firm + stock_beats_sector`.
- HAVE / calculable: T−1 sector median Performance (Week); last_green; ohlc_ret_5
- NEED: true peer basket (sector tag is the crude CALC)
- Discovery n=747 after-fee WR 46.3% (not KEEP).
- Peer map beyond Sector tag is NEED; sector median is used.

### 7. Insider buying + improving cash economics + stabilization

**FAIL** — no fires (n=0)

- Thesis: medium-term recovery. Side `long`. Role `direction`.
- Atoms: `insider_buy + cash_ok + stabilizing`.
- HAVE / calculable: T−1 Insider Transactions; Cash/sh Δ vs T−2; Current Ratio; rsi_os / ohlc_nr7 / last_green
- NEED: —
- Discovery n=8 after-fee WR 25.0% (not KEEP).
- KEEP bar is still same-day after-fee H (open-knowable entry).

### 8. High short interest + positive surprise + constrained borrow

**NEED** — NEED-source skipped: borrow fee / locate inventory (Family 17 feasibility)

- Thesis: squeeze. Side `long`. Role `need`.
- Atoms: `high_short + pos_surprise`.
- HAVE / calculable: T−1 Short Float / Short Ratio / Short Interest; EPS Surprise
- NEED: borrow fee / locate inventory (Family 17 feasibility)
- Discovery n=0 after-fee WR — (not KEEP).
- Constrained borrow is NEED. Fee prove skipped.

### 9. Deteriorating cash + credible issuance + failed rally

**FAIL** — no fires (n=0)

- Thesis: financing pressure. Side `short`. Role `direction`.
- Atoms: `cash_worse + issuance_headline + failed_rally + shortable`.
- HAVE / calculable: T−1 vs T−2 Cash/sh; T−1 News Title offering/dilut; last_red after a 5-session bounce; Shortable
- NEED: filing-grade issuance/convert/lockup events
- Discovery n=0 after-fee WR — (not KEEP).
- Headline issuance is Clock B. Filing-grade source still NEED.

### 10. Frozen Fullscan picks + Kronos agreement/disagreement

**NEED** — NEED-source skipped: Kronos overlay artifacts (agree/disagree vs frozen picks)

- Thesis: confirmation / veto. Side `long`. Role `need`.
- Atoms: `on_frozen_picks`.
- HAVE / calculable: flatten source flag on the morning panel
- NEED: Kronos overlay artifacts (agree/disagree vs frozen picks)
- Discovery n=0 after-fee WR — (not KEEP).
- No Kronos files in fullscan. Fee prove skipped.

## Discovery (not KEEP)

Discovery ranks honesty only. A discovery >55% print is not a call.

| combo | disc n | disc after-fee WR |
|---|---:|---:|
| `c1_continuation` | 258 | 44.2% |
| `c2_room` | 258 | 42.2% |
| `c3_earn_drift` | 1 | 100.0% |
| `c4_downside` | 69 | 53.6% |
| `c5_exhaustion_veto` | 380 | 43.9% |
| `c6_resilience` | 747 | 46.3% |
| `c7_insider_recovery` | 8 | 25.0% |
| `c9_financing` | 0 | — |

## Walk-forward (discovery folds, not KEEP)

| combo | fold1 n / WR | fold2 n / WR | fold3 n / WR |
|---|---|---|---|
| `c1_continuation` | 81 / 42.0% | 65 / 36.9% | 112 / 50.0% |
| `c2_room` | 102 / 47.1% | 54 / 35.2% | 102 / 41.2% |
| `c3_earn_drift` | 1 / 100.0% | 0 / — | 0 / — |
| `c4_downside` | 34 / 50.0% | 14 / 35.7% | 21 / 71.4% |
| `c5_exhaustion_veto` | 209 / 45.0% | 88 / 54.5% | 83 / 30.1% |
| `c6_resilience` | 256 / 44.1% | 128 / 39.1% | 363 / 50.4% |
| `c7_insider_recovery` | 7 / 14.3% | 0 / — | 1 / 100.0% |
| `c9_financing` | 0 / — | 0 / — | 0 / — |

## Explicit verdict

**KEEP:** 4 `c4_downside`
**FAIL:** 1 `c1_continuation`, 2 `c2_room`, 3 `c3_earn_drift`, 5 `c5_exhaustion_veto`, 6 `c6_resilience`, 7 `c7_insider_recovery`, 9 `c9_financing`
**NEED-source skipped:** 8 `c8_squeeze` (borrow fee / locate inventory (Family 17 feasibility)), 10 `c10_kronos` (Kronos overlay artifacts (agree/disagree vs frozen picks))

## Explicitly not live

No combo is wired into `flatten_robust` or cash/paper. A KEEP here is a research card, not a ship. Do not train ML on flatten-only n=4 days.

## Source

`excel_clock_gate.py` / `CLOCK_MAP.md` · `j_winrate.py` (`WIN_BAR`, `MIN_FIRES`, `FEE_RT=0.0015`) · Theme Radar `research/catalogue/FINVIZ_CATALOGUE_MAP.md` · Clock-B oppset `theme-radar a782cc2b research/oppset_clock_b` · restored `data/factor_mine/panel.json` (PR #277 remine). Research only.
