# Catalogue combo KEEP — fee-aware Clock-B prove

status=DONE verdict=**FAIL** TIME-SPLIT cutoff=2026-09-10 aisle_days=24 name-days=1941 KEEP=0 NEED=2 FAIL=8

Research only. Live `flatten_robust` is not imported and is not written.
Not a remine of `EXCEL_FACTOR_MINE` letter grids.

## Headline

FAIL. 0 of 10 catalogue combos cleared the Cyrus KEEP bar on holdout (cutoff 2026-09-10). 2 NEED-source skipped, 8 FAIL. Best near-miss: `c6_resilience` prove n=236 after-fee WR 50.4%. Aisle baseline prove n=477 after-fee WR 48.6%. Lift-only is not KEEP.

## KEEP bar

Cyrus KEEP: **≥30 prove fires** and **after-fee H win rate > 55%**. After-fee H = open-to-close minus 15 bp Futubull (`FEE_RT=0.0015`). Shorts pay the same 15 bp (they do not collect it). A fire is a multi-src panel name-day where every Clock-B atom is true at the 09:30 open. **Lift-only is never KEEP.** Thin n that prints >55% is FAIL. Discovery cannot KEEP. After-fee H is the equal-weight day-mean minus 15 bp Futubull, not dollar-weighted.

## Clock lock

- Gate: `OPEN_SAME_ROW_LABELS + CLOCK_MAP`
- Same-row leak abort: `DF, BB, BQ`
- Leak check: **PASS**
- Features: T−1 Finviz + panel prior tape (Clock B). Open is the fill, not a feature. Same-day Gap / Change / RelVol / minute Performance* are never flags. H/I are labels only.
- Split: TIME-SPLIT last 30% of multi-src aisle session dates (cutoff `2026-09-10`). Discovery feature date is strictly before cutoff.
- Aisle: restored multi-src morning panel (`lookback=full_session_cal`). Flatten-only / starved days excluded.
- Live: `flatten_robust` not imported, not written.

## Aisle

Panel `2026-09-18` n_rows=1969 lookback=`full_session_cal`. Multi-src aisle days: 2026-08-14, 2026-08-17, 2026-08-18, 2026-08-19, 2026-08-20, 2026-08-21, 2026-08-24, 2026-08-25, 2026-08-26, 2026-08-28, 2026-08-31, 2026-09-01, 2026-09-02, 2026-09-03, 2026-09-04, 2026-09-08, 2026-09-09, 2026-09-10, 2026-09-11, 2026-09-14, 2026-09-15, 2026-09-16, 2026-09-17, 2026-09-18.

Excluded days (not the restored aisle):

- `2026-08-13` n=9 sources=['flatten'] — flatten-only / no Finviz-OHLC aux
- `2026-08-27` n=19 sources=['flatten', 'mover_buy'] — flatten-only / no Finviz-OHLC aux

Holdout baseline (every aisle name-day, long): n=477 after-fee WR 48.6% mean_net=+0.0017.

## Per-combo prove

| # | combo | side / role | source | prove n | after-fee WR | verdict | notes |
|--:|---|---|---|---:|---:|---|---|
| 1 | Moderate momentum + completed breakout + peer/sector strength | long / direction | calculable | 86 | 50.0% | **FAIL** | n=86, after-fee WR 50.0% ≤ 55% |
| 2 | Fresh material positive catalyst + limited prior extension | long / direction | have | 21 | 38.1% | **FAIL** | thin n=21 (bar ≥30), after-fee WR 38.1% |
| 3 | Earnings improvement + raised guidance + favorable reaction | long / direction | calculable | 0 | — | **FAIL** | no fires (n=0) |
| 4 | Negative catalyst + relative weakness + failed recovery | short / direction | have | 3 | 100.0% | **FAIL** | thin n=3 (bar ≥30), after-fee WR 100.0% |
| 5 | Extreme extension + diminishing progress + failed breakout | long / veto | calculable | 461 | 47.9% | **FAIL** | n=461, after-fee WR 47.9% ≤ 55% |
| 6 | Stock holds firm while sector weakens | long / direction | calculable | 236 | 50.4% | **FAIL** | n=236, after-fee WR 50.4% ≤ 55% |
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
- Discovery n=261 after-fee WR 43.7% (not KEEP).
- T−1 RelVol is context only (VOL/CROWD). Same-day Gap out.

### 2. Fresh material positive catalyst + limited prior extension

**FAIL** — thin n=21 (bar ≥30), after-fee WR 38.1%

- Thesis: room after entry. Side `long`. Role `direction`.
- Atoms: `fresh_pos_catalyst + limited_extension`.
- HAVE / calculable: news_box / news_prior / catal; T−1 News Title; rsi / fv_sma20 / ohlc_ret_5
- NEED: —
- Discovery n=66 after-fee WR 59.1% (not KEEP).
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

**FAIL** — thin n=3 (bar ≥30), after-fee WR 100.0%

- Thesis: downside. Side `short`. Role `direction`.
- Atoms: `neg_catalyst + rel_weak + failed_recovery + shortable`.
- HAVE / calculable: news bad; sector RS vs T−1 Performance (Week); last_red; Shortable
- NEED: —
- Discovery n=11 after-fee WR 54.5% (not KEEP).
- Shorts only when Finviz Shortable=Yes. Shorts pay FEE_RT.

### 5. Extreme extension + diminishing progress + failed breakout

**FAIL** — n=461, after-fee WR 47.9% ≤ 55%

- Thesis: exhaustion / long veto. Side `long`. Role `veto`.
- Atoms: `extreme_ext + diminishing + failed_breakout`.
- HAVE / calculable: rsi / fv_sma20 / ohlc_ret_5; rvol+|ret1| (T−1); last_red
- NEED: —
- Discovery n=109 after-fee WR 56.0% (not KEEP).
- Veto complement prove n=461 WR 47.9%; flagged n=16 WR 68.8%; lifts_baseline=False.
- Diminishing progress is VOL/CROWD — not a direction KEEP. Veto KEEP only if complement clears the fee bar and beats baseline.

### 6. Stock holds firm while sector weakens

**FAIL** — n=236, after-fee WR 50.4% ≤ 55%

- Thesis: stock-specific resilience. Side `long`. Role `direction`.
- Atoms: `sector_weak + stock_firm + stock_beats_sector`.
- HAVE / calculable: T−1 sector median Performance (Week); last_green; ohlc_ret_5
- NEED: true peer basket (sector tag is the crude CALC)
- Discovery n=412 after-fee WR 39.1% (not KEEP).
- Peer map beyond Sector tag is NEED; sector median is used.

### 7. Insider buying + improving cash economics + stabilization

**FAIL** — no fires (n=0)

- Thesis: medium-term recovery. Side `long`. Role `direction`.
- Atoms: `insider_buy + cash_ok + stabilizing`.
- HAVE / calculable: T−1 Insider Transactions; Cash/sh Δ vs T−2; Current Ratio; rsi_os / ohlc_nr7 / last_green
- NEED: —
- Discovery n=1 after-fee WR 100.0% (not KEEP).
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
| `c1_continuation` | 261 | 43.7% |
| `c2_room` | 66 | 59.1% |
| `c3_earn_drift` | 1 | 100.0% |
| `c4_downside` | 11 | 54.5% |
| `c5_exhaustion_veto` | 109 | 56.0% |
| `c6_resilience` | 412 | 39.1% |
| `c7_insider_recovery` | 1 | 100.0% |
| `c9_financing` | 0 | — |

## Walk-forward (discovery folds, not KEEP)

| combo | fold1 n / WR | fold2 n / WR | fold3 n / WR |
|---|---|---|---|
| `c1_continuation` | 81 / 42.0% | 68 / 35.3% | 112 / 50.0% |
| `c2_room` | 22 / 68.2% | 15 / 20.0% | 29 / 72.4% |
| `c3_earn_drift` | 1 / 100.0% | 0 / — | 0 / — |
| `c4_downside` | 7 / 57.1% | 1 / 100.0% | 3 / 33.3% |
| `c5_exhaustion_veto` | 27 / 55.6% | 43 / 60.5% | 39 / 51.3% |
| `c6_resilience` | 113 / 39.8% | 93 / 29.0% | 206 / 43.2% |
| `c7_insider_recovery` | 0 / — | 0 / — | 1 / 100.0% |
| `c9_financing` | 0 / — | 0 / — | 0 / — |

## Explicit verdict

**KEEP:** none
**FAIL:** 1 `c1_continuation`, 2 `c2_room`, 3 `c3_earn_drift`, 4 `c4_downside`, 5 `c5_exhaustion_veto`, 6 `c6_resilience`, 7 `c7_insider_recovery`, 9 `c9_financing`
**NEED-source skipped:** 8 `c8_squeeze` (borrow fee / locate inventory (Family 17 feasibility)), 10 `c10_kronos` (Kronos overlay artifacts (agree/disagree vs frozen picks))

## Explicitly not live

No combo is wired into `flatten_robust` or cash/paper. A KEEP here is a research card, not a ship. Do not train ML on flatten-only n=4 days.

## Source

`excel_clock_gate.py` / `CLOCK_MAP.md` · `j_winrate.py` (`WIN_BAR`, `MIN_FIRES`, `FEE_RT=0.0015`) · Theme Radar `research/catalogue/FINVIZ_CATALOGUE_MAP.md` · restored `data/factor_mine/panel.json` (PR #277 remine). Research only.
