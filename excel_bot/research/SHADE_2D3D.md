# Shade hex — 2d / 3d cumulative stacked I

_Generated 2026-09-08 · live `flatten_robust` frozen · Yahoo/rows A–F seed only · no cards · no live push._

## Plain English

Same-day H already **DEMOTEd** open shade (M mid `#95CA82`) on the expanded universe. This beat re-cuts the **same open-only fills** against **2d and 3d cumulative** labels — not a new letter search.

**Family verdict: null** — KEEP 0 · KILL 8 · THIN 0 on I_sum 2d/3d shade-vs-parent (10 rows if the any-green parent is counted). H baseline excluded.

Open-only shade fills do not beat any-green parent on the same letter for 2d or 3d stacked I after Futubull fees, both SPY tapes, and the ghost bar. Same-day H baseline is not a KEEP either.

**Cyrus (visual):** Deep green/red look predictive because the loudest paints are same-row outcome color: Excel CF paints H>5% `#95CA82` and H>0 pale `#DCEDD5` (I>3% mint) after the close — that is H/I itself, not an open forecast; the one open-knowable mid-green (M `#95CA82` from yesterday's H × IY) is not that paint.

### Labels (clock-clean, documented)

Same definitions as `mine_hi_horizon.labels_for` / `HI_HORIZON.md`:

- **2d cumulative** = `I_sum` horizon 2 = `(1+I[t])×(1+I[t+1]) − 1`
- **3d cumulative** = `I_sum` horizon 3 = `(1+I[t])×(1+I[t+1])×(1+I[t+2]) − 1`
- **Same-day H** = Excel H (close vs open) — **baseline compare only**
- Same-row H/I are **labels**, never features. Lags from rows above are fair. Miner asserts I_sum against `labels_for` per ticker.

### Open-only gate (standing bar)

Source: `OPEN_SAME_ROW_LABELS.md` + `CLOCK_MAP.md` + `clock.py`. `excel_clock_gate.py` is not on this branch; the miner asserts `clock_map.json` `fill_mine_open` = A B C G J K L M O IR IS IT.

- Features at the open: only those 12 fills. This expand paints **M** (CF `IZ=1` from *prior-row* H and static IY). Known at 9:30.
- **Never** M's number `-(low-open)/open`. Close fills D E F H I N do not start a trade.
- Onset uses yesterday's M fill (lag). Soft-regime heat is the prior-5 mean of I (`[ei-5, ei)`). SPY tape is a ship-bar slice.
- `FILL_IDX` maps through `VISIBLE` (M=12). The 4.4k / +10.6% KEEP had `enumerate(OPEN_FILL)` so M read **H's fill** (H>5%). Aborted.

Panel **5193** tickers · **2018-09-04 → 2026-09-04** · **8,543,587** name-days · calendar days **2012**. Futubull 0.15% long off the recipe and the buy-everyone book. Beat book **and** any-green parent by ≥20 bp. Ghost: top-5 / drop-5 / July / day lottery / Q1. Both SPY tapes. Heat terciles cold ≤ -0.37%, hot ≥ 0.39%. Tip `c2537d95`.

A / G / K / L multi-shade and O mint are **not painted** on this expand (same M-only panel as #150/#151). IR/IS/IT have no CF.

### 2d stacked I (primary)

| meaning | holdout | vs book | vs parent | Q1 | SPY↑ | SPY↓ | top-5 | July | verdict | why | code |
|---|---|---|---|---|---|---|---|---|---|---|---|
| morning cell M is highlighted any green (known at 9:30) | -0.08% (n=318151) | -0.29 pp | — | -0.09% (n=740460) | +0.77% (n=355032) | -0.97% (n=298967) | -23% | 0% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_split,no_edge_vs_uncond,q1_sign,month_split | `M_green` |
| morning cell M is mid-or-deep green (score ≥ 1.5) (known at 9:30) | -0.08% (n=318151) | -0.29 pp | +0.00 pp | -0.09% (n=740460) | +0.77% (n=355032) | -0.97% (n=298967) | -23% | 0% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_split,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `M_ge15` |
| morning cell M is hex #95CA82 (mid green) (known at 9:30) | -0.08% (n=318151) | -0.29 pp | +0.00 pp | -0.09% (n=740460) | +0.77% (n=355032) | -0.97% (n=298967) | -23% | 0% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_split,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `M_hex_95CA82` |
| morning cell M was off or not green yesterday and is green today (known at 9:30) | -0.13% (n=249061) | -0.34 pp | -0.05 pp | -0.15% (n=582253) | +0.71% (n=270388) | -1.00% (n=245250) | -12% | 0% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_split,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `M_onset_green` |
| morning cell M flips onto hex #95CA82 (mid) today (known at 9:30) | -0.13% (n=249061) | -0.34 pp | -0.05 pp | -0.15% (n=582253) | +0.71% (n=270388) | -1.00% (n=245250) | -12% | 0% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_split,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `M_onset_hex_95CA82` |

### 3d stacked I (primary)

| meaning | holdout | vs book | vs parent | Q1 | SPY↑ | SPY↓ | top-5 | July | verdict | why | code |
|---|---|---|---|---|---|---|---|---|---|---|---|
| morning cell M is highlighted any green (known at 9:30) | -0.06% (n=318145) | -0.41 pp | — | -0.07% (n=740460) | +0.77% (n=355026) | -0.97% (n=298962) | -83% | 0% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split | `M_green` |
| morning cell M is mid-or-deep green (score ≥ 1.5) (known at 9:30) | -0.06% (n=318145) | -0.41 pp | +0.00 pp | -0.07% (n=740460) | +0.77% (n=355026) | -0.97% (n=298962) | -83% | 0% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `M_ge15` |
| morning cell M is hex #95CA82 (mid green) (known at 9:30) | -0.06% (n=318145) | -0.41 pp | +0.00 pp | -0.07% (n=740460) | +0.77% (n=355026) | -0.97% (n=298962) | -83% | 0% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `M_hex_95CA82` |
| morning cell M was off or not green yesterday and is green today (known at 9:30) | -0.15% (n=249056) | -0.51 pp | -0.09 pp | -0.17% (n=582253) | +0.69% (n=270383) | -1.08% (n=245245) | -13% | 0% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `M_onset_green` |
| morning cell M flips onto hex #95CA82 (mid) today (known at 9:30) | -0.15% (n=249056) | -0.51 pp | -0.09 pp | -0.17% (n=582253) | +0.69% (n=270383) | -1.08% (n=245245) | -13% | 0% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `M_onset_hex_95CA82` |

### Same-day H (baseline compare only)

Not the search. Standing 1d H DEMOTE from #150 is the compare.

| meaning | holdout | vs book | vs parent | Q1 | SPY↑ | SPY↓ | top-5 | July | verdict | why | code |
|---|---|---|---|---|---|---|---|---|---|---|---|
| morning cell M is highlighted any green (known at 9:30) | -0.19% (n=318152) | -0.09 pp | — | -0.21% (n=740460) | +0.27% (n=355032) | -0.76% (n=298973) | -3% | 0% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split | `M_green` |
| morning cell M is mid-or-deep green (score ≥ 1.5) (known at 9:30) | -0.19% (n=318152) | -0.09 pp | +0.00 pp | -0.21% (n=740460) | +0.27% (n=355032) | -0.76% (n=298973) | -3% | 0% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `M_ge15` |
| morning cell M is hex #95CA82 (mid green) (known at 9:30) | -0.19% (n=318152) | -0.09 pp | +0.00 pp | -0.21% (n=740460) | +0.27% (n=355032) | -0.76% (n=298973) | -3% | 0% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `M_hex_95CA82` |
| morning cell M was off or not green yesterday and is green today (known at 9:30) | -0.22% (n=249062) | -0.12 pp | -0.04 pp | -0.24% (n=582253) | +0.26% (n=270388) | -0.80% (n=245256) | -2% | 0% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `M_onset_green` |
| morning cell M flips onto hex #95CA82 (mid) today (known at 9:30) | -0.22% (n=249062) | -0.12 pp | -0.04 pp | -0.24% (n=582253) | +0.26% (n=270388) | -0.80% (n=245256) | -2% | 0% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `M_onset_hex_95CA82` |

### H-fill diagnostic (close-knowable — not a KEEP path)

Excel paints column H from **today's H number**. Using that fill as if it were an open shade is the 4.4k leak. These rows are forced `hfill_outcome_paint` / clock=close.

| meaning | label | holdout | vs book | verdict | why | code |
|---|---|---|---|---|---|---|
| same-row H > 0 paints pale #DCEDD5 (Excel CF pri 171) — close-knowable outcome paint, not an open feature | H@1d | +2.47% (n=1584170) | +2.57 pp | **KILL** | hfill_outcome_paint | `Hfill_gt00` |
| same-row H > 5% paints #95CA82 (Excel CF pri 126) — close-knowable outcome paint, not an open feature | H@1d | +11.19% (n=180956) | +11.29 pp | **KILL** | hfill_outcome_paint | `Hfill_gt05` |
| same-row H > 0 paints pale #DCEDD5 (Excel CF pri 171) — close-knowable outcome paint, not an open feature | I_sum@2d | +2.38% (n=1582111) | +2.17 pp | **KILL** | lottery_day,hfill_outcome_paint | `Hfill_gt00` |
| same-row H > 5% paints #95CA82 (Excel CF pri 126) — close-knowable outcome paint, not an open feature | I_sum@2d | +10.74% (n=180652) | +10.53 pp | **KILL** | disc_t,lottery_day,ticker_ghost,hfill_outcome_paint | `Hfill_gt05` |
| same-row H > 0 paints pale #DCEDD5 (Excel CF pri 171) — close-knowable outcome paint, not an open feature | I_sum@3d | +2.42% (n=1581237) | +2.07 pp | **KILL** | lottery_day,hfill_outcome_paint | `Hfill_gt00` |
| same-row H > 5% paints #95CA82 (Excel CF pri 126) — close-knowable outcome paint, not an open feature | I_sum@3d | +10.86% (n=180559) | +10.51 pp | **KILL** | disc_t,lottery_day,ticker_ghost,hfill_outcome_paint | `Hfill_gt05` |

### Ghost / name check (2d and 3d)

Holdout top-5 / drop-5 leftover vs book and any-green M / July / day lottery / Q1 / both SPY tapes. Futubull 0.15% on every print.

| recipe | label | holdout | vs book | vs parent | drop-5 | top-5 | July | day | Q1 | ghost |
|---|---|---|---|---|---|---|---|---|---|---|
| `M_ge15` | I_sum@2d | -0.08% (n=318151) | -0.29 pp | +0.00 pp | -0.10% (n=317475) | -19.3% (SFWL, SBET, GNPX, NYC, SNTI) | 0.0% | 2.9% | -0.10% (n=297218) | **FAIL** |
| `M_hex_95CA82` | I_sum@2d | -0.08% (n=318151) | -0.29 pp | +0.00 pp | -0.10% (n=317475) | -19.3% (SFWL, SBET, GNPX, NYC, SNTI) | 0.0% | 2.9% | -0.10% (n=297218) | **FAIL** |
| `M_onset_green` | I_sum@2d | -0.13% (n=249061) | -0.34 pp | -0.05 pp | -0.15% (n=248512) | -13.2% (SFWL, GNPX, NYC, SBET, BAOS) | 0.0% | 2.7% | -0.14% (n=233707) | **FAIL** |
| `M_onset_hex_95CA82` | I_sum@2d | -0.13% (n=249061) | -0.34 pp | -0.05 pp | -0.15% (n=248512) | -13.2% (SFWL, GNPX, NYC, SBET, BAOS) | 0.0% | 2.7% | -0.14% (n=233707) | **FAIL** |
| `Hfill_gt05` | I_sum@2d | +10.74% (n=180652) | +10.53 pp | +10.82 pp | +9.05% (n=179363) | 16.3% (NCPL, CHRD, SSM, BRTX, PROP) | 0.8% | 13.8% | +10.90% (n=165987) | **CONDITIONAL** |
| `M_ge15` | I_sum@3d | -0.06% (n=318145) | -0.41 pp | +0.00 pp | -0.08% (n=317472) | -29.6% (DEC, SBET, TXMD, NYC, RBNE) | 0.0% | 2.7% | -0.10% (n=297218) | **FAIL** |
| `M_hex_95CA82` | I_sum@3d | -0.06% (n=318145) | -0.41 pp | +0.00 pp | -0.08% (n=317472) | -29.6% (DEC, SBET, TXMD, NYC, RBNE) | 0.0% | 2.7% | -0.10% (n=297218) | **FAIL** |
| `M_onset_green` | I_sum@3d | -0.15% (n=249056) | -0.51 pp | -0.09 pp | -0.17% (n=248432) | -11.0% (DEC, TXMD, NYC, QMCO, COHN) | 0.0% | 3.5% | -0.17% (n=233707) | **FAIL** |
| `M_onset_hex_95CA82` | I_sum@3d | -0.15% (n=249056) | -0.51 pp | -0.09 pp | -0.17% (n=248432) | -11.0% (DEC, TXMD, NYC, QMCO, COHN) | 0.0% | 3.5% | -0.17% (n=233707) | **FAIL** |
| `Hfill_gt05` | I_sum@3d | +10.86% (n=180559) | +10.51 pp | +10.92 pp | +9.13% (n=179523) | 16.4% (NCPL, CHRD, SSM, INHD, DTST) | 0.9% | 13.6% | +10.99% (n=165987) | **CONDITIONAL** |
| `M_ge15` | H@1d | -0.19% (n=318152) | -0.09 pp | +0.00 pp | -0.20% (n=317523) | -4.2% (GDC, SSM, TSSI, NA, BESS) | 0.0% | 2.7% | -0.20% (n=297218) | **FAIL** |
| `M_hex_95CA82` | H@1d | -0.19% (n=318152) | -0.09 pp | +0.00 pp | -0.20% (n=317523) | -4.2% (GDC, SSM, TSSI, NA, BESS) | 0.0% | 2.7% | -0.20% (n=297218) | **FAIL** |
| `M_onset_green` | H@1d | -0.22% (n=249062) | -0.12 pp | -0.04 pp | -0.23% (n=248569) | -3.5% (GDC, SSM, NA, TSSI, BESS) | 0.0% | 3.5% | -0.23% (n=233707) | **FAIL** |
| `M_onset_hex_95CA82` | H@1d | -0.22% (n=249062) | -0.12 pp | -0.04 pp | -0.23% (n=248569) | -3.5% (GDC, SSM, NA, TSSI, BESS) | 0.0% | 3.5% | -0.23% (n=233707) | **FAIL** |
| `Hfill_gt05` | H@1d | +11.19% (n=180956) | +11.29 pp | +11.38 pp | +9.66% (n=179687) | 14.3% (IMTX, SSM, ILLR, RTB, BESS) | 1.1% | 9.5% | +11.32% (n=166174) | **PASS** |

### What this does not change

- Live `flatten_robust` is frozen. No card. No push.
- Same-day H shade DEMOTE from #150 stays demoted.
- Standing five-cell light+green O ± AH/FR is not remine.
- Close-entry fills stay out of the open clock.
- H-fill `#95CA82` is outcome paint — not an open shade KEEP.

Research only. Expanded panel is the #150/#151 Yahoo tape. Live frozen.
