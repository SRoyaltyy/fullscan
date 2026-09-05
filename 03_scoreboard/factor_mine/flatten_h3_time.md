# Factor mine action — `flatten_h3_time`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

Side **long** · universe `flatten` · top 8 · rank `list` · size `leftover` · sell `time` · S-boost `none` · sell at min-hold even if still listed

Cash book **+11.56%** ($11,156) · signal-only (no cash/fees) was +44.29%. Starts YES **16/17**. Fills 80 · skips 129 · realized $+735.17.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `flatten` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `time` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $33.10.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $97.53 | $10,153.12 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $97.53 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | $10,178.12 | +25.00 | MARA, LDI, BTBT | — | $63.95 | $10,435.42 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8 | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,178.12 vs prior close $10,153.12 (+25.00) because holdings re-marked: BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13 |
| 2026-08-17 | +2.25 | $63.95 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8 | $10,414.78 | -20.64 | TMC, DNN, HNST | — | $48.44 | $10,525.15 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8, TMC×1, DNN×2, HNST×1 | 09:30 open · cash $63.95 (unchanged overnight, no fees) · equity $10,414.78 vs prior close $10,435.42 (-20.64) because holdings re-marked: BTSG×20 yday $61.71 → 09:30 $61.69 -0.40; IREN×27 yday $44.06 → 09:30 $45.23 +31.59; TPG×24 yday $53.03 → 09:30 $52.67 -8.64; TGTX×25 yday $48.74 → 09:30 $48.74 +0.00; SLS×106 yday $12.78 → 09:30 $12.78 +0.00; HIMS×42 yday $28.15 → 09:30 $28.14 -0.42; INO×1543 yday $1.09 → 09:30 $1.07 -30.86; TNDM×53 yday $22.72 → 09:30 $22.50 -11.66; MARA×1 yday $9.20 → 09:30 $9.22 +0.02; LDI×13 yday $0.90 → 09:30 $0.91 +0.13; BTBT×8 yday $1.57 → 09:30 $1.52 -0.40 |
| 2026-08-18 | -6.20 | $48.44 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8, TMC×1, DNN×2, HNST×1 | $10,391.80 | -133.35 | — | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $10,309.06 | $10,355.74 | MARA×1, LDI×13, BTBT×8, TMC×1, DNN×2, HNST×1 | 09:30 open · cash $48.44 (unchanged overnight, no fees) · equity $10,391.80 vs prior close $10,525.15 (-133.35) because holdings re-marked: BTSG×20 yday $60.38 → 09:30 $60.00 -7.60; IREN×27 yday $44.90 → 09:30 $43.56 -36.18; TPG×24 yday $51.77 → 09:30 $51.77 +0.00; TGTX×25 yday $49.28 → 09:30 $49.28 +0.00; SLS×106 yday $13.00 → 09:30 $12.66 -36.04; HIMS×42 yday $28.61 → 09:30 $27.85 -31.92; INO×1543 yday $1.15 → 09:30 $1.14 -15.43; TNDM×53 yday $22.25 → 09:30 $22.16 -5.03; MARA×1 yday $9.72 → 09:30 $9.36 -0.36; LDI×13 yday $0.88 → 09:30 $0.87 -0.07; BTBT×8 yday $1.60 → 09:30 $1.54 -0.48; TMC×1 yday $3.77 → 09:30 $3.72 -0.05; DNN×2 yday $3.19 → 09:30 $3.11 -0.16; HNST×1 yday $4.70 → 09:30 $4.67 -0.03 |
| 2026-08-19 | -7.20 | $10,309.06 | MARA×1, LDI×13, BTBT×8, TMC×1, DNN×2, HNST×1 | $10,355.88 | +0.14 | — | MARA, LDI, BTBT | $10,340.32 | $10,355.75 | TMC×1, DNN×2, HNST×1 | 09:30 open · cash $10,309.06 (unchanged overnight, no fees) · equity $10,355.88 vs prior close $10,355.74 (+0.14) because holdings re-marked: MARA×1 yday $8.96 → 09:30 $8.91 -0.05; LDI×13 yday $0.86 → 09:30 $0.88 +0.29; BTBT×8 yday $1.45 → 09:30 $1.42 -0.24; TMC×1 yday $3.92 → 09:30 $3.93 +0.01; DNN×2 yday $3.15 → 09:30 $3.19 +0.08; HNST×1 yday $4.75 → 09:30 $4.80 +0.05 |
| 2026-08-20 | +1.12 | $10,340.32 | TMC×1, DNN×2, HNST×1 | $10,355.62 | -0.13 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | TMC, DNN, HNST | $209.64 | $10,569.98 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8 | 09:30 open · cash $10,340.32 (unchanged overnight, no fees) · equity $10,355.62 vs prior close $10,355.75 (-0.13) because holdings re-marked: TMC×1 yday $3.97 → 09:30 $3.92 -0.05; DNN×2 yday $3.22 → 09:30 $3.20 -0.04; HNST×1 yday $5.02 → 09:30 $4.98 -0.04 |
| 2026-08-21 | +3.25 | $209.64 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8 | $10,845.87 | +275.89 | AUPH, ARCT, AUTL, CRDL, CYPH | — | $94.04 | $10,844.89 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19 | 09:30 open · cash $209.64 (unchanged overnight, no fees) · equity $10,845.87 vs prior close $10,569.98 (+275.89) because holdings re-marked: AG×62 yday $21.19 → 09:30 $21.90 +44.02; BHP×14 yday $93.63 → 09:30 $95.72 +29.26; CDE×62 yday $21.11 → 09:30 $21.75 +39.68; HDSN×224 yday $5.57 → 09:30 $5.67 +22.40; IAG×65 yday $20.50 → 09:30 $21.17 +43.55; KGC×43 yday $31.43 → 09:30 $32.17 +31.82; NFGC×739 yday $1.75 → 09:30 $1.79 +29.56; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 |
| 2026-08-24 | -5.17 | $94.04 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19 | $10,974.27 | +129.38 | — | — | $94.04 | $10,815.87 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19 | 09:30 open · cash $94.04 (unchanged overnight, no fees) · equity $10,974.27 vs prior close $10,844.89 (+129.38) because holdings re-marked: AG×62 yday $21.09 → 09:30 $21.47 +23.56; BHP×14 yday $97.03 → 09:30 $97.34 +4.34; CDE×62 yday $20.97 → 09:30 $21.26 +17.98; HDSN×224 yday $5.63 → 09:30 $5.69 +13.44; IAG×65 yday $21.14 → 09:30 $21.44 +19.50; KGC×43 yday $32.76 → 09:30 $33.21 +19.35; NFGC×739 yday $1.84 → 09:30 $1.86 +14.78; WPM×8 yday $157.78 → 09:30 $158.96 +9.44; AUPH×1 yday $16.65 → 09:30 $16.60 -0.05; ARCT×2 yday $13.45 → 09:30 $13.26 -0.38; AUTL×10 yday $2.41 → 09:30 $2.36 -0.50; CRDL×13 yday $1.86 → 09:30 $1.87 +0.13; CYPH×19 yday $1.42 → 09:30 $1.83 +7.79 |
| 2026-08-25 | +1.80 | $94.04 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19 | $10,885.73 | +69.86 | MOS, OCUL, INSP, CRMD, RZLT, HCA | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $86.05 | $10,842.19 | AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19, MOS×74, OCUL×163, INSP×29, CRMD×216, RZLT×342, HCA×4 | 09:30 open · cash $94.04 (unchanged overnight, no fees) · equity $10,885.73 vs prior close $10,815.87 (+69.86) because holdings re-marked: AG×62 yday $20.57 → 09:30 $20.73 +9.92; BHP×14 yday $96.66 → 09:30 $95.95 -9.94; CDE×62 yday $20.49 → 09:30 $20.85 +22.32; HDSN×224 yday $5.57 → 09:30 $5.53 -8.96; IAG×65 yday $21.36 → 09:30 $21.63 +17.55; KGC×43 yday $32.47 → 09:30 $32.76 +12.47; NFGC×739 yday $1.90 → 09:30 $1.91 +7.39; WPM×8 yday $158.00 → 09:30 $160.00 +16.00; AUPH×1 yday $16.60 → 09:30 $16.71 +0.11; ARCT×2 yday $13.76 → 09:30 $14.34 +1.16; AUTL×10 yday $2.38 → 09:30 $2.32 -0.60; CRDL×13 yday $1.80 → 09:30 $1.90 +1.30; CYPH×19 yday $1.64 → 09:30 $1.70 +1.14 |
| 2026-08-26 | +2.02 | $86.05 | AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19, MOS×74, OCUL×163, INSP×29, CRMD×216, RZLT×342, HCA×4 | $10,842.19 | -0.00 | — | — | $86.05 | $10,844.33 | AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19, MOS×74, OCUL×163, INSP×29, CRMD×216, RZLT×342, HCA×4 | 09:30 open · cash $86.05 (unchanged overnight, no fees) · equity $10,842.19 vs prior close $10,842.19 (-0.00) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.71 +0.00; ARCT×2 yday $14.21 → 09:30 $14.21 +0.00; AUTL×10 yday $2.34 → 09:30 $2.34 +0.00; CRDL×13 yday $1.90 → 09:30 $1.90 +0.00; CYPH×19 yday $1.64 → 09:30 $1.64 +0.00; MOS×74 yday $23.75 → 09:30 $23.75 +0.00; OCUL×163 yday $10.92 → 09:30 $10.92 +0.00; INSP×29 yday $61.47 → 09:30 $61.47 +0.00; CRMD×216 yday $8.28 → 09:30 $8.28 +0.00; RZLT×342 yday $5.29 → 09:30 $5.29 +0.00; HCA×4 yday $428.50 → 09:30 $428.50 +0.00 |
| 2026-08-27 | — | $86.05 | AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19, MOS×74, OCUL×163, INSP×29, CRMD×216, RZLT×342, HCA×4 | $10,834.22 | -10.11 | RRC, CRK, SLI | AUPH, ARCT, AUTL, CRDL, CYPH | $29.71 | $10,794.34 | MOS×74, OCUL×163, INSP×29, CRMD×216, RZLT×342, HCA×4, RRC×1, CRK×5, SLI×27 | 09:30 open · cash $86.05 (unchanged overnight, no fees) · equity $10,834.22 vs prior close $10,844.33 (-10.11) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.60 -0.11; ARCT×2 yday $14.21 → 09:30 $15.35 +2.28; AUTL×10 yday $2.34 → 09:30 $2.41 +0.70; CRDL×13 yday $1.90 → 09:30 $2.03 +1.69; CYPH×19 yday $1.64 → 09:30 $1.60 -0.76; MOS×74 yday $23.75 → 09:30 $24.84 +80.66; OCUL×163 yday $10.92 → 09:30 $10.79 -21.19; INSP×29 yday $61.47 → 09:30 $60.07 -40.60; CRMD×216 yday $8.28 → 09:30 $8.60 +69.12; RZLT×342 yday $5.29 → 09:30 $5.01 -95.76; HCA×4 yday $428.50 → 09:30 $427.50 -4.00 |
| 2026-08-28 | +0.75 | $29.71 | MOS×74, OCUL×163, INSP×29, CRMD×216, RZLT×342, HCA×4, RRC×1, CRK×5, SLI×27 | $10,789.26 | -5.08 | MOS | MOS, OCUL, INSP, CRMD, RZLT, HCA | $23.64 | $10,664.06 | RRC×1, CRK×5, SLI×27, MOS×440 | 09:30 open · cash $29.71 (unchanged overnight, no fees) · equity $10,789.26 vs prior close $10,794.34 (-5.08) because holdings re-marked: MOS×74 yday $24.16 → 09:30 $24.00 -11.84; OCUL×163 yday $10.77 → 09:30 $10.63 -22.82; INSP×29 yday $61.80 → 09:30 $62.10 +8.70; CRMD×216 yday $8.39 → 09:30 $8.49 +21.60; RZLT×342 yday $5.04 → 09:30 $5.07 +10.26; HCA×4 yday $427.16 → 09:30 $424.61 -10.20; RRC×1 yday $41.55 → 09:30 $41.44 -0.11; CRK×5 yday $14.50 → 09:30 $14.42 -0.40; SLI×27 yday $2.61 → 09:30 $2.60 -0.27 |
| 2026-08-31 | -5.85 | $23.64 | RRC×1, CRK×5, SLI×27, MOS×440 | $10,655.32 | -8.74 | — | — | $23.64 | $10,668.94 | RRC×1, CRK×5, SLI×27, MOS×440 | 09:30 open · cash $23.64 (unchanged overnight, no fees) · equity $10,655.32 vs prior close $10,664.06 (-8.74) because holdings re-marked: RRC×1 yday $41.64 → 09:30 $41.11 -0.53; CRK×5 yday $14.62 → 09:30 $14.56 -0.30; SLI×27 yday $2.64 → 09:30 $2.51 -3.51; MOS×440 yday $23.76 → 09:30 $23.75 -4.40 |
| 2026-09-01 | -6.30 | $23.64 | RRC×1, CRK×5, SLI×27, MOS×440 | $10,769.41 | +100.47 | — | RRC, CRK, SLI | $207.39 | $10,877.39 | MOS×440 | 09:30 open · cash $23.64 (unchanged overnight, no fees) · equity $10,769.41 vs prior close $10,668.94 (+100.47) because holdings re-marked: RRC×1 yday $41.78 → 09:30 $41.32 -0.46; CRK×5 yday $14.51 → 09:30 $14.31 -1.00; SLI×27 yday $2.51 → 09:30 $2.70 +5.13; MOS×440 yday $23.78 → 09:30 $24.00 +96.80 |
| 2026-09-02 | -3.83 | $207.39 | MOS×440 | $10,740.99 | -136.40 | — | MOS | $10,735.16 | $10,735.16 | — | 09:30 open · cash $207.39 (unchanged overnight, no fees) · equity $10,740.99 vs prior close $10,877.39 (-136.40) because holdings re-marked: MOS×440 yday $24.25 → 09:30 $23.94 -136.40 |
| 2026-09-03 | -0.90 | $10,735.16 | — | $10,735.16 | -0.00 | ATRC, HRMY, CABA, VSTM, RVTY | — | $43.62 | $11,288.31 | ATRC×43, HRMY×51, CABA×656, VSTM×278, RVTY×17 | 09:30 open · cash $10,735.16 · no holdings · equity $10,735.16 vs prior close $10,735.16 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $43.62 | ATRC×43, HRMY×51, CABA×656, VSTM×278, RVTY×17 | $11,372.16 | +83.85 | NVAX | — | $33.10 | $11,156.11 | ATRC×43, HRMY×51, CABA×656, VSTM×278, RVTY×17, NVAX×1 | 09:30 open · cash $43.62 (unchanged overnight, no fees) · equity $11,372.16 vs prior close $11,288.31 (+83.85) because holdings re-marked: ATRC×43 yday $52.59 → 09:30 $52.88 +12.47; HRMY×51 yday $42.86 → 09:30 $42.93 +3.57; CABA×656 yday $3.57 → 09:30 $3.63 +39.36; VSTM×278 yday $8.02 → 09:30 $8.03 +2.78; RVTY×17 yday $130.94 → 09:30 $132.45 +25.67 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,178.12 vs prior close $10,153.12 (+25.00) because holdings re-marked: BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $88.43 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-13.5; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 13 | $0.94 | $0.16 | — | $76.09 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.5; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 8 | $1.50 | $0.14 | — | $63.95 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.95 | ▼ 09:30 equity $10,414.78 vs yday $10,435.42 (-20.64) | 09:30 open · cash $63.95 (unchanged overnight, no fees) · equity $10,414.78 vs prior close $10,435.42 (-20.64) because holdings re-marked: BTSG×20 yday $61.71 → 09:30 $61.69 -0.40; IREN×27 yday $44.06 → 09:30 $45.23 +31.59; TPG×24 yday $53.03 → 09:30 $52.67 -8.64; TGTX×25 yday $48.74 → 09:30 $48.74 +0.00; SLS×106 yday $12.78 → 09:30 $12.78 +0.00; HIMS×42 yday $28.15 → 09:30 $28.14 -0.42; INO×1543 yday $1.09 → 09:30 $1.07 -30.86; TNDM×53 yday $22.72 → 09:30 $22.50 -11.66; MARA×1 yday $9.20 → 09:30 $9.22 +0.02; LDI×13 yday $0.90 → 09:30 $0.91 +0.13; BTBT×8 yday $1.57 → 09:30 $1.52 -0.40 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 1 | $4.05 | $0.04 | — | $59.85 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; leftover $7.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 2 | $3.24 | $0.07 | — | $53.30 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; leftover $7.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 1 | $4.81 | $0.05 | — | $48.44 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-11.4; leftover $7.99 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.44 | ▼ 09:30 equity $10,391.80 vs yday $10,525.15 (-133.35) | 09:30 open · cash $48.44 (unchanged overnight, no fees) · equity $10,391.80 vs prior close $10,525.15 (-133.35) because holdings re-marked: BTSG×20 yday $60.38 → 09:30 $60.00 -7.60; IREN×27 yday $44.90 → 09:30 $43.56 -36.18; TPG×24 yday $51.77 → 09:30 $51.77 +0.00; TGTX×25 yday $49.28 → 09:30 $49.28 +0.00; SLS×106 yday $13.00 → 09:30 $12.66 -36.04; HIMS×42 yday $28.61 → 09:30 $27.85 -31.92; INO×1543 yday $1.15 → 09:30 $1.14 -15.43; TNDM×53 yday $22.25 → 09:30 $22.16 -5.03; MARA×1 yday $9.72 → 09:30 $9.36 -0.36; LDI×13 yday $0.88 → 09:30 $0.87 -0.07; BTBT×8 yday $1.60 → 09:30 $1.54 -0.48; TMC×1 yday $3.77 → 09:30 $3.72 -0.05; DNN×2 yday $3.19 → 09:30 $3.11 -0.16; HNST×1 yday $4.70 → 09:30 $4.67 -0.03 | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 20 | $60.00 | $2.07 | $-0.12 | $1,246.37 | ▼ -0.12 after sell → book $10,389.73; vs 09:30 mark -2.07 | time-stop after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 27 | $43.56 | $2.09 | $-69.50 | $2,420.40 | ▼ -69.50 after sell → book $10,387.64; vs 09:30 mark -2.09 | time-stop after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 24 | $51.77 | $2.08 | $+23.38 | $3,660.80 | ▲ +23.38 after sell → book $10,385.56; vs 09:30 mark -2.08 | time-stop after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 25 | $49.28 | $2.08 | $-14.65 | $4,890.71 | ▼ -14.65 after sell → book $10,383.47; vs 09:30 mark -2.09 | time-stop after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 106 | $12.66 | $2.34 | $+97.12 | $6,230.34 | ▲ +97.12 after sell → book $10,381.14; vs 09:30 mark -2.33 | time-stop after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 42 | $27.85 | $2.14 | $-83.63 | $7,397.90 | ▼ -83.63 after sell → book $10,379.00; vs 09:30 mark -2.14 | time-stop after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1543 | $1.14 | $20.17 | $+471.89 | $9,136.75 | ▲ +471.89 after sell → book $10,358.83; vs 09:30 mark -20.17 | time-stop after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 53 | $22.16 | $2.17 | $-66.33 | $10,309.06 | ▼ -66.33 after sell → book $10,356.66; vs 09:30 mark -2.17 | time-stop after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,309.06 | ▲ 09:30 equity $10,355.88 vs yday $10,355.74 (+0.14) | 09:30 open · cash $10,309.06 (unchanged overnight, no fees) · equity $10,355.88 vs prior close $10,355.74 (+0.14) because holdings re-marked: MARA×1 yday $8.96 → 09:30 $8.91 -0.05; LDI×13 yday $0.86 → 09:30 $0.88 +0.29; BTBT×8 yday $1.45 → 09:30 $1.42 -0.24; TMC×1 yday $3.92 → 09:30 $3.93 +0.01; DNN×2 yday $3.15 → 09:30 $3.19 +0.08; HNST×1 yday $4.75 → 09:30 $4.80 +0.05 | — |
| 2026-08-19 09:30 ET | **SELL** | `MARA` | 1 | $8.91 | $0.11 | $-0.31 | $10,317.85 | ▼ -0.31 after sell → book $10,355.76; vs 09:30 mark -0.12 | time-stop after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 13 | $0.88 | $0.17 | $-1.08 | $10,329.12 | ▼ -1.08 after sell → book $10,355.59; vs 09:30 mark -0.17 | time-stop after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 8 | $1.42 | $0.16 | $-0.94 | $10,340.32 | ▼ -0.94 after sell → book $10,355.43; vs 09:30 mark -0.16 | time-stop after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,340.32 | ▼ 09:30 equity $10,355.62 vs yday $10,355.75 (-0.13) | 09:30 open · cash $10,340.32 (unchanged overnight, no fees) · equity $10,355.62 vs prior close $10,355.75 (-0.13) because holdings re-marked: TMC×1 yday $3.97 → 09:30 $3.92 -0.05; DNN×2 yday $3.22 → 09:30 $3.20 -0.04; HNST×1 yday $5.02 → 09:30 $4.98 -0.04 | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 1 | $3.92 | $0.06 | $-0.24 | $10,344.18 | ▼ -0.24 after sell → book $10,355.56; vs 09:30 mark -0.06 | time-stop after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 2 | $3.20 | $0.09 | $-0.24 | $10,350.49 | ▼ -0.24 after sell → book $10,355.47; vs 09:30 mark -0.09 | time-stop after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `HNST` | 1 | $4.98 | $0.07 | $+0.05 | $10,355.40 | ▲ +0.05 after sell → book $10,355.40; vs 09:30 mark -0.07 | time-stop after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 62 | $20.55 | $2.18 | — | $9,079.12 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $1294.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $7,802.95 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $1294.42 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 62 | $20.65 | $2.18 | — | $6,520.47 | — | sell at min-hold even if still listed; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $1294.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 224 | $5.77 | $2.89 | — | $5,225.10 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $1294.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 65 | $19.63 | $2.19 | — | $3,946.97 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $1294.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 43 | $29.63 | $2.12 | — | $2,670.76 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $1294.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 739 | $1.75 | $9.53 | — | $1,367.98 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $1294.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $209.64 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $1294.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $209.64 | ▲ 09:30 equity $10,845.87 vs yday $10,569.98 (+275.89) | 09:30 open · cash $209.64 (unchanged overnight, no fees) · equity $10,845.87 vs prior close $10,569.98 (+275.89) because holdings re-marked: AG×62 yday $21.19 → 09:30 $21.90 +44.02; BHP×14 yday $93.63 → 09:30 $95.72 +29.26; CDE×62 yday $21.11 → 09:30 $21.75 +39.68; HDSN×224 yday $5.57 → 09:30 $5.67 +22.40; IAG×65 yday $20.50 → 09:30 $21.17 +43.55; KGC×43 yday $31.43 → 09:30 $32.17 +31.82; NFGC×739 yday $1.75 → 09:30 $1.79 +29.56; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $192.27 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $26.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $169.78 | — | sell at min-hold even if still listed; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $26.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 10 | $2.47 | $0.28 | — | $144.80 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $26.21 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 13 | $1.93 | $0.29 | — | $119.42 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $26.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 19 | $1.32 | $0.31 | — | $94.04 | — | sell at min-hold even if still listed; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $26.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $94.04 | ▲ 09:30 equity $10,974.27 vs yday $10,844.89 (+129.38) | 09:30 open · cash $94.04 (unchanged overnight, no fees) · equity $10,974.27 vs prior close $10,844.89 (+129.38) because holdings re-marked: AG×62 yday $21.09 → 09:30 $21.47 +23.56; BHP×14 yday $97.03 → 09:30 $97.34 +4.34; CDE×62 yday $20.97 → 09:30 $21.26 +17.98; HDSN×224 yday $5.63 → 09:30 $5.69 +13.44; IAG×65 yday $21.14 → 09:30 $21.44 +19.50; KGC×43 yday $32.76 → 09:30 $33.21 +19.35; NFGC×739 yday $1.84 → 09:30 $1.86 +14.78; WPM×8 yday $157.78 → 09:30 $158.96 +9.44; AUPH×1 yday $16.65 → 09:30 $16.60 -0.05; ARCT×2 yday $13.45 → 09:30 $13.26 -0.38; AUTL×10 yday $2.41 → 09:30 $2.36 -0.50; CRDL×13 yday $1.86 → 09:30 $1.87 +0.13; CYPH×19 yday $1.42 → 09:30 $1.83 +7.79 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $94.04 | ▲ 09:30 equity $10,885.73 vs yday $10,815.87 (+69.86) | 09:30 open · cash $94.04 (unchanged overnight, no fees) · equity $10,885.73 vs prior close $10,815.87 (+69.86) because holdings re-marked: AG×62 yday $20.57 → 09:30 $20.73 +9.92; BHP×14 yday $96.66 → 09:30 $95.95 -9.94; CDE×62 yday $20.49 → 09:30 $20.85 +22.32; HDSN×224 yday $5.57 → 09:30 $5.53 -8.96; IAG×65 yday $21.36 → 09:30 $21.63 +17.55; KGC×43 yday $32.47 → 09:30 $32.76 +12.47; NFGC×739 yday $1.90 → 09:30 $1.91 +7.39; WPM×8 yday $158.00 → 09:30 $160.00 +16.00; AUPH×1 yday $16.60 → 09:30 $16.71 +0.11; ARCT×2 yday $13.76 → 09:30 $14.34 +1.16; AUTL×10 yday $2.38 → 09:30 $2.32 -0.60; CRDL×13 yday $1.80 → 09:30 $1.90 +1.30; CYPH×19 yday $1.64 → 09:30 $1.70 +1.14 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 62 | $20.73 | $2.20 | $+6.79 | $1,377.10 | ▲ +6.79 after sell → book $10,883.53; vs 09:30 mark -2.20 | time-stop after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 14 | $95.95 | $2.05 | $+65.08 | $2,718.35 | ▲ +65.08 after sell → book $10,881.48; vs 09:30 mark -2.05 | time-stop after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 62 | $20.85 | $2.20 | $+8.03 | $4,008.85 | ▲ +8.03 after sell → book $10,879.28; vs 09:30 mark -2.20 | time-stop after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 224 | $5.53 | $2.94 | $-59.59 | $5,244.63 | ▼ -59.59 after sell → book $10,876.34; vs 09:30 mark -2.94 | time-stop after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 65 | $21.63 | $2.21 | $+125.61 | $6,648.38 | ▲ +125.61 after sell → book $10,874.14; vs 09:30 mark -2.20 | time-stop after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 43 | $32.76 | $2.14 | $+130.33 | $8,054.92 | ▲ +130.33 after sell → book $10,872.00; vs 09:30 mark -2.14 | time-stop after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 739 | $1.91 | $9.67 | $+99.04 | $9,456.74 | ▲ +99.04 after sell → book $10,862.33; vs 09:30 mark -9.67 | time-stop after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $160.00 | $2.03 | $+119.63 | $10,734.70 | ▲ +119.63 after sell → book $10,860.29; vs 09:30 mark -2.04 | time-stop after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 74 | $24.00 | $2.21 | — | $8,956.49 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.0; leftover $1789.12 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 163 | $10.92 | $2.48 | — | $7,174.05 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+10.4; leftover $1789.12 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 29 | $61.47 | $2.08 | — | $5,389.35 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+9.2; leftover $1789.12 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 216 | $8.28 | $2.79 | — | $3,598.08 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.8; leftover $1789.12 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 342 | $5.23 | $4.41 | — | $1,805.01 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ret5=+10.7; leftover $1789.12 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 4 | $429.24 | $2.00 | — | $86.05 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.1; leftover $1789.12 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $86.05 | ▲ 09:30 equity $10,842.19 vs yday $10,842.19 (-0.00) | 09:30 open · cash $86.05 (unchanged overnight, no fees) · equity $10,842.19 vs prior close $10,842.19 (-0.00) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.71 +0.00; ARCT×2 yday $14.21 → 09:30 $14.21 +0.00; AUTL×10 yday $2.34 → 09:30 $2.34 +0.00; CRDL×13 yday $1.90 → 09:30 $1.90 +0.00; CYPH×19 yday $1.64 → 09:30 $1.64 +0.00; MOS×74 yday $23.75 → 09:30 $23.75 +0.00; OCUL×163 yday $10.92 → 09:30 $10.92 +0.00; INSP×29 yday $61.47 → 09:30 $61.47 +0.00; CRMD×216 yday $8.28 → 09:30 $8.28 +0.00; RZLT×342 yday $5.29 → 09:30 $5.29 +0.00; HCA×4 yday $428.50 → 09:30 $428.50 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $86.05 | ▼ 09:30 equity $10,834.22 vs yday $10,844.33 (-10.11) | 09:30 open · cash $86.05 (unchanged overnight, no fees) · equity $10,834.22 vs prior close $10,844.33 (-10.11) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.60 -0.11; ARCT×2 yday $14.21 → 09:30 $15.35 +2.28; AUTL×10 yday $2.34 → 09:30 $2.41 +0.70; CRDL×13 yday $1.90 → 09:30 $2.03 +1.69; CYPH×19 yday $1.64 → 09:30 $1.60 -0.76; MOS×74 yday $23.75 → 09:30 $24.84 +80.66; OCUL×163 yday $10.92 → 09:30 $10.79 -21.19; INSP×29 yday $61.47 → 09:30 $60.07 -40.60; CRMD×216 yday $8.28 → 09:30 $8.60 +69.12; RZLT×342 yday $5.29 → 09:30 $5.01 -95.76; HCA×4 yday $428.50 → 09:30 $427.50 -4.00 | — |
| 2026-08-27 09:30 ET | **SELL** | `AUPH` | 1 | $16.60 | $0.19 | $-0.96 | $102.46 | ▼ -0.96 after sell → book $10,834.03; vs 09:30 mark -0.19 | time-stop after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 2 | $15.35 | $0.33 | $+7.88 | $132.82 | ▲ +7.88 after sell → book $10,833.69; vs 09:30 mark -0.34 | time-stop after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `AUTL` | 10 | $2.41 | $0.29 | $-1.17 | $156.63 | ▼ -1.17 after sell → book $10,833.40; vs 09:30 mark -0.29 | time-stop after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 13 | $2.03 | $0.32 | $+0.69 | $182.70 | ▲ +0.69 after sell → book $10,833.08; vs 09:30 mark -0.32 | time-stop after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 19 | $1.60 | $0.38 | $+4.63 | $212.72 | ▲ +4.63 after sell → book $10,832.70; vs 09:30 mark -0.38 | time-stop after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 1 | $40.72 | $0.41 | — | $171.59 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.8; leftover $70.91 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 5 | $14.09 | $0.72 | — | $100.42 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.1; leftover $70.91 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 27 | $2.59 | $0.78 | — | $29.71 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.2; leftover $70.91 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29.71 | ▼ 09:30 equity $10,789.26 vs yday $10,794.34 (-5.08) | 09:30 open · cash $29.71 (unchanged overnight, no fees) · equity $10,789.26 vs prior close $10,794.34 (-5.08) because holdings re-marked: MOS×74 yday $24.16 → 09:30 $24.00 -11.84; OCUL×163 yday $10.77 → 09:30 $10.63 -22.82; INSP×29 yday $61.80 → 09:30 $62.10 +8.70; CRMD×216 yday $8.39 → 09:30 $8.49 +21.60; RZLT×342 yday $5.04 → 09:30 $5.07 +10.26; HCA×4 yday $427.16 → 09:30 $424.61 -10.20; RRC×1 yday $41.55 → 09:30 $41.44 -0.11; CRK×5 yday $14.50 → 09:30 $14.42 -0.40; SLI×27 yday $2.61 → 09:30 $2.60 -0.27 | — |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 74 | $24.00 | $2.24 | $-4.45 | $1,803.47 | ▼ -4.45 after sell → book $10,787.02; vs 09:30 mark -2.24 | time-stop after 3 sess (min 3) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 163 | $10.63 | $2.52 | $-52.27 | $3,533.64 | ▼ -52.27 after sell → book $10,784.50; vs 09:30 mark -2.52 | time-stop after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 29 | $62.10 | $2.10 | $+14.09 | $5,332.44 | ▲ +14.09 after sell → book $10,782.40; vs 09:30 mark -2.10 | time-stop after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 216 | $8.49 | $2.84 | $+39.74 | $7,163.44 | ▲ +39.74 after sell → book $10,779.56; vs 09:30 mark -2.84 | time-stop after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 342 | $5.07 | $4.48 | $-63.61 | $8,892.90 | ▼ -63.61 after sell → book $10,775.08; vs 09:30 mark -4.48 | time-stop after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 4 | $424.61 | $2.03 | $-22.55 | $10,589.31 | ▼ -22.55 after sell → book $10,773.05; vs 09:30 mark -2.03 | time-stop after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `MOS` | 440 | $24.00 | $5.68 | — | $23.64 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ret5=+13.0; leftover $10589.31 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.64 | ▼ 09:30 equity $10,655.32 vs yday $10,664.06 (-8.74) | 09:30 open · cash $23.64 (unchanged overnight, no fees) · equity $10,655.32 vs prior close $10,664.06 (-8.74) because holdings re-marked: RRC×1 yday $41.64 → 09:30 $41.11 -0.53; CRK×5 yday $14.62 → 09:30 $14.56 -0.30; SLI×27 yday $2.64 → 09:30 $2.51 -3.51; MOS×440 yday $23.76 → 09:30 $23.75 -4.40 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.64 | ▲ 09:30 equity $10,769.41 vs yday $10,668.94 (+100.47) | 09:30 open · cash $23.64 (unchanged overnight, no fees) · equity $10,769.41 vs prior close $10,668.94 (+100.47) because holdings re-marked: RRC×1 yday $41.78 → 09:30 $41.32 -0.46; CRK×5 yday $14.51 → 09:30 $14.31 -1.00; SLI×27 yday $2.51 → 09:30 $2.70 +5.13; MOS×440 yday $23.78 → 09:30 $24.00 +96.80 | — |
| 2026-09-01 09:30 ET | **SELL** | `RRC` | 1 | $41.32 | $0.44 | $-0.25 | $64.52 | ▼ -0.25 after sell → book $10,768.97; vs 09:30 mark -0.44 | time-stop after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRK` | 5 | $14.31 | $0.75 | $-0.37 | $135.32 | ▼ -0.37 after sell → book $10,768.22; vs 09:30 mark -0.75 | time-stop after 3 sess (min 3) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 27 | $2.70 | $0.83 | $+1.36 | $207.39 | ▲ +1.36 after sell → book $10,767.39; vs 09:30 mark -0.83 | time-stop after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $207.39 | ▼ 09:30 equity $10,740.99 vs yday $10,877.39 (-136.40) | 09:30 open · cash $207.39 (unchanged overnight, no fees) · equity $10,740.99 vs prior close $10,877.39 (-136.40) because holdings re-marked: MOS×440 yday $24.25 → 09:30 $23.94 -136.40 | — |
| 2026-09-02 09:30 ET | **SELL** | `MOS` | 440 | $23.94 | $5.83 | $-37.91 | $10,735.16 | ▼ -37.91 after sell → book $10,735.16; vs 09:30 mark -5.83 | time-stop after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,735.16 | ▲ 09:30 equity $10,735.16 vs yday $10,735.16 (-0.00) | 09:30 open · cash $10,735.16 · no holdings · equity $10,735.16 vs prior close $10,735.16 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 43 | $49.76 | $2.12 | — | $8,593.36 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+10.6; leftover $2147.03 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 51 | $41.31 | $2.14 | — | $6,484.41 | — | sell at min-hold even if still listed; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.1; leftover $2147.03 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 656 | $3.27 | $8.46 | — | $4,330.82 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+13.8; leftover $2147.03 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 278 | $7.70 | $3.59 | — | $2,186.64 | — | sell at min-hold even if still listed; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.7; leftover $2147.03 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 17 | $125.94 | $2.04 | — | $43.62 | — | sell at min-hold even if still listed; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+6.8; leftover $2147.03 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $43.62 | ▲ 09:30 equity $11,372.16 vs yday $11,288.31 (+83.85) | 09:30 open · cash $43.62 (unchanged overnight, no fees) · equity $11,372.16 vs prior close $11,288.31 (+83.85) because holdings re-marked: ATRC×43 yday $52.59 → 09:30 $52.88 +12.47; HRMY×51 yday $42.86 → 09:30 $42.93 +3.57; CABA×656 yday $3.57 → 09:30 $3.63 +39.36; VSTM×278 yday $8.02 → 09:30 $8.03 +2.78; RVTY×17 yday $130.94 → 09:30 $132.45 +25.67 | — |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 1 | $10.41 | $0.11 | — | $33.10 | — | sell at min-hold even if still listed; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ⚪; ret5=+11.1; leftover $10.90 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TLN` | cash | leftover split 12.19 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 12.19 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 12.19 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 12.19 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 12.19 < 1 share @ 57.61 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 7.99 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 7.99 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 7.99 < 1 share @ 202.70 |
| 2026-08-17 | `TGB` | cash | leftover split 7.99 < 1 share @ 8.46 |
| 2026-08-17 | `ELF` | cash | leftover split 7.99 < 1 share @ 90.54 |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `HNST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `HNST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 26.21 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 26.21 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 26.21 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AUPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ARCT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AUTL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CRDL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `MOS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `MOS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ASND` | cash | leftover split 10.90 < 1 share @ 266.94 |
| 2026-09-04 | `OSCR` | cash | leftover split 10.90 < 1 share @ 30.65 |
| 2026-09-04 | `BVS` | cash | leftover split 10.90 < 1 share @ 14.50 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 43 | 2026-09-03 @ $49.76 | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+10.6; leftover $2147.03 |
| `HRMY` | 51 | 2026-09-03 @ $41.31 | sell at min-hold even if still listed; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.1; leftover $2147.03 |
| `CABA` | 656 | 2026-09-03 @ $3.27 | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+13.8; leftover $2147.03 |
| `VSTM` | 278 | 2026-09-03 @ $7.70 | sell at min-hold even if still listed; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.7; leftover $2147.03 |
| `RVTY` | 17 | 2026-09-03 @ $125.94 | sell at min-hold even if still listed; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+6.8; leftover $2147.03 |
| `NVAX` | 1 | 2026-09-04 @ $10.41 | sell at min-hold even if still listed; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ⚪; ret5=+11.1; leftover $10.90 |
