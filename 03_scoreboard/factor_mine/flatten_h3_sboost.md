# Factor mine action — `flatten_h3_sboost`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

Side **long** · universe `flatten` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `both` · S≥+5: sizeup + more names

Cash book **+11.69%** ($11,169) · signal-only (no cash/fees) was +44.29%. Starts YES **16/17**. Fills 81 · skips 132 · realized $+747.30.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `flatten` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8 (S≥+5 may raise this when S-boost is `both`).
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $3.44.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM, VOR | — | $123.82 | $10,195.74 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $123.82 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50 | $10,219.63 | +23.89 | MARA, LDI, BTBT | — | $87.76 | $10,434.38 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50, MARA×1, LDI×14, BTBT×9 | 09:30 open · cash $123.82 (unchanged overnight, no fees) · equity $10,219.63 vs prior close $10,195.74 (+23.89) because holdings re-marked: BTSG×18 yday $60.23 → 09:30 $59.65 -10.44; IREN×24 yday $44.76 → 09:30 $44.09 -16.08; TPG×21 yday $54.62 → 09:30 $55.29 +14.07; TGTX×22 yday $47.94 → 09:30 $47.27 -14.74; SLS×94 yday $12.36 → 09:30 $12.40 +3.76; HIMS×37 yday $28.77 → 09:30 $29.15 +14.06; INO×1371 yday $0.90 → 09:30 $0.93 +41.13; TNDM×47 yday $23.13 → 09:30 $22.92 -9.87; VOR×50 yday $23.29 → 09:30 $23.33 +2.00 |
| 2026-08-17 | +2.25 | $87.76 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50, MARA×1, LDI×14, BTBT×9 | $10,410.12 | -24.26 | TMC, TGB, DNN, HNST | — | $51.48 | $10,512.60 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50, MARA×1, LDI×14, BTBT×9, TMC×2, TGB×1, DNN×3, HNST×2 | 09:30 open · cash $87.76 (unchanged overnight, no fees) · equity $10,410.12 vs prior close $10,434.38 (-24.26) because holdings re-marked: BTSG×18 yday $61.71 → 09:30 $61.69 -0.36; IREN×24 yday $44.06 → 09:30 $45.23 +28.08; TPG×21 yday $53.03 → 09:30 $52.67 -7.56; TGTX×22 yday $48.74 → 09:30 $48.74 +0.00; SLS×94 yday $12.78 → 09:30 $12.78 +0.00; HIMS×37 yday $28.15 → 09:30 $28.14 -0.37; INO×1371 yday $1.09 → 09:30 $1.07 -27.42; TNDM×47 yday $22.72 → 09:30 $22.50 -10.34; VOR×50 yday $23.03 → 09:30 $22.91 -6.00; MARA×1 yday $9.20 → 09:30 $9.22 +0.02; LDI×14 yday $0.90 → 09:30 $0.91 +0.14; BTBT×9 yday $1.57 → 09:30 $1.52 -0.45 |
| 2026-08-18 | -6.20 | $51.48 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50, MARA×1, LDI×14, BTBT×9, TMC×2, TGB×1, DNN×3, HNST×2 | $10,384.26 | -128.34 | — | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM, VOR | $10,279.25 | $10,348.42 | MARA×1, LDI×14, BTBT×9, TMC×2, TGB×1, DNN×3, HNST×2 | 09:30 open · cash $51.48 (unchanged overnight, no fees) · equity $10,384.26 vs prior close $10,512.60 (-128.34) because holdings re-marked: BTSG×18 yday $60.38 → 09:30 $60.00 -6.84; IREN×24 yday $44.90 → 09:30 $43.56 -32.16; TPG×21 yday $51.77 → 09:30 $51.77 +0.00; TGTX×22 yday $49.28 → 09:30 $49.28 +0.00; SLS×94 yday $13.00 → 09:30 $12.66 -31.96; HIMS×37 yday $28.61 → 09:30 $27.85 -28.12; INO×1371 yday $1.15 → 09:30 $1.14 -13.71; TNDM×47 yday $22.25 → 09:30 $22.16 -4.46; VOR×50 yday $23.01 → 09:30 $22.82 -9.50; MARA×1 yday $9.72 → 09:30 $9.36 -0.36; LDI×14 yday $0.88 → 09:30 $0.87 -0.07; BTBT×9 yday $1.60 → 09:30 $1.54 -0.54; TMC×2 yday $3.77 → 09:30 $3.72 -0.10; TGB×1 yday $8.77 → 09:30 $8.55 -0.22; DNN×3 yday $3.19 → 09:30 $3.11 -0.24; HNST×2 yday $4.70 → 09:30 $4.67 -0.06 |
| 2026-08-19 | -7.20 | $10,279.25 | MARA×1, LDI×14, BTBT×9, TMC×2, TGB×1, DNN×3, HNST×2 | $10,348.99 | +0.57 | — | MARA, LDI, BTBT | $10,312.78 | $10,348.89 | TMC×2, TGB×1, DNN×3, HNST×2 | 09:30 open · cash $10,279.25 (unchanged overnight, no fees) · equity $10,348.99 vs prior close $10,348.42 (+0.57) because holdings re-marked: MARA×1 yday $8.96 → 09:30 $8.91 -0.05; LDI×14 yday $0.86 → 09:30 $0.88 +0.31; BTBT×9 yday $1.45 → 09:30 $1.42 -0.27; TMC×2 yday $3.92 → 09:30 $3.93 +0.02; TGB×1 yday $8.36 → 09:30 $8.70 +0.34; DNN×3 yday $3.15 → 09:30 $3.19 +0.12; HNST×2 yday $4.75 → 09:30 $4.80 +0.10 |
| 2026-08-20 | +1.12 | $10,312.78 | TMC×2, TGB×1, DNN×3, HNST×2 | $10,348.53 | -0.36 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | TMC, TGB, DNN, HNST | $202.32 | $10,562.66 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8 | 09:30 open · cash $10,312.78 (unchanged overnight, no fees) · equity $10,348.53 vs prior close $10,348.89 (-0.36) because holdings re-marked: TMC×2 yday $3.97 → 09:30 $3.92 -0.10; TGB×1 yday $8.47 → 09:30 $8.35 -0.12; DNN×3 yday $3.22 → 09:30 $3.20 -0.06; HNST×2 yday $5.02 → 09:30 $4.98 -0.08 |
| 2026-08-21 | +3.25 | $202.32 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8 | $10,838.55 | +275.89 | AUPH, ARCT, AUTL, CRDL, CYPH | — | $86.71 | $10,837.56 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19 | 09:30 open · cash $202.32 (unchanged overnight, no fees) · equity $10,838.55 vs prior close $10,562.66 (+275.89) because holdings re-marked: AG×62 yday $21.19 → 09:30 $21.90 +44.02; BHP×14 yday $93.63 → 09:30 $95.72 +29.26; CDE×62 yday $21.11 → 09:30 $21.75 +39.68; HDSN×224 yday $5.57 → 09:30 $5.67 +22.40; IAG×65 yday $20.50 → 09:30 $21.17 +43.55; KGC×43 yday $31.43 → 09:30 $32.17 +31.82; NFGC×739 yday $1.75 → 09:30 $1.79 +29.56; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 |
| 2026-08-24 | -5.17 | $86.71 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19 | $10,966.94 | +129.38 | — | — | $86.71 | $10,808.54 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19 | 09:30 open · cash $86.71 (unchanged overnight, no fees) · equity $10,966.94 vs prior close $10,837.56 (+129.38) because holdings re-marked: AG×62 yday $21.09 → 09:30 $21.47 +23.56; BHP×14 yday $97.03 → 09:30 $97.34 +4.34; CDE×62 yday $20.97 → 09:30 $21.26 +17.98; HDSN×224 yday $5.63 → 09:30 $5.69 +13.44; IAG×65 yday $21.14 → 09:30 $21.44 +19.50; KGC×43 yday $32.76 → 09:30 $33.21 +19.35; NFGC×739 yday $1.84 → 09:30 $1.86 +14.78; WPM×8 yday $157.78 → 09:30 $158.96 +9.44; AUPH×1 yday $16.65 → 09:30 $16.60 -0.05; ARCT×2 yday $13.45 → 09:30 $13.26 -0.38; AUTL×10 yday $2.41 → 09:30 $2.36 -0.50; CRDL×13 yday $1.86 → 09:30 $1.87 +0.13; CYPH×19 yday $1.42 → 09:30 $1.83 +7.79 |
| 2026-08-25 | +1.80 | $86.71 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19 | $10,878.40 | +69.86 | MOS, OCUL, INSP, CRMD, RZLT, HCA | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $92.26 | $10,834.83 | AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19, MOS×74, OCUL×163, INSP×29, CRMD×215, RZLT×341, HCA×4 | 09:30 open · cash $86.71 (unchanged overnight, no fees) · equity $10,878.40 vs prior close $10,808.54 (+69.86) because holdings re-marked: AG×62 yday $20.57 → 09:30 $20.73 +9.92; BHP×14 yday $96.66 → 09:30 $95.95 -9.94; CDE×62 yday $20.49 → 09:30 $20.85 +22.32; HDSN×224 yday $5.57 → 09:30 $5.53 -8.96; IAG×65 yday $21.36 → 09:30 $21.63 +17.55; KGC×43 yday $32.47 → 09:30 $32.76 +12.47; NFGC×739 yday $1.90 → 09:30 $1.91 +7.39; WPM×8 yday $158.00 → 09:30 $160.00 +16.00; AUPH×1 yday $16.60 → 09:30 $16.71 +0.11; ARCT×2 yday $13.76 → 09:30 $14.34 +1.16; AUTL×10 yday $2.38 → 09:30 $2.32 -0.60; CRDL×13 yday $1.80 → 09:30 $1.90 +1.30; CYPH×19 yday $1.64 → 09:30 $1.70 +1.14 |
| 2026-08-26 | +2.02 | $92.26 | AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19, MOS×74, OCUL×163, INSP×29, CRMD×215, RZLT×341, HCA×4 | $10,834.83 | -0.00 | — | — | $92.26 | $10,837.03 | AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19, MOS×74, OCUL×163, INSP×29, CRMD×215, RZLT×341, HCA×4 | 09:30 open · cash $92.26 (unchanged overnight, no fees) · equity $10,834.83 vs prior close $10,834.83 (-0.00) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.71 +0.00; ARCT×2 yday $14.21 → 09:30 $14.21 +0.00; AUTL×10 yday $2.34 → 09:30 $2.34 +0.00; CRDL×13 yday $1.90 → 09:30 $1.90 +0.00; CYPH×19 yday $1.64 → 09:30 $1.64 +0.00; MOS×74 yday $23.75 → 09:30 $23.75 +0.00; OCUL×163 yday $10.92 → 09:30 $10.92 +0.00; INSP×29 yday $61.47 → 09:30 $61.47 +0.00; CRMD×215 yday $8.28 → 09:30 $8.28 +0.00; RZLT×341 yday $5.29 → 09:30 $5.29 +0.00; HCA×4 yday $428.50 → 09:30 $428.50 +0.00 |
| 2026-08-27 | — | $92.26 | AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19, MOS×74, OCUL×163, INSP×29, CRMD×215, RZLT×341, HCA×4 | $10,826.82 | -10.21 | RRC, CRK, SLI | AUPH, ARCT, AUTL, CRDL, CYPH | $33.30 | $10,787.11 | MOS×74, OCUL×163, INSP×29, CRMD×215, RZLT×341, HCA×4, RRC×1, CRK×5, SLI×28 | 09:30 open · cash $92.26 (unchanged overnight, no fees) · equity $10,826.82 vs prior close $10,837.03 (-10.21) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.60 -0.11; ARCT×2 yday $14.21 → 09:30 $15.35 +2.28; AUTL×10 yday $2.34 → 09:30 $2.41 +0.70; CRDL×13 yday $1.90 → 09:30 $2.03 +1.69; CYPH×19 yday $1.64 → 09:30 $1.60 -0.76; MOS×74 yday $23.75 → 09:30 $24.84 +80.66; OCUL×163 yday $10.92 → 09:30 $10.79 -21.19; INSP×29 yday $61.47 → 09:30 $60.07 -40.60; CRMD×215 yday $8.28 → 09:30 $8.60 +68.80; RZLT×341 yday $5.29 → 09:30 $5.01 -95.48; HCA×4 yday $428.50 → 09:30 $427.50 -4.00 |
| 2026-08-28 | +0.75 | $33.30 | MOS×74, OCUL×163, INSP×29, CRMD×215, RZLT×341, HCA×4, RRC×1, CRK×5, SLI×28 | $10,781.89 | -5.22 | — | OCUL, INSP, CRMD, RZLT, HCA | $8,805.61 | $10,752.51 | MOS×74, RRC×1, CRK×5, SLI×28 | 09:30 open · cash $33.30 (unchanged overnight, no fees) · equity $10,781.89 vs prior close $10,787.11 (-5.22) because holdings re-marked: MOS×74 yday $24.16 → 09:30 $24.00 -11.84; OCUL×163 yday $10.77 → 09:30 $10.63 -22.82; INSP×29 yday $61.80 → 09:30 $62.10 +8.70; CRMD×215 yday $8.39 → 09:30 $8.49 +21.50; RZLT×341 yday $5.04 → 09:30 $5.07 +10.23; HCA×4 yday $427.16 → 09:30 $424.61 -10.20; RRC×1 yday $41.55 → 09:30 $41.44 -0.11; CRK×5 yday $14.50 → 09:30 $14.42 -0.40; SLI×28 yday $2.61 → 09:30 $2.60 -0.28 |
| 2026-08-31 | -5.85 | $8,805.61 | MOS×74, RRC×1, CRK×5, SLI×28 | $10,747.30 | -5.21 | — | MOS | $10,560.87 | $10,745.48 | RRC×1, CRK×5, SLI×28 | 09:30 open · cash $8,805.61 (unchanged overnight, no fees) · equity $10,747.30 vs prior close $10,752.51 (-5.21) because holdings re-marked: MOS×74 yday $23.76 → 09:30 $23.75 -0.74; RRC×1 yday $41.64 → 09:30 $41.11 -0.53; CRK×5 yday $14.62 → 09:30 $14.56 -0.30; SLI×28 yday $2.64 → 09:30 $2.51 -3.64 |
| 2026-09-01 | -6.30 | $10,560.87 | RRC×1, CRK×5, SLI×28 | $10,749.34 | +3.86 | — | RRC, CRK, SLI | $10,747.29 | $10,747.29 | — | 09:30 open · cash $10,560.87 (unchanged overnight, no fees) · equity $10,749.34 vs prior close $10,745.48 (+3.86) because holdings re-marked: RRC×1 yday $41.78 → 09:30 $41.32 -0.46; CRK×5 yday $14.51 → 09:30 $14.31 -1.00; SLI×28 yday $2.51 → 09:30 $2.70 +5.32 |
| 2026-09-02 | -3.83 | $10,747.29 | — | $10,747.29 | +0.00 | — | — | $10,747.29 | $10,747.29 | — | 09:30 open · cash $10,747.29 · no holdings · equity $10,747.29 vs prior close $10,747.29 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-03 | -0.90 | $10,747.29 | — | $10,747.29 | +0.00 | ATRC, HRMY, CABA, VSTM, RVTY | — | $3.44 | $11,302.58 | ATRC×43, HRMY×52, CABA×657, VSTM×279, RVTY×17 | 09:30 open · cash $10,747.29 · no holdings · equity $10,747.29 vs prior close $10,747.29 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $3.44 | ATRC×43, HRMY×52, CABA×657, VSTM×279, RVTY×17 | $11,386.57 | +83.99 | — | — | $3.44 | $11,169.43 | ATRC×43, HRMY×52, CABA×657, VSTM×279, RVTY×17 | 09:30 open · cash $3.44 (unchanged overnight, no fees) · equity $11,386.57 vs prior close $11,302.58 (+83.99) because holdings re-marked: ATRC×43 yday $52.59 → 09:30 $52.88 +12.47; HRMY×52 yday $42.86 → 09:30 $42.93 +3.64; CABA×657 yday $3.57 → 09:30 $3.63 +39.42; VSTM×279 yday $8.02 → 09:30 $8.03 +2.79; RVTY×17 yday $130.94 → 09:30 $132.45 +25.67 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 18 | $59.80 | $2.04 | — | $8,921.56 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 24 | $45.98 | $2.06 | — | $7,815.97 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+12.3; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 21 | $50.62 | $2.05 | — | $6,750.83 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+6.2; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 22 | $49.70 | $2.06 | — | $5,655.38 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 94 | $11.70 | $2.27 | — | $4,553.31 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 37 | $29.74 | $2.10 | — | $3,450.82 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1371 | $0.81 | $15.22 | — | $2,325.10 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.2; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 47 | $23.33 | $2.13 | — | $1,226.46 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+19.7; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 50 | $22.01 | $2.14 | — | $123.82 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $123.82 | ▲ 09:30 equity $10,219.63 vs yday $10,195.74 (+23.89) | 09:30 open · cash $123.82 (unchanged overnight, no fees) · equity $10,219.63 vs prior close $10,195.74 (+23.89) because holdings re-marked: BTSG×18 yday $60.23 → 09:30 $59.65 -10.44; IREN×24 yday $44.76 → 09:30 $44.09 -16.08; TPG×21 yday $54.62 → 09:30 $55.29 +14.07; TGTX×22 yday $47.94 → 09:30 $47.27 -14.74; SLS×94 yday $12.36 → 09:30 $12.40 +3.76; HIMS×37 yday $28.77 → 09:30 $29.15 +14.06; INO×1371 yday $0.90 → 09:30 $0.93 +41.13; TNDM×47 yday $23.13 → 09:30 $22.92 -9.87; VOR×50 yday $23.29 → 09:30 $23.33 +2.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $114.71 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-13.5; leftover $13.76 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 14 | $0.94 | $0.17 | — | $101.42 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.5; leftover $13.76 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 9 | $1.50 | $0.16 | — | $87.76 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $13.76 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $87.76 | ▼ 09:30 equity $10,410.12 vs yday $10,434.38 (-24.26) | 09:30 open · cash $87.76 (unchanged overnight, no fees) · equity $10,410.12 vs prior close $10,434.38 (-24.26) because holdings re-marked: BTSG×18 yday $61.71 → 09:30 $61.69 -0.36; IREN×24 yday $44.06 → 09:30 $45.23 +28.08; TPG×21 yday $53.03 → 09:30 $52.67 -7.56; TGTX×22 yday $48.74 → 09:30 $48.74 +0.00; SLS×94 yday $12.78 → 09:30 $12.78 +0.00; HIMS×37 yday $28.15 → 09:30 $28.14 -0.37; INO×1371 yday $1.09 → 09:30 $1.07 -27.42; TNDM×47 yday $22.72 → 09:30 $22.50 -10.34; VOR×50 yday $23.03 → 09:30 $22.91 -6.00; MARA×1 yday $9.20 → 09:30 $9.22 +0.02; LDI×14 yday $0.90 → 09:30 $0.91 +0.14; BTBT×9 yday $1.57 → 09:30 $1.52 -0.45 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 2 | $4.05 | $0.09 | — | $79.57 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; leftover $10.97 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 1 | $8.46 | $0.09 | — | $71.02 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.4; leftover $10.97 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 3 | $3.24 | $0.11 | — | $61.20 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; leftover $10.97 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 2 | $4.81 | $0.10 | — | $51.48 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-11.4; leftover $10.97 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.48 | ▼ 09:30 equity $10,384.26 vs yday $10,512.60 (-128.34) | 09:30 open · cash $51.48 (unchanged overnight, no fees) · equity $10,384.26 vs prior close $10,512.60 (-128.34) because holdings re-marked: BTSG×18 yday $60.38 → 09:30 $60.00 -6.84; IREN×24 yday $44.90 → 09:30 $43.56 -32.16; TPG×21 yday $51.77 → 09:30 $51.77 +0.00; TGTX×22 yday $49.28 → 09:30 $49.28 +0.00; SLS×94 yday $13.00 → 09:30 $12.66 -31.96; HIMS×37 yday $28.61 → 09:30 $27.85 -28.12; INO×1371 yday $1.15 → 09:30 $1.14 -13.71; TNDM×47 yday $22.25 → 09:30 $22.16 -4.46; VOR×50 yday $23.01 → 09:30 $22.82 -9.50; MARA×1 yday $9.72 → 09:30 $9.36 -0.36; LDI×14 yday $0.88 → 09:30 $0.87 -0.07; BTBT×9 yday $1.60 → 09:30 $1.54 -0.54; TMC×2 yday $3.77 → 09:30 $3.72 -0.10; TGB×1 yday $8.77 → 09:30 $8.55 -0.22; DNN×3 yday $3.19 → 09:30 $3.11 -0.24; HNST×2 yday $4.70 → 09:30 $4.67 -0.06 | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 18 | $60.00 | $2.06 | $-0.51 | $1,129.41 | ▼ -0.51 after sell → book $10,382.19; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 24 | $43.56 | $2.08 | $-62.22 | $2,172.77 | ▼ -62.22 after sell → book $10,380.11; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 21 | $51.77 | $2.07 | $+19.96 | $3,257.87 | ▲ +19.96 after sell → book $10,378.04; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 22 | $49.28 | $2.08 | $-13.37 | $4,339.95 | ▼ -13.37 after sell → book $10,375.96; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 94 | $12.66 | $2.30 | $+85.67 | $5,527.69 | ▲ +85.67 after sell → book $10,373.66; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 37 | $27.85 | $2.12 | $-74.15 | $6,556.02 | ▼ -74.15 after sell → book $10,371.54; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1371 | $1.14 | $17.93 | $+419.29 | $8,101.04 | ▲ +419.29 after sell → book $10,353.62; vs 09:30 mark -17.92 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 47 | $22.16 | $2.15 | $-59.27 | $9,140.41 | ▼ -59.27 after sell → book $10,351.47; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 50 | $22.82 | $2.16 | $+36.20 | $10,279.25 | ▲ +36.20 after sell → book $10,349.31; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,279.25 | ▲ 09:30 equity $10,348.99 vs yday $10,348.42 (+0.57) | 09:30 open · cash $10,279.25 (unchanged overnight, no fees) · equity $10,348.99 vs prior close $10,348.42 (+0.57) because holdings re-marked: MARA×1 yday $8.96 → 09:30 $8.91 -0.05; LDI×14 yday $0.86 → 09:30 $0.88 +0.31; BTBT×9 yday $1.45 → 09:30 $1.42 -0.27; TMC×2 yday $3.92 → 09:30 $3.93 +0.02; TGB×1 yday $8.36 → 09:30 $8.70 +0.34; DNN×3 yday $3.15 → 09:30 $3.19 +0.12; HNST×2 yday $4.75 → 09:30 $4.80 +0.10 | — |
| 2026-08-19 09:30 ET | **SELL** | `MARA` | 1 | $8.91 | $0.11 | $-0.31 | $10,288.04 | ▼ -0.31 after sell → book $10,348.87; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 14 | $0.88 | $0.19 | $-1.16 | $10,300.18 | ▼ -1.16 after sell → book $10,348.69; vs 09:30 mark -0.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 9 | $1.42 | $0.17 | $-1.06 | $10,312.78 | ▼ -1.06 after sell → book $10,348.51; vs 09:30 mark -0.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,312.78 | ▼ 09:30 equity $10,348.53 vs yday $10,348.89 (-0.36) | 09:30 open · cash $10,312.78 (unchanged overnight, no fees) · equity $10,348.53 vs prior close $10,348.89 (-0.36) because holdings re-marked: TMC×2 yday $3.97 → 09:30 $3.92 -0.10; TGB×1 yday $8.47 → 09:30 $8.35 -0.12; DNN×3 yday $3.22 → 09:30 $3.20 -0.06; HNST×2 yday $5.02 → 09:30 $4.98 -0.08 | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 2 | $3.92 | $0.10 | $-0.45 | $10,320.52 | ▼ -0.45 after sell → book $10,348.43; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGB` | 1 | $8.35 | $0.11 | $-0.30 | $10,328.76 | ▼ -0.30 after sell → book $10,348.32; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 3 | $3.20 | $0.12 | $-0.35 | $10,338.24 | ▼ -0.35 after sell → book $10,348.20; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `HNST` | 2 | $4.98 | $0.13 | $+0.11 | $10,348.07 | ▲ +0.11 after sell → book $10,348.07; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 62 | $20.55 | $2.18 | — | $9,071.80 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $1293.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $7,795.62 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $1293.51 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 62 | $20.65 | $2.18 | — | $6,513.15 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $1293.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 224 | $5.77 | $2.89 | — | $5,217.78 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $1293.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 65 | $19.63 | $2.19 | — | $3,939.64 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $1293.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 43 | $29.63 | $2.12 | — | $2,663.43 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $1293.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 739 | $1.75 | $9.53 | — | $1,360.65 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $1293.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $202.32 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $1293.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $202.32 | ▲ 09:30 equity $10,838.55 vs yday $10,562.66 (+275.89) | 09:30 open · cash $202.32 (unchanged overnight, no fees) · equity $10,838.55 vs prior close $10,562.66 (+275.89) because holdings re-marked: AG×62 yday $21.19 → 09:30 $21.90 +44.02; BHP×14 yday $93.63 → 09:30 $95.72 +29.26; CDE×62 yday $21.11 → 09:30 $21.75 +39.68; HDSN×224 yday $5.57 → 09:30 $5.67 +22.40; IAG×65 yday $20.50 → 09:30 $21.17 +43.55; KGC×43 yday $31.43 → 09:30 $32.17 +31.82; NFGC×739 yday $1.75 → 09:30 $1.79 +29.56; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $184.94 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $25.29 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $162.45 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $25.29 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 10 | $2.47 | $0.28 | — | $137.48 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $25.29 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 13 | $1.93 | $0.29 | — | $112.10 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $25.29 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 19 | $1.32 | $0.31 | — | $86.71 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $25.29 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $86.71 | ▲ 09:30 equity $10,966.94 vs yday $10,837.56 (+129.38) | 09:30 open · cash $86.71 (unchanged overnight, no fees) · equity $10,966.94 vs prior close $10,837.56 (+129.38) because holdings re-marked: AG×62 yday $21.09 → 09:30 $21.47 +23.56; BHP×14 yday $97.03 → 09:30 $97.34 +4.34; CDE×62 yday $20.97 → 09:30 $21.26 +17.98; HDSN×224 yday $5.63 → 09:30 $5.69 +13.44; IAG×65 yday $21.14 → 09:30 $21.44 +19.50; KGC×43 yday $32.76 → 09:30 $33.21 +19.35; NFGC×739 yday $1.84 → 09:30 $1.86 +14.78; WPM×8 yday $157.78 → 09:30 $158.96 +9.44; AUPH×1 yday $16.65 → 09:30 $16.60 -0.05; ARCT×2 yday $13.45 → 09:30 $13.26 -0.38; AUTL×10 yday $2.41 → 09:30 $2.36 -0.50; CRDL×13 yday $1.86 → 09:30 $1.87 +0.13; CYPH×19 yday $1.42 → 09:30 $1.83 +7.79 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $86.71 | ▲ 09:30 equity $10,878.40 vs yday $10,808.54 (+69.86) | 09:30 open · cash $86.71 (unchanged overnight, no fees) · equity $10,878.40 vs prior close $10,808.54 (+69.86) because holdings re-marked: AG×62 yday $20.57 → 09:30 $20.73 +9.92; BHP×14 yday $96.66 → 09:30 $95.95 -9.94; CDE×62 yday $20.49 → 09:30 $20.85 +22.32; HDSN×224 yday $5.57 → 09:30 $5.53 -8.96; IAG×65 yday $21.36 → 09:30 $21.63 +17.55; KGC×43 yday $32.47 → 09:30 $32.76 +12.47; NFGC×739 yday $1.90 → 09:30 $1.91 +7.39; WPM×8 yday $158.00 → 09:30 $160.00 +16.00; AUPH×1 yday $16.60 → 09:30 $16.71 +0.11; ARCT×2 yday $13.76 → 09:30 $14.34 +1.16; AUTL×10 yday $2.38 → 09:30 $2.32 -0.60; CRDL×13 yday $1.80 → 09:30 $1.90 +1.30; CYPH×19 yday $1.64 → 09:30 $1.70 +1.14 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 62 | $20.73 | $2.20 | $+6.79 | $1,369.77 | ▲ +6.79 after sell → book $10,876.20; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 14 | $95.95 | $2.05 | $+65.08 | $2,711.02 | ▲ +65.08 after sell → book $10,874.15; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 62 | $20.85 | $2.20 | $+8.03 | $4,001.52 | ▲ +8.03 after sell → book $10,871.95; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 224 | $5.53 | $2.94 | $-59.59 | $5,237.31 | ▼ -59.59 after sell → book $10,869.02; vs 09:30 mark -2.93 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 65 | $21.63 | $2.21 | $+125.61 | $6,641.05 | ▲ +125.61 after sell → book $10,866.81; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 43 | $32.76 | $2.14 | $+130.33 | $8,047.59 | ▲ +130.33 after sell → book $10,864.67; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 739 | $1.91 | $9.67 | $+99.04 | $9,449.41 | ▲ +99.04 after sell → book $10,855.00; vs 09:30 mark -9.67 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $160.00 | $2.03 | $+119.63 | $10,727.38 | ▲ +119.63 after sell → book $10,852.97; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 74 | $24.00 | $2.21 | — | $8,949.17 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.0; leftover $1787.90 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 163 | $10.92 | $2.48 | — | $7,166.73 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+10.4; leftover $1787.90 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 29 | $61.47 | $2.08 | — | $5,382.02 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+9.2; leftover $1787.90 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 215 | $8.28 | $2.77 | — | $3,599.05 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.8; leftover $1787.90 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 341 | $5.23 | $4.40 | — | $1,811.22 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+10.7; leftover $1787.90 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 4 | $429.24 | $2.00 | — | $92.26 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.1; leftover $1787.90 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $92.26 | ▲ 09:30 equity $10,834.83 vs yday $10,834.83 (-0.00) | 09:30 open · cash $92.26 (unchanged overnight, no fees) · equity $10,834.83 vs prior close $10,834.83 (-0.00) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.71 +0.00; ARCT×2 yday $14.21 → 09:30 $14.21 +0.00; AUTL×10 yday $2.34 → 09:30 $2.34 +0.00; CRDL×13 yday $1.90 → 09:30 $1.90 +0.00; CYPH×19 yday $1.64 → 09:30 $1.64 +0.00; MOS×74 yday $23.75 → 09:30 $23.75 +0.00; OCUL×163 yday $10.92 → 09:30 $10.92 +0.00; INSP×29 yday $61.47 → 09:30 $61.47 +0.00; CRMD×215 yday $8.28 → 09:30 $8.28 +0.00; RZLT×341 yday $5.29 → 09:30 $5.29 +0.00; HCA×4 yday $428.50 → 09:30 $428.50 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $92.26 | ▼ 09:30 equity $10,826.82 vs yday $10,837.03 (-10.21) | 09:30 open · cash $92.26 (unchanged overnight, no fees) · equity $10,826.82 vs prior close $10,837.03 (-10.21) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.60 -0.11; ARCT×2 yday $14.21 → 09:30 $15.35 +2.28; AUTL×10 yday $2.34 → 09:30 $2.41 +0.70; CRDL×13 yday $1.90 → 09:30 $2.03 +1.69; CYPH×19 yday $1.64 → 09:30 $1.60 -0.76; MOS×74 yday $23.75 → 09:30 $24.84 +80.66; OCUL×163 yday $10.92 → 09:30 $10.79 -21.19; INSP×29 yday $61.47 → 09:30 $60.07 -40.60; CRMD×215 yday $8.28 → 09:30 $8.60 +68.80; RZLT×341 yday $5.29 → 09:30 $5.01 -95.48; HCA×4 yday $428.50 → 09:30 $427.50 -4.00 | — |
| 2026-08-27 09:30 ET | **SELL** | `AUPH` | 1 | $16.60 | $0.19 | $-0.96 | $108.67 | ▼ -0.96 after sell → book $10,826.63; vs 09:30 mark -0.19 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 2 | $15.35 | $0.33 | $+7.88 | $139.03 | ▲ +7.88 after sell → book $10,826.29; vs 09:30 mark -0.34 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `AUTL` | 10 | $2.41 | $0.29 | $-1.17 | $162.84 | ▼ -1.17 after sell → book $10,826.00; vs 09:30 mark -0.29 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 13 | $2.03 | $0.32 | $+0.69 | $188.91 | ▲ +0.69 after sell → book $10,825.68; vs 09:30 mark -0.32 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 19 | $1.60 | $0.38 | $+4.63 | $218.93 | ▲ +4.63 after sell → book $10,825.30; vs 09:30 mark -0.38 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 1 | $40.72 | $0.41 | — | $177.80 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.8; leftover $72.98 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 5 | $14.09 | $0.72 | — | $106.63 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.1; leftover $72.98 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 28 | $2.59 | $0.81 | — | $33.30 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.2; leftover $72.98 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $33.30 | ▼ 09:30 equity $10,781.89 vs yday $10,787.11 (-5.22) | 09:30 open · cash $33.30 (unchanged overnight, no fees) · equity $10,781.89 vs prior close $10,787.11 (-5.22) because holdings re-marked: MOS×74 yday $24.16 → 09:30 $24.00 -11.84; OCUL×163 yday $10.77 → 09:30 $10.63 -22.82; INSP×29 yday $61.80 → 09:30 $62.10 +8.70; CRMD×215 yday $8.39 → 09:30 $8.49 +21.50; RZLT×341 yday $5.04 → 09:30 $5.07 +10.23; HCA×4 yday $427.16 → 09:30 $424.61 -10.20; RRC×1 yday $41.55 → 09:30 $41.44 -0.11; CRK×5 yday $14.50 → 09:30 $14.42 -0.40; SLI×28 yday $2.61 → 09:30 $2.60 -0.28 | — |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 163 | $10.63 | $2.52 | $-52.27 | $1,763.47 | ▼ -52.27 after sell → book $10,779.37; vs 09:30 mark -2.52 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 29 | $62.10 | $2.10 | $+14.09 | $3,562.27 | ▲ +14.09 after sell → book $10,777.27; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 215 | $8.49 | $2.82 | $+39.55 | $5,384.79 | ▲ +39.55 after sell → book $10,774.44; vs 09:30 mark -2.83 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 341 | $5.07 | $4.47 | $-63.43 | $7,109.20 | ▼ -63.43 after sell → book $10,769.98; vs 09:30 mark -4.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 4 | $424.61 | $2.03 | $-22.55 | $8,805.61 | ▼ -22.55 after sell → book $10,767.95; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,805.61 | ▼ 09:30 equity $10,747.30 vs yday $10,752.51 (-5.21) | 09:30 open · cash $8,805.61 (unchanged overnight, no fees) · equity $10,747.30 vs prior close $10,752.51 (-5.21) because holdings re-marked: MOS×74 yday $23.76 → 09:30 $23.75 -0.74; RRC×1 yday $41.64 → 09:30 $41.11 -0.53; CRK×5 yday $14.62 → 09:30 $14.56 -0.30; SLI×28 yday $2.64 → 09:30 $2.51 -3.64 | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 74 | $23.75 | $2.24 | $-22.95 | $10,560.87 | ▼ -22.95 after sell → book $10,745.06; vs 09:30 mark -2.24 | dropped from list after 4 sess (min 3) | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,560.87 | ▲ 09:30 equity $10,749.34 vs yday $10,745.48 (+3.86) | 09:30 open · cash $10,560.87 (unchanged overnight, no fees) · equity $10,749.34 vs prior close $10,745.48 (+3.86) because holdings re-marked: RRC×1 yday $41.78 → 09:30 $41.32 -0.46; CRK×5 yday $14.51 → 09:30 $14.31 -1.00; SLI×28 yday $2.51 → 09:30 $2.70 +5.32 | — |
| 2026-09-01 09:30 ET | **SELL** | `RRC` | 1 | $41.32 | $0.44 | $-0.25 | $10,601.76 | ▼ -0.25 after sell → book $10,748.91; vs 09:30 mark -0.43 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRK` | 5 | $14.31 | $0.75 | $-0.37 | $10,672.55 | ▼ -0.37 after sell → book $10,748.15; vs 09:30 mark -0.76 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 28 | $2.70 | $0.86 | $+1.41 | $10,747.29 | ▲ +1.41 after sell → book $10,747.29; vs 09:30 mark -0.86 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,747.29 | ▲ 09:30 equity $10,747.29 vs yday $10,747.29 (+0.00) | 09:30 open · cash $10,747.29 · no holdings · equity $10,747.29 vs prior close $10,747.29 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,747.29 | ▲ 09:30 equity $10,747.29 vs yday $10,747.29 (+0.00) | 09:30 open · cash $10,747.29 · no holdings · equity $10,747.29 vs prior close $10,747.29 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 43 | $49.76 | $2.12 | — | $8,605.50 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+10.6; leftover $2149.46 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 52 | $41.31 | $2.15 | — | $6,455.23 | — | S≥+5: sizeup + more names; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.1; leftover $2149.46 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 657 | $3.27 | $8.48 | — | $4,298.36 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+13.8; leftover $2149.46 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 279 | $7.70 | $3.60 | — | $2,146.47 | — | S≥+5: sizeup + more names; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.7; leftover $2149.46 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 17 | $125.94 | $2.04 | — | $3.44 | — | S≥+5: sizeup + more names; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+6.8; leftover $2149.46 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.44 | ▲ 09:30 equity $11,386.57 vs yday $11,302.58 (+83.99) | 09:30 open · cash $3.44 (unchanged overnight, no fees) · equity $11,386.57 vs prior close $11,302.58 (+83.99) because holdings re-marked: ATRC×43 yday $52.59 → 09:30 $52.88 +12.47; HRMY×52 yday $42.86 → 09:30 $42.93 +3.64; CABA×657 yday $3.57 → 09:30 $3.63 +39.42; VSTM×279 yday $8.02 → 09:30 $8.03 +2.79; RVTY×17 yday $130.94 → 09:30 $132.45 +25.67 | — |

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
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TLN` | cash | leftover split 13.76 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 13.76 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 13.76 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 13.76 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 13.76 < 1 share @ 57.61 |
| 2026-08-14 | `BETR` | cash | leftover split 13.76 < 1 share @ 14.80 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 10.97 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 10.97 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 10.97 < 1 share @ 202.70 |
| 2026-08-17 | `ELF` | cash | leftover split 10.97 < 1 share @ 90.54 |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
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
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-08-21 | `AU` | cash | leftover split 25.29 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 25.29 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 25.29 < 1 share @ 59.72 |
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
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
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
| 2026-09-04 | `ASND` | cash | leftover split 0.86 < 1 share @ 266.94 |
| 2026-09-04 | `OSCR` | cash | leftover split 0.86 < 1 share @ 30.65 |
| 2026-09-04 | `NVAX` | cash | leftover split 0.86 < 1 share @ 10.41 |
| 2026-09-04 | `BVS` | cash | leftover split 0.86 < 1 share @ 14.50 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 43 | 2026-09-03 @ $49.76 | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+10.6; leftover $2149.46 |
| `HRMY` | 52 | 2026-09-03 @ $41.31 | S≥+5: sizeup + more names; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.1; leftover $2149.46 |
| `CABA` | 657 | 2026-09-03 @ $3.27 | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+13.8; leftover $2149.46 |
| `VSTM` | 279 | 2026-09-03 @ $7.70 | S≥+5: sizeup + more names; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.7; leftover $2149.46 |
| `RVTY` | 17 | 2026-09-03 @ $125.94 | S≥+5: sizeup + more names; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+6.8; leftover $2149.46 |
