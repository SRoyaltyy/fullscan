# Factor mine action — `union_last_red_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ last_red, no 🚨

Cash book **+2.39%** ($10,239) · signal-only (no cash/fees) was +21.46%. Starts YES **8/17**. Fills 84 · skips 156 · realized $+306.03.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `last_red=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $28.32.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | TGTX, SLS, HIMS, VOR | — | $28.15 | $10,106.28 | TGTX×50, SLS×213, HIMS×84, VOR×113 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $28.15 | TGTX×50, SLS×213, HIMS×84, VOR×113 | $10,117.74 | +11.46 | — | — | $28.15 | $10,154.28 | TGTX×50, SLS×213, HIMS×84, VOR×113 | 09:30 open · cash $28.15 (unchanged overnight, no fees) · equity $10,117.74 vs prior close $10,106.28 (+11.46) because holdings re-marked: TGTX×50 yday $47.94 → 09:30 $47.27 -33.50; SLS×213 yday $12.36 → 09:30 $12.40 +8.52; HIMS×84 yday $28.77 → 09:30 $29.15 +31.92; VOR×113 yday $23.29 → 09:30 $23.33 +4.52 |
| 2026-08-17 | +2.25 | $28.15 | TGTX×50, SLS×213, HIMS×84, VOR×113 | $10,139.88 | -14.40 | DNN, INV, KLC | — | $18.95 | $10,263.84 | TGTX×50, SLS×213, HIMS×84, VOR×113, DNN×1, INV×2, KLC×1 | 09:30 open · cash $28.15 (unchanged overnight, no fees) · equity $10,139.88 vs prior close $10,154.28 (-14.40) because holdings re-marked: TGTX×50 yday $48.74 → 09:30 $48.74 +0.00; SLS×213 yday $12.78 → 09:30 $12.78 +0.00; HIMS×84 yday $28.15 → 09:30 $28.14 -0.84; VOR×113 yday $23.03 → 09:30 $22.91 -13.56 |
| 2026-08-18 | -6.20 | $18.95 | TGTX×50, SLS×213, HIMS×84, VOR×113, DNN×1, INV×2, KLC×1 | $10,105.87 | -157.97 | — | TGTX, SLS, HIMS, VOR | $10,087.97 | $10,096.49 | DNN×1, INV×2, KLC×1 | 09:30 open · cash $18.95 (unchanged overnight, no fees) · equity $10,105.87 vs prior close $10,263.84 (-157.97) because holdings re-marked: TGTX×50 yday $49.28 → 09:30 $49.28 +0.00; SLS×213 yday $13.00 → 09:30 $12.66 -72.42; HIMS×84 yday $28.61 → 09:30 $27.85 -63.84; VOR×113 yday $23.01 → 09:30 $22.82 -21.47; DNN×1 yday $3.19 → 09:30 $3.11 -0.08; INV×2 yday $1.39 → 09:30 $1.32 -0.12; KLC×1 yday $2.56 → 09:30 $2.52 -0.04 |
| 2026-08-19 | -7.20 | $10,087.97 | DNN×1, INV×2, KLC×1 | $10,096.61 | +0.12 | — | — | $10,087.97 | $10,097.18 | DNN×1, INV×2, KLC×1 | 09:30 open · cash $10,087.97 (unchanged overnight, no fees) · equity $10,096.61 vs prior close $10,096.49 (+0.12) because holdings re-marked: DNN×1 yday $3.15 → 09:30 $3.19 +0.04; INV×2 yday $1.32 → 09:30 $1.39 +0.13; KLC×1 yday $2.72 → 09:30 $2.67 -0.05 |
| 2026-08-20 | +1.12 | $10,087.97 | DNN×1, INV×2, KLC×1 | $10,097.15 | -0.03 | BHP, MRVI, WYFI, TOYO, DVLT, SAFX, AAP, AEG | DNN, INV, KLC | $85.55 | $10,157.32 | BHP×13, MRVI×171, WYFI×58, TOYO×284, DVLT×4207, SAFX×3565, AAP×26, AEG×140 | 09:30 open · cash $10,087.97 (unchanged overnight, no fees) · equity $10,097.15 vs prior close $10,097.18 (-0.03) because holdings re-marked: DNN×1 yday $3.22 → 09:30 $3.20 -0.02; INV×2 yday $1.54 → 09:30 $1.55 +0.02; KLC×1 yday $2.91 → 09:30 $2.88 -0.03 |
| 2026-08-21 | +3.25 | $85.55 | BHP×13, MRVI×171, WYFI×58, TOYO×284, DVLT×4207, SAFX×3565, AAP×26, AEG×140 | $10,230.73 | +73.41 | AUTL, CRDL, ENHA, CAN | — | $39.44 | $10,277.11 | BHP×13, MRVI×171, WYFI×58, TOYO×284, DVLT×4207, SAFX×3565, AAP×26, AEG×140, AUTL×4, CRDL×6, ENHA×7, CAN×41 | 09:30 open · cash $85.55 (unchanged overnight, no fees) · equity $10,230.73 vs prior close $10,157.32 (+73.41) because holdings re-marked: BHP×13 yday $93.63 → 09:30 $95.72 +27.17; MRVI×171 yday $8.26 → 09:30 $8.20 -10.26; WYFI×58 yday $21.16 → 09:30 $21.54 +22.04; TOYO×284 yday $4.51 → 09:30 $4.68 +46.86; DVLT×4207 yday $0.32 → 09:30 $0.31 -42.07; SAFX×3565 yday $0.34 → 09:30 $0.35 +24.95; AAP×26 yday $42.39 → 09:30 $42.41 +0.52; AEG×140 yday $9.01 → 09:30 $9.04 +4.20 |
| 2026-08-24 | -5.17 | $39.44 | BHP×13, MRVI×171, WYFI×58, TOYO×284, DVLT×4207, SAFX×3565, AAP×26, AEG×140, AUTL×4, CRDL×6, ENHA×7, CAN×41 | $10,238.97 | -38.14 | — | — | $39.44 | $10,248.96 | BHP×13, MRVI×171, WYFI×58, TOYO×284, DVLT×4207, SAFX×3565, AAP×26, AEG×140, AUTL×4, CRDL×6, ENHA×7, CAN×41 | 09:30 open · cash $39.44 (unchanged overnight, no fees) · equity $10,238.97 vs prior close $10,277.11 (-38.14) because holdings re-marked: BHP×13 yday $97.03 → 09:30 $97.34 +4.03; MRVI×171 yday $8.70 → 09:30 $8.59 -18.81; WYFI×58 yday $20.72 → 09:30 $20.02 -40.60; TOYO×284 yday $4.82 → 09:30 $4.58 -68.16; DVLT×4207 yday $0.32 → 09:30 $0.31 -42.07; SAFX×3565 yday $0.33 → 09:30 $0.35 +89.12; AAP×26 yday $42.58 → 09:30 $43.10 +13.52; AEG×140 yday $8.99 → 09:30 $9.16 +23.80; AUTL×4 yday $2.41 → 09:30 $2.36 -0.20; CRDL×6 yday $1.86 → 09:30 $1.87 +0.06; ENHA×7 yday $1.72 → 09:30 $1.74 +0.14; CAN×41 yday $0.35 → 09:30 $0.38 +1.03 |
| 2026-08-25 | +1.80 | $39.44 | BHP×13, MRVI×171, WYFI×58, TOYO×284, DVLT×4207, SAFX×3565, AAP×26, AEG×140, AUTL×4, CRDL×6, ENHA×7, CAN×41 | $10,344.52 | +95.56 | OCUL, CRMD, PUSA, CAPR, SUJA, FWDI, JANX | BHP, MRVI, WYFI, TOYO, DVLT, AAP, AEG | $12.04 | $10,398.54 | SAFX×3565, AUTL×4, CRDL×6, ENHA×7, CAN×41, OCUL×116, CRMD×154, PUSA×345, CAPR×188, SUJA×145, FWDI×213, JANX×68 | 09:30 open · cash $39.44 (unchanged overnight, no fees) · equity $10,344.52 vs prior close $10,248.96 (+95.56) because holdings re-marked: BHP×13 yday $96.66 → 09:30 $95.95 -9.23; MRVI×171 yday $8.26 → 09:30 $8.31 +8.55; WYFI×58 yday $20.79 → 09:30 $20.98 +11.02; TOYO×284 yday $4.61 → 09:30 $4.48 -36.92; DVLT×4207 yday $0.31 → 09:30 $0.32 +42.07; SAFX×3565 yday $0.35 → 09:30 $0.37 +71.30; AAP×26 yday $43.83 → 09:30 $43.61 -5.72; AEG×140 yday $9.19 → 09:30 $9.29 +14.00; AUTL×4 yday $2.38 → 09:30 $2.32 -0.24; CRDL×6 yday $1.80 → 09:30 $1.90 +0.60; ENHA×7 yday $1.69 → 09:30 $1.65 -0.28; CAN×41 yday $0.37 → 09:30 $0.38 +0.41 |
| 2026-08-26 | +2.02 | $12.04 | SAFX×3565, AUTL×4, CRDL×6, ENHA×7, CAN×41, OCUL×116, CRMD×154, PUSA×345, CAPR×188, SUJA×145, FWDI×213, JANX×68 | $10,398.54 | -0.00 | — | — | $12.04 | $10,283.54 | SAFX×3565, AUTL×4, CRDL×6, ENHA×7, CAN×41, OCUL×116, CRMD×154, PUSA×345, CAPR×188, SUJA×145, FWDI×213, JANX×68 | 09:30 open · cash $12.04 (unchanged overnight, no fees) · equity $10,398.54 vs prior close $10,398.54 (-0.00) because holdings re-marked: SAFX×3565 yday $0.37 → 09:30 $0.37 +0.00; AUTL×4 yday $2.34 → 09:30 $2.34 +0.00; CRDL×6 yday $1.90 → 09:30 $1.90 +0.00; ENHA×7 yday $1.66 → 09:30 $1.66 +0.00; CAN×41 yday $0.36 → 09:30 $0.36 +0.00; OCUL×116 yday $10.92 → 09:30 $10.92 +0.00; CRMD×154 yday $8.28 → 09:30 $8.28 +0.00; PUSA×345 yday $3.91 → 09:30 $3.91 +0.00; CAPR×188 yday $7.19 → 09:30 $7.19 +0.00; SUJA×145 yday $8.54 → 09:30 $8.54 +0.00; FWDI×213 yday $5.86 → 09:30 $5.86 +0.00; JANX×68 yday $18.99 → 09:30 $18.99 +0.00 |
| 2026-08-27 | — | $12.04 | SAFX×3565, AUTL×4, CRDL×6, ENHA×7, CAN×41, OCUL×116, CRMD×154, PUSA×345, CAPR×188, SUJA×145, FWDI×213, JANX×68 | $10,666.06 | +382.52 | ACMR, GGB, MT, TX | SAFX, AUTL, CRDL, ENHA, CAN | $779.03 | $10,823.74 | OCUL×116, CRMD×154, PUSA×345, CAPR×188, SUJA×145, FWDI×213, JANX×68, ACMR×1, GGB×36, MT×2, TX×2 | 09:30 open · cash $12.04 (unchanged overnight, no fees) · equity $10,666.06 vs prior close $10,283.54 (+382.52) because holdings re-marked: SAFX×3565 yday $0.37 → 09:30 $0.35 -71.30; AUTL×4 yday $2.34 → 09:30 $2.41 +0.28; CRDL×6 yday $1.90 → 09:30 $2.03 +0.78; ENHA×7 yday $1.66 → 09:30 $1.63 -0.21; CAN×41 yday $0.36 → 09:30 $0.40 +1.64; OCUL×116 yday $10.92 → 09:30 $10.79 -15.08; CRMD×154 yday $8.28 → 09:30 $8.60 +49.28; PUSA×345 yday $3.91 → 09:30 $3.84 -24.15; CAPR×188 yday $7.19 → 09:30 $8.29 +206.80; SUJA×145 yday $8.54 → 09:30 $9.39 +123.25; FWDI×213 yday $5.86 → 09:30 $5.97 +23.43; JANX×68 yday $18.99 → 09:30 $18.59 -27.20 |
| 2026-08-28 | +0.75 | $779.03 | OCUL×116, CRMD×154, PUSA×345, CAPR×188, SUJA×145, FWDI×213, JANX×68, ACMR×1, GGB×36, MT×2, TX×2 | $10,902.26 | +78.52 | SEDG, SMTC, OPTX, TTMI, BBWI, BTSG, CRDL | OCUL, CRMD, PUSA, SUJA, FWDI, JANX | $146.08 | $10,939.82 | CAPR×188, ACMR×1, GGB×36, MT×2, TX×2, SEDG×36, SMTC×8, OPTX×144, TTMI×9, BBWI×66, BTSG×20, CRDL×591 | 09:30 open · cash $779.03 (unchanged overnight, no fees) · equity $10,902.26 vs prior close $10,823.74 (+78.52) because holdings re-marked: OCUL×116 yday $10.77 → 09:30 $10.63 -16.24; CRMD×154 yday $8.39 → 09:30 $8.49 +15.40; PUSA×345 yday $3.85 → 09:30 $3.86 +3.45; CAPR×188 yday $9.36 → 09:30 $9.19 -31.96; SUJA×145 yday $9.44 → 09:30 $9.41 -4.35; FWDI×213 yday $5.93 → 09:30 $6.39 +97.98; JANX×68 yday $18.89 → 09:30 $19.00 +7.48; ACMR×1 yday $79.11 → 09:30 $81.65 +2.54; GGB×36 yday $4.46 → 09:30 $4.57 +3.96; MT×2 yday $74.53 → 09:30 $74.54 +0.02; TX×2 yday $55.13 → 09:30 $55.25 +0.24 |
| 2026-08-31 | -5.85 | $146.08 | CAPR×188, ACMR×1, GGB×36, MT×2, TX×2, SEDG×36, SMTC×8, OPTX×144, TTMI×9, BBWI×66, BTSG×20, CRDL×591 | $10,524.88 | -414.94 | — | CAPR | $1,918.20 | $10,531.53 | ACMR×1, GGB×36, MT×2, TX×2, SEDG×36, SMTC×8, OPTX×144, TTMI×9, BBWI×66, BTSG×20, CRDL×591 | 09:30 open · cash $146.08 (unchanged overnight, no fees) · equity $10,524.88 vs prior close $10,939.82 (-414.94) because holdings re-marked: CAPR×188 yday $10.06 → 09:30 $9.44 -116.56; ACMR×1 yday $80.49 → 09:30 $75.10 -5.39; GGB×36 yday $4.70 → 09:30 $4.55 -5.40; MT×2 yday $74.63 → 09:30 $75.07 +0.88; TX×2 yday $55.83 → 09:30 $54.84 -1.98; SEDG×36 yday $33.51 → 09:30 $31.50 -72.36; SMTC×8 yday $142.43 → 09:30 $133.04 -75.12; OPTX×144 yday $8.73 → 09:30 $8.52 -30.24; TTMI×9 yday $124.73 → 09:30 $117.20 -67.77; BBWI×66 yday $18.65 → 09:30 $19.30 +42.90; BTSG×20 yday $60.90 → 09:30 $59.66 -24.80; CRDL×591 yday $2.06 → 09:30 $1.96 -59.10 |
| 2026-09-01 | -6.30 | $1,918.20 | ACMR×1, GGB×36, MT×2, TX×2, SEDG×36, SMTC×8, OPTX×144, TTMI×9, BBWI×66, BTSG×20, CRDL×591 | $10,483.03 | -48.50 | — | ACMR, GGB, MT, TX | $2,408.50 | $10,419.90 | SEDG×36, SMTC×8, OPTX×144, TTMI×9, BBWI×66, BTSG×20, CRDL×591 | 09:30 open · cash $1,918.20 (unchanged overnight, no fees) · equity $10,483.03 vs prior close $10,531.53 (-48.50) because holdings re-marked: ACMR×1 yday $75.02 → 09:30 $71.24 -3.78; GGB×36 yday $4.55 → 09:30 $4.61 +2.16; MT×2 yday $75.06 → 09:30 $74.31 -1.50; TX×2 yday $54.84 → 09:30 $54.82 -0.04; SEDG×36 yday $31.27 → 09:30 $32.22 +34.20; SMTC×8 yday $132.54 → 09:30 $131.65 -7.12; OPTX×144 yday $8.52 → 09:30 $8.19 -47.52; TTMI×9 yday $120.19 → 09:30 $119.79 -3.60; BBWI×66 yday $19.22 → 09:30 $19.10 -7.92; BTSG×20 yday $59.66 → 09:30 $58.40 -25.20; CRDL×591 yday $1.96 → 09:30 $1.98 +11.82 |
| 2026-09-02 | -3.83 | $2,408.50 | SEDG×36, SMTC×8, OPTX×144, TTMI×9, BBWI×66, BTSG×20, CRDL×591 | $10,326.70 | -93.20 | — | SEDG, SMTC, OPTX, TTMI, BBWI, BTSG, CRDL | $10,306.04 | $10,306.04 | — | 09:30 open · cash $2,408.50 (unchanged overnight, no fees) · equity $10,326.70 vs prior close $10,419.90 (-93.20) because holdings re-marked: SEDG×36 yday $31.80 → 09:30 $31.87 +2.52; SMTC×8 yday $129.50 → 09:30 $127.63 -14.96; OPTX×144 yday $8.19 → 09:30 $7.94 -36.00; TTMI×9 yday $116.94 → 09:30 $116.68 -2.34; BBWI×66 yday $19.10 → 09:30 $18.77 -21.78; BTSG×20 yday $58.40 → 09:30 $58.55 +3.00; CRDL×591 yday $1.98 → 09:30 $1.94 -23.64 |
| 2026-09-03 | -0.90 | $10,306.04 | — | $10,306.04 | +0.00 | CABA, FRVO, CTMX, EIX, CRDL, SION, DUOL, SAFX | — | $36.13 | $10,428.44 | CABA×393, FRVO×70, CTMX×346, EIX×22, CRDL×596, SION×194, DUOL×8, SAFX×3303 | 09:30 open · cash $10,306.04 · no holdings · equity $10,306.04 vs prior close $10,306.04 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $36.13 | CABA×393, FRVO×70, CTMX×346, EIX×22, CRDL×596, SION×194, DUOL×8, SAFX×3303 | $10,516.32 | +87.88 | SLBT, IRD | — | $28.32 | $10,239.35 | CABA×393, FRVO×70, CTMX×346, EIX×22, CRDL×596, SION×194, DUOL×8, SAFX×3303, SLBT×1, IRD×1 | 09:30 open · cash $36.13 (unchanged overnight, no fees) · equity $10,516.32 vs prior close $10,428.44 (+87.88) because holdings re-marked: CABA×393 yday $3.57 → 09:30 $3.63 +23.58; FRVO×70 yday $17.98 → 09:30 $18.27 +20.30; CTMX×346 yday $3.72 → 09:30 $3.73 +3.46; EIX×22 yday $55.19 → 09:30 $55.42 +5.06; CRDL×596 yday $2.17 → 09:30 $2.18 +5.96; SION×194 yday $7.31 → 09:30 $7.31 +0.00; DUOL×8 yday $157.85 → 09:30 $161.54 +29.52; SAFX×3303 yday $0.38 → 09:30 $0.38 +0.00 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 50 | $49.70 | $2.14 | — | $7,512.86 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=-0.8; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 213 | $11.70 | $2.75 | — | $5,018.01 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=-0.8; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 84 | $29.74 | $2.24 | — | $2,517.61 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=-5.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 113 | $22.01 | $2.33 | — | $28.15 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=+0.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.15 | ▲ 09:30 equity $10,117.74 vs yday $10,106.28 (+11.46) | 09:30 open · cash $28.15 (unchanged overnight, no fees) · equity $10,117.74 vs prior close $10,106.28 (+11.46) because holdings re-marked: TGTX×50 yday $47.94 → 09:30 $47.27 -33.50; SLS×213 yday $12.36 → 09:30 $12.40 +8.52; HIMS×84 yday $28.77 → 09:30 $29.15 +31.92; VOR×113 yday $23.29 → 09:30 $23.33 +4.52 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.15 | ▼ 09:30 equity $10,139.88 vs yday $10,154.28 (-14.40) | 09:30 open · cash $28.15 (unchanged overnight, no fees) · equity $10,139.88 vs prior close $10,154.28 (-14.40) because holdings re-marked: TGTX×50 yday $48.74 → 09:30 $48.74 +0.00; SLS×213 yday $12.78 → 09:30 $12.78 +0.00; HIMS×84 yday $28.15 → 09:30 $28.14 -0.84; VOR×113 yday $23.03 → 09:30 $22.91 -13.56 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 1 | $3.24 | $0.04 | — | $24.88 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=+0.3; leftover $3.52 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `INV` | 2 | $1.62 | $0.04 | — | $21.60 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ⚪; ret5=-53.0; leftover $3.52 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `KLC` | 1 | $2.62 | $0.03 | — | $18.95 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ⚪; ret5=-49.7; leftover $3.52 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18.95 | ▼ 09:30 equity $10,105.87 vs yday $10,263.84 (-157.97) | 09:30 open · cash $18.95 (unchanged overnight, no fees) · equity $10,105.87 vs prior close $10,263.84 (-157.97) because holdings re-marked: TGTX×50 yday $49.28 → 09:30 $49.28 +0.00; SLS×213 yday $13.00 → 09:30 $12.66 -72.42; HIMS×84 yday $28.61 → 09:30 $27.85 -63.84; VOR×113 yday $23.01 → 09:30 $22.82 -21.47; DNN×1 yday $3.19 → 09:30 $3.11 -0.08; INV×2 yday $1.39 → 09:30 $1.32 -0.12; KLC×1 yday $2.56 → 09:30 $2.52 -0.04 | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 50 | $49.28 | $2.17 | $-25.31 | $2,480.78 | ▼ -25.31 after sell → book $10,103.70; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 213 | $12.66 | $2.80 | $+198.93 | $5,174.55 | ▲ +198.93 after sell → book $10,100.89; vs 09:30 mark -2.81 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 84 | $27.85 | $2.27 | $-163.28 | $7,511.68 | ▼ -163.28 after sell → book $10,098.62; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 113 | $22.82 | $2.37 | $+86.83 | $10,087.97 | ▲ +86.83 after sell → book $10,096.25; vs 09:30 mark -2.37 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,087.97 | ▲ 09:30 equity $10,096.61 vs yday $10,096.49 (+0.12) | 09:30 open · cash $10,087.97 (unchanged overnight, no fees) · equity $10,096.61 vs prior close $10,096.49 (+0.12) because holdings re-marked: DNN×1 yday $3.15 → 09:30 $3.19 +0.04; INV×2 yday $1.32 → 09:30 $1.39 +0.13; KLC×1 yday $2.72 → 09:30 $2.67 -0.05 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,087.97 | ▼ 09:30 equity $10,097.15 vs yday $10,097.18 (-0.03) | 09:30 open · cash $10,087.97 (unchanged overnight, no fees) · equity $10,097.15 vs prior close $10,097.18 (-0.03) because holdings re-marked: DNN×1 yday $3.22 → 09:30 $3.20 -0.02; INV×2 yday $1.54 → 09:30 $1.55 +0.02; KLC×1 yday $2.91 → 09:30 $2.88 -0.03 | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 1 | $3.20 | $0.06 | $-0.13 | $10,091.12 | ▼ -0.13 after sell → book $10,097.10; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `INV` | 2 | $1.55 | $0.06 | $-0.24 | $10,094.16 | ▼ -0.24 after sell → book $10,097.04; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `KLC` | 1 | $2.88 | $0.05 | $+0.18 | $10,096.99 | ▲ +0.18 after sell → book $10,096.99; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,911.83 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1262.12 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 171 | $7.38 | $2.50 | — | $7,647.35 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1262.12 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WYFI` | 58 | $21.40 | $2.16 | — | $6,403.98 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-25.2; leftover $1262.12 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 284 | $4.43 | $3.66 | — | $5,142.20 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-23.1; leftover $1262.12 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 4207 | $0.30 | $25.24 | — | $3,854.86 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-3.2; leftover $1262.12 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SAFX` | 3565 | $0.35 | $23.32 | — | $2,569.53 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-29.4; leftover $1262.12 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 26 | $46.85 | $2.07 | — | $1,349.36 | — | union ∩ last_red, no 🚨; gate last_red=True; list earn_react; 🔵; ret5=+5.0; leftover $1262.12 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 140 | $9.01 | $2.41 | — | $85.55 | — | union ∩ last_red, no 🚨; gate last_red=True; list earn_react; 🔵; ⚪; ret5=-1.3; leftover $1262.12 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.55 | ▲ 09:30 equity $10,230.73 vs yday $10,157.32 (+73.41) | 09:30 open · cash $85.55 (unchanged overnight, no fees) · equity $10,230.73 vs prior close $10,157.32 (+73.41) because holdings re-marked: BHP×13 yday $93.63 → 09:30 $95.72 +27.17; MRVI×171 yday $8.26 → 09:30 $8.20 -10.26; WYFI×58 yday $21.16 → 09:30 $21.54 +22.04; TOYO×284 yday $4.51 → 09:30 $4.68 +46.86; DVLT×4207 yday $0.32 → 09:30 $0.31 -42.07; SAFX×3565 yday $0.34 → 09:30 $0.35 +24.95; AAP×26 yday $42.39 → 09:30 $42.41 +0.52; AEG×140 yday $9.01 → 09:30 $9.04 +4.20 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 4 | $2.47 | $0.11 | — | $75.56 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $12.22 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 6 | $1.93 | $0.13 | — | $63.85 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $12.22 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ENHA` | 7 | $1.71 | $0.14 | — | $51.74 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $12.22 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAN` | 41 | $0.29 | $0.24 | — | $39.44 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $12.22 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.44 | ▼ 09:30 equity $10,238.97 vs yday $10,277.11 (-38.14) | 09:30 open · cash $39.44 (unchanged overnight, no fees) · equity $10,238.97 vs prior close $10,277.11 (-38.14) because holdings re-marked: BHP×13 yday $97.03 → 09:30 $97.34 +4.03; MRVI×171 yday $8.70 → 09:30 $8.59 -18.81; WYFI×58 yday $20.72 → 09:30 $20.02 -40.60; TOYO×284 yday $4.82 → 09:30 $4.58 -68.16; DVLT×4207 yday $0.32 → 09:30 $0.31 -42.07; SAFX×3565 yday $0.33 → 09:30 $0.35 +89.12; AAP×26 yday $42.58 → 09:30 $43.10 +13.52; AEG×140 yday $8.99 → 09:30 $9.16 +23.80; AUTL×4 yday $2.41 → 09:30 $2.36 -0.20; CRDL×6 yday $1.86 → 09:30 $1.87 +0.06; ENHA×7 yday $1.72 → 09:30 $1.74 +0.14; CAN×41 yday $0.35 → 09:30 $0.38 +1.03 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.44 | ▲ 09:30 equity $10,344.52 vs yday $10,248.96 (+95.56) | 09:30 open · cash $39.44 (unchanged overnight, no fees) · equity $10,344.52 vs prior close $10,248.96 (+95.56) because holdings re-marked: BHP×13 yday $96.66 → 09:30 $95.95 -9.23; MRVI×171 yday $8.26 → 09:30 $8.31 +8.55; WYFI×58 yday $20.79 → 09:30 $20.98 +11.02; TOYO×284 yday $4.61 → 09:30 $4.48 -36.92; DVLT×4207 yday $0.31 → 09:30 $0.32 +42.07; SAFX×3565 yday $0.35 → 09:30 $0.37 +71.30; AAP×26 yday $43.83 → 09:30 $43.61 -5.72; AEG×140 yday $9.19 → 09:30 $9.29 +14.00; AUTL×4 yday $2.38 → 09:30 $2.32 -0.24; CRDL×6 yday $1.80 → 09:30 $1.90 +0.60; ENHA×7 yday $1.69 → 09:30 $1.65 -0.28; CAN×41 yday $0.37 → 09:30 $0.38 +0.41 | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.95 | $2.05 | $+60.14 | $1,284.74 | ▲ +60.14 after sell → book $10,342.47; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MRVI` | 171 | $8.31 | $2.54 | $+153.98 | $2,703.21 | ▲ +153.98 after sell → book $10,339.93; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WYFI` | 58 | $20.98 | $2.18 | $-28.71 | $3,917.86 | ▼ -28.71 after sell → book $10,337.74; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `TOYO` | 284 | $4.48 | $3.72 | $+6.82 | $5,186.46 | ▲ +6.82 after sell → book $10,334.02; vs 09:30 mark -3.72 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DVLT` | 4207 | $0.32 | $26.79 | $+32.11 | $6,505.91 | ▲ +32.11 after sell → book $10,307.23; vs 09:30 mark -26.79 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AAP` | 26 | $43.61 | $2.09 | $-88.40 | $7,637.68 | ▼ -88.40 after sell → book $10,305.14; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AEG` | 140 | $9.29 | $2.44 | $+34.35 | $8,935.84 | ▲ +34.35 after sell → book $10,302.70; vs 09:30 mark -2.44 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 116 | $10.92 | $2.34 | — | $7,666.78 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ret5=+10.4; leftover $1276.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 154 | $8.28 | $2.45 | — | $6,389.21 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1276.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 345 | $3.70 | $4.45 | — | $5,108.26 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.8; leftover $1276.55 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 188 | $6.79 | $2.55 | — | $3,829.18 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.4; leftover $1276.55 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 145 | $8.79 | $2.42 | — | $2,552.21 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.5; leftover $1276.55 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FWDI` | 213 | $5.99 | $2.75 | — | $1,273.59 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+20.7; leftover $1276.55 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `JANX` | 68 | $18.52 | $2.19 | — | $12.04 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ret5=+7.9; leftover $1276.55 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.04 | ▲ 09:30 equity $10,398.54 vs yday $10,398.54 (-0.00) | 09:30 open · cash $12.04 (unchanged overnight, no fees) · equity $10,398.54 vs prior close $10,398.54 (-0.00) because holdings re-marked: SAFX×3565 yday $0.37 → 09:30 $0.37 +0.00; AUTL×4 yday $2.34 → 09:30 $2.34 +0.00; CRDL×6 yday $1.90 → 09:30 $1.90 +0.00; ENHA×7 yday $1.66 → 09:30 $1.66 +0.00; CAN×41 yday $0.36 → 09:30 $0.36 +0.00; OCUL×116 yday $10.92 → 09:30 $10.92 +0.00; CRMD×154 yday $8.28 → 09:30 $8.28 +0.00; PUSA×345 yday $3.91 → 09:30 $3.91 +0.00; CAPR×188 yday $7.19 → 09:30 $7.19 +0.00; SUJA×145 yday $8.54 → 09:30 $8.54 +0.00; FWDI×213 yday $5.86 → 09:30 $5.86 +0.00; JANX×68 yday $18.99 → 09:30 $18.99 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.04 | ▲ 09:30 equity $10,666.06 vs yday $10,283.54 (+382.52) | 09:30 open · cash $12.04 (unchanged overnight, no fees) · equity $10,666.06 vs prior close $10,283.54 (+382.52) because holdings re-marked: SAFX×3565 yday $0.37 → 09:30 $0.35 -71.30; AUTL×4 yday $2.34 → 09:30 $2.41 +0.28; CRDL×6 yday $1.90 → 09:30 $2.03 +0.78; ENHA×7 yday $1.66 → 09:30 $1.63 -0.21; CAN×41 yday $0.36 → 09:30 $0.40 +1.64; OCUL×116 yday $10.92 → 09:30 $10.79 -15.08; CRMD×154 yday $8.28 → 09:30 $8.60 +49.28; PUSA×345 yday $3.91 → 09:30 $3.84 -24.15; CAPR×188 yday $7.19 → 09:30 $8.29 +206.80; SUJA×145 yday $8.54 → 09:30 $9.39 +123.25; FWDI×213 yday $5.86 → 09:30 $5.97 +23.43; JANX×68 yday $18.99 → 09:30 $18.59 -27.20 | — |
| 2026-08-27 09:30 ET | **SELL** | `SAFX` | 3565 | $0.35 | $23.77 | $-61.35 | $1,236.01 | ▼ -61.35 after sell → book $10,642.28; vs 09:30 mark -23.78 | dropped from list after 5 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `AUTL` | 4 | $2.41 | $0.13 | $-0.48 | $1,245.52 | ▼ -0.48 after sell → book $10,642.15; vs 09:30 mark -0.13 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 6 | $2.03 | $0.16 | $+0.31 | $1,257.55 | ▲ +0.31 after sell → book $10,642.00; vs 09:30 mark -0.15 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ENHA` | 7 | $1.63 | $0.16 | $-0.86 | $1,268.80 | ▼ -0.86 after sell → book $10,641.84; vs 09:30 mark -0.16 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAN` | 41 | $0.40 | $0.31 | $+3.80 | $1,284.89 | ▲ +3.80 after sell → book $10,641.53; vs 09:30 mark -0.31 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 1 | $80.97 | $0.81 | — | $1,203.11 | — | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; 🔵; ret5=-1.3; leftover $160.61 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 36 | $4.42 | $1.70 | — | $1,042.29 | — | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; 🔵; ret5=-8.6; leftover $160.61 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 2 | $75.12 | $1.51 | — | $890.54 | — | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; 🔵; ret5=-2.2; leftover $160.61 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `TX` | 2 | $55.20 | $1.11 | — | $779.03 | — | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; 🔵; ret5=+3.0; leftover $160.61 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $779.03 | ▲ 09:30 equity $10,902.26 vs yday $10,823.74 (+78.52) | 09:30 open · cash $779.03 (unchanged overnight, no fees) · equity $10,902.26 vs prior close $10,823.74 (+78.52) because holdings re-marked: OCUL×116 yday $10.77 → 09:30 $10.63 -16.24; CRMD×154 yday $8.39 → 09:30 $8.49 +15.40; PUSA×345 yday $3.85 → 09:30 $3.86 +3.45; CAPR×188 yday $9.36 → 09:30 $9.19 -31.96; SUJA×145 yday $9.44 → 09:30 $9.41 -4.35; FWDI×213 yday $5.93 → 09:30 $6.39 +97.98; JANX×68 yday $18.89 → 09:30 $19.00 +7.48; ACMR×1 yday $79.11 → 09:30 $81.65 +2.54; GGB×36 yday $4.46 → 09:30 $4.57 +3.96; MT×2 yday $74.53 → 09:30 $74.54 +0.02; TX×2 yday $55.13 → 09:30 $55.25 +0.24 | — |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 116 | $10.63 | $2.37 | $-38.35 | $2,009.75 | ▼ -38.35 after sell → book $10,899.90; vs 09:30 mark -2.36 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 154 | $8.49 | $2.49 | $+27.40 | $3,314.72 | ▲ +27.40 after sell → book $10,897.41; vs 09:30 mark -2.49 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `PUSA` | 345 | $3.86 | $4.52 | $+46.23 | $4,641.90 | ▲ +46.23 after sell → book $10,892.89; vs 09:30 mark -4.52 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SUJA` | 145 | $9.41 | $2.46 | $+85.02 | $6,003.89 | ▲ +85.02 after sell → book $10,890.43; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FWDI` | 213 | $6.39 | $2.79 | $+79.66 | $7,362.17 | ▲ +79.66 after sell → book $10,887.64; vs 09:30 mark -2.79 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `JANX` | 68 | $19.00 | $2.22 | $+28.23 | $8,651.95 | ▲ +28.23 after sell → book $10,885.42; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 36 | $33.78 | $2.10 | — | $7,433.77 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1235.99 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $149.40 | $2.01 | — | $6,236.56 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1235.99 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 144 | $8.57 | $2.42 | — | $5,000.06 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; ret5=-3.4; leftover $1235.99 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 9 | $127.07 | $2.02 | — | $3,854.41 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=-21.0; leftover $1235.99 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 66 | $18.68 | $2.19 | — | $2,619.34 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; ret5=+0.2; leftover $1235.99 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BTSG` | 20 | $61.42 | $2.05 | — | $1,388.89 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; ret5=-4.6; leftover $1235.99 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CRDL` | 591 | $2.09 | $7.62 | — | $146.08 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; ret5=+3.3; leftover $1235.99 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $146.08 | ▼ 09:30 equity $10,524.88 vs yday $10,939.82 (-414.94) | 09:30 open · cash $146.08 (unchanged overnight, no fees) · equity $10,524.88 vs prior close $10,939.82 (-414.94) because holdings re-marked: CAPR×188 yday $10.06 → 09:30 $9.44 -116.56; ACMR×1 yday $80.49 → 09:30 $75.10 -5.39; GGB×36 yday $4.70 → 09:30 $4.55 -5.40; MT×2 yday $74.63 → 09:30 $75.07 +0.88; TX×2 yday $55.83 → 09:30 $54.84 -1.98; SEDG×36 yday $33.51 → 09:30 $31.50 -72.36; SMTC×8 yday $142.43 → 09:30 $133.04 -75.12; OPTX×144 yday $8.73 → 09:30 $8.52 -30.24; TTMI×9 yday $124.73 → 09:30 $117.20 -67.77; BBWI×66 yday $18.65 → 09:30 $19.30 +42.90; BTSG×20 yday $60.90 → 09:30 $59.66 -24.80; CRDL×591 yday $2.06 → 09:30 $1.96 -59.10 | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 188 | $9.44 | $2.60 | $+493.05 | $1,918.20 | ▲ +493.05 after sell → book $10,522.28; vs 09:30 mark -2.60 | dropped from list after 4 sess (min 3) | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,918.20 | ▼ 09:30 equity $10,483.03 vs yday $10,531.53 (-48.50) | 09:30 open · cash $1,918.20 (unchanged overnight, no fees) · equity $10,483.03 vs prior close $10,531.53 (-48.50) because holdings re-marked: ACMR×1 yday $75.02 → 09:30 $71.24 -3.78; GGB×36 yday $4.55 → 09:30 $4.61 +2.16; MT×2 yday $75.06 → 09:30 $74.31 -1.50; TX×2 yday $54.84 → 09:30 $54.82 -0.04; SEDG×36 yday $31.27 → 09:30 $32.22 +34.20; SMTC×8 yday $132.54 → 09:30 $131.65 -7.12; OPTX×144 yday $8.52 → 09:30 $8.19 -47.52; TTMI×9 yday $120.19 → 09:30 $119.79 -3.60; BBWI×66 yday $19.22 → 09:30 $19.10 -7.92; BTSG×20 yday $59.66 → 09:30 $58.40 -25.20; CRDL×591 yday $1.96 → 09:30 $1.98 +11.82 | — |
| 2026-09-01 09:30 ET | **SELL** | `ACMR` | 1 | $71.24 | $0.74 | $-11.28 | $1,988.70 | ▼ -11.28 after sell → book $10,482.29; vs 09:30 mark -0.74 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `GGB` | 36 | $4.61 | $1.79 | $+3.35 | $2,152.87 | ▲ +3.35 after sell → book $10,480.50; vs 09:30 mark -1.79 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `MT` | 2 | $74.31 | $1.51 | $-4.64 | $2,299.98 | ▼ -4.64 after sell → book $10,478.99; vs 09:30 mark -1.51 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `TX` | 2 | $54.82 | $1.12 | $-2.99 | $2,408.50 | ▼ -2.99 after sell → book $10,477.87; vs 09:30 mark -1.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,408.50 | ▼ 09:30 equity $10,326.70 vs yday $10,419.90 (-93.20) | 09:30 open · cash $2,408.50 (unchanged overnight, no fees) · equity $10,326.70 vs prior close $10,419.90 (-93.20) because holdings re-marked: SEDG×36 yday $31.80 → 09:30 $31.87 +2.52; SMTC×8 yday $129.50 → 09:30 $127.63 -14.96; OPTX×144 yday $8.19 → 09:30 $7.94 -36.00; TTMI×9 yday $116.94 → 09:30 $116.68 -2.34; BBWI×66 yday $19.10 → 09:30 $18.77 -21.78; BTSG×20 yday $58.40 → 09:30 $58.55 +3.00; CRDL×591 yday $1.98 → 09:30 $1.94 -23.64 | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 36 | $31.87 | $2.12 | $-72.98 | $3,553.70 | ▼ -72.98 after sell → book $10,324.58; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 8 | $127.63 | $2.03 | $-178.21 | $4,572.71 | ▼ -178.21 after sell → book $10,322.55; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `OPTX` | 144 | $7.94 | $2.46 | $-95.60 | $5,713.61 | ▼ -95.60 after sell → book $10,320.09; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TTMI` | 9 | $116.68 | $2.04 | $-97.56 | $6,761.69 | ▼ -97.56 after sell → book $10,318.05; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBWI` | 66 | $18.77 | $2.21 | $+1.54 | $7,998.31 | ▲ +1.54 after sell → book $10,315.85; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BTSG` | 20 | $58.55 | $2.07 | $-61.52 | $9,167.24 | ▼ -61.52 after sell → book $10,313.78; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CRDL` | 591 | $1.94 | $7.73 | $-104.01 | $10,306.04 | ▼ -104.01 after sell → book $10,306.04; vs 09:30 mark -7.74 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,306.04 | ▲ 09:30 equity $10,306.04 vs yday $10,306.04 (+0.00) | 09:30 open · cash $10,306.04 · no holdings · equity $10,306.04 vs prior close $10,306.04 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 393 | $3.27 | $5.07 | — | $9,015.86 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1288.26 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 70 | $18.40 | $2.20 | — | $7,725.66 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1288.26 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CTMX` | 346 | $3.72 | $4.46 | — | $6,434.08 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $1288.26 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 22 | $56.78 | $2.06 | — | $5,182.86 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer; ret5=+0.3; leftover $1288.26 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 596 | $2.16 | $7.69 | — | $3,887.82 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1288.26 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SION` | 194 | $6.63 | $2.57 | — | $2,599.02 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ret5=-18.1; leftover $1288.26 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DUOL` | 8 | $156.24 | $2.01 | — | $1,347.09 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ret5=+10.0; leftover $1288.26 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SAFX` | 3303 | $0.39 | $22.79 | — | $36.13 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; ret5=-26.5; leftover $1288.26 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $36.13 | ▲ 09:30 equity $10,516.32 vs yday $10,428.44 (+87.88) | 09:30 open · cash $36.13 (unchanged overnight, no fees) · equity $10,516.32 vs prior close $10,428.44 (+87.88) because holdings re-marked: CABA×393 yday $3.57 → 09:30 $3.63 +23.58; FRVO×70 yday $17.98 → 09:30 $18.27 +20.30; CTMX×346 yday $3.72 → 09:30 $3.73 +3.46; EIX×22 yday $55.19 → 09:30 $55.42 +5.06; CRDL×596 yday $2.17 → 09:30 $2.18 +5.96; SION×194 yday $7.31 → 09:30 $7.31 +0.00; DUOL×8 yday $157.85 → 09:30 $161.54 +29.52; SAFX×3303 yday $0.38 → 09:30 $0.38 +0.00 | — |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 1 | $3.07 | $0.03 | — | $33.03 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $6.02 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 1 | $4.66 | $0.05 | — | $28.32 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $6.02 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TLN` | cash | leftover split 3.52 < 1 share @ 359.83 |
| 2026-08-14 | `NRG` | cash | leftover split 3.52 < 1 share @ 120.00 |
| 2026-08-14 | `MARA` | cash | leftover split 3.52 < 1 share @ 9.01 |
| 2026-08-14 | `ARX` | cash | leftover split 3.52 < 1 share @ 19.57 |
| 2026-08-14 | `HLIT` | cash | leftover split 3.52 < 1 share @ 13.18 |
| 2026-08-14 | `SECZ` | cash | leftover split 3.52 < 1 share @ 5.84 |
| 2026-08-14 | `LFTO` | cash | leftover split 3.52 < 1 share @ 20.57 |
| 2026-08-14 | `REZI` | cash | leftover split 3.52 < 1 share @ 20.56 |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TMC` | cash | leftover split 3.52 < 1 share @ 4.05 |
| 2026-08-17 | `TGB` | cash | leftover split 3.52 < 1 share @ 8.46 |
| 2026-08-17 | `ELF` | cash | leftover split 3.52 < 1 share @ 90.54 |
| 2026-08-17 | `CAPR` | cash | leftover split 3.52 < 1 share @ 6.87 |
| 2026-08-17 | `NU` | cash | leftover split 3.52 < 1 share @ 15.40 |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `INV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `KLC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TBPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INDP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ENVX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `STUB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `INV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `KLC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `FN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WYFI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `TOYO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DVLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AAP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AEG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CRSP` | cash | leftover split 12.22 < 1 share @ 59.72 |
| 2026-08-21 | `FUTU` | cash | leftover split 12.22 < 1 share @ 115.18 |
| 2026-08-21 | `GMAB` | cash | leftover split 12.22 < 1 share @ 33.36 |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MRVI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WYFI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `TOYO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `DVLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SAFX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AAP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AEG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ENHA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CAN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `SBSW` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PAAS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ENHA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CAN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `SAFX` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AUTL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CRDL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ENHA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CAN` | no_price | no 09:30 open — carry |
| 2026-08-26 | `PUSA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SUJA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FWDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `JANX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-26 | `AVEX` | no_price | no 09:30 open |
| 2026-08-26 | `BE` | no_price | no 09:30 open |
| 2026-08-26 | `INDP` | no_price | no 09:30 open |
| 2026-08-26 | `AXTI` | no_price | no 09:30 open |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `PUSA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SUJA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FWDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `JANX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `MU` | cash | leftover split 160.61 < 1 share @ 925.74 |
| 2026-08-27 | `LRCX` | cash | leftover split 160.61 < 1 share @ 314.61 |
| 2026-08-27 | `MRVL` | cash | leftover split 160.61 < 1 share @ 240.00 |
| 2026-08-27 | `NUE` | cash | leftover split 160.61 < 1 share @ 248.91 |
| 2026-08-28 | `ACMR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `GGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `MT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `TX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ACMR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `GGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `MT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `TX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `OPTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TTMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BBWI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SNPS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `OPTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TTMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BBWI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OKE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `IRD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BEP` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `STIM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VLRS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `FRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CTMX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `EIX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DUOL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ASND` | cash | leftover split 6.02 < 1 share @ 266.94 |
| 2026-09-04 | `MLYS` | cash | leftover split 6.02 < 1 share @ 29.15 |
| 2026-09-04 | `CCOI` | cash | leftover split 6.02 < 1 share @ 10.22 |
| 2026-09-04 | `JLHL` | cash | leftover split 6.02 < 1 share @ 6.20 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CABA` | 393 | 2026-09-03 @ $3.27 | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1288.26 |
| `FRVO` | 70 | 2026-09-03 @ $18.40 | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1288.26 |
| `CTMX` | 346 | 2026-09-03 @ $3.72 | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $1288.26 |
| `EIX` | 22 | 2026-09-03 @ $56.78 | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer; ret5=+0.3; leftover $1288.26 |
| `CRDL` | 596 | 2026-09-03 @ $2.16 | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1288.26 |
| `SION` | 194 | 2026-09-03 @ $6.63 | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ret5=-18.1; leftover $1288.26 |
| `DUOL` | 8 | 2026-09-03 @ $156.24 | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ret5=+10.0; leftover $1288.26 |
| `SAFX` | 3303 | 2026-09-03 @ $0.39 | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; ret5=-26.5; leftover $1288.26 |
| `SLBT` | 1 | 2026-09-04 @ $3.07 | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $6.02 |
| `IRD` | 1 | 2026-09-04 @ $4.66 | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $6.02 |
