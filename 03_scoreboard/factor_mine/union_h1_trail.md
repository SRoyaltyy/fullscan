# Factor mine action — `union_h1_trail`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `trail` · S-boost `none` · after min-hold, trail 5% off peak

Cash book **+14.54%** ($11,454) · signal-only (no cash/fees) was +18.57%. Starts YES **16/17**. Fills 136 · skips 52 · realized $+1079.57.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `trail` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $65.24.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $97.53 | $10,153.12 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $97.53 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | $10,178.12 | +25.00 | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $592.27 | $10,193.91 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×22, MARA×140, LDI×1353, BTBT×845 | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,178.12 vs prior close $10,153.12 (+25.00) because holdings re-marked: BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13 |
| 2026-08-17 | +2.25 | $592.27 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×22, MARA×140, LDI×1353, BTBT×845 | $10,196.20 | +2.29 | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | $191.62 | $10,173.09 | DVN×27, EOG×8, FANG×6, TMC×313, TGB×150, ELF×14, DNN×391, HNST×263 | 09:30 open · cash $592.27 (unchanged overnight, no fees) · equity $10,196.20 vs prior close $10,193.91 (+2.29) because holdings re-marked: TLN×3 yday $362.74 → 09:30 $367.88 +15.42; VST×8 yday $148.13 → 09:30 $149.37 +9.92; NRG×10 yday $126.24 → 09:30 $127.40 +11.60; DAVE×3 yday $334.57 → 09:30 $336.94 +7.11; SLG×22 yday $56.09 → 09:30 $55.37 -15.84; MARA×140 yday $9.20 → 09:30 $9.22 +2.80; LDI×1353 yday $0.90 → 09:30 $0.91 +13.53; BTBT×845 yday $1.57 → 09:30 $1.52 -42.25 |
| 2026-08-18 | -6.20 | $191.62 | DVN×27, EOG×8, FANG×6, TMC×313, TGB×150, ELF×14, DNN×391, HNST×263 | $10,124.76 | -48.33 | — | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST | $10,101.41 | $10,101.41 | — | 09:30 open · cash $191.62 (unchanged overnight, no fees) · equity $10,124.76 vs prior close $10,173.09 (-48.33) because holdings re-marked: DVN×27 yday $47.57 → 09:30 $48.00 +11.61; EOG×8 yday $146.15 → 09:30 $148.04 +15.12; FANG×6 yday $206.29 → 09:30 $208.93 +15.84; TMC×313 yday $3.77 → 09:30 $3.72 -15.65; TGB×150 yday $8.77 → 09:30 $8.55 -33.00; ELF×14 yday $93.66 → 09:30 $93.44 -3.08; DNN×391 yday $3.19 → 09:30 $3.11 -31.28; HNST×263 yday $4.70 → 09:30 $4.67 -7.89 |
| 2026-08-19 | -7.20 | $10,101.41 | — | $10,101.41 | +0.00 | — | — | $10,101.41 | $10,101.41 | — | 09:30 open · cash $10,101.41 · no holdings · equity $10,101.41 vs prior close $10,101.41 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-20 | +1.12 | $10,101.41 | — | $10,101.41 | +0.00 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $203.57 | $10,311.13 | AG×61, BHP×13, CDE×61, HDSN×218, IAG×64, KGC×42, NFGC×721, WPM×8 | 09:30 open · cash $10,101.41 · no holdings · equity $10,101.41 vs prior close $10,101.41 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-21 | +3.25 | $203.57 | AG×61, BHP×13, CDE×61, HDSN×218, IAG×64, KGC×42, NFGC×721, WPM×8 | $10,580.85 | +269.72 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $14.75 | $10,781.93 | AU×11, AUPH×76, AEM×6, ARCT×118, AUTL×534, CRDL×683, CRSP×22, CYPH×999 | 09:30 open · cash $203.57 (unchanged overnight, no fees) · equity $10,580.85 vs prior close $10,311.13 (+269.72) because holdings re-marked: AG×61 yday $21.19 → 09:30 $21.90 +43.31; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×61 yday $21.11 → 09:30 $21.75 +39.04; HDSN×218 yday $5.57 → 09:30 $5.67 +21.80; IAG×64 yday $20.50 → 09:30 $21.17 +42.88; KGC×42 yday $31.43 → 09:30 $32.17 +31.08; NFGC×721 yday $1.75 → 09:30 $1.79 +28.84; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 |
| 2026-08-24 | -5.17 | $14.75 | AU×11, AUPH×76, AEM×6, ARCT×118, AUTL×534, CRDL×683, CRSP×22, CYPH×999 | $11,127.71 | +345.78 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $11,087.96 | $11,087.96 | — | 09:30 open · cash $14.75 (unchanged overnight, no fees) · equity $11,127.71 vs prior close $10,781.93 (+345.78) because holdings re-marked: AU×11 yday $121.22 → 09:30 $120.50 -7.92; AUPH×76 yday $16.65 → 09:30 $16.60 -3.80; AEM×6 yday $216.06 → 09:30 $217.03 +5.82; ARCT×118 yday $13.45 → 09:30 $13.26 -22.42; AUTL×534 yday $2.41 → 09:30 $2.36 -26.70; CRDL×683 yday $1.86 → 09:30 $1.87 +6.83; CRSP×22 yday $59.50 → 09:30 $58.79 -15.62; CYPH×999 yday $1.42 → 09:30 $1.83 +409.59 |
| 2026-08-25 | +1.80 | $11,087.96 | — | $11,087.96 | +0.00 | MOS, OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | — | $131.72 | $11,058.23 | MOS×57, OCUL×126, INSP×22, CRMD×167, RZLT×265, HCA×3, BMEA×855, NPWR×692 | 09:30 open · cash $11,087.96 · no holdings · equity $11,087.96 vs prior close $11,087.96 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-26 | +2.02 | $131.72 | MOS×57, OCUL×126, INSP×22, CRMD×167, RZLT×265, HCA×3, BMEA×855, NPWR×692 | $11,058.23 | +0.00 | — | — | $131.72 | $11,053.51 | MOS×57, OCUL×126, INSP×22, CRMD×167, RZLT×265, HCA×3, BMEA×855, NPWR×692 | 09:30 open · cash $131.72 (unchanged overnight, no fees) · equity $11,058.23 vs prior close $11,058.23 (+0.00) because holdings re-marked: MOS×57 yday $23.75 → 09:30 $23.75 +0.00; OCUL×126 yday $10.92 → 09:30 $10.92 +0.00; INSP×22 yday $61.47 → 09:30 $61.47 +0.00; CRMD×167 yday $8.28 → 09:30 $8.28 +0.00; RZLT×265 yday $5.29 → 09:30 $5.29 +0.00; HCA×3 yday $428.50 → 09:30 $428.50 +0.00; BMEA×855 yday $1.61 → 09:30 $1.61 +0.00; NPWR×692 yday $2.02 → 09:30 $2.02 +0.00 |
| 2026-08-27 | — | $131.72 | MOS×57, OCUL×126, INSP×22, CRMD×167, RZLT×265, HCA×3, BMEA×855, NPWR×692 | $11,106.84 | +53.33 | RRC, CRK, SLI, ACMR, GGB, MT, MU | OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | $515.09 | $11,074.71 | MOS×57, RRC×33, CRK×97, SLI×532, ACMR×17, GGB×312, MT×18, MU×1 | 09:30 open · cash $131.72 (unchanged overnight, no fees) · equity $11,106.84 vs prior close $11,053.51 (+53.33) because holdings re-marked: MOS×57 yday $23.75 → 09:30 $24.84 +62.13; OCUL×126 yday $10.92 → 09:30 $10.79 -16.38; INSP×22 yday $61.47 → 09:30 $60.07 -30.80; CRMD×167 yday $8.28 → 09:30 $8.60 +53.44; RZLT×265 yday $5.29 → 09:30 $5.01 -74.20; HCA×3 yday $428.50 → 09:30 $427.50 -3.00; BMEA×855 yday $1.61 → 09:30 $1.75 +119.70; NPWR×692 yday $2.02 → 09:30 $1.93 -62.28 |
| 2026-08-28 | +0.75 | $515.09 | MOS×57, RRC×33, CRK×97, SLI×532, ACMR×17, GGB×312, MT×18, MU×1 | $11,155.17 | +80.46 | ANF, BHVN, BZ, CAPR | ACMR, GGB, MT, MU | $97.34 | $11,205.29 | MOS×57, RRC×33, CRK×97, SLI×532, ANF×9, BHVN×83, BZ×76, CAPR×153 | 09:30 open · cash $515.09 (unchanged overnight, no fees) · equity $11,155.17 vs prior close $11,074.71 (+80.46) because holdings re-marked: MOS×57 yday $24.16 → 09:30 $24.00 -9.12; RRC×33 yday $41.55 → 09:30 $41.44 -3.63; CRK×97 yday $14.50 → 09:30 $14.42 -7.76; SLI×532 yday $2.61 → 09:30 $2.60 -5.32; ACMR×17 yday $79.11 → 09:30 $81.65 +43.18; GGB×312 yday $4.46 → 09:30 $4.57 +34.32; MT×18 yday $74.53 → 09:30 $74.54 +0.18; MU×1 yday $938.40 → 09:30 $967.01 +28.61 |
| 2026-08-31 | -5.85 | $97.34 | MOS×57, RRC×33, CRK×97, SLI×532, ANF×9, BHVN×83, BZ×76, CAPR×153 | $10,978.87 | -226.42 | — | MOS, RRC, CRK, SLI, ANF, BHVN, BZ, CAPR | $10,956.28 | $10,956.28 | — | 09:30 open · cash $97.34 (unchanged overnight, no fees) · equity $10,978.87 vs prior close $11,205.29 (-226.42) because holdings re-marked: MOS×57 yday $23.76 → 09:30 $23.75 -0.57; RRC×33 yday $41.64 → 09:30 $41.11 -17.49; CRK×97 yday $14.62 → 09:30 $14.56 -5.82; SLI×532 yday $2.64 → 09:30 $2.51 -69.16; ANF×9 yday $145.75 → 09:30 $148.67 +26.28; BHVN×83 yday $16.12 → 09:30 $15.44 -56.44; BZ×76 yday $18.00 → 09:30 $17.89 -8.36; CAPR×153 yday $10.06 → 09:30 $9.44 -94.86 |
| 2026-09-01 | -6.30 | $10,956.28 | — | $10,956.28 | -0.00 | — | — | $10,956.28 | $10,956.28 | — | 09:30 open · cash $10,956.28 · no holdings · equity $10,956.28 vs prior close $10,956.28 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-02 | -3.83 | $10,956.28 | — | $10,956.28 | -0.00 | — | — | $10,956.28 | $10,956.28 | — | 09:30 open · cash $10,956.28 · no holdings · equity $10,956.28 vs prior close $10,956.28 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-03 | -0.90 | $10,956.28 | — | $10,956.28 | -0.00 | ATRC, HRMY, CABA, VSTM, RVTY, GPRO, FRVO, CRK | — | $131.00 | $11,765.19 | ATRC×27, HRMY×33, CABA×418, VSTM×177, RVTY×10, GPRO×1122, FRVO×74, CRK×87 | 09:30 open · cash $10,956.28 · no holdings · equity $10,956.28 vs prior close $10,956.28 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $131.00 | ATRC×27, HRMY×33, CABA×418, VSTM×177, RVTY×10, GPRO×1122, FRVO×74, CRK×87 | $11,931.89 | +166.70 | ASND, OSCR, NVAX, BVS, BAK | HRMY, VSTM, RVTY, FRVO, CRK | $65.24 | $11,453.50 | ATRC×27, CABA×418, GPRO×1122, ASND×5, OSCR×45, NVAX×134, BVS×96, BAK×715 | 09:30 open · cash $131.00 (unchanged overnight, no fees) · equity $11,931.89 vs prior close $11,765.19 (+166.70) because holdings re-marked: ATRC×27 yday $52.59 → 09:30 $52.88 +7.83; HRMY×33 yday $42.86 → 09:30 $42.93 +2.31; CABA×418 yday $3.57 → 09:30 $3.63 +25.08; VSTM×177 yday $8.02 → 09:30 $8.03 +1.77; RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; GPRO×1122 yday $1.69 → 09:30 $1.78 +100.98; FRVO×74 yday $17.98 → 09:30 $18.27 +21.46; CRK×87 yday $15.54 → 09:30 $15.45 -7.83 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | after min-hold, trail 5% off peak; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | after min-hold, trail 5% off peak; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | after min-hold, trail 5% off peak; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | after min-hold, trail 5% off peak; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | after min-hold, trail 5% off peak; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | after min-hold, trail 5% off peak; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | after min-hold, trail 5% off peak; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | after min-hold, trail 5% off peak; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,178.12 vs prior close $10,153.12 (+25.00) because holdings re-marked: BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13 | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $1,288.46 | ▼ -7.12 after sell → book $10,176.05; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $2,476.80 | ▼ -55.19 after sell → book $10,173.96; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $3,801.68 | ▲ +107.86 after sell → book $10,171.88; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 25 | $47.27 | $2.08 | $-64.90 | $4,981.35 | ▼ -64.90 after sell → book $10,169.80; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $6,293.41 | ▲ +69.56 after sell → book $10,167.46; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $7,515.57 | ▼ -29.03 after sell → book $10,165.32; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $8,931.32 | ▲ +148.79 after sell → book $10,146.08; vs 09:30 mark -19.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $10,143.91 | ▼ -26.05 after sell → book $10,143.91; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $9,062.42 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ret5=+5.9; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $7,885.21 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ret5=+3.6; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $6,683.19 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ret5=+0.6; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $5,688.46 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 22 | $57.61 | $2.06 | — | $4,418.98 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ret5=+5.7; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 140 | $9.01 | $2.41 | — | $3,155.17 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1353 | $0.94 | $16.74 | — | $1,870.67 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 845 | $1.50 | $10.90 | — | $592.27 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $592.27 | ▲ 09:30 equity $10,196.20 vs yday $10,193.91 (+2.29) | 09:30 open · cash $592.27 (unchanged overnight, no fees) · equity $10,196.20 vs prior close $10,193.91 (+2.29) because holdings re-marked: TLN×3 yday $362.74 → 09:30 $367.88 +15.42; VST×8 yday $148.13 → 09:30 $149.37 +9.92; NRG×10 yday $126.24 → 09:30 $127.40 +11.60; DAVE×3 yday $334.57 → 09:30 $336.94 +7.11; SLG×22 yday $56.09 → 09:30 $55.37 -15.84; MARA×140 yday $9.20 → 09:30 $9.22 +2.80; LDI×1353 yday $0.90 → 09:30 $0.91 +13.53; BTBT×845 yday $1.57 → 09:30 $1.52 -42.25 | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 3 | $367.88 | $2.02 | $+20.13 | $1,693.89 | ▲ +20.13 after sell → book $10,194.18; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 8 | $149.37 | $2.03 | $+15.71 | $2,886.82 | ▲ +15.71 after sell → book $10,192.15; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $4,158.78 | ▲ +69.94 after sell → book $10,190.11; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 3 | $336.94 | $2.02 | $+14.07 | $5,167.58 | ▲ +14.07 after sell → book $10,188.09; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 22 | $55.37 | $2.08 | $-53.41 | $6,383.64 | ▼ -53.41 after sell → book $10,186.02; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 140 | $9.22 | $2.44 | $+24.55 | $7,672.00 | ▲ +24.55 after sell → book $10,183.57; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 1353 | $0.91 | $16.57 | $-73.89 | $8,882.61 | ▼ -73.89 after sell → book $10,167.01; vs 09:30 mark -16.56 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 845 | $1.52 | $11.05 | $-5.05 | $10,155.96 | ▼ -5.05 after sell → book $10,155.96; vs 09:30 mark -11.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 27 | $46.18 | $2.07 | — | $8,907.02 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ret5=+6.7; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $7,762.85 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ret5=+5.8; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $6,544.64 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ret5=+8.3; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 313 | $4.05 | $4.04 | — | $5,272.95 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 150 | $8.46 | $2.44 | — | $4,001.51 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 14 | $90.54 | $2.03 | — | $2,731.92 | — | after min-hold, trail 5% off peak; list flatten; ret5=-7.2; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 391 | $3.24 | $5.04 | — | $1,460.04 | — | after min-hold, trail 5% off peak; list flatten; ⚪; ret5=+0.3; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 263 | $4.81 | $3.39 | — | $191.62 | — | after min-hold, trail 5% off peak; list flatten; ⚪; ret5=-11.4; leftover $1269.49 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $191.62 | ▼ 09:30 equity $10,124.76 vs yday $10,173.09 (-48.33) | 09:30 open · cash $191.62 (unchanged overnight, no fees) · equity $10,124.76 vs prior close $10,173.09 (-48.33) because holdings re-marked: DVN×27 yday $47.57 → 09:30 $48.00 +11.61; EOG×8 yday $146.15 → 09:30 $148.04 +15.12; FANG×6 yday $206.29 → 09:30 $208.93 +15.84; TMC×313 yday $3.77 → 09:30 $3.72 -15.65; TGB×150 yday $8.77 → 09:30 $8.55 -33.00; ELF×14 yday $93.66 → 09:30 $93.44 -3.08; DNN×391 yday $3.19 → 09:30 $3.11 -31.28; HNST×263 yday $4.70 → 09:30 $4.67 -7.89 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 27 | $48.00 | $2.09 | $+44.98 | $1,485.52 | ▲ +44.98 after sell → book $10,122.66; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 8 | $148.04 | $2.03 | $+38.11 | $2,667.81 | ▲ +38.11 after sell → book $10,120.63; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $3,919.36 | ▲ +33.34 after sell → book $10,118.60; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 313 | $3.72 | $4.10 | $-111.43 | $5,079.62 | ▼ -111.43 after sell → book $10,114.50; vs 09:30 mark -4.10 | trail off peak after 1 sess (−5%) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 150 | $8.55 | $2.48 | $+8.58 | $6,359.65 | ▲ +8.58 after sell → book $10,112.03; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 14 | $93.44 | $2.05 | $+36.52 | $7,665.76 | ▲ +36.52 after sell → book $10,109.98; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 391 | $3.11 | $5.12 | $-60.99 | $8,876.65 | ▼ -60.99 after sell → book $10,104.86; vs 09:30 mark -5.12 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `HNST` | 263 | $4.67 | $3.45 | $-43.66 | $10,101.41 | ▼ -43.66 after sell → book $10,101.41; vs 09:30 mark -3.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,101.41 | ▲ 09:30 equity $10,101.41 vs yday $10,101.41 (+0.00) | 09:30 open · cash $10,101.41 · no holdings · equity $10,101.41 vs prior close $10,101.41 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,101.41 | ▲ 09:30 equity $10,101.41 vs yday $10,101.41 (+0.00) | 09:30 open · cash $10,101.41 · no holdings · equity $10,101.41 vs prior close $10,101.41 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 61 | $20.55 | $2.17 | — | $8,845.69 | — | after min-hold, trail 5% off peak; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1262.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,660.53 | — | after min-hold, trail 5% off peak; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1262.68 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 61 | $20.65 | $2.17 | — | $6,398.71 | — | after min-hold, trail 5% off peak; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1262.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 218 | $5.77 | $2.81 | — | $5,138.03 | — | after min-hold, trail 5% off peak; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1262.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 64 | $19.63 | $2.18 | — | $3,879.53 | — | after min-hold, trail 5% off peak; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1262.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $2,632.96 | — | after min-hold, trail 5% off peak; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1262.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 721 | $1.75 | $9.30 | — | $1,361.90 | — | after min-hold, trail 5% off peak; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1262.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $203.57 | — | after min-hold, trail 5% off peak; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1262.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $203.57 | ▲ 09:30 equity $10,580.85 vs yday $10,311.13 (+269.72) | 09:30 open · cash $203.57 (unchanged overnight, no fees) · equity $10,580.85 vs prior close $10,311.13 (+269.72) because holdings re-marked: AG×61 yday $21.19 → 09:30 $21.90 +43.31; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×61 yday $21.11 → 09:30 $21.75 +39.04; HDSN×218 yday $5.57 → 09:30 $5.67 +21.80; IAG×64 yday $20.50 → 09:30 $21.17 +42.88; KGC×42 yday $31.43 → 09:30 $32.17 +31.08; NFGC×721 yday $1.75 → 09:30 $1.79 +28.84; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 61 | $21.90 | $2.19 | $+77.98 | $1,537.28 | ▲ +77.98 after sell → book $10,578.66; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,779.59 | ▲ +57.15 after sell → book $10,576.61; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 61 | $21.75 | $2.19 | $+62.73 | $4,104.14 | ▲ +62.73 after sell → book $10,574.41; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 218 | $5.67 | $2.86 | $-27.47 | $5,337.35 | ▼ -27.47 after sell → book $10,571.56; vs 09:30 mark -2.85 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 64 | $21.17 | $2.20 | $+94.17 | $6,690.02 | ▲ +94.17 after sell → book $10,569.35; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 42 | $32.17 | $2.14 | $+102.43 | $8,039.02 | ▲ +102.43 after sell → book $10,567.21; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 721 | $1.79 | $9.43 | $+10.11 | $9,320.18 | ▲ +10.11 after sell → book $10,557.78; vs 09:30 mark -9.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $10,555.75 | ▲ +77.23 after sell → book $10,555.75; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 11 | $119.43 | $2.02 | — | $9,240.00 | — | after min-hold, trail 5% off peak; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $1319.47 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 76 | $17.20 | $2.22 | — | $7,930.58 | — | after min-hold, trail 5% off peak; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1319.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 6 | $216.30 | $2.01 | — | $6,630.77 | — | after min-hold, trail 5% off peak; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1319.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 118 | $11.13 | $2.34 | — | $5,315.09 | — | after min-hold, trail 5% off peak; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1319.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 534 | $2.47 | $6.89 | — | $3,989.22 | — | after min-hold, trail 5% off peak; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1319.47 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 683 | $1.93 | $8.81 | — | $2,662.22 | — | after min-hold, trail 5% off peak; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1319.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 22 | $59.72 | $2.06 | — | $1,346.32 | — | after min-hold, trail 5% off peak; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1319.47 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 999 | $1.32 | $12.89 | — | $14.75 | — | after min-hold, trail 5% off peak; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1319.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.75 | ▲ 09:30 equity $11,127.71 vs yday $10,781.93 (+345.78) | 09:30 open · cash $14.75 (unchanged overnight, no fees) · equity $11,127.71 vs prior close $10,781.93 (+345.78) because holdings re-marked: AU×11 yday $121.22 → 09:30 $120.50 -7.92; AUPH×76 yday $16.65 → 09:30 $16.60 -3.80; AEM×6 yday $216.06 → 09:30 $217.03 +5.82; ARCT×118 yday $13.45 → 09:30 $13.26 -22.42; AUTL×534 yday $2.41 → 09:30 $2.36 -26.70; CRDL×683 yday $1.86 → 09:30 $1.87 +6.83; CRSP×22 yday $59.50 → 09:30 $58.79 -15.62; CYPH×999 yday $1.42 → 09:30 $1.83 +409.59 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 11 | $120.50 | $2.04 | $+7.70 | $1,338.21 | ▲ +7.70 after sell → book $11,125.67; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 76 | $16.60 | $2.24 | $-50.06 | $2,597.57 | ▼ -50.06 after sell → book $11,123.43; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 6 | $217.03 | $2.03 | $+0.34 | $3,897.72 | ▲ +0.34 after sell → book $11,121.40; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 118 | $13.26 | $2.38 | $+246.62 | $5,460.03 | ▲ +246.62 after sell → book $11,119.03; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 534 | $2.36 | $6.99 | $-72.62 | $6,713.28 | ▼ -72.62 after sell → book $11,112.04; vs 09:30 mark -6.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 683 | $1.87 | $8.93 | $-58.72 | $7,981.55 | ▼ -58.72 after sell → book $11,103.10; vs 09:30 mark -8.94 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 22 | $58.79 | $2.08 | $-24.59 | $9,272.86 | ▼ -24.59 after sell → book $11,101.03; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 999 | $1.83 | $13.07 | $+483.54 | $11,087.96 | ▲ +483.54 after sell → book $11,087.96; vs 09:30 mark -13.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,087.96 | ▲ 09:30 equity $11,087.96 vs yday $11,087.96 (+0.00) | 09:30 open · cash $11,087.96 · no holdings · equity $11,087.96 vs prior close $11,087.96 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 57 | $24.00 | $2.16 | — | $9,717.80 | — | after min-hold, trail 5% off peak; list flatten; ⚪; ret5=+13.0; leftover $1386.00 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 126 | $10.92 | $2.37 | — | $8,339.51 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ret5=+10.4; leftover $1386.00 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 22 | $61.47 | $2.06 | — | $6,985.12 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ret5=+9.2; leftover $1386.00 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 167 | $8.28 | $2.49 | — | $5,599.86 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1386.00 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 265 | $5.23 | $3.42 | — | $4,210.50 | — | after min-hold, trail 5% off peak; list flatten; ret5=+10.7; leftover $1386.00 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $429.24 | $2.00 | — | $2,920.78 | — | after min-hold, trail 5% off peak; list flatten; ret5=+6.1; leftover $1386.00 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 855 | $1.62 | $11.03 | — | $1,524.65 | — | after min-hold, trail 5% off peak; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1386.00 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 692 | $2.00 | $8.93 | — | $131.72 | — | after min-hold, trail 5% off peak; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $1386.00 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $131.72 | ▲ 09:30 equity $11,058.23 vs yday $11,058.23 (+0.00) | 09:30 open · cash $131.72 (unchanged overnight, no fees) · equity $11,058.23 vs prior close $11,058.23 (+0.00) because holdings re-marked: MOS×57 yday $23.75 → 09:30 $23.75 +0.00; OCUL×126 yday $10.92 → 09:30 $10.92 +0.00; INSP×22 yday $61.47 → 09:30 $61.47 +0.00; CRMD×167 yday $8.28 → 09:30 $8.28 +0.00; RZLT×265 yday $5.29 → 09:30 $5.29 +0.00; HCA×3 yday $428.50 → 09:30 $428.50 +0.00; BMEA×855 yday $1.61 → 09:30 $1.61 +0.00; NPWR×692 yday $2.02 → 09:30 $2.02 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $131.72 | ▲ 09:30 equity $11,106.84 vs yday $11,053.51 (+53.33) | 09:30 open · cash $131.72 (unchanged overnight, no fees) · equity $11,106.84 vs prior close $11,053.51 (+53.33) because holdings re-marked: MOS×57 yday $23.75 → 09:30 $24.84 +62.13; OCUL×126 yday $10.92 → 09:30 $10.79 -16.38; INSP×22 yday $61.47 → 09:30 $60.07 -30.80; CRMD×167 yday $8.28 → 09:30 $8.60 +53.44; RZLT×265 yday $5.29 → 09:30 $5.01 -74.20; HCA×3 yday $428.50 → 09:30 $427.50 -3.00; BMEA×855 yday $1.61 → 09:30 $1.75 +119.70; NPWR×692 yday $2.02 → 09:30 $1.93 -62.28 | — |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 126 | $10.79 | $2.40 | $-21.15 | $1,488.86 | ▼ -21.15 after sell → book $11,104.44; vs 09:30 mark -2.40 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 22 | $60.07 | $2.08 | $-34.93 | $2,808.32 | ▼ -34.93 after sell → book $11,102.36; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 167 | $8.60 | $2.53 | $+48.42 | $4,241.99 | ▲ +48.42 after sell → book $11,099.83; vs 09:30 mark -2.53 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 265 | $5.01 | $3.47 | $-65.19 | $5,566.17 | ▼ -65.19 after sell → book $11,096.36; vs 09:30 mark -3.47 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 3 | $427.50 | $2.02 | $-9.24 | $6,846.65 | ▼ -9.24 after sell → book $11,094.34; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BMEA` | 855 | $1.75 | $11.18 | $+88.94 | $8,331.72 | ▲ +88.94 after sell → book $11,083.16; vs 09:30 mark -11.18 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NPWR` | 692 | $1.93 | $9.05 | $-66.42 | $9,658.23 | ▼ -66.42 after sell → book $11,074.11; vs 09:30 mark -9.05 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 33 | $40.72 | $2.09 | — | $8,312.38 | — | after min-hold, trail 5% off peak; list flatten; ret5=+1.8; leftover $1379.75 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 97 | $14.09 | $2.28 | — | $6,943.37 | — | after min-hold, trail 5% off peak; list flatten; ret5=+1.1; leftover $1379.75 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 532 | $2.59 | $6.86 | — | $5,558.62 | — | after min-hold, trail 5% off peak; list flatten; ret5=+4.2; leftover $1379.75 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 17 | $80.97 | $2.04 | — | $4,180.09 | — | after min-hold, trail 5% off peak; list mover_buy; 🔵; ret5=-1.3; leftover $1379.75 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 312 | $4.42 | $4.02 | — | $2,797.03 | — | after min-hold, trail 5% off peak; list mover_buy; 🔵; ret5=-8.6; leftover $1379.75 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 18 | $75.12 | $2.04 | — | $1,442.82 | — | after min-hold, trail 5% off peak; list mover_buy; 🔵; ret5=-2.2; leftover $1379.75 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $925.74 | $1.99 | — | $515.09 | — | after min-hold, trail 5% off peak; list mover_buy; 🔵; ret5=-0.5; leftover $1379.75 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $515.09 | ▲ 09:30 equity $11,155.17 vs yday $11,074.71 (+80.46) | 09:30 open · cash $515.09 (unchanged overnight, no fees) · equity $11,155.17 vs prior close $11,074.71 (+80.46) because holdings re-marked: MOS×57 yday $24.16 → 09:30 $24.00 -9.12; RRC×33 yday $41.55 → 09:30 $41.44 -3.63; CRK×97 yday $14.50 → 09:30 $14.42 -7.76; SLI×532 yday $2.61 → 09:30 $2.60 -5.32; ACMR×17 yday $79.11 → 09:30 $81.65 +43.18; GGB×312 yday $4.46 → 09:30 $4.57 +34.32; MT×18 yday $74.53 → 09:30 $74.54 +0.18; MU×1 yday $938.40 → 09:30 $967.01 +28.61 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 17 | $81.65 | $2.06 | $+7.46 | $1,901.08 | ▲ +7.46 after sell → book $11,153.11; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 312 | $4.57 | $4.09 | $+38.69 | $3,322.83 | ▲ +38.69 after sell → book $11,149.02; vs 09:30 mark -4.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 18 | $74.54 | $2.06 | $-14.55 | $4,662.49 | ▼ -14.55 after sell → book $11,146.96; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $967.01 | $2.01 | $+37.26 | $5,627.48 | ▲ +37.26 after sell → book $11,144.94; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $144.70 | $2.02 | — | $4,323.17 | — | after min-hold, trail 5% off peak; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1406.87 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 83 | $16.95 | $2.24 | — | $2,914.08 | — | after min-hold, trail 5% off peak; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1406.87 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 76 | $18.50 | $2.22 | — | $1,505.86 | — | after min-hold, trail 5% off peak; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1406.87 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 153 | $9.19 | $2.45 | — | $97.34 | — | after min-hold, trail 5% off peak; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1406.87 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.34 | ▼ 09:30 equity $10,978.87 vs yday $11,205.29 (-226.42) | 09:30 open · cash $97.34 (unchanged overnight, no fees) · equity $10,978.87 vs prior close $11,205.29 (-226.42) because holdings re-marked: MOS×57 yday $23.76 → 09:30 $23.75 -0.57; RRC×33 yday $41.64 → 09:30 $41.11 -17.49; CRK×97 yday $14.62 → 09:30 $14.56 -5.82; SLI×532 yday $2.64 → 09:30 $2.51 -69.16; ANF×9 yday $145.75 → 09:30 $148.67 +26.28; BHVN×83 yday $16.12 → 09:30 $15.44 -56.44; BZ×76 yday $18.00 → 09:30 $17.89 -8.36; CAPR×153 yday $10.06 → 09:30 $9.44 -94.86 | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 57 | $23.75 | $2.18 | $-18.59 | $1,448.91 | ▼ -18.59 after sell → book $10,976.69; vs 09:30 mark -2.18 | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 33 | $41.11 | $2.11 | $+8.67 | $2,803.43 | ▲ +8.67 after sell → book $10,974.58; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 97 | $14.56 | $2.31 | $+41.00 | $4,213.44 | ▲ +41.00 after sell → book $10,972.27; vs 09:30 mark -2.31 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 532 | $2.51 | $6.96 | $-56.38 | $5,541.80 | ▼ -56.38 after sell → book $10,965.31; vs 09:30 mark -6.96 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.67 | $2.04 | $+31.68 | $6,877.79 | ▲ +31.68 after sell → book $10,963.27; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BHVN` | 83 | $15.44 | $2.26 | $-129.83 | $8,157.05 | ▼ -129.83 after sell → book $10,961.01; vs 09:30 mark -2.26 | trail off peak after 1 sess (−5%) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 76 | $17.89 | $2.24 | $-50.82 | $9,514.45 | ▼ -50.82 after sell → book $10,958.77; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 153 | $9.44 | $2.49 | $+33.31 | $10,956.28 | ▲ +33.31 after sell → book $10,956.28; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,956.28 | ▲ 09:30 equity $10,956.28 vs yday $10,956.28 (-0.00) | 09:30 open · cash $10,956.28 · no holdings · equity $10,956.28 vs prior close $10,956.28 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,956.28 | ▲ 09:30 equity $10,956.28 vs yday $10,956.28 (-0.00) | 09:30 open · cash $10,956.28 · no holdings · equity $10,956.28 vs prior close $10,956.28 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,956.28 | ▲ 09:30 equity $10,956.28 vs yday $10,956.28 (-0.00) | 09:30 open · cash $10,956.28 · no holdings · equity $10,956.28 vs prior close $10,956.28 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 27 | $49.76 | $2.07 | — | $9,610.69 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1369.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 33 | $41.31 | $2.09 | — | $8,245.37 | — | after min-hold, trail 5% off peak; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1369.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 418 | $3.27 | $5.39 | — | $6,873.12 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1369.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 177 | $7.70 | $2.52 | — | $5,507.70 | — | after min-hold, trail 5% off peak; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1369.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $125.94 | $2.02 | — | $4,246.28 | — | after min-hold, trail 5% off peak; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1369.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1122 | $1.22 | $14.47 | — | $2,862.96 | — | after min-hold, trail 5% off peak; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1369.53 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 74 | $18.40 | $2.21 | — | $1,499.15 | — | after min-hold, trail 5% off peak; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1369.53 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 87 | $15.70 | $2.25 | — | $131.00 | — | after min-hold, trail 5% off peak; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1369.53 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $131.00 | ▲ 09:30 equity $11,931.89 vs yday $11,765.19 (+166.70) | 09:30 open · cash $131.00 (unchanged overnight, no fees) · equity $11,931.89 vs prior close $11,765.19 (+166.70) because holdings re-marked: ATRC×27 yday $52.59 → 09:30 $52.88 +7.83; HRMY×33 yday $42.86 → 09:30 $42.93 +2.31; CABA×418 yday $3.57 → 09:30 $3.63 +25.08; VSTM×177 yday $8.02 → 09:30 $8.03 +1.77; RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; GPRO×1122 yday $1.69 → 09:30 $1.78 +100.98; FRVO×74 yday $17.98 → 09:30 $18.27 +21.46; CRK×87 yday $15.54 → 09:30 $15.45 -7.83 | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 33 | $42.93 | $2.11 | $+49.26 | $1,545.58 | ▲ +49.26 after sell → book $11,929.78; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 177 | $8.03 | $2.56 | $+53.33 | $2,964.33 | ▲ +53.33 after sell → book $11,927.22; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $132.45 | $2.04 | $+61.04 | $4,286.79 | ▲ +61.04 after sell → book $11,925.18; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FRVO` | 74 | $18.27 | $2.24 | $-14.07 | $5,636.53 | ▼ -14.07 after sell → book $11,922.94; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 87 | $15.45 | $2.28 | $-26.28 | $6,978.41 | ▼ -26.28 after sell → book $11,920.67; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASND` | 5 | $266.94 | $2.00 | — | $5,641.70 | — | after min-hold, trail 5% off peak; list flatten; ret5=+1.9; leftover $1395.68 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OSCR` | 45 | $30.65 | $2.12 | — | $4,260.33 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ret5=-2.2; leftover $1395.68 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 134 | $10.41 | $2.39 | — | $2,862.99 | — | after min-hold, trail 5% off peak; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $1395.68 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 96 | $14.50 | $2.28 | — | $1,468.72 | — | after min-hold, trail 5% off peak; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1395.68 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 715 | $1.95 | $9.22 | — | $65.24 | — | after min-hold, trail 5% off peak; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1395.68 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `BMEA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `NPWR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
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
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 27 | 2026-09-03 @ $49.76 | after min-hold, trail 5% off peak; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1369.53 |
| `CABA` | 418 | 2026-09-03 @ $3.27 | after min-hold, trail 5% off peak; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1369.53 |
| `GPRO` | 1122 | 2026-09-03 @ $1.22 | after min-hold, trail 5% off peak; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1369.53 |
| `ASND` | 5 | 2026-09-04 @ $266.94 | after min-hold, trail 5% off peak; list flatten; ret5=+1.9; leftover $1395.68 |
| `OSCR` | 45 | 2026-09-04 @ $30.65 | after min-hold, trail 5% off peak; list flatten; 🔵; ret5=-2.2; leftover $1395.68 |
| `NVAX` | 134 | 2026-09-04 @ $10.41 | after min-hold, trail 5% off peak; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $1395.68 |
| `BVS` | 96 | 2026-09-04 @ $14.50 | after min-hold, trail 5% off peak; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1395.68 |
| `BAK` | 715 | 2026-09-04 @ $1.95 | after min-hold, trail 5% off peak; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1395.68 |
