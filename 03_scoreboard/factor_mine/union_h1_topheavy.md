# Factor mine action — `union_h1_topheavy`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `topheavy` · sell `list` · S-boost `none` · 40% to #1, rest split

Cash book **+15.68%** ($11,568) · signal-only (no cash/fees) was +18.57%. Starts YES **16/17**. Fills 134 · skips 53 · realized $+1116.60.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `topheavy` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $92.26.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $160.57 | $10,123.05 | BTSG×66, IREN×18, TPG×16, TGTX×17, SLS×73, HIMS×28, INO×1058, TNDM×36 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $160.57 | BTSG×66, IREN×18, TPG×16, TGTX×17, SLS×73, HIMS×28, INO×1058, TNDM×36 | $10,109.78 | -13.27 | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $465.70 | $10,139.92 | TLN×11, VST×5, NRG×7, DAVE×2, SLG×14, MARA×95, LDI×922, BTBT×576 | 09:30 open · cash $160.57 (unchanged overnight, no fees) · equity $10,109.78 vs prior close $10,123.05 (-13.27) because holdings re-marked: BTSG×66 yday $60.23 → 09:30 $59.65 -38.28; IREN×18 yday $44.76 → 09:30 $44.09 -12.06; TPG×16 yday $54.62 → 09:30 $55.29 +10.72; TGTX×17 yday $47.94 → 09:30 $47.27 -11.39; SLS×73 yday $12.36 → 09:30 $12.40 +2.92; HIMS×28 yday $28.77 → 09:30 $29.15 +10.64; INO×1058 yday $0.90 → 09:30 $0.93 +31.74; TNDM×36 yday $23.13 → 09:30 $22.92 -7.56 |
| 2026-08-17 | +2.25 | $465.70 | TLN×11, VST×5, NRG×7, DAVE×2, SLG×14, MARA×95, LDI×922, BTBT×576 | $10,187.76 | +47.84 | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | $173.44 | $10,259.27 | DVN×87, EOG×6, FANG×4, TMC×214, TGB×102, ELF×9, DNN×268, HNST×180 | 09:30 open · cash $465.70 (unchanged overnight, no fees) · equity $10,187.76 vs prior close $10,139.92 (+47.84) because holdings re-marked: TLN×11 yday $362.74 → 09:30 $367.88 +56.54; VST×5 yday $148.13 → 09:30 $149.37 +6.20; NRG×7 yday $126.24 → 09:30 $127.40 +8.12; DAVE×2 yday $334.57 → 09:30 $336.94 +4.74; SLG×14 yday $56.09 → 09:30 $55.37 -10.08; MARA×95 yday $9.20 → 09:30 $9.22 +1.90; LDI×922 yday $0.90 → 09:30 $0.91 +9.22; BTBT×576 yday $1.57 → 09:30 $1.52 -28.80 |
| 2026-08-18 | -6.20 | $173.44 | DVN×87, EOG×6, FANG×4, TMC×214, TGB×102, ELF×9, DNN×268, HNST×180 | $10,256.62 | -2.65 | — | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST | $10,237.02 | $10,237.02 | — | 09:30 open · cash $173.44 (unchanged overnight, no fees) · equity $10,256.62 vs prior close $10,259.27 (-2.65) because holdings re-marked: DVN×87 yday $47.57 → 09:30 $48.00 +37.41; EOG×6 yday $146.15 → 09:30 $148.04 +11.34; FANG×4 yday $206.29 → 09:30 $208.93 +10.56; TMC×214 yday $3.77 → 09:30 $3.72 -10.70; TGB×102 yday $8.77 → 09:30 $8.55 -22.44; ELF×9 yday $93.66 → 09:30 $93.44 -1.98; DNN×268 yday $3.19 → 09:30 $3.11 -21.44; HNST×180 yday $4.70 → 09:30 $4.67 -5.40 |
| 2026-08-19 | -7.20 | $10,237.02 | — | $10,237.02 | +0.00 | — | — | $10,237.02 | $10,237.02 | — | 09:30 open · cash $10,237.02 · no holdings · equity $10,237.02 vs prior close $10,237.02 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-20 | +1.12 | $10,237.02 | — | $10,237.02 | +0.00 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $95.33 | $10,479.79 | AG×199, BHP×9, CDE×42, HDSN×152, IAG×44, KGC×29, NFGC×501, WPM×6 | 09:30 open · cash $10,237.02 · no holdings · equity $10,237.02 vs prior close $10,237.02 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-21 | +3.25 | $95.33 | AG×199, BHP×9, CDE×42, HDSN×152, IAG×44, KGC×29, NFGC×501, WPM×6 | $10,779.65 | +299.86 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $79.01 | $10,962.33 | AU×36, AUPH×53, AEM×4, ARCT×82, AUTL×373, CRDL×477, CRSP×15, CYPH×698 | 09:30 open · cash $95.33 (unchanged overnight, no fees) · equity $10,779.65 vs prior close $10,479.79 (+299.86) because holdings re-marked: AG×199 yday $21.19 → 09:30 $21.90 +141.29; BHP×9 yday $93.63 → 09:30 $95.72 +18.81; CDE×42 yday $21.11 → 09:30 $21.75 +26.88; HDSN×152 yday $5.57 → 09:30 $5.67 +15.20; IAG×44 yday $20.50 → 09:30 $21.17 +29.48; KGC×29 yday $31.43 → 09:30 $32.17 +21.46; NFGC×501 yday $1.75 → 09:30 $1.79 +20.04; WPM×6 yday $150.25 → 09:30 $154.70 +26.70 |
| 2026-08-24 | -5.17 | $79.01 | AU×36, AUPH×53, AEM×4, ARCT×82, AUTL×373, CRDL×477, CRSP×15, CYPH×698 | $11,183.71 | +221.38 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $11,152.80 | $11,152.80 | — | 09:30 open · cash $79.01 (unchanged overnight, no fees) · equity $11,183.71 vs prior close $10,962.33 (+221.38) because holdings re-marked: AU×36 yday $121.22 → 09:30 $120.50 -25.92; AUPH×53 yday $16.65 → 09:30 $16.60 -2.65; AEM×4 yday $216.06 → 09:30 $217.03 +3.88; ARCT×82 yday $13.45 → 09:30 $13.26 -15.58; AUTL×373 yday $2.41 → 09:30 $2.36 -18.65; CRDL×477 yday $1.86 → 09:30 $1.87 +4.77; CRSP×15 yday $59.50 → 09:30 $58.79 -10.65; CYPH×698 yday $1.42 → 09:30 $1.83 +286.18 |
| 2026-08-25 | +1.80 | $11,152.80 | — | $11,152.80 | +0.00 | MOS, OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | — | $140.91 | $11,092.17 | MOS×185, OCUL×87, INSP×15, CRMD×115, RZLT×182, HCA×2, BMEA×590, NPWR×477 | 09:30 open · cash $11,152.80 · no holdings · equity $11,152.80 vs prior close $11,152.80 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-26 | +2.02 | $140.91 | MOS×185, OCUL×87, INSP×15, CRMD×115, RZLT×182, HCA×2, BMEA×590, NPWR×477 | $11,092.17 | -0.00 | — | — | $140.91 | $11,125.34 | MOS×185, OCUL×87, INSP×15, CRMD×115, RZLT×182, HCA×2, BMEA×590, NPWR×477 | 09:30 open · cash $140.91 (unchanged overnight, no fees) · equity $11,092.17 vs prior close $11,092.17 (-0.00) because holdings re-marked: MOS×185 yday $23.75 → 09:30 $23.75 +0.00; OCUL×87 yday $10.92 → 09:30 $10.92 +0.00; INSP×15 yday $61.47 → 09:30 $61.47 +0.00; CRMD×115 yday $8.28 → 09:30 $8.28 +0.00; RZLT×182 yday $5.29 → 09:30 $5.29 +0.00; HCA×2 yday $428.50 → 09:30 $428.50 +0.00; BMEA×590 yday $1.61 → 09:30 $1.61 +0.00; NPWR×477 yday $2.02 → 09:30 $2.02 +0.00 |
| 2026-08-27 | — | $140.91 | MOS×185, OCUL×87, INSP×15, CRMD×115, RZLT×182, HCA×2, BMEA×590, NPWR×477 | $11,285.02 | +159.68 | RRC, CRK, SLI, ACMR, GGB, MT | OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | $763.89 | $11,184.63 | MOS×185, RRC×65, CRK×47, SLI×257, ACMR×8, GGB×150, MT×8 | 09:30 open · cash $140.91 (unchanged overnight, no fees) · equity $11,285.02 vs prior close $11,125.34 (+159.68) because holdings re-marked: MOS×185 yday $23.75 → 09:30 $24.84 +201.65; OCUL×87 yday $10.92 → 09:30 $10.79 -11.31; INSP×15 yday $61.47 → 09:30 $60.07 -21.00; CRMD×115 yday $8.28 → 09:30 $8.60 +36.80; RZLT×182 yday $5.29 → 09:30 $5.01 -50.96; HCA×2 yday $428.50 → 09:30 $427.50 -2.00; BMEA×590 yday $1.61 → 09:30 $1.75 +82.60; NPWR×477 yday $2.02 → 09:30 $1.93 -42.93 |
| 2026-08-28 | +0.75 | $763.89 | MOS×185, RRC×65, CRK×47, SLI×257, ACMR×8, GGB×150, MT×8 | $11,178.45 | -6.18 | ANF, BHVN, BZ, CAPR | ACMR, GGB, MT | $76.16 | $11,169.43 | MOS×185, RRC×65, CRK×47, SLI×257, ANF×7, BHVN×31, BZ×29, CAPR×58 | 09:30 open · cash $763.89 (unchanged overnight, no fees) · equity $11,178.45 vs prior close $11,184.63 (-6.18) because holdings re-marked: MOS×185 yday $24.16 → 09:30 $24.00 -29.60; RRC×65 yday $41.55 → 09:30 $41.44 -7.15; CRK×47 yday $14.50 → 09:30 $14.42 -3.76; SLI×257 yday $2.61 → 09:30 $2.60 -2.57; ACMR×8 yday $79.11 → 09:30 $81.65 +20.32; GGB×150 yday $4.46 → 09:30 $4.57 +16.50; MT×8 yday $74.53 → 09:30 $74.54 +0.08 |
| 2026-08-31 | -5.85 | $76.16 | MOS×185, RRC×65, CRK×47, SLI×257, ANF×7, BHVN×31, BZ×29, CAPR×58 | $11,057.11 | -112.32 | — | MOS, RRC, CRK, SLI, ANF, BHVN, BZ, CAPR | $11,038.35 | $11,038.35 | — | 09:30 open · cash $76.16 (unchanged overnight, no fees) · equity $11,057.11 vs prior close $11,169.43 (-112.32) because holdings re-marked: MOS×185 yday $23.76 → 09:30 $23.75 -1.85; RRC×65 yday $41.64 → 09:30 $41.11 -34.45; CRK×47 yday $14.62 → 09:30 $14.56 -2.82; SLI×257 yday $2.64 → 09:30 $2.51 -33.41; ANF×7 yday $145.75 → 09:30 $148.67 +20.44; BHVN×31 yday $16.12 → 09:30 $15.44 -21.08; BZ×29 yday $18.00 → 09:30 $17.89 -3.19; CAPR×58 yday $10.06 → 09:30 $9.44 -35.96 |
| 2026-09-01 | -6.30 | $11,038.35 | — | $11,038.35 | +0.00 | — | — | $11,038.35 | $11,038.35 | — | 09:30 open · cash $11,038.35 · no holdings · equity $11,038.35 vs prior close $11,038.35 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-02 | -3.83 | $11,038.35 | — | $11,038.35 | +0.00 | — | — | $11,038.35 | $11,038.35 | — | 09:30 open · cash $11,038.35 · no holdings · equity $11,038.35 vs prior close $11,038.35 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-03 | -0.90 | $11,038.35 | — | $11,038.35 | +0.00 | ATRC, HRMY, CABA, VSTM, RVTY, GPRO, FRVO, CRK | — | $132.03 | $11,788.75 | ATRC×88, HRMY×22, CABA×289, VSTM×122, RVTY×7, GPRO×775, FRVO×51, CRK×60 | 09:30 open · cash $11,038.35 · no holdings · equity $11,038.35 vs prior close $11,038.35 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $132.03 | ATRC×88, HRMY×22, CABA×289, VSTM×122, RVTY×7, GPRO×775, FRVO×51, CRK×60 | $11,924.08 | +135.33 | ASND, OSCR, NVAX, BVS, BAK | HRMY, VSTM, RVTY, FRVO, CRK | $92.26 | $11,567.91 | ATRC×88, CABA×289, GPRO×775, ASND×7, OSCR×23, NVAX×69, BVS×49, BAK×371 | 09:30 open · cash $132.03 (unchanged overnight, no fees) · equity $11,924.08 vs prior close $11,788.75 (+135.33) because holdings re-marked: ATRC×88 yday $52.59 → 09:30 $52.88 +25.52; HRMY×22 yday $42.86 → 09:30 $42.93 +1.54; CABA×289 yday $3.57 → 09:30 $3.63 +17.34; VSTM×122 yday $8.02 → 09:30 $8.03 +1.22; RVTY×7 yday $130.94 → 09:30 $132.45 +10.57; GPRO×775 yday $1.69 → 09:30 $1.78 +69.75; FRVO×51 yday $17.98 → 09:30 $18.27 +14.79; CRK×60 yday $15.54 → 09:30 $15.45 -5.40 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 66 | $59.80 | $2.19 | — | $6,051.01 | — | 40% to #1, rest split; list flatten; ⚪; ret5=-5.3; leftover $4000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 18 | $45.98 | $2.04 | — | $5,221.33 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+12.3; leftover $857.14 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 16 | $50.62 | $2.04 | — | $4,409.32 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+6.2; leftover $857.14 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 17 | $49.70 | $2.04 | — | $3,562.38 | — | 40% to #1, rest split; list flatten; ⚪; ret5=-0.8; leftover $857.14 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 73 | $11.70 | $2.21 | — | $2,706.07 | — | 40% to #1, rest split; list flatten; ⚪; ret5=-0.8; leftover $857.14 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 28 | $29.74 | $2.07 | — | $1,871.27 | — | 40% to #1, rest split; list flatten; ⚪; ret5=-5.3; leftover $857.14 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1058 | $0.81 | $11.74 | — | $1,002.55 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+13.2; leftover $857.14 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 36 | $23.33 | $2.10 | — | $160.57 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+19.7; leftover $857.14 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $160.57 | ▼ 09:30 equity $10,109.78 vs yday $10,123.05 (-13.27) | 09:30 open · cash $160.57 (unchanged overnight, no fees) · equity $10,109.78 vs prior close $10,123.05 (-13.27) because holdings re-marked: BTSG×66 yday $60.23 → 09:30 $59.65 -38.28; IREN×18 yday $44.76 → 09:30 $44.09 -12.06; TPG×16 yday $54.62 → 09:30 $55.29 +10.72; TGTX×17 yday $47.94 → 09:30 $47.27 -11.39; SLS×73 yday $12.36 → 09:30 $12.40 +2.92; HIMS×28 yday $28.77 → 09:30 $29.15 +10.64; INO×1058 yday $0.90 → 09:30 $0.93 +31.74; TNDM×36 yday $23.13 → 09:30 $22.92 -7.56 | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 66 | $59.65 | $2.23 | $-14.32 | $4,095.24 | ▼ -14.32 after sell → book $10,107.55; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 18 | $44.09 | $2.06 | $-38.13 | $4,886.80 | ▼ -38.13 after sell → book $10,105.49; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 16 | $55.29 | $2.06 | $+70.57 | $5,769.38 | ▲ +70.57 after sell → book $10,103.43; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 17 | $47.27 | $2.06 | $-45.41 | $6,570.91 | ▼ -45.41 after sell → book $10,101.37; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 73 | $12.40 | $2.23 | $+46.66 | $7,473.88 | ▲ +46.66 after sell → book $10,099.14; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 28 | $29.15 | $2.09 | $-20.69 | $8,287.98 | ▼ -20.69 after sell → book $10,097.04; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1058 | $0.93 | $13.20 | $+102.02 | $9,258.73 | ▲ +102.02 after sell → book $10,083.85; vs 09:30 mark -13.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 36 | $22.92 | $2.12 | $-18.98 | $10,081.73 | ▼ -18.98 after sell → book $10,081.73; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 11 | $359.83 | $2.02 | — | $6,121.57 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+5.9; leftover $4032.69 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 5 | $146.90 | $2.00 | — | $5,385.07 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+3.6; leftover $864.15 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 7 | $120.00 | $2.01 | — | $4,543.06 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+0.6; leftover $864.15 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 2 | $330.91 | $2.00 | — | $3,879.24 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=-8.6; leftover $864.15 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 14 | $57.61 | $2.03 | — | $3,070.67 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+5.7; leftover $864.15 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 95 | $9.01 | $2.27 | — | $2,212.45 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=-13.5; leftover $864.15 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 922 | $0.94 | $11.41 | — | $1,337.13 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+0.5; leftover $864.15 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 576 | $1.50 | $7.43 | — | $465.70 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+9.2; leftover $864.15 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $465.70 | ▲ 09:30 equity $10,187.76 vs yday $10,139.92 (+47.84) | 09:30 open · cash $465.70 (unchanged overnight, no fees) · equity $10,187.76 vs prior close $10,139.92 (+47.84) because holdings re-marked: TLN×11 yday $362.74 → 09:30 $367.88 +56.54; VST×5 yday $148.13 → 09:30 $149.37 +6.20; NRG×7 yday $126.24 → 09:30 $127.40 +8.12; DAVE×2 yday $334.57 → 09:30 $336.94 +4.74; SLG×14 yday $56.09 → 09:30 $55.37 -10.08; MARA×95 yday $9.20 → 09:30 $9.22 +1.90; LDI×922 yday $0.90 → 09:30 $0.91 +9.22; BTBT×576 yday $1.57 → 09:30 $1.52 -28.80 | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 11 | $367.88 | $2.07 | $+84.46 | $4,510.31 | ▲ +84.46 after sell → book $10,185.69; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 5 | $149.37 | $2.02 | $+8.32 | $5,255.14 | ▲ +8.32 after sell → book $10,183.67; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 7 | $127.40 | $2.03 | $+47.76 | $6,144.90 | ▲ +47.76 after sell → book $10,181.64; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 2 | $336.94 | $2.02 | $+8.05 | $6,816.77 | ▲ +8.05 after sell → book $10,179.62; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 14 | $55.37 | $2.05 | $-35.44 | $7,589.90 | ▼ -35.44 after sell → book $10,177.57; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 95 | $9.22 | $2.30 | $+15.37 | $8,463.50 | ▲ +15.37 after sell → book $10,175.27; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 922 | $0.91 | $11.29 | $-50.36 | $9,288.46 | ▼ -50.36 after sell → book $10,163.98; vs 09:30 mark -11.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 576 | $1.52 | $7.54 | $-3.45 | $10,156.44 | ▼ -3.45 after sell → book $10,156.44; vs 09:30 mark -7.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 87 | $46.18 | $2.25 | — | $6,136.53 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+6.7; leftover $4062.58 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 6 | $142.77 | $2.01 | — | $5,277.90 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+5.8; leftover $870.55 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 4 | $202.70 | $2.00 | — | $4,465.10 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+8.3; leftover $870.55 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 214 | $4.05 | $2.76 | — | $3,595.64 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=-12.3; leftover $870.55 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 102 | $8.46 | $2.30 | — | $2,730.42 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+0.4; leftover $870.55 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 9 | $90.54 | $2.02 | — | $1,913.55 | — | 40% to #1, rest split; list flatten; ret5=-7.2; leftover $870.55 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 268 | $3.24 | $3.46 | — | $1,041.77 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+0.3; leftover $870.55 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 180 | $4.81 | $2.53 | — | $173.44 | — | 40% to #1, rest split; list flatten; ⚪; ret5=-11.4; leftover $870.55 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $173.44 | ▼ 09:30 equity $10,256.62 vs yday $10,259.27 (-2.65) | 09:30 open · cash $173.44 (unchanged overnight, no fees) · equity $10,256.62 vs prior close $10,259.27 (-2.65) because holdings re-marked: DVN×87 yday $47.57 → 09:30 $48.00 +37.41; EOG×6 yday $146.15 → 09:30 $148.04 +11.34; FANG×4 yday $206.29 → 09:30 $208.93 +10.56; TMC×214 yday $3.77 → 09:30 $3.72 -10.70; TGB×102 yday $8.77 → 09:30 $8.55 -22.44; ELF×9 yday $93.66 → 09:30 $93.44 -1.98; DNN×268 yday $3.19 → 09:30 $3.11 -21.44; HNST×180 yday $4.70 → 09:30 $4.67 -5.40 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 87 | $48.00 | $2.30 | $+153.79 | $4,347.14 | ▲ +153.79 after sell → book $10,254.32; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 6 | $148.04 | $2.03 | $+27.58 | $5,233.35 | ▲ +27.58 after sell → book $10,252.29; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 4 | $208.93 | $2.02 | $+20.90 | $6,067.05 | ▲ +20.90 after sell → book $10,250.27; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 214 | $3.72 | $2.81 | $-76.19 | $6,860.33 | ▼ -76.19 after sell → book $10,247.47; vs 09:30 mark -2.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 102 | $8.55 | $2.32 | $+4.56 | $7,730.10 | ▲ +4.56 after sell → book $10,245.14; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 9 | $93.44 | $2.04 | $+22.05 | $8,569.03 | ▲ +22.05 after sell → book $10,243.11; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 268 | $3.11 | $3.51 | $-41.81 | $9,398.99 | ▼ -41.81 after sell → book $10,239.59; vs 09:30 mark -3.52 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `HNST` | 180 | $4.67 | $2.57 | $-30.30 | $10,237.02 | ▼ -30.30 after sell → book $10,237.02; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,237.02 | ▲ 09:30 equity $10,237.02 vs yday $10,237.02 (+0.00) | 09:30 open · cash $10,237.02 · no holdings · equity $10,237.02 vs prior close $10,237.02 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,237.02 | ▲ 09:30 equity $10,237.02 vs yday $10,237.02 (+0.00) | 09:30 open · cash $10,237.02 · no holdings · equity $10,237.02 vs prior close $10,237.02 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 199 | $20.55 | $2.59 | — | $6,144.99 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $4094.81 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 9 | $91.01 | $2.02 | — | $5,323.88 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $877.46 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 42 | $20.65 | $2.12 | — | $4,454.46 | — | 40% to #1, rest split; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $877.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 152 | $5.77 | $2.45 | — | $3,574.98 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $877.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 44 | $19.63 | $2.12 | — | $2,709.14 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $877.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 29 | $29.63 | $2.08 | — | $1,847.79 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $877.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 501 | $1.75 | $6.46 | — | $964.58 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $877.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 6 | $144.54 | $2.01 | — | $95.33 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $877.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.33 | ▲ 09:30 equity $10,779.65 vs yday $10,479.79 (+299.86) | 09:30 open · cash $95.33 (unchanged overnight, no fees) · equity $10,779.65 vs prior close $10,479.79 (+299.86) because holdings re-marked: AG×199 yday $21.19 → 09:30 $21.90 +141.29; BHP×9 yday $93.63 → 09:30 $95.72 +18.81; CDE×42 yday $21.11 → 09:30 $21.75 +26.88; HDSN×152 yday $5.57 → 09:30 $5.67 +15.20; IAG×44 yday $20.50 → 09:30 $21.17 +29.48; KGC×29 yday $31.43 → 09:30 $32.17 +21.46; NFGC×501 yday $1.75 → 09:30 $1.79 +20.04; WPM×6 yday $150.25 → 09:30 $154.70 +26.70 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 199 | $21.90 | $2.65 | $+263.41 | $4,450.77 | ▲ +263.41 after sell → book $10,776.99; vs 09:30 mark -2.66 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 9 | $95.72 | $2.04 | $+38.34 | $5,310.22 | ▲ +38.34 after sell → book $10,774.96; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 42 | $21.75 | $2.14 | $+41.95 | $6,221.58 | ▲ +41.95 after sell → book $10,772.82; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 152 | $5.67 | $2.48 | $-20.13 | $7,080.94 | ▼ -20.13 after sell → book $10,770.34; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 44 | $21.17 | $2.14 | $+63.50 | $8,010.28 | ▲ +63.50 after sell → book $10,768.20; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 29 | $32.17 | $2.10 | $+69.49 | $8,941.11 | ▲ +69.49 after sell → book $10,766.10; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 501 | $1.79 | $6.56 | $+7.02 | $9,831.34 | ▲ +7.02 after sell → book $10,759.54; vs 09:30 mark -6.56 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 6 | $154.70 | $2.03 | $+56.92 | $10,757.52 | ▲ +56.92 after sell → book $10,757.52; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 36 | $119.43 | $2.10 | — | $6,455.94 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $4303.01 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 53 | $17.20 | $2.15 | — | $5,542.19 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $922.07 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 4 | $216.30 | $2.00 | — | $4,674.99 | — | 40% to #1, rest split; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $922.07 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 82 | $11.13 | $2.24 | — | $3,760.09 | — | 40% to #1, rest split; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $922.07 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 373 | $2.47 | $4.81 | — | $2,833.97 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $922.07 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 477 | $1.93 | $6.15 | — | $1,907.21 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $922.07 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 15 | $59.72 | $2.04 | — | $1,009.37 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $922.07 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 698 | $1.32 | $9.00 | — | $79.01 | — | 40% to #1, rest split; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $922.07 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.01 | ▲ 09:30 equity $11,183.71 vs yday $10,962.33 (+221.38) | 09:30 open · cash $79.01 (unchanged overnight, no fees) · equity $11,183.71 vs prior close $10,962.33 (+221.38) because holdings re-marked: AU×36 yday $121.22 → 09:30 $120.50 -25.92; AUPH×53 yday $16.65 → 09:30 $16.60 -2.65; AEM×4 yday $216.06 → 09:30 $217.03 +3.88; ARCT×82 yday $13.45 → 09:30 $13.26 -15.58; AUTL×373 yday $2.41 → 09:30 $2.36 -18.65; CRDL×477 yday $1.86 → 09:30 $1.87 +4.77; CRSP×15 yday $59.50 → 09:30 $58.79 -10.65; CYPH×698 yday $1.42 → 09:30 $1.83 +286.18 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 36 | $120.50 | $2.14 | $+34.28 | $4,414.86 | ▲ +34.28 after sell → book $11,181.56; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 53 | $16.60 | $2.17 | $-36.12 | $5,292.49 | ▼ -36.12 after sell → book $11,179.39; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 4 | $217.03 | $2.02 | $-1.10 | $6,158.59 | ▼ -1.10 after sell → book $11,177.37; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 82 | $13.26 | $2.26 | $+170.16 | $7,243.65 | ▲ +170.16 after sell → book $11,175.11; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 373 | $2.36 | $4.88 | $-50.73 | $8,119.05 | ▼ -50.73 after sell → book $11,170.23; vs 09:30 mark -4.88 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 477 | $1.87 | $6.24 | $-41.02 | $9,004.80 | ▼ -41.02 after sell → book $11,163.99; vs 09:30 mark -6.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 15 | $58.79 | $2.06 | $-18.04 | $9,884.59 | ▼ -18.04 after sell → book $11,161.93; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 698 | $1.83 | $9.13 | $+337.85 | $11,152.80 | ▲ +337.85 after sell → book $11,152.80; vs 09:30 mark -9.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,152.80 | ▲ 09:30 equity $11,152.80 vs yday $11,152.80 (+0.00) | 09:30 open · cash $11,152.80 · no holdings · equity $11,152.80 vs prior close $11,152.80 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 185 | $24.00 | $2.54 | — | $6,710.26 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+13.0; leftover $4461.12 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 87 | $10.92 | $2.25 | — | $5,757.97 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+10.4; leftover $955.95 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 15 | $61.47 | $2.04 | — | $4,833.88 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+9.2; leftover $955.95 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 115 | $8.28 | $2.33 | — | $3,879.35 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+8.8; leftover $955.95 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 182 | $5.23 | $2.54 | — | $2,924.95 | — | 40% to #1, rest split; list flatten; ret5=+10.7; leftover $955.95 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 2 | $429.24 | $2.00 | — | $2,064.47 | — | 40% to #1, rest split; list flatten; ret5=+6.1; leftover $955.95 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 590 | $1.62 | $7.61 | — | $1,101.06 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $955.95 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 477 | $2.00 | $6.15 | — | $140.91 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $955.95 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $140.91 | ▲ 09:30 equity $11,092.17 vs yday $11,092.17 (-0.00) | 09:30 open · cash $140.91 (unchanged overnight, no fees) · equity $11,092.17 vs prior close $11,092.17 (-0.00) because holdings re-marked: MOS×185 yday $23.75 → 09:30 $23.75 +0.00; OCUL×87 yday $10.92 → 09:30 $10.92 +0.00; INSP×15 yday $61.47 → 09:30 $61.47 +0.00; CRMD×115 yday $8.28 → 09:30 $8.28 +0.00; RZLT×182 yday $5.29 → 09:30 $5.29 +0.00; HCA×2 yday $428.50 → 09:30 $428.50 +0.00; BMEA×590 yday $1.61 → 09:30 $1.61 +0.00; NPWR×477 yday $2.02 → 09:30 $2.02 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $140.91 | ▲ 09:30 equity $11,285.02 vs yday $11,125.34 (+159.68) | 09:30 open · cash $140.91 (unchanged overnight, no fees) · equity $11,285.02 vs prior close $11,125.34 (+159.68) because holdings re-marked: MOS×185 yday $23.75 → 09:30 $24.84 +201.65; OCUL×87 yday $10.92 → 09:30 $10.79 -11.31; INSP×15 yday $61.47 → 09:30 $60.07 -21.00; CRMD×115 yday $8.28 → 09:30 $8.60 +36.80; RZLT×182 yday $5.29 → 09:30 $5.01 -50.96; HCA×2 yday $428.50 → 09:30 $427.50 -2.00; BMEA×590 yday $1.61 → 09:30 $1.75 +82.60; NPWR×477 yday $2.02 → 09:30 $1.93 -42.93 | — |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 87 | $10.79 | $2.28 | $-15.84 | $1,077.36 | ▼ -15.84 after sell → book $11,282.74; vs 09:30 mark -2.28 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 15 | $60.07 | $2.06 | $-25.09 | $1,976.36 | ▼ -25.09 after sell → book $11,280.69; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 115 | $8.60 | $2.36 | $+32.10 | $2,963.00 | ▲ +32.10 after sell → book $11,278.33; vs 09:30 mark -2.36 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 182 | $5.01 | $2.58 | $-45.15 | $3,872.24 | ▼ -45.15 after sell → book $11,275.75; vs 09:30 mark -2.58 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 2 | $427.50 | $2.02 | $-7.49 | $4,725.22 | ▼ -7.49 after sell → book $11,273.73; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BMEA` | 590 | $1.75 | $7.72 | $+61.37 | $5,750.00 | ▲ +61.37 after sell → book $11,266.01; vs 09:30 mark -7.72 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NPWR` | 477 | $1.93 | $6.24 | $-45.79 | $6,664.37 | ▼ -45.79 after sell → book $11,259.77; vs 09:30 mark -6.24 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 65 | $40.72 | $2.19 | — | $4,015.39 | — | 40% to #1, rest split; list flatten; ret5=+1.8; leftover $2665.75 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 47 | $14.09 | $2.13 | — | $3,351.03 | — | 40% to #1, rest split; list flatten; ret5=+1.1; leftover $666.44 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 257 | $2.59 | $3.32 | — | $2,682.08 | — | 40% to #1, rest split; list flatten; ret5=+4.2; leftover $666.44 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 8 | $80.97 | $2.01 | — | $2,032.31 | — | 40% to #1, rest split; list mover_buy; 🔵; ret5=-1.3; leftover $666.44 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 150 | $4.42 | $2.44 | — | $1,366.87 | — | 40% to #1, rest split; list mover_buy; 🔵; ret5=-8.6; leftover $666.44 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 8 | $75.12 | $2.01 | — | $763.89 | — | 40% to #1, rest split; list mover_buy; 🔵; ret5=-2.2; leftover $666.44 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $763.89 | ▼ 09:30 equity $11,178.45 vs yday $11,184.63 (-6.18) | 09:30 open · cash $763.89 (unchanged overnight, no fees) · equity $11,178.45 vs prior close $11,184.63 (-6.18) because holdings re-marked: MOS×185 yday $24.16 → 09:30 $24.00 -29.60; RRC×65 yday $41.55 → 09:30 $41.44 -7.15; CRK×47 yday $14.50 → 09:30 $14.42 -3.76; SLI×257 yday $2.61 → 09:30 $2.60 -2.57; ACMR×8 yday $79.11 → 09:30 $81.65 +20.32; GGB×150 yday $4.46 → 09:30 $4.57 +16.50; MT×8 yday $74.53 → 09:30 $74.54 +0.08 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 8 | $81.65 | $2.03 | $+1.39 | $1,415.06 | ▲ +1.39 after sell → book $11,176.42; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 150 | $4.57 | $2.47 | $+17.59 | $2,098.08 | ▲ +17.59 after sell → book $11,173.94; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 8 | $74.54 | $2.03 | $-8.69 | $2,692.37 | ▼ -8.69 after sell → book $11,171.91; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 7 | $144.70 | $2.01 | — | $1,677.46 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1076.95 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 31 | $16.95 | $2.08 | — | $1,149.93 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $538.47 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 29 | $18.50 | $2.08 | — | $611.35 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $538.47 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 58 | $9.19 | $2.16 | — | $76.16 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $538.47 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $76.16 | ▼ 09:30 equity $11,057.11 vs yday $11,169.43 (-112.32) | 09:30 open · cash $76.16 (unchanged overnight, no fees) · equity $11,057.11 vs prior close $11,169.43 (-112.32) because holdings re-marked: MOS×185 yday $23.76 → 09:30 $23.75 -1.85; RRC×65 yday $41.64 → 09:30 $41.11 -34.45; CRK×47 yday $14.62 → 09:30 $14.56 -2.82; SLI×257 yday $2.64 → 09:30 $2.51 -33.41; ANF×7 yday $145.75 → 09:30 $148.67 +20.44; BHVN×31 yday $16.12 → 09:30 $15.44 -21.08; BZ×29 yday $18.00 → 09:30 $17.89 -3.19; CAPR×58 yday $10.06 → 09:30 $9.44 -35.96 | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 185 | $23.75 | $2.61 | $-51.41 | $4,467.30 | ▼ -51.41 after sell → book $11,054.50; vs 09:30 mark -2.61 | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 65 | $41.11 | $2.22 | $+20.95 | $7,137.24 | ▲ +20.95 after sell → book $11,052.29; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 47 | $14.56 | $2.15 | $+17.81 | $7,819.41 | ▲ +17.81 after sell → book $11,050.14; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 257 | $2.51 | $3.37 | $-27.24 | $8,461.11 | ▼ -27.24 after sell → book $11,046.77; vs 09:30 mark -3.37 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 7 | $148.67 | $2.03 | $+23.75 | $9,499.77 | ▲ +23.75 after sell → book $11,044.74; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BHVN` | 31 | $15.44 | $2.10 | $-51.00 | $9,976.30 | ▼ -51.00 after sell → book $11,042.63; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 29 | $17.89 | $2.10 | $-21.86 | $10,493.02 | ▼ -21.86 after sell → book $11,040.54; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 58 | $9.44 | $2.18 | $+10.15 | $11,038.35 | ▲ +10.15 after sell → book $11,038.35; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,038.35 | ▲ 09:30 equity $11,038.35 vs yday $11,038.35 (+0.00) | 09:30 open · cash $11,038.35 · no holdings · equity $11,038.35 vs prior close $11,038.35 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,038.35 | ▲ 09:30 equity $11,038.35 vs yday $11,038.35 (+0.00) | 09:30 open · cash $11,038.35 · no holdings · equity $11,038.35 vs prior close $11,038.35 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,038.35 | ▲ 09:30 equity $11,038.35 vs yday $11,038.35 (+0.00) | 09:30 open · cash $11,038.35 · no holdings · equity $11,038.35 vs prior close $11,038.35 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 88 | $49.76 | $2.25 | — | $6,657.22 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+10.6; leftover $4415.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 22 | $41.31 | $2.06 | — | $5,746.34 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $946.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 289 | $3.27 | $3.73 | — | $4,797.58 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+13.8; leftover $946.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 122 | $7.70 | $2.36 | — | $3,855.83 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $946.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 7 | $125.94 | $2.01 | — | $2,972.24 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $946.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 775 | $1.22 | $10.00 | — | $2,016.74 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $946.14 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 51 | $18.40 | $2.14 | — | $1,076.20 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $946.14 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 60 | $15.70 | $2.17 | — | $132.03 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $946.14 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $132.03 | ▲ 09:30 equity $11,924.08 vs yday $11,788.75 (+135.33) | 09:30 open · cash $132.03 (unchanged overnight, no fees) · equity $11,924.08 vs prior close $11,788.75 (+135.33) because holdings re-marked: ATRC×88 yday $52.59 → 09:30 $52.88 +25.52; HRMY×22 yday $42.86 → 09:30 $42.93 +1.54; CABA×289 yday $3.57 → 09:30 $3.63 +17.34; VSTM×122 yday $8.02 → 09:30 $8.03 +1.22; RVTY×7 yday $130.94 → 09:30 $132.45 +10.57; GPRO×775 yday $1.69 → 09:30 $1.78 +69.75; FRVO×51 yday $17.98 → 09:30 $18.27 +14.79; CRK×60 yday $15.54 → 09:30 $15.45 -5.40 | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 22 | $42.93 | $2.08 | $+31.51 | $1,074.41 | ▲ +31.51 after sell → book $11,922.00; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 122 | $8.03 | $2.39 | $+35.52 | $2,051.68 | ▲ +35.52 after sell → book $11,919.61; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 7 | $132.45 | $2.03 | $+41.53 | $2,976.80 | ▲ +41.53 after sell → book $11,917.58; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FRVO` | 51 | $18.27 | $2.16 | $-10.94 | $3,906.41 | ▼ -10.94 after sell → book $11,915.42; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 60 | $15.45 | $2.19 | $-19.36 | $4,831.22 | ▼ -19.36 after sell → book $11,913.23; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASND` | 7 | $266.94 | $2.01 | — | $2,960.63 | — | 40% to #1, rest split; list flatten; ret5=+1.9; leftover $1932.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OSCR` | 23 | $30.65 | $2.06 | — | $2,253.62 | — | 40% to #1, rest split; list flatten; 🔵; ret5=-2.2; leftover $724.68 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 69 | $10.41 | $2.20 | — | $1,533.13 | — | 40% to #1, rest split; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $724.68 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 49 | $14.50 | $2.14 | — | $820.50 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+0.8; leftover $724.68 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 371 | $1.95 | $4.79 | — | $92.26 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $724.68 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

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
| 2026-08-27 | `MU` | cash | leftover split 666.44 < 1 share @ 925.74 |
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
| `ATRC` | 88 | 2026-09-03 @ $49.76 | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+10.6; leftover $4415.34 |
| `CABA` | 289 | 2026-09-03 @ $3.27 | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+13.8; leftover $946.14 |
| `GPRO` | 775 | 2026-09-03 @ $1.22 | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $946.14 |
| `ASND` | 7 | 2026-09-04 @ $266.94 | 40% to #1, rest split; list flatten; ret5=+1.9; leftover $1932.49 |
| `OSCR` | 23 | 2026-09-04 @ $30.65 | 40% to #1, rest split; list flatten; 🔵; ret5=-2.2; leftover $724.68 |
| `NVAX` | 69 | 2026-09-04 @ $10.41 | 40% to #1, rest split; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $724.68 |
| `BVS` | 49 | 2026-09-04 @ $14.50 | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+0.8; leftover $724.68 |
| `BAK` | 371 | 2026-09-04 @ $1.95 | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $724.68 |
