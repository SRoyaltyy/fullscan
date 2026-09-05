# Factor mine action — `short_last_red_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · last bar red

Cash book **-8.15%** ($9,185) · signal-only (no cash/fees) was -7.21%. Starts YES **2/17**. Fills 136 · skips 59 · realized $-772.68.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `last_red=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $14,681.12.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | TGTX, SLS, HIMS, VOR | — | $14,955.47 | $-5,021.24 | $9,934.23 | TGTX×25, SLS×106, HIMS×42, VOR×56 | SHORT TGTX x25 @ 49.70; SHORT SLS x106 @ 11.70; SHORT HIMS x42 @ 29.74; SHORT VOR x56 @ 22.01 |
| 2026-08-14 | +5.50 | $14,955.47 | TGTX×25, SLS×106, HIMS×42, VOR×56 | TLN, NRG, MARA, FOSL, ARX, CRMD, BIRK, HLIT | TGTX, SLS, HIMS, VOR | $14,532.11 | $-4,658.72 | $9,873.39 | TLN×1, NRG×5, MARA×68, FOSL×109, ARX×31, CRMD×77, BIRK×15, HLIT×47 | SELL TGTX (dropped from list after 1 sess (min 1)); SELL SLS (dropped from list after 1 sess (min 1)); SELL HIMS (dropped from list after 1 sess (min 1)); SELL VOR (dropped from list after 1 sess (min 1)); SHORT TLN x1 @ 359.83; SHORT NRG x5 @ 120.00; SHORT MARA x68 @ 9.01; SHORT FOSL x109 @ 5.64; SHORT ARX x31 @ 19.57; SHORT CRMD x77 @ 8.05; SHORT BIRK x15 @ 39.75; SHORT HLIT x47 @ 13.18 |
| 2026-08-17 | +2.25 | $14,532.11 | TLN×1, NRG×5, MARA×68, FOSL×109, ARX×31, CRMD×77, BIRK×15, HLIT×47 | TMC, TGB, ELF, DNN, HNST, CAPR, BYND, NU | TLN, NRG, MARA, FOSL, ARX, CRMD, BIRK, HLIT | $14,648.94 | $-4,758.66 | $9,890.28 | TMC×152, TGB×72, ELF×6, DNN×190, HNST×128, CAPR×89, BYND×47, NU×39 | SELL TLN (dropped from list after 1 sess (min 1)); SELL NRG (dropped from list after 1 sess (min 1)); SELL MARA (dropped from list after 1 sess (min 1)); SELL FOSL (dropped from list after 1 sess (min 1)); SELL ARX (dropped from list after 1 sess (min 1)); SELL CRMD (dropped from list after 1 sess (min 1)); SELL BIRK (dropped from list after 1 sess (min 1)); SELL HLIT (dropped from list after 1 sess (min 1)); SHORT TMC x152 @ 4.05; SHORT TGB x72 @ 8.46; SHORT ELF x6 @ 90.54; SHORT DNN x190 @ 3.24; SHORT HNST x128 @ 4.81; SHORT CAPR x89 @ 6.87; SHORT BYND x47 @ 12.83; SHORT NU x39 @ 15.40 |
| 2026-08-18 | -6.20 | $14,648.94 | TMC×152, TGB×72, ELF×6, DNN×190, HNST×128, CAPR×89, BYND×47, NU×39 | — | TMC, TGB, ELF, DNN, HNST, CAPR, BYND, NU | $9,943.70 | $0.00 | $9,943.70 | — | SELL TMC (dropped from list after 1 sess (min 1)); SELL TGB (dropped from list after 1 sess (min 1)); SELL ELF (dropped from list after 1 sess (min 1)); SELL DNN (dropped from list after 1 sess (min 1)); SELL HNST (dropped from list after 1 sess (min 1)); SELL CAPR (dropped from list after 1 sess (min 1)); SELL BYND (dropped from list after 1 sess (min 1)); SELL NU (dropped from list after 1 sess (min 1)); hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $9,943.70 | — | — | — | $9,943.70 | $0.00 | $9,943.70 | — | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $9,943.70 | — | BHP, MRVI, WYFI, TOYO, DVLT, SAFX, AAP, AEG | — | $14,776.99 | $-4,929.80 | $9,847.20 | BHP×6, MRVI×84, WYFI×29, TOYO×140, DVLT×2071, SAFX×1755, AAP×13, AEG×68 | SHORT BHP x6 @ 91.01; SHORT MRVI x84 @ 7.38; SHORT WYFI x29 @ 21.40; SHORT TOYO x140 @ 4.43; SHORT DVLT x2071 @ 0.30; SHORT SAFX x1755 @ 0.35; SHORT AAP x13 @ 46.85; SHORT AEG x68 @ 9.01 |
| 2026-08-21 | +3.25 | $14,776.99 | BHP×6, MRVI×84, WYFI×29, TOYO×140, DVLT×2071, SAFX×1755, AAP×13, AEG×68 | AUTL, CRDL, CRSP, FUTU, GMAB, ENHA, CAN | BHP, WYFI, TOYO, DVLT, SAFX, AAP, AEG | $15,235.59 | $-5,692.11 | $9,543.49 | MRVI×84, AUTL×282, CRDL×361, CRSP×11, FUTU×6, GMAB×20, ENHA×408, CAN×2375 | SELL BHP (dropped from list after 1 sess (min 1)); SELL WYFI (dropped from list after 1 sess (min 1)); SELL TOYO (dropped from list after 1 sess (min 1)); SELL DVLT (dropped from list after 1 sess (min 1)); SELL SAFX (dropped from list after 1 sess (min 1)); SELL AAP (dropped from list after 1 sess (min 1)); SELL AEG (dropped from list after 1 sess (min 1)); SHORT AUTL x282 @ 2.47; SHORT CRDL x361 @ 1.93; SHORT CRSP x11 @ 59.72; SHORT FUTU x6 @ 115.18; SHORT GMAB x20 @ 33.36; SHORT ENHA x408 @ 1.71; SHORT CAN x2375 @ 0.29 |
| 2026-08-24 | -5.17 | $15,235.59 | MRVI×84, AUTL×282, CRDL×361, CRSP×11, FUTU×6, GMAB×20, ENHA×408, CAN×2375 | — | MRVI, AUTL, CRDL, CRSP, FUTU, GMAB, ENHA, CAN | $9,494.68 | $0.00 | $9,494.68 | — | SELL MRVI (dropped from list after 2 sess (min 1)); SELL AUTL (dropped from list after 1 sess (min 1)); SELL CRDL (dropped from list after 1 sess (min 1)); SELL CRSP (dropped from list after 1 sess (min 1)); SELL FUTU (dropped from list after 1 sess (min 1)); SELL GMAB (dropped from list after 1 sess (min 1)); SELL ENHA (dropped from list after 1 sess (min 1)); SELL CAN (dropped from list after 1 sess (min 1)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $9,494.68 | — | OCUL, CRMD, PUSA, CAPR, SAFX, SUJA, FWDI, JANX | — | $14,195.71 | $-4,781.80 | $9,413.91 | OCUL×54, CRMD×71, PUSA×160, CAPR×87, SAFX×1603, SUJA×67, FWDI×99, JANX×32 | SHORT OCUL x54 @ 10.92; SHORT CRMD x71 @ 8.28; SHORT PUSA x160 @ 3.70; SHORT CAPR x87 @ 6.79; SHORT SAFX x1603 @ 0.37; SHORT SUJA x67 @ 8.79; SHORT FWDI x99 @ 5.99; SHORT JANX x32 @ 18.52 |
| 2026-08-26 | +2.02 | $14,195.71 | OCUL×54, CRMD×71, PUSA×160, CAPR×87, SAFX×1603, SUJA×67, FWDI×99, JANX×32 | — | — | $14,195.71 | $-4,727.98 | $9,467.73 | OCUL×54, CRMD×71, PUSA×160, CAPR×87, SAFX×1603, SUJA×67, FWDI×99, JANX×32 | hold OCUL,CRMD,PUSA,CAPR,SAFX,SUJA,FWDI,JANX |
| 2026-08-27 | — | $14,195.71 | OCUL×54, CRMD×71, PUSA×160, CAPR×87, SAFX×1603, SUJA×67, FWDI×99, JANX×32 | ACMR, GGB, MT, TX, LRCX, MRVL, NUE | OCUL, CRMD, PUSA, CAPR, SAFX, SUJA, FWDI, JANX | $12,766.09 | $-3,519.74 | $9,246.35 | ACMR×7, GGB×131, MT×7, TX×10, LRCX×1, MRVL×2, NUE×2 | SELL OCUL (dropped from list after 2 sess (min 1)); SELL CRMD (dropped from list after 2 sess (min 1)); SELL PUSA (dropped from list after 2 sess (min 1)); SELL CAPR (dropped from list after 2 sess (min 1)); SELL SAFX (dropped from list after 2 sess (min 1)); SELL SUJA (dropped from list after 2 sess (min 1)); SELL FWDI (dropped from list after 2 sess (min 1)); SELL JANX (dropped from list after 2 sess (min 1)); SHORT ACMR x7 @ 80.97; SHORT GGB x131 @ 4.42; SHORT MT x7 @ 75.12; SHORT TX x10 @ 55.20; SHORT LRCX x1 @ 314.61; SHORT MRVL x2 @ 240.00; SHORT NUE x2 @ 248.91 |
| 2026-08-28 | +0.75 | $12,766.09 | ACMR×7, GGB×131, MT×7, TX×10, LRCX×1, MRVL×2, NUE×2 | CAPR, SEDG, SMTC, PYXS, SAFX, OPTX, TTMI, APMD | ACMR, GGB, MT, TX, LRCX, MRVL, NUE | $13,490.58 | $-4,326.21 | $9,164.37 | CAPR×62, SEDG×16, SMTC×3, PYXS×173, SAFX×1470, OPTX×66, TTMI×4, APMD×19 | SELL ACMR (dropped from list after 1 sess (min 1)); SELL GGB (dropped from list after 1 sess (min 1)); SELL MT (dropped from list after 1 sess (min 1)); SELL TX (dropped from list after 1 sess (min 1)); SELL LRCX (dropped from list after 1 sess (min 1)); SELL MRVL (dropped from list after 1 sess (min 1)); SELL NUE (dropped from list after 1 sess (min 1)); SHORT CAPR x62 @ 9.19; SHORT SEDG x16 @ 33.78; SHORT SMTC x3 @ 149.40; SHORT PYXS x173 @ 3.31; SHORT SAFX x1470 @ 0.39; SHORT OPTX x66 @ 8.57; SHORT TTMI x4 @ 127.07; SHORT APMD x19 @ 29.50 |
| 2026-08-31 | -5.85 | $13,490.58 | CAPR×62, SEDG×16, SMTC×3, PYXS×173, SAFX×1470, OPTX×66, TTMI×4, APMD×19 | — | CAPR, SEDG, SMTC, PYXS, SAFX, OPTX, TTMI, APMD | $9,262.51 | $0.00 | $9,262.51 | — | SELL CAPR (dropped from list after 1 sess (min 1)); SELL SEDG (dropped from list after 1 sess (min 1)); SELL SMTC (dropped from list after 1 sess (min 1)); SELL PYXS (dropped from list after 1 sess (min 1)); SELL SAFX (dropped from list after 1 sess (min 1)); SELL OPTX (dropped from list after 1 sess (min 1)); SELL TTMI (dropped from list after 1 sess (min 1)); SELL APMD (dropped from list after 1 sess (min 1)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $9,262.51 | — | — | — | $9,262.51 | $0.00 | $9,262.51 | — | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $9,262.51 | — | — | — | $9,262.51 | $0.00 | $9,262.51 | — | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $9,262.51 | — | CABA, FRVO, CTMX, EIX, CRDL, SION, DUOL, SAFX | — | $13,731.65 | $-4,572.77 | $9,158.88 | CABA×177, FRVO×31, CTMX×155, EIX×10, CRDL×268, SION×87, DUOL×3, SAFX×1484 | SHORT CABA x177 @ 3.27; SHORT FRVO x31 @ 18.40; SHORT CTMX x155 @ 3.72; SHORT EIX x10 @ 56.78; SHORT CRDL x268 @ 2.16; SHORT SION x87 @ 6.63; SHORT DUOL x3 @ 156.24; SHORT SAFX x1484 @ 0.39 |
| 2026-09-04 | — | $13,731.65 | CABA×177, FRVO×31, CTMX×155, EIX×10, CRDL×268, SION×87, DUOL×3, SAFX×1484 | ASND, SLBT, MLYS, CCOI, IRD, JLHL | FRVO, CTMX, EIX, CRDL, DUOL, SAFX | $14,681.12 | $-5,496.20 | $9,184.92 | CABA×177, SION×87, ASND×2, SLBT×247, MLYS×26, CCOI×74, IRD×162, JLHL×122 | SELL FRVO (dropped from list after 1 sess (min 1)); SELL CTMX (dropped from list after 1 sess (min 1)); SELL EIX (dropped from list after 1 sess (min 1)); SELL CRDL (dropped from list after 1 sess (min 1)); SELL DUOL (dropped from list after 1 sess (min 1)); SELL SAFX (dropped from list after 1 sess (min 1)); SHORT ASND x2 @ 266.94; SHORT SLBT x247 @ 3.07; SHORT MLYS x26 @ 29.15; SHORT CCOI x74 @ 10.22; SHORT IRD x162 @ 4.66; SHORT JLHL x122 @ 6.20 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **SHORT** | `TGTX` | 25 | $49.70 | $2.12 | — | $11,240.38 | last bar red; gate last_red=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **SHORT** | `SLS` | 106 | $11.70 | $2.37 | — | $12,478.21 | last bar red; gate last_red=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **SHORT** | `HIMS` | 42 | $29.74 | $2.17 | — | $13,725.12 | last bar red; gate last_red=True; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **SHORT** | `VOR` | 56 | $22.01 | $2.21 | — | $14,955.47 | last bar red; gate last_red=True; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **COVER** | `TGTX` | 25 | $47.27 | $2.06 | $+56.57 | $13,771.65 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **COVER** | `SLS` | 106 | $12.40 | $2.31 | $-78.88 | $12,454.95 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **COVER** | `HIMS` | 42 | $29.15 | $2.12 | $+20.49 | $11,228.53 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **COVER** | `VOR` | 56 | $23.33 | $2.16 | $-78.29 | $9,919.89 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SHORT** | `TLN` | 1 | $359.83 | $2.02 | — | $10,277.70 | last bar red; gate last_red=True; list flatten; 🔵; ret5=+5.9; leftover $619.99 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `NRG` | 5 | $120.00 | $2.04 | — | $10,875.66 | last bar red; gate last_red=True; list flatten; 🔵; ret5=+0.6; leftover $619.99 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `MARA` | 68 | $9.01 | $2.23 | — | $11,486.11 | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=-13.5; leftover $619.99 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `FOSL` | 109 | $5.64 | $2.36 | — | $12,098.50 | last bar red; gate last_red=True; list probable; 🔵; ret5=-4.1; leftover $619.99 | join🟢 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `ARX` | 31 | $19.57 | $2.12 | — | $12,703.05 | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $619.99 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `CRMD` | 77 | $8.05 | $2.26 | — | $13,320.64 | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $619.99 | join🟢 sector🔴 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `BIRK` | 15 | $39.75 | $2.07 | — | $13,914.82 | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=+10.2; leftover $619.99 | join🟢 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `HLIT` | 47 | $13.18 | $2.17 | — | $14,532.11 | last bar red; gate last_red=True; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $619.99 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **COVER** | `TLN` | 1 | $367.88 | $1.99 | $-12.07 | $14,162.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `NRG` | 5 | $127.40 | $2.00 | $-41.05 | $13,523.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `MARA` | 68 | $9.22 | $2.19 | $-18.71 | $12,894.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `FOSL` | 109 | $5.50 | $2.32 | $+10.58 | $12,292.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `ARX` | 31 | $19.57 | $2.08 | $-4.20 | $11,683.51 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `CRMD` | 77 | $7.55 | $2.22 | $+34.02 | $11,099.94 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `BIRK` | 15 | $39.48 | $2.04 | $-0.06 | $10,505.71 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `HLIT` | 47 | $13.84 | $2.13 | $-35.32 | $9,853.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SHORT** | `TMC` | 152 | $4.05 | $2.50 | — | $10,466.20 | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $615.82 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `TGB` | 72 | $8.46 | $2.24 | — | $11,073.07 | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=+0.4; leftover $615.82 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `ELF` | 6 | $90.54 | $2.04 | — | $11,614.27 | last bar red; gate last_red=True; list flatten; ret5=-7.2; leftover $615.82 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `DNN` | 190 | $3.24 | $2.62 | — | $12,227.25 | last bar red; gate last_red=True; list flatten; ⚪; ret5=+0.3; leftover $615.82 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `HNST` | 128 | $4.81 | $2.42 | — | $12,840.51 | last bar red; gate last_red=True; list flatten; ⚪; ret5=-11.4; leftover $615.82 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `CAPR` | 89 | $6.87 | $2.30 | — | $13,449.64 | last bar red; gate last_red=True; list yday_gainer,yday_mover; ret5=+62.6; leftover $615.82 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `BYND` | 47 | $12.83 | $2.17 | — | $14,050.48 | last bar red; gate last_red=True; list yday_gainer,yday_mover; ⚪; ret5=-34.1; leftover $615.82 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `NU` | 39 | $15.40 | $2.14 | — | $14,648.94 | last bar red; gate last_red=True; list yday_gainer; 🔵; ⚪; ret5=+10.0; leftover $615.82 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **COVER** | `TMC` | 152 | $3.72 | $2.45 | $+45.22 | $14,081.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `TGB` | 72 | $8.55 | $2.21 | $-10.93 | $13,463.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `ELF` | 6 | $93.44 | $2.01 | $-21.45 | $12,900.60 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `DNN` | 190 | $3.11 | $2.56 | $+19.52 | $12,307.14 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **COVER** | `HNST` | 128 | $4.67 | $2.37 | $+13.12 | $11,707.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `CAPR` | 89 | $7.50 | $2.26 | $-60.63 | $11,037.25 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **COVER** | `BYND` | 47 | $11.12 | $2.13 | $+76.07 | $10,512.48 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **COVER** | `NU` | 39 | $14.53 | $2.11 | $+29.68 | $9,943.70 | dropped from list after 1 sess (min 1) | — |
| 2026-08-20 09:30 ET | **SHORT** | `BHP` | 6 | $91.01 | $2.04 | — | $10,487.72 | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $621.48 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `MRVI` | 84 | $7.38 | $2.28 | — | $11,105.35 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $621.48 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 29 | $21.40 | $2.11 | — | $11,723.84 | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-25.2; leftover $621.48 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 140 | $4.43 | $2.46 | — | $12,341.58 | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-23.1; leftover $621.48 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `DVLT` | 2071 | $0.30 | $12.80 | — | $12,950.08 | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-3.2; leftover $621.48 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `SAFX` | 1755 | $0.35 | $11.80 | — | $13,559.56 | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-29.4; leftover $621.48 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 13 | $46.85 | $2.07 | — | $14,166.54 | last bar red; gate last_red=True; list earn_react; 🔵; ret5=+5.0; leftover $621.48 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AEG` | 68 | $9.01 | $2.23 | — | $14,776.99 | last bar red; gate last_red=True; list earn_react; 🔵; ⚪; ret5=-1.3; leftover $621.48 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **COVER** | `BHP` | 6 | $95.72 | $2.01 | $-32.31 | $14,200.66 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `WYFI` | 29 | $21.54 | $2.08 | $-8.25 | $13,573.93 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `TOYO` | 140 | $4.68 | $2.41 | $-39.87 | $12,916.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `DVLT` | 2071 | $0.31 | $12.63 | $-46.14 | $12,261.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `SAFX` | 1755 | $0.35 | $11.41 | $-16.18 | $11,636.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `AAP` | 13 | $42.41 | $2.03 | $+53.63 | $11,082.66 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **COVER** | `AEG` | 68 | $9.04 | $2.19 | $-6.47 | $10,465.74 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUTL` | 282 | $2.47 | $3.71 | — | $11,158.57 | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $698.35 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `CRDL` | 361 | $1.93 | $4.75 | — | $11,850.55 | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $698.35 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `CRSP` | 11 | $59.72 | $2.06 | — | $12,505.41 | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $698.35 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `FUTU` | 6 | $115.18 | $2.05 | — | $13,194.44 | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $698.35 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `GMAB` | 20 | $33.36 | $2.09 | — | $13,859.56 | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.6; leftover $698.35 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `ENHA` | 408 | $1.71 | $5.36 | — | $14,551.88 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $698.35 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `CAN` | 2375 | $0.29 | $14.53 | — | $15,235.59 | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $698.35 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **COVER** | `MRVI` | 84 | $8.59 | $2.24 | $-106.16 | $14,511.79 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `AUTL` | 282 | $2.36 | $3.64 | $+23.67 | $13,842.64 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `CRDL` | 361 | $1.87 | $4.66 | $+12.26 | $13,162.91 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `CRSP` | 11 | $58.79 | $2.02 | $+6.15 | $12,514.20 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **COVER** | `FUTU` | 6 | $120.87 | $2.01 | $-38.19 | $11,786.97 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **COVER** | `GMAB` | 20 | $32.82 | $2.05 | $+6.66 | $11,128.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `ENHA` | 408 | $1.74 | $5.26 | $-22.86 | $10,413.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `CAN` | 2375 | $0.38 | $16.15 | $-234.93 | $9,494.68 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `OCUL` | 54 | $10.92 | $2.19 | — | $10,082.18 | last bar red; gate last_red=True; list flatten; 🔵; ret5=+10.4; leftover $593.42 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **SHORT** | `CRMD` | 71 | $8.28 | $2.24 | — | $10,667.81 | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=+8.8; leftover $593.42 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `PUSA` | 160 | $3.70 | $2.52 | — | $11,257.29 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.8; leftover $593.42 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `CAPR` | 87 | $6.79 | $2.29 | — | $11,845.73 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.4; leftover $593.42 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `SAFX` | 1603 | $0.37 | $11.03 | — | $12,427.81 | last bar red; gate last_red=True; list probable,yday_gainer; ret5=-26.5; leftover $593.42 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `SUJA` | 67 | $8.79 | $2.23 | — | $13,014.51 | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.5; leftover $593.42 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `FWDI` | 99 | $5.99 | $2.33 | — | $13,605.19 | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+20.7; leftover $593.42 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `JANX` | 32 | $18.52 | $2.12 | — | $14,195.71 | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=+7.9; leftover $593.42 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-27 09:30 ET | **COVER** | `OCUL` | 54 | $10.79 | $2.15 | $+2.68 | $13,610.90 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `CRMD` | 71 | $8.60 | $2.20 | $-27.16 | $12,998.09 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `PUSA` | 160 | $3.84 | $2.47 | $-27.39 | $12,381.22 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `CAPR` | 87 | $8.29 | $2.25 | $-135.04 | $11,657.74 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `SAFX` | 1603 | $0.35 | $10.42 | $+10.61 | $11,086.27 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `SUJA` | 67 | $9.39 | $2.19 | $-44.62 | $10,454.95 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `FWDI` | 99 | $5.97 | $2.29 | $-2.64 | $9,861.63 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `JANX` | 32 | $18.59 | $2.09 | $-6.45 | $9,264.67 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SHORT** | `ACMR` | 7 | $80.97 | $2.05 | — | $9,829.41 | last bar red; gate last_red=True; list mover_buy; 🔵; ret5=-1.3; leftover $579.04 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `GGB` | 131 | $4.42 | $2.43 | — | $10,406.00 | last bar red; gate last_red=True; list mover_buy; 🔵; ret5=-8.6; leftover $579.04 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 7 | $75.12 | $2.05 | — | $10,929.80 | last bar red; gate last_red=True; list mover_buy; 🔵; ret5=-2.2; leftover $579.04 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 10 | $55.20 | $2.06 | — | $11,479.74 | last bar red; gate last_red=True; list mover_buy; 🔵; ret5=+3.0; leftover $579.04 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `LRCX` | 1 | $314.61 | $2.02 | — | $11,792.33 | last bar red; gate last_red=True; list mover_buy; 🔵; ret5=-5.5; leftover $579.04 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `MRVL` | 2 | $240.00 | $2.03 | — | $12,270.30 | last bar red; gate last_red=True; list mover_buy; 🔵; ret5=+6.8; leftover $579.04 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `NUE` | 2 | $248.91 | $2.03 | — | $12,766.09 | last bar red; gate last_red=True; list mover_buy; 🔵; ret5=-9.4; leftover $579.04 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **COVER** | `ACMR` | 7 | $81.65 | $2.01 | $-8.82 | $12,192.53 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `GGB` | 131 | $4.57 | $2.38 | $-24.46 | $11,591.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `MT` | 7 | $74.54 | $2.01 | $+0.00 | $11,067.69 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `TX` | 10 | $55.25 | $2.02 | $-4.58 | $10,513.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `LRCX` | 1 | $318.88 | $1.99 | $-8.28 | $10,192.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `MRVL` | 2 | $253.44 | $2.00 | $-30.91 | $9,683.42 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **COVER** | `NUE` | 2 | $252.00 | $2.00 | $-10.21 | $9,177.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SHORT** | `CAPR` | 62 | $9.19 | $2.21 | — | $9,744.99 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $573.59 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `SEDG` | 16 | $33.78 | $2.07 | — | $10,283.40 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $573.59 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `SMTC` | 3 | $149.40 | $2.03 | — | $10,729.56 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $573.59 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `PYXS` | 173 | $3.31 | $2.56 | — | $11,299.63 | last bar red; gate last_red=True; list yday_gainer; ret5=+2.3; leftover $573.59 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `SAFX` | 1470 | $0.39 | $10.41 | — | $11,862.52 | last bar red; gate last_red=True; list yday_gainer; ret5=-26.5; leftover $573.59 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `OPTX` | 66 | $8.57 | $2.22 | — | $12,425.91 | last bar red; gate last_red=True; list yday_gainer; ret5=-3.4; leftover $573.59 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `TTMI` | 4 | $127.07 | $2.04 | — | $12,932.16 | last bar red; gate last_red=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=-21.0; leftover $573.59 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `APMD` | 19 | $29.50 | $2.08 | — | $13,490.58 | last bar red; gate last_red=True; list yday_gainer; ret5=-11.7; leftover $573.59 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-31 09:30 ET | **COVER** | `CAPR` | 62 | $9.44 | $2.18 | $-19.89 | $12,903.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `SEDG` | 16 | $31.50 | $2.04 | $+32.37 | $12,397.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `SMTC` | 3 | $133.04 | $2.00 | $+45.05 | $11,995.96 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `PYXS` | 173 | $3.23 | $2.51 | $+8.77 | $11,434.66 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `SAFX` | 1470 | $0.38 | $10.00 | $-5.71 | $10,866.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `OPTX` | 66 | $8.52 | $2.19 | $-1.11 | $10,301.56 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `TTMI` | 4 | $117.20 | $2.00 | $+35.44 | $9,830.76 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `APMD` | 19 | $29.80 | $2.05 | $-9.83 | $9,262.51 | dropped from list after 1 sess (min 1) | — |
| 2026-09-03 09:30 ET | **SHORT** | `CABA` | 177 | $3.27 | $2.58 | — | $9,838.72 | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $578.91 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `FRVO` | 31 | $18.40 | $2.12 | — | $10,407.01 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $578.91 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `CTMX` | 155 | $3.72 | $2.51 | — | $10,981.10 | last bar red; gate last_red=True; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $578.91 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `EIX` | 10 | $56.78 | $2.06 | — | $11,546.84 | last bar red; gate last_red=True; list probable,yday_gainer; ret5=+0.3; leftover $578.91 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `CRDL` | 268 | $2.16 | $3.53 | — | $12,122.20 | last bar red; gate last_red=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $578.91 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `SION` | 87 | $6.63 | $2.29 | — | $12,696.71 | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=-18.1; leftover $578.91 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `DUOL` | 3 | $156.24 | $2.03 | — | $13,163.40 | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=+10.0; leftover $578.91 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `SAFX` | 1484 | $0.39 | $10.51 | — | $13,731.65 | last bar red; gate last_red=True; list yday_gainer; ret5=-26.5; leftover $578.91 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **COVER** | `FRVO` | 31 | $18.27 | $2.08 | $-0.17 | $13,163.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `CTMX` | 155 | $3.73 | $2.46 | $-6.51 | $12,582.59 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `EIX` | 10 | $55.42 | $2.02 | $+9.52 | $12,026.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `CRDL` | 268 | $2.18 | $3.46 | $-12.34 | $11,438.68 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `DUOL` | 3 | $161.54 | $2.00 | $-19.93 | $10,952.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `SAFX` | 1484 | $0.38 | $10.09 | $-5.76 | $10,378.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SHORT** | `ASND` | 2 | $266.94 | $2.03 | — | $10,909.90 | last bar red; gate last_red=True; list flatten; ret5=+1.9; leftover $758.30 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `SLBT` | 247 | $3.07 | $3.26 | — | $11,664.93 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $758.30 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `MLYS` | 26 | $29.15 | $2.11 | — | $12,420.72 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $758.30 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `CCOI` | 74 | $10.22 | $2.26 | — | $13,174.74 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $758.30 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `IRD` | 162 | $4.66 | $2.53 | — | $13,927.13 | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $758.30 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `JLHL` | 122 | $6.20 | $2.41 | — | $14,681.12 | last bar red; gate last_red=True; list yday_gainer,yday_mover; ret5=-8.2; leftover $758.30 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `TBPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INDP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ENHA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ENVX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INV` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `INMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `XHG` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AEM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SBSW` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `PUSA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `SAFX` | no_price | no 09:30 open — carry |
| 2026-08-26 | `SUJA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `FWDI` | no_price | no 09:30 open — carry |
| 2026-08-26 | `JANX` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-26 | `AVEX` | no_price | no 09:30 open |
| 2026-08-26 | `BE` | no_price | no 09:30 open |
| 2026-08-26 | `INDP` | no_price | no 09:30 open |
| 2026-08-26 | `AXTI` | no_price | no 09:30 open |
| 2026-08-27 | `MU` | cash | leftover split 579.04 < 1 share @ 925.74 |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SNPS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
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

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CABA` | 177 | 2026-09-03 @ $3.27 | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $578.91 |
| `SION` | 87 | 2026-09-03 @ $6.63 | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=-18.1; leftover $578.91 |
| `ASND` | 2 | 2026-09-04 @ $266.94 | last bar red; gate last_red=True; list flatten; ret5=+1.9; leftover $758.30 |
| `SLBT` | 247 | 2026-09-04 @ $3.07 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $758.30 |
| `MLYS` | 26 | 2026-09-04 @ $29.15 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $758.30 |
| `CCOI` | 74 | 2026-09-04 @ $10.22 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $758.30 |
| `IRD` | 162 | 2026-09-04 @ $4.66 | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $758.30 |
| `JLHL` | 122 | 2026-09-04 @ $6.20 | last bar red; gate last_red=True; list yday_gainer,yday_mover; ret5=-8.2; leftover $758.30 |
