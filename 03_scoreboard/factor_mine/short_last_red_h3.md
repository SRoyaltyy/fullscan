# Factor mine action — `short_last_red_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · last bar red

Cash book **-7.96%** ($9,204) · signal-only (no cash/fees) was -22.66%. Starts YES **4/17**. Fills 126 · skips 174 · realized $-786.62.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `last_red=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $17,916.05.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | TGTX, SLS, HIMS, VOR | — | $14,955.47 | $-5,021.24 | $9,934.23 | TGTX×25, SLS×106, HIMS×42, VOR×56 | SHORT TGTX x25 @ 49.70; SHORT SLS x106 @ 11.70; SHORT HIMS x42 @ 29.74; SHORT VOR x56 @ 22.01 |
| 2026-08-14 | +5.50 | $14,955.47 | TGTX×25, SLS×106, HIMS×42, VOR×56 | TLN, NRG, MARA, FOSL, ARX, CRMD, BIRK, HLIT | — | $19,573.33 | $-9,709.45 | $9,863.88 | TGTX×25, SLS×106, HIMS×42, VOR×56, TLN×1, NRG×5, MARA×68, FOSL×110, ARX×31, CRMD×77, BIRK×15, HLIT×47 | SHORT TLN x1 @ 359.83; SHORT NRG x5 @ 120.00; SHORT MARA x68 @ 9.01; SHORT FOSL x110 @ 5.64; SHORT ARX x31 @ 19.57; SHORT CRMD x77 @ 8.05; SHORT BIRK x15 @ 39.75; SHORT HLIT x47 @ 13.18 |
| 2026-08-17 | +2.25 | $19,573.33 | TGTX×25, SLS×106, HIMS×42, VOR×56, TLN×1, NRG×5, MARA×68, FOSL×110, ARX×31, CRMD×77, BIRK×15, HLIT×47 | TMC, TGB, ELF, DNN, HNST, CAPR, BYND, NU | — | $24,397.40 | $-14,541.78 | $9,855.62 | TGTX×25, SLS×106, HIMS×42, VOR×56, TLN×1, NRG×5, MARA×68, FOSL×110, ARX×31, CRMD×77, BIRK×15, HLIT×47, TMC×152, TGB×72, ELF×6, DNN×190, HNST×128, CAPR×89, BYND×48, NU×40 | SHORT TMC x152 @ 4.05; SHORT TGB x72 @ 8.46; SHORT ELF x6 @ 90.54; SHORT DNN x190 @ 3.24; SHORT HNST x128 @ 4.81; SHORT CAPR x89 @ 6.87; SHORT BYND x48 @ 12.83; SHORT NU x40 @ 15.40 |
| 2026-08-18 | -6.20 | $24,397.40 | TGTX×25, SLS×106, HIMS×42, VOR×56, TLN×1, NRG×5, MARA×68, FOSL×110, ARX×31, CRMD×77, BIRK×15, HLIT×47, TMC×152, TGB×72, ELF×6, DNN×190, HNST×128, CAPR×89, BYND×48, NU×40 | — | TGTX, SLS, HIMS, VOR | $19,367.17 | $-9,276.91 | $10,090.26 | TLN×1, NRG×5, MARA×68, FOSL×110, ARX×31, CRMD×77, BIRK×15, HLIT×47, TMC×152, TGB×72, ELF×6, DNN×190, HNST×128, CAPR×89, BYND×48, NU×40 | SELL TGTX (dropped from list after 3 sess (min 3)); SELL SLS (dropped from list after 3 sess (min 3)); SELL HIMS (dropped from list after 3 sess (min 3)); SELL VOR (dropped from list after 3 sess (min 3)); hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $19,367.17 | TLN×1, NRG×5, MARA×68, FOSL×110, ARX×31, CRMD×77, BIRK×15, HLIT×47, TMC×152, TGB×72, ELF×6, DNN×190, HNST×128, CAPR×89, BYND×48, NU×40 | — | TLN, NRG, MARA, FOSL, ARX, CRMD, BIRK, HLIT | $14,818.03 | $-5,036.00 | $9,782.03 | TMC×152, TGB×72, ELF×6, DNN×190, HNST×128, CAPR×89, BYND×48, NU×40 | SELL TLN (dropped from list after 3 sess (min 3)); SELL NRG (dropped from list after 3 sess (min 3)); SELL MARA (dropped from list after 3 sess (min 3)); SELL FOSL (dropped from list after 3 sess (min 3)); SELL ARX (dropped from list after 3 sess (min 3)); SELL CRMD (dropped from list after 3 sess (min 3)); SELL BIRK (dropped from list after 3 sess (min 3)); SELL HLIT (dropped from list after 3 sess (min 3)); hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $14,818.03 | TMC×152, TGB×72, ELF×6, DNN×190, HNST×128, CAPR×89, BYND×48, NU×40 | BHP, MRVI, WYFI, TOYO, DVLT, SAFX, AAP, AEG | TMC, TGB, ELF, DNN, HNST, CAPR, BYND, NU | $14,627.80 | $-4,878.79 | $9,749.01 | BHP×6, MRVI×83, WYFI×28, TOYO×138, DVLT×2050, SAFX×1738, AAP×13, AEG×68 | SELL TMC (dropped from list after 3 sess (min 3)); SELL TGB (dropped from list after 3 sess (min 3)); SELL ELF (dropped from list after 3 sess (min 3)); SELL DNN (dropped from list after 3 sess (min 3)); SELL HNST (dropped from list after 3 sess (min 3)); SELL CAPR (dropped from list after 3 sess (min 3)); SELL BYND (dropped from list after 3 sess (min 3)); SELL NU (dropped from list after 3 sess (min 3)); SHORT BHP x6 @ 91.01; SHORT MRVI x83 @ 7.38; SHORT WYFI x28 @ 21.40; SHORT TOYO x138 @ 4.43; SHORT DVLT x2050 @ 0.30; SHORT SAFX x1738 @ 0.35; SHORT AAP x13 @ 46.85; SHORT AEG x68 @ 9.01 |
| 2026-08-21 | +3.25 | $14,627.80 | BHP×6, MRVI×83, WYFI×28, TOYO×138, DVLT×2050, SAFX×1738, AAP×13, AEG×68 | AUTL, CRDL, CRSP, FUTU, GMAB, ENHA, CAN | — | $19,379.50 | $-9,877.59 | $9,501.91 | BHP×6, MRVI×83, WYFI×28, TOYO×138, DVLT×2050, SAFX×1738, AAP×13, AEG×68, AUTL×280, CRDL×359, CRSP×11, FUTU×6, GMAB×20, ENHA×405, CAN×2360 | SHORT AUTL x280 @ 2.47; SHORT CRDL x359 @ 1.93; SHORT CRSP x11 @ 59.72; SHORT FUTU x6 @ 115.18; SHORT GMAB x20 @ 33.36; SHORT ENHA x405 @ 1.71; SHORT CAN x2360 @ 0.29 |
| 2026-08-24 | -5.17 | $19,379.50 | BHP×6, MRVI×83, WYFI×28, TOYO×138, DVLT×2050, SAFX×1738, AAP×13, AEG×68, AUTL×280, CRDL×359, CRSP×11, FUTU×6, GMAB×20, ENHA×405, CAN×2360 | — | — | $19,379.50 | $-9,778.75 | $9,600.75 | BHP×6, MRVI×83, WYFI×28, TOYO×138, DVLT×2050, SAFX×1738, AAP×13, AEG×68, AUTL×280, CRDL×359, CRSP×11, FUTU×6, GMAB×20, ENHA×405, CAN×2360 | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $19,379.50 | BHP×6, MRVI×83, WYFI×28, TOYO×138, DVLT×2050, SAFX×1738, AAP×13, AEG×68, AUTL×280, CRDL×359, CRSP×11, FUTU×6, GMAB×20, ENHA×405, CAN×2360 | OCUL, CRMD, PUSA, CAPR, SUJA, FWDI, JANX | BHP, MRVI, WYFI, TOYO, DVLT, AAP, AEG | $19,729.33 | $-10,292.65 | $9,436.68 | SAFX×1738, AUTL×280, CRDL×359, CRSP×11, FUTU×6, GMAB×20, ENHA×405, CAN×2360, OCUL×62, CRMD×81, PUSA×183, CAPR×99, SUJA×77, FWDI×113, JANX×36 | SELL BHP (dropped from list after 3 sess (min 3)); SELL MRVI (dropped from list after 3 sess (min 3)); SELL WYFI (dropped from list after 3 sess (min 3)); SELL TOYO (dropped from list after 3 sess (min 3)); SELL DVLT (dropped from list after 3 sess (min 3)); SELL AAP (dropped from list after 3 sess (min 3)); SELL AEG (dropped from list after 3 sess (min 3)); SHORT OCUL x62 @ 10.92; SHORT CRMD x81 @ 8.28; SHORT PUSA x183 @ 3.70; SHORT CAPR x99 @ 6.79; SHORT SUJA x77 @ 8.79; SHORT FWDI x113 @ 5.99; SHORT JANX x36 @ 18.52 |
| 2026-08-26 | +2.02 | $19,729.33 | SAFX×1738, AUTL×280, CRDL×359, CRSP×11, FUTU×6, GMAB×20, ENHA×405, CAN×2360, OCUL×62, CRMD×81, PUSA×183, CAPR×99, SUJA×77, FWDI×113, JANX×36 | — | — | $19,729.33 | $-10,262.18 | $9,467.15 | SAFX×1738, AUTL×280, CRDL×359, CRSP×11, FUTU×6, GMAB×20, ENHA×405, CAN×2360, OCUL×62, CRMD×81, PUSA×183, CAPR×99, SUJA×77, FWDI×113, JANX×36 | hold SAFX,AUTL,CRDL,CRSP,FUTU,GMAB,ENHA,CAN,OCUL,CRMD,PUSA,CAPR,SUJA,FWDI,JANX |
| 2026-08-27 | — | $19,729.33 | SAFX×1738, AUTL×280, CRDL×359, CRSP×11, FUTU×6, GMAB×20, ENHA×405, CAN×2360, OCUL×62, CRMD×81, PUSA×183, CAPR×99, SUJA×77, FWDI×113, JANX×36 | ACMR, GGB, MT, TX, LRCX, MRVL, NUE | SAFX, AUTL, CRDL, CRSP, FUTU, GMAB, ENHA, CAN | $17,383.14 | $-8,478.32 | $8,904.82 | OCUL×62, CRMD×81, PUSA×183, CAPR×99, SUJA×77, FWDI×113, JANX×36, ACMR×6, GGB×127, MT×7, TX×10, LRCX×1, MRVL×2, NUE×2 | SELL SAFX (dropped from list after 5 sess (min 3)); SELL AUTL (dropped from list after 4 sess (min 3)); SELL CRDL (dropped from list after 4 sess (min 3)); SELL CRSP (dropped from list after 4 sess (min 3)); SELL FUTU (dropped from list after 4 sess (min 3)); SELL GMAB (dropped from list after 4 sess (min 3)); SELL ENHA (dropped from list after 4 sess (min 3)); SELL CAN (dropped from list after 4 sess (min 3)); SHORT ACMR x6 @ 80.97; SHORT GGB x127 @ 4.42; SHORT MT x7 @ 75.12; SHORT TX x10 @ 55.20; SHORT LRCX x1 @ 314.61; SHORT MRVL x2 @ 240.00; SHORT NUE x2 @ 248.91 |
| 2026-08-28 | +0.75 | $17,383.14 | OCUL×62, CRMD×81, PUSA×183, CAPR×99, SUJA×77, FWDI×113, JANX×36, ACMR×6, GGB×127, MT×7, TX×10, LRCX×1, MRVL×2, NUE×2 | SEDG, SMTC, PYXS, SAFX, OPTX, TTMI, APMD | OCUL, CRMD, PUSA, SUJA, FWDI, JANX | $17,374.55 | $-8,598.86 | $8,775.69 | CAPR×99, ACMR×6, GGB×127, MT×7, TX×10, LRCX×1, MRVL×2, NUE×2, SEDG×18, SMTC×4, PYXS×189, SAFX×1612, OPTX×73, TTMI×4, APMD×21 | SELL OCUL (dropped from list after 3 sess (min 3)); SELL CRMD (dropped from list after 3 sess (min 3)); SELL PUSA (dropped from list after 3 sess (min 3)); SELL SUJA (dropped from list after 3 sess (min 3)); SELL FWDI (dropped from list after 3 sess (min 3)); SELL JANX (dropped from list after 3 sess (min 3)); SHORT SEDG x18 @ 33.78; SHORT SMTC x4 @ 149.40; SHORT PYXS x189 @ 3.31; SHORT SAFX x1612 @ 0.39; SHORT OPTX x73 @ 8.57; SHORT TTMI x4 @ 127.07; SHORT APMD x21 @ 29.50 |
| 2026-08-31 | -5.85 | $17,374.55 | CAPR×99, ACMR×6, GGB×127, MT×7, TX×10, LRCX×1, MRVL×2, NUE×2, SEDG×18, SMTC×4, PYXS×189, SAFX×1612, OPTX×73, TTMI×4, APMD×21 | — | CAPR | $16,437.70 | $-7,367.99 | $9,069.71 | ACMR×6, GGB×127, MT×7, TX×10, LRCX×1, MRVL×2, NUE×2, SEDG×18, SMTC×4, PYXS×189, SAFX×1612, OPTX×73, TTMI×4, APMD×21 | SELL CAPR (dropped from list after 4 sess (min 3)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $16,437.70 | ACMR×6, GGB×127, MT×7, TX×10, LRCX×1, MRVL×2, NUE×2, SEDG×18, SMTC×4, PYXS×189, SAFX×1612, OPTX×73, TTMI×4, APMD×21 | — | ACMR, GGB, MT, TX, LRCX, MRVL, NUE | $13,124.72 | $-3,891.93 | $9,232.79 | SEDG×18, SMTC×4, PYXS×189, SAFX×1612, OPTX×73, TTMI×4, APMD×21 | SELL ACMR (dropped from list after 3 sess (min 3)); SELL GGB (dropped from list after 3 sess (min 3)); SELL MT (dropped from list after 3 sess (min 3)); SELL TX (dropped from list after 3 sess (min 3)); SELL LRCX (dropped from list after 3 sess (min 3)); SELL MRVL (dropped from list after 3 sess (min 3)); SELL NUE (dropped from list after 3 sess (min 3)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $13,124.72 | SEDG×18, SMTC×4, PYXS×189, SAFX×1612, OPTX×73, TTMI×4, APMD×21 | — | SEDG, SMTC, PYXS, SAFX, OPTX, TTMI, APMD | $9,213.42 | $0.00 | $9,213.42 | — | SELL SEDG (dropped from list after 3 sess (min 3)); SELL SMTC (dropped from list after 3 sess (min 3)); SELL PYXS (dropped from list after 3 sess (min 3)); SELL SAFX (dropped from list after 3 sess (min 3)); SELL OPTX (dropped from list after 3 sess (min 3)); SELL TTMI (dropped from list after 3 sess (min 3)); SELL APMD (dropped from list after 3 sess (min 3)); hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $9,213.42 | — | CABA, FRVO, CTMX, EIX, CRDL, SION, DUOL, SAFX | — | $13,661.59 | $-4,550.79 | $9,110.80 | CABA×176, FRVO×31, CTMX×154, EIX×10, CRDL×266, SION×86, DUOL×3, SAFX×1476 | SHORT CABA x176 @ 3.27; SHORT FRVO x31 @ 18.40; SHORT CTMX x154 @ 3.72; SHORT EIX x10 @ 56.78; SHORT CRDL x266 @ 2.16; SHORT SION x86 @ 6.63; SHORT DUOL x3 @ 156.24; SHORT SAFX x1476 @ 0.39 |
| 2026-09-04 | — | $13,661.59 | CABA×176, FRVO×31, CTMX×154, EIX×10, CRDL×266, SION×86, DUOL×3, SAFX×1476 | ASND, SLBT, MLYS, CCOI, IRD, JLHL | — | $17,916.05 | $-8,711.97 | $9,204.08 | CABA×176, FRVO×31, CTMX×154, EIX×10, CRDL×266, SION×86, DUOL×3, SAFX×1476, ASND×2, SLBT×246, MLYS×25, CCOI×73, IRD×162, JLHL×121 | SHORT ASND x2 @ 266.94; SHORT SLBT x246 @ 3.07; SHORT MLYS x25 @ 29.15; SHORT CCOI x73 @ 10.22; SHORT IRD x162 @ 4.66; SHORT JLHL x121 @ 6.20 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **SHORT** | `TGTX` | 25 | $49.70 | $2.12 | — | $11,240.38 | last bar red; gate last_red=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **SHORT** | `SLS` | 106 | $11.70 | $2.37 | — | $12,478.21 | last bar red; gate last_red=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **SHORT** | `HIMS` | 42 | $29.74 | $2.17 | — | $13,725.12 | last bar red; gate last_red=True; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **SHORT** | `VOR` | 56 | $22.01 | $2.21 | — | $14,955.47 | last bar red; gate last_red=True; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **SHORT** | `TLN` | 1 | $359.83 | $2.02 | — | $15,313.28 | last bar red; gate last_red=True; list flatten; 🔵; ret5=+5.9; leftover $620.53 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `NRG` | 5 | $120.00 | $2.04 | — | $15,911.24 | last bar red; gate last_red=True; list flatten; 🔵; ret5=+0.6; leftover $620.53 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `MARA` | 68 | $9.01 | $2.23 | — | $16,521.68 | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=-13.5; leftover $620.53 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `FOSL` | 110 | $5.64 | $2.37 | — | $17,139.72 | last bar red; gate last_red=True; list probable; 🔵; ret5=-4.1; leftover $620.53 | join🟢 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `ARX` | 31 | $19.57 | $2.12 | — | $17,744.27 | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $620.53 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `CRMD` | 77 | $8.05 | $2.26 | — | $18,361.86 | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $620.53 | join🟢 sector🔴 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `BIRK` | 15 | $39.75 | $2.07 | — | $18,956.04 | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=+10.2; leftover $620.53 | join🟢 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `HLIT` | 47 | $13.18 | $2.17 | — | $19,573.33 | last bar red; gate last_red=True; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $620.53 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `TMC` | 152 | $4.05 | $2.50 | — | $20,186.43 | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $616.74 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `TGB` | 72 | $8.46 | $2.24 | — | $20,793.31 | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=+0.4; leftover $616.74 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `ELF` | 6 | $90.54 | $2.04 | — | $21,334.50 | last bar red; gate last_red=True; list flatten; ret5=-7.2; leftover $616.74 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `DNN` | 190 | $3.24 | $2.62 | — | $21,947.48 | last bar red; gate last_red=True; list flatten; ⚪; ret5=+0.3; leftover $616.74 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `HNST` | 128 | $4.81 | $2.42 | — | $22,560.74 | last bar red; gate last_red=True; list flatten; ⚪; ret5=-11.4; leftover $616.74 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `CAPR` | 89 | $6.87 | $2.30 | — | $23,169.87 | last bar red; gate last_red=True; list yday_gainer,yday_mover; ret5=+62.6; leftover $616.74 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `BYND` | 48 | $12.83 | $2.17 | — | $23,783.54 | last bar red; gate last_red=True; list yday_gainer,yday_mover; ⚪; ret5=-34.1; leftover $616.74 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `NU` | 40 | $15.40 | $2.15 | — | $24,397.40 | last bar red; gate last_red=True; list yday_gainer; 🔵; ⚪; ret5=+10.0; leftover $616.74 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **COVER** | `TGTX` | 25 | $49.28 | $2.06 | $+6.32 | $23,163.33 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **COVER** | `SLS` | 106 | $12.66 | $2.31 | $-106.44 | $21,819.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **COVER** | `HIMS` | 42 | $27.85 | $2.12 | $+75.09 | $20,647.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **COVER** | `VOR` | 56 | $22.82 | $2.16 | $-49.73 | $19,367.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `TLN` | 1 | $321.00 | $1.99 | $+34.81 | $19,044.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `NRG` | 5 | $116.20 | $2.00 | $+14.95 | $18,461.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `MARA` | 68 | $8.91 | $2.19 | $+2.37 | $17,853.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `FOSL` | 110 | $5.54 | $2.32 | $+6.31 | $17,241.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `ARX` | 31 | $19.58 | $2.08 | $-4.51 | $16,632.31 | dropped from list after 3 sess (min 3) | join🟡 sector🟡 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **COVER** | `CRMD` | 77 | $8.30 | $2.22 | $-23.73 | $15,990.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `BIRK` | 15 | $37.50 | $2.04 | $+29.64 | $15,426.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `HLIT` | 47 | $12.90 | $2.13 | $+8.86 | $14,818.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `TMC` | 152 | $3.92 | $2.45 | $+14.82 | $14,219.74 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `TGB` | 72 | $8.35 | $2.21 | $+3.47 | $13,616.33 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ELF` | 6 | $98.15 | $2.01 | $-49.71 | $13,025.43 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `DNN` | 190 | $3.20 | $2.56 | $+2.42 | $12,414.87 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HNST` | 128 | $4.98 | $2.37 | $-26.56 | $11,775.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `CAPR` | 89 | $7.66 | $2.26 | $-74.87 | $11,091.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `BYND` | 48 | $13.60 | $2.13 | $-41.26 | $10,436.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `NU` | 40 | $14.74 | $2.11 | $+21.94 | $9,844.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SHORT** | `BHP` | 6 | $91.01 | $2.04 | — | $10,388.23 | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $615.26 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `MRVI` | 83 | $7.38 | $2.28 | — | $10,998.49 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $615.26 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 28 | $21.40 | $2.11 | — | $11,595.58 | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-25.2; leftover $615.26 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 138 | $4.43 | $2.45 | — | $12,204.47 | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-23.1; leftover $615.26 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `DVLT` | 2050 | $0.30 | $12.67 | — | $12,806.80 | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-3.2; leftover $615.26 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `SAFX` | 1738 | $0.35 | $11.68 | — | $13,410.37 | last bar red; gate last_red=True; list yday_mover; 🔵; ret5=-29.4; leftover $615.26 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 13 | $46.85 | $2.07 | — | $14,017.35 | last bar red; gate last_red=True; list earn_react; 🔵; ret5=+5.0; leftover $615.26 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AEG` | 68 | $9.01 | $2.23 | — | $14,627.80 | last bar red; gate last_red=True; list earn_react; 🔵; ⚪; ret5=-1.3; leftover $615.26 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `AUTL` | 280 | $2.47 | $3.69 | — | $15,315.71 | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $693.86 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `CRDL` | 359 | $1.93 | $4.72 | — | $16,003.86 | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $693.86 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `CRSP` | 11 | $59.72 | $2.06 | — | $16,658.72 | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $693.86 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `FUTU` | 6 | $115.18 | $2.05 | — | $17,347.76 | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $693.86 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `GMAB` | 20 | $33.36 | $2.09 | — | $18,012.87 | last bar red; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.6; leftover $693.86 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `ENHA` | 405 | $1.71 | $5.32 | — | $18,700.10 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $693.86 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `CAN` | 2360 | $0.29 | $14.44 | — | $19,379.50 | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $693.86 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **COVER** | `BHP` | 6 | $95.95 | $2.01 | $-33.69 | $18,801.79 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `MRVI` | 83 | $8.31 | $2.24 | $-81.71 | $18,109.82 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 28 | $20.98 | $2.07 | $+7.58 | $17,520.31 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TOYO` | 138 | $4.48 | $2.40 | $-11.76 | $16,899.66 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `DVLT` | 2050 | $0.32 | $12.71 | $-66.38 | $16,230.95 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AAP` | 13 | $43.61 | $2.03 | $+38.03 | $15,661.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AEG` | 68 | $9.29 | $2.19 | $-23.47 | $15,028.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SHORT** | `OCUL` | 62 | $10.92 | $2.21 | — | $15,702.90 | last bar red; gate last_red=True; list flatten; 🔵; ret5=+10.4; leftover $677.38 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **SHORT** | `CRMD` | 81 | $8.28 | $2.27 | — | $16,371.31 | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=+8.8; leftover $677.38 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `PUSA` | 183 | $3.70 | $2.60 | — | $17,045.81 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.8; leftover $677.38 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `CAPR` | 99 | $6.79 | $2.33 | — | $17,715.69 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.4; leftover $677.38 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `SUJA` | 77 | $8.79 | $2.26 | — | $18,390.26 | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.5; leftover $677.38 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `FWDI` | 113 | $5.99 | $2.38 | — | $19,064.75 | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+20.7; leftover $677.38 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `JANX` | 36 | $18.52 | $2.14 | — | $19,729.33 | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=+7.9; leftover $677.38 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-27 09:30 ET | **COVER** | `SAFX` | 1738 | $0.35 | $11.30 | $-16.03 | $19,109.74 | dropped from list after 5 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `AUTL` | 280 | $2.41 | $3.61 | $+9.50 | $18,431.33 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `CRDL` | 359 | $2.03 | $4.63 | $-45.25 | $17,697.92 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `CRSP` | 11 | $60.18 | $2.02 | $-9.14 | $17,033.92 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `FUTU` | 6 | $124.67 | $2.01 | $-60.99 | $16,283.89 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `GMAB` | 20 | $33.78 | $2.05 | $-12.54 | $15,606.24 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `ENHA` | 405 | $1.63 | $5.22 | $+21.85 | $14,940.87 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `CAN` | 2360 | $0.40 | $16.52 | $-281.12 | $13,980.35 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SHORT** | `ACMR` | 6 | $80.97 | $2.04 | — | $14,464.13 | last bar red; gate last_red=True; list mover_buy; 🔵; ret5=-1.3; leftover $564.03 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `GGB` | 127 | $4.42 | $2.42 | — | $15,023.05 | last bar red; gate last_red=True; list mover_buy; 🔵; ret5=-8.6; leftover $564.03 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 7 | $75.12 | $2.05 | — | $15,546.84 | last bar red; gate last_red=True; list mover_buy; 🔵; ret5=-2.2; leftover $564.03 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 10 | $55.20 | $2.06 | — | $16,096.79 | last bar red; gate last_red=True; list mover_buy; 🔵; ret5=+3.0; leftover $564.03 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `LRCX` | 1 | $314.61 | $2.02 | — | $16,409.38 | last bar red; gate last_red=True; list mover_buy; 🔵; ret5=-5.5; leftover $564.03 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `MRVL` | 2 | $240.00 | $2.03 | — | $16,887.35 | last bar red; gate last_red=True; list mover_buy; 🔵; ret5=+6.8; leftover $564.03 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SHORT** | `NUE` | 2 | $248.91 | $2.03 | — | $17,383.14 | last bar red; gate last_red=True; list mover_buy; 🔵; ret5=-9.4; leftover $564.03 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **COVER** | `OCUL` | 62 | $10.63 | $2.18 | $+13.59 | $16,721.90 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `CRMD` | 81 | $8.49 | $2.23 | $-21.52 | $16,031.98 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `PUSA` | 183 | $3.86 | $2.54 | $-34.42 | $15,323.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `SUJA` | 77 | $9.41 | $2.22 | $-52.22 | $14,596.27 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `FWDI` | 113 | $6.39 | $2.33 | $-49.91 | $13,871.87 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `JANX` | 36 | $19.00 | $2.10 | $-21.51 | $13,185.77 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SHORT** | `SEDG` | 18 | $33.78 | $2.08 | — | $13,791.73 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $628.69 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `SMTC` | 4 | $149.40 | $2.04 | — | $14,387.29 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $628.69 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `PYXS` | 189 | $3.31 | $2.62 | — | $15,010.27 | last bar red; gate last_red=True; list yday_gainer; ret5=+2.3; leftover $628.69 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `SAFX` | 1612 | $0.39 | $11.42 | — | $15,627.53 | last bar red; gate last_red=True; list yday_gainer; ret5=-26.5; leftover $628.69 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `OPTX` | 73 | $8.57 | $2.25 | — | $16,250.89 | last bar red; gate last_red=True; list yday_gainer; ret5=-3.4; leftover $628.69 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `TTMI` | 4 | $127.07 | $2.04 | — | $16,757.14 | last bar red; gate last_red=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=-21.0; leftover $628.69 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `APMD` | 21 | $29.50 | $2.09 | — | $17,374.55 | last bar red; gate last_red=True; list yday_gainer; ret5=-11.7; leftover $628.69 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-31 09:30 ET | **COVER** | `CAPR` | 99 | $9.44 | $2.29 | $-266.97 | $16,437.70 | dropped from list after 4 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `ACMR` | 6 | $71.24 | $2.01 | $+54.33 | $16,008.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `GGB` | 127 | $4.61 | $2.37 | $-28.92 | $15,420.41 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `MT` | 7 | $74.31 | $2.01 | $+1.61 | $14,898.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `TX` | 10 | $54.82 | $2.02 | $-0.28 | $14,348.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `LRCX` | 1 | $300.97 | $1.99 | $+9.63 | $14,045.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `MRVL` | 2 | $210.57 | $2.00 | $+54.83 | $13,621.91 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `NUE` | 2 | $247.60 | $2.00 | $-1.41 | $13,124.72 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `SEDG` | 18 | $31.87 | $2.04 | $+30.26 | $12,549.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `SMTC` | 4 | $127.63 | $2.00 | $+83.04 | $12,036.49 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `PYXS` | 189 | $3.24 | $2.56 | $+8.06 | $11,421.57 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `SAFX` | 1612 | $0.37 | $10.80 | $+10.02 | $10,814.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `OPTX` | 73 | $7.94 | $2.21 | $+41.53 | $10,232.50 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `TTMI` | 4 | $116.68 | $2.00 | $+37.52 | $9,763.78 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `APMD` | 21 | $26.11 | $2.05 | $+67.05 | $9,213.42 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **SHORT** | `CABA` | 176 | $3.27 | $2.57 | — | $9,786.36 | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $575.84 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `FRVO` | 31 | $18.40 | $2.12 | — | $10,354.65 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $575.84 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `CTMX` | 154 | $3.72 | $2.50 | — | $10,925.02 | last bar red; gate last_red=True; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $575.84 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `EIX` | 10 | $56.78 | $2.06 | — | $11,490.77 | last bar red; gate last_red=True; list probable,yday_gainer; ret5=+0.3; leftover $575.84 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `CRDL` | 266 | $2.16 | $3.50 | — | $12,061.83 | last bar red; gate last_red=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $575.84 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `SION` | 86 | $6.63 | $2.29 | — | $12,629.72 | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=-18.1; leftover $575.84 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `DUOL` | 3 | $156.24 | $2.03 | — | $13,096.41 | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=+10.0; leftover $575.84 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `SAFX` | 1476 | $0.39 | $10.46 | — | $13,661.59 | last bar red; gate last_red=True; list yday_gainer; ret5=-26.5; leftover $575.84 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `ASND` | 2 | $266.94 | $2.03 | — | $14,193.44 | last bar red; gate last_red=True; list flatten; ret5=+1.9; leftover $756.14 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `SLBT` | 246 | $3.07 | $3.24 | — | $14,945.42 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $756.14 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `MLYS` | 25 | $29.15 | $2.10 | — | $15,672.06 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $756.14 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `CCOI` | 73 | $10.22 | $2.25 | — | $16,415.87 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $756.14 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `IRD` | 162 | $4.66 | $2.53 | — | $17,168.26 | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $756.14 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `JLHL` | 121 | $6.20 | $2.40 | — | $17,916.05 | last bar red; gate last_red=True; list yday_gainer,yday_mover; ret5=-8.2; leftover $756.14 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `NRG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `FOSL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `CRMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BIRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HLIT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NRG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `FOSL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BIRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HLIT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ELF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `HNST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `BYND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `NU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TBPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INDP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ENHA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ENVX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INV` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ELF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `HNST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `BYND` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `NU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `INMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WYFI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `TOYO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DVLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AAP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AEG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MRVI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WYFI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `TOYO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `DVLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AAP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AEG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRSP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `FUTU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `GMAB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ENHA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CAN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `XHG` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AEM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SBSW` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `FUTU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `GMAB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ENHA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CAN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `SAFX` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AUTL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CRDL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CRSP` | no_price | no 09:30 open — carry |
| 2026-08-26 | `FUTU` | no_price | no 09:30 open — carry |
| 2026-08-26 | `GMAB` | no_price | no 09:30 open — carry |
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
| 2026-08-27 | `MU` | cash | leftover split 564.03 < 1 share @ 925.74 |
| 2026-08-28 | `ACMR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `GGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `MT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `TX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `LRCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `MRVL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `NUE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ACMR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `GGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `MT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `TX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `LRCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `MRVL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `NUE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `PYXS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `OPTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TTMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `APMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
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
| 2026-09-01 | `PYXS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SAFX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `OPTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TTMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `APMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
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

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CABA` | 176 | 2026-09-03 @ $3.27 | last bar red; gate last_red=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $575.84 |
| `FRVO` | 31 | 2026-09-03 @ $18.40 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $575.84 |
| `CTMX` | 154 | 2026-09-03 @ $3.72 | last bar red; gate last_red=True; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $575.84 |
| `EIX` | 10 | 2026-09-03 @ $56.78 | last bar red; gate last_red=True; list probable,yday_gainer; ret5=+0.3; leftover $575.84 |
| `CRDL` | 266 | 2026-09-03 @ $2.16 | last bar red; gate last_red=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $575.84 |
| `SION` | 86 | 2026-09-03 @ $6.63 | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=-18.1; leftover $575.84 |
| `DUOL` | 3 | 2026-09-03 @ $156.24 | last bar red; gate last_red=True; list yday_gainer; 🔵; ret5=+10.0; leftover $575.84 |
| `SAFX` | 1476 | 2026-09-03 @ $0.39 | last bar red; gate last_red=True; list yday_gainer; ret5=-26.5; leftover $575.84 |
| `ASND` | 2 | 2026-09-04 @ $266.94 | last bar red; gate last_red=True; list flatten; ret5=+1.9; leftover $756.14 |
| `SLBT` | 246 | 2026-09-04 @ $3.07 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $756.14 |
| `MLYS` | 25 | 2026-09-04 @ $29.15 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $756.14 |
| `CCOI` | 73 | 2026-09-04 @ $10.22 | last bar red; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $756.14 |
| `IRD` | 162 | 2026-09-04 @ $4.66 | last bar red; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $756.14 |
| `JLHL` | 121 | 2026-09-04 @ $6.20 | last bar red; gate last_red=True; list yday_gainer,yday_mover; ret5=-8.2; leftover $756.14 |
