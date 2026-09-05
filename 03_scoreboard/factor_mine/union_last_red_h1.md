# Factor mine action — `union_last_red_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ last_red, no 🚨

Cash book **+0.75%** ($10,075) · signal-only (no cash/fees) was +6.47%. Starts YES **5/17**. Fills 138 · skips 55 · realized $+33.45.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `last_red=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $199.47.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | TGTX, SLS, HIMS, VOR | — | $28.15 | $10,078.13 | $10,106.28 | TGTX×50, SLS×213, HIMS×84, VOR×113 | BUY TGTX x50 @ 49.70; BUY SLS x213 @ 11.70; BUY HIMS x84 @ 29.74; BUY VOR x113 @ 22.01 |
| 2026-08-14 | +5.50 | $28.15 | TGTX×50, SLS×213, HIMS×84, VOR×113 | TLN, NRG, MARA, ARX, HLIT, SECZ, LFTO, REZI | TGTX, SLS, HIMS, VOR | $274.27 | $9,994.61 | $10,268.88 | TLN×3, NRG×10, MARA×140, ARX×64, HLIT×95, SECZ×216, LFTO×61, REZI×61 | SELL TGTX (dropped from list after 1 sess (min 1)); SELL SLS (dropped from list after 1 sess (min 1)); SELL HIMS (dropped from list after 1 sess (min 1)); SELL VOR (dropped from list after 1 sess (min 1)); BUY TLN x3 @ 359.83; BUY NRG x10 @ 120.00; BUY MARA x140 @ 9.01; BUY ARX x64 @ 19.57; BUY HLIT x95 @ 13.18; BUY SECZ x216 @ 5.84; BUY LFTO x61 @ 20.57; BUY REZI x61 @ 20.56 |
| 2026-08-17 | +2.25 | $274.27 | TLN×3, NRG×10, MARA×140, ARX×64, HLIT×95, SECZ×216, LFTO×61, REZI×61 | TMC, TGB, ELF, DNN, CAPR, NU, INV, KLC | TLN, NRG, MARA, ARX, HLIT, SECZ, LFTO, REZI | $2.16 | $10,004.95 | $10,007.11 | TMC×315, TGB×151, ELF×14, DNN×394, CAPR×185, NU×82, INV×788, KLC×487 | SELL TLN (dropped from list after 1 sess (min 1)); SELL NRG (dropped from list after 1 sess (min 1)); SELL MARA (dropped from list after 1 sess (min 1)); SELL ARX (dropped from list after 1 sess (min 1)); SELL HLIT (dropped from list after 1 sess (min 1)); SELL SECZ (dropped from list after 1 sess (min 1)); SELL LFTO (dropped from list after 1 sess (min 1)); SELL REZI (dropped from list after 1 sess (min 1)); BUY TMC x315 @ 4.05; BUY TGB x151 @ 8.46; BUY ELF x14 @ 90.54; BUY DNN x394 @ 3.24; BUY CAPR x185 @ 6.87; BUY NU x82 @ 15.40; BUY INV x788 @ 1.62; BUY KLC x487 @ 2.62 |
| 2026-08-18 | -6.20 | $2.16 | TMC×315, TGB×151, ELF×14, DNN×394, CAPR×185, NU×82, INV×788, KLC×487 | — | TMC, TGB, ELF, DNN, CAPR, NU, INV, KLC | $9,813.47 | $0.00 | $9,813.47 | — | SELL TMC (dropped from list after 1 sess (min 1)); SELL TGB (dropped from list after 1 sess (min 1)); SELL ELF (dropped from list after 1 sess (min 1)); SELL DNN (dropped from list after 1 sess (min 1)); SELL CAPR (dropped from list after 1 sess (min 1)); SELL NU (dropped from list after 1 sess (min 1)); SELL INV (dropped from list after 1 sess (min 1)); SELL KLC (dropped from list after 1 sess (min 1)); hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $9,813.47 | — | — | — | $9,813.47 | $0.00 | $9,813.47 | — | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $9,813.47 | — | BHP, MRVI, WYFI, TOYO, DVLT, SAFX, AAP, AEG | — | $4.42 | $9,864.77 | $9,869.18 | BHP×13, MRVI×166, WYFI×57, TOYO×276, DVLT×4088, SAFX×3465, AAP×26, AEG×136 | BUY BHP x13 @ 91.01; BUY MRVI x166 @ 7.38; BUY WYFI x57 @ 21.40; BUY TOYO x276 @ 4.43; BUY DVLT x4088 @ 0.30; BUY SAFX x3465 @ 0.35; BUY AAP x26 @ 46.85; BUY AEG x136 @ 9.01 |
| 2026-08-21 | +3.25 | $4.42 | BHP×13, MRVI×166, WYFI×57, TOYO×276, DVLT×4088, SAFX×3465, AAP×26, AEG×136 | AUTL, CRDL, CRSP, FUTU, GMAB, ENHA, CAN | BHP, WYFI, TOYO, DVLT, SAFX, AAP, AEG | $53.89 | $10,124.59 | $10,178.48 | MRVI×166, AUTL×492, CRDL×630, CRSP×20, FUTU×10, GMAB×36, ENHA×711, CAN×4139 | SELL BHP (dropped from list after 1 sess (min 1)); SELL WYFI (dropped from list after 1 sess (min 1)); SELL TOYO (dropped from list after 1 sess (min 1)); SELL DVLT (dropped from list after 1 sess (min 1)); SELL SAFX (dropped from list after 1 sess (min 1)); SELL AAP (dropped from list after 1 sess (min 1)); SELL AEG (dropped from list after 1 sess (min 1)); BUY AUTL x492 @ 2.47; BUY CRDL x630 @ 1.93; BUY CRSP x20 @ 59.72; BUY FUTU x10 @ 115.18; BUY GMAB x36 @ 33.36; BUY ENHA x711 @ 1.71; BUY CAN x4139 @ 0.29 |
| 2026-08-24 | -5.17 | $53.89 | MRVI×166, AUTL×492, CRDL×630, CRSP×20, FUTU×10, GMAB×36, ENHA×711, CAN×4139 | — | MRVI, AUTL, CRDL, FUTU, GMAB, ENHA, CAN | $8,959.72 | $1,138.20 | $10,097.92 | CRSP×20 | SELL MRVI (dropped from list after 2 sess (min 1)); SELL AUTL (dropped from list after 1 sess (min 1)); SELL CRDL (dropped from list after 1 sess (min 1)); SELL FUTU (dropped from list after 1 sess (min 1)); SELL GMAB (dropped from list after 1 sess (min 1)); SELL ENHA (dropped from list after 1 sess (min 1)); SELL CAN (dropped from list after 1 sess (min 1)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $8,959.72 | CRSP×20 | OCUL, CRMD, PUSA, CAPR, SAFX, SUJA, FWDI, JANX | CRSP | $5.77 | $10,164.04 | $10,169.81 | OCUL×115, CRMD×152, PUSA×341, CAPR×185, SAFX×3411, SUJA×143, FWDI×210, JANX×67 | SELL CRSP (dropped from list after 2 sess (min 1)); BUY OCUL x115 @ 10.92; BUY CRMD x152 @ 8.28; BUY PUSA x341 @ 3.70; BUY CAPR x185 @ 6.79; BUY SAFX x3411 @ 0.37; BUY SUJA x143 @ 8.79; BUY FWDI x210 @ 5.99; BUY JANX x67 @ 18.52 |
| 2026-08-26 | +2.02 | $5.77 | OCUL×115, CRMD×152, PUSA×341, CAPR×185, SAFX×3411, SUJA×143, FWDI×210, JANX×67 | — | — | $5.77 | $10,049.99 | $10,055.76 | OCUL×115, CRMD×152, PUSA×341, CAPR×185, SAFX×3411, SUJA×143, FWDI×210, JANX×67 | hold OCUL,CRMD,PUSA,CAPR,SAFX,SUJA,FWDI,JANX |
| 2026-08-27 | — | $5.77 | OCUL×115, CRMD×152, PUSA×341, CAPR×185, SAFX×3411, SUJA×143, FWDI×210, JANX×67 | ACMR, GGB, MT, MU, TX, LRCX, MRVL, NUE | OCUL, CRMD, PUSA, CAPR, SAFX, SUJA, FWDI, JANX | $606.82 | $9,787.01 | $10,393.83 | ACMR×16, GGB×293, MT×17, MU×1, TX×23, LRCX×4, MRVL×5, NUE×5 | SELL OCUL (dropped from list after 2 sess (min 1)); SELL CRMD (dropped from list after 2 sess (min 1)); SELL PUSA (dropped from list after 2 sess (min 1)); SELL CAPR (dropped from list after 2 sess (min 1)); SELL SAFX (dropped from list after 2 sess (min 1)); SELL SUJA (dropped from list after 2 sess (min 1)); SELL FWDI (dropped from list after 2 sess (min 1)); SELL JANX (dropped from list after 2 sess (min 1)); BUY ACMR x16 @ 80.97; BUY GGB x293 @ 4.42; BUY MT x17 @ 75.12; BUY MU x1 @ 925.74; BUY TX x23 @ 55.20; BUY LRCX x4 @ 314.61; BUY MRVL x5 @ 240.00; BUY NUE x5 @ 248.91 |
| 2026-08-28 | +0.75 | $606.82 | ACMR×16, GGB×293, MT×17, MU×1, TX×23, LRCX×4, MRVL×5, NUE×5 | CAPR, SEDG, SMTC, OPTX, TTMI, BBWI, BTSG, CRDL | ACMR, GGB, MT, MU, TX, LRCX, MRVL, NUE | $195.55 | $10,350.10 | $10,545.65 | CAPR×143, SEDG×39, SMTC×8, OPTX×153, TTMI×10, BBWI×70, BTSG×21, CRDL×630 | SELL ACMR (dropped from list after 1 sess (min 1)); SELL GGB (dropped from list after 1 sess (min 1)); SELL MT (dropped from list after 1 sess (min 1)); SELL MU (dropped from list after 1 sess (min 1)); SELL TX (dropped from list after 1 sess (min 1)); SELL LRCX (dropped from list after 1 sess (min 1)); SELL MRVL (dropped from list after 1 sess (min 1)); SELL NUE (dropped from list after 1 sess (min 1)); BUY CAPR x143 @ 9.19; BUY SEDG x39 @ 33.78; BUY SMTC x8 @ 149.40; BUY OPTX x153 @ 8.57; BUY TTMI x10 @ 127.07; BUY BBWI x70 @ 18.68; BUY BTSG x21 @ 61.42; BUY CRDL x630 @ 2.09 |
| 2026-08-31 | -5.85 | $195.55 | CAPR×143, SEDG×39, SMTC×8, OPTX×153, TTMI×10, BBWI×70, BTSG×21, CRDL×630 | — | CAPR, SEDG, SMTC, OPTX, TTMI, BBWI, BTSG, CRDL | $10,128.84 | $0.00 | $10,128.84 | — | SELL CAPR (dropped from list after 1 sess (min 1)); SELL SEDG (dropped from list after 1 sess (min 1)); SELL SMTC (dropped from list after 1 sess (min 1)); SELL OPTX (dropped from list after 1 sess (min 1)); SELL TTMI (dropped from list after 1 sess (min 1)); SELL BBWI (dropped from list after 1 sess (min 1)); SELL BTSG (dropped from list after 1 sess (min 1)); SELL CRDL (dropped from list after 1 sess (min 1)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $10,128.84 | — | — | — | $10,128.84 | $0.00 | $10,128.84 | — | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $10,128.84 | — | — | — | $10,128.84 | $0.00 | $10,128.84 | — | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $10,128.84 | — | CABA, FRVO, CTMX, EIX, CRDL, SION, DUOL, SAFX | — | $8.71 | $10,240.01 | $10,248.72 | CABA×387, FRVO×68, CTMX×340, EIX×22, CRDL×586, SION×190, DUOL×8, SAFX×3246 | BUY CABA x387 @ 3.27; BUY FRVO x68 @ 18.40; BUY CTMX x340 @ 3.72; BUY EIX x22 @ 56.78; BUY CRDL x586 @ 2.16; BUY SION x190 @ 6.63; BUY DUOL x8 @ 156.24; BUY SAFX x3246 @ 0.39 |
| 2026-09-04 | — | $8.71 | CABA×387, FRVO×68, CTMX×340, EIX×22, CRDL×586, SION×190, DUOL×8, SAFX×3246 | ASND, SLBT, MLYS, CCOI, IRD, JLHL | FRVO, CTMX, EIX, CRDL, DUOL, SAFX | $199.47 | $9,875.67 | $10,075.14 | CABA×387, SION×190, ASND×4, SLBT×407, MLYS×42, CCOI×122, IRD×268, JLHL×201 | SELL FRVO (dropped from list after 1 sess (min 1)); SELL CTMX (dropped from list after 1 sess (min 1)); SELL EIX (dropped from list after 1 sess (min 1)); SELL CRDL (dropped from list after 1 sess (min 1)); SELL DUOL (dropped from list after 1 sess (min 1)); SELL SAFX (dropped from list after 1 sess (min 1)); BUY ASND x4 @ 266.94; BUY SLBT x407 @ 3.07; BUY MLYS x42 @ 29.15; BUY CCOI x122 @ 10.22; BUY IRD x268 @ 4.66; BUY JLHL x201 @ 6.20 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 50 | $49.70 | $2.14 | — | $7,512.86 | ▼ $9,997.86 (-2.14) | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=-0.8; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 213 | $11.70 | $2.75 | — | $5,018.01 | ▼ $9,995.11 (-4.89) | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=-0.8; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 84 | $29.74 | $2.24 | — | $2,517.61 | ▼ $9,992.87 (-7.13) | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=-5.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 113 | $22.01 | $2.33 | — | $28.15 | ▼ $9,990.54 (-9.46) | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=+0.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 50 | $47.27 | $2.17 | $-125.81 | $2,389.48 | ▲ $10,115.57 (+115.57) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 213 | $12.40 | $2.80 | $+143.55 | $5,027.88 | ▲ $10,112.77 (+112.77) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 84 | $29.15 | $2.28 | $-54.08 | $7,474.20 | ▲ $10,110.49 (+110.49) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 113 | $23.33 | $2.37 | $+144.46 | $10,108.12 | ▲ $10,108.12 (+108.12) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $9,026.63 | ▲ $10,106.12 (+106.12) | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ret5=+5.9; leftover $1263.52 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $7,824.61 | ▲ $10,104.10 (+104.10) | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ret5=+0.6; leftover $1263.52 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 140 | $9.01 | $2.41 | — | $6,560.80 | ▲ $10,101.69 (+101.69) | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1263.52 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 64 | $19.57 | $2.18 | — | $5,306.14 | ▲ $10,099.51 (+99.51) | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1263.52 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 95 | $13.18 | $2.27 | — | $4,051.77 | ▲ $10,097.24 (+97.24) | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1263.52 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SECZ` | 216 | $5.84 | $2.79 | — | $2,787.54 | ▲ $10,094.45 (+94.45) | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ⚪; ret5=-20.7; leftover $1263.52 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LFTO` | 61 | $20.57 | $2.17 | — | $1,530.60 | ▲ $10,092.28 (+92.28) | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ⚪; ret5=-14.0; leftover $1263.52 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `REZI` | 61 | $20.56 | $2.17 | — | $274.27 | ▲ $10,090.11 (+90.11) | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ⚪; ret5=-21.5; leftover $1263.52 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 3 | $367.88 | $2.02 | $+20.13 | $1,375.89 | ▲ $10,236.80 (+236.80) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $2,647.85 | ▲ $10,234.76 (+234.76) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 140 | $9.22 | $2.44 | $+24.55 | $3,936.20 | ▲ $10,232.31 (+232.31) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 64 | $19.57 | $2.20 | $-4.38 | $5,186.48 | ▲ $10,230.11 (+230.11) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 95 | $13.84 | $2.30 | $+58.12 | $6,498.98 | ▲ $10,227.81 (+227.81) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SECZ` | 216 | $5.45 | $2.83 | $-89.86 | $7,673.35 | ▲ $10,224.98 (+224.98) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LFTO` | 61 | $21.00 | $2.19 | $+21.86 | $8,952.15 | ▲ $10,222.78 (+222.78) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `REZI` | 61 | $20.83 | $2.19 | $+12.10 | $10,220.59 | ▲ $10,220.59 (+220.59) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 315 | $4.05 | $4.06 | — | $8,940.78 | ▲ $10,216.53 (+216.53) | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1277.57 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 151 | $8.46 | $2.44 | — | $7,660.87 | ▲ $10,214.08 (+214.08) | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1277.57 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 14 | $90.54 | $2.03 | — | $6,391.28 | ▲ $10,212.05 (+212.05) | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ret5=-7.2; leftover $1277.57 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 394 | $3.24 | $5.08 | — | $5,109.64 | ▲ $10,206.97 (+206.97) | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=+0.3; leftover $1277.57 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 185 | $6.87 | $2.54 | — | $3,836.14 | ▲ $10,204.42 (+204.42) | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; ret5=+62.6; leftover $1277.57 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NU` | 82 | $15.40 | $2.24 | — | $2,571.11 | ▲ $10,202.19 (+202.19) | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ⚪; ret5=+10.0; leftover $1277.57 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `INV` | 788 | $1.62 | $10.17 | — | $1,284.38 | ▲ $10,192.02 (+192.02) | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ⚪; ret5=-53.0; leftover $1277.57 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `KLC` | 487 | $2.62 | $6.28 | — | $2.16 | ▲ $10,185.74 (+185.74) | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ⚪; ret5=-49.7; leftover $1277.57 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 315 | $3.72 | $4.13 | $-112.14 | $1,169.83 | ▼ $9,844.68 (-155.32) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 151 | $8.55 | $2.48 | $+8.67 | $2,458.41 | ▼ $9,842.21 (-157.79) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 14 | $93.44 | $2.05 | $+36.52 | $3,764.51 | ▼ $9,840.15 (-159.85) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 394 | $3.11 | $5.16 | $-61.46 | $4,984.70 | ▼ $9,835.00 (-165.00) | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `CAPR` | 185 | $7.50 | $2.59 | $+111.42 | $6,369.61 | ▼ $9,832.41 (-167.59) | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `NU` | 82 | $14.53 | $2.26 | $-75.84 | $7,558.81 | ▼ $9,830.15 (-169.85) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `INV` | 788 | $1.32 | $10.31 | $-252.93 | $8,592.60 | ▼ $9,819.84 (-180.16) | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `KLC` | 487 | $2.52 | $6.37 | $-61.36 | $9,813.47 | ▼ $9,813.47 (-186.53) | dropped from list after 1 sess (min 1) | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,628.31 | ▼ $9,811.44 (-188.56) | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1226.68 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 166 | $7.38 | $2.49 | — | $7,400.74 | ▼ $9,808.95 (-191.05) | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1226.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WYFI` | 57 | $21.40 | $2.16 | — | $6,178.78 | ▼ $9,806.79 (-193.21) | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-25.2; leftover $1226.68 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 276 | $4.43 | $3.56 | — | $4,952.54 | ▼ $9,803.23 (-196.77) | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-23.1; leftover $1226.68 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 4088 | $0.30 | $24.53 | — | $3,701.61 | ▼ $9,778.70 (-221.30) | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-3.2; leftover $1226.68 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SAFX` | 3465 | $0.35 | $22.66 | — | $2,452.34 | ▼ $9,756.04 (-243.96) | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-29.4; leftover $1226.68 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 26 | $46.85 | $2.07 | — | $1,232.17 | ▼ $9,753.97 (-246.03) | union ∩ last_red, no 🚨; gate last_red=True; list earn_react; 🔵; ret5=+5.0; leftover $1226.68 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 136 | $9.01 | $2.40 | — | $4.42 | ▼ $9,751.58 (-248.42) | union ∩ last_red, no 🚨; gate last_red=True; list earn_react; 🔵; ⚪; ret5=-1.3; leftover $1226.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $1,246.73 | ▼ $9,939.52 (-60.48) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WYFI` | 57 | $21.54 | $2.18 | $+3.64 | $2,472.33 | ▼ $9,937.34 (-62.66) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `TOYO` | 276 | $4.68 | $3.62 | $+61.82 | $3,760.39 | ▼ $9,933.72 (-66.28) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DVLT` | 4088 | $0.31 | $25.63 | $-9.27 | $5,002.04 | ▼ $9,908.09 (-91.91) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SAFX` | 3465 | $0.35 | $23.11 | $-59.63 | $6,191.69 | ▼ $9,884.99 (-115.01) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AAP` | 26 | $42.41 | $2.09 | $-119.60 | $7,292.26 | ▼ $9,882.90 (-117.10) | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `AEG` | 136 | $9.04 | $2.43 | $-0.75 | $8,519.27 | ▼ $9,880.47 (-119.53) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 492 | $2.47 | $6.35 | — | $7,297.68 | ▼ $9,874.12 (-125.88) | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1217.04 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 630 | $1.93 | $8.13 | — | $6,073.65 | ▼ $9,865.99 (-134.01) | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1217.04 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 20 | $59.72 | $2.05 | — | $4,877.20 | ▼ $9,863.94 (-136.06) | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1217.04 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 10 | $115.18 | $2.02 | — | $3,723.38 | ▼ $9,861.92 (-138.08) | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1217.04 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GMAB` | 36 | $33.36 | $2.10 | — | $2,520.33 | ▼ $9,859.83 (-140.17) | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.6; leftover $1217.04 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ENHA` | 711 | $1.71 | $9.17 | — | $1,295.34 | ▼ $9,850.65 (-149.35) | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $1217.04 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAN` | 4139 | $0.29 | $24.59 | — | $53.89 | ▼ $9,826.07 (-173.93) | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $1217.04 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 166 | $8.59 | $2.53 | $+195.85 | $1,477.31 | ▲ $10,192.51 (+192.51) | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 492 | $2.36 | $6.44 | $-66.91 | $2,631.99 | ▲ $10,186.07 (+186.07) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 630 | $1.87 | $8.24 | $-54.17 | $3,801.85 | ▲ $10,177.83 (+177.83) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 10 | $120.87 | $2.04 | $+52.84 | $5,008.51 | ▲ $10,175.79 (+175.79) | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `GMAB` | 36 | $32.82 | $2.12 | $-23.66 | $6,187.91 | ▲ $10,173.67 (+173.67) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ENHA` | 711 | $1.74 | $9.30 | $+2.86 | $7,415.75 | ▲ $10,164.37 (+164.37) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAN` | 4139 | $0.38 | $28.84 | $+302.52 | $8,959.72 | ▲ $10,135.52 (+135.52) | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 20 | $57.00 | $2.07 | $-58.52 | $10,097.65 | ▲ $10,097.65 (+97.65) | dropped from list after 2 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 115 | $10.92 | $2.33 | — | $8,839.52 | ▲ $10,095.32 (+95.32) | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ret5=+10.4; leftover $1262.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 152 | $8.28 | $2.45 | — | $7,578.51 | ▲ $10,092.87 (+92.87) | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1262.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 341 | $3.70 | $4.40 | — | $6,312.41 | ▲ $10,088.47 (+88.47) | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.8; leftover $1262.21 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 185 | $6.79 | $2.54 | — | $5,053.72 | ▲ $10,085.93 (+85.93) | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.4; leftover $1262.21 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3411 | $0.37 | $22.85 | — | $3,768.79 | ▲ $10,063.07 (+63.07) | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer; ret5=-26.5; leftover $1262.21 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 143 | $8.79 | $2.42 | — | $2,509.41 | ▲ $10,060.66 (+60.66) | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.5; leftover $1262.21 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FWDI` | 210 | $5.99 | $2.71 | — | $1,248.80 | ▲ $10,057.95 (+57.95) | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+20.7; leftover $1262.21 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `JANX` | 67 | $18.52 | $2.19 | — | $5.77 | ▲ $10,055.76 (+55.76) | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ret5=+7.9; leftover $1262.21 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 115 | $10.79 | $2.36 | $-19.65 | $1,244.25 | ▲ $10,430.39 (+430.39) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 152 | $8.60 | $2.48 | $+43.71 | $2,548.97 | ▲ $10,427.91 (+427.91) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PUSA` | 341 | $3.84 | $4.47 | $+38.88 | $3,853.94 | ▲ $10,423.44 (+423.44) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 185 | $8.29 | $2.59 | $+272.37 | $5,385.01 | ▲ $10,420.86 (+420.86) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SAFX` | 3411 | $0.35 | $22.75 | $-113.82 | $6,556.11 | ▲ $10,398.11 (+398.11) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SUJA` | 143 | $9.39 | $2.45 | $+80.93 | $7,896.42 | ▲ $10,395.65 (+395.65) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FWDI` | 210 | $5.97 | $2.75 | $-9.66 | $9,147.37 | ▲ $10,392.90 (+392.90) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `JANX` | 67 | $18.59 | $2.21 | $+0.29 | $10,390.69 | ▲ $10,390.69 (+390.69) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 16 | $80.97 | $2.04 | — | $9,093.13 | ▲ $10,388.65 (+388.65) | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; 🔵; ret5=-1.3; leftover $1298.84 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 293 | $4.42 | $3.78 | — | $7,794.29 | ▲ $10,384.87 (+384.87) | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; 🔵; ret5=-8.6; leftover $1298.84 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 17 | $75.12 | $2.04 | — | $6,515.21 | ▲ $10,382.83 (+382.83) | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; 🔵; ret5=-2.2; leftover $1298.84 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $925.74 | $1.99 | — | $5,587.48 | ▲ $10,380.84 (+380.84) | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; 🔵; ret5=-0.5; leftover $1298.84 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `TX` | 23 | $55.20 | $2.06 | — | $4,315.82 | ▲ $10,378.78 (+378.78) | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; 🔵; ret5=+3.0; leftover $1298.84 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 4 | $314.61 | $2.00 | — | $3,055.38 | ▲ $10,376.78 (+376.78) | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; 🔵; ret5=-5.5; leftover $1298.84 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 5 | $240.00 | $2.00 | — | $1,853.37 | ▲ $10,374.77 (+374.77) | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; 🔵; ret5=+6.8; leftover $1298.84 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NUE` | 5 | $248.91 | $2.00 | — | $606.82 | ▲ $10,372.77 (+372.77) | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; 🔵; ret5=-9.4; leftover $1298.84 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 16 | $81.65 | $2.06 | $+6.78 | $1,911.16 | ▲ $10,557.83 (+557.83) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 293 | $4.57 | $3.84 | $+36.33 | $3,246.33 | ▲ $10,553.99 (+553.99) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 17 | $74.54 | $2.06 | $-13.96 | $4,511.45 | ▲ $10,551.93 (+551.93) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $967.01 | $2.01 | $+37.26 | $5,476.44 | ▲ $10,549.91 (+549.91) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TX` | 23 | $55.25 | $2.08 | $-2.99 | $6,745.11 | ▲ $10,547.83 (+547.83) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 4 | $318.88 | $2.02 | $+13.06 | $8,018.61 | ▲ $10,545.81 (+545.81) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MRVL` | 5 | $253.44 | $2.03 | $+63.17 | $9,283.79 | ▲ $10,543.79 (+543.79) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `NUE` | 5 | $252.00 | $2.03 | $+11.42 | $10,541.76 | ▲ $10,541.76 (+541.76) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 143 | $9.19 | $2.42 | — | $9,225.17 | ▲ $10,539.34 (+539.34) | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1317.72 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 39 | $33.78 | $2.11 | — | $7,905.65 | ▲ $10,537.24 (+537.24) | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1317.72 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $149.40 | $2.01 | — | $6,708.43 | ▲ $10,535.22 (+535.22) | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1317.72 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 153 | $8.57 | $2.45 | — | $5,394.77 | ▲ $10,532.77 (+532.77) | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; ret5=-3.4; leftover $1317.72 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 10 | $127.07 | $2.02 | — | $4,122.05 | ▲ $10,530.75 (+530.75) | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=-21.0; leftover $1317.72 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 70 | $18.68 | $2.20 | — | $2,812.25 | ▲ $10,528.55 (+528.55) | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; ret5=+0.2; leftover $1317.72 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BTSG` | 21 | $61.42 | $2.05 | — | $1,520.38 | ▲ $10,526.50 (+526.50) | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; ret5=-4.6; leftover $1317.72 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CRDL` | 630 | $2.09 | $8.13 | — | $195.55 | ▲ $10,518.37 (+518.37) | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; ret5=+3.3; leftover $1317.72 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 143 | $9.44 | $2.45 | $+30.88 | $1,543.02 | ▲ $10,150.06 (+150.06) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 39 | $31.50 | $2.13 | $-93.15 | $2,769.39 | ▲ $10,147.93 (+147.93) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 8 | $133.04 | $2.03 | $-134.93 | $3,831.68 | ▲ $10,145.90 (+145.90) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OPTX` | 153 | $8.52 | $2.48 | $-12.58 | $5,132.75 | ▲ $10,143.41 (+143.41) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TTMI` | 10 | $117.20 | $2.04 | $-102.76 | $6,302.71 | ▲ $10,141.37 (+141.37) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 70 | $19.30 | $2.22 | $+38.98 | $7,651.49 | ▲ $10,139.15 (+139.15) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BTSG` | 21 | $59.66 | $2.07 | $-41.09 | $8,902.28 | ▲ $10,137.08 (+137.08) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRDL` | 630 | $1.96 | $8.24 | $-98.27 | $10,128.84 | ▲ $10,128.84 (+128.84) | dropped from list after 1 sess (min 1) | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 387 | $3.27 | $4.99 | — | $8,858.35 | ▲ $10,123.84 (+123.84) | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1266.10 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 68 | $18.40 | $2.19 | — | $7,604.96 | ▲ $10,121.65 (+121.65) | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1266.10 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CTMX` | 340 | $3.72 | $4.39 | — | $6,335.77 | ▲ $10,117.26 (+117.26) | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $1266.10 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 22 | $56.78 | $2.06 | — | $5,084.56 | ▲ $10,115.21 (+115.21) | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer; ret5=+0.3; leftover $1266.10 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 586 | $2.16 | $7.56 | — | $3,811.24 | ▲ $10,107.65 (+107.65) | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1266.10 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SION` | 190 | $6.63 | $2.56 | — | $2,548.98 | ▲ $10,105.09 (+105.09) | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ret5=-18.1; leftover $1266.10 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DUOL` | 8 | $156.24 | $2.01 | — | $1,297.05 | ▲ $10,103.08 (+103.08) | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ret5=+10.0; leftover $1266.10 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SAFX` | 3246 | $0.39 | $22.40 | — | $8.71 | ▲ $10,080.68 (+80.68) | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; ret5=-26.5; leftover $1266.10 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `FRVO` | 68 | $18.27 | $2.22 | $-13.25 | $1,248.85 | ▲ $10,333.28 (+333.28) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CTMX` | 340 | $3.73 | $4.45 | $-5.44 | $2,512.60 | ▲ $10,328.83 (+328.83) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `EIX` | 22 | $55.42 | $2.08 | $-34.05 | $3,729.76 | ▲ $10,326.75 (+326.75) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 586 | $2.18 | $7.67 | $-3.51 | $4,999.58 | ▲ $10,319.09 (+319.09) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DUOL` | 8 | $161.54 | $2.03 | $+38.35 | $6,289.86 | ▲ $10,317.05 (+317.05) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SAFX` | 3246 | $0.38 | $22.62 | $-77.48 | $7,500.72 | ▲ $10,294.43 (+294.43) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASND` | 4 | $266.94 | $2.00 | — | $6,430.96 | ▲ $10,292.43 (+292.43) | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ret5=+1.9; leftover $1250.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 407 | $3.07 | $5.25 | — | $5,176.22 | ▲ $10,287.18 (+287.18) | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $1250.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 42 | $29.15 | $2.12 | — | $3,949.80 | ▲ $10,285.06 (+285.06) | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1250.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CCOI` | 122 | $10.22 | $2.36 | — | $2,700.61 | ▲ $10,282.71 (+282.71) | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $1250.12 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 268 | $4.66 | $3.46 | — | $1,448.27 | ▲ $10,279.25 (+279.25) | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $1250.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `JLHL` | 201 | $6.20 | $2.60 | — | $199.47 | ▲ $10,276.65 (+276.65) | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; ret5=-8.2; leftover $1250.12 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `TBPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INDP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ENVX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `STUB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `FN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `SBSW` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PAAS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new buys |
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
| `CABA` | 387 | 2026-09-03 @ $3.27 | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1266.10 |
| `SION` | 190 | 2026-09-03 @ $6.63 | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ret5=-18.1; leftover $1266.10 |
| `ASND` | 4 | 2026-09-04 @ $266.94 | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ret5=+1.9; leftover $1250.12 |
| `SLBT` | 407 | 2026-09-04 @ $3.07 | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $1250.12 |
| `MLYS` | 42 | 2026-09-04 @ $29.15 | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1250.12 |
| `CCOI` | 122 | 2026-09-04 @ $10.22 | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $1250.12 |
| `IRD` | 268 | 2026-09-04 @ $4.66 | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $1250.12 |
| `JLHL` | 201 | 2026-09-04 @ $6.20 | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; ret5=-8.2; leftover $1250.12 |
