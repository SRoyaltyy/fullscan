# Factor mine action — `union_coil_off_h5`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ coil_off hold 5, no 🚨

Cash book **+9.72%** ($10,972) · signal-only (no cash/fees) was +7.47%. Starts YES **10/17**. Fills 76 · skips 199 · realized $+698.88.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $101.23.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | TPG, VOR | — | $37.44 | $10,639.59 | $10,677.03 | TPG×98, VOR×227 | BUY TPG x98 @ 50.62; BUY VOR x227 @ 22.01 |
| 2026-08-14 | +5.50 | $37.44 | TPG×98, VOR×227 | LDI, BTBT, ANGX, HYLN | — | $20.51 | $10,441.48 | $10,461.99 | TPG×98, VOR×227, LDI×4, BTBT×3, ANGX×1, HYLN×1 | BUY LDI x4 @ 0.94; BUY BTBT x3 @ 1.50; BUY ANGX x1 @ 4.31; BUY HYLN x1 @ 4.18 |
| 2026-08-17 | +2.25 | $20.51 | TPG×98, VOR×227, LDI×4, BTBT×3, ANGX×1, HYLN×1 | DNN | — | $17.24 | $10,317.02 | $10,334.26 | TPG×98, VOR×227, LDI×4, BTBT×3, ANGX×1, HYLN×1, DNN×1 | BUY DNN x1 @ 3.24 |
| 2026-08-18 | -6.20 | $17.24 | TPG×98, VOR×227, LDI×4, BTBT×3, ANGX×1, HYLN×1, DNN×1 | — | — | $17.24 | $10,402.16 | $10,419.40 | TPG×98, VOR×227, LDI×4, BTBT×3, ANGX×1, HYLN×1, DNN×1 | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $17.24 | TPG×98, VOR×227, LDI×4, BTBT×3, ANGX×1, HYLN×1, DNN×1 | — | — | $17.24 | $10,583.49 | $10,600.73 | TPG×98, VOR×227, LDI×4, BTBT×3, ANGX×1, HYLN×1, DNN×1 | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $17.24 | TPG×98, VOR×227, LDI×4, BTBT×3, ANGX×1, HYLN×1, DNN×1 | AG, BHP, HDSN, IAG, KGC, NFGC, DNA, EXK | TPG, VOR | $33.66 | $10,510.55 | $10,544.20 | LDI×4, BTBT×3, ANGX×1, HYLN×1, DNN×1, AG×63, BHP×14, HDSN×226, IAG×66, KGC×44, NFGC×746, DNA×175, EXK×121 | SELL TPG (dropped from list after 5 sess (min 5)); SELL VOR (dropped from list after 5 sess (min 5)); BUY AG x63 @ 20.55; BUY BHP x14 @ 91.01; BUY HDSN x226 @ 5.77; BUY IAG x66 @ 19.63; BUY KGC x44 @ 29.63; BUY NFGC x746 @ 1.75; BUY DNA x175 @ 7.45; BUY EXK x121 @ 10.77 |
| 2026-08-21 | +3.25 | $33.66 | LDI×4, BTBT×3, ANGX×1, HYLN×1, DNN×1, AG×63, BHP×14, HDSN×226, IAG×66, KGC×44, NFGC×746, DNA×175, EXK×121 | ORBS, EMBC, HITI | LDI, ANGX, HYLN | $28.25 | $10,773.19 | $10,801.44 | BTBT×3, DNN×1, AG×63, BHP×14, HDSN×226, IAG×66, KGC×44, NFGC×746, DNA×175, EXK×121, ORBS×7, EMBC×1, HITI×2 | SELL LDI (dropped from list after 5 sess (min 5)); SELL ANGX (dropped from list after 5 sess (min 5)); SELL HYLN (dropped from list after 5 sess (min 5)); BUY ORBS x7 @ 0.86; BUY EMBC x1 @ 5.43; BUY HITI x2 @ 2.43 |
| 2026-08-24 | -5.17 | $28.25 | BTBT×3, DNN×1, AG×63, BHP×14, HDSN×226, IAG×66, KGC×44, NFGC×746, DNA×175, EXK×121, ORBS×7, EMBC×1, HITI×2 | — | BTBT | $32.83 | $10,704.34 | $10,737.17 | DNN×1, AG×63, BHP×14, HDSN×226, IAG×66, KGC×44, NFGC×746, DNA×175, EXK×121, ORBS×7, EMBC×1, HITI×2 | SELL BTBT (dropped from list after 6 sess (min 5)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $32.83 | DNN×1, AG×63, BHP×14, HDSN×226, IAG×66, KGC×44, NFGC×746, DNA×175, EXK×121, ORBS×7, EMBC×1, HITI×2 | BMEA | DNN | $33.03 | $10,671.59 | $10,704.62 | AG×63, BHP×14, HDSN×226, IAG×66, KGC×44, NFGC×746, DNA×175, EXK×121, ORBS×7, EMBC×1, HITI×2, BMEA×2 | SELL DNN (dropped from list after 6 sess (min 5)); BUY BMEA x2 @ 1.62 |
| 2026-08-26 | +2.02 | $33.03 | AG×63, BHP×14, HDSN×226, IAG×66, KGC×44, NFGC×746, DNA×175, EXK×121, ORBS×7, EMBC×1, HITI×2, BMEA×2 | — | — | $33.03 | $10,702.67 | $10,735.70 | AG×63, BHP×14, HDSN×226, IAG×66, KGC×44, NFGC×746, DNA×175, EXK×121, ORBS×7, EMBC×1, HITI×2, BMEA×2 | hold AG,BHP,HDSN,IAG,KGC,NFGC,DNA,EXK,ORBS,EMBC,HITI,BMEA |
| 2026-08-27 | — | $33.03 | AG×63, BHP×14, HDSN×226, IAG×66, KGC×44, NFGC×746, DNA×175, EXK×121, ORBS×7, EMBC×1, HITI×2, BMEA×2 | RRC, CRK, SLI, TX, DLO, GEN, MRVL, PGY | AG, BHP, HDSN, IAG, KGC, NFGC, DNA, EXK | $214.08 | $10,794.89 | $11,008.97 | ORBS×7, EMBC×1, HITI×2, BMEA×2, RRC×33, CRK×96, SLI×524, TX×24, DLO×87, GEN×47, MRVL×5, PGY×61 | SELL AG (dropped from list after 5 sess (min 5)); SELL BHP (dropped from list after 5 sess (min 5)); SELL HDSN (dropped from list after 5 sess (min 5)); SELL IAG (dropped from list after 5 sess (min 5)); SELL KGC (dropped from list after 5 sess (min 5)); SELL NFGC (dropped from list after 5 sess (min 5)); SELL DNA (dropped from list after 5 sess (min 5)); SELL EXK (dropped from list after 5 sess (min 5)); BUY RRC x33 @ 40.72; BUY CRK x96 @ 14.09; BUY SLI x524 @ 2.59; BUY TX x24 @ 55.20; BUY DLO x87 @ 15.60; BUY GEN x47 @ 28.89; BUY MRVL x5 @ 240.00; BUY PGY x61 @ 21.97 |
| 2026-08-28 | +0.75 | $214.08 | ORBS×7, EMBC×1, HITI×2, BMEA×2, RRC×33, CRK×96, SLI×524, TX×24, DLO×87, GEN×47, MRVL×5, PGY×61 | BZ, BBWI, GENB, ADBT | ORBS, EMBC, HITI | $74.19 | $11,030.82 | $11,105.01 | BMEA×2, RRC×33, CRK×96, SLI×524, TX×24, DLO×87, GEN×47, MRVL×5, PGY×61, BZ×2, BBWI×2, GENB×2, ADBT×9 | SELL ORBS (dropped from list after 5 sess (min 5)); SELL EMBC (dropped from list after 5 sess (min 5)); SELL HITI (dropped from list after 5 sess (min 5)); BUY BZ x2 @ 18.50; BUY BBWI x2 @ 18.68; BUY GENB x2 @ 17.10; BUY ADBT x9 @ 5.02 |
| 2026-08-31 | -5.85 | $74.19 | BMEA×2, RRC×33, CRK×96, SLI×524, TX×24, DLO×87, GEN×47, MRVL×5, PGY×61, BZ×2, BBWI×2, GENB×2, ADBT×9 | — | — | $74.19 | $10,740.01 | $10,814.20 | BMEA×2, RRC×33, CRK×96, SLI×524, TX×24, DLO×87, GEN×47, MRVL×5, PGY×61, BZ×2, BBWI×2, GENB×2, ADBT×9 | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $74.19 | BMEA×2, RRC×33, CRK×96, SLI×524, TX×24, DLO×87, GEN×47, MRVL×5, PGY×61, BZ×2, BBWI×2, GENB×2, ADBT×9 | — | BMEA | $77.43 | $10,707.97 | $10,785.40 | RRC×33, CRK×96, SLI×524, TX×24, DLO×87, GEN×47, MRVL×5, PGY×61, BZ×2, BBWI×2, GENB×2, ADBT×9 | SELL BMEA (dropped from list after 5 sess (min 5)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $77.43 | RRC×33, CRK×96, SLI×524, TX×24, DLO×87, GEN×47, MRVL×5, PGY×61, BZ×2, BBWI×2, GENB×2, ADBT×9 | — | — | $77.43 | $10,745.42 | $10,822.85 | RRC×33, CRK×96, SLI×524, TX×24, DLO×87, GEN×47, MRVL×5, PGY×61, BZ×2, BBWI×2, GENB×2, ADBT×9 | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $77.43 | RRC×33, CRK×96, SLI×524, TX×24, DLO×87, GEN×47, MRVL×5, PGY×61, BZ×2, BBWI×2, GENB×2, ADBT×9 | RVTY, GPRO, MMED, EIX, CLYM, CNXC, BMEA | RRC, SLI, TX, DLO, GEN, MRVL, PGY | $102.72 | $11,286.90 | $11,389.62 | CRK×96, BZ×2, BBWI×2, GENB×2, ADBT×9, RVTY×10, GPRO×1065, MMED×57, EIX×22, CLYM×87, CNXC×40, BMEA×722 | SELL RRC (dropped from list after 5 sess (min 5)); SELL SLI (dropped from list after 5 sess (min 5)); SELL TX (dropped from list after 5 sess (min 5)); SELL DLO (dropped from list after 5 sess (min 5)); SELL GEN (dropped from list after 5 sess (min 5)); SELL MRVL (dropped from list after 5 sess (min 5)); SELL PGY (dropped from list after 5 sess (min 5)); BUY RVTY x10 @ 125.94; BUY GPRO x1065 @ 1.22; BUY MMED x57 @ 22.78; BUY EIX x22 @ 56.78; BUY CLYM x87 @ 14.79; BUY CNXC x40 @ 31.80; BUY BMEA x722 @ 1.80 |
| 2026-09-04 | — | $102.72 | CRK×96, BZ×2, BBWI×2, GENB×2, ADBT×9, RVTY×10, GPRO×1065, MMED×57, EIX×22, CLYM×87, CNXC×40, BMEA×722 | BVS, MLYS, SGLD, FMC, TARS, SCZM, PLAY | CRK, BZ, BBWI, GENB, ADBT | $101.23 | $10,870.88 | $10,972.11 | RVTY×10, GPRO×1065, MMED×57, EIX×22, CLYM×87, CNXC×40, BMEA×722, BVS×16, MLYS×8, SGLD×37, FMC×18, TARS×2, SCZM×23, PLAY×25 | SELL CRK (dropped from list after 6 sess (min 5)); SELL BZ (dropped from list after 5 sess (min 5)); SELL BBWI (dropped from list after 5 sess (min 5)); SELL GENB (dropped from list after 5 sess (min 5)); SELL ADBT (dropped from list after 5 sess (min 5)); BUY BVS x16 @ 14.50; BUY MLYS x8 @ 29.15; BUY SGLD x37 @ 6.48; BUY FMC x18 @ 13.30; BUY TARS x2 @ 82.76; BUY SCZM x23 @ 10.50; BUY PLAY x25 @ 9.36 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 98 | $50.62 | $2.28 | — | $5,036.64 | ▼ $9,997.72 (-2.28) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ⚪; ret5=+6.2; leftover $5000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 227 | $22.01 | $2.93 | — | $37.44 | ▼ $9,994.79 (-5.21) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ⚪; ret5=+0.3; leftover $5000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 4 | $0.94 | $0.05 | — | $33.65 | ▲ $10,751.72 (+751.72) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.5; leftover $4.68 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 3 | $1.50 | $0.05 | — | $29.09 | ▲ $10,751.67 (+751.67) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $4.68 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 1 | $4.31 | $0.05 | — | $24.74 | ▲ $10,751.62 (+751.62) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $4.68 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 1 | $4.18 | $0.04 | — | $20.51 | ▲ $10,751.58 (+751.58) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $4.68 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 1 | $3.24 | $0.04 | — | $17.24 | ▲ $10,399.59 (+399.59) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ⚪; ret5=+0.3; leftover $4.10 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 98 | $53.06 | $2.34 | $+234.18 | $5,214.77 | ▲ $10,466.35 (+466.35) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `VOR` | 227 | $23.05 | $3.01 | $+230.14 | $10,444.12 | ▲ $10,463.35 (+463.35) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 63 | $20.55 | $2.18 | — | $9,147.29 | ▲ $10,461.17 (+461.17) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1305.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $7,871.12 | ▲ $10,459.13 (+459.13) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1305.51 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 226 | $5.77 | $2.92 | — | $6,564.18 | ▲ $10,456.22 (+456.22) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1305.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 66 | $19.63 | $2.19 | — | $5,266.41 | ▲ $10,454.03 (+454.03) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1305.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 44 | $29.63 | $2.12 | — | $3,960.57 | ▲ $10,451.91 (+451.91) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1305.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 746 | $1.75 | $9.62 | — | $2,645.45 | ▲ $10,442.29 (+442.29) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1305.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 175 | $7.45 | $2.52 | — | $1,339.18 | ▲ $10,439.77 (+439.77) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1305.51 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 121 | $10.77 | $2.35 | — | $33.66 | ▲ $10,437.42 (+437.42) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1305.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 4 | $0.87 | $0.07 | $-0.40 | $37.06 | ▲ $10,815.25 (+815.25) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ANGX` | 1 | $4.43 | $0.07 | $+0.01 | $41.42 | ▲ $10,815.18 (+815.18) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `HYLN` | 1 | $3.42 | $0.06 | $-0.86 | $44.79 | ▲ $10,815.13 (+815.13) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 7 | $0.86 | $0.08 | — | $38.66 | ▲ $10,815.04 (+815.04) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $6.40 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `EMBC` | 1 | $5.43 | $0.06 | — | $33.17 | ▲ $10,814.99 (+814.99) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+7.0; leftover $6.40 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HITI` | 2 | $2.43 | $0.05 | — | $28.25 | ▲ $10,814.93 (+814.93) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+5.6; leftover $6.40 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 3 | $1.55 | $0.08 | $+0.02 | $32.83 | ▲ $10,920.53 (+920.53) | dropped from list after 6 sess (min 5) | — |
| 2026-08-25 09:30 ET | **SELL** | `DNN` | 1 | $3.54 | $0.06 | $+0.21 | $36.31 | ▲ $10,735.74 (+735.74) | dropped from list after 6 sess (min 5) | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 2 | $1.62 | $0.04 | — | $33.03 | ▲ $10,735.70 (+735.70) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $4.54 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 63 | $20.63 | $2.20 | $+0.66 | $1,330.52 | ▲ $10,912.67 (+912.67) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 14 | $96.99 | $2.05 | $+79.64 | $2,686.33 | ▲ $10,910.62 (+910.62) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 226 | $5.51 | $2.96 | $-64.64 | $3,928.63 | ▲ $10,907.66 (+907.66) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 66 | $21.64 | $2.21 | $+128.26 | $5,354.66 | ▲ $10,905.45 (+905.45) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 44 | $32.90 | $2.14 | $+139.61 | $6,800.11 | ▲ $10,903.30 (+903.30) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 746 | $2.00 | $9.76 | $+167.12 | $8,282.35 | ▲ $10,893.54 (+893.54) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `DNA` | 175 | $7.33 | $2.55 | $-26.07 | $9,562.55 | ▲ $10,890.99 (+890.99) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `EXK` | 121 | $10.82 | $2.38 | $+1.31 | $10,869.39 | ▲ $10,888.61 (+888.61) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 33 | $40.72 | $2.09 | — | $9,523.54 | ▲ $10,886.52 (+886.52) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+1.8; leftover $1358.67 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 96 | $14.09 | $2.28 | — | $8,168.62 | ▲ $10,884.24 (+884.24) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+1.1; leftover $1358.67 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 524 | $2.59 | $6.76 | — | $6,804.70 | ▲ $10,877.48 (+877.48) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+4.2; leftover $1358.67 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `TX` | 24 | $55.20 | $2.06 | — | $5,477.84 | ▲ $10,875.42 (+875.42) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list mover_buy; 🔵; ret5=+3.0; leftover $1358.67 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 87 | $15.60 | $2.25 | — | $4,118.39 | ▲ $10,873.17 (+873.17) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list mover_buy; 🔵; ret5=+7.1; leftover $1358.67 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 47 | $28.89 | $2.13 | — | $2,758.42 | ▲ $10,871.03 (+871.03) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list mover_buy; 🔵; ret5=+1.6; leftover $1358.67 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 5 | $240.00 | $2.00 | — | $1,556.42 | ▲ $10,869.03 (+869.03) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list mover_buy; 🔵; ret5=+6.8; leftover $1358.67 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PGY` | 61 | $21.97 | $2.17 | — | $214.08 | ▲ $10,866.86 (+866.86) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list mover_buy; 🔵; ret5=+0.6; leftover $1358.67 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `ORBS` | 7 | $0.82 | $0.10 | $-0.49 | $219.72 | ▲ $11,075.03 (+1,075.03) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `EMBC` | 1 | $4.90 | $0.07 | $-0.66 | $224.55 | ▲ $11,074.96 (+1,074.96) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `HITI` | 2 | $2.52 | $0.08 | $+0.05 | $229.51 | ▲ $11,074.88 (+1,074.88) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 2 | $18.50 | $0.38 | — | $192.13 | ▲ $11,074.50 (+1,074.50) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $45.90 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 2 | $18.68 | $0.38 | — | $154.39 | ▲ $11,074.12 (+1,074.12) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+0.2; leftover $45.90 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GENB` | 2 | $17.10 | $0.35 | — | $119.85 | ▲ $11,073.78 (+1,073.78) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; ret5=+3.1; leftover $45.90 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADBT` | 9 | $5.02 | $0.48 | — | $74.19 | ▲ $11,073.30 (+1,073.30) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; ret5=+0.0; leftover $45.90 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `BMEA` | 2 | $1.65 | $0.06 | $-0.04 | $77.43 | ▲ $10,799.58 (+799.58) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 33 | $42.10 | $2.11 | $+41.34 | $1,464.62 | ▲ $10,761.92 (+761.92) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 524 | $2.49 | $6.86 | $-66.02 | $2,762.52 | ▲ $10,755.06 (+755.06) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `TX` | 24 | $56.17 | $2.08 | $+19.14 | $4,108.52 | ▲ $10,752.98 (+752.98) | dropped from list after 5 sess (min 5) | join🔴 sector🟢 gen🟡 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SELL** | `DLO` | 87 | $14.82 | $2.28 | $-72.39 | $5,395.58 | ▲ $10,750.70 (+750.70) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `GEN` | 47 | $30.04 | $2.15 | $+49.77 | $6,805.31 | ▲ $10,748.55 (+748.55) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `MRVL` | 5 | $205.25 | $2.02 | $-177.78 | $7,829.54 | ▲ $10,746.53 (+746.53) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `PGY` | 61 | $20.88 | $2.19 | $-70.86 | $9,101.02 | ▲ $10,744.33 (+744.33) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $125.94 | $2.02 | — | $7,839.60 | ▲ $10,742.31 (+742.31) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1300.15 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1065 | $1.22 | $13.74 | — | $6,526.56 | ▲ $10,728.57 (+728.57) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1300.15 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 57 | $22.78 | $2.16 | — | $5,225.94 | ▲ $10,726.41 (+726.41) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1300.15 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 22 | $56.78 | $2.06 | — | $3,974.73 | ▲ $10,724.36 (+724.36) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; ret5=+0.3; leftover $1300.15 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CLYM` | 87 | $14.79 | $2.25 | — | $2,685.75 | ▲ $10,722.11 (+722.11) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+5.8; leftover $1300.15 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 40 | $31.80 | $2.11 | — | $1,411.64 | ▲ $10,720.00 (+720.00) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+3.7; leftover $1300.15 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 722 | $1.80 | $9.31 | — | $102.72 | ▲ $10,710.68 (+710.68) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1300.15 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 96 | $15.45 | $2.31 | $+125.98 | $1,583.62 | ▲ $11,425.68 (+1,425.68) | dropped from list after 6 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BZ` | 2 | $17.31 | $0.37 | $-3.13 | $1,617.86 | ▲ $11,425.30 (+1,425.30) | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BBWI` | 2 | $18.59 | $0.40 | $-0.96 | $1,654.65 | ▲ $11,424.91 (+1,424.91) | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `GENB` | 2 | $16.40 | $0.35 | $-2.10 | $1,687.09 | ▲ $11,424.55 (+1,424.55) | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `ADBT` | 9 | $1.52 | $0.18 | $-32.16 | $1,700.59 | ▲ $11,424.37 (+1,424.37) | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 16 | $14.50 | $2.04 | — | $1,466.55 | ▲ $11,422.33 (+1,422.33) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.8; leftover $242.94 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 8 | $29.15 | $2.01 | — | $1,231.34 | ▲ $11,420.32 (+1,420.32) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $242.94 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SGLD` | 37 | $6.48 | $2.10 | — | $989.48 | ▲ $11,418.22 (+1,418.22) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer,yday_mover; ret5=+0.0; leftover $242.94 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `FMC` | 18 | $13.30 | $2.04 | — | $748.03 | ▲ $11,416.17 (+1,416.17) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer,yday_mover; ret5=+8.6; leftover $242.94 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 2 | $82.76 | $1.66 | — | $580.85 | ▲ $11,414.51 (+1,414.51) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+5.1; leftover $242.94 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SCZM` | 23 | $10.50 | $2.06 | — | $337.29 | ▲ $11,412.45 (+1,412.45) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+9.3; leftover $242.94 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `PLAY` | 25 | $9.36 | $2.06 | — | $101.23 | ▲ $11,410.39 (+1,410.39) | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+0.6; leftover $242.94 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TLN` | cash | leftover split 4.68 < 1 share @ 359.83 |
| 2026-08-14 | `SLG` | cash | leftover split 4.68 < 1 share @ 57.61 |
| 2026-08-14 | `WDC` | cash | leftover split 4.68 < 1 share @ 503.50 |
| 2026-08-14 | `ADUR` | cash | leftover split 4.68 < 1 share @ 16.50 |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 4.10 < 1 share @ 46.18 |
| 2026-08-17 | `OCC` | cash | leftover split 4.10 < 1 share @ 18.24 |
| 2026-08-17 | `ALM` | cash | leftover split 4.10 < 1 share @ 16.20 |
| 2026-08-17 | `NEWP` | cash | leftover split 4.10 < 1 share @ 6.94 |
| 2026-08-18 | `TPG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `VOR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `TPG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `VOR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `LDI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ANGX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `HYLN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AEHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MXL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `PAYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | `LDI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `BTBT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ANGX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `HYLN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `DNN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-21 | `DNN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `DNA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `EXK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `CRSP` | cash | leftover split 6.40 < 1 share @ 59.72 |
| 2026-08-21 | `TXG` | cash | leftover split 6.40 < 1 share @ 64.39 |
| 2026-08-21 | `DXYZ` | cash | leftover split 6.40 < 1 share @ 34.89 |
| 2026-08-21 | `BEKE` | cash | leftover split 6.40 < 1 share @ 17.93 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `DNA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `EXK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `ORBS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `EMBC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `HITI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABAT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `BHP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `HDSN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `IAG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `KGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `NFGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `DNA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `EXK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `ORBS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `EMBC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `HITI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CRMD` | cash | leftover split 4.54 < 1 share @ 8.28 |
| 2026-08-25 | `HCA` | cash | leftover split 4.54 < 1 share @ 429.24 |
| 2026-08-25 | `ALIT` | cash | leftover split 4.54 < 1 share @ 14.86 |
| 2026-08-25 | `ZURA` | cash | leftover split 4.54 < 1 share @ 6.38 |
| 2026-08-25 | `JANX` | cash | leftover split 4.54 < 1 share @ 18.52 |
| 2026-08-25 | `KURA` | cash | leftover split 4.54 < 1 share @ 13.30 |
| 2026-08-25 | `EZPW` | cash | leftover split 4.54 < 1 share @ 34.48 |
| 2026-08-26 | `AG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `BHP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `HDSN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `IAG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `KGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `NFGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `DNA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `EXK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `ORBS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `EMBC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `HITI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `HCA` | no_price | no 09:30 open |
| 2026-08-26 | `CRMD` | no_price | no 09:30 open |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `BZ` | no_price | no 09:30 open |
| 2026-08-26 | `CNTN` | no_price | no 09:30 open |
| 2026-08-26 | `OSUR` | no_price | no 09:30 open |
| 2026-08-26 | `INO` | no_price | no 09:30 open |
| 2026-08-27 | `ORBS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `EMBC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `HITI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `BMEA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `TX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `DLO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `GEN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `MRVL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `PGY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `ANF` | cash | leftover split 45.90 < 1 share @ 144.70 |
| 2026-08-31 | `BMEA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `TX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `GEN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `MRVL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `PGY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `BBWI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `GENB` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `ADBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `INO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DINO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `SLI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `TX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `DLO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `GEN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `MRVL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `PGY` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `BBWI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `GENB` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `ADBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRGP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLDB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OHI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `XLAB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `RRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `CRK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `SLI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `TX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `DLO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `GEN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `MRVL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `PGY` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `BZ` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `BBWI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `GENB` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `ADBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HELP` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SCZM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | `BZ` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `BBWI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `GENB` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `ADBT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `EIX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CLYM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CNXC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `BMEA` | min_hold | dropped but min-hold 1/5 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `RVTY` | 10 | 2026-09-03 @ $125.94 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1300.15 |
| `GPRO` | 1065 | 2026-09-03 @ $1.22 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1300.15 |
| `MMED` | 57 | 2026-09-03 @ $22.78 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1300.15 |
| `EIX` | 22 | 2026-09-03 @ $56.78 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; ret5=+0.3; leftover $1300.15 |
| `CLYM` | 87 | 2026-09-03 @ $14.79 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+5.8; leftover $1300.15 |
| `CNXC` | 40 | 2026-09-03 @ $31.80 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+3.7; leftover $1300.15 |
| `BMEA` | 722 | 2026-09-03 @ $1.80 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1300.15 |
| `BVS` | 16 | 2026-09-04 @ $14.50 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.8; leftover $242.94 |
| `MLYS` | 8 | 2026-09-04 @ $29.15 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $242.94 |
| `SGLD` | 37 | 2026-09-04 @ $6.48 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer,yday_mover; ret5=+0.0; leftover $242.94 |
| `FMC` | 18 | 2026-09-04 @ $13.30 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer,yday_mover; ret5=+8.6; leftover $242.94 |
| `TARS` | 2 | 2026-09-04 @ $82.76 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+5.1; leftover $242.94 |
| `SCZM` | 23 | 2026-09-04 @ $10.50 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+9.3; leftover $242.94 |
| `PLAY` | 25 | 2026-09-04 @ $9.36 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+0.6; leftover $242.94 |
