# Factor mine action — `coil_h3_exit_alarm`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · coil, exit on 🚨

Cash book **+4.05%** ($10,405) · signal-only (no cash/fees) was +8.25%. Starts YES **7/17**. Fills 76 · skips 152 · realized $+165.45.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $49.62.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | TPG, VOR | — | $37.44 | $10,639.59 | $10,677.03 | TPG×98, VOR×227 | BUY TPG x98 @ 50.62; BUY VOR x227 @ 22.01 |
| 2026-08-14 | +5.50 | $37.44 | TPG×98, VOR×227 | LDI, BTBT, ANGX, HYLN | — | $20.51 | $10,441.48 | $10,461.99 | TPG×98, VOR×227, LDI×4, BTBT×3, ANGX×1, HYLN×1 | BUY LDI x4 @ 0.94; BUY BTBT x3 @ 1.50; BUY ANGX x1 @ 4.31; BUY HYLN x1 @ 4.18 |
| 2026-08-17 | +2.25 | $20.51 | TPG×98, VOR×227, LDI×4, BTBT×3, ANGX×1, HYLN×1 | — | — | $20.51 | $10,313.83 | $10,334.34 | TPG×98, VOR×227, LDI×4, BTBT×3, ANGX×1, HYLN×1 | hold TPG,VOR,LDI,BTBT,ANGX,HYLN |
| 2026-08-18 | -6.20 | $20.51 | TPG×98, VOR×227, LDI×4, BTBT×3, ANGX×1, HYLN×1 | — | TPG, VOR | $10,268.76 | $16.49 | $10,285.26 | LDI×4, BTBT×3, ANGX×1, HYLN×1 | SELL TPG (dropped from list after 3 sess (min 3)); SELL VOR (dropped from list after 3 sess (min 3)); hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $10,268.76 | LDI×4, BTBT×3, ANGX×1, HYLN×1 | — | LDI, BTBT, ANGX, HYLN | $10,284.93 | $0.00 | $10,284.93 | — | SELL LDI (dropped from list after 3 sess (min 3)); SELL BTBT (dropped from list after 3 sess (min 3)); SELL ANGX (dropped from list after 3 sess (min 3)); SELL HYLN (dropped from list after 3 sess (min 3)); hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $10,284.93 | — | AG, BHP, HDSN, IAG, KGC, NFGC, DNA, EXK | — | $32.48 | $10,332.18 | $10,364.66 | AG×62, BHP×14, HDSN×222, IAG×65, KGC×43, NFGC×734, DNA×172, EXK×119 | BUY AG x62 @ 20.55; BUY BHP x14 @ 91.01; BUY HDSN x222 @ 5.77; BUY IAG x65 @ 19.63; BUY KGC x43 @ 29.63; BUY NFGC x734 @ 1.75; BUY DNA x172 @ 7.45; BUY EXK x119 @ 10.77 |
| 2026-08-21 | +3.25 | $32.48 | AG×62, BHP×14, HDSN×222, IAG×65, KGC×43, NFGC×734, DNA×172, EXK×119 | BTBT, ORBS, QTRX | — | $22.48 | $10,595.35 | $10,617.83 | AG×62, BHP×14, HDSN×222, IAG×65, KGC×43, NFGC×734, DNA×172, EXK×119, BTBT×2, ORBS×4, QTRX×1 | BUY BTBT x2 @ 1.66; BUY ORBS x4 @ 0.86; BUY QTRX x1 @ 3.11 |
| 2026-08-24 | -5.17 | $22.48 | AG×62, BHP×14, HDSN×222, IAG×65, KGC×43, NFGC×734, DNA×172, EXK×119, BTBT×2, ORBS×4, QTRX×1 | — | — | $22.48 | $10,532.23 | $10,554.71 | AG×62, BHP×14, HDSN×222, IAG×65, KGC×43, NFGC×734, DNA×172, EXK×119, BTBT×2, ORBS×4, QTRX×1 | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $22.48 | AG×62, BHP×14, HDSN×222, IAG×65, KGC×43, NFGC×734, DNA×172, EXK×119, BTBT×2, ORBS×4, QTRX×1 | INSP, CRMD, HCA, BMEA, ALIT, ZURA, JANX, KURA | AG, BHP, HDSN, IAG, KGC, NFGC, DNA, EXK | $69.17 | $10,507.24 | $10,576.41 | BTBT×2, ORBS×4, QTRX×1, INSP×21, CRMD×158, HCA×3, BMEA×811, ALIT×88, ZURA×206, JANX×70, KURA×98 | SELL AG (dropped from list after 3 sess (min 3)); SELL BHP (dropped from list after 3 sess (min 3)); SELL HDSN (dropped from list after 3 sess (min 3)); SELL IAG (dropped from list after 3 sess (min 3)); SELL KGC (dropped from list after 3 sess (min 3)); SELL NFGC (dropped from list after 3 sess (min 3)); SELL DNA (dropped from list after 3 sess (min 3)); SELL EXK (dropped from list after 3 sess (min 3)); BUY INSP x21 @ 61.47; BUY CRMD x158 @ 8.28; BUY HCA x3 @ 429.24; BUY BMEA x811 @ 1.62; BUY ALIT x88 @ 14.86; BUY ZURA x206 @ 6.38; BUY JANX x70 @ 18.52; BUY KURA x98 @ 13.30 |
| 2026-08-26 | +2.02 | $69.17 | BTBT×2, ORBS×4, QTRX×1, INSP×21, CRMD×158, HCA×3, BMEA×811, ALIT×88, ZURA×206, JANX×70, KURA×98 | — | — | $69.17 | $10,431.71 | $10,500.88 | BTBT×2, ORBS×4, QTRX×1, INSP×21, CRMD×158, HCA×3, BMEA×811, ALIT×88, ZURA×206, JANX×70, KURA×98 | hold BTBT,ORBS,QTRX,INSP,CRMD,HCA,BMEA,ALIT,ZURA,JANX,KURA |
| 2026-08-27 | — | $69.17 | BTBT×2, ORBS×4, QTRX×1, INSP×21, CRMD×158, HCA×3, BMEA×811, ALIT×88, ZURA×206, JANX×70, KURA×98 | SLI | BTBT, ORBS, QTRX | $70.24 | $10,396.70 | $10,466.94 | INSP×21, CRMD×158, HCA×3, BMEA×811, ALIT×88, ZURA×206, JANX×70, KURA×98, SLI×3 | SELL BTBT (dropped from list after 4 sess (min 3)); SELL ORBS (dropped from list after 4 sess (min 3)); SELL QTRX (dropped from list after 4 sess (min 3)); BUY SLI x3 @ 2.59 |
| 2026-08-28 | +0.75 | $70.24 | INSP×21, CRMD×158, HCA×3, BMEA×811, ALIT×88, ZURA×206, JANX×70, KURA×98, SLI×3 | RRC, CRK, ANF, BZ, LVWR, BBWI, CRDL | INSP, CRMD, HCA, BMEA, ALIT, ZURA, JANX, KURA | $47.34 | $10,374.30 | $10,421.64 | SLI×3, RRC×36, CRK×103, ANF×10, BZ×81, LVWR×1086, BBWI×80, CRDL×717 | SELL INSP (dropped from list after 3 sess (min 3)); SELL CRMD (dropped from list after 3 sess (min 3)); SELL HCA (dropped from list after 3 sess (min 3)); SELL BMEA (dropped from list after 3 sess (min 3)); SELL ALIT (dropped from list after 3 sess (min 3)); SELL ZURA (dropped from list after 3 sess (min 3)); SELL JANX (dropped from list after 3 sess (min 3)); SELL KURA (dropped from list after 3 sess (min 3)); BUY RRC x36 @ 41.44; BUY CRK x103 @ 14.42; BUY ANF x10 @ 144.70; BUY BZ x81 @ 18.50; BUY LVWR x1086 @ 1.38; BUY BBWI x80 @ 18.68; BUY CRDL x717 @ 2.09 |
| 2026-08-31 | -5.85 | $47.34 | SLI×3, RRC×36, CRK×103, ANF×10, BZ×81, LVWR×1086, BBWI×80, CRDL×717 | — | — | $47.34 | $10,347.00 | $10,394.34 | SLI×3, RRC×36, CRK×103, ANF×10, BZ×81, LVWR×1086, BBWI×80, CRDL×717 | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $47.34 | SLI×3, RRC×36, CRK×103, ANF×10, BZ×81, LVWR×1086, BBWI×80, CRDL×717 | — | SLI | $55.33 | $10,072.13 | $10,127.46 | RRC×36, CRK×103, ANF×10, BZ×81, LVWR×1086, BBWI×80, CRDL×717 | SELL SLI (dropped from list after 3 sess (min 3)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $55.33 | RRC×36, CRK×103, ANF×10, BZ×81, LVWR×1086, BBWI×80, CRDL×717 | — | RRC, CRK, ANF, BZ, LVWR, BBWI, CRDL | $10,165.46 | $0.00 | $10,165.46 | — | SELL RRC (dropped from list after 3 sess (min 3)); SELL CRK (dropped from list after 3 sess (min 3)); SELL ANF (dropped from list after 3 sess (min 3)); SELL BZ (dropped from list after 3 sess (min 3)); SELL LVWR (dropped from list after 3 sess (min 3)); SELL BBWI (dropped from list after 3 sess (min 3)); SELL CRDL (dropped from list after 3 sess (min 3)); hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $10,165.46 | — | HRMY, VSTM, RVTY, GPRO, CRK, MMED, EIX, CRDL | — | $64.06 | $10,717.93 | $10,781.99 | HRMY×30, VSTM×165, RVTY×10, GPRO×1041, CRK×80, MMED×55, EIX×22, CRDL×588 | BUY HRMY x30 @ 41.31; BUY VSTM x165 @ 7.70; BUY RVTY x10 @ 125.94; BUY GPRO x1041 @ 1.22; BUY CRK x80 @ 15.70; BUY MMED x55 @ 22.78; BUY EIX x22 @ 56.78; BUY CRDL x588 @ 2.16 |
| 2026-09-04 | — | $64.06 | HRMY×30, VSTM×165, RVTY×10, GPRO×1041, CRK×80, MMED×55, EIX×22, CRDL×588 | BAK, SGLD | — | $49.62 | $10,355.16 | $10,404.78 | HRMY×30, VSTM×165, RVTY×10, GPRO×1041, CRK×80, MMED×55, EIX×22, CRDL×588, BAK×4, SGLD×1 | BUY BAK x4 @ 1.95; BUY SGLD x1 @ 6.48 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 98 | $50.62 | $2.28 | — | $5,036.64 | ▼ $9,997.72 (-2.28) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=+6.2; leftover $5000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 227 | $22.01 | $2.93 | — | $37.44 | ▼ $9,994.79 (-5.21) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=+0.3; leftover $5000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 4 | $0.94 | $0.05 | — | $33.65 | ▲ $10,751.72 (+751.72) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.5; leftover $4.68 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 3 | $1.50 | $0.05 | — | $29.09 | ▲ $10,751.67 (+751.67) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $4.68 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 1 | $4.31 | $0.05 | — | $24.74 | ▲ $10,751.62 (+751.62) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $4.68 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 1 | $4.18 | $0.04 | — | $20.51 | ▲ $10,751.58 (+751.58) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $4.68 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 98 | $51.77 | $2.34 | $+107.76 | $5,091.63 | ▲ $10,288.61 (+288.61) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 227 | $22.82 | $3.01 | $+177.93 | $10,268.76 | ▲ $10,285.60 (+285.60) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 4 | $0.88 | $0.07 | $-0.34 | $10,272.22 | ▲ $10,285.14 (+285.14) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 3 | $1.42 | $0.07 | $-0.37 | $10,276.40 | ▲ $10,285.06 (+285.06) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 1 | $4.79 | $0.07 | $+0.36 | $10,281.12 | ▲ $10,284.99 (+284.99) | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🟢 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `HYLN` | 1 | $3.87 | $0.06 | $-0.42 | $10,284.93 | ▲ $10,284.93 (+284.93) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 62 | $20.55 | $2.18 | — | $9,008.66 | ▲ $10,282.76 (+282.76) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1285.62 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $7,732.48 | ▲ $10,280.72 (+280.72) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1285.62 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 222 | $5.77 | $2.86 | — | $6,448.68 | ▲ $10,277.86 (+277.86) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1285.62 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 65 | $19.63 | $2.19 | — | $5,170.55 | ▲ $10,275.68 (+275.68) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1285.62 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 43 | $29.63 | $2.12 | — | $3,894.34 | ▲ $10,273.56 (+273.56) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1285.62 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 734 | $1.75 | $9.47 | — | $2,600.37 | ▲ $10,264.09 (+264.09) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1285.62 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 172 | $7.45 | $2.51 | — | $1,316.46 | ▲ $10,261.58 (+261.58) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1285.62 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 119 | $10.77 | $2.35 | — | $32.48 | ▲ $10,259.23 (+259.23) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1285.62 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 2 | $1.66 | $0.04 | — | $29.13 | ▲ $10,631.23 (+631.23) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $4.06 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 4 | $0.86 | $0.05 | — | $25.62 | ▲ $10,631.18 (+631.18) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $4.06 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `QTRX` | 1 | $3.11 | $0.03 | — | $22.48 | ▲ $10,631.14 (+631.14) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+9.1; leftover $4.06 | join🟡 sector🟢 gen🟢 news🔴 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 62 | $20.73 | $2.20 | $+6.79 | $1,305.54 | ▲ $10,551.09 (+551.09) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 14 | $95.95 | $2.05 | $+65.08 | $2,646.79 | ▲ $10,549.04 (+549.04) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 222 | $5.53 | $2.91 | $-59.05 | $3,871.54 | ▲ $10,546.13 (+546.13) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 65 | $21.63 | $2.21 | $+125.61 | $5,275.28 | ▲ $10,543.92 (+543.92) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 43 | $32.76 | $2.14 | $+130.33 | $6,681.82 | ▲ $10,541.78 (+541.78) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 734 | $1.91 | $9.60 | $+98.37 | $8,074.16 | ▲ $10,532.18 (+532.18) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DNA` | 172 | $6.82 | $2.54 | $-113.41 | $9,244.66 | ▲ $10,529.64 (+529.64) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `EXK` | 119 | $10.72 | $2.38 | $-10.67 | $10,517.96 | ▲ $10,527.26 (+527.26) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 21 | $61.47 | $2.05 | — | $9,225.04 | ▲ $10,525.21 (+525.21) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ret5=+9.2; leftover $1314.74 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 158 | $8.28 | $2.46 | — | $7,914.33 | ▲ $10,522.74 (+522.74) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1314.74 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $429.24 | $2.00 | — | $6,624.61 | ▲ $10,520.74 (+520.74) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; ret5=+6.1; leftover $1314.74 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 811 | $1.62 | $10.46 | — | $5,300.33 | ▲ $10,510.28 (+510.28) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1314.74 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALIT` | 88 | $14.86 | $2.25 | — | $3,990.40 | ▲ $10,508.03 (+508.03) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+3.0; leftover $1314.74 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 206 | $6.38 | $2.66 | — | $2,673.46 | ▲ $10,505.37 (+505.37) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1314.74 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `JANX` | 70 | $18.52 | $2.20 | — | $1,374.86 | ▲ $10,503.17 (+503.17) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ret5=+7.9; leftover $1314.74 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 98 | $13.30 | $2.28 | — | $69.17 | ▲ $10,500.88 (+500.88) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ret5=+9.5; leftover $1314.74 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `BTBT` | 2 | $1.53 | $0.06 | $-0.36 | $72.18 | ▲ $10,606.85 (+606.85) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ORBS` | 4 | $0.80 | $0.06 | $-0.37 | $75.31 | ▲ $10,606.78 (+606.78) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `QTRX` | 1 | $2.83 | $0.05 | $-0.37 | $78.09 | ▲ $10,606.73 (+606.73) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 3 | $2.59 | $0.09 | — | $70.24 | ▲ $10,606.65 (+606.65) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; ret5=+4.2; leftover $9.76 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 21 | $62.10 | $2.07 | $+9.10 | $1,372.26 | ▲ $10,528.13 (+528.13) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 158 | $8.49 | $2.50 | $+28.22 | $2,711.18 | ▲ $10,525.63 (+525.63) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 3 | $424.61 | $2.02 | $-17.91 | $3,982.99 | ▲ $10,523.61 (+523.61) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 811 | $1.74 | $10.61 | $+76.25 | $5,383.52 | ▲ $10,513.00 (+513.00) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALIT` | 88 | $14.54 | $2.28 | $-32.69 | $6,660.77 | ▲ $10,510.73 (+510.73) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZURA` | 206 | $6.02 | $2.70 | $-79.52 | $7,898.18 | ▲ $10,508.02 (+508.02) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `JANX` | 70 | $19.00 | $2.22 | $+29.18 | $9,225.96 | ▲ $10,505.80 (+505.80) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 98 | $12.98 | $2.31 | $-35.95 | $10,495.69 | ▲ $10,503.49 (+503.49) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 36 | $41.44 | $2.10 | — | $9,001.75 | ▲ $10,501.39 (+501.39) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; ret5=+1.8; leftover $1499.38 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CRK` | 103 | $14.42 | $2.30 | — | $7,514.19 | ▲ $10,499.09 (+499.09) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; ret5=+1.1; leftover $1499.38 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 10 | $144.70 | $2.02 | — | $6,065.17 | ▲ $10,497.07 (+497.07) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1499.38 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 81 | $18.50 | $2.23 | — | $4,564.44 | ▲ $10,494.84 (+494.84) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1499.38 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 1086 | $1.38 | $14.01 | — | $3,051.75 | ▲ $10,480.83 (+480.83) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+0.0; leftover $1499.38 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 80 | $18.68 | $2.23 | — | $1,555.12 | ▲ $10,478.60 (+478.60) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; ret5=+0.2; leftover $1499.38 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CRDL` | 717 | $2.09 | $9.25 | — | $47.34 | ▲ $10,469.35 (+469.35) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; ret5=+3.3; leftover $1499.38 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 3 | $2.70 | $0.11 | $+0.13 | $55.33 | ▲ $10,121.03 (+121.03) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `RRC` | 36 | $41.94 | $2.12 | $+13.78 | $1,563.05 | ▲ $10,197.92 (+197.92) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CRK` | 103 | $15.82 | $2.33 | $+139.57 | $3,190.18 | ▲ $10,195.59 (+195.59) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 10 | $142.00 | $2.04 | $-31.06 | $4,608.14 | ▲ $10,193.55 (+193.55) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BZ` | 81 | $17.29 | $2.26 | $-102.50 | $6,006.37 | ▲ $10,191.29 (+191.29) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `LVWR` | 1086 | $1.19 | $14.20 | $-234.55 | $7,284.51 | ▲ $10,177.09 (+177.09) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBWI` | 80 | $18.77 | $2.26 | $+2.71 | $8,783.86 | ▲ $10,174.84 (+174.84) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CRDL` | 717 | $1.94 | $9.38 | $-126.18 | $10,165.46 | ▲ $10,165.46 (+165.46) | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 30 | $41.31 | $2.08 | — | $8,924.08 | ▲ $10,163.38 (+163.38) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1270.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 165 | $7.70 | $2.48 | — | $7,651.10 | ▲ $10,160.90 (+160.90) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1270.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $125.94 | $2.02 | — | $6,389.68 | ▲ $10,158.88 (+158.88) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1270.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1041 | $1.22 | $13.43 | — | $5,106.23 | ▲ $10,145.45 (+145.45) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1270.68 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 80 | $15.70 | $2.23 | — | $3,848.00 | ▲ $10,143.22 (+143.22) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1270.68 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 55 | $22.78 | $2.15 | — | $2,592.94 | ▲ $10,141.06 (+141.06) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1270.68 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 22 | $56.78 | $2.06 | — | $1,341.73 | ▲ $10,139.01 (+139.01) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; ret5=+0.3; leftover $1270.68 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 588 | $2.16 | $7.59 | — | $64.06 | ▲ $10,131.42 (+131.42) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1270.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 4 | $1.95 | $0.09 | — | $56.17 | ▲ $10,904.78 (+904.78) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $9.15 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SGLD` | 1 | $6.48 | $0.07 | — | $49.62 | ▲ $10,904.71 (+904.71) | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list yday_gainer,yday_mover; ret5=+0.0; leftover $9.15 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TLN` | cash | leftover split 4.68 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 4.68 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 4.68 < 1 share @ 120.00 |
| 2026-08-14 | `SLG` | cash | leftover split 4.68 < 1 share @ 57.61 |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 2.56 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 2.56 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 2.56 < 1 share @ 202.70 |
| 2026-08-17 | `TGB` | cash | leftover split 2.56 < 1 share @ 8.46 |
| 2026-08-17 | `DNN` | cash | leftover split 2.56 < 1 share @ 3.24 |
| 2026-08-17 | `OCC` | cash | leftover split 2.56 < 1 share @ 18.24 |
| 2026-08-17 | `ALM` | cash | leftover split 2.56 < 1 share @ 16.20 |
| 2026-08-17 | `NEWP` | cash | leftover split 2.56 < 1 share @ 6.94 |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TBPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AEHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MXL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `EXK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CRSP` | cash | leftover split 4.06 < 1 share @ 59.72 |
| 2026-08-21 | `EMBC` | cash | leftover split 4.06 < 1 share @ 5.43 |
| 2026-08-21 | `TXG` | cash | leftover split 4.06 < 1 share @ 64.39 |
| 2026-08-21 | `DXYZ` | cash | leftover split 4.06 < 1 share @ 34.89 |
| 2026-08-21 | `BEKE` | cash | leftover split 4.06 < 1 share @ 17.93 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `DNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `EXK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `QTRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABAT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `QTRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `BTBT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ORBS` | no_price | no 09:30 open — carry |
| 2026-08-26 | `QTRX` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALIT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `JANX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `BZ` | no_price | no 09:30 open |
| 2026-08-26 | `CNTN` | no_price | no 09:30 open |
| 2026-08-26 | `OSUR` | no_price | no 09:30 open |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALIT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `JANX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `KURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RRC` | cash | leftover split 9.76 < 1 share @ 40.72 |
| 2026-08-27 | `CRK` | cash | leftover split 9.76 < 1 share @ 14.09 |
| 2026-08-27 | `TX` | cash | leftover split 9.76 < 1 share @ 55.20 |
| 2026-08-27 | `DLO` | cash | leftover split 9.76 < 1 share @ 15.60 |
| 2026-08-27 | `GEN` | cash | leftover split 9.76 < 1 share @ 28.89 |
| 2026-08-27 | `MRVL` | cash | leftover split 9.76 < 1 share @ 240.00 |
| 2026-08-27 | `PGY` | cash | leftover split 9.76 < 1 share @ 21.97 |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `LVWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BBWI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SRPT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WEN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `LVWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BBWI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRGP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLDB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NAGE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OHI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `EIX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ASND` | cash | leftover split 9.15 < 1 share @ 266.94 |
| 2026-09-04 | `BVS` | cash | leftover split 9.15 < 1 share @ 14.50 |
| 2026-09-04 | `MLYS` | cash | leftover split 9.15 < 1 share @ 29.15 |
| 2026-09-04 | `FMC` | cash | leftover split 9.15 < 1 share @ 13.30 |
| 2026-09-04 | `TARS` | cash | leftover split 9.15 < 1 share @ 82.76 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `HRMY` | 30 | 2026-09-03 @ $41.31 | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1270.68 |
| `VSTM` | 165 | 2026-09-03 @ $7.70 | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1270.68 |
| `RVTY` | 10 | 2026-09-03 @ $125.94 | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1270.68 |
| `GPRO` | 1041 | 2026-09-03 @ $1.22 | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1270.68 |
| `CRK` | 80 | 2026-09-03 @ $15.70 | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1270.68 |
| `MMED` | 55 | 2026-09-03 @ $22.78 | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1270.68 |
| `EIX` | 22 | 2026-09-03 @ $56.78 | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; ret5=+0.3; leftover $1270.68 |
| `CRDL` | 588 | 2026-09-03 @ $2.16 | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1270.68 |
| `BAK` | 4 | 2026-09-04 @ $1.95 | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $9.15 |
| `SGLD` | 1 | 2026-09-04 @ $6.48 | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list yday_gainer,yday_mover; ret5=+0.0; leftover $9.15 |
