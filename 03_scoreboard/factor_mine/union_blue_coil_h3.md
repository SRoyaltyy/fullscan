# Factor mine action — `union_blue_coil_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-2.31%** ($9,769) · signal-only (no cash/fees) was +3.07%. Starts YES **8/17**. Fills 90 · skips 146 · realized $-273.93.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `blue=True,ret_5_max=10.0` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $59.28.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-14 | +5.50 | $10,000.00 | — | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | — | $560.20 | $9,491.27 | $10,051.46 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833 | BUY TLN x3 @ 359.83; BUY VST x8 @ 146.90; BUY NRG x10 @ 120.00; BUY DAVE x3 @ 330.91; BUY SLG x21 @ 57.61; BUY MARA x138 @ 9.01; BUY LDI x1334 @ 0.94; BUY BTBT x833 @ 1.50 |
| 2026-08-17 | +2.25 | $560.20 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833 | DVN, TMC, TGB, ABX, ALM, INV | — | $175.14 | $9,878.01 | $10,053.14 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833, DVN×1, TMC×17, TGB×8, ABX×7, ALM×4, INV×43 | BUY DVN x1 @ 46.18; BUY TMC x17 @ 4.05; BUY TGB x8 @ 8.46; BUY ABX x7 @ 9.12; BUY ALM x4 @ 16.20; BUY INV x43 @ 1.62 |
| 2026-08-18 | -6.20 | $175.14 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833, DVN×1, TMC×17, TGB×8, ABX×7, ALM×4, INV×43 | — | — | $175.14 | $9,378.50 | $9,553.63 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833, DVN×1, TMC×17, TGB×8, ABX×7, ALM×4, INV×43 | hard-red sit S=-6.20 |
| 2026-08-19 | -7.20 | $175.14 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833, DVN×1, TMC×17, TGB×8, ABX×7, ALM×4, INV×43 | — | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | $9,182.43 | $378.43 | $9,560.86 | DVN×1, TMC×17, TGB×8, ABX×7, ALM×4, INV×43 | SELL TLN (dropped from list after 3 sess (min 3)); SELL VST (dropped from list after 3 sess (min 3)); SELL NRG (dropped from list after 3 sess (min 3)); SELL DAVE (dropped from list after 3 sess (min 3)); SELL SLG (dropped from list after 3 sess (min 3)); SELL MARA (dropped from list after 3 sess (min 3)); SELL LDI (dropped from list after 3 sess (min 3)); SELL BTBT (dropped from list after 3 sess (min 3)); hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $9,182.43 | DVN×1, TMC×17, TGB×8, ABX×7, ALM×4, INV×43 | AG, BHP, HDSN, IAG, KGC, NFGC, WPM, ABUS | DVN, TMC, TGB, ABX, ALM, INV | $62.39 | $9,630.67 | $9,693.06 | AG×58, BHP×13, HDSN×206, IAG×60, KGC×40, NFGC×682, WPM×8, ABUS×242 | SELL DVN (dropped from list after 3 sess (min 3)); SELL TMC (dropped from list after 3 sess (min 3)); SELL TGB (dropped from list after 3 sess (min 3)); SELL ABX (dropped from list after 3 sess (min 3)); SELL ALM (dropped from list after 3 sess (min 3)); SELL INV (dropped from list after 3 sess (min 3)); BUY AG x58 @ 20.55; BUY BHP x13 @ 91.01; BUY HDSN x206 @ 5.77; BUY IAG x60 @ 19.63; BUY KGC x40 @ 29.63; BUY NFGC x682 @ 1.75; BUY WPM x8 @ 144.54; BUY ABUS x242 @ 4.92 |
| 2026-08-21 | +3.25 | $62.39 | AG×58, BHP×13, HDSN×206, IAG×60, KGC×40, NFGC×682, WPM×8, ABUS×242 | BTBT | — | $55.67 | $10,007.25 | $10,062.92 | AG×58, BHP×13, HDSN×206, IAG×60, KGC×40, NFGC×682, WPM×8, ABUS×242, BTBT×4 | BUY BTBT x4 @ 1.66 |
| 2026-08-24 | -5.17 | $55.67 | AG×58, BHP×13, HDSN×206, IAG×60, KGC×40, NFGC×682, WPM×8, ABUS×242, BTBT×4 | — | — | $55.67 | $10,001.90 | $10,057.57 | AG×58, BHP×13, HDSN×206, IAG×60, KGC×40, NFGC×682, WPM×8, ABUS×242, BTBT×4 | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $55.67 | AG×58, BHP×13, HDSN×206, IAG×60, KGC×40, NFGC×682, WPM×8, ABUS×242, BTBT×4 | INSP, CRMD, BMEA, NPWR, PUSA, ALVO, CAPR, ALIT | AG, BHP, HDSN, IAG, KGC, NFGC, WPM, ABUS | $19.23 | $10,193.14 | $10,212.37 | BTBT×4, INSP×20, CRMD×152, BMEA×778, NPWR×630, PUSA×340, ALVO×241, CAPR×185, ALIT×84 | SELL AG (dropped from list after 3 sess (min 3)); SELL BHP (dropped from list after 3 sess (min 3)); SELL HDSN (dropped from list after 3 sess (min 3)); SELL IAG (dropped from list after 3 sess (min 3)); SELL KGC (dropped from list after 3 sess (min 3)); SELL NFGC (dropped from list after 3 sess (min 3)); SELL WPM (dropped from list after 3 sess (min 3)); SELL ABUS (dropped from list after 3 sess (min 3)); BUY INSP x20 @ 61.47; BUY CRMD x152 @ 8.28; BUY BMEA x778 @ 1.62; BUY NPWR x630 @ 2.00; BUY PUSA x340 @ 3.70; BUY ALVO x241 @ 5.22; BUY CAPR x185 @ 6.79; BUY ALIT x84 @ 14.86 |
| 2026-08-26 | +2.02 | $19.23 | BTBT×4, INSP×20, CRMD×152, BMEA×778, NPWR×630, PUSA×340, ALVO×241, CAPR×185, ALIT×84 | — | — | $19.23 | $10,034.93 | $10,054.16 | BTBT×4, INSP×20, CRMD×152, BMEA×778, NPWR×630, PUSA×340, ALVO×241, CAPR×185, ALIT×84 | hold BTBT,INSP,CRMD,BMEA,NPWR,PUSA,ALVO,CAPR,ALIT |
| 2026-08-27 | — | $19.23 | BTBT×4, INSP×20, CRMD×152, BMEA×778, NPWR×630, PUSA×340, ALVO×241, CAPR×185, ALIT×84 | — | BTBT | $25.25 | $10,409.59 | $10,434.84 | INSP×20, CRMD×152, BMEA×778, NPWR×630, PUSA×340, ALVO×241, CAPR×185, ALIT×84 | SELL BTBT (dropped from list after 4 sess (min 3)) |
| 2026-08-28 | +0.75 | $25.25 | INSP×20, CRMD×152, BMEA×778, NPWR×630, PUSA×340, ALVO×241, CAPR×185, ALIT×84 | ANF, SEDG, SMTC, GRRR, URBN, VYX, TTMI, NVRI | INSP, CRMD, BMEA, NPWR, PUSA, ALVO, CAPR, ALIT | $246.51 | $10,011.60 | $10,258.11 | ANF×9, SEDG×38, SMTC×8, GRRR×81, URBN×15, VYX×145, TTMI×10, NVRI×56 | SELL INSP (dropped from list after 3 sess (min 3)); SELL CRMD (dropped from list after 3 sess (min 3)); SELL BMEA (dropped from list after 3 sess (min 3)); SELL NPWR (dropped from list after 3 sess (min 3)); SELL PUSA (dropped from list after 3 sess (min 3)); SELL ALVO (dropped from list after 3 sess (min 3)); SELL CAPR (dropped from list after 3 sess (min 3)); SELL ALIT (dropped from list after 3 sess (min 3)); BUY ANF x9 @ 144.70; BUY SEDG x38 @ 33.78; BUY SMTC x8 @ 149.40; BUY GRRR x81 @ 15.94; BUY URBN x15 @ 82.70; BUY VYX x145 @ 8.95; BUY TTMI x10 @ 127.07; BUY NVRI x56 @ 23.11 |
| 2026-08-31 | -5.85 | $246.51 | ANF×9, SEDG×38, SMTC×8, GRRR×81, URBN×15, VYX×145, TTMI×10, NVRI×56 | — | — | $246.51 | $9,698.73 | $9,945.24 | ANF×9, SEDG×38, SMTC×8, GRRR×81, URBN×15, VYX×145, TTMI×10, NVRI×56 | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $246.51 | ANF×9, SEDG×38, SMTC×8, GRRR×81, URBN×15, VYX×145, TTMI×10, NVRI×56 | — | — | $246.51 | $9,556.78 | $9,803.29 | ANF×9, SEDG×38, SMTC×8, GRRR×81, URBN×15, VYX×145, TTMI×10, NVRI×56 | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $246.51 | ANF×9, SEDG×38, SMTC×8, GRRR×81, URBN×15, VYX×145, TTMI×10, NVRI×56 | — | ANF, SEDG, SMTC, GRRR, URBN, VYX, TTMI, NVRI | $9,726.08 | $0.00 | $9,726.08 | — | SELL ANF (dropped from list after 3 sess (min 3)); SELL SEDG (dropped from list after 3 sess (min 3)); SELL SMTC (dropped from list after 3 sess (min 3)); SELL GRRR (dropped from list after 3 sess (min 3)); SELL URBN (dropped from list after 3 sess (min 3)); SELL VYX (dropped from list after 3 sess (min 3)); SELL TTMI (dropped from list after 3 sess (min 3)); SELL NVRI (dropped from list after 3 sess (min 3)); hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $9,726.08 | — | HRMY, VSTM, RVTY, CRK, MMED, CTMX, CRDL, CLYM | — | $105.45 | $9,802.76 | $9,908.21 | HRMY×29, VSTM×157, RVTY×9, CRK×77, MMED×53, CTMX×326, CRDL×562, CLYM×82 | BUY HRMY x29 @ 41.31; BUY VSTM x157 @ 7.70; BUY RVTY x9 @ 125.94; BUY CRK x77 @ 15.70; BUY MMED x53 @ 22.78; BUY CTMX x326 @ 3.72; BUY CRDL x562 @ 2.16; BUY CLYM x82 @ 14.79 |
| 2026-09-04 | — | $105.45 | HRMY×29, VSTM×157, RVTY×9, CRK×77, MMED×53, CTMX×326, CRDL×562, CLYM×82 | GPRO, EOSE, SLBT, CCOI | — | $59.28 | $9,709.93 | $9,769.21 | HRMY×29, VSTM×157, RVTY×9, CRK×77, MMED×53, CTMX×326, CRDL×562, CLYM×82, GPRO×7, EOSE×3, SLBT×4, CCOI×1 | BUY GPRO x7 @ 1.78; BUY EOSE x3 @ 3.57; BUY SLBT x4 @ 3.07; BUY CCOI x1 @ 10.22 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $8,918.51 | ▼ $9,998.00 (-2.00) | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+5.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $7,741.30 | ▼ $9,995.99 (-4.01) | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+3.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $6,539.28 | ▼ $9,993.97 (-6.03) | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $5,544.55 | ▼ $9,991.97 (-8.03) | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 21 | $57.61 | $2.05 | — | $4,332.68 | ▼ $9,989.91 (-10.09) | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+5.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 138 | $9.01 | $2.40 | — | $3,086.90 | ▼ $9,987.51 (-12.49) | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1334 | $0.94 | $16.50 | — | $1,820.44 | ▼ $9,971.01 (-28.99) | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $560.20 | ▼ $9,960.26 (-39.74) | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 1 | $46.18 | $0.46 | — | $513.55 | ▲ $10,054.38 (+54.38) | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+6.7; leftover $70.02 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 17 | $4.05 | $0.74 | — | $443.96 | ▲ $10,053.64 (+53.64) | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=-12.3; leftover $70.02 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 8 | $8.46 | $0.70 | — | $375.58 | ▲ $10,052.94 (+52.94) | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=+0.4; leftover $70.02 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 7 | $9.12 | $0.66 | — | $311.08 | ▲ $10,052.28 (+52.28) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $70.02 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 4 | $16.20 | $0.66 | — | $245.62 | ▲ $10,051.62 (+51.62) | combo gate; gate blue=True,ret_5_max=10.0; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $70.02 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `INV` | 43 | $1.62 | $0.83 | — | $175.14 | ▲ $10,050.79 (+50.79) | combo gate; gate blue=True,ret_5_max=10.0; list yday_mover; 🔵; ⚪; ret5=-53.0; leftover $70.02 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `TLN` | 3 | $321.00 | $2.02 | $-120.51 | $1,136.12 | ▼ $9,592.06 (-407.94) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `VST` | 8 | $140.74 | $2.03 | $-53.33 | $2,260.00 | ▼ $9,590.02 (-409.98) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `NRG` | 10 | $116.20 | $2.04 | $-42.06 | $3,419.96 | ▼ $9,587.98 (-412.02) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `DAVE` | 3 | $334.00 | $2.02 | $+5.25 | $4,419.94 | ▼ $9,585.96 (-414.04) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `SLG` | 21 | $57.50 | $2.07 | $-6.44 | $5,625.37 | ▼ $9,583.89 (-416.11) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `MARA` | 138 | $8.91 | $2.44 | $-18.64 | $6,852.51 | ▼ $9,581.45 (-418.55) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 1334 | $0.88 | $15.97 | $-108.51 | $8,010.46 | ▼ $9,565.48 (-434.52) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 833 | $1.42 | $10.89 | $-88.28 | $9,182.43 | ▼ $9,554.59 (-445.41) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DVN` | 1 | $49.02 | $0.51 | $+1.86 | $9,230.93 | ▼ $9,558.17 (-441.83) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 17 | $3.92 | $0.74 | $-3.69 | $9,296.84 | ▼ $9,557.44 (-442.56) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGB` | 8 | $8.35 | $0.71 | $-2.29 | $9,362.92 | ▼ $9,556.72 (-443.28) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `ABX` | 7 | $9.13 | $0.68 | $-1.27 | $9,426.15 | ▼ $9,556.04 (-443.96) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `ALM` | 4 | $15.81 | $0.66 | $-2.88 | $9,488.73 | ▼ $9,555.38 (-444.62) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `INV` | 43 | $1.55 | $0.82 | $-4.65 | $9,554.56 | ▼ $9,554.56 (-445.44) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 58 | $20.55 | $2.16 | — | $8,360.50 | ▼ $9,552.40 (-447.60) | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1194.32 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,175.34 | ▼ $9,550.37 (-449.63) | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1194.32 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 206 | $5.77 | $2.66 | — | $5,984.06 | ▼ $9,547.71 (-452.29) | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1194.32 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 60 | $19.63 | $2.17 | — | $4,804.09 | ▼ $9,545.54 (-454.46) | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1194.32 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 40 | $29.63 | $2.11 | — | $3,616.78 | ▼ $9,543.43 (-456.57) | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1194.32 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 682 | $1.75 | $8.80 | — | $2,414.49 | ▼ $9,534.64 (-465.36) | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1194.32 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $1,256.15 | ▼ $9,532.62 (-467.38) | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1194.32 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 242 | $4.92 | $3.12 | — | $62.39 | ▼ $9,529.50 (-470.50) | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1194.32 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 4 | $1.66 | $0.08 | — | $55.67 | ▲ $10,018.67 (+18.67) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $7.80 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 58 | $20.73 | $2.18 | $+6.09 | $1,255.83 | ▲ $10,112.30 (+112.30) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.95 | $2.05 | $+60.14 | $2,501.13 | ▲ $10,110.25 (+110.25) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 206 | $5.53 | $2.70 | $-54.80 | $3,637.61 | ▲ $10,107.55 (+107.55) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 60 | $21.63 | $2.19 | $+115.64 | $4,933.22 | ▲ $10,105.36 (+105.36) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 40 | $32.76 | $2.13 | $+120.96 | $6,241.49 | ▲ $10,103.23 (+103.23) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 682 | $1.91 | $8.92 | $+91.40 | $7,535.19 | ▲ $10,094.31 (+94.31) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $160.00 | $2.03 | $+119.63 | $8,813.15 | ▲ $10,092.27 (+92.27) | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `ABUS` | 242 | $5.26 | $3.17 | $+75.99 | $10,082.90 | ▲ $10,089.10 (+89.10) | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 20 | $61.47 | $2.05 | — | $8,851.45 | ▲ $10,087.05 (+87.05) | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+9.2; leftover $1260.36 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 152 | $8.28 | $2.45 | — | $7,590.44 | ▲ $10,084.60 (+84.60) | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1260.36 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 778 | $1.62 | $10.04 | — | $6,320.05 | ▲ $10,074.57 (+74.57) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1260.36 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 630 | $2.00 | $8.13 | — | $5,051.92 | ▲ $10,066.44 (+66.44) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $1260.36 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 340 | $3.70 | $4.39 | — | $3,789.53 | ▲ $10,062.05 (+62.05) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.8; leftover $1260.36 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 241 | $5.22 | $3.11 | — | $2,528.40 | ▲ $10,058.94 (+58.94) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1260.36 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 185 | $6.79 | $2.54 | — | $1,269.71 | ▲ $10,056.40 (+56.40) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.4; leftover $1260.36 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALIT` | 84 | $14.86 | $2.24 | — | $19.23 | ▲ $10,054.16 (+54.16) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+3.0; leftover $1260.36 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `BTBT` | 4 | $1.53 | $0.09 | $-0.69 | $25.25 | ▲ $10,398.08 (+398.08) | dropped from list after 4 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 20 | $62.10 | $2.07 | $+8.48 | $1,265.18 | ▲ $10,472.27 (+472.27) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 152 | $8.49 | $2.48 | $+26.99 | $2,553.18 | ▲ $10,469.79 (+469.79) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 778 | $1.74 | $10.18 | $+73.15 | $3,896.73 | ▲ $10,459.62 (+459.62) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `NPWR` | 630 | $1.83 | $8.24 | $-123.47 | $5,041.39 | ▲ $10,451.38 (+451.38) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `PUSA` | 340 | $3.86 | $4.45 | $+45.56 | $6,349.33 | ▲ $10,446.92 (+446.92) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALVO` | 241 | $4.88 | $3.16 | $-88.21 | $7,522.25 | ▲ $10,443.76 (+443.76) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CAPR` | 185 | $9.19 | $2.59 | $+438.87 | $9,219.81 | ▲ $10,441.17 (+441.17) | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `ALIT` | 84 | $14.54 | $2.27 | $-31.39 | $10,438.91 | ▲ $10,438.91 (+438.91) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $144.70 | $2.02 | — | $9,134.59 | ▲ $10,436.89 (+436.89) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1304.86 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 38 | $33.78 | $2.10 | — | $7,848.85 | ▲ $10,434.79 (+434.79) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1304.86 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $149.40 | $2.01 | — | $6,651.63 | ▲ $10,432.77 (+432.77) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1304.86 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 81 | $15.94 | $2.23 | — | $5,358.26 | ▲ $10,430.54 (+430.54) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.9; leftover $1304.86 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 15 | $82.70 | $2.04 | — | $4,115.73 | ▲ $10,428.51 (+428.51) | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer,yday_mover; 🔵; ret5=-4.6; leftover $1304.86 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VYX` | 145 | $8.95 | $2.42 | — | $2,815.55 | ▲ $10,426.08 (+426.08) | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer; 🔵; ret5=-3.1; leftover $1304.86 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 10 | $127.07 | $2.02 | — | $1,542.83 | ▲ $10,424.06 (+424.06) | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer,mover_buy; 🔵; ⚪; ret5=-21.0; leftover $1304.86 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NVRI` | 56 | $23.11 | $2.16 | — | $246.51 | ▲ $10,421.90 (+421.90) | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer; 🔵; ret5=+0.3; leftover $1304.86 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 9 | $142.00 | $2.04 | $-28.35 | $1,522.47 | ▼ $9,741.22 (-258.78) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 38 | $31.87 | $2.12 | $-76.81 | $2,731.41 | ▼ $9,739.10 (-260.90) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 8 | $127.63 | $2.03 | $-178.21 | $3,750.42 | ▼ $9,737.07 (-262.93) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GRRR` | 81 | $14.75 | $2.26 | $-100.88 | $4,942.91 | ▼ $9,734.81 (-265.19) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `URBN` | 15 | $79.12 | $2.06 | $-57.79 | $6,127.66 | ▼ $9,732.76 (-267.24) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `VYX` | 145 | $8.30 | $2.46 | $-99.13 | $7,328.70 | ▼ $9,730.30 (-269.70) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TTMI` | 10 | $116.68 | $2.04 | $-107.96 | $8,493.46 | ▼ $9,728.26 (-271.74) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `NVRI` | 56 | $22.05 | $2.18 | $-63.70 | $9,726.08 | ▼ $9,726.08 (-273.92) | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 29 | $41.31 | $2.08 | — | $8,526.01 | ▼ $9,724.00 (-276.00) | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1215.76 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 157 | $7.70 | $2.46 | — | $7,314.65 | ▼ $9,721.54 (-278.46) | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1215.76 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $6,179.17 | ▼ $9,719.52 (-280.48) | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1215.76 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 77 | $15.70 | $2.22 | — | $4,968.05 | ▼ $9,717.30 (-282.70) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1215.76 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 53 | $22.78 | $2.15 | — | $3,758.56 | ▼ $9,715.15 (-284.85) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1215.76 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CTMX` | 326 | $3.72 | $4.21 | — | $2,541.64 | ▼ $9,710.95 (-289.05) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $1215.76 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 562 | $2.16 | $7.25 | — | $1,320.47 | ▼ $9,703.70 (-296.30) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1215.76 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CLYM` | 82 | $14.79 | $2.24 | — | $105.45 | ▼ $9,701.46 (-298.54) | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer; 🔵; ret5=+5.8; leftover $1215.76 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `GPRO` | 7 | $1.78 | $0.15 | — | $92.85 | ▼ $9,844.19 (-155.81) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+5.9; leftover $13.18 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 3 | $3.57 | $0.12 | — | $82.02 | ▼ $9,844.07 (-155.93) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $13.18 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 4 | $3.07 | $0.13 | — | $69.61 | ▼ $9,843.94 (-156.06) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $13.18 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CCOI` | 1 | $10.22 | $0.11 | — | $59.28 | ▼ $9,843.83 (-156.17) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $13.18 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `TLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `VST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `NRG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `DAVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `SLG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `EOG` | cash | leftover split 70.02 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 70.02 < 1 share @ 202.70 |
| 2026-08-18 | `TLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `VST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NRG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `DAVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `SLG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `DVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ABX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ALM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `INV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-19 | `DVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ABX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ALM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `INV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ULTA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `UGI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ABUS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CRSP` | cash | leftover split 7.80 < 1 share @ 59.72 |
| 2026-08-21 | `FUTU` | cash | leftover split 7.80 < 1 share @ 115.18 |
| 2026-08-21 | `GMAB` | cash | leftover split 7.80 < 1 share @ 33.36 |
| 2026-08-21 | `MRVI` | cash | leftover split 7.80 < 1 share @ 8.20 |
| 2026-08-21 | `DE` | cash | leftover split 7.80 < 1 share @ 623.26 |
| 2026-08-21 | `WOLF` | cash | leftover split 7.80 < 1 share @ 26.86 |
| 2026-08-21 | `AMRC` | cash | leftover split 7.80 < 1 share @ 22.51 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABUS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BJ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `BTBT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `INSP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `PUSA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALIT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-26 | `FLNC` | no_price | no 09:30 open |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `AVEX` | no_price | no 09:30 open |
| 2026-08-26 | `BZ` | no_price | no 09:30 open |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `PUSA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALIT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ACMR` | cash | leftover split 3.16 < 1 share @ 80.97 |
| 2026-08-27 | `GGB` | cash | leftover split 3.16 < 1 share @ 4.42 |
| 2026-08-27 | `MT` | cash | leftover split 3.16 < 1 share @ 75.12 |
| 2026-08-27 | `MU` | cash | leftover split 3.16 < 1 share @ 925.74 |
| 2026-08-27 | `TX` | cash | leftover split 3.16 < 1 share @ 55.20 |
| 2026-08-27 | `ANET` | cash | leftover split 3.16 < 1 share @ 190.90 |
| 2026-08-27 | `ASML` | cash | leftover split 3.16 < 1 share @ 1746.33 |
| 2026-08-27 | `DLO` | cash | leftover split 3.16 < 1 share @ 15.60 |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GRRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `URBN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `VYX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TTMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `NVRI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SRPT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ZJYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GRRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `URBN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `VYX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TTMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NVRI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `OKE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AME` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLDB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NAGE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OHI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BMRN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CTMX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CLYM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `OSCR` | cash | leftover split 13.18 < 1 share @ 30.65 |
| 2026-09-04 | `BVS` | cash | leftover split 13.18 < 1 share @ 14.50 |
| 2026-09-04 | `DELL` | cash | leftover split 13.18 < 1 share @ 486.31 |
| 2026-09-04 | `MLYS` | cash | leftover split 13.18 < 1 share @ 29.15 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `HRMY` | 29 | 2026-09-03 @ $41.31 | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1215.76 |
| `VSTM` | 157 | 2026-09-03 @ $7.70 | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1215.76 |
| `RVTY` | 9 | 2026-09-03 @ $125.94 | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1215.76 |
| `CRK` | 77 | 2026-09-03 @ $15.70 | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1215.76 |
| `MMED` | 53 | 2026-09-03 @ $22.78 | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1215.76 |
| `CTMX` | 326 | 2026-09-03 @ $3.72 | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $1215.76 |
| `CRDL` | 562 | 2026-09-03 @ $2.16 | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1215.76 |
| `CLYM` | 82 | 2026-09-03 @ $14.79 | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer; 🔵; ret5=+5.8; leftover $1215.76 |
| `GPRO` | 7 | 2026-09-04 @ $1.78 | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+5.9; leftover $13.18 |
| `EOSE` | 3 | 2026-09-04 @ $3.57 | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $13.18 |
| `SLBT` | 4 | 2026-09-04 @ $3.07 | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $13.18 |
| `CCOI` | 1 | 2026-09-04 @ $10.22 | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $13.18 |
