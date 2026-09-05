# Factor mine action — `union_blue_coil_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-2.42%** ($9,758) · signal-only (no cash/fees) was +3.85%. Starts YES **3/17**. Fills 134 · skips 47 · realized $+7.85.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `blue=True,ret_5_max=10.0` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $312.92.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-14 | +5.50 | $10,000.00 | — | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | — | $560.20 | $9,491.27 | $10,051.46 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833 | BUY TLN x3 @ 359.83; BUY VST x8 @ 146.90; BUY NRG x10 @ 120.00; BUY DAVE x3 @ 330.91; BUY SLG x21 @ 57.61; BUY MARA x138 @ 9.01; BUY LDI x1334 @ 0.94; BUY BTBT x833 @ 1.50 |
| 2026-08-17 | +2.25 | $560.20 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833 | DVN, EOG, FANG, TMC, TGB, ABX, ALM, INV | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | $140.13 | $9,723.83 | $9,863.96 | DVN×27, EOG×8, FANG×6, TMC×309, TGB×147, ABX×137, ALM×77, INV×772 | SELL TLN (dropped from list after 1 sess (min 1)); SELL VST (dropped from list after 1 sess (min 1)); SELL NRG (dropped from list after 1 sess (min 1)); SELL DAVE (dropped from list after 1 sess (min 1)); SELL SLG (dropped from list after 1 sess (min 1)); SELL MARA (dropped from list after 1 sess (min 1)); SELL LDI (dropped from list after 1 sess (min 1)); SELL BTBT (dropped from list after 1 sess (min 1)); BUY DVN x27 @ 46.18; BUY EOG x8 @ 142.77; BUY FANG x6 @ 202.70; BUY TMC x309 @ 4.05; BUY TGB x147 @ 8.46; BUY ABX x137 @ 9.12; BUY ALM x77 @ 16.20; BUY INV x772 @ 1.62 |
| 2026-08-18 | -6.20 | $140.13 | DVN×27, EOG×8, FANG×6, TMC×309, TGB×147, ABX×137, ALM×77, INV×772 | — | DVN, EOG, FANG, TMC, TGB, ABX, ALM, INV | $9,727.99 | $0.00 | $9,727.99 | — | SELL DVN (dropped from list after 1 sess (min 1)); SELL EOG (dropped from list after 1 sess (min 1)); SELL FANG (dropped from list after 1 sess (min 1)); SELL TMC (dropped from list after 1 sess (min 1)); SELL TGB (dropped from list after 1 sess (min 1)); SELL ABX (dropped from list after 1 sess (min 1)); SELL ALM (dropped from list after 1 sess (min 1)); SELL INV (dropped from list after 1 sess (min 1)) |
| 2026-08-19 | -7.20 | $9,727.99 | — | — | — | $9,727.99 | $0.00 | $9,727.99 | — | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $9,727.99 | — | AG, BHP, HDSN, IAG, KGC, NFGC, WPM, ABUS | — | $97.05 | $9,770.92 | $9,867.97 | AG×59, BHP×13, HDSN×210, IAG×61, KGC×41, NFGC×694, WPM×8, ABUS×247 | BUY AG x59 @ 20.55; BUY BHP x13 @ 91.01; BUY HDSN x210 @ 5.77; BUY IAG x61 @ 19.63; BUY KGC x41 @ 29.63; BUY NFGC x694 @ 1.75; BUY WPM x8 @ 144.54; BUY ABUS x247 @ 4.92 |
| 2026-08-21 | +3.25 | $97.05 | AG×59, BHP×13, HDSN×210, IAG×61, KGC×41, NFGC×694, WPM×8, ABUS×247 | CRSP, FUTU, GMAB, BTBT, MRVI, DE, WOLF, AMRC | AG, BHP, HDSN, IAG, KGC, NFGC, WPM, ABUS | $47.50 | $10,104.06 | $10,151.56 | CRSP×21, FUTU×11, GMAB×38, BTBT×766, MRVI×155, DE×2, WOLF×47, AMRC×56 | SELL AG (dropped from list after 1 sess (min 1)); SELL BHP (dropped from list after 1 sess (min 1)); SELL HDSN (dropped from list after 1 sess (min 1)); SELL IAG (dropped from list after 1 sess (min 1)); SELL KGC (dropped from list after 1 sess (min 1)); SELL NFGC (dropped from list after 1 sess (min 1)); SELL WPM (dropped from list after 1 sess (min 1)); SELL ABUS (dropped from list after 1 sess (min 1)); BUY CRSP x21 @ 59.72; BUY FUTU x11 @ 115.18; BUY GMAB x38 @ 33.36; BUY BTBT x766 @ 1.66; BUY MRVI x155 @ 8.20; BUY DE x2 @ 623.26; BUY WOLF x47 @ 26.86; BUY AMRC x56 @ 22.51 |
| 2026-08-24 | -5.17 | $47.50 | CRSP×21, FUTU×11, GMAB×38, BTBT×766, MRVI×155, DE×2, WOLF×47, AMRC×56 | — | CRSP, FUTU, GMAB, BTBT, MRVI, DE, WOLF, AMRC | $10,024.64 | $0.00 | $10,024.64 | — | SELL CRSP (dropped from list after 1 sess (min 1)); SELL FUTU (dropped from list after 1 sess (min 1)); SELL GMAB (dropped from list after 1 sess (min 1)); SELL BTBT (dropped from list after 1 sess (min 1)); SELL MRVI (dropped from list after 1 sess (min 1)); SELL DE (dropped from list after 1 sess (min 1)); SELL WOLF (dropped from list after 1 sess (min 1)); SELL AMRC (dropped from list after 1 sess (min 1)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $10,024.64 | — | INSP, CRMD, BMEA, NPWR, PUSA, ALVO, CAPR, ALIT | — | $4.92 | $10,142.35 | $10,147.27 | INSP×20, CRMD×151, BMEA×773, NPWR×626, PUSA×338, ALVO×240, CAPR×184, ALIT×84 | BUY INSP x20 @ 61.47; BUY CRMD x151 @ 8.28; BUY BMEA x773 @ 1.62; BUY NPWR x626 @ 2.00; BUY PUSA x338 @ 3.70; BUY ALVO x240 @ 5.22; BUY CAPR x184 @ 6.79; BUY ALIT x84 @ 14.86 |
| 2026-08-26 | +2.02 | $4.92 | INSP×20, CRMD×151, BMEA×773, NPWR×626, PUSA×338, ALVO×240, CAPR×184, ALIT×84 | — | — | $4.92 | $9,984.94 | $9,989.86 | INSP×20, CRMD×151, BMEA×773, NPWR×626, PUSA×338, ALVO×240, CAPR×184, ALIT×84 | hold INSP,CRMD,BMEA,NPWR,PUSA,ALVO,CAPR,ALIT |
| 2026-08-27 | — | $4.92 | INSP×20, CRMD×151, BMEA×773, NPWR×626, PUSA×338, ALVO×240, CAPR×184, ALIT×84 | ACMR, GGB, MT, MU, TX, ANET, DLO | INSP, CRMD, BMEA, NPWR, PUSA, ALVO, CAPR, ALIT | $1,882.59 | $8,430.93 | $10,313.52 | ACMR×15, GGB×291, MT×17, MU×1, TX×23, ANET×6, DLO×82 | SELL INSP (dropped from list after 2 sess (min 1)); SELL CRMD (dropped from list after 2 sess (min 1)); SELL BMEA (dropped from list after 2 sess (min 1)); SELL NPWR (dropped from list after 2 sess (min 1)); SELL PUSA (dropped from list after 2 sess (min 1)); SELL ALVO (dropped from list after 2 sess (min 1)); SELL CAPR (dropped from list after 2 sess (min 1)); SELL ALIT (dropped from list after 2 sess (min 1)); BUY ACMR x15 @ 80.97; BUY GGB x291 @ 4.42; BUY MT x17 @ 75.12; BUY MU x1 @ 925.74; BUY TX x23 @ 55.20; BUY ANET x6 @ 190.90; BUY DLO x82 @ 15.60 |
| 2026-08-28 | +0.75 | $1,882.59 | ACMR×15, GGB×291, MT×17, MU×1, TX×23, ANET×6, DLO×82 | ANF, SEDG, SMTC, GRRR, URBN, VYX, TTMI, NVRI | ACMR, GGB, MT, MU, TX, ANET, DLO | $370.60 | $9,865.85 | $10,236.45 | ANF×8, SEDG×38, SMTC×8, GRRR×81, URBN×15, VYX×145, TTMI×10, NVRI×56 | SELL ACMR (dropped from list after 1 sess (min 1)); SELL GGB (dropped from list after 1 sess (min 1)); SELL MT (dropped from list after 1 sess (min 1)); SELL MU (dropped from list after 1 sess (min 1)); SELL TX (dropped from list after 1 sess (min 1)); SELL ANET (dropped from list after 1 sess (min 1)); SELL DLO (dropped from list after 1 sess (min 1)); BUY ANF x8 @ 144.70; BUY SEDG x38 @ 33.78; BUY SMTC x8 @ 149.40; BUY GRRR x81 @ 15.94; BUY URBN x15 @ 82.70; BUY VYX x145 @ 8.95; BUY TTMI x10 @ 127.07; BUY NVRI x56 @ 23.11 |
| 2026-08-31 | -5.85 | $370.60 | ANF×8, SEDG×38, SMTC×8, GRRR×81, URBN×15, VYX×145, TTMI×10, NVRI×56 | — | ANF, SEDG, SMTC, GRRR, URBN, VYX, TTMI, NVRI | $9,913.75 | $0.00 | $9,913.75 | — | SELL ANF (dropped from list after 1 sess (min 1)); SELL SEDG (dropped from list after 1 sess (min 1)); SELL SMTC (dropped from list after 1 sess (min 1)); SELL GRRR (dropped from list after 1 sess (min 1)); SELL URBN (dropped from list after 1 sess (min 1)); SELL VYX (dropped from list after 1 sess (min 1)); SELL TTMI (dropped from list after 1 sess (min 1)); SELL NVRI (dropped from list after 1 sess (min 1)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $9,913.75 | — | — | — | $9,913.75 | $0.00 | $9,913.75 | — | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $9,913.75 | — | — | — | $9,913.75 | $0.00 | $9,913.75 | — | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $9,913.75 | — | HRMY, VSTM, RVTY, CRK, MMED, CTMX, CRDL, CLYM | — | $166.71 | $9,931.08 | $10,097.79 | HRMY×29, VSTM×160, RVTY×9, CRK×78, MMED×54, CTMX×333, CRDL×573, CLYM×83 | BUY HRMY x29 @ 41.31; BUY VSTM x160 @ 7.70; BUY RVTY x9 @ 125.94; BUY CRK x78 @ 15.70; BUY MMED x54 @ 22.78; BUY CTMX x333 @ 3.72; BUY CRDL x573 @ 2.16; BUY CLYM x83 @ 14.79 |
| 2026-09-04 | — | $166.71 | HRMY×29, VSTM×160, RVTY×9, CRK×78, MMED×54, CTMX×333, CRDL×573, CLYM×83 | OSCR, BVS, GPRO, EOSE, SLBT, DELL, MLYS, CCOI | HRMY, VSTM, RVTY, CRK, MMED, CTMX, CRDL, CLYM | $312.92 | $9,445.07 | $9,757.99 | OSCR×40, BVS×86, GPRO×702, EOSE×350, SLBT×407, DELL×2, MLYS×42, CCOI×122 | SELL HRMY (dropped from list after 1 sess (min 1)); SELL VSTM (dropped from list after 1 sess (min 1)); SELL RVTY (dropped from list after 1 sess (min 1)); SELL CRK (dropped from list after 1 sess (min 1)); SELL MMED (dropped from list after 1 sess (min 1)); SELL CTMX (dropped from list after 1 sess (min 1)); SELL CRDL (dropped from list after 1 sess (min 1)); SELL CLYM (dropped from list after 1 sess (min 1)); BUY OSCR x40 @ 30.65; BUY BVS x86 @ 14.50; BUY GPRO x702 @ 1.78; BUY EOSE x350 @ 3.57; BUY SLBT x407 @ 3.07; BUY DELL x2 @ 486.31; BUY MLYS x42 @ 29.15; BUY CCOI x122 @ 10.22 |

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
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 3 | $367.88 | $2.02 | $+20.13 | $1,661.82 | ▲ $10,052.82 (+52.82) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 8 | $149.37 | $2.03 | $+15.71 | $2,854.74 | ▲ $10,050.79 (+50.79) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $4,126.70 | ▲ $10,048.75 (+48.75) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 3 | $336.94 | $2.02 | $+14.07 | $5,135.50 | ▲ $10,046.73 (+46.73) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 21 | $55.37 | $2.07 | $-51.17 | $6,296.20 | ▲ $10,044.66 (+44.66) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 138 | $9.22 | $2.44 | $+24.14 | $7,566.12 | ▲ $10,042.22 (+42.22) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 1334 | $0.91 | $16.33 | $-72.85 | $8,759.73 | ▲ $10,025.89 (+25.89) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 833 | $1.52 | $10.89 | $-4.98 | $10,014.99 | ▲ $10,014.99 (+14.99) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 27 | $46.18 | $2.07 | — | $8,766.06 | ▲ $10,012.92 (+12.92) | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+6.7; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $7,621.89 | ▲ $10,010.91 (+10.91) | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+5.8; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $6,403.68 | ▲ $10,008.90 (+8.90) | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+8.3; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 309 | $4.05 | $3.99 | — | $5,148.25 | ▲ $10,004.92 (+4.92) | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 147 | $8.46 | $2.43 | — | $3,902.19 | ▲ $10,002.48 (+2.48) | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 137 | $9.12 | $2.40 | — | $2,650.35 | ▲ $10,000.08 (+0.08) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 77 | $16.20 | $2.22 | — | $1,400.73 | ▼ $9,997.86 (-2.14) | combo gate; gate blue=True,ret_5_max=10.0; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `INV` | 772 | $1.62 | $9.96 | — | $140.13 | ▼ $9,987.90 (-12.10) | combo gate; gate blue=True,ret_5_max=10.0; list yday_mover; 🔵; ⚪; ret5=-53.0; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 27 | $48.00 | $2.09 | $+44.98 | $1,434.04 | ▼ $9,753.34 (-246.66) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 8 | $148.04 | $2.03 | $+38.11 | $2,616.33 | ▼ $9,751.31 (-248.69) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $3,867.88 | ▼ $9,749.28 (-250.72) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 309 | $3.72 | $4.05 | $-110.00 | $5,013.31 | ▼ $9,745.23 (-254.77) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 147 | $8.55 | $2.47 | $+8.33 | $6,267.70 | ▼ $9,742.77 (-257.23) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 137 | $9.03 | $2.43 | $-17.16 | $7,502.37 | ▼ $9,740.33 (-259.67) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 77 | $15.78 | $2.24 | $-36.80 | $8,715.19 | ▼ $9,738.09 (-261.91) | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `INV` | 772 | $1.32 | $10.10 | $-247.80 | $9,727.99 | ▼ $9,727.99 (-272.01) | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 59 | $20.55 | $2.17 | — | $8,513.38 | ▼ $9,725.83 (-274.17) | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1216.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,328.22 | ▼ $9,723.80 (-276.20) | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1216.00 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 210 | $5.77 | $2.71 | — | $6,113.81 | ▼ $9,721.09 (-278.91) | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1216.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 61 | $19.63 | $2.17 | — | $4,914.20 | ▼ $9,718.91 (-281.09) | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1216.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 41 | $29.63 | $2.11 | — | $3,697.26 | ▼ $9,716.80 (-283.20) | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1216.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 694 | $1.75 | $8.95 | — | $2,473.81 | ▼ $9,707.85 (-292.15) | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1216.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $1,315.48 | ▼ $9,705.84 (-294.16) | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1216.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 247 | $4.92 | $3.19 | — | $97.05 | ▼ $9,702.65 (-297.35) | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1216.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 59 | $21.90 | $2.19 | $+75.30 | $1,386.96 | ▲ $10,196.62 (+196.62) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,629.27 | ▲ $10,194.57 (+194.57) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 210 | $5.67 | $2.75 | $-26.46 | $3,817.22 | ▲ $10,191.82 (+191.82) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 61 | $21.17 | $2.19 | $+89.57 | $5,106.40 | ▲ $10,189.63 (+189.63) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 41 | $32.17 | $2.13 | $+99.89 | $6,423.23 | ▲ $10,187.49 (+187.49) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 694 | $1.79 | $9.08 | $+9.73 | $7,656.41 | ▲ $10,178.41 (+178.41) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $8,891.98 | ▲ $10,176.38 (+176.38) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 247 | $5.20 | $3.24 | $+62.74 | $10,173.14 | ▲ $10,173.14 (+173.14) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $8,916.97 | ▲ $10,171.09 (+171.09) | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1271.64 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 11 | $115.18 | $2.02 | — | $7,647.97 | ▲ $10,169.07 (+169.07) | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1271.64 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GMAB` | 38 | $33.36 | $2.10 | — | $6,378.18 | ▲ $10,166.96 (+166.96) | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+6.6; leftover $1271.64 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 766 | $1.66 | $9.88 | — | $5,096.74 | ▲ $10,157.08 (+157.08) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1271.64 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 155 | $8.20 | $2.46 | — | $3,823.29 | ▲ $10,154.63 (+154.63) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1271.64 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $2,574.77 | ▲ $10,152.63 (+152.63) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1271.64 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `WOLF` | 47 | $26.86 | $2.13 | — | $1,310.22 | ▲ $10,150.50 (+150.50) | combo gate; gate blue=True,ret_5_max=10.0; list yday_mover; 🔵; ret5=-16.4; leftover $1271.64 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AMRC` | 56 | $22.51 | $2.16 | — | $47.50 | ▲ $10,148.34 (+148.34) | combo gate; gate blue=True,ret_5_max=10.0; list yday_mover; 🔵; ret5=-20.2; leftover $1271.64 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 21 | $58.79 | $2.07 | $-23.66 | $1,280.02 | ▲ $10,047.67 (+47.67) | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 11 | $120.87 | $2.04 | $+58.52 | $2,607.54 | ▲ $10,045.62 (+45.62) | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `GMAB` | 38 | $32.82 | $2.12 | $-24.75 | $3,852.58 | ▲ $10,043.50 (+43.50) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 766 | $1.55 | $10.02 | $-104.16 | $5,029.86 | ▲ $10,033.48 (+33.48) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 155 | $8.59 | $2.49 | $+55.50 | $6,358.82 | ▲ $10,030.99 (+30.99) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.62 | $2.02 | $+56.71 | $7,664.04 | ▲ $10,028.97 (+28.97) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `WOLF` | 47 | $25.07 | $2.15 | $-88.41 | $8,840.18 | ▲ $10,026.82 (+26.82) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AMRC` | 56 | $21.19 | $2.18 | $-78.26 | $10,024.64 | ▲ $10,024.64 (+24.64) | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 20 | $61.47 | $2.05 | — | $8,793.19 | ▲ $10,022.59 (+22.59) | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+9.2; leftover $1253.08 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 151 | $8.28 | $2.44 | — | $7,540.47 | ▲ $10,020.15 (+20.15) | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1253.08 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 773 | $1.62 | $9.97 | — | $6,278.24 | ▲ $10,010.18 (+10.18) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1253.08 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 626 | $2.00 | $8.08 | — | $5,018.16 | ▲ $10,002.10 (+2.10) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $1253.08 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 338 | $3.70 | $4.36 | — | $3,763.20 | ▼ $9,997.74 (-2.26) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.8; leftover $1253.08 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 240 | $5.22 | $3.10 | — | $2,507.31 | ▼ $9,994.65 (-5.35) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1253.08 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 184 | $6.79 | $2.54 | — | $1,255.41 | ▼ $9,992.11 (-7.89) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.4; leftover $1253.08 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALIT` | 84 | $14.86 | $2.24 | — | $4.92 | ▼ $9,989.86 (-10.14) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+3.0; leftover $1253.08 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 20 | $60.07 | $2.07 | $-32.12 | $1,204.25 | ▲ $10,329.66 (+329.66) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 151 | $8.60 | $2.48 | $+43.40 | $2,500.38 | ▲ $10,327.19 (+327.19) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BMEA` | 773 | $1.75 | $10.11 | $+80.41 | $3,843.02 | ▲ $10,317.08 (+317.08) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NPWR` | 626 | $1.93 | $8.19 | $-60.08 | $5,043.01 | ▲ $10,308.89 (+308.89) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PUSA` | 338 | $3.84 | $4.43 | $+38.53 | $6,336.50 | ▲ $10,304.46 (+304.46) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALVO` | 240 | $4.98 | $3.15 | $-63.84 | $7,528.55 | ▲ $10,301.31 (+301.31) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 184 | $8.29 | $2.58 | $+270.87 | $9,051.33 | ▲ $10,298.73 (+298.73) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALIT` | 84 | $14.85 | $2.27 | $-5.35 | $10,296.46 | ▲ $10,296.46 (+296.46) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 15 | $80.97 | $2.04 | — | $9,079.88 | ▲ $10,294.43 (+294.43) | combo gate; gate blue=True,ret_5_max=10.0; list mover_buy; 🔵; ret5=-1.3; leftover $1287.06 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 291 | $4.42 | $3.75 | — | $7,789.90 | ▲ $10,290.67 (+290.67) | combo gate; gate blue=True,ret_5_max=10.0; list mover_buy; 🔵; ret5=-8.6; leftover $1287.06 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 17 | $75.12 | $2.04 | — | $6,510.82 | ▲ $10,288.63 (+288.63) | combo gate; gate blue=True,ret_5_max=10.0; list mover_buy; 🔵; ret5=-2.2; leftover $1287.06 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $925.74 | $1.99 | — | $5,583.09 | ▲ $10,286.64 (+286.64) | combo gate; gate blue=True,ret_5_max=10.0; list mover_buy; 🔵; ret5=-0.5; leftover $1287.06 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `TX` | 23 | $55.20 | $2.06 | — | $4,311.43 | ▲ $10,284.58 (+284.58) | combo gate; gate blue=True,ret_5_max=10.0; list mover_buy; 🔵; ret5=+3.0; leftover $1287.06 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ANET` | 6 | $190.90 | $2.01 | — | $3,164.02 | ▲ $10,282.57 (+282.57) | combo gate; gate blue=True,ret_5_max=10.0; list mover_buy; 🔵; ret5=-5.1; leftover $1287.06 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 82 | $15.60 | $2.24 | — | $1,882.59 | ▲ $10,280.34 (+280.34) | combo gate; gate blue=True,ret_5_max=10.0; list mover_buy; 🔵; ret5=+7.1; leftover $1287.06 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 15 | $81.65 | $2.06 | $+6.11 | $3,105.28 | ▲ $10,432.55 (+432.55) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 291 | $4.57 | $3.81 | $+36.08 | $4,431.34 | ▲ $10,428.74 (+428.74) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 17 | $74.54 | $2.06 | $-13.96 | $5,696.46 | ▲ $10,426.68 (+426.68) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $967.01 | $2.01 | $+37.26 | $6,661.45 | ▲ $10,424.66 (+424.66) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TX` | 23 | $55.25 | $2.08 | $-2.99 | $7,930.13 | ▲ $10,422.59 (+422.59) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ANET` | 6 | $205.90 | $2.03 | $+85.96 | $9,163.50 | ▲ $10,420.56 (+420.56) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DLO` | 82 | $15.33 | $2.26 | $-26.64 | $10,418.30 | ▲ $10,418.30 (+418.30) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $144.70 | $2.01 | — | $9,258.68 | ▲ $10,416.28 (+416.28) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1302.29 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 38 | $33.78 | $2.10 | — | $7,972.94 | ▲ $10,414.18 (+414.18) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1302.29 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $149.40 | $2.01 | — | $6,775.73 | ▲ $10,412.17 (+412.17) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1302.29 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 81 | $15.94 | $2.23 | — | $5,482.35 | ▲ $10,409.93 (+409.93) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.9; leftover $1302.29 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 15 | $82.70 | $2.04 | — | $4,239.82 | ▲ $10,407.90 (+407.90) | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer,yday_mover; 🔵; ret5=-4.6; leftover $1302.29 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VYX` | 145 | $8.95 | $2.42 | — | $2,939.64 | ▲ $10,405.47 (+405.47) | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer; 🔵; ret5=-3.1; leftover $1302.29 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 10 | $127.07 | $2.02 | — | $1,666.92 | ▲ $10,403.45 (+403.45) | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer,mover_buy; 🔵; ⚪; ret5=-21.0; leftover $1302.29 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NVRI` | 56 | $23.11 | $2.16 | — | $370.60 | ▲ $10,401.29 (+401.29) | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer; 🔵; ret5=+0.3; leftover $1302.29 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 8 | $148.67 | $2.03 | $+27.71 | $1,557.93 | ▼ $9,928.90 (-71.10) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 38 | $31.50 | $2.12 | $-90.87 | $2,752.81 | ▼ $9,926.78 (-73.22) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 8 | $133.04 | $2.03 | $-134.93 | $3,815.09 | ▼ $9,924.74 (-75.26) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 81 | $14.32 | $2.26 | $-135.71 | $4,972.76 | ▼ $9,922.49 (-77.51) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 15 | $81.09 | $2.06 | $-28.24 | $6,187.05 | ▼ $9,920.43 (-79.57) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `VYX` | 145 | $9.06 | $2.46 | $+11.07 | $7,498.29 | ▼ $9,917.97 (-82.03) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TTMI` | 10 | $117.20 | $2.04 | $-102.76 | $8,668.25 | ▼ $9,915.93 (-84.07) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NVRI` | 56 | $22.28 | $2.18 | $-50.82 | $9,913.75 | ▼ $9,913.75 (-86.25) | dropped from list after 1 sess (min 1) | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 29 | $41.31 | $2.08 | — | $8,713.69 | ▼ $9,911.68 (-88.32) | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1239.22 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 160 | $7.70 | $2.47 | — | $7,479.22 | ▼ $9,909.21 (-90.79) | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1239.22 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $6,343.74 | ▼ $9,907.19 (-92.81) | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1239.22 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 78 | $15.70 | $2.22 | — | $5,116.92 | ▼ $9,904.97 (-95.03) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1239.22 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 54 | $22.78 | $2.15 | — | $3,884.64 | ▼ $9,902.81 (-97.19) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1239.22 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CTMX` | 333 | $3.72 | $4.30 | — | $2,641.59 | ▼ $9,898.52 (-101.48) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $1239.22 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 573 | $2.16 | $7.39 | — | $1,396.52 | ▼ $9,891.13 (-108.87) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1239.22 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CLYM` | 83 | $14.79 | $2.24 | — | $166.71 | ▼ $9,888.89 (-111.11) | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer; 🔵; ret5=+5.8; leftover $1239.22 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 29 | $42.93 | $2.10 | $+42.81 | $1,409.58 | ▲ $10,030.96 (+30.96) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 160 | $8.03 | $2.51 | $+47.82 | $2,691.87 | ▲ $10,028.45 (+28.45) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $132.45 | $2.04 | $+54.54 | $3,881.89 | ▲ $10,026.42 (+26.42) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 78 | $15.45 | $2.25 | $-23.97 | $5,084.74 | ▲ $10,024.17 (+24.17) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 54 | $23.88 | $2.17 | $+55.08 | $6,372.09 | ▲ $10,022.00 (+22.00) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CTMX` | 333 | $3.73 | $4.36 | $-5.33 | $7,609.82 | ▲ $10,017.64 (+17.64) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 573 | $2.18 | $7.50 | $-3.43 | $8,851.46 | ▲ $10,010.14 (+10.14) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CLYM` | 83 | $13.96 | $2.26 | $-73.39 | $10,007.88 | ▲ $10,007.88 (+7.88) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `OSCR` | 40 | $30.65 | $2.11 | — | $8,779.77 | ▲ $10,005.77 (+5.77) | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=-2.2; leftover $1250.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 86 | $14.50 | $2.25 | — | $7,530.52 | ▲ $10,003.52 (+3.52) | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1250.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `GPRO` | 702 | $1.78 | $9.06 | — | $6,271.90 | ▼ $9,994.46 (-5.54) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+5.9; leftover $1250.98 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 350 | $3.57 | $4.51 | — | $5,017.89 | ▼ $9,989.95 (-10.05) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $1250.98 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 407 | $3.07 | $5.25 | — | $3,763.15 | ▼ $9,984.70 (-15.30) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $1250.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $2,788.53 | ▼ $9,982.70 (-17.30) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1250.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 42 | $29.15 | $2.12 | — | $1,562.12 | ▼ $9,980.59 (-19.41) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1250.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CCOI` | 122 | $10.22 | $2.36 | — | $312.92 | ▼ $9,978.23 (-21.77) | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $1250.98 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ULTA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `UGI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BJ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `INSP` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMEA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `NPWR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `PUSA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ALVO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ALIT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-26 | `FLNC` | no_price | no 09:30 open |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `AVEX` | no_price | no 09:30 open |
| 2026-08-26 | `BZ` | no_price | no 09:30 open |
| 2026-08-27 | `ASML` | cash | leftover split 1287.06 < 1 share @ 1746.33 |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SRPT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ZJYL` | hard_red | hard-red S=-5.85 sit; no new buys |
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

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `OSCR` | 40 | 2026-09-04 @ $30.65 | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=-2.2; leftover $1250.98 |
| `BVS` | 86 | 2026-09-04 @ $14.50 | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1250.98 |
| `GPRO` | 702 | 2026-09-04 @ $1.78 | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+5.9; leftover $1250.98 |
| `EOSE` | 350 | 2026-09-04 @ $3.57 | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $1250.98 |
| `SLBT` | 407 | 2026-09-04 @ $3.07 | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $1250.98 |
| `DELL` | 2 | 2026-09-04 @ $486.31 | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1250.98 |
| `MLYS` | 42 | 2026-09-04 @ $29.15 | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1250.98 |
| `CCOI` | 122 | 2026-09-04 @ $10.22 | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $1250.98 |
