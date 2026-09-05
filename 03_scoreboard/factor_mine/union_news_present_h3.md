# Factor mine action — `union_news_present_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ news_present, no 🚨

Cash book **+2.81%** ($10,281) · signal-only (no cash/fees) was +23.08%. Starts YES **16/17**. Fills 89 · skips 144 · realized $+29.95.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news_present=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $74.42.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-14 | +5.50 | $10,000.00 | — | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | — | $560.20 | $9,491.27 | $10,051.46 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833 | BUY TLN x3 @ 359.83; BUY VST x8 @ 146.90; BUY NRG x10 @ 120.00; BUY DAVE x3 @ 330.91; BUY SLG x21 @ 57.61; BUY MARA x138 @ 9.01; BUY LDI x1334 @ 0.94; BUY BTBT x833 @ 1.50 |
| 2026-08-17 | +2.25 | $560.20 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833 | DVN, TMC, TGB, DNN, NB | — | $240.19 | $9,818.69 | $10,058.88 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833, DVN×1, TMC×17, TGB×8, DNN×21, NB×13 | BUY DVN x1 @ 46.18; BUY TMC x17 @ 4.05; BUY TGB x8 @ 8.46; BUY DNN x21 @ 3.24; BUY NB x13 @ 5.07 |
| 2026-08-18 | -6.20 | $240.19 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833, DVN×1, TMC×17, TGB×8, DNN×21, NB×13 | — | — | $240.19 | $9,321.09 | $9,561.28 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833, DVN×1, TMC×17, TGB×8, DNN×21, NB×13 | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $240.19 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833, DVN×1, TMC×17, TGB×8, DNN×21, NB×13 | — | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | $9,247.48 | $309.04 | $9,556.52 | DVN×1, TMC×17, TGB×8, DNN×21, NB×13 | SELL TLN (dropped from list after 3 sess (min 3)); SELL VST (dropped from list after 3 sess (min 3)); SELL NRG (dropped from list after 3 sess (min 3)); SELL DAVE (dropped from list after 3 sess (min 3)); SELL SLG (dropped from list after 3 sess (min 3)); SELL MARA (dropped from list after 3 sess (min 3)); SELL LDI (dropped from list after 3 sess (min 3)); SELL BTBT (dropped from list after 3 sess (min 3)); hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $9,247.48 | DVN×1, TMC×17, TGB×8, DNN×21, NB×13 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | DVN, TMC, TGB, DNN, NB | $74.01 | $9,679.60 | $9,753.61 | AG×58, BHP×13, CDE×57, HDSN×206, IAG×60, KGC×40, NFGC×682, WPM×8 | SELL DVN (dropped from list after 3 sess (min 3)); SELL TMC (dropped from list after 3 sess (min 3)); SELL TGB (dropped from list after 3 sess (min 3)); SELL DNN (dropped from list after 3 sess (min 3)); SELL NB (dropped from list after 3 sess (min 3)); BUY AG x58 @ 20.55; BUY BHP x13 @ 91.01; BUY CDE x57 @ 20.65; BUY HDSN x206 @ 5.77; BUY IAG x60 @ 19.63; BUY KGC x40 @ 29.63; BUY NFGC x682 @ 1.75; BUY WPM x8 @ 144.54 |
| 2026-08-21 | +3.25 | $74.01 | AG×58, BHP×13, CDE×57, HDSN×206, IAG×60, KGC×40, NFGC×682, WPM×8 | AUTL, CRDL, CYPH | — | $49.36 | $9,960.21 | $10,009.57 | AG×58, BHP×13, CDE×57, HDSN×206, IAG×60, KGC×40, NFGC×682, WPM×8, AUTL×3, CRDL×4, CYPH×7 | BUY AUTL x3 @ 2.47; BUY CRDL x4 @ 1.93; BUY CYPH x7 @ 1.32 |
| 2026-08-24 | -5.17 | $49.36 | AG×58, BHP×13, CDE×57, HDSN×206, IAG×60, KGC×40, NFGC×682, WPM×8, AUTL×3, CRDL×4, CYPH×7 | — | — | $49.36 | $9,931.01 | $9,980.37 | AG×58, BHP×13, CDE×57, HDSN×206, IAG×60, KGC×40, NFGC×682, WPM×8, AUTL×3, CRDL×4, CYPH×7 | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $49.36 | AG×58, BHP×13, CDE×57, HDSN×206, IAG×60, KGC×40, NFGC×682, WPM×8, AUTL×3, CRDL×4, CYPH×7 | MOS, OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $396.55 | $9,595.19 | $9,991.74 | AUTL×3, CRDL×4, CYPH×7, MOS×52, OCUL×114, INSP×20, CRMD×150, RZLT×238, HCA×2, BMEA×771, NPWR×624 | SELL AG (dropped from list after 3 sess (min 3)); SELL BHP (dropped from list after 3 sess (min 3)); SELL CDE (dropped from list after 3 sess (min 3)); SELL HDSN (dropped from list after 3 sess (min 3)); SELL IAG (dropped from list after 3 sess (min 3)); SELL KGC (dropped from list after 3 sess (min 3)); SELL NFGC (dropped from list after 3 sess (min 3)); SELL WPM (dropped from list after 3 sess (min 3)); BUY MOS x52 @ 24.00; BUY OCUL x114 @ 10.92; BUY INSP x20 @ 61.47; BUY CRMD x150 @ 8.28; BUY RZLT x238 @ 5.23; BUY HCA x2 @ 429.24; BUY BMEA x771 @ 1.62; BUY NPWR x624 @ 2.00 |
| 2026-08-26 | +2.02 | $396.55 | AUTL×3, CRDL×4, CYPH×7, MOS×52, OCUL×114, INSP×20, CRMD×150, RZLT×238, HCA×2, BMEA×771, NPWR×624 | — | — | $396.55 | $9,590.98 | $9,987.53 | AUTL×3, CRDL×4, CYPH×7, MOS×52, OCUL×114, INSP×20, CRMD×150, RZLT×238, HCA×2, BMEA×771, NPWR×624 | hold AUTL,CRDL,CYPH,MOS,OCUL,INSP,CRMD,RZLT,HCA,BMEA,NPWR |
| 2026-08-27 | — | $396.55 | AUTL×3, CRDL×4, CYPH×7, MOS×52, OCUL×114, INSP×20, CRMD×150, RZLT×238, HCA×2, BMEA×771, NPWR×624 | RRC | AUTL, CRDL, CYPH | $381.61 | $9,521.84 | $9,903.45 | MOS×52, OCUL×114, INSP×20, CRMD×150, RZLT×238, HCA×2, BMEA×771, NPWR×624, RRC×1 | SELL AUTL (dropped from list after 4 sess (min 3)); SELL CRDL (dropped from list after 4 sess (min 3)); SELL CYPH (dropped from list after 4 sess (min 3)); BUY RRC x1 @ 40.72 |
| 2026-08-28 | +0.75 | $381.61 | MOS×52, OCUL×114, INSP×20, CRMD×150, RZLT×238, HCA×2, BMEA×771, NPWR×624, RRC×1 | CRK, SLI, ANF, BHVN, BZ, CAPR | OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | $152.61 | $9,803.01 | $9,955.62 | MOS×52, RRC×1, CRK×99, SLI×552, ANF×9, BHVN×84, BZ×77, CAPR×156 | SELL OCUL (dropped from list after 3 sess (min 3)); SELL INSP (dropped from list after 3 sess (min 3)); SELL CRMD (dropped from list after 3 sess (min 3)); SELL RZLT (dropped from list after 3 sess (min 3)); SELL HCA (dropped from list after 3 sess (min 3)); SELL BMEA (dropped from list after 3 sess (min 3)); SELL NPWR (dropped from list after 3 sess (min 3)); BUY CRK x99 @ 14.42; BUY SLI x552 @ 2.60; BUY ANF x9 @ 144.70; BUY BHVN x84 @ 16.95; BUY BZ x77 @ 18.50; BUY CAPR x156 @ 9.19 |
| 2026-08-31 | -5.85 | $152.61 | MOS×52, RRC×1, CRK×99, SLI×552, ANF×9, BHVN×84, BZ×77, CAPR×156 | — | MOS | $1,385.45 | $8,339.37 | $9,724.82 | RRC×1, CRK×99, SLI×552, ANF×9, BHVN×84, BZ×77, CAPR×156 | SELL MOS (dropped from list after 4 sess (min 3)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $1,385.45 | RRC×1, CRK×99, SLI×552, ANF×9, BHVN×84, BZ×77, CAPR×156 | — | RRC | $1,426.33 | $8,462.03 | $9,888.36 | CRK×99, SLI×552, ANF×9, BHVN×84, BZ×77, CAPR×156 | SELL RRC (dropped from list after 3 sess (min 3)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $1,426.33 | CRK×99, SLI×552, ANF×9, BHVN×84, BZ×77, CAPR×156 | — | CRK, SLI, ANF, BHVN, BZ, CAPR | $10,029.97 | $0.00 | $10,029.97 | — | SELL CRK (dropped from list after 3 sess (min 3)); SELL SLI (dropped from list after 3 sess (min 3)); SELL ANF (dropped from list after 3 sess (min 3)); SELL BHVN (dropped from list after 3 sess (min 3)); SELL BZ (dropped from list after 3 sess (min 3)); SELL CAPR (dropped from list after 3 sess (min 3)); hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $10,029.97 | — | ATRC, HRMY, CABA, VSTM, RVTY, GPRO, FRVO, CRK | — | $137.72 | $10,631.49 | $10,769.21 | ATRC×25, HRMY×30, CABA×383, VSTM×162, RVTY×9, GPRO×1027, FRVO×68, CRK×79 | BUY ATRC x25 @ 49.76; BUY HRMY x30 @ 41.31; BUY CABA x383 @ 3.27; BUY VSTM x162 @ 7.70; BUY RVTY x9 @ 125.94; BUY GPRO x1027 @ 1.22; BUY FRVO x68 @ 18.40; BUY CRK x79 @ 15.70 |
| 2026-09-04 | — | $137.72 | ATRC×25, HRMY×30, CABA×383, VSTM×162, RVTY×9, GPRO×1027, FRVO×68, CRK×79 | NVAX, BVS, BAK | — | $74.42 | $10,206.23 | $10,280.65 | ATRC×25, HRMY×30, CABA×383, VSTM×162, RVTY×9, GPRO×1027, FRVO×68, CRK×79, NVAX×2, BVS×1, BAK×14 | BUY NVAX x2 @ 10.41; BUY BVS x1 @ 14.50; BUY BAK x14 @ 1.95 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $8,918.51 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+5.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $7,741.30 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+3.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $6,539.28 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $5,544.55 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 21 | $57.61 | $2.05 | — | $4,332.68 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+5.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 138 | $9.01 | $2.40 | — | $3,086.90 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1334 | $0.94 | $16.50 | — | $1,820.44 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $560.20 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 1 | $46.18 | $0.46 | — | $513.55 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+6.7; leftover $70.02 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 17 | $4.05 | $0.74 | — | $443.96 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $70.02 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 8 | $8.46 | $0.70 | — | $375.58 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+0.4; leftover $70.02 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 21 | $3.24 | $0.74 | — | $306.80 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ⚪; ret5=+0.3; leftover $70.02 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NB` | 13 | $5.07 | $0.70 | — | $240.19 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=-4.7; leftover $70.02 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `TLN` | 3 | $321.00 | $2.02 | $-120.51 | $1,201.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `VST` | 8 | $140.74 | $2.03 | $-53.33 | $2,325.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `NRG` | 10 | $116.20 | $2.04 | $-42.06 | $3,485.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `DAVE` | 3 | $334.00 | $2.02 | $+5.25 | $4,485.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `SLG` | 21 | $57.50 | $2.07 | $-6.44 | $5,690.42 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `MARA` | 138 | $8.91 | $2.44 | $-18.64 | $6,917.57 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 1334 | $0.88 | $15.97 | $-108.51 | $8,075.51 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 833 | $1.42 | $10.89 | $-88.28 | $9,247.48 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DVN` | 1 | $49.02 | $0.51 | $+1.86 | $9,295.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 17 | $3.92 | $0.74 | $-3.69 | $9,361.89 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGB` | 8 | $8.35 | $0.71 | $-2.29 | $9,427.98 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 21 | $3.20 | $0.76 | $-2.34 | $9,494.42 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `NB` | 13 | $4.45 | $0.64 | $-9.40 | $9,551.64 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 58 | $20.55 | $2.16 | — | $8,357.57 | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1193.95 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,172.41 | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1193.95 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 57 | $20.65 | $2.16 | — | $5,993.20 | union ∩ news_present, no 🚨; gate news_present=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1193.95 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 206 | $5.77 | $2.66 | — | $4,801.92 | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1193.95 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 60 | $19.63 | $2.17 | — | $3,621.95 | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1193.95 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 40 | $29.63 | $2.11 | — | $2,434.64 | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1193.95 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 682 | $1.75 | $8.80 | — | $1,232.35 | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1193.95 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $74.01 | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1193.95 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 3 | $2.47 | $0.08 | — | $66.52 | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $9.25 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 4 | $1.93 | $0.09 | — | $58.71 | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $9.25 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 7 | $1.32 | $0.11 | — | $49.36 | union ∩ news_present, no 🚨; gate news_present=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $9.25 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 58 | $20.73 | $2.18 | $+6.09 | $1,249.51 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.95 | $2.05 | $+60.14 | $2,494.81 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 57 | $20.85 | $2.18 | $+7.06 | $3,681.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 206 | $5.53 | $2.70 | $-54.80 | $4,817.56 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 60 | $21.63 | $2.19 | $+115.64 | $6,113.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 40 | $32.76 | $2.13 | $+120.96 | $7,421.44 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 682 | $1.91 | $8.92 | $+91.40 | $8,715.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $160.00 | $2.03 | $+119.63 | $9,993.10 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 52 | $24.00 | $2.15 | — | $8,742.96 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ⚪; ret5=+13.0; leftover $1249.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 114 | $10.92 | $2.33 | — | $7,495.75 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+10.4; leftover $1249.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 20 | $61.47 | $2.05 | — | $6,264.30 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+9.2; leftover $1249.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 150 | $8.28 | $2.44 | — | $5,019.86 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1249.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 238 | $5.23 | $3.07 | — | $3,772.05 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+10.7; leftover $1249.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 2 | $429.24 | $2.00 | — | $2,911.57 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+6.1; leftover $1249.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 771 | $1.62 | $9.95 | — | $1,652.60 | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1249.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 624 | $2.00 | $8.05 | — | $396.55 | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $1249.14 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `AUTL` | 3 | $2.41 | $0.10 | $-0.36 | $403.68 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 4 | $2.03 | $0.11 | $+0.20 | $411.69 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 7 | $1.60 | $0.15 | $+1.69 | $422.74 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 1 | $40.72 | $0.41 | — | $381.61 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+1.8; leftover $70.46 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 114 | $10.63 | $2.36 | $-37.75 | $1,591.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 20 | $62.10 | $2.07 | $+8.48 | $2,831.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 150 | $8.49 | $2.48 | $+26.58 | $4,102.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 238 | $5.07 | $3.12 | $-44.27 | $5,305.56 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 2 | $424.61 | $2.02 | $-13.27 | $6,152.77 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 771 | $1.74 | $10.08 | $+72.49 | $7,484.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `NPWR` | 624 | $1.83 | $8.16 | $-122.29 | $8,617.98 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `CRK` | 99 | $14.42 | $2.29 | — | $7,188.11 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+1.1; leftover $1436.33 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `SLI` | 552 | $2.60 | $7.12 | — | $5,745.79 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+4.2; leftover $1436.33 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $144.70 | $2.02 | — | $4,441.47 | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1436.33 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 84 | $16.95 | $2.24 | — | $3,015.43 | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1436.33 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 77 | $18.50 | $2.22 | — | $1,588.71 | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1436.33 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 156 | $9.19 | $2.46 | — | $152.61 | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1436.33 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 52 | $23.75 | $2.17 | $-17.31 | $1,385.45 | dropped from list after 4 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `RRC` | 1 | $41.32 | $0.44 | $-0.25 | $1,426.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CRK` | 99 | $15.82 | $2.32 | $+134.00 | $2,990.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SLI` | 552 | $2.67 | $7.22 | $+24.29 | $4,456.81 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 9 | $142.00 | $2.04 | $-28.35 | $5,732.77 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BHVN` | 84 | $15.39 | $2.27 | $-135.55 | $7,023.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BZ` | 77 | $17.29 | $2.24 | $-97.64 | $8,352.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CAPR` | 156 | $10.77 | $2.50 | $+241.52 | $10,029.97 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 25 | $49.76 | $2.06 | — | $8,783.91 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1253.75 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 30 | $41.31 | $2.08 | — | $7,542.53 | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1253.75 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 383 | $3.27 | $4.94 | — | $6,285.18 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1253.75 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 162 | $7.70 | $2.48 | — | $5,035.30 | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1253.75 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $3,899.83 | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1253.75 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1027 | $1.22 | $13.25 | — | $2,633.64 | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1253.75 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 68 | $18.40 | $2.19 | — | $1,380.24 | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1253.75 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 79 | $15.70 | $2.23 | — | $137.72 | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1253.75 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 2 | $10.41 | $0.21 | — | $116.68 | union ∩ news_present, no 🚨; gate news_present=True; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $27.54 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 1 | $14.50 | $0.15 | — | $102.03 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+0.8; leftover $27.54 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 14 | $1.95 | $0.32 | — | $74.42 | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $27.54 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

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
| 2026-08-17 | `ELF` | cash | leftover split 70.02 < 1 share @ 90.54 |
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
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `NB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `NB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 9.25 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 9.25 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 9.25 < 1 share @ 216.30 |
| 2026-08-21 | `ARCT` | cash | leftover split 9.25 < 1 share @ 11.13 |
| 2026-08-21 | `CRSP` | cash | leftover split 9.25 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AUTL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CRDL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-27 | `MOS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ACMR` | cash | leftover split 70.46 < 1 share @ 80.97 |
| 2026-08-27 | `MU` | cash | leftover split 70.46 < 1 share @ 925.74 |
| 2026-08-27 | `ASML` | cash | leftover split 70.46 < 1 share @ 1746.33 |
| 2026-08-27 | `LRCX` | cash | leftover split 70.46 < 1 share @ 314.61 |
| 2026-08-27 | `NVDA` | cash | leftover split 70.46 < 1 share @ 212.64 |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `FRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ASND` | cash | leftover split 27.54 < 1 share @ 266.94 |
| 2026-09-04 | `OSCR` | cash | leftover split 27.54 < 1 share @ 30.65 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 25 | 2026-09-03 @ $49.76 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1253.75 |
| `HRMY` | 30 | 2026-09-03 @ $41.31 | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1253.75 |
| `CABA` | 383 | 2026-09-03 @ $3.27 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1253.75 |
| `VSTM` | 162 | 2026-09-03 @ $7.70 | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1253.75 |
| `RVTY` | 9 | 2026-09-03 @ $125.94 | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1253.75 |
| `GPRO` | 1027 | 2026-09-03 @ $1.22 | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1253.75 |
| `FRVO` | 68 | 2026-09-03 @ $18.40 | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1253.75 |
| `CRK` | 79 | 2026-09-03 @ $15.70 | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1253.75 |
| `NVAX` | 2 | 2026-09-04 @ $10.41 | union ∩ news_present, no 🚨; gate news_present=True; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $27.54 |
| `BVS` | 1 | 2026-09-04 @ $14.50 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+0.8; leftover $27.54 |
| `BAK` | 14 | 2026-09-04 @ $1.95 | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $27.54 |
