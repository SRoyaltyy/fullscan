# Factor mine action — `union_news_present_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ news_present, no 🚨

Cash book **+13.17%** ($11,317) · signal-only (no cash/fees) was +15.48%. Starts YES **16/17**. Fills 124 · skips 52 · realized $+947.04.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news_present=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $65.28.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-14 | +5.50 | $10,000.00 | — | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | — | $560.20 | $9,491.27 | $10,051.46 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833 | BUY TLN x3 @ 359.83; BUY VST x8 @ 146.90; BUY NRG x10 @ 120.00; BUY DAVE x3 @ 330.91; BUY SLG x21 @ 57.61; BUY MARA x138 @ 9.01; BUY LDI x1334 @ 0.94; BUY BTBT x833 @ 1.50 |
| 2026-08-17 | +2.25 | $560.20 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833 | DVN, EOG, FANG, TMC, TGB, ELF, DNN, NB | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | $217.13 | $9,777.63 | $9,994.76 | DVN×27, EOG×8, FANG×6, TMC×309, TGB×147, ELF×13, DNN×386, NB×246 | SELL TLN (dropped from list after 1 sess (min 1)); SELL VST (dropped from list after 1 sess (min 1)); SELL NRG (dropped from list after 1 sess (min 1)); SELL DAVE (dropped from list after 1 sess (min 1)); SELL SLG (dropped from list after 1 sess (min 1)); SELL MARA (dropped from list after 1 sess (min 1)); SELL LDI (dropped from list after 1 sess (min 1)); SELL BTBT (dropped from list after 1 sess (min 1)); BUY DVN x27 @ 46.18; BUY EOG x8 @ 142.77; BUY FANG x6 @ 202.70; BUY TMC x309 @ 4.05; BUY TGB x147 @ 8.46; BUY ELF x13 @ 90.54; BUY DNN x386 @ 3.24; BUY NB x246 @ 5.07 |
| 2026-08-18 | -6.20 | $217.13 | DVN×27, EOG×8, FANG×6, TMC×309, TGB×147, ELF×13, DNN×386, NB×246 | — | DVN, EOG, FANG, TMC, TGB, ELF, DNN, NB | $9,895.91 | $0.00 | $9,895.91 | — | SELL DVN (dropped from list after 1 sess (min 1)); SELL EOG (dropped from list after 1 sess (min 1)); SELL FANG (dropped from list after 1 sess (min 1)); SELL TMC (dropped from list after 1 sess (min 1)); SELL TGB (dropped from list after 1 sess (min 1)); SELL ELF (dropped from list after 1 sess (min 1)); SELL DNN (dropped from list after 1 sess (min 1)); SELL NB (dropped from list after 1 sess (min 1)); hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $9,895.91 | — | — | — | $9,895.91 | $0.00 | $9,895.91 | — | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $9,895.91 | — | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $158.77 | $9,943.69 | $10,102.46 | AG×60, BHP×13, CDE×59, HDSN×214, IAG×63, KGC×41, NFGC×706, WPM×8 | BUY AG x60 @ 20.55; BUY BHP x13 @ 91.01; BUY CDE x59 @ 20.65; BUY HDSN x214 @ 5.77; BUY IAG x63 @ 19.63; BUY KGC x41 @ 29.63; BUY NFGC x706 @ 1.75; BUY WPM x8 @ 144.54 |
| 2026-08-21 | +3.25 | $158.77 | AG×60, BHP×13, CDE×59, HDSN×214, IAG×63, KGC×41, NFGC×706, WPM×8 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $318.05 | $10,245.90 | $10,563.95 | AU×10, AUPH×75, AEM×5, ARCT×116, AUTL×523, CRDL×669, CRSP×21, CYPH×979 | SELL AG (dropped from list after 1 sess (min 1)); SELL BHP (dropped from list after 1 sess (min 1)); SELL CDE (dropped from list after 1 sess (min 1)); SELL HDSN (dropped from list after 1 sess (min 1)); SELL IAG (dropped from list after 1 sess (min 1)); SELL KGC (dropped from list after 1 sess (min 1)); SELL NFGC (dropped from list after 1 sess (min 1)); SELL WPM (dropped from list after 1 sess (min 1)); BUY AU x10 @ 119.43; BUY AUPH x75 @ 17.20; BUY AEM x5 @ 216.30; BUY ARCT x116 @ 11.13; BUY AUTL x523 @ 2.47; BUY CRDL x669 @ 1.93; BUY CRSP x21 @ 59.72; BUY CYPH x979 @ 1.32 |
| 2026-08-24 | -5.17 | $318.05 | AU×10, AUPH×75, AEM×5, ARCT×116, AUTL×523, CRDL×669, CRSP×21, CYPH×979 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $10,863.68 | $0.00 | $10,863.68 | — | SELL AU (dropped from list after 1 sess (min 1)); SELL AUPH (dropped from list after 1 sess (min 1)); SELL AEM (dropped from list after 1 sess (min 1)); SELL ARCT (dropped from list after 1 sess (min 1)); SELL AUTL (dropped from list after 1 sess (min 1)); SELL CRDL (dropped from list after 1 sess (min 1)); SELL CRSP (dropped from list after 1 sess (min 1)); SELL CYPH (dropped from list after 1 sess (min 1)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $10,863.68 | — | MOS, OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | — | $65.54 | $10,768.69 | $10,834.23 | MOS×56, OCUL×124, INSP×22, CRMD×164, RZLT×259, HCA×3, BMEA×838, NPWR×678 | BUY MOS x56 @ 24.00; BUY OCUL x124 @ 10.92; BUY INSP x22 @ 61.47; BUY CRMD x164 @ 8.28; BUY RZLT x259 @ 5.23; BUY HCA x3 @ 429.24; BUY BMEA x838 @ 1.62; BUY NPWR x678 @ 2.00 |
| 2026-08-26 | +2.02 | $65.54 | MOS×56, OCUL×124, INSP×22, CRMD×164, RZLT×259, HCA×3, BMEA×838, NPWR×678 | — | — | $65.54 | $10,764.19 | $10,829.73 | MOS×56, OCUL×124, INSP×22, CRMD×164, RZLT×259, HCA×3, BMEA×838, NPWR×678 | hold MOS,OCUL,INSP,CRMD,RZLT,HCA,BMEA,NPWR |
| 2026-08-27 | — | $65.54 | MOS×56, OCUL×124, INSP×22, CRMD×164, RZLT×259, HCA×3, BMEA×838, NPWR×678 | RRC, ACMR, MU, ASML, LRCX, NVDA | MOS, OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | $1,315.75 | $9,494.34 | $10,810.09 | RRC×44, ACMR×22, MU×1, ASML×1, LRCX×5, NVDA×8 | SELL MOS (dropped from list after 2 sess (min 1)); SELL OCUL (dropped from list after 2 sess (min 1)); SELL INSP (dropped from list after 2 sess (min 1)); SELL CRMD (dropped from list after 2 sess (min 1)); SELL RZLT (dropped from list after 2 sess (min 1)); SELL HCA (dropped from list after 2 sess (min 1)); SELL BMEA (dropped from list after 2 sess (min 1)); SELL NPWR (dropped from list after 2 sess (min 1)); BUY RRC x44 @ 40.72; BUY ACMR x22 @ 80.97; BUY MU x1 @ 925.74; BUY ASML x1 @ 1746.33; BUY LRCX x5 @ 314.61; BUY NVDA x8 @ 212.64 |
| 2026-08-28 | +0.75 | $1,315.75 | RRC×44, ACMR×22, MU×1, ASML×1, LRCX×5, NVDA×8 | CRK, MOS, SLI, ANF, BHVN, BZ, CAPR | ACMR, MU, ASML, LRCX, NVDA | $44.26 | $11,020.33 | $11,064.59 | RRC×44, CRK×91, MOS×54, SLI×505, ANF×9, BHVN×77, BZ×70, CAPR×142 | SELL ACMR (dropped from list after 1 sess (min 1)); SELL MU (dropped from list after 1 sess (min 1)); SELL ASML (dropped from list after 1 sess (min 1)); SELL LRCX (dropped from list after 1 sess (min 1)); SELL NVDA (dropped from list after 1 sess (min 1)); BUY CRK x91 @ 14.42; BUY MOS x54 @ 24.00; BUY SLI x505 @ 2.60; BUY ANF x9 @ 144.70; BUY BHVN x77 @ 16.95; BUY BZ x70 @ 18.50; BUY CAPR x142 @ 9.19 |
| 2026-08-31 | -5.85 | $44.26 | RRC×44, CRK×91, MOS×54, SLI×505, ANF×9, BHVN×77, BZ×70, CAPR×142 | — | RRC, CRK, MOS, SLI, ANF, BHVN, BZ, CAPR | $10,825.63 | $0.00 | $10,825.63 | — | SELL RRC (dropped from list after 2 sess (min 1)); SELL CRK (dropped from list after 1 sess (min 1)); SELL MOS (dropped from list after 1 sess (min 1)); SELL SLI (dropped from list after 1 sess (min 1)); SELL ANF (dropped from list after 1 sess (min 1)); SELL BHVN (dropped from list after 1 sess (min 1)); SELL BZ (dropped from list after 1 sess (min 1)); SELL CAPR (dropped from list after 1 sess (min 1)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $10,825.63 | — | — | — | $10,825.63 | $0.00 | $10,825.63 | — | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $10,825.63 | — | — | — | $10,825.63 | $0.00 | $10,825.63 | — | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $10,825.63 | — | ATRC, HRMY, CABA, VSTM, RVTY, GPRO, FRVO, CRK | — | $123.62 | $11,501.95 | $11,625.57 | ATRC×27, HRMY×32, CABA×413, VSTM×175, RVTY×10, GPRO×1109, FRVO×73, CRK×86 | BUY ATRC x27 @ 49.76; BUY HRMY x32 @ 41.31; BUY CABA x413 @ 3.27; BUY VSTM x175 @ 7.70; BUY RVTY x10 @ 125.94; BUY GPRO x1109 @ 1.22; BUY FRVO x73 @ 18.40; BUY CRK x86 @ 15.70 |
| 2026-09-04 | — | $123.62 | ATRC×27, HRMY×32, CABA×413, VSTM×175, RVTY×10, GPRO×1109, FRVO×73, CRK×86 | ASND, OSCR, NVAX, BVS, BAK | HRMY, VSTM, RVTY, FRVO, CRK | $65.28 | $11,251.75 | $11,317.03 | ATRC×27, CABA×413, GPRO×1109, ASND×5, OSCR×44, NVAX×132, BVS×94, BAK×705 | SELL HRMY (dropped from list after 1 sess (min 1)); SELL VSTM (dropped from list after 1 sess (min 1)); SELL RVTY (dropped from list after 1 sess (min 1)); SELL FRVO (dropped from list after 1 sess (min 1)); SELL CRK (dropped from list after 1 sess (min 1)); BUY ASND x5 @ 266.94; BUY OSCR x44 @ 30.65; BUY NVAX x132 @ 10.41; BUY BVS x94 @ 14.50; BUY BAK x705 @ 1.95 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $8,918.51 | ▼ $9,998.00 (-2.00) | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+5.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $7,741.30 | ▼ $9,995.99 (-4.01) | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+3.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $6,539.28 | ▼ $9,993.97 (-6.03) | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $5,544.55 | ▼ $9,991.97 (-8.03) | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 21 | $57.61 | $2.05 | — | $4,332.68 | ▼ $9,989.91 (-10.09) | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+5.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 138 | $9.01 | $2.40 | — | $3,086.90 | ▼ $9,987.51 (-12.49) | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1334 | $0.94 | $16.50 | — | $1,820.44 | ▼ $9,971.01 (-28.99) | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $560.20 | ▼ $9,960.26 (-39.74) | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 3 | $367.88 | $2.02 | $+20.13 | $1,661.82 | ▲ $10,052.82 (+52.82) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 8 | $149.37 | $2.03 | $+15.71 | $2,854.74 | ▲ $10,050.79 (+50.79) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $4,126.70 | ▲ $10,048.75 (+48.75) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 3 | $336.94 | $2.02 | $+14.07 | $5,135.50 | ▲ $10,046.73 (+46.73) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 21 | $55.37 | $2.07 | $-51.17 | $6,296.20 | ▲ $10,044.66 (+44.66) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 138 | $9.22 | $2.44 | $+24.14 | $7,566.12 | ▲ $10,042.22 (+42.22) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 1334 | $0.91 | $16.33 | $-72.85 | $8,759.73 | ▲ $10,025.89 (+25.89) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 833 | $1.52 | $10.89 | $-4.98 | $10,014.99 | ▲ $10,014.99 (+14.99) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 27 | $46.18 | $2.07 | — | $8,766.06 | ▲ $10,012.92 (+12.92) | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+6.7; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $7,621.89 | ▲ $10,010.91 (+10.91) | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+5.8; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $6,403.68 | ▲ $10,008.90 (+8.90) | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+8.3; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 309 | $4.05 | $3.99 | — | $5,148.25 | ▲ $10,004.92 (+4.92) | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 147 | $8.46 | $2.43 | — | $3,902.19 | ▲ $10,002.48 (+2.48) | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 13 | $90.54 | $2.03 | — | $2,723.15 | ▲ $10,000.46 (+0.46) | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=-7.2; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 386 | $3.24 | $4.98 | — | $1,467.53 | ▼ $9,995.48 (-4.52) | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ⚪; ret5=+0.3; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NB` | 246 | $5.07 | $3.17 | — | $217.13 | ▼ $9,992.30 (-7.70) | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=-4.7; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 27 | $48.00 | $2.09 | $+44.98 | $1,511.04 | ▼ $9,916.81 (-83.19) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 8 | $148.04 | $2.03 | $+38.11 | $2,693.33 | ▼ $9,914.78 (-85.22) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $3,944.88 | ▼ $9,912.75 (-87.25) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 309 | $3.72 | $4.05 | $-110.00 | $5,090.31 | ▼ $9,908.70 (-91.30) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 147 | $8.55 | $2.47 | $+8.33 | $6,344.70 | ▼ $9,906.24 (-93.76) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 13 | $93.44 | $2.05 | $+33.62 | $7,557.37 | ▼ $9,904.19 (-95.81) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 386 | $3.11 | $5.05 | $-60.21 | $8,752.77 | ▼ $9,899.13 (-100.87) | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `NB` | 246 | $4.66 | $3.22 | $-107.26 | $9,895.91 | ▼ $9,895.91 (-104.09) | dropped from list after 1 sess (min 1) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 60 | $20.55 | $2.17 | — | $8,660.74 | ▼ $9,893.74 (-106.26) | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1236.99 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,475.58 | ▼ $9,891.71 (-108.29) | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1236.99 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 59 | $20.65 | $2.17 | — | $6,255.06 | ▼ $9,889.54 (-110.46) | union ∩ news_present, no 🚨; gate news_present=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1236.99 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 214 | $5.77 | $2.76 | — | $5,017.52 | ▼ $9,886.78 (-113.22) | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1236.99 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 63 | $19.63 | $2.18 | — | $3,778.65 | ▼ $9,884.60 (-115.40) | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1236.99 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 41 | $29.63 | $2.11 | — | $2,561.71 | ▼ $9,882.49 (-117.51) | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1236.99 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 706 | $1.75 | $9.11 | — | $1,317.10 | ▼ $9,873.38 (-126.62) | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1236.99 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $158.77 | ▼ $9,871.37 (-128.63) | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1236.99 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 60 | $21.90 | $2.19 | $+76.64 | $1,470.58 | ▲ $10,365.59 (+365.59) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,712.89 | ▲ $10,363.54 (+363.54) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 59 | $21.75 | $2.19 | $+60.55 | $3,993.95 | ▲ $10,361.35 (+361.35) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 214 | $5.67 | $2.81 | $-26.97 | $5,204.53 | ▲ $10,358.55 (+358.55) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 63 | $21.17 | $2.20 | $+92.64 | $6,536.04 | ▲ $10,356.35 (+356.35) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 41 | $32.17 | $2.13 | $+99.89 | $7,852.87 | ▲ $10,354.21 (+354.21) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 706 | $1.79 | $9.23 | $+9.90 | $9,107.38 | ▲ $10,344.98 (+344.98) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $10,342.94 | ▲ $10,342.94 (+342.94) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $9,146.62 | ▲ $10,340.92 (+340.92) | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $1292.87 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 75 | $17.20 | $2.21 | — | $7,854.41 | ▲ $10,338.71 (+338.71) | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1292.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $6,770.90 | ▲ $10,336.70 (+336.70) | union ∩ news_present, no 🚨; gate news_present=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1292.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 116 | $11.13 | $2.34 | — | $5,477.49 | ▲ $10,334.37 (+334.37) | union ∩ news_present, no 🚨; gate news_present=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1292.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 523 | $2.47 | $6.75 | — | $4,178.93 | ▲ $10,327.62 (+327.62) | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1292.87 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 669 | $1.93 | $8.63 | — | $2,879.13 | ▲ $10,318.99 (+318.99) | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1292.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $1,622.96 | ▲ $10,316.94 (+316.94) | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1292.87 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 979 | $1.32 | $12.63 | — | $318.05 | ▲ $10,304.31 (+304.31) | union ∩ news_present, no 🚨; gate news_present=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1292.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.50 | $2.04 | $+6.64 | $1,521.01 | ▲ $10,900.79 (+900.79) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 75 | $16.60 | $2.24 | $-49.45 | $2,763.77 | ▲ $10,898.55 (+898.55) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,846.89 | ▲ $10,896.52 (+896.52) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 116 | $13.26 | $2.37 | $+242.37 | $5,382.69 | ▲ $10,894.16 (+894.16) | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 523 | $2.36 | $6.84 | $-71.12 | $6,610.12 | ▲ $10,887.31 (+887.31) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 669 | $1.87 | $8.75 | $-57.52 | $7,852.40 | ▲ $10,878.56 (+878.56) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 21 | $58.79 | $2.07 | $-23.66 | $9,084.92 | ▲ $10,876.49 (+876.49) | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 979 | $1.83 | $12.81 | $+473.86 | $10,863.68 | ▲ $10,863.68 (+863.68) | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 56 | $24.00 | $2.16 | — | $9,517.52 | ▲ $10,861.52 (+861.52) | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ⚪; ret5=+13.0; leftover $1357.96 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 124 | $10.92 | $2.36 | — | $8,161.08 | ▲ $10,859.16 (+859.16) | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+10.4; leftover $1357.96 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 22 | $61.47 | $2.06 | — | $6,806.69 | ▲ $10,857.11 (+857.11) | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+9.2; leftover $1357.96 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 164 | $8.28 | $2.48 | — | $5,446.28 | ▲ $10,854.62 (+854.62) | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1357.96 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 259 | $5.23 | $3.34 | — | $4,088.37 | ▲ $10,851.28 (+851.28) | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+10.7; leftover $1357.96 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $429.24 | $2.00 | — | $2,798.65 | ▲ $10,849.28 (+849.28) | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+6.1; leftover $1357.96 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 838 | $1.62 | $10.81 | — | $1,430.28 | ▲ $10,838.47 (+838.47) | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1357.96 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 678 | $2.00 | $8.75 | — | $65.54 | ▲ $10,829.73 (+829.73) | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $1357.96 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `MOS` | 56 | $24.84 | $2.18 | $+42.70 | $1,454.40 | ▲ $10,879.43 (+879.43) | dropped from list after 2 sess (min 1) | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 124 | $10.79 | $2.39 | $-20.88 | $2,789.96 | ▲ $10,877.03 (+877.03) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 22 | $60.07 | $2.08 | $-34.93 | $4,109.43 | ▲ $10,874.96 (+874.96) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 164 | $8.60 | $2.52 | $+47.48 | $5,517.31 | ▲ $10,872.44 (+872.44) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 259 | $5.01 | $3.39 | $-63.72 | $6,811.50 | ▲ $10,869.04 (+869.04) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 3 | $427.50 | $2.02 | $-9.24 | $8,091.98 | ▲ $10,867.02 (+867.02) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BMEA` | 838 | $1.75 | $10.96 | $+87.17 | $9,547.52 | ▲ $10,856.06 (+856.06) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NPWR` | 678 | $1.93 | $8.87 | $-65.08 | $10,847.19 | ▲ $10,847.19 (+847.19) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 44 | $40.72 | $2.12 | — | $9,053.39 | ▲ $10,845.07 (+845.07) | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+1.8; leftover $1807.87 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 22 | $80.97 | $2.06 | — | $7,270.00 | ▲ $10,843.02 (+843.02) | union ∩ news_present, no 🚨; gate news_present=True; list mover_buy; 🔵; ret5=-1.3; leftover $1807.87 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $925.74 | $1.99 | — | $6,342.26 | ▲ $10,841.02 (+841.02) | union ∩ news_present, no 🚨; gate news_present=True; list mover_buy; 🔵; ret5=-0.5; leftover $1807.87 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ASML` | 1 | $1746.33 | $1.99 | — | $4,593.94 | ▲ $10,839.03 (+839.03) | union ∩ news_present, no 🚨; gate news_present=True; list mover_buy; 🔵; ret5=-4.4; leftover $1807.87 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 5 | $314.61 | $2.00 | — | $3,018.88 | ▲ $10,837.02 (+837.02) | union ∩ news_present, no 🚨; gate news_present=True; list mover_buy; 🔵; ret5=-5.5; leftover $1807.87 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 8 | $212.64 | $2.01 | — | $1,315.75 | ▲ $10,835.01 (+835.01) | union ∩ news_present, no 🚨; gate news_present=True; list mover_buy; 🔵; ret5=-4.6; leftover $1807.87 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 22 | $81.65 | $2.08 | $+10.82 | $3,109.97 | ▲ $11,024.15 (+1,024.15) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $967.01 | $2.01 | $+37.26 | $4,074.97 | ▲ $11,022.14 (+1,022.14) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ASML` | 1 | $1746.53 | $2.02 | $-3.81 | $5,819.48 | ▲ $11,020.12 (+1,020.12) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 5 | $318.88 | $2.03 | $+17.32 | $7,411.85 | ▲ $11,018.09 (+1,018.09) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 8 | $222.86 | $2.04 | $+77.71 | $9,192.69 | ▲ $11,016.05 (+1,016.05) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `CRK` | 91 | $14.42 | $2.26 | — | $7,878.21 | ▲ $11,013.79 (+1,013.79) | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+1.1; leftover $1313.24 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `MOS` | 54 | $24.00 | $2.15 | — | $6,580.06 | ▲ $11,011.64 (+1,011.64) | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+13.0; leftover $1313.24 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `SLI` | 505 | $2.60 | $6.51 | — | $5,260.54 | ▲ $11,005.12 (+1,005.12) | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+4.2; leftover $1313.24 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $144.70 | $2.02 | — | $3,956.23 | ▲ $11,003.11 (+1,003.11) | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1313.24 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 77 | $16.95 | $2.22 | — | $2,648.86 | ▲ $11,000.89 (+1,000.89) | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1313.24 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 70 | $18.50 | $2.20 | — | $1,351.66 | ▲ $10,998.69 (+998.69) | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1313.24 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 142 | $9.19 | $2.42 | — | $44.26 | ▲ $10,996.27 (+996.27) | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1313.24 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 44 | $41.11 | $2.15 | $+12.89 | $1,850.95 | ▲ $10,845.65 (+845.65) | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 91 | $14.56 | $2.29 | $+8.19 | $3,173.63 | ▲ $10,843.37 (+843.37) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 54 | $23.75 | $2.17 | $-17.82 | $4,453.95 | ▲ $10,841.19 (+841.19) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 505 | $2.51 | $6.61 | $-58.57 | $5,714.89 | ▲ $10,834.58 (+834.58) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.67 | $2.04 | $+31.68 | $7,050.89 | ▲ $10,832.55 (+832.55) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BHVN` | 77 | $15.44 | $2.24 | $-120.73 | $8,237.52 | ▲ $10,830.30 (+830.30) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 70 | $17.89 | $2.22 | $-47.12 | $9,487.60 | ▲ $10,828.08 (+828.08) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 142 | $9.44 | $2.45 | $+30.63 | $10,825.63 | ▲ $10,825.63 (+825.63) | dropped from list after 1 sess (min 1) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 27 | $49.76 | $2.07 | — | $9,480.04 | ▲ $10,823.56 (+823.56) | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1353.20 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 32 | $41.31 | $2.09 | — | $8,156.03 | ▲ $10,821.47 (+821.47) | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1353.20 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 413 | $3.27 | $5.33 | — | $6,800.20 | ▲ $10,816.15 (+816.15) | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1353.20 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 175 | $7.70 | $2.52 | — | $5,450.18 | ▲ $10,813.63 (+813.63) | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1353.20 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $125.94 | $2.02 | — | $4,188.76 | ▲ $10,811.61 (+811.61) | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1353.20 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1109 | $1.22 | $14.31 | — | $2,821.48 | ▲ $10,797.31 (+797.31) | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1353.20 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 73 | $18.40 | $2.21 | — | $1,476.07 | ▲ $10,795.10 (+795.10) | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1353.20 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 86 | $15.70 | $2.25 | — | $123.62 | ▲ $10,792.85 (+792.85) | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1353.20 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 32 | $42.93 | $2.11 | $+47.65 | $1,495.27 | ▲ $11,788.40 (+1,788.40) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 175 | $8.03 | $2.56 | $+52.68 | $2,897.97 | ▲ $11,785.85 (+1,785.85) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $132.45 | $2.04 | $+61.04 | $4,220.43 | ▲ $11,783.81 (+1,783.81) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FRVO` | 73 | $18.27 | $2.23 | $-13.93 | $5,551.90 | ▲ $11,781.57 (+1,781.57) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 86 | $15.45 | $2.27 | $-26.02 | $6,878.33 | ▲ $11,779.30 (+1,779.30) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASND` | 5 | $266.94 | $2.00 | — | $5,541.63 | ▲ $11,777.30 (+1,777.30) | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+1.9; leftover $1375.67 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OSCR` | 44 | $30.65 | $2.12 | — | $4,190.90 | ▲ $11,775.17 (+1,775.17) | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=-2.2; leftover $1375.67 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 132 | $10.41 | $2.39 | — | $2,814.40 | ▲ $11,772.79 (+1,772.79) | union ∩ news_present, no 🚨; gate news_present=True; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $1375.67 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 94 | $14.50 | $2.27 | — | $1,449.13 | ▲ $11,770.52 (+1,770.52) | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1375.67 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 705 | $1.95 | $9.09 | — | $65.28 | ▲ $11,761.42 (+1,761.42) | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1375.67 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

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
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
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
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 27 | 2026-09-03 @ $49.76 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1353.20 |
| `CABA` | 413 | 2026-09-03 @ $3.27 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1353.20 |
| `GPRO` | 1109 | 2026-09-03 @ $1.22 | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1353.20 |
| `ASND` | 5 | 2026-09-04 @ $266.94 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+1.9; leftover $1375.67 |
| `OSCR` | 44 | 2026-09-04 @ $30.65 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=-2.2; leftover $1375.67 |
| `NVAX` | 132 | 2026-09-04 @ $10.41 | union ∩ news_present, no 🚨; gate news_present=True; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $1375.67 |
| `BVS` | 94 | 2026-09-04 @ $14.50 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1375.67 |
| `BAK` | 705 | 2026-09-04 @ $1.95 | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1375.67 |
