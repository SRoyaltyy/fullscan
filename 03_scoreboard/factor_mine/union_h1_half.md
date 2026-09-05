# Factor mine action — `union_h1_half`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `half` · sell `list` · S-boost `none` · deploy half leftover

Cash book **+5.58%** ($10,557) · signal-only (no cash/fees) was +18.57%. Starts YES **16/17**. Fills 134 · skips 53 · realized $+381.95.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `half` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $4,308.94.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $5,101.72 | $4,969.43 | $10,071.15 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26 | BUY BTSG x10 @ 59.80; BUY IREN x13 @ 45.98; BUY TPG x12 @ 50.62; BUY TGTX x12 @ 49.70; BUY SLS x53 @ 11.70; BUY HIMS x21 @ 29.74; BUY INO x771 @ 0.81; BUY TNDM x26 @ 23.33 |
| 2026-08-14 | +5.50 | $5,101.72 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26 | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $5,701.01 | $4,376.45 | $10,077.45 | TLN×1, VST×4, NRG×5, DAVE×1, SLG×10, MARA×69, LDI×671, BTBT×419 | SELL BTSG (dropped from list after 1 sess (min 1)); SELL IREN (dropped from list after 1 sess (min 1)); SELL TPG (dropped from list after 1 sess (min 1)); SELL TGTX (dropped from list after 1 sess (min 1)); SELL SLS (dropped from list after 1 sess (min 1)); SELL HIMS (dropped from list after 1 sess (min 1)); SELL INO (dropped from list after 1 sess (min 1)); SELL TNDM (dropped from list after 1 sess (min 1)); BUY TLN x1 @ 359.83; BUY VST x4 @ 146.90; BUY NRG x5 @ 120.00; BUY DAVE x1 @ 330.91; BUY SLG x10 @ 57.61; BUY MARA x69 @ 9.01; BUY LDI x671 @ 0.94; BUY BTBT x419 @ 1.50 |
| 2026-08-17 | +2.25 | $5,701.01 | TLN×1, VST×4, NRG×5, DAVE×1, SLG×10, MARA×69, LDI×671, BTBT×419 | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | $5,204.80 | $4,843.84 | $10,048.64 | DVN×13, EOG×4, FANG×3, TMC×155, TGB×74, ELF×6, DNN×193, HNST×130 | SELL TLN (dropped from list after 1 sess (min 1)); SELL VST (dropped from list after 1 sess (min 1)); SELL NRG (dropped from list after 1 sess (min 1)); SELL DAVE (dropped from list after 1 sess (min 1)); SELL SLG (dropped from list after 1 sess (min 1)); SELL MARA (dropped from list after 1 sess (min 1)); SELL LDI (dropped from list after 1 sess (min 1)); SELL BTBT (dropped from list after 1 sess (min 1)); BUY DVN x13 @ 46.18; BUY EOG x4 @ 142.77; BUY FANG x3 @ 202.70; BUY TMC x155 @ 4.05; BUY TGB x74 @ 8.46; BUY ELF x6 @ 90.54; BUY DNN x193 @ 3.24; BUY HNST x130 @ 4.81 |
| 2026-08-18 | -6.20 | $5,204.80 | DVN×13, EOG×4, FANG×3, TMC×155, TGB×74, ELF×6, DNN×193, HNST×130 | — | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST | $10,007.16 | $0.00 | $10,007.16 | — | SELL DVN (dropped from list after 1 sess (min 1)); SELL EOG (dropped from list after 1 sess (min 1)); SELL FANG (dropped from list after 1 sess (min 1)); SELL TMC (dropped from list after 1 sess (min 1)); SELL TGB (dropped from list after 1 sess (min 1)); SELL ELF (dropped from list after 1 sess (min 1)); SELL DNN (dropped from list after 1 sess (min 1)); SELL HNST (dropped from list after 1 sess (min 1)); hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $10,007.16 | — | — | — | $10,007.16 | $0.00 | $10,007.16 | — | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $10,007.16 | — | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $5,149.04 | $4,953.62 | $10,102.66 | AG×30, BHP×6, CDE×30, HDSN×108, IAG×31, KGC×21, NFGC×357, WPM×4 | BUY AG x30 @ 20.55; BUY BHP x6 @ 91.01; BUY CDE x30 @ 20.65; BUY HDSN x108 @ 5.77; BUY IAG x31 @ 19.63; BUY KGC x21 @ 29.63; BUY NFGC x357 @ 1.75; BUY WPM x4 @ 144.54 |
| 2026-08-21 | +3.25 | $5,149.04 | AG×30, BHP×6, CDE×30, HDSN×108, IAG×31, KGC×21, NFGC×357, WPM×4 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $5,381.87 | $4,937.36 | $10,319.23 | AU×5, AUPH×37, AEM×2, ARCT×57, AUTL×258, CRDL×330, CRSP×10, CYPH×483 | SELL AG (dropped from list after 1 sess (min 1)); SELL BHP (dropped from list after 1 sess (min 1)); SELL CDE (dropped from list after 1 sess (min 1)); SELL HDSN (dropped from list after 1 sess (min 1)); SELL IAG (dropped from list after 1 sess (min 1)); SELL KGC (dropped from list after 1 sess (min 1)); SELL NFGC (dropped from list after 1 sess (min 1)); SELL WPM (dropped from list after 1 sess (min 1)); BUY AU x5 @ 119.43; BUY AUPH x37 @ 17.20; BUY AEM x2 @ 216.30; BUY ARCT x57 @ 11.13; BUY AUTL x258 @ 2.47; BUY CRDL x330 @ 1.93; BUY CRSP x10 @ 59.72; BUY CYPH x483 @ 1.32 |
| 2026-08-24 | -5.17 | $5,381.87 | AU×5, AUPH×37, AEM×2, ARCT×57, AUTL×258, CRDL×330, CRSP×10, CYPH×483 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $10,461.82 | $0.00 | $10,461.82 | — | SELL AU (dropped from list after 1 sess (min 1)); SELL AUPH (dropped from list after 1 sess (min 1)); SELL AEM (dropped from list after 1 sess (min 1)); SELL ARCT (dropped from list after 1 sess (min 1)); SELL AUTL (dropped from list after 1 sess (min 1)); SELL CRDL (dropped from list after 1 sess (min 1)); SELL CRSP (dropped from list after 1 sess (min 1)); SELL CYPH (dropped from list after 1 sess (min 1)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $10,461.82 | — | MOS, OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | — | $5,498.90 | $4,943.17 | $10,442.07 | MOS×27, OCUL×59, INSP×10, CRMD×78, RZLT×125, HCA×1, BMEA×403, NPWR×326 | BUY MOS x27 @ 24.00; BUY OCUL x59 @ 10.92; BUY INSP x10 @ 61.47; BUY CRMD x78 @ 8.28; BUY RZLT x125 @ 5.23; BUY HCA x1 @ 429.24; BUY BMEA x403 @ 1.62; BUY NPWR x326 @ 2.00 |
| 2026-08-26 | +2.02 | $5,498.90 | MOS×27, OCUL×59, INSP×10, CRMD×78, RZLT×125, HCA×1, BMEA×403, NPWR×326 | — | — | $5,498.90 | $4,940.67 | $10,439.57 | MOS×27, OCUL×59, INSP×10, CRMD×78, RZLT×125, HCA×1, BMEA×403, NPWR×326 | hold MOS,OCUL,INSP,CRMD,RZLT,HCA,BMEA,NPWR |
| 2026-08-27 | — | $5,498.90 | MOS×27, OCUL×59, INSP×10, CRMD×78, RZLT×125, HCA×1, BMEA×403, NPWR×326 | RRC, CRK, SLI, ACMR, GGB, MT | OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | $5,663.48 | $4,775.13 | $10,438.61 | MOS×27, RRC×17, CRK×49, SLI×269, ACMR×8, GGB×157, MT×9 | SELL OCUL (dropped from list after 2 sess (min 1)); SELL INSP (dropped from list after 2 sess (min 1)); SELL CRMD (dropped from list after 2 sess (min 1)); SELL RZLT (dropped from list after 2 sess (min 1)); SELL HCA (dropped from list after 2 sess (min 1)); SELL BMEA (dropped from list after 2 sess (min 1)); SELL NPWR (dropped from list after 2 sess (min 1)); BUY RRC x17 @ 40.72; BUY CRK x49 @ 14.09; BUY SLI x269 @ 2.59; BUY ACMR x8 @ 80.97; BUY GGB x157 @ 4.42; BUY MT x9 @ 75.12 |
| 2026-08-28 | +0.75 | $5,663.48 | MOS×27, RRC×17, CRK×49, SLI×269, ACMR×8, GGB×157, MT×9 | ANF, BHVN, BZ, CAPR | ACMR, GGB, MT | $3,954.69 | $6,535.40 | $10,490.09 | MOS×27, RRC×17, CRK×49, SLI×269, ANF×6, BHVN×56, BZ×52, CAPR×104 | SELL ACMR (dropped from list after 1 sess (min 1)); SELL GGB (dropped from list after 1 sess (min 1)); SELL MT (dropped from list after 1 sess (min 1)); BUY ANF x6 @ 144.70; BUY BHVN x56 @ 16.95; BUY BZ x52 @ 18.50; BUY CAPR x104 @ 9.19 |
| 2026-08-31 | -5.85 | $3,954.69 | MOS×27, RRC×17, CRK×49, SLI×269, ANF×6, BHVN×56, BZ×52, CAPR×104 | — | MOS, RRC, CRK, SLI, ANF, BHVN, BZ, CAPR | $10,333.61 | $0.00 | $10,333.61 | — | SELL MOS (dropped from list after 4 sess (min 1)); SELL RRC (dropped from list after 2 sess (min 1)); SELL CRK (dropped from list after 2 sess (min 1)); SELL SLI (dropped from list after 2 sess (min 1)); SELL ANF (dropped from list after 1 sess (min 1)); SELL BHVN (dropped from list after 1 sess (min 1)); SELL BZ (dropped from list after 1 sess (min 1)); SELL CAPR (dropped from list after 1 sess (min 1)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $10,333.61 | — | — | — | $10,333.61 | $0.00 | $10,333.61 | — | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $10,333.61 | — | — | — | $10,333.61 | $0.00 | $10,333.61 | — | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $10,333.61 | — | ATRC, HRMY, CABA, VSTM, RVTY, GPRO, FRVO, CRK | — | $5,248.85 | $5,458.08 | $10,706.93 | ATRC×12, HRMY×15, CABA×197, VSTM×83, RVTY×5, GPRO×529, FRVO×35, CRK×41 | BUY ATRC x12 @ 49.76; BUY HRMY x15 @ 41.31; BUY CABA x197 @ 3.27; BUY VSTM x83 @ 7.70; BUY RVTY x5 @ 125.94; BUY GPRO x529 @ 1.22; BUY FRVO x35 @ 18.40; BUY CRK x41 @ 15.70 |
| 2026-09-04 | — | $5,248.85 | ATRC×12, HRMY×15, CABA×197, VSTM×83, RVTY×5, GPRO×529, FRVO×35, CRK×41 | ASND, OSCR, NVAX, BVS, BAK | HRMY, VSTM, RVTY, FRVO, CRK | $4,308.94 | $6,248.55 | $10,557.49 | ATRC×12, CABA×197, GPRO×529, ASND×3, OSCR×27, NVAX×81, BVS×58, BAK×435 | SELL HRMY (dropped from list after 1 sess (min 1)); SELL VSTM (dropped from list after 1 sess (min 1)); SELL RVTY (dropped from list after 1 sess (min 1)); SELL FRVO (dropped from list after 1 sess (min 1)); SELL CRK (dropped from list after 1 sess (min 1)); BUY ASND x3 @ 266.94; BUY OSCR x27 @ 30.65; BUY NVAX x81 @ 10.41; BUY BVS x58 @ 14.50; BUY BAK x435 @ 1.95 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 10 | $59.80 | $2.02 | — | $9,399.98 | deploy half leftover; list flatten; ⚪; ret5=-5.3; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 13 | $45.98 | $2.03 | — | $8,800.21 | deploy half leftover; list flatten; ⚪; ret5=+12.3; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 12 | $50.62 | $2.03 | — | $8,190.71 | deploy half leftover; list flatten; ⚪; ret5=+6.2; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 12 | $49.70 | $2.03 | — | $7,592.28 | deploy half leftover; list flatten; ⚪; ret5=-0.8; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 53 | $11.70 | $2.15 | — | $6,970.03 | deploy half leftover; list flatten; ⚪; ret5=-0.8; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 21 | $29.74 | $2.05 | — | $6,343.44 | deploy half leftover; list flatten; ⚪; ret5=-5.3; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 771 | $0.81 | $8.56 | — | $5,710.37 | deploy half leftover; list flatten; ⚪; ret5=+13.2; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 26 | $23.33 | $2.07 | — | $5,101.72 | deploy half leftover; list flatten; ⚪; ret5=+19.7; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 10 | $59.65 | $2.04 | $-5.56 | $5,696.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 13 | $44.09 | $2.05 | $-28.65 | $6,267.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 12 | $55.29 | $2.05 | $+51.93 | $6,928.74 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 12 | $47.27 | $2.05 | $-33.23 | $7,493.93 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 53 | $12.40 | $2.17 | $+32.78 | $8,148.96 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 21 | $29.15 | $2.07 | $-16.52 | $8,759.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 771 | $0.93 | $9.62 | $+74.34 | $9,466.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 26 | $22.92 | $2.09 | $-14.82 | $10,060.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 1 | $359.83 | $1.99 | — | $9,698.46 | deploy half leftover; list flatten; 🔵; ret5=+5.9; leftover $628.77 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 4 | $146.90 | $2.00 | — | $9,108.86 | deploy half leftover; list flatten; 🔵; ret5=+3.6; leftover $628.77 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 5 | $120.00 | $2.00 | — | $8,506.85 | deploy half leftover; list flatten; 🔵; ret5=+0.6; leftover $628.77 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 1 | $330.91 | $1.99 | — | $8,173.95 | deploy half leftover; list flatten; 🔵; ⚪; ret5=-8.6; leftover $628.77 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 10 | $57.61 | $2.02 | — | $7,595.83 | deploy half leftover; list flatten; 🔵; ret5=+5.7; leftover $628.77 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 69 | $9.01 | $2.20 | — | $6,971.94 | deploy half leftover; list flatten; 🔵; ⚪; ret5=-13.5; leftover $628.77 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 671 | $0.94 | $8.30 | — | $6,334.91 | deploy half leftover; list flatten; 🔵; ⚪; ret5=+0.5; leftover $628.77 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 419 | $1.50 | $5.41 | — | $5,701.01 | deploy half leftover; list flatten; 🔵; ⚪; ret5=+9.2; leftover $628.77 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 1 | $367.88 | $2.01 | $+4.04 | $6,066.87 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 4 | $149.37 | $2.02 | $+5.86 | $6,662.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 5 | $127.40 | $2.02 | $+32.97 | $7,297.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 1 | $336.94 | $2.01 | $+2.02 | $7,632.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 10 | $55.37 | $2.04 | $-26.46 | $8,183.89 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 69 | $9.22 | $2.22 | $+10.07 | $8,817.86 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 671 | $0.91 | $8.22 | $-36.65 | $9,418.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 419 | $1.52 | $5.48 | $-2.51 | $10,049.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 13 | $46.18 | $2.03 | — | $9,447.26 | deploy half leftover; list flatten; 🔵; ret5=+6.7; leftover $628.10 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 4 | $142.77 | $2.00 | — | $8,874.18 | deploy half leftover; list flatten; 🔵; ret5=+5.8; leftover $628.10 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 3 | $202.70 | $2.00 | — | $8,264.08 | deploy half leftover; list flatten; 🔵; ret5=+8.3; leftover $628.10 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 155 | $4.05 | $2.46 | — | $7,633.87 | deploy half leftover; list flatten; 🔵; ⚪; ret5=-12.3; leftover $628.10 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 74 | $8.46 | $2.21 | — | $7,005.62 | deploy half leftover; list flatten; 🔵; ⚪; ret5=+0.4; leftover $628.10 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 6 | $90.54 | $2.01 | — | $6,460.37 | deploy half leftover; list flatten; ret5=-7.2; leftover $628.10 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 193 | $3.24 | $2.57 | — | $5,832.48 | deploy half leftover; list flatten; ⚪; ret5=+0.3; leftover $628.10 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 130 | $4.81 | $2.38 | — | $5,204.80 | deploy half leftover; list flatten; ⚪; ret5=-11.4; leftover $628.10 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 13 | $48.00 | $2.05 | $+19.58 | $5,826.76 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 4 | $148.04 | $2.02 | $+17.06 | $6,416.89 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 3 | $208.93 | $2.02 | $+14.67 | $7,041.66 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 155 | $3.72 | $2.49 | $-56.10 | $7,615.77 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 74 | $8.55 | $2.23 | $+2.21 | $8,246.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 6 | $93.44 | $2.03 | $+13.36 | $8,804.85 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 193 | $3.11 | $2.61 | $-30.27 | $9,402.47 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `HNST` | 130 | $4.67 | $2.41 | $-22.99 | $10,007.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 30 | $20.55 | $2.08 | — | $9,388.58 | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $625.45 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 6 | $91.01 | $2.01 | — | $8,840.51 | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $625.45 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 30 | $20.65 | $2.08 | — | $8,218.93 | deploy half leftover; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $625.45 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 108 | $5.77 | $2.31 | — | $7,593.46 | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $625.45 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 31 | $19.63 | $2.08 | — | $6,982.84 | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $625.45 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 21 | $29.63 | $2.05 | — | $6,358.56 | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $625.45 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 357 | $1.75 | $4.61 | — | $5,729.21 | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $625.45 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 4 | $144.54 | $2.00 | — | $5,149.04 | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $625.45 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 30 | $21.90 | $2.10 | $+36.32 | $5,803.94 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 6 | $95.72 | $2.03 | $+24.22 | $6,376.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 30 | $21.75 | $2.10 | $+28.82 | $7,026.64 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 108 | $5.67 | $2.34 | $-15.46 | $7,636.65 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 31 | $21.17 | $2.10 | $+43.55 | $8,290.82 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 21 | $32.17 | $2.07 | $+49.21 | $8,964.32 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 357 | $1.79 | $4.67 | $+5.00 | $9,598.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 4 | $154.70 | $2.02 | $+36.62 | $10,215.45 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 5 | $119.43 | $2.00 | — | $9,616.30 | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $638.47 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 37 | $17.20 | $2.10 | — | $8,977.79 | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $638.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 2 | $216.30 | $2.00 | — | $8,543.20 | deploy half leftover; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $638.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 57 | $11.13 | $2.16 | — | $7,906.63 | deploy half leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $638.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 258 | $2.47 | $3.33 | — | $7,266.04 | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $638.47 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 330 | $1.93 | $4.26 | — | $6,624.88 | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $638.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 10 | $59.72 | $2.02 | — | $6,025.66 | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $638.47 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 483 | $1.32 | $6.23 | — | $5,381.87 | deploy half leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $638.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 5 | $120.50 | $2.02 | $+1.32 | $5,982.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 37 | $16.60 | $2.12 | $-26.42 | $6,594.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 2 | $217.03 | $2.02 | $-2.55 | $7,026.47 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 57 | $13.26 | $2.18 | $+117.07 | $7,780.11 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 258 | $2.36 | $3.38 | $-35.09 | $8,385.61 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 330 | $1.87 | $4.32 | $-28.38 | $8,998.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 10 | $58.79 | $2.04 | $-13.36 | $9,584.25 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 483 | $1.83 | $6.32 | $+233.78 | $10,461.82 | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 27 | $24.00 | $2.07 | — | $9,811.74 | deploy half leftover; list flatten; ⚪; ret5=+13.0; leftover $653.86 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 59 | $10.92 | $2.17 | — | $9,165.30 | deploy half leftover; list flatten; 🔵; ret5=+10.4; leftover $653.86 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 10 | $61.47 | $2.02 | — | $8,548.58 | deploy half leftover; list flatten; 🔵; ret5=+9.2; leftover $653.86 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 78 | $8.28 | $2.22 | — | $7,900.51 | deploy half leftover; list flatten; 🔵; ⚪; ret5=+8.8; leftover $653.86 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 125 | $5.23 | $2.37 | — | $7,244.40 | deploy half leftover; list flatten; ret5=+10.7; leftover $653.86 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 1 | $429.24 | $1.99 | — | $6,813.17 | deploy half leftover; list flatten; ret5=+6.1; leftover $653.86 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 403 | $1.62 | $5.20 | — | $6,155.11 | deploy half leftover; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $653.86 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 326 | $2.00 | $4.21 | — | $5,498.90 | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $653.86 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 59 | $10.79 | $2.19 | $-12.02 | $6,133.32 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 10 | $60.07 | $2.04 | $-18.06 | $6,731.98 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 78 | $8.60 | $2.25 | $+20.49 | $7,400.54 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 125 | $5.01 | $2.40 | $-32.26 | $8,024.39 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 1 | $427.50 | $2.01 | $-5.75 | $8,449.88 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BMEA` | 403 | $1.75 | $5.28 | $+41.92 | $9,149.85 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NPWR` | 326 | $1.93 | $4.27 | $-31.29 | $9,774.76 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 17 | $40.72 | $2.04 | — | $9,080.48 | deploy half leftover; list flatten; ret5=+1.8; leftover $698.20 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 49 | $14.09 | $2.14 | — | $8,387.94 | deploy half leftover; list flatten; ret5=+1.1; leftover $698.20 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 269 | $2.59 | $3.47 | — | $7,687.76 | deploy half leftover; list flatten; ret5=+4.2; leftover $698.20 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 8 | $80.97 | $2.01 | — | $7,037.98 | deploy half leftover; list mover_buy; 🔵; ret5=-1.3; leftover $698.20 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 157 | $4.42 | $2.46 | — | $6,341.58 | deploy half leftover; list mover_buy; ret5=-8.6; leftover $698.20 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 9 | $75.12 | $2.02 | — | $5,663.48 | deploy half leftover; list mover_buy; ret5=-2.2; leftover $698.20 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 8 | $81.65 | $2.03 | $+1.39 | $6,314.65 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 157 | $4.57 | $2.50 | $+18.59 | $7,029.64 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 9 | $74.54 | $2.04 | $-9.27 | $7,698.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 6 | $144.70 | $2.01 | — | $6,828.26 | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $962.31 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 56 | $16.95 | $2.16 | — | $5,876.90 | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $962.31 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 52 | $18.50 | $2.15 | — | $4,912.75 | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $962.31 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 104 | $9.19 | $2.30 | — | $3,954.69 | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $962.31 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 27 | $23.75 | $2.09 | $-10.91 | $4,593.85 | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 17 | $41.11 | $2.06 | $+2.53 | $5,290.66 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 49 | $14.56 | $2.16 | $+18.74 | $6,001.94 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 269 | $2.51 | $3.52 | $-28.51 | $6,673.61 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 6 | $148.67 | $2.03 | $+19.78 | $7,563.60 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BHVN` | 56 | $15.44 | $2.18 | $-88.90 | $8,426.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 52 | $17.89 | $2.17 | $-36.03 | $9,354.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 104 | $9.44 | $2.33 | $+21.37 | $10,333.61 | dropped from list after 1 sess (min 1) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 12 | $49.76 | $2.03 | — | $9,734.46 | deploy half leftover; list flatten; 🔵; ⚪; ret5=+10.6; leftover $645.85 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 15 | $41.31 | $2.04 | — | $9,112.78 | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $645.85 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 197 | $3.27 | $2.58 | — | $8,466.00 | deploy half leftover; list flatten; 🔵; ⚪; ret5=+13.8; leftover $645.85 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 83 | $7.70 | $2.24 | — | $7,824.67 | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $645.85 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 5 | $125.94 | $2.00 | — | $7,192.96 | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $645.85 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 529 | $1.22 | $6.82 | — | $6,540.76 | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $645.85 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 35 | $18.40 | $2.10 | — | $5,894.66 | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $645.85 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 41 | $15.70 | $2.11 | — | $5,248.85 | deploy half leftover; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $645.85 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 15 | $42.93 | $2.06 | $+20.21 | $5,890.74 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 83 | $8.03 | $2.26 | $+22.89 | $6,554.97 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 5 | $132.45 | $2.02 | $+28.52 | $7,215.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FRVO` | 35 | $18.27 | $2.12 | $-8.76 | $7,852.53 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 41 | $15.45 | $2.13 | $-14.50 | $8,483.85 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASND` | 3 | $266.94 | $2.00 | — | $7,681.03 | deploy half leftover; list flatten; ret5=+1.9; leftover $848.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OSCR` | 27 | $30.65 | $2.07 | — | $6,851.41 | deploy half leftover; list flatten; 🔵; ret5=-2.2; leftover $848.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 81 | $10.41 | $2.23 | — | $6,005.96 | deploy half leftover; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $848.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 58 | $14.50 | $2.16 | — | $5,162.80 | deploy half leftover; list flatten; 🔵; ⚪; ret5=+0.8; leftover $848.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 435 | $1.95 | $5.61 | — | $4,308.94 | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $848.38 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

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
| 2026-08-27 | `MU` | cash | leftover split 698.20 < 1 share @ 925.74 |
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
| `ATRC` | 12 | 2026-09-03 @ $49.76 | deploy half leftover; list flatten; 🔵; ⚪; ret5=+10.6; leftover $645.85 |
| `CABA` | 197 | 2026-09-03 @ $3.27 | deploy half leftover; list flatten; 🔵; ⚪; ret5=+13.8; leftover $645.85 |
| `GPRO` | 529 | 2026-09-03 @ $1.22 | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $645.85 |
| `ASND` | 3 | 2026-09-04 @ $266.94 | deploy half leftover; list flatten; ret5=+1.9; leftover $848.38 |
| `OSCR` | 27 | 2026-09-04 @ $30.65 | deploy half leftover; list flatten; 🔵; ret5=-2.2; leftover $848.38 |
| `NVAX` | 81 | 2026-09-04 @ $10.41 | deploy half leftover; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $848.38 |
| `BVS` | 58 | 2026-09-04 @ $14.50 | deploy half leftover; list flatten; 🔵; ⚪; ret5=+0.8; leftover $848.38 |
| `BAK` | 435 | 2026-09-04 @ $1.95 | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $848.38 |
