# Factor mine action — `union_h3_half`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `half` · sell `list` · S-boost `none` · deploy half leftover

Cash book **+4.95%** ($10,495) · signal-only (no cash/fees) was +34.19%. Starts YES **16/17**. Fills 125 · skips 168 · realized $+360.44.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `half` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $2,865.39.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $5,101.72 | $4,969.43 | $10,071.15 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26 | BUY BTSG x10 @ 59.80; BUY IREN x13 @ 45.98; BUY TPG x12 @ 50.62; BUY TGTX x12 @ 49.70; BUY SLS x53 @ 11.70; BUY HIMS x21 @ 29.74; BUY INO x771 @ 0.81; BUY TNDM x26 @ 23.33 |
| 2026-08-14 | +5.50 | $5,101.72 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26 | VST, NRG, SLG, MARA, LDI, BTBT | — | $3,312.91 | $6,899.73 | $10,212.64 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26, VST×2, NRG×2, SLG×5, MARA×35, LDI×340, BTBT×212 | BUY VST x2 @ 146.90; BUY NRG x2 @ 120.00; BUY SLG x5 @ 57.61; BUY MARA x35 @ 9.01; BUY LDI x340 @ 0.94; BUY BTBT x212 @ 1.50 |
| 2026-08-17 | +2.25 | $3,312.91 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26, VST×2, NRG×2, SLG×5, MARA×35, LDI×340, BTBT×212 | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST | — | $1,765.50 | $8,485.46 | $10,250.96 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26, VST×2, NRG×2, SLG×5, MARA×35, LDI×340, BTBT×212, DVN×4, EOG×1, FANG×1, TMC×51, TGB×24, ELF×2, DNN×63, HNST×43 | BUY DVN x4 @ 46.18; BUY EOG x1 @ 142.77; BUY FANG x1 @ 202.70; BUY TMC x51 @ 4.05; BUY TGB x24 @ 8.46; BUY ELF x2 @ 90.54; BUY DNN x63 @ 3.24; BUY HNST x43 @ 4.81 |
| 2026-08-18 | -6.20 | $1,765.50 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26, VST×2, NRG×2, SLG×5, MARA×35, LDI×340, BTBT×212, DVN×4, EOG×1, FANG×1, TMC×51, TGB×24, ELF×2, DNN×63, HNST×43 | — | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $6,830.71 | $3,247.40 | $10,078.11 | VST×2, NRG×2, SLG×5, MARA×35, LDI×340, BTBT×212, DVN×4, EOG×1, FANG×1, TMC×51, TGB×24, ELF×2, DNN×63, HNST×43 | SELL BTSG (dropped from list after 3 sess (min 3)); SELL IREN (dropped from list after 3 sess (min 3)); SELL TPG (dropped from list after 3 sess (min 3)); SELL TGTX (dropped from list after 3 sess (min 3)); SELL SLS (dropped from list after 3 sess (min 3)); SELL HIMS (dropped from list after 3 sess (min 3)); SELL INO (dropped from list after 3 sess (min 3)); SELL TNDM (dropped from list after 3 sess (min 3)); hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $6,830.71 | VST×2, NRG×2, SLG×5, MARA×35, LDI×340, BTBT×212, DVN×4, EOG×1, FANG×1, TMC×51, TGB×24, ELF×2, DNN×63, HNST×43 | — | VST, NRG, SLG, MARA, LDI, BTBT | $8,529.15 | $1,574.56 | $10,103.71 | DVN×4, EOG×1, FANG×1, TMC×51, TGB×24, ELF×2, DNN×63, HNST×43 | SELL VST (dropped from list after 3 sess (min 3)); SELL NRG (dropped from list after 3 sess (min 3)); SELL SLG (dropped from list after 3 sess (min 3)); SELL MARA (dropped from list after 3 sess (min 3)); SELL LDI (dropped from list after 3 sess (min 3)); SELL BTBT (dropped from list after 3 sess (min 3)); hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $8,529.15 | DVN×4, EOG×1, FANG×1, TMC×51, TGB×24, ELF×2, DNN×63, HNST×43 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST | $5,197.63 | $4,984.94 | $10,182.57 | AG×30, BHP×6, CDE×30, HDSN×109, IAG×32, KGC×21, NFGC×360, WPM×4 | SELL DVN (dropped from list after 3 sess (min 3)); SELL EOG (dropped from list after 3 sess (min 3)); SELL FANG (dropped from list after 3 sess (min 3)); SELL TMC (dropped from list after 3 sess (min 3)); SELL TGB (dropped from list after 3 sess (min 3)); SELL ELF (dropped from list after 3 sess (min 3)); SELL DNN (dropped from list after 3 sess (min 3)); SELL HNST (dropped from list after 3 sess (min 3)); BUY AG x30 @ 20.55; BUY BHP x6 @ 91.01; BUY CDE x30 @ 20.65; BUY HDSN x109 @ 5.77; BUY IAG x32 @ 19.63; BUY KGC x21 @ 29.63; BUY NFGC x360 @ 1.75; BUY WPM x4 @ 144.54 |
| 2026-08-21 | +3.25 | $5,197.63 | AG×30, BHP×6, CDE×30, HDSN×109, IAG×32, KGC×21, NFGC×360, WPM×4 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | — | $2,820.80 | $7,538.87 | $10,359.67 | AG×30, BHP×6, CDE×30, HDSN×109, IAG×32, KGC×21, NFGC×360, WPM×4, AU×2, AUPH×18, AEM×1, ARCT×29, AUTL×131, CRDL×168, CRSP×5, CYPH×246 | BUY AU x2 @ 119.43; BUY AUPH x18 @ 17.20; BUY AEM x1 @ 216.30; BUY ARCT x29 @ 11.13; BUY AUTL x131 @ 2.47; BUY CRDL x168 @ 1.93; BUY CRSP x5 @ 59.72; BUY CYPH x246 @ 1.32 |
| 2026-08-24 | -5.17 | $2,820.80 | AG×30, BHP×6, CDE×30, HDSN×109, IAG×32, KGC×21, NFGC×360, WPM×4, AU×2, AUPH×18, AEM×1, ARCT×29, AUTL×131, CRDL×168, CRSP×5, CYPH×246 | — | — | $2,820.80 | $7,551.69 | $10,372.49 | AG×30, BHP×6, CDE×30, HDSN×109, IAG×32, KGC×21, NFGC×360, WPM×4, AU×2, AUPH×18, AEM×1, ARCT×29, AUTL×131, CRDL×168, CRSP×5, CYPH×246 | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $2,820.80 | AG×30, BHP×6, CDE×30, HDSN×109, IAG×32, KGC×21, NFGC×360, WPM×4, AU×2, AUPH×18, AEM×1, ARCT×29, AUTL×131, CRDL×168, CRSP×5, CYPH×246 | MOS, OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $4,052.89 | $6,343.74 | $10,396.63 | AU×2, AUPH×18, AEM×1, ARCT×29, AUTL×131, CRDL×168, CRSP×5, CYPH×246, MOS×20, OCUL×45, INSP×8, CRMD×59, RZLT×94, HCA×1, BMEA×306, NPWR×247 | SELL AG (dropped from list after 3 sess (min 3)); SELL BHP (dropped from list after 3 sess (min 3)); SELL CDE (dropped from list after 3 sess (min 3)); SELL HDSN (dropped from list after 3 sess (min 3)); SELL IAG (dropped from list after 3 sess (min 3)); SELL KGC (dropped from list after 3 sess (min 3)); SELL NFGC (dropped from list after 3 sess (min 3)); SELL WPM (dropped from list after 3 sess (min 3)); BUY MOS x20 @ 24.00; BUY OCUL x45 @ 10.92; BUY INSP x8 @ 61.47; BUY CRMD x59 @ 8.28; BUY RZLT x94 @ 5.23; BUY HCA x1 @ 429.24; BUY BMEA x306 @ 1.62; BUY NPWR x247 @ 2.00 |
| 2026-08-26 | +2.02 | $4,052.89 | AU×2, AUPH×18, AEM×1, ARCT×29, AUTL×131, CRDL×168, CRSP×5, CYPH×246, MOS×20, OCUL×45, INSP×8, CRMD×59, RZLT×94, HCA×1, BMEA×306, NPWR×247 | — | — | $4,052.89 | $6,344.62 | $10,397.51 | AU×2, AUPH×18, AEM×1, ARCT×29, AUTL×131, CRDL×168, CRSP×5, CYPH×246, MOS×20, OCUL×45, INSP×8, CRMD×59, RZLT×94, HCA×1, BMEA×306, NPWR×247 | hold AU,AUPH,AEM,ARCT,AUTL,CRDL,CRSP,CYPH,MOS,OCUL,INSP,CRMD,RZLT,HCA,BMEA,NPWR |
| 2026-08-27 | — | $4,052.89 | AU×2, AUPH×18, AEM×1, ARCT×29, AUTL×131, CRDL×168, CRSP×5, CYPH×246, MOS×20, OCUL×45, INSP×8, CRMD×59, RZLT×94, HCA×1, BMEA×306, NPWR×247 | RRC, CRK, SLI, ACMR, GGB, MT | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $3,870.07 | $6,551.96 | $10,422.03 | MOS×20, OCUL×45, INSP×8, CRMD×59, RZLT×94, HCA×1, BMEA×306, NPWR×247, RRC×11, CRK×33, SLI×181, ACMR×5, GGB×106, MT×6 | SELL AU (dropped from list after 4 sess (min 3)); SELL AUPH (dropped from list after 4 sess (min 3)); SELL AEM (dropped from list after 4 sess (min 3)); SELL ARCT (dropped from list after 4 sess (min 3)); SELL AUTL (dropped from list after 4 sess (min 3)); SELL CRDL (dropped from list after 4 sess (min 3)); SELL CRSP (dropped from list after 4 sess (min 3)); SELL CYPH (dropped from list after 4 sess (min 3)); BUY RRC x11 @ 40.72; BUY CRK x33 @ 14.09; BUY SLI x181 @ 2.59; BUY ACMR x5 @ 80.97; BUY GGB x106 @ 4.42; BUY MT x6 @ 75.12 |
| 2026-08-28 | +0.75 | $3,870.07 | MOS×20, OCUL×45, INSP×8, CRMD×59, RZLT×94, HCA×1, BMEA×306, NPWR×247, RRC×11, CRK×33, SLI×181, ACMR×5, GGB×106, MT×6 | ANF, BHVN, BZ, CAPR | OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | $3,650.10 | $6,820.71 | $10,470.81 | MOS×20, RRC×11, CRK×33, SLI×181, ACMR×5, GGB×106, MT×6, ANF×6, BHVN×53, BZ×48, CAPR×98 | SELL OCUL (dropped from list after 3 sess (min 3)); SELL INSP (dropped from list after 3 sess (min 3)); SELL CRMD (dropped from list after 3 sess (min 3)); SELL RZLT (dropped from list after 3 sess (min 3)); SELL HCA (dropped from list after 3 sess (min 3)); SELL BMEA (dropped from list after 3 sess (min 3)); SELL NPWR (dropped from list after 3 sess (min 3)); BUY ANF x6 @ 144.70; BUY BHVN x53 @ 16.95; BUY BZ x48 @ 18.50; BUY CAPR x98 @ 9.19 |
| 2026-08-31 | -5.85 | $3,650.10 | MOS×20, RRC×11, CRK×33, SLI×181, ACMR×5, GGB×106, MT×6, ANF×6, BHVN×53, BZ×48, CAPR×98 | — | MOS | $4,123.03 | $6,188.84 | $10,311.87 | RRC×11, CRK×33, SLI×181, ACMR×5, GGB×106, MT×6, ANF×6, BHVN×53, BZ×48, CAPR×98 | SELL MOS (dropped from list after 4 sess (min 3)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $4,123.03 | RRC×11, CRK×33, SLI×181, ACMR×5, GGB×106, MT×6, ANF×6, BHVN×53, BZ×48, CAPR×98 | — | RRC, CRK, SLI, ACMR, GGB, MT | $6,816.09 | $3,499.63 | $10,315.72 | ANF×6, BHVN×53, BZ×48, CAPR×98 | SELL RRC (dropped from list after 3 sess (min 3)); SELL CRK (dropped from list after 3 sess (min 3)); SELL SLI (dropped from list after 3 sess (min 3)); SELL ACMR (dropped from list after 3 sess (min 3)); SELL GGB (dropped from list after 3 sess (min 3)); SELL MT (dropped from list after 3 sess (min 3)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $6,816.09 | ANF×6, BHVN×53, BZ×48, CAPR×98 | — | ANF, BHVN, BZ, CAPR | $10,360.47 | $0.00 | $10,360.47 | — | SELL ANF (dropped from list after 3 sess (min 3)); SELL BHVN (dropped from list after 3 sess (min 3)); SELL BZ (dropped from list after 3 sess (min 3)); SELL CAPR (dropped from list after 3 sess (min 3)); hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $10,360.47 | — | ATRC, HRMY, CABA, VSTM, RVTY, GPRO, FRVO, CRK | — | $5,213.74 | $5,523.95 | $10,737.69 | ATRC×13, HRMY×15, CABA×198, VSTM×84, RVTY×5, GPRO×530, FRVO×35, CRK×41 | BUY ATRC x13 @ 49.76; BUY HRMY x15 @ 41.31; BUY CABA x198 @ 3.27; BUY VSTM x84 @ 7.70; BUY RVTY x5 @ 125.94; BUY GPRO x530 @ 1.22; BUY FRVO x35 @ 18.40; BUY CRK x41 @ 15.70 |
| 2026-09-04 | — | $5,213.74 | ATRC×13, HRMY×15, CABA×198, VSTM×84, RVTY×5, GPRO×530, FRVO×35, CRK×41 | ASND, OSCR, NVAX, BVS, BAK | — | $2,865.39 | $7,629.42 | $10,494.81 | ATRC×13, HRMY×15, CABA×198, VSTM×84, RVTY×5, GPRO×530, FRVO×35, CRK×41, ASND×1, OSCR×17, NVAX×50, BVS×35, BAK×267 | BUY ASND x1 @ 266.94; BUY OSCR x17 @ 30.65; BUY NVAX x50 @ 10.41; BUY BVS x35 @ 14.50; BUY BAK x267 @ 1.95 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 10 | $59.80 | $2.02 | — | $9,399.98 | ▼ $9,997.98 (-2.02) | deploy half leftover; list flatten; ⚪; ret5=-5.3; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 13 | $45.98 | $2.03 | — | $8,800.21 | ▼ $9,995.95 (-4.05) | deploy half leftover; list flatten; ⚪; ret5=+12.3; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 12 | $50.62 | $2.03 | — | $8,190.71 | ▼ $9,993.92 (-6.08) | deploy half leftover; list flatten; ⚪; ret5=+6.2; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 12 | $49.70 | $2.03 | — | $7,592.28 | ▼ $9,991.90 (-8.10) | deploy half leftover; list flatten; ⚪; ret5=-0.8; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 53 | $11.70 | $2.15 | — | $6,970.03 | ▼ $9,989.75 (-10.25) | deploy half leftover; list flatten; ⚪; ret5=-0.8; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 21 | $29.74 | $2.05 | — | $6,343.44 | ▼ $9,987.70 (-12.30) | deploy half leftover; list flatten; ⚪; ret5=-5.3; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 771 | $0.81 | $8.56 | — | $5,710.37 | ▼ $9,979.14 (-20.86) | deploy half leftover; list flatten; ⚪; ret5=+13.2; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 26 | $23.33 | $2.07 | — | $5,101.72 | ▼ $9,977.07 (-22.93) | deploy half leftover; list flatten; ⚪; ret5=+19.7; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 2 | $146.90 | $2.00 | — | $4,805.93 | ▲ $10,082.42 (+82.42) | deploy half leftover; list flatten; 🔵; ret5=+3.6; leftover $318.86 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 2 | $120.00 | $2.00 | — | $4,563.93 | ▲ $10,080.42 (+80.42) | deploy half leftover; list flatten; 🔵; ret5=+0.6; leftover $318.86 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 5 | $57.61 | $2.00 | — | $4,273.88 | ▲ $10,078.42 (+78.42) | deploy half leftover; list flatten; 🔵; ret5=+5.7; leftover $318.86 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 35 | $9.01 | $2.10 | — | $3,956.43 | ▲ $10,076.32 (+76.32) | deploy half leftover; list flatten; 🔵; ⚪; ret5=-13.5; leftover $318.86 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 340 | $0.94 | $4.21 | — | $3,633.64 | ▲ $10,072.11 (+72.11) | deploy half leftover; list flatten; 🔵; ⚪; ret5=+0.5; leftover $318.86 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 212 | $1.50 | $2.73 | — | $3,312.91 | ▲ $10,069.38 (+69.38) | deploy half leftover; list flatten; 🔵; ⚪; ret5=+9.2; leftover $318.86 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 4 | $46.18 | $1.86 | — | $3,126.33 | ▲ $10,194.82 (+194.82) | deploy half leftover; list flatten; 🔵; ret5=+6.7; leftover $207.06 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 1 | $142.77 | $1.43 | — | $2,982.13 | ▲ $10,193.39 (+193.39) | deploy half leftover; list flatten; 🔵; ret5=+5.8; leftover $207.06 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 1 | $202.70 | $1.99 | — | $2,777.44 | ▲ $10,191.40 (+191.40) | deploy half leftover; list flatten; 🔵; ret5=+8.3; leftover $207.06 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 51 | $4.05 | $2.14 | — | $2,568.74 | ▲ $10,189.25 (+189.25) | deploy half leftover; list flatten; 🔵; ⚪; ret5=-12.3; leftover $207.06 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 24 | $8.46 | $2.06 | — | $2,363.64 | ▲ $10,187.19 (+187.19) | deploy half leftover; list flatten; 🔵; ⚪; ret5=+0.4; leftover $207.06 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 2 | $90.54 | $1.82 | — | $2,180.75 | ▲ $10,185.38 (+185.38) | deploy half leftover; list flatten; ret5=-7.2; leftover $207.06 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 63 | $3.24 | $2.18 | — | $1,974.45 | ▲ $10,183.20 (+183.20) | deploy half leftover; list flatten; ⚪; ret5=+0.3; leftover $207.06 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 43 | $4.81 | $2.12 | — | $1,765.50 | ▲ $10,181.08 (+181.08) | deploy half leftover; list flatten; ⚪; ret5=-11.4; leftover $207.06 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 10 | $60.00 | $2.04 | $-2.06 | $2,363.46 | ▲ $10,143.50 (+143.50) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 13 | $43.56 | $2.05 | $-35.54 | $2,927.69 | ▲ $10,141.45 (+141.45) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 12 | $51.77 | $2.05 | $+9.69 | $3,546.88 | ▲ $10,139.40 (+139.40) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 12 | $49.28 | $2.05 | $-9.11 | $4,136.20 | ▲ $10,137.36 (+137.36) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 53 | $12.66 | $2.17 | $+46.56 | $4,805.01 | ▲ $10,135.19 (+135.19) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 21 | $27.85 | $2.07 | $-43.82 | $5,387.78 | ▲ $10,133.11 (+133.11) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 771 | $1.14 | $10.08 | $+235.79 | $6,256.64 | ▲ $10,123.03 (+123.03) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 26 | $22.16 | $2.09 | $-34.58 | $6,830.71 | ▲ $10,120.94 (+120.94) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `VST` | 2 | $140.74 | $2.02 | $-16.33 | $7,110.18 | ▲ $10,104.35 (+104.35) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `NRG` | 2 | $116.20 | $2.02 | $-11.61 | $7,340.56 | ▲ $10,102.33 (+102.33) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `SLG` | 5 | $57.50 | $2.02 | $-4.58 | $7,626.04 | ▲ $10,100.31 (+100.31) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `MARA` | 35 | $8.91 | $2.12 | $-7.71 | $7,935.77 | ▲ $10,098.19 (+98.19) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 340 | $0.88 | $4.08 | $-27.66 | $8,230.89 | ▲ $10,094.11 (+94.11) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 212 | $1.42 | $2.78 | $-22.47 | $8,529.15 | ▲ $10,091.33 (+91.33) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DVN` | 4 | $49.02 | $1.99 | $+7.51 | $8,723.24 | ▲ $10,100.56 (+100.56) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `EOG` | 1 | $151.45 | $1.54 | $+5.71 | $8,873.15 | ▲ $10,099.02 (+99.02) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `FANG` | 1 | $213.51 | $2.01 | $+6.80 | $9,084.65 | ▲ $10,097.01 (+97.01) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 51 | $3.92 | $2.16 | $-10.94 | $9,282.41 | ▲ $10,094.85 (+94.85) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGB` | 24 | $8.35 | $2.08 | $-6.78 | $9,480.72 | ▲ $10,092.76 (+92.76) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `ELF` | 2 | $98.15 | $1.99 | $+11.41 | $9,675.03 | ▲ $10,090.77 (+90.77) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 63 | $3.20 | $2.20 | $-6.90 | $9,874.44 | ▲ $10,088.58 (+88.58) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `HNST` | 43 | $4.98 | $2.14 | $+3.05 | $10,086.44 | ▲ $10,086.44 (+86.44) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 30 | $20.55 | $2.08 | — | $9,467.86 | ▲ $10,084.36 (+84.36) | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $630.40 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 6 | $91.01 | $2.01 | — | $8,919.79 | ▲ $10,082.35 (+82.35) | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $630.40 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 30 | $20.65 | $2.08 | — | $8,298.21 | ▲ $10,080.27 (+80.27) | deploy half leftover; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $630.40 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 109 | $5.77 | $2.32 | — | $7,666.96 | ▲ $10,077.95 (+77.95) | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $630.40 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 32 | $19.63 | $2.09 | — | $7,036.72 | ▲ $10,075.87 (+75.87) | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $630.40 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 21 | $29.63 | $2.05 | — | $6,412.43 | ▲ $10,073.81 (+73.81) | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $630.40 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 360 | $1.75 | $4.64 | — | $5,777.79 | ▲ $10,069.17 (+69.17) | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $630.40 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 4 | $144.54 | $2.00 | — | $5,197.63 | ▲ $10,067.17 (+67.17) | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $630.40 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 2 | $119.43 | $2.00 | — | $4,956.77 | ▲ $10,313.69 (+313.69) | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $324.85 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 18 | $17.20 | $2.04 | — | $4,645.13 | ▲ $10,311.65 (+311.65) | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $324.85 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 1 | $216.30 | $1.99 | — | $4,426.83 | ▲ $10,309.65 (+309.65) | deploy half leftover; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $324.85 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 29 | $11.13 | $2.08 | — | $4,101.99 | ▲ $10,307.58 (+307.58) | deploy half leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $324.85 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 131 | $2.47 | $2.38 | — | $3,776.03 | ▲ $10,305.19 (+305.19) | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $324.85 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 168 | $1.93 | $2.49 | — | $3,449.30 | ▲ $10,302.70 (+302.70) | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $324.85 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 5 | $59.72 | $2.00 | — | $3,148.69 | ▲ $10,300.69 (+300.69) | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $324.85 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 246 | $1.32 | $3.17 | — | $2,820.80 | ▲ $10,297.52 (+297.52) | deploy half leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $324.85 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 30 | $20.73 | $2.10 | $+1.22 | $3,440.60 | ▲ $10,434.65 (+434.65) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 6 | $95.95 | $2.03 | $+25.60 | $4,014.27 | ▲ $10,432.62 (+432.62) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 30 | $20.85 | $2.10 | $+1.82 | $4,637.67 | ▲ $10,430.52 (+430.52) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 109 | $5.53 | $2.35 | $-30.82 | $5,238.10 | ▲ $10,428.18 (+428.18) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 32 | $21.63 | $2.11 | $+59.81 | $5,928.15 | ▲ $10,426.07 (+426.07) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 21 | $32.76 | $2.07 | $+61.60 | $6,614.04 | ▲ $10,424.00 (+424.00) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 360 | $1.91 | $4.71 | $+48.24 | $7,296.93 | ▲ $10,419.29 (+419.29) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 4 | $160.00 | $2.02 | $+57.82 | $7,934.90 | ▲ $10,417.26 (+417.26) | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 20 | $24.00 | $2.05 | — | $7,452.85 | ▲ $10,415.21 (+415.21) | deploy half leftover; list flatten; ⚪; ret5=+13.0; leftover $495.93 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 45 | $10.92 | $2.12 | — | $6,959.33 | ▲ $10,413.09 (+413.09) | deploy half leftover; list flatten; 🔵; ret5=+10.4; leftover $495.93 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 8 | $61.47 | $2.01 | — | $6,465.55 | ▲ $10,411.07 (+411.07) | deploy half leftover; list flatten; 🔵; ret5=+9.2; leftover $495.93 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 59 | $8.28 | $2.17 | — | $5,974.87 | ▲ $10,408.91 (+408.91) | deploy half leftover; list flatten; 🔵; ⚪; ret5=+8.8; leftover $495.93 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 94 | $5.23 | $2.27 | — | $5,480.98 | ▲ $10,406.64 (+406.64) | deploy half leftover; list flatten; ret5=+10.7; leftover $495.93 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 1 | $429.24 | $1.99 | — | $5,049.74 | ▲ $10,404.64 (+404.64) | deploy half leftover; list flatten; ret5=+6.1; leftover $495.93 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 306 | $1.62 | $3.95 | — | $4,550.07 | ▲ $10,400.69 (+400.69) | deploy half leftover; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $495.93 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 247 | $2.00 | $3.19 | — | $4,052.89 | ▲ $10,397.51 (+397.51) | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $495.93 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `AU` | 2 | $119.80 | $2.02 | $-3.27 | $4,290.47 | ▲ $10,486.13 (+486.13) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `AUPH` | 18 | $16.60 | $2.06 | $-14.91 | $4,587.21 | ▲ $10,484.07 (+484.07) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `AEM` | 1 | $219.50 | $2.01 | $-0.81 | $4,804.70 | ▲ $10,482.06 (+482.06) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 29 | $15.35 | $2.10 | $+118.21 | $5,247.75 | ▲ $10,479.96 (+479.96) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `AUTL` | 131 | $2.41 | $2.41 | $-12.66 | $5,561.04 | ▲ $10,477.54 (+477.54) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 168 | $2.03 | $2.53 | $+11.77 | $5,899.55 | ▲ $10,475.01 (+475.01) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRSP` | 5 | $60.18 | $2.02 | $-1.73 | $6,198.43 | ▲ $10,472.99 (+472.99) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 246 | $1.60 | $3.22 | $+62.48 | $6,588.80 | ▲ $10,469.76 (+469.76) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 11 | $40.72 | $2.02 | — | $6,138.86 | ▲ $10,467.74 (+467.74) | deploy half leftover; list flatten; ret5=+1.8; leftover $470.63 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 33 | $14.09 | $2.09 | — | $5,671.80 | ▲ $10,465.65 (+465.65) | deploy half leftover; list flatten; ret5=+1.1; leftover $470.63 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 181 | $2.59 | $2.53 | — | $5,200.48 | ▲ $10,463.12 (+463.12) | deploy half leftover; list flatten; ret5=+4.2; leftover $470.63 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 5 | $80.97 | $2.00 | — | $4,793.62 | ▲ $10,461.11 (+461.11) | deploy half leftover; list mover_buy; 🔵; ret5=-1.3; leftover $470.63 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 106 | $4.42 | $2.31 | — | $4,322.79 | ▲ $10,458.80 (+458.80) | deploy half leftover; list mover_buy; 🔵; ret5=-8.6; leftover $470.63 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 6 | $75.12 | $2.01 | — | $3,870.07 | ▲ $10,456.80 (+456.80) | deploy half leftover; list mover_buy; 🔵; ret5=-2.2; leftover $470.63 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 45 | $10.63 | $2.15 | $-17.32 | $4,346.27 | ▲ $10,451.83 (+451.83) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 8 | $62.10 | $2.03 | $+0.99 | $4,841.04 | ▲ $10,449.80 (+449.80) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 59 | $8.49 | $2.19 | $+8.04 | $5,339.76 | ▲ $10,447.61 (+447.61) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 94 | $5.07 | $2.30 | $-19.61 | $5,814.04 | ▲ $10,445.31 (+445.31) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 1 | $424.61 | $2.01 | $-8.64 | $6,236.64 | ▲ $10,443.30 (+443.30) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 306 | $1.74 | $4.01 | $+28.76 | $6,765.07 | ▲ $10,439.29 (+439.29) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `NPWR` | 247 | $1.83 | $3.24 | $-48.41 | $7,213.84 | ▲ $10,436.05 (+436.05) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 6 | $144.70 | $2.01 | — | $6,343.64 | ▲ $10,434.05 (+434.05) | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $901.73 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 53 | $16.95 | $2.15 | — | $5,443.14 | ▲ $10,431.90 (+431.90) | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $901.73 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 48 | $18.50 | $2.13 | — | $4,553.00 | ▲ $10,429.76 (+429.76) | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $901.73 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 98 | $9.19 | $2.28 | — | $3,650.10 | ▲ $10,427.48 (+427.48) | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $901.73 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 20 | $23.75 | $2.07 | $-9.12 | $4,123.03 | ▲ $10,312.43 (+312.43) | dropped from list after 4 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `RRC` | 11 | $41.32 | $2.04 | $+2.53 | $4,575.51 | ▲ $10,356.73 (+356.73) | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRK` | 33 | $14.31 | $2.11 | $+3.06 | $5,045.63 | ▲ $10,354.62 (+354.62) | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 181 | $2.70 | $2.57 | $+14.80 | $5,531.75 | ▲ $10,352.04 (+352.04) | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `ACMR` | 5 | $71.24 | $2.02 | $-52.68 | $5,885.93 | ▲ $10,350.02 (+350.02) | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `GGB` | 106 | $4.61 | $2.34 | $+15.50 | $6,372.25 | ▲ $10,347.68 (+347.68) | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `MT` | 6 | $74.31 | $2.03 | $-8.90 | $6,816.09 | ▲ $10,345.66 (+345.66) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 6 | $142.00 | $2.03 | $-20.24 | $7,666.06 | ▲ $10,367.11 (+367.11) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BHVN` | 53 | $15.39 | $2.17 | $-87.00 | $8,479.56 | ▲ $10,364.94 (+364.94) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BZ` | 48 | $17.29 | $2.15 | $-62.37 | $9,307.32 | ▲ $10,362.78 (+362.78) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CAPR` | 98 | $10.77 | $2.31 | $+150.25 | $10,360.47 | ▲ $10,360.47 (+360.47) | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 13 | $49.76 | $2.03 | — | $9,711.57 | ▲ $10,358.45 (+358.45) | deploy half leftover; list flatten; 🔵; ⚪; ret5=+10.6; leftover $647.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 15 | $41.31 | $2.04 | — | $9,089.88 | ▲ $10,356.41 (+356.41) | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $647.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 198 | $3.27 | $2.58 | — | $8,439.84 | ▲ $10,353.83 (+353.83) | deploy half leftover; list flatten; 🔵; ⚪; ret5=+13.8; leftover $647.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 84 | $7.70 | $2.24 | — | $7,790.79 | ▲ $10,351.58 (+351.58) | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $647.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 5 | $125.94 | $2.00 | — | $7,159.09 | ▲ $10,349.58 (+349.58) | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $647.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 530 | $1.22 | $6.84 | — | $6,505.65 | ▲ $10,342.74 (+342.74) | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $647.53 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 35 | $18.40 | $2.10 | — | $5,859.56 | ▲ $10,340.65 (+340.65) | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $647.53 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 41 | $15.70 | $2.11 | — | $5,213.74 | ▲ $10,338.53 (+338.53) | deploy half leftover; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $647.53 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `ASND` | 1 | $266.94 | $1.99 | — | $4,944.81 | ▲ $10,814.95 (+814.95) | deploy half leftover; list flatten; ret5=+1.9; leftover $521.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OSCR` | 17 | $30.65 | $2.04 | — | $4,421.72 | ▲ $10,812.91 (+812.91) | deploy half leftover; list flatten; 🔵; ret5=-2.2; leftover $521.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 50 | $10.41 | $2.14 | — | $3,899.08 | ▲ $10,810.77 (+810.77) | deploy half leftover; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $521.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 35 | $14.50 | $2.10 | — | $3,389.49 | ▲ $10,808.68 (+808.68) | deploy half leftover; list flatten; 🔵; ⚪; ret5=+0.8; leftover $521.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 267 | $1.95 | $3.44 | — | $2,865.39 | ▲ $10,805.23 (+805.23) | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $521.37 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TLN` | cash | leftover split 318.86 < 1 share @ 359.83 |
| 2026-08-14 | `DAVE` | cash | leftover split 318.86 < 1 share @ 330.91 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `NRG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `SLG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `VST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NRG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `SLG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `DVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `EOG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `FANG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ELF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `HNST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `EOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `FANG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ELF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `HNST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRSP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AU` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AUPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AEM` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ARCT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AUTL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CRDL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CRSP` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `MU` | cash | leftover split 470.63 < 1 share @ 925.74 |
| 2026-08-28 | `ACMR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `GGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `MT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ACMR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `GGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `MT` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `FRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 13 | 2026-09-03 @ $49.76 | deploy half leftover; list flatten; 🔵; ⚪; ret5=+10.6; leftover $647.53 |
| `HRMY` | 15 | 2026-09-03 @ $41.31 | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $647.53 |
| `CABA` | 198 | 2026-09-03 @ $3.27 | deploy half leftover; list flatten; 🔵; ⚪; ret5=+13.8; leftover $647.53 |
| `VSTM` | 84 | 2026-09-03 @ $7.70 | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $647.53 |
| `RVTY` | 5 | 2026-09-03 @ $125.94 | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $647.53 |
| `GPRO` | 530 | 2026-09-03 @ $1.22 | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $647.53 |
| `FRVO` | 35 | 2026-09-03 @ $18.40 | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $647.53 |
| `CRK` | 41 | 2026-09-03 @ $15.70 | deploy half leftover; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $647.53 |
| `ASND` | 1 | 2026-09-04 @ $266.94 | deploy half leftover; list flatten; ret5=+1.9; leftover $521.37 |
| `OSCR` | 17 | 2026-09-04 @ $30.65 | deploy half leftover; list flatten; 🔵; ret5=-2.2; leftover $521.37 |
| `NVAX` | 50 | 2026-09-04 @ $10.41 | deploy half leftover; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $521.37 |
| `BVS` | 35 | 2026-09-04 @ $14.50 | deploy half leftover; list flatten; 🔵; ⚪; ret5=+0.8; leftover $521.37 |
| `BAK` | 267 | 2026-09-04 @ $1.95 | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $521.37 |
