# Factor mine action — `union_h5_half`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `half` · sell `list` · S-boost `none` · deploy half leftover

Cash book **+9.27%** ($10,927) · signal-only (no cash/fees) was +58.01%. Starts YES **14/17**. Fills 122 · skips 269 · realized $+786.41.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `half` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $3,800.01.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $5,101.72 | $4,969.43 | $10,071.15 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26 | BUY BTSG x10 @ 59.80; BUY IREN x13 @ 45.98; BUY TPG x12 @ 50.62; BUY TGTX x12 @ 49.70; BUY SLS x53 @ 11.70; BUY HIMS x21 @ 29.74; BUY INO x771 @ 0.81; BUY TNDM x26 @ 23.33 |
| 2026-08-14 | +5.50 | $5,101.72 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26 | VST, NRG, SLG, MARA, LDI, BTBT | — | $3,312.91 | $6,899.73 | $10,212.64 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26, VST×2, NRG×2, SLG×5, MARA×35, LDI×340, BTBT×212 | BUY VST x2 @ 146.90; BUY NRG x2 @ 120.00; BUY SLG x5 @ 57.61; BUY MARA x35 @ 9.01; BUY LDI x340 @ 0.94; BUY BTBT x212 @ 1.50 |
| 2026-08-17 | +2.25 | $3,312.91 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26, VST×2, NRG×2, SLG×5, MARA×35, LDI×340, BTBT×212 | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST | — | $1,765.50 | $8,485.46 | $10,250.96 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26, VST×2, NRG×2, SLG×5, MARA×35, LDI×340, BTBT×212, DVN×4, EOG×1, FANG×1, TMC×51, TGB×24, ELF×2, DNN×63, HNST×43 | BUY DVN x4 @ 46.18; BUY EOG x1 @ 142.77; BUY FANG x1 @ 202.70; BUY TMC x51 @ 4.05; BUY TGB x24 @ 8.46; BUY ELF x2 @ 90.54; BUY DNN x63 @ 3.24; BUY HNST x43 @ 4.81 |
| 2026-08-18 | -6.20 | $1,765.50 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26, VST×2, NRG×2, SLG×5, MARA×35, LDI×340, BTBT×212, DVN×4, EOG×1, FANG×1, TMC×51, TGB×24, ELF×2, DNN×63, HNST×43 | — | — | $1,765.50 | $8,427.43 | $10,192.93 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26, VST×2, NRG×2, SLG×5, MARA×35, LDI×340, BTBT×212, DVN×4, EOG×1, FANG×1, TMC×51, TGB×24, ELF×2, DNN×63, HNST×43 | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $1,765.50 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26, VST×2, NRG×2, SLG×5, MARA×35, LDI×340, BTBT×212, DVN×4, EOG×1, FANG×1, TMC×51, TGB×24, ELF×2, DNN×63, HNST×43 | — | — | $1,765.50 | $8,729.62 | $10,495.12 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26, VST×2, NRG×2, SLG×5, MARA×35, LDI×340, BTBT×212, DVN×4, EOG×1, FANG×1, TMC×51, TGB×24, ELF×2, DNN×63, HNST×43 | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $1,765.50 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26, VST×2, NRG×2, SLG×5, MARA×35, LDI×340, BTBT×212, DVN×4, EOG×1, FANG×1, TMC×51, TGB×24, ELF×2, DNN×63, HNST×43 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $3,670.69 | $6,912.16 | $10,582.85 | VST×2, NRG×2, SLG×5, MARA×35, LDI×340, BTBT×212, DVN×4, EOG×1, FANG×1, TMC×51, TGB×24, ELF×2, DNN×63, HNST×43, AG×21, BHP×4, CDE×21, HDSN×77, IAG×22, KGC×15, NFGC×254, WPM×3 | SELL BTSG (dropped from list after 5 sess (min 5)); SELL IREN (dropped from list after 5 sess (min 5)); SELL TPG (dropped from list after 5 sess (min 5)); SELL TGTX (dropped from list after 5 sess (min 5)); SELL SLS (dropped from list after 5 sess (min 5)); SELL HIMS (dropped from list after 5 sess (min 5)); SELL INO (dropped from list after 5 sess (min 5)); SELL TNDM (dropped from list after 5 sess (min 5)); BUY AG x21 @ 20.55; BUY BHP x4 @ 91.01; BUY CDE x21 @ 20.65; BUY HDSN x77 @ 5.77; BUY IAG x22 @ 19.63; BUY KGC x15 @ 29.63; BUY NFGC x254 @ 1.75; BUY WPM x3 @ 144.54 |
| 2026-08-21 | +3.25 | $3,670.69 | VST×2, NRG×2, SLG×5, MARA×35, LDI×340, BTBT×212, DVN×4, EOG×1, FANG×1, TMC×51, TGB×24, ELF×2, DNN×63, HNST×43, AG×21, BHP×4, CDE×21, HDSN×77, IAG×22, KGC×15, NFGC×254, WPM×3 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | VST, NRG, SLG, MARA, LDI, BTBT | $3,036.72 | $7,797.81 | $10,834.53 | DVN×4, EOG×1, FANG×1, TMC×51, TGB×24, ELF×2, DNN×63, HNST×43, AG×21, BHP×4, CDE×21, HDSN×77, IAG×22, KGC×15, NFGC×254, WPM×3, AU×2, AUPH×20, AEM×1, ARCT×30, AUTL×139, CRDL×178, CRSP×5, CYPH×261 | SELL VST (dropped from list after 5 sess (min 5)); SELL NRG (dropped from list after 5 sess (min 5)); SELL SLG (dropped from list after 5 sess (min 5)); SELL MARA (dropped from list after 5 sess (min 5)); SELL LDI (dropped from list after 5 sess (min 5)); SELL BTBT (dropped from list after 5 sess (min 5)); BUY AU x2 @ 119.43; BUY AUPH x20 @ 17.20; BUY AEM x1 @ 216.30; BUY ARCT x30 @ 11.13; BUY AUTL x139 @ 2.47; BUY CRDL x178 @ 1.93; BUY CRSP x5 @ 59.72; BUY CYPH x261 @ 1.32 |
| 2026-08-24 | -5.17 | $3,036.72 | DVN×4, EOG×1, FANG×1, TMC×51, TGB×24, ELF×2, DNN×63, HNST×43, AG×21, BHP×4, CDE×21, HDSN×77, IAG×22, KGC×15, NFGC×254, WPM×3, AU×2, AUPH×20, AEM×1, ARCT×30, AUTL×139, CRDL×178, CRSP×5, CYPH×261 | — | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST | $4,674.03 | $6,151.37 | $10,825.40 | AG×21, BHP×4, CDE×21, HDSN×77, IAG×22, KGC×15, NFGC×254, WPM×3, AU×2, AUPH×20, AEM×1, ARCT×30, AUTL×139, CRDL×178, CRSP×5, CYPH×261 | SELL DVN (dropped from list after 5 sess (min 5)); SELL EOG (dropped from list after 5 sess (min 5)); SELL FANG (dropped from list after 5 sess (min 5)); SELL TMC (dropped from list after 5 sess (min 5)); SELL TGB (dropped from list after 5 sess (min 5)); SELL ELF (dropped from list after 5 sess (min 5)); SELL DNN (dropped from list after 5 sess (min 5)); SELL HNST (dropped from list after 5 sess (min 5)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $4,674.03 | AG×21, BHP×4, CDE×21, HDSN×77, IAG×22, KGC×15, NFGC×254, WPM×3, AU×2, AUPH×20, AEM×1, ARCT×30, AUTL×139, CRDL×178, CRSP×5, CYPH×261 | MOS, OCUL, INSP, CRMD, RZLT, BMEA, NPWR | — | $2,679.88 | $8,164.20 | $10,844.08 | AG×21, BHP×4, CDE×21, HDSN×77, IAG×22, KGC×15, NFGC×254, WPM×3, AU×2, AUPH×20, AEM×1, ARCT×30, AUTL×139, CRDL×178, CRSP×5, CYPH×261, MOS×12, OCUL×26, INSP×4, CRMD×35, RZLT×55, BMEA×180, NPWR×146 | BUY MOS x12 @ 24.00; BUY OCUL x26 @ 10.92; BUY INSP x4 @ 61.47; BUY CRMD x35 @ 8.28; BUY RZLT x55 @ 5.23; BUY BMEA x180 @ 1.62; BUY NPWR x146 @ 2.00 |
| 2026-08-26 | +2.02 | $2,679.88 | AG×21, BHP×4, CDE×21, HDSN×77, IAG×22, KGC×15, NFGC×254, WPM×3, AU×2, AUPH×20, AEM×1, ARCT×30, AUTL×139, CRDL×178, CRSP×5, CYPH×261, MOS×12, OCUL×26, INSP×4, CRMD×35, RZLT×55, BMEA×180, NPWR×146 | — | — | $2,679.88 | $8,187.22 | $10,867.10 | AG×21, BHP×4, CDE×21, HDSN×77, IAG×22, KGC×15, NFGC×254, WPM×3, AU×2, AUPH×20, AEM×1, ARCT×30, AUTL×139, CRDL×178, CRSP×5, CYPH×261, MOS×12, OCUL×26, INSP×4, CRMD×35, RZLT×55, BMEA×180, NPWR×146 | hold AG,BHP,CDE,HDSN,IAG,KGC,NFGC,WPM,AU,AUPH,AEM,ARCT,AUTL,CRDL,CRSP,CYPH,MOS,OCUL,INSP,CRMD,RZLT,BMEA,NPWR |
| 2026-08-27 | — | $2,679.88 | AG×21, BHP×4, CDE×21, HDSN×77, IAG×22, KGC×15, NFGC×254, WPM×3, AU×2, AUPH×20, AEM×1, ARCT×30, AUTL×139, CRDL×178, CRSP×5, CYPH×261, MOS×12, OCUL×26, INSP×4, CRMD×35, RZLT×55, BMEA×180, NPWR×146 | RRC, CRK, SLI, ACMR, GGB, MT | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $3,736.25 | $7,219.12 | $10,955.37 | AU×2, AUPH×20, AEM×1, ARCT×30, AUTL×139, CRDL×178, CRSP×5, CYPH×261, MOS×12, OCUL×26, INSP×4, CRMD×35, RZLT×55, BMEA×180, NPWR×146, RRC×11, CRK×31, SLI×173, ACMR×5, GGB×101, MT×5 | SELL AG (dropped from list after 5 sess (min 5)); SELL BHP (dropped from list after 5 sess (min 5)); SELL CDE (dropped from list after 5 sess (min 5)); SELL HDSN (dropped from list after 5 sess (min 5)); SELL IAG (dropped from list after 5 sess (min 5)); SELL KGC (dropped from list after 5 sess (min 5)); SELL NFGC (dropped from list after 5 sess (min 5)); SELL WPM (dropped from list after 5 sess (min 5)); BUY RRC x11 @ 40.72; BUY CRK x31 @ 14.09; BUY SLI x173 @ 2.59; BUY ACMR x5 @ 80.97; BUY GGB x101 @ 4.42; BUY MT x5 @ 75.12 |
| 2026-08-28 | +0.75 | $3,736.25 | AU×2, AUPH×20, AEM×1, ARCT×30, AUTL×139, CRDL×178, CRSP×5, CYPH×261, MOS×12, OCUL×26, INSP×4, CRMD×35, RZLT×55, BMEA×180, NPWR×146, RRC×11, CRK×31, SLI×173, ACMR×5, GGB×101, MT×5 | ANF, BHVN, BZ, CAPR | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $3,291.29 | $7,706.70 | $10,997.99 | MOS×12, OCUL×26, INSP×4, CRMD×35, RZLT×55, BMEA×180, NPWR×146, RRC×11, CRK×31, SLI×173, ACMR×5, GGB×101, MT×5, ANF×5, BHVN×47, BZ×43, CAPR×87 | SELL AU (dropped from list after 5 sess (min 5)); SELL AUPH (dropped from list after 5 sess (min 5)); SELL AEM (dropped from list after 5 sess (min 5)); SELL ARCT (dropped from list after 5 sess (min 5)); SELL AUTL (dropped from list after 5 sess (min 5)); SELL CRDL (dropped from list after 5 sess (min 5)); SELL CRSP (dropped from list after 5 sess (min 5)); SELL CYPH (dropped from list after 5 sess (min 5)); BUY ANF x5 @ 144.70; BUY BHVN x47 @ 16.95; BUY BZ x43 @ 18.50; BUY CAPR x87 @ 9.19 |
| 2026-08-31 | -5.85 | $3,291.29 | MOS×12, OCUL×26, INSP×4, CRMD×35, RZLT×55, BMEA×180, NPWR×146, RRC×11, CRK×31, SLI×173, ACMR×5, GGB×101, MT×5, ANF×5, BHVN×47, BZ×43, CAPR×87 | — | — | $3,291.29 | $7,526.39 | $10,817.68 | MOS×12, OCUL×26, INSP×4, CRMD×35, RZLT×55, BMEA×180, NPWR×146, RRC×11, CRK×31, SLI×173, ACMR×5, GGB×101, MT×5, ANF×5, BHVN×47, BZ×43, CAPR×87 | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $3,291.29 | MOS×12, OCUL×26, INSP×4, CRMD×35, RZLT×55, BMEA×180, NPWR×146, RRC×11, CRK×31, SLI×173, ACMR×5, GGB×101, MT×5, ANF×5, BHVN×47, BZ×43, CAPR×87 | — | MOS, OCUL, INSP, CRMD, RZLT, BMEA, NPWR | $5,192.69 | $5,640.77 | $10,833.46 | RRC×11, CRK×31, SLI×173, ACMR×5, GGB×101, MT×5, ANF×5, BHVN×47, BZ×43, CAPR×87 | SELL MOS (dropped from list after 5 sess (min 5)); SELL OCUL (dropped from list after 5 sess (min 5)); SELL INSP (dropped from list after 5 sess (min 5)); SELL CRMD (dropped from list after 5 sess (min 5)); SELL RZLT (dropped from list after 5 sess (min 5)); SELL BMEA (dropped from list after 5 sess (min 5)); SELL NPWR (dropped from list after 5 sess (min 5)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $5,192.69 | RRC×11, CRK×31, SLI×173, ACMR×5, GGB×101, MT×5, ANF×5, BHVN×47, BZ×43, CAPR×87 | — | — | $5,192.69 | $5,652.93 | $10,845.62 | RRC×11, CRK×31, SLI×173, ACMR×5, GGB×101, MT×5, ANF×5, BHVN×47, BZ×43, CAPR×87 | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $5,192.69 | RRC×11, CRK×31, SLI×173, ACMR×5, GGB×101, MT×5, ANF×5, BHVN×47, BZ×43, CAPR×87 | ATRC, HRMY, CABA, VSTM, RVTY, GPRO, FRVO | RRC, SLI, ACMR, GGB, MT | $3,697.20 | $7,398.83 | $11,096.03 | CRK×31, ANF×5, BHVN×47, BZ×43, CAPR×87, ATRC×10, HRMY×12, CABA×159, VSTM×67, RVTY×4, GPRO×426, FRVO×28 | SELL RRC (dropped from list after 5 sess (min 5)); SELL SLI (dropped from list after 5 sess (min 5)); SELL ACMR (dropped from list after 5 sess (min 5)); SELL GGB (dropped from list after 5 sess (min 5)); SELL MT (dropped from list after 5 sess (min 5)); BUY ATRC x10 @ 49.76; BUY HRMY x12 @ 41.31; BUY CABA x159 @ 3.27; BUY VSTM x67 @ 7.70; BUY RVTY x4 @ 125.94; BUY GPRO x426 @ 1.22; BUY FRVO x28 @ 18.40 |
| 2026-09-04 | — | $3,697.20 | CRK×31, ANF×5, BHVN×47, BZ×43, CAPR×87, ATRC×10, HRMY×12, CABA×159, VSTM×67, RVTY×4, GPRO×426, FRVO×28 | ASND, OSCR, NVAX, BVS, BAK | CRK, ANF, BHVN, BZ, CAPR | $3,800.01 | $7,126.76 | $10,926.77 | ATRC×10, HRMY×12, CABA×159, VSTM×67, RVTY×4, GPRO×426, FRVO×28, ASND×2, OSCR×23, NVAX×69, BVS×49, BAK×369 | SELL CRK (dropped from list after 6 sess (min 5)); SELL ANF (dropped from list after 5 sess (min 5)); SELL BHVN (dropped from list after 5 sess (min 5)); SELL BZ (dropped from list after 5 sess (min 5)); SELL CAPR (dropped from list after 5 sess (min 5)); BUY ASND x2 @ 266.94; BUY OSCR x23 @ 30.65; BUY NVAX x69 @ 10.41; BUY BVS x49 @ 14.50; BUY BAK x369 @ 1.95 |

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
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 10 | $58.64 | $2.04 | $-15.66 | $2,349.86 | ▲ $10,486.02 (+486.02) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 13 | $42.46 | $2.05 | $-49.84 | $2,899.79 | ▲ $10,483.97 (+483.97) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 12 | $53.06 | $2.05 | $+25.17 | $3,534.46 | ▲ $10,481.92 (+481.92) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGTX` | 12 | $51.65 | $2.05 | $+19.33 | $4,152.22 | ▲ $10,479.88 (+479.88) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 53 | $13.84 | $2.17 | $+109.10 | $4,883.57 | ▲ $10,477.71 (+477.71) | dropped from list after 5 sess (min 5) | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SELL** | `HIMS` | 21 | $30.66 | $2.07 | $+15.19 | $5,525.35 | ▲ $10,475.63 (+475.63) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `INO` | 771 | $1.30 | $10.08 | $+359.15 | $6,517.57 | ▲ $10,465.55 (+465.55) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 26 | $23.11 | $2.09 | $-9.88 | $7,116.34 | ▲ $10,463.46 (+463.46) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 21 | $20.55 | $2.05 | — | $6,682.74 | ▲ $10,461.41 (+461.41) | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $444.77 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 4 | $91.01 | $2.00 | — | $6,316.70 | ▲ $10,459.41 (+459.41) | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $444.77 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 21 | $20.65 | $2.05 | — | $5,880.99 | ▲ $10,457.35 (+457.35) | deploy half leftover; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $444.77 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 77 | $5.77 | $2.22 | — | $5,434.48 | ▲ $10,455.13 (+455.13) | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $444.77 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 22 | $19.63 | $2.06 | — | $5,000.57 | ▲ $10,453.08 (+453.08) | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $444.77 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 15 | $29.63 | $2.04 | — | $4,554.08 | ▲ $10,451.04 (+451.04) | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $444.77 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 254 | $1.75 | $3.28 | — | $4,106.31 | ▲ $10,447.77 (+447.77) | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $444.77 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 3 | $144.54 | $2.00 | — | $3,670.69 | ▲ $10,445.77 (+445.77) | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $444.77 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `VST` | 2 | $139.99 | $2.02 | $-17.83 | $3,948.65 | ▲ $10,736.60 (+736.60) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `NRG` | 2 | $116.58 | $2.02 | $-10.85 | $4,179.79 | ▲ $10,734.58 (+734.58) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `SLG` | 5 | $58.63 | $2.02 | $+1.07 | $4,470.92 | ▲ $10,732.56 (+732.56) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 35 | $11.70 | $2.12 | $+89.94 | $4,878.30 | ▲ $10,730.44 (+730.44) | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 340 | $0.87 | $4.03 | $-32.04 | $5,169.05 | ▲ $10,726.41 (+726.41) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 212 | $1.66 | $2.78 | $+28.41 | $5,518.19 | ▲ $10,723.63 (+723.63) | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 2 | $119.43 | $2.00 | — | $5,277.33 | ▲ $10,721.63 (+721.63) | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $344.89 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 20 | $17.20 | $2.05 | — | $4,931.28 | ▲ $10,719.58 (+719.58) | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $344.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 1 | $216.30 | $1.99 | — | $4,712.99 | ▲ $10,717.59 (+717.59) | deploy half leftover; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $344.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 30 | $11.13 | $2.08 | — | $4,377.01 | ▲ $10,715.51 (+715.51) | deploy half leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $344.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 139 | $2.47 | $2.41 | — | $4,031.27 | ▲ $10,713.10 (+713.10) | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $344.89 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 178 | $1.93 | $2.52 | — | $3,685.21 | ▲ $10,710.58 (+710.58) | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $344.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 5 | $59.72 | $2.00 | — | $3,384.61 | ▲ $10,708.58 (+708.58) | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $344.89 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 261 | $1.32 | $3.37 | — | $3,036.72 | ▲ $10,705.21 (+705.21) | deploy half leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $344.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `DVN` | 4 | $48.84 | $1.99 | $+6.80 | $3,230.09 | ▲ $10,952.47 (+952.47) | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `EOG` | 1 | $152.61 | $1.55 | $+6.86 | $3,381.15 | ▲ $10,950.92 (+950.92) | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `FANG` | 1 | $209.47 | $2.01 | $+2.76 | $3,588.61 | ▲ $10,948.91 (+948.91) | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 51 | $4.57 | $2.16 | $+22.21 | $3,819.52 | ▲ $10,946.75 (+946.75) | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `TGB` | 24 | $9.26 | $2.08 | $+15.06 | $4,039.68 | ▲ $10,944.67 (+944.67) | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `ELF` | 2 | $101.53 | $2.02 | $+18.15 | $4,240.72 | ▲ $10,942.65 (+942.65) | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 63 | $3.50 | $2.20 | $+12.00 | $4,459.02 | ▲ $10,940.45 (+940.45) | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟡 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `HNST` | 43 | $5.05 | $2.14 | $+6.06 | $4,674.03 | ▲ $10,938.31 (+938.31) | dropped from list after 5 sess (min 5) | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 12 | $24.00 | $2.03 | — | $4,384.01 | ▲ $10,880.38 (+880.38) | deploy half leftover; list flatten; ⚪; ret5=+13.0; leftover $292.13 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 26 | $10.92 | $2.07 | — | $4,098.02 | ▲ $10,878.31 (+878.31) | deploy half leftover; list flatten; 🔵; ret5=+10.4; leftover $292.13 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 4 | $61.47 | $2.00 | — | $3,850.14 | ▲ $10,876.31 (+876.31) | deploy half leftover; list flatten; 🔵; ret5=+9.2; leftover $292.13 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 35 | $8.28 | $2.10 | — | $3,558.24 | ▲ $10,874.21 (+874.21) | deploy half leftover; list flatten; 🔵; ⚪; ret5=+8.8; leftover $292.13 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 55 | $5.23 | $2.15 | — | $3,268.44 | ▲ $10,872.06 (+872.06) | deploy half leftover; list flatten; ret5=+10.7; leftover $292.13 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 180 | $1.62 | $2.53 | — | $2,974.31 | ▲ $10,869.53 (+869.53) | deploy half leftover; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $292.13 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 146 | $2.00 | $2.43 | — | $2,679.88 | ▲ $10,867.10 (+867.10) | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $292.13 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 21 | $20.63 | $2.07 | $-2.45 | $3,111.03 | ▲ $10,983.29 (+983.29) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 4 | $96.99 | $2.02 | $+19.90 | $3,496.97 | ▲ $10,981.27 (+981.27) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 21 | $21.00 | $2.07 | $+3.22 | $3,935.90 | ▲ $10,979.20 (+979.20) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 77 | $5.51 | $2.24 | $-24.48 | $4,357.93 | ▲ $10,976.96 (+976.96) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 22 | $21.64 | $2.08 | $+40.09 | $4,831.93 | ▲ $10,974.88 (+974.88) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 15 | $32.90 | $2.06 | $+44.96 | $5,323.37 | ▲ $10,972.82 (+972.82) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 254 | $2.00 | $3.33 | $+56.89 | $5,828.05 | ▲ $10,969.50 (+969.50) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 3 | $160.93 | $2.02 | $+45.15 | $6,308.82 | ▲ $10,967.48 (+967.48) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 11 | $40.72 | $2.02 | — | $5,858.87 | ▲ $10,965.45 (+965.45) | deploy half leftover; list flatten; ret5=+1.8; leftover $450.63 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 31 | $14.09 | $2.08 | — | $5,420.00 | ▲ $10,963.37 (+963.37) | deploy half leftover; list flatten; ret5=+1.1; leftover $450.63 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 173 | $2.59 | $2.51 | — | $4,969.42 | ▲ $10,960.86 (+960.86) | deploy half leftover; list flatten; ret5=+4.2; leftover $450.63 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 5 | $80.97 | $2.00 | — | $4,562.57 | ▲ $10,958.86 (+958.86) | deploy half leftover; list mover_buy; 🔵; ret5=-1.3; leftover $450.63 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 101 | $4.42 | $2.29 | — | $4,113.85 | ▲ $10,956.56 (+956.56) | deploy half leftover; list mover_buy; 🔵; ret5=-8.6; leftover $450.63 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 5 | $75.12 | $2.00 | — | $3,736.25 | ▲ $10,954.56 (+954.56) | deploy half leftover; list mover_buy; 🔵; ret5=-2.2; leftover $450.63 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 2 | $117.41 | $2.02 | $-8.05 | $3,969.05 | ▲ $10,995.95 (+995.95) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 20 | $16.47 | $2.07 | $-18.72 | $4,296.38 | ▲ $10,993.88 (+993.88) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AEM` | 1 | $214.11 | $2.01 | $-6.20 | $4,508.48 | ▲ $10,991.87 (+991.87) | dropped from list after 5 sess (min 5) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 30 | $15.74 | $2.10 | $+134.12 | $4,978.58 | ▲ $10,989.77 (+989.77) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 139 | $2.32 | $2.44 | $-25.70 | $5,298.62 | ▲ $10,987.33 (+987.33) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 178 | $2.09 | $2.56 | $+23.39 | $5,668.08 | ▲ $10,984.77 (+984.77) | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `CRSP` | 5 | $59.12 | $2.02 | $-7.03 | $5,961.65 | ▲ $10,982.74 (+982.74) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 261 | $1.75 | $3.42 | $+105.44 | $6,414.98 | ▲ $10,979.32 (+979.32) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 5 | $144.70 | $2.00 | — | $5,689.48 | ▲ $10,977.32 (+977.32) | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $801.87 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 47 | $16.95 | $2.13 | — | $4,890.69 | ▲ $10,975.18 (+975.18) | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $801.87 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 43 | $18.50 | $2.12 | — | $4,093.08 | ▲ $10,973.07 (+973.07) | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $801.87 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 87 | $9.19 | $2.25 | — | $3,291.29 | ▲ $10,970.81 (+970.81) | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $801.87 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `MOS` | 12 | $24.00 | $2.05 | $-4.07 | $3,577.25 | ▲ $10,857.53 (+857.53) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `OCUL` | 26 | $10.49 | $2.09 | $-15.34 | $3,847.90 | ▲ $10,855.44 (+855.44) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `INSP` | 4 | $63.05 | $2.02 | $+2.30 | $4,098.08 | ▲ $10,853.42 (+853.42) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 35 | $8.26 | $2.12 | $-4.91 | $4,385.06 | ▲ $10,851.30 (+851.30) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 55 | $4.69 | $2.17 | $-34.03 | $4,640.84 | ▲ $10,849.13 (+849.13) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `BMEA` | 180 | $1.65 | $2.57 | $+0.30 | $4,935.27 | ▲ $10,846.56 (+846.56) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `NPWR` | 146 | $1.78 | $2.46 | $-37.01 | $5,192.69 | ▲ $10,844.10 (+844.10) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 11 | $42.10 | $2.04 | $+11.11 | $5,653.74 | ▲ $10,862.80 (+862.80) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 173 | $2.49 | $2.55 | $-22.36 | $6,081.97 | ▲ $10,860.26 (+860.26) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `ACMR` | 5 | $70.52 | $2.02 | $-56.28 | $6,432.54 | ▲ $10,858.23 (+858.23) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `GGB` | 101 | $4.81 | $2.32 | $+34.78 | $6,916.03 | ▲ $10,855.91 (+855.91) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `MT` | 5 | $73.86 | $2.02 | $-10.33 | $7,283.31 | ▲ $10,853.89 (+853.89) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 10 | $49.76 | $2.02 | — | $6,783.69 | ▲ $10,851.87 (+851.87) | deploy half leftover; list flatten; 🔵; ⚪; ret5=+10.6; leftover $520.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 12 | $41.31 | $2.03 | — | $6,285.94 | ▲ $10,849.84 (+849.84) | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $520.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 159 | $3.27 | $2.47 | — | $5,763.54 | ▲ $10,847.37 (+847.37) | deploy half leftover; list flatten; 🔵; ⚪; ret5=+13.8; leftover $520.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 67 | $7.70 | $2.19 | — | $5,245.45 | ▲ $10,845.18 (+845.18) | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $520.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 4 | $125.94 | $2.00 | — | $4,739.69 | ▲ $10,843.18 (+843.18) | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $520.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 426 | $1.22 | $5.50 | — | $4,214.47 | ▲ $10,837.68 (+837.68) | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $520.24 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 28 | $18.40 | $2.07 | — | $3,697.20 | ▲ $10,835.61 (+835.61) | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $520.24 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 31 | $15.45 | $2.10 | $+37.97 | $4,174.05 | ▲ $11,167.70 (+1,167.70) | dropped from list after 6 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `ANF` | 5 | $137.70 | $2.02 | $-39.03 | $4,860.52 | ▲ $11,165.67 (+1,165.67) | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BHVN` | 47 | $15.89 | $2.15 | $-54.10 | $5,605.20 | ▲ $11,163.52 (+1,163.52) | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BZ` | 43 | $17.31 | $2.14 | $-55.43 | $6,347.39 | ▲ $11,161.38 (+1,161.38) | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `CAPR` | 87 | $9.83 | $2.28 | $+51.15 | $7,200.33 | ▲ $11,159.11 (+1,159.11) | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASND` | 2 | $266.94 | $2.00 | — | $6,664.45 | ▲ $11,157.11 (+1,157.11) | deploy half leftover; list flatten; ret5=+1.9; leftover $720.03 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OSCR` | 23 | $30.65 | $2.06 | — | $5,957.44 | ▲ $11,155.05 (+1,155.05) | deploy half leftover; list flatten; 🔵; ret5=-2.2; leftover $720.03 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 69 | $10.41 | $2.20 | — | $5,236.96 | ▲ $11,152.86 (+1,152.86) | deploy half leftover; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $720.03 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 49 | $14.50 | $2.14 | — | $4,524.32 | ▲ $11,150.72 (+1,150.72) | deploy half leftover; list flatten; 🔵; ⚪; ret5=+0.8; leftover $720.03 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 369 | $1.95 | $4.76 | — | $3,800.01 | ▲ $11,145.96 (+1,145.96) | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $720.03 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TGTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TLN` | cash | leftover split 318.86 < 1 share @ 359.83 |
| 2026-08-14 | `DAVE` | cash | leftover split 318.86 < 1 share @ 330.91 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `VST` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `NRG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `SLG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `BTSG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `IREN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TPG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TGTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `SLS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `HIMS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `INO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TNDM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `VST` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `NRG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `SLG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `DVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `EOG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `FANG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `TGB` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `ELF` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `HNST` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `BTSG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `IREN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TPG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TGTX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `SLS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `HIMS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `INO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TNDM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `VST` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `NRG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `SLG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `MARA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `LDI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `DVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `EOG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `FANG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `ELF` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `HNST` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | `VST` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `NRG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `SLG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `MARA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `LDI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `BTBT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `DVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `EOG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `FANG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `TMC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `TGB` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `ELF` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `DNN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `HNST` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-21 | `DVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `EOG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `FANG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `TMC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `TGB` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `ELF` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `DNN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `HNST` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `AU` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `AEM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CRSP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `BHP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `CDE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `HDSN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `IAG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `KGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `NFGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `WPM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `AU` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `AEM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CRSP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `HCA` | cash | leftover split 292.13 < 1 share @ 429.24 |
| 2026-08-26 | `AG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `BHP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `CDE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `HDSN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `IAG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `KGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `NFGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `WPM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AUPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AEM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `ARCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AUTL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRDL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRSP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `NPWR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `HCA` | no_price | no 09:30 open |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AEM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AUTL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRDL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRSP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CYPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `NPWR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `MU` | cash | leftover split 450.63 < 1 share @ 925.74 |
| 2026-08-28 | `OCUL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `INSP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `CRMD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `RZLT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `BMEA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `NPWR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `ACMR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `GGB` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `MT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `MOS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `OCUL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `INSP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `CRMD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `RZLT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `BMEA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `NPWR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `ACMR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `GGB` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `MT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `BHVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `SLI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `ACMR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `GGB` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `MT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `BHVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `CAPR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `RRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `CRK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `SLI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `ACMR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `GGB` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `MT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `ANF` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `BHVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `BZ` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `CAPR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | `ANF` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `BHVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `BZ` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `CAPR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `FRVO` | min_hold | dropped but min-hold 1/5 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 10 | 2026-09-03 @ $49.76 | deploy half leftover; list flatten; 🔵; ⚪; ret5=+10.6; leftover $520.24 |
| `HRMY` | 12 | 2026-09-03 @ $41.31 | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $520.24 |
| `CABA` | 159 | 2026-09-03 @ $3.27 | deploy half leftover; list flatten; 🔵; ⚪; ret5=+13.8; leftover $520.24 |
| `VSTM` | 67 | 2026-09-03 @ $7.70 | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $520.24 |
| `RVTY` | 4 | 2026-09-03 @ $125.94 | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $520.24 |
| `GPRO` | 426 | 2026-09-03 @ $1.22 | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $520.24 |
| `FRVO` | 28 | 2026-09-03 @ $18.40 | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $520.24 |
| `ASND` | 2 | 2026-09-04 @ $266.94 | deploy half leftover; list flatten; ret5=+1.9; leftover $720.03 |
| `OSCR` | 23 | 2026-09-04 @ $30.65 | deploy half leftover; list flatten; 🔵; ret5=-2.2; leftover $720.03 |
| `NVAX` | 69 | 2026-09-04 @ $10.41 | deploy half leftover; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $720.03 |
| `BVS` | 49 | 2026-09-04 @ $14.50 | deploy half leftover; list flatten; 🔵; ⚪; ret5=+0.8; leftover $720.03 |
| `BAK` | 369 | 2026-09-04 @ $1.95 | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $720.03 |
