# Factor mine action — `flatten_h5_half`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

Side **long** · universe `flatten` · top 8 · rank `list` · size `half` · sell `list` · S-boost `none` · deploy half leftover

Cash book **+12.43%** ($11,243) · signal-only (no cash/fees) was +67.92%. Starts YES **16/17**. Fills 101 · skips 222 · realized $+1010.50.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `flatten` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `half` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $3,019.19.

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
| 2026-08-25 | +1.80 | $4,674.03 | AG×21, BHP×4, CDE×21, HDSN×77, IAG×22, KGC×15, NFGC×254, WPM×3, AU×2, AUPH×20, AEM×1, ARCT×30, AUTL×139, CRDL×178, CRSP×5, CYPH×261 | MOS, OCUL, INSP, CRMD, RZLT | — | $2,752.35 | $8,095.57 | $10,847.92 | AG×21, BHP×4, CDE×21, HDSN×77, IAG×22, KGC×15, NFGC×254, WPM×3, AU×2, AUPH×20, AEM×1, ARCT×30, AUTL×139, CRDL×178, CRSP×5, CYPH×261, MOS×16, OCUL×35, INSP×6, CRMD×47, RZLT×74 | BUY MOS x16 @ 24.00; BUY OCUL x35 @ 10.92; BUY INSP x6 @ 61.47; BUY CRMD x47 @ 8.28; BUY RZLT x74 @ 5.23 |
| 2026-08-26 | +2.02 | $2,752.35 | AG×21, BHP×4, CDE×21, HDSN×77, IAG×22, KGC×15, NFGC×254, WPM×3, AU×2, AUPH×20, AEM×1, ARCT×30, AUTL×139, CRDL×178, CRSP×5, CYPH×261, MOS×16, OCUL×35, INSP×6, CRMD×47, RZLT×74 | — | — | $2,752.35 | $8,119.57 | $10,871.92 | AG×21, BHP×4, CDE×21, HDSN×77, IAG×22, KGC×15, NFGC×254, WPM×3, AU×2, AUPH×20, AEM×1, ARCT×30, AUTL×139, CRDL×178, CRSP×5, CYPH×261, MOS×16, OCUL×35, INSP×6, CRMD×47, RZLT×74 | hold AG,BHP,CDE,HDSN,IAG,KGC,NFGC,WPM,AU,AUPH,AEM,ARCT,AUTL,CRDL,CRSP,CYPH,MOS,OCUL,INSP,CRMD,RZLT |
| 2026-08-27 | — | $2,752.35 | AG×21, BHP×4, CDE×21, HDSN×77, IAG×22, KGC×15, NFGC×254, WPM×3, AU×2, AUPH×20, AEM×1, ARCT×30, AUTL×139, CRDL×178, CRSP×5, CYPH×261, MOS×16, OCUL×35, INSP×6, CRMD×47, RZLT×74 | RRC, CRK, SLI | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $3,194.34 | $7,821.83 | $11,016.17 | AU×2, AUPH×20, AEM×1, ARCT×30, AUTL×139, CRDL×178, CRSP×5, CYPH×261, MOS×16, OCUL×35, INSP×6, CRMD×47, RZLT×74, RRC×26, CRK×75, SLI×410 | SELL AG (dropped from list after 5 sess (min 5)); SELL BHP (dropped from list after 5 sess (min 5)); SELL CDE (dropped from list after 5 sess (min 5)); SELL HDSN (dropped from list after 5 sess (min 5)); SELL IAG (dropped from list after 5 sess (min 5)); SELL KGC (dropped from list after 5 sess (min 5)); SELL NFGC (dropped from list after 5 sess (min 5)); SELL WPM (dropped from list after 5 sess (min 5)); BUY RRC x26 @ 40.72; BUY CRK x75 @ 14.09; BUY SLI x410 @ 2.59 |
| 2026-08-28 | +0.75 | $3,194.34 | AU×2, AUPH×20, AEM×1, ARCT×30, AUTL×139, CRDL×178, CRSP×5, CYPH×261, MOS×16, OCUL×35, INSP×6, CRMD×47, RZLT×74, RRC×26, CRK×75, SLI×410 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $5,873.08 | $5,144.41 | $11,017.49 | MOS×16, OCUL×35, INSP×6, CRMD×47, RZLT×74, RRC×26, CRK×75, SLI×410 | SELL AU (dropped from list after 5 sess (min 5)); SELL AUPH (dropped from list after 5 sess (min 5)); SELL AEM (dropped from list after 5 sess (min 5)); SELL ARCT (dropped from list after 5 sess (min 5)); SELL AUTL (dropped from list after 5 sess (min 5)); SELL CRDL (dropped from list after 5 sess (min 5)); SELL CRSP (dropped from list after 5 sess (min 5)); SELL CYPH (dropped from list after 5 sess (min 5)) |
| 2026-08-31 | -5.85 | $5,873.08 | MOS×16, OCUL×35, INSP×6, CRMD×47, RZLT×74, RRC×26, CRK×75, SLI×410 | — | — | $5,873.08 | $5,047.33 | $10,920.41 | MOS×16, OCUL×35, INSP×6, CRMD×47, RZLT×74, RRC×26, CRK×75, SLI×410 | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $5,873.08 | MOS×16, OCUL×35, INSP×6, CRMD×47, RZLT×74, RRC×26, CRK×75, SLI×410 | — | MOS, OCUL, INSP, CRMD, RZLT | $7,727.22 | $3,298.82 | $11,026.04 | RRC×26, CRK×75, SLI×410 | SELL MOS (dropped from list after 5 sess (min 5)); SELL OCUL (dropped from list after 5 sess (min 5)); SELL INSP (dropped from list after 5 sess (min 5)); SELL CRMD (dropped from list after 5 sess (min 5)); SELL RZLT (dropped from list after 5 sess (min 5)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $7,727.22 | RRC×26, CRK×75, SLI×410 | — | — | $7,727.22 | $3,324.80 | $11,052.02 | RRC×26, CRK×75, SLI×410 | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $7,727.22 | RRC×26, CRK×75, SLI×410 | ATRC, HRMY, CABA, VSTM, RVTY | RRC, CRK, SLI | $5,629.22 | $5,657.22 | $11,286.44 | ATRC×22, HRMY×26, CABA×336, VSTM×142, RVTY×8 | SELL RRC (dropped from list after 5 sess (min 5)); SELL CRK (dropped from list after 5 sess (min 5)); SELL SLI (dropped from list after 5 sess (min 5)); BUY ATRC x22 @ 49.76; BUY HRMY x26 @ 41.31; BUY CABA x336 @ 3.27; BUY VSTM x142 @ 7.70; BUY RVTY x8 @ 125.94 |
| 2026-09-04 | — | $5,629.22 | ATRC×22, HRMY×26, CABA×336, VSTM×142, RVTY×8 | ASND, OSCR, NVAX, BVS | — | $3,019.19 | $8,223.54 | $11,242.73 | ATRC×22, HRMY×26, CABA×336, VSTM×142, RVTY×8, ASND×2, OSCR×22, NVAX×67, BVS×48 | BUY ASND x2 @ 266.94; BUY OSCR x22 @ 30.65; BUY NVAX x67 @ 10.41; BUY BVS x48 @ 14.50 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 10 | $59.80 | $2.02 | — | $9,399.98 | ▼ $9,997.98 (-2.02) | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 13 | $45.98 | $2.03 | — | $8,800.21 | ▼ $9,995.95 (-4.05) | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+12.3; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 12 | $50.62 | $2.03 | — | $8,190.71 | ▼ $9,993.92 (-6.08) | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+6.2; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 12 | $49.70 | $2.03 | — | $7,592.28 | ▼ $9,991.90 (-8.10) | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 53 | $11.70 | $2.15 | — | $6,970.03 | ▼ $9,989.75 (-10.25) | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 21 | $29.74 | $2.05 | — | $6,343.44 | ▼ $9,987.70 (-12.30) | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 771 | $0.81 | $8.56 | — | $5,710.37 | ▼ $9,979.14 (-20.86) | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.2; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 26 | $23.33 | $2.07 | — | $5,101.72 | ▼ $9,977.07 (-22.93) | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+19.7; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 2 | $146.90 | $2.00 | — | $4,805.93 | ▲ $10,082.42 (+82.42) | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+3.6; leftover $318.86 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 2 | $120.00 | $2.00 | — | $4,563.93 | ▲ $10,080.42 (+80.42) | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+0.6; leftover $318.86 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 5 | $57.61 | $2.00 | — | $4,273.88 | ▲ $10,078.42 (+78.42) | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+5.7; leftover $318.86 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 35 | $9.01 | $2.10 | — | $3,956.43 | ▲ $10,076.32 (+76.32) | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-13.5; leftover $318.86 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 340 | $0.94 | $4.21 | — | $3,633.64 | ▲ $10,072.11 (+72.11) | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.5; leftover $318.86 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 212 | $1.50 | $2.73 | — | $3,312.91 | ▲ $10,069.38 (+69.38) | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $318.86 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 4 | $46.18 | $1.86 | — | $3,126.33 | ▲ $10,194.82 (+194.82) | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+6.7; leftover $207.06 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 1 | $142.77 | $1.43 | — | $2,982.13 | ▲ $10,193.39 (+193.39) | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+5.8; leftover $207.06 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 1 | $202.70 | $1.99 | — | $2,777.44 | ▲ $10,191.40 (+191.40) | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+8.3; leftover $207.06 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 51 | $4.05 | $2.14 | — | $2,568.74 | ▲ $10,189.25 (+189.25) | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; leftover $207.06 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 24 | $8.46 | $2.06 | — | $2,363.64 | ▲ $10,187.19 (+187.19) | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.4; leftover $207.06 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 2 | $90.54 | $1.82 | — | $2,180.75 | ▲ $10,185.38 (+185.38) | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=-7.2; leftover $207.06 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 63 | $3.24 | $2.18 | — | $1,974.45 | ▲ $10,183.20 (+183.20) | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; leftover $207.06 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 43 | $4.81 | $2.12 | — | $1,765.50 | ▲ $10,181.08 (+181.08) | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-11.4; leftover $207.06 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 10 | $58.64 | $2.04 | $-15.66 | $2,349.86 | ▲ $10,486.02 (+486.02) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 13 | $42.46 | $2.05 | $-49.84 | $2,899.79 | ▲ $10,483.97 (+483.97) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 12 | $53.06 | $2.05 | $+25.17 | $3,534.46 | ▲ $10,481.92 (+481.92) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGTX` | 12 | $51.65 | $2.05 | $+19.33 | $4,152.22 | ▲ $10,479.88 (+479.88) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 53 | $13.84 | $2.17 | $+109.10 | $4,883.57 | ▲ $10,477.71 (+477.71) | dropped from list after 5 sess (min 5) | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SELL** | `HIMS` | 21 | $30.66 | $2.07 | $+15.19 | $5,525.35 | ▲ $10,475.63 (+475.63) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `INO` | 771 | $1.30 | $10.08 | $+359.15 | $6,517.57 | ▲ $10,465.55 (+465.55) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 26 | $23.11 | $2.09 | $-9.88 | $7,116.34 | ▲ $10,463.46 (+463.46) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 21 | $20.55 | $2.05 | — | $6,682.74 | ▲ $10,461.41 (+461.41) | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $444.77 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 4 | $91.01 | $2.00 | — | $6,316.70 | ▲ $10,459.41 (+459.41) | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $444.77 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 21 | $20.65 | $2.05 | — | $5,880.99 | ▲ $10,457.35 (+457.35) | deploy half leftover; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $444.77 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 77 | $5.77 | $2.22 | — | $5,434.48 | ▲ $10,455.13 (+455.13) | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $444.77 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 22 | $19.63 | $2.06 | — | $5,000.57 | ▲ $10,453.08 (+453.08) | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $444.77 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 15 | $29.63 | $2.04 | — | $4,554.08 | ▲ $10,451.04 (+451.04) | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $444.77 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 254 | $1.75 | $3.28 | — | $4,106.31 | ▲ $10,447.77 (+447.77) | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $444.77 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 3 | $144.54 | $2.00 | — | $3,670.69 | ▲ $10,445.77 (+445.77) | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $444.77 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `VST` | 2 | $139.99 | $2.02 | $-17.83 | $3,948.65 | ▲ $10,736.60 (+736.60) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `NRG` | 2 | $116.58 | $2.02 | $-10.85 | $4,179.79 | ▲ $10,734.58 (+734.58) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `SLG` | 5 | $58.63 | $2.02 | $+1.07 | $4,470.92 | ▲ $10,732.56 (+732.56) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 35 | $11.70 | $2.12 | $+89.94 | $4,878.30 | ▲ $10,730.44 (+730.44) | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 340 | $0.87 | $4.03 | $-32.04 | $5,169.05 | ▲ $10,726.41 (+726.41) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 212 | $1.66 | $2.78 | $+28.41 | $5,518.19 | ▲ $10,723.63 (+723.63) | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 2 | $119.43 | $2.00 | — | $5,277.33 | ▲ $10,721.63 (+721.63) | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+20.4; leftover $344.89 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 20 | $17.20 | $2.05 | — | $4,931.28 | ▲ $10,719.58 (+719.58) | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $344.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 1 | $216.30 | $1.99 | — | $4,712.99 | ▲ $10,717.59 (+717.59) | deploy half leftover; list flatten,ohlc_hot,mover_buy; live flatten mover; 🔵; ⚪; ret5=+17.6; leftover $344.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 30 | $11.13 | $2.08 | — | $4,377.01 | ▲ $10,715.51 (+715.51) | deploy half leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $344.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 139 | $2.47 | $2.41 | — | $4,031.27 | ▲ $10,713.10 (+713.10) | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $344.89 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 178 | $1.93 | $2.52 | — | $3,685.21 | ▲ $10,710.58 (+710.58) | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $344.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 5 | $59.72 | $2.00 | — | $3,384.61 | ▲ $10,708.58 (+708.58) | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.6; leftover $344.89 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 261 | $1.32 | $3.37 | — | $3,036.72 | ▲ $10,705.21 (+705.21) | deploy half leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $344.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `DVN` | 4 | $48.84 | $1.99 | $+6.80 | $3,230.09 | ▲ $10,952.47 (+952.47) | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `EOG` | 1 | $152.61 | $1.55 | $+6.86 | $3,381.15 | ▲ $10,950.92 (+950.92) | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `FANG` | 1 | $209.47 | $2.01 | $+2.76 | $3,588.61 | ▲ $10,948.91 (+948.91) | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 51 | $4.57 | $2.16 | $+22.21 | $3,819.52 | ▲ $10,946.75 (+946.75) | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `TGB` | 24 | $9.26 | $2.08 | $+15.06 | $4,039.68 | ▲ $10,944.67 (+944.67) | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `ELF` | 2 | $101.53 | $2.02 | $+18.15 | $4,240.72 | ▲ $10,942.65 (+942.65) | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 63 | $3.50 | $2.20 | $+12.00 | $4,459.02 | ▲ $10,940.45 (+940.45) | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟡 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `HNST` | 43 | $5.05 | $2.14 | $+6.06 | $4,674.03 | ▲ $10,938.31 (+938.31) | dropped from list after 5 sess (min 5) | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 16 | $24.00 | $2.04 | — | $4,287.99 | ▲ $10,880.36 (+880.36) | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.0; leftover $389.50 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 35 | $10.92 | $2.10 | — | $3,903.70 | ▲ $10,878.27 (+878.27) | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+10.4; leftover $389.50 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 6 | $61.47 | $2.01 | — | $3,532.87 | ▲ $10,876.26 (+876.26) | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+9.2; leftover $389.50 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 47 | $8.28 | $2.13 | — | $3,141.58 | ▲ $10,874.13 (+874.13) | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.8; leftover $389.50 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 74 | $5.23 | $2.21 | — | $2,752.35 | ▲ $10,871.92 (+871.92) | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+10.7; leftover $389.50 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 21 | $20.63 | $2.07 | $-2.45 | $3,183.50 | ▲ $10,973.98 (+973.98) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 4 | $96.99 | $2.02 | $+19.90 | $3,569.44 | ▲ $10,971.96 (+971.96) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 21 | $21.00 | $2.07 | $+3.22 | $4,008.37 | ▲ $10,969.89 (+969.89) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 77 | $5.51 | $2.24 | $-24.48 | $4,430.40 | ▲ $10,967.65 (+967.65) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 22 | $21.64 | $2.08 | $+40.09 | $4,904.40 | ▲ $10,965.57 (+965.57) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 15 | $32.90 | $2.06 | $+44.96 | $5,395.84 | ▲ $10,963.51 (+963.51) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 254 | $2.00 | $3.33 | $+56.89 | $5,900.52 | ▲ $10,960.19 (+960.19) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 3 | $160.93 | $2.02 | $+45.15 | $6,381.29 | ▲ $10,958.17 (+958.17) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 26 | $40.72 | $2.07 | — | $5,320.50 | ▲ $10,956.10 (+956.10) | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.8; leftover $1063.55 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 75 | $14.09 | $2.21 | — | $4,261.53 | ▲ $10,953.88 (+953.88) | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.1; leftover $1063.55 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 410 | $2.59 | $5.29 | — | $3,194.34 | ▲ $10,948.59 (+948.59) | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.2; leftover $1063.55 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 2 | $117.41 | $2.02 | $-8.05 | $3,427.15 | ▲ $11,017.51 (+1,017.51) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 20 | $16.47 | $2.07 | $-18.72 | $3,754.48 | ▲ $11,015.44 (+1,015.44) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AEM` | 1 | $214.11 | $2.01 | $-6.20 | $3,966.58 | ▲ $11,013.43 (+1,013.43) | dropped from list after 5 sess (min 5) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 30 | $15.74 | $2.10 | $+134.12 | $4,436.68 | ▲ $11,011.33 (+1,011.33) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 139 | $2.32 | $2.44 | $-25.70 | $4,756.72 | ▲ $11,008.89 (+1,008.89) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 178 | $2.09 | $2.56 | $+23.39 | $5,126.17 | ▲ $11,006.32 (+1,006.32) | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `CRSP` | 5 | $59.12 | $2.02 | $-7.03 | $5,419.75 | ▲ $11,004.30 (+1,004.30) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 261 | $1.75 | $3.42 | $+105.44 | $5,873.08 | ▲ $11,000.88 (+1,000.88) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `MOS` | 16 | $24.00 | $2.06 | $-4.10 | $6,255.02 | ▲ $10,990.32 (+990.32) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `OCUL` | 35 | $10.49 | $2.12 | $-19.26 | $6,620.05 | ▲ $10,988.20 (+988.20) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `INSP` | 6 | $63.05 | $2.03 | $+5.44 | $6,996.33 | ▲ $10,986.18 (+986.18) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 47 | $8.26 | $2.15 | $-5.22 | $7,382.39 | ▲ $10,984.02 (+984.02) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 74 | $4.69 | $2.23 | $-44.41 | $7,727.22 | ▲ $10,981.79 (+981.79) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 26 | $42.10 | $2.09 | $+31.72 | $8,819.73 | ▲ $11,018.13 (+1,018.13) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `CRK` | 75 | $15.70 | $2.24 | $+116.30 | $9,995.00 | ▲ $11,015.90 (+1,015.90) | dropped from list after 5 sess (min 5) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 410 | $2.49 | $5.37 | $-51.66 | $11,010.53 | ▲ $11,010.53 (+1,010.53) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 22 | $49.76 | $2.06 | — | $9,913.75 | ▲ $11,008.47 (+1,008.47) | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+10.6; leftover $1101.05 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 26 | $41.31 | $2.07 | — | $8,837.62 | ▲ $11,006.40 (+1,006.40) | deploy half leftover; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.1; leftover $1101.05 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 336 | $3.27 | $4.33 | — | $7,734.57 | ▲ $11,002.07 (+1,002.07) | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+13.8; leftover $1101.05 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 142 | $7.70 | $2.42 | — | $6,638.75 | ▲ $10,999.65 (+999.65) | deploy half leftover; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.7; leftover $1101.05 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 8 | $125.94 | $2.01 | — | $5,629.22 | ▲ $10,997.64 (+997.64) | deploy half leftover; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+6.8; leftover $1101.05 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ASND` | 2 | $266.94 | $2.00 | — | $5,093.34 | ▲ $11,326.30 (+1,326.30) | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.9; leftover $703.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OSCR` | 22 | $30.65 | $2.06 | — | $4,416.99 | ▲ $11,324.25 (+1,324.25) | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-2.2; leftover $703.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 67 | $10.41 | $2.19 | — | $3,717.33 | ▲ $11,322.06 (+1,322.06) | deploy half leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ⚪; ret5=+11.1; leftover $703.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 48 | $14.50 | $2.13 | — | $3,019.19 | ▲ $11,319.92 (+1,319.92) | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.8; leftover $703.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |

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
| 2026-08-25 | `HCA` | cash | leftover split 389.50 < 1 share @ 429.24 |
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
| 2026-08-26 | `HCA` | no_price | no 09:30 open |
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
| 2026-08-28 | `OCUL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `INSP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `CRMD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `RZLT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `MOS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `OCUL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `INSP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `CRMD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `RZLT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `SLI` | min_hold | dropped but min-hold 3/5 sess — no sell |
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
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/5 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 22 | 2026-09-03 @ $49.76 | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+10.6; leftover $1101.05 |
| `HRMY` | 26 | 2026-09-03 @ $41.31 | deploy half leftover; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.1; leftover $1101.05 |
| `CABA` | 336 | 2026-09-03 @ $3.27 | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+13.8; leftover $1101.05 |
| `VSTM` | 142 | 2026-09-03 @ $7.70 | deploy half leftover; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.7; leftover $1101.05 |
| `RVTY` | 8 | 2026-09-03 @ $125.94 | deploy half leftover; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+6.8; leftover $1101.05 |
| `ASND` | 2 | 2026-09-04 @ $266.94 | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.9; leftover $703.65 |
| `OSCR` | 22 | 2026-09-04 @ $30.65 | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-2.2; leftover $703.65 |
| `NVAX` | 67 | 2026-09-04 @ $10.41 | deploy half leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ⚪; ret5=+11.1; leftover $703.65 |
| `BVS` | 48 | 2026-09-04 @ $14.50 | deploy half leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.8; leftover $703.65 |
