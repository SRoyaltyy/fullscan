# Factor mine action — `flatten_h5_topheavy`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

Side **long** · universe `flatten` · top 8 · rank `list` · size `topheavy` · sell `list` · S-boost `none` · 40% to #1, rest split

Cash book **+18.26%** ($11,826) · signal-only (no cash/fees) was +67.92%. Starts YES **17/17**. Fills 76 · skips 194 · realized $+1341.89.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `flatten` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `topheavy` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $61.94.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $160.57 | $9,962.48 | $10,123.05 | BTSG×66, IREN×18, TPG×16, TGTX×17, SLS×73, HIMS×28, INO×1058, TNDM×36 | BUY BTSG x66 @ 59.80; BUY IREN x18 @ 45.98; BUY TPG x16 @ 50.62; BUY TGTX x17 @ 49.70; BUY SLS x73 @ 11.70; BUY HIMS x28 @ 29.74; BUY INO x1058 @ 0.81; BUY TNDM x36 @ 23.33 |
| 2026-08-14 | +5.50 | $160.57 | BTSG×66, IREN×18, TPG×16, TGTX×17, SLS×73, HIMS×28, INO×1058, TNDM×36 | MARA, LDI, BTBT | — | $124.52 | $10,271.17 | $10,395.68 | BTSG×66, IREN×18, TPG×16, TGTX×17, SLS×73, HIMS×28, INO×1058, TNDM×36, MARA×1, LDI×14, BTBT×9 | BUY MARA x1 @ 9.01; BUY LDI x14 @ 0.94; BUY BTBT x9 @ 1.50 |
| 2026-08-17 | +2.25 | $124.52 | BTSG×66, IREN×18, TPG×16, TGTX×17, SLS×73, HIMS×28, INO×1058, TNDM×36, MARA×1, LDI×14, BTBT×9 | DVN, TMC, TGB, DNN, HNST | — | $41.59 | $10,346.54 | $10,388.13 | BTSG×66, IREN×18, TPG×16, TGTX×17, SLS×73, HIMS×28, INO×1058, TNDM×36, MARA×1, LDI×14, BTBT×9, DVN×1, TMC×2, TGB×1, DNN×3, HNST×2 | BUY DVN x1 @ 46.18; BUY TMC x2 @ 4.05; BUY TGB x1 @ 8.46; BUY DNN x3 @ 3.24; BUY HNST x2 @ 4.81 |
| 2026-08-18 | -6.20 | $41.59 | BTSG×66, IREN×18, TPG×16, TGTX×17, SLS×73, HIMS×28, INO×1058, TNDM×36, MARA×1, LDI×14, BTBT×9, DVN×1, TMC×2, TGB×1, DNN×3, HNST×2 | — | — | $41.59 | $10,333.84 | $10,375.43 | BTSG×66, IREN×18, TPG×16, TGTX×17, SLS×73, HIMS×28, INO×1058, TNDM×36, MARA×1, LDI×14, BTBT×9, DVN×1, TMC×2, TGB×1, DNN×3, HNST×2 | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $41.59 | BTSG×66, IREN×18, TPG×16, TGTX×17, SLS×73, HIMS×28, INO×1058, TNDM×36, MARA×1, LDI×14, BTBT×9, DVN×1, TMC×2, TGB×1, DNN×3, HNST×2 | — | — | $41.59 | $10,636.85 | $10,678.44 | BTSG×66, IREN×18, TPG×16, TGTX×17, SLS×73, HIMS×28, INO×1058, TNDM×36, MARA×1, LDI×14, BTBT×9, DVN×1, TMC×2, TGB×1, DNN×3, HNST×2 | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $41.59 | BTSG×66, IREN×18, TPG×16, TGTX×17, SLS×73, HIMS×28, INO×1058, TNDM×36, MARA×1, LDI×14, BTBT×9, DVN×1, TMC×2, TGB×1, DNN×3, HNST×2 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $121.79 | $10,699.43 | $10,821.22 | MARA×1, LDI×14, BTBT×9, DVN×1, TMC×2, TGB×1, DNN×3, HNST×2, AG×203, BHP×9, CDE×43, HDSN×155, IAG×45, KGC×30, NFGC×511, WPM×6 | SELL BTSG (dropped from list after 5 sess (min 5)); SELL IREN (dropped from list after 5 sess (min 5)); SELL TPG (dropped from list after 5 sess (min 5)); SELL TGTX (dropped from list after 5 sess (min 5)); SELL SLS (dropped from list after 5 sess (min 5)); SELL HIMS (dropped from list after 5 sess (min 5)); SELL INO (dropped from list after 5 sess (min 5)); SELL TNDM (dropped from list after 5 sess (min 5)); BUY AG x203 @ 20.55; BUY BHP x9 @ 91.01; BUY CDE x43 @ 20.65; BUY HDSN x155 @ 5.77; BUY IAG x45 @ 19.63; BUY KGC x30 @ 29.63; BUY NFGC x511 @ 1.75; BUY WPM x6 @ 144.54 |
| 2026-08-21 | +3.25 | $121.79 | MARA×1, LDI×14, BTBT×9, DVN×1, TMC×2, TGB×1, DNN×3, HNST×2, AG×203, BHP×9, CDE×43, HDSN×155, IAG×45, KGC×30, NFGC×511, WPM×6 | ARCT, AUTL, CRDL, CYPH | MARA, LDI, BTBT | $109.29 | $10,891.11 | $11,000.40 | DVN×1, TMC×2, TGB×1, DNN×3, HNST×2, AG×203, BHP×9, CDE×43, HDSN×155, IAG×45, KGC×30, NFGC×511, WPM×6, ARCT×1, AUTL×5, CRDL×7, CYPH×10 | SELL MARA (dropped from list after 5 sess (min 5)); SELL LDI (dropped from list after 5 sess (min 5)); SELL BTBT (dropped from list after 5 sess (min 5)); BUY ARCT x1 @ 11.13; BUY AUTL x5 @ 2.47; BUY CRDL x7 @ 1.93; BUY CYPH x10 @ 1.32 |
| 2026-08-24 | -5.17 | $109.29 | DVN×1, TMC×2, TGB×1, DNN×3, HNST×2, AG×203, BHP×9, CDE×43, HDSN×155, IAG×45, KGC×30, NFGC×511, WPM×6, ARCT×1, AUTL×5, CRDL×7, CYPH×10 | — | DVN, TMC, TGB, DNN, HNST | $196.13 | $10,698.93 | $10,895.06 | AG×203, BHP×9, CDE×43, HDSN×155, IAG×45, KGC×30, NFGC×511, WPM×6, ARCT×1, AUTL×5, CRDL×7, CYPH×10 | SELL DVN (dropped from list after 5 sess (min 5)); SELL TMC (dropped from list after 5 sess (min 5)); SELL TGB (dropped from list after 5 sess (min 5)); SELL DNN (dropped from list after 5 sess (min 5)); SELL HNST (dropped from list after 5 sess (min 5)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $196.13 | AG×203, BHP×9, CDE×43, HDSN×155, IAG×45, KGC×30, NFGC×511, WPM×6, ARCT×1, AUTL×5, CRDL×7, CYPH×10 | MOS, OCUL, CRMD, RZLT | — | $63.46 | $10,853.89 | $10,917.35 | AG×203, BHP×9, CDE×43, HDSN×155, IAG×45, KGC×30, NFGC×511, WPM×6, ARCT×1, AUTL×5, CRDL×7, CYPH×10, MOS×3, OCUL×2, CRMD×2, RZLT×4 | BUY MOS x3 @ 24.00; BUY OCUL x2 @ 10.92; BUY CRMD x2 @ 8.28; BUY RZLT x4 @ 5.23 |
| 2026-08-26 | +2.02 | $63.46 | AG×203, BHP×9, CDE×43, HDSN×155, IAG×45, KGC×30, NFGC×511, WPM×6, ARCT×1, AUTL×5, CRDL×7, CYPH×10, MOS×3, OCUL×2, CRMD×2, RZLT×4 | — | — | $63.46 | $10,905.16 | $10,968.62 | AG×203, BHP×9, CDE×43, HDSN×155, IAG×45, KGC×30, NFGC×511, WPM×6, ARCT×1, AUTL×5, CRDL×7, CYPH×10, MOS×3, OCUL×2, CRMD×2, RZLT×4 | hold AG,BHP,CDE,HDSN,IAG,KGC,NFGC,WPM,ARCT,AUTL,CRDL,CYPH,MOS,OCUL,CRMD,RZLT |
| 2026-08-27 | — | $63.46 | AG×203, BHP×9, CDE×43, HDSN×155, IAG×45, KGC×30, NFGC×511, WPM×6, ARCT×1, AUTL×5, CRDL×7, CYPH×10, MOS×3, OCUL×2, CRMD×2, RZLT×4 | RRC, CRK, SLI | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $1.87 | $11,181.08 | $11,182.95 | ARCT×1, AUTL×5, CRDL×7, CYPH×10, MOS×3, OCUL×2, CRMD×2, RZLT×4, RRC×106, CRK×230, SLI×1246 | SELL AG (dropped from list after 5 sess (min 5)); SELL BHP (dropped from list after 5 sess (min 5)); SELL CDE (dropped from list after 5 sess (min 5)); SELL HDSN (dropped from list after 5 sess (min 5)); SELL IAG (dropped from list after 5 sess (min 5)); SELL KGC (dropped from list after 5 sess (min 5)); SELL NFGC (dropped from list after 5 sess (min 5)); SELL WPM (dropped from list after 5 sess (min 5)); BUY RRC x106 @ 40.72; BUY CRK x230 @ 14.09; BUY SLI x1246 @ 2.59 |
| 2026-08-28 | +0.75 | $1.87 | ARCT×1, AUTL×5, CRDL×7, CYPH×10, MOS×3, OCUL×2, CRMD×2, RZLT×4, RRC×106, CRK×230, SLI×1246 | — | ARCT, AUTL, CRDL, CYPH | $60.59 | $11,195.34 | $11,255.93 | MOS×3, OCUL×2, CRMD×2, RZLT×4, RRC×106, CRK×230, SLI×1246 | SELL ARCT (dropped from list after 5 sess (min 5)); SELL AUTL (dropped from list after 5 sess (min 5)); SELL CRDL (dropped from list after 5 sess (min 5)); SELL CYPH (dropped from list after 5 sess (min 5)) |
| 2026-08-31 | -5.85 | $60.59 | MOS×3, OCUL×2, CRMD×2, RZLT×4, RRC×106, CRK×230, SLI×1246 | — | — | $60.59 | $11,020.58 | $11,081.17 | MOS×3, OCUL×2, CRMD×2, RZLT×4, RRC×106, CRK×230, SLI×1246 | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $60.59 | MOS×3, OCUL×2, CRMD×2, RZLT×4, RRC×106, CRK×230, SLI×1246 | — | MOS, OCUL, CRMD, RZLT | $187.46 | $11,171.12 | $11,358.58 | RRC×106, CRK×230, SLI×1246 | SELL MOS (dropped from list after 5 sess (min 5)); SELL OCUL (dropped from list after 5 sess (min 5)); SELL CRMD (dropped from list after 5 sess (min 5)); SELL RZLT (dropped from list after 5 sess (min 5)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $187.46 | RRC×106, CRK×230, SLI×1246 | — | — | $187.46 | $11,281.54 | $11,469.00 | RRC×106, CRK×230, SLI×1246 | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $187.46 | RRC×106, CRK×230, SLI×1246 | ATRC, HRMY, CABA, VSTM, RVTY | RRC, CRK, SLI | $72.46 | $11,865.97 | $11,938.43 | ATRC×91, HRMY×41, CABA×520, VSTM×220, RVTY×13 | SELL RRC (dropped from list after 5 sess (min 5)); SELL CRK (dropped from list after 5 sess (min 5)); SELL SLI (dropped from list after 5 sess (min 5)); BUY ATRC x91 @ 49.76; BUY HRMY x41 @ 41.31; BUY CABA x520 @ 3.27; BUY VSTM x220 @ 7.70; BUY RVTY x13 @ 125.94 |
| 2026-09-04 | — | $72.46 | ATRC×91, HRMY×41, CABA×520, VSTM×220, RVTY×13 | NVAX | — | $61.94 | $11,763.85 | $11,825.79 | ATRC×91, HRMY×41, CABA×520, VSTM×220, RVTY×13, NVAX×1 | BUY NVAX x1 @ 10.41 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 66 | $59.80 | $2.19 | — | $6,051.01 | ▼ $9,997.81 (-2.19) | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $4000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 18 | $45.98 | $2.04 | — | $5,221.33 | ▼ $9,995.77 (-4.23) | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+12.3; leftover $857.14 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 16 | $50.62 | $2.04 | — | $4,409.32 | ▼ $9,993.73 (-6.27) | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+6.2; leftover $857.14 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 17 | $49.70 | $2.04 | — | $3,562.38 | ▼ $9,991.69 (-8.31) | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $857.14 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 73 | $11.70 | $2.21 | — | $2,706.07 | ▼ $9,989.48 (-10.52) | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $857.14 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 28 | $29.74 | $2.07 | — | $1,871.27 | ▼ $9,987.41 (-12.59) | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $857.14 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1058 | $0.81 | $11.74 | — | $1,002.55 | ▼ $9,975.66 (-24.34) | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.2; leftover $857.14 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 36 | $23.33 | $2.10 | — | $160.57 | ▼ $9,973.56 (-26.44) | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+19.7; leftover $857.14 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $151.47 | ▲ $10,109.69 (+109.69) | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-13.5; leftover $13.76 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 14 | $0.94 | $0.17 | — | $138.18 | ▲ $10,109.52 (+109.52) | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.5; leftover $13.76 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 9 | $1.50 | $0.16 | — | $124.52 | ▲ $10,109.35 (+109.35) | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $13.76 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 1 | $46.18 | $0.46 | — | $77.87 | ▲ $10,379.55 (+379.55) | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+6.7; leftover $49.81 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 2 | $4.05 | $0.09 | — | $69.68 | ▲ $10,379.46 (+379.46) | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; leftover $10.67 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 1 | $8.46 | $0.09 | — | $61.14 | ▲ $10,379.38 (+379.38) | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.4; leftover $10.67 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 3 | $3.24 | $0.11 | — | $51.31 | ▲ $10,379.27 (+379.27) | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; leftover $10.67 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 2 | $4.81 | $0.10 | — | $41.59 | ▲ $10,379.17 (+379.17) | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-11.4; leftover $10.67 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 66 | $58.64 | $2.23 | $-80.98 | $3,909.60 | ▲ $10,597.32 (+597.32) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 18 | $42.46 | $2.06 | $-67.47 | $4,671.82 | ▲ $10,595.25 (+595.25) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 16 | $53.06 | $2.06 | $+34.89 | $5,518.72 | ▲ $10,593.20 (+593.20) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGTX` | 17 | $51.65 | $2.06 | $+29.05 | $6,394.71 | ▲ $10,591.14 (+591.14) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 73 | $13.84 | $2.23 | $+151.78 | $7,402.79 | ▲ $10,588.90 (+588.90) | dropped from list after 5 sess (min 5) | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SELL** | `HIMS` | 28 | $30.66 | $2.09 | $+21.59 | $8,259.18 | ▲ $10,586.81 (+586.81) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `INO` | 1058 | $1.30 | $13.83 | $+492.84 | $9,620.75 | ▲ $10,572.98 (+572.98) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 36 | $23.11 | $2.12 | $-12.14 | $10,450.59 | ▲ $10,570.86 (+570.86) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 203 | $20.55 | $2.62 | — | $6,276.32 | ▲ $10,568.24 (+568.24) | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $4180.24 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 9 | $91.01 | $2.02 | — | $5,455.21 | ▲ $10,566.22 (+566.22) | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $895.76 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 43 | $20.65 | $2.12 | — | $4,565.14 | ▲ $10,564.10 (+564.10) | 40% to #1, rest split; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $895.76 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 155 | $5.77 | $2.46 | — | $3,668.34 | ▲ $10,561.65 (+561.65) | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $895.76 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 45 | $19.63 | $2.12 | — | $2,782.86 | ▲ $10,559.52 (+559.52) | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $895.76 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 30 | $29.63 | $2.08 | — | $1,891.88 | ▲ $10,557.44 (+557.44) | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $895.76 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 511 | $1.75 | $6.59 | — | $991.04 | ▲ $10,550.85 (+550.85) | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $895.76 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 6 | $144.54 | $2.01 | — | $121.79 | ▲ $10,548.84 (+548.84) | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $895.76 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 1 | $11.70 | $0.14 | $+2.46 | $133.35 | ▲ $11,128.63 (+1,128.63) | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 14 | $0.87 | $0.18 | $-1.34 | $145.31 | ▲ $11,128.45 (+1,128.45) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 9 | $1.66 | $0.20 | $+1.08 | $160.05 | ▲ $11,128.25 (+1,128.25) | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $148.81 | ▲ $11,128.14 (+1,128.14) | 40% to #1, rest split; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $13.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 5 | $2.47 | $0.14 | — | $136.32 | ▲ $11,128.00 (+1,128.00) | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $13.72 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 7 | $1.93 | $0.16 | — | $122.65 | ▲ $11,127.84 (+1,127.84) | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $13.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 10 | $1.32 | $0.16 | — | $109.29 | ▲ $11,127.68 (+1,127.68) | 40% to #1, rest split; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $13.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `DVN` | 1 | $48.84 | $0.51 | $+1.68 | $157.62 | ▲ $11,148.99 (+1,148.99) | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 2 | $4.57 | $0.12 | $+0.84 | $166.64 | ▲ $11,148.87 (+1,148.87) | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `TGB` | 1 | $9.26 | $0.12 | $+0.60 | $175.79 | ▲ $11,148.76 (+1,148.76) | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 3 | $3.50 | $0.13 | $+0.54 | $186.15 | ▲ $11,148.62 (+1,148.62) | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟡 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `HNST` | 2 | $5.05 | $0.13 | $+0.25 | $196.13 | ▲ $11,148.50 (+1,148.50) | dropped from list after 5 sess (min 5) | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 3 | $24.00 | $0.73 | — | $123.40 | ▲ $10,969.24 (+969.24) | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.0; leftover $78.45 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 2 | $10.92 | $0.22 | — | $101.33 | ▲ $10,969.01 (+969.01) | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+10.4; leftover $23.54 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 2 | $8.28 | $0.17 | — | $84.60 | ▲ $10,968.84 (+968.84) | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.8; leftover $23.54 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 4 | $5.23 | $0.22 | — | $63.46 | ▲ $10,968.62 (+968.62) | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ret5=+10.7; leftover $23.54 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 203 | $20.63 | $2.69 | $+10.94 | $4,248.66 | ▲ $11,017.95 (+1,017.95) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 9 | $96.99 | $2.04 | $+49.77 | $5,119.54 | ▲ $11,015.92 (+1,015.92) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 43 | $21.00 | $2.14 | $+10.79 | $6,020.40 | ▲ $11,013.78 (+1,013.78) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 155 | $5.51 | $2.49 | $-45.25 | $6,871.96 | ▲ $11,011.29 (+1,011.29) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 45 | $21.64 | $2.15 | $+86.18 | $7,843.61 | ▲ $11,009.14 (+1,009.14) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 30 | $32.90 | $2.10 | $+93.92 | $8,828.51 | ▲ $11,007.04 (+1,007.04) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 511 | $2.00 | $6.69 | $+114.47 | $9,843.82 | ▲ $11,000.35 (+1,000.35) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 6 | $160.93 | $2.03 | $+94.30 | $10,807.38 | ▲ $10,998.33 (+998.33) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 106 | $40.72 | $2.31 | — | $6,488.75 | ▲ $10,996.02 (+996.02) | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.8; leftover $4322.95 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 230 | $14.09 | $2.97 | — | $3,245.08 | ▲ $10,993.05 (+993.05) | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.1; leftover $3242.21 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 1246 | $2.59 | $16.07 | — | $1.87 | ▲ $10,976.98 (+976.98) | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.2; leftover $3242.21 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 1 | $15.74 | $0.18 | $+4.32 | $17.43 | ▲ $11,140.52 (+1,140.52) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 5 | $2.32 | $0.15 | $-1.04 | $28.88 | ▲ $11,140.37 (+1,140.37) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 7 | $2.09 | $0.19 | $+0.78 | $43.32 | ▲ $11,140.18 (+1,140.18) | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 10 | $1.75 | $0.23 | $+3.91 | $60.59 | ▲ $11,139.95 (+1,139.95) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `MOS` | 3 | $24.00 | $0.75 | $-1.48 | $131.85 | ▲ $11,223.53 (+1,223.53) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `OCUL` | 2 | $10.49 | $0.24 | $-1.32 | $152.59 | ▲ $11,223.29 (+1,223.29) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 2 | $8.26 | $0.19 | $-0.40 | $168.92 | ▲ $11,223.10 (+1,223.10) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 4 | $4.69 | $0.22 | $-2.60 | $187.46 | ▲ $11,222.88 (+1,222.88) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 106 | $42.10 | $2.36 | $+141.61 | $4,647.70 | ▲ $11,361.24 (+1,361.24) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `CRK` | 230 | $15.70 | $3.03 | $+364.30 | $8,255.66 | ▲ $11,358.20 (+1,358.20) | dropped from list after 5 sess (min 5) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 1246 | $2.49 | $16.31 | $-156.98 | $11,341.90 | ▲ $11,341.90 (+1,341.90) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 91 | $49.76 | $2.26 | — | $6,811.48 | ▲ $11,339.64 (+1,339.64) | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+10.6; leftover $4536.76 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 41 | $41.31 | $2.11 | — | $5,115.65 | ▲ $11,337.52 (+1,337.52) | 40% to #1, rest split; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.1; leftover $1701.28 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 520 | $3.27 | $6.71 | — | $3,408.54 | ▲ $11,330.81 (+1,330.81) | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+13.8; leftover $1701.28 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 220 | $7.70 | $2.84 | — | $1,711.71 | ▲ $11,327.98 (+1,327.98) | 40% to #1, rest split; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.7; leftover $1701.28 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 13 | $125.94 | $2.03 | — | $72.46 | ▲ $11,325.95 (+1,325.95) | 40% to #1, rest split; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+6.8; leftover $1701.28 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 1 | $10.41 | $0.11 | — | $61.94 | ▲ $12,020.61 (+2,020.61) | 40% to #1, rest split; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ⚪; ret5=+11.1; leftover $14.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |

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
| 2026-08-14 | `TLN` | cash | leftover split 64.23 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 13.76 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 13.76 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 13.76 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 13.76 < 1 share @ 57.61 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `EOG` | cash | leftover split 10.67 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 10.67 < 1 share @ 202.70 |
| 2026-08-17 | `ELF` | cash | leftover split 10.67 < 1 share @ 90.54 |
| 2026-08-18 | `BTSG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `IREN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TPG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TGTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `SLS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `HIMS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `INO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TNDM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `DVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `TGB` | min_hold | dropped but min-hold 1/5 sess — no sell |
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
| 2026-08-19 | `MARA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `LDI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `DVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-08-20 | `MARA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `LDI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `BTBT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `DVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `TMC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `TGB` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `DNN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `HNST` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-21 | `DVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `TMC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `TGB` | min_hold | dropped but min-hold 4/5 sess — no sell |
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
| 2026-08-21 | `AU` | cash | leftover split 64.02 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 13.72 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 13.72 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 13.72 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/5 sess — no sell |
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
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `INSP` | cash | leftover split 23.54 < 1 share @ 61.47 |
| 2026-08-25 | `HCA` | cash | leftover split 23.54 < 1 share @ 429.24 |
| 2026-08-26 | `AG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `BHP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `CDE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `HDSN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `IAG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `KGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `NFGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `WPM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `ARCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AUTL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRDL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `HCA` | no_price | no 09:30 open |
| 2026-08-26 | `INSP` | no_price | no 09:30 open |
| 2026-08-27 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AUTL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRDL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CYPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `OCUL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `CRMD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `RZLT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `MOS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `OCUL` | min_hold | dropped but min-hold 4/5 sess — no sell |
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
| 2026-09-04 | `ASND` | cash | leftover split 28.98 < 1 share @ 266.94 |
| 2026-09-04 | `OSCR` | cash | leftover split 14.49 < 1 share @ 30.65 |
| 2026-09-04 | `BVS` | cash | leftover split 14.49 < 1 share @ 14.50 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 91 | 2026-09-03 @ $49.76 | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+10.6; leftover $4536.76 |
| `HRMY` | 41 | 2026-09-03 @ $41.31 | 40% to #1, rest split; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.1; leftover $1701.28 |
| `CABA` | 520 | 2026-09-03 @ $3.27 | 40% to #1, rest split; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+13.8; leftover $1701.28 |
| `VSTM` | 220 | 2026-09-03 @ $7.70 | 40% to #1, rest split; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.7; leftover $1701.28 |
| `RVTY` | 13 | 2026-09-03 @ $125.94 | 40% to #1, rest split; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+6.8; leftover $1701.28 |
| `NVAX` | 1 | 2026-09-04 @ $10.41 | 40% to #1, rest split; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ⚪; ret5=+11.1; leftover $14.49 |
