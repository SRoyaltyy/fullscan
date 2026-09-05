# Factor mine action — `flatten_h3_rankw`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

Side **long** · universe `flatten` · top 8 · rank `list` · size `rank_w` · sell `list` · S-boost `none` · rank-weighted leftover

Cash book **+7.59%** ($10,759) · signal-only (no cash/fees) was +44.29%. Starts YES **17/17**. Fills 78 · skips 127 · realized $+347.12.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `flatten` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `rank_w` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $85.01.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $128.05 | $9,988.98 | $10,117.03 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11 | BUY BTSG x37 @ 59.80; BUY IREN x42 @ 45.98; BUY TPG x32 @ 50.62; BUY TGTX x27 @ 49.70; BUY SLS x94 @ 11.70; BUY HIMS x28 @ 29.74; BUY INO x685 @ 0.81; BUY TNDM x11 @ 23.33 |
| 2026-08-14 | +5.50 | $128.05 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11 | MARA, LDI, BTBT | — | $109.27 | $10,151.44 | $10,260.71 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11, MARA×1, LDI×7, BTBT×2 | BUY MARA x1 @ 9.01; BUY LDI x7 @ 0.94; BUY BTBT x2 @ 1.50 |
| 2026-08-17 | +2.25 | $109.27 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11, MARA×1, LDI×7, BTBT×2 | TMC, TGB, DNN | — | $85.16 | $10,205.01 | $10,290.17 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11, MARA×1, LDI×7, BTBT×2, TMC×3, TGB×1, DNN×1 | BUY TMC x3 @ 4.05; BUY TGB x1 @ 8.46; BUY DNN x1 @ 3.24 |
| 2026-08-18 | -6.20 | $85.16 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11, MARA×1, LDI×7, BTBT×2, TMC×3, TGB×1, DNN×1 | — | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $10,092.52 | $41.14 | $10,133.65 | MARA×1, LDI×7, BTBT×2, TMC×3, TGB×1, DNN×1 | SELL BTSG (dropped from list after 3 sess (min 3)); SELL IREN (dropped from list after 3 sess (min 3)); SELL TPG (dropped from list after 3 sess (min 3)); SELL TGTX (dropped from list after 3 sess (min 3)); SELL SLS (dropped from list after 3 sess (min 3)); SELL HIMS (dropped from list after 3 sess (min 3)); SELL INO (dropped from list after 3 sess (min 3)); SELL TNDM (dropped from list after 3 sess (min 3)); hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $10,092.52 | MARA×1, LDI×7, BTBT×2, TMC×3, TGB×1, DNN×1 | — | MARA, LDI, BTBT | $10,110.16 | $23.60 | $10,133.76 | TMC×3, TGB×1, DNN×1 | SELL MARA (dropped from list after 3 sess (min 3)); SELL LDI (dropped from list after 3 sess (min 3)); SELL BTBT (dropped from list after 3 sess (min 3)); hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $10,110.16 | TMC×3, TGB×1, DNN×1 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | TMC, TGB, DNN | $232.84 | $10,099.90 | $10,332.74 | AG×109, BHP×21, CDE×81, HDSN×243, IAG×57, KGC×28, NFGC×321, WPM×1 | SELL TMC (dropped from list after 3 sess (min 3)); SELL TGB (dropped from list after 3 sess (min 3)); SELL DNN (dropped from list after 3 sess (min 3)); BUY AG x109 @ 20.55; BUY BHP x21 @ 91.01; BUY CDE x81 @ 20.65; BUY HDSN x243 @ 5.77; BUY IAG x57 @ 19.63; BUY KGC x28 @ 29.63; BUY NFGC x321 @ 1.75; BUY WPM x1 @ 144.54 |
| 2026-08-21 | +3.25 | $232.84 | AG×109, BHP×21, CDE×81, HDSN×243, IAG×57, KGC×28, NFGC×321, WPM×1 | AUPH, ARCT, AUTL, CRDL, CYPH | — | $125.76 | $10,382.36 | $10,508.12 | AG×109, BHP×21, CDE×81, HDSN×243, IAG×57, KGC×28, NFGC×321, WPM×1, AUPH×2, ARCT×2, AUTL×10, CRDL×10, CYPH×4 | BUY AUPH x2 @ 17.20; BUY ARCT x2 @ 11.13; BUY AUTL x10 @ 2.47; BUY CRDL x10 @ 1.93; BUY CYPH x4 @ 1.32 |
| 2026-08-24 | -5.17 | $125.76 | AG×109, BHP×21, CDE×81, HDSN×243, IAG×57, KGC×28, NFGC×321, WPM×1, AUPH×2, ARCT×2, AUTL×10, CRDL×10, CYPH×4 | — | — | $125.76 | $10,288.85 | $10,414.61 | AG×109, BHP×21, CDE×81, HDSN×243, IAG×57, KGC×28, NFGC×321, WPM×1, AUPH×2, ARCT×2, AUTL×10, CRDL×10, CYPH×4 | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $125.76 | AG×109, BHP×21, CDE×81, HDSN×243, IAG×57, KGC×28, NFGC×321, WPM×1, AUPH×2, ARCT×2, AUTL×10, CRDL×10, CYPH×4 | MOS, OCUL, INSP, CRMD, RZLT, HCA | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $59.46 | $10,352.95 | $10,412.41 | AUPH×2, ARCT×2, AUTL×10, CRDL×10, CYPH×4, MOS×123, OCUL×225, INSP×32, CRMD×178, RZLT×188, HCA×1 | SELL AG (dropped from list after 3 sess (min 3)); SELL BHP (dropped from list after 3 sess (min 3)); SELL CDE (dropped from list after 3 sess (min 3)); SELL HDSN (dropped from list after 3 sess (min 3)); SELL IAG (dropped from list after 3 sess (min 3)); SELL KGC (dropped from list after 3 sess (min 3)); SELL NFGC (dropped from list after 3 sess (min 3)); SELL WPM (dropped from list after 3 sess (min 3)); BUY MOS x123 @ 24.00; BUY OCUL x225 @ 10.92; BUY INSP x32 @ 61.47; BUY CRMD x178 @ 8.28; BUY RZLT x188 @ 5.23; BUY HCA x1 @ 429.24 |
| 2026-08-26 | +2.02 | $59.46 | AUPH×2, ARCT×2, AUTL×10, CRDL×10, CYPH×4, MOS×123, OCUL×225, INSP×32, CRMD×178, RZLT×188, HCA×1 | — | — | $59.46 | $10,373.46 | $10,432.92 | AUPH×2, ARCT×2, AUTL×10, CRDL×10, CYPH×4, MOS×123, OCUL×225, INSP×32, CRMD×178, RZLT×188, HCA×1 | hold AUPH,ARCT,AUTL,CRDL,CYPH,MOS,OCUL,INSP,CRMD,RZLT,HCA |
| 2026-08-27 | — | $59.46 | AUPH×2, ARCT×2, AUTL×10, CRDL×10, CYPH×4, MOS×123, OCUL×225, INSP×32, CRMD×178, RZLT×188, HCA×1 | RRC, CRK, SLI | AUPH, ARCT, AUTL, CRDL, CYPH | $4.82 | $10,410.44 | $10,415.26 | MOS×123, OCUL×225, INSP×32, CRMD×178, RZLT×188, HCA×1, RRC×2, CRK×4, SLI×11 | SELL AUPH (dropped from list after 4 sess (min 3)); SELL ARCT (dropped from list after 4 sess (min 3)); SELL AUTL (dropped from list after 4 sess (min 3)); SELL CRDL (dropped from list after 4 sess (min 3)); SELL CYPH (dropped from list after 4 sess (min 3)); BUY RRC x2 @ 40.72; BUY CRK x4 @ 14.09; BUY SLI x11 @ 2.59 |
| 2026-08-28 | +0.75 | $4.82 | MOS×123, OCUL×225, INSP×32, CRMD×178, RZLT×188, HCA×1, RRC×2, CRK×4, SLI×11 | — | OCUL, INSP, CRMD, RZLT, HCA | $7,260.52 | $3,093.28 | $10,353.80 | MOS×123, RRC×2, CRK×4, SLI×11 | SELL OCUL (dropped from list after 3 sess (min 3)); SELL INSP (dropped from list after 3 sess (min 3)); SELL CRMD (dropped from list after 3 sess (min 3)); SELL RZLT (dropped from list after 3 sess (min 3)); SELL HCA (dropped from list after 3 sess (min 3)) |
| 2026-08-31 | -5.85 | $7,260.52 | MOS×123, RRC×2, CRK×4, SLI×11 | — | MOS | $10,179.36 | $169.21 | $10,348.57 | RRC×2, CRK×4, SLI×11 | SELL MOS (dropped from list after 4 sess (min 3)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $10,179.36 | RRC×2, CRK×4, SLI×11 | — | RRC, CRK, SLI | $10,347.14 | $0.00 | $10,347.14 | — | SELL RRC (dropped from list after 3 sess (min 3)); SELL CRK (dropped from list after 3 sess (min 3)); SELL SLI (dropped from list after 3 sess (min 3)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $10,347.14 | — | — | — | $10,347.14 | $0.00 | $10,347.14 | — | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $10,347.14 | — | ATRC, HRMY, CABA, VSTM, RVTY | — | $95.53 | $10,803.99 | $10,899.52 | ATRC×69, HRMY×66, CABA×632, VSTM×179, RVTY×5 | BUY ATRC x69 @ 49.76; BUY HRMY x66 @ 41.31; BUY CABA x632 @ 3.27; BUY VSTM x179 @ 7.70; BUY RVTY x5 @ 125.94 |
| 2026-09-04 | — | $95.53 | ATRC×69, HRMY×66, CABA×632, VSTM×179, RVTY×5 | NVAX | — | $85.01 | $10,673.77 | $10,758.78 | ATRC×69, HRMY×66, CABA×632, VSTM×179, RVTY×5, NVAX×1 | BUY NVAX x1 @ 10.41 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 37 | $59.80 | $2.10 | — | $7,785.30 | ▼ $9,997.90 (-2.10) | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $2222.22 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 42 | $45.98 | $2.12 | — | $5,852.02 | ▼ $9,995.78 (-4.22) | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+12.3; leftover $1944.44 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 32 | $50.62 | $2.09 | — | $4,229.99 | ▼ $9,993.70 (-6.30) | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+6.2; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 27 | $49.70 | $2.07 | — | $2,886.02 | ▼ $9,991.63 (-8.37) | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $1388.89 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 94 | $11.70 | $2.27 | — | $1,783.95 | ▼ $9,989.35 (-10.65) | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 28 | $29.74 | $2.07 | — | $949.16 | ▼ $9,987.28 (-12.72) | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $833.33 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 685 | $0.81 | $7.60 | — | $386.70 | ▼ $9,979.68 (-20.32) | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.2; leftover $555.56 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 11 | $23.33 | $2.02 | — | $128.05 | ▼ $9,977.65 (-22.35) | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+19.7; leftover $277.78 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $118.95 | ▲ $10,103.33 (+103.33) | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-13.5; leftover $10.67 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 7 | $0.94 | $0.09 | — | $112.30 | ▲ $10,103.24 (+103.24) | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.5; leftover $7.11 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 2 | $1.50 | $0.04 | — | $109.27 | ▲ $10,103.21 (+103.21) | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $3.56 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 3 | $4.05 | $0.13 | — | $96.99 | ▲ $10,281.04 (+281.04) | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; leftover $15.18 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 1 | $8.46 | $0.09 | — | $88.44 | ▲ $10,280.96 (+280.96) | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.4; leftover $12.14 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 1 | $3.24 | $0.04 | — | $85.16 | ▲ $10,280.92 (+280.92) | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; leftover $6.07 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 37 | $60.00 | $2.13 | $+3.17 | $2,303.03 | ▲ $10,155.60 (+155.60) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 42 | $43.56 | $2.14 | $-105.90 | $4,130.41 | ▲ $10,153.46 (+153.46) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 32 | $51.77 | $2.11 | $+32.50 | $5,784.94 | ▲ $10,151.35 (+151.35) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 27 | $49.28 | $2.09 | $-15.50 | $7,113.41 | ▲ $10,149.26 (+149.26) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 94 | $12.66 | $2.30 | $+85.67 | $8,301.16 | ▲ $10,146.97 (+146.97) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 28 | $27.85 | $2.09 | $-57.09 | $9,078.86 | ▲ $10,144.87 (+144.87) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 685 | $1.14 | $8.96 | $+209.49 | $9,850.80 | ▲ $10,135.91 (+135.91) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 11 | $22.16 | $2.04 | $-16.94 | $10,092.52 | ▲ $10,133.87 (+133.87) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `MARA` | 1 | $8.91 | $0.11 | $-0.31 | $10,101.32 | ▲ $10,134.00 (+134.00) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 7 | $0.88 | $0.10 | $-0.59 | $10,107.37 | ▲ $10,133.89 (+133.89) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 2 | $1.42 | $0.05 | $-0.25 | $10,110.16 | ▲ $10,133.84 (+133.84) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 3 | $3.92 | $0.15 | $-0.67 | $10,121.77 | ▲ $10,133.32 (+133.32) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGB` | 1 | $8.35 | $0.11 | $-0.30 | $10,130.02 | ▲ $10,133.22 (+133.22) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 1 | $3.20 | $0.06 | $-0.13 | $10,133.16 | ▲ $10,133.16 (+133.16) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 109 | $20.55 | $2.32 | — | $7,890.89 | ▲ $10,130.84 (+130.84) | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $2251.81 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 21 | $91.01 | $2.05 | — | $5,977.63 | ▲ $10,128.79 (+128.79) | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $1970.34 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 81 | $20.65 | $2.23 | — | $4,302.75 | ▲ $10,126.56 (+126.56) | rank-weighted leftover; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $1688.86 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 243 | $5.77 | $3.13 | — | $2,897.50 | ▲ $10,123.42 (+123.42) | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $1407.38 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 57 | $19.63 | $2.16 | — | $1,776.43 | ▲ $10,121.26 (+121.26) | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $1125.91 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 28 | $29.63 | $2.07 | — | $944.72 | ▲ $10,119.19 (+119.19) | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $844.43 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 321 | $1.75 | $4.14 | — | $378.83 | ▲ $10,115.05 (+115.05) | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $562.95 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 1 | $144.54 | $1.45 | — | $232.84 | ▲ $10,113.60 (+113.60) | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $281.48 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 2 | $17.20 | $0.35 | — | $198.09 | ▲ $10,606.01 (+606.01) | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $45.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $175.60 | ▲ $10,605.78 (+605.78) | rank-weighted leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $32.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 10 | $2.47 | $0.28 | — | $150.62 | ▲ $10,605.50 (+605.50) | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $25.87 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 10 | $1.93 | $0.22 | — | $131.10 | ▲ $10,605.28 (+605.28) | rank-weighted leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $19.40 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 4 | $1.32 | $0.06 | — | $125.76 | ▲ $10,605.22 (+605.22) | rank-weighted leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $6.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 109 | $20.73 | $2.35 | $+14.95 | $2,382.97 | ▲ $10,464.96 (+464.96) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 21 | $95.95 | $2.08 | $+99.61 | $4,395.84 | ▲ $10,462.88 (+462.88) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 81 | $20.85 | $2.26 | $+11.71 | $6,082.43 | ▲ $10,460.62 (+460.62) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 243 | $5.53 | $3.19 | $-64.64 | $7,423.04 | ▲ $10,457.44 (+457.44) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 57 | $21.63 | $2.18 | $+109.66 | $8,653.77 | ▲ $10,455.26 (+455.26) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 28 | $32.76 | $2.09 | $+83.47 | $9,568.95 | ▲ $10,453.16 (+453.16) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 321 | $1.91 | $4.20 | $+43.01 | $10,177.86 | ▲ $10,448.96 (+448.96) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 1 | $160.00 | $1.62 | $+12.39 | $10,336.23 | ▲ $10,447.33 (+447.33) | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 123 | $24.00 | $2.36 | — | $7,381.88 | ▲ $10,444.98 (+444.98) | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.0; leftover $2953.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 225 | $10.92 | $2.90 | — | $4,921.97 | ▲ $10,442.07 (+442.07) | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+10.4; leftover $2461.01 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 32 | $61.47 | $2.09 | — | $2,952.85 | ▲ $10,439.99 (+439.99) | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+9.2; leftover $1968.81 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 178 | $8.28 | $2.52 | — | $1,476.48 | ▲ $10,437.46 (+437.46) | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.8; leftover $1476.60 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 188 | $5.23 | $2.55 | — | $490.69 | ▲ $10,434.91 (+434.91) | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+10.7; leftover $984.40 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 1 | $429.24 | $1.99 | — | $59.46 | ▲ $10,432.92 (+432.92) | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.1; leftover $492.20 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `AUPH` | 2 | $16.60 | $0.36 | $-1.91 | $92.30 | ▲ $10,479.29 (+479.29) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 2 | $15.35 | $0.33 | $+7.88 | $122.67 | ▲ $10,478.96 (+478.96) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `AUTL` | 10 | $2.41 | $0.29 | $-1.17 | $146.47 | ▲ $10,478.66 (+478.66) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 10 | $2.03 | $0.25 | $+0.52 | $166.52 | ▲ $10,478.41 (+478.41) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 4 | $1.60 | $0.10 | $+0.96 | $172.83 | ▲ $10,478.32 (+478.32) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 2 | $40.72 | $0.82 | — | $90.57 | ▲ $10,477.49 (+477.49) | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.8; leftover $86.41 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 4 | $14.09 | $0.58 | — | $33.63 | ▲ $10,476.92 (+476.92) | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.1; leftover $57.61 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 11 | $2.59 | $0.32 | — | $4.82 | ▲ $10,476.60 (+476.60) | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.2; leftover $28.80 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 225 | $10.63 | $2.96 | $-71.11 | $2,393.61 | ▲ $10,390.96 (+390.96) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 32 | $62.10 | $2.11 | $+15.96 | $4,378.70 | ▲ $10,388.85 (+388.85) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 178 | $8.49 | $2.57 | $+32.29 | $5,887.36 | ▲ $10,386.28 (+386.28) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 188 | $5.07 | $2.60 | $-35.23 | $6,837.92 | ▲ $10,383.69 (+383.69) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 1 | $424.61 | $2.01 | $-8.64 | $7,260.52 | ▲ $10,381.68 (+381.68) | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 123 | $23.75 | $2.40 | $-35.51 | $10,179.36 | ▲ $10,347.43 (+347.43) | dropped from list after 4 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `RRC` | 2 | $41.32 | $0.85 | $-0.47 | $10,261.15 | ▲ $10,348.09 (+348.09) | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRK` | 4 | $14.31 | $0.60 | $-0.30 | $10,317.79 | ▲ $10,347.49 (+347.49) | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 11 | $2.70 | $0.35 | $+0.54 | $10,347.14 | ▲ $10,347.14 (+347.14) | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 69 | $49.76 | $2.20 | — | $6,911.50 | ▲ $10,344.94 (+344.94) | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+10.6; leftover $3449.05 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 66 | $41.31 | $2.19 | — | $4,182.85 | ▲ $10,342.75 (+342.75) | rank-weighted leftover; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.1; leftover $2759.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 632 | $3.27 | $8.15 | — | $2,108.06 | ▲ $10,334.60 (+334.60) | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+13.8; leftover $2069.43 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 179 | $7.70 | $2.53 | — | $727.23 | ▲ $10,332.07 (+332.07) | rank-weighted leftover; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.7; leftover $1379.62 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 5 | $125.94 | $2.00 | — | $95.53 | ▲ $10,330.07 (+330.07) | rank-weighted leftover; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+6.8; leftover $689.81 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 1 | $10.41 | $0.11 | — | $85.01 | ▲ $10,971.30 (+971.30) | rank-weighted leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ⚪; ret5=+11.1; leftover $19.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |

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
| 2026-08-14 | `TLN` | cash | leftover split 28.46 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 24.90 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 21.34 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 17.78 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 14.23 < 1 share @ 57.61 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 24.28 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 21.25 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 18.21 < 1 share @ 202.70 |
| 2026-08-17 | `ELF` | cash | leftover split 9.11 < 1 share @ 90.54 |
| 2026-08-17 | `HNST` | cash | leftover split 3.04 < 1 share @ 4.81 |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-08-21 | `AU` | cash | leftover split 51.74 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 38.81 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 12.94 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AUPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ARCT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AUTL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CRDL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
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
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ASND` | cash | leftover split 38.21 < 1 share @ 266.94 |
| 2026-09-04 | `OSCR` | cash | leftover split 28.66 < 1 share @ 30.65 |
| 2026-09-04 | `BVS` | cash | leftover split 9.55 < 1 share @ 14.50 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 69 | 2026-09-03 @ $49.76 | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+10.6; leftover $3449.05 |
| `HRMY` | 66 | 2026-09-03 @ $41.31 | rank-weighted leftover; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.1; leftover $2759.24 |
| `CABA` | 632 | 2026-09-03 @ $3.27 | rank-weighted leftover; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+13.8; leftover $2069.43 |
| `VSTM` | 179 | 2026-09-03 @ $7.70 | rank-weighted leftover; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.7; leftover $1379.62 |
| `RVTY` | 5 | 2026-09-03 @ $125.94 | rank-weighted leftover; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+6.8; leftover $689.81 |
| `NVAX` | 1 | 2026-09-04 @ $10.41 | rank-weighted leftover; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ⚪; ret5=+11.1; leftover $19.11 |
