# Factor mine action — `flatten_h5_sboost`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

Side **long** · universe `flatten` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `both` · S≥+5: sizeup + more names

Cash book **+22.18%** ($12,218) · signal-only (no cash/fees) was +67.92%. Starts YES **16/17**. Fills 79 · skips 202 · realized $+1759.87.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `flatten` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8 (S≥+5 may raise this when S-boost is `both`).
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $71.34.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM, VOR | — | $123.82 | $10,071.92 | $10,195.74 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50 | S=+8.53 more_names top_n=12; S=+8.53 sizeup x1.35; BUY BTSG x18 @ 59.80; BUY IREN x24 @ 45.98; BUY TPG x21 @ 50.62; BUY TGTX x22 @ 49.70; BUY SLS x94 @ 11.70; BUY HIMS x37 @ 29.74; BUY INO x1371 @ 0.81; BUY TNDM x47 @ 23.33; BUY VOR x50 @ 22.01 |
| 2026-08-14 | +5.50 | $123.82 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50 | MARA, LDI, BTBT | — | $87.76 | $10,346.62 | $10,434.38 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50, MARA×1, LDI×14, BTBT×9 | S=+5.50 more_names top_n=12; S=+5.50 sizeup x1.35; BUY MARA x1 @ 9.01; BUY LDI x14 @ 0.94; BUY BTBT x9 @ 1.50 |
| 2026-08-17 | +2.25 | $87.76 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50, MARA×1, LDI×14, BTBT×9 | TMC, TGB, DNN, HNST | — | $51.48 | $10,461.12 | $10,512.60 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50, MARA×1, LDI×14, BTBT×9, TMC×2, TGB×1, DNN×3, HNST×2 | BUY TMC x2 @ 4.05; BUY TGB x1 @ 8.46; BUY DNN x3 @ 3.24; BUY HNST x2 @ 4.81 |
| 2026-08-18 | -6.20 | $51.48 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50, MARA×1, LDI×14, BTBT×9, TMC×2, TGB×1, DNN×3, HNST×2 | — | — | $51.48 | $10,515.65 | $10,567.13 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50, MARA×1, LDI×14, BTBT×9, TMC×2, TGB×1, DNN×3, HNST×2 | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $51.48 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50, MARA×1, LDI×14, BTBT×9, TMC×2, TGB×1, DNN×3, HNST×2 | — | — | $51.48 | $10,936.83 | $10,988.31 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50, MARA×1, LDI×14, BTBT×9, TMC×2, TGB×1, DNN×3, HNST×2 | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $51.48 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50, MARA×1, LDI×14, BTBT×9, TMC×2, TGB×1, DNN×3, HNST×2 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM, VOR | $157.45 | $10,941.64 | $11,099.09 | MARA×1, LDI×14, BTBT×9, TMC×2, TGB×1, DNN×3, HNST×2, AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×771, WPM×9 | SELL BTSG (dropped from list after 5 sess (min 5)); SELL IREN (dropped from list after 5 sess (min 5)); SELL TPG (dropped from list after 5 sess (min 5)); SELL TGTX (dropped from list after 5 sess (min 5)); SELL SLS (dropped from list after 5 sess (min 5)); SELL HIMS (dropped from list after 5 sess (min 5)); SELL INO (dropped from list after 5 sess (min 5)); SELL TNDM (dropped from list after 5 sess (min 5)); SELL VOR (dropped from list after 5 sess (min 5)); BUY AG x65 @ 20.55; BUY BHP x14 @ 91.01; BUY CDE x65 @ 20.65; BUY HDSN x233 @ 5.77; BUY IAG x68 @ 19.63; BUY KGC x45 @ 29.63; BUY NFGC x771 @ 1.75; BUY WPM x9 @ 144.54 |
| 2026-08-21 | +3.25 | $157.45 | MARA×1, LDI×14, BTBT×9, TMC×2, TGB×1, DNN×3, HNST×2, AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×771, WPM×9 | AUPH, ARCT, AUTL, CRDL, CYPH | MARA, LDI, BTBT | $85.89 | $11,306.98 | $11,392.87 | TMC×2, TGB×1, DNN×3, HNST×2, AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×771, WPM×9, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18 | SELL MARA (dropped from list after 5 sess (min 5)); SELL LDI (dropped from list after 5 sess (min 5)); SELL BTBT (dropped from list after 5 sess (min 5)); BUY AUPH x1 @ 17.20; BUY ARCT x2 @ 11.13; BUY AUTL x9 @ 2.47; BUY CRDL x12 @ 1.93; BUY CYPH x18 @ 1.32 |
| 2026-08-24 | -5.17 | $85.89 | TMC×2, TGB×1, DNN×3, HNST×2, AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×771, WPM×9, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18 | — | TMC, TGB, DNN, HNST | $124.40 | $11,237.14 | $11,361.54 | AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×771, WPM×9, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18 | SELL TMC (dropped from list after 5 sess (min 5)); SELL TGB (dropped from list after 5 sess (min 5)); SELL DNN (dropped from list after 5 sess (min 5)); SELL HNST (dropped from list after 5 sess (min 5)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $124.40 | AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×771, WPM×9, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18 | OCUL, CRMD, RZLT | — | $80.78 | $11,290.62 | $11,371.40 | AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×771, WPM×9, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18, OCUL×1, CRMD×2, RZLT×3 | BUY OCUL x1 @ 10.92; BUY CRMD x2 @ 8.28; BUY RZLT x3 @ 5.23 |
| 2026-08-26 | +2.02 | $80.78 | AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×771, WPM×9, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18, OCUL×1, CRMD×2, RZLT×3 | — | — | $80.78 | $11,354.98 | $11,435.76 | AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×771, WPM×9, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18, OCUL×1, CRMD×2, RZLT×3 | hold AG,BHP,CDE,HDSN,IAG,KGC,NFGC,WPM,AUPH,ARCT,AUTL,CRDL,CYPH,OCUL,CRMD,RZLT |
| 2026-08-27 | — | $80.78 | AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×771, WPM×9, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18, OCUL×1, CRMD×2, RZLT×3 | RRC, CRK, MOS, SLI | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $13.96 | $11,560.50 | $11,574.46 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18, OCUL×1, CRMD×2, RZLT×3, RRC×69, CRK×201, MOS×114, SLI×1095 | SELL AG (dropped from list after 5 sess (min 5)); SELL BHP (dropped from list after 5 sess (min 5)); SELL CDE (dropped from list after 5 sess (min 5)); SELL HDSN (dropped from list after 5 sess (min 5)); SELL IAG (dropped from list after 5 sess (min 5)); SELL KGC (dropped from list after 5 sess (min 5)); SELL NFGC (dropped from list after 5 sess (min 5)); SELL WPM (dropped from list after 5 sess (min 5)); BUY RRC x69 @ 40.72; BUY CRK x201 @ 14.09; BUY MOS x114 @ 24.84; BUY SLI x1095 @ 2.59 |
| 2026-08-28 | +0.75 | $13.96 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18, OCUL×1, CRMD×2, RZLT×3, RRC×69, CRK×201, MOS×114, SLI×1095 | — | AUPH, ARCT, AUTL, CRDL, CYPH | $137.89 | $11,453.60 | $11,591.49 | OCUL×1, CRMD×2, RZLT×3, RRC×69, CRK×201, MOS×114, SLI×1095 | SELL AUPH (dropped from list after 5 sess (min 5)); SELL ARCT (dropped from list after 5 sess (min 5)); SELL AUTL (dropped from list after 5 sess (min 5)); SELL CRDL (dropped from list after 5 sess (min 5)); SELL CYPH (dropped from list after 5 sess (min 5)) |
| 2026-08-31 | -5.85 | $137.89 | OCUL×1, CRMD×2, RZLT×3, RRC×69, CRK×201, MOS×114, SLI×1095 | — | — | $137.89 | $11,299.52 | $11,437.41 | OCUL×1, CRMD×2, RZLT×3, RRC×69, CRK×201, MOS×114, SLI×1095 | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $137.89 | OCUL×1, CRMD×2, RZLT×3, RRC×69, CRK×201, MOS×114, SLI×1095 | — | OCUL, CRMD, RZLT | $178.49 | $11,566.98 | $11,745.47 | RRC×69, CRK×201, MOS×114, SLI×1095 | SELL OCUL (dropped from list after 5 sess (min 5)); SELL CRMD (dropped from list after 5 sess (min 5)); SELL RZLT (dropped from list after 5 sess (min 5)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $178.49 | RRC×69, CRK×201, MOS×114, SLI×1095 | — | — | $178.49 | $11,697.09 | $11,875.58 | RRC×69, CRK×201, MOS×114, SLI×1095 | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $178.49 | RRC×69, CRK×201, MOS×114, SLI×1095 | ATRC, HRMY, CABA, VSTM, RVTY | RRC, CRK, MOS, SLI | $121.67 | $12,241.74 | $12,363.41 | ATRC×47, HRMY×56, CABA×719, VSTM×305, RVTY×18 | SELL RRC (dropped from list after 5 sess (min 5)); SELL CRK (dropped from list after 5 sess (min 5)); SELL MOS (dropped from list after 5 sess (min 5)); SELL SLI (dropped from list after 5 sess (min 5)); BUY ATRC x47 @ 49.76; BUY HRMY x56 @ 41.31; BUY CABA x719 @ 3.27; BUY VSTM x305 @ 7.70; BUY RVTY x18 @ 125.94 |
| 2026-09-04 | — | $121.67 | ATRC×47, HRMY×56, CABA×719, VSTM×305, RVTY×18 | NVAX, BVS | — | $71.34 | $12,146.54 | $12,217.88 | ATRC×47, HRMY×56, CABA×719, VSTM×305, RVTY×18, NVAX×2, BVS×2 | BUY NVAX x2 @ 10.41; BUY BVS x2 @ 14.50 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 18 | $59.80 | $2.04 | — | $8,921.56 | ▼ $9,997.96 (-2.04) | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 24 | $45.98 | $2.06 | — | $7,815.97 | ▼ $9,995.89 (-4.11) | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+12.3; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 21 | $50.62 | $2.05 | — | $6,750.83 | ▼ $9,993.84 (-6.16) | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+6.2; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 22 | $49.70 | $2.06 | — | $5,655.38 | ▼ $9,991.78 (-8.22) | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 94 | $11.70 | $2.27 | — | $4,553.31 | ▼ $9,989.51 (-10.49) | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 37 | $29.74 | $2.10 | — | $3,450.82 | ▼ $9,987.41 (-12.59) | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1371 | $0.81 | $15.22 | — | $2,325.10 | ▼ $9,972.19 (-27.81) | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.2; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 47 | $23.33 | $2.13 | — | $1,226.46 | ▼ $9,970.06 (-29.94) | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+19.7; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 50 | $22.01 | $2.14 | — | $123.82 | ▼ $9,967.92 (-32.08) | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $114.71 | ▲ $10,219.53 (+219.53) | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-13.5; leftover $13.76 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 14 | $0.94 | $0.17 | — | $101.42 | ▲ $10,219.36 (+219.36) | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.5; leftover $13.76 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 9 | $1.50 | $0.16 | — | $87.76 | ▲ $10,219.20 (+219.20) | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $13.76 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 2 | $4.05 | $0.09 | — | $79.57 | ▲ $10,410.03 (+410.03) | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; leftover $10.97 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 1 | $8.46 | $0.09 | — | $71.02 | ▲ $10,409.94 (+409.94) | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.4; leftover $10.97 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 3 | $3.24 | $0.11 | — | $61.20 | ▲ $10,409.84 (+409.84) | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; leftover $10.97 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 2 | $4.81 | $0.10 | — | $51.48 | ▲ $10,409.73 (+409.73) | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-11.4; leftover $10.97 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 18 | $58.64 | $2.06 | $-24.99 | $1,104.93 | ▲ $10,902.13 (+902.13) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 24 | $42.46 | $2.08 | $-88.62 | $2,121.89 | ▲ $10,900.05 (+900.05) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 21 | $53.06 | $2.07 | $+47.05 | $3,234.08 | ▲ $10,897.98 (+897.98) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGTX` | 22 | $51.65 | $2.08 | $+38.77 | $4,368.30 | ▲ $10,895.90 (+895.90) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 94 | $13.84 | $2.30 | $+196.59 | $5,666.96 | ▲ $10,893.60 (+893.60) | dropped from list after 5 sess (min 5) | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SELL** | `HIMS` | 37 | $30.66 | $2.12 | $+29.82 | $6,799.26 | ▲ $10,891.48 (+891.48) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `INO` | 1371 | $1.30 | $17.93 | $+638.64 | $8,563.63 | ▲ $10,873.55 (+873.55) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 47 | $23.11 | $2.15 | $-14.62 | $9,647.65 | ▲ $10,871.40 (+871.40) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `VOR` | 50 | $23.05 | $2.16 | $+47.70 | $10,797.99 | ▲ $10,869.24 (+869.24) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 65 | $20.55 | $2.19 | — | $9,460.06 | ▲ $10,867.06 (+867.06) | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $1349.75 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $8,183.89 | ▲ $10,865.03 (+865.03) | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $1349.75 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 65 | $20.65 | $2.19 | — | $6,839.45 | ▲ $10,862.84 (+862.84) | S≥+5: sizeup + more names; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $1349.75 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 233 | $5.77 | $3.01 | — | $5,492.04 | ▲ $10,859.83 (+859.83) | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $1349.75 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 68 | $19.63 | $2.19 | — | $4,155.00 | ▲ $10,857.64 (+857.64) | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $1349.75 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 45 | $29.63 | $2.12 | — | $2,819.53 | ▲ $10,855.52 (+855.52) | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $1349.75 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 771 | $1.75 | $9.95 | — | $1,460.33 | ▲ $10,845.57 (+845.57) | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $1349.75 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 9 | $144.54 | $2.02 | — | $157.45 | ▲ $10,843.55 (+843.55) | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $1349.75 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 1 | $11.70 | $0.14 | $+2.46 | $169.01 | ▲ $11,390.96 (+1,390.96) | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 14 | $0.87 | $0.18 | $-1.34 | $180.97 | ▲ $11,390.78 (+1,390.78) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 9 | $1.66 | $0.20 | $+1.08 | $195.71 | ▲ $11,390.58 (+1,390.58) | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $178.34 | ▲ $11,390.41 (+1,390.41) | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $24.46 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $155.85 | ▲ $11,390.18 (+1,390.18) | S≥+5: sizeup + more names; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $24.46 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 9 | $2.47 | $0.25 | — | $133.37 | ▲ $11,389.93 (+1,389.93) | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $24.46 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 12 | $1.93 | $0.27 | — | $109.94 | ▲ $11,389.66 (+1,389.66) | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $24.46 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 18 | $1.32 | $0.29 | — | $85.89 | ▲ $11,389.37 (+1,389.37) | S≥+5: sizeup + more names; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $24.46 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 2 | $4.57 | $0.12 | $+0.84 | $94.91 | ▲ $11,527.56 (+1,527.56) | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `TGB` | 1 | $9.26 | $0.12 | $+0.60 | $104.06 | ▲ $11,527.45 (+1,527.45) | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 3 | $3.50 | $0.13 | $+0.54 | $114.42 | ▲ $11,527.31 (+1,527.31) | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟡 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `HNST` | 2 | $5.05 | $0.13 | $+0.25 | $124.40 | ▲ $11,527.19 (+1,527.19) | dropped from list after 5 sess (min 5) | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 1 | $10.92 | $0.11 | — | $113.36 | ▲ $11,436.09 (+1,436.09) | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+10.4; leftover $20.73 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 2 | $8.28 | $0.17 | — | $96.63 | ▲ $11,435.92 (+1,435.92) | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.8; leftover $20.73 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 3 | $5.23 | $0.17 | — | $80.78 | ▲ $11,435.76 (+1,435.76) | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+10.7; leftover $20.73 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 65 | $20.63 | $2.21 | $+0.81 | $1,419.52 | ▲ $11,533.77 (+1,533.77) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 14 | $96.99 | $2.05 | $+79.64 | $2,775.33 | ▲ $11,531.72 (+1,531.72) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 65 | $21.00 | $2.21 | $+18.36 | $4,138.12 | ▲ $11,529.51 (+1,529.51) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 233 | $5.51 | $3.05 | $-66.64 | $5,418.90 | ▲ $11,526.46 (+1,526.46) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 68 | $21.64 | $2.22 | $+132.27 | $6,888.20 | ▲ $11,524.24 (+1,524.24) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 45 | $32.90 | $2.15 | $+142.88 | $8,366.55 | ▲ $11,522.09 (+1,522.09) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 771 | $2.00 | $10.09 | $+172.72 | $9,898.47 | ▲ $11,512.01 (+1,512.01) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 9 | $160.93 | $2.04 | $+143.45 | $11,344.80 | ▲ $11,509.97 (+1,509.97) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 69 | $40.72 | $2.20 | — | $8,532.92 | ▲ $11,507.77 (+1,507.77) | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.8; leftover $2836.20 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 201 | $14.09 | $2.60 | — | $5,698.23 | ▲ $11,505.17 (+1,505.17) | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.1; leftover $2836.20 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 114 | $24.84 | $2.33 | — | $2,864.14 | ▲ $11,502.84 (+1,502.84) | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+13.0; leftover $2836.20 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 1095 | $2.59 | $14.13 | — | $13.96 | ▲ $11,488.71 (+1,488.71) | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.2; leftover $2836.20 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 1 | $16.47 | $0.19 | $-1.09 | $30.25 | ▲ $11,522.79 (+1,522.79) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 2 | $15.74 | $0.34 | $+8.65 | $61.39 | ▲ $11,522.45 (+1,522.45) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 9 | $2.32 | $0.26 | $-1.86 | $82.01 | ▲ $11,522.19 (+1,522.19) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 12 | $2.09 | $0.31 | $+1.35 | $106.78 | ▲ $11,521.88 (+1,521.88) | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 18 | $1.75 | $0.39 | $+7.06 | $137.89 | ▲ $11,521.49 (+1,521.49) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `OCUL` | 1 | $10.49 | $0.13 | $-0.67 | $148.26 | ▲ $11,598.74 (+1,598.74) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 2 | $8.26 | $0.19 | $-0.40 | $164.59 | ▲ $11,598.55 (+1,598.55) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 3 | $4.69 | $0.17 | $-1.96 | $178.49 | ▲ $11,598.38 (+1,598.38) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 69 | $42.10 | $2.23 | $+90.79 | $3,081.15 | ▲ $11,779.20 (+1,779.20) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `CRK` | 201 | $15.70 | $2.66 | $+318.36 | $6,234.20 | ▲ $11,776.55 (+1,776.55) | dropped from list after 5 sess (min 5) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **SELL** | `MOS` | 114 | $24.70 | $2.37 | $-20.67 | $9,047.62 | ▲ $11,774.17 (+1,774.17) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 1095 | $2.49 | $14.33 | $-137.95 | $11,759.84 | ▲ $11,759.84 (+1,759.84) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 47 | $49.76 | $2.13 | — | $9,418.99 | ▲ $11,757.71 (+1,757.71) | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+10.6; leftover $2351.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 56 | $41.31 | $2.16 | — | $7,103.48 | ▲ $11,755.56 (+1,755.56) | S≥+5: sizeup + more names; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.1; leftover $2351.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 719 | $3.27 | $9.28 | — | $4,743.07 | ▲ $11,746.28 (+1,746.28) | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+13.8; leftover $2351.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 305 | $7.70 | $3.93 | — | $2,390.64 | ▲ $11,742.35 (+1,742.35) | S≥+5: sizeup + more names; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.7; leftover $2351.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 18 | $125.94 | $2.04 | — | $121.67 | ▲ $11,740.30 (+1,740.30) | S≥+5: sizeup + more names; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+6.8; leftover $2351.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 2 | $10.41 | $0.21 | — | $100.64 | ▲ $12,454.12 (+2,454.12) | S≥+5: sizeup + more names; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ⚪; ret5=+11.1; leftover $30.42 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 2 | $14.50 | $0.30 | — | $71.34 | ▲ $12,453.82 (+2,453.82) | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.8; leftover $30.42 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |

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
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TLN` | cash | leftover split 13.76 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 13.76 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 13.76 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 13.76 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 13.76 < 1 share @ 57.61 |
| 2026-08-14 | `BETR` | cash | leftover split 13.76 < 1 share @ 14.80 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 10.97 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 10.97 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 10.97 < 1 share @ 202.70 |
| 2026-08-17 | `ELF` | cash | leftover split 10.97 < 1 share @ 90.54 |
| 2026-08-18 | `BTSG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `IREN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TPG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TGTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `SLS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `HIMS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `INO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TNDM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `VOR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-08-19 | `VOR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `MARA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `LDI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
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
| 2026-08-20 | `TMC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `TGB` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `DNN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `HNST` | min_hold | dropped but min-hold 3/5 sess — no sell |
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
| 2026-08-21 | `AU` | cash | leftover split 24.46 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 24.46 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 24.46 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
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
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `MOS` | cash | leftover split 20.73 < 1 share @ 24.00 |
| 2026-08-25 | `INSP` | cash | leftover split 20.73 < 1 share @ 61.47 |
| 2026-08-25 | `HCA` | cash | leftover split 20.73 < 1 share @ 429.24 |
| 2026-08-26 | `AG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `BHP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `CDE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `HDSN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `IAG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `KGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `NFGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `WPM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `AUPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `ARCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AUTL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRDL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `HCA` | no_price | no 09:30 open |
| 2026-08-26 | `MOS` | no_price | no 09:30 open |
| 2026-08-26 | `INSP` | no_price | no 09:30 open |
| 2026-08-27 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
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
| 2026-08-31 | `OCUL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `CRMD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `RZLT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `MOS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `MOS` | min_hold | dropped but min-hold 3/5 sess — no sell |
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
| 2026-09-02 | `MOS` | min_hold | dropped but min-hold 4/5 sess — no sell |
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
| 2026-09-04 | `ASND` | cash | leftover split 30.42 < 1 share @ 266.94 |
| 2026-09-04 | `OSCR` | cash | leftover split 30.42 < 1 share @ 30.65 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 47 | 2026-09-03 @ $49.76 | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+10.6; leftover $2351.97 |
| `HRMY` | 56 | 2026-09-03 @ $41.31 | S≥+5: sizeup + more names; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.1; leftover $2351.97 |
| `CABA` | 719 | 2026-09-03 @ $3.27 | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+13.8; leftover $2351.97 |
| `VSTM` | 305 | 2026-09-03 @ $7.70 | S≥+5: sizeup + more names; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.7; leftover $2351.97 |
| `RVTY` | 18 | 2026-09-03 @ $125.94 | S≥+5: sizeup + more names; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+6.8; leftover $2351.97 |
| `NVAX` | 2 | 2026-09-04 @ $10.41 | S≥+5: sizeup + more names; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ⚪; ret5=+11.1; leftover $30.42 |
| `BVS` | 2 | 2026-09-04 @ $14.50 | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.8; leftover $30.42 |
