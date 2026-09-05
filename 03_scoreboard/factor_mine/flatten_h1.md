# Factor mine action — `flatten_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

Side **long** · universe `flatten` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **+15.53%** ($11,553) · signal-only (no cash/fees) was +21.67%. Starts YES **16/17**. Fills 110 · skips 41 · realized $+1228.89.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `flatten` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $140.95.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $97.53 | $10,055.59 | $10,153.12 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | BUY BTSG x20 @ 59.80; BUY IREN x27 @ 45.98; BUY TPG x24 @ 50.62; BUY TGTX x25 @ 49.70; BUY SLS x106 @ 11.70; BUY HIMS x42 @ 29.74; BUY INO x1543 @ 0.81; BUY TNDM x53 @ 23.33 |
| 2026-08-14 | +5.50 | $97.53 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $592.27 | $9,601.64 | $10,193.91 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×22, MARA×140, LDI×1353, BTBT×845 | SELL BTSG (dropped from list after 1 sess (min 1)); SELL IREN (dropped from list after 1 sess (min 1)); SELL TPG (dropped from list after 1 sess (min 1)); SELL TGTX (dropped from list after 1 sess (min 1)); SELL SLS (dropped from list after 1 sess (min 1)); SELL HIMS (dropped from list after 1 sess (min 1)); SELL INO (dropped from list after 1 sess (min 1)); SELL TNDM (dropped from list after 1 sess (min 1)); BUY TLN x3 @ 359.83; BUY VST x8 @ 146.90; BUY NRG x10 @ 120.00; BUY DAVE x3 @ 330.91; BUY SLG x22 @ 57.61; BUY MARA x140 @ 9.01; BUY LDI x1353 @ 0.94; BUY BTBT x845 @ 1.50 |
| 2026-08-17 | +2.25 | $592.27 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×22, MARA×140, LDI×1353, BTBT×845 | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | $191.62 | $9,981.47 | $10,173.09 | DVN×27, EOG×8, FANG×6, TMC×313, TGB×150, ELF×14, DNN×391, HNST×263 | SELL TLN (dropped from list after 1 sess (min 1)); SELL VST (dropped from list after 1 sess (min 1)); SELL NRG (dropped from list after 1 sess (min 1)); SELL DAVE (dropped from list after 1 sess (min 1)); SELL SLG (dropped from list after 1 sess (min 1)); SELL MARA (dropped from list after 1 sess (min 1)); SELL LDI (dropped from list after 1 sess (min 1)); SELL BTBT (dropped from list after 1 sess (min 1)); BUY DVN x27 @ 46.18; BUY EOG x8 @ 142.77; BUY FANG x6 @ 202.70; BUY TMC x313 @ 4.05; BUY TGB x150 @ 8.46; BUY ELF x14 @ 90.54; BUY DNN x391 @ 3.24; BUY HNST x263 @ 4.81 |
| 2026-08-18 | -6.20 | $191.62 | DVN×27, EOG×8, FANG×6, TMC×313, TGB×150, ELF×14, DNN×391, HNST×263 | — | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST | $10,101.41 | $0.00 | $10,101.41 | — | SELL DVN (dropped from list after 1 sess (min 1)); SELL EOG (dropped from list after 1 sess (min 1)); SELL FANG (dropped from list after 1 sess (min 1)); SELL TMC (dropped from list after 1 sess (min 1)); SELL TGB (dropped from list after 1 sess (min 1)); SELL ELF (dropped from list after 1 sess (min 1)); SELL DNN (dropped from list after 1 sess (min 1)); SELL HNST (dropped from list after 1 sess (min 1)); hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $10,101.41 | — | — | — | $10,101.41 | $0.00 | $10,101.41 | — | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $10,101.41 | — | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $203.57 | $10,107.56 | $10,311.13 | AG×61, BHP×13, CDE×61, HDSN×218, IAG×64, KGC×42, NFGC×721, WPM×8 | BUY AG x61 @ 20.55; BUY BHP x13 @ 91.01; BUY CDE x61 @ 20.65; BUY HDSN x218 @ 5.77; BUY IAG x64 @ 19.63; BUY KGC x42 @ 29.63; BUY NFGC x721 @ 1.75; BUY WPM x8 @ 144.54 |
| 2026-08-21 | +3.25 | $203.57 | AG×61, BHP×13, CDE×61, HDSN×218, IAG×64, KGC×42, NFGC×721, WPM×8 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $14.75 | $10,767.18 | $10,781.93 | AU×11, AUPH×76, AEM×6, ARCT×118, AUTL×534, CRDL×683, CRSP×22, CYPH×999 | SELL AG (dropped from list after 1 sess (min 1)); SELL BHP (dropped from list after 1 sess (min 1)); SELL CDE (dropped from list after 1 sess (min 1)); SELL HDSN (dropped from list after 1 sess (min 1)); SELL IAG (dropped from list after 1 sess (min 1)); SELL KGC (dropped from list after 1 sess (min 1)); SELL NFGC (dropped from list after 1 sess (min 1)); SELL WPM (dropped from list after 1 sess (min 1)); BUY AU x11 @ 119.43; BUY AUPH x76 @ 17.20; BUY AEM x6 @ 216.30; BUY ARCT x118 @ 11.13; BUY AUTL x534 @ 2.47; BUY CRDL x683 @ 1.93; BUY CRSP x22 @ 59.72; BUY CYPH x999 @ 1.32 |
| 2026-08-24 | -5.17 | $14.75 | AU×11, AUPH×76, AEM×6, ARCT×118, AUTL×534, CRDL×683, CRSP×22, CYPH×999 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $11,087.96 | $0.00 | $11,087.96 | — | SELL AU (dropped from list after 1 sess (min 1)); SELL AUPH (dropped from list after 1 sess (min 1)); SELL AEM (dropped from list after 1 sess (min 1)); SELL ARCT (dropped from list after 1 sess (min 1)); SELL AUTL (dropped from list after 1 sess (min 1)); SELL CRDL (dropped from list after 1 sess (min 1)); SELL CRSP (dropped from list after 1 sess (min 1)); SELL CYPH (dropped from list after 1 sess (min 1)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $11,087.96 | — | MOS, OCUL, INSP, CRMD, RZLT, HCA | — | $148.56 | $10,922.39 | $11,070.95 | MOS×76, OCUL×169, INSP×30, CRMD×223, RZLT×353, HCA×4 | BUY MOS x76 @ 24.00; BUY OCUL x169 @ 10.92; BUY INSP x30 @ 61.47; BUY CRMD x223 @ 8.28; BUY RZLT x353 @ 5.23; BUY HCA x4 @ 429.24 |
| 2026-08-26 | +2.02 | $148.56 | MOS×76, OCUL×169, INSP×30, CRMD×223, RZLT×353, HCA×4 | — | — | $148.56 | $10,923.17 | $11,071.73 | MOS×76, OCUL×169, INSP×30, CRMD×223, RZLT×353, HCA×4 | hold MOS,OCUL,INSP,CRMD,RZLT,HCA |
| 2026-08-27 | — | $148.56 | MOS×76, OCUL×169, INSP×30, CRMD×223, RZLT×353, HCA×4 | RRC, CRK, SLI | OCUL, INSP, CRMD, RZLT, HCA | $28.34 | $11,117.44 | $11,145.78 | MOS×76, RRC×74, CRK×216, SLI×1178 | SELL OCUL (dropped from list after 2 sess (min 1)); SELL INSP (dropped from list after 2 sess (min 1)); SELL CRMD (dropped from list after 2 sess (min 1)); SELL RZLT (dropped from list after 2 sess (min 1)); SELL HCA (dropped from list after 2 sess (min 1)); BUY RRC x74 @ 40.72; BUY CRK x216 @ 14.09; BUY SLI x1178 @ 2.59 |
| 2026-08-28 | +0.75 | $28.34 | MOS×76, RRC×74, CRK×216, SLI×1178 | — | — | $28.34 | $11,154.96 | $11,183.30 | MOS×76, RRC×74, CRK×216, SLI×1178 | hold MOS,RRC,CRK,SLI |
| 2026-08-31 | -5.85 | $28.34 | MOS×76, RRC×74, CRK×216, SLI×1178 | — | MOS, RRC, CRK, SLI | $10,954.47 | $0.00 | $10,954.47 | — | SELL MOS (dropped from list after 4 sess (min 1)); SELL RRC (dropped from list after 2 sess (min 1)); SELL CRK (dropped from list after 2 sess (min 1)); SELL SLI (dropped from list after 2 sess (min 1)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $10,954.47 | — | — | — | $10,954.47 | $0.00 | $10,954.47 | — | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $10,954.47 | — | — | — | $10,954.47 | $0.00 | $10,954.47 | — | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $10,954.47 | — | ATRC, HRMY, CABA, VSTM, RVTY | — | $41.58 | $11,477.53 | $11,519.11 | ATRC×44, HRMY×53, CABA×669, VSTM×284, RVTY×17 | BUY ATRC x44 @ 49.76; BUY HRMY x53 @ 41.31; BUY CABA x669 @ 3.27; BUY VSTM x284 @ 7.70; BUY RVTY x17 @ 125.94 |
| 2026-09-04 | — | $41.58 | ATRC×44, HRMY×53, CABA×669, VSTM×284, RVTY×17 | ASND, OSCR, NVAX, BVS | HRMY, VSTM, RVTY | $140.95 | $11,412.16 | $11,553.11 | ATRC×44, CABA×669, ASND×6, OSCR×55, NVAX×164, BVS×117 | SELL HRMY (dropped from list after 1 sess (min 1)); SELL VSTM (dropped from list after 1 sess (min 1)); SELL RVTY (dropped from list after 1 sess (min 1)); BUY ASND x6 @ 266.94; BUY OSCR x55 @ 30.65; BUY NVAX x164 @ 10.41; BUY BVS x117 @ 14.50 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | ▼ $9,997.95 (-2.05) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | ▼ $9,995.88 (-4.12) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | ▼ $9,993.82 (-6.18) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | ▼ $9,991.75 (-8.25) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | ▼ $9,989.44 (-10.56) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | ▼ $9,987.33 (-12.67) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | ▼ $9,970.20 (-29.80) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | ▼ $9,968.05 (-31.95) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $1,288.46 | ▲ $10,176.05 (+176.05) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $2,476.80 | ▲ $10,173.96 (+173.96) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $3,801.68 | ▲ $10,171.88 (+171.88) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 25 | $47.27 | $2.08 | $-64.90 | $4,981.35 | ▲ $10,169.80 (+169.80) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $6,293.41 | ▲ $10,167.46 (+167.46) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $7,515.57 | ▲ $10,165.32 (+165.32) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $8,931.32 | ▲ $10,146.08 (+146.08) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $10,143.91 | ▲ $10,143.91 (+143.91) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $9,062.42 | ▲ $10,141.91 (+141.91) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+5.9; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $7,885.21 | ▲ $10,139.90 (+139.90) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+3.6; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $6,683.19 | ▲ $10,137.88 (+137.88) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+0.6; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $5,688.46 | ▲ $10,135.88 (+135.88) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-8.6; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 22 | $57.61 | $2.06 | — | $4,418.98 | ▲ $10,133.82 (+133.82) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+5.7; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 140 | $9.01 | $2.41 | — | $3,155.17 | ▲ $10,131.41 (+131.41) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-13.5; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1353 | $0.94 | $16.74 | — | $1,870.67 | ▲ $10,114.67 (+114.67) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.5; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 845 | $1.50 | $10.90 | — | $592.27 | ▲ $10,103.77 (+103.77) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 3 | $367.88 | $2.02 | $+20.13 | $1,693.89 | ▲ $10,194.18 (+194.18) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 8 | $149.37 | $2.03 | $+15.71 | $2,886.82 | ▲ $10,192.15 (+192.15) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $4,158.78 | ▲ $10,190.11 (+190.11) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 3 | $336.94 | $2.02 | $+14.07 | $5,167.58 | ▲ $10,188.09 (+188.09) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 22 | $55.37 | $2.08 | $-53.41 | $6,383.64 | ▲ $10,186.02 (+186.02) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 140 | $9.22 | $2.44 | $+24.55 | $7,672.00 | ▲ $10,183.57 (+183.57) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 1353 | $0.91 | $16.57 | $-73.89 | $8,882.61 | ▲ $10,167.01 (+167.01) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 845 | $1.52 | $11.05 | $-5.05 | $10,155.96 | ▲ $10,155.96 (+155.96) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 27 | $46.18 | $2.07 | — | $8,907.02 | ▲ $10,153.88 (+153.88) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+6.7; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $7,762.85 | ▲ $10,151.87 (+151.87) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+5.8; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $6,544.64 | ▲ $10,149.86 (+149.86) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+8.3; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 313 | $4.05 | $4.04 | — | $5,272.95 | ▲ $10,145.82 (+145.82) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 150 | $8.46 | $2.44 | — | $4,001.51 | ▲ $10,143.38 (+143.38) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.4; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 14 | $90.54 | $2.03 | — | $2,731.92 | ▲ $10,141.35 (+141.35) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=-7.2; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 391 | $3.24 | $5.04 | — | $1,460.04 | ▲ $10,136.31 (+136.31) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 263 | $4.81 | $3.39 | — | $191.62 | ▲ $10,132.92 (+132.92) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-11.4; leftover $1269.49 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 27 | $48.00 | $2.09 | $+44.98 | $1,485.52 | ▲ $10,122.66 (+122.66) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 8 | $148.04 | $2.03 | $+38.11 | $2,667.81 | ▲ $10,120.63 (+120.63) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $3,919.36 | ▲ $10,118.60 (+118.60) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 313 | $3.72 | $4.10 | $-111.43 | $5,079.62 | ▲ $10,114.50 (+114.50) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 150 | $8.55 | $2.48 | $+8.58 | $6,359.65 | ▲ $10,112.03 (+112.03) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 14 | $93.44 | $2.05 | $+36.52 | $7,665.76 | ▲ $10,109.98 (+109.98) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 391 | $3.11 | $5.12 | $-60.99 | $8,876.65 | ▲ $10,104.86 (+104.86) | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `HNST` | 263 | $4.67 | $3.45 | $-43.66 | $10,101.41 | ▲ $10,101.41 (+101.41) | dropped from list after 1 sess (min 1) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 61 | $20.55 | $2.17 | — | $8,845.69 | ▲ $10,099.24 (+99.24) | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $1262.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,660.53 | ▲ $10,097.21 (+97.21) | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $1262.68 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 61 | $20.65 | $2.17 | — | $6,398.71 | ▲ $10,095.04 (+95.04) | baseline list, no extra gate; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $1262.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 218 | $5.77 | $2.81 | — | $5,138.03 | ▲ $10,092.22 (+92.22) | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $1262.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 64 | $19.63 | $2.18 | — | $3,879.53 | ▲ $10,090.04 (+90.04) | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $1262.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $2,632.96 | ▲ $10,087.92 (+87.92) | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $1262.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 721 | $1.75 | $9.30 | — | $1,361.90 | ▲ $10,078.62 (+78.62) | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $1262.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $203.57 | ▲ $10,076.61 (+76.61) | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $1262.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 61 | $21.90 | $2.19 | $+77.98 | $1,537.28 | ▲ $10,578.66 (+578.66) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,779.59 | ▲ $10,576.61 (+576.61) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 61 | $21.75 | $2.19 | $+62.73 | $4,104.14 | ▲ $10,574.41 (+574.41) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 218 | $5.67 | $2.86 | $-27.47 | $5,337.35 | ▲ $10,571.56 (+571.56) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 64 | $21.17 | $2.20 | $+94.17 | $6,690.02 | ▲ $10,569.35 (+569.35) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 42 | $32.17 | $2.14 | $+102.43 | $8,039.02 | ▲ $10,567.21 (+567.21) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 721 | $1.79 | $9.43 | $+10.11 | $9,320.18 | ▲ $10,557.78 (+557.78) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $10,555.75 | ▲ $10,555.75 (+555.75) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 11 | $119.43 | $2.02 | — | $9,240.00 | ▲ $10,553.73 (+553.73) | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+20.4; leftover $1319.47 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 76 | $17.20 | $2.22 | — | $7,930.58 | ▲ $10,551.51 (+551.51) | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $1319.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 6 | $216.30 | $2.01 | — | $6,630.77 | ▲ $10,549.50 (+549.50) | baseline list, no extra gate; list flatten,ohlc_hot,mover_buy; live flatten mover; 🔵; ⚪; ret5=+17.6; leftover $1319.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 118 | $11.13 | $2.34 | — | $5,315.09 | ▲ $10,547.16 (+547.16) | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $1319.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 534 | $2.47 | $6.89 | — | $3,989.22 | ▲ $10,540.27 (+540.27) | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $1319.47 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 683 | $1.93 | $8.81 | — | $2,662.22 | ▲ $10,531.46 (+531.46) | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $1319.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 22 | $59.72 | $2.06 | — | $1,346.32 | ▲ $10,529.40 (+529.40) | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.6; leftover $1319.47 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 999 | $1.32 | $12.89 | — | $14.75 | ▲ $10,516.51 (+516.51) | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $1319.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 11 | $120.50 | $2.04 | $+7.70 | $1,338.21 | ▲ $11,125.67 (+1,125.67) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 76 | $16.60 | $2.24 | $-50.06 | $2,597.57 | ▲ $11,123.43 (+1,123.43) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 6 | $217.03 | $2.03 | $+0.34 | $3,897.72 | ▲ $11,121.40 (+1,121.40) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 118 | $13.26 | $2.38 | $+246.62 | $5,460.03 | ▲ $11,119.03 (+1,119.03) | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 534 | $2.36 | $6.99 | $-72.62 | $6,713.28 | ▲ $11,112.04 (+1,112.04) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 683 | $1.87 | $8.93 | $-58.72 | $7,981.55 | ▲ $11,103.10 (+1,103.10) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 22 | $58.79 | $2.08 | $-24.59 | $9,272.86 | ▲ $11,101.03 (+1,101.03) | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 999 | $1.83 | $13.07 | $+483.54 | $11,087.96 | ▲ $11,087.96 (+1,087.96) | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 76 | $24.00 | $2.22 | — | $9,261.74 | ▲ $11,085.74 (+1,085.74) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.0; leftover $1847.99 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 169 | $10.92 | $2.50 | — | $7,413.77 | ▲ $11,083.25 (+1,083.25) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+10.4; leftover $1847.99 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 30 | $61.47 | $2.08 | — | $5,567.59 | ▲ $11,081.17 (+1,081.17) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+9.2; leftover $1847.99 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 223 | $8.28 | $2.88 | — | $3,718.27 | ▲ $11,078.29 (+1,078.29) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.8; leftover $1847.99 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 353 | $5.23 | $4.55 | — | $1,867.52 | ▲ $11,073.73 (+1,073.73) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+10.7; leftover $1847.99 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 4 | $429.24 | $2.00 | — | $148.56 | ▲ $11,071.73 (+1,071.73) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.1; leftover $1847.99 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 169 | $10.79 | $2.54 | $-27.01 | $1,969.53 | ▲ $11,055.80 (+1,055.80) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 30 | $60.07 | $2.10 | $-46.18 | $3,769.53 | ▲ $11,053.70 (+1,053.70) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 223 | $8.60 | $2.93 | $+65.55 | $5,684.40 | ▲ $11,050.77 (+1,050.77) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 353 | $5.01 | $4.63 | $-86.84 | $7,448.30 | ▲ $11,046.14 (+1,046.14) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 4 | $427.50 | $2.03 | $-10.99 | $9,156.28 | ▲ $11,044.12 (+1,044.12) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 74 | $40.72 | $2.21 | — | $6,140.79 | ▲ $11,041.91 (+1,041.91) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.8; leftover $3052.09 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 216 | $14.09 | $2.79 | — | $3,094.56 | ▲ $11,039.12 (+1,039.12) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.1; leftover $3052.09 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 1178 | $2.59 | $15.20 | — | $28.34 | ▲ $11,023.92 (+1,023.92) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.2; leftover $3052.09 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 76 | $23.75 | $2.25 | $-23.46 | $1,831.10 | ▲ $10,974.98 (+974.98) | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 74 | $41.11 | $2.25 | $+24.40 | $4,870.99 | ▲ $10,972.73 (+972.73) | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 216 | $14.56 | $2.85 | $+95.89 | $8,013.10 | ▲ $10,969.88 (+969.88) | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 1178 | $2.51 | $15.42 | $-124.85 | $10,954.47 | ▲ $10,954.47 (+954.47) | dropped from list after 2 sess (min 1) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 44 | $49.76 | $2.12 | — | $8,762.90 | ▲ $10,952.34 (+952.34) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+10.6; leftover $2190.89 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 53 | $41.31 | $2.15 | — | $6,571.33 | ▲ $10,950.20 (+950.20) | baseline list, no extra gate; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.1; leftover $2190.89 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 669 | $3.27 | $8.63 | — | $4,375.07 | ▲ $10,941.57 (+941.57) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+13.8; leftover $2190.89 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 284 | $7.70 | $3.66 | — | $2,184.60 | ▲ $10,937.90 (+937.90) | baseline list, no extra gate; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.7; leftover $2190.89 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 17 | $125.94 | $2.04 | — | $41.58 | ▲ $10,935.86 (+935.86) | baseline list, no extra gate; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+6.8; leftover $2190.89 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 53 | $42.93 | $2.18 | $+81.53 | $2,314.69 | ▲ $11,602.05 (+1,602.05) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 284 | $8.03 | $3.73 | $+86.33 | $4,591.48 | ▲ $11,598.32 (+1,598.32) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 17 | $132.45 | $2.07 | $+106.56 | $6,841.07 | ▲ $11,596.26 (+1,596.26) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASND` | 6 | $266.94 | $2.01 | — | $5,237.42 | ▲ $11,594.25 (+1,594.25) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.9; leftover $1710.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OSCR` | 55 | $30.65 | $2.15 | — | $3,549.51 | ▲ $11,592.09 (+1,592.09) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-2.2; leftover $1710.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 164 | $10.41 | $2.48 | — | $1,839.79 | ▲ $11,589.61 (+1,589.61) | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ⚪; ret5=+11.1; leftover $1710.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 117 | $14.50 | $2.34 | — | $140.95 | ▲ $11,587.27 (+1,587.27) | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.8; leftover $1710.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |

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

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 44 | 2026-09-03 @ $49.76 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+10.6; leftover $2190.89 |
| `CABA` | 669 | 2026-09-03 @ $3.27 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+13.8; leftover $2190.89 |
| `ASND` | 6 | 2026-09-04 @ $266.94 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.9; leftover $1710.27 |
| `OSCR` | 55 | 2026-09-04 @ $30.65 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-2.2; leftover $1710.27 |
| `NVAX` | 164 | 2026-09-04 @ $10.41 | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ⚪; ret5=+11.1; leftover $1710.27 |
| `BVS` | 117 | 2026-09-04 @ $14.50 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.8; leftover $1710.27 |
