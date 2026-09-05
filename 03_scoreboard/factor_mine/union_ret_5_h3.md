# Factor mine action — `union_ret_5_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `ret_5` · size `leftover` · sell `list` · S-boost `none` · rank by ret_5

Cash book **+15.42%** ($11,542) · signal-only (no cash/fees) was +21.11%. Starts YES **14/17**. Fills 109 · skips 164 · realized $+1189.04.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `ret_5` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $7.87.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | TNDM, INO, IREN, TPG, VOR, SLS, TGTX, BTSG | — | $114.01 | $10,151.49 | $10,265.50 | TNDM×53, INO×1543, IREN×27, TPG×24, VOR×56, SLS×106, TGTX×25, BTSG×20 | BUY TNDM x53 @ 23.33; BUY INO x1543 @ 0.81; BUY IREN x27 @ 45.98; BUY TPG x24 @ 50.62; BUY VOR x56 @ 22.01; BUY SLS x106 @ 11.70; BUY TGTX x25 @ 49.70; BUY BTSG x20 @ 59.80 |
| 2026-08-14 | +5.50 | $114.01 | TNDM×53, INO×1543, IREN×27, TPG×24, VOR×56, SLS×106, TGTX×25, BTSG×20 | ZENA, AIRO, BCAR | — | $77.12 | $10,479.50 | $10,556.62 | TNDM×53, INO×1543, IREN×27, TPG×24, VOR×56, SLS×106, TGTX×25, BTSG×20, ZENA×6, AIRO×1, BCAR×2 | BUY ZENA x6 @ 2.20; BUY AIRO x1 @ 11.12; BUY BCAR x2 @ 6.09 |
| 2026-08-17 | +2.25 | $77.12 | TNDM×53, INO×1543, IREN×27, TPG×24, VOR×56, SLS×106, TGTX×25, BTSG×20, ZENA×6, AIRO×1, BCAR×2 | XHG, CAPR, KOPN, NPWR | — | $46.51 | $10,577.49 | $10,624.01 | TNDM×53, INO×1543, IREN×27, TPG×24, VOR×56, SLS×106, TGTX×25, BTSG×20, ZENA×6, AIRO×1, BCAR×2, XHG×2, CAPR×1, KOPN×1, NPWR×5 | BUY XHG x2 @ 4.19; BUY CAPR x1 @ 6.87; BUY KOPN x1 @ 5.43; BUY NPWR x5 @ 1.92 |
| 2026-08-18 | -6.20 | $46.51 | TNDM×53, INO×1543, IREN×27, TPG×24, VOR×56, SLS×106, TGTX×25, BTSG×20, ZENA×6, AIRO×1, BCAR×2, XHG×2, CAPR×1, KOPN×1, NPWR×5 | — | TNDM, INO, IREN, TPG, VOR, SLS, TGTX, BTSG | $10,415.30 | $60.81 | $10,476.11 | ZENA×6, AIRO×1, BCAR×2, XHG×2, CAPR×1, KOPN×1, NPWR×5 | SELL TNDM (dropped from list after 3 sess (min 3)); SELL INO (dropped from list after 3 sess (min 3)); SELL IREN (dropped from list after 3 sess (min 3)); SELL TPG (dropped from list after 3 sess (min 3)); SELL VOR (dropped from list after 3 sess (min 3)); SELL SLS (dropped from list after 3 sess (min 3)); SELL TGTX (dropped from list after 3 sess (min 3)); SELL BTSG (dropped from list after 3 sess (min 3)); hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $10,415.30 | ZENA×6, AIRO×1, BCAR×2, XHG×2, CAPR×1, KOPN×1, NPWR×5 | — | ZENA, AIRO, BCAR | $10,446.70 | $29.92 | $10,476.62 | XHG×2, CAPR×1, KOPN×1, NPWR×5 | SELL ZENA (dropped from list after 3 sess (min 3)); SELL AIRO (dropped from list after 3 sess (min 3)); SELL BCAR (dropped from list after 3 sess (min 3)); hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $10,446.70 | XHG×2, CAPR×1, KOPN×1, NPWR×5 | MRNA, CYPH, AZI, BTGO, BNTX, AUTL, ASST, BRR | XHG, CAPR, KOPN, NPWR | $76.33 | $10,452.75 | $10,529.08 | MRNA×8, CYPH×1138, AZI×955, BTGO×198, BNTX×12, AUTL×530, ASST×81, BRR×629 | SELL XHG (dropped from list after 3 sess (min 3)); SELL CAPR (dropped from list after 3 sess (min 3)); SELL KOPN (dropped from list after 3 sess (min 3)); SELL NPWR (dropped from list after 3 sess (min 3)); BUY MRNA x8 @ 150.14; BUY CYPH x1138 @ 1.15; BUY AZI x955 @ 1.37; BUY BTGO x198 @ 6.61; BUY BNTX x12 @ 109.06; BUY AUTL x530 @ 2.47; BUY ASST x81 @ 16.00; BUY BRR x629 @ 2.08 |
| 2026-08-21 | +3.25 | $76.33 | MRNA×8, CYPH×1138, AZI×955, BTGO×198, BNTX×12, AUTL×530, ASST×81, BRR×629 | CAPR, ARCT, IOVA, INO, CAN, INDP | — | $11.01 | $11,087.09 | $11,098.10 | MRNA×8, CYPH×1138, AZI×955, BTGO×198, BNTX×12, AUTL×530, ASST×81, BRR×629, CAPR×1, ARCT×1, IOVA×1, INO×10, CAN×43, INDP×9 | BUY CAPR x1 @ 6.81; BUY ARCT x1 @ 11.13; BUY IOVA x1 @ 9.08; BUY INO x10 @ 1.23; BUY CAN x43 @ 0.29; BUY INDP x9 @ 1.39 |
| 2026-08-24 | -5.17 | $11.01 | MRNA×8, CYPH×1138, AZI×955, BTGO×198, BNTX×12, AUTL×530, ASST×81, BRR×629, CAPR×1, ARCT×1, IOVA×1, INO×10, CAN×43, INDP×9 | — | — | $11.01 | $11,326.66 | $11,337.67 | MRNA×8, CYPH×1138, AZI×955, BTGO×198, BNTX×12, AUTL×530, ASST×81, BRR×629, CAPR×1, ARCT×1, IOVA×1, INO×10, CAN×43, INDP×9 | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $11.01 | MRNA×8, CYPH×1138, AZI×955, BTGO×198, BNTX×12, AUTL×530, ASST×81, BRR×629, CAPR×1, ARCT×1, IOVA×1, INO×10, CAN×43, INDP×9 | DFDV, BMNR, AU, RUM, HMY, FWDI | MRNA, AZI, BTGO, BNTX, AUTL, BRR | $121.45 | $11,064.50 | $11,185.95 | CYPH×1138, ASST×81, CAPR×1, ARCT×1, IOVA×1, INO×10, CAN×43, INDP×9, DFDV×300, BMNR×52, AU×10, RUM×137, HMY×56, FWDI×215 | SELL MRNA (dropped from list after 3 sess (min 3)); SELL AZI (dropped from list after 3 sess (min 3)); SELL BTGO (dropped from list after 3 sess (min 3)); SELL BNTX (dropped from list after 3 sess (min 3)); SELL AUTL (dropped from list after 3 sess (min 3)); SELL BRR (dropped from list after 3 sess (min 3)); BUY DFDV x300 @ 4.29; BUY BMNR x52 @ 24.73; BUY AU x10 @ 119.46; BUY RUM x137 @ 9.36; BUY HMY x56 @ 22.65; BUY FWDI x215 @ 5.99 |
| 2026-08-26 | +2.02 | $121.45 | CYPH×1138, ASST×81, CAPR×1, ARCT×1, IOVA×1, INO×10, CAN×43, INDP×9, DFDV×300, BMNR×52, AU×10, RUM×137, HMY×56, FWDI×215 | — | — | $121.45 | $11,302.22 | $11,423.67 | CYPH×1138, ASST×81, CAPR×1, ARCT×1, IOVA×1, INO×10, CAN×43, INDP×9, DFDV×300, BMNR×52, AU×10, RUM×137, HMY×56, FWDI×215 | hold CYPH,ASST,CAPR,ARCT,IOVA,INO,CAN,INDP,DFDV,BMNR,AU,RUM,HMY,FWDI |
| 2026-08-27 | — | $121.45 | CYPH×1138, ASST×81, CAPR×1, ARCT×1, IOVA×1, INO×10, CAN×43, INDP×9, DFDV×300, BMNR×52, AU×10, RUM×137, HMY×56, FWDI×215 | MOS, DLO, MRVL, SLI, PLTR, TX, RRC, GEN | CYPH, ASST, CAPR, ARCT, IOVA, INO, CAN, INDP | $395.46 | $10,927.60 | $11,323.06 | DFDV×300, BMNR×52, AU×10, RUM×137, HMY×56, FWDI×215, MOS×18, DLO×29, MRVL×1, SLI×177, PLTR×2, TX×8, RRC×11, GEN×15 | SELL CYPH (dropped from list after 5 sess (min 3)); SELL ASST (dropped from list after 5 sess (min 3)); SELL CAPR (dropped from list after 4 sess (min 3)); SELL ARCT (dropped from list after 4 sess (min 3)); SELL IOVA (dropped from list after 4 sess (min 3)); SELL INO (dropped from list after 4 sess (min 3)); SELL CAN (dropped from list after 4 sess (min 3)); SELL INDP (dropped from list after 4 sess (min 3)); BUY MOS x18 @ 24.84; BUY DLO x29 @ 15.60; BUY MRVL x1 @ 240.00; BUY SLI x177 @ 2.59; BUY PLTR x2 @ 170.60; BUY TX x8 @ 55.20; BUY RRC x11 @ 40.72; BUY GEN x15 @ 28.89 |
| 2026-08-28 | +0.75 | $395.46 | DFDV×300, BMNR×52, AU×10, RUM×137, HMY×56, FWDI×215, MOS×18, DLO×29, MRVL×1, SLI×177, PLTR×2, TX×8, RRC×11, GEN×15 | FIGR, WPM, SCCO, AMTX, SBSW, EQX, ERO, TXG | DFDV, BMNR, AU, RUM, HMY, FWDI | $313.27 | $11,229.05 | $11,542.32 | MOS×18, DLO×29, MRVL×1, SLI×177, PLTR×2, TX×8, RRC×11, GEN×15, FIGR×27, WPM×6, SCCO×4, AMTX×546, SBSW×85, EQX×75, ERO×26, TXG×15 | SELL DFDV (dropped from list after 3 sess (min 3)); SELL BMNR (dropped from list after 3 sess (min 3)); SELL AU (dropped from list after 3 sess (min 3)); SELL RUM (dropped from list after 3 sess (min 3)); SELL HMY (dropped from list after 3 sess (min 3)); SELL FWDI (dropped from list after 3 sess (min 3)); BUY FIGR x27 @ 37.42; BUY WPM x6 @ 155.89; BUY SCCO x4 @ 214.82; BUY AMTX x546 @ 1.87; BUY SBSW x85 @ 12.01; BUY EQX x75 @ 13.57; BUY ERO x26 @ 39.20; BUY TXG x15 @ 64.10 |
| 2026-08-31 | -5.85 | $313.27 | MOS×18, DLO×29, MRVL×1, SLI×177, PLTR×2, TX×8, RRC×11, GEN×15, FIGR×27, WPM×6, SCCO×4, AMTX×546, SBSW×85, EQX×75, ERO×26, TXG×15 | — | — | $313.27 | $10,923.22 | $11,236.49 | MOS×18, DLO×29, MRVL×1, SLI×177, PLTR×2, TX×8, RRC×11, GEN×15, FIGR×27, WPM×6, SCCO×4, AMTX×546, SBSW×85, EQX×75, ERO×26, TXG×15 | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $313.27 | MOS×18, DLO×29, MRVL×1, SLI×177, PLTR×2, TX×8, RRC×11, GEN×15, FIGR×27, WPM×6, SCCO×4, AMTX×546, SBSW×85, EQX×75, ERO×26, TXG×15 | — | MOS, DLO, MRVL, SLI, PLTR, TX, RRC, GEN | $3,570.89 | $7,476.34 | $11,047.23 | FIGR×27, WPM×6, SCCO×4, AMTX×546, SBSW×85, EQX×75, ERO×26, TXG×15 | SELL MOS (dropped from list after 3 sess (min 3)); SELL DLO (dropped from list after 3 sess (min 3)); SELL MRVL (dropped from list after 3 sess (min 3)); SELL SLI (dropped from list after 3 sess (min 3)); SELL PLTR (dropped from list after 3 sess (min 3)); SELL TX (dropped from list after 3 sess (min 3)); SELL RRC (dropped from list after 3 sess (min 3)); SELL GEN (dropped from list after 3 sess (min 3)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $3,570.89 | FIGR×27, WPM×6, SCCO×4, AMTX×546, SBSW×85, EQX×75, ERO×26, TXG×15 | — | FIGR, AMTX, ERO, TXG | $7,402.97 | $3,580.62 | $10,983.59 | WPM×6, SCCO×4, SBSW×85, EQX×75 | SELL FIGR (dropped from list after 3 sess (min 3)); SELL AMTX (dropped from list after 3 sess (min 3)); SELL ERO (dropped from list after 3 sess (min 3)); SELL TXG (dropped from list after 3 sess (min 3)); hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $7,402.97 | WPM×6, SCCO×4, SBSW×85, EQX×75 | MRNA, ARCT, CAN, ALEC, DEFT, FUTU | SBSW, EQX | $116.46 | $11,146.01 | $11,262.47 | WPM×6, SCCO×4, MRNA×10, ARCT×95, CAN×5216, ALEC×652, DEFT×2335, FUTU×12 | SELL SBSW (dropped from list after 4 sess (min 3)); SELL EQX (dropped from list after 4 sess (min 3)); BUY MRNA x10 @ 151.40; BUY ARCT x95 @ 16.46; BUY CAN x5216 @ 0.30; BUY ALEC x652 @ 2.40; BUY DEFT x2335 @ 0.67; BUY FUTU x12 @ 119.46 |
| 2026-09-04 | — | $116.46 | WPM×6, SCCO×4, MRNA×10, ARCT×95, CAN×5216, ALEC×652, DEFT×2335, FUTU×12 | OABI, BRR, HQ | — | $7.87 | $11,534.39 | $11,542.26 | WPM×6, SCCO×4, MRNA×10, ARCT×95, CAN×5216, ALEC×652, DEFT×2335, FUTU×12, OABI×7, BRR×16, HQ×2 | BUY OABI x7 @ 5.08; BUY BRR x16 @ 2.36; BUY HQ x2 @ 17.06 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $8,761.36 | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $7,494.40 | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $6,250.87 | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $5,033.85 | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 56 | $22.01 | $2.16 | — | $3,799.14 | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $2,556.63 | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $1,312.06 | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $114.01 | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 6 | $2.20 | $0.15 | — | $100.66 | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $14.25 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 1 | $11.12 | $0.11 | — | $89.43 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $14.25 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BCAR` | 2 | $6.09 | $0.13 | — | $77.12 | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+27.6; leftover $14.25 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 2 | $4.19 | $0.09 | — | $68.65 | rank by ret_5; rank ret_5; list yday_mover; ⚪; ret5=+291.8; leftover $9.64 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 1 | $6.87 | $0.07 | — | $61.71 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+62.6; leftover $9.64 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `KOPN` | 1 | $5.43 | $0.06 | — | $56.22 | rank by ret_5; rank ret_5; list yday_gainer; ⚪; ret5=+28.8; leftover $9.64 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 5 | $1.92 | $0.11 | — | $46.51 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $9.64 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 53 | $22.16 | $2.17 | $-66.33 | $1,218.82 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1543 | $1.14 | $20.17 | $+471.89 | $2,957.67 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 27 | $43.56 | $2.09 | $-69.50 | $4,131.70 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 24 | $51.77 | $2.08 | $+23.38 | $5,372.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 56 | $22.82 | $2.18 | $+41.02 | $6,647.84 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 106 | $12.66 | $2.34 | $+97.12 | $7,987.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 25 | $49.28 | $2.08 | $-14.65 | $9,217.37 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 20 | $60.00 | $2.07 | $-0.12 | $10,415.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ZENA` | 6 | $2.01 | $0.16 | $-1.45 | $10,427.21 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `AIRO` | 1 | $9.10 | $0.11 | $-2.25 | $10,436.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BCAR` | 2 | $5.32 | $0.13 | $-1.80 | $10,446.70 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `XHG` | 2 | $4.10 | $0.11 | $-0.38 | $10,454.79 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `CAPR` | 1 | $7.66 | $0.10 | $+0.62 | $10,462.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `KOPN` | 1 | $4.87 | $0.07 | $-0.69 | $10,467.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `NPWR` | 5 | $1.64 | $0.12 | $-1.63 | $10,475.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $9,272.10 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1309.40 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1138 | $1.15 | $14.68 | — | $7,948.72 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1309.40 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 955 | $1.37 | $12.32 | — | $6,628.05 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $1309.40 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 198 | $6.61 | $2.58 | — | $5,317.68 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1309.40 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BNTX` | 12 | $109.06 | $2.03 | — | $4,006.93 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+22.0; leftover $1309.40 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 530 | $2.47 | $6.84 | — | $2,690.99 | rank by ret_5; rank ret_5; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1309.40 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 81 | $16.00 | $2.23 | — | $1,392.76 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1309.40 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BRR` | 629 | $2.08 | $8.11 | — | $76.33 | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+18.0; leftover $1309.40 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 1 | $6.81 | $0.07 | — | $69.44 | rank by ret_5; rank ret_5; list yday_mover; 🔵; ret5=+62.5; leftover $12.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $58.20 | rank by ret_5; rank ret_5; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $12.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 1 | $9.08 | $0.09 | — | $49.03 | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $12.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `INO` | 10 | $1.23 | $0.15 | — | $36.57 | rank by ret_5; rank ret_5; list mover_buy; 🔵; ⚪; ret5=+34.4; leftover $12.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAN` | 43 | $0.29 | $0.26 | — | $23.68 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $12.72 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `INDP` | 9 | $1.39 | $0.15 | — | $11.01 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+30.2; leftover $12.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `MRNA` | 8 | $141.19 | $2.03 | $-75.65 | $1,138.50 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AZI` | 955 | $1.33 | $12.49 | $-63.01 | $2,396.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BTGO` | 198 | $6.89 | $2.63 | $+51.22 | $3,757.75 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BNTX` | 12 | $113.13 | $2.05 | $+44.77 | $5,113.27 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AUTL` | 530 | $2.32 | $6.93 | $-93.27 | $6,335.93 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BRR` | 629 | $2.25 | $8.23 | $+90.59 | $7,742.95 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `DFDV` | 300 | $4.29 | $3.87 | — | $6,452.08 | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+28.3; leftover $1290.49 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMNR` | 52 | $24.73 | $2.15 | — | $5,163.98 | rank by ret_5; rank ret_5; list yday_gainer; ret5=+26.3; leftover $1290.49 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 10 | $119.46 | $2.02 | — | $3,967.36 | rank by ret_5; rank ret_5; list mover_buy; 🔵; ⚪; ret5=+25.9; leftover $1290.49 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 137 | $9.36 | $2.40 | — | $2,682.64 | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+21.3; leftover $1290.49 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HMY` | 56 | $22.65 | $2.16 | — | $1,412.08 | rank by ret_5; rank ret_5; list mover_buy; ⚪; ret5=+21.1; leftover $1290.49 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FWDI` | 215 | $5.99 | $2.77 | — | $121.45 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+20.7; leftover $1290.49 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 1138 | $1.60 | $14.88 | $+482.54 | $1,927.37 | dropped from list after 5 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ASST` | 81 | $20.72 | $2.26 | $+377.83 | $3,603.43 | dropped from list after 5 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 1 | $8.29 | $0.11 | $+1.30 | $3,611.61 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 1 | $15.35 | $0.18 | $+3.93 | $3,626.79 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `IOVA` | 1 | $8.34 | $0.11 | $-0.94 | $3,635.02 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `INO` | 10 | $1.28 | $0.18 | $+0.17 | $3,647.64 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAN` | 43 | $0.40 | $0.32 | $+3.98 | $3,664.52 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `INDP` | 9 | $1.09 | $0.15 | $-3.00 | $3,674.19 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 18 | $24.84 | $2.04 | — | $3,225.02 | rank by ret_5; rank ret_5; list flatten; ret5=+13.0; leftover $459.27 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 29 | $15.60 | $2.08 | — | $2,770.55 | rank by ret_5; rank ret_5; list mover_buy; 🔵; ret5=+7.1; leftover $459.27 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 1 | $240.00 | $1.99 | — | $2,528.55 | rank by ret_5; rank ret_5; list mover_buy; 🔵; ret5=+6.8; leftover $459.27 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 177 | $2.59 | $2.52 | — | $2,067.60 | rank by ret_5; rank ret_5; list flatten; ret5=+4.2; leftover $459.27 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PLTR` | 2 | $170.60 | $2.00 | — | $1,724.41 | rank by ret_5; rank ret_5; list mover_buy; 🔵; ret5=+3.4; leftover $459.27 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `TX` | 8 | $55.20 | $2.01 | — | $1,280.79 | rank by ret_5; rank ret_5; list mover_buy; 🔵; ret5=+3.0; leftover $459.27 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 11 | $40.72 | $2.02 | — | $830.85 | rank by ret_5; rank ret_5; list flatten; ret5=+1.8; leftover $459.27 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 15 | $28.89 | $2.04 | — | $395.46 | rank by ret_5; rank ret_5; list mover_buy; 🔵; ret5=+1.6; leftover $459.27 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `DFDV` | 300 | $4.81 | $3.93 | $+148.20 | $1,834.53 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMNR` | 52 | $25.91 | $2.17 | $+57.05 | $3,179.69 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 10 | $117.41 | $2.04 | $-24.56 | $4,351.75 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RUM` | 137 | $9.51 | $2.43 | $+15.71 | $5,652.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HMY` | 56 | $20.70 | $2.18 | $-113.54 | $6,809.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FWDI` | 215 | $6.39 | $2.82 | $+80.41 | $8,180.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `FIGR` | 27 | $37.42 | $2.07 | — | $7,167.82 | rank by ret_5; rank ret_5; list yday_mover; ret5=+24.4; leftover $1022.53 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `WPM` | 6 | $155.89 | $2.01 | — | $6,230.47 | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+17.6; leftover $1022.53 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SCCO` | 4 | $214.82 | $2.00 | — | $5,369.19 | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+17.0; leftover $1022.53 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AMTX` | 546 | $1.87 | $7.04 | — | $4,341.13 | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+16.9; leftover $1022.53 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SBSW` | 85 | $12.01 | $2.25 | — | $3,318.03 | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+16.8; leftover $1022.53 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `EQX` | 75 | $13.57 | $2.21 | — | $2,298.07 | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+16.6; leftover $1022.53 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERO` | 26 | $39.20 | $2.07 | — | $1,276.80 | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+16.6; leftover $1022.53 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `TXG` | 15 | $64.10 | $2.04 | — | $313.27 | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+16.1; leftover $1022.53 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `MOS` | 18 | $24.00 | $2.06 | $-19.23 | $743.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `DLO` | 29 | $14.88 | $2.10 | $-25.05 | $1,172.63 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `MRVL` | 1 | $210.57 | $2.01 | $-33.44 | $1,381.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 177 | $2.70 | $2.56 | $+14.39 | $1,856.52 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `PLTR` | 2 | $185.52 | $2.02 | $+25.83 | $2,225.55 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `TX` | 8 | $54.82 | $2.03 | $-7.09 | $2,662.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `RRC` | 11 | $41.32 | $2.04 | $+2.53 | $3,114.55 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `GEN` | 15 | $30.56 | $2.06 | $+20.96 | $3,570.89 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FIGR` | 27 | $35.46 | $2.09 | $-57.08 | $4,526.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `AMTX` | 546 | $1.88 | $7.14 | $-8.73 | $5,545.56 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ERO` | 26 | $35.95 | $2.09 | $-88.66 | $6,478.17 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-02 09:30 ET | **SELL** | `TXG` | 15 | $61.79 | $2.06 | $-38.74 | $7,402.97 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **SELL** | `SBSW` | 85 | $12.37 | $2.27 | $+26.09 | $8,452.15 | dropped from list after 4 sess (min 3) | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SELL** | `EQX` | 75 | $12.54 | $2.24 | $-81.70 | $9,390.41 | dropped from list after 4 sess (min 3) | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 10 | $151.40 | $2.02 | — | $7,874.39 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1565.07 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 95 | $16.46 | $2.27 | — | $6,308.41 | rank by ret_5; rank ret_5; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1565.07 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CAN` | 5216 | $0.30 | $31.30 | — | $4,712.32 | rank by ret_5; rank ret_5; list yday_mover; 🔵; ret5=+54.3; leftover $1565.07 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ALEC` | 652 | $2.40 | $8.41 | — | $3,139.11 | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+20.4; leftover $1565.07 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DEFT` | 2335 | $0.67 | $22.65 | — | $1,552.01 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1565.07 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FUTU` | 12 | $119.46 | $2.03 | — | $116.46 | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+17.5; leftover $1565.07 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 7 | $5.08 | $0.38 | — | $80.53 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $38.82 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 16 | $2.36 | $0.43 | — | $42.34 | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+28.0; leftover $38.82 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `HQ` | 2 | $17.06 | $0.35 | — | $7.87 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+17.3; leftover $38.82 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🔴 vol🔴 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `QMCO` | cash | leftover split 14.25 < 1 share @ 24.68 |
| 2026-08-14 | `ARX` | cash | leftover split 14.25 < 1 share @ 19.57 |
| 2026-08-14 | `BRUN` | cash | leftover split 14.25 < 1 share @ 26.25 |
| 2026-08-14 | `SNDK` | cash | leftover split 14.25 < 1 share @ 1646.93 |
| 2026-08-14 | `TBBB` | cash | leftover split 14.25 < 1 share @ 48.82 |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `ZENA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AIRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BCAR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `STDN` | cash | leftover split 9.64 < 1 share @ 13.64 |
| 2026-08-17 | `HTFL` | cash | leftover split 9.64 < 1 share @ 41.23 |
| 2026-08-17 | `UMAC` | cash | leftover split 9.64 < 1 share @ 32.55 |
| 2026-08-17 | `SMJF` | cash | leftover split 9.64 < 1 share @ 10.10 |
| 2026-08-18 | `ZENA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AIRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BCAR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `XHG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `KOPN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CRDL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ALM` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `XHG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `KOPN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NMAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ANGX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AZI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BTGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BNTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AZI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BTGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BNTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CAN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `INDP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `KGC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SBSW` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `IOVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CAN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `INDP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ASST` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CAPR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ARCT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `IOVA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `INO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CAN` | no_price | no 09:30 open — carry |
| 2026-08-26 | `DFDV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMNR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `RUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `HMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FWDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-26 | `BRR` | no_price | no 09:30 open |
| 2026-08-26 | `SENS` | no_price | no 09:30 open |
| 2026-08-26 | `SUJA` | no_price | no 09:30 open |
| 2026-08-26 | `DEFT` | no_price | no 09:30 open |
| 2026-08-26 | `WPM` | no_price | no 09:30 open |
| 2026-08-26 | `FUTU` | no_price | no 09:30 open |
| 2026-08-27 | `DFDV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMNR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FWDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `MOS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `DLO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `MRVL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `SLI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `PLTR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `TX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `RRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `GEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MOS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `DLO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `MRVL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `PLTR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `TX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `GEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `FIGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SCCO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SBSW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `EQX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ERO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TXG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CAN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DFDV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MSTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FWDI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `FIGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SCCO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `EQX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ERO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ALEC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SUJA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `GUTS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PRQR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SUJA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SSRM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CAN` | min_hold | dropped but min-hold 1/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `WPM` | 6 | 2026-08-28 @ $155.89 | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+17.6; leftover $1022.53 |
| `SCCO` | 4 | 2026-08-28 @ $214.82 | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+17.0; leftover $1022.53 |
| `MRNA` | 10 | 2026-09-03 @ $151.40 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1565.07 |
| `ARCT` | 95 | 2026-09-03 @ $16.46 | rank by ret_5; rank ret_5; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1565.07 |
| `CAN` | 5216 | 2026-09-03 @ $0.30 | rank by ret_5; rank ret_5; list yday_mover; 🔵; ret5=+54.3; leftover $1565.07 |
| `ALEC` | 652 | 2026-09-03 @ $2.40 | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+20.4; leftover $1565.07 |
| `DEFT` | 2335 | 2026-09-03 @ $0.67 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1565.07 |
| `FUTU` | 12 | 2026-09-03 @ $119.46 | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+17.5; leftover $1565.07 |
| `OABI` | 7 | 2026-09-04 @ $5.08 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $38.82 |
| `BRR` | 16 | 2026-09-04 @ $2.36 | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+28.0; leftover $38.82 |
| `HQ` | 2 | 2026-09-04 @ $17.06 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+17.3; leftover $38.82 |
