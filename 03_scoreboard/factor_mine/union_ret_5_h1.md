# Factor mine action — `union_ret_5_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `ret_5` · size `leftover` · sell `list` · S-boost `none` · rank by ret_5

Cash book **+4.96%** ($10,496) · signal-only (no cash/fees) was -1.46%. Starts YES **11/17**. Fills 136 · skips 64 · realized $+438.34.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `ret_5` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10.38.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | TNDM, INO, IREN, TPG, VOR, SLS, TGTX, BTSG | — | $114.01 | $10,151.49 | $10,265.50 | TNDM×53, INO×1543, IREN×27, TPG×24, VOR×56, SLS×106, TGTX×25, BTSG×20 | BUY TNDM x53 @ 23.33; BUY INO x1543 @ 0.81; BUY IREN x27 @ 45.98; BUY TPG x24 @ 50.62; BUY VOR x56 @ 22.01; BUY SLS x106 @ 11.70; BUY TGTX x25 @ 49.70; BUY BTSG x20 @ 59.80 |
| 2026-08-14 | +5.50 | $114.01 | TNDM×53, INO×1543, IREN×27, TPG×24, VOR×56, SLS×106, TGTX×25, BTSG×20 | QMCO, ARX, ZENA, AIRO, BRUN, BCAR, TBBB | TNDM, INO, IREN, TPG, VOR, SLS, TGTX, BTSG | $1,325.75 | $8,515.68 | $9,841.43 | QMCO×51, ARX×65, ZENA×581, AIRO×115, BRUN×48, BCAR×210, TBBB×26 | SELL TNDM (dropped from list after 1 sess (min 1)); SELL INO (dropped from list after 1 sess (min 1)); SELL IREN (dropped from list after 1 sess (min 1)); SELL TPG (dropped from list after 1 sess (min 1)); SELL VOR (dropped from list after 1 sess (min 1)); SELL SLS (dropped from list after 1 sess (min 1)); SELL TGTX (dropped from list after 1 sess (min 1)); SELL BTSG (dropped from list after 1 sess (min 1)); BUY QMCO x51 @ 24.68; BUY ARX x65 @ 19.57; BUY ZENA x581 @ 2.20; BUY AIRO x115 @ 11.12; BUY BRUN x48 @ 26.25; BUY BCAR x210 @ 6.09; BUY TBBB x26 @ 48.82 |
| 2026-08-17 | +2.25 | $1,325.75 | QMCO×51, ARX×65, ZENA×581, AIRO×115, BRUN×48, BCAR×210, TBBB×26 | XHG, CAPR, STDN, HTFL, UMAC, KOPN, NPWR, SMJF | QMCO, ARX, ZENA, AIRO, BRUN, BCAR, TBBB | $31.98 | $9,511.45 | $9,543.43 | XHG×290, CAPR×177, STDN×89, HTFL×29, UMAC×37, KOPN×224, NPWR×634, SMJF×120 | SELL QMCO (dropped from list after 1 sess (min 1)); SELL ARX (dropped from list after 1 sess (min 1)); SELL ZENA (dropped from list after 1 sess (min 1)); SELL AIRO (dropped from list after 1 sess (min 1)); SELL BRUN (dropped from list after 1 sess (min 1)); SELL BCAR (dropped from list after 1 sess (min 1)); SELL TBBB (dropped from list after 1 sess (min 1)); BUY XHG x290 @ 4.19; BUY CAPR x177 @ 6.87; BUY STDN x89 @ 13.64; BUY HTFL x29 @ 41.23; BUY UMAC x37 @ 32.55; BUY KOPN x224 @ 5.43; BUY NPWR x634 @ 1.92; BUY SMJF x120 @ 10.10 |
| 2026-08-18 | -6.20 | $31.98 | XHG×290, CAPR×177, STDN×89, HTFL×29, UMAC×37, KOPN×224, NPWR×634, SMJF×120 | — | XHG, STDN, HTFL, UMAC, KOPN, NPWR, SMJF | $8,055.12 | $1,253.16 | $9,308.28 | CAPR×177 | SELL XHG (dropped from list after 1 sess (min 1)); SELL STDN (dropped from list after 1 sess (min 1)); SELL HTFL (dropped from list after 1 sess (min 1)); SELL UMAC (dropped from list after 1 sess (min 1)); SELL KOPN (dropped from list after 1 sess (min 1)); SELL NPWR (dropped from list after 1 sess (min 1)); SELL SMJF (dropped from list after 1 sess (min 1)); hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $8,055.12 | CAPR×177 | — | CAPR | $9,325.18 | $0.00 | $9,325.18 | — | SELL CAPR (dropped from list after 2 sess (min 1)); hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $9,325.18 | — | MRNA, CYPH, AZI, BTGO, BNTX, AUTL, ASST, BRR | — | $165.42 | $9,207.63 | $9,373.05 | MRNA×7, CYPH×1013, AZI×850, BTGO×176, BNTX×10, AUTL×471, ASST×72, BRR×560 | BUY MRNA x7 @ 150.14; BUY CYPH x1013 @ 1.15; BUY AZI x850 @ 1.37; BUY BTGO x176 @ 6.61; BUY BNTX x10 @ 109.06; BUY AUTL x471 @ 2.47; BUY ASST x72 @ 16.00; BUY BRR x560 @ 2.08 |
| 2026-08-21 | +3.25 | $165.42 | MRNA×7, CYPH×1013, AZI×850, BTGO×176, BNTX×10, AUTL×471, ASST×72, BRR×560 | CAPR, ARCT, IOVA, INO, CAN, INDP | AZI, BTGO, BNTX, AUTL, ASST, BRR | $0.65 | $9,974.03 | $9,974.68 | MRNA×7, CYPH×1013, CAPR×181, ARCT×110, IOVA×135, INO×1003, CAN×4196, INDP×860 | SELL AZI (dropped from list after 1 sess (min 1)); SELL BTGO (dropped from list after 1 sess (min 1)); SELL BNTX (dropped from list after 1 sess (min 1)); SELL AUTL (dropped from list after 1 sess (min 1)); SELL ASST (dropped from list after 1 sess (min 1)); SELL BRR (dropped from list after 1 sess (min 1)); BUY CAPR x181 @ 6.81; BUY ARCT x110 @ 11.13; BUY IOVA x135 @ 9.08; BUY INO x1003 @ 1.23; BUY CAN x4196 @ 0.29; BUY INDP x860 @ 1.39 |
| 2026-08-24 | -5.17 | $0.65 | MRNA×7, CYPH×1013, CAPR×181, ARCT×110, IOVA×135, INO×1003, CAN×4196, INDP×860 | — | MRNA, CYPH, CAPR, ARCT, IOVA, INO, CAN, INDP | $10,636.74 | $0.00 | $10,636.74 | — | SELL MRNA (dropped from list after 2 sess (min 1)); SELL CYPH (dropped from list after 2 sess (min 1)); SELL CAPR (dropped from list after 1 sess (min 1)); SELL ARCT (dropped from list after 1 sess (min 1)); SELL IOVA (dropped from list after 1 sess (min 1)); SELL INO (dropped from list after 1 sess (min 1)); SELL CAN (dropped from list after 1 sess (min 1)); SELL INDP (dropped from list after 1 sess (min 1)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $10,636.74 | — | CYPH, ASST, DFDV, BMNR, AU, RUM, HMY, FWDI | — | $45.81 | $10,355.46 | $10,401.27 | CYPH×782, ASST×63, DFDV×309, BMNR×53, AU×11, RUM×142, HMY×58, FWDI×221 | BUY CYPH x782 @ 1.70; BUY ASST x63 @ 20.90; BUY DFDV x309 @ 4.29; BUY BMNR x53 @ 24.73; BUY AU x11 @ 119.46; BUY RUM x142 @ 9.36; BUY HMY x58 @ 22.65; BUY FWDI x221 @ 5.99 |
| 2026-08-26 | +2.02 | $45.81 | CYPH×782, ASST×63, DFDV×309, BMNR×53, AU×11, RUM×142, HMY×58, FWDI×221 | — | — | $45.81 | $10,563.07 | $10,608.88 | CYPH×782, ASST×63, DFDV×309, BMNR×53, AU×11, RUM×142, HMY×58, FWDI×221 | hold CYPH,ASST,DFDV,BMNR,AU,RUM,HMY,FWDI |
| 2026-08-27 | — | $45.81 | CYPH×782, ASST×63, DFDV×309, BMNR×53, AU×11, RUM×142, HMY×58, FWDI×221 | MOS, DLO, MRVL, SLI, PLTR, TX, RRC, GEN | CYPH, ASST, DFDV, BMNR, AU, RUM, HMY, FWDI | $332.78 | $10,301.26 | $10,634.04 | MOS×53, DLO×84, MRVL×5, SLI×510, PLTR×7, TX×23, RRC×32, GEN×45 | SELL CYPH (dropped from list after 2 sess (min 1)); SELL ASST (dropped from list after 2 sess (min 1)); SELL DFDV (dropped from list after 2 sess (min 1)); SELL BMNR (dropped from list after 2 sess (min 1)); SELL AU (dropped from list after 2 sess (min 1)); SELL RUM (dropped from list after 2 sess (min 1)); SELL HMY (dropped from list after 2 sess (min 1)); SELL FWDI (dropped from list after 2 sess (min 1)); BUY MOS x53 @ 24.84; BUY DLO x84 @ 15.60; BUY MRVL x5 @ 240.00; BUY SLI x510 @ 2.59; BUY PLTR x7 @ 170.60; BUY TX x23 @ 55.20; BUY RRC x32 @ 40.72; BUY GEN x45 @ 28.89 |
| 2026-08-28 | +0.75 | $332.78 | MOS×53, DLO×84, MRVL×5, SLI×510, PLTR×7, TX×23, RRC×32, GEN×45 | FIGR, WPM, SCCO, AMTX, SBSW, EQX, ERO, TXG | MOS, DLO, MRVL, SLI, PLTR, TX, RRC, GEN | $226.85 | $10,501.50 | $10,728.35 | FIGR×35, WPM×8, SCCO×6, AMTX×712, SBSW×110, EQX×98, ERO×33, TXG×20 | SELL MOS (dropped from list after 1 sess (min 1)); SELL DLO (dropped from list after 1 sess (min 1)); SELL MRVL (dropped from list after 1 sess (min 1)); SELL SLI (dropped from list after 1 sess (min 1)); SELL PLTR (dropped from list after 1 sess (min 1)); SELL TX (dropped from list after 1 sess (min 1)); SELL RRC (dropped from list after 1 sess (min 1)); SELL GEN (dropped from list after 1 sess (min 1)); BUY FIGR x35 @ 37.42; BUY WPM x8 @ 155.89; BUY SCCO x6 @ 214.82; BUY AMTX x712 @ 1.87; BUY SBSW x110 @ 12.01; BUY EQX x98 @ 13.57; BUY ERO x33 @ 39.20; BUY TXG x20 @ 64.10 |
| 2026-08-31 | -5.85 | $226.85 | FIGR×35, WPM×8, SCCO×6, AMTX×712, SBSW×110, EQX×98, ERO×33, TXG×20 | — | FIGR, WPM, SCCO, AMTX, SBSW, EQX, ERO, TXG | $10,348.02 | $0.00 | $10,348.02 | — | SELL FIGR (dropped from list after 1 sess (min 1)); SELL WPM (dropped from list after 1 sess (min 1)); SELL SCCO (dropped from list after 1 sess (min 1)); SELL AMTX (dropped from list after 1 sess (min 1)); SELL SBSW (dropped from list after 1 sess (min 1)); SELL EQX (dropped from list after 1 sess (min 1)); SELL ERO (dropped from list after 1 sess (min 1)); SELL TXG (dropped from list after 1 sess (min 1)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $10,348.02 | — | — | — | $10,348.02 | $0.00 | $10,348.02 | — | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $10,348.02 | — | — | — | $10,348.02 | $0.00 | $10,348.02 | — | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $10,348.02 | — | MRNA, ARCT, CAN, ALEC, DEFT, WPM, FUTU, SCCO | — | $300.82 | $10,180.11 | $10,480.93 | MRNA×8, ARCT×78, CAN×4311, ALEC×538, DEFT×1930, WPM×8, FUTU×10, SCCO×6 | BUY MRNA x8 @ 151.40; BUY ARCT x78 @ 16.46; BUY CAN x4311 @ 0.30; BUY ALEC x538 @ 2.40; BUY DEFT x1930 @ 0.67; BUY WPM x8 @ 148.89; BUY FUTU x10 @ 119.46; BUY SCCO x6 @ 204.50 |
| 2026-09-04 | — | $300.82 | MRNA×8, ARCT×78, CAN×4311, ALEC×538, DEFT×1930, WPM×8, FUTU×10, SCCO×6 | OABI, BRR, HQ | MRNA, ARCT, CAN | $10.38 | $10,485.31 | $10,495.69 | ALEC×538, DEFT×1930, WPM×8, FUTU×10, SCCO×6, OABI×276, BRR×594, HQ×81 | SELL MRNA (dropped from list after 1 sess (min 1)); SELL ARCT (dropped from list after 1 sess (min 1)); SELL CAN (dropped from list after 1 sess (min 1)); BUY OABI x276 @ 5.08; BUY BRR x594 @ 2.36; BUY HQ x81 @ 17.06 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $8,761.36 | ▼ $9,997.85 (-2.15) | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $7,494.40 | ▼ $9,980.72 (-19.28) | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $6,250.87 | ▼ $9,978.65 (-21.35) | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $5,033.85 | ▼ $9,976.59 (-23.41) | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 56 | $22.01 | $2.16 | — | $3,799.14 | ▼ $9,974.43 (-25.57) | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $2,556.63 | ▼ $9,972.12 (-27.88) | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $1,312.06 | ▼ $9,970.06 (-29.94) | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $114.01 | ▼ $9,968.01 (-31.99) | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $1,326.60 | ▲ $10,274.61 (+274.61) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $2,742.35 | ▲ $10,255.37 (+255.37) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $3,930.69 | ▲ $10,253.28 (+253.28) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $5,255.56 | ▲ $10,251.19 (+251.19) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 56 | $23.33 | $2.18 | $+69.58 | $6,559.87 | ▲ $10,249.02 (+249.02) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $7,871.93 | ▲ $10,246.68 (+246.68) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 25 | $47.27 | $2.08 | $-64.90 | $9,051.59 | ▲ $10,244.59 (+244.59) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $10,242.52 | ▲ $10,242.52 (+242.52) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 51 | $24.68 | $2.14 | — | $8,981.70 | ▲ $10,240.38 (+240.38) | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+111.3; leftover $1280.32 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 65 | $19.57 | $2.19 | — | $7,707.47 | ▲ $10,238.20 (+238.20) | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1280.32 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 581 | $2.20 | $7.49 | — | $6,421.77 | ▲ $10,230.70 (+230.70) | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $1280.32 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 115 | $11.12 | $2.33 | — | $5,140.64 | ▲ $10,228.37 (+228.37) | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1280.32 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BRUN` | 48 | $26.25 | $2.13 | — | $3,878.74 | ▲ $10,226.23 (+226.23) | rank by ret_5; rank ret_5; list earn_react; 🔵; ⚪; ret5=+31.2; leftover $1280.32 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BCAR` | 210 | $6.09 | $2.71 | — | $2,597.13 | ▲ $10,223.52 (+223.52) | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+27.6; leftover $1280.32 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `TBBB` | 26 | $48.82 | $2.07 | — | $1,325.75 | ▲ $10,221.46 (+221.46) | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.0; leftover $1280.32 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 51 | $24.83 | $2.16 | $+3.34 | $2,589.91 | ▼ $9,767.94 (-232.06) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 65 | $19.57 | $2.21 | $-4.39 | $3,859.76 | ▼ $9,765.73 (-234.27) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 581 | $2.08 | $7.60 | $-81.91 | $5,063.54 | ▼ $9,758.13 (-241.87) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 115 | $9.57 | $2.36 | $-182.95 | $6,161.73 | ▼ $9,755.77 (-244.23) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BRUN` | 48 | $23.00 | $2.15 | $-160.05 | $7,263.57 | ▼ $9,753.61 (-246.39) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BCAR` | 210 | $5.99 | $2.75 | $-26.46 | $8,518.72 | ▼ $9,750.86 (-249.14) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TBBB` | 26 | $47.39 | $2.09 | $-41.34 | $9,748.77 | ▼ $9,748.77 (-251.23) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 290 | $4.19 | $3.74 | — | $8,529.93 | ▼ $9,745.03 (-254.97) | rank by ret_5; rank ret_5; list yday_mover; ⚪; ret5=+291.8; leftover $1218.60 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 177 | $6.87 | $2.52 | — | $7,311.42 | ▼ $9,742.51 (-257.49) | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+62.6; leftover $1218.60 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 89 | $13.64 | $2.26 | — | $6,095.20 | ▼ $9,740.25 (-259.75) | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+57.7; leftover $1218.60 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 29 | $41.23 | $2.08 | — | $4,897.45 | ▼ $9,738.17 (-261.83) | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+46.0; leftover $1218.60 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 37 | $32.55 | $2.10 | — | $3,691.00 | ▼ $9,736.07 (-263.93) | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1218.60 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `KOPN` | 224 | $5.43 | $2.89 | — | $2,471.79 | ▼ $9,733.18 (-266.82) | rank by ret_5; rank ret_5; list yday_gainer; ⚪; ret5=+28.8; leftover $1218.60 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 634 | $1.92 | $8.18 | — | $1,246.33 | ▼ $9,725.00 (-275.00) | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1218.60 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `SMJF` | 120 | $10.10 | $2.35 | — | $31.98 | ▼ $9,722.65 (-277.35) | rank by ret_5; rank ret_5; list mover_buy; ret5=+22.8; leftover $1218.60 | join🔴 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 290 | $3.94 | $3.80 | $-80.04 | $1,170.79 | ▼ $9,402.73 (-597.27) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 89 | $13.31 | $2.28 | $-33.91 | $2,353.09 | ▼ $9,400.44 (-599.56) | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 29 | $41.50 | $2.10 | $+3.66 | $3,554.50 | ▼ $9,398.35 (-601.65) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 37 | $28.59 | $2.12 | $-150.74 | $4,610.21 | ▼ $9,396.23 (-603.77) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `KOPN` | 224 | $5.03 | $2.94 | $-95.43 | $5,733.99 | ▼ $9,393.29 (-606.71) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 634 | $1.70 | $8.29 | $-155.95 | $6,803.50 | ▼ $9,385.00 (-615.00) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `SMJF` | 120 | $10.45 | $2.38 | $+37.27 | $8,055.12 | ▼ $9,382.62 (-617.38) | dropped from list after 1 sess (min 1) | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 177 | $7.19 | $2.56 | $+51.56 | $9,325.18 | ▼ $9,325.18 (-674.82) | dropped from list after 2 sess (min 1) | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 7 | $150.14 | $2.01 | — | $8,272.19 | ▼ $9,323.17 (-676.83) | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1165.65 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1013 | $1.15 | $13.07 | — | $7,094.18 | ▼ $9,310.11 (-689.89) | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1165.65 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 850 | $1.37 | $10.96 | — | $5,918.71 | ▼ $9,299.14 (-700.86) | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $1165.65 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 176 | $6.61 | $2.52 | — | $4,753.71 | ▼ $9,296.62 (-703.38) | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1165.65 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BNTX` | 10 | $109.06 | $2.02 | — | $3,661.09 | ▼ $9,294.60 (-705.40) | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+22.0; leftover $1165.65 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 471 | $2.47 | $6.08 | — | $2,491.65 | ▼ $9,288.53 (-711.47) | rank by ret_5; rank ret_5; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1165.65 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 72 | $16.00 | $2.21 | — | $1,337.44 | ▼ $9,286.32 (-713.68) | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1165.65 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BRR` | 560 | $2.08 | $7.22 | — | $165.42 | ▼ $9,279.10 (-720.90) | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+18.0; leftover $1165.65 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 850 | $1.46 | $11.12 | $+54.42 | $1,395.30 | ▼ $9,691.52 (-308.48) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 176 | $6.95 | $2.56 | $+55.64 | $2,615.94 | ▼ $9,688.96 (-311.04) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BNTX` | 10 | $110.92 | $2.04 | $+14.54 | $3,723.10 | ▼ $9,686.92 (-313.08) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AUTL` | 471 | $2.47 | $6.16 | $-12.24 | $4,880.31 | ▼ $9,680.76 (-319.24) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 72 | $17.66 | $2.23 | $+115.09 | $6,149.60 | ▼ $9,678.53 (-321.47) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BRR` | 560 | $2.25 | $7.33 | $+80.65 | $7,402.27 | ▼ $9,671.20 (-328.80) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 181 | $6.81 | $2.53 | — | $6,167.13 | ▼ $9,668.67 (-331.33) | rank by ret_5; rank ret_5; list yday_mover; 🔵; ret5=+62.5; leftover $1233.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 110 | $11.13 | $2.32 | — | $4,940.51 | ▼ $9,666.35 (-333.65) | rank by ret_5; rank ret_5; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1233.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 135 | $9.08 | $2.40 | — | $3,712.32 | ▼ $9,663.96 (-336.04) | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $1233.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `INO` | 1003 | $1.23 | $12.94 | — | $2,465.69 | ▼ $9,651.02 (-348.98) | rank by ret_5; rank ret_5; list mover_buy; 🔵; ⚪; ret5=+34.4; leftover $1233.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAN` | 4196 | $0.29 | $24.92 | — | $1,207.14 | ▼ $9,626.09 (-373.91) | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $1233.71 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `INDP` | 860 | $1.39 | $11.09 | — | $0.65 | ▼ $9,615.00 (-385.00) | rank by ret_5; rank ret_5; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+30.2; leftover $1233.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 7 | $142.70 | $2.03 | $-56.12 | $997.51 | ▲ $10,710.94 (+710.94) | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1013 | $1.83 | $13.25 | $+662.52 | $2,838.05 | ▲ $10,697.69 (+697.69) | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 181 | $8.01 | $2.57 | $+212.09 | $4,285.29 | ▲ $10,695.12 (+695.12) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 110 | $13.26 | $2.35 | $+229.63 | $5,741.54 | ▲ $10,692.77 (+692.77) | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `IOVA` | 135 | $8.05 | $2.43 | $-143.87 | $6,825.86 | ▲ $10,690.34 (+690.34) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `INO` | 1003 | $1.20 | $13.12 | $-56.14 | $8,016.35 | ▲ $10,677.23 (+677.23) | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟡 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CAN` | 4196 | $0.38 | $29.24 | $+306.69 | $9,581.58 | ▲ $10,647.98 (+647.98) | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `INDP` | 860 | $1.24 | $11.25 | $-151.34 | $10,636.74 | ▲ $10,636.74 (+636.74) | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 782 | $1.70 | $10.09 | — | $9,297.25 | ▲ $10,626.65 (+626.65) | rank by ret_5; rank ret_5; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+120.5; leftover $1329.59 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 63 | $20.90 | $2.18 | — | $7,978.37 | ▲ $10,624.47 (+624.47) | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+47.9; leftover $1329.59 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DFDV` | 309 | $4.29 | $3.99 | — | $6,648.78 | ▲ $10,620.49 (+620.49) | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+28.3; leftover $1329.59 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMNR` | 53 | $24.73 | $2.15 | — | $5,335.94 | ▲ $10,618.34 (+618.34) | rank by ret_5; rank ret_5; list yday_gainer; ret5=+26.3; leftover $1329.59 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 11 | $119.46 | $2.02 | — | $4,019.85 | ▲ $10,616.31 (+616.31) | rank by ret_5; rank ret_5; list mover_buy; 🔵; ⚪; ret5=+25.9; leftover $1329.59 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 142 | $9.36 | $2.42 | — | $2,688.32 | ▲ $10,613.90 (+613.90) | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+21.3; leftover $1329.59 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HMY` | 58 | $22.65 | $2.16 | — | $1,372.45 | ▲ $10,611.73 (+611.73) | rank by ret_5; rank ret_5; list mover_buy; ⚪; ret5=+21.1; leftover $1329.59 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FWDI` | 221 | $5.99 | $2.85 | — | $45.81 | ▲ $10,608.88 (+608.88) | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+20.7; leftover $1329.59 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 782 | $1.60 | $10.23 | $-98.52 | $1,286.78 | ▲ $10,586.74 (+586.74) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ASST` | 63 | $20.72 | $2.20 | $-15.72 | $2,589.94 | ▲ $10,584.54 (+584.54) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DFDV` | 309 | $4.35 | $4.05 | $+10.51 | $3,930.05 | ▲ $10,580.50 (+580.50) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BMNR` | 53 | $24.24 | $2.17 | $-30.29 | $5,212.60 | ▲ $10,578.33 (+578.33) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AU` | 11 | $119.80 | $2.04 | $-0.33 | $6,528.35 | ▲ $10,576.28 (+576.28) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RUM` | 142 | $10.07 | $2.45 | $+95.95 | $7,955.84 | ▲ $10,573.83 (+573.83) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HMY` | 58 | $22.39 | $2.18 | $-19.43 | $9,252.28 | ▲ $10,571.65 (+571.65) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FWDI` | 221 | $5.97 | $2.90 | $-10.17 | $10,568.75 | ▲ $10,568.75 (+568.75) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 53 | $24.84 | $2.15 | — | $9,250.08 | ▲ $10,566.60 (+566.60) | rank by ret_5; rank ret_5; list flatten; ret5=+13.0; leftover $1321.09 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 84 | $15.60 | $2.24 | — | $7,937.44 | ▲ $10,564.36 (+564.36) | rank by ret_5; rank ret_5; list mover_buy; 🔵; ret5=+7.1; leftover $1321.09 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 5 | $240.00 | $2.00 | — | $6,735.43 | ▲ $10,562.35 (+562.35) | rank by ret_5; rank ret_5; list mover_buy; 🔵; ret5=+6.8; leftover $1321.09 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 510 | $2.59 | $6.58 | — | $5,407.96 | ▲ $10,555.78 (+555.78) | rank by ret_5; rank ret_5; list flatten; ret5=+4.2; leftover $1321.09 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PLTR` | 7 | $170.60 | $2.01 | — | $4,211.74 | ▲ $10,553.76 (+553.76) | rank by ret_5; rank ret_5; list mover_buy; 🔵; ret5=+3.4; leftover $1321.09 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `TX` | 23 | $55.20 | $2.06 | — | $2,940.09 | ▲ $10,551.71 (+551.71) | rank by ret_5; rank ret_5; list mover_buy; 🔵; ret5=+3.0; leftover $1321.09 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 32 | $40.72 | $2.09 | — | $1,634.96 | ▲ $10,549.62 (+549.62) | rank by ret_5; rank ret_5; list flatten; ret5=+1.8; leftover $1321.09 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 45 | $28.89 | $2.12 | — | $332.78 | ▲ $10,547.49 (+547.49) | rank by ret_5; rank ret_5; list mover_buy; 🔵; ret5=+1.6; leftover $1321.09 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 53 | $24.00 | $2.17 | $-48.84 | $1,602.62 | ▲ $10,673.97 (+673.97) | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `DLO` | 84 | $15.33 | $2.27 | $-27.19 | $2,888.07 | ▲ $10,671.70 (+671.70) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MRVL` | 5 | $253.44 | $2.03 | $+63.17 | $4,153.24 | ▲ $10,669.67 (+669.67) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 510 | $2.60 | $6.67 | $-8.15 | $5,472.57 | ▲ $10,663.00 (+663.00) | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `PLTR` | 7 | $178.75 | $2.03 | $+53.01 | $6,721.79 | ▲ $10,660.97 (+660.97) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TX` | 23 | $55.25 | $2.08 | $-2.99 | $7,990.46 | ▲ $10,658.89 (+658.89) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `RRC` | 32 | $41.44 | $2.11 | $+18.85 | $9,314.43 | ▲ $10,656.78 (+656.78) | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 45 | $29.83 | $2.15 | $+38.03 | $10,654.64 | ▲ $10,654.64 (+654.64) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `FIGR` | 35 | $37.42 | $2.10 | — | $9,342.84 | ▲ $10,652.54 (+652.54) | rank by ret_5; rank ret_5; list yday_mover; ret5=+24.4; leftover $1331.83 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `WPM` | 8 | $155.89 | $2.01 | — | $8,093.71 | ▲ $10,650.53 (+650.53) | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+17.6; leftover $1331.83 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SCCO` | 6 | $214.82 | $2.01 | — | $6,802.78 | ▲ $10,648.52 (+648.52) | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+17.0; leftover $1331.83 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AMTX` | 712 | $1.87 | $9.18 | — | $5,462.16 | ▲ $10,639.34 (+639.34) | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+16.9; leftover $1331.83 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SBSW` | 110 | $12.01 | $2.32 | — | $4,138.74 | ▲ $10,637.02 (+637.02) | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+16.8; leftover $1331.83 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `EQX` | 98 | $13.57 | $2.28 | — | $2,806.59 | ▲ $10,634.73 (+634.73) | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+16.6; leftover $1331.83 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERO` | 33 | $39.20 | $2.09 | — | $1,510.90 | ▲ $10,632.64 (+632.64) | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+16.6; leftover $1331.83 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `TXG` | 20 | $64.10 | $2.05 | — | $226.85 | ▲ $10,630.59 (+630.59) | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+16.1; leftover $1331.83 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `FIGR` | 35 | $35.50 | $2.12 | $-71.41 | $1,467.24 | ▲ $10,370.24 (+370.24) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `WPM` | 8 | $152.49 | $2.03 | $-31.25 | $2,685.12 | ▲ $10,368.20 (+368.20) | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `SCCO` | 6 | $207.95 | $2.03 | $-45.26 | $3,930.80 | ▲ $10,366.18 (+366.18) | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `AMTX` | 712 | $1.90 | $9.31 | $+2.86 | $5,274.28 | ▲ $10,356.86 (+356.86) | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `SBSW` | 110 | $12.14 | $2.35 | $+9.63 | $6,607.33 | ▲ $10,354.51 (+354.51) | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟡 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `EQX` | 98 | $12.81 | $2.31 | $-79.07 | $7,860.40 | ▲ $10,352.20 (+352.20) | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `ERO` | 33 | $38.60 | $2.11 | $-24.00 | $9,132.09 | ▲ $10,350.09 (+350.09) | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-31 09:30 ET | **SELL** | `TXG` | 20 | $60.90 | $2.07 | $-68.12 | $10,348.02 | ▲ $10,348.02 (+348.02) | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $151.40 | $2.01 | — | $9,134.81 | ▲ $10,346.01 (+346.01) | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1293.50 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 78 | $16.46 | $2.22 | — | $7,848.70 | ▲ $10,343.78 (+343.78) | rank by ret_5; rank ret_5; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1293.50 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CAN` | 4311 | $0.30 | $25.87 | — | $6,529.54 | ▲ $10,317.92 (+317.92) | rank by ret_5; rank ret_5; list yday_mover; 🔵; ret5=+54.3; leftover $1293.50 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ALEC` | 538 | $2.40 | $6.94 | — | $5,231.40 | ▲ $10,310.98 (+310.98) | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+20.4; leftover $1293.50 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DEFT` | 1930 | $0.67 | $18.72 | — | $3,919.58 | ▲ $10,292.26 (+292.26) | rank by ret_5; rank ret_5; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1293.50 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `WPM` | 8 | $148.89 | $2.01 | — | $2,726.44 | ▲ $10,290.24 (+290.24) | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+17.6; leftover $1293.50 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FUTU` | 10 | $119.46 | $2.02 | — | $1,529.82 | ▲ $10,288.22 (+288.22) | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+17.5; leftover $1293.50 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SCCO` | 6 | $204.50 | $2.01 | — | $300.82 | ▲ $10,286.22 (+286.22) | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+17.0; leftover $1293.50 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 8 | $145.95 | $2.03 | $-47.65 | $1,466.38 | ▲ $10,626.68 (+626.68) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 78 | $16.77 | $2.25 | $+19.71 | $2,772.19 | ▲ $10,624.43 (+624.43) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CAN` | 4311 | $0.34 | $28.32 | $+118.26 | $4,209.62 | ▲ $10,596.12 (+596.12) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 276 | $5.08 | $3.56 | — | $2,803.98 | ▲ $10,592.56 (+592.56) | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1403.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 594 | $2.36 | $7.66 | — | $1,394.47 | ▲ $10,584.89 (+584.89) | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+28.0; leftover $1403.21 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `HQ` | 81 | $17.06 | $2.23 | — | $10.38 | ▲ $10,582.66 (+582.66) | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+17.3; leftover $1403.21 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🔴 vol🔴 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1280.32 < 1 share @ 1646.93 |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CRDL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ALM` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NMAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ANGX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `KGC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SBSW` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ASST` | no_price | no 09:30 open — carry |
| 2026-08-26 | `DFDV` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMNR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AU` | no_price | no 09:30 open — carry |
| 2026-08-26 | `RUM` | no_price | no 09:30 open — carry |
| 2026-08-26 | `HMY` | no_price | no 09:30 open — carry |
| 2026-08-26 | `FWDI` | no_price | no 09:30 open — carry |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-26 | `BRR` | no_price | no 09:30 open |
| 2026-08-26 | `SENS` | no_price | no 09:30 open |
| 2026-08-26 | `INDP` | no_price | no 09:30 open |
| 2026-08-26 | `SUJA` | no_price | no 09:30 open |
| 2026-08-26 | `DEFT` | no_price | no 09:30 open |
| 2026-08-26 | `WPM` | no_price | no 09:30 open |
| 2026-08-26 | `FUTU` | no_price | no 09:30 open |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CAN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DFDV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MSTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FWDI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ALEC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SUJA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SBSW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TXG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `GUTS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PRQR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SUJA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `WPM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SSRM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SCCO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SBSW` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EQX` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ALEC` | 538 | 2026-09-03 @ $2.40 | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+20.4; leftover $1293.50 |
| `DEFT` | 1930 | 2026-09-03 @ $0.67 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1293.50 |
| `WPM` | 8 | 2026-09-03 @ $148.89 | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+17.6; leftover $1293.50 |
| `FUTU` | 10 | 2026-09-03 @ $119.46 | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+17.5; leftover $1293.50 |
| `SCCO` | 6 | 2026-09-03 @ $204.50 | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+17.0; leftover $1293.50 |
| `OABI` | 276 | 2026-09-04 @ $5.08 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1403.21 |
| `BRR` | 594 | 2026-09-04 @ $2.36 | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+28.0; leftover $1403.21 |
| `HQ` | 81 | 2026-09-04 @ $17.06 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+17.3; leftover $1403.21 |
