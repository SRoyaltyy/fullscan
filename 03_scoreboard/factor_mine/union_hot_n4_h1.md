# Factor mine action — `union_hot_n4_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `hot_score` · size `leftover` · sell `list` · S-boost `none` · top 4 by hot

Cash book **+9.79%** ($10,979) · signal-only (no cash/fees) was +2.44%. Starts YES **10/17**. Fills 70 · skips 29 · realized $+1197.97.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `hot_score` · **top_n** 4.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $0.85.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | IREN, TNDM, TPG, INO | — | $0.54 | $10,344.83 | $10,345.37 | IREN×54, TNDM×107, TPG×49, INO×3085 | BUY IREN x54 @ 45.98; BUY TNDM x107 @ 23.33; BUY TPG x49 @ 50.62; BUY INO x3085 @ 0.81 |
| 2026-08-14 | +5.50 | $0.54 | IREN×54, TNDM×107, TPG×49, INO×3085 | QMCO, ARX, ZENA, AIRO | IREN, TNDM, TPG, INO | $9.09 | $10,057.70 | $10,066.79 | QMCO×105, ARX×132, ZENA×1178, AIRO×231 | SELL IREN (dropped from list after 1 sess (min 1)); SELL TNDM (dropped from list after 1 sess (min 1)); SELL TPG (dropped from list after 1 sess (min 1)); SELL INO (dropped from list after 1 sess (min 1)); BUY QMCO x105 @ 24.68; BUY ARX x132 @ 19.57; BUY ZENA x1178 @ 2.20; BUY AIRO x231 @ 11.12 |
| 2026-08-17 | +2.25 | $9.09 | QMCO×105, ARX×132, ZENA×1178, AIRO×231 | XHG, CAPR, STDN, HTFL | QMCO, ARX, ZENA, AIRO | $19.42 | $9,832.53 | $9,851.95 | XHG×587, CAPR×358, STDN×180, HTFL×59 | SELL QMCO (dropped from list after 1 sess (min 1)); SELL ARX (dropped from list after 1 sess (min 1)); SELL ZENA (dropped from list after 1 sess (min 1)); SELL AIRO (dropped from list after 1 sess (min 1)); BUY XHG x587 @ 4.19; BUY CAPR x358 @ 6.87; BUY STDN x180 @ 13.64; BUY HTFL x59 @ 41.23 |
| 2026-08-18 | -6.20 | $19.42 | XHG×587, CAPR×358, STDN×180, HTFL×59 | — | XHG, STDN, HTFL | $7,164.03 | $2,534.64 | $9,698.67 | CAPR×358 | SELL XHG (dropped from list after 1 sess (min 1)); SELL STDN (dropped from list after 1 sess (min 1)); SELL HTFL (dropped from list after 1 sess (min 1)); hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $7,164.03 | CAPR×358 | — | CAPR | $9,733.36 | $0.00 | $9,733.36 | — | SELL CAPR (dropped from list after 2 sess (min 1)); hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $9,733.36 | — | MRNA, CYPH, ABCL, AZI | — | $1.24 | $9,566.30 | $9,567.54 | MRNA×16, CYPH×2115, ABCL×205, AZI×1767 | BUY MRNA x16 @ 150.14; BUY CYPH x2115 @ 1.15; BUY ABCL x205 @ 11.81; BUY AZI x1767 @ 1.37 |
| 2026-08-21 | +3.25 | $1.24 | MRNA×16, CYPH×2115, ABCL×205, AZI×1767 | XHG, CAPR | ABCL, AZI | $3.27 | $10,006.46 | $10,009.73 | MRNA×16, CYPH×2115, XHG×548, CAPR×360 | SELL ABCL (dropped from list after 1 sess (min 1)); SELL AZI (dropped from list after 1 sess (min 1)); BUY XHG x548 @ 4.49; BUY CAPR x360 @ 6.81 |
| 2026-08-24 | -5.17 | $3.27 | MRNA×16, CYPH×2115, XHG×548, CAPR×360 | — | MRNA, CYPH, XHG, CAPR | $11,322.40 | $0.00 | $11,322.40 | — | SELL MRNA (dropped from list after 2 sess (min 1)); SELL CYPH (dropped from list after 2 sess (min 1)); SELL XHG (dropped from list after 1 sess (min 1)); SELL CAPR (dropped from list after 1 sess (min 1)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $11,322.40 | — | CYPH, XHG, ASST, AU | — | $57.73 | $11,035.45 | $11,093.18 | CYPH×1665, XHG×704, ASST×135, AU×23 | BUY CYPH x1665 @ 1.70; BUY XHG x704 @ 4.02; BUY ASST x135 @ 20.90; BUY AU x23 @ 119.46 |
| 2026-08-26 | +2.02 | $57.73 | CYPH×1665, XHG×704, ASST×135, AU×23 | — | — | $57.73 | $11,229.66 | $11,287.39 | CYPH×1665, XHG×704, ASST×135, AU×23 | hold CYPH,XHG,ASST,AU |
| 2026-08-27 | — | $57.73 | CYPH×1665, XHG×704, ASST×135, AU×23 | MOS, DLO, SLI, MRVL | CYPH, XHG, ASST, AU | $93.17 | $10,768.59 | $10,861.76 | MOS×109, DLO×175, SLI×1054, MRVL×11 | SELL CYPH (dropped from list after 2 sess (min 1)); SELL XHG (dropped from list after 2 sess (min 1)); SELL ASST (dropped from list after 2 sess (min 1)); SELL AU (dropped from list after 2 sess (min 1)); BUY MOS x109 @ 24.84; BUY DLO x175 @ 15.60; BUY SLI x1054 @ 2.59; BUY MRVL x11 @ 240.00 |
| 2026-08-28 | +0.75 | $93.17 | MOS×109, DLO×175, SLI×1054, MRVL×11 | FIGR, NIQ, ERO, TRLV | MOS, DLO, SLI, MRVL | $46.07 | $10,886.34 | $10,932.41 | FIGR×72, NIQ×145, ERO×69, TRLV×239 | SELL MOS (dropped from list after 1 sess (min 1)); SELL DLO (dropped from list after 1 sess (min 1)); SELL SLI (dropped from list after 1 sess (min 1)); SELL MRVL (dropped from list after 1 sess (min 1)); BUY FIGR x72 @ 37.42; BUY NIQ x145 @ 18.79; BUY ERO x69 @ 39.20; BUY TRLV x239 @ 11.38 |
| 2026-08-31 | -5.85 | $46.07 | FIGR×72, NIQ×145, ERO×69, TRLV×239 | — | FIGR, NIQ, ERO, TRLV | $11,005.37 | $0.00 | $11,005.37 | — | SELL FIGR (dropped from list after 1 sess (min 1)); SELL NIQ (dropped from list after 1 sess (min 1)); SELL ERO (dropped from list after 1 sess (min 1)); SELL TRLV (dropped from list after 1 sess (min 1)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $11,005.37 | — | — | — | $11,005.37 | $0.00 | $11,005.37 | — | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $11,005.37 | — | — | — | $11,005.37 | $0.00 | $11,005.37 | — | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $11,005.37 | — | MRNA, XHG, ARCT, CAN | — | $0.83 | $10,869.89 | $10,870.72 | MRNA×18, XHG×770, ARCT×167, CAN×9043 | BUY MRNA x18 @ 151.40; BUY XHG x770 @ 3.57; BUY ARCT x167 @ 16.46; BUY CAN x9043 @ 0.30 |
| 2026-09-04 | — | $0.83 | MRNA×18, XHG×770, ARCT×167, CAN×9043 | HQ, NIQ, DEFT | MRNA, ARCT, CAN | $0.85 | $10,978.34 | $10,979.19 | XHG×770, HQ×164, NIQ×150, DEFT×4301 | SELL MRNA (dropped from list after 1 sess (min 1)); SELL ARCT (dropped from list after 1 sess (min 1)); SELL CAN (dropped from list after 1 sess (min 1)); BUY HQ x164 @ 17.06; BUY NIQ x150 @ 18.66; BUY DEFT x4301 @ 0.65 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 54 | $45.98 | $2.15 | — | $7,514.93 | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+12.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 107 | $23.33 | $2.31 | — | $5,016.31 | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+19.7; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 49 | $50.62 | $2.14 | — | $2,533.63 | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+6.2; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 3085 | $0.81 | $34.24 | — | $0.54 | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+13.2; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 54 | $44.09 | $2.18 | $-106.39 | $2,379.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 107 | $22.92 | $2.35 | $-48.53 | $4,829.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 49 | $55.29 | $2.17 | $+224.37 | $7,536.35 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 3085 | $0.93 | $38.48 | $+297.48 | $10,366.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 105 | $24.68 | $2.31 | — | $7,773.22 | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+111.3; leftover $2591.73 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 132 | $19.57 | $2.39 | — | $5,187.59 | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $2591.73 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 1178 | $2.20 | $15.20 | — | $2,580.79 | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $2591.73 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 231 | $11.12 | $2.98 | — | $9.09 | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $2591.73 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 105 | $24.83 | $2.34 | $+11.10 | $2,613.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 132 | $19.57 | $2.43 | $-4.81 | $5,194.71 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 1178 | $2.08 | $15.41 | $-166.08 | $7,635.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 231 | $9.57 | $3.04 | $-364.07 | $9,843.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 587 | $4.19 | $7.57 | — | $7,375.96 | top 4 by hot; rank hot_score; list yday_mover; ⚪; ret5=+291.8; leftover $2460.77 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 358 | $6.87 | $4.62 | — | $4,911.88 | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; leftover $2460.77 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 180 | $13.64 | $2.53 | — | $2,454.15 | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; leftover $2460.77 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 59 | $41.23 | $2.17 | — | $19.42 | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; leftover $2460.77 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 587 | $3.94 | $7.69 | $-162.01 | $2,324.51 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 180 | $13.31 | $2.58 | $-64.51 | $4,717.73 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 59 | $41.50 | $2.20 | $+11.57 | $7,164.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 358 | $7.19 | $4.70 | $+105.24 | $9,733.36 | dropped from list after 2 sess (min 1) | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 16 | $150.14 | $2.04 | — | $7,329.08 | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $2433.34 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 2115 | $1.15 | $27.28 | — | $4,869.54 | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $2433.34 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 205 | $11.81 | $2.64 | — | $2,444.82 | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $2433.34 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 1767 | $1.37 | $22.79 | — | $1.24 | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $2433.34 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 205 | $11.57 | $2.70 | $-55.57 | $2,370.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 1767 | $1.46 | $23.11 | $+113.13 | $4,927.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 548 | $4.49 | $7.07 | — | $2,459.51 | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; leftover $2463.55 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 360 | $6.81 | $4.64 | — | $3.27 | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+62.5; leftover $2463.55 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 16 | $142.70 | $2.07 | $-123.14 | $2,284.40 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 2115 | $1.83 | $27.67 | $+1383.25 | $6,127.19 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 548 | $4.24 | $7.18 | $-151.25 | $8,443.53 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 360 | $8.01 | $4.73 | $+422.63 | $11,322.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 1665 | $1.70 | $21.48 | — | $8,470.42 | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+120.5; leftover $2830.60 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 704 | $4.02 | $9.08 | — | $5,631.26 | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+16.1; leftover $2830.60 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 135 | $20.90 | $2.40 | — | $2,807.37 | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+47.9; leftover $2830.60 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 23 | $119.46 | $2.06 | — | $57.73 | top 4 by hot; rank hot_score; list mover_buy; 🔵; ⚪; ret5=+25.9; leftover $2830.60 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 1665 | $1.60 | $21.78 | $-209.75 | $2,699.95 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `XHG` | 704 | $3.81 | $9.22 | $-166.14 | $5,372.97 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ASST` | 135 | $20.72 | $2.44 | $-29.13 | $8,167.73 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AU` | 23 | $119.80 | $2.09 | $+3.67 | $10,921.04 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 109 | $24.84 | $2.32 | — | $8,211.17 | top 4 by hot; rank hot_score; list flatten; ret5=+13.0; leftover $2730.26 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 175 | $15.60 | $2.52 | — | $5,478.65 | top 4 by hot; rank hot_score; list mover_buy; 🔵; ret5=+7.1; leftover $2730.26 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 1054 | $2.59 | $13.60 | — | $2,735.19 | top 4 by hot; rank hot_score; list flatten; ret5=+4.2; leftover $2730.26 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 11 | $240.00 | $2.02 | — | $93.17 | top 4 by hot; rank hot_score; list mover_buy; 🔵; ret5=+6.8; leftover $2730.26 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 109 | $24.00 | $2.36 | $-96.23 | $2,706.81 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `DLO` | 175 | $15.33 | $2.57 | $-52.33 | $5,387.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 1054 | $2.60 | $13.79 | $-16.85 | $8,113.61 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `MRVL` | 11 | $253.44 | $2.06 | $+143.76 | $10,899.39 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FIGR` | 72 | $37.42 | $2.21 | — | $8,202.94 | top 4 by hot; rank hot_score; list yday_mover; ret5=+24.4; leftover $2724.85 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `NIQ` | 145 | $18.79 | $2.42 | — | $5,475.97 | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+7.6; leftover $2724.85 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERO` | 69 | $39.20 | $2.20 | — | $2,768.97 | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+16.6; leftover $2724.85 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `TRLV` | 239 | $11.38 | $3.08 | — | $46.07 | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+15.0; leftover $2724.85 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `FIGR` | 72 | $35.50 | $2.24 | $-142.68 | $2,599.83 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NIQ` | 145 | $19.20 | $2.47 | $+54.55 | $5,381.36 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🔴 digest🟢 judge🟡 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `ERO` | 69 | $38.60 | $2.23 | $-45.83 | $8,042.53 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-31 09:30 ET | **SELL** | `TRLV` | 239 | $12.41 | $3.15 | $+239.94 | $11,005.37 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 18 | $151.40 | $2.04 | — | $8,278.13 | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $2751.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `XHG` | 770 | $3.57 | $9.93 | — | $5,519.30 | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+16.1; leftover $2751.34 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 167 | $16.46 | $2.49 | — | $2,767.99 | top 4 by hot; rank hot_score; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $2751.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CAN` | 9043 | $0.30 | $54.26 | — | $0.83 | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+54.3; leftover $2751.34 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 18 | $145.95 | $2.08 | $-102.22 | $2,625.85 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 167 | $16.77 | $2.54 | $+46.74 | $5,423.90 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CAN` | 9043 | $0.34 | $59.40 | $+248.06 | $8,439.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `HQ` | 164 | $17.06 | $2.48 | — | $5,638.80 | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+17.3; leftover $2813.04 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NIQ` | 150 | $18.66 | $2.44 | — | $2,837.36 | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+7.6; leftover $2813.04 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DEFT` | 4301 | $0.65 | $40.86 | — | $0.85 | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+17.6; leftover $2813.04 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ASST` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AU` | no_price | no 09:30 open — carry |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-26 | `SENS` | no_price | no 09:30 open |
| 2026-08-26 | `NIQ` | no_price | no 09:30 open |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `XHG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CAN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OMER` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TXG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `XHG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `OMER` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TRLV` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `XHG` | 770 | 2026-09-03 @ $3.57 | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+16.1; leftover $2751.34 |
| `HQ` | 164 | 2026-09-04 @ $17.06 | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+17.3; leftover $2813.04 |
| `NIQ` | 150 | 2026-09-04 @ $18.66 | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+7.6; leftover $2813.04 |
| `DEFT` | 4301 | 2026-09-04 @ $0.65 | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+17.6; leftover $2813.04 |
