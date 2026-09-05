# Factor mine action — `union_cond_n4_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · top 4 by cond

Cash book **-0.18%** ($9,982) · signal-only (no cash/fees) was +5.57%. Starts YES **5/17**. Fills 46 · skips 82 · realized $-28.82.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `cond` · **top_n** 4.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $18.11.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | BTSG, HIMS, INO, IREN | — | $26.70 | $10,080.55 | $10,107.25 | BTSG×41, HIMS×84, INO×3086, IREN×54 | BUY BTSG x41 @ 59.80; BUY HIMS x84 @ 29.74; BUY INO x3086 @ 0.81; BUY IREN x54 @ 45.98 |
| 2026-08-14 | +5.50 | $26.70 | BTSG×41, HIMS×84, INO×3086, IREN×54 | BTBT | — | $20.63 | $10,643.97 | $10,664.60 | BTSG×41, HIMS×84, INO×3086, IREN×54, BTBT×4 | BUY BTBT x4 @ 1.50 |
| 2026-08-17 | +2.25 | $20.63 | BTSG×41, HIMS×84, INO×3086, IREN×54, BTBT×4 | INV, XHG | — | $11.47 | $10,866.78 | $10,878.26 | BTSG×41, HIMS×84, INO×3086, IREN×54, BTBT×4, INV×3, XHG×1 | BUY INV x3 @ 1.62; BUY XHG x1 @ 4.19 |
| 2026-08-18 | -6.20 | $11.47 | BTSG×41, HIMS×84, INO×3086, IREN×54, BTBT×4, INV×3, XHG×1 | — | BTSG, HIMS, INO, IREN | $10,634.21 | $14.05 | $10,648.26 | BTBT×4, INV×3, XHG×1 | SELL BTSG (dropped from list after 3 sess (min 3)); SELL HIMS (dropped from list after 3 sess (min 3)); SELL INO (dropped from list after 3 sess (min 3)); SELL IREN (dropped from list after 3 sess (min 3)); hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $10,634.21 | BTBT×4, INV×3, XHG×1 | — | BTBT | $10,639.80 | $8.95 | $10,648.75 | INV×3, XHG×1 | SELL BTBT (dropped from list after 3 sess (min 3)); hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $10,639.80 | INV×3, XHG×1 | AG, BHP, CDE, HDSN | INV, XHG | $42.22 | $10,718.63 | $10,760.85 | AG×129, BHP×29, CDE×128, HDSN×461 | SELL INV (dropped from list after 3 sess (min 3)); SELL XHG (dropped from list after 3 sess (min 3)); BUY AG x129 @ 20.55; BUY BHP x29 @ 91.01; BUY CDE x128 @ 20.65; BUY HDSN x461 @ 5.77 |
| 2026-08-21 | +3.25 | $42.22 | AG×129, BHP×29, CDE×128, HDSN×461 | — | — | $42.22 | $10,814.07 | $10,856.29 | AG×129, BHP×29, CDE×128, HDSN×461 | hold AG,BHP,CDE,HDSN |
| 2026-08-24 | -5.17 | $42.22 | AG×129, BHP×29, CDE×128, HDSN×461 | — | — | $42.22 | $10,647.16 | $10,689.38 | AG×129, BHP×29, CDE×128, HDSN×461 | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $42.22 | AG×129, BHP×29, CDE×128, HDSN×461 | AU, ERO, FCX, CNH | AG, BHP, CDE, HDSN | $85.91 | $10,631.66 | $10,717.57 | AU×22, ERO×70, FCX×34, CNH×228 | SELL AG (dropped from list after 3 sess (min 3)); SELL BHP (dropped from list after 3 sess (min 3)); SELL CDE (dropped from list after 3 sess (min 3)); SELL HDSN (dropped from list after 3 sess (min 3)); BUY AU x22 @ 119.46; BUY ERO x70 @ 38.00; BUY FCX x34 @ 77.90; BUY CNH x228 @ 11.72 |
| 2026-08-26 | +2.02 | $85.91 | AU×22, ERO×70, FCX×34, CNH×228 | — | — | $85.91 | $10,608.88 | $10,694.79 | AU×22, ERO×70, FCX×34, CNH×228 | hold AU,ERO,FCX,CNH |
| 2026-08-27 | — | $85.91 | AU×22, ERO×70, FCX×34, CNH×228 | GGB | — | $68.05 | $10,698.42 | $10,766.47 | AU×22, ERO×70, FCX×34, CNH×228, GGB×4 | BUY GGB x4 @ 4.42 |
| 2026-08-28 | +0.75 | $68.05 | AU×22, ERO×70, FCX×34, CNH×228, GGB×4 | KEYS, SMTC, CIEN, MPWR | AU, ERO, FCX, CNH | $468.10 | $10,067.93 | $10,536.03 | GGB×4, KEYS×8, SMTC×17, CIEN×6, MPWR×2 | SELL AU (dropped from list after 3 sess (min 3)); SELL ERO (dropped from list after 3 sess (min 3)); SELL FCX (dropped from list after 3 sess (min 3)); SELL CNH (dropped from list after 3 sess (min 3)); BUY KEYS x8 @ 323.82; BUY SMTC x17 @ 149.40; BUY CIEN x6 @ 411.53; BUY MPWR x2 @ 1319.75 |
| 2026-08-31 | -5.85 | $468.10 | GGB×4, KEYS×8, SMTC×17, CIEN×6, MPWR×2 | — | — | $468.10 | $9,642.76 | $10,110.86 | GGB×4, KEYS×8, SMTC×17, CIEN×6, MPWR×2 | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $468.10 | GGB×4, KEYS×8, SMTC×17, CIEN×6, MPWR×2 | — | GGB | $486.33 | $9,558.90 | $10,045.23 | KEYS×8, SMTC×17, CIEN×6, MPWR×2 | SELL GGB (dropped from list after 3 sess (min 3)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $486.33 | KEYS×8, SMTC×17, CIEN×6, MPWR×2 | — | KEYS, SMTC, CIEN, MPWR | $9,971.18 | $0.00 | $9,971.18 | — | SELL KEYS (dropped from list after 3 sess (min 3)); SELL SMTC (dropped from list after 3 sess (min 3)); SELL CIEN (dropped from list after 3 sess (min 3)); SELL MPWR (dropped from list after 3 sess (min 3)); hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $9,971.18 | — | ARCT, BMEA, CRDL, HRMY | — | $27.24 | $10,231.78 | $10,259.02 | ARCT×151, BMEA×1384, CRDL×1154, HRMY×59 | BUY ARCT x151 @ 16.46; BUY BMEA x1384 @ 1.80; BUY CRDL x1154 @ 2.16; BUY HRMY x59 @ 41.31 |
| 2026-09-04 | — | $27.24 | ARCT×151, BMEA×1384, CRDL×1154, HRMY×59 | CABA, ALEC | — | $18.11 | $9,963.88 | $9,981.99 | ARCT×151, BMEA×1384, CRDL×1154, HRMY×59, CABA×1, ALEC×2 | BUY CABA x1 @ 3.63; BUY ALEC x2 @ 2.70 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 41 | $59.80 | $2.11 | — | $7,546.09 | top 4 by cond; rank cond; list flatten; ⚪; ret5=-5.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 84 | $29.74 | $2.24 | — | $5,045.69 | top 4 by cond; rank cond; list flatten; ⚪; ret5=-5.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 3086 | $0.81 | $34.25 | — | $2,511.77 | top 4 by cond; rank cond; list flatten; ⚪; ret5=+13.2; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 54 | $45.98 | $2.15 | — | $26.70 | top 4 by cond; rank cond; list flatten; ⚪; ret5=+12.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 4 | $1.50 | $0.07 | — | $20.63 | top 4 by cond; rank cond; list flatten; 🔵; ⚪; ret5=+9.2; leftover $6.67 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `INV` | 3 | $1.62 | $0.06 | — | $15.71 | top 4 by cond; rank cond; list yday_mover; 🔵; ⚪; ret5=-53.0; leftover $5.16 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 1 | $4.19 | $0.04 | — | $11.47 | top 4 by cond; rank cond; list yday_mover; ⚪; ret5=+291.8; leftover $5.16 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 41 | $60.00 | $2.14 | $+3.94 | $2,469.33 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 84 | $27.85 | $2.27 | $-163.28 | $4,806.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 3086 | $1.14 | $40.35 | $+943.78 | $8,284.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 54 | $43.56 | $2.18 | $-135.01 | $10,634.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 4 | $1.42 | $0.09 | $-0.48 | $10,639.80 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `INV` | 3 | $1.55 | $0.08 | $-0.34 | $10,644.37 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `XHG` | 1 | $4.10 | $0.06 | $-0.20 | $10,648.41 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 129 | $20.55 | $2.38 | — | $7,995.08 | top 4 by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $2662.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 29 | $91.01 | $2.08 | — | $5,353.71 | top 4 by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $2662.10 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 128 | $20.65 | $2.37 | — | $2,708.14 | top 4 by cond; rank cond; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $2662.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 461 | $5.77 | $5.95 | — | $42.22 | top 4 by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $2662.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 129 | $20.73 | $2.42 | $+18.42 | $2,713.97 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 29 | $95.95 | $2.11 | $+139.07 | $5,494.41 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 128 | $20.85 | $2.42 | $+20.81 | $8,160.80 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 461 | $5.53 | $6.04 | $-122.63 | $10,704.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 22 | $119.46 | $2.06 | — | $8,073.91 | top 4 by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+25.9; leftover $2676.02 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ERO` | 70 | $38.00 | $2.20 | — | $5,411.71 | top 4 by cond; rank cond; list ohlc_hot,mover_buy; ⚪; ret5=+16.6; leftover $2676.02 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 34 | $77.90 | $2.09 | — | $2,761.02 | top 4 by cond; rank cond; list ohlc_hot,mover_buy; ⚪; ret5=+15.3; leftover $2676.02 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CNH` | 228 | $11.72 | $2.94 | — | $85.91 | top 4 by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+13.7; leftover $2676.02 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 4 | $4.42 | $0.19 | — | $68.05 | top 4 by cond; rank cond; list mover_buy; ret5=-8.6; leftover $21.48 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 22 | $117.41 | $2.09 | $-49.24 | $2,648.98 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ERO` | 70 | $39.20 | $2.23 | $+79.57 | $5,390.74 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 34 | $78.83 | $2.12 | $+27.40 | $8,068.84 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 catal🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `CNH` | 228 | $11.62 | $3.00 | $-28.74 | $10,715.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 8 | $323.82 | $2.01 | — | $8,122.63 | top 4 by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=-11.7; leftover $2678.80 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 17 | $149.40 | $2.04 | — | $5,580.79 | top 4 by cond; rank cond; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $2678.80 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 6 | $411.53 | $2.01 | — | $3,109.60 | top 4 by cond; rank cond; list mover_buy; 🔵; ret5=-7.7; leftover $2678.80 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 2 | $1319.75 | $2.00 | — | $468.10 | top 4 by cond; rank cond; list mover_buy; 🔵; ret5=-6.1; leftover $2678.80 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `GGB` | 4 | $4.61 | $0.22 | $+0.35 | $486.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 8 | $321.47 | $2.04 | $-22.86 | $3,056.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 17 | $127.63 | $2.07 | $-374.20 | $5,223.68 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CIEN` | 6 | $376.89 | $2.04 | $-211.88 | $7,482.99 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `MPWR` | 2 | $1245.11 | $2.03 | $-153.30 | $9,971.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 151 | $16.46 | $2.44 | — | $7,483.28 | top 4 by cond; rank cond; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $2492.80 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 1384 | $1.80 | $17.85 | — | $4,974.22 | top 4 by cond; rank cond; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $2492.80 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 1154 | $2.16 | $14.89 | — | $2,466.70 | top 4 by cond; rank cond; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $2492.80 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 59 | $41.31 | $2.17 | — | $27.24 | top 4 by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $2492.80 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 1 | $3.63 | $0.04 | — | $23.57 | top 4 by cond; rank cond; list flatten; 🔵; ⚪; ret5=+13.8; leftover $6.81 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 2 | $2.70 | $0.06 | — | $18.11 | top 4 by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $6.81 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `ARX` | cash | leftover split 6.67 < 1 share @ 19.57 |
| 2026-08-14 | `BETR` | cash | leftover split 6.67 < 1 share @ 14.80 |
| 2026-08-14 | `FIGR` | cash | leftover split 6.67 < 1 share @ 32.12 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ABX` | cash | leftover split 5.16 < 1 share @ 9.12 |
| 2026-08-17 | `NU` | cash | leftover split 5.16 < 1 share @ 15.40 |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `INV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `XHG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AURA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `INV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `XHG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BHP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BSBR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 10.56 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 10.56 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 10.56 < 1 share @ 216.30 |
| 2026-08-21 | `ARCT` | cash | leftover split 10.56 < 1 share @ 11.13 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CNH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `AEM` | no_price | no 09:30 open |
| 2026-08-26 | `HOOD` | no_price | no 09:30 open |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ERO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CNH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ACMR` | cash | leftover split 21.48 < 1 share @ 80.97 |
| 2026-08-27 | `MT` | cash | leftover split 21.48 < 1 share @ 75.12 |
| 2026-08-27 | `MU` | cash | leftover split 21.48 < 1 share @ 925.74 |
| 2026-08-28 | `GGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CIEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACIW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CIEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `MPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ALAB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ACB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ADM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ALVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ATRC` | cash | leftover split 6.81 < 1 share @ 52.88 |
| 2026-09-04 | `MLYS` | cash | leftover split 6.81 < 1 share @ 29.15 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ARCT` | 151 | 2026-09-03 @ $16.46 | top 4 by cond; rank cond; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $2492.80 |
| `BMEA` | 1384 | 2026-09-03 @ $1.80 | top 4 by cond; rank cond; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $2492.80 |
| `CRDL` | 1154 | 2026-09-03 @ $2.16 | top 4 by cond; rank cond; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $2492.80 |
| `HRMY` | 59 | 2026-09-03 @ $41.31 | top 4 by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $2492.80 |
| `CABA` | 1 | 2026-09-04 @ $3.63 | top 4 by cond; rank cond; list flatten; 🔵; ⚪; ret5=+13.8; leftover $6.81 |
| `ALEC` | 2 | 2026-09-04 @ $2.70 | top 4 by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $6.81 |
