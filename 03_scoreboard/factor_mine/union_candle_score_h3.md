# Factor mine action — `union_candle_score_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `candle_score` · size `leftover` · sell `list` · S-boost `none` · rank by candle_score

Cash book **+11.72%** ($11,172) · signal-only (no cash/fees) was +23.12%. Starts YES **16/17**. Fills 98 · skips 151 · realized $+1049.66.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `candle_score` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $36.65.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | TNDM, TPG, HIMS, IREN, INO, VOR, BTSG, SLS | — | $107.38 | $10,161.33 | $10,268.71 | TNDM×53, TPG×24, HIMS×42, IREN×27, INO×1543, VOR×56, BTSG×20, SLS×106 | BUY TNDM x53 @ 23.33; BUY TPG x24 @ 50.62; BUY HIMS x42 @ 29.74; BUY IREN x27 @ 45.98; BUY INO x1543 @ 0.81; BUY VOR x56 @ 22.01; BUY BTSG x20 @ 59.80; BUY SLS x106 @ 11.70 |
| 2026-08-14 | +5.50 | $107.38 | TNDM×53, TPG×24, HIMS×42, IREN×27, INO×1543, VOR×56, BTSG×20, SLS×106 | SATL, NMAX | — | $85.30 | $10,431.70 | $10,517.00 | TNDM×53, TPG×24, HIMS×42, IREN×27, INO×1543, VOR×56, BTSG×20, SLS×106, SATL×2, NMAX×1 | BUY SATL x2 @ 5.98; BUY NMAX x1 @ 9.89 |
| 2026-08-17 | +2.25 | $85.30 | TNDM×53, TPG×24, HIMS×42, IREN×27, INO×1543, VOR×56, BTSG×20, SLS×106, SATL×2, NMAX×1 | NPWR, SMJF, BORR | — | $54.16 | $10,536.34 | $10,590.50 | TNDM×53, TPG×24, HIMS×42, IREN×27, INO×1543, VOR×56, BTSG×20, SLS×106, SATL×2, NMAX×1, NPWR×6, SMJF×1, BORR×2 | BUY NPWR x6 @ 1.92; BUY SMJF x1 @ 10.10; BUY BORR x2 @ 4.59 |
| 2026-08-18 | -6.20 | $54.16 | TNDM×53, TPG×24, HIMS×42, IREN×27, INO×1543, VOR×56, BTSG×20, SLS×106, SATL×2, NMAX×1, NPWR×6, SMJF×1, BORR×2 | — | TNDM, TPG, HIMS, IREN, INO, VOR, BTSG, SLS | $10,360.61 | $52.55 | $10,413.16 | SATL×2, NMAX×1, NPWR×6, SMJF×1, BORR×2 | SELL TNDM (dropped from list after 3 sess (min 3)); SELL TPG (dropped from list after 3 sess (min 3)); SELL HIMS (dropped from list after 3 sess (min 3)); SELL IREN (dropped from list after 3 sess (min 3)); SELL INO (dropped from list after 3 sess (min 3)); SELL VOR (dropped from list after 3 sess (min 3)); SELL BTSG (dropped from list after 3 sess (min 3)); SELL SLS (dropped from list after 3 sess (min 3)); hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $10,360.61 | SATL×2, NMAX×1, NPWR×6, SMJF×1, BORR×2 | — | SATL | $10,372.10 | $40.45 | $10,412.55 | NMAX×1, NPWR×6, SMJF×1, BORR×2 | SELL SATL (dropped from list after 3 sess (min 3)); hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $10,372.10 | NMAX×1, NPWR×6, SMJF×1, BORR×2 | IOND, NBP, IMMX, ABCL, MRNA, ABUS, CYPH, GENB | NMAX, NPWR, SMJF, BORR | $139.14 | $10,058.89 | $10,198.03 | IOND×19, NBP×660, IMMX×100, ABCL×110, MRNA×8, ABUS×264, CYPH×1131, GENB×77 | SELL NMAX (dropped from list after 4 sess (min 3)); SELL NPWR (dropped from list after 3 sess (min 3)); SELL SMJF (dropped from list after 3 sess (min 3)); SELL BORR (dropped from list after 3 sess (min 3)); BUY IOND x19 @ 65.60; BUY NBP x660 @ 1.97; BUY IMMX x100 @ 12.98; BUY ABCL x110 @ 11.81; BUY MRNA x8 @ 150.14; BUY ABUS x264 @ 4.92; BUY CYPH x1131 @ 1.15; BUY GENB x77 @ 16.76 |
| 2026-08-21 | +3.25 | $139.14 | IOND×19, NBP×660, IMMX×100, ABCL×110, MRNA×8, ABUS×264, CYPH×1131, GENB×77 | IOVA, ARCT | — | $109.55 | $10,650.84 | $10,760.39 | IOND×19, NBP×660, IMMX×100, ABCL×110, MRNA×8, ABUS×264, CYPH×1131, GENB×77, IOVA×2, ARCT×1 | BUY IOVA x2 @ 9.08; BUY ARCT x1 @ 11.13 |
| 2026-08-24 | -5.17 | $109.55 | IOND×19, NBP×660, IMMX×100, ABCL×110, MRNA×8, ABUS×264, CYPH×1131, GENB×77, IOVA×2, ARCT×1 | — | — | $109.55 | $10,721.01 | $10,830.56 | IOND×19, NBP×660, IMMX×100, ABCL×110, MRNA×8, ABUS×264, CYPH×1131, GENB×77, IOVA×2, ARCT×1 | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $109.55 | IOND×19, NBP×660, IMMX×100, ABCL×110, MRNA×8, ABUS×264, CYPH×1131, GENB×77, IOVA×2, ARCT×1 | OMER, SG, AVAH, RUM, AU, TRLV, BMNR | IOND, NBP, IMMX, ABCL, MRNA, ABUS, GENB | $121.65 | $10,769.76 | $10,891.41 | CYPH×1131, IOVA×2, ARCT×1, OMER×68, SG×184, AVAH×94, RUM×137, AU×10, TRLV×117, BMNR×52 | SELL IOND (dropped from list after 3 sess (min 3)); SELL NBP (dropped from list after 3 sess (min 3)); SELL IMMX (dropped from list after 3 sess (min 3)); SELL ABCL (dropped from list after 3 sess (min 3)); SELL MRNA (dropped from list after 3 sess (min 3)); SELL ABUS (dropped from list after 3 sess (min 3)); SELL GENB (dropped from list after 3 sess (min 3)); BUY OMER x68 @ 18.75; BUY SG x184 @ 7.00; BUY AVAH x94 @ 13.70; BUY RUM x137 @ 9.36; BUY AU x10 @ 119.46; BUY TRLV x117 @ 11.02; BUY BMNR x52 @ 24.73 |
| 2026-08-26 | +2.02 | $121.65 | CYPH×1131, IOVA×2, ARCT×1, OMER×68, SG×184, AVAH×94, RUM×137, AU×10, TRLV×117, BMNR×52 | — | — | $121.65 | $10,856.06 | $10,977.71 | CYPH×1131, IOVA×2, ARCT×1, OMER×68, SG×184, AVAH×94, RUM×137, AU×10, TRLV×117, BMNR×52 | hold CYPH,IOVA,ARCT,OMER,SG,AVAH,RUM,AU,TRLV,BMNR |
| 2026-08-27 | — | $121.65 | CYPH×1131, IOVA×2, ARCT×1, OMER×68, SG×184, AVAH×94, RUM×137, AU×10, TRLV×117, BMNR×52 | RRC, GEN, DLO, MOS, PLTR, SLI, PGY, MT | CYPH, IOVA, ARCT | $158.67 | $10,664.08 | $10,822.75 | OMER×68, SG×184, AVAH×94, RUM×137, AU×10, TRLV×117, BMNR×52, RRC×5, GEN×8, DLO×15, MOS×9, PLTR×1, SLI×94, PGY×11, MT×3 | SELL CYPH (dropped from list after 5 sess (min 3)); SELL IOVA (dropped from list after 4 sess (min 3)); SELL ARCT (dropped from list after 4 sess (min 3)); BUY RRC x5 @ 40.72; BUY GEN x8 @ 28.89; BUY DLO x15 @ 15.60; BUY MOS x9 @ 24.84; BUY PLTR x1 @ 170.60; BUY SLI x94 @ 2.59; BUY PGY x11 @ 21.97; BUY MT x3 @ 75.12 |
| 2026-08-28 | +0.75 | $158.67 | OMER×68, SG×184, AVAH×94, RUM×137, AU×10, TRLV×117, BMNR×52, RRC×5, GEN×8, DLO×15, MOS×9, PLTR×1, SLI×94, PGY×11, MT×3 | ZYME, CLYM, NVAX, VIRT, AMTX, ESTC, FIGR | OMER, SG, AVAH, RUM, AU, BMNR | $137.36 | $10,660.87 | $10,798.23 | TRLV×117, RRC×5, GEN×8, DLO×15, MOS×9, PLTR×1, SLI×94, PGY×11, MT×3, ZYME×37, CLYM×68, NVAX×121, VIRT×16, AMTX×592, ESTC×13, FIGR×29 | SELL OMER (dropped from list after 3 sess (min 3)); SELL SG (dropped from list after 3 sess (min 3)); SELL AVAH (dropped from list after 3 sess (min 3)); SELL RUM (dropped from list after 3 sess (min 3)); SELL AU (dropped from list after 3 sess (min 3)); SELL BMNR (dropped from list after 3 sess (min 3)); BUY ZYME x37 @ 29.33; BUY CLYM x68 @ 16.09; BUY NVAX x121 @ 9.12; BUY VIRT x16 @ 65.42; BUY AMTX x592 @ 1.87; BUY ESTC x13 @ 82.64; BUY FIGR x29 @ 37.42 |
| 2026-08-31 | -5.85 | $137.36 | TRLV×117, RRC×5, GEN×8, DLO×15, MOS×9, PLTR×1, SLI×94, PGY×11, MT×3, ZYME×37, CLYM×68, NVAX×121, VIRT×16, AMTX×592, ESTC×13, FIGR×29 | — | TRLV | $1,586.96 | $9,475.03 | $11,061.99 | RRC×5, GEN×8, DLO×15, MOS×9, PLTR×1, SLI×94, PGY×11, MT×3, ZYME×37, CLYM×68, NVAX×121, VIRT×16, AMTX×592, ESTC×13, FIGR×29 | SELL TRLV (dropped from list after 4 sess (min 3)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $1,586.96 | RRC×5, GEN×8, DLO×15, MOS×9, PLTR×1, SLI×94, PGY×11, MT×3, ZYME×37, CLYM×68, NVAX×121, VIRT×16, AMTX×592, ESTC×13, FIGR×29 | — | RRC, GEN, DLO, MOS, PLTR, SLI, PGY, MT | $3,362.13 | $7,585.27 | $10,947.40 | ZYME×37, CLYM×68, NVAX×121, VIRT×16, AMTX×592, ESTC×13, FIGR×29 | SELL RRC (dropped from list after 3 sess (min 3)); SELL GEN (dropped from list after 3 sess (min 3)); SELL DLO (dropped from list after 3 sess (min 3)); SELL MOS (dropped from list after 3 sess (min 3)); SELL PLTR (dropped from list after 3 sess (min 3)); SELL SLI (dropped from list after 3 sess (min 3)); SELL PGY (dropped from list after 3 sess (min 3)); SELL MT (dropped from list after 3 sess (min 3)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $3,362.13 | ZYME×37, CLYM×68, NVAX×121, VIRT×16, AMTX×592, ESTC×13, FIGR×29 | — | CLYM, VIRT, AMTX, ESTC, FIGR | $8,722.07 | $2,322.31 | $11,044.38 | ZYME×37, NVAX×121 | SELL CLYM (dropped from list after 3 sess (min 3)); SELL VIRT (dropped from list after 3 sess (min 3)); SELL AMTX (dropped from list after 3 sess (min 3)); SELL ESTC (dropped from list after 3 sess (min 3)); SELL FIGR (dropped from list after 3 sess (min 3)); hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $8,722.07 | ZYME×37, NVAX×121 | OMER, SG, ATRC, RVTY, ARCT, TRLV, CLYM | NVAX | $74.92 | $11,250.78 | $11,325.70 | ZYME×37, OMER×75, SG×221, ATRC×28, RVTY×11, ARCT×86, TRLV×120, CLYM×96 | SELL NVAX (dropped from list after 4 sess (min 3)); BUY OMER x75 @ 18.97; BUY SG x221 @ 6.43; BUY ATRC x28 @ 49.76; BUY RVTY x11 @ 125.94; BUY ARCT x86 @ 16.46; BUY TRLV x120 @ 11.78; BUY CLYM x96 @ 14.79 |
| 2026-09-04 | — | $74.92 | ZYME×37, OMER×75, SG×221, ATRC×28, RVTY×11, ARCT×86, TRLV×120, CLYM×96 | HQ, NVAX | — | $36.65 | $11,135.71 | $11,172.36 | ZYME×37, OMER×75, SG×221, ATRC×28, RVTY×11, ARCT×86, TRLV×120, CLYM×96, HQ×1, NVAX×2 | BUY HQ x1 @ 17.06; BUY NVAX x2 @ 10.41 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $8,761.36 | ▼ $9,997.85 (-2.15) | rank by candle_score; rank candle_score; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $7,544.34 | ▼ $9,995.79 (-4.21) | rank by candle_score; rank candle_score; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $6,293.15 | ▼ $9,993.67 (-6.33) | rank by candle_score; rank candle_score; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $5,049.62 | ▼ $9,991.60 (-8.40) | rank by candle_score; rank candle_score; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $3,782.66 | ▼ $9,974.47 (-25.53) | rank by candle_score; rank candle_score; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 56 | $22.01 | $2.16 | — | $2,547.94 | ▼ $9,972.32 (-27.68) | rank by candle_score; rank candle_score; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $1,349.89 | ▼ $9,970.27 (-29.73) | rank by candle_score; rank candle_score; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $107.38 | ▼ $9,967.96 (-32.04) | rank by candle_score; rank candle_score; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **BUY** | `SATL` | 2 | $5.98 | $0.13 | — | $95.30 | ▲ $10,312.58 (+312.58) | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+16.9; leftover $13.42 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NMAX` | 1 | $9.89 | $0.10 | — | $85.30 | ▲ $10,312.47 (+312.47) | rank by candle_score; rank candle_score; list ohlc_hot,earn_react; 🔵; ⚪; ret5=+10.9; leftover $13.42 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 6 | $1.92 | $0.13 | — | $73.65 | ▲ $10,489.88 (+489.88) | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $12.19 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `SMJF` | 1 | $10.10 | $0.10 | — | $63.44 | ▲ $10,489.77 (+489.77) | rank by candle_score; rank candle_score; list mover_buy; ret5=+22.8; leftover $12.19 | join🔴 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `BORR` | 2 | $4.59 | $0.10 | — | $54.16 | ▲ $10,489.67 (+489.67) | rank by candle_score; rank candle_score; list yday_gainer,ohlc_hot; ⚪; ret5=+14.8; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 53 | $22.16 | $2.17 | $-66.33 | $1,226.48 | ▲ $10,444.76 (+444.76) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 24 | $51.77 | $2.08 | $+23.38 | $2,466.87 | ▲ $10,442.67 (+442.67) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 42 | $27.85 | $2.14 | $-83.63 | $3,634.44 | ▲ $10,440.54 (+440.54) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 27 | $43.56 | $2.09 | $-69.50 | $4,808.47 | ▲ $10,438.45 (+438.45) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1543 | $1.14 | $20.17 | $+471.89 | $6,547.31 | ▲ $10,418.27 (+418.27) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 56 | $22.82 | $2.18 | $+41.02 | $7,823.05 | ▲ $10,416.09 (+416.09) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 20 | $60.00 | $2.07 | $-0.12 | $9,020.98 | ▲ $10,414.02 (+414.02) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 106 | $12.66 | $2.34 | $+97.12 | $10,360.61 | ▲ $10,411.69 (+411.69) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `SATL` | 2 | $5.82 | $0.14 | $-0.59 | $10,372.10 | ▲ $10,413.53 (+413.53) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `NMAX` | 1 | $10.89 | $0.13 | $+0.76 | $10,382.86 | ▲ $10,412.34 (+412.34) | dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `NPWR` | 6 | $1.64 | $0.14 | $-1.95 | $10,392.57 | ▲ $10,412.21 (+412.21) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `SMJF` | 1 | $10.72 | $0.13 | $+0.39 | $10,403.16 | ▲ $10,412.08 (+412.08) | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SELL** | `BORR` | 2 | $4.46 | $0.12 | $-0.47 | $10,411.96 | ▲ $10,411.96 (+411.96) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `IOND` | 19 | $65.60 | $2.05 | — | $9,163.51 | ▲ $10,409.91 (+409.91) | rank by candle_score; rank candle_score; list earn_react; 🔵; ⚪; ret5=+3.7; leftover $1301.50 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NBP` | 660 | $1.97 | $8.51 | — | $7,854.80 | ▲ $10,401.40 (+401.40) | rank by candle_score; rank candle_score; list earn_react; 🔵; ret5=+5.9; leftover $1301.50 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IMMX` | 100 | $12.98 | $2.29 | — | $6,554.51 | ▲ $10,399.11 (+399.11) | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1301.50 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 110 | $11.81 | $2.32 | — | $5,252.54 | ▲ $10,396.79 (+396.79) | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1301.50 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $4,049.41 | ▲ $10,394.78 (+394.78) | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1301.50 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 264 | $4.92 | $3.41 | — | $2,747.12 | ▲ $10,391.37 (+391.37) | rank by candle_score; rank candle_score; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1301.50 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1131 | $1.15 | $14.59 | — | $1,431.88 | ▲ $10,376.78 (+376.78) | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1301.50 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `GENB` | 77 | $16.76 | $2.22 | — | $139.14 | ▲ $10,374.56 (+374.56) | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+12.5; leftover $1301.50 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 2 | $9.08 | $0.19 | — | $120.79 | ▲ $10,478.34 (+478.34) | rank by candle_score; rank candle_score; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $19.88 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $109.55 | ▲ $10,478.23 (+478.23) | rank by candle_score; rank candle_score; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $19.88 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `IOND` | 19 | $68.27 | $2.07 | $+46.62 | $1,404.61 | ▲ $11,014.66 (+1,014.66) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NBP` | 660 | $1.89 | $8.63 | $-69.95 | $2,643.38 | ▲ $11,006.03 (+1,006.03) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IMMX` | 100 | $13.40 | $2.32 | $+37.39 | $3,981.06 | ▲ $11,003.71 (+1,003.71) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ABCL` | 110 | $10.77 | $2.35 | $-119.62 | $5,163.41 | ▲ $11,001.36 (+1,001.36) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MRNA` | 8 | $141.19 | $2.03 | $-75.65 | $6,290.90 | ▲ $10,999.33 (+999.33) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ABUS` | 264 | $5.26 | $3.46 | $+82.89 | $7,676.08 | ▲ $10,995.87 (+995.87) | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `GENB` | 77 | $17.75 | $2.24 | $+71.76 | $9,040.58 | ▲ $10,993.62 (+993.62) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `OMER` | 68 | $18.75 | $2.19 | — | $7,763.39 | ▲ $10,991.43 (+991.43) | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+12.1; leftover $1291.51 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SG` | 184 | $7.00 | $2.54 | — | $6,472.85 | ▲ $10,988.89 (+988.89) | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+11.3; leftover $1291.51 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AVAH` | 94 | $13.70 | $2.27 | — | $5,182.77 | ▲ $10,986.61 (+986.61) | rank by candle_score; rank candle_score; list mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1291.51 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 137 | $9.36 | $2.40 | — | $3,898.05 | ▲ $10,984.21 (+984.21) | rank by candle_score; rank candle_score; list yday_gainer; 🔵; ret5=+21.3; leftover $1291.51 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 10 | $119.46 | $2.02 | — | $2,701.43 | ▲ $10,982.19 (+982.19) | rank by candle_score; rank candle_score; list mover_buy; 🔵; ⚪; ret5=+25.9; leftover $1291.51 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `TRLV` | 117 | $11.02 | $2.34 | — | $1,409.75 | ▲ $10,979.85 (+979.85) | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+15.0; leftover $1291.51 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMNR` | 52 | $24.73 | $2.15 | — | $121.65 | ▲ $10,977.71 (+977.71) | rank by candle_score; rank candle_score; list yday_gainer; ret5=+26.3; leftover $1291.51 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 1131 | $1.60 | $14.79 | $+479.57 | $1,916.45 | ▲ $10,950.47 (+950.47) | dropped from list after 5 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `IOVA` | 2 | $8.34 | $0.19 | $-1.86 | $1,932.94 | ▲ $10,950.28 (+950.28) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 1 | $15.35 | $0.18 | $+3.93 | $1,948.11 | ▲ $10,950.10 (+950.10) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 5 | $40.72 | $2.00 | — | $1,742.51 | ▲ $10,948.10 (+948.10) | rank by candle_score; rank candle_score; list flatten; ret5=+1.8; leftover $243.51 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 8 | $28.89 | $2.01 | — | $1,509.38 | ▲ $10,946.09 (+946.09) | rank by candle_score; rank candle_score; list mover_buy; 🔵; ret5=+1.6; leftover $243.51 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 15 | $15.60 | $2.04 | — | $1,273.34 | ▲ $10,944.05 (+944.05) | rank by candle_score; rank candle_score; list mover_buy; 🔵; ret5=+7.1; leftover $243.51 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 9 | $24.84 | $2.02 | — | $1,047.76 | ▲ $10,942.03 (+942.03) | rank by candle_score; rank candle_score; list flatten; ret5=+13.0; leftover $243.51 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `PLTR` | 1 | $170.60 | $1.71 | — | $875.45 | ▲ $10,940.32 (+940.32) | rank by candle_score; rank candle_score; list mover_buy; 🔵; ret5=+3.4; leftover $243.51 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 94 | $2.59 | $2.27 | — | $629.72 | ▲ $10,938.05 (+938.05) | rank by candle_score; rank candle_score; list flatten; ret5=+4.2; leftover $243.51 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PGY` | 11 | $21.97 | $2.02 | — | $386.03 | ▲ $10,936.03 (+936.03) | rank by candle_score; rank candle_score; list mover_buy; 🔵; ret5=+0.6; leftover $243.51 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 3 | $75.12 | $2.00 | — | $158.67 | ▲ $10,934.03 (+934.03) | rank by candle_score; rank candle_score; list mover_buy; 🔵; ret5=-2.2; leftover $243.51 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `OMER` | 68 | $18.24 | $2.22 | $-39.09 | $1,396.78 | ▲ $10,887.68 (+887.68) | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `SG` | 184 | $6.87 | $2.58 | $-29.04 | $2,658.27 | ▲ $10,885.09 (+885.09) | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `AVAH` | 94 | $13.62 | $2.30 | $-12.09 | $3,936.25 | ▲ $10,882.79 (+882.79) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RUM` | 137 | $9.51 | $2.43 | $+15.71 | $5,236.69 | ▲ $10,880.36 (+880.36) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 10 | $117.41 | $2.04 | $-24.56 | $6,408.75 | ▲ $10,878.32 (+878.32) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMNR` | 52 | $25.91 | $2.17 | $+57.05 | $7,753.90 | ▲ $10,876.15 (+876.15) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 37 | $29.33 | $2.10 | — | $6,666.59 | ▲ $10,874.05 (+874.05) | rank by candle_score; rank candle_score; list yday_gainer,ohlc_hot; ret5=+14.1; leftover $1107.70 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CLYM` | 68 | $16.09 | $2.19 | — | $5,570.28 | ▲ $10,871.86 (+871.86) | rank by candle_score; rank candle_score; list yday_mover; ret5=+5.8; leftover $1107.70 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NVAX` | 121 | $9.12 | $2.35 | — | $4,464.41 | ▲ $10,869.51 (+869.51) | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+11.1; leftover $1107.70 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VIRT` | 16 | $65.42 | $2.04 | — | $3,415.65 | ▲ $10,867.47 (+867.47) | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+13.2; leftover $1107.70 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AMTX` | 592 | $1.87 | $7.64 | — | $2,300.97 | ▲ $10,859.83 (+859.83) | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+16.9; leftover $1107.70 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 13 | $82.64 | $2.03 | — | $1,224.62 | ▲ $10,857.80 (+857.80) | rank by candle_score; rank candle_score; list earn_react; ret5=-0.9; leftover $1107.70 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FIGR` | 29 | $37.42 | $2.08 | — | $137.36 | ▲ $10,855.72 (+855.72) | rank by candle_score; rank candle_score; list yday_mover; ret5=+24.4; leftover $1107.70 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🟡 buy🟢 |
| 2026-08-31 09:30 ET | **SELL** | `TRLV` | 117 | $12.41 | $2.37 | $+157.92 | $1,586.96 | ▲ $11,036.80 (+1,036.80) | dropped from list after 4 sess (min 3) | join🔴 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `RRC` | 5 | $41.32 | $2.02 | $-1.03 | $1,791.54 | ▲ $10,999.41 (+999.41) | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `GEN` | 8 | $30.56 | $2.03 | $+9.31 | $2,033.98 | ▲ $10,997.37 (+997.37) | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `DLO` | 15 | $14.88 | $2.06 | $-14.89 | $2,255.13 | ▲ $10,995.32 (+995.32) | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `MOS` | 9 | $24.00 | $2.04 | $-11.61 | $2,469.09 | ▲ $10,993.28 (+993.28) | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `PLTR` | 1 | $185.52 | $1.88 | $+11.33 | $2,652.73 | ▲ $10,991.40 (+991.40) | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 94 | $2.70 | $2.30 | $+5.77 | $2,904.24 | ▲ $10,989.11 (+989.11) | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `PGY` | 11 | $21.73 | $2.04 | $-6.71 | $3,141.22 | ▲ $10,987.06 (+987.06) | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `MT` | 3 | $74.31 | $2.02 | $-6.45 | $3,362.13 | ▲ $10,985.04 (+985.04) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CLYM` | 68 | $13.88 | $2.22 | $-154.69 | $4,303.76 | ▲ $10,934.06 (+934.06) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `VIRT` | 16 | $65.38 | $2.06 | $-4.74 | $5,347.78 | ▲ $10,932.00 (+932.00) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `AMTX` | 592 | $1.88 | $7.75 | $-9.46 | $6,453.00 | ▲ $10,924.26 (+924.26) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ESTC` | 13 | $95.76 | $2.05 | $+166.48 | $7,695.83 | ▲ $10,922.21 (+922.21) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FIGR` | 29 | $35.46 | $2.10 | $-61.01 | $8,722.07 | ▲ $10,920.11 (+920.11) | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **SELL** | `NVAX` | 121 | $10.27 | $2.38 | $+134.41 | $9,962.36 | ▲ $11,072.36 (+1,072.36) | dropped from list after 4 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OMER` | 75 | $18.97 | $2.21 | — | $8,537.39 | ▲ $11,070.14 (+1,070.14) | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+12.1; leftover $1423.19 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SG` | 221 | $6.43 | $2.85 | — | $7,113.51 | ▲ $11,067.29 (+1,067.29) | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+11.3; leftover $1423.19 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 28 | $49.76 | $2.07 | — | $5,718.16 | ▲ $11,065.22 (+1,065.22) | rank by candle_score; rank candle_score; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1423.19 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 11 | $125.94 | $2.02 | — | $4,330.79 | ▲ $11,063.19 (+1,063.19) | rank by candle_score; rank candle_score; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1423.19 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 86 | $16.46 | $2.25 | — | $2,912.99 | ▲ $11,060.95 (+1,060.95) | rank by candle_score; rank candle_score; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1423.19 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `TRLV` | 120 | $11.78 | $2.35 | — | $1,497.04 | ▲ $11,058.60 (+1,058.60) | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+15.0; leftover $1423.19 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CLYM` | 96 | $14.79 | $2.28 | — | $74.92 | ▲ $11,056.32 (+1,056.32) | rank by candle_score; rank candle_score; list yday_gainer; 🔵; ret5=+5.8; leftover $1423.19 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `HQ` | 1 | $17.06 | $0.17 | — | $57.68 | ▲ $11,297.09 (+1,297.09) | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; ret5=+17.3; leftover $24.97 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 2 | $10.41 | $0.21 | — | $36.65 | ▲ $11,296.88 (+1,296.88) | rank by candle_score; rank candle_score; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $24.97 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `ZS` | cash | leftover split 13.42 < 1 share @ 190.00 |
| 2026-08-14 | `BETA` | cash | leftover split 13.42 < 1 share @ 25.21 |
| 2026-08-14 | `BRZE` | cash | leftover split 13.42 < 1 share @ 30.00 |
| 2026-08-14 | `MH` | cash | leftover split 13.42 < 1 share @ 13.55 |
| 2026-08-14 | `GLOB` | cash | leftover split 13.42 < 1 share @ 38.21 |
| 2026-08-14 | `LUNR` | cash | leftover split 13.42 < 1 share @ 19.17 |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SATL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `JBIO` | cash | leftover split 12.19 < 1 share @ 24.60 |
| 2026-08-17 | `HTFL` | cash | leftover split 12.19 < 1 share @ 41.23 |
| 2026-08-17 | `STDN` | cash | leftover split 12.19 < 1 share @ 13.64 |
| 2026-08-17 | `CLYM` | cash | leftover split 12.19 < 1 share @ 16.25 |
| 2026-08-18 | `SATL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NMAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `SMJF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `BORR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ADCT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CERS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KYTX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OVID` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KYMR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `SMJF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `BORR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `MTDR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `PSKY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RDZN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBNX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMTX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `IOND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NBP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IMMX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ABCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ABUS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `GENB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SM` | cash | leftover split 19.88 < 1 share @ 37.81 |
| 2026-08-21 | `ARIS` | cash | leftover split 19.88 < 1 share @ 20.90 |
| 2026-08-21 | `DXYZ` | cash | leftover split 19.88 < 1 share @ 34.89 |
| 2026-08-21 | `ILMN` | cash | leftover split 19.88 < 1 share @ 212.40 |
| 2026-08-21 | `AEM` | cash | leftover split 19.88 < 1 share @ 216.30 |
| 2026-08-24 | `IOND` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NBP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IMMX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABUS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `GENB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SG` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ZYME` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `IOVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `IOVA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ARCT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `RUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMNR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-26 | `INSP` | no_price | no 09:30 open |
| 2026-08-26 | `SENS` | no_price | no 09:30 open |
| 2026-08-26 | `ZYME` | no_price | no 09:30 open |
| 2026-08-26 | `NVAX` | no_price | no 09:30 open |
| 2026-08-27 | `OMER` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `TRLV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMNR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `RRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `GEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `DLO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `MOS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `PLTR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `SLI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `PGY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `MT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `GEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `DLO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `MOS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `PLTR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `PGY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `MT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CLYM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `VIRT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ESTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FIGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `OMER` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WEN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ZYME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CLYM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `VIRT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ESTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FIGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `OMER` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CELH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RANI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `OMER` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TRLV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ACB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NOG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CLYM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VIRT` | cash | leftover split 24.97 < 1 share @ 63.37 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ZYME` | 37 | 2026-08-28 @ $29.33 | rank by candle_score; rank candle_score; list yday_gainer,ohlc_hot; ret5=+14.1; leftover $1107.70 |
| `OMER` | 75 | 2026-09-03 @ $18.97 | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+12.1; leftover $1423.19 |
| `SG` | 221 | 2026-09-03 @ $6.43 | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+11.3; leftover $1423.19 |
| `ATRC` | 28 | 2026-09-03 @ $49.76 | rank by candle_score; rank candle_score; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1423.19 |
| `RVTY` | 11 | 2026-09-03 @ $125.94 | rank by candle_score; rank candle_score; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1423.19 |
| `ARCT` | 86 | 2026-09-03 @ $16.46 | rank by candle_score; rank candle_score; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1423.19 |
| `TRLV` | 120 | 2026-09-03 @ $11.78 | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+15.0; leftover $1423.19 |
| `CLYM` | 96 | 2026-09-03 @ $14.79 | rank by candle_score; rank candle_score; list yday_gainer; 🔵; ret5=+5.8; leftover $1423.19 |
| `HQ` | 1 | 2026-09-04 @ $17.06 | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; ret5=+17.3; leftover $24.97 |
| `NVAX` | 2 | 2026-09-04 @ $10.41 | rank by candle_score; rank candle_score; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $24.97 |
