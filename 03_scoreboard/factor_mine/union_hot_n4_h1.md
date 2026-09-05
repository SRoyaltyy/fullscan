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

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | IREN, TNDM, TPG, INO | — | $0.54 | $10,345.37 | IREN×54, TNDM×107, TPG×49, INO×3085 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $0.54 | IREN×54, TNDM×107, TPG×49, INO×3085 | $10,412.10 | +66.73 | QMCO, ARX, ZENA, AIRO | IREN, TNDM, TPG, INO | $9.09 | $10,066.79 | QMCO×105, ARX×132, ZENA×1178, AIRO×231 | 09:30 open · cash $0.54 (unchanged overnight, no fees) · equity $10,412.10 vs prior close $10,345.37 (+66.73) because holdings re-marked: IREN×54 yday $44.76 → 09:30 $44.09 -36.18; TNDM×107 yday $23.13 → 09:30 $22.92 -22.47; TPG×49 yday $54.62 → 09:30 $55.29 +32.83; INO×3085 yday $0.90 → 09:30 $0.93 +92.55 |
| 2026-08-17 | +2.25 | $9.09 | QMCO×105, ARX×132, ZENA×1178, AIRO×231 | $9,866.28 | -200.51 | XHG, CAPR, STDN, HTFL | QMCO, ARX, ZENA, AIRO | $19.42 | $9,851.95 | XHG×587, CAPR×358, STDN×180, HTFL×59 | 09:30 open · cash $9.09 (unchanged overnight, no fees) · equity $9,866.28 vs prior close $10,066.79 (-200.51) because holdings re-marked: QMCO×105 yday $26.11 → 09:30 $24.83 -134.40; ARX×132 yday $19.58 → 09:30 $19.57 -1.32; ZENA×1178 yday $2.14 → 09:30 $2.08 -64.79; AIRO×231 yday $9.57 → 09:30 $9.57 +0.00 |
| 2026-08-18 | -6.20 | $19.42 | XHG×587, CAPR×358, STDN×180, HTFL×59 | $9,861.50 | +9.55 | — | XHG, STDN, HTFL | $7,164.03 | $9,698.67 | CAPR×358 | 09:30 open · cash $19.42 (unchanged overnight, no fees) · equity $9,861.50 vs prior close $9,851.95 (+9.55) because holdings re-marked: XHG×587 yday $3.91 → 09:30 $3.94 +17.61; CAPR×358 yday $7.45 → 09:30 $7.50 +17.90; STDN×180 yday $13.31 → 09:30 $13.31 +0.00; HTFL×59 yday $41.94 → 09:30 $41.50 -25.96 |
| 2026-08-19 | -7.20 | $7,164.03 | CAPR×358 | $9,738.05 | +39.38 | — | CAPR | $9,733.36 | $9,733.36 | — | 09:30 open · cash $7,164.03 (unchanged overnight, no fees) · equity $9,738.05 vs prior close $9,698.67 (+39.38) because holdings re-marked: CAPR×358 yday $7.08 → 09:30 $7.19 +39.38 |
| 2026-08-20 | +1.12 | $9,733.36 | — | $9,733.36 | -0.00 | MRNA, CYPH, ABCL, AZI | — | $1.24 | $9,567.54 | MRNA×16, CYPH×2115, ABCL×205, AZI×1767 | 09:30 open · cash $9,733.36 · no holdings · equity $9,733.36 vs prior close $9,733.36 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-21 | +3.25 | $1.24 | MRNA×16, CYPH×2115, ABCL×205, AZI×1767 | $9,874.47 | +306.93 | XHG, CAPR | ABCL, AZI | $3.27 | $10,009.73 | MRNA×16, CYPH×2115, XHG×548, CAPR×360 | 09:30 open · cash $1.24 (unchanged overnight, no fees) · equity $9,874.47 vs prior close $9,567.54 (+306.93) because holdings re-marked: MRNA×16 yday $133.32 → 09:30 $133.11 -3.36; CYPH×2115 yday $1.19 → 09:30 $1.32 +274.95; ABCL×205 yday $11.57 → 09:30 $11.57 +0.00; AZI×1767 yday $1.44 → 09:30 $1.46 +35.34 |
| 2026-08-24 | -5.17 | $3.27 | MRNA×16, CYPH×2115, XHG×548, CAPR×360 | $11,364.04 | +1,354.31 | — | MRNA, CYPH, XHG, CAPR | $11,322.40 | $11,322.40 | — | 09:30 open · cash $3.27 (unchanged overnight, no fees) · equity $11,364.04 vs prior close $10,009.73 (+1354.31) because holdings re-marked: MRNA×16 yday $145.13 → 09:30 $142.70 -38.88; CYPH×2115 yday $1.42 → 09:30 $1.83 +867.15; XHG×548 yday $4.41 → 09:30 $4.24 -93.16; CAPR×360 yday $6.29 → 09:30 $8.01 +619.20 |
| 2026-08-25 | +1.80 | $11,322.40 | — | $11,322.40 | +0.00 | CYPH, XHG, ASST, AU | — | $57.73 | $11,093.18 | CYPH×1665, XHG×704, ASST×135, AU×23 | 09:30 open · cash $11,322.40 · no holdings · equity $11,322.40 vs prior close $11,322.40 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-26 | +2.02 | $57.73 | CYPH×1665, XHG×704, ASST×135, AU×23 | $11,093.18 | -0.00 | — | — | $57.73 | $11,287.39 | CYPH×1665, XHG×704, ASST×135, AU×23 | 09:30 open · cash $57.73 (unchanged overnight, no fees) · equity $11,093.18 vs prior close $11,093.18 (-0.00) because holdings re-marked: CYPH×1665 yday $1.64 → 09:30 $1.64 +0.00; XHG×704 yday $4.05 → 09:30 $4.05 +0.00; ASST×135 yday $20.20 → 09:30 $20.20 +0.00; AU×23 yday $118.55 → 09:30 $118.55 +0.00 |
| 2026-08-27 | — | $57.73 | CYPH×1665, XHG×704, ASST×135, AU×23 | $10,956.57 | -330.82 | MOS, DLO, SLI, MRVL | CYPH, XHG, ASST, AU | $93.17 | $10,861.76 | MOS×109, DLO×175, SLI×1054, MRVL×11 | 09:30 open · cash $57.73 (unchanged overnight, no fees) · equity $10,956.57 vs prior close $11,287.39 (-330.82) because holdings re-marked: CYPH×1665 yday $1.64 → 09:30 $1.60 -66.60; XHG×704 yday $4.05 → 09:30 $3.81 -168.96; ASST×135 yday $20.20 → 09:30 $20.72 +70.20; AU×23 yday $118.55 → 09:30 $119.80 +28.75 |
| 2026-08-28 | +0.75 | $93.17 | MOS×109, DLO×175, SLI×1054, MRVL×11 | $10,920.16 | +58.40 | FIGR, NIQ, ERO, TRLV | MOS, DLO, SLI, MRVL | $46.07 | $10,932.41 | FIGR×72, NIQ×145, ERO×69, TRLV×239 | 09:30 open · cash $93.17 (unchanged overnight, no fees) · equity $10,920.16 vs prior close $10,861.76 (+58.40) because holdings re-marked: MOS×109 yday $24.16 → 09:30 $24.00 -17.44; DLO×175 yday $15.36 → 09:30 $15.33 -5.25; SLI×1054 yday $2.61 → 09:30 $2.60 -10.54; MRVL×11 yday $245.11 → 09:30 $253.44 +91.63 |
| 2026-08-31 | -5.85 | $46.07 | FIGR×72, NIQ×145, ERO×69, TRLV×239 | $11,015.46 | +83.05 | — | FIGR, NIQ, ERO, TRLV | $11,005.37 | $11,005.37 | — | 09:30 open · cash $46.07 (unchanged overnight, no fees) · equity $11,015.46 vs prior close $10,932.41 (+83.05) because holdings re-marked: FIGR×72 yday $38.02 → 09:30 $35.50 -181.44; NIQ×145 yday $19.07 → 09:30 $19.20 +18.85; ERO×69 yday $39.82 → 09:30 $38.60 -84.18; TRLV×239 yday $11.03 → 09:30 $12.41 +329.82 |
| 2026-09-01 | -6.30 | $11,005.37 | — | $11,005.37 | +0.00 | — | — | $11,005.37 | $11,005.37 | — | 09:30 open · cash $11,005.37 · no holdings · equity $11,005.37 vs prior close $11,005.37 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-02 | -3.83 | $11,005.37 | — | $11,005.37 | +0.00 | — | — | $11,005.37 | $11,005.37 | — | 09:30 open · cash $11,005.37 · no holdings · equity $11,005.37 vs prior close $11,005.37 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-03 | -0.90 | $11,005.37 | — | $11,005.37 | +0.00 | MRNA, XHG, ARCT, CAN | — | $0.83 | $10,870.72 | MRNA×18, XHG×770, ARCT×167, CAN×9043 | 09:30 open · cash $11,005.37 · no holdings · equity $11,005.37 vs prior close $11,005.37 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $0.83 | MRNA×18, XHG×770, ARCT×167, CAN×9043 | $11,105.74 | +235.02 | HQ, NIQ, DEFT | MRNA, ARCT, CAN | $0.85 | $10,979.19 | XHG×770, HQ×164, NIQ×150, DEFT×4301 | 09:30 open · cash $0.83 (unchanged overnight, no fees) · equity $11,105.74 vs prior close $10,870.72 (+235.02) because holdings re-marked: MRNA×18 yday $150.81 → 09:30 $145.95 -87.48; XHG×770 yday $3.32 → 09:30 $3.38 +46.20; ARCT×167 yday $16.74 → 09:30 $16.77 +5.01; CAN×9043 yday $0.31 → 09:30 $0.34 +271.29 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 54 | $45.98 | $2.15 | — | $7,514.93 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+12.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 107 | $23.33 | $2.31 | — | $5,016.31 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+19.7; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 49 | $50.62 | $2.14 | — | $2,533.63 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+6.2; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 3085 | $0.81 | $34.24 | — | $0.54 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+13.2; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.54 | ▲ 09:30 equity $10,412.10 vs yday $10,345.37 (+66.73) | 09:30 open · cash $0.54 (unchanged overnight, no fees) · equity $10,412.10 vs prior close $10,345.37 (+66.73) because holdings re-marked: IREN×54 yday $44.76 → 09:30 $44.09 -36.18; TNDM×107 yday $23.13 → 09:30 $22.92 -22.47; TPG×49 yday $54.62 → 09:30 $55.29 +32.83; INO×3085 yday $0.90 → 09:30 $0.93 +92.55 | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 54 | $44.09 | $2.18 | $-106.39 | $2,379.22 | ▼ -106.39 after sell → book $10,409.92; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 107 | $22.92 | $2.35 | $-48.53 | $4,829.31 | ▼ -48.53 after sell → book $10,407.57; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 49 | $55.29 | $2.17 | $+224.37 | $7,536.35 | ▲ +224.37 after sell → book $10,405.40; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 3085 | $0.93 | $38.48 | $+297.48 | $10,366.92 | ▲ +297.48 after sell → book $10,366.92; vs 09:30 mark -38.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 105 | $24.68 | $2.31 | — | $7,773.22 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+111.3; leftover $2591.73 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 132 | $19.57 | $2.39 | — | $5,187.59 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $2591.73 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 1178 | $2.20 | $15.20 | — | $2,580.79 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $2591.73 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 231 | $11.12 | $2.98 | — | $9.09 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $2591.73 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.09 | ▼ 09:30 equity $9,866.28 vs yday $10,066.79 (-200.51) | 09:30 open · cash $9.09 (unchanged overnight, no fees) · equity $9,866.28 vs prior close $10,066.79 (-200.51) because holdings re-marked: QMCO×105 yday $26.11 → 09:30 $24.83 -134.40; ARX×132 yday $19.58 → 09:30 $19.57 -1.32; ZENA×1178 yday $2.14 → 09:30 $2.08 -64.79; AIRO×231 yday $9.57 → 09:30 $9.57 +0.00 | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 105 | $24.83 | $2.34 | $+11.10 | $2,613.90 | ▲ +11.10 after sell → book $9,863.94; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 132 | $19.57 | $2.43 | $-4.81 | $5,194.71 | ▼ -4.81 after sell → book $9,861.51; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 1178 | $2.08 | $15.41 | $-166.08 | $7,635.43 | ▼ -166.08 after sell → book $9,846.10; vs 09:30 mark -15.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 231 | $9.57 | $3.04 | $-364.07 | $9,843.06 | ▼ -364.07 after sell → book $9,843.06; vs 09:30 mark -3.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 587 | $4.19 | $7.57 | — | $7,375.96 | — | top 4 by hot; rank hot_score; list yday_mover; ⚪; ret5=+291.8; leftover $2460.77 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 358 | $6.87 | $4.62 | — | $4,911.88 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; leftover $2460.77 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 180 | $13.64 | $2.53 | — | $2,454.15 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; leftover $2460.77 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 59 | $41.23 | $2.17 | — | $19.42 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; leftover $2460.77 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.42 | ▲ 09:30 equity $9,861.50 vs yday $9,851.95 (+9.55) | 09:30 open · cash $19.42 (unchanged overnight, no fees) · equity $9,861.50 vs prior close $9,851.95 (+9.55) because holdings re-marked: XHG×587 yday $3.91 → 09:30 $3.94 +17.61; CAPR×358 yday $7.45 → 09:30 $7.50 +17.90; STDN×180 yday $13.31 → 09:30 $13.31 +0.00; HTFL×59 yday $41.94 → 09:30 $41.50 -25.96 | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 587 | $3.94 | $7.69 | $-162.01 | $2,324.51 | ▼ -162.01 after sell → book $9,853.81; vs 09:30 mark -7.69 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 180 | $13.31 | $2.58 | $-64.51 | $4,717.73 | ▼ -64.51 after sell → book $9,851.23; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 59 | $41.50 | $2.20 | $+11.57 | $7,164.03 | ▲ +11.57 after sell → book $9,849.03; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,164.03 | ▲ 09:30 equity $9,738.05 vs yday $9,698.67 (+39.38) | 09:30 open · cash $7,164.03 (unchanged overnight, no fees) · equity $9,738.05 vs prior close $9,698.67 (+39.38) because holdings re-marked: CAPR×358 yday $7.08 → 09:30 $7.19 +39.38 | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 358 | $7.19 | $4.70 | $+105.24 | $9,733.36 | ▲ +105.24 after sell → book $9,733.36; vs 09:30 mark -4.69 | dropped from list after 2 sess (min 1) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,733.36 | ▲ 09:30 equity $9,733.36 vs yday $9,733.36 (-0.00) | 09:30 open · cash $9,733.36 · no holdings · equity $9,733.36 vs prior close $9,733.36 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 16 | $150.14 | $2.04 | — | $7,329.08 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $2433.34 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 2115 | $1.15 | $27.28 | — | $4,869.54 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $2433.34 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 205 | $11.81 | $2.64 | — | $2,444.82 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $2433.34 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 1767 | $1.37 | $22.79 | — | $1.24 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $2433.34 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.24 | ▲ 09:30 equity $9,874.47 vs yday $9,567.54 (+306.93) | 09:30 open · cash $1.24 (unchanged overnight, no fees) · equity $9,874.47 vs prior close $9,567.54 (+306.93) because holdings re-marked: MRNA×16 yday $133.32 → 09:30 $133.11 -3.36; CYPH×2115 yday $1.19 → 09:30 $1.32 +274.95; ABCL×205 yday $11.57 → 09:30 $11.57 +0.00; AZI×1767 yday $1.44 → 09:30 $1.46 +35.34 | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 205 | $11.57 | $2.70 | $-55.57 | $2,370.39 | ▼ -55.57 after sell → book $9,871.77; vs 09:30 mark -2.70 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 1767 | $1.46 | $23.11 | $+113.13 | $4,927.10 | ▲ +113.13 after sell → book $9,848.66; vs 09:30 mark -23.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 548 | $4.49 | $7.07 | — | $2,459.51 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; leftover $2463.55 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 360 | $6.81 | $4.64 | — | $3.27 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+62.5; leftover $2463.55 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.27 | ▲ 09:30 equity $11,364.04 vs yday $10,009.73 (+1,354.31) | 09:30 open · cash $3.27 (unchanged overnight, no fees) · equity $11,364.04 vs prior close $10,009.73 (+1354.31) because holdings re-marked: MRNA×16 yday $145.13 → 09:30 $142.70 -38.88; CYPH×2115 yday $1.42 → 09:30 $1.83 +867.15; XHG×548 yday $4.41 → 09:30 $4.24 -93.16; CAPR×360 yday $6.29 → 09:30 $8.01 +619.20 | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 16 | $142.70 | $2.07 | $-123.14 | $2,284.40 | ▼ -123.14 after sell → book $11,361.97; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 2115 | $1.83 | $27.67 | $+1383.25 | $6,127.19 | ▲ +1,383.25 after sell → book $11,334.31; vs 09:30 mark -27.66 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 548 | $4.24 | $7.18 | $-151.25 | $8,443.53 | ▼ -151.25 after sell → book $11,327.13; vs 09:30 mark -7.18 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 360 | $8.01 | $4.73 | $+422.63 | $11,322.40 | ▲ +422.63 after sell → book $11,322.40; vs 09:30 mark -4.73 | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,322.40 | ▲ 09:30 equity $11,322.40 vs yday $11,322.40 (+0.00) | 09:30 open · cash $11,322.40 · no holdings · equity $11,322.40 vs prior close $11,322.40 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 1665 | $1.70 | $21.48 | — | $8,470.42 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+120.5; leftover $2830.60 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 704 | $4.02 | $9.08 | — | $5,631.26 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+16.1; leftover $2830.60 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 135 | $20.90 | $2.40 | — | $2,807.37 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+47.9; leftover $2830.60 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 23 | $119.46 | $2.06 | — | $57.73 | — | top 4 by hot; rank hot_score; list mover_buy; 🔵; ⚪; ret5=+25.9; leftover $2830.60 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.73 | ▲ 09:30 equity $11,093.18 vs yday $11,093.18 (-0.00) | 09:30 open · cash $57.73 (unchanged overnight, no fees) · equity $11,093.18 vs prior close $11,093.18 (-0.00) because holdings re-marked: CYPH×1665 yday $1.64 → 09:30 $1.64 +0.00; XHG×704 yday $4.05 → 09:30 $4.05 +0.00; ASST×135 yday $20.20 → 09:30 $20.20 +0.00; AU×23 yday $118.55 → 09:30 $118.55 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.73 | ▼ 09:30 equity $10,956.57 vs yday $11,287.39 (-330.82) | 09:30 open · cash $57.73 (unchanged overnight, no fees) · equity $10,956.57 vs prior close $11,287.39 (-330.82) because holdings re-marked: CYPH×1665 yday $1.64 → 09:30 $1.60 -66.60; XHG×704 yday $4.05 → 09:30 $3.81 -168.96; ASST×135 yday $20.20 → 09:30 $20.72 +70.20; AU×23 yday $118.55 → 09:30 $119.80 +28.75 | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 1665 | $1.60 | $21.78 | $-209.75 | $2,699.95 | ▼ -209.75 after sell → book $10,934.79; vs 09:30 mark -21.78 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `XHG` | 704 | $3.81 | $9.22 | $-166.14 | $5,372.97 | ▼ -166.14 after sell → book $10,925.57; vs 09:30 mark -9.22 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ASST` | 135 | $20.72 | $2.44 | $-29.13 | $8,167.73 | ▼ -29.13 after sell → book $10,923.13; vs 09:30 mark -2.44 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AU` | 23 | $119.80 | $2.09 | $+3.67 | $10,921.04 | ▲ +3.67 after sell → book $10,921.04; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 109 | $24.84 | $2.32 | — | $8,211.17 | — | top 4 by hot; rank hot_score; list flatten; ret5=+13.0; leftover $2730.26 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 175 | $15.60 | $2.52 | — | $5,478.65 | — | top 4 by hot; rank hot_score; list mover_buy; 🔵; ret5=+7.1; leftover $2730.26 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 1054 | $2.59 | $13.60 | — | $2,735.19 | — | top 4 by hot; rank hot_score; list flatten; ret5=+4.2; leftover $2730.26 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 11 | $240.00 | $2.02 | — | $93.17 | — | top 4 by hot; rank hot_score; list mover_buy; 🔵; ret5=+6.8; leftover $2730.26 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $93.17 | ▲ 09:30 equity $10,920.16 vs yday $10,861.76 (+58.40) | 09:30 open · cash $93.17 (unchanged overnight, no fees) · equity $10,920.16 vs prior close $10,861.76 (+58.40) because holdings re-marked: MOS×109 yday $24.16 → 09:30 $24.00 -17.44; DLO×175 yday $15.36 → 09:30 $15.33 -5.25; SLI×1054 yday $2.61 → 09:30 $2.60 -10.54; MRVL×11 yday $245.11 → 09:30 $253.44 +91.63 | — |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 109 | $24.00 | $2.36 | $-96.23 | $2,706.81 | ▼ -96.23 after sell → book $10,917.80; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `DLO` | 175 | $15.33 | $2.57 | $-52.33 | $5,387.00 | ▼ -52.33 after sell → book $10,915.24; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 1054 | $2.60 | $13.79 | $-16.85 | $8,113.61 | ▼ -16.85 after sell → book $10,901.45; vs 09:30 mark -13.79 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `MRVL` | 11 | $253.44 | $2.06 | $+143.76 | $10,899.39 | ▲ +143.76 after sell → book $10,899.39; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FIGR` | 72 | $37.42 | $2.21 | — | $8,202.94 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+24.4; leftover $2724.85 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `NIQ` | 145 | $18.79 | $2.42 | — | $5,475.97 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+7.6; leftover $2724.85 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERO` | 69 | $39.20 | $2.20 | — | $2,768.97 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+16.6; leftover $2724.85 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `TRLV` | 239 | $11.38 | $3.08 | — | $46.07 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+15.0; leftover $2724.85 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $46.07 | ▲ 09:30 equity $11,015.46 vs yday $10,932.41 (+83.05) | 09:30 open · cash $46.07 (unchanged overnight, no fees) · equity $11,015.46 vs prior close $10,932.41 (+83.05) because holdings re-marked: FIGR×72 yday $38.02 → 09:30 $35.50 -181.44; NIQ×145 yday $19.07 → 09:30 $19.20 +18.85; ERO×69 yday $39.82 → 09:30 $38.60 -84.18; TRLV×239 yday $11.03 → 09:30 $12.41 +329.82 | — |
| 2026-08-31 09:30 ET | **SELL** | `FIGR` | 72 | $35.50 | $2.24 | $-142.68 | $2,599.83 | ▼ -142.68 after sell → book $11,013.22; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NIQ` | 145 | $19.20 | $2.47 | $+54.55 | $5,381.36 | ▲ +54.55 after sell → book $11,010.75; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🔴 digest🟢 judge🟡 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `ERO` | 69 | $38.60 | $2.23 | $-45.83 | $8,042.53 | ▼ -45.83 after sell → book $11,008.52; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-31 09:30 ET | **SELL** | `TRLV` | 239 | $12.41 | $3.15 | $+239.94 | $11,005.37 | ▲ +239.94 after sell → book $11,005.37; vs 09:30 mark -3.15 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,005.37 | ▲ 09:30 equity $11,005.37 vs yday $11,005.37 (+0.00) | 09:30 open · cash $11,005.37 · no holdings · equity $11,005.37 vs prior close $11,005.37 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,005.37 | ▲ 09:30 equity $11,005.37 vs yday $11,005.37 (+0.00) | 09:30 open · cash $11,005.37 · no holdings · equity $11,005.37 vs prior close $11,005.37 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,005.37 | ▲ 09:30 equity $11,005.37 vs yday $11,005.37 (+0.00) | 09:30 open · cash $11,005.37 · no holdings · equity $11,005.37 vs prior close $11,005.37 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 18 | $151.40 | $2.04 | — | $8,278.13 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $2751.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `XHG` | 770 | $3.57 | $9.93 | — | $5,519.30 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+16.1; leftover $2751.34 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 167 | $16.46 | $2.49 | — | $2,767.99 | — | top 4 by hot; rank hot_score; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $2751.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CAN` | 9043 | $0.30 | $54.26 | — | $0.83 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+54.3; leftover $2751.34 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.83 | ▲ 09:30 equity $11,105.74 vs yday $10,870.72 (+235.02) | 09:30 open · cash $0.83 (unchanged overnight, no fees) · equity $11,105.74 vs prior close $10,870.72 (+235.02) because holdings re-marked: MRNA×18 yday $150.81 → 09:30 $145.95 -87.48; XHG×770 yday $3.32 → 09:30 $3.38 +46.20; ARCT×167 yday $16.74 → 09:30 $16.77 +5.01; CAN×9043 yday $0.31 → 09:30 $0.34 +271.29 | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 18 | $145.95 | $2.08 | $-102.22 | $2,625.85 | ▼ -102.22 after sell → book $11,103.66; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 167 | $16.77 | $2.54 | $+46.74 | $5,423.90 | ▲ +46.74 after sell → book $11,101.12; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CAN` | 9043 | $0.34 | $59.40 | $+248.06 | $8,439.12 | ▲ +248.06 after sell → book $11,041.72; vs 09:30 mark -59.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `HQ` | 164 | $17.06 | $2.48 | — | $5,638.80 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+17.3; leftover $2813.04 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NIQ` | 150 | $18.66 | $2.44 | — | $2,837.36 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+7.6; leftover $2813.04 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DEFT` | 4301 | $0.65 | $40.86 | — | $0.85 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+17.6; leftover $2813.04 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |

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
