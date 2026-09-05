# Factor mine action — `flatten_live_h1_half`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

New buys only when the live flatten gate fires (green S, ≥5 priced BUYs, prior book). io/HOLD mornings sit.

Side **long** · universe `flatten` · top 8 · rank `list` · size `half` · sell `list` · S-boost `none` · deploy half leftover

Cash book **+4.55%** ($10,455) · signal-only (no cash/fees) was +4.99%. Starts YES **7/17**. Fills 32 · skips 0 · realized $+454.65.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `flatten` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `half` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** New buys only when the live flatten gate fires (green S, ≥5 priced BUYs, prior book). io/HOLD mornings sit.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,454.66.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-17 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 30 | — | $20.55 | +0.00 | $21.19 | +19.20 | +19.20 | +0.00 | +19.20 |
| 2026-08-20 | `BHP` | 6 | — | $91.01 | +0.00 | $93.63 | +15.72 | +15.72 | +0.00 | +15.72 |
| 2026-08-20 | `CDE` | 30 | — | $20.65 | +0.00 | $21.11 | +13.80 | +13.80 | +0.00 | +13.80 |
| 2026-08-20 | `HDSN` | 108 | — | $5.77 | +0.00 | $5.57 | -21.60 | -21.60 | +0.00 | -21.60 |
| 2026-08-20 | `IAG` | 31 | — | $19.63 | +0.00 | $20.50 | +26.97 | +26.97 | +0.00 | +26.97 |
| 2026-08-20 | `KGC` | 21 | — | $29.63 | +0.00 | $31.43 | +37.80 | +37.80 | +0.00 | +37.80 |
| 2026-08-20 | `NFGC` | 357 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 4 | — | $144.54 | +0.00 | $150.25 | +22.84 | +22.84 | +0.00 | +22.84 |
| 2026-08-21 | `AG` | 30 | $21.19 | $21.90 | +21.30 | — | +0.00 | +21.30 | +40.50 | — |
| 2026-08-21 | `BHP` | 6 | $93.63 | $95.72 | +12.54 | — | +0.00 | +12.54 | +28.26 | — |
| 2026-08-21 | `CDE` | 30 | $21.11 | $21.75 | +19.20 | — | +0.00 | +19.20 | +33.00 | — |
| 2026-08-21 | `HDSN` | 108 | $5.57 | $5.67 | +10.80 | — | +0.00 | +10.80 | -10.80 | — |
| 2026-08-21 | `IAG` | 31 | $20.50 | $21.17 | +20.77 | — | +0.00 | +20.77 | +47.74 | — |
| 2026-08-21 | `KGC` | 21 | $31.43 | $32.17 | +15.54 | — | +0.00 | +15.54 | +53.34 | — |
| 2026-08-21 | `NFGC` | 357 | $1.75 | $1.79 | +14.28 | — | +0.00 | +14.28 | +14.28 | — |
| 2026-08-21 | `WPM` | 4 | $150.25 | $154.70 | +17.80 | — | +0.00 | +17.80 | +40.64 | — |
| 2026-08-21 | `AU` | 5 | — | $119.43 | +0.00 | $121.22 | +8.95 | +8.95 | +0.00 | +8.95 |
| 2026-08-21 | `AUPH` | 37 | — | $17.20 | +0.00 | $16.65 | -20.35 | -20.35 | +0.00 | -20.35 |
| 2026-08-21 | `AEM` | 2 | — | $216.30 | +0.00 | $216.06 | -0.48 | -0.48 | +0.00 | -0.48 |
| 2026-08-21 | `ARCT` | 57 | — | $11.13 | +0.00 | $13.45 | +132.24 | +132.24 | +0.00 | +132.24 |
| 2026-08-21 | `AUTL` | 258 | — | $2.47 | +0.00 | $2.41 | -15.48 | -15.48 | +0.00 | -15.48 |
| 2026-08-21 | `CRDL` | 330 | — | $1.93 | +0.00 | $1.86 | -23.10 | -23.10 | +0.00 | -23.10 |
| 2026-08-21 | `CRSP` | 10 | — | $59.72 | +0.00 | $59.50 | -2.20 | -2.20 | +0.00 | -2.20 |
| 2026-08-21 | `CYPH` | 483 | — | $1.32 | +0.00 | $1.42 | +48.30 | +48.30 | +0.00 | +48.30 |
| 2026-08-24 | `AU` | 5 | $121.22 | $120.50 | -3.60 | — | +0.00 | -3.60 | +5.35 | — |
| 2026-08-24 | `AUPH` | 37 | $16.65 | $16.60 | -1.85 | — | +0.00 | -1.85 | -22.20 | — |
| 2026-08-24 | `AEM` | 2 | $216.06 | $217.03 | +1.94 | — | +0.00 | +1.94 | +1.46 | — |
| 2026-08-24 | `ARCT` | 57 | $13.45 | $13.26 | -10.83 | — | +0.00 | -10.83 | +121.41 | — |
| 2026-08-24 | `AUTL` | 258 | $2.41 | $2.36 | -12.90 | — | +0.00 | -12.90 | -28.38 | — |
| 2026-08-24 | `CRDL` | 330 | $1.86 | $1.87 | +3.30 | — | +0.00 | +3.30 | -19.80 | — |
| 2026-08-24 | `CRSP` | 10 | $59.50 | $58.79 | -7.10 | — | +0.00 | -7.10 | -9.30 | — |
| 2026-08-24 | `CYPH` | 483 | $1.42 | $1.83 | +198.03 | — | +0.00 | +198.03 | +246.33 | — |
| 2026-08-25 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-26 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-27 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-28 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-31 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-04 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-17 | +2.25 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-18 | -6.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-19 | -7.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-20 | +1.12 | $10,000.00 | — | $10,000.00 | +0.00 | +114.73 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $5,141.88 | $10,095.50 | AG×30, BHP×6, CDE×30, HDSN×108, IAG×31, KGC×21, NFGC×357, WPM×4 |
| 2026-08-21 | +3.25 | $5,141.88 | AG×30, BHP×6, CDE×30, HDSN×108, IAG×31, KGC×21, NFGC×357, WPM×4 | $10,227.73 | +132.23 | +127.88 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $5,374.71 | $10,312.07 | AU×5, AUPH×37, AEM×2, ARCT×57, AUTL×258, CRDL×330, CRSP×10, CYPH×483 |
| 2026-08-24 | -5.17 | $5,374.71 | AU×5, AUPH×37, AEM×2, ARCT×57, AUTL×258, CRDL×330, CRSP×10, CYPH×483 | $10,479.06 | +166.99 | +0.00 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $10,454.66 | $10,454.66 | — |
| 2026-08-25 | +1.80 | $10,454.66 | — | $10,454.66 | -0.00 | +0.00 | — | — | $10,454.66 | $10,454.66 | — |
| 2026-08-26 | +2.02 | $10,454.66 | — | $10,454.66 | -0.00 | +0.00 | — | — | $10,454.66 | $10,454.66 | — |
| 2026-08-27 | — | $10,454.66 | — | $10,454.66 | -0.00 | +0.00 | — | — | $10,454.66 | $10,454.66 | — |
| 2026-08-28 | +0.75 | $10,454.66 | — | $10,454.66 | -0.00 | +0.00 | — | — | $10,454.66 | $10,454.66 | — |
| 2026-08-31 | -5.85 | $10,454.66 | — | $10,454.66 | -0.00 | +0.00 | — | — | $10,454.66 | $10,454.66 | — |
| 2026-09-01 | -6.30 | $10,454.66 | — | $10,454.66 | -0.00 | +0.00 | — | — | $10,454.66 | $10,454.66 | — |
| 2026-09-02 | -3.83 | $10,454.66 | — | $10,454.66 | -0.00 | +0.00 | — | — | $10,454.66 | $10,454.66 | — |
| 2026-09-03 | -0.90 | $10,454.66 | — | $10,454.66 | -0.00 | +0.00 | — | — | $10,454.66 | $10,454.66 | — |
| 2026-09-04 | — | $10,454.66 | — | $10,454.66 | -0.00 | +0.00 | — | — | $10,454.66 | $10,454.66 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 30 | $20.55 | $2.08 | — | $9,381.42 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $625.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 6 | $91.01 | $2.01 | — | $8,833.35 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $625.00 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 30 | $20.65 | $2.08 | — | $8,211.77 | — | deploy half leftover; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $625.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 108 | $5.77 | $2.31 | — | $7,586.30 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $625.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 31 | $19.63 | $2.08 | — | $6,975.69 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $625.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 21 | $29.63 | $2.05 | — | $6,351.40 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $625.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 357 | $1.75 | $4.61 | — | $5,722.05 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $625.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 4 | $144.54 | $2.00 | — | $5,141.88 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $625.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,141.88 | ▲ close $10,095.50 vs 09:30 $10,000.00 (session +114.73) | 16:00 close · cash $5,141.88 · equity $10,095.50 vs 09:30 $10,000.00 (+95.50; session marks +114.73) · 8 name(s) marked open→close (per-name table). AG×30 09:30 $20.55 → close $21.19 +19.20; BHP×6 09:30 $91.01 → close $93.63 +15.72; CDE×30 09:30 $20.65 → close $21.11 +13.80; HDSN×108 09:30 $5.77 → close $5.57 -21.60; IAG×31 09:30 $19.63 → close $20.50 +26.97; KGC×21 09:30 $29.63 → close $31.43 +37.80; NFGC×357 09:30 $1.75 → close $1.75 +0.00; WPM×4 09:30 $144.54 → close $150.25 +22.84 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,141.88 | ▲ 09:30 equity $10,227.73 vs yday $10,095.50 (+132.23) | 09:30 open · cash $5,141.88 (unchanged overnight, no fees) · equity $10,227.73 vs prior close $10,095.50 (+132.23) · 8 name(s) re-marked at the open (per-name table). AG×30 yday $21.19 → 09:30 $21.90 +21.30; BHP×6 yday $93.63 → 09:30 $95.72 +12.54; CDE×30 yday $21.11 → 09:30 $21.75 +19.20; HDSN×108 yday $5.57 → 09:30 $5.67 +10.80; IAG×31 yday $20.50 → 09:30 $21.17 +20.77; KGC×21 yday $31.43 → 09:30 $32.17 +15.54; NFGC×357 yday $1.75 → 09:30 $1.79 +14.28; WPM×4 yday $150.25 → 09:30 $154.70 +17.80 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 30 | $21.90 | $2.10 | $+36.32 | $5,796.78 | ▲ +36.32 after sell → book $10,225.63; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 6 | $95.72 | $2.03 | $+24.22 | $6,369.08 | ▲ +24.22 after sell → book $10,223.61; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 30 | $21.75 | $2.10 | $+28.82 | $7,019.48 | ▲ +28.82 after sell → book $10,221.51; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 108 | $5.67 | $2.34 | $-15.46 | $7,629.49 | ▼ -15.46 after sell → book $10,219.16; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 31 | $21.17 | $2.10 | $+43.55 | $8,283.66 | ▲ +43.55 after sell → book $10,217.06; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 21 | $32.17 | $2.07 | $+49.21 | $8,957.16 | ▲ +49.21 after sell → book $10,214.99; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 357 | $1.79 | $4.67 | $+5.00 | $9,591.51 | ▲ +5.00 after sell → book $10,210.31; vs 09:30 mark -4.68 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 4 | $154.70 | $2.02 | $+36.62 | $10,208.29 | ▲ +36.62 after sell → book $10,208.29; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 5 | $119.43 | $2.00 | — | $9,609.14 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+20.4; leftover $638.02 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 37 | $17.20 | $2.10 | — | $8,970.64 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $638.02 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 2 | $216.30 | $2.00 | — | $8,536.04 | — | deploy half leftover; list flatten,ohlc_hot,mover_buy; live flatten mover; 🔵; ⚪; ret5=+17.6; leftover $638.02 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 57 | $11.13 | $2.16 | — | $7,899.47 | — | deploy half leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $638.02 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 258 | $2.47 | $3.33 | — | $7,258.88 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $638.02 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 330 | $1.93 | $4.26 | — | $6,617.72 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $638.02 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 10 | $59.72 | $2.02 | — | $6,018.50 | — | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.6; leftover $638.02 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 483 | $1.32 | $6.23 | — | $5,374.71 | — | deploy half leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $638.02 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,374.71 | ▲ close $10,312.07 vs 09:30 $10,227.73 (session +127.88) | 16:00 close · cash $5,374.71 · equity $10,312.07 vs 09:30 $10,227.73 (+84.34; session marks +127.88) · 8 name(s) marked open→close (per-name table). AU×5 09:30 $119.43 → close $121.22 +8.95; AUPH×37 09:30 $17.20 → close $16.65 -20.35; AEM×2 09:30 $216.30 → close $216.06 -0.48; ARCT×57 09:30 $11.13 → close $13.45 +132.24; AUTL×258 09:30 $2.47 → close $2.41 -15.48; CRDL×330 09:30 $1.93 → close $1.86 -23.10; CRSP×10 09:30 $59.72 → close $59.50 -2.20; CYPH×483 09:30 $1.32 → close $1.42 +48.30 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,374.71 | ▲ 09:30 equity $10,479.06 vs yday $10,312.07 (+166.99) | 09:30 open · cash $5,374.71 (unchanged overnight, no fees) · equity $10,479.06 vs prior close $10,312.07 (+166.99) · 8 name(s) re-marked at the open (per-name table). AU×5 yday $121.22 → 09:30 $120.50 -3.60; AUPH×37 yday $16.65 → 09:30 $16.60 -1.85; AEM×2 yday $216.06 → 09:30 $217.03 +1.94; ARCT×57 yday $13.45 → 09:30 $13.26 -10.83; AUTL×258 yday $2.41 → 09:30 $2.36 -12.90; CRDL×330 yday $1.86 → 09:30 $1.87 +3.30; CRSP×10 yday $59.50 → 09:30 $58.79 -7.10; CYPH×483 yday $1.42 → 09:30 $1.83 +198.03 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 5 | $120.50 | $2.02 | $+1.32 | $5,975.19 | ▲ +1.32 after sell → book $10,477.04; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 37 | $16.60 | $2.12 | $-26.42 | $6,587.27 | ▼ -26.42 after sell → book $10,474.92; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 2 | $217.03 | $2.02 | $-2.55 | $7,019.31 | ▼ -2.55 after sell → book $10,472.90; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 57 | $13.26 | $2.18 | $+117.07 | $7,772.95 | ▲ +117.07 after sell → book $10,470.72; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 258 | $2.36 | $3.38 | $-35.09 | $8,378.45 | ▼ -35.09 after sell → book $10,467.34; vs 09:30 mark -3.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 330 | $1.87 | $4.32 | $-28.38 | $8,991.23 | ▼ -28.38 after sell → book $10,463.02; vs 09:30 mark -4.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 10 | $58.79 | $2.04 | $-13.36 | $9,577.09 | ▼ -13.36 after sell → book $10,460.98; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 483 | $1.83 | $6.32 | $+233.78 | $10,454.66 | ▲ +233.78 after sell → book $10,454.66; vs 09:30 mark -6.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,454.66 | ▲ close $10,454.66 vs 09:30 $10,479.06 (session +0.00) | 16:00 close · cash $10,454.66 · no lots left · equity $10,454.66. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,454.66 | ▲ 09:30 equity $10,454.66 vs yday $10,454.66 (-0.00) | 09:30 open · cash $10,454.66 · no holdings · equity $10,454.66 vs prior close $10,454.66 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,454.66 | ▲ close $10,454.66 vs 09:30 $10,454.66 (session +0.00) | 16:00 close · cash $10,454.66 · no lots left · equity $10,454.66. | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,454.66 | ▲ 09:30 equity $10,454.66 vs yday $10,454.66 (-0.00) | 09:30 open · cash $10,454.66 · no holdings · equity $10,454.66 vs prior close $10,454.66 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,454.66 | ▲ close $10,454.66 vs 09:30 $10,454.66 (session +0.00) | 16:00 close · cash $10,454.66 · no lots left · equity $10,454.66. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,454.66 | ▲ 09:30 equity $10,454.66 vs yday $10,454.66 (-0.00) | 09:30 open · cash $10,454.66 · no holdings · equity $10,454.66 vs prior close $10,454.66 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,454.66 | ▲ close $10,454.66 vs 09:30 $10,454.66 (session +0.00) | 16:00 close · cash $10,454.66 · no lots left · equity $10,454.66. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,454.66 | ▲ 09:30 equity $10,454.66 vs yday $10,454.66 (-0.00) | 09:30 open · cash $10,454.66 · no holdings · equity $10,454.66 vs prior close $10,454.66 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,454.66 | ▲ close $10,454.66 vs 09:30 $10,454.66 (session +0.00) | 16:00 close · cash $10,454.66 · no lots left · equity $10,454.66. | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,454.66 | ▲ 09:30 equity $10,454.66 vs yday $10,454.66 (-0.00) | 09:30 open · cash $10,454.66 · no holdings · equity $10,454.66 vs prior close $10,454.66 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,454.66 | ▲ close $10,454.66 vs 09:30 $10,454.66 (session +0.00) | 16:00 close · cash $10,454.66 · no lots left · equity $10,454.66. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,454.66 | ▲ 09:30 equity $10,454.66 vs yday $10,454.66 (-0.00) | 09:30 open · cash $10,454.66 · no holdings · equity $10,454.66 vs prior close $10,454.66 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,454.66 | ▲ close $10,454.66 vs 09:30 $10,454.66 (session +0.00) | 16:00 close · cash $10,454.66 · no lots left · equity $10,454.66. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,454.66 | ▲ 09:30 equity $10,454.66 vs yday $10,454.66 (-0.00) | 09:30 open · cash $10,454.66 · no holdings · equity $10,454.66 vs prior close $10,454.66 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,454.66 | ▲ close $10,454.66 vs 09:30 $10,454.66 (session +0.00) | 16:00 close · cash $10,454.66 · no lots left · equity $10,454.66. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,454.66 | ▲ 09:30 equity $10,454.66 vs yday $10,454.66 (-0.00) | 09:30 open · cash $10,454.66 · no holdings · equity $10,454.66 vs prior close $10,454.66 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,454.66 | ▲ close $10,454.66 vs 09:30 $10,454.66 (session +0.00) | 16:00 close · cash $10,454.66 · no lots left · equity $10,454.66. | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,454.66 | ▲ 09:30 equity $10,454.66 vs yday $10,454.66 (-0.00) | 09:30 open · cash $10,454.66 · no holdings · equity $10,454.66 vs prior close $10,454.66 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,454.66 | ▲ close $10,454.66 vs 09:30 $10,454.66 (session +0.00) | 16:00 close · cash $10,454.66 · no lots left · equity $10,454.66. | — |
