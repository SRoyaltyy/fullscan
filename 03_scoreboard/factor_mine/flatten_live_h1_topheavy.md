# Factor mine action — `flatten_live_h1_topheavy`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

New buys only when the live flatten gate fires (green S, ≥5 priced BUYs, prior book). io/HOLD mornings sit.

Side **long** · universe `flatten` · top 8 · rank `list` · size `topheavy` · sell `list` · S-boost `none` · 40% to #1, rest split

Cash book **+8.83%** ($10,883) · signal-only (no cash/fees) was +4.99%. Starts YES **7/17**. Fills 32 · skips 0 · realized $+883.25.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `flatten` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `topheavy` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** New buys only when the live flatten gate fires (green S, ≥5 priced BUYs, prior book). io/HOLD mornings sit.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,883.24.

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
| 2026-08-20 | `AG` | 194 | — | $20.55 | +0.00 | $21.19 | +124.16 | +124.16 | +0.00 | +124.16 |
| 2026-08-20 | `BHP` | 9 | — | $91.01 | +0.00 | $93.63 | +23.58 | +23.58 | +0.00 | +23.58 |
| 2026-08-20 | `CDE` | 41 | — | $20.65 | +0.00 | $21.11 | +18.86 | +18.86 | +0.00 | +18.86 |
| 2026-08-20 | `HDSN` | 148 | — | $5.77 | +0.00 | $5.57 | -29.60 | -29.60 | +0.00 | -29.60 |
| 2026-08-20 | `IAG` | 43 | — | $19.63 | +0.00 | $20.50 | +37.41 | +37.41 | +0.00 | +37.41 |
| 2026-08-20 | `KGC` | 28 | — | $29.63 | +0.00 | $31.43 | +50.40 | +50.40 | +0.00 | +50.40 |
| 2026-08-20 | `NFGC` | 489 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 5 | — | $144.54 | +0.00 | $150.25 | +28.55 | +28.55 | +0.00 | +28.55 |
| 2026-08-21 | `AG` | 194 | $21.19 | $21.90 | +137.74 | — | +0.00 | +137.74 | +261.90 | — |
| 2026-08-21 | `BHP` | 9 | $93.63 | $95.72 | +18.81 | — | +0.00 | +18.81 | +42.39 | — |
| 2026-08-21 | `CDE` | 41 | $21.11 | $21.75 | +26.24 | — | +0.00 | +26.24 | +45.10 | — |
| 2026-08-21 | `HDSN` | 148 | $5.57 | $5.67 | +14.80 | — | +0.00 | +14.80 | -14.80 | — |
| 2026-08-21 | `IAG` | 43 | $20.50 | $21.17 | +28.81 | — | +0.00 | +28.81 | +66.22 | — |
| 2026-08-21 | `KGC` | 28 | $31.43 | $32.17 | +20.72 | — | +0.00 | +20.72 | +71.12 | — |
| 2026-08-21 | `NFGC` | 489 | $1.75 | $1.79 | +19.56 | — | +0.00 | +19.56 | +19.56 | — |
| 2026-08-21 | `WPM` | 5 | $150.25 | $154.70 | +22.25 | — | +0.00 | +22.25 | +50.80 | — |
| 2026-08-21 | `AU` | 35 | — | $119.43 | +0.00 | $121.22 | +62.65 | +62.65 | +0.00 | +62.65 |
| 2026-08-21 | `AUPH` | 52 | — | $17.20 | +0.00 | $16.65 | -28.60 | -28.60 | +0.00 | -28.60 |
| 2026-08-21 | `AEM` | 4 | — | $216.30 | +0.00 | $216.06 | -0.96 | -0.96 | +0.00 | -0.96 |
| 2026-08-21 | `ARCT` | 80 | — | $11.13 | +0.00 | $13.45 | +185.60 | +185.60 | +0.00 | +185.60 |
| 2026-08-21 | `AUTL` | 364 | — | $2.47 | +0.00 | $2.41 | -21.84 | -21.84 | +0.00 | -21.84 |
| 2026-08-21 | `CRDL` | 466 | — | $1.93 | +0.00 | $1.86 | -32.62 | -32.62 | +0.00 | -32.62 |
| 2026-08-21 | `CRSP` | 15 | — | $59.72 | +0.00 | $59.50 | -3.30 | -3.30 | +0.00 | -3.30 |
| 2026-08-21 | `CYPH` | 681 | — | $1.32 | +0.00 | $1.42 | +68.10 | +68.10 | +0.00 | +68.10 |
| 2026-08-24 | `AU` | 35 | $121.22 | $120.50 | -25.20 | — | +0.00 | -25.20 | +37.45 | — |
| 2026-08-24 | `AUPH` | 52 | $16.65 | $16.60 | -2.60 | — | +0.00 | -2.60 | -31.20 | — |
| 2026-08-24 | `AEM` | 4 | $216.06 | $217.03 | +3.88 | — | +0.00 | +3.88 | +2.92 | — |
| 2026-08-24 | `ARCT` | 80 | $13.45 | $13.26 | -15.20 | — | +0.00 | -15.20 | +170.40 | — |
| 2026-08-24 | `AUTL` | 364 | $2.41 | $2.36 | -18.20 | — | +0.00 | -18.20 | -40.04 | — |
| 2026-08-24 | `CRDL` | 466 | $1.86 | $1.87 | +4.66 | — | +0.00 | +4.66 | -27.96 | — |
| 2026-08-24 | `CRSP` | 15 | $59.50 | $58.79 | -10.65 | — | +0.00 | -10.65 | -13.95 | — |
| 2026-08-24 | `CYPH` | 681 | $1.42 | $1.83 | +279.21 | — | +0.00 | +279.21 | +347.31 | — |
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
| 2026-08-20 | +1.12 | $10,000.00 | — | $10,000.00 | +0.00 | +253.36 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $219.78 | $10,231.72 | AG×194, BHP×9, CDE×41, HDSN×148, IAG×43, KGC×28, NFGC×489, WPM×5 |
| 2026-08-21 | +3.25 | $219.78 | AG×194, BHP×9, CDE×41, HDSN×148, IAG×43, KGC×28, NFGC×489, WPM×5 | $10,520.65 | +288.93 | +229.03 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $45.48 | $10,697.74 | AU×35, AUPH×52, AEM×4, ARCT×80, AUTL×364, CRDL×466, CRSP×15, CYPH×681 |
| 2026-08-24 | -5.17 | $45.48 | AU×35, AUPH×52, AEM×4, ARCT×80, AUTL×364, CRDL×466, CRSP×15, CYPH×681 | $10,913.64 | +215.90 | +0.00 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $10,883.24 | $10,883.24 | — |
| 2026-08-25 | +1.80 | $10,883.24 | — | $10,883.24 | -0.00 | +0.00 | — | — | $10,883.24 | $10,883.24 | — |
| 2026-08-26 | +2.02 | $10,883.24 | — | $10,883.24 | -0.00 | +0.00 | — | — | $10,883.24 | $10,883.24 | — |
| 2026-08-27 | — | $10,883.24 | — | $10,883.24 | -0.00 | +0.00 | — | — | $10,883.24 | $10,883.24 | — |
| 2026-08-28 | +0.75 | $10,883.24 | — | $10,883.24 | -0.00 | +0.00 | — | — | $10,883.24 | $10,883.24 | — |
| 2026-08-31 | -5.85 | $10,883.24 | — | $10,883.24 | -0.00 | +0.00 | — | — | $10,883.24 | $10,883.24 | — |
| 2026-09-01 | -6.30 | $10,883.24 | — | $10,883.24 | -0.00 | +0.00 | — | — | $10,883.24 | $10,883.24 | — |
| 2026-09-02 | -3.83 | $10,883.24 | — | $10,883.24 | -0.00 | +0.00 | — | — | $10,883.24 | $10,883.24 | — |
| 2026-09-03 | -0.90 | $10,883.24 | — | $10,883.24 | -0.00 | +0.00 | — | — | $10,883.24 | $10,883.24 | — |
| 2026-09-04 | — | $10,883.24 | — | $10,883.24 | -0.00 | +0.00 | — | — | $10,883.24 | $10,883.24 | — |

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
| 2026-08-20 09:30 ET | **BUY** | `AG` | 194 | $20.55 | $2.57 | — | $6,010.73 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $4000.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 9 | $91.01 | $2.02 | — | $5,189.62 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $857.14 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 41 | $20.65 | $2.11 | — | $4,340.86 | — | 40% to #1, rest split; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $857.14 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 148 | $5.77 | $2.43 | — | $3,484.46 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $857.14 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 43 | $19.63 | $2.12 | — | $2,638.25 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $857.14 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 28 | $29.63 | $2.07 | — | $1,806.54 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $857.14 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 489 | $1.75 | $6.31 | — | $944.48 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $857.14 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 5 | $144.54 | $2.00 | — | $219.78 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $857.14 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $219.78 | ▲ close $10,231.72 vs 09:30 $10,000.00 (session +253.36) | 16:00 close · cash $219.78 · equity $10,231.72 vs 09:30 $10,000.00 (+231.72; session marks +253.36) · 8 name(s) marked open→close (per-name table). AG×194 09:30 $20.55 → close $21.19 +124.16; BHP×9 09:30 $91.01 → close $93.63 +23.58; CDE×41 09:30 $20.65 → close $21.11 +18.86; HDSN×148 09:30 $5.77 → close $5.57 -29.60; IAG×43 09:30 $19.63 → close $20.50 +37.41; KGC×28 09:30 $29.63 → close $31.43 +50.40; NFGC×489 09:30 $1.75 → close $1.75 +0.00; WPM×5 09:30 $144.54 → close $150.25 +28.55 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $219.78 | ▲ 09:30 equity $10,520.65 vs yday $10,231.72 (+288.93) | 09:30 open · cash $219.78 (unchanged overnight, no fees) · equity $10,520.65 vs prior close $10,231.72 (+288.93) · 8 name(s) re-marked at the open (per-name table). AG×194 yday $21.19 → 09:30 $21.90 +137.74; BHP×9 yday $93.63 → 09:30 $95.72 +18.81; CDE×41 yday $21.11 → 09:30 $21.75 +26.24; HDSN×148 yday $5.57 → 09:30 $5.67 +14.80; IAG×43 yday $20.50 → 09:30 $21.17 +28.81; KGC×28 yday $31.43 → 09:30 $32.17 +20.72; NFGC×489 yday $1.75 → 09:30 $1.79 +19.56; WPM×5 yday $150.25 → 09:30 $154.70 +22.25 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 194 | $21.90 | $2.64 | $+256.69 | $4,465.74 | ▲ +256.69 after sell → book $10,518.01; vs 09:30 mark -2.64 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 9 | $95.72 | $2.04 | $+38.34 | $5,325.18 | ▲ +38.34 after sell → book $10,515.97; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 41 | $21.75 | $2.13 | $+40.85 | $6,214.80 | ▲ +40.85 after sell → book $10,513.84; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 148 | $5.67 | $2.47 | $-19.70 | $7,051.49 | ▼ -19.70 after sell → book $10,511.37; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 43 | $21.17 | $2.14 | $+61.96 | $7,959.66 | ▲ +61.96 after sell → book $10,509.23; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 28 | $32.17 | $2.09 | $+66.95 | $8,858.33 | ▲ +66.95 after sell → book $10,507.14; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 489 | $1.79 | $6.40 | $+6.85 | $9,727.24 | ▲ +6.85 after sell → book $10,500.74; vs 09:30 mark -6.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 5 | $154.70 | $2.02 | $+46.77 | $10,498.71 | ▲ +46.77 after sell → book $10,498.71; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 35 | $119.43 | $2.10 | — | $6,316.57 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+20.4; leftover $4199.49 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 52 | $17.20 | $2.15 | — | $5,420.02 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $899.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 4 | $216.30 | $2.00 | — | $4,552.82 | — | 40% to #1, rest split; list flatten,ohlc_hot,mover_buy; live flatten mover; 🔵; ⚪; ret5=+17.6; leftover $899.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 80 | $11.13 | $2.23 | — | $3,660.19 | — | 40% to #1, rest split; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $899.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 364 | $2.47 | $4.70 | — | $2,756.42 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $899.89 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 466 | $1.93 | $6.01 | — | $1,851.02 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $899.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 15 | $59.72 | $2.04 | — | $953.19 | — | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.6; leftover $899.89 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 681 | $1.32 | $8.78 | — | $45.48 | — | 40% to #1, rest split; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $899.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $45.48 | ▲ close $10,697.74 vs 09:30 $10,520.65 (session +229.03) | 16:00 close · cash $45.48 · equity $10,697.74 vs 09:30 $10,520.65 (+177.09; session marks +229.03) · 8 name(s) marked open→close (per-name table). AU×35 09:30 $119.43 → close $121.22 +62.65; AUPH×52 09:30 $17.20 → close $16.65 -28.60; AEM×4 09:30 $216.30 → close $216.06 -0.96; ARCT×80 09:30 $11.13 → close $13.45 +185.60; AUTL×364 09:30 $2.47 → close $2.41 -21.84; CRDL×466 09:30 $1.93 → close $1.86 -32.62; CRSP×15 09:30 $59.72 → close $59.50 -3.30; CYPH×681 09:30 $1.32 → close $1.42 +68.10 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $45.48 | ▲ 09:30 equity $10,913.64 vs yday $10,697.74 (+215.90) | 09:30 open · cash $45.48 (unchanged overnight, no fees) · equity $10,913.64 vs prior close $10,697.74 (+215.90) · 8 name(s) re-marked at the open (per-name table). AU×35 yday $121.22 → 09:30 $120.50 -25.20; AUPH×52 yday $16.65 → 09:30 $16.60 -2.60; AEM×4 yday $216.06 → 09:30 $217.03 +3.88; ARCT×80 yday $13.45 → 09:30 $13.26 -15.20; AUTL×364 yday $2.41 → 09:30 $2.36 -18.20; CRDL×466 yday $1.86 → 09:30 $1.87 +4.66; CRSP×15 yday $59.50 → 09:30 $58.79 -10.65; CYPH×681 yday $1.42 → 09:30 $1.83 +279.21 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 35 | $120.50 | $2.14 | $+33.22 | $4,260.85 | ▲ +33.22 after sell → book $10,911.51; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 52 | $16.60 | $2.17 | $-35.51 | $5,121.88 | ▼ -35.51 after sell → book $10,909.34; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 4 | $217.03 | $2.02 | $-1.10 | $5,987.98 | ▼ -1.10 after sell → book $10,907.32; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 80 | $13.26 | $2.25 | $+165.92 | $7,046.52 | ▲ +165.92 after sell → book $10,905.06; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 364 | $2.36 | $4.77 | $-49.50 | $7,900.80 | ▼ -49.50 after sell → book $10,900.30; vs 09:30 mark -4.76 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 466 | $1.87 | $6.10 | $-40.07 | $8,766.12 | ▼ -40.07 after sell → book $10,894.20; vs 09:30 mark -6.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 15 | $58.79 | $2.06 | $-18.04 | $9,645.91 | ▼ -18.04 after sell → book $10,892.14; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 681 | $1.83 | $8.91 | $+329.62 | $10,883.24 | ▲ +329.62 after sell → book $10,883.24; vs 09:30 mark -8.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,883.24 | ▲ close $10,883.24 vs 09:30 $10,913.64 (session +0.00) | 16:00 close · cash $10,883.24 · no lots left · equity $10,883.24. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,883.24 | ▲ 09:30 equity $10,883.24 vs yday $10,883.24 (-0.00) | 09:30 open · cash $10,883.24 · no holdings · equity $10,883.24 vs prior close $10,883.24 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,883.24 | ▲ close $10,883.24 vs 09:30 $10,883.24 (session +0.00) | 16:00 close · cash $10,883.24 · no lots left · equity $10,883.24. | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,883.24 | ▲ 09:30 equity $10,883.24 vs yday $10,883.24 (-0.00) | 09:30 open · cash $10,883.24 · no holdings · equity $10,883.24 vs prior close $10,883.24 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,883.24 | ▲ close $10,883.24 vs 09:30 $10,883.24 (session +0.00) | 16:00 close · cash $10,883.24 · no lots left · equity $10,883.24. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,883.24 | ▲ 09:30 equity $10,883.24 vs yday $10,883.24 (-0.00) | 09:30 open · cash $10,883.24 · no holdings · equity $10,883.24 vs prior close $10,883.24 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,883.24 | ▲ close $10,883.24 vs 09:30 $10,883.24 (session +0.00) | 16:00 close · cash $10,883.24 · no lots left · equity $10,883.24. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,883.24 | ▲ 09:30 equity $10,883.24 vs yday $10,883.24 (-0.00) | 09:30 open · cash $10,883.24 · no holdings · equity $10,883.24 vs prior close $10,883.24 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,883.24 | ▲ close $10,883.24 vs 09:30 $10,883.24 (session +0.00) | 16:00 close · cash $10,883.24 · no lots left · equity $10,883.24. | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,883.24 | ▲ 09:30 equity $10,883.24 vs yday $10,883.24 (-0.00) | 09:30 open · cash $10,883.24 · no holdings · equity $10,883.24 vs prior close $10,883.24 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,883.24 | ▲ close $10,883.24 vs 09:30 $10,883.24 (session +0.00) | 16:00 close · cash $10,883.24 · no lots left · equity $10,883.24. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,883.24 | ▲ 09:30 equity $10,883.24 vs yday $10,883.24 (-0.00) | 09:30 open · cash $10,883.24 · no holdings · equity $10,883.24 vs prior close $10,883.24 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,883.24 | ▲ close $10,883.24 vs 09:30 $10,883.24 (session +0.00) | 16:00 close · cash $10,883.24 · no lots left · equity $10,883.24. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,883.24 | ▲ 09:30 equity $10,883.24 vs yday $10,883.24 (-0.00) | 09:30 open · cash $10,883.24 · no holdings · equity $10,883.24 vs prior close $10,883.24 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,883.24 | ▲ close $10,883.24 vs 09:30 $10,883.24 (session +0.00) | 16:00 close · cash $10,883.24 · no lots left · equity $10,883.24. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,883.24 | ▲ 09:30 equity $10,883.24 vs yday $10,883.24 (-0.00) | 09:30 open · cash $10,883.24 · no holdings · equity $10,883.24 vs prior close $10,883.24 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,883.24 | ▲ close $10,883.24 vs 09:30 $10,883.24 (session +0.00) | 16:00 close · cash $10,883.24 · no lots left · equity $10,883.24. | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,883.24 | ▲ 09:30 equity $10,883.24 vs yday $10,883.24 (-0.00) | 09:30 open · cash $10,883.24 · no holdings · equity $10,883.24 vs prior close $10,883.24 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,883.24 | ▲ close $10,883.24 vs 09:30 $10,883.24 (session +0.00) | 16:00 close · cash $10,883.24 · no lots left · equity $10,883.24. | — |
