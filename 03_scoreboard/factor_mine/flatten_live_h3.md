# Factor mine action — `flatten_live_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

New buys only when the live flatten gate fires (green S, ≥5 priced BUYs, prior book). io/HOLD mornings sit.

Side **long** · universe `flatten` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · 09:30 tickets only when flatten_robust gate fires (mover)

Cash book **+4.92%** ($10,492) · signal-only (no cash/fees) was +9.59%. Starts YES **7/17**. Fills 26 · skips 34 · realized $+491.55.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `flatten` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** New buys only when the live flatten gate fires (green S, ≥5 priced BUYs, prior book). io/HOLD mornings sit.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,491.55.

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
| 2026-08-20 | `AG` | 60 | — | $20.55 | +0.00 | $21.19 | +38.40 | +38.40 | +0.00 | +38.40 |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `CDE` | 60 | — | $20.65 | +0.00 | $21.11 | +27.60 | +27.60 | +0.00 | +27.60 |
| 2026-08-20 | `HDSN` | 216 | — | $5.77 | +0.00 | $5.57 | -43.20 | -43.20 | +0.00 | -43.20 |
| 2026-08-20 | `IAG` | 63 | — | $19.63 | +0.00 | $20.50 | +54.81 | +54.81 | +0.00 | +54.81 |
| 2026-08-20 | `KGC` | 42 | — | $29.63 | +0.00 | $31.43 | +75.60 | +75.60 | +0.00 | +75.60 |
| 2026-08-20 | `NFGC` | 714 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-21 | `AG` | 60 | $21.19 | $21.90 | +42.60 | $21.09 | -48.60 | -6.00 | +81.00 | +32.40 |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | $97.03 | +17.03 | +44.20 | +61.23 | +78.26 |
| 2026-08-21 | `CDE` | 60 | $21.11 | $21.75 | +38.40 | $20.97 | -46.80 | -8.40 | +66.00 | +19.20 |
| 2026-08-21 | `HDSN` | 216 | $5.57 | $5.67 | +21.60 | $5.63 | -8.64 | +12.96 | -21.60 | -30.24 |
| 2026-08-21 | `IAG` | 63 | $20.50 | $21.17 | +42.21 | $21.14 | -1.89 | +40.32 | +97.02 | +95.13 |
| 2026-08-21 | `KGC` | 42 | $31.43 | $32.17 | +31.08 | $32.76 | +24.78 | +55.86 | +106.68 | +131.46 |
| 2026-08-21 | `NFGC` | 714 | $1.75 | $1.79 | +28.56 | $1.84 | +35.70 | +64.26 | +28.56 | +64.26 |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | $157.78 | +24.64 | +60.24 | +81.28 | +105.92 |
| 2026-08-21 | `AUPH` | 1 | — | $17.20 | +0.00 | $16.65 | -0.55 | -0.55 | +0.00 | -0.55 |
| 2026-08-21 | `ARCT` | 2 | — | $11.13 | +0.00 | $13.45 | +4.64 | +4.64 | +0.00 | +4.64 |
| 2026-08-21 | `AUTL` | 9 | — | $2.47 | +0.00 | $2.41 | -0.54 | -0.54 | +0.00 | -0.54 |
| 2026-08-21 | `CRDL` | 12 | — | $1.93 | +0.00 | $1.86 | -0.84 | -0.84 | +0.00 | -0.84 |
| 2026-08-21 | `CYPH` | 17 | — | $1.32 | +0.00 | $1.42 | +1.70 | +1.70 | +0.00 | +1.70 |
| 2026-08-24 | `AG` | 60 | $21.09 | $21.47 | +22.80 | $20.57 | -54.00 | -31.20 | +55.20 | +1.20 |
| 2026-08-24 | `BHP` | 13 | $97.03 | $97.34 | +4.03 | $96.66 | -8.84 | -4.81 | +82.29 | +73.45 |
| 2026-08-24 | `CDE` | 60 | $20.97 | $21.26 | +17.40 | $20.49 | -46.20 | -28.80 | +36.60 | -9.60 |
| 2026-08-24 | `HDSN` | 216 | $5.63 | $5.69 | +12.96 | $5.57 | -25.92 | -12.96 | -17.28 | -43.20 |
| 2026-08-24 | `IAG` | 63 | $21.14 | $21.44 | +18.90 | $21.36 | -5.04 | +13.86 | +114.03 | +108.99 |
| 2026-08-24 | `KGC` | 42 | $32.76 | $33.21 | +18.90 | $32.47 | -31.08 | -12.18 | +150.36 | +119.28 |
| 2026-08-24 | `NFGC` | 714 | $1.84 | $1.86 | +14.28 | $1.90 | +28.56 | +42.84 | +78.54 | +107.10 |
| 2026-08-24 | `WPM` | 8 | $157.78 | $158.96 | +9.44 | $158.00 | -7.68 | +1.76 | +115.36 | +107.68 |
| 2026-08-24 | `AUPH` | 1 | $16.65 | $16.60 | -0.05 | $16.60 | +0.00 | -0.05 | -0.60 | -0.60 |
| 2026-08-24 | `ARCT` | 2 | $13.45 | $13.26 | -0.38 | $13.76 | +1.00 | +0.62 | +4.26 | +5.26 |
| 2026-08-24 | `AUTL` | 9 | $2.41 | $2.36 | -0.45 | $2.38 | +0.18 | -0.27 | -0.99 | -0.81 |
| 2026-08-24 | `CRDL` | 12 | $1.86 | $1.87 | +0.12 | $1.80 | -0.84 | -0.72 | -0.72 | -1.56 |
| 2026-08-24 | `CYPH` | 17 | $1.42 | $1.83 | +6.97 | $1.64 | -3.23 | +3.74 | +8.67 | +5.44 |
| 2026-08-25 | `AG` | 60 | $20.57 | $20.73 | +9.60 | — | +0.00 | +9.60 | +10.80 | — |
| 2026-08-25 | `BHP` | 13 | $96.66 | $95.95 | -9.23 | — | +0.00 | -9.23 | +64.22 | — |
| 2026-08-25 | `CDE` | 60 | $20.49 | $20.85 | +21.60 | — | +0.00 | +21.60 | +12.00 | — |
| 2026-08-25 | `HDSN` | 216 | $5.57 | $5.53 | -8.64 | — | +0.00 | -8.64 | -51.84 | — |
| 2026-08-25 | `IAG` | 63 | $21.36 | $21.63 | +17.01 | — | +0.00 | +17.01 | +126.00 | — |
| 2026-08-25 | `KGC` | 42 | $32.47 | $32.76 | +12.18 | — | +0.00 | +12.18 | +131.46 | — |
| 2026-08-25 | `NFGC` | 714 | $1.90 | $1.91 | +7.14 | — | +0.00 | +7.14 | +114.24 | — |
| 2026-08-25 | `WPM` | 8 | $158.00 | $160.00 | +16.00 | — | +0.00 | +16.00 | +123.68 | — |
| 2026-08-25 | `AUPH` | 1 | $16.60 | $16.71 | +0.11 | $16.71 | +0.00 | +0.11 | -0.49 | -0.49 |
| 2026-08-25 | `ARCT` | 2 | $13.76 | $14.34 | +1.16 | $14.21 | -0.26 | +0.90 | +6.42 | +6.16 |
| 2026-08-25 | `AUTL` | 9 | $2.38 | $2.32 | -0.54 | $2.34 | +0.18 | -0.36 | -1.35 | -1.17 |
| 2026-08-25 | `CRDL` | 12 | $1.80 | $1.90 | +1.20 | $1.90 | +0.00 | +1.20 | -0.36 | -0.36 |
| 2026-08-25 | `CYPH` | 17 | $1.64 | $1.70 | +1.02 | $1.64 | -1.02 | +0.00 | +6.46 | +5.44 |
| 2026-08-26 | `AUPH` | 1 | $16.71 | $16.71 | +0.00 | $16.71 | +0.00 | +0.00 | -0.49 | -0.49 |
| 2026-08-26 | `ARCT` | 2 | $14.21 | $14.21 | +0.00 | $14.21 | +0.00 | +0.00 | +6.16 | +6.16 |
| 2026-08-26 | `AUTL` | 9 | $2.34 | $2.34 | +0.00 | $2.34 | +0.00 | +0.00 | -1.17 | -1.17 |
| 2026-08-26 | `CRDL` | 12 | $1.90 | $1.90 | +0.00 | $1.90 | +0.00 | +0.00 | -0.36 | -0.36 |
| 2026-08-26 | `CYPH` | 17 | $1.64 | $1.64 | +0.00 | $1.64 | +0.00 | +0.00 | +5.44 | +5.44 |
| 2026-08-27 | `AUPH` | 1 | $16.71 | $16.60 | -0.11 | — | +0.00 | -0.11 | -0.60 | — |
| 2026-08-27 | `ARCT` | 2 | $14.21 | $15.35 | +2.28 | — | +0.00 | +2.28 | +8.44 | — |
| 2026-08-27 | `AUTL` | 9 | $2.34 | $2.41 | +0.63 | — | +0.00 | +0.63 | -0.54 | — |
| 2026-08-27 | `CRDL` | 12 | $1.90 | $2.03 | +1.56 | — | +0.00 | +1.56 | +1.20 | — |
| 2026-08-27 | `CYPH` | 17 | $1.64 | $1.60 | -0.68 | — | +0.00 | -0.68 | +4.76 | — |
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
| 2026-08-20 | +1.12 | $10,000.00 | — | $10,000.00 | +0.00 | +232.95 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $186.91 | $10,208.28 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8 |
| 2026-08-21 | +3.25 | $186.91 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8 | $10,475.50 | +267.22 | +0.63 | AUPH, ARCT, AUTL, CRDL, CYPH | — | $78.42 | $10,474.93 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 |
| 2026-08-24 | -5.17 | $78.42 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 | $10,599.85 | +124.92 | -153.09 | — | — | $78.42 | $10,446.76 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 |
| 2026-08-25 | +1.80 | $78.42 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 | $10,515.37 | +68.61 | -1.10 | — | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $10,372.43 | $10,489.30 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 |
| 2026-08-26 | +2.02 | $10,372.43 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 | $10,489.30 | -0.00 | +0.00 | — | — | $10,372.43 | $10,489.30 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 |
| 2026-08-27 | — | $10,372.43 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×17 | $10,492.98 | +3.68 | +0.00 | — | AUPH, ARCT, AUTL, CRDL, CYPH | $10,491.55 | $10,491.55 | — |
| 2026-08-28 | +0.75 | $10,491.55 | — | $10,491.55 | -0.00 | +0.00 | — | — | $10,491.55 | $10,491.55 | — |
| 2026-08-31 | -5.85 | $10,491.55 | — | $10,491.55 | -0.00 | +0.00 | — | — | $10,491.55 | $10,491.55 | — |
| 2026-09-01 | -6.30 | $10,491.55 | — | $10,491.55 | -0.00 | +0.00 | — | — | $10,491.55 | $10,491.55 | — |
| 2026-09-02 | -3.83 | $10,491.55 | — | $10,491.55 | -0.00 | +0.00 | — | — | $10,491.55 | $10,491.55 | — |
| 2026-09-03 | -0.90 | $10,491.55 | — | $10,491.55 | -0.00 | +0.00 | — | — | $10,491.55 | $10,491.55 | — |
| 2026-09-04 | — | $10,491.55 | — | $10,491.55 | -0.00 | +0.00 | — | — | $10,491.55 | $10,491.55 | — |

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
| 2026-08-20 09:30 ET | **BUY** | `AG` | 60 | $20.55 | $2.17 | — | $8,764.83 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,579.67 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 60 | $20.65 | $2.17 | — | $6,338.50 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 216 | $5.77 | $2.79 | — | $5,089.39 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 63 | $19.63 | $2.18 | — | $3,850.53 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $2,603.95 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 714 | $1.75 | $9.21 | — | $1,345.24 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $186.91 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $186.91 | ▲ close $10,208.28 vs 09:30 $10,000.00 (session +232.95) | 16:00 close · cash $186.91 · equity $10,208.28 vs 09:30 $10,000.00 (+208.28; session marks +232.95) · 8 name(s) marked open→close (per-name table). AG×60 09:30 $20.55 → close $21.19 +38.40; BHP×13 09:30 $91.01 → close $93.63 +34.06; CDE×60 09:30 $20.65 → close $21.11 +27.60; HDSN×216 09:30 $5.77 → close $5.57 -43.20; IAG×63 09:30 $19.63 → close $20.50 +54.81; KGC×42 09:30 $29.63 → close $31.43 +75.60; NFGC×714 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $186.91 | ▲ 09:30 equity $10,475.50 vs yday $10,208.28 (+267.22) | 09:30 open · cash $186.91 (unchanged overnight, no fees) · equity $10,475.50 vs prior close $10,208.28 (+267.22) · 8 name(s) re-marked at the open (per-name table). AG×60 yday $21.19 → 09:30 $21.90 +42.60; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×60 yday $21.11 → 09:30 $21.75 +38.40; HDSN×216 yday $5.57 → 09:30 $5.67 +21.60; IAG×63 yday $20.50 → 09:30 $21.17 +42.21; KGC×42 yday $31.43 → 09:30 $32.17 +31.08; NFGC×714 yday $1.75 → 09:30 $1.79 +28.56; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $169.53 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $147.04 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 9 | $2.47 | $0.25 | — | $124.56 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 12 | $1.93 | $0.27 | — | $101.13 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 17 | $1.32 | $0.28 | — | $78.42 | — | 09:30 tickets only when flatten_robust gate fires (mover); list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $23.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.42 | ▲ close $10,474.93 vs 09:30 $10,475.50 (session +0.63) | 16:00 close · cash $78.42 · equity $10,474.93 vs 09:30 $10,475.50 (-0.57; session marks +0.63) · 13 name(s) marked open→close (per-name table). AG×60 09:30 $21.90 → close $21.09 -48.60; BHP×13 09:30 $95.72 → close $97.03 +17.03; CDE×60 09:30 $21.75 → close $20.97 -46.80; HDSN×216 09:30 $5.67 → close $5.63 -8.64; IAG×63 09:30 $21.17 → close $21.14 -1.89; KGC×42 09:30 $32.17 → close $32.76 +24.78; NFGC×714 09:30 $1.79 → close $1.84 +35.70; WPM×8 09:30 $154.70 → close $157.78 +24.64; AUPH×1 09:30 $17.20 → close $16.65 -0.55; ARCT×2 09:30 $11.13 → close $13.45 +4.64; AUTL×9 09:30 $2.47 → close $2.41 -0.54; CRDL×12 09:30 $1.93 → close $1.86 -0.84; CYPH×17 09:30 $1.32 → close $1.42 +1.70 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.42 | ▲ 09:30 equity $10,599.85 vs yday $10,474.93 (+124.92) | 09:30 open · cash $78.42 (unchanged overnight, no fees) · equity $10,599.85 vs prior close $10,474.93 (+124.92) · 13 name(s) re-marked at the open (per-name table). AG×60 yday $21.09 → 09:30 $21.47 +22.80; BHP×13 yday $97.03 → 09:30 $97.34 +4.03; CDE×60 yday $20.97 → 09:30 $21.26 +17.40; HDSN×216 yday $5.63 → 09:30 $5.69 +12.96; IAG×63 yday $21.14 → 09:30 $21.44 +18.90; KGC×42 yday $32.76 → 09:30 $33.21 +18.90; NFGC×714 yday $1.84 → 09:30 $1.86 +14.28; WPM×8 yday $157.78 → 09:30 $158.96 +9.44; AUPH×1 yday $16.65 → 09:30 $16.60 -0.05; ARCT×2 yday $13.45 → 09:30 $13.26 -0.38; AUTL×9 yday $2.41 → 09:30 $2.36 -0.45; CRDL×12 yday $1.86 → 09:30 $1.87 +0.12; CYPH×17 yday $1.42 → 09:30 $1.83 +6.97 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.42 | ▼ close $10,446.76 vs 09:30 $10,599.85 (session -153.09) | 16:00 close · cash $78.42 · equity $10,446.76 vs 09:30 $10,599.85 (-153.09; session marks -153.09) · 13 name(s) marked open→close (per-name table). AG×60 09:30 $21.47 → close $20.57 -54.00; BHP×13 09:30 $97.34 → close $96.66 -8.84; CDE×60 09:30 $21.26 → close $20.49 -46.20; HDSN×216 09:30 $5.69 → close $5.57 -25.92; IAG×63 09:30 $21.44 → close $21.36 -5.04; KGC×42 09:30 $33.21 → close $32.47 -31.08; NFGC×714 09:30 $1.86 → close $1.90 +28.56; WPM×8 09:30 $158.96 → close $158.00 -7.68; AUPH×1 09:30 $16.60 → close $16.60 +0.00; ARCT×2 09:30 $13.26 → close $13.76 +1.00; AUTL×9 09:30 $2.36 → close $2.38 +0.18; CRDL×12 09:30 $1.87 → close $1.80 -0.84; CYPH×17 09:30 $1.83 → close $1.64 -3.23 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.42 | ▲ 09:30 equity $10,515.37 vs yday $10,446.76 (+68.61) | 09:30 open · cash $78.42 (unchanged overnight, no fees) · equity $10,515.37 vs prior close $10,446.76 (+68.61) · 13 name(s) re-marked at the open (per-name table). AG×60 yday $20.57 → 09:30 $20.73 +9.60; BHP×13 yday $96.66 → 09:30 $95.95 -9.23; CDE×60 yday $20.49 → 09:30 $20.85 +21.60; HDSN×216 yday $5.57 → 09:30 $5.53 -8.64; IAG×63 yday $21.36 → 09:30 $21.63 +17.01; KGC×42 yday $32.47 → 09:30 $32.76 +12.18; NFGC×714 yday $1.90 → 09:30 $1.91 +7.14; WPM×8 yday $158.00 → 09:30 $160.00 +16.00; AUPH×1 yday $16.60 → 09:30 $16.71 +0.11; ARCT×2 yday $13.76 → 09:30 $14.34 +1.16; AUTL×9 yday $2.38 → 09:30 $2.32 -0.54; CRDL×12 yday $1.80 → 09:30 $1.90 +1.20; CYPH×17 yday $1.64 → 09:30 $1.70 +1.02 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 60 | $20.73 | $2.19 | $+6.44 | $1,320.03 | ▲ +6.44 after sell → book $10,513.18; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.95 | $2.05 | $+60.14 | $2,565.33 | ▲ +60.14 after sell → book $10,511.13; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 60 | $20.85 | $2.19 | $+7.64 | $3,814.14 | ▲ +7.64 after sell → book $10,508.94; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 216 | $5.53 | $2.83 | $-57.46 | $5,005.79 | ▼ -57.46 after sell → book $10,506.11; vs 09:30 mark -2.83 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 63 | $21.63 | $2.20 | $+121.62 | $6,366.28 | ▲ +121.62 after sell → book $10,503.91; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 42 | $32.76 | $2.14 | $+127.21 | $7,740.06 | ▲ +127.21 after sell → book $10,501.77; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 714 | $1.91 | $9.34 | $+95.69 | $9,094.46 | ▲ +95.69 after sell → book $10,492.43; vs 09:30 mark -9.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $160.00 | $2.03 | $+119.63 | $10,372.43 | ▲ +119.63 after sell → book $10,490.40; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,372.43 | ▼ close $10,489.30 vs 09:30 $10,515.37 (session -1.10) | 16:00 close · cash $10,372.43 · equity $10,489.30 vs 09:30 $10,515.37 (-26.07; session marks -1.10) · 5 name(s) marked open→close (per-name table). AUPH×1 09:30 $16.71 → close $16.71 +0.00; ARCT×2 09:30 $14.34 → close $14.21 -0.26; AUTL×9 09:30 $2.32 → close $2.34 +0.18; CRDL×12 09:30 $1.90 → close $1.90 +0.00; CYPH×17 09:30 $1.70 → close $1.64 -1.02 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,372.43 | ▲ 09:30 equity $10,489.30 vs yday $10,489.30 (-0.00) | 09:30 open · cash $10,372.43 (unchanged overnight, no fees) · equity $10,489.30 vs prior close $10,489.30 (-0.00) · 5 name(s) re-marked at the open (per-name table). AUPH×1 yday $16.71 → 09:30 $16.71 +0.00; ARCT×2 yday $14.21 → 09:30 $14.21 +0.00; AUTL×9 yday $2.34 → 09:30 $2.34 +0.00; CRDL×12 yday $1.90 → 09:30 $1.90 +0.00; CYPH×17 yday $1.64 → 09:30 $1.64 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,372.43 | ▲ close $10,489.30 vs 09:30 $10,489.30 (session +0.00) | 16:00 close · cash $10,372.43 · equity $10,489.30 vs 09:30 $10,489.30 (-0.00; session marks +0.00) · 5 name(s) marked open→close (per-name table). AUPH×1 09:30 $16.71 → close $16.71 +0.00; ARCT×2 09:30 $14.21 → close $14.21 +0.00; AUTL×9 09:30 $2.34 → close $2.34 +0.00; CRDL×12 09:30 $1.90 → close $1.90 +0.00; CYPH×17 09:30 $1.64 → close $1.64 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,372.43 | ▲ 09:30 equity $10,492.98 vs yday $10,489.30 (+3.68) | 09:30 open · cash $10,372.43 (unchanged overnight, no fees) · equity $10,492.98 vs prior close $10,489.30 (+3.68) · 5 name(s) re-marked at the open (per-name table). AUPH×1 yday $16.71 → 09:30 $16.60 -0.11; ARCT×2 yday $14.21 → 09:30 $15.35 +2.28; AUTL×9 yday $2.34 → 09:30 $2.41 +0.63; CRDL×12 yday $1.90 → 09:30 $2.03 +1.56; CYPH×17 yday $1.64 → 09:30 $1.60 -0.68 | — |
| 2026-08-27 09:30 ET | **SELL** | `AUPH` | 1 | $16.60 | $0.19 | $-0.96 | $10,388.84 | ▼ -0.96 after sell → book $10,492.79; vs 09:30 mark -0.19 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 2 | $15.35 | $0.33 | $+7.88 | $10,419.20 | ▲ +7.88 after sell → book $10,492.45; vs 09:30 mark -0.34 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `AUTL` | 9 | $2.41 | $0.26 | $-1.05 | $10,440.63 | ▼ -1.05 after sell → book $10,492.19; vs 09:30 mark -0.26 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 12 | $2.03 | $0.30 | $+0.63 | $10,464.69 | ▲ +0.63 after sell → book $10,491.89; vs 09:30 mark -0.30 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 17 | $1.60 | $0.34 | $+4.14 | $10,491.55 | ▲ +4.14 after sell → book $10,491.55; vs 09:30 mark -0.34 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,491.55 | ▲ close $10,491.55 vs 09:30 $10,492.98 (session +0.00) | 16:00 close · cash $10,491.55 · no lots left · equity $10,491.55. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,491.55 | ▲ 09:30 equity $10,491.55 vs yday $10,491.55 (-0.00) | 09:30 open · cash $10,491.55 · no holdings · equity $10,491.55 vs prior close $10,491.55 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,491.55 | ▲ close $10,491.55 vs 09:30 $10,491.55 (session +0.00) | 16:00 close · cash $10,491.55 · no lots left · equity $10,491.55. | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,491.55 | ▲ 09:30 equity $10,491.55 vs yday $10,491.55 (-0.00) | 09:30 open · cash $10,491.55 · no holdings · equity $10,491.55 vs prior close $10,491.55 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,491.55 | ▲ close $10,491.55 vs 09:30 $10,491.55 (session +0.00) | 16:00 close · cash $10,491.55 · no lots left · equity $10,491.55. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,491.55 | ▲ 09:30 equity $10,491.55 vs yday $10,491.55 (-0.00) | 09:30 open · cash $10,491.55 · no holdings · equity $10,491.55 vs prior close $10,491.55 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,491.55 | ▲ close $10,491.55 vs 09:30 $10,491.55 (session +0.00) | 16:00 close · cash $10,491.55 · no lots left · equity $10,491.55. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,491.55 | ▲ 09:30 equity $10,491.55 vs yday $10,491.55 (-0.00) | 09:30 open · cash $10,491.55 · no holdings · equity $10,491.55 vs prior close $10,491.55 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,491.55 | ▲ close $10,491.55 vs 09:30 $10,491.55 (session +0.00) | 16:00 close · cash $10,491.55 · no lots left · equity $10,491.55. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,491.55 | ▲ 09:30 equity $10,491.55 vs yday $10,491.55 (-0.00) | 09:30 open · cash $10,491.55 · no holdings · equity $10,491.55 vs prior close $10,491.55 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,491.55 | ▲ close $10,491.55 vs 09:30 $10,491.55 (session +0.00) | 16:00 close · cash $10,491.55 · no lots left · equity $10,491.55. | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,491.55 | ▲ 09:30 equity $10,491.55 vs yday $10,491.55 (-0.00) | 09:30 open · cash $10,491.55 · no holdings · equity $10,491.55 vs prior close $10,491.55 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,491.55 | ▲ close $10,491.55 vs 09:30 $10,491.55 (session +0.00) | 16:00 close · cash $10,491.55 · no lots left · equity $10,491.55. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 23.36 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 23.36 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 23.36 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AUPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ARCT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AUTL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CRDL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
