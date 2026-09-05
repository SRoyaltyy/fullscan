# Factor mine action — `flatten_vol_g_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

Side **long** · universe `flatten` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · flatten wish-list ∩ vol🟢

Cash book **-1.69%** ($9,831) · signal-only (no cash/fees) was +0.40%. Starts YES **14/17**. Fills 34 · skips 43 · realized $-516.65.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `flatten` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `vol=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $2.59.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `BTBT` | 3333 | — | $1.50 | +0.00 | $1.57 | +233.31 | +233.31 | +0.00 | +233.31 |
| 2026-08-14 | `BETR` | 334 | — | $14.80 | +0.00 | $13.73 | -357.38 | -357.38 | +0.00 | -357.38 |
| 2026-08-17 | `BTBT` | 3333 | $1.57 | $1.52 | -166.65 | $1.60 | +266.64 | +99.99 | +66.66 | +333.30 |
| 2026-08-17 | `BETR` | 334 | $13.73 | $13.67 | -20.04 | $13.54 | -43.42 | -63.46 | -377.42 | -420.84 |
| 2026-08-17 | `TMC` | 2 | — | $4.05 | +0.00 | $3.77 | -0.56 | -0.56 | +0.00 | -0.56 |
| 2026-08-18 | `BTBT` | 3333 | $1.60 | $1.54 | -199.98 | $1.45 | -299.97 | -499.95 | +133.32 | -166.65 |
| 2026-08-18 | `BETR` | 334 | $13.54 | $13.21 | -110.22 | $13.05 | -53.44 | -163.66 | -531.06 | -584.50 |
| 2026-08-18 | `TMC` | 2 | $3.77 | $3.72 | -0.10 | $3.92 | +0.40 | +0.30 | -0.66 | -0.26 |
| 2026-08-19 | `BTBT` | 3333 | $1.45 | $1.42 | -99.99 | — | +0.00 | -99.99 | -266.64 | — |
| 2026-08-19 | `BETR` | 334 | $13.05 | $13.03 | -6.68 | — | +0.00 | -6.68 | -591.18 | — |
| 2026-08-19 | `TMC` | 2 | $3.92 | $3.93 | +0.02 | $3.97 | +0.08 | +0.10 | -0.24 | -0.16 |
| 2026-08-20 | `TMC` | 2 | $3.97 | $3.92 | -0.10 | — | +0.00 | -0.10 | -0.26 | — |
| 2026-08-20 | `AG` | 55 | — | $20.55 | +0.00 | $21.19 | +35.20 | +35.20 | +0.00 | +35.20 |
| 2026-08-20 | `BHP` | 12 | — | $91.01 | +0.00 | $93.63 | +31.44 | +31.44 | +0.00 | +31.44 |
| 2026-08-20 | `CDE` | 54 | — | $20.65 | +0.00 | $21.11 | +24.84 | +24.84 | +0.00 | +24.84 |
| 2026-08-20 | `HDSN` | 195 | — | $5.77 | +0.00 | $5.57 | -39.00 | -39.00 | +0.00 | -39.00 |
| 2026-08-20 | `IAG` | 57 | — | $19.63 | +0.00 | $20.50 | +49.59 | +49.59 | +0.00 | +49.59 |
| 2026-08-20 | `KGC` | 38 | — | $29.63 | +0.00 | $31.43 | +68.40 | +68.40 | +0.00 | +68.40 |
| 2026-08-20 | `NFGC` | 646 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 7 | — | $144.54 | +0.00 | $150.25 | +39.97 | +39.97 | +0.00 | +39.97 |
| 2026-08-21 | `AG` | 55 | $21.19 | $21.90 | +39.05 | $21.09 | -44.55 | -5.50 | +74.25 | +29.70 |
| 2026-08-21 | `BHP` | 12 | $93.63 | $95.72 | +25.08 | $97.03 | +15.72 | +40.80 | +56.52 | +72.24 |
| 2026-08-21 | `CDE` | 54 | $21.11 | $21.75 | +34.56 | $20.97 | -42.12 | -7.56 | +59.40 | +17.28 |
| 2026-08-21 | `HDSN` | 195 | $5.57 | $5.67 | +19.50 | $5.63 | -7.80 | +11.70 | -19.50 | -27.30 |
| 2026-08-21 | `IAG` | 57 | $20.50 | $21.17 | +38.19 | $21.14 | -1.71 | +36.48 | +87.78 | +86.07 |
| 2026-08-21 | `KGC` | 38 | $31.43 | $32.17 | +28.12 | $32.76 | +22.42 | +50.54 | +96.52 | +118.94 |
| 2026-08-21 | `NFGC` | 646 | $1.75 | $1.79 | +25.84 | $1.84 | +32.30 | +58.14 | +25.84 | +58.14 |
| 2026-08-21 | `WPM` | 7 | $150.25 | $154.70 | +31.15 | $157.78 | +21.56 | +52.71 | +71.12 | +92.68 |
| 2026-08-21 | `AUPH` | 1 | — | $17.20 | +0.00 | $16.65 | -0.55 | -0.55 | +0.00 | -0.55 |
| 2026-08-21 | `ARCT` | 1 | — | $11.13 | +0.00 | $13.45 | +2.32 | +2.32 | +0.00 | +2.32 |
| 2026-08-21 | `AUTL` | 8 | — | $2.47 | +0.00 | $2.41 | -0.48 | -0.48 | +0.00 | -0.48 |
| 2026-08-21 | `CRDL` | 11 | — | $1.93 | +0.00 | $1.86 | -0.77 | -0.77 | +0.00 | -0.77 |
| 2026-08-21 | `CYPH` | 16 | — | $1.32 | +0.00 | $1.42 | +1.60 | +1.60 | +0.00 | +1.60 |
| 2026-08-24 | `AG` | 55 | $21.09 | $21.47 | +20.90 | $20.57 | -49.50 | -28.60 | +50.60 | +1.10 |
| 2026-08-24 | `BHP` | 12 | $97.03 | $97.34 | +3.72 | $96.66 | -8.16 | -4.44 | +75.96 | +67.80 |
| 2026-08-24 | `CDE` | 54 | $20.97 | $21.26 | +15.66 | $20.49 | -41.58 | -25.92 | +32.94 | -8.64 |
| 2026-08-24 | `HDSN` | 195 | $5.63 | $5.69 | +11.70 | $5.57 | -23.40 | -11.70 | -15.60 | -39.00 |
| 2026-08-24 | `IAG` | 57 | $21.14 | $21.44 | +17.10 | $21.36 | -4.56 | +12.54 | +103.17 | +98.61 |
| 2026-08-24 | `KGC` | 38 | $32.76 | $33.21 | +17.10 | $32.47 | -28.12 | -11.02 | +136.04 | +107.92 |
| 2026-08-24 | `NFGC` | 646 | $1.84 | $1.86 | +12.92 | $1.90 | +25.84 | +38.76 | +71.06 | +96.90 |
| 2026-08-24 | `WPM` | 7 | $157.78 | $158.96 | +8.26 | $158.00 | -6.72 | +1.54 | +100.94 | +94.22 |
| 2026-08-24 | `AUPH` | 1 | $16.65 | $16.60 | -0.05 | $16.60 | +0.00 | -0.05 | -0.60 | -0.60 |
| 2026-08-24 | `ARCT` | 1 | $13.45 | $13.26 | -0.19 | $13.76 | +0.50 | +0.31 | +2.13 | +2.63 |
| 2026-08-24 | `AUTL` | 8 | $2.41 | $2.36 | -0.40 | $2.38 | +0.16 | -0.24 | -0.88 | -0.72 |
| 2026-08-24 | `CRDL` | 11 | $1.86 | $1.87 | +0.11 | $1.80 | -0.77 | -0.66 | -0.66 | -1.43 |
| 2026-08-24 | `CYPH` | 16 | $1.42 | $1.83 | +6.56 | $1.64 | -3.04 | +3.52 | +8.16 | +5.12 |
| 2026-08-25 | `AG` | 55 | $20.57 | $20.73 | +8.80 | — | +0.00 | +8.80 | +9.90 | — |
| 2026-08-25 | `BHP` | 12 | $96.66 | $95.95 | -8.52 | — | +0.00 | -8.52 | +59.28 | — |
| 2026-08-25 | `CDE` | 54 | $20.49 | $20.85 | +19.44 | — | +0.00 | +19.44 | +10.80 | — |
| 2026-08-25 | `HDSN` | 195 | $5.57 | $5.53 | -7.80 | — | +0.00 | -7.80 | -46.80 | — |
| 2026-08-25 | `IAG` | 57 | $21.36 | $21.63 | +15.39 | — | +0.00 | +15.39 | +114.00 | — |
| 2026-08-25 | `KGC` | 38 | $32.47 | $32.76 | +11.02 | — | +0.00 | +11.02 | +118.94 | — |
| 2026-08-25 | `NFGC` | 646 | $1.90 | $1.91 | +6.46 | — | +0.00 | +6.46 | +103.36 | — |
| 2026-08-25 | `WPM` | 7 | $158.00 | $160.00 | +14.00 | — | +0.00 | +14.00 | +108.22 | — |
| 2026-08-25 | `AUPH` | 1 | $16.60 | $16.71 | +0.11 | $16.71 | +0.00 | +0.11 | -0.49 | -0.49 |
| 2026-08-25 | `ARCT` | 1 | $13.76 | $14.34 | +0.58 | $14.21 | -0.13 | +0.45 | +3.21 | +3.08 |
| 2026-08-25 | `AUTL` | 8 | $2.38 | $2.32 | -0.48 | $2.34 | +0.16 | -0.32 | -1.20 | -1.04 |
| 2026-08-25 | `CRDL` | 11 | $1.80 | $1.90 | +1.10 | $1.90 | +0.00 | +1.10 | -0.33 | -0.33 |
| 2026-08-25 | `CYPH` | 16 | $1.64 | $1.70 | +0.96 | $1.64 | -0.96 | +0.00 | +6.08 | +5.12 |
| 2026-08-26 | `AUPH` | 1 | $16.71 | $16.71 | +0.00 | $16.71 | +0.00 | +0.00 | -0.49 | -0.49 |
| 2026-08-26 | `ARCT` | 1 | $14.21 | $14.21 | +0.00 | $14.21 | +0.00 | +0.00 | +3.08 | +3.08 |
| 2026-08-26 | `AUTL` | 8 | $2.34 | $2.34 | +0.00 | $2.34 | +0.00 | +0.00 | -1.04 | -1.04 |
| 2026-08-26 | `CRDL` | 11 | $1.90 | $1.90 | +0.00 | $1.90 | +0.00 | +0.00 | -0.33 | -0.33 |
| 2026-08-26 | `CYPH` | 16 | $1.64 | $1.64 | +0.00 | $1.64 | +0.00 | +0.00 | +5.12 | +5.12 |
| 2026-08-27 | `AUPH` | 1 | $16.71 | $16.60 | -0.11 | — | +0.00 | -0.11 | -0.60 | — |
| 2026-08-27 | `ARCT` | 1 | $14.21 | $15.35 | +1.14 | — | +0.00 | +1.14 | +4.22 | — |
| 2026-08-27 | `AUTL` | 8 | $2.34 | $2.41 | +0.56 | — | +0.00 | +0.56 | -0.48 | — |
| 2026-08-27 | `CRDL` | 11 | $1.90 | $2.03 | +1.43 | — | +0.00 | +1.43 | +1.10 | — |
| 2026-08-27 | `CYPH` | 16 | $1.64 | $1.60 | -0.64 | — | +0.00 | -0.64 | +4.48 | — |
| 2026-08-28 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-31 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `RVTY` | 75 | — | $125.94 | +0.00 | $130.94 | +375.00 | +375.00 | +0.00 | +375.00 |
| 2026-09-04 | `RVTY` | 75 | $130.94 | $132.45 | +113.25 | $130.63 | -136.50 | -23.25 | +488.25 | +351.75 |
| 2026-09-04 | `CABA` | 9 | — | $3.63 | +0.00 | $3.48 | -1.35 | -1.35 | +0.00 | -1.35 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -124.07 | BTBT, BETR | — | $10.00 | $9,828.63 | BTBT×3333, BETR×334 |
| 2026-08-17 | +2.25 | $10.00 | BTBT×3333, BETR×334 | $9,641.94 | -186.69 | +222.66 | TMC | — | $1.81 | $9,864.51 | BTBT×3333, BETR×334, TMC×2 |
| 2026-08-18 | -6.20 | $1.81 | BTBT×3333, BETR×334, TMC×2 | $9,554.21 | -310.30 | -353.01 | — | — | $1.81 | $9,201.20 | BTBT×3333, BETR×334, TMC×2 |
| 2026-08-19 | -7.20 | $1.81 | BTBT×3333, BETR×334, TMC×2 | $9,094.55 | -106.65 | +0.08 | — | BTBT, BETR | $9,038.70 | $9,046.64 | TMC×2 |
| 2026-08-20 | +1.12 | $9,038.70 | TMC×2 | $9,046.54 | -0.10 | +210.44 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | TMC | $173.17 | $9,233.36 | AG×55, BHP×12, CDE×54, HDSN×195, IAG×57, KGC×38, NFGC×646, WPM×7 |
| 2026-08-21 | +3.25 | $173.17 | AG×55, BHP×12, CDE×54, HDSN×195, IAG×57, KGC×38, NFGC×646, WPM×7 | $9,474.85 | +241.49 | -2.06 | AUPH, ARCT, AUTL, CRDL, CYPH | — | $81.72 | $9,471.78 | AG×55, BHP×12, CDE×54, HDSN×195, IAG×57, KGC×38, NFGC×646, WPM×7, AUPH×1, ARCT×1, AUTL×8, CRDL×11, CYPH×16 |
| 2026-08-24 | -5.17 | $81.72 | AG×55, BHP×12, CDE×54, HDSN×195, IAG×57, KGC×38, NFGC×646, WPM×7, AUPH×1, ARCT×1, AUTL×8, CRDL×11, CYPH×16 | $9,585.17 | +113.39 | -139.35 | — | — | $81.72 | $9,445.82 | AG×55, BHP×12, CDE×54, HDSN×195, IAG×57, KGC×38, NFGC×646, WPM×7, AUPH×1, ARCT×1, AUTL×8, CRDL×11, CYPH×16 |
| 2026-08-25 | +1.80 | $81.72 | AG×55, BHP×12, CDE×54, HDSN×195, IAG×57, KGC×38, NFGC×646, WPM×7, AUPH×1, ARCT×1, AUTL×8, CRDL×11, CYPH×16 | $9,506.88 | +61.06 | -0.93 | — | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $9,385.37 | $9,482.15 | AUPH×1, ARCT×1, AUTL×8, CRDL×11, CYPH×16 |
| 2026-08-26 | +2.02 | $9,385.37 | AUPH×1, ARCT×1, AUTL×8, CRDL×11, CYPH×16 | $9,482.15 | -0.00 | +0.00 | — | — | $9,385.37 | $9,482.15 | AUPH×1, ARCT×1, AUTL×8, CRDL×11, CYPH×16 |
| 2026-08-27 | — | $9,385.37 | AUPH×1, ARCT×1, AUTL×8, CRDL×11, CYPH×16 | $9,484.53 | +2.38 | +0.00 | — | AUPH, ARCT, AUTL, CRDL, CYPH | $9,483.33 | $9,483.33 | — |
| 2026-08-28 | +0.75 | $9,483.33 | — | $9,483.33 | -0.00 | +0.00 | — | — | $9,483.33 | $9,483.33 | — |
| 2026-08-31 | -5.85 | $9,483.33 | — | $9,483.33 | -0.00 | +0.00 | — | — | $9,483.33 | $9,483.33 | — |
| 2026-09-01 | -6.30 | $9,483.33 | — | $9,483.33 | -0.00 | +0.00 | — | — | $9,483.33 | $9,483.33 | — |
| 2026-09-02 | -3.83 | $9,483.33 | — | $9,483.33 | -0.00 | +0.00 | — | — | $9,483.33 | $9,483.33 | — |
| 2026-09-03 | -0.90 | $9,483.33 | — | $9,483.33 | -0.00 | +375.00 | RVTY | — | $35.61 | $9,856.11 | RVTY×75 |
| 2026-09-04 | — | $35.61 | RVTY×75 | $9,969.36 | +113.25 | -137.85 | CABA | — | $2.59 | $9,831.16 | RVTY×75, CABA×9 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 3333 | $1.50 | $43.00 | — | $4,957.50 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $5000.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 334 | $14.80 | $4.31 | — | $10.00 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-9.9; leftover $5000.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.00 | ▼ close $9,828.63 vs 09:30 $10,000.00 (session -124.07) | 16:00 close · cash $10.00 · equity $9,828.63 vs 09:30 $10,000.00 (-171.37; session marks -124.07) · 2 name(s) marked open→close (per-name table). BTBT×3333 09:30 $1.50 → close $1.57 +233.31; BETR×334 09:30 $14.80 → close $13.73 -357.38 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.00 | ▼ 09:30 equity $9,641.94 vs yday $9,828.63 (-186.69) | 09:30 open · cash $10.00 (unchanged overnight, no fees) · equity $9,641.94 vs prior close $9,828.63 (-186.69) · 2 name(s) re-marked at the open (per-name table). BTBT×3333 yday $1.57 → 09:30 $1.52 -166.65; BETR×334 yday $13.73 → 09:30 $13.67 -20.04 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 2 | $4.05 | $0.09 | — | $1.81 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; leftover $10.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.81 | ▲ close $9,864.51 vs 09:30 $9,641.94 (session +222.66) | 16:00 close · cash $1.81 · equity $9,864.51 vs 09:30 $9,641.94 (+222.57; session marks +222.66) · 3 name(s) marked open→close (per-name table). BTBT×3333 09:30 $1.52 → close $1.60 +266.64; BETR×334 09:30 $13.67 → close $13.54 -43.42; TMC×2 09:30 $4.05 → close $3.77 -0.56 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.81 | ▼ 09:30 equity $9,554.21 vs yday $9,864.51 (-310.30) | 09:30 open · cash $1.81 (unchanged overnight, no fees) · equity $9,554.21 vs prior close $9,864.51 (-310.30) · 3 name(s) re-marked at the open (per-name table). BTBT×3333 yday $1.60 → 09:30 $1.54 -199.98; BETR×334 yday $13.54 → 09:30 $13.21 -110.22; TMC×2 yday $3.77 → 09:30 $3.72 -0.10 | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.81 | ▼ close $9,201.20 vs 09:30 $9,554.21 (session -353.01) | 16:00 close · cash $1.81 · equity $9,201.20 vs 09:30 $9,554.21 (-353.01; session marks -353.01) · 3 name(s) marked open→close (per-name table). BTBT×3333 09:30 $1.54 → close $1.45 -299.97; BETR×334 09:30 $13.21 → close $13.05 -53.44; TMC×2 09:30 $3.72 → close $3.92 +0.40 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.81 | ▼ 09:30 equity $9,094.55 vs yday $9,201.20 (-106.65) | 09:30 open · cash $1.81 (unchanged overnight, no fees) · equity $9,094.55 vs prior close $9,201.20 (-106.65) · 3 name(s) re-marked at the open (per-name table). BTBT×3333 yday $1.45 → 09:30 $1.42 -99.99; BETR×334 yday $13.05 → 09:30 $13.03 -6.68; TMC×2 yday $3.92 → 09:30 $3.93 +0.02 | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 3333 | $1.42 | $43.59 | $-353.22 | $4,691.08 | ▼ -353.22 after sell → book $9,050.96; vs 09:30 mark -43.59 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BETR` | 334 | $13.03 | $4.40 | $-599.89 | $9,038.70 | ▼ -599.89 after sell → book $9,046.56; vs 09:30 mark -4.40 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,038.70 | ▲ close $9,046.64 vs 09:30 $9,094.55 (session +0.08) | 16:00 close · cash $9,038.70 · equity $9,046.64 vs 09:30 $9,094.55 (-47.91; session marks +0.08) · 1 name(s) marked open→close (per-name table). TMC×2 09:30 $3.93 → close $3.97 +0.08 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,038.70 | ▼ 09:30 equity $9,046.54 vs yday $9,046.64 (-0.10) | 09:30 open · cash $9,038.70 (unchanged overnight, no fees) · equity $9,046.54 vs prior close $9,046.64 (-0.10) · 1 name(s) re-marked at the open (per-name table). TMC×2 yday $3.97 → 09:30 $3.92 -0.10 | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 2 | $3.92 | $0.10 | $-0.45 | $9,046.44 | ▼ -0.45 after sell → book $9,046.44; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 55 | $20.55 | $2.15 | — | $7,914.03 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $1130.80 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 12 | $91.01 | $2.03 | — | $6,819.89 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $1130.80 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 54 | $20.65 | $2.15 | — | $5,702.64 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $1130.80 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 195 | $5.77 | $2.58 | — | $4,574.91 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $1130.80 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 57 | $19.63 | $2.16 | — | $3,453.84 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $1130.80 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 38 | $29.63 | $2.10 | — | $2,325.80 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $1130.80 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 646 | $1.75 | $8.33 | — | $1,186.96 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $1130.80 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 7 | $144.54 | $2.01 | — | $173.17 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $1130.80 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $173.17 | ▲ close $9,233.36 vs 09:30 $9,046.54 (session +210.44) | 16:00 close · cash $173.17 · equity $9,233.36 vs 09:30 $9,046.54 (+186.82; session marks +210.44) · 8 name(s) marked open→close (per-name table). AG×55 09:30 $20.55 → close $21.19 +35.20; BHP×12 09:30 $91.01 → close $93.63 +31.44; CDE×54 09:30 $20.65 → close $21.11 +24.84; HDSN×195 09:30 $5.77 → close $5.57 -39.00; IAG×57 09:30 $19.63 → close $20.50 +49.59; KGC×38 09:30 $29.63 → close $31.43 +68.40; NFGC×646 09:30 $1.75 → close $1.75 +0.00; WPM×7 09:30 $144.54 → close $150.25 +39.97 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $173.17 | ▲ 09:30 equity $9,474.85 vs yday $9,233.36 (+241.49) | 09:30 open · cash $173.17 (unchanged overnight, no fees) · equity $9,474.85 vs prior close $9,233.36 (+241.49) · 8 name(s) re-marked at the open (per-name table). AG×55 yday $21.19 → 09:30 $21.90 +39.05; BHP×12 yday $93.63 → 09:30 $95.72 +25.08; CDE×54 yday $21.11 → 09:30 $21.75 +34.56; HDSN×195 yday $5.57 → 09:30 $5.67 +19.50; IAG×57 yday $20.50 → 09:30 $21.17 +38.19; KGC×38 yday $31.43 → 09:30 $32.17 +28.12; NFGC×646 yday $1.75 → 09:30 $1.79 +25.84; WPM×7 yday $150.25 → 09:30 $154.70 +31.15 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $155.80 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $21.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $144.55 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $21.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 8 | $2.47 | $0.22 | — | $124.57 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $21.65 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 11 | $1.93 | $0.25 | — | $103.09 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $21.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 16 | $1.32 | $0.26 | — | $81.72 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $21.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $81.72 | ▼ close $9,471.78 vs 09:30 $9,474.85 (session -2.06) | 16:00 close · cash $81.72 · equity $9,471.78 vs 09:30 $9,474.85 (-3.07; session marks -2.06) · 13 name(s) marked open→close (per-name table). AG×55 09:30 $21.90 → close $21.09 -44.55; BHP×12 09:30 $95.72 → close $97.03 +15.72; CDE×54 09:30 $21.75 → close $20.97 -42.12; HDSN×195 09:30 $5.67 → close $5.63 -7.80; IAG×57 09:30 $21.17 → close $21.14 -1.71; KGC×38 09:30 $32.17 → close $32.76 +22.42; NFGC×646 09:30 $1.79 → close $1.84 +32.30; WPM×7 09:30 $154.70 → close $157.78 +21.56; AUPH×1 09:30 $17.20 → close $16.65 -0.55; ARCT×1 09:30 $11.13 → close $13.45 +2.32; AUTL×8 09:30 $2.47 → close $2.41 -0.48; CRDL×11 09:30 $1.93 → close $1.86 -0.77; CYPH×16 09:30 $1.32 → close $1.42 +1.60 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $81.72 | ▲ 09:30 equity $9,585.17 vs yday $9,471.78 (+113.39) | 09:30 open · cash $81.72 (unchanged overnight, no fees) · equity $9,585.17 vs prior close $9,471.78 (+113.39) · 13 name(s) re-marked at the open (per-name table). AG×55 yday $21.09 → 09:30 $21.47 +20.90; BHP×12 yday $97.03 → 09:30 $97.34 +3.72; CDE×54 yday $20.97 → 09:30 $21.26 +15.66; HDSN×195 yday $5.63 → 09:30 $5.69 +11.70; IAG×57 yday $21.14 → 09:30 $21.44 +17.10; KGC×38 yday $32.76 → 09:30 $33.21 +17.10; NFGC×646 yday $1.84 → 09:30 $1.86 +12.92; WPM×7 yday $157.78 → 09:30 $158.96 +8.26; AUPH×1 yday $16.65 → 09:30 $16.60 -0.05; ARCT×1 yday $13.45 → 09:30 $13.26 -0.19; AUTL×8 yday $2.41 → 09:30 $2.36 -0.40; CRDL×11 yday $1.86 → 09:30 $1.87 +0.11; CYPH×16 yday $1.42 → 09:30 $1.83 +6.56 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $81.72 | ▼ close $9,445.82 vs 09:30 $9,585.17 (session -139.35) | 16:00 close · cash $81.72 · equity $9,445.82 vs 09:30 $9,585.17 (-139.35; session marks -139.35) · 13 name(s) marked open→close (per-name table). AG×55 09:30 $21.47 → close $20.57 -49.50; BHP×12 09:30 $97.34 → close $96.66 -8.16; CDE×54 09:30 $21.26 → close $20.49 -41.58; HDSN×195 09:30 $5.69 → close $5.57 -23.40; IAG×57 09:30 $21.44 → close $21.36 -4.56; KGC×38 09:30 $33.21 → close $32.47 -28.12; NFGC×646 09:30 $1.86 → close $1.90 +25.84; WPM×7 09:30 $158.96 → close $158.00 -6.72; AUPH×1 09:30 $16.60 → close $16.60 +0.00; ARCT×1 09:30 $13.26 → close $13.76 +0.50; AUTL×8 09:30 $2.36 → close $2.38 +0.16; CRDL×11 09:30 $1.87 → close $1.80 -0.77; CYPH×16 09:30 $1.83 → close $1.64 -3.04 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $81.72 | ▲ 09:30 equity $9,506.88 vs yday $9,445.82 (+61.06) | 09:30 open · cash $81.72 (unchanged overnight, no fees) · equity $9,506.88 vs prior close $9,445.82 (+61.06) · 13 name(s) re-marked at the open (per-name table). AG×55 yday $20.57 → 09:30 $20.73 +8.80; BHP×12 yday $96.66 → 09:30 $95.95 -8.52; CDE×54 yday $20.49 → 09:30 $20.85 +19.44; HDSN×195 yday $5.57 → 09:30 $5.53 -7.80; IAG×57 yday $21.36 → 09:30 $21.63 +15.39; KGC×38 yday $32.47 → 09:30 $32.76 +11.02; NFGC×646 yday $1.90 → 09:30 $1.91 +6.46; WPM×7 yday $158.00 → 09:30 $160.00 +14.00; AUPH×1 yday $16.60 → 09:30 $16.71 +0.11; ARCT×1 yday $13.76 → 09:30 $14.34 +0.58; AUTL×8 yday $2.38 → 09:30 $2.32 -0.48; CRDL×11 yday $1.80 → 09:30 $1.90 +1.10; CYPH×16 yday $1.64 → 09:30 $1.70 +0.96 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 55 | $20.73 | $2.17 | $+5.57 | $1,219.69 | ▲ +5.57 after sell → book $9,504.70; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 12 | $95.95 | $2.05 | $+55.21 | $2,369.04 | ▲ +55.21 after sell → book $9,502.65; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 54 | $20.85 | $2.17 | $+6.48 | $3,492.77 | ▲ +6.48 after sell → book $9,500.48; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 195 | $5.53 | $2.62 | $-51.99 | $4,568.51 | ▼ -51.99 after sell → book $9,497.87; vs 09:30 mark -2.61 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 57 | $21.63 | $2.18 | $+109.66 | $5,799.23 | ▲ +109.66 after sell → book $9,495.68; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 38 | $32.76 | $2.12 | $+114.71 | $7,041.99 | ▲ +114.71 after sell → book $9,493.56; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 646 | $1.91 | $8.45 | $+86.58 | $8,267.40 | ▲ +86.58 after sell → book $9,485.11; vs 09:30 mark -8.45 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 7 | $160.00 | $2.03 | $+104.18 | $9,385.37 | ▲ +104.18 after sell → book $9,483.08; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,385.37 | ▼ close $9,482.15 vs 09:30 $9,506.88 (session -0.93) | 16:00 close · cash $9,385.37 · equity $9,482.15 vs 09:30 $9,506.88 (-24.73; session marks -0.93) · 5 name(s) marked open→close (per-name table). AUPH×1 09:30 $16.71 → close $16.71 +0.00; ARCT×1 09:30 $14.34 → close $14.21 -0.13; AUTL×8 09:30 $2.32 → close $2.34 +0.16; CRDL×11 09:30 $1.90 → close $1.90 +0.00; CYPH×16 09:30 $1.70 → close $1.64 -0.96 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,385.37 | ▲ 09:30 equity $9,482.15 vs yday $9,482.15 (-0.00) | 09:30 open · cash $9,385.37 (unchanged overnight, no fees) · equity $9,482.15 vs prior close $9,482.15 (-0.00) · 5 name(s) re-marked at the open (per-name table). AUPH×1 yday $16.71 → 09:30 $16.71 +0.00; ARCT×1 yday $14.21 → 09:30 $14.21 +0.00; AUTL×8 yday $2.34 → 09:30 $2.34 +0.00; CRDL×11 yday $1.90 → 09:30 $1.90 +0.00; CYPH×16 yday $1.64 → 09:30 $1.64 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,385.37 | ▲ close $9,482.15 vs 09:30 $9,482.15 (session +0.00) | 16:00 close · cash $9,385.37 · equity $9,482.15 vs 09:30 $9,482.15 (-0.00; session marks +0.00) · 5 name(s) marked open→close (per-name table). AUPH×1 09:30 $16.71 → close $16.71 +0.00; ARCT×1 09:30 $14.21 → close $14.21 +0.00; AUTL×8 09:30 $2.34 → close $2.34 +0.00; CRDL×11 09:30 $1.90 → close $1.90 +0.00; CYPH×16 09:30 $1.64 → close $1.64 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,385.37 | ▲ 09:30 equity $9,484.53 vs yday $9,482.15 (+2.38) | 09:30 open · cash $9,385.37 (unchanged overnight, no fees) · equity $9,484.53 vs prior close $9,482.15 (+2.38) · 5 name(s) re-marked at the open (per-name table). AUPH×1 yday $16.71 → 09:30 $16.60 -0.11; ARCT×1 yday $14.21 → 09:30 $15.35 +1.14; AUTL×8 yday $2.34 → 09:30 $2.41 +0.56; CRDL×11 yday $1.90 → 09:30 $2.03 +1.43; CYPH×16 yday $1.64 → 09:30 $1.60 -0.64 | — |
| 2026-08-27 09:30 ET | **SELL** | `AUPH` | 1 | $16.60 | $0.19 | $-0.96 | $9,401.78 | ▼ -0.96 after sell → book $9,484.34; vs 09:30 mark -0.19 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 1 | $15.35 | $0.18 | $+3.93 | $9,416.95 | ▲ +3.93 after sell → book $9,484.16; vs 09:30 mark -0.18 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `AUTL` | 8 | $2.41 | $0.24 | $-0.94 | $9,436.00 | ▼ -0.94 after sell → book $9,483.93; vs 09:30 mark -0.23 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 11 | $2.03 | $0.28 | $+0.58 | $9,458.05 | ▲ +0.58 after sell → book $9,483.65; vs 09:30 mark -0.28 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 16 | $1.60 | $0.32 | $+3.90 | $9,483.33 | ▲ +3.90 after sell → book $9,483.33; vs 09:30 mark -0.32 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,483.33 | ▲ close $9,483.33 vs 09:30 $9,484.53 (session +0.00) | 16:00 close · cash $9,483.33 · no lots left · equity $9,483.33. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,483.33 | ▲ 09:30 equity $9,483.33 vs yday $9,483.33 (-0.00) | 09:30 open · cash $9,483.33 · no holdings · equity $9,483.33 vs prior close $9,483.33 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,483.33 | ▲ close $9,483.33 vs 09:30 $9,483.33 (session +0.00) | 16:00 close · cash $9,483.33 · no lots left · equity $9,483.33. | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,483.33 | ▲ 09:30 equity $9,483.33 vs yday $9,483.33 (-0.00) | 09:30 open · cash $9,483.33 · no holdings · equity $9,483.33 vs prior close $9,483.33 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,483.33 | ▲ close $9,483.33 vs 09:30 $9,483.33 (session +0.00) | 16:00 close · cash $9,483.33 · no lots left · equity $9,483.33. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,483.33 | ▲ 09:30 equity $9,483.33 vs yday $9,483.33 (-0.00) | 09:30 open · cash $9,483.33 · no holdings · equity $9,483.33 vs prior close $9,483.33 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,483.33 | ▲ close $9,483.33 vs 09:30 $9,483.33 (session +0.00) | 16:00 close · cash $9,483.33 · no lots left · equity $9,483.33. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,483.33 | ▲ 09:30 equity $9,483.33 vs yday $9,483.33 (-0.00) | 09:30 open · cash $9,483.33 · no holdings · equity $9,483.33 vs prior close $9,483.33 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,483.33 | ▲ close $9,483.33 vs 09:30 $9,483.33 (session +0.00) | 16:00 close · cash $9,483.33 · no lots left · equity $9,483.33. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,483.33 | ▲ 09:30 equity $9,483.33 vs yday $9,483.33 (-0.00) | 09:30 open · cash $9,483.33 · no holdings · equity $9,483.33 vs prior close $9,483.33 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 75 | $125.94 | $2.21 | — | $35.61 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+6.8; leftover $9483.33 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.61 | ▲ close $9,856.11 vs 09:30 $9,483.33 (session +375.00) | 16:00 close · cash $35.61 · equity $9,856.11 vs 09:30 $9,483.33 (+372.78; session marks +375.00) · 1 name(s) marked open→close (per-name table). RVTY×75 09:30 $125.94 → close $130.94 +375.00 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.61 | ▲ 09:30 equity $9,969.36 vs yday $9,856.11 (+113.25) | 09:30 open · cash $35.61 (unchanged overnight, no fees) · equity $9,969.36 vs prior close $9,856.11 (+113.25) · 1 name(s) re-marked at the open (per-name table). RVTY×75 yday $130.94 → 09:30 $132.45 +113.25 | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 9 | $3.63 | $0.35 | — | $2.59 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+13.8; leftover $35.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.59 | ▼ close $9,831.16 vs 09:30 $9,969.36 (session -137.85) | 16:00 close · cash $2.59 · equity $9,831.16 vs 09:30 $9,969.36 (-138.20; session marks -137.85) · 2 name(s) marked open→close (per-name table). RVTY×75 09:30 $132.45 → close $130.63 -136.50; CABA×9 09:30 $3.63 → close $3.48 -1.35 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BETR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BETR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 21.65 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 21.65 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 21.65 < 1 share @ 59.72 |
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
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
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
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `RVTY` | 75 | 2026-09-03 @ $125.94 | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+6.8; leftover $9483.33 |
| `CABA` | 9 | 2026-09-04 @ $3.63 | flatten wish-list ∩ vol🟢; gate vol=good; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+13.8; leftover $35.61 |
