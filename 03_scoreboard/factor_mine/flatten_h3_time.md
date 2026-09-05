# Factor mine action — `flatten_h3_time`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

Side **long** · universe `flatten` · top 8 · rank `list` · size `leftover` · sell `time` · S-boost `none` · sell at min-hold even if still listed

Cash book **+11.56%** ($11,156) · signal-only (no cash/fees) was +44.29%. Starts YES **16/17**. Fills 80 · skips 129 · realized $+735.17.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `flatten` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `time` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $33.10.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `BTSG` | 20 | — | $59.80 | +0.00 | $60.23 | +8.60 | +8.60 | +0.00 | +8.60 |
| 2026-08-13 | `IREN` | 27 | — | $45.98 | +0.00 | $44.76 | -32.94 | -32.94 | +0.00 | -32.94 |
| 2026-08-13 | `TPG` | 24 | — | $50.62 | +0.00 | $54.62 | +95.92 | +95.92 | +0.00 | +95.92 |
| 2026-08-13 | `TGTX` | 25 | — | $49.70 | +0.00 | $47.94 | -44.00 | -44.00 | +0.00 | -44.00 |
| 2026-08-13 | `SLS` | 106 | — | $11.70 | +0.00 | $12.36 | +69.96 | +69.96 | +0.00 | +69.96 |
| 2026-08-13 | `HIMS` | 42 | — | $29.74 | +0.00 | $28.77 | -40.74 | -40.74 | +0.00 | -40.74 |
| 2026-08-13 | `INO` | 1543 | — | $0.81 | +0.00 | $0.90 | +138.87 | +138.87 | +0.00 | +138.87 |
| 2026-08-13 | `TNDM` | 53 | — | $23.33 | +0.00 | $23.13 | -10.60 | -10.60 | +0.00 | -10.60 |
| 2026-08-14 | `BTSG` | 20 | $60.23 | $59.65 | -11.60 | $61.71 | +41.20 | +29.60 | -3.00 | +38.20 |
| 2026-08-14 | `IREN` | 27 | $44.76 | $44.09 | -18.09 | $44.06 | -0.81 | -18.90 | -51.03 | -51.84 |
| 2026-08-14 | `TPG` | 24 | $54.62 | $55.29 | +16.08 | $53.03 | -54.24 | -38.16 | +112.00 | +57.76 |
| 2026-08-14 | `TGTX` | 25 | $47.94 | $47.27 | -16.75 | $48.74 | +36.75 | +20.00 | -60.75 | -24.00 |
| 2026-08-14 | `SLS` | 106 | $12.36 | $12.40 | +4.24 | $12.78 | +40.28 | +44.52 | +74.20 | +114.48 |
| 2026-08-14 | `HIMS` | 42 | $28.77 | $29.15 | +15.96 | $28.15 | -42.00 | -26.04 | -24.78 | -66.78 |
| 2026-08-14 | `INO` | 1543 | $0.90 | $0.93 | +46.29 | $1.09 | +246.88 | +293.17 | +185.16 | +432.04 |
| 2026-08-14 | `TNDM` | 53 | $23.13 | $22.92 | -11.13 | $22.72 | -10.60 | -21.73 | -21.73 | -32.33 |
| 2026-08-14 | `MARA` | 1 | — | $9.01 | +0.00 | $9.20 | +0.19 | +0.19 | +0.00 | +0.19 |
| 2026-08-14 | `LDI` | 13 | — | $0.94 | +0.00 | $0.90 | -0.52 | -0.52 | +0.00 | -0.52 |
| 2026-08-14 | `BTBT` | 8 | — | $1.50 | +0.00 | $1.57 | +0.56 | +0.56 | +0.00 | +0.56 |
| 2026-08-17 | `BTSG` | 20 | $61.71 | $61.69 | -0.40 | $60.38 | -26.20 | -26.60 | +37.80 | +11.60 |
| 2026-08-17 | `IREN` | 27 | $44.06 | $45.23 | +31.59 | $44.90 | -8.91 | +22.68 | -20.25 | -29.16 |
| 2026-08-17 | `TPG` | 24 | $53.03 | $52.67 | -8.64 | $51.77 | -21.60 | -30.24 | +49.12 | +27.52 |
| 2026-08-17 | `TGTX` | 25 | $48.74 | $48.74 | +0.00 | $49.28 | +13.50 | +13.50 | -24.00 | -10.50 |
| 2026-08-17 | `SLS` | 106 | $12.78 | $12.78 | +0.00 | $13.00 | +23.32 | +23.32 | +114.48 | +137.80 |
| 2026-08-17 | `HIMS` | 42 | $28.15 | $28.14 | -0.42 | $28.61 | +19.74 | +19.32 | -67.20 | -47.46 |
| 2026-08-17 | `INO` | 1543 | $1.09 | $1.07 | -30.86 | $1.15 | +123.44 | +92.58 | +401.18 | +524.62 |
| 2026-08-17 | `TNDM` | 53 | $22.72 | $22.50 | -11.66 | $22.25 | -12.99 | -24.65 | -43.99 | -56.97 |
| 2026-08-17 | `MARA` | 1 | $9.20 | $9.22 | +0.02 | $9.72 | +0.50 | +0.52 | +0.21 | +0.71 |
| 2026-08-17 | `LDI` | 13 | $0.90 | $0.91 | +0.13 | $0.88 | -0.42 | -0.29 | -0.39 | -0.81 |
| 2026-08-17 | `BTBT` | 8 | $1.57 | $1.52 | -0.40 | $1.60 | +0.64 | +0.24 | +0.16 | +0.80 |
| 2026-08-17 | `TMC` | 1 | — | $4.05 | +0.00 | $3.77 | -0.28 | -0.28 | +0.00 | -0.28 |
| 2026-08-17 | `DNN` | 2 | — | $3.24 | +0.00 | $3.19 | -0.10 | -0.10 | +0.00 | -0.10 |
| 2026-08-17 | `HNST` | 1 | — | $4.81 | +0.00 | $4.70 | -0.11 | -0.11 | +0.00 | -0.11 |
| 2026-08-18 | `BTSG` | 20 | $60.38 | $60.00 | -7.60 | — | +0.00 | -7.60 | +4.00 | — |
| 2026-08-18 | `IREN` | 27 | $44.90 | $43.56 | -36.18 | — | +0.00 | -36.18 | -65.34 | — |
| 2026-08-18 | `TPG` | 24 | $51.77 | $51.77 | +0.00 | — | +0.00 | +0.00 | +27.52 | — |
| 2026-08-18 | `TGTX` | 25 | $49.28 | $49.28 | +0.00 | — | +0.00 | +0.00 | -10.50 | — |
| 2026-08-18 | `SLS` | 106 | $13.00 | $12.66 | -36.04 | — | +0.00 | -36.04 | +101.76 | — |
| 2026-08-18 | `HIMS` | 42 | $28.61 | $27.85 | -31.92 | — | +0.00 | -31.92 | -79.38 | — |
| 2026-08-18 | `INO` | 1543 | $1.15 | $1.14 | -15.43 | — | +0.00 | -15.43 | +509.19 | — |
| 2026-08-18 | `TNDM` | 53 | $22.25 | $22.16 | -5.03 | — | +0.00 | -5.03 | -62.01 | — |
| 2026-08-18 | `MARA` | 1 | $9.72 | $9.36 | -0.36 | $8.96 | -0.40 | -0.76 | +0.35 | -0.05 |
| 2026-08-18 | `LDI` | 13 | $0.88 | $0.87 | -0.07 | $0.86 | -0.16 | -0.23 | -0.87 | -1.03 |
| 2026-08-18 | `BTBT` | 8 | $1.60 | $1.54 | -0.48 | $1.45 | -0.72 | -1.20 | +0.32 | -0.40 |
| 2026-08-18 | `TMC` | 1 | $3.77 | $3.72 | -0.05 | $3.92 | +0.20 | +0.15 | -0.33 | -0.13 |
| 2026-08-18 | `DNN` | 2 | $3.19 | $3.11 | -0.16 | $3.15 | +0.08 | -0.08 | -0.26 | -0.18 |
| 2026-08-18 | `HNST` | 1 | $4.70 | $4.67 | -0.03 | $4.75 | +0.08 | +0.05 | -0.14 | -0.06 |
| 2026-08-19 | `MARA` | 1 | $8.96 | $8.91 | -0.05 | — | +0.00 | -0.05 | -0.10 | — |
| 2026-08-19 | `LDI` | 13 | $0.86 | $0.88 | +0.29 | — | +0.00 | +0.29 | -0.74 | — |
| 2026-08-19 | `BTBT` | 8 | $1.45 | $1.42 | -0.24 | — | +0.00 | -0.24 | -0.64 | — |
| 2026-08-19 | `TMC` | 1 | $3.92 | $3.93 | +0.01 | $3.97 | +0.04 | +0.05 | -0.12 | -0.08 |
| 2026-08-19 | `DNN` | 2 | $3.15 | $3.19 | +0.08 | $3.22 | +0.06 | +0.14 | -0.10 | -0.04 |
| 2026-08-19 | `HNST` | 1 | $4.75 | $4.80 | +0.05 | $5.02 | +0.22 | +0.27 | -0.01 | +0.21 |
| 2026-08-20 | `TMC` | 1 | $3.97 | $3.92 | -0.05 | — | +0.00 | -0.05 | -0.13 | — |
| 2026-08-20 | `DNN` | 2 | $3.22 | $3.20 | -0.04 | — | +0.00 | -0.04 | -0.08 | — |
| 2026-08-20 | `HNST` | 1 | $5.02 | $4.98 | -0.04 | — | +0.00 | -0.04 | +0.17 | — |
| 2026-08-20 | `AG` | 62 | — | $20.55 | +0.00 | $21.19 | +39.68 | +39.68 | +0.00 | +39.68 |
| 2026-08-20 | `BHP` | 14 | — | $91.01 | +0.00 | $93.63 | +36.68 | +36.68 | +0.00 | +36.68 |
| 2026-08-20 | `CDE` | 62 | — | $20.65 | +0.00 | $21.11 | +28.52 | +28.52 | +0.00 | +28.52 |
| 2026-08-20 | `HDSN` | 224 | — | $5.77 | +0.00 | $5.57 | -44.80 | -44.80 | +0.00 | -44.80 |
| 2026-08-20 | `IAG` | 65 | — | $19.63 | +0.00 | $20.50 | +56.55 | +56.55 | +0.00 | +56.55 |
| 2026-08-20 | `KGC` | 43 | — | $29.63 | +0.00 | $31.43 | +77.40 | +77.40 | +0.00 | +77.40 |
| 2026-08-20 | `NFGC` | 739 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-21 | `AG` | 62 | $21.19 | $21.90 | +44.02 | $21.09 | -50.22 | -6.20 | +83.70 | +33.48 |
| 2026-08-21 | `BHP` | 14 | $93.63 | $95.72 | +29.26 | $97.03 | +18.34 | +47.60 | +65.94 | +84.28 |
| 2026-08-21 | `CDE` | 62 | $21.11 | $21.75 | +39.68 | $20.97 | -48.36 | -8.68 | +68.20 | +19.84 |
| 2026-08-21 | `HDSN` | 224 | $5.57 | $5.67 | +22.40 | $5.63 | -8.96 | +13.44 | -22.40 | -31.36 |
| 2026-08-21 | `IAG` | 65 | $20.50 | $21.17 | +43.55 | $21.14 | -1.95 | +41.60 | +100.10 | +98.15 |
| 2026-08-21 | `KGC` | 43 | $31.43 | $32.17 | +31.82 | $32.76 | +25.37 | +57.19 | +109.22 | +134.59 |
| 2026-08-21 | `NFGC` | 739 | $1.75 | $1.79 | +29.56 | $1.84 | +36.95 | +66.51 | +29.56 | +66.51 |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | $157.78 | +24.64 | +60.24 | +81.28 | +105.92 |
| 2026-08-21 | `AUPH` | 1 | — | $17.20 | +0.00 | $16.65 | -0.55 | -0.55 | +0.00 | -0.55 |
| 2026-08-21 | `ARCT` | 2 | — | $11.13 | +0.00 | $13.45 | +4.64 | +4.64 | +0.00 | +4.64 |
| 2026-08-21 | `AUTL` | 10 | — | $2.47 | +0.00 | $2.41 | -0.60 | -0.60 | +0.00 | -0.60 |
| 2026-08-21 | `CRDL` | 13 | — | $1.93 | +0.00 | $1.86 | -0.91 | -0.91 | +0.00 | -0.91 |
| 2026-08-21 | `CYPH` | 19 | — | $1.32 | +0.00 | $1.42 | +1.90 | +1.90 | +0.00 | +1.90 |
| 2026-08-24 | `AG` | 62 | $21.09 | $21.47 | +23.56 | $20.57 | -55.80 | -32.24 | +57.04 | +1.24 |
| 2026-08-24 | `BHP` | 14 | $97.03 | $97.34 | +4.34 | $96.66 | -9.52 | -5.18 | +88.62 | +79.10 |
| 2026-08-24 | `CDE` | 62 | $20.97 | $21.26 | +17.98 | $20.49 | -47.74 | -29.76 | +37.82 | -9.92 |
| 2026-08-24 | `HDSN` | 224 | $5.63 | $5.69 | +13.44 | $5.57 | -26.88 | -13.44 | -17.92 | -44.80 |
| 2026-08-24 | `IAG` | 65 | $21.14 | $21.44 | +19.50 | $21.36 | -5.20 | +14.30 | +117.65 | +112.45 |
| 2026-08-24 | `KGC` | 43 | $32.76 | $33.21 | +19.35 | $32.47 | -31.82 | -12.47 | +153.94 | +122.12 |
| 2026-08-24 | `NFGC` | 739 | $1.84 | $1.86 | +14.78 | $1.90 | +29.56 | +44.34 | +81.29 | +110.85 |
| 2026-08-24 | `WPM` | 8 | $157.78 | $158.96 | +9.44 | $158.00 | -7.68 | +1.76 | +115.36 | +107.68 |
| 2026-08-24 | `AUPH` | 1 | $16.65 | $16.60 | -0.05 | $16.60 | +0.00 | -0.05 | -0.60 | -0.60 |
| 2026-08-24 | `ARCT` | 2 | $13.45 | $13.26 | -0.38 | $13.76 | +1.00 | +0.62 | +4.26 | +5.26 |
| 2026-08-24 | `AUTL` | 10 | $2.41 | $2.36 | -0.50 | $2.38 | +0.20 | -0.30 | -1.10 | -0.90 |
| 2026-08-24 | `CRDL` | 13 | $1.86 | $1.87 | +0.13 | $1.80 | -0.91 | -0.78 | -0.78 | -1.69 |
| 2026-08-24 | `CYPH` | 19 | $1.42 | $1.83 | +7.79 | $1.64 | -3.61 | +4.18 | +9.69 | +6.08 |
| 2026-08-25 | `AG` | 62 | $20.57 | $20.73 | +9.92 | — | +0.00 | +9.92 | +11.16 | — |
| 2026-08-25 | `BHP` | 14 | $96.66 | $95.95 | -9.94 | — | +0.00 | -9.94 | +69.16 | — |
| 2026-08-25 | `CDE` | 62 | $20.49 | $20.85 | +22.32 | — | +0.00 | +22.32 | +12.40 | — |
| 2026-08-25 | `HDSN` | 224 | $5.57 | $5.53 | -8.96 | — | +0.00 | -8.96 | -53.76 | — |
| 2026-08-25 | `IAG` | 65 | $21.36 | $21.63 | +17.55 | — | +0.00 | +17.55 | +130.00 | — |
| 2026-08-25 | `KGC` | 43 | $32.47 | $32.76 | +12.47 | — | +0.00 | +12.47 | +134.59 | — |
| 2026-08-25 | `NFGC` | 739 | $1.90 | $1.91 | +7.39 | — | +0.00 | +7.39 | +118.24 | — |
| 2026-08-25 | `WPM` | 8 | $158.00 | $160.00 | +16.00 | — | +0.00 | +16.00 | +123.68 | — |
| 2026-08-25 | `AUPH` | 1 | $16.60 | $16.71 | +0.11 | $16.71 | +0.00 | +0.11 | -0.49 | -0.49 |
| 2026-08-25 | `ARCT` | 2 | $13.76 | $14.34 | +1.16 | $14.21 | -0.26 | +0.90 | +6.42 | +6.16 |
| 2026-08-25 | `AUTL` | 10 | $2.38 | $2.32 | -0.60 | $2.34 | +0.20 | -0.40 | -1.50 | -1.30 |
| 2026-08-25 | `CRDL` | 13 | $1.80 | $1.90 | +1.30 | $1.90 | +0.00 | +1.30 | -0.39 | -0.39 |
| 2026-08-25 | `CYPH` | 19 | $1.64 | $1.70 | +1.14 | $1.64 | -1.14 | +0.00 | +7.22 | +6.08 |
| 2026-08-25 | `MOS` | 74 | — | $24.00 | +0.00 | $23.75 | -18.50 | -18.50 | +0.00 | -18.50 |
| 2026-08-25 | `OCUL` | 163 | — | $10.92 | +0.00 | $10.92 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `INSP` | 29 | — | $61.47 | +0.00 | $61.47 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `CRMD` | 216 | — | $8.28 | +0.00 | $8.28 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `RZLT` | 342 | — | $5.23 | +0.00 | $5.29 | +20.52 | +20.52 | +0.00 | +20.52 |
| 2026-08-25 | `HCA` | 4 | — | $429.24 | +0.00 | $428.50 | -2.96 | -2.96 | +0.00 | -2.96 |
| 2026-08-26 | `AUPH` | 1 | $16.71 | $16.71 | +0.00 | $16.71 | +0.00 | +0.00 | -0.49 | -0.49 |
| 2026-08-26 | `ARCT` | 2 | $14.21 | $14.21 | +0.00 | $14.21 | +0.00 | +0.00 | +6.16 | +6.16 |
| 2026-08-26 | `AUTL` | 10 | $2.34 | $2.34 | +0.00 | $2.34 | +0.00 | +0.00 | -1.30 | -1.30 |
| 2026-08-26 | `CRDL` | 13 | $1.90 | $1.90 | +0.00 | $1.90 | +0.00 | +0.00 | -0.39 | -0.39 |
| 2026-08-26 | `CYPH` | 19 | $1.64 | $1.64 | +0.00 | $1.64 | +0.00 | +0.00 | +6.08 | +6.08 |
| 2026-08-26 | `MOS` | 74 | $23.75 | $23.75 | +0.00 | $23.75 | +0.00 | +0.00 | -18.50 | -18.50 |
| 2026-08-26 | `OCUL` | 163 | $10.92 | $10.92 | +0.00 | $10.92 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `INSP` | 29 | $61.47 | $61.47 | +0.00 | $61.47 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `CRMD` | 216 | $8.28 | $8.28 | +0.00 | $8.28 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `RZLT` | 342 | $5.29 | $5.29 | +0.00 | $5.29 | +0.00 | +0.00 | +20.52 | +20.52 |
| 2026-08-26 | `HCA` | 4 | $428.50 | $428.50 | +0.00 | $428.50 | +0.00 | +0.00 | -2.96 | -2.96 |
| 2026-08-27 | `AUPH` | 1 | $16.71 | $16.60 | -0.11 | — | +0.00 | -0.11 | -0.60 | — |
| 2026-08-27 | `ARCT` | 2 | $14.21 | $15.35 | +2.28 | — | +0.00 | +2.28 | +8.44 | — |
| 2026-08-27 | `AUTL` | 10 | $2.34 | $2.41 | +0.70 | — | +0.00 | +0.70 | -0.60 | — |
| 2026-08-27 | `CRDL` | 13 | $1.90 | $2.03 | +1.69 | — | +0.00 | +1.69 | +1.30 | — |
| 2026-08-27 | `CYPH` | 19 | $1.64 | $1.60 | -0.76 | — | +0.00 | -0.76 | +5.32 | — |
| 2026-08-27 | `MOS` | 74 | $23.75 | $24.84 | +80.66 | $24.16 | -50.32 | +30.34 | +62.16 | +11.84 |
| 2026-08-27 | `OCUL` | 163 | $10.92 | $10.79 | -21.19 | $10.77 | -3.26 | -24.45 | -21.19 | -24.45 |
| 2026-08-27 | `INSP` | 29 | $61.47 | $60.07 | -40.60 | $61.80 | +50.17 | +9.57 | -40.60 | +9.57 |
| 2026-08-27 | `CRMD` | 216 | $8.28 | $8.60 | +69.12 | $8.39 | -45.36 | +23.76 | +69.12 | +23.76 |
| 2026-08-27 | `RZLT` | 342 | $5.29 | $5.01 | -95.76 | $5.04 | +10.26 | -85.50 | -75.24 | -64.98 |
| 2026-08-27 | `HCA` | 4 | $428.50 | $427.50 | -4.00 | $427.16 | -1.36 | -5.36 | -6.96 | -8.32 |
| 2026-08-27 | `RRC` | 1 | — | $40.72 | +0.00 | $41.55 | +0.83 | +0.83 | +0.00 | +0.83 |
| 2026-08-27 | `CRK` | 5 | — | $14.09 | +0.00 | $14.50 | +2.05 | +2.05 | +0.00 | +2.05 |
| 2026-08-27 | `SLI` | 27 | — | $2.59 | +0.00 | $2.61 | +0.54 | +0.54 | +0.00 | +0.54 |
| 2026-08-28 | `MOS` | 440 | $24.16 | $24.00 | -11.84 | $23.76 | -105.60 | -117.44 | +0.00 | -105.60 |
| 2026-08-28 | `OCUL` | 163 | $10.77 | $10.63 | -22.82 | — | +0.00 | -22.82 | -47.27 | — |
| 2026-08-28 | `INSP` | 29 | $61.80 | $62.10 | +8.70 | — | +0.00 | +8.70 | +18.27 | — |
| 2026-08-28 | `CRMD` | 216 | $8.39 | $8.49 | +21.60 | — | +0.00 | +21.60 | +45.36 | — |
| 2026-08-28 | `RZLT` | 342 | $5.04 | $5.07 | +10.26 | — | +0.00 | +10.26 | -54.72 | — |
| 2026-08-28 | `HCA` | 4 | $427.16 | $424.61 | -10.20 | — | +0.00 | -10.20 | -18.52 | — |
| 2026-08-28 | `RRC` | 1 | $41.55 | $41.44 | -0.11 | $41.64 | +0.20 | +0.09 | +0.72 | +0.92 |
| 2026-08-28 | `CRK` | 5 | $14.50 | $14.42 | -0.40 | $14.62 | +1.00 | +0.60 | +1.65 | +2.65 |
| 2026-08-28 | `SLI` | 27 | $2.61 | $2.60 | -0.27 | $2.64 | +1.08 | +0.81 | +0.27 | +1.35 |
| 2026-08-31 | `RRC` | 1 | $41.64 | $41.11 | -0.53 | $41.78 | +0.67 | +0.14 | +0.39 | +1.06 |
| 2026-08-31 | `CRK` | 5 | $14.62 | $14.56 | -0.30 | $14.51 | -0.25 | -0.55 | +2.35 | +2.10 |
| 2026-08-31 | `SLI` | 27 | $2.64 | $2.51 | -3.51 | $2.51 | +0.00 | -3.51 | -2.16 | -2.16 |
| 2026-08-31 | `MOS` | 440 | $23.76 | $23.75 | -4.40 | $23.78 | +13.20 | +8.80 | -110.00 | -96.80 |
| 2026-09-01 | `RRC` | 1 | $41.78 | $41.32 | -0.46 | — | +0.00 | -0.46 | +0.60 | — |
| 2026-09-01 | `CRK` | 5 | $14.51 | $14.31 | -1.00 | — | +0.00 | -1.00 | +1.10 | — |
| 2026-09-01 | `SLI` | 27 | $2.51 | $2.70 | +5.13 | — | +0.00 | +5.13 | +2.97 | — |
| 2026-09-01 | `MOS` | 440 | $23.78 | $24.00 | +96.80 | $24.25 | +110.00 | +206.80 | +0.00 | +110.00 |
| 2026-09-02 | `MOS` | 440 | $24.25 | $23.94 | -136.40 | — | +0.00 | -136.40 | -26.40 | — |
| 2026-09-03 | `ATRC` | 43 | — | $49.76 | +0.00 | $52.59 | +121.69 | +121.69 | +0.00 | +121.69 |
| 2026-09-03 | `HRMY` | 51 | — | $41.31 | +0.00 | $42.86 | +79.05 | +79.05 | +0.00 | +79.05 |
| 2026-09-03 | `CABA` | 656 | — | $3.27 | +0.00 | $3.57 | +196.80 | +196.80 | +0.00 | +196.80 |
| 2026-09-03 | `VSTM` | 278 | — | $7.70 | +0.00 | $8.02 | +88.96 | +88.96 | +0.00 | +88.96 |
| 2026-09-03 | `RVTY` | 17 | — | $125.94 | +0.00 | $130.94 | +85.00 | +85.00 | +0.00 | +85.00 |
| 2026-09-04 | `ATRC` | 43 | $52.59 | $52.88 | +12.47 | $52.46 | -18.06 | -5.59 | +134.16 | +116.10 |
| 2026-09-04 | `HRMY` | 51 | $42.86 | $42.93 | +3.57 | $41.86 | -54.57 | -51.00 | +82.62 | +28.05 |
| 2026-09-04 | `CABA` | 656 | $3.57 | $3.63 | +39.36 | $3.48 | -98.40 | -59.04 | +236.16 | +137.76 |
| 2026-09-04 | `VSTM` | 278 | $8.02 | $8.03 | +2.78 | $7.98 | -13.90 | -11.12 | +91.74 | +77.84 |
| 2026-09-04 | `RVTY` | 17 | $130.94 | $132.45 | +25.67 | $130.63 | -30.94 | -5.27 | +110.67 | +79.73 |
| 2026-09-04 | `NVAX` | 1 | — | $10.41 | +0.00 | $10.34 | -0.07 | -0.07 | +0.00 | -0.07 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +185.07 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $97.53 | $10,153.12 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 |
| 2026-08-14 | +5.50 | $97.53 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | $10,178.12 | +25.00 | +257.69 | MARA, LDI, BTBT | — | $63.95 | $10,435.42 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8 |
| 2026-08-17 | +2.25 | $63.95 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8 | $10,414.78 | -20.64 | +110.53 | TMC, DNN, HNST | — | $48.44 | $10,525.15 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8, TMC×1, DNN×2, HNST×1 |
| 2026-08-18 | -6.20 | $48.44 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8, TMC×1, DNN×2, HNST×1 | $10,391.80 | -133.35 | -0.92 | — | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $10,309.06 | $10,355.74 | MARA×1, LDI×13, BTBT×8, TMC×1, DNN×2, HNST×1 |
| 2026-08-19 | -7.20 | $10,309.06 | MARA×1, LDI×13, BTBT×8, TMC×1, DNN×2, HNST×1 | $10,355.88 | +0.14 | +0.32 | — | MARA, LDI, BTBT | $10,340.32 | $10,355.75 | TMC×1, DNN×2, HNST×1 |
| 2026-08-20 | +1.12 | $10,340.32 | TMC×1, DNN×2, HNST×1 | $10,355.62 | -0.13 | +239.71 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | TMC, DNN, HNST | $209.64 | $10,569.98 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8 |
| 2026-08-21 | +3.25 | $209.64 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8 | $10,845.87 | +275.89 | +0.29 | AUPH, ARCT, AUTL, CRDL, CYPH | — | $94.04 | $10,844.89 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19 |
| 2026-08-24 | -5.17 | $94.04 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19 | $10,974.27 | +129.38 | -158.40 | — | — | $94.04 | $10,815.87 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19 |
| 2026-08-25 | +1.80 | $94.04 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19 | $10,885.73 | +69.86 | -2.14 | MOS, OCUL, INSP, CRMD, RZLT, HCA | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $86.05 | $10,842.19 | AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19, MOS×74, OCUL×163, INSP×29, CRMD×216, RZLT×342, HCA×4 |
| 2026-08-26 | +2.02 | $86.05 | AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19, MOS×74, OCUL×163, INSP×29, CRMD×216, RZLT×342, HCA×4 | $10,842.19 | -0.00 | +0.00 | — | — | $86.05 | $10,842.19 | AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19, MOS×74, OCUL×163, INSP×29, CRMD×216, RZLT×342, HCA×4 |
| 2026-08-27 | — | $86.05 | AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19, MOS×74, OCUL×163, INSP×29, CRMD×216, RZLT×342, HCA×4 | $10,834.22 | -7.97 | -36.45 | RRC, CRK, SLI | AUPH, ARCT, AUTL, CRDL, CYPH | $29.71 | $10,794.34 | MOS×74, OCUL×163, INSP×29, CRMD×216, RZLT×342, HCA×4, RRC×1, CRK×5, SLI×27 |
| 2026-08-28 | +0.75 | $29.71 | MOS×74, OCUL×163, INSP×29, CRMD×216, RZLT×342, HCA×4, RRC×1, CRK×5, SLI×27 | $10,789.26 | -5.08 | -103.32 | MOS | MOS, OCUL, INSP, CRMD, RZLT, HCA | $23.64 | $10,664.06 | RRC×1, CRK×5, SLI×27, MOS×440 |
| 2026-08-31 | -5.85 | $23.64 | RRC×1, CRK×5, SLI×27, MOS×440 | $10,655.32 | -8.74 | +13.62 | — | — | $23.64 | $10,668.94 | RRC×1, CRK×5, SLI×27, MOS×440 |
| 2026-09-01 | -6.30 | $23.64 | RRC×1, CRK×5, SLI×27, MOS×440 | $10,769.41 | +100.47 | +110.00 | — | RRC, CRK, SLI | $207.39 | $10,877.39 | MOS×440 |
| 2026-09-02 | -3.83 | $207.39 | MOS×440 | $10,740.99 | -136.40 | +0.00 | — | MOS | $10,735.16 | $10,735.16 | — |
| 2026-09-03 | -0.90 | $10,735.16 | — | $10,735.16 | -0.00 | +571.50 | ATRC, HRMY, CABA, VSTM, RVTY | — | $43.62 | $11,288.31 | ATRC×43, HRMY×51, CABA×656, VSTM×278, RVTY×17 |
| 2026-09-04 | — | $43.62 | ATRC×43, HRMY×51, CABA×656, VSTM×278, RVTY×17 | $11,372.16 | +83.85 | -215.94 | NVAX | — | $33.10 | $11,156.11 | ATRC×43, HRMY×51, CABA×656, VSTM×278, RVTY×17, NVAX×1 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.53 | ▲ close $10,153.12 vs 09:30 $10,000.00 (session +185.07) | 16:00 close · cash $97.53 · equity $10,153.12 vs 09:30 $10,000.00 (+153.12; session marks +185.07) · 8 name(s) marked open→close (per-name table). BTSG×20 09:30 $59.80 → close $60.23 +8.60; IREN×27 09:30 $45.98 → close $44.76 -32.94; TPG×24 09:30 $50.62 → close $54.62 +95.92; TGTX×25 09:30 $49.70 → close $47.94 -44.00; SLS×106 09:30 $11.70 → close $12.36 +69.96; HIMS×42 09:30 $29.74 → close $28.77 -40.74; INO×1543 09:30 $0.81 → close $0.90 +138.87; TNDM×53 09:30 $23.33 → close $23.13 -10.60 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,178.12 vs prior close $10,153.12 (+25.00) · 8 name(s) re-marked at the open (per-name table). BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $88.43 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-13.5; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 13 | $0.94 | $0.16 | — | $76.09 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.5; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 8 | $1.50 | $0.14 | — | $63.95 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.95 | ▲ close $10,435.42 vs 09:30 $10,178.12 (session +257.69) | 16:00 close · cash $63.95 · equity $10,435.42 vs 09:30 $10,178.12 (+257.30; session marks +257.69) · 11 name(s) marked open→close (per-name table). BTSG×20 09:30 $59.65 → close $61.71 +41.20; IREN×27 09:30 $44.09 → close $44.06 -0.81; TPG×24 09:30 $55.29 → close $53.03 -54.24; TGTX×25 09:30 $47.27 → close $48.74 +36.75; SLS×106 09:30 $12.40 → close $12.78 +40.28; HIMS×42 09:30 $29.15 → close $28.15 -42.00; INO×1543 09:30 $0.93 → close $1.09 +246.88; TNDM×53 09:30 $22.92 → close $22.72 -10.60; MARA×1 09:30 $9.01 → close $9.20 +0.19; LDI×13 09:30 $0.94 → close $0.90 -0.52; BTBT×8 09:30 $1.50 → close $1.57 +0.56 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.95 | ▼ 09:30 equity $10,414.78 vs yday $10,435.42 (-20.64) | 09:30 open · cash $63.95 (unchanged overnight, no fees) · equity $10,414.78 vs prior close $10,435.42 (-20.64) · 11 name(s) re-marked at the open (per-name table). BTSG×20 yday $61.71 → 09:30 $61.69 -0.40; IREN×27 yday $44.06 → 09:30 $45.23 +31.59; TPG×24 yday $53.03 → 09:30 $52.67 -8.64; TGTX×25 yday $48.74 → 09:30 $48.74 +0.00; SLS×106 yday $12.78 → 09:30 $12.78 +0.00; HIMS×42 yday $28.15 → 09:30 $28.14 -0.42; INO×1543 yday $1.09 → 09:30 $1.07 -30.86; TNDM×53 yday $22.72 → 09:30 $22.50 -11.66; MARA×1 yday $9.20 → 09:30 $9.22 +0.02; LDI×13 yday $0.90 → 09:30 $0.91 +0.13; BTBT×8 yday $1.57 → 09:30 $1.52 -0.40 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 1 | $4.05 | $0.04 | — | $59.85 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; leftover $7.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 2 | $3.24 | $0.07 | — | $53.30 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; leftover $7.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 1 | $4.81 | $0.05 | — | $48.44 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-11.4; leftover $7.99 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.44 | ▲ close $10,525.15 vs 09:30 $10,414.78 (session +110.53) | 16:00 close · cash $48.44 · equity $10,525.15 vs 09:30 $10,414.78 (+110.37; session marks +110.53) · 14 name(s) marked open→close (per-name table). BTSG×20 09:30 $61.69 → close $60.38 -26.20; IREN×27 09:30 $45.23 → close $44.90 -8.91; TPG×24 09:30 $52.67 → close $51.77 -21.60; TGTX×25 09:30 $48.74 → close $49.28 +13.50; SLS×106 09:30 $12.78 → close $13.00 +23.32; HIMS×42 09:30 $28.14 → close $28.61 +19.74; INO×1543 09:30 $1.07 → close $1.15 +123.44; TNDM×53 09:30 $22.50 → close $22.25 -12.99; MARA×1 09:30 $9.22 → close $9.72 +0.50; LDI×13 09:30 $0.91 → close $0.88 -0.42; BTBT×8 09:30 $1.52 → close $1.60 +0.64; TMC×1 09:30 $4.05 → close $3.77 -0.28; DNN×2 09:30 $3.24 → close $3.19 -0.10; HNST×1 09:30 $4.81 → close $4.70 -0.11 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.44 | ▼ 09:30 equity $10,391.80 vs yday $10,525.15 (-133.35) | 09:30 open · cash $48.44 (unchanged overnight, no fees) · equity $10,391.80 vs prior close $10,525.15 (-133.35) · 14 name(s) re-marked at the open (per-name table). BTSG×20 yday $60.38 → 09:30 $60.00 -7.60; IREN×27 yday $44.90 → 09:30 $43.56 -36.18; TPG×24 yday $51.77 → 09:30 $51.77 +0.00; TGTX×25 yday $49.28 → 09:30 $49.28 +0.00; SLS×106 yday $13.00 → 09:30 $12.66 -36.04; HIMS×42 yday $28.61 → 09:30 $27.85 -31.92; INO×1543 yday $1.15 → 09:30 $1.14 -15.43; TNDM×53 yday $22.25 → 09:30 $22.16 -5.03; MARA×1 yday $9.72 → 09:30 $9.36 -0.36; LDI×13 yday $0.88 → 09:30 $0.87 -0.07; BTBT×8 yday $1.60 → 09:30 $1.54 -0.48; TMC×1 yday $3.77 → 09:30 $3.72 -0.05; DNN×2 yday $3.19 → 09:30 $3.11 -0.16; HNST×1 yday $4.70 → 09:30 $4.67 -0.03 | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 20 | $60.00 | $2.07 | $-0.12 | $1,246.37 | ▼ -0.12 after sell → book $10,389.73; vs 09:30 mark -2.07 | time-stop after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 27 | $43.56 | $2.09 | $-69.50 | $2,420.40 | ▼ -69.50 after sell → book $10,387.64; vs 09:30 mark -2.09 | time-stop after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 24 | $51.77 | $2.08 | $+23.38 | $3,660.80 | ▲ +23.38 after sell → book $10,385.56; vs 09:30 mark -2.08 | time-stop after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 25 | $49.28 | $2.08 | $-14.65 | $4,890.71 | ▼ -14.65 after sell → book $10,383.47; vs 09:30 mark -2.09 | time-stop after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 106 | $12.66 | $2.34 | $+97.12 | $6,230.34 | ▲ +97.12 after sell → book $10,381.14; vs 09:30 mark -2.33 | time-stop after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 42 | $27.85 | $2.14 | $-83.63 | $7,397.90 | ▼ -83.63 after sell → book $10,379.00; vs 09:30 mark -2.14 | time-stop after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1543 | $1.14 | $20.17 | $+471.89 | $9,136.75 | ▲ +471.89 after sell → book $10,358.83; vs 09:30 mark -20.17 | time-stop after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 53 | $22.16 | $2.17 | $-66.33 | $10,309.06 | ▼ -66.33 after sell → book $10,356.66; vs 09:30 mark -2.17 | time-stop after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,309.06 | ▼ close $10,355.74 vs 09:30 $10,391.80 (session -0.92) | 16:00 close · cash $10,309.06 · equity $10,355.74 vs 09:30 $10,391.80 (-36.06; session marks -0.92) · 6 name(s) marked open→close (per-name table). MARA×1 09:30 $9.36 → close $8.96 -0.40; LDI×13 09:30 $0.87 → close $0.86 -0.16; BTBT×8 09:30 $1.54 → close $1.45 -0.72; TMC×1 09:30 $3.72 → close $3.92 +0.20; DNN×2 09:30 $3.11 → close $3.15 +0.08; HNST×1 09:30 $4.67 → close $4.75 +0.08 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,309.06 | ▲ 09:30 equity $10,355.88 vs yday $10,355.74 (+0.14) | 09:30 open · cash $10,309.06 (unchanged overnight, no fees) · equity $10,355.88 vs prior close $10,355.74 (+0.14) · 6 name(s) re-marked at the open (per-name table). MARA×1 yday $8.96 → 09:30 $8.91 -0.05; LDI×13 yday $0.86 → 09:30 $0.88 +0.29; BTBT×8 yday $1.45 → 09:30 $1.42 -0.24; TMC×1 yday $3.92 → 09:30 $3.93 +0.01; DNN×2 yday $3.15 → 09:30 $3.19 +0.08; HNST×1 yday $4.75 → 09:30 $4.80 +0.05 | — |
| 2026-08-19 09:30 ET | **SELL** | `MARA` | 1 | $8.91 | $0.11 | $-0.31 | $10,317.85 | ▼ -0.31 after sell → book $10,355.76; vs 09:30 mark -0.12 | time-stop after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 13 | $0.88 | $0.17 | $-1.08 | $10,329.12 | ▼ -1.08 after sell → book $10,355.59; vs 09:30 mark -0.17 | time-stop after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 8 | $1.42 | $0.16 | $-0.94 | $10,340.32 | ▼ -0.94 after sell → book $10,355.43; vs 09:30 mark -0.16 | time-stop after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,340.32 | ▲ close $10,355.75 vs 09:30 $10,355.88 (session +0.32) | 16:00 close · cash $10,340.32 · equity $10,355.75 vs 09:30 $10,355.88 (-0.13; session marks +0.32) · 3 name(s) marked open→close (per-name table). TMC×1 09:30 $3.93 → close $3.97 +0.04; DNN×2 09:30 $3.19 → close $3.22 +0.06; HNST×1 09:30 $4.80 → close $5.02 +0.22 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,340.32 | ▼ 09:30 equity $10,355.62 vs yday $10,355.75 (-0.13) | 09:30 open · cash $10,340.32 (unchanged overnight, no fees) · equity $10,355.62 vs prior close $10,355.75 (-0.13) · 3 name(s) re-marked at the open (per-name table). TMC×1 yday $3.97 → 09:30 $3.92 -0.05; DNN×2 yday $3.22 → 09:30 $3.20 -0.04; HNST×1 yday $5.02 → 09:30 $4.98 -0.04 | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 1 | $3.92 | $0.06 | $-0.24 | $10,344.18 | ▼ -0.24 after sell → book $10,355.56; vs 09:30 mark -0.06 | time-stop after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 2 | $3.20 | $0.09 | $-0.24 | $10,350.49 | ▼ -0.24 after sell → book $10,355.47; vs 09:30 mark -0.09 | time-stop after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `HNST` | 1 | $4.98 | $0.07 | $+0.05 | $10,355.40 | ▲ +0.05 after sell → book $10,355.40; vs 09:30 mark -0.07 | time-stop after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 62 | $20.55 | $2.18 | — | $9,079.12 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $1294.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $7,802.95 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $1294.42 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 62 | $20.65 | $2.18 | — | $6,520.47 | — | sell at min-hold even if still listed; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $1294.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 224 | $5.77 | $2.89 | — | $5,225.10 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $1294.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 65 | $19.63 | $2.19 | — | $3,946.97 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $1294.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 43 | $29.63 | $2.12 | — | $2,670.76 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $1294.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 739 | $1.75 | $9.53 | — | $1,367.98 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $1294.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $209.64 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $1294.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $209.64 | ▲ close $10,569.98 vs 09:30 $10,355.62 (session +239.71) | 16:00 close · cash $209.64 · equity $10,569.98 vs 09:30 $10,355.62 (+214.36; session marks +239.71) · 8 name(s) marked open→close (per-name table). AG×62 09:30 $20.55 → close $21.19 +39.68; BHP×14 09:30 $91.01 → close $93.63 +36.68; CDE×62 09:30 $20.65 → close $21.11 +28.52; HDSN×224 09:30 $5.77 → close $5.57 -44.80; IAG×65 09:30 $19.63 → close $20.50 +56.55; KGC×43 09:30 $29.63 → close $31.43 +77.40; NFGC×739 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $209.64 | ▲ 09:30 equity $10,845.87 vs yday $10,569.98 (+275.89) | 09:30 open · cash $209.64 (unchanged overnight, no fees) · equity $10,845.87 vs prior close $10,569.98 (+275.89) · 8 name(s) re-marked at the open (per-name table). AG×62 yday $21.19 → 09:30 $21.90 +44.02; BHP×14 yday $93.63 → 09:30 $95.72 +29.26; CDE×62 yday $21.11 → 09:30 $21.75 +39.68; HDSN×224 yday $5.57 → 09:30 $5.67 +22.40; IAG×65 yday $20.50 → 09:30 $21.17 +43.55; KGC×43 yday $31.43 → 09:30 $32.17 +31.82; NFGC×739 yday $1.75 → 09:30 $1.79 +29.56; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $192.27 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $26.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $169.78 | — | sell at min-hold even if still listed; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $26.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 10 | $2.47 | $0.28 | — | $144.80 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $26.21 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 13 | $1.93 | $0.29 | — | $119.42 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $26.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 19 | $1.32 | $0.31 | — | $94.04 | — | sell at min-hold even if still listed; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $26.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $94.04 | ▲ close $10,844.89 vs 09:30 $10,845.87 (session +0.29) | 16:00 close · cash $94.04 · equity $10,844.89 vs 09:30 $10,845.87 (-0.98; session marks +0.29) · 13 name(s) marked open→close (per-name table). AG×62 09:30 $21.90 → close $21.09 -50.22; BHP×14 09:30 $95.72 → close $97.03 +18.34; CDE×62 09:30 $21.75 → close $20.97 -48.36; HDSN×224 09:30 $5.67 → close $5.63 -8.96; IAG×65 09:30 $21.17 → close $21.14 -1.95; KGC×43 09:30 $32.17 → close $32.76 +25.37; NFGC×739 09:30 $1.79 → close $1.84 +36.95; WPM×8 09:30 $154.70 → close $157.78 +24.64; AUPH×1 09:30 $17.20 → close $16.65 -0.55; ARCT×2 09:30 $11.13 → close $13.45 +4.64; AUTL×10 09:30 $2.47 → close $2.41 -0.60; CRDL×13 09:30 $1.93 → close $1.86 -0.91; CYPH×19 09:30 $1.32 → close $1.42 +1.90 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $94.04 | ▲ 09:30 equity $10,974.27 vs yday $10,844.89 (+129.38) | 09:30 open · cash $94.04 (unchanged overnight, no fees) · equity $10,974.27 vs prior close $10,844.89 (+129.38) · 13 name(s) re-marked at the open (per-name table). AG×62 yday $21.09 → 09:30 $21.47 +23.56; BHP×14 yday $97.03 → 09:30 $97.34 +4.34; CDE×62 yday $20.97 → 09:30 $21.26 +17.98; HDSN×224 yday $5.63 → 09:30 $5.69 +13.44; IAG×65 yday $21.14 → 09:30 $21.44 +19.50; KGC×43 yday $32.76 → 09:30 $33.21 +19.35; NFGC×739 yday $1.84 → 09:30 $1.86 +14.78; WPM×8 yday $157.78 → 09:30 $158.96 +9.44; AUPH×1 yday $16.65 → 09:30 $16.60 -0.05; ARCT×2 yday $13.45 → 09:30 $13.26 -0.38; AUTL×10 yday $2.41 → 09:30 $2.36 -0.50; CRDL×13 yday $1.86 → 09:30 $1.87 +0.13; CYPH×19 yday $1.42 → 09:30 $1.83 +7.79 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $94.04 | ▼ close $10,815.87 vs 09:30 $10,974.27 (session -158.40) | 16:00 close · cash $94.04 · equity $10,815.87 vs 09:30 $10,974.27 (-158.40; session marks -158.40) · 13 name(s) marked open→close (per-name table). AG×62 09:30 $21.47 → close $20.57 -55.80; BHP×14 09:30 $97.34 → close $96.66 -9.52; CDE×62 09:30 $21.26 → close $20.49 -47.74; HDSN×224 09:30 $5.69 → close $5.57 -26.88; IAG×65 09:30 $21.44 → close $21.36 -5.20; KGC×43 09:30 $33.21 → close $32.47 -31.82; NFGC×739 09:30 $1.86 → close $1.90 +29.56; WPM×8 09:30 $158.96 → close $158.00 -7.68; AUPH×1 09:30 $16.60 → close $16.60 +0.00; ARCT×2 09:30 $13.26 → close $13.76 +1.00; AUTL×10 09:30 $2.36 → close $2.38 +0.20; CRDL×13 09:30 $1.87 → close $1.80 -0.91; CYPH×19 09:30 $1.83 → close $1.64 -3.61 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $94.04 | ▲ 09:30 equity $10,885.73 vs yday $10,815.87 (+69.86) | 09:30 open · cash $94.04 (unchanged overnight, no fees) · equity $10,885.73 vs prior close $10,815.87 (+69.86) · 13 name(s) re-marked at the open (per-name table). AG×62 yday $20.57 → 09:30 $20.73 +9.92; BHP×14 yday $96.66 → 09:30 $95.95 -9.94; CDE×62 yday $20.49 → 09:30 $20.85 +22.32; HDSN×224 yday $5.57 → 09:30 $5.53 -8.96; IAG×65 yday $21.36 → 09:30 $21.63 +17.55; KGC×43 yday $32.47 → 09:30 $32.76 +12.47; NFGC×739 yday $1.90 → 09:30 $1.91 +7.39; WPM×8 yday $158.00 → 09:30 $160.00 +16.00; AUPH×1 yday $16.60 → 09:30 $16.71 +0.11; ARCT×2 yday $13.76 → 09:30 $14.34 +1.16; AUTL×10 yday $2.38 → 09:30 $2.32 -0.60; CRDL×13 yday $1.80 → 09:30 $1.90 +1.30; CYPH×19 yday $1.64 → 09:30 $1.70 +1.14 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 62 | $20.73 | $2.20 | $+6.79 | $1,377.10 | ▲ +6.79 after sell → book $10,883.53; vs 09:30 mark -2.20 | time-stop after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 14 | $95.95 | $2.05 | $+65.08 | $2,718.35 | ▲ +65.08 after sell → book $10,881.48; vs 09:30 mark -2.05 | time-stop after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 62 | $20.85 | $2.20 | $+8.03 | $4,008.85 | ▲ +8.03 after sell → book $10,879.28; vs 09:30 mark -2.20 | time-stop after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 224 | $5.53 | $2.94 | $-59.59 | $5,244.63 | ▼ -59.59 after sell → book $10,876.34; vs 09:30 mark -2.94 | time-stop after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 65 | $21.63 | $2.21 | $+125.61 | $6,648.38 | ▲ +125.61 after sell → book $10,874.14; vs 09:30 mark -2.20 | time-stop after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 43 | $32.76 | $2.14 | $+130.33 | $8,054.92 | ▲ +130.33 after sell → book $10,872.00; vs 09:30 mark -2.14 | time-stop after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 739 | $1.91 | $9.67 | $+99.04 | $9,456.74 | ▲ +99.04 after sell → book $10,862.33; vs 09:30 mark -9.67 | time-stop after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $160.00 | $2.03 | $+119.63 | $10,734.70 | ▲ +119.63 after sell → book $10,860.29; vs 09:30 mark -2.04 | time-stop after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 74 | $24.00 | $2.21 | — | $8,956.49 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.0; leftover $1789.12 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 163 | $10.92 | $2.48 | — | $7,174.05 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+10.4; leftover $1789.12 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 29 | $61.47 | $2.08 | — | $5,389.35 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+9.2; leftover $1789.12 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 216 | $8.28 | $2.79 | — | $3,598.08 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.8; leftover $1789.12 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 342 | $5.23 | $4.41 | — | $1,805.01 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ret5=+10.7; leftover $1789.12 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 4 | $429.24 | $2.00 | — | $86.05 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.1; leftover $1789.12 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $86.05 | ▼ close $10,842.19 vs 09:30 $10,885.73 (session -2.14) | 16:00 close · cash $86.05 · equity $10,842.19 vs 09:30 $10,885.73 (-43.54; session marks -2.14) · 11 name(s) marked open→close (per-name table). AUPH×1 09:30 $16.71 → close $16.71 +0.00; ARCT×2 09:30 $14.34 → close $14.21 -0.26; AUTL×10 09:30 $2.32 → close $2.34 +0.20; CRDL×13 09:30 $1.90 → close $1.90 +0.00; CYPH×19 09:30 $1.70 → close $1.64 -1.14; MOS×74 09:30 $24.00 → close $23.75 -18.50; OCUL×163 09:30 $10.92 → close $10.92 +0.00; INSP×29 09:30 $61.47 → close $61.47 +0.00; CRMD×216 09:30 $8.28 → close $8.28 +0.00; RZLT×342 09:30 $5.23 → close $5.29 +20.52; HCA×4 09:30 $429.24 → close $428.50 -2.96 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $86.05 | ▲ 09:30 equity $10,842.19 vs yday $10,842.19 (-0.00) | 09:30 open · cash $86.05 (unchanged overnight, no fees) · equity $10,842.19 vs prior close $10,842.19 (-0.00) · 11 name(s) re-marked at the open (per-name table). AUPH×1 yday $16.71 → 09:30 $16.71 +0.00; ARCT×2 yday $14.21 → 09:30 $14.21 +0.00; AUTL×10 yday $2.34 → 09:30 $2.34 +0.00; CRDL×13 yday $1.90 → 09:30 $1.90 +0.00; CYPH×19 yday $1.64 → 09:30 $1.64 +0.00; MOS×74 yday $23.75 → 09:30 $23.75 +0.00; OCUL×163 yday $10.92 → 09:30 $10.92 +0.00; INSP×29 yday $61.47 → 09:30 $61.47 +0.00; CRMD×216 yday $8.28 → 09:30 $8.28 +0.00; RZLT×342 yday $5.29 → 09:30 $5.29 +0.00; HCA×4 yday $428.50 → 09:30 $428.50 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $86.05 | ▲ close $10,842.19 vs 09:30 $10,842.19 (session +0.00) | 16:00 close · cash $86.05 · equity $10,842.19 vs 09:30 $10,842.19 (-0.00; session marks +0.00) · 11 name(s) marked open→close (per-name table). AUPH×1 09:30 $16.71 → close $16.71 +0.00; ARCT×2 09:30 $14.21 → close $14.21 +0.00; AUTL×10 09:30 $2.34 → close $2.34 +0.00; CRDL×13 09:30 $1.90 → close $1.90 +0.00; CYPH×19 09:30 $1.64 → close $1.64 +0.00; MOS×74 09:30 $23.75 → close $23.75 +0.00; OCUL×163 09:30 $10.92 → close $10.92 +0.00; INSP×29 09:30 $61.47 → close $61.47 +0.00; CRMD×216 09:30 $8.28 → close $8.28 +0.00; RZLT×342 09:30 $5.29 → close $5.29 +0.00; HCA×4 09:30 $428.50 → close $428.50 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $86.05 | ▼ 09:30 equity $10,834.22 vs yday $10,842.19 (-7.97) | 09:30 open · cash $86.05 (unchanged overnight, no fees) · equity $10,834.22 vs prior close $10,842.19 (-7.97) · 11 name(s) re-marked at the open (per-name table). AUPH×1 yday $16.71 → 09:30 $16.60 -0.11; ARCT×2 yday $14.21 → 09:30 $15.35 +2.28; AUTL×10 yday $2.34 → 09:30 $2.41 +0.70; CRDL×13 yday $1.90 → 09:30 $2.03 +1.69; CYPH×19 yday $1.64 → 09:30 $1.60 -0.76; MOS×74 yday $23.75 → 09:30 $24.84 +80.66; OCUL×163 yday $10.92 → 09:30 $10.79 -21.19; INSP×29 yday $61.47 → 09:30 $60.07 -40.60; CRMD×216 yday $8.28 → 09:30 $8.60 +69.12; RZLT×342 yday $5.29 → 09:30 $5.01 -95.76; HCA×4 yday $428.50 → 09:30 $427.50 -4.00 | — |
| 2026-08-27 09:30 ET | **SELL** | `AUPH` | 1 | $16.60 | $0.19 | $-0.96 | $102.46 | ▼ -0.96 after sell → book $10,834.03; vs 09:30 mark -0.19 | time-stop after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 2 | $15.35 | $0.33 | $+7.88 | $132.82 | ▲ +7.88 after sell → book $10,833.69; vs 09:30 mark -0.34 | time-stop after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `AUTL` | 10 | $2.41 | $0.29 | $-1.17 | $156.63 | ▼ -1.17 after sell → book $10,833.40; vs 09:30 mark -0.29 | time-stop after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 13 | $2.03 | $0.32 | $+0.69 | $182.70 | ▲ +0.69 after sell → book $10,833.08; vs 09:30 mark -0.32 | time-stop after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 19 | $1.60 | $0.38 | $+4.63 | $212.72 | ▲ +4.63 after sell → book $10,832.70; vs 09:30 mark -0.38 | time-stop after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 1 | $40.72 | $0.41 | — | $171.59 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.8; leftover $70.91 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 5 | $14.09 | $0.72 | — | $100.42 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.1; leftover $70.91 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 27 | $2.59 | $0.78 | — | $29.71 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.2; leftover $70.91 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $29.71 | ▼ close $10,794.34 vs 09:30 $10,834.22 (session -36.45) | 16:00 close · cash $29.71 · equity $10,794.34 vs 09:30 $10,834.22 (-39.88; session marks -36.45) · 9 name(s) marked open→close (per-name table). MOS×74 09:30 $24.84 → close $24.16 -50.32; OCUL×163 09:30 $10.79 → close $10.77 -3.26; INSP×29 09:30 $60.07 → close $61.80 +50.17; CRMD×216 09:30 $8.60 → close $8.39 -45.36; RZLT×342 09:30 $5.01 → close $5.04 +10.26; HCA×4 09:30 $427.50 → close $427.16 -1.36; RRC×1 09:30 $40.72 → close $41.55 +0.83; CRK×5 09:30 $14.09 → close $14.50 +2.05; SLI×27 09:30 $2.59 → close $2.61 +0.54 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29.71 | ▼ 09:30 equity $10,789.26 vs yday $10,794.34 (-5.08) | 09:30 open · cash $29.71 (unchanged overnight, no fees) · equity $10,789.26 vs prior close $10,794.34 (-5.08) · 9 name(s) re-marked at the open (per-name table). MOS×74 yday $24.16 → 09:30 $24.00 -11.84; OCUL×163 yday $10.77 → 09:30 $10.63 -22.82; INSP×29 yday $61.80 → 09:30 $62.10 +8.70; CRMD×216 yday $8.39 → 09:30 $8.49 +21.60; RZLT×342 yday $5.04 → 09:30 $5.07 +10.26; HCA×4 yday $427.16 → 09:30 $424.61 -10.20; RRC×1 yday $41.55 → 09:30 $41.44 -0.11; CRK×5 yday $14.50 → 09:30 $14.42 -0.40; SLI×27 yday $2.61 → 09:30 $2.60 -0.27 | — |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 74 | $24.00 | $2.24 | $-4.45 | $1,803.47 | ▼ -4.45 after sell → book $10,787.02; vs 09:30 mark -2.24 | time-stop after 3 sess (min 3) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 163 | $10.63 | $2.52 | $-52.27 | $3,533.64 | ▼ -52.27 after sell → book $10,784.50; vs 09:30 mark -2.52 | time-stop after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 29 | $62.10 | $2.10 | $+14.09 | $5,332.44 | ▲ +14.09 after sell → book $10,782.40; vs 09:30 mark -2.10 | time-stop after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 216 | $8.49 | $2.84 | $+39.74 | $7,163.44 | ▲ +39.74 after sell → book $10,779.56; vs 09:30 mark -2.84 | time-stop after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 342 | $5.07 | $4.48 | $-63.61 | $8,892.90 | ▼ -63.61 after sell → book $10,775.08; vs 09:30 mark -4.48 | time-stop after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 4 | $424.61 | $2.03 | $-22.55 | $10,589.31 | ▼ -22.55 after sell → book $10,773.05; vs 09:30 mark -2.03 | time-stop after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `MOS` | 440 | $24.00 | $5.68 | — | $23.64 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ret5=+13.0; leftover $10589.31 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.64 | ▼ close $10,664.06 vs 09:30 $10,789.26 (session -103.32) | 16:00 close · cash $23.64 · equity $10,664.06 vs 09:30 $10,789.26 (-125.20; session marks -103.32) · 4 name(s) marked open→close (per-name table). MOS×440 09:30 $24.00 → close $23.76 -105.60; RRC×1 09:30 $41.44 → close $41.64 +0.20; CRK×5 09:30 $14.42 → close $14.62 +1.00; SLI×27 09:30 $2.60 → close $2.64 +1.08 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.64 | ▼ 09:30 equity $10,655.32 vs yday $10,664.06 (-8.74) | 09:30 open · cash $23.64 (unchanged overnight, no fees) · equity $10,655.32 vs prior close $10,664.06 (-8.74) · 4 name(s) re-marked at the open (per-name table). RRC×1 yday $41.64 → 09:30 $41.11 -0.53; CRK×5 yday $14.62 → 09:30 $14.56 -0.30; SLI×27 yday $2.64 → 09:30 $2.51 -3.51; MOS×440 yday $23.76 → 09:30 $23.75 -4.40 | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.64 | ▲ close $10,668.94 vs 09:30 $10,655.32 (session +13.62) | 16:00 close · cash $23.64 · equity $10,668.94 vs 09:30 $10,655.32 (+13.62; session marks +13.62) · 4 name(s) marked open→close (per-name table). RRC×1 09:30 $41.11 → close $41.78 +0.67; CRK×5 09:30 $14.56 → close $14.51 -0.25; SLI×27 09:30 $2.51 → close $2.51 +0.00; MOS×440 09:30 $23.75 → close $23.78 +13.20 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.64 | ▲ 09:30 equity $10,769.41 vs yday $10,668.94 (+100.47) | 09:30 open · cash $23.64 (unchanged overnight, no fees) · equity $10,769.41 vs prior close $10,668.94 (+100.47) · 4 name(s) re-marked at the open (per-name table). RRC×1 yday $41.78 → 09:30 $41.32 -0.46; CRK×5 yday $14.51 → 09:30 $14.31 -1.00; SLI×27 yday $2.51 → 09:30 $2.70 +5.13; MOS×440 yday $23.78 → 09:30 $24.00 +96.80 | — |
| 2026-09-01 09:30 ET | **SELL** | `RRC` | 1 | $41.32 | $0.44 | $-0.25 | $64.52 | ▼ -0.25 after sell → book $10,768.97; vs 09:30 mark -0.44 | time-stop after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRK` | 5 | $14.31 | $0.75 | $-0.37 | $135.32 | ▼ -0.37 after sell → book $10,768.22; vs 09:30 mark -0.75 | time-stop after 3 sess (min 3) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 27 | $2.70 | $0.83 | $+1.36 | $207.39 | ▲ +1.36 after sell → book $10,767.39; vs 09:30 mark -0.83 | time-stop after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $207.39 | ▲ close $10,877.39 vs 09:30 $10,769.41 (session +110.00) | 16:00 close · cash $207.39 · equity $10,877.39 vs 09:30 $10,769.41 (+107.98; session marks +110.00) · 1 name(s) marked open→close (per-name table). MOS×440 09:30 $24.00 → close $24.25 +110.00 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $207.39 | ▼ 09:30 equity $10,740.99 vs yday $10,877.39 (-136.40) | 09:30 open · cash $207.39 (unchanged overnight, no fees) · equity $10,740.99 vs prior close $10,877.39 (-136.40) · 1 name(s) re-marked at the open (per-name table). MOS×440 yday $24.25 → 09:30 $23.94 -136.40 | — |
| 2026-09-02 09:30 ET | **SELL** | `MOS` | 440 | $23.94 | $5.83 | $-37.91 | $10,735.16 | ▼ -37.91 after sell → book $10,735.16; vs 09:30 mark -5.83 | time-stop after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,735.16 | ▲ close $10,735.16 vs 09:30 $10,740.99 (session +0.00) | 16:00 close · cash $10,735.16 · no lots left · equity $10,735.16. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,735.16 | ▲ 09:30 equity $10,735.16 vs yday $10,735.16 (-0.00) | 09:30 open · cash $10,735.16 · no holdings · equity $10,735.16 vs prior close $10,735.16 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 43 | $49.76 | $2.12 | — | $8,593.36 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+10.6; leftover $2147.03 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 51 | $41.31 | $2.14 | — | $6,484.41 | — | sell at min-hold even if still listed; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.1; leftover $2147.03 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 656 | $3.27 | $8.46 | — | $4,330.82 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+13.8; leftover $2147.03 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 278 | $7.70 | $3.59 | — | $2,186.64 | — | sell at min-hold even if still listed; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.7; leftover $2147.03 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 17 | $125.94 | $2.04 | — | $43.62 | — | sell at min-hold even if still listed; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+6.8; leftover $2147.03 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $43.62 | ▲ close $11,288.31 vs 09:30 $10,735.16 (session +571.50) | 16:00 close · cash $43.62 · equity $11,288.31 vs 09:30 $10,735.16 (+553.15; session marks +571.50) · 5 name(s) marked open→close (per-name table). ATRC×43 09:30 $49.76 → close $52.59 +121.69; HRMY×51 09:30 $41.31 → close $42.86 +79.05; CABA×656 09:30 $3.27 → close $3.57 +196.80; VSTM×278 09:30 $7.70 → close $8.02 +88.96; RVTY×17 09:30 $125.94 → close $130.94 +85.00 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $43.62 | ▲ 09:30 equity $11,372.16 vs yday $11,288.31 (+83.85) | 09:30 open · cash $43.62 (unchanged overnight, no fees) · equity $11,372.16 vs prior close $11,288.31 (+83.85) · 5 name(s) re-marked at the open (per-name table). ATRC×43 yday $52.59 → 09:30 $52.88 +12.47; HRMY×51 yday $42.86 → 09:30 $42.93 +3.57; CABA×656 yday $3.57 → 09:30 $3.63 +39.36; VSTM×278 yday $8.02 → 09:30 $8.03 +2.78; RVTY×17 yday $130.94 → 09:30 $132.45 +25.67 | — |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 1 | $10.41 | $0.11 | — | $33.10 | — | sell at min-hold even if still listed; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ⚪; ret5=+11.1; leftover $10.90 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $33.10 | ▼ close $11,156.11 vs 09:30 $11,372.16 (session -215.94) | 16:00 close · cash $33.10 · equity $11,156.11 vs 09:30 $11,372.16 (-216.05; session marks -215.94) · 6 name(s) marked open→close (per-name table). ATRC×43 09:30 $52.88 → close $52.46 -18.06; HRMY×51 09:30 $42.93 → close $41.86 -54.57; CABA×656 09:30 $3.63 → close $3.48 -98.40; VSTM×278 09:30 $8.03 → close $7.98 -13.90; RVTY×17 09:30 $132.45 → close $130.63 -30.94; NVAX×1 09:30 $10.41 → close $10.34 -0.07 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TLN` | cash | leftover split 12.19 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 12.19 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 12.19 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 12.19 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 12.19 < 1 share @ 57.61 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 7.99 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 7.99 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 7.99 < 1 share @ 202.70 |
| 2026-08-17 | `TGB` | cash | leftover split 7.99 < 1 share @ 8.46 |
| 2026-08-17 | `ELF` | cash | leftover split 7.99 < 1 share @ 90.54 |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `HNST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `HNST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 26.21 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 26.21 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 26.21 < 1 share @ 59.72 |
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
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
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
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `MOS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `MOS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ASND` | cash | leftover split 10.90 < 1 share @ 266.94 |
| 2026-09-04 | `OSCR` | cash | leftover split 10.90 < 1 share @ 30.65 |
| 2026-09-04 | `BVS` | cash | leftover split 10.90 < 1 share @ 14.50 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 43 | 2026-09-03 @ $49.76 | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+10.6; leftover $2147.03 |
| `HRMY` | 51 | 2026-09-03 @ $41.31 | sell at min-hold even if still listed; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.1; leftover $2147.03 |
| `CABA` | 656 | 2026-09-03 @ $3.27 | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+13.8; leftover $2147.03 |
| `VSTM` | 278 | 2026-09-03 @ $7.70 | sell at min-hold even if still listed; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.7; leftover $2147.03 |
| `RVTY` | 17 | 2026-09-03 @ $125.94 | sell at min-hold even if still listed; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+6.8; leftover $2147.03 |
| `NVAX` | 1 | 2026-09-04 @ $10.41 | sell at min-hold even if still listed; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ⚪; ret5=+11.1; leftover $10.90 |
