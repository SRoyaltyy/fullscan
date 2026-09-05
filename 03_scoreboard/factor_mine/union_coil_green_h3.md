# Factor mine action — `union_coil_green_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **+2.12%** ($10,212) · signal-only (no cash/fees) was +0.72%. Starts YES **7/17**. Fills 62 · skips 126 · realized $+106.95.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $22.30.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `TPG` | 197 | — | $50.62 | +0.00 | $54.62 | +787.37 | +787.37 | +0.00 | +787.37 |
| 2026-08-14 | `TPG` | 197 | $54.62 | $55.29 | +131.99 | $53.03 | -445.22 | -313.23 | +919.36 | +474.14 |
| 2026-08-14 | `LDI` | 3 | — | $0.94 | +0.00 | $0.90 | -0.12 | -0.12 | +0.00 | -0.12 |
| 2026-08-14 | `BTBT` | 2 | — | $1.50 | +0.00 | $1.57 | +0.14 | +0.14 | +0.00 | +0.14 |
| 2026-08-17 | `TPG` | 197 | $53.03 | $52.67 | -70.92 | $51.77 | -177.30 | -248.22 | +403.22 | +225.92 |
| 2026-08-17 | `LDI` | 3 | $0.90 | $0.91 | +0.03 | $0.88 | -0.10 | -0.07 | -0.09 | -0.19 |
| 2026-08-17 | `BTBT` | 2 | $1.57 | $1.52 | -0.10 | $1.60 | +0.16 | +0.06 | +0.04 | +0.20 |
| 2026-08-18 | `TPG` | 197 | $51.77 | $51.77 | +0.00 | — | +0.00 | +0.00 | +225.92 | — |
| 2026-08-18 | `LDI` | 3 | $0.88 | $0.87 | -0.02 | $0.86 | -0.04 | -0.06 | -0.20 | -0.24 |
| 2026-08-18 | `BTBT` | 2 | $1.60 | $1.54 | -0.12 | $1.45 | -0.18 | -0.30 | +0.08 | -0.10 |
| 2026-08-19 | `LDI` | 3 | $0.86 | $0.88 | +0.07 | — | +0.00 | +0.07 | -0.17 | — |
| 2026-08-19 | `BTBT` | 2 | $1.45 | $1.42 | -0.06 | — | +0.00 | -0.06 | -0.16 | — |
| 2026-08-20 | `AG` | 62 | — | $20.55 | +0.00 | $21.19 | +39.68 | +39.68 | +0.00 | +39.68 |
| 2026-08-20 | `HDSN` | 221 | — | $5.77 | +0.00 | $5.57 | -44.20 | -44.20 | +0.00 | -44.20 |
| 2026-08-20 | `IAG` | 65 | — | $19.63 | +0.00 | $20.50 | +56.55 | +56.55 | +0.00 | +56.55 |
| 2026-08-20 | `KGC` | 43 | — | $29.63 | +0.00 | $31.43 | +77.40 | +77.40 | +0.00 | +77.40 |
| 2026-08-20 | `NFGC` | 730 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `DNA` | 171 | — | $7.45 | +0.00 | $6.96 | -83.79 | -83.79 | +0.00 | -83.79 |
| 2026-08-20 | `EXK` | 118 | — | $10.77 | +0.00 | $10.97 | +23.60 | +23.60 | +0.00 | +23.60 |
| 2026-08-20 | `SCZM` | 134 | — | $9.46 | +0.00 | $9.76 | +40.20 | +40.20 | +0.00 | +40.20 |
| 2026-08-21 | `AG` | 62 | $21.19 | $21.90 | +44.02 | $21.09 | -50.22 | -6.20 | +83.70 | +33.48 |
| 2026-08-21 | `HDSN` | 221 | $5.57 | $5.67 | +22.10 | $5.63 | -8.84 | +13.26 | -22.10 | -30.94 |
| 2026-08-21 | `IAG` | 65 | $20.50 | $21.17 | +43.55 | $21.14 | -1.95 | +41.60 | +100.10 | +98.15 |
| 2026-08-21 | `KGC` | 43 | $31.43 | $32.17 | +31.82 | $32.76 | +25.37 | +57.19 | +109.22 | +134.59 |
| 2026-08-21 | `NFGC` | 730 | $1.75 | $1.79 | +29.20 | $1.84 | +36.50 | +65.70 | +29.20 | +65.70 |
| 2026-08-21 | `DNA` | 171 | $6.96 | $7.09 | +22.23 | $7.40 | +53.01 | +75.24 | -61.56 | -8.55 |
| 2026-08-21 | `EXK` | 118 | $10.97 | $11.34 | +43.66 | $10.62 | -84.96 | -41.30 | +67.26 | -17.70 |
| 2026-08-21 | `SCZM` | 134 | $9.76 | $10.26 | +67.00 | $9.68 | -78.39 | -11.39 | +107.20 | +28.81 |
| 2026-08-21 | `ORBS` | 1 | — | $0.86 | +0.00 | $0.88 | +0.02 | +0.02 | +0.00 | +0.02 |
| 2026-08-24 | `AG` | 62 | $21.09 | $21.47 | +23.56 | $20.57 | -55.80 | -32.24 | +57.04 | +1.24 |
| 2026-08-24 | `HDSN` | 221 | $5.63 | $5.69 | +13.26 | $5.57 | -26.52 | -13.26 | -17.68 | -44.20 |
| 2026-08-24 | `IAG` | 65 | $21.14 | $21.44 | +19.50 | $21.36 | -5.20 | +14.30 | +117.65 | +112.45 |
| 2026-08-24 | `KGC` | 43 | $32.76 | $33.21 | +19.35 | $32.47 | -31.82 | -12.47 | +153.94 | +122.12 |
| 2026-08-24 | `NFGC` | 730 | $1.84 | $1.86 | +14.60 | $1.90 | +29.20 | +43.80 | +80.30 | +109.50 |
| 2026-08-24 | `DNA` | 171 | $7.40 | $7.26 | -23.94 | $6.98 | -47.88 | -71.82 | -32.49 | -80.37 |
| 2026-08-24 | `EXK` | 118 | $10.62 | $11.01 | +46.02 | $10.74 | -31.86 | +14.16 | +28.32 | -3.54 |
| 2026-08-24 | `SCZM` | 134 | $9.68 | $9.82 | +19.43 | $9.53 | -38.86 | -19.43 | +48.24 | +9.38 |
| 2026-08-24 | `ORBS` | 1 | $0.88 | $0.89 | +0.01 | $0.85 | -0.04 | -0.03 | +0.03 | -0.01 |
| 2026-08-25 | `AG` | 62 | $20.57 | $20.73 | +9.92 | — | +0.00 | +9.92 | +11.16 | — |
| 2026-08-25 | `HDSN` | 221 | $5.57 | $5.53 | -8.84 | — | +0.00 | -8.84 | -53.04 | — |
| 2026-08-25 | `IAG` | 65 | $21.36 | $21.63 | +17.55 | — | +0.00 | +17.55 | +130.00 | — |
| 2026-08-25 | `KGC` | 43 | $32.47 | $32.76 | +12.47 | — | +0.00 | +12.47 | +134.59 | — |
| 2026-08-25 | `NFGC` | 730 | $1.90 | $1.91 | +7.30 | — | +0.00 | +7.30 | +116.80 | — |
| 2026-08-25 | `DNA` | 171 | $6.98 | $6.82 | -27.36 | — | +0.00 | -27.36 | -107.73 | — |
| 2026-08-25 | `EXK` | 118 | $10.74 | $10.72 | -2.36 | — | +0.00 | -2.36 | -5.90 | — |
| 2026-08-25 | `SCZM` | 134 | $9.53 | $9.57 | +5.36 | — | +0.00 | +5.36 | +14.74 | — |
| 2026-08-25 | `ORBS` | 1 | $0.85 | $0.85 | +0.00 | $0.84 | -0.01 | -0.01 | -0.01 | -0.02 |
| 2026-08-25 | `HCA` | 3 | — | $429.24 | +0.00 | $428.50 | -2.22 | -2.22 | +0.00 | -2.22 |
| 2026-08-25 | `ALIT` | 87 | — | $14.86 | +0.00 | $14.87 | +0.87 | +0.87 | +0.00 | +0.87 |
| 2026-08-25 | `ZURA` | 203 | — | $6.38 | +0.00 | $6.50 | +24.36 | +24.36 | +0.00 | +24.36 |
| 2026-08-25 | `KURA` | 97 | — | $13.30 | +0.00 | $13.58 | +27.16 | +27.16 | +0.00 | +27.16 |
| 2026-08-25 | `EZPW` | 37 | — | $34.48 | +0.00 | $34.69 | +7.77 | +7.77 | +0.00 | +7.77 |
| 2026-08-25 | `CTKB` | 284 | — | $4.58 | +0.00 | $4.56 | -5.68 | -5.68 | +0.00 | -5.68 |
| 2026-08-25 | `BZ` | 84 | — | $15.34 | +0.00 | $16.32 | +82.32 | +82.32 | +0.00 | +82.32 |
| 2026-08-25 | `VIPS` | 93 | — | $13.91 | +0.00 | $13.83 | -7.44 | -7.44 | +0.00 | -7.44 |
| 2026-08-26 | `ORBS` | 1 | $0.84 | $0.84 | +0.00 | $0.84 | +0.00 | +0.00 | -0.02 | -0.02 |
| 2026-08-26 | `HCA` | 3 | $428.50 | $428.50 | +0.00 | $428.50 | +0.00 | +0.00 | -2.22 | -2.22 |
| 2026-08-26 | `ALIT` | 87 | $14.87 | $14.87 | +0.00 | $14.87 | +0.00 | +0.00 | +0.87 | +0.87 |
| 2026-08-26 | `ZURA` | 203 | $6.50 | $6.50 | +0.00 | $6.50 | +0.00 | +0.00 | +24.36 | +24.36 |
| 2026-08-26 | `KURA` | 97 | $13.58 | $13.58 | +0.00 | $13.58 | +0.00 | +0.00 | +27.16 | +27.16 |
| 2026-08-26 | `EZPW` | 37 | $34.69 | $34.69 | +0.00 | $34.69 | +0.00 | +0.00 | +7.77 | +7.77 |
| 2026-08-26 | `CTKB` | 284 | $4.56 | $4.56 | +0.00 | $4.56 | +0.00 | +0.00 | -5.68 | -5.68 |
| 2026-08-26 | `BZ` | 84 | $16.32 | $16.32 | +0.00 | $16.32 | +0.00 | +0.00 | +82.32 | +82.32 |
| 2026-08-26 | `VIPS` | 93 | $13.83 | $13.83 | +0.00 | $13.83 | +0.00 | +0.00 | -7.44 | -7.44 |
| 2026-08-27 | `ORBS` | 1 | $0.84 | $0.80 | -0.04 | — | +0.00 | -0.04 | -0.06 | — |
| 2026-08-27 | `HCA` | 3 | $428.50 | $427.50 | -3.00 | $427.16 | -1.02 | -4.02 | -5.22 | -6.24 |
| 2026-08-27 | `ALIT` | 87 | $14.87 | $14.85 | -1.74 | $14.33 | -45.24 | -46.98 | -0.87 | -46.11 |
| 2026-08-27 | `ZURA` | 203 | $6.50 | $6.13 | -75.11 | $5.99 | -28.42 | -103.53 | -50.75 | -79.17 |
| 2026-08-27 | `KURA` | 97 | $13.58 | $13.63 | +4.85 | $13.06 | -55.29 | -50.44 | +32.01 | -23.28 |
| 2026-08-27 | `EZPW` | 37 | $34.69 | $35.70 | +37.37 | $33.90 | -66.60 | -29.23 | +45.14 | -21.46 |
| 2026-08-27 | `CTKB` | 284 | $4.56 | $4.53 | -8.52 | $4.57 | +11.36 | +2.84 | -14.20 | -2.84 |
| 2026-08-27 | `BZ` | 84 | $16.32 | $16.77 | +37.80 | $18.84 | +173.88 | +211.68 | +120.12 | +294.00 |
| 2026-08-27 | `VIPS` | 93 | $13.83 | $14.00 | +15.81 | $14.08 | +7.44 | +23.25 | +8.37 | +15.81 |
| 2026-08-27 | `SLI` | 3 | — | $2.59 | +0.00 | $2.61 | +0.06 | +0.06 | +0.00 | +0.06 |
| 2026-08-28 | `HCA` | 3 | $427.16 | $424.61 | -7.65 | — | +0.00 | -7.65 | -13.89 | — |
| 2026-08-28 | `ALIT` | 87 | $14.33 | $14.54 | +18.27 | — | +0.00 | +18.27 | -27.84 | — |
| 2026-08-28 | `ZURA` | 203 | $5.99 | $6.02 | +6.09 | — | +0.00 | +6.09 | -73.08 | — |
| 2026-08-28 | `KURA` | 97 | $13.06 | $12.98 | -7.76 | — | +0.00 | -7.76 | -31.04 | — |
| 2026-08-28 | `EZPW` | 37 | $33.90 | $33.50 | -14.80 | — | +0.00 | -14.80 | -36.26 | — |
| 2026-08-28 | `CTKB` | 284 | $4.57 | $4.57 | +0.00 | — | +0.00 | +0.00 | -2.84 | — |
| 2026-08-28 | `BZ` | 84 | $18.84 | $18.50 | -28.56 | $18.00 | -42.00 | -70.56 | +265.44 | +223.44 |
| 2026-08-28 | `VIPS` | 93 | $14.08 | $14.00 | -7.44 | — | +0.00 | -7.44 | +8.37 | — |
| 2026-08-28 | `SLI` | 3 | $2.61 | $2.60 | -0.03 | $2.64 | +0.12 | +0.09 | +0.03 | +0.15 |
| 2026-08-28 | `RRC` | 35 | — | $41.44 | +0.00 | $41.64 | +7.00 | +7.00 | +0.00 | +7.00 |
| 2026-08-28 | `CRK` | 102 | — | $14.42 | +0.00 | $14.62 | +20.40 | +20.40 | +0.00 | +20.40 |
| 2026-08-28 | `ANF` | 10 | — | $144.70 | +0.00 | $145.75 | +10.50 | +10.50 | +0.00 | +10.50 |
| 2026-08-28 | `GENB` | 86 | — | $17.10 | +0.00 | $15.77 | -114.38 | -114.38 | +0.00 | -114.38 |
| 2026-08-28 | `CLYM` | 92 | — | $16.09 | +0.00 | $15.06 | -94.76 | -94.76 | +0.00 | -94.76 |
| 2026-08-28 | `MNRO` | 118 | — | $12.56 | +0.00 | $12.25 | -36.58 | -36.58 | +0.00 | -36.58 |
| 2026-08-31 | `BZ` | 84 | $18.00 | $17.89 | -9.24 | — | +0.00 | -9.24 | +214.20 | — |
| 2026-08-31 | `SLI` | 3 | $2.64 | $2.51 | -0.39 | $2.51 | +0.00 | -0.39 | -0.24 | -0.24 |
| 2026-08-31 | `RRC` | 35 | $41.64 | $41.11 | -18.55 | $41.78 | +23.45 | +4.90 | -11.55 | +11.90 |
| 2026-08-31 | `CRK` | 102 | $14.62 | $14.56 | -6.12 | $14.51 | -5.10 | -11.22 | +14.28 | +9.18 |
| 2026-08-31 | `ANF` | 10 | $145.75 | $148.67 | +29.20 | $149.28 | +6.10 | +35.30 | +39.70 | +45.80 |
| 2026-08-31 | `GENB` | 86 | $15.77 | $15.33 | -37.84 | $15.35 | +1.72 | -36.12 | -152.22 | -150.50 |
| 2026-08-31 | `CLYM` | 92 | $15.06 | $14.65 | -37.72 | $14.65 | +0.00 | -37.72 | -132.48 | -132.48 |
| 2026-08-31 | `MNRO` | 118 | $12.25 | $12.96 | +83.78 | $12.96 | +0.00 | +83.78 | +47.20 | +47.20 |
| 2026-09-01 | `SLI` | 3 | $2.51 | $2.70 | +0.57 | — | +0.00 | +0.57 | +0.33 | — |
| 2026-09-01 | `RRC` | 35 | $41.78 | $41.32 | -16.10 | $41.32 | +0.00 | -16.10 | -4.20 | -4.20 |
| 2026-09-01 | `CRK` | 102 | $14.51 | $14.31 | -20.40 | $14.90 | +60.18 | +39.78 | -11.22 | +48.96 |
| 2026-09-01 | `ANF` | 10 | $149.28 | $142.47 | -68.10 | $143.00 | +5.30 | -62.80 | -22.30 | -17.00 |
| 2026-09-01 | `GENB` | 86 | $15.35 | $15.51 | +13.76 | $15.30 | -18.06 | -4.30 | -136.74 | -154.80 |
| 2026-09-01 | `CLYM` | 92 | $14.65 | $13.60 | -96.60 | $13.60 | +0.00 | -96.60 | -229.08 | -229.08 |
| 2026-09-01 | `MNRO` | 118 | $12.96 | $12.46 | -59.00 | $12.46 | +0.00 | -59.00 | -11.80 | -11.80 |
| 2026-09-02 | `RRC` | 35 | $41.32 | $41.94 | +21.70 | — | +0.00 | +21.70 | +17.50 | — |
| 2026-09-02 | `CRK` | 102 | $14.90 | $15.82 | +93.84 | — | +0.00 | +93.84 | +142.80 | — |
| 2026-09-02 | `ANF` | 10 | $143.00 | $142.00 | -10.00 | — | +0.00 | -10.00 | -27.00 | — |
| 2026-09-02 | `GENB` | 86 | $15.30 | $15.12 | -15.48 | — | +0.00 | -15.48 | -170.28 | — |
| 2026-09-02 | `CLYM` | 92 | $13.60 | $13.88 | +25.76 | — | +0.00 | +25.76 | -203.32 | — |
| 2026-09-02 | `MNRO` | 118 | $12.46 | $12.28 | -21.24 | — | +0.00 | -21.24 | -33.04 | — |
| 2026-09-03 | `RVTY` | 10 | — | $125.94 | +0.00 | $130.94 | +50.00 | +50.00 | +0.00 | +50.00 |
| 2026-09-03 | `GPRO` | 1035 | — | $1.22 | +0.00 | $1.69 | +486.45 | +486.45 | +0.00 | +486.45 |
| 2026-09-03 | `CRK` | 80 | — | $15.70 | +0.00 | $15.54 | -12.80 | -12.80 | +0.00 | -12.80 |
| 2026-09-03 | `MMED` | 55 | — | $22.78 | +0.00 | $23.76 | +53.90 | +53.90 | +0.00 | +53.90 |
| 2026-09-03 | `CLYM` | 85 | — | $14.79 | +0.00 | $15.05 | +22.10 | +22.10 | +0.00 | +22.10 |
| 2026-09-03 | `CNXC` | 39 | — | $31.80 | +0.00 | $32.37 | +22.23 | +22.23 | +0.00 | +22.23 |
| 2026-09-03 | `VIR` | 108 | — | $11.63 | +0.00 | $11.50 | -14.04 | -14.04 | +0.00 | -14.04 |
| 2026-09-03 | `CDXS` | 831 | — | $1.52 | +0.00 | $1.48 | -33.24 | -33.24 | +0.00 | -33.24 |
| 2026-09-04 | `RVTY` | 10 | $130.94 | $132.45 | +15.10 | $130.63 | -18.20 | -3.10 | +65.10 | +46.90 |
| 2026-09-04 | `GPRO` | 1035 | $1.69 | $1.78 | +93.15 | $1.39 | -403.65 | -310.50 | +579.60 | +175.95 |
| 2026-09-04 | `CRK` | 80 | $15.54 | $15.45 | -7.20 | $14.95 | -40.00 | -47.20 | -20.00 | -60.00 |
| 2026-09-04 | `MMED` | 55 | $23.76 | $23.88 | +6.60 | $23.84 | -2.20 | +4.40 | +60.50 | +58.30 |
| 2026-09-04 | `CLYM` | 85 | $15.05 | $13.96 | -92.65 | $14.59 | +53.55 | -39.10 | -70.55 | -17.00 |
| 2026-09-04 | `CNXC` | 39 | $32.37 | $32.88 | +19.89 | $32.85 | -1.17 | +18.72 | +42.12 | +40.95 |
| 2026-09-04 | `VIR` | 108 | $11.50 | $11.54 | +4.32 | $11.45 | -9.72 | -5.40 | -9.72 | -19.44 |
| 2026-09-04 | `CDXS` | 831 | $1.48 | $1.48 | +0.00 | $1.42 | -49.86 | -49.86 | -33.24 | -83.10 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +787.37 | TPG | — | $24.65 | $10,784.79 | TPG×197 |
| 2026-08-14 | +5.50 | $24.65 | TPG×197 | $10,916.78 | +131.99 | -445.20 | LDI, BTBT | — | $18.76 | $10,471.51 | TPG×197, LDI×3, BTBT×2 |
| 2026-08-17 | +2.25 | $18.76 | TPG×197, LDI×3, BTBT×2 | $10,400.52 | -70.99 | -177.24 | — | — | $18.76 | $10,223.28 | TPG×197, LDI×3, BTBT×2 |
| 2026-08-18 | -6.20 | $18.76 | TPG×197, LDI×3, BTBT×2 | $10,223.14 | -0.14 | -0.22 | — | TPG | $10,214.76 | $10,220.23 | LDI×3, BTBT×2 |
| 2026-08-19 | -7.20 | $10,214.76 | LDI×3, BTBT×2 | $10,220.24 | +0.01 | +0.00 | — | LDI, BTBT | $10,220.13 | $10,220.13 | — |
| 2026-08-20 | +1.12 | $10,220.13 | — | $10,220.13 | -0.00 | +109.44 | AG, HDSN, IAG, KGC, NFGC, DNA, EXK, SCZM | — | $4.88 | $10,303.58 | AG×62, HDSN×221, IAG×65, KGC×43, NFGC×730, DNA×171, EXK×118, SCZM×134 |
| 2026-08-21 | +3.25 | $4.88 | AG×62, HDSN×221, IAG×65, KGC×43, NFGC×730, DNA×171, EXK×118, SCZM×134 | $10,607.16 | +303.58 | -109.46 | ORBS | — | $4.01 | $10,497.69 | AG×62, HDSN×221, IAG×65, KGC×43, NFGC×730, DNA×171, EXK×118, SCZM×134, ORBS×1 |
| 2026-08-24 | -5.17 | $4.01 | AG×62, HDSN×221, IAG×65, KGC×43, NFGC×730, DNA×171, EXK×118, SCZM×134, ORBS×1 | $10,629.48 | +131.79 | -208.78 | — | — | $4.01 | $10,420.70 | AG×62, HDSN×221, IAG×65, KGC×43, NFGC×730, DNA×171, EXK×118, SCZM×134, ORBS×1 |
| 2026-08-25 | +1.80 | $4.01 | AG×62, HDSN×221, IAG×65, KGC×43, NFGC×730, DNA×171, EXK×118, SCZM×134, ORBS×1 | $10,434.74 | +14.04 | +127.13 | HCA, ALIT, ZURA, KURA, EZPW, CTKB, BZ, VIPS | AG, HDSN, IAG, KGC, NFGC, DNA, EXK, SCZM | $63.68 | $10,516.11 | ORBS×1, HCA×3, ALIT×87, ZURA×203, KURA×97, EZPW×37, CTKB×284, BZ×84, VIPS×93 |
| 2026-08-26 | +2.02 | $63.68 | ORBS×1, HCA×3, ALIT×87, ZURA×203, KURA×97, EZPW×37, CTKB×284, BZ×84, VIPS×93 | $10,516.11 | +0.00 | +0.00 | — | — | $63.68 | $10,516.11 | ORBS×1, HCA×3, ALIT×87, ZURA×203, KURA×97, EZPW×37, CTKB×284, BZ×84, VIPS×93 |
| 2026-08-27 | — | $63.68 | ORBS×1, HCA×3, ALIT×87, ZURA×203, KURA×97, EZPW×37, CTKB×284, BZ×84, VIPS×93 | $10,523.53 | +7.42 | -3.83 | SLI | ORBS | $56.59 | $10,519.58 | HCA×3, ALIT×87, ZURA×203, KURA×97, EZPW×37, CTKB×284, BZ×84, VIPS×93, SLI×3 |
| 2026-08-28 | +0.75 | $56.59 | HCA×3, ALIT×87, ZURA×203, KURA×97, EZPW×37, CTKB×284, BZ×84, VIPS×93, SLI×3 | $10,477.70 | -41.88 | -249.70 | RRC, CRK, ANF, GENB, CLYM, MNRO | HCA, ALIT, ZURA, KURA, EZPW, CTKB, VIPS | $84.03 | $10,197.33 | BZ×84, SLI×3, RRC×35, CRK×102, ANF×10, GENB×86, CLYM×92, MNRO×118 |
| 2026-08-31 | -5.85 | $84.03 | BZ×84, SLI×3, RRC×35, CRK×102, ANF×10, GENB×86, CLYM×92, MNRO×118 | $10,200.45 | +3.12 | +26.17 | — | BZ | $1,584.53 | $10,224.36 | SLI×3, RRC×35, CRK×102, ANF×10, GENB×86, CLYM×92, MNRO×118 |
| 2026-09-01 | -6.30 | $1,584.53 | SLI×3, RRC×35, CRK×102, ANF×10, GENB×86, CLYM×92, MNRO×118 | $9,978.49 | -245.87 | +47.42 | — | SLI | $1,592.52 | $10,025.80 | RRC×35, CRK×102, ANF×10, GENB×86, CLYM×92, MNRO×118 |
| 2026-09-02 | -3.83 | $1,592.52 | RRC×35, CRK×102, ANF×10, GENB×86, CLYM×92, MNRO×118 | $10,120.38 | +94.58 | +0.00 | — | RRC, CRK, ANF, GENB, CLYM, MNRO | $10,106.95 | $10,106.95 | — |
| 2026-09-03 | -0.90 | $10,106.95 | — | $10,106.95 | +0.00 | +574.60 | RVTY, GPRO, CRK, MMED, CLYM, CNXC, VIR, CDXS | — | $22.30 | $10,644.41 | RVTY×10, GPRO×1035, CRK×80, MMED×55, CLYM×85, CNXC×39, VIR×108, CDXS×831 |
| 2026-09-04 | — | $22.30 | RVTY×10, GPRO×1035, CRK×80, MMED×55, CLYM×85, CNXC×39, VIR×108, CDXS×831 | $10,683.62 | +39.21 | -471.25 | — | — | $22.30 | $10,212.37 | RVTY×10, GPRO×1035, CRK×80, MMED×55, CLYM×85, CNXC×39, VIR×108, CDXS×831 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 197 | $50.62 | $2.58 | — | $24.65 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ⚪; ret5=+6.2; leftover $10000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.65 | ▲ close $10,784.79 vs 09:30 $10,000.00 (session +787.37) | 16:00 close · cash $24.65 · equity $10,784.79 vs 09:30 $10,000.00 (+784.79; session marks +787.37) · 1 name(s) marked open→close (per-name table). TPG×197 09:30 $50.62 → close $54.62 +787.37 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.65 | ▲ 09:30 equity $10,916.78 vs yday $10,784.79 (+131.99) | 09:30 open · cash $24.65 (unchanged overnight, no fees) · equity $10,916.78 vs prior close $10,784.79 (+131.99) · 1 name(s) re-marked at the open (per-name table). TPG×197 yday $54.62 → 09:30 $55.29 +131.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 3 | $0.94 | $0.04 | — | $21.80 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.5; leftover $3.08 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 2 | $1.50 | $0.04 | — | $18.76 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $3.08 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18.76 | ▼ close $10,471.51 vs 09:30 $10,916.78 (session -445.20) | 16:00 close · cash $18.76 · equity $10,471.51 vs 09:30 $10,916.78 (-445.27; session marks -445.20) · 3 name(s) marked open→close (per-name table). TPG×197 09:30 $55.29 → close $53.03 -445.22; LDI×3 09:30 $0.94 → close $0.90 -0.12; BTBT×2 09:30 $1.50 → close $1.57 +0.14 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18.76 | ▼ 09:30 equity $10,400.52 vs yday $10,471.51 (-70.99) | 09:30 open · cash $18.76 (unchanged overnight, no fees) · equity $10,400.52 vs prior close $10,471.51 (-70.99) · 3 name(s) re-marked at the open (per-name table). TPG×197 yday $53.03 → 09:30 $52.67 -70.92; LDI×3 yday $0.90 → 09:30 $0.91 +0.03; BTBT×2 yday $1.57 → 09:30 $1.52 -0.10 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18.76 | ▼ close $10,223.28 vs 09:30 $10,400.52 (session -177.24) | 16:00 close · cash $18.76 · equity $10,223.28 vs 09:30 $10,400.52 (-177.24; session marks -177.24) · 3 name(s) marked open→close (per-name table). TPG×197 09:30 $52.67 → close $51.77 -177.30; LDI×3 09:30 $0.91 → close $0.88 -0.10; BTBT×2 09:30 $1.52 → close $1.60 +0.16 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18.76 | ▼ 09:30 equity $10,223.14 vs yday $10,223.28 (-0.14) | 09:30 open · cash $18.76 (unchanged overnight, no fees) · equity $10,223.14 vs prior close $10,223.28 (-0.14) · 3 name(s) re-marked at the open (per-name table). TPG×197 yday $51.77 → 09:30 $51.77 +0.00; LDI×3 yday $0.88 → 09:30 $0.87 -0.02; BTBT×2 yday $1.60 → 09:30 $1.54 -0.12 | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 197 | $51.77 | $2.70 | $+220.64 | $10,214.76 | ▲ +220.64 after sell → book $10,220.45; vs 09:30 mark -2.69 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,214.76 | ▼ close $10,220.23 vs 09:30 $10,223.14 (session -0.22) | 16:00 close · cash $10,214.76 · equity $10,220.23 vs 09:30 $10,223.14 (-2.91; session marks -0.22) · 2 name(s) marked open→close (per-name table). LDI×3 09:30 $0.87 → close $0.86 -0.04; BTBT×2 09:30 $1.54 → close $1.45 -0.18 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,214.76 | ▲ 09:30 equity $10,220.24 vs yday $10,220.23 (+0.01) | 09:30 open · cash $10,214.76 (unchanged overnight, no fees) · equity $10,220.24 vs prior close $10,220.23 (+0.01) · 2 name(s) re-marked at the open (per-name table). LDI×3 yday $0.86 → 09:30 $0.88 +0.07; BTBT×2 yday $1.45 → 09:30 $1.42 -0.06 | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 3 | $0.88 | $0.06 | $-0.26 | $10,217.34 | ▼ -0.26 after sell → book $10,220.18; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 2 | $1.42 | $0.05 | $-0.25 | $10,220.13 | ▼ -0.25 after sell → book $10,220.13; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,220.13 | ▲ close $10,220.13 vs 09:30 $10,220.24 (session +0.00) | 16:00 close · cash $10,220.13 · no lots left · equity $10,220.13. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,220.13 | ▲ 09:30 equity $10,220.13 vs yday $10,220.13 (-0.00) | 09:30 open · cash $10,220.13 · no holdings · equity $10,220.13 vs prior close $10,220.13 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 62 | $20.55 | $2.18 | — | $8,943.85 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1277.52 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 221 | $5.77 | $2.85 | — | $7,665.83 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1277.52 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 65 | $19.63 | $2.19 | — | $6,387.70 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1277.52 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 43 | $29.63 | $2.12 | — | $5,111.49 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1277.52 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 730 | $1.75 | $9.42 | — | $3,824.57 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1277.52 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 171 | $7.45 | $2.50 | — | $2,548.12 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1277.52 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 118 | $10.77 | $2.34 | — | $1,274.91 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1277.52 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 134 | $9.46 | $2.39 | — | $4.88 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1277.52 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.88 | ▲ close $10,303.58 vs 09:30 $10,220.13 (session +109.44) | 16:00 close · cash $4.88 · equity $10,303.58 vs 09:30 $10,220.13 (+83.45; session marks +109.44) · 8 name(s) marked open→close (per-name table). AG×62 09:30 $20.55 → close $21.19 +39.68; HDSN×221 09:30 $5.77 → close $5.57 -44.20; IAG×65 09:30 $19.63 → close $20.50 +56.55; KGC×43 09:30 $29.63 → close $31.43 +77.40; NFGC×730 09:30 $1.75 → close $1.75 +0.00; DNA×171 09:30 $7.45 → close $6.96 -83.79; EXK×118 09:30 $10.77 → close $10.97 +23.60; SCZM×134 09:30 $9.46 → close $9.76 +40.20 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.88 | ▲ 09:30 equity $10,607.16 vs yday $10,303.58 (+303.58) | 09:30 open · cash $4.88 (unchanged overnight, no fees) · equity $10,607.16 vs prior close $10,303.58 (+303.58) · 8 name(s) re-marked at the open (per-name table). AG×62 yday $21.19 → 09:30 $21.90 +44.02; HDSN×221 yday $5.57 → 09:30 $5.67 +22.10; IAG×65 yday $20.50 → 09:30 $21.17 +43.55; KGC×43 yday $31.43 → 09:30 $32.17 +31.82; NFGC×730 yday $1.75 → 09:30 $1.79 +29.20; DNA×171 yday $6.96 → 09:30 $7.09 +22.23; EXK×118 yday $10.97 → 09:30 $11.34 +43.66; SCZM×134 yday $9.76 → 09:30 $10.26 +67.00 | — |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1 | $0.86 | $0.01 | — | $4.01 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $0.98 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.01 | ▼ close $10,497.69 vs 09:30 $10,607.16 (session -109.46) | 16:00 close · cash $4.01 · equity $10,497.69 vs 09:30 $10,607.16 (-109.47; session marks -109.46) · 9 name(s) marked open→close (per-name table). AG×62 09:30 $21.90 → close $21.09 -50.22; HDSN×221 09:30 $5.67 → close $5.63 -8.84; IAG×65 09:30 $21.17 → close $21.14 -1.95; KGC×43 09:30 $32.17 → close $32.76 +25.37; NFGC×730 09:30 $1.79 → close $1.84 +36.50; DNA×171 09:30 $7.09 → close $7.40 +53.01; EXK×118 09:30 $11.34 → close $10.62 -84.96; SCZM×134 09:30 $10.26 → close $9.68 -78.39; ORBS×1 09:30 $0.86 → close $0.88 +0.02 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.01 | ▲ 09:30 equity $10,629.48 vs yday $10,497.69 (+131.79) | 09:30 open · cash $4.01 (unchanged overnight, no fees) · equity $10,629.48 vs prior close $10,497.69 (+131.79) · 9 name(s) re-marked at the open (per-name table). AG×62 yday $21.09 → 09:30 $21.47 +23.56; HDSN×221 yday $5.63 → 09:30 $5.69 +13.26; IAG×65 yday $21.14 → 09:30 $21.44 +19.50; KGC×43 yday $32.76 → 09:30 $33.21 +19.35; NFGC×730 yday $1.84 → 09:30 $1.86 +14.60; DNA×171 yday $7.40 → 09:30 $7.26 -23.94; EXK×118 yday $10.62 → 09:30 $11.01 +46.02; SCZM×134 yday $9.68 → 09:30 $9.82 +19.43; ORBS×1 yday $0.88 → 09:30 $0.89 +0.01 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.01 | ▼ close $10,420.70 vs 09:30 $10,629.48 (session -208.78) | 16:00 close · cash $4.01 · equity $10,420.70 vs 09:30 $10,629.48 (-208.78; session marks -208.78) · 9 name(s) marked open→close (per-name table). AG×62 09:30 $21.47 → close $20.57 -55.80; HDSN×221 09:30 $5.69 → close $5.57 -26.52; IAG×65 09:30 $21.44 → close $21.36 -5.20; KGC×43 09:30 $33.21 → close $32.47 -31.82; NFGC×730 09:30 $1.86 → close $1.90 +29.20; DNA×171 09:30 $7.26 → close $6.98 -47.88; EXK×118 09:30 $11.01 → close $10.74 -31.86; SCZM×134 09:30 $9.82 → close $9.53 -38.86; ORBS×1 09:30 $0.89 → close $0.85 -0.04 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.01 | ▲ 09:30 equity $10,434.74 vs yday $10,420.70 (+14.04) | 09:30 open · cash $4.01 (unchanged overnight, no fees) · equity $10,434.74 vs prior close $10,420.70 (+14.04) · 9 name(s) re-marked at the open (per-name table). AG×62 yday $20.57 → 09:30 $20.73 +9.92; HDSN×221 yday $5.57 → 09:30 $5.53 -8.84; IAG×65 yday $21.36 → 09:30 $21.63 +17.55; KGC×43 yday $32.47 → 09:30 $32.76 +12.47; NFGC×730 yday $1.90 → 09:30 $1.91 +7.30; DNA×171 yday $6.98 → 09:30 $6.82 -27.36; EXK×118 yday $10.74 → 09:30 $10.72 -2.36; SCZM×134 yday $9.53 → 09:30 $9.57 +5.36; ORBS×1 yday $0.85 → 09:30 $0.85 +0.00 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 62 | $20.73 | $2.20 | $+6.79 | $1,287.07 | ▲ +6.79 after sell → book $10,432.54; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 221 | $5.53 | $2.90 | $-58.79 | $2,506.30 | ▼ -58.79 after sell → book $10,429.64; vs 09:30 mark -2.90 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 65 | $21.63 | $2.21 | $+125.61 | $3,910.05 | ▲ +125.61 after sell → book $10,427.44; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 43 | $32.76 | $2.14 | $+130.33 | $5,316.59 | ▲ +130.33 after sell → book $10,425.30; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 730 | $1.91 | $9.55 | $+97.83 | $6,701.34 | ▲ +97.83 after sell → book $10,415.75; vs 09:30 mark -9.55 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DNA` | 171 | $6.82 | $2.54 | $-112.77 | $7,865.01 | ▼ -112.77 after sell → book $10,413.20; vs 09:30 mark -2.55 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `EXK` | 118 | $10.72 | $2.37 | $-10.62 | $9,127.60 | ▼ -10.62 after sell → book $10,410.83; vs 09:30 mark -2.37 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `SCZM` | 134 | $9.57 | $2.42 | $+9.92 | $10,407.56 | ▲ +9.92 after sell → book $10,408.41; vs 09:30 mark -2.42 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $429.24 | $2.00 | — | $9,117.84 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+6.1; leftover $1300.94 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALIT` | 87 | $14.86 | $2.25 | — | $7,822.77 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+3.0; leftover $1300.94 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 203 | $6.38 | $2.62 | — | $6,525.01 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1300.94 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 97 | $13.30 | $2.28 | — | $5,232.63 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+9.5; leftover $1300.94 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 37 | $34.48 | $2.10 | — | $3,954.77 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1300.94 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CTKB` | 284 | $4.58 | $3.66 | — | $2,650.38 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; 🔵; ret5=+2.6; leftover $1300.94 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 84 | $15.34 | $2.24 | — | $1,359.58 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=+2.8; leftover $1300.94 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `VIPS` | 93 | $13.91 | $2.27 | — | $63.68 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=+2.5; leftover $1300.94 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.68 | ▲ close $10,516.11 vs 09:30 $10,434.74 (session +127.13) | 16:00 close · cash $63.68 · equity $10,516.11 vs 09:30 $10,434.74 (+81.37; session marks +127.13) · 9 name(s) marked open→close (per-name table). ORBS×1 09:30 $0.85 → close $0.84 -0.01; HCA×3 09:30 $429.24 → close $428.50 -2.22; ALIT×87 09:30 $14.86 → close $14.87 +0.87; ZURA×203 09:30 $6.38 → close $6.50 +24.36; KURA×97 09:30 $13.30 → close $13.58 +27.16; EZPW×37 09:30 $34.48 → close $34.69 +7.77; CTKB×284 09:30 $4.58 → close $4.56 -5.68; BZ×84 09:30 $15.34 → close $16.32 +82.32; VIPS×93 09:30 $13.91 → close $13.83 -7.44 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.68 | ▲ 09:30 equity $10,516.11 vs yday $10,516.11 (+0.00) | 09:30 open · cash $63.68 (unchanged overnight, no fees) · equity $10,516.11 vs prior close $10,516.11 (+0.00) · 9 name(s) re-marked at the open (per-name table). ORBS×1 yday $0.84 → 09:30 $0.84 +0.00; HCA×3 yday $428.50 → 09:30 $428.50 +0.00; ALIT×87 yday $14.87 → 09:30 $14.87 +0.00; ZURA×203 yday $6.50 → 09:30 $6.50 +0.00; KURA×97 yday $13.58 → 09:30 $13.58 +0.00; EZPW×37 yday $34.69 → 09:30 $34.69 +0.00; CTKB×284 yday $4.56 → 09:30 $4.56 +0.00; BZ×84 yday $16.32 → 09:30 $16.32 +0.00; VIPS×93 yday $13.83 → 09:30 $13.83 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.68 | ▲ close $10,516.11 vs 09:30 $10,516.11 (session +0.00) | 16:00 close · cash $63.68 · equity $10,516.11 vs 09:30 $10,516.11 (+0.00; session marks +0.00) · 9 name(s) marked open→close (per-name table). ORBS×1 09:30 $0.84 → close $0.84 +0.00; HCA×3 09:30 $428.50 → close $428.50 +0.00; ALIT×87 09:30 $14.87 → close $14.87 +0.00; ZURA×203 09:30 $6.50 → close $6.50 +0.00; KURA×97 09:30 $13.58 → close $13.58 +0.00; EZPW×37 09:30 $34.69 → close $34.69 +0.00; CTKB×284 09:30 $4.56 → close $4.56 +0.00; BZ×84 09:30 $16.32 → close $16.32 +0.00; VIPS×93 09:30 $13.83 → close $13.83 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.68 | ▲ 09:30 equity $10,523.53 vs yday $10,516.11 (+7.42) | 09:30 open · cash $63.68 (unchanged overnight, no fees) · equity $10,523.53 vs prior close $10,516.11 (+7.42) · 9 name(s) re-marked at the open (per-name table). ORBS×1 yday $0.84 → 09:30 $0.80 -0.04; HCA×3 yday $428.50 → 09:30 $427.50 -3.00; ALIT×87 yday $14.87 → 09:30 $14.85 -1.74; ZURA×203 yday $6.50 → 09:30 $6.13 -75.11; KURA×97 yday $13.58 → 09:30 $13.63 +4.85; EZPW×37 yday $34.69 → 09:30 $35.70 +37.37; CTKB×284 yday $4.56 → 09:30 $4.53 -8.52; BZ×84 yday $16.32 → 09:30 $16.77 +37.80; VIPS×93 yday $13.83 → 09:30 $14.00 +15.81 | — |
| 2026-08-27 09:30 ET | **SELL** | `ORBS` | 1 | $0.80 | $0.03 | $-0.11 | $64.45 | ▼ -0.11 after sell → book $10,523.50; vs 09:30 mark -0.03 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 3 | $2.59 | $0.09 | — | $56.59 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+4.2; leftover $9.21 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $56.59 | ▼ close $10,519.58 vs 09:30 $10,523.53 (session -3.83) | 16:00 close · cash $56.59 · equity $10,519.58 vs 09:30 $10,523.53 (-3.95; session marks -3.83) · 9 name(s) marked open→close (per-name table). HCA×3 09:30 $427.50 → close $427.16 -1.02; ALIT×87 09:30 $14.85 → close $14.33 -45.24; ZURA×203 09:30 $6.13 → close $5.99 -28.42; KURA×97 09:30 $13.63 → close $13.06 -55.29; EZPW×37 09:30 $35.70 → close $33.90 -66.60; CTKB×284 09:30 $4.53 → close $4.57 +11.36; BZ×84 09:30 $16.77 → close $18.84 +173.88; VIPS×93 09:30 $14.00 → close $14.08 +7.44; SLI×3 09:30 $2.59 → close $2.61 +0.06 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $56.59 | ▼ 09:30 equity $10,477.70 vs yday $10,519.58 (-41.88) | 09:30 open · cash $56.59 (unchanged overnight, no fees) · equity $10,477.70 vs prior close $10,519.58 (-41.88) · 9 name(s) re-marked at the open (per-name table). HCA×3 yday $427.16 → 09:30 $424.61 -7.65; ALIT×87 yday $14.33 → 09:30 $14.54 +18.27; ZURA×203 yday $5.99 → 09:30 $6.02 +6.09; KURA×97 yday $13.06 → 09:30 $12.98 -7.76; EZPW×37 yday $33.90 → 09:30 $33.50 -14.80; CTKB×284 yday $4.57 → 09:30 $4.57 +0.00; BZ×84 yday $18.84 → 09:30 $18.50 -28.56; VIPS×93 yday $14.08 → 09:30 $14.00 -7.44; SLI×3 yday $2.61 → 09:30 $2.60 -0.03 | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 3 | $424.61 | $2.02 | $-17.91 | $1,328.40 | ▼ -17.91 after sell → book $10,475.68; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALIT` | 87 | $14.54 | $2.28 | $-32.37 | $2,591.11 | ▼ -32.37 after sell → book $10,473.41; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZURA` | 203 | $6.02 | $2.66 | $-78.36 | $3,810.51 | ▼ -78.36 after sell → book $10,470.75; vs 09:30 mark -2.66 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 97 | $12.98 | $2.31 | $-35.63 | $5,067.26 | ▼ -35.63 after sell → book $10,468.44; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EZPW` | 37 | $33.50 | $2.12 | $-40.48 | $6,304.64 | ▼ -40.48 after sell → book $10,466.32; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CTKB` | 284 | $4.57 | $3.72 | $-10.22 | $7,598.80 | ▼ -10.22 after sell → book $10,462.60; vs 09:30 mark -3.72 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `VIPS` | 93 | $14.00 | $2.29 | $+3.81 | $8,898.50 | ▲ +3.81 after sell → book $10,460.30; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 35 | $41.44 | $2.10 | — | $7,446.01 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+1.8; leftover $1483.08 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CRK` | 102 | $14.42 | $2.30 | — | $5,972.87 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+1.1; leftover $1483.08 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 10 | $144.70 | $2.02 | — | $4,523.85 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1483.08 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GENB` | 86 | $17.10 | $2.25 | — | $3,051.00 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; ret5=+3.1; leftover $1483.08 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CLYM` | 92 | $16.09 | $2.27 | — | $1,568.46 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; ret5=+5.8; leftover $1483.08 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MNRO` | 118 | $12.56 | $2.34 | — | $84.03 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; ret5=+9.3; leftover $1483.08 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $84.03 | ▼ close $10,197.33 vs 09:30 $10,477.70 (session -249.70) | 16:00 close · cash $84.03 · equity $10,197.33 vs 09:30 $10,477.70 (-280.37; session marks -249.70) · 8 name(s) marked open→close (per-name table). BZ×84 09:30 $18.50 → close $18.00 -42.00; SLI×3 09:30 $2.60 → close $2.64 +0.12; RRC×35 09:30 $41.44 → close $41.64 +7.00; CRK×102 09:30 $14.42 → close $14.62 +20.40; ANF×10 09:30 $144.70 → close $145.75 +10.50; GENB×86 09:30 $17.10 → close $15.77 -114.38; CLYM×92 09:30 $16.09 → close $15.06 -94.76; MNRO×118 09:30 $12.56 → close $12.25 -36.58 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $84.03 | ▲ 09:30 equity $10,200.45 vs yday $10,197.33 (+3.12) | 09:30 open · cash $84.03 (unchanged overnight, no fees) · equity $10,200.45 vs prior close $10,197.33 (+3.12) · 8 name(s) re-marked at the open (per-name table). BZ×84 yday $18.00 → 09:30 $17.89 -9.24; SLI×3 yday $2.64 → 09:30 $2.51 -0.39; RRC×35 yday $41.64 → 09:30 $41.11 -18.55; CRK×102 yday $14.62 → 09:30 $14.56 -6.12; ANF×10 yday $145.75 → 09:30 $148.67 +29.20; GENB×86 yday $15.77 → 09:30 $15.33 -37.84; CLYM×92 yday $15.06 → 09:30 $14.65 -37.72; MNRO×118 yday $12.25 → 09:30 $12.96 +83.78 | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 84 | $17.89 | $2.27 | $+209.69 | $1,584.53 | ▲ +209.69 after sell → book $10,198.19; vs 09:30 mark -2.26 | dropped from list after 4 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,584.53 | ▲ close $10,224.36 vs 09:30 $10,200.45 (session +26.17) | 16:00 close · cash $1,584.53 · equity $10,224.36 vs 09:30 $10,200.45 (+23.91; session marks +26.17) · 7 name(s) marked open→close (per-name table). SLI×3 09:30 $2.51 → close $2.51 +0.00; RRC×35 09:30 $41.11 → close $41.78 +23.45; CRK×102 09:30 $14.56 → close $14.51 -5.10; ANF×10 09:30 $148.67 → close $149.28 +6.10; GENB×86 09:30 $15.33 → close $15.35 +1.72; CLYM×92 09:30 $14.65 → close $14.65 +0.00; MNRO×118 09:30 $12.96 → close $12.96 +0.00 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,584.53 | ▼ 09:30 equity $9,978.49 vs yday $10,224.36 (-245.87) | 09:30 open · cash $1,584.53 (unchanged overnight, no fees) · equity $9,978.49 vs prior close $10,224.36 (-245.87) · 7 name(s) re-marked at the open (per-name table). SLI×3 yday $2.51 → 09:30 $2.70 +0.57; RRC×35 yday $41.78 → 09:30 $41.32 -16.10; CRK×102 yday $14.51 → 09:30 $14.31 -20.40; ANF×10 yday $149.28 → 09:30 $142.47 -68.10; GENB×86 yday $15.35 → 09:30 $15.51 +13.76; CLYM×92 yday $14.65 → 09:30 $13.60 -96.60; MNRO×118 yday $12.96 → 09:30 $12.46 -59.00 | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 3 | $2.70 | $0.11 | $+0.13 | $1,592.52 | ▲ +0.13 after sell → book $9,978.38; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,592.52 | ▲ close $10,025.80 vs 09:30 $9,978.49 (session +47.42) | 16:00 close · cash $1,592.52 · equity $10,025.80 vs 09:30 $9,978.49 (+47.31; session marks +47.42) · 6 name(s) marked open→close (per-name table). RRC×35 09:30 $41.32 → close $41.32 +0.00; CRK×102 09:30 $14.31 → close $14.90 +60.18; ANF×10 09:30 $142.47 → close $143.00 +5.30; GENB×86 09:30 $15.51 → close $15.30 -18.06; CLYM×92 09:30 $13.60 → close $13.60 +0.00; MNRO×118 09:30 $12.46 → close $12.46 +0.00 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,592.52 | ▲ 09:30 equity $10,120.38 vs yday $10,025.80 (+94.58) | 09:30 open · cash $1,592.52 (unchanged overnight, no fees) · equity $10,120.38 vs prior close $10,025.80 (+94.58) · 6 name(s) re-marked at the open (per-name table). RRC×35 yday $41.32 → 09:30 $41.94 +21.70; CRK×102 yday $14.90 → 09:30 $15.82 +93.84; ANF×10 yday $143.00 → 09:30 $142.00 -10.00; GENB×86 yday $15.30 → 09:30 $15.12 -15.48; CLYM×92 yday $13.60 → 09:30 $13.88 +25.76; MNRO×118 yday $12.46 → 09:30 $12.28 -21.24 | — |
| 2026-09-02 09:30 ET | **SELL** | `RRC` | 35 | $41.94 | $2.12 | $+13.29 | $3,058.30 | ▲ +13.29 after sell → book $10,118.26; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CRK` | 102 | $15.82 | $2.33 | $+138.18 | $4,669.61 | ▲ +138.18 after sell → book $10,115.93; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 10 | $142.00 | $2.04 | $-31.06 | $6,087.57 | ▼ -31.06 after sell → book $10,113.89; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GENB` | 86 | $15.12 | $2.27 | $-174.80 | $7,385.62 | ▼ -174.80 after sell → book $10,111.62; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CLYM` | 92 | $13.88 | $2.29 | $-207.88 | $8,660.29 | ▼ -207.88 after sell → book $10,109.33; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `MNRO` | 118 | $12.28 | $2.38 | $-37.76 | $10,106.95 | ▼ -37.76 after sell → book $10,106.95; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,106.95 | ▲ close $10,106.95 vs 09:30 $10,120.38 (session +0.00) | 16:00 close · cash $10,106.95 · no lots left · equity $10,106.95. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,106.95 | ▲ 09:30 equity $10,106.95 vs yday $10,106.95 (+0.00) | 09:30 open · cash $10,106.95 · no holdings · equity $10,106.95 vs prior close $10,106.95 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $125.94 | $2.02 | — | $8,845.53 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1263.37 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1035 | $1.22 | $13.35 | — | $7,569.48 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1263.37 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 80 | $15.70 | $2.23 | — | $6,311.25 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1263.37 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 55 | $22.78 | $2.15 | — | $5,056.20 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1263.37 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CLYM` | 85 | $14.79 | $2.25 | — | $3,796.80 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+5.8; leftover $1263.37 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 39 | $31.80 | $2.11 | — | $2,554.49 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+3.7; leftover $1263.37 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VIR` | 108 | $11.63 | $2.31 | — | $1,296.14 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+5.8; leftover $1263.37 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CDXS` | 831 | $1.52 | $10.72 | — | $22.30 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; ret5=+7.1; leftover $1263.37 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22.30 | ▲ close $10,644.41 vs 09:30 $10,106.95 (session +574.60) | 16:00 close · cash $22.30 · equity $10,644.41 vs 09:30 $10,106.95 (+537.46; session marks +574.60) · 8 name(s) marked open→close (per-name table). RVTY×10 09:30 $125.94 → close $130.94 +50.00; GPRO×1035 09:30 $1.22 → close $1.69 +486.45; CRK×80 09:30 $15.70 → close $15.54 -12.80; MMED×55 09:30 $22.78 → close $23.76 +53.90; CLYM×85 09:30 $14.79 → close $15.05 +22.10; CNXC×39 09:30 $31.80 → close $32.37 +22.23; VIR×108 09:30 $11.63 → close $11.50 -14.04; CDXS×831 09:30 $1.52 → close $1.48 -33.24 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.30 | ▲ 09:30 equity $10,683.62 vs yday $10,644.41 (+39.21) | 09:30 open · cash $22.30 (unchanged overnight, no fees) · equity $10,683.62 vs prior close $10,644.41 (+39.21) · 8 name(s) re-marked at the open (per-name table). RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; GPRO×1035 yday $1.69 → 09:30 $1.78 +93.15; CRK×80 yday $15.54 → 09:30 $15.45 -7.20; MMED×55 yday $23.76 → 09:30 $23.88 +6.60; CLYM×85 yday $15.05 → 09:30 $13.96 -92.65; CNXC×39 yday $32.37 → 09:30 $32.88 +19.89; VIR×108 yday $11.50 → 09:30 $11.54 +4.32; CDXS×831 yday $1.48 → 09:30 $1.48 +0.00 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22.30 | ▼ close $10,212.37 vs 09:30 $10,683.62 (session -471.25) | 16:00 close · cash $22.30 · equity $10,212.37 vs 09:30 $10,683.62 (-471.25; session marks -471.25) · 8 name(s) marked open→close (per-name table). RVTY×10 09:30 $132.45 → close $130.63 -18.20; GPRO×1035 09:30 $1.78 → close $1.39 -403.65; CRK×80 09:30 $15.45 → close $14.95 -40.00; MMED×55 09:30 $23.88 → close $23.84 -2.20; CLYM×85 09:30 $13.96 → close $14.59 +53.55; CNXC×39 09:30 $32.88 → close $32.85 -1.17; VIR×108 09:30 $11.54 → close $11.45 -9.72; CDXS×831 09:30 $1.48 → close $1.42 -49.86 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLG` | cash | leftover split 3.08 < 1 share @ 57.61 |
| 2026-08-14 | `ANGX` | cash | leftover split 3.08 < 1 share @ 4.31 |
| 2026-08-14 | `HYLN` | cash | leftover split 3.08 < 1 share @ 4.18 |
| 2026-08-14 | `WDC` | cash | leftover split 3.08 < 1 share @ 503.50 |
| 2026-08-14 | `ADUR` | cash | leftover split 3.08 < 1 share @ 16.50 |
| 2026-08-14 | `ALGM` | cash | leftover split 3.08 < 1 share @ 44.06 |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 4.69 < 1 share @ 46.18 |
| 2026-08-17 | `OCC` | cash | leftover split 4.69 < 1 share @ 18.24 |
| 2026-08-17 | `ALM` | cash | leftover split 4.69 < 1 share @ 16.20 |
| 2026-08-17 | `NEWP` | cash | leftover split 4.69 < 1 share @ 6.94 |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `PAYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBNX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BETA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `U` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `VSTM` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `EXK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SCZM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BTBT` | cash | leftover split 0.98 < 1 share @ 1.66 |
| 2026-08-21 | `EMBC` | cash | leftover split 0.98 < 1 share @ 5.43 |
| 2026-08-21 | `TXG` | cash | leftover split 0.98 < 1 share @ 64.39 |
| 2026-08-21 | `DXYZ` | cash | leftover split 0.98 < 1 share @ 34.89 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `DNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `EXK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SCZM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABAT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `ORBS` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ALIT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EZPW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CTKB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `VIPS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `OSUR` | no_price | no 09:30 open |
| 2026-08-26 | `ANF` | no_price | no 09:30 open |
| 2026-08-26 | `INTU` | no_price | no 09:30 open |
| 2026-08-26 | `SJM` | no_price | no 09:30 open |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALIT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `KURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EZPW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CTKB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `VIPS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RRC` | cash | leftover split 9.21 < 1 share @ 40.72 |
| 2026-08-27 | `CRK` | cash | leftover split 9.21 < 1 share @ 14.09 |
| 2026-08-27 | `DLO` | cash | leftover split 9.21 < 1 share @ 15.60 |
| 2026-08-27 | `GEN` | cash | leftover split 9.21 < 1 share @ 28.89 |
| 2026-08-27 | `PGY` | cash | leftover split 9.21 < 1 share @ 21.97 |
| 2026-08-27 | `PLTR` | cash | leftover split 9.21 < 1 share @ 170.60 |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GENB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CLYM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MNRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DINO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DLO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GENB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CLYM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `MNRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `VFF` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HELP` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ACB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CLYM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CNXC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VIR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CDXS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `BVS` | cash | leftover split 3.19 < 1 share @ 14.50 |
| 2026-09-04 | `FMC` | cash | leftover split 3.19 < 1 share @ 13.30 |
| 2026-09-04 | `TARS` | cash | leftover split 3.19 < 1 share @ 82.76 |
| 2026-09-04 | `PLAY` | cash | leftover split 3.19 < 1 share @ 9.36 |
| 2026-09-04 | `ASAN` | cash | leftover split 3.19 < 1 share @ 10.16 |
| 2026-09-04 | `GWRE` | cash | leftover split 3.19 < 1 share @ 198.00 |
| 2026-09-04 | `LULU` | cash | leftover split 3.19 < 1 share @ 121.15 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `RVTY` | 10 | 2026-09-03 @ $125.94 | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1263.37 |
| `GPRO` | 1035 | 2026-09-03 @ $1.22 | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1263.37 |
| `CRK` | 80 | 2026-09-03 @ $15.70 | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1263.37 |
| `MMED` | 55 | 2026-09-03 @ $22.78 | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1263.37 |
| `CLYM` | 85 | 2026-09-03 @ $14.79 | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+5.8; leftover $1263.37 |
| `CNXC` | 39 | 2026-09-03 @ $31.80 | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+3.7; leftover $1263.37 |
| `VIR` | 108 | 2026-09-03 @ $11.63 | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+5.8; leftover $1263.37 |
| `CDXS` | 831 | 2026-09-03 @ $1.52 | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; ret5=+7.1; leftover $1263.37 |
