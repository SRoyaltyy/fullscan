# Factor mine action — `union_coil_green_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **+3.41%** ($10,341) · signal-only (no cash/fees) was +4.19%. Starts YES **7/17**. Fills 114 · skips 48 · realized $+294.50.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $221.06.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `TPG` | 197 | — | $50.62 | +0.00 | $54.62 | +787.37 | +787.37 | +0.00 | +787.37 |
| 2026-08-14 | `TPG` | 197 | $54.62 | $55.29 | +131.99 | — | +0.00 | +131.99 | +919.36 | — |
| 2026-08-14 | `SLG` | 23 | — | $57.61 | +0.00 | $56.09 | -34.96 | -34.96 | +0.00 | -34.96 |
| 2026-08-14 | `LDI` | 1455 | — | $0.94 | +0.00 | $0.90 | -58.20 | -58.20 | +0.00 | -58.20 |
| 2026-08-14 | `BTBT` | 909 | — | $1.50 | +0.00 | $1.57 | +63.63 | +63.63 | +0.00 | +63.63 |
| 2026-08-14 | `ANGX` | 316 | — | $4.31 | +0.00 | $4.37 | +18.96 | +18.96 | +0.00 | +18.96 |
| 2026-08-14 | `HYLN` | 326 | — | $4.18 | +0.00 | $4.06 | -39.12 | -39.12 | +0.00 | -39.12 |
| 2026-08-14 | `WDC` | 2 | — | $503.50 | +0.00 | $508.80 | +10.60 | +10.60 | +0.00 | +10.60 |
| 2026-08-14 | `ADUR` | 82 | — | $16.50 | +0.00 | $16.17 | -27.06 | -27.06 | +0.00 | -27.06 |
| 2026-08-14 | `ALGM` | 30 | — | $44.06 | +0.00 | $44.39 | +9.90 | +9.90 | +0.00 | +9.90 |
| 2026-08-17 | `SLG` | 23 | $56.09 | $55.37 | -16.56 | — | +0.00 | -16.56 | -51.52 | — |
| 2026-08-17 | `LDI` | 1455 | $0.90 | $0.91 | +14.55 | — | +0.00 | +14.55 | -43.65 | — |
| 2026-08-17 | `BTBT` | 909 | $1.57 | $1.52 | -45.45 | — | +0.00 | -45.45 | +18.18 | — |
| 2026-08-17 | `ANGX` | 316 | $4.37 | $4.60 | +72.68 | — | +0.00 | +72.68 | +91.64 | — |
| 2026-08-17 | `HYLN` | 326 | $4.06 | $4.10 | +13.04 | — | +0.00 | +13.04 | -26.08 | — |
| 2026-08-17 | `WDC` | 2 | $508.80 | $525.53 | +33.46 | — | +0.00 | +33.46 | +44.06 | — |
| 2026-08-17 | `ADUR` | 82 | $16.17 | $15.73 | -36.08 | — | +0.00 | -36.08 | -63.14 | — |
| 2026-08-17 | `ALGM` | 30 | $44.39 | $45.32 | +27.90 | — | +0.00 | +27.90 | +37.80 | — |
| 2026-08-17 | `DVN` | 58 | — | $46.18 | +0.00 | $47.57 | +80.62 | +80.62 | +0.00 | +80.62 |
| 2026-08-17 | `OCC` | 148 | — | $18.24 | +0.00 | $17.12 | -165.76 | -165.76 | +0.00 | -165.76 |
| 2026-08-17 | `ALM` | 167 | — | $16.20 | +0.00 | $16.36 | +26.72 | +26.72 | +0.00 | +26.72 |
| 2026-08-17 | `NEWP` | 390 | — | $6.94 | +0.00 | $6.66 | -109.20 | -109.20 | +0.00 | -109.20 |
| 2026-08-18 | `DVN` | 58 | $47.57 | $48.00 | +24.94 | — | +0.00 | +24.94 | +105.56 | — |
| 2026-08-18 | `OCC` | 148 | $17.12 | $16.20 | -136.16 | — | +0.00 | -136.16 | -301.92 | — |
| 2026-08-18 | `ALM` | 167 | $16.36 | $15.78 | -96.86 | — | +0.00 | -96.86 | -70.14 | — |
| 2026-08-18 | `NEWP` | 390 | $6.66 | $6.51 | -58.50 | — | +0.00 | -58.50 | -167.70 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 63 | — | $20.55 | +0.00 | $21.19 | +40.32 | +40.32 | +0.00 | +40.32 |
| 2026-08-20 | `HDSN` | 224 | — | $5.77 | +0.00 | $5.57 | -44.80 | -44.80 | +0.00 | -44.80 |
| 2026-08-20 | `IAG` | 66 | — | $19.63 | +0.00 | $20.50 | +57.42 | +57.42 | +0.00 | +57.42 |
| 2026-08-20 | `KGC` | 43 | — | $29.63 | +0.00 | $31.43 | +77.40 | +77.40 | +0.00 | +77.40 |
| 2026-08-20 | `NFGC` | 740 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `DNA` | 173 | — | $7.45 | +0.00 | $6.96 | -84.77 | -84.77 | +0.00 | -84.77 |
| 2026-08-20 | `EXK` | 120 | — | $10.77 | +0.00 | $10.97 | +24.00 | +24.00 | +0.00 | +24.00 |
| 2026-08-20 | `SCZM` | 137 | — | $9.46 | +0.00 | $9.76 | +41.10 | +41.10 | +0.00 | +41.10 |
| 2026-08-21 | `AG` | 63 | $21.19 | $21.90 | +44.73 | — | +0.00 | +44.73 | +85.05 | — |
| 2026-08-21 | `HDSN` | 224 | $5.57 | $5.67 | +22.40 | — | +0.00 | +22.40 | -22.40 | — |
| 2026-08-21 | `IAG` | 66 | $20.50 | $21.17 | +44.22 | — | +0.00 | +44.22 | +101.64 | — |
| 2026-08-21 | `KGC` | 43 | $31.43 | $32.17 | +31.82 | — | +0.00 | +31.82 | +109.22 | — |
| 2026-08-21 | `NFGC` | 740 | $1.75 | $1.79 | +29.60 | — | +0.00 | +29.60 | +29.60 | — |
| 2026-08-21 | `DNA` | 173 | $6.96 | $7.09 | +22.49 | — | +0.00 | +22.49 | -62.28 | — |
| 2026-08-21 | `EXK` | 120 | $10.97 | $11.34 | +44.40 | — | +0.00 | +44.40 | +68.40 | — |
| 2026-08-21 | `SCZM` | 137 | $9.76 | $10.26 | +68.50 | — | +0.00 | +68.50 | +109.60 | — |
| 2026-08-21 | `BTBT` | 1293 | — | $1.66 | +0.00 | $1.53 | -168.09 | -168.09 | +0.00 | -168.09 |
| 2026-08-21 | `ORBS` | 2485 | — | $0.86 | +0.00 | $0.88 | +39.76 | +39.76 | +0.00 | +39.76 |
| 2026-08-21 | `EMBC` | 395 | — | $5.43 | +0.00 | $5.23 | -79.00 | -79.00 | +0.00 | -79.00 |
| 2026-08-21 | `TXG` | 33 | — | $64.39 | +0.00 | $65.12 | +24.09 | +24.09 | +0.00 | +24.09 |
| 2026-08-21 | `DXYZ` | 60 | — | $34.89 | +0.00 | $34.43 | -27.60 | -27.60 | +0.00 | -27.60 |
| 2026-08-24 | `BTBT` | 1293 | $1.53 | $1.55 | +25.86 | — | +0.00 | +25.86 | -142.23 | — |
| 2026-08-24 | `ORBS` | 2485 | $0.88 | $0.89 | +24.85 | — | +0.00 | +24.85 | +64.61 | — |
| 2026-08-24 | `EMBC` | 395 | $5.23 | $5.21 | -7.90 | — | +0.00 | -7.90 | -86.90 | — |
| 2026-08-24 | `TXG` | 33 | $65.12 | $63.07 | -67.65 | — | +0.00 | -67.65 | -43.56 | — |
| 2026-08-24 | `DXYZ` | 60 | $34.43 | $33.12 | -78.60 | — | +0.00 | -78.60 | -106.20 | — |
| 2026-08-25 | `HCA` | 3 | — | $429.24 | +0.00 | $428.50 | -2.22 | -2.22 | +0.00 | -2.22 |
| 2026-08-25 | `ALIT` | 86 | — | $14.86 | +0.00 | $14.87 | +0.86 | +0.86 | +0.00 | +0.86 |
| 2026-08-25 | `ZURA` | 202 | — | $6.38 | +0.00 | $6.50 | +24.24 | +24.24 | +0.00 | +24.24 |
| 2026-08-25 | `KURA` | 96 | — | $13.30 | +0.00 | $13.58 | +26.88 | +26.88 | +0.00 | +26.88 |
| 2026-08-25 | `EZPW` | 37 | — | $34.48 | +0.00 | $34.69 | +7.77 | +7.77 | +0.00 | +7.77 |
| 2026-08-25 | `CTKB` | 281 | — | $4.58 | +0.00 | $4.56 | -5.62 | -5.62 | +0.00 | -5.62 |
| 2026-08-25 | `BZ` | 84 | — | $15.34 | +0.00 | $16.32 | +82.32 | +82.32 | +0.00 | +82.32 |
| 2026-08-25 | `VIPS` | 92 | — | $13.91 | +0.00 | $13.83 | -7.36 | -7.36 | +0.00 | -7.36 |
| 2026-08-26 | `HCA` | 3 | $428.50 | $428.50 | +0.00 | $428.50 | +0.00 | +0.00 | -2.22 | -2.22 |
| 2026-08-26 | `ALIT` | 86 | $14.87 | $14.87 | +0.00 | $14.87 | +0.00 | +0.00 | +0.86 | +0.86 |
| 2026-08-26 | `ZURA` | 202 | $6.50 | $6.50 | +0.00 | $6.50 | +0.00 | +0.00 | +24.24 | +24.24 |
| 2026-08-26 | `KURA` | 96 | $13.58 | $13.58 | +0.00 | $13.58 | +0.00 | +0.00 | +26.88 | +26.88 |
| 2026-08-26 | `EZPW` | 37 | $34.69 | $34.69 | +0.00 | $34.69 | +0.00 | +0.00 | +7.77 | +7.77 |
| 2026-08-26 | `CTKB` | 281 | $4.56 | $4.56 | +0.00 | $4.56 | +0.00 | +0.00 | -5.62 | -5.62 |
| 2026-08-26 | `BZ` | 84 | $16.32 | $16.32 | +0.00 | $16.32 | +0.00 | +0.00 | +82.32 | +82.32 |
| 2026-08-26 | `VIPS` | 92 | $13.83 | $13.83 | +0.00 | $13.83 | +0.00 | +0.00 | -7.36 | -7.36 |
| 2026-08-27 | `HCA` | 3 | $428.50 | $427.50 | -3.00 | — | +0.00 | -3.00 | -5.22 | — |
| 2026-08-27 | `ALIT` | 86 | $14.87 | $14.85 | -1.72 | — | +0.00 | -1.72 | -0.86 | — |
| 2026-08-27 | `ZURA` | 202 | $6.50 | $6.13 | -74.74 | — | +0.00 | -74.74 | -50.50 | — |
| 2026-08-27 | `KURA` | 96 | $13.58 | $13.63 | +4.80 | — | +0.00 | +4.80 | +31.68 | — |
| 2026-08-27 | `EZPW` | 37 | $34.69 | $35.70 | +37.37 | — | +0.00 | +37.37 | +45.14 | — |
| 2026-08-27 | `CTKB` | 281 | $4.56 | $4.53 | -8.43 | — | +0.00 | -8.43 | -14.05 | — |
| 2026-08-27 | `BZ` | 84 | $16.32 | $16.77 | +37.80 | — | +0.00 | +37.80 | +120.12 | — |
| 2026-08-27 | `VIPS` | 92 | $13.83 | $14.00 | +15.64 | — | +0.00 | +15.64 | +8.28 | — |
| 2026-08-27 | `RRC` | 36 | — | $40.72 | +0.00 | $41.55 | +29.88 | +29.88 | +0.00 | +29.88 |
| 2026-08-27 | `CRK` | 105 | — | $14.09 | +0.00 | $14.50 | +43.05 | +43.05 | +0.00 | +43.05 |
| 2026-08-27 | `SLI` | 573 | — | $2.59 | +0.00 | $2.61 | +11.46 | +11.46 | +0.00 | +11.46 |
| 2026-08-27 | `DLO` | 95 | — | $15.60 | +0.00 | $15.36 | -22.80 | -22.80 | +0.00 | -22.80 |
| 2026-08-27 | `GEN` | 51 | — | $28.89 | +0.00 | $29.64 | +38.25 | +38.25 | +0.00 | +38.25 |
| 2026-08-27 | `PGY` | 67 | — | $21.97 | +0.00 | $22.41 | +29.48 | +29.48 | +0.00 | +29.48 |
| 2026-08-27 | `PLTR` | 8 | — | $170.60 | +0.00 | $177.50 | +55.20 | +55.20 | +0.00 | +55.20 |
| 2026-08-28 | `RRC` | 36 | $41.55 | $41.44 | -3.96 | $41.64 | +7.20 | +3.24 | +25.92 | +33.12 |
| 2026-08-28 | `CRK` | 105 | $14.50 | $14.42 | -8.40 | $14.62 | +21.00 | +12.60 | +34.65 | +55.65 |
| 2026-08-28 | `SLI` | 573 | $2.61 | $2.60 | -5.73 | $2.64 | +22.92 | +17.19 | +5.73 | +28.65 |
| 2026-08-28 | `DLO` | 95 | $15.36 | $15.33 | -2.85 | — | +0.00 | -2.85 | -25.65 | — |
| 2026-08-28 | `GEN` | 51 | $29.64 | $29.83 | +9.69 | — | +0.00 | +9.69 | +47.94 | — |
| 2026-08-28 | `PGY` | 67 | $22.41 | $22.93 | +34.84 | — | +0.00 | +34.84 | +64.32 | — |
| 2026-08-28 | `PLTR` | 8 | $177.50 | $178.75 | +10.00 | — | +0.00 | +10.00 | +65.20 | — |
| 2026-08-28 | `ANF` | 8 | — | $144.70 | +0.00 | $145.75 | +8.40 | +8.40 | +0.00 | +8.40 |
| 2026-08-28 | `BZ` | 65 | — | $18.50 | +0.00 | $18.00 | -32.50 | -32.50 | +0.00 | -32.50 |
| 2026-08-28 | `GENB` | 71 | — | $17.10 | +0.00 | $15.77 | -94.43 | -94.43 | +0.00 | -94.43 |
| 2026-08-28 | `CLYM` | 75 | — | $16.09 | +0.00 | $15.06 | -77.25 | -77.25 | +0.00 | -77.25 |
| 2026-08-28 | `MNRO` | 97 | — | $12.56 | +0.00 | $12.25 | -30.07 | -30.07 | +0.00 | -30.07 |
| 2026-08-31 | `RRC` | 36 | $41.64 | $41.11 | -19.08 | — | +0.00 | -19.08 | +14.04 | — |
| 2026-08-31 | `CRK` | 105 | $14.62 | $14.56 | -6.30 | — | +0.00 | -6.30 | +49.35 | — |
| 2026-08-31 | `SLI` | 573 | $2.64 | $2.51 | -74.49 | — | +0.00 | -74.49 | -45.84 | — |
| 2026-08-31 | `ANF` | 8 | $145.75 | $148.67 | +23.36 | — | +0.00 | +23.36 | +31.76 | — |
| 2026-08-31 | `BZ` | 65 | $18.00 | $17.89 | -7.15 | — | +0.00 | -7.15 | -39.65 | — |
| 2026-08-31 | `GENB` | 71 | $15.77 | $15.33 | -31.24 | — | +0.00 | -31.24 | -125.67 | — |
| 2026-08-31 | `CLYM` | 75 | $15.06 | $14.65 | -30.75 | — | +0.00 | -30.75 | -108.00 | — |
| 2026-08-31 | `MNRO` | 97 | $12.25 | $12.96 | +68.87 | — | +0.00 | +68.87 | +38.80 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `RVTY` | 10 | — | $125.94 | +0.00 | $130.94 | +50.00 | +50.00 | +0.00 | +50.00 |
| 2026-09-03 | `GPRO` | 1056 | — | $1.22 | +0.00 | $1.69 | +496.32 | +496.32 | +0.00 | +496.32 |
| 2026-09-03 | `CRK` | 82 | — | $15.70 | +0.00 | $15.54 | -13.12 | -13.12 | +0.00 | -13.12 |
| 2026-09-03 | `MMED` | 56 | — | $22.78 | +0.00 | $23.76 | +54.88 | +54.88 | +0.00 | +54.88 |
| 2026-09-03 | `CLYM` | 87 | — | $14.79 | +0.00 | $15.05 | +22.62 | +22.62 | +0.00 | +22.62 |
| 2026-09-03 | `CNXC` | 40 | — | $31.80 | +0.00 | $32.37 | +22.80 | +22.80 | +0.00 | +22.80 |
| 2026-09-03 | `VIR` | 110 | — | $11.63 | +0.00 | $11.50 | -14.30 | -14.30 | +0.00 | -14.30 |
| 2026-09-03 | `CDXS` | 847 | — | $1.52 | +0.00 | $1.48 | -33.88 | -33.88 | +0.00 | -33.88 |
| 2026-09-04 | `RVTY` | 10 | $130.94 | $132.45 | +15.10 | — | +0.00 | +15.10 | +65.10 | — |
| 2026-09-04 | `GPRO` | 1056 | $1.69 | $1.78 | +95.04 | $1.39 | -411.84 | -316.80 | +591.36 | +179.52 |
| 2026-09-04 | `CRK` | 82 | $15.54 | $15.45 | -7.38 | — | +0.00 | -7.38 | -20.50 | — |
| 2026-09-04 | `MMED` | 56 | $23.76 | $23.88 | +6.72 | — | +0.00 | +6.72 | +61.60 | — |
| 2026-09-04 | `CLYM` | 87 | $15.05 | $13.96 | -94.83 | — | +0.00 | -94.83 | -72.21 | — |
| 2026-09-04 | `CNXC` | 40 | $32.37 | $32.88 | +20.40 | — | +0.00 | +20.40 | +43.20 | — |
| 2026-09-04 | `VIR` | 110 | $11.50 | $11.54 | +4.40 | — | +0.00 | +4.40 | -9.90 | — |
| 2026-09-04 | `CDXS` | 847 | $1.48 | $1.48 | +0.00 | — | +0.00 | +0.00 | -33.88 | — |
| 2026-09-04 | `BVS` | 88 | — | $14.50 | +0.00 | $14.36 | -12.32 | -12.32 | +0.00 | -12.32 |
| 2026-09-04 | `FMC` | 96 | — | $13.30 | +0.00 | $12.98 | -30.72 | -30.72 | +0.00 | -30.72 |
| 2026-09-04 | `TARS` | 15 | — | $82.76 | +0.00 | $83.21 | +6.75 | +6.75 | +0.00 | +6.75 |
| 2026-09-04 | `PLAY` | 137 | — | $9.36 | +0.00 | $8.67 | -94.53 | -94.53 | +0.00 | -94.53 |
| 2026-09-04 | `ASAN` | 126 | — | $10.16 | +0.00 | $10.09 | -8.82 | -8.82 | +0.00 | -8.82 |
| 2026-09-04 | `GWRE` | 6 | — | $198.00 | +0.00 | $202.86 | +29.16 | +29.16 | +0.00 | +29.16 |
| 2026-09-04 | `LULU` | 10 | — | $121.15 | +0.00 | $121.77 | +6.20 | +6.20 | +0.00 | +6.20 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +787.37 | TPG | — | $24.65 | $10,784.79 | TPG×197 |
| 2026-08-14 | +5.50 | $24.65 | TPG×197 | $10,916.78 | +131.99 | -56.25 | SLG, LDI, BTBT, ANGX, HYLN, WDC, ADUR, ALGM | TPG | $409.40 | $10,811.45 | SLG×23, LDI×1455, BTBT×909, ANGX×316, HYLN×326, WDC×2, ADUR×82, ALGM×30 |
| 2026-08-17 | +2.25 | $409.40 | SLG×23, LDI×1455, BTBT×909, ANGX×316, HYLN×326, WDC×2, ADUR×82, ALGM×30 | $10,874.99 | +63.54 | -167.62 | DVN, OCC, ALM, NEWP | SLG, LDI, BTBT, ANGX, HYLN, WDC, ADUR, ALGM | $26.34 | $10,648.68 | DVN×58, OCC×148, ALM×167, NEWP×390 |
| 2026-08-18 | -6.20 | $26.34 | DVN×58, OCC×148, ALM×167, NEWP×390 | $10,382.10 | -266.58 | +0.00 | — | DVN, OCC, ALM, NEWP | $10,369.77 | $10,369.77 | — |
| 2026-08-19 | -7.20 | $10,369.77 | — | $10,369.77 | +0.00 | +0.00 | — | — | $10,369.77 | $10,369.77 | — |
| 2026-08-20 | +1.12 | $10,369.77 | — | $10,369.77 | +0.00 | +110.67 | AG, HDSN, IAG, KGC, NFGC, DNA, EXK, SCZM | — | $14.52 | $10,454.26 | AG×63, HDSN×224, IAG×66, KGC×43, NFGC×740, DNA×173, EXK×120, SCZM×137 |
| 2026-08-21 | +3.25 | $14.52 | AG×63, HDSN×224, IAG×66, KGC×43, NFGC×740, DNA×173, EXK×120, SCZM×137 | $10,762.42 | +308.16 | -210.84 | BTBT, ORBS, EMBC, TXG, DXYZ | AG, HDSN, IAG, KGC, NFGC, DNA, EXK, SCZM | $24.39 | $10,470.09 | BTBT×1293, ORBS×2485, EMBC×395, TXG×33, DXYZ×60 |
| 2026-08-24 | -5.17 | $24.39 | BTBT×1293, ORBS×2485, EMBC×395, TXG×33, DXYZ×60 | $10,366.65 | -103.44 | +0.00 | — | BTBT, ORBS, EMBC, TXG, DXYZ | $10,310.25 | $10,310.25 | — |
| 2026-08-25 | +1.80 | $10,310.25 | — | $10,310.25 | -0.00 | +126.87 | HCA, ALIT, ZURA, KURA, EZPW, CTKB, BZ, VIPS | — | $28.62 | $10,417.75 | HCA×3, ALIT×86, ZURA×202, KURA×96, EZPW×37, CTKB×281, BZ×84, VIPS×92 |
| 2026-08-26 | +2.02 | $28.62 | HCA×3, ALIT×86, ZURA×202, KURA×96, EZPW×37, CTKB×281, BZ×84, VIPS×92 | $10,417.75 | +0.00 | +0.00 | — | — | $28.62 | $10,417.75 | HCA×3, ALIT×86, ZURA×202, KURA×96, EZPW×37, CTKB×281, BZ×84, VIPS×92 |
| 2026-08-27 | — | $28.62 | HCA×3, ALIT×86, ZURA×202, KURA×96, EZPW×37, CTKB×281, BZ×84, VIPS×92 | $10,425.47 | +7.72 | +184.52 | RRC, CRK, SLI, DLO, GEN, PGY, PLTR | HCA, ALIT, ZURA, KURA, EZPW, CTKB, BZ, VIPS | $163.83 | $10,569.97 | RRC×36, CRK×105, SLI×573, DLO×95, GEN×51, PGY×67, PLTR×8 |
| 2026-08-28 | +0.75 | $163.83 | RRC×36, CRK×105, SLI×573, DLO×95, GEN×51, PGY×67, PLTR×8 | $10,603.56 | +33.59 | -174.73 | ANF, BZ, GENB, CLYM, MNRO | DLO, GEN, PGY, PLTR | $88.93 | $10,409.21 | RRC×36, CRK×105, SLI×573, ANF×8, BZ×65, GENB×71, CLYM×75, MNRO×97 |
| 2026-08-31 | -5.85 | $88.93 | RRC×36, CRK×105, SLI×573, ANF×8, BZ×65, GENB×71, CLYM×75, MNRO×97 | $10,332.43 | -76.78 | +0.00 | — | RRC, CRK, SLI, ANF, BZ, GENB, CLYM, MNRO | $10,309.47 | $10,309.47 | — |
| 2026-09-01 | -6.30 | $10,309.47 | — | $10,309.47 | +0.00 | +0.00 | — | — | $10,309.47 | $10,309.47 | — |
| 2026-09-02 | -3.83 | $10,309.47 | — | $10,309.47 | +0.00 | +0.00 | — | — | $10,309.47 | $10,309.47 | — |
| 2026-09-03 | -0.90 | $10,309.47 | — | $10,309.47 | +0.00 | +585.32 | RVTY, GPRO, CRK, MMED, CLYM, CNXC, VIR, CDXS | — | $35.56 | $10,857.15 | RVTY×10, GPRO×1056, CRK×82, MMED×56, CLYM×87, CNXC×40, VIR×110, CDXS×847 |
| 2026-09-04 | — | $35.56 | RVTY×10, GPRO×1056, CRK×82, MMED×56, CLYM×87, CNXC×40, VIR×110, CDXS×847 | $10,896.60 | +39.45 | -516.12 | BVS, FMC, TARS, PLAY, ASAN, GWRE, LULU | RVTY, CRK, MMED, CLYM, CNXC, VIR, CDXS | $221.06 | $10,340.80 | GPRO×1056, BVS×88, FMC×96, TARS×15, PLAY×137, ASAN×126, GWRE×6, LULU×10 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 197 | $50.62 | $2.58 | — | $24.65 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ⚪; ret5=+6.2; leftover $10000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.65 | ▲ close $10,784.79 vs 09:30 $10,000.00 (session +787.37) | 16:00 close · cash $24.65 · equity $10,784.79 vs 09:30 $10,000.00 (+784.79; session marks +787.37) · 1 name(s) marked open→close (per-name table). TPG×197 09:30 $50.62 → close $54.62 +787.37 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.65 | ▲ 09:30 equity $10,916.78 vs yday $10,784.79 (+131.99) | 09:30 open · cash $24.65 (unchanged overnight, no fees) · equity $10,916.78 vs prior close $10,784.79 (+131.99) · 1 name(s) re-marked at the open (per-name table). TPG×197 yday $54.62 → 09:30 $55.29 +131.99 | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 197 | $55.29 | $2.70 | $+914.08 | $10,914.08 | ▲ +914.08 after sell → book $10,914.08; vs 09:30 mark -2.70 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 23 | $57.61 | $2.06 | — | $9,586.99 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ret5=+5.7; leftover $1364.26 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1455 | $0.94 | $18.00 | — | $8,205.66 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1364.26 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 909 | $1.50 | $11.73 | — | $6,830.43 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1364.26 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 316 | $4.31 | $4.08 | — | $5,464.39 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1364.26 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 326 | $4.18 | $4.21 | — | $4,097.51 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1364.26 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 2 | $503.50 | $2.00 | — | $3,088.51 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable; 🔵; ⚪; ret5=+7.9; leftover $1364.26 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 82 | $16.50 | $2.24 | — | $1,733.28 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1364.26 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ALGM` | 30 | $44.06 | $2.08 | — | $409.40 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable; 🔵; ret5=+3.9; leftover $1364.26 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $409.40 | ▼ close $10,811.45 vs 09:30 $10,916.78 (session -56.25) | 16:00 close · cash $409.40 · equity $10,811.45 vs 09:30 $10,916.78 (-105.33; session marks -56.25) · 8 name(s) marked open→close (per-name table). SLG×23 09:30 $57.61 → close $56.09 -34.96; LDI×1455 09:30 $0.94 → close $0.90 -58.20; BTBT×909 09:30 $1.50 → close $1.57 +63.63; ANGX×316 09:30 $4.31 → close $4.37 +18.96; HYLN×326 09:30 $4.18 → close $4.06 -39.12; WDC×2 09:30 $503.50 → close $508.80 +10.60; ADUR×82 09:30 $16.50 → close $16.17 -27.06; ALGM×30 09:30 $44.06 → close $44.39 +9.90 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $409.40 | ▲ 09:30 equity $10,874.99 vs yday $10,811.45 (+63.54) | 09:30 open · cash $409.40 (unchanged overnight, no fees) · equity $10,874.99 vs prior close $10,811.45 (+63.54) · 8 name(s) re-marked at the open (per-name table). SLG×23 yday $56.09 → 09:30 $55.37 -16.56; LDI×1455 yday $0.90 → 09:30 $0.91 +14.55; BTBT×909 yday $1.57 → 09:30 $1.52 -45.45; ANGX×316 yday $4.37 → 09:30 $4.60 +72.68; HYLN×326 yday $4.06 → 09:30 $4.10 +13.04; WDC×2 yday $508.80 → 09:30 $525.53 +33.46; ADUR×82 yday $16.17 → 09:30 $15.73 -36.08; ALGM×30 yday $44.39 → 09:30 $45.32 +27.90 | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 23 | $55.37 | $2.08 | $-55.66 | $1,680.83 | ▼ -55.66 after sell → book $10,872.91; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 1455 | $0.91 | $17.81 | $-79.46 | $2,982.70 | ▼ -79.46 after sell → book $10,855.10; vs 09:30 mark -17.81 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 909 | $1.52 | $11.89 | $-5.43 | $4,352.49 | ▼ -5.43 after sell → book $10,843.21; vs 09:30 mark -11.89 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 316 | $4.60 | $4.14 | $+83.42 | $5,801.95 | ▲ +83.42 after sell → book $10,839.07; vs 09:30 mark -4.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 326 | $4.10 | $4.27 | $-34.56 | $7,134.28 | ▼ -34.56 after sell → book $10,834.80; vs 09:30 mark -4.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 2 | $525.53 | $2.02 | $+40.05 | $8,183.32 | ▲ +40.05 after sell → book $10,832.78; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 82 | $15.73 | $2.26 | $-67.64 | $9,470.92 | ▼ -67.64 after sell → book $10,830.52; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ALGM` | 30 | $45.32 | $2.10 | $+33.62 | $10,828.42 | ▲ +33.62 after sell → book $10,828.42; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 58 | $46.18 | $2.16 | — | $8,147.82 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ret5=+6.7; leftover $2707.11 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 148 | $18.24 | $2.43 | — | $5,445.86 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $2707.11 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 167 | $16.20 | $2.49 | — | $2,737.97 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $2707.11 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NEWP` | 390 | $6.94 | $5.03 | — | $26.34 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.1; leftover $2707.11 | join🟡 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26.34 | ▼ close $10,648.68 vs 09:30 $10,874.99 (session -167.62) | 16:00 close · cash $26.34 · equity $10,648.68 vs 09:30 $10,874.99 (-226.31; session marks -167.62) · 4 name(s) marked open→close (per-name table). DVN×58 09:30 $46.18 → close $47.57 +80.62; OCC×148 09:30 $18.24 → close $17.12 -165.76; ALM×167 09:30 $16.20 → close $16.36 +26.72; NEWP×390 09:30 $6.94 → close $6.66 -109.20 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26.34 | ▼ 09:30 equity $10,382.10 vs yday $10,648.68 (-266.58) | 09:30 open · cash $26.34 (unchanged overnight, no fees) · equity $10,382.10 vs prior close $10,648.68 (-266.58) · 4 name(s) re-marked at the open (per-name table). DVN×58 yday $47.57 → 09:30 $48.00 +24.94; OCC×148 yday $17.12 → 09:30 $16.20 -136.16; ALM×167 yday $16.36 → 09:30 $15.78 -96.86; NEWP×390 yday $6.66 → 09:30 $6.51 -58.50 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 58 | $48.00 | $2.20 | $+101.20 | $2,808.15 | ▲ +101.20 after sell → book $10,379.91; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 148 | $16.20 | $2.48 | $-306.83 | $5,203.27 | ▼ -306.83 after sell → book $10,377.43; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 167 | $15.78 | $2.54 | $-75.17 | $7,835.99 | ▼ -75.17 after sell → book $10,374.89; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `NEWP` | 390 | $6.51 | $5.12 | $-177.85 | $10,369.77 | ▼ -177.85 after sell → book $10,369.77; vs 09:30 mark -5.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,369.77 | ▲ close $10,369.77 vs 09:30 $10,382.10 (session +0.00) | 16:00 close · cash $10,369.77 · no lots left · equity $10,369.77. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,369.77 | ▲ 09:30 equity $10,369.77 vs yday $10,369.77 (+0.00) | 09:30 open · cash $10,369.77 · no holdings · equity $10,369.77 vs prior close $10,369.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,369.77 | ▲ close $10,369.77 vs 09:30 $10,369.77 (session +0.00) | 16:00 close · cash $10,369.77 · no lots left · equity $10,369.77. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,369.77 | ▲ 09:30 equity $10,369.77 vs yday $10,369.77 (+0.00) | 09:30 open · cash $10,369.77 · no holdings · equity $10,369.77 vs prior close $10,369.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 63 | $20.55 | $2.18 | — | $9,072.94 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1296.22 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 224 | $5.77 | $2.89 | — | $7,777.57 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1296.22 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 66 | $19.63 | $2.19 | — | $6,479.81 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1296.22 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 43 | $29.63 | $2.12 | — | $5,203.60 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1296.22 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 740 | $1.75 | $9.55 | — | $3,899.05 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1296.22 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 173 | $7.45 | $2.51 | — | $2,607.69 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1296.22 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 120 | $10.77 | $2.35 | — | $1,312.94 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1296.22 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 137 | $9.46 | $2.40 | — | $14.52 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1296.22 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.52 | ▲ close $10,454.26 vs 09:30 $10,369.77 (session +110.67) | 16:00 close · cash $14.52 · equity $10,454.26 vs 09:30 $10,369.77 (+84.49; session marks +110.67) · 8 name(s) marked open→close (per-name table). AG×63 09:30 $20.55 → close $21.19 +40.32; HDSN×224 09:30 $5.77 → close $5.57 -44.80; IAG×66 09:30 $19.63 → close $20.50 +57.42; KGC×43 09:30 $29.63 → close $31.43 +77.40; NFGC×740 09:30 $1.75 → close $1.75 +0.00; DNA×173 09:30 $7.45 → close $6.96 -84.77; EXK×120 09:30 $10.77 → close $10.97 +24.00; SCZM×137 09:30 $9.46 → close $9.76 +41.10 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.52 | ▲ 09:30 equity $10,762.42 vs yday $10,454.26 (+308.16) | 09:30 open · cash $14.52 (unchanged overnight, no fees) · equity $10,762.42 vs prior close $10,454.26 (+308.16) · 8 name(s) re-marked at the open (per-name table). AG×63 yday $21.19 → 09:30 $21.90 +44.73; HDSN×224 yday $5.57 → 09:30 $5.67 +22.40; IAG×66 yday $20.50 → 09:30 $21.17 +44.22; KGC×43 yday $31.43 → 09:30 $32.17 +31.82; NFGC×740 yday $1.75 → 09:30 $1.79 +29.60; DNA×173 yday $6.96 → 09:30 $7.09 +22.49; EXK×120 yday $10.97 → 09:30 $11.34 +44.40; SCZM×137 yday $9.76 → 09:30 $10.26 +68.50 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 63 | $21.90 | $2.20 | $+80.67 | $1,392.02 | ▲ +80.67 after sell → book $10,760.22; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 224 | $5.67 | $2.94 | $-28.23 | $2,659.16 | ▼ -28.23 after sell → book $10,757.28; vs 09:30 mark -2.94 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 66 | $21.17 | $2.21 | $+97.24 | $4,054.17 | ▲ +97.24 after sell → book $10,755.07; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 43 | $32.17 | $2.14 | $+104.96 | $5,435.34 | ▲ +104.96 after sell → book $10,752.93; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 740 | $1.79 | $9.68 | $+10.37 | $6,750.26 | ▲ +10.37 after sell → book $10,743.25; vs 09:30 mark -9.68 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DNA` | 173 | $7.09 | $2.55 | $-67.34 | $7,974.29 | ▼ -67.34 after sell → book $10,740.71; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `EXK` | 120 | $11.34 | $2.38 | $+63.67 | $9,332.70 | ▲ +63.67 after sell → book $10,738.32; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SCZM` | 137 | $10.26 | $2.44 | $+104.76 | $10,735.89 | ▲ +104.76 after sell → book $10,735.89; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 1293 | $1.66 | $16.68 | — | $8,572.83 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $2147.18 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 2485 | $0.86 | $28.93 | — | $6,396.86 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $2147.18 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `EMBC` | 395 | $5.43 | $5.10 | — | $4,246.92 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+7.0; leftover $2147.18 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `TXG` | 33 | $64.39 | $2.09 | — | $2,119.96 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $2147.18 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DXYZ` | 60 | $34.89 | $2.17 | — | $24.39 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.6; leftover $2147.18 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 vol🔴 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.39 | ▼ close $10,470.09 vs 09:30 $10,762.42 (session -210.84) | 16:00 close · cash $24.39 · equity $10,470.09 vs 09:30 $10,762.42 (-292.33; session marks -210.84) · 5 name(s) marked open→close (per-name table). BTBT×1293 09:30 $1.66 → close $1.53 -168.09; ORBS×2485 09:30 $0.86 → close $0.88 +39.76; EMBC×395 09:30 $5.43 → close $5.23 -79.00; TXG×33 09:30 $64.39 → close $65.12 +24.09; DXYZ×60 09:30 $34.89 → close $34.43 -27.60 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.39 | ▼ 09:30 equity $10,366.65 vs yday $10,470.09 (-103.44) | 09:30 open · cash $24.39 (unchanged overnight, no fees) · equity $10,366.65 vs prior close $10,470.09 (-103.44) · 5 name(s) re-marked at the open (per-name table). BTBT×1293 yday $1.53 → 09:30 $1.55 +25.86; ORBS×2485 yday $0.88 → 09:30 $0.89 +24.85; EMBC×395 yday $5.23 → 09:30 $5.21 -7.90; TXG×33 yday $65.12 → 09:30 $63.07 -67.65; DXYZ×60 yday $34.43 → 09:30 $33.12 -78.60 | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 1293 | $1.55 | $16.91 | $-175.82 | $2,011.63 | ▼ -175.82 after sell → book $10,349.74; vs 09:30 mark -16.91 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 2485 | $0.89 | $30.00 | $+5.68 | $4,193.28 | ▲ +5.68 after sell → book $10,319.74; vs 09:30 mark -30.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `EMBC` | 395 | $5.21 | $5.18 | $-97.17 | $6,246.05 | ▼ -97.17 after sell → book $10,314.56; vs 09:30 mark -5.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TXG` | 33 | $63.07 | $2.12 | $-47.76 | $8,325.24 | ▼ -47.76 after sell → book $10,312.44; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `DXYZ` | 60 | $33.12 | $2.20 | $-110.57 | $10,310.25 | ▼ -110.57 after sell → book $10,310.25; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,310.25 | ▲ close $10,310.25 vs 09:30 $10,366.65 (session +0.00) | 16:00 close · cash $10,310.25 · no lots left · equity $10,310.25. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,310.25 | ▲ 09:30 equity $10,310.25 vs yday $10,310.25 (-0.00) | 09:30 open · cash $10,310.25 · no holdings · equity $10,310.25 vs prior close $10,310.25 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $429.24 | $2.00 | — | $9,020.53 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+6.1; leftover $1288.78 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALIT` | 86 | $14.86 | $2.25 | — | $7,740.32 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+3.0; leftover $1288.78 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 202 | $6.38 | $2.61 | — | $6,448.96 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1288.78 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 96 | $13.30 | $2.28 | — | $5,169.88 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+9.5; leftover $1288.78 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 37 | $34.48 | $2.10 | — | $3,892.02 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1288.78 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CTKB` | 281 | $4.58 | $3.62 | — | $2,601.41 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; 🔵; ret5=+2.6; leftover $1288.78 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 84 | $15.34 | $2.24 | — | $1,310.61 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=+2.8; leftover $1288.78 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `VIPS` | 92 | $13.91 | $2.27 | — | $28.62 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=+2.5; leftover $1288.78 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.62 | ▲ close $10,417.75 vs 09:30 $10,310.25 (session +126.87) | 16:00 close · cash $28.62 · equity $10,417.75 vs 09:30 $10,310.25 (+107.50; session marks +126.87) · 8 name(s) marked open→close (per-name table). HCA×3 09:30 $429.24 → close $428.50 -2.22; ALIT×86 09:30 $14.86 → close $14.87 +0.86; ZURA×202 09:30 $6.38 → close $6.50 +24.24; KURA×96 09:30 $13.30 → close $13.58 +26.88; EZPW×37 09:30 $34.48 → close $34.69 +7.77; CTKB×281 09:30 $4.58 → close $4.56 -5.62; BZ×84 09:30 $15.34 → close $16.32 +82.32; VIPS×92 09:30 $13.91 → close $13.83 -7.36 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.62 | ▲ 09:30 equity $10,417.75 vs yday $10,417.75 (+0.00) | 09:30 open · cash $28.62 (unchanged overnight, no fees) · equity $10,417.75 vs prior close $10,417.75 (+0.00) · 8 name(s) re-marked at the open (per-name table). HCA×3 yday $428.50 → 09:30 $428.50 +0.00; ALIT×86 yday $14.87 → 09:30 $14.87 +0.00; ZURA×202 yday $6.50 → 09:30 $6.50 +0.00; KURA×96 yday $13.58 → 09:30 $13.58 +0.00; EZPW×37 yday $34.69 → 09:30 $34.69 +0.00; CTKB×281 yday $4.56 → 09:30 $4.56 +0.00; BZ×84 yday $16.32 → 09:30 $16.32 +0.00; VIPS×92 yday $13.83 → 09:30 $13.83 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.62 | ▲ close $10,417.75 vs 09:30 $10,417.75 (session +0.00) | 16:00 close · cash $28.62 · equity $10,417.75 vs 09:30 $10,417.75 (+0.00; session marks +0.00) · 8 name(s) marked open→close (per-name table). HCA×3 09:30 $428.50 → close $428.50 +0.00; ALIT×86 09:30 $14.87 → close $14.87 +0.00; ZURA×202 09:30 $6.50 → close $6.50 +0.00; KURA×96 09:30 $13.58 → close $13.58 +0.00; EZPW×37 09:30 $34.69 → close $34.69 +0.00; CTKB×281 09:30 $4.56 → close $4.56 +0.00; BZ×84 09:30 $16.32 → close $16.32 +0.00; VIPS×92 09:30 $13.83 → close $13.83 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.62 | ▲ 09:30 equity $10,425.47 vs yday $10,417.75 (+7.72) | 09:30 open · cash $28.62 (unchanged overnight, no fees) · equity $10,425.47 vs prior close $10,417.75 (+7.72) · 8 name(s) re-marked at the open (per-name table). HCA×3 yday $428.50 → 09:30 $427.50 -3.00; ALIT×86 yday $14.87 → 09:30 $14.85 -1.72; ZURA×202 yday $6.50 → 09:30 $6.13 -74.74; KURA×96 yday $13.58 → 09:30 $13.63 +4.80; EZPW×37 yday $34.69 → 09:30 $35.70 +37.37; CTKB×281 yday $4.56 → 09:30 $4.53 -8.43; BZ×84 yday $16.32 → 09:30 $16.77 +37.80; VIPS×92 yday $13.83 → 09:30 $14.00 +15.64 | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 3 | $427.50 | $2.02 | $-9.24 | $1,309.10 | ▼ -9.24 after sell → book $10,423.45; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALIT` | 86 | $14.85 | $2.27 | $-5.38 | $2,583.93 | ▼ -5.38 after sell → book $10,421.18; vs 09:30 mark -2.27 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ZURA` | 202 | $6.13 | $2.65 | $-55.76 | $3,819.54 | ▼ -55.76 after sell → book $10,418.53; vs 09:30 mark -2.65 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `KURA` | 96 | $13.63 | $2.30 | $+27.10 | $5,125.72 | ▲ +27.10 after sell → book $10,416.23; vs 09:30 mark -2.30 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `EZPW` | 37 | $35.70 | $2.12 | $+40.92 | $6,444.50 | ▲ +40.92 after sell → book $10,414.11; vs 09:30 mark -2.12 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CTKB` | 281 | $4.53 | $3.68 | $-21.36 | $7,713.75 | ▼ -21.36 after sell → book $10,410.43; vs 09:30 mark -3.68 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BZ` | 84 | $16.77 | $2.27 | $+115.61 | $9,120.16 | ▲ +115.61 after sell → book $10,408.16; vs 09:30 mark -2.27 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `VIPS` | 92 | $14.00 | $2.29 | $+3.72 | $10,405.87 | ▲ +3.72 after sell → book $10,405.87; vs 09:30 mark -2.29 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 36 | $40.72 | $2.10 | — | $8,937.85 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+1.8; leftover $1486.55 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 105 | $14.09 | $2.31 | — | $7,456.09 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+1.1; leftover $1486.55 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 573 | $2.59 | $7.39 | — | $5,964.63 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+4.2; leftover $1486.55 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 95 | $15.60 | $2.27 | — | $4,480.36 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list mover_buy; 🔵; ret5=+7.1; leftover $1486.55 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 51 | $28.89 | $2.14 | — | $3,004.82 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list mover_buy; 🔵; ret5=+1.6; leftover $1486.55 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PGY` | 67 | $21.97 | $2.19 | — | $1,530.64 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list mover_buy; 🔵; ret5=+0.6; leftover $1486.55 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PLTR` | 8 | $170.60 | $2.01 | — | $163.83 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list mover_buy; 🔵; ret5=+3.4; leftover $1486.55 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $163.83 | ▲ close $10,569.97 vs 09:30 $10,425.47 (session +184.52) | 16:00 close · cash $163.83 · equity $10,569.97 vs 09:30 $10,425.47 (+144.50; session marks +184.52) · 7 name(s) marked open→close (per-name table). RRC×36 09:30 $40.72 → close $41.55 +29.88; CRK×105 09:30 $14.09 → close $14.50 +43.05; SLI×573 09:30 $2.59 → close $2.61 +11.46; DLO×95 09:30 $15.60 → close $15.36 -22.80; GEN×51 09:30 $28.89 → close $29.64 +38.25; PGY×67 09:30 $21.97 → close $22.41 +29.48; PLTR×8 09:30 $170.60 → close $177.50 +55.20 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $163.83 | ▲ 09:30 equity $10,603.56 vs yday $10,569.97 (+33.59) | 09:30 open · cash $163.83 (unchanged overnight, no fees) · equity $10,603.56 vs prior close $10,569.97 (+33.59) · 7 name(s) re-marked at the open (per-name table). RRC×36 yday $41.55 → 09:30 $41.44 -3.96; CRK×105 yday $14.50 → 09:30 $14.42 -8.40; SLI×573 yday $2.61 → 09:30 $2.60 -5.73; DLO×95 yday $15.36 → 09:30 $15.33 -2.85; GEN×51 yday $29.64 → 09:30 $29.83 +9.69; PGY×67 yday $22.41 → 09:30 $22.93 +34.84; PLTR×8 yday $177.50 → 09:30 $178.75 +10.00 | — |
| 2026-08-28 09:30 ET | **SELL** | `DLO` | 95 | $15.33 | $2.30 | $-30.23 | $1,617.88 | ▼ -30.23 after sell → book $10,601.26; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 51 | $29.83 | $2.17 | $+43.63 | $3,137.04 | ▲ +43.63 after sell → book $10,599.09; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `PGY` | 67 | $22.93 | $2.21 | $+59.91 | $4,671.14 | ▲ +59.91 after sell → book $10,596.88; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `PLTR` | 8 | $178.75 | $2.04 | $+61.15 | $6,099.10 | ▲ +61.15 after sell → book $10,594.84; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $144.70 | $2.01 | — | $4,939.49 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1219.82 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 65 | $18.50 | $2.19 | — | $3,734.80 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1219.82 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `GENB` | 71 | $17.10 | $2.20 | — | $2,518.50 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; ret5=+3.1; leftover $1219.82 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CLYM` | 75 | $16.09 | $2.21 | — | $1,309.53 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; ret5=+5.8; leftover $1219.82 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MNRO` | 97 | $12.56 | $2.28 | — | $88.93 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; ret5=+9.3; leftover $1219.82 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $88.93 | ▼ close $10,409.21 vs 09:30 $10,603.56 (session -174.73) | 16:00 close · cash $88.93 · equity $10,409.21 vs 09:30 $10,603.56 (-194.35; session marks -174.73) · 8 name(s) marked open→close (per-name table). RRC×36 09:30 $41.44 → close $41.64 +7.20; CRK×105 09:30 $14.42 → close $14.62 +21.00; SLI×573 09:30 $2.60 → close $2.64 +22.92; ANF×8 09:30 $144.70 → close $145.75 +8.40; BZ×65 09:30 $18.50 → close $18.00 -32.50; GENB×71 09:30 $17.10 → close $15.77 -94.43; CLYM×75 09:30 $16.09 → close $15.06 -77.25; MNRO×97 09:30 $12.56 → close $12.25 -30.07 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $88.93 | ▼ 09:30 equity $10,332.43 vs yday $10,409.21 (-76.78) | 09:30 open · cash $88.93 (unchanged overnight, no fees) · equity $10,332.43 vs prior close $10,409.21 (-76.78) · 8 name(s) re-marked at the open (per-name table). RRC×36 yday $41.64 → 09:30 $41.11 -19.08; CRK×105 yday $14.62 → 09:30 $14.56 -6.30; SLI×573 yday $2.64 → 09:30 $2.51 -74.49; ANF×8 yday $145.75 → 09:30 $148.67 +23.36; BZ×65 yday $18.00 → 09:30 $17.89 -7.15; GENB×71 yday $15.77 → 09:30 $15.33 -31.24; CLYM×75 yday $15.06 → 09:30 $14.65 -30.75; MNRO×97 yday $12.25 → 09:30 $12.96 +68.87 | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 36 | $41.11 | $2.12 | $+9.82 | $1,566.77 | ▲ +9.82 after sell → book $10,330.31; vs 09:30 mark -2.12 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 105 | $14.56 | $2.33 | $+44.71 | $3,093.24 | ▲ +44.71 after sell → book $10,327.98; vs 09:30 mark -2.33 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 573 | $2.51 | $7.50 | $-60.73 | $4,523.97 | ▼ -60.73 after sell → book $10,320.48; vs 09:30 mark -7.50 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 8 | $148.67 | $2.03 | $+27.71 | $5,711.30 | ▲ +27.71 after sell → book $10,318.45; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 65 | $17.89 | $2.21 | $-44.04 | $6,871.94 | ▼ -44.04 after sell → book $10,316.24; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GENB` | 71 | $15.33 | $2.22 | $-130.10 | $7,958.15 | ▼ -130.10 after sell → book $10,314.02; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CLYM` | 75 | $14.65 | $2.24 | $-112.45 | $9,054.66 | ▼ -112.45 after sell → book $10,311.78; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MNRO` | 97 | $12.96 | $2.31 | $+34.21 | $10,309.47 | ▲ +34.21 after sell → book $10,309.47; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,309.47 | ▲ close $10,309.47 vs 09:30 $10,332.43 (session +0.00) | 16:00 close · cash $10,309.47 · no lots left · equity $10,309.47. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,309.47 | ▲ 09:30 equity $10,309.47 vs yday $10,309.47 (+0.00) | 09:30 open · cash $10,309.47 · no holdings · equity $10,309.47 vs prior close $10,309.47 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,309.47 | ▲ close $10,309.47 vs 09:30 $10,309.47 (session +0.00) | 16:00 close · cash $10,309.47 · no lots left · equity $10,309.47. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,309.47 | ▲ 09:30 equity $10,309.47 vs yday $10,309.47 (+0.00) | 09:30 open · cash $10,309.47 · no holdings · equity $10,309.47 vs prior close $10,309.47 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,309.47 | ▲ close $10,309.47 vs 09:30 $10,309.47 (session +0.00) | 16:00 close · cash $10,309.47 · no lots left · equity $10,309.47. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,309.47 | ▲ 09:30 equity $10,309.47 vs yday $10,309.47 (+0.00) | 09:30 open · cash $10,309.47 · no holdings · equity $10,309.47 vs prior close $10,309.47 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $125.94 | $2.02 | — | $9,048.05 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1288.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1056 | $1.22 | $13.62 | — | $7,746.11 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1288.68 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 82 | $15.70 | $2.24 | — | $6,456.47 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1288.68 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 56 | $22.78 | $2.16 | — | $5,178.63 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1288.68 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CLYM` | 87 | $14.79 | $2.25 | — | $3,889.65 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+5.8; leftover $1288.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 40 | $31.80 | $2.11 | — | $2,615.54 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+3.7; leftover $1288.68 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VIR` | 110 | $11.63 | $2.32 | — | $1,333.92 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+5.8; leftover $1288.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CDXS` | 847 | $1.52 | $10.93 | — | $35.56 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; ret5=+7.1; leftover $1288.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.56 | ▲ close $10,857.15 vs 09:30 $10,309.47 (session +585.32) | 16:00 close · cash $35.56 · equity $10,857.15 vs 09:30 $10,309.47 (+547.68; session marks +585.32) · 8 name(s) marked open→close (per-name table). RVTY×10 09:30 $125.94 → close $130.94 +50.00; GPRO×1056 09:30 $1.22 → close $1.69 +496.32; CRK×82 09:30 $15.70 → close $15.54 -13.12; MMED×56 09:30 $22.78 → close $23.76 +54.88; CLYM×87 09:30 $14.79 → close $15.05 +22.62; CNXC×40 09:30 $31.80 → close $32.37 +22.80; VIR×110 09:30 $11.63 → close $11.50 -14.30; CDXS×847 09:30 $1.52 → close $1.48 -33.88 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.56 | ▲ 09:30 equity $10,896.60 vs yday $10,857.15 (+39.45) | 09:30 open · cash $35.56 (unchanged overnight, no fees) · equity $10,896.60 vs prior close $10,857.15 (+39.45) · 8 name(s) re-marked at the open (per-name table). RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; GPRO×1056 yday $1.69 → 09:30 $1.78 +95.04; CRK×82 yday $15.54 → 09:30 $15.45 -7.38; MMED×56 yday $23.76 → 09:30 $23.88 +6.72; CLYM×87 yday $15.05 → 09:30 $13.96 -94.83; CNXC×40 yday $32.37 → 09:30 $32.88 +20.40; VIR×110 yday $11.50 → 09:30 $11.54 +4.40; CDXS×847 yday $1.48 → 09:30 $1.48 +0.00 | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $132.45 | $2.04 | $+61.04 | $1,358.02 | ▲ +61.04 after sell → book $10,894.56; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 82 | $15.45 | $2.26 | $-25.00 | $2,622.66 | ▼ -25.00 after sell → book $10,892.30; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 56 | $23.88 | $2.18 | $+57.26 | $3,957.76 | ▲ +57.26 after sell → book $10,890.12; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CLYM` | 87 | $13.96 | $2.28 | $-76.74 | $5,170.00 | ▼ -76.74 after sell → book $10,887.84; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNXC` | 40 | $32.88 | $2.13 | $+38.96 | $6,483.07 | ▲ +38.96 after sell → book $10,885.71; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VIR` | 110 | $11.54 | $2.35 | $-14.57 | $7,750.12 | ▼ -14.57 after sell → book $10,883.36; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CDXS` | 847 | $1.48 | $11.08 | $-55.88 | $8,992.61 | ▼ -55.88 after sell → book $10,872.29; vs 09:30 mark -11.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 88 | $14.50 | $2.25 | — | $7,714.35 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1284.66 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `FMC` | 96 | $13.30 | $2.28 | — | $6,435.28 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer,yday_mover; ret5=+8.6; leftover $1284.66 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 15 | $82.76 | $2.04 | — | $5,191.84 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+5.1; leftover $1284.66 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `PLAY` | 137 | $9.36 | $2.40 | — | $3,907.12 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+0.6; leftover $1284.66 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 126 | $10.16 | $2.37 | — | $2,624.59 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list earn_react; 🔵; ret5=+4.8; leftover $1284.66 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 6 | $198.00 | $2.01 | — | $1,434.58 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list earn_react; ret5=+7.7; leftover $1284.66 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 10 | $121.15 | $2.02 | — | $221.06 | — | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list earn_react; ret5=+1.3; leftover $1284.66 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $221.06 | ▼ close $10,340.80 vs 09:30 $10,896.60 (session -516.12) | 16:00 close · cash $221.06 · equity $10,340.80 vs 09:30 $10,896.60 (-555.80; session marks -516.12) · 8 name(s) marked open→close (per-name table). GPRO×1056 09:30 $1.78 → close $1.39 -411.84; BVS×88 09:30 $14.50 → close $14.36 -12.32; FMC×96 09:30 $13.30 → close $12.98 -30.72; TARS×15 09:30 $82.76 → close $83.21 +6.75; PLAY×137 09:30 $9.36 → close $8.67 -94.53; ASAN×126 09:30 $10.16 → close $10.09 -8.82; GWRE×6 09:30 $198.00 → close $202.86 +29.16; LULU×10 09:30 $121.15 → close $121.77 +6.20 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
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
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABAT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `ALIT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ZURA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `EZPW` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CTKB` | no_price | no 09:30 open — carry |
| 2026-08-26 | `VIPS` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `OSUR` | no_price | no 09:30 open |
| 2026-08-26 | `ANF` | no_price | no 09:30 open |
| 2026-08-26 | `INTU` | no_price | no 09:30 open |
| 2026-08-26 | `SJM` | no_price | no 09:30 open |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DINO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DLO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `VFF` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CRK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HELP` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ACB` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GPRO` | 1056 | 2026-09-03 @ $1.22 | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1288.68 |
| `BVS` | 88 | 2026-09-04 @ $14.50 | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1284.66 |
| `FMC` | 96 | 2026-09-04 @ $13.30 | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer,yday_mover; ret5=+8.6; leftover $1284.66 |
| `TARS` | 15 | 2026-09-04 @ $82.76 | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+5.1; leftover $1284.66 |
| `PLAY` | 137 | 2026-09-04 @ $9.36 | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+0.6; leftover $1284.66 |
| `ASAN` | 126 | 2026-09-04 @ $10.16 | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list earn_react; 🔵; ret5=+4.8; leftover $1284.66 |
| `GWRE` | 6 | 2026-09-04 @ $198.00 | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list earn_react; ret5=+7.7; leftover $1284.66 |
| `LULU` | 10 | 2026-09-04 @ $121.15 | combo gate; gate last_green=True,ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list earn_react; ret5=+1.3; leftover $1284.66 |
