# Factor mine action — `union_break10_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ break10, no 🚨

Cash book **+0.00%** ($10,000) · signal-only (no cash/fees) was -3.22%. Starts YES **11/17**. Fills 112 · skips 54 · realized $+23.70.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `break_10=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $43.00.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `IREN` | 217 | — | $45.98 | +0.00 | $44.76 | -264.74 | -264.74 | +0.00 | -264.74 |
| 2026-08-14 | `IREN` | 217 | $44.76 | $44.09 | -145.39 | — | +0.00 | -145.39 | -410.13 | — |
| 2026-08-14 | `SLG` | 20 | — | $57.61 | +0.00 | $56.09 | -30.40 | -30.40 | +0.00 | -30.40 |
| 2026-08-14 | `ADUR` | 72 | — | $16.50 | +0.00 | $16.17 | -23.76 | -23.76 | +0.00 | -23.76 |
| 2026-08-14 | `ARX` | 61 | — | $19.57 | +0.00 | $19.58 | +0.61 | +0.61 | +0.00 | +0.61 |
| 2026-08-14 | `AIRO` | 107 | — | $11.12 | +0.00 | $9.57 | -165.85 | -165.85 | +0.00 | -165.85 |
| 2026-08-14 | `TBBB` | 24 | — | $48.82 | +0.00 | $47.79 | -24.72 | -24.72 | +0.00 | -24.72 |
| 2026-08-14 | `AMPY` | 242 | — | $4.94 | +0.00 | $4.78 | -38.72 | -38.72 | +0.00 | -38.72 |
| 2026-08-14 | `MH` | 88 | — | $13.55 | +0.00 | $13.10 | -39.60 | -39.60 | +0.00 | -39.60 |
| 2026-08-17 | `SLG` | 20 | $56.09 | $55.37 | -14.40 | — | +0.00 | -14.40 | -44.80 | — |
| 2026-08-17 | `ADUR` | 72 | $16.17 | $15.73 | -31.68 | — | +0.00 | -31.68 | -55.44 | — |
| 2026-08-17 | `ARX` | 61 | $19.58 | $19.57 | -0.61 | — | +0.00 | -0.61 | +0.00 | — |
| 2026-08-17 | `AIRO` | 107 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -165.85 | — |
| 2026-08-17 | `TBBB` | 24 | $47.79 | $47.39 | -9.60 | — | +0.00 | -9.60 | -34.32 | — |
| 2026-08-17 | `AMPY` | 242 | $4.78 | $4.86 | +19.36 | — | +0.00 | +19.36 | -19.36 | — |
| 2026-08-17 | `MH` | 88 | $13.10 | $13.16 | +5.28 | — | +0.00 | +5.28 | -34.32 | — |
| 2026-08-17 | `DVN` | 24 | — | $46.18 | +0.00 | $47.57 | +33.36 | +33.36 | +0.00 | +33.36 |
| 2026-08-17 | `OCC` | 63 | — | $18.24 | +0.00 | $17.12 | -70.56 | -70.56 | +0.00 | -70.56 |
| 2026-08-17 | `ALM` | 70 | — | $16.20 | +0.00 | $16.36 | +11.20 | +11.20 | +0.00 | +11.20 |
| 2026-08-17 | `CAPR` | 167 | — | $6.87 | +0.00 | $7.45 | +96.86 | +96.86 | +0.00 | +96.86 |
| 2026-08-17 | `HTFL` | 27 | — | $41.23 | +0.00 | $41.94 | +19.17 | +19.17 | +0.00 | +19.17 |
| 2026-08-17 | `UMAC` | 35 | — | $32.55 | +0.00 | $30.15 | -84.00 | -84.00 | +0.00 | -84.00 |
| 2026-08-17 | `NPWR` | 598 | — | $1.92 | +0.00 | $1.73 | -113.62 | -113.62 | +0.00 | -113.62 |
| 2026-08-17 | `LPTH` | 76 | — | $14.94 | +0.00 | $14.80 | -10.64 | -10.64 | +0.00 | -10.64 |
| 2026-08-18 | `DVN` | 24 | $47.57 | $48.00 | +10.32 | — | +0.00 | +10.32 | +43.68 | — |
| 2026-08-18 | `OCC` | 63 | $17.12 | $16.20 | -57.96 | — | +0.00 | -57.96 | -128.52 | — |
| 2026-08-18 | `ALM` | 70 | $16.36 | $15.78 | -40.60 | — | +0.00 | -40.60 | -29.40 | — |
| 2026-08-18 | `CAPR` | 167 | $7.45 | $7.50 | +8.35 | — | +0.00 | +8.35 | +105.21 | — |
| 2026-08-18 | `HTFL` | 27 | $41.94 | $41.50 | -11.88 | — | +0.00 | -11.88 | +7.29 | — |
| 2026-08-18 | `UMAC` | 35 | $30.15 | $28.59 | -54.60 | — | +0.00 | -54.60 | -138.60 | — |
| 2026-08-18 | `NPWR` | 598 | $1.73 | $1.70 | -17.94 | — | +0.00 | -17.94 | -131.56 | — |
| 2026-08-18 | `LPTH` | 76 | $14.80 | $14.01 | -60.04 | — | +0.00 | -60.04 | -70.68 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 53 | — | $20.55 | +0.00 | $21.19 | +33.92 | +33.92 | +0.00 | +33.92 |
| 2026-08-20 | `BHP` | 12 | — | $91.01 | +0.00 | $93.63 | +31.44 | +31.44 | +0.00 | +31.44 |
| 2026-08-20 | `CDE` | 53 | — | $20.65 | +0.00 | $21.11 | +24.38 | +24.38 | +0.00 | +24.38 |
| 2026-08-20 | `IAG` | 56 | — | $19.63 | +0.00 | $20.50 | +48.72 | +48.72 | +0.00 | +48.72 |
| 2026-08-20 | `KGC` | 37 | — | $29.63 | +0.00 | $31.43 | +66.60 | +66.60 | +0.00 | +66.60 |
| 2026-08-20 | `NFGC` | 629 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 7 | — | $144.54 | +0.00 | $150.25 | +39.97 | +39.97 | +0.00 | +39.97 |
| 2026-08-20 | `ABUS` | 223 | — | $4.92 | +0.00 | $4.77 | -33.45 | -33.45 | +0.00 | -33.45 |
| 2026-08-21 | `AG` | 53 | $21.19 | $21.90 | +37.63 | — | +0.00 | +37.63 | +71.55 | — |
| 2026-08-21 | `BHP` | 12 | $93.63 | $95.72 | +25.08 | — | +0.00 | +25.08 | +56.52 | — |
| 2026-08-21 | `CDE` | 53 | $21.11 | $21.75 | +33.92 | — | +0.00 | +33.92 | +58.30 | — |
| 2026-08-21 | `IAG` | 56 | $20.50 | $21.17 | +37.52 | — | +0.00 | +37.52 | +86.24 | — |
| 2026-08-21 | `KGC` | 37 | $31.43 | $32.17 | +27.38 | — | +0.00 | +27.38 | +93.98 | — |
| 2026-08-21 | `NFGC` | 629 | $1.75 | $1.79 | +25.16 | — | +0.00 | +25.16 | +25.16 | — |
| 2026-08-21 | `WPM` | 7 | $150.25 | $154.70 | +31.15 | — | +0.00 | +31.15 | +71.12 | — |
| 2026-08-21 | `ABUS` | 223 | $4.77 | $5.20 | +95.89 | — | +0.00 | +95.89 | +62.44 | — |
| 2026-08-21 | `AU` | 9 | — | $119.43 | +0.00 | $121.22 | +16.11 | +16.11 | +0.00 | +16.11 |
| 2026-08-21 | `AUPH` | 67 | — | $17.20 | +0.00 | $16.65 | -36.85 | -36.85 | +0.00 | -36.85 |
| 2026-08-21 | `AEM` | 5 | — | $216.30 | +0.00 | $216.06 | -1.20 | -1.20 | +0.00 | -1.20 |
| 2026-08-21 | `CYPH` | 879 | — | $1.32 | +0.00 | $1.42 | +87.90 | +87.90 | +0.00 | +87.90 |
| 2026-08-21 | `ORBS` | 1343 | — | $0.86 | +0.00 | $0.88 | +21.49 | +21.49 | +0.00 | +21.49 |
| 2026-08-21 | `CAN` | 3948 | — | $0.29 | +0.00 | $0.35 | +240.83 | +240.83 | +0.00 | +240.83 |
| 2026-08-21 | `DFDV` | 287 | — | $4.04 | +0.00 | $3.94 | -28.70 | -28.70 | +0.00 | -28.70 |
| 2026-08-21 | `TEM` | 17 | — | $65.60 | +0.00 | $72.69 | +120.53 | +120.53 | +0.00 | +120.53 |
| 2026-08-24 | `AU` | 9 | $121.22 | $120.50 | -6.48 | — | +0.00 | -6.48 | +9.63 | — |
| 2026-08-24 | `AUPH` | 67 | $16.65 | $16.60 | -3.35 | — | +0.00 | -3.35 | -40.20 | — |
| 2026-08-24 | `AEM` | 5 | $216.06 | $217.03 | +4.85 | — | +0.00 | +4.85 | +3.65 | — |
| 2026-08-24 | `CYPH` | 879 | $1.42 | $1.83 | +360.39 | — | +0.00 | +360.39 | +448.29 | — |
| 2026-08-24 | `ORBS` | 1343 | $0.88 | $0.89 | +13.43 | — | +0.00 | +13.43 | +34.92 | — |
| 2026-08-24 | `CAN` | 3948 | $0.35 | $0.38 | +98.70 | — | +0.00 | +98.70 | +339.53 | — |
| 2026-08-24 | `DFDV` | 287 | $3.94 | $4.15 | +60.27 | — | +0.00 | +60.27 | +31.57 | — |
| 2026-08-24 | `TEM` | 17 | $72.69 | $70.07 | -44.54 | — | +0.00 | -44.54 | +75.99 | — |
| 2026-08-25 | `MOS` | 52 | — | $24.00 | +0.00 | $23.75 | -13.00 | -13.00 | +0.00 | -13.00 |
| 2026-08-25 | `INSP` | 20 | — | $61.47 | +0.00 | $61.47 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `RZLT` | 240 | — | $5.23 | +0.00 | $5.29 | +14.40 | +14.40 | +0.00 | +14.40 |
| 2026-08-25 | `HCA` | 2 | — | $429.24 | +0.00 | $428.50 | -1.48 | -1.48 | +0.00 | -1.48 |
| 2026-08-25 | `ALVO` | 240 | — | $5.22 | +0.00 | $5.25 | +7.20 | +7.20 | +0.00 | +7.20 |
| 2026-08-25 | `CYPH` | 739 | — | $1.70 | +0.00 | $1.64 | -44.34 | -44.34 | +0.00 | -44.34 |
| 2026-08-25 | `DEFT` | 1964 | — | $0.64 | +0.00 | $0.62 | -39.28 | -39.28 | +0.00 | -39.28 |
| 2026-08-25 | `ASST` | 60 | — | $20.90 | +0.00 | $20.20 | -42.00 | -42.00 | +0.00 | -42.00 |
| 2026-08-26 | `MOS` | 52 | $23.75 | $23.75 | +0.00 | $23.75 | +0.00 | +0.00 | -13.00 | -13.00 |
| 2026-08-26 | `INSP` | 20 | $61.47 | $61.47 | +0.00 | $61.47 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `RZLT` | 240 | $5.29 | $5.29 | +0.00 | $5.29 | +0.00 | +0.00 | +14.40 | +14.40 |
| 2026-08-26 | `HCA` | 2 | $428.50 | $428.50 | +0.00 | $428.50 | +0.00 | +0.00 | -1.48 | -1.48 |
| 2026-08-26 | `ALVO` | 240 | $5.25 | $5.25 | +0.00 | $5.25 | +0.00 | +0.00 | +7.20 | +7.20 |
| 2026-08-26 | `CYPH` | 739 | $1.64 | $1.64 | +0.00 | $1.64 | +0.00 | +0.00 | -44.34 | -44.34 |
| 2026-08-26 | `DEFT` | 1964 | $0.62 | $0.62 | +0.00 | $0.62 | +0.00 | +0.00 | -39.28 | -39.28 |
| 2026-08-26 | `ASST` | 60 | $20.20 | $20.20 | +0.00 | $20.20 | +0.00 | +0.00 | -42.00 | -42.00 |
| 2026-08-27 | `MOS` | 52 | $23.75 | $24.84 | +56.68 | $24.16 | -35.36 | +21.32 | +43.68 | +8.32 |
| 2026-08-27 | `INSP` | 20 | $61.47 | $60.07 | -28.00 | — | +0.00 | -28.00 | -28.00 | — |
| 2026-08-27 | `RZLT` | 240 | $5.29 | $5.01 | -67.20 | — | +0.00 | -67.20 | -52.80 | — |
| 2026-08-27 | `HCA` | 2 | $428.50 | $427.50 | -2.00 | — | +0.00 | -2.00 | -3.48 | — |
| 2026-08-27 | `ALVO` | 240 | $5.25 | $4.98 | -64.80 | — | +0.00 | -64.80 | -57.60 | — |
| 2026-08-27 | `CYPH` | 739 | $1.64 | $1.60 | -29.56 | — | +0.00 | -29.56 | -73.90 | — |
| 2026-08-27 | `DEFT` | 1964 | $0.62 | $0.60 | -39.28 | — | +0.00 | -39.28 | -78.56 | — |
| 2026-08-27 | `ASST` | 60 | $20.20 | $20.72 | +31.20 | — | +0.00 | +31.20 | -10.80 | — |
| 2026-08-28 | `MOS` | 52 | $24.16 | $24.00 | -8.32 | $23.76 | -12.48 | -20.80 | +0.00 | -12.48 |
| 2026-08-28 | `ZYME` | 41 | — | $29.33 | +0.00 | $29.01 | -13.12 | -13.12 | +0.00 | -13.12 |
| 2026-08-28 | `FIGR` | 32 | — | $37.42 | +0.00 | $38.02 | +19.20 | +19.20 | +0.00 | +19.20 |
| 2026-08-28 | `NIQ` | 64 | — | $18.79 | +0.00 | $19.07 | +17.92 | +17.92 | +0.00 | +17.92 |
| 2026-08-28 | `ERO` | 30 | — | $39.20 | +0.00 | $39.82 | +18.60 | +18.60 | +0.00 | +18.60 |
| 2026-08-28 | `TRLV` | 105 | — | $11.38 | +0.00 | $11.03 | -36.75 | -36.75 | +0.00 | -36.75 |
| 2026-08-28 | `CVI` | 30 | — | $40.04 | +0.00 | $39.76 | -8.40 | -8.40 | +0.00 | -8.40 |
| 2026-08-28 | `VIRT` | 18 | — | $65.42 | +0.00 | $67.04 | +29.16 | +29.16 | +0.00 | +29.16 |
| 2026-08-31 | `MOS` | 52 | $23.76 | $23.75 | -0.52 | — | +0.00 | -0.52 | -13.00 | — |
| 2026-08-31 | `ZYME` | 41 | $29.01 | $28.27 | -30.34 | — | +0.00 | -30.34 | -43.46 | — |
| 2026-08-31 | `FIGR` | 32 | $38.02 | $35.50 | -80.64 | — | +0.00 | -80.64 | -61.44 | — |
| 2026-08-31 | `NIQ` | 64 | $19.07 | $19.20 | +8.32 | $19.20 | +0.00 | +8.32 | +26.24 | +26.24 |
| 2026-08-31 | `ERO` | 30 | $39.82 | $38.60 | -36.60 | — | +0.00 | -36.60 | -18.00 | — |
| 2026-08-31 | `TRLV` | 105 | $11.03 | $12.41 | +144.90 | — | +0.00 | +144.90 | +108.15 | — |
| 2026-08-31 | `CVI` | 30 | $39.76 | $41.76 | +60.00 | — | +0.00 | +60.00 | +51.60 | — |
| 2026-08-31 | `VIRT` | 18 | $67.04 | $66.39 | -11.70 | — | +0.00 | -11.70 | +17.46 | — |
| 2026-09-01 | `NIQ` | 64 | $19.20 | $19.06 | -8.96 | — | +0.00 | -8.96 | +17.28 | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 24 | — | $49.76 | +0.00 | $52.59 | +67.92 | +67.92 | +0.00 | +67.92 |
| 2026-09-03 | `RVTY` | 9 | — | $125.94 | +0.00 | $130.94 | +45.00 | +45.00 | +0.00 | +45.00 |
| 2026-09-03 | `DEFT` | 1809 | — | $0.67 | +0.00 | $0.65 | -36.18 | -36.18 | +0.00 | -36.18 |
| 2026-09-03 | `ARCT` | 73 | — | $16.46 | +0.00 | $16.74 | +20.44 | +20.44 | +0.00 | +20.44 |
| 2026-09-03 | `SID` | 1054 | — | $1.15 | +0.00 | $1.17 | +21.08 | +21.08 | +0.00 | +21.08 |
| 2026-09-03 | `NVAX` | 118 | — | $10.27 | +0.00 | $10.32 | +5.90 | +5.90 | +0.00 | +5.90 |
| 2026-09-03 | `CAN` | 4041 | — | $0.30 | +0.00 | $0.31 | +40.41 | +40.41 | +0.00 | +40.41 |
| 2026-09-03 | `CDXS` | 797 | — | $1.52 | +0.00 | $1.48 | -31.88 | -31.88 | +0.00 | -31.88 |
| 2026-09-04 | `ATRC` | 24 | $52.59 | $52.88 | +6.96 | $52.46 | -10.08 | -3.12 | +74.88 | +64.80 |
| 2026-09-04 | `RVTY` | 9 | $130.94 | $132.45 | +13.59 | — | +0.00 | +13.59 | +58.59 | — |
| 2026-09-04 | `DEFT` | 1809 | $0.65 | $0.65 | +0.00 | $0.68 | +54.27 | +54.27 | -36.18 | +18.09 |
| 2026-09-04 | `ARCT` | 73 | $16.74 | $16.77 | +2.19 | — | +0.00 | +2.19 | +22.63 | — |
| 2026-09-04 | `SID` | 1054 | $1.17 | $1.36 | +200.26 | — | +0.00 | +200.26 | +221.34 | — |
| 2026-09-04 | `NVAX` | 118 | $10.32 | $10.41 | +10.62 | $10.34 | -8.26 | +2.36 | +16.52 | +8.26 |
| 2026-09-04 | `CAN` | 4041 | $0.31 | $0.34 | +121.23 | — | +0.00 | +121.23 | +161.64 | — |
| 2026-09-04 | `CDXS` | 797 | $1.48 | $1.48 | +0.00 | — | +0.00 | +0.00 | -31.88 | — |
| 2026-09-04 | `HQ` | 74 | — | $17.06 | +0.00 | $15.79 | -93.98 | -93.98 | +0.00 | -93.98 |
| 2026-09-04 | `NIQ` | 68 | — | $18.66 | +0.00 | $18.82 | +10.88 | +10.88 | +0.00 | +10.88 |
| 2026-09-04 | `OMER` | 67 | — | $18.99 | +0.00 | $19.11 | +8.04 | +8.04 | +0.00 | +8.04 |
| 2026-09-04 | `ERO` | 35 | — | $35.82 | +0.00 | $35.32 | -17.50 | -17.50 | +0.00 | -17.50 |
| 2026-09-04 | `TRLV` | 107 | — | $11.89 | +0.00 | $11.99 | +10.70 | +10.70 | +0.00 | +10.70 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | -264.74 | IREN | — | $19.54 | $9,732.46 | IREN×217 |
| 2026-08-14 | +5.50 | $19.54 | IREN×217 | $9,587.07 | -145.39 | -322.44 | SLG, ADUR, ARX, AIRO, TBBB, AMPY, MH | IREN | $1,284.61 | $9,245.54 | SLG×20, ADUR×72, ARX×61, AIRO×107, TBBB×24, AMPY×242, MH×88 |
| 2026-08-17 | +2.25 | $1,284.61 | SLG×20, ADUR×72, ARX×61, AIRO×107, TBBB×24, AMPY×242, MH×88 | $9,213.89 | -31.65 | -118.23 | DVN, OCC, ALM, CAPR, HTFL, UMAC, NPWR, LPTH | SLG, ADUR, ARX, AIRO, TBBB, AMPY, MH | $99.71 | $9,056.27 | DVN×24, OCC×63, ALM×70, CAPR×167, HTFL×27, UMAC×35, NPWR×598, LPTH×76 |
| 2026-08-18 | -6.20 | $99.71 | DVN×24, OCC×63, ALM×70, CAPR×167, HTFL×27, UMAC×35, NPWR×598, LPTH×76 | $8,831.92 | -224.35 | +0.00 | — | DVN, OCC, ALM, CAPR, HTFL, UMAC, NPWR, LPTH | $8,808.62 | $8,808.62 | — |
| 2026-08-19 | -7.20 | $8,808.62 | — | $8,808.62 | -0.00 | +0.00 | — | — | $8,808.62 | $8,808.62 | — |
| 2026-08-20 | +1.12 | $8,808.62 | — | $8,808.62 | -0.00 | +211.58 | AG, BHP, CDE, IAG, KGC, NFGC, WPM, ABUS | — | $104.03 | $8,996.61 | AG×53, BHP×12, CDE×53, IAG×56, KGC×37, NFGC×629, WPM×7, ABUS×223 |
| 2026-08-21 | +3.25 | $104.03 | AG×53, BHP×12, CDE×53, IAG×56, KGC×37, NFGC×629, WPM×7, ABUS×223 | $9,310.34 | +313.73 | +420.11 | AU, AUPH, AEM, CYPH, ORBS, CAN, DFDV, TEM | AG, BHP, CDE, IAG, KGC, NFGC, WPM, ABUS | $159.30 | $9,644.20 | AU×9, AUPH×67, AEM×5, CYPH×879, ORBS×1343, CAN×3948, DFDV×287, TEM×17 |
| 2026-08-24 | -5.17 | $159.30 | AU×9, AUPH×67, AEM×5, CYPH×879, ORBS×1343, CAN×3948, DFDV×287, TEM×17 | $10,127.47 | +483.27 | +0.00 | — | AU, AUPH, AEM, CYPH, ORBS, CAN, DFDV, TEM | $10,060.15 | $10,060.15 | — |
| 2026-08-25 | +1.80 | $10,060.15 | — | $10,060.15 | +0.00 | -118.50 | MOS, INSP, RZLT, HCA, ALVO, CYPH, DEFT, ASST | — | $406.46 | $9,899.10 | MOS×52, INSP×20, RZLT×240, HCA×2, ALVO×240, CYPH×739, DEFT×1964, ASST×60 |
| 2026-08-26 | +2.02 | $406.46 | MOS×52, INSP×20, RZLT×240, HCA×2, ALVO×240, CYPH×739, DEFT×1964, ASST×60 | $9,899.10 | +0.00 | +0.00 | — | — | $406.46 | $9,899.10 | MOS×52, INSP×20, RZLT×240, HCA×2, ALVO×240, CYPH×739, DEFT×1964, ASST×60 |
| 2026-08-27 | — | $406.46 | MOS×52, INSP×20, RZLT×240, HCA×2, ALVO×240, CYPH×739, DEFT×1964, ASST×60 | $9,756.14 | -142.96 | -35.36 | — | INSP, RZLT, HCA, ALVO, CYPH, DEFT, ASST | $8,424.22 | $9,680.54 | MOS×52 |
| 2026-08-28 | +0.75 | $8,424.22 | MOS×52 | $9,672.22 | -8.32 | +14.13 | ZYME, FIGR, NIQ, ERO, TRLV, CVI, VIRT | — | $57.14 | $9,671.46 | MOS×52, ZYME×41, FIGR×32, NIQ×64, ERO×30, TRLV×105, CVI×30, VIRT×18 |
| 2026-08-31 | -5.85 | $57.14 | MOS×52, ZYME×41, FIGR×32, NIQ×64, ERO×30, TRLV×105, CVI×30, VIRT×18 | $9,724.88 | +53.42 | +0.00 | — | MOS, ZYME, FIGR, ERO, TRLV, CVI, VIRT | $8,481.08 | $9,709.88 | NIQ×64 |
| 2026-09-01 | -6.30 | $8,481.08 | NIQ×64 | $9,700.92 | -8.96 | +0.00 | — | NIQ | $9,698.71 | $9,698.71 | — |
| 2026-09-02 | -3.83 | $9,698.71 | — | $9,698.71 | +0.00 | +0.00 | — | — | $9,698.71 | $9,698.71 | — |
| 2026-09-03 | -0.90 | $9,698.71 | — | $9,698.71 | +0.00 | +132.69 | ATRC, RVTY, DEFT, ARCT, SID, NVAX, CAN, CDXS | — | $35.40 | $9,757.10 | ATRC×24, RVTY×9, DEFT×1809, ARCT×73, SID×1054, NVAX×118, CAN×4041, CDXS×797 |
| 2026-09-04 | — | $35.40 | ATRC×24, RVTY×9, DEFT×1809, ARCT×73, SID×1054, NVAX×118, CAN×4041, CDXS×797 | $10,111.95 | +354.85 | -45.93 | HQ, NIQ, OMER, ERO, TRLV | RVTY, ARCT, SID, CAN, CDXS | $43.00 | $10,000.00 | ATRC×24, DEFT×1809, NVAX×118, HQ×74, NIQ×68, OMER×67, ERO×35, TRLV×107 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 217 | $45.98 | $2.80 | — | $19.54 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; ⚪; ret5=+12.3; leftover $10000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.54 | ▼ close $9,732.46 vs 09:30 $10,000.00 (session -264.74) | 16:00 close · cash $19.54 · equity $9,732.46 vs 09:30 $10,000.00 (-267.54; session marks -264.74) · 1 name(s) marked open→close (per-name table). IREN×217 09:30 $45.98 → close $44.76 -264.74 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.54 | ▼ 09:30 equity $9,587.07 vs yday $9,732.46 (-145.39) | 09:30 open · cash $19.54 (unchanged overnight, no fees) · equity $9,587.07 vs prior close $9,732.46 (-145.39) · 1 name(s) re-marked at the open (per-name table). IREN×217 yday $44.76 → 09:30 $44.09 -145.39 | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 217 | $44.09 | $2.91 | $-415.84 | $9,584.16 | ▼ -415.84 after sell → book $9,584.16; vs 09:30 mark -2.91 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 20 | $57.61 | $2.05 | — | $8,429.91 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; 🔵; ret5=+5.7; leftover $1198.02 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 72 | $16.50 | $2.21 | — | $7,239.70 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1198.02 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 61 | $19.57 | $2.17 | — | $6,043.76 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1198.02 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 107 | $11.12 | $2.31 | — | $4,851.61 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1198.02 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `TBBB` | 24 | $48.82 | $2.06 | — | $3,677.87 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.0; leftover $1198.02 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AMPY` | 242 | $4.94 | $3.12 | — | $2,479.27 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.4; leftover $1198.02 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 88 | $13.55 | $2.25 | — | $1,284.61 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1198.02 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,284.61 | ▼ close $9,245.54 vs 09:30 $9,587.07 (session -322.44) | 16:00 close · cash $1,284.61 · equity $9,245.54 vs 09:30 $9,587.07 (-341.53; session marks -322.44) · 7 name(s) marked open→close (per-name table). SLG×20 09:30 $57.61 → close $56.09 -30.40; ADUR×72 09:30 $16.50 → close $16.17 -23.76; ARX×61 09:30 $19.57 → close $19.58 +0.61; AIRO×107 09:30 $11.12 → close $9.57 -165.85; TBBB×24 09:30 $48.82 → close $47.79 -24.72; AMPY×242 09:30 $4.94 → close $4.78 -38.72; MH×88 09:30 $13.55 → close $13.10 -39.60 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,284.61 | ▼ 09:30 equity $9,213.89 vs yday $9,245.54 (-31.65) | 09:30 open · cash $1,284.61 (unchanged overnight, no fees) · equity $9,213.89 vs prior close $9,245.54 (-31.65) · 7 name(s) re-marked at the open (per-name table). SLG×20 yday $56.09 → 09:30 $55.37 -14.40; ADUR×72 yday $16.17 → 09:30 $15.73 -31.68; ARX×61 yday $19.58 → 09:30 $19.57 -0.61; AIRO×107 yday $9.57 → 09:30 $9.57 +0.00; TBBB×24 yday $47.79 → 09:30 $47.39 -9.60; AMPY×242 yday $4.78 → 09:30 $4.86 +19.36; MH×88 yday $13.10 → 09:30 $13.16 +5.28 | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 20 | $55.37 | $2.07 | $-48.92 | $2,389.94 | ▼ -48.92 after sell → book $9,211.82; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 72 | $15.73 | $2.23 | $-59.87 | $3,520.27 | ▼ -59.87 after sell → book $9,209.59; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 61 | $19.57 | $2.19 | $-4.37 | $4,711.85 | ▼ -4.37 after sell → book $9,207.40; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 107 | $9.57 | $2.34 | $-170.50 | $5,733.50 | ▼ -170.50 after sell → book $9,205.06; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TBBB` | 24 | $47.39 | $2.08 | $-38.46 | $6,868.78 | ▼ -38.46 after sell → book $9,202.98; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AMPY` | 242 | $4.86 | $3.17 | $-25.65 | $8,041.73 | ▼ -25.65 after sell → book $9,199.81; vs 09:30 mark -3.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 88 | $13.16 | $2.28 | $-38.85 | $9,197.53 | ▼ -38.85 after sell → book $9,197.53; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 24 | $46.18 | $2.06 | — | $8,087.15 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; 🔵; ret5=+6.7; leftover $1149.69 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 63 | $18.24 | $2.18 | — | $6,935.85 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $1149.69 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 70 | $16.20 | $2.20 | — | $5,799.65 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1149.69 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 167 | $6.87 | $2.49 | — | $4,649.87 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+62.6; leftover $1149.69 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 27 | $41.23 | $2.07 | — | $3,534.59 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+46.0; leftover $1149.69 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 35 | $32.55 | $2.10 | — | $2,393.24 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1149.69 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 598 | $1.92 | $7.71 | — | $1,237.37 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1149.69 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `LPTH` | 76 | $14.94 | $2.22 | — | $99.71 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $1149.69 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟢 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $99.71 | ▼ close $9,056.27 vs 09:30 $9,213.89 (session -118.23) | 16:00 close · cash $99.71 · equity $9,056.27 vs 09:30 $9,213.89 (-157.62; session marks -118.23) · 8 name(s) marked open→close (per-name table). DVN×24 09:30 $46.18 → close $47.57 +33.36; OCC×63 09:30 $18.24 → close $17.12 -70.56; ALM×70 09:30 $16.20 → close $16.36 +11.20; CAPR×167 09:30 $6.87 → close $7.45 +96.86; HTFL×27 09:30 $41.23 → close $41.94 +19.17; UMAC×35 09:30 $32.55 → close $30.15 -84.00; NPWR×598 09:30 $1.92 → close $1.73 -113.62; LPTH×76 09:30 $14.94 → close $14.80 -10.64 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $99.71 | ▼ 09:30 equity $8,831.92 vs yday $9,056.27 (-224.35) | 09:30 open · cash $99.71 (unchanged overnight, no fees) · equity $8,831.92 vs prior close $9,056.27 (-224.35) · 8 name(s) re-marked at the open (per-name table). DVN×24 yday $47.57 → 09:30 $48.00 +10.32; OCC×63 yday $17.12 → 09:30 $16.20 -57.96; ALM×70 yday $16.36 → 09:30 $15.78 -40.60; CAPR×167 yday $7.45 → 09:30 $7.50 +8.35; HTFL×27 yday $41.94 → 09:30 $41.50 -11.88; UMAC×35 yday $30.15 → 09:30 $28.59 -54.60; NPWR×598 yday $1.73 → 09:30 $1.70 -17.94; LPTH×76 yday $14.80 → 09:30 $14.01 -60.04 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 24 | $48.00 | $2.08 | $+39.54 | $1,249.63 | ▲ +39.54 after sell → book $8,829.84; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 63 | $16.20 | $2.20 | $-132.90 | $2,268.03 | ▼ -132.90 after sell → book $8,827.64; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 70 | $15.78 | $2.22 | $-33.82 | $3,370.41 | ▼ -33.82 after sell → book $8,825.42; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `CAPR` | 167 | $7.50 | $2.53 | $+100.19 | $4,620.38 | ▲ +100.19 after sell → book $8,822.89; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 27 | $41.50 | $2.09 | $+3.13 | $5,738.79 | ▲ +3.13 after sell → book $8,820.80; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 35 | $28.59 | $2.12 | $-142.81 | $6,737.32 | ▼ -142.81 after sell → book $8,818.68; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 598 | $1.70 | $7.82 | $-147.10 | $7,746.10 | ▼ -147.10 after sell → book $8,810.86; vs 09:30 mark -7.82 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `LPTH` | 76 | $14.01 | $2.24 | $-75.14 | $8,808.62 | ▼ -75.14 after sell → book $8,808.62; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,808.62 | ▲ close $8,808.62 vs 09:30 $8,831.92 (session +0.00) | 16:00 close · cash $8,808.62 · no lots left · equity $8,808.62. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,808.62 | ▲ 09:30 equity $8,808.62 vs yday $8,808.62 (-0.00) | 09:30 open · cash $8,808.62 · no holdings · equity $8,808.62 vs prior close $8,808.62 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,808.62 | ▲ close $8,808.62 vs 09:30 $8,808.62 (session +0.00) | 16:00 close · cash $8,808.62 · no lots left · equity $8,808.62. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,808.62 | ▲ 09:30 equity $8,808.62 vs yday $8,808.62 (-0.00) | 09:30 open · cash $8,808.62 · no holdings · equity $8,808.62 vs prior close $8,808.62 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 53 | $20.55 | $2.15 | — | $7,717.32 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1101.08 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 12 | $91.01 | $2.03 | — | $6,623.17 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1101.08 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 53 | $20.65 | $2.15 | — | $5,526.57 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1101.08 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 56 | $19.63 | $2.16 | — | $4,425.13 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1101.08 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 37 | $29.63 | $2.10 | — | $3,326.72 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1101.08 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 629 | $1.75 | $8.11 | — | $2,217.86 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1101.08 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 7 | $144.54 | $2.01 | — | $1,204.07 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1101.08 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 223 | $4.92 | $2.88 | — | $104.03 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1101.08 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $104.03 | ▲ close $8,996.61 vs 09:30 $8,808.62 (session +211.58) | 16:00 close · cash $104.03 · equity $8,996.61 vs 09:30 $8,808.62 (+187.99; session marks +211.58) · 8 name(s) marked open→close (per-name table). AG×53 09:30 $20.55 → close $21.19 +33.92; BHP×12 09:30 $91.01 → close $93.63 +31.44; CDE×53 09:30 $20.65 → close $21.11 +24.38; IAG×56 09:30 $19.63 → close $20.50 +48.72; KGC×37 09:30 $29.63 → close $31.43 +66.60; NFGC×629 09:30 $1.75 → close $1.75 +0.00; WPM×7 09:30 $144.54 → close $150.25 +39.97; ABUS×223 09:30 $4.92 → close $4.77 -33.45 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $104.03 | ▲ 09:30 equity $9,310.34 vs yday $8,996.61 (+313.73) | 09:30 open · cash $104.03 (unchanged overnight, no fees) · equity $9,310.34 vs prior close $8,996.61 (+313.73) · 8 name(s) re-marked at the open (per-name table). AG×53 yday $21.19 → 09:30 $21.90 +37.63; BHP×12 yday $93.63 → 09:30 $95.72 +25.08; CDE×53 yday $21.11 → 09:30 $21.75 +33.92; IAG×56 yday $20.50 → 09:30 $21.17 +37.52; KGC×37 yday $31.43 → 09:30 $32.17 +27.38; NFGC×629 yday $1.75 → 09:30 $1.79 +25.16; WPM×7 yday $150.25 → 09:30 $154.70 +31.15; ABUS×223 yday $4.77 → 09:30 $5.20 +95.89 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 53 | $21.90 | $2.17 | $+67.23 | $1,262.56 | ▲ +67.23 after sell → book $9,308.17; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 12 | $95.72 | $2.05 | $+52.45 | $2,409.16 | ▲ +52.45 after sell → book $9,306.13; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 53 | $21.75 | $2.17 | $+53.98 | $3,559.74 | ▲ +53.98 after sell → book $9,303.96; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 56 | $21.17 | $2.18 | $+81.90 | $4,743.08 | ▲ +81.90 after sell → book $9,301.78; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 37 | $32.17 | $2.12 | $+89.76 | $5,931.25 | ▲ +89.76 after sell → book $9,299.66; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 629 | $1.79 | $8.23 | $+8.82 | $7,048.93 | ▲ +8.82 after sell → book $9,291.43; vs 09:30 mark -8.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 7 | $154.70 | $2.03 | $+67.08 | $8,129.80 | ▲ +67.08 after sell → book $9,289.40; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 223 | $5.20 | $2.92 | $+56.64 | $9,286.48 | ▲ +56.64 after sell → book $9,286.48; vs 09:30 mark -2.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 9 | $119.43 | $2.02 | — | $8,209.59 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $1160.81 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 67 | $17.20 | $2.19 | — | $7,055.00 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1160.81 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $5,971.49 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1160.81 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 879 | $1.32 | $11.34 | — | $4,799.87 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1160.81 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1343 | $0.86 | $15.63 | — | $3,623.89 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $1160.81 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAN` | 3948 | $0.29 | $23.45 | — | $2,439.73 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $1160.81 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DFDV` | 287 | $4.04 | $3.70 | — | $1,276.54 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+23.6; leftover $1160.81 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `TEM` | 17 | $65.60 | $2.04 | — | $159.30 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+22.8; leftover $1160.81 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $159.30 | ▲ close $9,644.20 vs 09:30 $9,310.34 (session +420.11) | 16:00 close · cash $159.30 · equity $9,644.20 vs 09:30 $9,310.34 (+333.86; session marks +420.11) · 8 name(s) marked open→close (per-name table). AU×9 09:30 $119.43 → close $121.22 +16.11; AUPH×67 09:30 $17.20 → close $16.65 -36.85; AEM×5 09:30 $216.30 → close $216.06 -1.20; CYPH×879 09:30 $1.32 → close $1.42 +87.90; ORBS×1343 09:30 $0.86 → close $0.88 +21.49; CAN×3948 09:30 $0.29 → close $0.35 +240.83; DFDV×287 09:30 $4.04 → close $3.94 -28.70; TEM×17 09:30 $65.60 → close $72.69 +120.53 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $159.30 | ▲ 09:30 equity $10,127.47 vs yday $9,644.20 (+483.27) | 09:30 open · cash $159.30 (unchanged overnight, no fees) · equity $10,127.47 vs prior close $9,644.20 (+483.27) · 8 name(s) re-marked at the open (per-name table). AU×9 yday $121.22 → 09:30 $120.50 -6.48; AUPH×67 yday $16.65 → 09:30 $16.60 -3.35; AEM×5 yday $216.06 → 09:30 $217.03 +4.85; CYPH×879 yday $1.42 → 09:30 $1.83 +360.39; ORBS×1343 yday $0.88 → 09:30 $0.89 +13.43; CAN×3948 yday $0.35 → 09:30 $0.38 +98.70; DFDV×287 yday $3.94 → 09:30 $4.15 +60.27; TEM×17 yday $72.69 → 09:30 $70.07 -44.54 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 9 | $120.50 | $2.04 | $+5.58 | $1,241.77 | ▲ +5.58 after sell → book $10,125.44; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 67 | $16.60 | $2.21 | $-44.60 | $2,351.75 | ▼ -44.60 after sell → book $10,123.22; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,434.88 | ▼ -0.38 after sell → book $10,121.20; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 879 | $1.83 | $11.50 | $+425.45 | $5,031.95 | ▲ +425.45 after sell → book $10,109.70; vs 09:30 mark -11.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 1343 | $0.89 | $16.21 | $+3.07 | $6,211.01 | ▲ +3.07 after sell → book $10,093.49; vs 09:30 mark -16.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAN` | 3948 | $0.38 | $27.51 | $+288.56 | $7,683.73 | ▲ +288.56 after sell → book $10,065.97; vs 09:30 mark -27.52 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `DFDV` | 287 | $4.15 | $3.76 | $+24.11 | $8,871.02 | ▲ +24.11 after sell → book $10,062.21; vs 09:30 mark -3.76 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TEM` | 17 | $70.07 | $2.06 | $+71.89 | $10,060.15 | ▲ +71.89 after sell → book $10,060.15; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,060.15 | ▲ close $10,060.15 vs 09:30 $10,127.47 (session +0.00) | 16:00 close · cash $10,060.15 · no lots left · equity $10,060.15. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,060.15 | ▲ 09:30 equity $10,060.15 vs yday $10,060.15 (+0.00) | 09:30 open · cash $10,060.15 · no holdings · equity $10,060.15 vs prior close $10,060.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 52 | $24.00 | $2.15 | — | $8,810.01 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; ⚪; ret5=+13.0; leftover $1257.52 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 20 | $61.47 | $2.05 | — | $7,578.56 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; 🔵; ret5=+9.2; leftover $1257.52 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 240 | $5.23 | $3.10 | — | $6,320.26 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; ret5=+10.7; leftover $1257.52 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 2 | $429.24 | $2.00 | — | $5,459.78 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; ret5=+6.1; leftover $1257.52 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 240 | $5.22 | $3.10 | — | $4,203.89 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1257.52 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 739 | $1.70 | $9.53 | — | $2,938.05 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+120.5; leftover $1257.52 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 1964 | $0.64 | $18.46 | — | $1,662.63 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1257.52 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 60 | $20.90 | $2.17 | — | $406.46 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; 🔵; ret5=+47.9; leftover $1257.52 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $406.46 | ▼ close $9,899.10 vs 09:30 $10,060.15 (session -118.50) | 16:00 close · cash $406.46 · equity $9,899.10 vs 09:30 $10,060.15 (-161.05; session marks -118.50) · 8 name(s) marked open→close (per-name table). MOS×52 09:30 $24.00 → close $23.75 -13.00; INSP×20 09:30 $61.47 → close $61.47 +0.00; RZLT×240 09:30 $5.23 → close $5.29 +14.40; HCA×2 09:30 $429.24 → close $428.50 -1.48; ALVO×240 09:30 $5.22 → close $5.25 +7.20; CYPH×739 09:30 $1.70 → close $1.64 -44.34; DEFT×1964 09:30 $0.64 → close $0.62 -39.28; ASST×60 09:30 $20.90 → close $20.20 -42.00 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $406.46 | ▲ 09:30 equity $9,899.10 vs yday $9,899.10 (+0.00) | 09:30 open · cash $406.46 (unchanged overnight, no fees) · equity $9,899.10 vs prior close $9,899.10 (+0.00) · 8 name(s) re-marked at the open (per-name table). MOS×52 yday $23.75 → 09:30 $23.75 +0.00; INSP×20 yday $61.47 → 09:30 $61.47 +0.00; RZLT×240 yday $5.29 → 09:30 $5.29 +0.00; HCA×2 yday $428.50 → 09:30 $428.50 +0.00; ALVO×240 yday $5.25 → 09:30 $5.25 +0.00; CYPH×739 yday $1.64 → 09:30 $1.64 +0.00; DEFT×1964 yday $0.62 → 09:30 $0.62 +0.00; ASST×60 yday $20.20 → 09:30 $20.20 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $406.46 | ▲ close $9,899.10 vs 09:30 $9,899.10 (session +0.00) | 16:00 close · cash $406.46 · equity $9,899.10 vs 09:30 $9,899.10 (+0.00; session marks +0.00) · 8 name(s) marked open→close (per-name table). MOS×52 09:30 $23.75 → close $23.75 +0.00; INSP×20 09:30 $61.47 → close $61.47 +0.00; RZLT×240 09:30 $5.29 → close $5.29 +0.00; HCA×2 09:30 $428.50 → close $428.50 +0.00; ALVO×240 09:30 $5.25 → close $5.25 +0.00; CYPH×739 09:30 $1.64 → close $1.64 +0.00; DEFT×1964 09:30 $0.62 → close $0.62 +0.00; ASST×60 09:30 $20.20 → close $20.20 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $406.46 | ▼ 09:30 equity $9,756.14 vs yday $9,899.10 (-142.96) | 09:30 open · cash $406.46 (unchanged overnight, no fees) · equity $9,756.14 vs prior close $9,899.10 (-142.96) · 8 name(s) re-marked at the open (per-name table). MOS×52 yday $23.75 → 09:30 $24.84 +56.68; INSP×20 yday $61.47 → 09:30 $60.07 -28.00; RZLT×240 yday $5.29 → 09:30 $5.01 -67.20; HCA×2 yday $428.50 → 09:30 $427.50 -2.00; ALVO×240 yday $5.25 → 09:30 $4.98 -64.80; CYPH×739 yday $1.64 → 09:30 $1.60 -29.56; DEFT×1964 yday $0.62 → 09:30 $0.60 -39.28; ASST×60 yday $20.20 → 09:30 $20.72 +31.20 | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 20 | $60.07 | $2.07 | $-32.12 | $1,605.79 | ▼ -32.12 after sell → book $9,754.07; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 240 | $5.01 | $3.15 | $-59.04 | $2,805.05 | ▼ -59.04 after sell → book $9,750.93; vs 09:30 mark -3.14 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 2 | $427.50 | $2.02 | $-7.49 | $3,658.03 | ▼ -7.49 after sell → book $9,748.91; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALVO` | 240 | $4.98 | $3.15 | $-63.84 | $4,850.08 | ▼ -63.84 after sell → book $9,745.76; vs 09:30 mark -3.15 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 739 | $1.60 | $9.67 | $-93.10 | $6,022.82 | ▼ -93.10 after sell → book $9,736.10; vs 09:30 mark -9.66 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DEFT` | 1964 | $0.60 | $18.01 | $-115.03 | $7,183.21 | ▼ -115.03 after sell → book $9,718.09; vs 09:30 mark -18.01 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ASST` | 60 | $20.72 | $2.19 | $-15.16 | $8,424.22 | ▼ -15.16 after sell → book $9,715.90; vs 09:30 mark -2.19 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,424.22 | ▼ close $9,680.54 vs 09:30 $9,756.14 (session -35.36) | 16:00 close · cash $8,424.22 · equity $9,680.54 vs 09:30 $9,756.14 (-75.60; session marks -35.36) · 1 name(s) marked open→close (per-name table). MOS×52 09:30 $24.84 → close $24.16 -35.36 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,424.22 | ▼ 09:30 equity $9,672.22 vs yday $9,680.54 (-8.32) | 09:30 open · cash $8,424.22 (unchanged overnight, no fees) · equity $9,672.22 vs prior close $9,680.54 (-8.32) · 1 name(s) re-marked at the open (per-name table). MOS×52 yday $24.16 → 09:30 $24.00 -8.32 | — |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 41 | $29.33 | $2.11 | — | $7,219.57 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,ohlc_hot; ret5=+14.1; leftover $1203.46 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FIGR` | 32 | $37.42 | $2.09 | — | $6,020.05 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_mover; ret5=+24.4; leftover $1203.46 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `NIQ` | 64 | $18.79 | $2.18 | — | $4,815.31 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+7.6; leftover $1203.46 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERO` | 30 | $39.20 | $2.08 | — | $3,637.23 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+16.6; leftover $1203.46 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `TRLV` | 105 | $11.38 | $2.31 | — | $2,440.02 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+15.0; leftover $1203.46 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CVI` | 30 | $40.04 | $2.08 | — | $1,236.74 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+12.1; leftover $1203.46 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VIRT` | 18 | $65.42 | $2.04 | — | $57.14 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+13.2; leftover $1203.46 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.14 | ▲ close $9,671.46 vs 09:30 $9,672.22 (session +14.13) | 16:00 close · cash $57.14 · equity $9,671.46 vs 09:30 $9,672.22 (-0.76; session marks +14.13) · 8 name(s) marked open→close (per-name table). MOS×52 09:30 $24.00 → close $23.76 -12.48; ZYME×41 09:30 $29.33 → close $29.01 -13.12; FIGR×32 09:30 $37.42 → close $38.02 +19.20; NIQ×64 09:30 $18.79 → close $19.07 +17.92; ERO×30 09:30 $39.20 → close $39.82 +18.60; TRLV×105 09:30 $11.38 → close $11.03 -36.75; CVI×30 09:30 $40.04 → close $39.76 -8.40; VIRT×18 09:30 $65.42 → close $67.04 +29.16 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.14 | ▲ 09:30 equity $9,724.88 vs yday $9,671.46 (+53.42) | 09:30 open · cash $57.14 (unchanged overnight, no fees) · equity $9,724.88 vs prior close $9,671.46 (+53.42) · 8 name(s) re-marked at the open (per-name table). MOS×52 yday $23.76 → 09:30 $23.75 -0.52; ZYME×41 yday $29.01 → 09:30 $28.27 -30.34; FIGR×32 yday $38.02 → 09:30 $35.50 -80.64; NIQ×64 yday $19.07 → 09:30 $19.20 +8.32; ERO×30 yday $39.82 → 09:30 $38.60 -36.60; TRLV×105 yday $11.03 → 09:30 $12.41 +144.90; CVI×30 yday $39.76 → 09:30 $41.76 +60.00; VIRT×18 yday $67.04 → 09:30 $66.39 -11.70 | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 52 | $23.75 | $2.17 | $-17.31 | $1,289.97 | ▼ -17.31 after sell → book $9,722.71; vs 09:30 mark -2.17 | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 41 | $28.27 | $2.13 | $-47.71 | $2,446.91 | ▼ -47.71 after sell → book $9,720.58; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `FIGR` | 32 | $35.50 | $2.11 | $-65.63 | $3,580.80 | ▼ -65.63 after sell → book $9,718.47; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERO` | 30 | $38.60 | $2.10 | $-22.18 | $4,736.70 | ▼ -22.18 after sell → book $9,716.37; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-31 09:30 ET | **SELL** | `TRLV` | 105 | $12.41 | $2.33 | $+103.51 | $6,037.42 | ▲ +103.51 after sell → book $9,714.04; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `CVI` | 30 | $41.76 | $2.10 | $+47.42 | $7,288.12 | ▲ +47.42 after sell → book $9,711.94; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `VIRT` | 18 | $66.39 | $2.06 | $+13.35 | $8,481.08 | ▲ +13.35 after sell → book $9,709.88; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,481.08 | ▲ close $9,709.88 vs 09:30 $9,724.88 (session +0.00) | 16:00 close · cash $8,481.08 · equity $9,709.88 vs 09:30 $9,724.88 (-15.00; session marks +0.00) · 1 name(s) marked open→close (per-name table). NIQ×64 09:30 $19.20 → close $19.20 +0.00 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,481.08 | ▼ 09:30 equity $9,700.92 vs yday $9,709.88 (-8.96) | 09:30 open · cash $8,481.08 (unchanged overnight, no fees) · equity $9,700.92 vs prior close $9,709.88 (-8.96) · 1 name(s) re-marked at the open (per-name table). NIQ×64 yday $19.20 → 09:30 $19.06 -8.96 | — |
| 2026-09-01 09:30 ET | **SELL** | `NIQ` | 64 | $19.06 | $2.20 | $+12.90 | $9,698.71 | ▲ +12.90 after sell → book $9,698.71; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,698.71 | ▲ close $9,698.71 vs 09:30 $9,700.92 (session +0.00) | 16:00 close · cash $9,698.71 · no lots left · equity $9,698.71. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,698.71 | ▲ 09:30 equity $9,698.71 vs yday $9,698.71 (+0.00) | 09:30 open · cash $9,698.71 · no holdings · equity $9,698.71 vs prior close $9,698.71 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,698.71 | ▲ close $9,698.71 vs 09:30 $9,698.71 (session +0.00) | 16:00 close · cash $9,698.71 · no lots left · equity $9,698.71. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,698.71 | ▲ 09:30 equity $9,698.71 vs yday $9,698.71 (+0.00) | 09:30 open · cash $9,698.71 · no holdings · equity $9,698.71 vs prior close $9,698.71 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 24 | $49.76 | $2.06 | — | $8,502.41 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1212.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $7,366.93 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1212.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DEFT` | 1809 | $0.67 | $17.55 | — | $6,137.36 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1212.34 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 73 | $16.46 | $2.21 | — | $4,933.57 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1212.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SID` | 1054 | $1.15 | $13.60 | — | $3,707.87 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; 🔵; ret5=+10.1; leftover $1212.34 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 118 | $10.27 | $2.34 | — | $2,493.67 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1212.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CAN` | 4041 | $0.30 | $24.25 | — | $1,257.12 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_mover; 🔵; ret5=+54.3; leftover $1212.34 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CDXS` | 797 | $1.52 | $10.28 | — | $35.40 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_mover; ret5=+7.1; leftover $1212.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.40 | ▲ close $9,757.10 vs 09:30 $9,698.71 (session +132.69) | 16:00 close · cash $35.40 · equity $9,757.10 vs 09:30 $9,698.71 (+58.39; session marks +132.69) · 8 name(s) marked open→close (per-name table). ATRC×24 09:30 $49.76 → close $52.59 +67.92; RVTY×9 09:30 $125.94 → close $130.94 +45.00; DEFT×1809 09:30 $0.67 → close $0.65 -36.18; ARCT×73 09:30 $16.46 → close $16.74 +20.44; SID×1054 09:30 $1.15 → close $1.17 +21.08; NVAX×118 09:30 $10.27 → close $10.32 +5.90; CAN×4041 09:30 $0.30 → close $0.31 +40.41; CDXS×797 09:30 $1.52 → close $1.48 -31.88 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.40 | ▲ 09:30 equity $10,111.95 vs yday $9,757.10 (+354.85) | 09:30 open · cash $35.40 (unchanged overnight, no fees) · equity $10,111.95 vs prior close $9,757.10 (+354.85) · 8 name(s) re-marked at the open (per-name table). ATRC×24 yday $52.59 → 09:30 $52.88 +6.96; RVTY×9 yday $130.94 → 09:30 $132.45 +13.59; DEFT×1809 yday $0.65 → 09:30 $0.65 +0.00; ARCT×73 yday $16.74 → 09:30 $16.77 +2.19; SID×1054 yday $1.17 → 09:30 $1.36 +200.26; NVAX×118 yday $10.32 → 09:30 $10.41 +10.62; CAN×4041 yday $0.31 → 09:30 $0.34 +121.23; CDXS×797 yday $1.48 → 09:30 $1.48 +0.00 | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $132.45 | $2.04 | $+54.54 | $1,225.41 | ▲ +54.54 after sell → book $10,109.91; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 73 | $16.77 | $2.23 | $+18.19 | $2,447.39 | ▲ +18.19 after sell → book $10,107.68; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SID` | 1054 | $1.36 | $13.78 | $+193.96 | $3,867.05 | ▲ +193.96 after sell → book $10,093.90; vs 09:30 mark -13.78 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CAN` | 4041 | $0.34 | $26.54 | $+110.85 | $5,214.44 | ▲ +110.85 after sell → book $10,067.35; vs 09:30 mark -26.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CDXS` | 797 | $1.48 | $10.42 | $-52.58 | $6,383.58 | ▼ -52.58 after sell → book $10,056.93; vs 09:30 mark -10.42 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `HQ` | 74 | $17.06 | $2.21 | — | $5,118.93 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+17.3; leftover $1276.72 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NIQ` | 68 | $18.66 | $2.19 | — | $3,847.85 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; 🔵; ret5=+7.6; leftover $1276.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OMER` | 67 | $18.99 | $2.19 | — | $2,573.33 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; 🔵; ret5=+12.1; leftover $1276.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ERO` | 35 | $35.82 | $2.10 | — | $1,317.54 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+16.6; leftover $1276.72 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TRLV` | 107 | $11.89 | $2.31 | — | $43.00 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; 🔵; ⚪; ret5=+15.0; leftover $1276.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $43.00 | ▼ close $10,000.00 vs 09:30 $10,111.95 (session -45.93) | 16:00 close · cash $43.00 · equity $10,000.00 vs 09:30 $10,111.95 (-111.95; session marks -45.93) · 8 name(s) marked open→close (per-name table). ATRC×24 09:30 $52.88 → close $52.46 -10.08; DEFT×1809 09:30 $0.65 → close $0.68 +54.27; NVAX×118 09:30 $10.41 → close $10.34 -8.26; HQ×74 09:30 $17.06 → close $15.79 -93.98; NIQ×68 09:30 $18.66 → close $18.82 +10.88; OMER×67 09:30 $18.99 → close $19.11 +8.04; ERO×35 09:30 $35.82 → close $35.32 -17.50; TRLV×107 09:30 $11.89 → close $11.99 +10.70 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1198.02 < 1 share @ 1646.93 |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `UGI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ALEC` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `ALVO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ASST` | no_price | no 09:30 open — carry |
| 2026-08-26 | `SENS` | no_price | no 09:30 open |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-26 | `NIQ` | no_price | no 09:30 open |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MSTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CAN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WEN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DEFT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ELMT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OMER` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TXG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `GUTS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `UEC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SIBN` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `OMER` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TRLV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TXG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GUTS` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 24 | 2026-09-03 @ $49.76 | union ∩ break10, no 🚨; gate break_10=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1212.34 |
| `DEFT` | 1809 | 2026-09-03 @ $0.67 | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1212.34 |
| `NVAX` | 118 | 2026-09-03 @ $10.27 | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1212.34 |
| `HQ` | 74 | 2026-09-04 @ $17.06 | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ret5=+17.3; leftover $1276.72 |
| `NIQ` | 68 | 2026-09-04 @ $18.66 | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; 🔵; ret5=+7.6; leftover $1276.72 |
| `OMER` | 67 | 2026-09-04 @ $18.99 | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; 🔵; ret5=+12.1; leftover $1276.72 |
| `ERO` | 35 | 2026-09-04 @ $35.82 | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+16.6; leftover $1276.72 |
| `TRLV` | 107 | 2026-09-04 @ $11.89 | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; 🔵; ⚪; ret5=+15.0; leftover $1276.72 |
