# Factor mine action — `union_break10_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ break10, no 🚨

Cash book **+2.04%** ($10,204) · signal-only (no cash/fees) was +6.99%. Starts YES **16/17**. Fills 65 · skips 127 · realized $-192.58.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `break_10=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $50.24.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `IREN` | 217 | — | $45.98 | +0.00 | $44.76 | -264.74 | -264.74 | +0.00 | -264.74 |
| 2026-08-14 | `IREN` | 217 | $44.76 | $44.09 | -145.39 | $44.06 | -6.51 | -151.90 | -410.13 | -416.64 |
| 2026-08-17 | `IREN` | 217 | $44.06 | $45.23 | +253.89 | $44.90 | -71.61 | +182.28 | -162.75 | -234.36 |
| 2026-08-17 | `NPWR` | 1 | — | $1.92 | +0.00 | $1.73 | -0.19 | -0.19 | +0.00 | -0.19 |
| 2026-08-18 | `IREN` | 217 | $44.90 | $43.56 | -290.78 | — | +0.00 | -290.78 | -525.14 | — |
| 2026-08-18 | `NPWR` | 1 | $1.73 | $1.70 | -0.03 | $1.65 | -0.05 | -0.08 | -0.22 | -0.27 |
| 2026-08-19 | `NPWR` | 1 | $1.65 | $1.70 | +0.05 | $1.67 | -0.03 | +0.02 | -0.22 | -0.25 |
| 2026-08-20 | `NPWR` | 1 | $1.67 | $1.64 | -0.03 | — | +0.00 | -0.03 | -0.28 | — |
| 2026-08-20 | `AG` | 57 | — | $20.55 | +0.00 | $21.19 | +36.48 | +36.48 | +0.00 | +36.48 |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `CDE` | 57 | — | $20.65 | +0.00 | $21.11 | +26.22 | +26.22 | +0.00 | +26.22 |
| 2026-08-20 | `IAG` | 60 | — | $19.63 | +0.00 | $20.50 | +52.20 | +52.20 | +0.00 | +52.20 |
| 2026-08-20 | `KGC` | 39 | — | $29.63 | +0.00 | $31.43 | +70.20 | +70.20 | +0.00 | +70.20 |
| 2026-08-20 | `NFGC` | 676 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-20 | `ABUS` | 240 | — | $4.92 | +0.00 | $4.77 | -36.00 | -36.00 | +0.00 | -36.00 |
| 2026-08-21 | `AG` | 57 | $21.19 | $21.90 | +40.47 | $21.09 | -46.17 | -5.70 | +76.95 | +30.78 |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | $97.03 | +17.03 | +44.20 | +61.23 | +78.26 |
| 2026-08-21 | `CDE` | 57 | $21.11 | $21.75 | +36.48 | $20.97 | -44.46 | -7.98 | +62.70 | +18.24 |
| 2026-08-21 | `IAG` | 60 | $20.50 | $21.17 | +40.20 | $21.14 | -1.80 | +38.40 | +92.40 | +90.60 |
| 2026-08-21 | `KGC` | 39 | $31.43 | $32.17 | +28.86 | $32.76 | +23.01 | +51.87 | +99.06 | +122.07 |
| 2026-08-21 | `NFGC` | 676 | $1.75 | $1.79 | +27.04 | $1.84 | +33.80 | +60.84 | +27.04 | +60.84 |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | $157.78 | +24.64 | +60.24 | +81.28 | +105.92 |
| 2026-08-21 | `ABUS` | 240 | $4.77 | $5.20 | +103.20 | $5.21 | +2.40 | +105.60 | +67.20 | +69.60 |
| 2026-08-21 | `CYPH` | 5 | — | $1.32 | +0.00 | $1.42 | +0.50 | +0.50 | +0.00 | +0.50 |
| 2026-08-21 | `ORBS` | 8 | — | $0.86 | +0.00 | $0.88 | +0.13 | +0.13 | +0.00 | +0.13 |
| 2026-08-21 | `CAN` | 25 | — | $0.29 | +0.00 | $0.35 | +1.52 | +1.52 | +0.00 | +1.52 |
| 2026-08-21 | `DFDV` | 1 | — | $4.04 | +0.00 | $3.94 | -0.10 | -0.10 | +0.00 | -0.10 |
| 2026-08-24 | `AG` | 57 | $21.09 | $21.47 | +21.66 | $20.57 | -51.30 | -29.64 | +52.44 | +1.14 |
| 2026-08-24 | `BHP` | 13 | $97.03 | $97.34 | +4.03 | $96.66 | -8.84 | -4.81 | +82.29 | +73.45 |
| 2026-08-24 | `CDE` | 57 | $20.97 | $21.26 | +16.53 | $20.49 | -43.89 | -27.36 | +34.77 | -9.12 |
| 2026-08-24 | `IAG` | 60 | $21.14 | $21.44 | +18.00 | $21.36 | -4.80 | +13.20 | +108.60 | +103.80 |
| 2026-08-24 | `KGC` | 39 | $32.76 | $33.21 | +17.55 | $32.47 | -28.86 | -11.31 | +139.62 | +110.76 |
| 2026-08-24 | `NFGC` | 676 | $1.84 | $1.86 | +13.52 | $1.90 | +27.04 | +40.56 | +74.36 | +101.40 |
| 2026-08-24 | `WPM` | 8 | $157.78 | $158.96 | +9.44 | $158.00 | -7.68 | +1.76 | +115.36 | +107.68 |
| 2026-08-24 | `ABUS` | 240 | $5.21 | $5.18 | -7.20 | $5.20 | +4.80 | -2.40 | +62.40 | +67.20 |
| 2026-08-24 | `CYPH` | 5 | $1.42 | $1.83 | +2.05 | $1.64 | -0.95 | +1.10 | +2.55 | +1.60 |
| 2026-08-24 | `ORBS` | 8 | $0.88 | $0.89 | +0.08 | $0.85 | -0.32 | -0.24 | +0.21 | -0.11 |
| 2026-08-24 | `CAN` | 25 | $0.35 | $0.38 | +0.63 | $0.37 | -0.25 | +0.38 | +2.15 | +1.90 |
| 2026-08-24 | `DFDV` | 1 | $3.94 | $4.15 | +0.21 | $4.19 | +0.04 | +0.25 | +0.11 | +0.15 |
| 2026-08-25 | `AG` | 57 | $20.57 | $20.73 | +9.12 | — | +0.00 | +9.12 | +10.26 | — |
| 2026-08-25 | `BHP` | 13 | $96.66 | $95.95 | -9.23 | — | +0.00 | -9.23 | +64.22 | — |
| 2026-08-25 | `CDE` | 57 | $20.49 | $20.85 | +20.52 | — | +0.00 | +20.52 | +11.40 | — |
| 2026-08-25 | `IAG` | 60 | $21.36 | $21.63 | +16.20 | — | +0.00 | +16.20 | +120.00 | — |
| 2026-08-25 | `KGC` | 39 | $32.47 | $32.76 | +11.31 | — | +0.00 | +11.31 | +122.07 | — |
| 2026-08-25 | `NFGC` | 676 | $1.90 | $1.91 | +6.76 | — | +0.00 | +6.76 | +108.16 | — |
| 2026-08-25 | `WPM` | 8 | $158.00 | $160.00 | +16.00 | — | +0.00 | +16.00 | +123.68 | — |
| 2026-08-25 | `ABUS` | 240 | $5.20 | $5.26 | +14.40 | — | +0.00 | +14.40 | +81.60 | — |
| 2026-08-25 | `CYPH` | 5 | $1.64 | $1.70 | +0.30 | $1.64 | -0.30 | +0.00 | +1.90 | +1.60 |
| 2026-08-25 | `ORBS` | 8 | $0.85 | $0.85 | +0.00 | $0.84 | -0.08 | -0.08 | -0.11 | -0.19 |
| 2026-08-25 | `CAN` | 25 | $0.37 | $0.38 | +0.25 | $0.36 | -0.50 | -0.25 | +2.15 | +1.65 |
| 2026-08-25 | `DFDV` | 1 | $4.19 | $4.29 | +0.10 | $4.16 | -0.13 | -0.03 | +0.25 | +0.12 |
| 2026-08-25 | `MOS` | 59 | — | $24.00 | +0.00 | $23.75 | -14.75 | -14.75 | +0.00 | -14.75 |
| 2026-08-25 | `INSP` | 23 | — | $61.47 | +0.00 | $61.47 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `RZLT` | 274 | — | $5.23 | +0.00 | $5.29 | +16.44 | +16.44 | +0.00 | +16.44 |
| 2026-08-25 | `HCA` | 3 | — | $429.24 | +0.00 | $428.50 | -2.22 | -2.22 | +0.00 | -2.22 |
| 2026-08-25 | `ALVO` | 274 | — | $5.22 | +0.00 | $5.25 | +8.22 | +8.22 | +0.00 | +8.22 |
| 2026-08-25 | `DEFT` | 2240 | — | $0.64 | +0.00 | $0.62 | -44.80 | -44.80 | +0.00 | -44.80 |
| 2026-08-25 | `ASST` | 68 | — | $20.90 | +0.00 | $20.20 | -47.60 | -47.60 | +0.00 | -47.60 |
| 2026-08-26 | `CYPH` | 5 | $1.64 | $1.64 | +0.00 | $1.64 | +0.00 | +0.00 | +1.60 | +1.60 |
| 2026-08-26 | `ORBS` | 8 | $0.84 | $0.84 | +0.00 | $0.84 | +0.00 | +0.00 | -0.19 | -0.19 |
| 2026-08-26 | `CAN` | 25 | $0.36 | $0.36 | +0.00 | $0.36 | +0.00 | +0.00 | +1.65 | +1.65 |
| 2026-08-26 | `DFDV` | 1 | $4.16 | $4.16 | +0.00 | $4.16 | +0.00 | +0.00 | +0.12 | +0.12 |
| 2026-08-26 | `MOS` | 59 | $23.75 | $23.75 | +0.00 | $23.75 | +0.00 | +0.00 | -14.75 | -14.75 |
| 2026-08-26 | `INSP` | 23 | $61.47 | $61.47 | +0.00 | $61.47 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `RZLT` | 274 | $5.29 | $5.29 | +0.00 | $5.29 | +0.00 | +0.00 | +16.44 | +16.44 |
| 2026-08-26 | `HCA` | 3 | $428.50 | $428.50 | +0.00 | $428.50 | +0.00 | +0.00 | -2.22 | -2.22 |
| 2026-08-26 | `ALVO` | 274 | $5.25 | $5.25 | +0.00 | $5.25 | +0.00 | +0.00 | +8.22 | +8.22 |
| 2026-08-26 | `DEFT` | 2240 | $0.62 | $0.62 | +0.00 | $0.62 | +0.00 | +0.00 | -44.80 | -44.80 |
| 2026-08-26 | `ASST` | 68 | $20.20 | $20.20 | +0.00 | $20.20 | +0.00 | +0.00 | -47.60 | -47.60 |
| 2026-08-27 | `CYPH` | 5 | $1.64 | $1.60 | -0.20 | — | +0.00 | -0.20 | +1.40 | — |
| 2026-08-27 | `ORBS` | 8 | $0.84 | $0.80 | -0.32 | — | +0.00 | -0.32 | -0.51 | — |
| 2026-08-27 | `CAN` | 25 | $0.36 | $0.40 | +1.00 | — | +0.00 | +1.00 | +2.65 | — |
| 2026-08-27 | `DFDV` | 1 | $4.16 | $4.35 | +0.19 | — | +0.00 | +0.19 | +0.31 | — |
| 2026-08-27 | `MOS` | 59 | $23.75 | $24.84 | +64.31 | $24.16 | -40.12 | +24.19 | +49.56 | +9.44 |
| 2026-08-27 | `INSP` | 23 | $61.47 | $60.07 | -32.20 | $61.80 | +39.79 | +7.59 | -32.20 | +7.59 |
| 2026-08-27 | `RZLT` | 274 | $5.29 | $5.01 | -76.72 | $5.04 | +8.22 | -68.50 | -60.28 | -52.06 |
| 2026-08-27 | `HCA` | 3 | $428.50 | $427.50 | -3.00 | $427.16 | -1.02 | -4.02 | -5.22 | -6.24 |
| 2026-08-27 | `ALVO` | 274 | $5.25 | $4.98 | -73.98 | $4.91 | -19.18 | -93.16 | -65.76 | -84.94 |
| 2026-08-27 | `DEFT` | 2240 | $0.62 | $0.60 | -44.80 | $0.59 | -22.40 | -67.20 | -89.60 | -112.00 |
| 2026-08-27 | `ASST` | 68 | $20.20 | $20.72 | +35.36 | $21.50 | +53.04 | +88.40 | -12.24 | +40.80 |
| 2026-08-28 | `MOS` | 59 | $24.16 | $24.00 | -9.44 | $23.76 | -14.16 | -23.60 | +0.00 | -14.16 |
| 2026-08-28 | `INSP` | 23 | $61.80 | $62.10 | +6.90 | — | +0.00 | +6.90 | +14.49 | — |
| 2026-08-28 | `RZLT` | 274 | $5.04 | $5.07 | +8.22 | — | +0.00 | +8.22 | -43.84 | — |
| 2026-08-28 | `HCA` | 3 | $427.16 | $424.61 | -7.65 | — | +0.00 | -7.65 | -13.89 | — |
| 2026-08-28 | `ALVO` | 274 | $4.91 | $4.88 | -8.22 | — | +0.00 | -8.22 | -93.16 | — |
| 2026-08-28 | `DEFT` | 2240 | $0.59 | $0.60 | +22.40 | — | +0.00 | +22.40 | -89.60 | — |
| 2026-08-28 | `ASST` | 68 | $21.50 | $22.45 | +64.60 | — | +0.00 | +64.60 | +105.40 | — |
| 2026-08-28 | `ZYME` | 41 | — | $29.33 | +0.00 | $29.01 | -13.12 | -13.12 | +0.00 | -13.12 |
| 2026-08-28 | `FIGR` | 32 | — | $37.42 | +0.00 | $38.02 | +19.20 | +19.20 | +0.00 | +19.20 |
| 2026-08-28 | `NIQ` | 64 | — | $18.79 | +0.00 | $19.07 | +17.92 | +17.92 | +0.00 | +17.92 |
| 2026-08-28 | `ERO` | 30 | — | $39.20 | +0.00 | $39.82 | +18.60 | +18.60 | +0.00 | +18.60 |
| 2026-08-28 | `TRLV` | 106 | — | $11.38 | +0.00 | $11.03 | -37.10 | -37.10 | +0.00 | -37.10 |
| 2026-08-28 | `CVI` | 30 | — | $40.04 | +0.00 | $39.76 | -8.40 | -8.40 | +0.00 | -8.40 |
| 2026-08-28 | `VIRT` | 18 | — | $65.42 | +0.00 | $67.04 | +29.16 | +29.16 | +0.00 | +29.16 |
| 2026-08-31 | `MOS` | 59 | $23.76 | $23.75 | -0.59 | — | +0.00 | -0.59 | -14.75 | — |
| 2026-08-31 | `ZYME` | 41 | $29.01 | $28.27 | -30.34 | $28.27 | +0.00 | -30.34 | -43.46 | -43.46 |
| 2026-08-31 | `FIGR` | 32 | $38.02 | $35.50 | -80.64 | $36.41 | +29.12 | -51.52 | -61.44 | -32.32 |
| 2026-08-31 | `NIQ` | 64 | $19.07 | $19.20 | +8.32 | $19.20 | +0.00 | +8.32 | +26.24 | +26.24 |
| 2026-08-31 | `ERO` | 30 | $39.82 | $38.60 | -36.60 | $38.49 | -3.30 | -39.90 | -18.00 | -21.30 |
| 2026-08-31 | `TRLV` | 106 | $11.03 | $12.41 | +146.28 | $12.41 | +0.00 | +146.28 | +109.18 | +109.18 |
| 2026-08-31 | `CVI` | 30 | $39.76 | $41.76 | +60.00 | $41.76 | +0.00 | +60.00 | +51.60 | +51.60 |
| 2026-08-31 | `VIRT` | 18 | $67.04 | $66.39 | -11.70 | $66.39 | +0.00 | -11.70 | +17.46 | +17.46 |
| 2026-09-01 | `ZYME` | 41 | $28.27 | $29.32 | +43.05 | $29.33 | +0.41 | +43.46 | -0.41 | +0.00 |
| 2026-09-01 | `FIGR` | 32 | $36.41 | $36.80 | +12.48 | $35.70 | -35.20 | -22.72 | -19.84 | -55.04 |
| 2026-09-01 | `NIQ` | 64 | $19.20 | $19.06 | -8.96 | $19.06 | +0.00 | -8.96 | +17.28 | +17.28 |
| 2026-09-01 | `ERO` | 30 | $38.49 | $37.30 | -35.70 | $36.01 | -38.70 | -74.40 | -57.00 | -95.70 |
| 2026-09-01 | `TRLV` | 106 | $12.41 | $11.89 | -55.12 | $11.89 | +0.00 | -55.12 | +54.06 | +54.06 |
| 2026-09-01 | `CVI` | 30 | $41.76 | $42.86 | +33.00 | $42.86 | +0.00 | +33.00 | +84.60 | +84.60 |
| 2026-09-01 | `VIRT` | 18 | $66.39 | $65.64 | -13.50 | $65.64 | +0.00 | -13.50 | +3.96 | +3.96 |
| 2026-09-02 | `ZYME` | 41 | $29.33 | $29.32 | -0.41 | — | +0.00 | -0.41 | -0.41 | — |
| 2026-09-02 | `FIGR` | 32 | $35.70 | $35.46 | -7.68 | — | +0.00 | -7.68 | -62.72 | — |
| 2026-09-02 | `NIQ` | 64 | $19.06 | $19.00 | -3.84 | — | +0.00 | -3.84 | +13.44 | — |
| 2026-09-02 | `ERO` | 30 | $36.01 | $35.95 | -1.80 | — | +0.00 | -1.80 | -97.50 | — |
| 2026-09-02 | `TRLV` | 106 | $11.89 | $11.54 | -37.10 | $11.74 | +21.20 | -15.90 | +16.96 | +38.16 |
| 2026-09-02 | `CVI` | 30 | $42.86 | $42.94 | +2.40 | — | +0.00 | +2.40 | +87.00 | — |
| 2026-09-02 | `VIRT` | 18 | $65.64 | $65.38 | -4.68 | — | +0.00 | -4.68 | -0.72 | — |
| 2026-09-03 | `TRLV` | 106 | $11.74 | $11.78 | +4.24 | — | +0.00 | +4.24 | +42.40 | — |
| 2026-09-03 | `ATRC` | 24 | — | $49.76 | +0.00 | $52.59 | +67.92 | +67.92 | +0.00 | +67.92 |
| 2026-09-03 | `RVTY` | 9 | — | $125.94 | +0.00 | $130.94 | +45.00 | +45.00 | +0.00 | +45.00 |
| 2026-09-03 | `DEFT` | 1829 | — | $0.67 | +0.00 | $0.65 | -36.58 | -36.58 | +0.00 | -36.58 |
| 2026-09-03 | `ARCT` | 74 | — | $16.46 | +0.00 | $16.74 | +20.72 | +20.72 | +0.00 | +20.72 |
| 2026-09-03 | `SID` | 1066 | — | $1.15 | +0.00 | $1.17 | +21.32 | +21.32 | +0.00 | +21.32 |
| 2026-09-03 | `NVAX` | 119 | — | $10.27 | +0.00 | $10.32 | +5.95 | +5.95 | +0.00 | +5.95 |
| 2026-09-03 | `CAN` | 4086 | — | $0.30 | +0.00 | $0.31 | +40.86 | +40.86 | +0.00 | +40.86 |
| 2026-09-03 | `CDXS` | 806 | — | $1.52 | +0.00 | $1.48 | -32.24 | -32.24 | +0.00 | -32.24 |
| 2026-09-04 | `ATRC` | 24 | $52.59 | $52.88 | +6.96 | $52.46 | -10.08 | -3.12 | +74.88 | +64.80 |
| 2026-09-04 | `RVTY` | 9 | $130.94 | $132.45 | +13.59 | $130.63 | -16.38 | -2.79 | +58.59 | +42.21 |
| 2026-09-04 | `DEFT` | 1829 | $0.65 | $0.65 | +0.00 | $0.68 | +54.87 | +54.87 | -36.58 | +18.29 |
| 2026-09-04 | `ARCT` | 74 | $16.74 | $16.77 | +2.22 | $15.56 | -89.54 | -87.32 | +22.94 | -66.60 |
| 2026-09-04 | `SID` | 1066 | $1.17 | $1.36 | +202.54 | $1.26 | -106.60 | +95.94 | +223.86 | +117.26 |
| 2026-09-04 | `NVAX` | 119 | $10.32 | $10.41 | +10.71 | $10.34 | -8.33 | +2.38 | +16.66 | +8.33 |
| 2026-09-04 | `CAN` | 4086 | $0.31 | $0.34 | +122.58 | $0.39 | +204.30 | +326.88 | +163.44 | +367.74 |
| 2026-09-04 | `CDXS` | 806 | $1.48 | $1.48 | +0.00 | $1.42 | -48.36 | -48.36 | -32.24 | -80.60 |
| 2026-09-04 | `TRLV` | 1 | — | $11.89 | +0.00 | $11.99 | +0.10 | +0.10 | +0.00 | +0.10 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | -264.74 | IREN | — | $19.54 | $9,732.46 | IREN×217 |
| 2026-08-14 | +5.50 | $19.54 | IREN×217 | $9,587.07 | -145.39 | -6.51 | — | — | $19.54 | $9,580.56 | IREN×217 |
| 2026-08-17 | +2.25 | $19.54 | IREN×217 | $9,834.45 | +253.89 | -71.80 | NPWR | — | $17.60 | $9,762.63 | IREN×217, NPWR×1 |
| 2026-08-18 | -6.20 | $17.60 | IREN×217, NPWR×1 | $9,471.82 | -290.81 | -0.05 | — | IREN | $9,467.21 | $9,468.86 | NPWR×1 |
| 2026-08-19 | -7.20 | $9,467.21 | NPWR×1 | $9,468.91 | +0.05 | -0.03 | — | — | $9,467.21 | $9,468.88 | NPWR×1 |
| 2026-08-20 | +1.12 | $9,467.21 | NPWR×1 | $9,468.85 | -0.03 | +228.84 | AG, BHP, CDE, IAG, KGC, NFGC, WPM, ABUS | NPWR | $59.33 | $9,673.19 | AG×57, BHP×13, CDE×57, IAG×60, KGC×39, NFGC×676, WPM×8, ABUS×240 |
| 2026-08-21 | +3.25 | $59.33 | AG×57, BHP×13, CDE×57, IAG×60, KGC×39, NFGC×676, WPM×8, ABUS×240 | $10,012.21 | +339.02 | +10.50 | CYPH, ORBS, CAN, DFDV | — | $34.06 | $10,022.35 | AG×57, BHP×13, CDE×57, IAG×60, KGC×39, NFGC×676, WPM×8, ABUS×240, CYPH×5, ORBS×8, CAN×25, DFDV×1 |
| 2026-08-24 | -5.17 | $34.06 | AG×57, BHP×13, CDE×57, IAG×60, KGC×39, NFGC×676, WPM×8, ABUS×240, CYPH×5, ORBS×8, CAN×25, DFDV×1 | $10,118.84 | +96.49 | -115.01 | — | — | $34.06 | $10,003.83 | AG×57, BHP×13, CDE×57, IAG×60, KGC×39, NFGC×676, WPM×8, ABUS×240, CYPH×5, ORBS×8, CAN×25, DFDV×1 |
| 2026-08-25 | +1.80 | $34.06 | AG×57, BHP×13, CDE×57, IAG×60, KGC×39, NFGC×676, WPM×8, ABUS×240, CYPH×5, ORBS×8, CAN×25, DFDV×1 | $10,089.56 | +85.73 | -85.72 | MOS, INSP, RZLT, HCA, ALVO, DEFT, ASST | AG, BHP, CDE, IAG, KGC, NFGC, WPM, ABUS | $163.55 | $9,942.55 | CYPH×5, ORBS×8, CAN×25, DFDV×1, MOS×59, INSP×23, RZLT×274, HCA×3, ALVO×274, DEFT×2240, ASST×68 |
| 2026-08-26 | +2.02 | $163.55 | CYPH×5, ORBS×8, CAN×25, DFDV×1, MOS×59, INSP×23, RZLT×274, HCA×3, ALVO×274, DEFT×2240, ASST×68 | $9,942.55 | -0.00 | +0.00 | — | — | $163.55 | $9,942.55 | CYPH×5, ORBS×8, CAN×25, DFDV×1, MOS×59, INSP×23, RZLT×274, HCA×3, ALVO×274, DEFT×2240, ASST×68 |
| 2026-08-27 | — | $163.55 | CYPH×5, ORBS×8, CAN×25, DFDV×1, MOS×59, INSP×23, RZLT×274, HCA×3, ALVO×274, DEFT×2240, ASST×68 | $9,812.19 | -130.36 | +18.33 | — | CYPH, ORBS, CAN, DFDV | $191.81 | $9,830.03 | MOS×59, INSP×23, RZLT×274, HCA×3, ALVO×274, DEFT×2240, ASST×68 |
| 2026-08-28 | +0.75 | $191.81 | MOS×59, INSP×23, RZLT×274, HCA×3, ALVO×274, DEFT×2240, ASST×68 | $9,906.84 | +76.81 | +12.10 | ZYME, FIGR, NIQ, ERO, TRLV, CVI, VIRT | INSP, RZLT, HCA, ALVO, DEFT, ASST | $78.34 | $9,870.01 | MOS×59, ZYME×41, FIGR×32, NIQ×64, ERO×30, TRLV×106, CVI×30, VIRT×18 |
| 2026-08-31 | -5.85 | $78.34 | MOS×59, ZYME×41, FIGR×32, NIQ×64, ERO×30, TRLV×106, CVI×30, VIRT×18 | $9,924.74 | +54.73 | +25.82 | — | MOS | $1,477.40 | $9,948.37 | ZYME×41, FIGR×32, NIQ×64, ERO×30, TRLV×106, CVI×30, VIRT×18 |
| 2026-09-01 | -6.30 | $1,477.40 | ZYME×41, FIGR×32, NIQ×64, ERO×30, TRLV×106, CVI×30, VIRT×18 | $9,923.62 | -24.75 | -73.49 | — | — | $1,477.40 | $9,850.13 | ZYME×41, FIGR×32, NIQ×64, ERO×30, TRLV×106, CVI×30, VIRT×18 |
| 2026-09-02 | -3.83 | $1,477.40 | ZYME×41, FIGR×32, NIQ×64, ERO×30, TRLV×106, CVI×30, VIRT×18 | $9,797.02 | -53.11 | +21.20 | — | ZYME, FIGR, NIQ, ERO, CVI, VIRT | $8,561.07 | $9,805.51 | TRLV×106 |
| 2026-09-03 | -0.90 | $8,561.07 | TRLV×106 | $9,809.75 | +4.24 | +132.95 | ATRC, RVTY, DEFT, ARCT, SID, NVAX, CAN, CDXS | TRLV | $62.25 | $9,865.32 | ATRC×24, RVTY×9, DEFT×1829, ARCT×74, SID×1066, NVAX×119, CAN×4086, CDXS×806 |
| 2026-09-04 | — | $62.25 | ATRC×24, RVTY×9, DEFT×1829, ARCT×74, SID×1066, NVAX×119, CAN×4086, CDXS×806 | $10,223.92 | +358.60 | -20.02 | TRLV | — | $50.24 | $10,203.78 | ATRC×24, RVTY×9, DEFT×1829, ARCT×74, SID×1066, NVAX×119, CAN×4086, CDXS×806, TRLV×1 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 217 | $45.98 | $2.80 | — | $19.54 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; ⚪; ret5=+12.3; leftover $10000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.54 | ▼ close $9,732.46 vs 09:30 $10,000.00 (session -264.74) | 16:00 close · cash $19.54 · equity $9,732.46 vs 09:30 $10,000.00 (-267.54; session marks -264.74) · 1 name(s) marked open→close (per-name table). IREN×217 09:30 $45.98 → close $44.76 -264.74 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.54 | ▼ 09:30 equity $9,587.07 vs yday $9,732.46 (-145.39) | 09:30 open · cash $19.54 (unchanged overnight, no fees) · equity $9,587.07 vs prior close $9,732.46 (-145.39) · 1 name(s) re-marked at the open (per-name table). IREN×217 yday $44.76 → 09:30 $44.09 -145.39 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.54 | ▼ close $9,580.56 vs 09:30 $9,587.07 (session -6.51) | 16:00 close · cash $19.54 · equity $9,580.56 vs 09:30 $9,587.07 (-6.51; session marks -6.51) · 1 name(s) marked open→close (per-name table). IREN×217 09:30 $44.09 → close $44.06 -6.51 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.54 | ▲ 09:30 equity $9,834.45 vs yday $9,580.56 (+253.89) | 09:30 open · cash $19.54 (unchanged overnight, no fees) · equity $9,834.45 vs prior close $9,580.56 (+253.89) · 1 name(s) re-marked at the open (per-name table). IREN×217 yday $44.06 → 09:30 $45.23 +253.89 | — |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 1 | $1.92 | $0.02 | — | $17.60 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $2.44 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.60 | ▼ close $9,762.63 vs 09:30 $9,834.45 (session -71.80) | 16:00 close · cash $17.60 · equity $9,762.63 vs 09:30 $9,834.45 (-71.82; session marks -71.80) · 2 name(s) marked open→close (per-name table). IREN×217 09:30 $45.23 → close $44.90 -71.61; NPWR×1 09:30 $1.92 → close $1.73 -0.19 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.60 | ▼ 09:30 equity $9,471.82 vs yday $9,762.63 (-290.81) | 09:30 open · cash $17.60 (unchanged overnight, no fees) · equity $9,471.82 vs prior close $9,762.63 (-290.81) · 2 name(s) re-marked at the open (per-name table). IREN×217 yday $44.90 → 09:30 $43.56 -290.78; NPWR×1 yday $1.73 → 09:30 $1.70 -0.03 | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 217 | $43.56 | $2.91 | $-530.85 | $9,467.21 | ▼ -530.85 after sell → book $9,468.91; vs 09:30 mark -2.91 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,467.21 | ▼ close $9,468.86 vs 09:30 $9,471.82 (session -0.05) | 16:00 close · cash $9,467.21 · equity $9,468.86 vs 09:30 $9,471.82 (-2.96; session marks -0.05) · 1 name(s) marked open→close (per-name table). NPWR×1 09:30 $1.70 → close $1.65 -0.05 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,467.21 | ▲ 09:30 equity $9,468.91 vs yday $9,468.86 (+0.05) | 09:30 open · cash $9,467.21 (unchanged overnight, no fees) · equity $9,468.91 vs prior close $9,468.86 (+0.05) · 1 name(s) re-marked at the open (per-name table). NPWR×1 yday $1.65 → 09:30 $1.70 +0.05 | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,467.21 | ▼ close $9,468.88 vs 09:30 $9,468.91 (session -0.03) | 16:00 close · cash $9,467.21 · equity $9,468.88 vs 09:30 $9,468.91 (-0.03; session marks -0.03) · 1 name(s) marked open→close (per-name table). NPWR×1 09:30 $1.70 → close $1.67 -0.03 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,467.21 | ▼ 09:30 equity $9,468.85 vs yday $9,468.88 (-0.03) | 09:30 open · cash $9,467.21 (unchanged overnight, no fees) · equity $9,468.85 vs prior close $9,468.88 (-0.03) · 1 name(s) re-marked at the open (per-name table). NPWR×1 yday $1.67 → 09:30 $1.64 -0.03 | — |
| 2026-08-20 09:30 ET | **SELL** | `NPWR` | 1 | $1.64 | $0.04 | $-0.34 | $9,468.81 | ▼ -0.34 after sell → book $9,468.81; vs 09:30 mark -0.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 57 | $20.55 | $2.16 | — | $8,295.30 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1183.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,110.14 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1183.60 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 57 | $20.65 | $2.16 | — | $5,930.93 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1183.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 60 | $19.63 | $2.17 | — | $4,750.96 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1183.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 39 | $29.63 | $2.11 | — | $3,593.28 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1183.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 676 | $1.75 | $8.72 | — | $2,401.56 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1183.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $1,243.23 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1183.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 240 | $4.92 | $3.10 | — | $59.33 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1183.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $59.33 | ▲ close $9,673.19 vs 09:30 $9,468.85 (session +228.84) | 16:00 close · cash $59.33 · equity $9,673.19 vs 09:30 $9,468.85 (+204.34; session marks +228.84) · 8 name(s) marked open→close (per-name table). AG×57 09:30 $20.55 → close $21.19 +36.48; BHP×13 09:30 $91.01 → close $93.63 +34.06; CDE×57 09:30 $20.65 → close $21.11 +26.22; IAG×60 09:30 $19.63 → close $20.50 +52.20; KGC×39 09:30 $29.63 → close $31.43 +70.20; NFGC×676 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68; ABUS×240 09:30 $4.92 → close $4.77 -36.00 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.33 | ▲ 09:30 equity $10,012.21 vs yday $9,673.19 (+339.02) | 09:30 open · cash $59.33 (unchanged overnight, no fees) · equity $10,012.21 vs prior close $9,673.19 (+339.02) · 8 name(s) re-marked at the open (per-name table). AG×57 yday $21.19 → 09:30 $21.90 +40.47; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×57 yday $21.11 → 09:30 $21.75 +36.48; IAG×60 yday $20.50 → 09:30 $21.17 +40.20; KGC×39 yday $31.43 → 09:30 $32.17 +28.86; NFGC×676 yday $1.75 → 09:30 $1.79 +27.04; WPM×8 yday $150.25 → 09:30 $154.70 +35.60; ABUS×240 yday $4.77 → 09:30 $5.20 +103.20 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 5 | $1.32 | $0.08 | — | $52.65 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $7.42 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 8 | $0.86 | $0.09 | — | $45.64 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $7.42 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAN` | 25 | $0.29 | $0.15 | — | $38.15 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $7.42 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DFDV` | 1 | $4.04 | $0.04 | — | $34.06 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+23.6; leftover $7.42 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.06 | ▲ close $10,022.35 vs 09:30 $10,012.21 (session +10.50) | 16:00 close · cash $34.06 · equity $10,022.35 vs 09:30 $10,012.21 (+10.14; session marks +10.50) · 12 name(s) marked open→close (per-name table). AG×57 09:30 $21.90 → close $21.09 -46.17; BHP×13 09:30 $95.72 → close $97.03 +17.03; CDE×57 09:30 $21.75 → close $20.97 -44.46; IAG×60 09:30 $21.17 → close $21.14 -1.80; KGC×39 09:30 $32.17 → close $32.76 +23.01; NFGC×676 09:30 $1.79 → close $1.84 +33.80; WPM×8 09:30 $154.70 → close $157.78 +24.64; ABUS×240 09:30 $5.20 → close $5.21 +2.40; CYPH×5 09:30 $1.32 → close $1.42 +0.50; ORBS×8 09:30 $0.86 → close $0.88 +0.13; CAN×25 09:30 $0.29 → close $0.35 +1.52; DFDV×1 09:30 $4.04 → close $3.94 -0.10 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.06 | ▲ 09:30 equity $10,118.84 vs yday $10,022.35 (+96.49) | 09:30 open · cash $34.06 (unchanged overnight, no fees) · equity $10,118.84 vs prior close $10,022.35 (+96.49) · 12 name(s) re-marked at the open (per-name table). AG×57 yday $21.09 → 09:30 $21.47 +21.66; BHP×13 yday $97.03 → 09:30 $97.34 +4.03; CDE×57 yday $20.97 → 09:30 $21.26 +16.53; IAG×60 yday $21.14 → 09:30 $21.44 +18.00; KGC×39 yday $32.76 → 09:30 $33.21 +17.55; NFGC×676 yday $1.84 → 09:30 $1.86 +13.52; WPM×8 yday $157.78 → 09:30 $158.96 +9.44; ABUS×240 yday $5.21 → 09:30 $5.18 -7.20; CYPH×5 yday $1.42 → 09:30 $1.83 +2.05; ORBS×8 yday $0.88 → 09:30 $0.89 +0.08; CAN×25 yday $0.35 → 09:30 $0.38 +0.63; DFDV×1 yday $3.94 → 09:30 $4.15 +0.21 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.06 | ▼ close $10,003.83 vs 09:30 $10,118.84 (session -115.01) | 16:00 close · cash $34.06 · equity $10,003.83 vs 09:30 $10,118.84 (-115.01; session marks -115.01) · 12 name(s) marked open→close (per-name table). AG×57 09:30 $21.47 → close $20.57 -51.30; BHP×13 09:30 $97.34 → close $96.66 -8.84; CDE×57 09:30 $21.26 → close $20.49 -43.89; IAG×60 09:30 $21.44 → close $21.36 -4.80; KGC×39 09:30 $33.21 → close $32.47 -28.86; NFGC×676 09:30 $1.86 → close $1.90 +27.04; WPM×8 09:30 $158.96 → close $158.00 -7.68; ABUS×240 09:30 $5.18 → close $5.20 +4.80; CYPH×5 09:30 $1.83 → close $1.64 -0.95; ORBS×8 09:30 $0.89 → close $0.85 -0.32; CAN×25 09:30 $0.38 → close $0.37 -0.25; DFDV×1 09:30 $4.15 → close $4.19 +0.04 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.06 | ▲ 09:30 equity $10,089.56 vs yday $10,003.83 (+85.73) | 09:30 open · cash $34.06 (unchanged overnight, no fees) · equity $10,089.56 vs prior close $10,003.83 (+85.73) · 12 name(s) re-marked at the open (per-name table). AG×57 yday $20.57 → 09:30 $20.73 +9.12; BHP×13 yday $96.66 → 09:30 $95.95 -9.23; CDE×57 yday $20.49 → 09:30 $20.85 +20.52; IAG×60 yday $21.36 → 09:30 $21.63 +16.20; KGC×39 yday $32.47 → 09:30 $32.76 +11.31; NFGC×676 yday $1.90 → 09:30 $1.91 +6.76; WPM×8 yday $158.00 → 09:30 $160.00 +16.00; ABUS×240 yday $5.20 → 09:30 $5.26 +14.40; CYPH×5 yday $1.64 → 09:30 $1.70 +0.30; ORBS×8 yday $0.85 → 09:30 $0.85 +0.00; CAN×25 yday $0.37 → 09:30 $0.38 +0.25; DFDV×1 yday $4.19 → 09:30 $4.29 +0.10 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 57 | $20.73 | $2.18 | $+5.92 | $1,213.49 | ▲ +5.92 after sell → book $10,087.38; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.95 | $2.05 | $+60.14 | $2,458.79 | ▲ +60.14 after sell → book $10,085.33; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 57 | $20.85 | $2.18 | $+7.06 | $3,645.06 | ▲ +7.06 after sell → book $10,083.15; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 60 | $21.63 | $2.19 | $+115.64 | $4,940.67 | ▲ +115.64 after sell → book $10,080.96; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 39 | $32.76 | $2.13 | $+117.84 | $6,216.18 | ▲ +117.84 after sell → book $10,078.83; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 676 | $1.91 | $8.84 | $+90.60 | $7,498.50 | ▲ +90.60 after sell → book $10,069.99; vs 09:30 mark -8.84 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $160.00 | $2.03 | $+119.63 | $8,776.47 | ▲ +119.63 after sell → book $10,067.96; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `ABUS` | 240 | $5.26 | $3.15 | $+75.36 | $10,035.72 | ▲ +75.36 after sell → book $10,064.81; vs 09:30 mark -3.15 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 59 | $24.00 | $2.17 | — | $8,617.55 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; ⚪; ret5=+13.0; leftover $1433.67 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 23 | $61.47 | $2.06 | — | $7,201.68 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; 🔵; ret5=+9.2; leftover $1433.67 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 274 | $5.23 | $3.53 | — | $5,765.13 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; ret5=+10.7; leftover $1433.67 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $429.24 | $2.00 | — | $4,475.41 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; ret5=+6.1; leftover $1433.67 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 274 | $5.22 | $3.53 | — | $3,041.60 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1433.67 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 2240 | $0.64 | $21.06 | — | $1,586.94 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1433.67 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 68 | $20.90 | $2.19 | — | $163.55 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; 🔵; ret5=+47.9; leftover $1433.67 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $163.55 | ▼ close $9,942.55 vs 09:30 $10,089.56 (session -85.72) | 16:00 close · cash $163.55 · equity $9,942.55 vs 09:30 $10,089.56 (-147.01; session marks -85.72) · 11 name(s) marked open→close (per-name table). CYPH×5 09:30 $1.70 → close $1.64 -0.30; ORBS×8 09:30 $0.85 → close $0.84 -0.08; CAN×25 09:30 $0.38 → close $0.36 -0.50; DFDV×1 09:30 $4.29 → close $4.16 -0.13; MOS×59 09:30 $24.00 → close $23.75 -14.75; INSP×23 09:30 $61.47 → close $61.47 +0.00; RZLT×274 09:30 $5.23 → close $5.29 +16.44; HCA×3 09:30 $429.24 → close $428.50 -2.22; ALVO×274 09:30 $5.22 → close $5.25 +8.22; DEFT×2240 09:30 $0.64 → close $0.62 -44.80; ASST×68 09:30 $20.90 → close $20.20 -47.60 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $163.55 | ▲ 09:30 equity $9,942.55 vs yday $9,942.55 (-0.00) | 09:30 open · cash $163.55 (unchanged overnight, no fees) · equity $9,942.55 vs prior close $9,942.55 (-0.00) · 11 name(s) re-marked at the open (per-name table). CYPH×5 yday $1.64 → 09:30 $1.64 +0.00; ORBS×8 yday $0.84 → 09:30 $0.84 +0.00; CAN×25 yday $0.36 → 09:30 $0.36 +0.00; DFDV×1 yday $4.16 → 09:30 $4.16 +0.00; MOS×59 yday $23.75 → 09:30 $23.75 +0.00; INSP×23 yday $61.47 → 09:30 $61.47 +0.00; RZLT×274 yday $5.29 → 09:30 $5.29 +0.00; HCA×3 yday $428.50 → 09:30 $428.50 +0.00; ALVO×274 yday $5.25 → 09:30 $5.25 +0.00; DEFT×2240 yday $0.62 → 09:30 $0.62 +0.00; ASST×68 yday $20.20 → 09:30 $20.20 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $163.55 | ▲ close $9,942.55 vs 09:30 $9,942.55 (session +0.00) | 16:00 close · cash $163.55 · equity $9,942.55 vs 09:30 $9,942.55 (-0.00; session marks +0.00) · 11 name(s) marked open→close (per-name table). CYPH×5 09:30 $1.64 → close $1.64 +0.00; ORBS×8 09:30 $0.84 → close $0.84 +0.00; CAN×25 09:30 $0.36 → close $0.36 +0.00; DFDV×1 09:30 $4.16 → close $4.16 +0.00; MOS×59 09:30 $23.75 → close $23.75 +0.00; INSP×23 09:30 $61.47 → close $61.47 +0.00; RZLT×274 09:30 $5.29 → close $5.29 +0.00; HCA×3 09:30 $428.50 → close $428.50 +0.00; ALVO×274 09:30 $5.25 → close $5.25 +0.00; DEFT×2240 09:30 $0.62 → close $0.62 +0.00; ASST×68 09:30 $20.20 → close $20.20 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $163.55 | ▼ 09:30 equity $9,812.19 vs yday $9,942.55 (-130.36) | 09:30 open · cash $163.55 (unchanged overnight, no fees) · equity $9,812.19 vs prior close $9,942.55 (-130.36) · 11 name(s) re-marked at the open (per-name table). CYPH×5 yday $1.64 → 09:30 $1.60 -0.20; ORBS×8 yday $0.84 → 09:30 $0.80 -0.32; CAN×25 yday $0.36 → 09:30 $0.40 +1.00; DFDV×1 yday $4.16 → 09:30 $4.35 +0.19; MOS×59 yday $23.75 → 09:30 $24.84 +64.31; INSP×23 yday $61.47 → 09:30 $60.07 -32.20; RZLT×274 yday $5.29 → 09:30 $5.01 -76.72; HCA×3 yday $428.50 → 09:30 $427.50 -3.00; ALVO×274 yday $5.25 → 09:30 $4.98 -73.98; DEFT×2240 yday $0.62 → 09:30 $0.60 -44.80; ASST×68 yday $20.20 → 09:30 $20.72 +35.36 | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 5 | $1.60 | $0.12 | $+1.20 | $171.43 | ▲ +1.20 after sell → book $9,812.07; vs 09:30 mark -0.12 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ORBS` | 8 | $0.80 | $0.11 | $-0.71 | $177.72 | ▼ -0.71 after sell → book $9,811.96; vs 09:30 mark -0.11 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAN` | 25 | $0.40 | $0.20 | $+2.31 | $187.53 | ▲ +2.31 after sell → book $9,811.77; vs 09:30 mark -0.19 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `DFDV` | 1 | $4.35 | $0.07 | $+0.20 | $191.81 | ▲ +0.20 after sell → book $9,811.70; vs 09:30 mark -0.07 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $191.81 | ▲ close $9,830.03 vs 09:30 $9,812.19 (session +18.33) | 16:00 close · cash $191.81 · equity $9,830.03 vs 09:30 $9,812.19 (+17.84; session marks +18.33) · 7 name(s) marked open→close (per-name table). MOS×59 09:30 $24.84 → close $24.16 -40.12; INSP×23 09:30 $60.07 → close $61.80 +39.79; RZLT×274 09:30 $5.01 → close $5.04 +8.22; HCA×3 09:30 $427.50 → close $427.16 -1.02; ALVO×274 09:30 $4.98 → close $4.91 -19.18; DEFT×2240 09:30 $0.60 → close $0.59 -22.40; ASST×68 09:30 $20.72 → close $21.50 +53.04 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $191.81 | ▲ 09:30 equity $9,906.84 vs yday $9,830.03 (+76.81) | 09:30 open · cash $191.81 (unchanged overnight, no fees) · equity $9,906.84 vs prior close $9,830.03 (+76.81) · 7 name(s) re-marked at the open (per-name table). MOS×59 yday $24.16 → 09:30 $24.00 -9.44; INSP×23 yday $61.80 → 09:30 $62.10 +6.90; RZLT×274 yday $5.04 → 09:30 $5.07 +8.22; HCA×3 yday $427.16 → 09:30 $424.61 -7.65; ALVO×274 yday $4.91 → 09:30 $4.88 -8.22; DEFT×2240 yday $0.59 → 09:30 $0.60 +22.40; ASST×68 yday $21.50 → 09:30 $22.45 +64.60 | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 23 | $62.10 | $2.08 | $+10.35 | $1,618.03 | ▲ +10.35 after sell → book $9,904.76; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 274 | $5.07 | $3.59 | $-50.97 | $3,003.62 | ▼ -50.97 after sell → book $9,901.17; vs 09:30 mark -3.59 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 3 | $424.61 | $2.02 | $-17.91 | $4,275.43 | ▼ -17.91 after sell → book $9,899.15; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALVO` | 274 | $4.88 | $3.59 | $-100.29 | $5,608.96 | ▼ -100.29 after sell → book $9,895.56; vs 09:30 mark -3.59 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DEFT` | 2240 | $0.60 | $20.54 | $-131.20 | $6,932.42 | ▼ -131.20 after sell → book $9,875.02; vs 09:30 mark -20.54 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `ASST` | 68 | $22.45 | $2.22 | $+100.99 | $8,456.80 | ▲ +100.99 after sell → book $9,872.80; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 41 | $29.33 | $2.11 | — | $7,252.16 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,ohlc_hot; ret5=+14.1; leftover $1208.11 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FIGR` | 32 | $37.42 | $2.09 | — | $6,052.63 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_mover; ret5=+24.4; leftover $1208.11 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `NIQ` | 64 | $18.79 | $2.18 | — | $4,847.89 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+7.6; leftover $1208.11 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERO` | 30 | $39.20 | $2.08 | — | $3,669.81 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+16.6; leftover $1208.11 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `TRLV` | 106 | $11.38 | $2.31 | — | $2,461.22 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+15.0; leftover $1208.11 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CVI` | 30 | $40.04 | $2.08 | — | $1,257.94 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+12.1; leftover $1208.11 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VIRT` | 18 | $65.42 | $2.04 | — | $78.34 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+13.2; leftover $1208.11 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.34 | ▲ close $9,870.01 vs 09:30 $9,906.84 (session +12.10) | 16:00 close · cash $78.34 · equity $9,870.01 vs 09:30 $9,906.84 (-36.83; session marks +12.10) · 8 name(s) marked open→close (per-name table). MOS×59 09:30 $24.00 → close $23.76 -14.16; ZYME×41 09:30 $29.33 → close $29.01 -13.12; FIGR×32 09:30 $37.42 → close $38.02 +19.20; NIQ×64 09:30 $18.79 → close $19.07 +17.92; ERO×30 09:30 $39.20 → close $39.82 +18.60; TRLV×106 09:30 $11.38 → close $11.03 -37.10; CVI×30 09:30 $40.04 → close $39.76 -8.40; VIRT×18 09:30 $65.42 → close $67.04 +29.16 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.34 | ▲ 09:30 equity $9,924.74 vs yday $9,870.01 (+54.73) | 09:30 open · cash $78.34 (unchanged overnight, no fees) · equity $9,924.74 vs prior close $9,870.01 (+54.73) · 8 name(s) re-marked at the open (per-name table). MOS×59 yday $23.76 → 09:30 $23.75 -0.59; ZYME×41 yday $29.01 → 09:30 $28.27 -30.34; FIGR×32 yday $38.02 → 09:30 $35.50 -80.64; NIQ×64 yday $19.07 → 09:30 $19.20 +8.32; ERO×30 yday $39.82 → 09:30 $38.60 -36.60; TRLV×106 yday $11.03 → 09:30 $12.41 +146.28; CVI×30 yday $39.76 → 09:30 $41.76 +60.00; VIRT×18 yday $67.04 → 09:30 $66.39 -11.70 | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 59 | $23.75 | $2.19 | $-19.11 | $1,477.40 | ▼ -19.11 after sell → book $9,922.55; vs 09:30 mark -2.19 | dropped from list after 4 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,477.40 | ▲ close $9,948.37 vs 09:30 $9,924.74 (session +25.82) | 16:00 close · cash $1,477.40 · equity $9,948.37 vs 09:30 $9,924.74 (+23.63; session marks +25.82) · 7 name(s) marked open→close (per-name table). ZYME×41 09:30 $28.27 → close $28.27 +0.00; FIGR×32 09:30 $35.50 → close $36.41 +29.12; NIQ×64 09:30 $19.20 → close $19.20 +0.00; ERO×30 09:30 $38.60 → close $38.49 -3.30; TRLV×106 09:30 $12.41 → close $12.41 +0.00; CVI×30 09:30 $41.76 → close $41.76 +0.00; VIRT×18 09:30 $66.39 → close $66.39 +0.00 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,477.40 | ▼ 09:30 equity $9,923.62 vs yday $9,948.37 (-24.75) | 09:30 open · cash $1,477.40 (unchanged overnight, no fees) · equity $9,923.62 vs prior close $9,948.37 (-24.75) · 7 name(s) re-marked at the open (per-name table). ZYME×41 yday $28.27 → 09:30 $29.32 +43.05; FIGR×32 yday $36.41 → 09:30 $36.80 +12.48; NIQ×64 yday $19.20 → 09:30 $19.06 -8.96; ERO×30 yday $38.49 → 09:30 $37.30 -35.70; TRLV×106 yday $12.41 → 09:30 $11.89 -55.12; CVI×30 yday $41.76 → 09:30 $42.86 +33.00; VIRT×18 yday $66.39 → 09:30 $65.64 -13.50 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,477.40 | ▼ close $9,850.13 vs 09:30 $9,923.62 (session -73.49) | 16:00 close · cash $1,477.40 · equity $9,850.13 vs 09:30 $9,923.62 (-73.49; session marks -73.49) · 7 name(s) marked open→close (per-name table). ZYME×41 09:30 $29.32 → close $29.33 +0.41; FIGR×32 09:30 $36.80 → close $35.70 -35.20; NIQ×64 09:30 $19.06 → close $19.06 +0.00; ERO×30 09:30 $37.30 → close $36.01 -38.70; TRLV×106 09:30 $11.89 → close $11.89 +0.00; CVI×30 09:30 $42.86 → close $42.86 +0.00; VIRT×18 09:30 $65.64 → close $65.64 +0.00 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,477.40 | ▼ 09:30 equity $9,797.02 vs yday $9,850.13 (-53.11) | 09:30 open · cash $1,477.40 (unchanged overnight, no fees) · equity $9,797.02 vs prior close $9,850.13 (-53.11) · 7 name(s) re-marked at the open (per-name table). ZYME×41 yday $29.33 → 09:30 $29.32 -0.41; FIGR×32 yday $35.70 → 09:30 $35.46 -7.68; NIQ×64 yday $19.06 → 09:30 $19.00 -3.84; ERO×30 yday $36.01 → 09:30 $35.95 -1.80; TRLV×106 yday $11.89 → 09:30 $11.54 -37.10; CVI×30 yday $42.86 → 09:30 $42.94 +2.40; VIRT×18 yday $65.64 → 09:30 $65.38 -4.68 | — |
| 2026-09-02 09:30 ET | **SELL** | `ZYME` | 41 | $29.32 | $2.13 | $-4.66 | $2,677.39 | ▼ -4.66 after sell → book $9,794.89; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-02 09:30 ET | **SELL** | `FIGR` | 32 | $35.46 | $2.11 | $-66.91 | $3,810.00 | ▼ -66.91 after sell → book $9,792.78; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `NIQ` | 64 | $19.00 | $2.20 | $+9.06 | $5,023.80 | ▲ +9.06 after sell → book $9,790.58; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ERO` | 30 | $35.95 | $2.10 | $-101.68 | $6,100.20 | ▼ -101.68 after sell → book $9,788.48; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-02 09:30 ET | **SELL** | `CVI` | 30 | $42.94 | $2.10 | $+82.82 | $7,386.30 | ▲ +82.82 after sell → book $9,786.38; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-02 09:30 ET | **SELL** | `VIRT` | 18 | $65.38 | $2.06 | $-4.83 | $8,561.07 | ▼ -4.83 after sell → book $9,784.31; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,561.07 | ▲ close $9,805.51 vs 09:30 $9,797.02 (session +21.20) | 16:00 close · cash $8,561.07 · equity $9,805.51 vs 09:30 $9,797.02 (+8.49; session marks +21.20) · 1 name(s) marked open→close (per-name table). TRLV×106 09:30 $11.54 → close $11.74 +21.20 | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,561.07 | ▲ 09:30 equity $9,809.75 vs yday $9,805.51 (+4.24) | 09:30 open · cash $8,561.07 (unchanged overnight, no fees) · equity $9,809.75 vs prior close $9,805.51 (+4.24) · 1 name(s) re-marked at the open (per-name table). TRLV×106 yday $11.74 → 09:30 $11.78 +4.24 | — |
| 2026-09-03 09:30 ET | **SELL** | `TRLV` | 106 | $11.78 | $2.34 | $+37.76 | $9,807.42 | ▲ +37.76 after sell → book $9,807.42; vs 09:30 mark -2.33 | dropped from list after 4 sess (min 3) | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 24 | $49.76 | $2.06 | — | $8,611.12 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1225.93 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $7,475.64 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1225.93 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DEFT` | 1829 | $0.67 | $17.74 | — | $6,232.47 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1225.93 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 74 | $16.46 | $2.21 | — | $5,012.21 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1225.93 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SID` | 1066 | $1.15 | $13.75 | — | $3,772.56 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; 🔵; ret5=+10.1; leftover $1225.93 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 119 | $10.27 | $2.35 | — | $2,548.09 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1225.93 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CAN` | 4086 | $0.30 | $24.52 | — | $1,297.77 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_mover; 🔵; ret5=+54.3; leftover $1225.93 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CDXS` | 806 | $1.52 | $10.40 | — | $62.25 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_mover; ret5=+7.1; leftover $1225.93 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $62.25 | ▲ close $9,865.32 vs 09:30 $9,809.75 (session +132.95) | 16:00 close · cash $62.25 · equity $9,865.32 vs 09:30 $9,809.75 (+55.57; session marks +132.95) · 8 name(s) marked open→close (per-name table). ATRC×24 09:30 $49.76 → close $52.59 +67.92; RVTY×9 09:30 $125.94 → close $130.94 +45.00; DEFT×1829 09:30 $0.67 → close $0.65 -36.58; ARCT×74 09:30 $16.46 → close $16.74 +20.72; SID×1066 09:30 $1.15 → close $1.17 +21.32; NVAX×119 09:30 $10.27 → close $10.32 +5.95; CAN×4086 09:30 $0.30 → close $0.31 +40.86; CDXS×806 09:30 $1.52 → close $1.48 -32.24 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $62.25 | ▲ 09:30 equity $10,223.92 vs yday $9,865.32 (+358.60) | 09:30 open · cash $62.25 (unchanged overnight, no fees) · equity $10,223.92 vs prior close $9,865.32 (+358.60) · 8 name(s) re-marked at the open (per-name table). ATRC×24 yday $52.59 → 09:30 $52.88 +6.96; RVTY×9 yday $130.94 → 09:30 $132.45 +13.59; DEFT×1829 yday $0.65 → 09:30 $0.65 +0.00; ARCT×74 yday $16.74 → 09:30 $16.77 +2.22; SID×1066 yday $1.17 → 09:30 $1.36 +202.54; NVAX×119 yday $10.32 → 09:30 $10.41 +10.71; CAN×4086 yday $0.31 → 09:30 $0.34 +122.58; CDXS×806 yday $1.48 → 09:30 $1.48 +0.00 | — |
| 2026-09-04 09:30 ET | **BUY** | `TRLV` | 1 | $11.89 | $0.12 | — | $50.24 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; 🔵; ⚪; ret5=+15.0; leftover $12.45 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $50.24 | ▼ close $10,203.78 vs 09:30 $10,223.92 (session -20.02) | 16:00 close · cash $50.24 · equity $10,203.78 vs 09:30 $10,223.92 (-20.14; session marks -20.02) · 9 name(s) marked open→close (per-name table). ATRC×24 09:30 $52.88 → close $52.46 -10.08; RVTY×9 09:30 $132.45 → close $130.63 -16.38; DEFT×1829 09:30 $0.65 → close $0.68 +54.87; ARCT×74 09:30 $16.77 → close $15.56 -89.54; SID×1066 09:30 $1.36 → close $1.26 -106.60; NVAX×119 09:30 $10.41 → close $10.34 -8.33; CAN×4086 09:30 $0.34 → close $0.39 +204.30; CDXS×806 09:30 $1.48 → close $1.42 -48.36; TRLV×1 09:30 $11.89 → close $11.99 +0.10 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLG` | cash | leftover split 2.44 < 1 share @ 57.61 |
| 2026-08-14 | `ADUR` | cash | leftover split 2.44 < 1 share @ 16.50 |
| 2026-08-14 | `ARX` | cash | leftover split 2.44 < 1 share @ 19.57 |
| 2026-08-14 | `AIRO` | cash | leftover split 2.44 < 1 share @ 11.12 |
| 2026-08-14 | `TBBB` | cash | leftover split 2.44 < 1 share @ 48.82 |
| 2026-08-14 | `AMPY` | cash | leftover split 2.44 < 1 share @ 4.94 |
| 2026-08-14 | `SNDK` | cash | leftover split 2.44 < 1 share @ 1646.93 |
| 2026-08-14 | `MH` | cash | leftover split 2.44 < 1 share @ 13.55 |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 2.44 < 1 share @ 46.18 |
| 2026-08-17 | `OCC` | cash | leftover split 2.44 < 1 share @ 18.24 |
| 2026-08-17 | `ALM` | cash | leftover split 2.44 < 1 share @ 16.20 |
| 2026-08-17 | `CAPR` | cash | leftover split 2.44 < 1 share @ 6.87 |
| 2026-08-17 | `HTFL` | cash | leftover split 2.44 < 1 share @ 41.23 |
| 2026-08-17 | `UMAC` | cash | leftover split 2.44 < 1 share @ 32.55 |
| 2026-08-17 | `LPTH` | cash | leftover split 2.44 < 1 share @ 14.94 |
| 2026-08-18 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `UGI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ALEC` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ABUS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 7.42 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 7.42 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 7.42 < 1 share @ 216.30 |
| 2026-08-21 | `TEM` | cash | leftover split 7.42 < 1 share @ 65.60 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABUS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CAN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `DFDV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CAN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `DFDV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ORBS` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CAN` | no_price | no 09:30 open — carry |
| 2026-08-26 | `DFDV` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SENS` | no_price | no 09:30 open |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-26 | `NIQ` | no_price | no 09:30 open |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ZYME` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FIGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ERO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TRLV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CVI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `VIRT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MSTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CAN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WEN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DEFT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ZYME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FIGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NIQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ERO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CVI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `VIRT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ELMT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OMER` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TXG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `GUTS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `UEC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SIBN` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `OMER` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TXG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GUTS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SID` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CAN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CDXS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `HQ` | cash | leftover split 12.45 < 1 share @ 17.06 |
| 2026-09-04 | `NIQ` | cash | leftover split 12.45 < 1 share @ 18.66 |
| 2026-09-04 | `OMER` | cash | leftover split 12.45 < 1 share @ 18.99 |
| 2026-09-04 | `ERO` | cash | leftover split 12.45 < 1 share @ 35.82 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 24 | 2026-09-03 @ $49.76 | union ∩ break10, no 🚨; gate break_10=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1225.93 |
| `RVTY` | 9 | 2026-09-03 @ $125.94 | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1225.93 |
| `DEFT` | 1829 | 2026-09-03 @ $0.67 | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1225.93 |
| `ARCT` | 74 | 2026-09-03 @ $16.46 | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1225.93 |
| `SID` | 1066 | 2026-09-03 @ $1.15 | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; 🔵; ret5=+10.1; leftover $1225.93 |
| `NVAX` | 119 | 2026-09-03 @ $10.27 | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1225.93 |
| `CAN` | 4086 | 2026-09-03 @ $0.30 | union ∩ break10, no 🚨; gate break_10=True; list yday_mover; 🔵; ret5=+54.3; leftover $1225.93 |
| `CDXS` | 806 | 2026-09-03 @ $1.52 | union ∩ break10, no 🚨; gate break_10=True; list yday_mover; ret5=+7.1; leftover $1225.93 |
| `TRLV` | 1 | 2026-09-04 @ $11.89 | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; 🔵; ⚪; ret5=+15.0; leftover $12.45 |
