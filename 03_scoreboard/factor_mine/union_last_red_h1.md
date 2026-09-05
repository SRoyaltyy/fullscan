# Factor mine action — `union_last_red_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ last_red, no 🚨

Cash book **+0.75%** ($10,075) · signal-only (no cash/fees) was +6.47%. Starts YES **5/17**. Fills 138 · skips 55 · realized $+33.45.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `last_red=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $199.47.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `TGTX` | 50 | — | $49.70 | +0.00 | $47.94 | -88.00 | -88.00 | +0.00 | -88.00 |
| 2026-08-13 | `SLS` | 213 | — | $11.70 | +0.00 | $12.36 | +140.58 | +140.58 | +0.00 | +140.58 |
| 2026-08-13 | `HIMS` | 84 | — | $29.74 | +0.00 | $28.77 | -81.48 | -81.48 | +0.00 | -81.48 |
| 2026-08-13 | `VOR` | 113 | — | $22.01 | +0.00 | $23.29 | +144.64 | +144.64 | +0.00 | +144.64 |
| 2026-08-14 | `TGTX` | 50 | $47.94 | $47.27 | -33.50 | — | +0.00 | -33.50 | -121.50 | — |
| 2026-08-14 | `SLS` | 213 | $12.36 | $12.40 | +8.52 | — | +0.00 | +8.52 | +149.10 | — |
| 2026-08-14 | `HIMS` | 84 | $28.77 | $29.15 | +31.92 | — | +0.00 | +31.92 | -49.56 | — |
| 2026-08-14 | `VOR` | 113 | $23.29 | $23.33 | +4.52 | — | +0.00 | +4.52 | +149.16 | — |
| 2026-08-14 | `TLN` | 3 | — | $359.83 | +0.00 | $362.74 | +8.73 | +8.73 | +0.00 | +8.73 |
| 2026-08-14 | `NRG` | 10 | — | $120.00 | +0.00 | $126.24 | +62.40 | +62.40 | +0.00 | +62.40 |
| 2026-08-14 | `MARA` | 140 | — | $9.01 | +0.00 | $9.20 | +26.60 | +26.60 | +0.00 | +26.60 |
| 2026-08-14 | `ARX` | 64 | — | $19.57 | +0.00 | $19.58 | +0.64 | +0.64 | +0.00 | +0.64 |
| 2026-08-14 | `HLIT` | 95 | — | $13.18 | +0.00 | $13.92 | +70.30 | +70.30 | +0.00 | +70.30 |
| 2026-08-14 | `SECZ` | 216 | — | $5.84 | +0.00 | $5.61 | -49.68 | -49.68 | +0.00 | -49.68 |
| 2026-08-14 | `LFTO` | 61 | — | $20.57 | +0.00 | $21.61 | +63.44 | +63.44 | +0.00 | +63.44 |
| 2026-08-14 | `REZI` | 61 | — | $20.56 | +0.00 | $20.50 | -3.66 | -3.66 | +0.00 | -3.66 |
| 2026-08-17 | `TLN` | 3 | $362.74 | $367.88 | +15.42 | — | +0.00 | +15.42 | +24.15 | — |
| 2026-08-17 | `NRG` | 10 | $126.24 | $127.40 | +11.60 | — | +0.00 | +11.60 | +74.00 | — |
| 2026-08-17 | `MARA` | 140 | $9.20 | $9.22 | +2.80 | — | +0.00 | +2.80 | +29.40 | — |
| 2026-08-17 | `ARX` | 64 | $19.58 | $19.57 | -0.64 | — | +0.00 | -0.64 | +0.00 | — |
| 2026-08-17 | `HLIT` | 95 | $13.92 | $13.84 | -7.60 | — | +0.00 | -7.60 | +62.70 | — |
| 2026-08-17 | `SECZ` | 216 | $5.61 | $5.45 | -34.56 | — | +0.00 | -34.56 | -84.24 | — |
| 2026-08-17 | `LFTO` | 61 | $21.61 | $21.00 | -37.21 | — | +0.00 | -37.21 | +26.23 | — |
| 2026-08-17 | `REZI` | 61 | $20.50 | $20.83 | +20.13 | — | +0.00 | +20.13 | +16.47 | — |
| 2026-08-17 | `TMC` | 315 | — | $4.05 | +0.00 | $3.77 | -88.20 | -88.20 | +0.00 | -88.20 |
| 2026-08-17 | `TGB` | 151 | — | $8.46 | +0.00 | $8.77 | +46.81 | +46.81 | +0.00 | +46.81 |
| 2026-08-17 | `ELF` | 14 | — | $90.54 | +0.00 | $93.66 | +43.68 | +43.68 | +0.00 | +43.68 |
| 2026-08-17 | `DNN` | 394 | — | $3.24 | +0.00 | $3.19 | -19.70 | -19.70 | +0.00 | -19.70 |
| 2026-08-17 | `CAPR` | 185 | — | $6.87 | +0.00 | $7.45 | +107.30 | +107.30 | +0.00 | +107.30 |
| 2026-08-17 | `NU` | 82 | — | $15.40 | +0.00 | $14.74 | -54.12 | -54.12 | +0.00 | -54.12 |
| 2026-08-17 | `INV` | 788 | — | $1.62 | +0.00 | $1.39 | -185.18 | -185.18 | +0.00 | -185.18 |
| 2026-08-17 | `KLC` | 487 | — | $2.62 | +0.00 | $2.56 | -29.22 | -29.22 | +0.00 | -29.22 |
| 2026-08-18 | `TMC` | 315 | $3.77 | $3.72 | -15.75 | — | +0.00 | -15.75 | -103.95 | — |
| 2026-08-18 | `TGB` | 151 | $8.77 | $8.55 | -33.22 | — | +0.00 | -33.22 | +13.59 | — |
| 2026-08-18 | `ELF` | 14 | $93.66 | $93.44 | -3.08 | — | +0.00 | -3.08 | +40.60 | — |
| 2026-08-18 | `DNN` | 394 | $3.19 | $3.11 | -31.52 | — | +0.00 | -31.52 | -51.22 | — |
| 2026-08-18 | `CAPR` | 185 | $7.45 | $7.50 | +9.25 | — | +0.00 | +9.25 | +116.55 | — |
| 2026-08-18 | `NU` | 82 | $14.74 | $14.53 | -17.22 | — | +0.00 | -17.22 | -71.34 | — |
| 2026-08-18 | `INV` | 788 | $1.39 | $1.32 | -47.28 | — | +0.00 | -47.28 | -232.46 | — |
| 2026-08-18 | `KLC` | 487 | $2.56 | $2.52 | -19.48 | — | +0.00 | -19.48 | -48.70 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `MRVI` | 166 | — | $7.38 | +0.00 | $8.26 | +146.08 | +146.08 | +0.00 | +146.08 |
| 2026-08-20 | `WYFI` | 57 | — | $21.40 | +0.00 | $21.16 | -13.68 | -13.68 | +0.00 | -13.68 |
| 2026-08-20 | `TOYO` | 276 | — | $4.43 | +0.00 | $4.51 | +23.46 | +23.46 | +0.00 | +23.46 |
| 2026-08-20 | `DVLT` | 4088 | — | $0.30 | +0.00 | $0.32 | +81.76 | +81.76 | +0.00 | +81.76 |
| 2026-08-20 | `SAFX` | 3465 | — | $0.35 | +0.00 | $0.34 | -38.11 | -38.11 | +0.00 | -38.11 |
| 2026-08-20 | `AAP` | 26 | — | $46.85 | +0.00 | $42.39 | -115.96 | -115.96 | +0.00 | -115.96 |
| 2026-08-20 | `AEG` | 136 | — | $9.01 | +0.00 | $9.01 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `MRVI` | 166 | $8.26 | $8.20 | -9.96 | $8.70 | +83.00 | +73.04 | +136.12 | +219.12 |
| 2026-08-21 | `WYFI` | 57 | $21.16 | $21.54 | +21.66 | — | +0.00 | +21.66 | +7.98 | — |
| 2026-08-21 | `TOYO` | 276 | $4.51 | $4.68 | +45.54 | — | +0.00 | +45.54 | +69.00 | — |
| 2026-08-21 | `DVLT` | 4088 | $0.32 | $0.31 | -40.88 | — | +0.00 | -40.88 | +40.88 | — |
| 2026-08-21 | `SAFX` | 3465 | $0.34 | $0.35 | +24.25 | — | +0.00 | +24.25 | -13.86 | — |
| 2026-08-21 | `AAP` | 26 | $42.39 | $42.41 | +0.52 | — | +0.00 | +0.52 | -115.44 | — |
| 2026-08-21 | `AEG` | 136 | $9.01 | $9.04 | +4.08 | — | +0.00 | +4.08 | +4.08 | — |
| 2026-08-21 | `AUTL` | 492 | — | $2.47 | +0.00 | $2.41 | -29.52 | -29.52 | +0.00 | -29.52 |
| 2026-08-21 | `CRDL` | 630 | — | $1.93 | +0.00 | $1.86 | -44.10 | -44.10 | +0.00 | -44.10 |
| 2026-08-21 | `CRSP` | 20 | — | $59.72 | +0.00 | $59.50 | -4.40 | -4.40 | +0.00 | -4.40 |
| 2026-08-21 | `FUTU` | 10 | — | $115.18 | +0.00 | $123.64 | +84.60 | +84.60 | +0.00 | +84.60 |
| 2026-08-21 | `GMAB` | 36 | — | $33.36 | +0.00 | $33.45 | +3.24 | +3.24 | +0.00 | +3.24 |
| 2026-08-21 | `ENHA` | 711 | — | $1.71 | +0.00 | $1.72 | +7.11 | +7.11 | +0.00 | +7.11 |
| 2026-08-21 | `CAN` | 4139 | — | $0.29 | +0.00 | $0.35 | +252.48 | +252.48 | +0.00 | +252.48 |
| 2026-08-24 | `MRVI` | 166 | $8.70 | $8.59 | -18.26 | — | +0.00 | -18.26 | +200.86 | — |
| 2026-08-24 | `AUTL` | 492 | $2.41 | $2.36 | -24.60 | — | +0.00 | -24.60 | -54.12 | — |
| 2026-08-24 | `CRDL` | 630 | $1.86 | $1.87 | +6.30 | — | +0.00 | +6.30 | -37.80 | — |
| 2026-08-24 | `CRSP` | 20 | $59.50 | $58.79 | -14.20 | $56.91 | -37.60 | -51.80 | -18.60 | -56.20 |
| 2026-08-24 | `FUTU` | 10 | $123.64 | $120.87 | -27.70 | — | +0.00 | -27.70 | +56.90 | — |
| 2026-08-24 | `GMAB` | 36 | $33.45 | $32.82 | -22.68 | — | +0.00 | -22.68 | -19.44 | — |
| 2026-08-24 | `ENHA` | 711 | $1.72 | $1.74 | +14.22 | — | +0.00 | +14.22 | +21.33 | — |
| 2026-08-24 | `CAN` | 4139 | $0.35 | $0.38 | +103.48 | — | +0.00 | +103.48 | +355.95 | — |
| 2026-08-25 | `CRSP` | 20 | $56.91 | $57.00 | +1.80 | — | +0.00 | +1.80 | -54.40 | — |
| 2026-08-25 | `OCUL` | 115 | — | $10.92 | +0.00 | $10.92 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `CRMD` | 152 | — | $8.28 | +0.00 | $8.28 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `PUSA` | 341 | — | $3.70 | +0.00 | $3.91 | +71.61 | +71.61 | +0.00 | +71.61 |
| 2026-08-25 | `CAPR` | 185 | — | $6.79 | +0.00 | $7.19 | +74.00 | +74.00 | +0.00 | +74.00 |
| 2026-08-25 | `SAFX` | 3411 | — | $0.37 | +0.00 | $0.37 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `SUJA` | 143 | — | $8.79 | +0.00 | $8.54 | -35.75 | -35.75 | +0.00 | -35.75 |
| 2026-08-25 | `FWDI` | 210 | — | $5.99 | +0.00 | $5.86 | -27.30 | -27.30 | +0.00 | -27.30 |
| 2026-08-25 | `JANX` | 67 | — | $18.52 | +0.00 | $18.99 | +31.49 | +31.49 | +0.00 | +31.49 |
| 2026-08-26 | `OCUL` | 115 | $10.92 | $10.92 | +0.00 | $10.92 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `CRMD` | 152 | $8.28 | $8.28 | +0.00 | $8.28 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `PUSA` | 341 | $3.91 | $3.91 | +0.00 | $3.91 | +0.00 | +0.00 | +71.61 | +71.61 |
| 2026-08-26 | `CAPR` | 185 | $7.19 | $7.19 | +0.00 | $7.19 | +0.00 | +0.00 | +74.00 | +74.00 |
| 2026-08-26 | `SAFX` | 3411 | $0.37 | $0.37 | +0.00 | $0.37 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `SUJA` | 143 | $8.54 | $8.54 | +0.00 | $8.54 | +0.00 | +0.00 | -35.75 | -35.75 |
| 2026-08-26 | `FWDI` | 210 | $5.86 | $5.86 | +0.00 | $5.86 | +0.00 | +0.00 | -27.30 | -27.30 |
| 2026-08-26 | `JANX` | 67 | $18.99 | $18.99 | +0.00 | $18.99 | +0.00 | +0.00 | +31.49 | +31.49 |
| 2026-08-27 | `OCUL` | 115 | $10.92 | $10.79 | -14.95 | — | +0.00 | -14.95 | -14.95 | — |
| 2026-08-27 | `CRMD` | 152 | $8.28 | $8.60 | +48.64 | — | +0.00 | +48.64 | +48.64 | — |
| 2026-08-27 | `PUSA` | 341 | $3.91 | $3.84 | -23.87 | — | +0.00 | -23.87 | +47.74 | — |
| 2026-08-27 | `CAPR` | 185 | $7.19 | $8.29 | +203.50 | — | +0.00 | +203.50 | +277.50 | — |
| 2026-08-27 | `SAFX` | 3411 | $0.37 | $0.35 | -68.22 | — | +0.00 | -68.22 | -68.22 | — |
| 2026-08-27 | `SUJA` | 143 | $8.54 | $9.39 | +121.55 | — | +0.00 | +121.55 | +85.80 | — |
| 2026-08-27 | `FWDI` | 210 | $5.86 | $5.97 | +23.10 | — | +0.00 | +23.10 | -4.20 | — |
| 2026-08-27 | `JANX` | 67 | $18.99 | $18.59 | -26.80 | — | +0.00 | -26.80 | +4.69 | — |
| 2026-08-27 | `ACMR` | 16 | — | $80.97 | +0.00 | $79.11 | -29.76 | -29.76 | +0.00 | -29.76 |
| 2026-08-27 | `GGB` | 293 | — | $4.42 | +0.00 | $4.46 | +11.72 | +11.72 | +0.00 | +11.72 |
| 2026-08-27 | `MT` | 17 | — | $75.12 | +0.00 | $74.53 | -10.03 | -10.03 | +0.00 | -10.03 |
| 2026-08-27 | `MU` | 1 | — | $925.74 | +0.00 | $938.40 | +12.66 | +12.66 | +0.00 | +12.66 |
| 2026-08-27 | `TX` | 23 | — | $55.20 | +0.00 | $55.13 | -1.61 | -1.61 | +0.00 | -1.61 |
| 2026-08-27 | `LRCX` | 4 | — | $314.61 | +0.00 | $312.88 | -6.92 | -6.92 | +0.00 | -6.92 |
| 2026-08-27 | `MRVL` | 5 | — | $240.00 | +0.00 | $245.11 | +25.55 | +25.55 | +0.00 | +25.55 |
| 2026-08-27 | `NUE` | 5 | — | $248.91 | +0.00 | $252.80 | +19.45 | +19.45 | +0.00 | +19.45 |
| 2026-08-28 | `ACMR` | 16 | $79.11 | $81.65 | +40.64 | — | +0.00 | +40.64 | +10.88 | — |
| 2026-08-28 | `GGB` | 293 | $4.46 | $4.57 | +32.23 | — | +0.00 | +32.23 | +43.95 | — |
| 2026-08-28 | `MT` | 17 | $74.53 | $74.54 | +0.17 | — | +0.00 | +0.17 | -9.86 | — |
| 2026-08-28 | `MU` | 1 | $938.40 | $967.01 | +28.61 | — | +0.00 | +28.61 | +41.27 | — |
| 2026-08-28 | `TX` | 23 | $55.13 | $55.25 | +2.76 | — | +0.00 | +2.76 | +1.15 | — |
| 2026-08-28 | `LRCX` | 4 | $312.88 | $318.88 | +24.00 | — | +0.00 | +24.00 | +17.08 | — |
| 2026-08-28 | `MRVL` | 5 | $245.11 | $253.44 | +41.65 | — | +0.00 | +41.65 | +67.20 | — |
| 2026-08-28 | `NUE` | 5 | $252.80 | $252.00 | -4.00 | — | +0.00 | -4.00 | +15.45 | — |
| 2026-08-28 | `CAPR` | 143 | — | $9.19 | +0.00 | $10.06 | +124.41 | +124.41 | +0.00 | +124.41 |
| 2026-08-28 | `SEDG` | 39 | — | $33.78 | +0.00 | $33.51 | -10.53 | -10.53 | +0.00 | -10.53 |
| 2026-08-28 | `SMTC` | 8 | — | $149.40 | +0.00 | $142.43 | -55.76 | -55.76 | +0.00 | -55.76 |
| 2026-08-28 | `OPTX` | 153 | — | $8.57 | +0.00 | $8.73 | +24.48 | +24.48 | +0.00 | +24.48 |
| 2026-08-28 | `TTMI` | 10 | — | $127.07 | +0.00 | $124.73 | -23.40 | -23.40 | +0.00 | -23.40 |
| 2026-08-28 | `BBWI` | 70 | — | $18.68 | +0.00 | $18.65 | -2.10 | -2.10 | +0.00 | -2.10 |
| 2026-08-28 | `BTSG` | 21 | — | $61.42 | +0.00 | $60.90 | -10.92 | -10.92 | +0.00 | -10.92 |
| 2026-08-28 | `CRDL` | 630 | — | $2.09 | +0.00 | $2.06 | -18.90 | -18.90 | +0.00 | -18.90 |
| 2026-08-31 | `CAPR` | 143 | $10.06 | $9.44 | -88.66 | — | +0.00 | -88.66 | +35.75 | — |
| 2026-08-31 | `SEDG` | 39 | $33.51 | $31.50 | -78.39 | — | +0.00 | -78.39 | -88.92 | — |
| 2026-08-31 | `SMTC` | 8 | $142.43 | $133.04 | -75.12 | — | +0.00 | -75.12 | -130.88 | — |
| 2026-08-31 | `OPTX` | 153 | $8.73 | $8.52 | -32.13 | — | +0.00 | -32.13 | -7.65 | — |
| 2026-08-31 | `TTMI` | 10 | $124.73 | $117.20 | -75.30 | — | +0.00 | -75.30 | -98.70 | — |
| 2026-08-31 | `BBWI` | 70 | $18.65 | $19.30 | +45.50 | — | +0.00 | +45.50 | +43.40 | — |
| 2026-08-31 | `BTSG` | 21 | $60.90 | $59.66 | -26.04 | — | +0.00 | -26.04 | -36.96 | — |
| 2026-08-31 | `CRDL` | 630 | $2.06 | $1.96 | -63.00 | — | +0.00 | -63.00 | -81.90 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `CABA` | 387 | — | $3.27 | +0.00 | $3.57 | +116.10 | +116.10 | +0.00 | +116.10 |
| 2026-09-03 | `FRVO` | 68 | — | $18.40 | +0.00 | $17.98 | -28.56 | -28.56 | +0.00 | -28.56 |
| 2026-09-03 | `CTMX` | 340 | — | $3.72 | +0.00 | $3.72 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `EIX` | 22 | — | $56.78 | +0.00 | $55.19 | -34.98 | -34.98 | +0.00 | -34.98 |
| 2026-09-03 | `CRDL` | 586 | — | $2.16 | +0.00 | $2.17 | +5.86 | +5.86 | +0.00 | +5.86 |
| 2026-09-03 | `SION` | 190 | — | $6.63 | +0.00 | $7.31 | +129.20 | +129.20 | +0.00 | +129.20 |
| 2026-09-03 | `DUOL` | 8 | — | $156.24 | +0.00 | $157.85 | +12.88 | +12.88 | +0.00 | +12.88 |
| 2026-09-03 | `SAFX` | 3246 | — | $0.39 | +0.00 | $0.38 | -32.46 | -32.46 | +0.00 | -32.46 |
| 2026-09-04 | `CABA` | 387 | $3.57 | $3.63 | +23.22 | $3.48 | -58.05 | -34.83 | +139.32 | +81.27 |
| 2026-09-04 | `FRVO` | 68 | $17.98 | $18.27 | +19.72 | — | +0.00 | +19.72 | -8.84 | — |
| 2026-09-04 | `CTMX` | 340 | $3.72 | $3.73 | +3.40 | — | +0.00 | +3.40 | +3.40 | — |
| 2026-09-04 | `EIX` | 22 | $55.19 | $55.42 | +5.06 | — | +0.00 | +5.06 | -29.92 | — |
| 2026-09-04 | `CRDL` | 586 | $2.17 | $2.18 | +5.86 | — | +0.00 | +5.86 | +11.72 | — |
| 2026-09-04 | `SION` | 190 | $7.31 | $7.31 | +0.00 | $6.75 | -106.40 | -106.40 | +129.20 | +22.80 |
| 2026-09-04 | `DUOL` | 8 | $157.85 | $161.54 | +29.52 | — | +0.00 | +29.52 | +42.40 | — |
| 2026-09-04 | `SAFX` | 3246 | $0.38 | $0.38 | +0.00 | — | +0.00 | +0.00 | -32.46 | — |
| 2026-09-04 | `ASND` | 4 | — | $266.94 | +0.00 | $271.12 | +16.72 | +16.72 | +0.00 | +16.72 |
| 2026-09-04 | `SLBT` | 407 | — | $3.07 | +0.00 | $3.15 | +32.56 | +32.56 | +0.00 | +32.56 |
| 2026-09-04 | `MLYS` | 42 | — | $29.15 | +0.00 | $28.27 | -36.96 | -36.96 | +0.00 | -36.96 |
| 2026-09-04 | `CCOI` | 122 | — | $10.22 | +0.00 | $9.98 | -29.28 | -29.28 | +0.00 | -29.28 |
| 2026-09-04 | `IRD` | 268 | — | $4.66 | +0.00 | $4.60 | -16.08 | -16.08 | +0.00 | -16.08 |
| 2026-09-04 | `JLHL` | 201 | — | $6.20 | +0.00 | $6.18 | -4.02 | -4.02 | +0.00 | -4.02 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +115.74 | TGTX, SLS, HIMS, VOR | — | $28.15 | $10,106.28 | TGTX×50, SLS×213, HIMS×84, VOR×113 |
| 2026-08-14 | +5.50 | $28.15 | TGTX×50, SLS×213, HIMS×84, VOR×113 | $10,117.74 | +11.46 | +178.77 | TLN, NRG, MARA, ARX, HLIT, SECZ, LFTO, REZI | TGTX, SLS, HIMS, VOR | $274.27 | $10,268.88 | TLN×3, NRG×10, MARA×140, ARX×64, HLIT×95, SECZ×216, LFTO×61, REZI×61 |
| 2026-08-17 | +2.25 | $274.27 | TLN×3, NRG×10, MARA×140, ARX×64, HLIT×95, SECZ×216, LFTO×61, REZI×61 | $10,238.82 | -30.06 | -178.63 | TMC, TGB, ELF, DNN, CAPR, NU, INV, KLC | TLN, NRG, MARA, ARX, HLIT, SECZ, LFTO, REZI | $2.16 | $10,007.11 | TMC×315, TGB×151, ELF×14, DNN×394, CAPR×185, NU×82, INV×788, KLC×487 |
| 2026-08-18 | -6.20 | $2.16 | TMC×315, TGB×151, ELF×14, DNN×394, CAPR×185, NU×82, INV×788, KLC×487 | $9,848.81 | -158.30 | +0.00 | — | TMC, TGB, ELF, DNN, CAPR, NU, INV, KLC | $9,813.47 | $9,813.47 | — |
| 2026-08-19 | -7.20 | $9,813.47 | — | $9,813.47 | -0.00 | +0.00 | — | — | $9,813.47 | $9,813.47 | — |
| 2026-08-20 | +1.12 | $9,813.47 | — | $9,813.47 | -0.00 | +117.61 | BHP, MRVI, WYFI, TOYO, DVLT, SAFX, AAP, AEG | — | $4.42 | $9,869.18 | BHP×13, MRVI×166, WYFI×57, TOYO×276, DVLT×4088, SAFX×3465, AAP×26, AEG×136 |
| 2026-08-21 | +3.25 | $4.42 | BHP×13, MRVI×166, WYFI×57, TOYO×276, DVLT×4088, SAFX×3465, AAP×26, AEG×136 | $9,941.57 | +72.39 | +352.41 | AUTL, CRDL, CRSP, FUTU, GMAB, ENHA, CAN | BHP, WYFI, TOYO, DVLT, SAFX, AAP, AEG | $53.89 | $10,178.48 | MRVI×166, AUTL×492, CRDL×630, CRSP×20, FUTU×10, GMAB×36, ENHA×711, CAN×4139 |
| 2026-08-24 | -5.17 | $53.89 | MRVI×166, AUTL×492, CRDL×630, CRSP×20, FUTU×10, GMAB×36, ENHA×711, CAN×4139 | $10,195.03 | +16.55 | -37.60 | — | MRVI, AUTL, CRDL, FUTU, GMAB, ENHA, CAN | $8,959.72 | $10,097.92 | CRSP×20 |
| 2026-08-25 | +1.80 | $8,959.72 | CRSP×20 | $10,099.72 | +1.80 | +114.05 | OCUL, CRMD, PUSA, CAPR, SAFX, SUJA, FWDI, JANX | CRSP | $5.77 | $10,169.81 | OCUL×115, CRMD×152, PUSA×341, CAPR×185, SAFX×3411, SUJA×143, FWDI×210, JANX×67 |
| 2026-08-26 | +2.02 | $5.77 | OCUL×115, CRMD×152, PUSA×341, CAPR×185, SAFX×3411, SUJA×143, FWDI×210, JANX×67 | $10,169.81 | -0.00 | +0.00 | — | — | $5.77 | $10,169.81 | OCUL×115, CRMD×152, PUSA×341, CAPR×185, SAFX×3411, SUJA×143, FWDI×210, JANX×67 |
| 2026-08-27 | — | $5.77 | OCUL×115, CRMD×152, PUSA×341, CAPR×185, SAFX×3411, SUJA×143, FWDI×210, JANX×67 | $10,432.76 | +262.95 | +21.06 | ACMR, GGB, MT, MU, TX, LRCX, MRVL, NUE | OCUL, CRMD, PUSA, CAPR, SAFX, SUJA, FWDI, JANX | $606.82 | $10,393.83 | ACMR×16, GGB×293, MT×17, MU×1, TX×23, LRCX×4, MRVL×5, NUE×5 |
| 2026-08-28 | +0.75 | $606.82 | ACMR×16, GGB×293, MT×17, MU×1, TX×23, LRCX×4, MRVL×5, NUE×5 | $10,559.89 | +166.06 | +27.28 | CAPR, SEDG, SMTC, OPTX, TTMI, BBWI, BTSG, CRDL | ACMR, GGB, MT, MU, TX, LRCX, MRVL, NUE | $195.55 | $10,545.65 | CAPR×143, SEDG×39, SMTC×8, OPTX×153, TTMI×10, BBWI×70, BTSG×21, CRDL×630 |
| 2026-08-31 | -5.85 | $195.55 | CAPR×143, SEDG×39, SMTC×8, OPTX×153, TTMI×10, BBWI×70, BTSG×21, CRDL×630 | $10,152.51 | -393.14 | +0.00 | — | CAPR, SEDG, SMTC, OPTX, TTMI, BBWI, BTSG, CRDL | $10,128.84 | $10,128.84 | — |
| 2026-09-01 | -6.30 | $10,128.84 | — | $10,128.84 | -0.00 | +0.00 | — | — | $10,128.84 | $10,128.84 | — |
| 2026-09-02 | -3.83 | $10,128.84 | — | $10,128.84 | -0.00 | +0.00 | — | — | $10,128.84 | $10,128.84 | — |
| 2026-09-03 | -0.90 | $10,128.84 | — | $10,128.84 | -0.00 | +168.04 | CABA, FRVO, CTMX, EIX, CRDL, SION, DUOL, SAFX | — | $8.71 | $10,248.72 | CABA×387, FRVO×68, CTMX×340, EIX×22, CRDL×586, SION×190, DUOL×8, SAFX×3246 |
| 2026-09-04 | — | $8.71 | CABA×387, FRVO×68, CTMX×340, EIX×22, CRDL×586, SION×190, DUOL×8, SAFX×3246 | $10,335.50 | +86.78 | -201.51 | ASND, SLBT, MLYS, CCOI, IRD, JLHL | FRVO, CTMX, EIX, CRDL, DUOL, SAFX | $199.47 | $10,075.14 | CABA×387, SION×190, ASND×4, SLBT×407, MLYS×42, CCOI×122, IRD×268, JLHL×201 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 50 | $49.70 | $2.14 | — | $7,512.86 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=-0.8; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 213 | $11.70 | $2.75 | — | $5,018.01 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=-0.8; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 84 | $29.74 | $2.24 | — | $2,517.61 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=-5.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 113 | $22.01 | $2.33 | — | $28.15 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=+0.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.15 | ▲ close $10,106.28 vs 09:30 $10,000.00 (session +115.74) | 16:00 close · cash $28.15 · equity $10,106.28 vs 09:30 $10,000.00 (+106.28; session marks +115.74) · 4 name(s) marked open→close (per-name table). TGTX×50 09:30 $49.70 → close $47.94 -88.00; SLS×213 09:30 $11.70 → close $12.36 +140.58; HIMS×84 09:30 $29.74 → close $28.77 -81.48; VOR×113 09:30 $22.01 → close $23.29 +144.64 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.15 | ▲ 09:30 equity $10,117.74 vs yday $10,106.28 (+11.46) | 09:30 open · cash $28.15 (unchanged overnight, no fees) · equity $10,117.74 vs prior close $10,106.28 (+11.46) · 4 name(s) re-marked at the open (per-name table). TGTX×50 yday $47.94 → 09:30 $47.27 -33.50; SLS×213 yday $12.36 → 09:30 $12.40 +8.52; HIMS×84 yday $28.77 → 09:30 $29.15 +31.92; VOR×113 yday $23.29 → 09:30 $23.33 +4.52 | — |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 50 | $47.27 | $2.17 | $-125.81 | $2,389.48 | ▼ -125.81 after sell → book $10,115.57; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 213 | $12.40 | $2.80 | $+143.55 | $5,027.88 | ▲ +143.55 after sell → book $10,112.77; vs 09:30 mark -2.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 84 | $29.15 | $2.28 | $-54.08 | $7,474.20 | ▼ -54.08 after sell → book $10,110.49; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 113 | $23.33 | $2.37 | $+144.46 | $10,108.12 | ▲ +144.46 after sell → book $10,108.12; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $9,026.63 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ret5=+5.9; leftover $1263.52 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $7,824.61 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ret5=+0.6; leftover $1263.52 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 140 | $9.01 | $2.41 | — | $6,560.80 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1263.52 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 64 | $19.57 | $2.18 | — | $5,306.14 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1263.52 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 95 | $13.18 | $2.27 | — | $4,051.77 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1263.52 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SECZ` | 216 | $5.84 | $2.79 | — | $2,787.54 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ⚪; ret5=-20.7; leftover $1263.52 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LFTO` | 61 | $20.57 | $2.17 | — | $1,530.60 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ⚪; ret5=-14.0; leftover $1263.52 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `REZI` | 61 | $20.56 | $2.17 | — | $274.27 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ⚪; ret5=-21.5; leftover $1263.52 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $274.27 | ▲ close $10,268.88 vs 09:30 $10,117.74 (session +178.77) | 16:00 close · cash $274.27 · equity $10,268.88 vs 09:30 $10,117.74 (+151.14; session marks +178.77) · 8 name(s) marked open→close (per-name table). TLN×3 09:30 $359.83 → close $362.74 +8.73; NRG×10 09:30 $120.00 → close $126.24 +62.40; MARA×140 09:30 $9.01 → close $9.20 +26.60; ARX×64 09:30 $19.57 → close $19.58 +0.64; HLIT×95 09:30 $13.18 → close $13.92 +70.30; SECZ×216 09:30 $5.84 → close $5.61 -49.68; LFTO×61 09:30 $20.57 → close $21.61 +63.44; REZI×61 09:30 $20.56 → close $20.50 -3.66 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $274.27 | ▼ 09:30 equity $10,238.82 vs yday $10,268.88 (-30.06) | 09:30 open · cash $274.27 (unchanged overnight, no fees) · equity $10,238.82 vs prior close $10,268.88 (-30.06) · 8 name(s) re-marked at the open (per-name table). TLN×3 yday $362.74 → 09:30 $367.88 +15.42; NRG×10 yday $126.24 → 09:30 $127.40 +11.60; MARA×140 yday $9.20 → 09:30 $9.22 +2.80; ARX×64 yday $19.58 → 09:30 $19.57 -0.64; HLIT×95 yday $13.92 → 09:30 $13.84 -7.60; SECZ×216 yday $5.61 → 09:30 $5.45 -34.56; LFTO×61 yday $21.61 → 09:30 $21.00 -37.21; REZI×61 yday $20.50 → 09:30 $20.83 +20.13 | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 3 | $367.88 | $2.02 | $+20.13 | $1,375.89 | ▲ +20.13 after sell → book $10,236.80; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $2,647.85 | ▲ +69.94 after sell → book $10,234.76; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 140 | $9.22 | $2.44 | $+24.55 | $3,936.20 | ▲ +24.55 after sell → book $10,232.31; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 64 | $19.57 | $2.20 | $-4.38 | $5,186.48 | ▼ -4.38 after sell → book $10,230.11; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 95 | $13.84 | $2.30 | $+58.12 | $6,498.98 | ▲ +58.12 after sell → book $10,227.81; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SECZ` | 216 | $5.45 | $2.83 | $-89.86 | $7,673.35 | ▼ -89.86 after sell → book $10,224.98; vs 09:30 mark -2.83 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LFTO` | 61 | $21.00 | $2.19 | $+21.86 | $8,952.15 | ▲ +21.86 after sell → book $10,222.78; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `REZI` | 61 | $20.83 | $2.19 | $+12.10 | $10,220.59 | ▲ +12.10 after sell → book $10,220.59; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 315 | $4.05 | $4.06 | — | $8,940.78 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1277.57 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 151 | $8.46 | $2.44 | — | $7,660.87 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1277.57 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 14 | $90.54 | $2.03 | — | $6,391.28 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ret5=-7.2; leftover $1277.57 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 394 | $3.24 | $5.08 | — | $5,109.64 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ⚪; ret5=+0.3; leftover $1277.57 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 185 | $6.87 | $2.54 | — | $3,836.14 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; ret5=+62.6; leftover $1277.57 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NU` | 82 | $15.40 | $2.24 | — | $2,571.11 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ⚪; ret5=+10.0; leftover $1277.57 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `INV` | 788 | $1.62 | $10.17 | — | $1,284.38 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ⚪; ret5=-53.0; leftover $1277.57 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `KLC` | 487 | $2.62 | $6.28 | — | $2.16 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ⚪; ret5=-49.7; leftover $1277.57 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.16 | ▼ close $10,007.11 vs 09:30 $10,238.82 (session -178.63) | 16:00 close · cash $2.16 · equity $10,007.11 vs 09:30 $10,238.82 (-231.71; session marks -178.63) · 8 name(s) marked open→close (per-name table). TMC×315 09:30 $4.05 → close $3.77 -88.20; TGB×151 09:30 $8.46 → close $8.77 +46.81; ELF×14 09:30 $90.54 → close $93.66 +43.68; DNN×394 09:30 $3.24 → close $3.19 -19.70; CAPR×185 09:30 $6.87 → close $7.45 +107.30; NU×82 09:30 $15.40 → close $14.74 -54.12; INV×788 09:30 $1.62 → close $1.39 -185.18; KLC×487 09:30 $2.62 → close $2.56 -29.22 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.16 | ▼ 09:30 equity $9,848.81 vs yday $10,007.11 (-158.30) | 09:30 open · cash $2.16 (unchanged overnight, no fees) · equity $9,848.81 vs prior close $10,007.11 (-158.30) · 8 name(s) re-marked at the open (per-name table). TMC×315 yday $3.77 → 09:30 $3.72 -15.75; TGB×151 yday $8.77 → 09:30 $8.55 -33.22; ELF×14 yday $93.66 → 09:30 $93.44 -3.08; DNN×394 yday $3.19 → 09:30 $3.11 -31.52; CAPR×185 yday $7.45 → 09:30 $7.50 +9.25; NU×82 yday $14.74 → 09:30 $14.53 -17.22; INV×788 yday $1.39 → 09:30 $1.32 -47.28; KLC×487 yday $2.56 → 09:30 $2.52 -19.48 | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 315 | $3.72 | $4.13 | $-112.14 | $1,169.83 | ▼ -112.14 after sell → book $9,844.68; vs 09:30 mark -4.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 151 | $8.55 | $2.48 | $+8.67 | $2,458.41 | ▲ +8.67 after sell → book $9,842.21; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 14 | $93.44 | $2.05 | $+36.52 | $3,764.51 | ▲ +36.52 after sell → book $9,840.15; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 394 | $3.11 | $5.16 | $-61.46 | $4,984.70 | ▼ -61.46 after sell → book $9,835.00; vs 09:30 mark -5.15 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `CAPR` | 185 | $7.50 | $2.59 | $+111.42 | $6,369.61 | ▲ +111.42 after sell → book $9,832.41; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `NU` | 82 | $14.53 | $2.26 | $-75.84 | $7,558.81 | ▼ -75.84 after sell → book $9,830.15; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `INV` | 788 | $1.32 | $10.31 | $-252.93 | $8,592.60 | ▼ -252.93 after sell → book $9,819.84; vs 09:30 mark -10.31 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `KLC` | 487 | $2.52 | $6.37 | $-61.36 | $9,813.47 | ▼ -61.36 after sell → book $9,813.47; vs 09:30 mark -6.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,813.47 | ▲ close $9,813.47 vs 09:30 $9,848.81 (session +0.00) | 16:00 close · cash $9,813.47 · no lots left · equity $9,813.47. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,813.47 | ▲ 09:30 equity $9,813.47 vs yday $9,813.47 (-0.00) | 09:30 open · cash $9,813.47 · no holdings · equity $9,813.47 vs prior close $9,813.47 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,813.47 | ▲ close $9,813.47 vs 09:30 $9,813.47 (session +0.00) | 16:00 close · cash $9,813.47 · no lots left · equity $9,813.47. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,813.47 | ▲ 09:30 equity $9,813.47 vs yday $9,813.47 (-0.00) | 09:30 open · cash $9,813.47 · no holdings · equity $9,813.47 vs prior close $9,813.47 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,628.31 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1226.68 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 166 | $7.38 | $2.49 | — | $7,400.74 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1226.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WYFI` | 57 | $21.40 | $2.16 | — | $6,178.78 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-25.2; leftover $1226.68 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 276 | $4.43 | $3.56 | — | $4,952.54 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-23.1; leftover $1226.68 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 4088 | $0.30 | $24.53 | — | $3,701.61 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-3.2; leftover $1226.68 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SAFX` | 3465 | $0.35 | $22.66 | — | $2,452.34 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_mover; 🔵; ret5=-29.4; leftover $1226.68 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 26 | $46.85 | $2.07 | — | $1,232.17 | — | union ∩ last_red, no 🚨; gate last_red=True; list earn_react; 🔵; ret5=+5.0; leftover $1226.68 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 136 | $9.01 | $2.40 | — | $4.42 | — | union ∩ last_red, no 🚨; gate last_red=True; list earn_react; 🔵; ⚪; ret5=-1.3; leftover $1226.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.42 | ▲ close $9,869.18 vs 09:30 $9,813.47 (session +117.61) | 16:00 close · cash $4.42 · equity $9,869.18 vs 09:30 $9,813.47 (+55.71; session marks +117.61) · 8 name(s) marked open→close (per-name table). BHP×13 09:30 $91.01 → close $93.63 +34.06; MRVI×166 09:30 $7.38 → close $8.26 +146.08; WYFI×57 09:30 $21.40 → close $21.16 -13.68; TOYO×276 09:30 $4.43 → close $4.51 +23.46; DVLT×4088 09:30 $0.30 → close $0.32 +81.76; SAFX×3465 09:30 $0.35 → close $0.34 -38.11; AAP×26 09:30 $46.85 → close $42.39 -115.96; AEG×136 09:30 $9.01 → close $9.01 +0.00 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.42 | ▲ 09:30 equity $9,941.57 vs yday $9,869.18 (+72.39) | 09:30 open · cash $4.42 (unchanged overnight, no fees) · equity $9,941.57 vs prior close $9,869.18 (+72.39) · 8 name(s) re-marked at the open (per-name table). BHP×13 yday $93.63 → 09:30 $95.72 +27.17; MRVI×166 yday $8.26 → 09:30 $8.20 -9.96; WYFI×57 yday $21.16 → 09:30 $21.54 +21.66; TOYO×276 yday $4.51 → 09:30 $4.68 +45.54; DVLT×4088 yday $0.32 → 09:30 $0.31 -40.88; SAFX×3465 yday $0.34 → 09:30 $0.35 +24.25; AAP×26 yday $42.39 → 09:30 $42.41 +0.52; AEG×136 yday $9.01 → 09:30 $9.04 +4.08 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $1,246.73 | ▲ +57.15 after sell → book $9,939.52; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WYFI` | 57 | $21.54 | $2.18 | $+3.64 | $2,472.33 | ▲ +3.64 after sell → book $9,937.34; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `TOYO` | 276 | $4.68 | $3.62 | $+61.82 | $3,760.39 | ▲ +61.82 after sell → book $9,933.72; vs 09:30 mark -3.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DVLT` | 4088 | $0.31 | $25.63 | $-9.27 | $5,002.04 | ▼ -9.27 after sell → book $9,908.09; vs 09:30 mark -25.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SAFX` | 3465 | $0.35 | $23.11 | $-59.63 | $6,191.69 | ▼ -59.63 after sell → book $9,884.99; vs 09:30 mark -23.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AAP` | 26 | $42.41 | $2.09 | $-119.60 | $7,292.26 | ▼ -119.60 after sell → book $9,882.90; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `AEG` | 136 | $9.04 | $2.43 | $-0.75 | $8,519.27 | ▼ -0.75 after sell → book $9,880.47; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 492 | $2.47 | $6.35 | — | $7,297.68 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1217.04 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 630 | $1.93 | $8.13 | — | $6,073.65 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1217.04 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 20 | $59.72 | $2.05 | — | $4,877.20 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1217.04 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 10 | $115.18 | $2.02 | — | $3,723.38 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1217.04 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GMAB` | 36 | $33.36 | $2.10 | — | $2,520.33 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.6; leftover $1217.04 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ENHA` | 711 | $1.71 | $9.17 | — | $1,295.34 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $1217.04 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAN` | 4139 | $0.29 | $24.59 | — | $53.89 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $1217.04 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $53.89 | ▲ close $10,178.48 vs 09:30 $9,941.57 (session +352.41) | 16:00 close · cash $53.89 · equity $10,178.48 vs 09:30 $9,941.57 (+236.91; session marks +352.41) · 8 name(s) marked open→close (per-name table). MRVI×166 09:30 $8.20 → close $8.70 +83.00; AUTL×492 09:30 $2.47 → close $2.41 -29.52; CRDL×630 09:30 $1.93 → close $1.86 -44.10; CRSP×20 09:30 $59.72 → close $59.50 -4.40; FUTU×10 09:30 $115.18 → close $123.64 +84.60; GMAB×36 09:30 $33.36 → close $33.45 +3.24; ENHA×711 09:30 $1.71 → close $1.72 +7.11; CAN×4139 09:30 $0.29 → close $0.35 +252.48 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $53.89 | ▲ 09:30 equity $10,195.03 vs yday $10,178.48 (+16.55) | 09:30 open · cash $53.89 (unchanged overnight, no fees) · equity $10,195.03 vs prior close $10,178.48 (+16.55) · 8 name(s) re-marked at the open (per-name table). MRVI×166 yday $8.70 → 09:30 $8.59 -18.26; AUTL×492 yday $2.41 → 09:30 $2.36 -24.60; CRDL×630 yday $1.86 → 09:30 $1.87 +6.30; CRSP×20 yday $59.50 → 09:30 $58.79 -14.20; FUTU×10 yday $123.64 → 09:30 $120.87 -27.70; GMAB×36 yday $33.45 → 09:30 $32.82 -22.68; ENHA×711 yday $1.72 → 09:30 $1.74 +14.22; CAN×4139 yday $0.35 → 09:30 $0.38 +103.48 | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 166 | $8.59 | $2.53 | $+195.85 | $1,477.31 | ▲ +195.85 after sell → book $10,192.51; vs 09:30 mark -2.52 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 492 | $2.36 | $6.44 | $-66.91 | $2,631.99 | ▼ -66.91 after sell → book $10,186.07; vs 09:30 mark -6.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 630 | $1.87 | $8.24 | $-54.17 | $3,801.85 | ▼ -54.17 after sell → book $10,177.83; vs 09:30 mark -8.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 10 | $120.87 | $2.04 | $+52.84 | $5,008.51 | ▲ +52.84 after sell → book $10,175.79; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `GMAB` | 36 | $32.82 | $2.12 | $-23.66 | $6,187.91 | ▼ -23.66 after sell → book $10,173.67; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ENHA` | 711 | $1.74 | $9.30 | $+2.86 | $7,415.75 | ▲ +2.86 after sell → book $10,164.37; vs 09:30 mark -9.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAN` | 4139 | $0.38 | $28.84 | $+302.52 | $8,959.72 | ▲ +302.52 after sell → book $10,135.52; vs 09:30 mark -28.85 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,959.72 | ▼ close $10,097.92 vs 09:30 $10,195.03 (session -37.60) | 16:00 close · cash $8,959.72 · equity $10,097.92 vs 09:30 $10,195.03 (-97.11; session marks -37.60) · 1 name(s) marked open→close (per-name table). CRSP×20 09:30 $58.79 → close $56.91 -37.60 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,959.72 | ▲ 09:30 equity $10,099.72 vs yday $10,097.92 (+1.80) | 09:30 open · cash $8,959.72 (unchanged overnight, no fees) · equity $10,099.72 vs prior close $10,097.92 (+1.80) · 1 name(s) re-marked at the open (per-name table). CRSP×20 yday $56.91 → 09:30 $57.00 +1.80 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 20 | $57.00 | $2.07 | $-58.52 | $10,097.65 | ▼ -58.52 after sell → book $10,097.65; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 115 | $10.92 | $2.33 | — | $8,839.52 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ret5=+10.4; leftover $1262.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 152 | $8.28 | $2.45 | — | $7,578.51 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1262.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 341 | $3.70 | $4.40 | — | $6,312.41 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.8; leftover $1262.21 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 185 | $6.79 | $2.54 | — | $5,053.72 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.4; leftover $1262.21 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3411 | $0.37 | $22.85 | — | $3,768.79 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer; ret5=-26.5; leftover $1262.21 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 143 | $8.79 | $2.42 | — | $2,509.41 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.5; leftover $1262.21 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FWDI` | 210 | $5.99 | $2.71 | — | $1,248.80 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ret5=+20.7; leftover $1262.21 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `JANX` | 67 | $18.52 | $2.19 | — | $5.77 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ret5=+7.9; leftover $1262.21 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.77 | ▲ close $10,169.81 vs 09:30 $10,099.72 (session +114.05) | 16:00 close · cash $5.77 · equity $10,169.81 vs 09:30 $10,099.72 (+70.09; session marks +114.05) · 8 name(s) marked open→close (per-name table). OCUL×115 09:30 $10.92 → close $10.92 +0.00; CRMD×152 09:30 $8.28 → close $8.28 +0.00; PUSA×341 09:30 $3.70 → close $3.91 +71.61; CAPR×185 09:30 $6.79 → close $7.19 +74.00; SAFX×3411 09:30 $0.37 → close $0.37 +0.00; SUJA×143 09:30 $8.79 → close $8.54 -35.75; FWDI×210 09:30 $5.99 → close $5.86 -27.30; JANX×67 09:30 $18.52 → close $18.99 +31.49 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.77 | ▲ 09:30 equity $10,169.81 vs yday $10,169.81 (-0.00) | 09:30 open · cash $5.77 (unchanged overnight, no fees) · equity $10,169.81 vs prior close $10,169.81 (-0.00) · 8 name(s) re-marked at the open (per-name table). OCUL×115 yday $10.92 → 09:30 $10.92 +0.00; CRMD×152 yday $8.28 → 09:30 $8.28 +0.00; PUSA×341 yday $3.91 → 09:30 $3.91 +0.00; CAPR×185 yday $7.19 → 09:30 $7.19 +0.00; SAFX×3411 yday $0.37 → 09:30 $0.37 +0.00; SUJA×143 yday $8.54 → 09:30 $8.54 +0.00; FWDI×210 yday $5.86 → 09:30 $5.86 +0.00; JANX×67 yday $18.99 → 09:30 $18.99 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.77 | ▲ close $10,169.81 vs 09:30 $10,169.81 (session +0.00) | 16:00 close · cash $5.77 · equity $10,169.81 vs 09:30 $10,169.81 (-0.00; session marks +0.00) · 8 name(s) marked open→close (per-name table). OCUL×115 09:30 $10.92 → close $10.92 +0.00; CRMD×152 09:30 $8.28 → close $8.28 +0.00; PUSA×341 09:30 $3.91 → close $3.91 +0.00; CAPR×185 09:30 $7.19 → close $7.19 +0.00; SAFX×3411 09:30 $0.37 → close $0.37 +0.00; SUJA×143 09:30 $8.54 → close $8.54 +0.00; FWDI×210 09:30 $5.86 → close $5.86 +0.00; JANX×67 09:30 $18.99 → close $18.99 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.77 | ▲ 09:30 equity $10,432.76 vs yday $10,169.81 (+262.95) | 09:30 open · cash $5.77 (unchanged overnight, no fees) · equity $10,432.76 vs prior close $10,169.81 (+262.95) · 8 name(s) re-marked at the open (per-name table). OCUL×115 yday $10.92 → 09:30 $10.79 -14.95; CRMD×152 yday $8.28 → 09:30 $8.60 +48.64; PUSA×341 yday $3.91 → 09:30 $3.84 -23.87; CAPR×185 yday $7.19 → 09:30 $8.29 +203.50; SAFX×3411 yday $0.37 → 09:30 $0.35 -68.22; SUJA×143 yday $8.54 → 09:30 $9.39 +121.55; FWDI×210 yday $5.86 → 09:30 $5.97 +23.10; JANX×67 yday $18.99 → 09:30 $18.59 -26.80 | — |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 115 | $10.79 | $2.36 | $-19.65 | $1,244.25 | ▼ -19.65 after sell → book $10,430.39; vs 09:30 mark -2.37 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 152 | $8.60 | $2.48 | $+43.71 | $2,548.97 | ▲ +43.71 after sell → book $10,427.91; vs 09:30 mark -2.48 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PUSA` | 341 | $3.84 | $4.47 | $+38.88 | $3,853.94 | ▲ +38.88 after sell → book $10,423.44; vs 09:30 mark -4.47 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 185 | $8.29 | $2.59 | $+272.37 | $5,385.01 | ▲ +272.37 after sell → book $10,420.86; vs 09:30 mark -2.58 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SAFX` | 3411 | $0.35 | $22.75 | $-113.82 | $6,556.11 | ▼ -113.82 after sell → book $10,398.11; vs 09:30 mark -22.75 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SUJA` | 143 | $9.39 | $2.45 | $+80.93 | $7,896.42 | ▲ +80.93 after sell → book $10,395.65; vs 09:30 mark -2.46 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FWDI` | 210 | $5.97 | $2.75 | $-9.66 | $9,147.37 | ▼ -9.66 after sell → book $10,392.90; vs 09:30 mark -2.75 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `JANX` | 67 | $18.59 | $2.21 | $+0.29 | $10,390.69 | ▲ +0.29 after sell → book $10,390.69; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 16 | $80.97 | $2.04 | — | $9,093.13 | — | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; 🔵; ret5=-1.3; leftover $1298.84 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 293 | $4.42 | $3.78 | — | $7,794.29 | — | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; 🔵; ret5=-8.6; leftover $1298.84 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 17 | $75.12 | $2.04 | — | $6,515.21 | — | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; 🔵; ret5=-2.2; leftover $1298.84 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $925.74 | $1.99 | — | $5,587.48 | — | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; 🔵; ret5=-0.5; leftover $1298.84 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `TX` | 23 | $55.20 | $2.06 | — | $4,315.82 | — | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; 🔵; ret5=+3.0; leftover $1298.84 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 4 | $314.61 | $2.00 | — | $3,055.38 | — | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; 🔵; ret5=-5.5; leftover $1298.84 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 5 | $240.00 | $2.00 | — | $1,853.37 | — | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; 🔵; ret5=+6.8; leftover $1298.84 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NUE` | 5 | $248.91 | $2.00 | — | $606.82 | — | union ∩ last_red, no 🚨; gate last_red=True; list mover_buy; 🔵; ret5=-9.4; leftover $1298.84 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $606.82 | ▲ close $10,393.83 vs 09:30 $10,432.76 (session +21.06) | 16:00 close · cash $606.82 · equity $10,393.83 vs 09:30 $10,432.76 (-38.93; session marks +21.06) · 8 name(s) marked open→close (per-name table). ACMR×16 09:30 $80.97 → close $79.11 -29.76; GGB×293 09:30 $4.42 → close $4.46 +11.72; MT×17 09:30 $75.12 → close $74.53 -10.03; MU×1 09:30 $925.74 → close $938.40 +12.66; TX×23 09:30 $55.20 → close $55.13 -1.61; LRCX×4 09:30 $314.61 → close $312.88 -6.92; MRVL×5 09:30 $240.00 → close $245.11 +25.55; NUE×5 09:30 $248.91 → close $252.80 +19.45 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $606.82 | ▲ 09:30 equity $10,559.89 vs yday $10,393.83 (+166.06) | 09:30 open · cash $606.82 (unchanged overnight, no fees) · equity $10,559.89 vs prior close $10,393.83 (+166.06) · 8 name(s) re-marked at the open (per-name table). ACMR×16 yday $79.11 → 09:30 $81.65 +40.64; GGB×293 yday $4.46 → 09:30 $4.57 +32.23; MT×17 yday $74.53 → 09:30 $74.54 +0.17; MU×1 yday $938.40 → 09:30 $967.01 +28.61; TX×23 yday $55.13 → 09:30 $55.25 +2.76; LRCX×4 yday $312.88 → 09:30 $318.88 +24.00; MRVL×5 yday $245.11 → 09:30 $253.44 +41.65; NUE×5 yday $252.80 → 09:30 $252.00 -4.00 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 16 | $81.65 | $2.06 | $+6.78 | $1,911.16 | ▲ +6.78 after sell → book $10,557.83; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 293 | $4.57 | $3.84 | $+36.33 | $3,246.33 | ▲ +36.33 after sell → book $10,553.99; vs 09:30 mark -3.84 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 17 | $74.54 | $2.06 | $-13.96 | $4,511.45 | ▼ -13.96 after sell → book $10,551.93; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $967.01 | $2.01 | $+37.26 | $5,476.44 | ▲ +37.26 after sell → book $10,549.91; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TX` | 23 | $55.25 | $2.08 | $-2.99 | $6,745.11 | ▼ -2.99 after sell → book $10,547.83; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 4 | $318.88 | $2.02 | $+13.06 | $8,018.61 | ▲ +13.06 after sell → book $10,545.81; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MRVL` | 5 | $253.44 | $2.03 | $+63.17 | $9,283.79 | ▲ +63.17 after sell → book $10,543.79; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `NUE` | 5 | $252.00 | $2.03 | $+11.42 | $10,541.76 | ▲ +11.42 after sell → book $10,541.76; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 143 | $9.19 | $2.42 | — | $9,225.17 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1317.72 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 39 | $33.78 | $2.11 | — | $7,905.65 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1317.72 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $149.40 | $2.01 | — | $6,708.43 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1317.72 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 153 | $8.57 | $2.45 | — | $5,394.77 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; ret5=-3.4; leftover $1317.72 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 10 | $127.07 | $2.02 | — | $4,122.05 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=-21.0; leftover $1317.72 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 70 | $18.68 | $2.20 | — | $2,812.25 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; ret5=+0.2; leftover $1317.72 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BTSG` | 21 | $61.42 | $2.05 | — | $1,520.38 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; ret5=-4.6; leftover $1317.72 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CRDL` | 630 | $2.09 | $8.13 | — | $195.55 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; ret5=+3.3; leftover $1317.72 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $195.55 | ▲ close $10,545.65 vs 09:30 $10,559.89 (session +27.28) | 16:00 close · cash $195.55 · equity $10,545.65 vs 09:30 $10,559.89 (-14.24; session marks +27.28) · 8 name(s) marked open→close (per-name table). CAPR×143 09:30 $9.19 → close $10.06 +124.41; SEDG×39 09:30 $33.78 → close $33.51 -10.53; SMTC×8 09:30 $149.40 → close $142.43 -55.76; OPTX×153 09:30 $8.57 → close $8.73 +24.48; TTMI×10 09:30 $127.07 → close $124.73 -23.40; BBWI×70 09:30 $18.68 → close $18.65 -2.10; BTSG×21 09:30 $61.42 → close $60.90 -10.92; CRDL×630 09:30 $2.09 → close $2.06 -18.90 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $195.55 | ▼ 09:30 equity $10,152.51 vs yday $10,545.65 (-393.14) | 09:30 open · cash $195.55 (unchanged overnight, no fees) · equity $10,152.51 vs prior close $10,545.65 (-393.14) · 8 name(s) re-marked at the open (per-name table). CAPR×143 yday $10.06 → 09:30 $9.44 -88.66; SEDG×39 yday $33.51 → 09:30 $31.50 -78.39; SMTC×8 yday $142.43 → 09:30 $133.04 -75.12; OPTX×153 yday $8.73 → 09:30 $8.52 -32.13; TTMI×10 yday $124.73 → 09:30 $117.20 -75.30; BBWI×70 yday $18.65 → 09:30 $19.30 +45.50; BTSG×21 yday $60.90 → 09:30 $59.66 -26.04; CRDL×630 yday $2.06 → 09:30 $1.96 -63.00 | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 143 | $9.44 | $2.45 | $+30.88 | $1,543.02 | ▲ +30.88 after sell → book $10,150.06; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 39 | $31.50 | $2.13 | $-93.15 | $2,769.39 | ▼ -93.15 after sell → book $10,147.93; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 8 | $133.04 | $2.03 | $-134.93 | $3,831.68 | ▼ -134.93 after sell → book $10,145.90; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OPTX` | 153 | $8.52 | $2.48 | $-12.58 | $5,132.75 | ▼ -12.58 after sell → book $10,143.41; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TTMI` | 10 | $117.20 | $2.04 | $-102.76 | $6,302.71 | ▼ -102.76 after sell → book $10,141.37; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 70 | $19.30 | $2.22 | $+38.98 | $7,651.49 | ▲ +38.98 after sell → book $10,139.15; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BTSG` | 21 | $59.66 | $2.07 | $-41.09 | $8,902.28 | ▼ -41.09 after sell → book $10,137.08; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRDL` | 630 | $1.96 | $8.24 | $-98.27 | $10,128.84 | ▼ -98.27 after sell → book $10,128.84; vs 09:30 mark -8.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,128.84 | ▲ close $10,128.84 vs 09:30 $10,152.51 (session +0.00) | 16:00 close · cash $10,128.84 · no lots left · equity $10,128.84. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,128.84 | ▲ 09:30 equity $10,128.84 vs yday $10,128.84 (-0.00) | 09:30 open · cash $10,128.84 · no holdings · equity $10,128.84 vs prior close $10,128.84 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,128.84 | ▲ close $10,128.84 vs 09:30 $10,128.84 (session +0.00) | 16:00 close · cash $10,128.84 · no lots left · equity $10,128.84. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,128.84 | ▲ 09:30 equity $10,128.84 vs yday $10,128.84 (-0.00) | 09:30 open · cash $10,128.84 · no holdings · equity $10,128.84 vs prior close $10,128.84 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,128.84 | ▲ close $10,128.84 vs 09:30 $10,128.84 (session +0.00) | 16:00 close · cash $10,128.84 · no lots left · equity $10,128.84. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,128.84 | ▲ 09:30 equity $10,128.84 vs yday $10,128.84 (-0.00) | 09:30 open · cash $10,128.84 · no holdings · equity $10,128.84 vs prior close $10,128.84 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 387 | $3.27 | $4.99 | — | $8,858.35 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1266.10 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 68 | $18.40 | $2.19 | — | $7,604.96 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1266.10 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CTMX` | 340 | $3.72 | $4.39 | — | $6,335.77 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $1266.10 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 22 | $56.78 | $2.06 | — | $5,084.56 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer; ret5=+0.3; leftover $1266.10 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 586 | $2.16 | $7.56 | — | $3,811.24 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1266.10 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SION` | 190 | $6.63 | $2.56 | — | $2,548.98 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ret5=-18.1; leftover $1266.10 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DUOL` | 8 | $156.24 | $2.01 | — | $1,297.05 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ret5=+10.0; leftover $1266.10 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SAFX` | 3246 | $0.39 | $22.40 | — | $8.71 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; ret5=-26.5; leftover $1266.10 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.71 | ▲ close $10,248.72 vs 09:30 $10,128.84 (session +168.04) | 16:00 close · cash $8.71 · equity $10,248.72 vs 09:30 $10,128.84 (+119.88; session marks +168.04) · 8 name(s) marked open→close (per-name table). CABA×387 09:30 $3.27 → close $3.57 +116.10; FRVO×68 09:30 $18.40 → close $17.98 -28.56; CTMX×340 09:30 $3.72 → close $3.72 +0.00; EIX×22 09:30 $56.78 → close $55.19 -34.98; CRDL×586 09:30 $2.16 → close $2.17 +5.86; SION×190 09:30 $6.63 → close $7.31 +129.20; DUOL×8 09:30 $156.24 → close $157.85 +12.88; SAFX×3246 09:30 $0.39 → close $0.38 -32.46 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.71 | ▲ 09:30 equity $10,335.50 vs yday $10,248.72 (+86.78) | 09:30 open · cash $8.71 (unchanged overnight, no fees) · equity $10,335.50 vs prior close $10,248.72 (+86.78) · 8 name(s) re-marked at the open (per-name table). CABA×387 yday $3.57 → 09:30 $3.63 +23.22; FRVO×68 yday $17.98 → 09:30 $18.27 +19.72; CTMX×340 yday $3.72 → 09:30 $3.73 +3.40; EIX×22 yday $55.19 → 09:30 $55.42 +5.06; CRDL×586 yday $2.17 → 09:30 $2.18 +5.86; SION×190 yday $7.31 → 09:30 $7.31 +0.00; DUOL×8 yday $157.85 → 09:30 $161.54 +29.52; SAFX×3246 yday $0.38 → 09:30 $0.38 +0.00 | — |
| 2026-09-04 09:30 ET | **SELL** | `FRVO` | 68 | $18.27 | $2.22 | $-13.25 | $1,248.85 | ▼ -13.25 after sell → book $10,333.28; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CTMX` | 340 | $3.73 | $4.45 | $-5.44 | $2,512.60 | ▼ -5.44 after sell → book $10,328.83; vs 09:30 mark -4.45 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `EIX` | 22 | $55.42 | $2.08 | $-34.05 | $3,729.76 | ▼ -34.05 after sell → book $10,326.75; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 586 | $2.18 | $7.67 | $-3.51 | $4,999.58 | ▼ -3.51 after sell → book $10,319.09; vs 09:30 mark -7.66 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DUOL` | 8 | $161.54 | $2.03 | $+38.35 | $6,289.86 | ▲ +38.35 after sell → book $10,317.05; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SAFX` | 3246 | $0.38 | $22.62 | $-77.48 | $7,500.72 | ▼ -77.48 after sell → book $10,294.43; vs 09:30 mark -22.62 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASND` | 4 | $266.94 | $2.00 | — | $6,430.96 | — | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ret5=+1.9; leftover $1250.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 407 | $3.07 | $5.25 | — | $5,176.22 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $1250.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 42 | $29.15 | $2.12 | — | $3,949.80 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1250.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CCOI` | 122 | $10.22 | $2.36 | — | $2,700.61 | — | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $1250.12 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 268 | $4.66 | $3.46 | — | $1,448.27 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $1250.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `JLHL` | 201 | $6.20 | $2.60 | — | $199.47 | — | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; ret5=-8.2; leftover $1250.12 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $199.47 | ▼ close $10,075.14 vs 09:30 $10,335.50 (session -201.51) | 16:00 close · cash $199.47 · equity $10,075.14 vs 09:30 $10,335.50 (-260.36; session marks -201.51) · 8 name(s) marked open→close (per-name table). CABA×387 09:30 $3.63 → close $3.48 -58.05; SION×190 09:30 $7.31 → close $6.75 -106.40; ASND×4 09:30 $266.94 → close $271.12 +16.72; SLBT×407 09:30 $3.07 → close $3.15 +32.56; MLYS×42 09:30 $29.15 → close $28.27 -36.96; CCOI×122 09:30 $10.22 → close $9.98 -29.28; IRD×268 09:30 $4.66 → close $4.60 -16.08; JLHL×201 09:30 $6.20 → close $6.18 -4.02 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `TBPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INDP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ENVX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `STUB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `FN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `SBSW` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PAAS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `PUSA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `SAFX` | no_price | no 09:30 open — carry |
| 2026-08-26 | `SUJA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `FWDI` | no_price | no 09:30 open — carry |
| 2026-08-26 | `JANX` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-26 | `AVEX` | no_price | no 09:30 open |
| 2026-08-26 | `BE` | no_price | no 09:30 open |
| 2026-08-26 | `INDP` | no_price | no 09:30 open |
| 2026-08-26 | `AXTI` | no_price | no 09:30 open |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SNPS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OKE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `IRD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BEP` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `STIM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VLRS` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CABA` | 387 | 2026-09-03 @ $3.27 | union ∩ last_red, no 🚨; gate last_red=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1266.10 |
| `SION` | 190 | 2026-09-03 @ $6.63 | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer; 🔵; ret5=-18.1; leftover $1266.10 |
| `ASND` | 4 | 2026-09-04 @ $266.94 | union ∩ last_red, no 🚨; gate last_red=True; list flatten; ret5=+1.9; leftover $1250.12 |
| `SLBT` | 407 | 2026-09-04 @ $3.07 | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $1250.12 |
| `MLYS` | 42 | 2026-09-04 @ $29.15 | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1250.12 |
| `CCOI` | 122 | 2026-09-04 @ $10.22 | union ∩ last_red, no 🚨; gate last_red=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $1250.12 |
| `IRD` | 268 | 2026-09-04 @ $4.66 | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $1250.12 |
| `JLHL` | 201 | 2026-09-04 @ $6.20 | union ∩ last_red, no 🚨; gate last_red=True; list yday_gainer,yday_mover; ret5=-8.2; leftover $1250.12 |
