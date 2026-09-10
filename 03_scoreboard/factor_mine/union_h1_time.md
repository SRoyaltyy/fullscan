# Factor mine action — `union_h1_time`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `time` · S-boost `none` · sell at min-hold even if still listed

Cash book **+4.40%** ($10,440) · signal-only (no cash/fees) was -0.56%. Starts YES **6/19**. Fills 176 · skips 64 · realized $+440.17.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the timer rings. They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- Keep the first 8 names in list order.
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- This is a LONG sleeve: it buys shares and wants the price to go up.

### When it sells

- Sell first, then buy. Never sell a ticker we do not hold.
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- Time-stop: once 1 session(s) are up, sell at 09:30 even if the name is still on the list.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `time` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,440.21.

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
| 2026-08-14 | `BTSG` | 20 | $60.23 | $59.65 | -11.60 | — | +0.00 | -11.60 | -3.00 | — |
| 2026-08-14 | `IREN` | 27 | $44.76 | $44.09 | -18.09 | — | +0.00 | -18.09 | -51.03 | — |
| 2026-08-14 | `TPG` | 24 | $54.62 | $55.29 | +16.08 | — | +0.00 | +16.08 | +112.00 | — |
| 2026-08-14 | `TGTX` | 25 | $47.94 | $47.27 | -16.75 | — | +0.00 | -16.75 | -60.75 | — |
| 2026-08-14 | `SLS` | 106 | $12.36 | $12.40 | +4.24 | — | +0.00 | +4.24 | +74.20 | — |
| 2026-08-14 | `HIMS` | 42 | $28.77 | $29.15 | +15.96 | — | +0.00 | +15.96 | -24.78 | — |
| 2026-08-14 | `INO` | 1543 | $0.90 | $0.93 | +46.29 | — | +0.00 | +46.29 | +185.16 | — |
| 2026-08-14 | `TNDM` | 53 | $23.13 | $22.92 | -11.13 | — | +0.00 | -11.13 | -21.73 | — |
| 2026-08-14 | `TLN` | 3 | — | $359.83 | +0.00 | $362.74 | +8.73 | +8.73 | +0.00 | +8.73 |
| 2026-08-14 | `VST` | 8 | — | $146.90 | +0.00 | $148.13 | +9.84 | +9.84 | +0.00 | +9.84 |
| 2026-08-14 | `NRG` | 10 | — | $120.00 | +0.00 | $126.24 | +62.40 | +62.40 | +0.00 | +62.40 |
| 2026-08-14 | `DAVE` | 3 | — | $330.91 | +0.00 | $334.57 | +10.98 | +10.98 | +0.00 | +10.98 |
| 2026-08-14 | `SLG` | 22 | — | $57.61 | +0.00 | $56.09 | -33.44 | -33.44 | +0.00 | -33.44 |
| 2026-08-14 | `MARA` | 140 | — | $9.01 | +0.00 | $9.20 | +26.60 | +26.60 | +0.00 | +26.60 |
| 2026-08-14 | `LDI` | 1353 | — | $0.94 | +0.00 | $0.90 | -54.12 | -54.12 | +0.00 | -54.12 |
| 2026-08-14 | `BTBT` | 845 | — | $1.50 | +0.00 | $1.57 | +59.15 | +59.15 | +0.00 | +59.15 |
| 2026-08-17 | `TLN` | 3 | $362.74 | $367.88 | +15.42 | — | +0.00 | +15.42 | +24.15 | — |
| 2026-08-17 | `VST` | 8 | $148.13 | $149.37 | +9.92 | — | +0.00 | +9.92 | +19.76 | — |
| 2026-08-17 | `NRG` | 10 | $126.24 | $127.40 | +11.60 | — | +0.00 | +11.60 | +74.00 | — |
| 2026-08-17 | `DAVE` | 3 | $334.57 | $336.94 | +7.11 | — | +0.00 | +7.11 | +18.09 | — |
| 2026-08-17 | `SLG` | 22 | $56.09 | $55.37 | -15.84 | — | +0.00 | -15.84 | -49.28 | — |
| 2026-08-17 | `MARA` | 140 | $9.20 | $9.22 | +2.80 | — | +0.00 | +2.80 | +29.40 | — |
| 2026-08-17 | `LDI` | 1353 | $0.90 | $0.91 | +13.53 | — | +0.00 | +13.53 | -40.59 | — |
| 2026-08-17 | `BTBT` | 845 | $1.57 | $1.52 | -42.25 | — | +0.00 | -42.25 | +16.90 | — |
| 2026-08-17 | `DVN` | 27 | — | $46.18 | +0.00 | $47.57 | +37.53 | +37.53 | +0.00 | +37.53 |
| 2026-08-17 | `EOG` | 8 | — | $142.77 | +0.00 | $146.15 | +27.04 | +27.04 | +0.00 | +27.04 |
| 2026-08-17 | `FANG` | 6 | — | $202.70 | +0.00 | $206.29 | +21.54 | +21.54 | +0.00 | +21.54 |
| 2026-08-17 | `TMC` | 313 | — | $4.05 | +0.00 | $3.77 | -87.64 | -87.64 | +0.00 | -87.64 |
| 2026-08-17 | `TGB` | 150 | — | $8.46 | +0.00 | $8.77 | +46.50 | +46.50 | +0.00 | +46.50 |
| 2026-08-17 | `ELF` | 14 | — | $90.54 | +0.00 | $93.66 | +43.68 | +43.68 | +0.00 | +43.68 |
| 2026-08-17 | `DNN` | 391 | — | $3.24 | +0.00 | $3.19 | -19.55 | -19.55 | +0.00 | -19.55 |
| 2026-08-17 | `HNST` | 263 | — | $4.81 | +0.00 | $4.70 | -28.93 | -28.93 | +0.00 | -28.93 |
| 2026-08-18 | `DVN` | 27 | $47.57 | $48.00 | +11.61 | — | +0.00 | +11.61 | +49.14 | — |
| 2026-08-18 | `EOG` | 8 | $146.15 | $148.04 | +15.12 | — | +0.00 | +15.12 | +42.16 | — |
| 2026-08-18 | `FANG` | 6 | $206.29 | $208.93 | +15.84 | — | +0.00 | +15.84 | +37.38 | — |
| 2026-08-18 | `TMC` | 313 | $3.77 | $3.72 | -15.65 | — | +0.00 | -15.65 | -103.29 | — |
| 2026-08-18 | `TGB` | 150 | $8.77 | $8.55 | -33.00 | — | +0.00 | -33.00 | +13.50 | — |
| 2026-08-18 | `ELF` | 14 | $93.66 | $93.44 | -3.08 | — | +0.00 | -3.08 | +40.60 | — |
| 2026-08-18 | `DNN` | 391 | $3.19 | $3.11 | -31.28 | — | +0.00 | -31.28 | -50.83 | — |
| 2026-08-18 | `HNST` | 263 | $4.70 | $4.67 | -7.89 | — | +0.00 | -7.89 | -36.82 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 61 | — | $20.55 | +0.00 | $21.19 | +39.04 | +39.04 | +0.00 | +39.04 |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `CDE` | 61 | — | $20.65 | +0.00 | $21.11 | +28.06 | +28.06 | +0.00 | +28.06 |
| 2026-08-20 | `HDSN` | 218 | — | $5.77 | +0.00 | $5.57 | -43.60 | -43.60 | +0.00 | -43.60 |
| 2026-08-20 | `IAG` | 64 | — | $19.63 | +0.00 | $20.50 | +55.68 | +55.68 | +0.00 | +55.68 |
| 2026-08-20 | `KGC` | 42 | — | $29.63 | +0.00 | $31.43 | +75.60 | +75.60 | +0.00 | +75.60 |
| 2026-08-20 | `NFGC` | 721 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-21 | `AG` | 61 | $21.19 | $21.90 | +43.31 | — | +0.00 | +43.31 | +82.35 | — |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `CDE` | 61 | $21.11 | $21.75 | +39.04 | — | +0.00 | +39.04 | +67.10 | — |
| 2026-08-21 | `HDSN` | 218 | $5.57 | $5.67 | +21.80 | — | +0.00 | +21.80 | -21.80 | — |
| 2026-08-21 | `IAG` | 64 | $20.50 | $21.17 | +42.88 | — | +0.00 | +42.88 | +98.56 | — |
| 2026-08-21 | `KGC` | 42 | $31.43 | $32.17 | +31.08 | — | +0.00 | +31.08 | +106.68 | — |
| 2026-08-21 | `NFGC` | 721 | $1.75 | $1.79 | +28.84 | — | +0.00 | +28.84 | +28.84 | — |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | — | +0.00 | +35.60 | +81.28 | — |
| 2026-08-21 | `AU` | 11 | — | $119.43 | +0.00 | $121.22 | +19.69 | +19.69 | +0.00 | +19.69 |
| 2026-08-21 | `AUPH` | 76 | — | $17.20 | +0.00 | $16.65 | -41.80 | -41.80 | +0.00 | -41.80 |
| 2026-08-21 | `AEM` | 6 | — | $216.30 | +0.00 | $216.06 | -1.44 | -1.44 | +0.00 | -1.44 |
| 2026-08-21 | `ARCT` | 118 | — | $11.13 | +0.00 | $13.45 | +273.76 | +273.76 | +0.00 | +273.76 |
| 2026-08-21 | `AUTL` | 534 | — | $2.47 | +0.00 | $2.41 | -32.04 | -32.04 | +0.00 | -32.04 |
| 2026-08-21 | `CRDL` | 683 | — | $1.93 | +0.00 | $1.86 | -47.81 | -47.81 | +0.00 | -47.81 |
| 2026-08-21 | `CRSP` | 22 | — | $59.72 | +0.00 | $59.50 | -4.84 | -4.84 | +0.00 | -4.84 |
| 2026-08-21 | `CYPH` | 999 | — | $1.32 | +0.00 | $1.42 | +99.90 | +99.90 | +0.00 | +99.90 |
| 2026-08-24 | `AU` | 11 | $121.22 | $120.51 | -7.81 | — | +0.00 | -7.81 | +11.88 | — |
| 2026-08-24 | `AUPH` | 76 | $16.65 | $16.57 | -6.08 | — | +0.00 | -6.08 | -47.88 | — |
| 2026-08-24 | `AEM` | 6 | $216.06 | $217.03 | +5.82 | — | +0.00 | +5.82 | +4.38 | — |
| 2026-08-24 | `ARCT` | 118 | $13.45 | $13.33 | -14.16 | — | +0.00 | -14.16 | +259.60 | — |
| 2026-08-24 | `AUTL` | 534 | $2.41 | $2.40 | -5.34 | — | +0.00 | -5.34 | -37.38 | — |
| 2026-08-24 | `CRDL` | 683 | $1.86 | $1.88 | +13.66 | — | +0.00 | +13.66 | -34.15 | — |
| 2026-08-24 | `CRSP` | 22 | $59.50 | $58.75 | -16.50 | — | +0.00 | -16.50 | -21.34 | — |
| 2026-08-24 | `CYPH` | 999 | $1.42 | $1.83 | +409.59 | — | +0.00 | +409.59 | +509.49 | — |
| 2026-08-25 | `MOS` | 58 | — | $23.77 | +0.00 | $24.27 | +29.00 | +29.00 | +0.00 | +29.00 |
| 2026-08-25 | `OCUL` | 126 | — | $10.98 | +0.00 | $10.88 | -12.60 | -12.60 | +0.00 | -12.60 |
| 2026-08-25 | `INSP` | 22 | — | $61.19 | +0.00 | $61.07 | -2.64 | -2.64 | +0.00 | -2.64 |
| 2026-08-25 | `CRMD` | 166 | — | $8.35 | +0.00 | $8.56 | +34.86 | +34.86 | +0.00 | +34.86 |
| 2026-08-25 | `RZLT` | 281 | — | $4.94 | +0.00 | $5.01 | +19.67 | +19.67 | +0.00 | +19.67 |
| 2026-08-25 | `HCA` | 3 | — | $426.97 | +0.00 | $428.76 | +5.37 | +5.37 | +0.00 | +5.37 |
| 2026-08-25 | `CAPR` | 191 | — | $7.25 | +0.00 | $8.29 | +198.64 | +198.64 | +0.00 | +198.64 |
| 2026-08-25 | `SAFX` | 3883 | — | $0.36 | +0.00 | $0.35 | -15.53 | -15.53 | +0.00 | -15.53 |
| 2026-08-26 | `MOS` | 56 | $24.27 | $24.84 | +33.06 | $24.16 | -38.08 | -5.02 | +62.06 | -38.08 |
| 2026-08-26 | `OCUL` | 130 | $10.88 | $10.79 | -11.34 | $10.77 | -2.60 | -13.94 | -23.94 | -2.60 |
| 2026-08-26 | `INSP` | 23 | $61.07 | $60.07 | -22.00 | $61.80 | +39.79 | +17.79 | -24.64 | +39.79 |
| 2026-08-26 | `CRMD` | 164 | $8.56 | $8.60 | +6.64 | $8.39 | -34.44 | -27.80 | +41.50 | -34.44 |
| 2026-08-26 | `RZLT` | 281 | $5.01 | $5.01 | +0.00 | $5.04 | +8.43 | +8.43 | +19.67 | +8.43 |
| 2026-08-26 | `HCA` | 3 | $428.76 | $427.50 | -3.78 | $427.16 | -1.02 | -4.80 | +1.59 | -1.02 |
| 2026-08-26 | `CAPR` | 191 | $8.29 | $8.29 | +0.00 | — | +0.00 | +0.00 | +198.64 | — |
| 2026-08-26 | `SAFX` | 3883 | $0.35 | $0.35 | -3.88 | — | +0.00 | -3.88 | -19.42 | — |
| 2026-08-26 | `AVBP` | 45 | — | $31.21 | +0.00 | $31.14 | -3.15 | -3.15 | +0.00 | -3.15 |
| 2026-08-26 | `FLNC` | 126 | — | $11.12 | +0.00 | $11.08 | -5.04 | -5.04 | +0.00 | -5.04 |
| 2026-08-27 | `HCA` | 3 | $427.16 | $424.61 | -7.65 | — | +0.00 | -7.65 | -8.67 | — |
| 2026-08-27 | `MOS` | 58 | $24.16 | $24.00 | -8.96 | $23.76 | -13.92 | -22.88 | -47.04 | -13.92 |
| 2026-08-27 | `OCUL` | 130 | $10.77 | $10.63 | -18.20 | — | +0.00 | -18.20 | -20.80 | — |
| 2026-08-27 | `INSP` | 23 | $61.80 | $62.10 | +6.90 | — | +0.00 | +6.90 | +46.69 | — |
| 2026-08-27 | `CRMD` | 164 | $8.39 | $8.49 | +16.40 | — | +0.00 | +16.40 | -18.04 | — |
| 2026-08-27 | `RZLT` | 281 | $5.04 | $5.07 | +8.43 | — | +0.00 | +8.43 | +16.86 | — |
| 2026-08-27 | `AVBP` | 45 | $31.14 | $30.79 | -15.75 | — | +0.00 | -15.75 | -18.90 | — |
| 2026-08-27 | `FLNC` | 126 | $11.08 | $11.52 | +55.44 | — | +0.00 | +55.44 | +50.40 | — |
| 2026-08-27 | `RRC` | 33 | — | $41.44 | +0.00 | $41.64 | +6.60 | +6.60 | +0.00 | +6.60 |
| 2026-08-27 | `CRK` | 97 | — | $14.42 | +0.00 | $14.62 | +19.40 | +19.40 | +0.00 | +19.40 |
| 2026-08-27 | `SLI` | 540 | — | $2.60 | +0.00 | $2.64 | +21.60 | +21.60 | +0.00 | +21.60 |
| 2026-08-27 | `ACMR` | 17 | — | $81.65 | +0.00 | $80.49 | -19.72 | -19.72 | +0.00 | -19.72 |
| 2026-08-27 | `GGB` | 307 | — | $4.57 | +0.00 | $4.70 | +39.91 | +39.91 | +0.00 | +39.91 |
| 2026-08-27 | `MT` | 18 | — | $74.54 | +0.00 | $74.63 | +1.62 | +1.62 | +0.00 | +1.62 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-28 | `RRC` | 33 | $41.64 | $41.74 | +3.30 | $41.46 | -9.24 | -5.94 | +9.90 | -9.24 |
| 2026-08-28 | `CRK` | 95 | $14.62 | $14.63 | +0.97 | $14.29 | -32.30 | -31.33 | +20.37 | -32.30 |
| 2026-08-28 | `MOS` | 58 | $23.76 | $23.95 | +11.02 | $23.60 | -20.30 | -9.28 | -2.90 | -20.30 |
| 2026-08-28 | `SLI` | 523 | $2.64 | $2.68 | +21.60 | $2.55 | -67.99 | -46.39 | +43.20 | -67.99 |
| 2026-08-28 | `ACMR` | 17 | $80.49 | $79.27 | -20.74 | — | +0.00 | -20.74 | -40.46 | — |
| 2026-08-28 | `GGB` | 307 | $4.70 | $4.67 | -9.21 | — | +0.00 | -9.21 | +30.70 | — |
| 2026-08-28 | `MT` | 18 | $74.63 | $75.39 | +13.68 | — | +0.00 | +13.68 | +15.30 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `SEDG` | 42 | — | $32.90 | +0.00 | $31.41 | -62.58 | -62.58 | +0.00 | -62.58 |
| 2026-08-28 | `GRRR` | 89 | — | $15.66 | +0.00 | $14.41 | -111.25 | -111.25 | +0.00 | -111.25 |
| 2026-08-28 | `URBN` | 17 | — | $79.42 | +0.00 | $81.09 | +28.39 | +28.39 | +0.00 | +28.39 |
| 2026-08-28 | `PYXS` | 422 | — | $3.32 | +0.00 | $3.23 | -37.98 | -37.98 | +0.00 | -37.98 |
| 2026-08-31 | `RRC` | 33 | $41.46 | $42.00 | +17.82 | — | +0.00 | +17.82 | +8.58 | — |
| 2026-08-31 | `CRK` | 95 | $14.29 | $14.54 | +23.75 | — | +0.00 | +23.75 | -8.55 | — |
| 2026-08-31 | `MOS` | 58 | $23.60 | $23.68 | +4.64 | — | +0.00 | +4.64 | -15.66 | — |
| 2026-08-31 | `SLI` | 523 | $2.55 | $2.58 | +15.69 | — | +0.00 | +15.69 | -52.30 | — |
| 2026-08-31 | `SEDG` | 42 | $31.41 | $31.15 | -10.92 | — | +0.00 | -10.92 | -73.50 | — |
| 2026-08-31 | `GRRR` | 89 | $14.41 | $14.44 | +2.67 | — | +0.00 | +2.67 | -108.58 | — |
| 2026-08-31 | `URBN` | 17 | $81.09 | $80.44 | -11.05 | — | +0.00 | -11.05 | +17.34 | — |
| 2026-08-31 | `PYXS` | 422 | $3.23 | $3.20 | -12.66 | — | +0.00 | -12.66 | -50.64 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 25 | — | $52.88 | +0.00 | $52.46 | -10.50 | -10.50 | +0.00 | -10.50 |
| 2026-09-03 | `HRMY` | 31 | — | $42.93 | +0.00 | $41.86 | -33.17 | -33.17 | +0.00 | -33.17 |
| 2026-09-03 | `CABA` | 375 | — | $3.63 | +0.00 | $3.48 | -56.25 | -56.25 | +0.00 | -56.25 |
| 2026-09-03 | `VSTM` | 169 | — | $8.03 | +0.00 | $7.98 | -8.45 | -8.45 | +0.00 | -8.45 |
| 2026-09-03 | `RVTY` | 10 | — | $132.45 | +0.00 | $130.63 | -18.20 | -18.20 | +0.00 | -18.20 |
| 2026-09-03 | `CRK` | 88 | — | $15.45 | +0.00 | $14.95 | -44.00 | -44.00 | +0.00 | -44.00 |
| 2026-09-03 | `MRNA` | 9 | — | $145.94 | +0.00 | $148.87 | +26.33 | +26.33 | +0.00 | +26.33 |
| 2026-09-03 | `ARCT` | 81 | — | $16.77 | +0.00 | $15.56 | -98.01 | -98.01 | +0.00 | -98.01 |
| 2026-09-04 | `ATRC` | 25 | $52.46 | $52.03 | -10.75 | $51.52 | -12.75 | -23.50 | -21.25 | -12.75 |
| 2026-09-04 | `HRMY` | 31 | $41.86 | $41.50 | -11.16 | — | +0.00 | -11.16 | -44.33 | — |
| 2026-09-04 | `CABA` | 383 | $3.48 | $3.46 | -7.50 | $3.47 | +3.83 | -3.67 | -63.75 | +3.83 |
| 2026-09-04 | `VSTM` | 169 | $7.98 | $7.91 | -11.83 | — | +0.00 | -11.83 | -20.28 | — |
| 2026-09-04 | `RVTY` | 10 | $130.63 | $130.03 | -6.00 | — | +0.00 | -6.00 | -24.20 | — |
| 2026-09-04 | `CRK` | 88 | $14.95 | $15.00 | +4.40 | — | +0.00 | +4.40 | -39.60 | — |
| 2026-09-04 | `MRNA` | 9 | $148.87 | $153.62 | +42.75 | — | +0.00 | +42.75 | +69.08 | — |
| 2026-09-04 | `ARCT` | 81 | $15.56 | $15.61 | +4.05 | — | +0.00 | +4.05 | -93.96 | — |
| 2026-09-04 | `ALEC` | 526 | — | $2.52 | +0.00 | $2.46 | -31.56 | -31.56 | +0.00 | -31.56 |
| 2026-09-04 | `BHC` | 197 | — | $6.71 | +0.00 | $6.56 | -29.55 | -29.55 | +0.00 | -29.55 |
| 2026-09-04 | `BMEA` | 698 | — | $1.90 | +0.00 | $2.03 | +90.74 | +90.74 | +0.00 | +90.74 |
| 2026-09-04 | `OABI` | 277 | — | $4.78 | +0.00 | $4.33 | -124.65 | -124.65 | +0.00 | -124.65 |
| 2026-09-04 | `OPK` | 834 | — | $1.59 | +0.00 | $1.64 | +41.70 | +41.70 | +0.00 | +41.70 |
| 2026-09-04 | `VIR` | 117 | — | $11.31 | +0.00 | $11.38 | +8.77 | +8.77 | +0.00 | +8.77 |
| 2026-09-08 | `CABA` | 383 | $3.47 | $3.43 | -15.32 | — | +0.00 | -15.32 | -11.49 | — |
| 2026-09-08 | `ALEC` | 526 | $2.46 | $2.38 | -42.08 | — | +0.00 | -42.08 | -73.64 | — |
| 2026-09-08 | `BHC` | 197 | $6.56 | $6.57 | +1.97 | — | +0.00 | +1.97 | -27.58 | — |
| 2026-09-08 | `BMEA` | 698 | $2.03 | $2.00 | -20.94 | — | +0.00 | -20.94 | +69.80 | — |
| 2026-09-08 | `OABI` | 277 | $4.33 | $4.30 | -8.31 | — | +0.00 | -8.31 | -132.96 | — |
| 2026-09-08 | `OPK` | 834 | $1.64 | $1.63 | -8.34 | — | +0.00 | -8.34 | +33.36 | — |
| 2026-09-08 | `VIR` | 117 | $11.38 | $11.22 | -19.30 | — | +0.00 | -19.30 | -10.53 | — |
| 2026-09-08 | `ATRC` | 25 | $51.52 | $54.31 | +69.75 | — | +0.00 | +69.75 | +57.00 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +185.07 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $97.53 | $10,153.12 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 |
| 2026-08-14 | +5.50 | $97.53 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | $10,178.12 | +25.00 | +90.14 | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $592.27 | $10,193.91 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×22, MARA×140, LDI×1353, BTBT×845 |
| 2026-08-17 | +2.25 | $592.27 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×22, MARA×140, LDI×1353, BTBT×845 | $10,196.20 | +2.29 | +40.17 | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | $191.62 | $10,173.09 | DVN×27, EOG×8, FANG×6, TMC×313, TGB×150, ELF×14, DNN×391, HNST×263 |
| 2026-08-18 | -6.20 | $191.62 | DVN×27, EOG×8, FANG×6, TMC×313, TGB×150, ELF×14, DNN×391, HNST×263 | $10,124.76 | -48.33 | +0.00 | — | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST | $10,101.41 | $10,101.41 | — |
| 2026-08-19 | -7.20 | $10,101.41 | — | $10,101.41 | +0.00 | +0.00 | — | — | $10,101.41 | $10,101.41 | — |
| 2026-08-20 | +1.12 | $10,101.41 | — | $10,101.41 | +0.00 | +234.52 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $203.57 | $10,311.13 | AG×61, BHP×13, CDE×61, HDSN×218, IAG×64, KGC×42, NFGC×721, WPM×8 |
| 2026-08-21 | +3.25 | $203.57 | AG×61, BHP×13, CDE×61, HDSN×218, IAG×64, KGC×42, NFGC×721, WPM×8 | $10,580.85 | +269.72 | +265.42 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $14.75 | $10,781.93 | AU×11, AUPH×76, AEM×6, ARCT×118, AUTL×534, CRDL×683, CRSP×22, CYPH×999 |
| 2026-08-24 | -5.17 | $14.75 | AU×11, AUPH×76, AEM×6, ARCT×118, AUTL×534, CRDL×683, CRSP×22, CYPH×999 | $11,161.11 | +379.18 | +0.00 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $11,121.36 | $11,121.36 | — |
| 2026-08-25 | +1.80 | $11,121.36 | — | $11,121.36 | -0.00 | +256.77 | MOS, OCUL, INSP, CRMD, RZLT, HCA, CAPR, SAFX | — | $140.21 | $11,335.31 | MOS×58, OCUL×126, INSP×22, CRMD×166, RZLT×281, HCA×3, CAPR×191, SAFX×3883 |
| 2026-08-26 | +2.02 | $140.21 | MOS×58, OCUL×126, INSP×22, CRMD×166, RZLT×281, HCA×3, CAPR×191, SAFX×3883 | $11,334.01 | -1.30 | -36.11 | HCA, MOS, OCUL, INSP, CRMD, RZLT, AVBP, FLNC | MOS, OCUL, INSP, CRMD, RZLT, HCA, CAPR, SAFX | $189.68 | $11,235.20 | HCA×3, MOS×56, OCUL×130, INSP×23, CRMD×164, RZLT×281, AVBP×45, FLNC×126 |
| 2026-08-27 | — | $189.68 | HCA×3, MOS×56, OCUL×130, INSP×23, CRMD×164, RZLT×281, AVBP×45, FLNC×126 | $11,271.81 | +36.61 | +23.87 | RRC, CRK, MOS, SLI, ACMR, GGB, MT, MU | HCA, MOS, OCUL, INSP, CRMD, RZLT, AVBP, FLNC | $566.80 | $11,252.70 | RRC×33, CRK×97, MOS×58, SLI×540, ACMR×17, GGB×307, MT×18, MU×1 |
| 2026-08-28 | +0.75 | $566.80 | RRC×33, CRK×97, MOS×58, SLI×540, ACMR×17, GGB×307, MT×18, MU×1 | $11,257.22 | +4.52 | -313.25 | RRC, CRK, MOS, SLI, SEDG, GRRR, URBN, PYXS | RRC, CRK, MOS, SLI, ACMR, GGB, MT, MU | $123.52 | $10,895.00 | RRC×33, CRK×95, MOS×58, SLI×523, SEDG×42, GRRR×89, URBN×17, PYXS×422 |
| 2026-08-31 | -5.85 | $123.52 | RRC×33, CRK×95, MOS×58, SLI×523, SEDG×42, GRRR×89, URBN×17, PYXS×422 | $10,924.94 | +29.94 | +0.00 | — | RRC, CRK, MOS, SLI, SEDG, GRRR, URBN, PYXS | $10,899.49 | $10,899.49 | — |
| 2026-09-01 | -6.30 | $10,899.49 | — | $10,899.49 | +0.00 | +0.00 | — | — | $10,899.49 | $10,899.49 | — |
| 2026-09-02 | -3.83 | $10,899.49 | — | $10,899.49 | +0.00 | +0.00 | — | — | $10,899.49 | $10,899.49 | — |
| 2026-09-03 | -0.90 | $10,899.49 | — | $10,899.49 | +0.00 | -242.25 | ATRC, HRMY, CABA, VSTM, RVTY, CRK, MRNA, ARCT | — | $152.36 | $10,637.23 | ATRC×25, HRMY×31, CABA×375, VSTM×169, RVTY×10, CRK×88, MRNA×9, ARCT×81 |
| 2026-09-04 | +2.25 | $152.36 | ATRC×25, HRMY×31, CABA×375, VSTM×169, RVTY×10, CRK×88, MRNA×9, ARCT×81 | $10,641.19 | +3.96 | -53.47 | CABA, ALEC, BHC, BMEA, OABI, OPK, VIR, ATRC | ATRC, HRMY, CABA, VSTM, RVTY, CRK, MRNA, ARCT | $5.99 | $10,525.43 | CABA×383, ALEC×526, BHC×197, BMEA×698, OABI×277, OPK×834, VIR×117, ATRC×25 |
| 2026-09-08 | -11.47 | $5.99 | CABA×383, ALEC×526, BHC×197, BMEA×698, OABI×277, OPK×834, VIR×117, ATRC×25 | $10,482.86 | -42.57 | +0.00 | — | CABA, ALEC, BHC, BMEA, OABI, OPK, VIR, ATRC | $10,440.21 | $10,440.21 | — |
| 2026-09-09 | -13.95 | $10,440.21 | — | $10,440.21 | -0.00 | +0.00 | — | — | $10,440.21 | $10,440.21 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | sell at min-hold even if still listed; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | sell at min-hold even if still listed; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | sell at min-hold even if still listed; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | sell at min-hold even if still listed; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | sell at min-hold even if still listed; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | sell at min-hold even if still listed; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | sell at min-hold even if still listed; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | sell at min-hold even if still listed; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.53 | ▲ close $10,153.12 vs 09:30 $10,000.00 (session +185.07) | 16:00 close · cash $97.53 · equity $10,153.12 vs 09:30 $10,000.00 (+153.12; session marks +185.07) · 8 name(s) marked open→close (per-name table). BTSG×20 09:30 $59.80 → close $60.23 +8.60; IREN×27 09:30 $45.98 → close $44.76 -32.94; TPG×24 09:30 $50.62 → close $54.62 +95.92; TGTX×25 09:30 $49.70 → close $47.94 -44.00; SLS×106 09:30 $11.70 → close $12.36 +69.96; HIMS×42 09:30 $29.74 → close $28.77 -40.74; INO×1543 09:30 $0.81 → close $0.90 +138.87; TNDM×53 09:30 $23.33 → close $23.13 -10.60 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,178.12 vs prior close $10,153.12 (+25.00) · 8 name(s) re-marked at the open (per-name table). BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13 | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $1,288.46 | ▼ -7.12 after sell → book $10,176.05; vs 09:30 mark -2.07 | time-stop after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $2,476.80 | ▼ -55.19 after sell → book $10,173.96; vs 09:30 mark -2.09 | time-stop after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $3,801.68 | ▲ +107.86 after sell → book $10,171.88; vs 09:30 mark -2.08 | time-stop after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 25 | $47.27 | $2.08 | $-64.90 | $4,981.35 | ▼ -64.90 after sell → book $10,169.80; vs 09:30 mark -2.08 | time-stop after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $6,293.41 | ▲ +69.56 after sell → book $10,167.46; vs 09:30 mark -2.34 | time-stop after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $7,515.57 | ▼ -29.03 after sell → book $10,165.32; vs 09:30 mark -2.14 | time-stop after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $8,931.32 | ▲ +148.79 after sell → book $10,146.08; vs 09:30 mark -19.24 | time-stop after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $10,143.91 | ▼ -26.05 after sell → book $10,143.91; vs 09:30 mark -2.17 | time-stop after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $9,062.42 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=+5.9; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟢 judge🔴 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $7,885.21 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=+3.6; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟢 judge🔴 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $6,683.19 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=+0.6; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟢 judge🔴 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $5,688.46 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 22 | $57.61 | $2.06 | — | $4,418.98 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+5.7; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 140 | $9.01 | $2.41 | — | $3,155.17 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1353 | $0.94 | $16.74 | — | $1,870.67 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 845 | $1.50 | $10.90 | — | $592.27 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $592.27 | ▲ close $10,193.91 vs 09:30 $10,178.12 (session +90.14) | 16:00 close · cash $592.27 · equity $10,193.91 vs 09:30 $10,178.12 (+15.79; session marks +90.14) · 8 name(s) marked open→close (per-name table). TLN×3 09:30 $359.83 → close $362.74 +8.73; VST×8 09:30 $146.90 → close $148.13 +9.84; NRG×10 09:30 $120.00 → close $126.24 +62.40; DAVE×3 09:30 $330.91 → close $334.57 +10.98; SLG×22 09:30 $57.61 → close $56.09 -33.44; MARA×140 09:30 $9.01 → close $9.20 +26.60; LDI×1353 09:30 $0.94 → close $0.90 -54.12; BTBT×845 09:30 $1.50 → close $1.57 +59.15 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $592.27 | ▲ 09:30 equity $10,196.20 vs yday $10,193.91 (+2.29) | 09:30 open · cash $592.27 (unchanged overnight, no fees) · equity $10,196.20 vs prior close $10,193.91 (+2.29) · 8 name(s) re-marked at the open (per-name table). TLN×3 yday $362.74 → 09:30 $367.88 +15.42; VST×8 yday $148.13 → 09:30 $149.37 +9.92; NRG×10 yday $126.24 → 09:30 $127.40 +11.60; DAVE×3 yday $334.57 → 09:30 $336.94 +7.11; SLG×22 yday $56.09 → 09:30 $55.37 -15.84; MARA×140 yday $9.20 → 09:30 $9.22 +2.80; LDI×1353 yday $0.90 → 09:30 $0.91 +13.53; BTBT×845 yday $1.57 → 09:30 $1.52 -42.25 | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 3 | $367.88 | $2.02 | $+20.13 | $1,693.89 | ▲ +20.13 after sell → book $10,194.18; vs 09:30 mark -2.02 | time-stop after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 8 | $149.37 | $2.03 | $+15.71 | $2,886.82 | ▲ +15.71 after sell → book $10,192.15; vs 09:30 mark -2.03 | time-stop after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $4,158.78 | ▲ +69.94 after sell → book $10,190.11; vs 09:30 mark -2.04 | time-stop after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 3 | $336.94 | $2.02 | $+14.07 | $5,167.58 | ▲ +14.07 after sell → book $10,188.09; vs 09:30 mark -2.02 | time-stop after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 22 | $55.37 | $2.08 | $-53.41 | $6,383.64 | ▼ -53.41 after sell → book $10,186.02; vs 09:30 mark -2.07 | time-stop after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 140 | $9.22 | $2.44 | $+24.55 | $7,672.00 | ▲ +24.55 after sell → book $10,183.57; vs 09:30 mark -2.45 | time-stop after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 1353 | $0.91 | $16.57 | $-73.89 | $8,882.61 | ▼ -73.89 after sell → book $10,167.01; vs 09:30 mark -16.56 | time-stop after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 845 | $1.52 | $11.05 | $-5.05 | $10,155.96 | ▼ -5.05 after sell → book $10,155.96; vs 09:30 mark -11.05 | time-stop after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 27 | $46.18 | $2.07 | — | $8,907.02 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=+6.7; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $7,762.85 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=+5.8; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $6,544.64 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=+8.3; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 313 | $4.05 | $4.04 | — | $5,272.95 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 150 | $8.46 | $2.44 | — | $4,001.51 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 14 | $90.54 | $2.03 | — | $2,731.92 | — | sell at min-hold even if still listed; list flatten; ret5=-7.2; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 391 | $3.24 | $5.04 | — | $1,460.04 | — | sell at min-hold even if still listed; list flatten; ⚪; ret5=+0.3; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 263 | $4.81 | $3.39 | — | $191.62 | — | sell at min-hold even if still listed; list flatten; ⚪; ret5=-11.4; leftover $1269.49 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $191.62 | ▲ close $10,173.09 vs 09:30 $10,196.20 (session +40.17) | 16:00 close · cash $191.62 · equity $10,173.09 vs 09:30 $10,196.20 (-23.11; session marks +40.17) · 8 name(s) marked open→close (per-name table). DVN×27 09:30 $46.18 → close $47.57 +37.53; EOG×8 09:30 $142.77 → close $146.15 +27.04; FANG×6 09:30 $202.70 → close $206.29 +21.54; TMC×313 09:30 $4.05 → close $3.77 -87.64; TGB×150 09:30 $8.46 → close $8.77 +46.50; ELF×14 09:30 $90.54 → close $93.66 +43.68; DNN×391 09:30 $3.24 → close $3.19 -19.55; HNST×263 09:30 $4.81 → close $4.70 -28.93 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $191.62 | ▼ 09:30 equity $10,124.76 vs yday $10,173.09 (-48.33) | 09:30 open · cash $191.62 (unchanged overnight, no fees) · equity $10,124.76 vs prior close $10,173.09 (-48.33) · 8 name(s) re-marked at the open (per-name table). DVN×27 yday $47.57 → 09:30 $48.00 +11.61; EOG×8 yday $146.15 → 09:30 $148.04 +15.12; FANG×6 yday $206.29 → 09:30 $208.93 +15.84; TMC×313 yday $3.77 → 09:30 $3.72 -15.65; TGB×150 yday $8.77 → 09:30 $8.55 -33.00; ELF×14 yday $93.66 → 09:30 $93.44 -3.08; DNN×391 yday $3.19 → 09:30 $3.11 -31.28; HNST×263 yday $4.70 → 09:30 $4.67 -7.89 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 27 | $48.00 | $2.09 | $+44.98 | $1,485.52 | ▲ +44.98 after sell → book $10,122.66; vs 09:30 mark -2.10 | time-stop after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 8 | $148.04 | $2.03 | $+38.11 | $2,667.81 | ▲ +38.11 after sell → book $10,120.63; vs 09:30 mark -2.03 | time-stop after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $3,919.36 | ▲ +33.34 after sell → book $10,118.60; vs 09:30 mark -2.03 | time-stop after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 313 | $3.72 | $4.10 | $-111.43 | $5,079.62 | ▼ -111.43 after sell → book $10,114.50; vs 09:30 mark -4.10 | time-stop after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 150 | $8.55 | $2.48 | $+8.58 | $6,359.65 | ▲ +8.58 after sell → book $10,112.03; vs 09:30 mark -2.47 | time-stop after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 14 | $93.44 | $2.05 | $+36.52 | $7,665.76 | ▲ +36.52 after sell → book $10,109.98; vs 09:30 mark -2.05 | time-stop after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 391 | $3.11 | $5.12 | $-60.99 | $8,876.65 | ▼ -60.99 after sell → book $10,104.86; vs 09:30 mark -5.12 | time-stop after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 judge🟢 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `HNST` | 263 | $4.67 | $3.45 | $-43.66 | $10,101.41 | ▼ -43.66 after sell → book $10,101.41; vs 09:30 mark -3.45 | time-stop after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,101.41 | ▲ close $10,101.41 vs 09:30 $10,124.76 (session +0.00) | 16:00 close · cash $10,101.41 · no lots left · equity $10,101.41. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,101.41 | ▲ 09:30 equity $10,101.41 vs yday $10,101.41 (+0.00) | 09:30 open · cash $10,101.41 · no holdings · equity $10,101.41 vs prior close $10,101.41 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,101.41 | ▲ close $10,101.41 vs 09:30 $10,101.41 (session +0.00) | 16:00 close · cash $10,101.41 · no lots left · equity $10,101.41. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,101.41 | ▲ 09:30 equity $10,101.41 vs yday $10,101.41 (+0.00) | 09:30 open · cash $10,101.41 · no holdings · equity $10,101.41 vs prior close $10,101.41 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 61 | $20.55 | $2.17 | — | $8,845.69 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1262.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,660.53 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1262.68 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 61 | $20.65 | $2.17 | — | $6,398.71 | — | sell at min-hold even if still listed; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1262.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 218 | $5.77 | $2.81 | — | $5,138.03 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1262.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 64 | $19.63 | $2.18 | — | $3,879.53 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1262.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $2,632.96 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1262.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 721 | $1.75 | $9.30 | — | $1,361.90 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1262.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $203.57 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1262.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $203.57 | ▲ close $10,311.13 vs 09:30 $10,101.41 (session +234.52) | 16:00 close · cash $203.57 · equity $10,311.13 vs 09:30 $10,101.41 (+209.72; session marks +234.52) · 8 name(s) marked open→close (per-name table). AG×61 09:30 $20.55 → close $21.19 +39.04; BHP×13 09:30 $91.01 → close $93.63 +34.06; CDE×61 09:30 $20.65 → close $21.11 +28.06; HDSN×218 09:30 $5.77 → close $5.57 -43.60; IAG×64 09:30 $19.63 → close $20.50 +55.68; KGC×42 09:30 $29.63 → close $31.43 +75.60; NFGC×721 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $203.57 | ▲ 09:30 equity $10,580.85 vs yday $10,311.13 (+269.72) | 09:30 open · cash $203.57 (unchanged overnight, no fees) · equity $10,580.85 vs prior close $10,311.13 (+269.72) · 8 name(s) re-marked at the open (per-name table). AG×61 yday $21.19 → 09:30 $21.90 +43.31; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×61 yday $21.11 → 09:30 $21.75 +39.04; HDSN×218 yday $5.57 → 09:30 $5.67 +21.80; IAG×64 yday $20.50 → 09:30 $21.17 +42.88; KGC×42 yday $31.43 → 09:30 $32.17 +31.08; NFGC×721 yday $1.75 → 09:30 $1.79 +28.84; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 61 | $21.90 | $2.19 | $+77.98 | $1,537.28 | ▲ +77.98 after sell → book $10,578.66; vs 09:30 mark -2.19 | time-stop after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,779.59 | ▲ +57.15 after sell → book $10,576.61; vs 09:30 mark -2.05 | time-stop after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 61 | $21.75 | $2.19 | $+62.73 | $4,104.14 | ▲ +62.73 after sell → book $10,574.41; vs 09:30 mark -2.20 | time-stop after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 218 | $5.67 | $2.86 | $-27.47 | $5,337.35 | ▼ -27.47 after sell → book $10,571.56; vs 09:30 mark -2.85 | time-stop after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 64 | $21.17 | $2.20 | $+94.17 | $6,690.02 | ▲ +94.17 after sell → book $10,569.35; vs 09:30 mark -2.21 | time-stop after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 42 | $32.17 | $2.14 | $+102.43 | $8,039.02 | ▲ +102.43 after sell → book $10,567.21; vs 09:30 mark -2.14 | time-stop after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 721 | $1.79 | $9.43 | $+10.11 | $9,320.18 | ▲ +10.11 after sell → book $10,557.78; vs 09:30 mark -9.43 | time-stop after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $10,555.75 | ▲ +77.23 after sell → book $10,555.75; vs 09:30 mark -2.03 | time-stop after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 11 | $119.43 | $2.02 | — | $9,240.00 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1319.47 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 76 | $17.20 | $2.22 | — | $7,930.58 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1319.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 6 | $216.30 | $2.01 | — | $6,630.77 | — | sell at min-hold even if still listed; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1319.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 118 | $11.13 | $2.34 | — | $5,315.09 | — | sell at min-hold even if still listed; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1319.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 534 | $2.47 | $6.89 | — | $3,989.22 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1319.47 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 683 | $1.93 | $8.81 | — | $2,662.22 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1319.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 22 | $59.72 | $2.06 | — | $1,346.32 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1319.47 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 999 | $1.32 | $12.89 | — | $14.75 | — | sell at min-hold even if still listed; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1319.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.75 | ▲ close $10,781.93 vs 09:30 $10,580.85 (session +265.42) | 16:00 close · cash $14.75 · equity $10,781.93 vs 09:30 $10,580.85 (+201.08; session marks +265.42) · 8 name(s) marked open→close (per-name table). AU×11 09:30 $119.43 → close $121.22 +19.69; AUPH×76 09:30 $17.20 → close $16.65 -41.80; AEM×6 09:30 $216.30 → close $216.06 -1.44; ARCT×118 09:30 $11.13 → close $13.45 +273.76; AUTL×534 09:30 $2.47 → close $2.41 -32.04; CRDL×683 09:30 $1.93 → close $1.86 -47.81; CRSP×22 09:30 $59.72 → close $59.50 -4.84; CYPH×999 09:30 $1.32 → close $1.42 +99.90 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.75 | ▲ 09:30 equity $11,161.11 vs yday $10,781.93 (+379.18) | 09:30 open · cash $14.75 (unchanged overnight, no fees) · equity $11,161.11 vs prior close $10,781.93 (+379.18) · 8 name(s) re-marked at the open (per-name table). AU×11 yday $121.22 → 09:30 $120.51 -7.81; AUPH×76 yday $16.65 → 09:30 $16.57 -6.08; AEM×6 yday $216.06 → 09:30 $217.03 +5.82; ARCT×118 yday $13.45 → 09:30 $13.33 -14.16; AUTL×534 yday $2.41 → 09:30 $2.40 -5.34; CRDL×683 yday $1.86 → 09:30 $1.88 +13.66; CRSP×22 yday $59.50 → 09:30 $58.75 -16.50; CYPH×999 yday $1.42 → 09:30 $1.83 +409.59 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 11 | $120.51 | $2.04 | $+7.81 | $1,338.32 | ▲ +7.81 after sell → book $11,159.07; vs 09:30 mark -2.04 | time-stop after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 76 | $16.57 | $2.24 | $-52.34 | $2,595.40 | ▼ -52.34 after sell → book $11,156.83; vs 09:30 mark -2.24 | time-stop after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 6 | $217.03 | $2.03 | $+0.34 | $3,895.55 | ▲ +0.34 after sell → book $11,154.80; vs 09:30 mark -2.03 | time-stop after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 118 | $13.33 | $2.38 | $+254.88 | $5,466.12 | ▲ +254.88 after sell → book $11,152.43; vs 09:30 mark -2.37 | time-stop after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 534 | $2.40 | $6.99 | $-51.26 | $6,740.73 | ▼ -51.26 after sell → book $11,145.44; vs 09:30 mark -6.99 | time-stop after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 683 | $1.88 | $8.93 | $-51.90 | $8,015.83 | ▼ -51.90 after sell → book $11,136.50; vs 09:30 mark -8.94 | time-stop after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 22 | $58.75 | $2.08 | $-25.47 | $9,306.26 | ▼ -25.47 after sell → book $11,134.43; vs 09:30 mark -2.07 | time-stop after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 999 | $1.83 | $13.07 | $+483.54 | $11,121.36 | ▲ +483.54 after sell → book $11,121.36; vs 09:30 mark -13.07 | time-stop after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,121.36 | ▲ close $11,121.36 vs 09:30 $11,161.11 (session +0.00) | 16:00 close · cash $11,121.36 · no lots left · equity $11,121.36. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,121.36 | ▲ 09:30 equity $11,121.36 vs yday $11,121.36 (-0.00) | 09:30 open · cash $11,121.36 · no holdings · equity $11,121.36 vs prior close $11,121.36 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 58 | $23.77 | $2.16 | — | $9,740.54 | — | sell at min-hold even if still listed; list flatten; ⚪; ret5=+13.0; leftover $1390.17 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 126 | $10.98 | $2.37 | — | $8,354.69 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=+1.2; leftover $1390.17 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 22 | $61.19 | $2.06 | — | $7,006.45 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=+7.4; leftover $1390.17 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 166 | $8.35 | $2.49 | — | $5,617.86 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1390.17 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 281 | $4.94 | $3.62 | — | $4,226.10 | — | sell at min-hold even if still listed; list flatten; ret5=+7.1; leftover $1390.17 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $426.97 | $2.00 | — | $2,943.19 | — | sell at min-hold even if still listed; list flatten; ret5=+6.0; leftover $1390.17 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 191 | $7.25 | $2.56 | — | $1,555.88 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1390.17 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3883 | $0.36 | $25.55 | — | $140.21 | — | sell at min-hold even if still listed; list probable,yday_gainer; ret5=-15.6; leftover $1390.17 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $140.21 | ▲ close $11,335.31 vs 09:30 $11,121.36 (session +256.77) | 16:00 close · cash $140.21 · equity $11,335.31 vs 09:30 $11,121.36 (+213.95; session marks +256.77) · 8 name(s) marked open→close (per-name table). MOS×58 09:30 $23.77 → close $24.27 +29.00; OCUL×126 09:30 $10.98 → close $10.88 -12.60; INSP×22 09:30 $61.19 → close $61.07 -2.64; CRMD×166 09:30 $8.35 → close $8.56 +34.86; RZLT×281 09:30 $4.94 → close $5.01 +19.67; HCA×3 09:30 $426.97 → close $428.76 +5.37; CAPR×191 09:30 $7.25 → close $8.29 +198.64; SAFX×3883 09:30 $0.36 → close $0.35 -15.53 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $140.21 | ▼ 09:30 equity $11,334.01 vs yday $11,335.31 (-1.30) | 09:30 open · cash $140.21 (unchanged overnight, no fees) · equity $11,334.01 vs prior close $11,335.31 (-1.30) · 8 name(s) re-marked at the open (per-name table). MOS×58 yday $24.27 → 09:30 $24.84 +33.06; OCUL×126 yday $10.88 → 09:30 $10.79 -11.34; INSP×22 yday $61.07 → 09:30 $60.07 -22.00; CRMD×166 yday $8.56 → 09:30 $8.60 +6.64; RZLT×281 yday $5.01 → 09:30 $5.01 +0.00; HCA×3 yday $428.76 → 09:30 $427.50 -3.78; CAPR×191 yday $8.29 → 09:30 $8.29 +0.00; SAFX×3883 yday $0.35 → 09:30 $0.35 -3.88 | — |
| 2026-08-26 09:30 ET | **SELL** | `MOS` | 58 | $24.84 | $2.19 | $+57.71 | $1,578.75 | ▲ +57.71 after sell → book $11,331.83; vs 09:30 mark -2.18 | time-stop after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **SELL** | `OCUL` | 126 | $10.79 | $2.40 | $-28.71 | $2,935.89 | ▼ -28.71 after sell → book $11,329.43; vs 09:30 mark -2.40 | time-stop after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **SELL** | `INSP` | 22 | $60.07 | $2.08 | $-28.77 | $4,255.35 | ▼ -28.77 after sell → book $11,327.35; vs 09:30 mark -2.08 | time-stop after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **SELL** | `CRMD` | 166 | $8.60 | $2.53 | $+36.48 | $5,680.42 | ▲ +36.48 after sell → book $11,324.82; vs 09:30 mark -2.53 | time-stop after 1 sess (min 1) | join🟡 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `RZLT` | 281 | $5.01 | $3.68 | $+12.36 | $7,084.55 | ▲ +12.36 after sell → book $11,321.14; vs 09:30 mark -3.68 | time-stop after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `HCA` | 3 | $427.50 | $2.02 | $-2.43 | $8,365.03 | ▼ -2.43 after sell → book $11,319.12; vs 09:30 mark -2.02 | time-stop after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `CAPR` | 191 | $8.29 | $2.61 | $+193.47 | $9,945.81 | ▲ +193.47 after sell → book $11,316.51; vs 09:30 mark -2.61 | time-stop after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `SAFX` | 3883 | $0.35 | $26.01 | $-70.98 | $11,290.50 | ▼ -70.98 after sell → book $11,290.50; vs 09:30 mark -26.01 | time-stop after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `HCA` | 3 | $427.50 | $2.00 | — | $10,006.00 | — | sell at min-hold even if still listed; list flatten; ret5=+4.1; leftover $1411.31 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `MOS` | 56 | $24.84 | $2.16 | — | $8,612.80 | — | sell at min-hold even if still listed; list flatten; ret5=+14.8; leftover $1411.31 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **BUY** | `OCUL` | 130 | $10.79 | $2.38 | — | $7,207.72 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=-1.2; leftover $1411.31 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **BUY** | `INSP` | 23 | $60.07 | $2.06 | — | $5,824.06 | — | sell at min-hold even if still listed; list flatten; ret5=+6.4; leftover $1411.31 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **BUY** | `CRMD` | 164 | $8.60 | $2.48 | — | $4,411.17 | — | sell at min-hold even if still listed; list flatten; 🔵; ret5=+4.8; leftover $1411.31 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `RZLT` | 281 | $5.01 | $3.62 | — | $2,999.74 | — | sell at min-hold even if still listed; list flatten,yday_gainer; 🔵; ret5=+7.5; leftover $1411.31 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 45 | $31.21 | $2.12 | — | $1,593.16 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $1411.31 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 126 | $11.12 | $2.37 | — | $189.68 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $1411.31 | join🔴 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $189.68 | ▼ close $11,235.20 vs 09:30 $11,334.01 (session -36.11) | 16:00 close · cash $189.68 · equity $11,235.20 vs 09:30 $11,334.01 (-98.81; session marks -36.11) · 8 name(s) marked open→close (per-name table). MOS×56 09:30 $24.84 → close $24.16 -38.08; OCUL×130 09:30 $10.79 → close $10.77 -2.60; INSP×23 09:30 $60.07 → close $61.80 +39.79; CRMD×164 09:30 $8.60 → close $8.39 -34.44; RZLT×281 09:30 $5.01 → close $5.04 +8.43; HCA×3 09:30 $427.50 → close $427.16 -1.02; AVBP×45 09:30 $31.21 → close $31.14 -3.15; FLNC×126 09:30 $11.12 → close $11.08 -5.04 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $189.68 | ▲ 09:30 equity $11,271.81 vs yday $11,235.20 (+36.61) | 09:30 open · cash $189.68 (unchanged overnight, no fees) · equity $11,271.81 vs prior close $11,235.20 (+36.61) · 8 name(s) re-marked at the open (per-name table). HCA×3 yday $427.16 → 09:30 $424.61 -7.65; MOS×56 yday $24.16 → 09:30 $24.00 -8.96; OCUL×130 yday $10.77 → 09:30 $10.63 -18.20; INSP×23 yday $61.80 → 09:30 $62.10 +6.90; CRMD×164 yday $8.39 → 09:30 $8.49 +16.40; RZLT×281 yday $5.04 → 09:30 $5.07 +8.43; AVBP×45 yday $31.14 → 09:30 $30.79 -15.75; FLNC×126 yday $11.08 → 09:30 $11.52 +55.44 | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 3 | $424.61 | $2.02 | $-12.69 | $1,461.49 | ▼ -12.69 after sell → book $11,269.79; vs 09:30 mark -2.02 | time-stop after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `MOS` | 56 | $24.00 | $2.18 | $-51.38 | $2,803.31 | ▼ -51.38 after sell → book $11,267.61; vs 09:30 mark -2.18 | time-stop after 1 sess (min 1) | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 130 | $10.63 | $2.41 | $-25.59 | $4,182.80 | ▼ -25.59 after sell → book $11,265.20; vs 09:30 mark -2.41 | time-stop after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 23 | $62.10 | $2.08 | $+42.55 | $5,609.02 | ▲ +42.55 after sell → book $11,263.12; vs 09:30 mark -2.08 | time-stop after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 164 | $8.49 | $2.52 | $-23.04 | $6,998.85 | ▼ -23.04 after sell → book $11,260.59; vs 09:30 mark -2.53 | time-stop after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 281 | $5.07 | $3.68 | $+9.55 | $8,419.84 | ▲ +9.55 after sell → book $11,256.91; vs 09:30 mark -3.68 | time-stop after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AVBP` | 45 | $30.79 | $2.15 | $-23.17 | $9,803.25 | ▼ -23.17 after sell → book $11,254.77; vs 09:30 mark -2.14 | time-stop after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FLNC` | 126 | $11.52 | $2.40 | $+45.63 | $11,252.37 | ▲ +45.63 after sell → book $11,252.37; vs 09:30 mark -2.40 | time-stop after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 33 | $41.44 | $2.09 | — | $9,882.76 | — | sell at min-hold even if still listed; list flatten; ret5=+3.1; leftover $1406.55 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 97 | $14.42 | $2.28 | — | $8,481.74 | — | sell at min-hold even if still listed; list flatten; ret5=+7.1; leftover $1406.55 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 58 | $24.00 | $2.16 | — | $7,087.57 | — | sell at min-hold even if still listed; list flatten; ret5=+8.7; leftover $1406.55 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 540 | $2.60 | $6.97 | — | $5,676.61 | — | sell at min-hold even if still listed; list flatten; ret5=+13.0; leftover $1406.55 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 17 | $81.65 | $2.04 | — | $4,286.51 | — | sell at min-hold even if still listed; list mover_buy; 🔵; ret5=+2.0; leftover $1406.55 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 307 | $4.57 | $3.96 | — | $2,879.56 | — | sell at min-hold even if still listed; list mover_buy; 🔵; ret5=+1.1; leftover $1406.55 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 18 | $74.54 | $2.04 | — | $1,535.80 | — | sell at min-hold even if still listed; list mover_buy; 🔵; ret5=-0.1; leftover $1406.55 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $566.80 | — | sell at min-hold even if still listed; list mover_buy; 🔵; ret5=+0.1; leftover $1406.55 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $566.80 | ▲ close $11,252.70 vs 09:30 $11,271.81 (session +23.87) | 16:00 close · cash $566.80 · equity $11,252.70 vs 09:30 $11,271.81 (-19.11; session marks +23.87) · 8 name(s) marked open→close (per-name table). MOS×58 09:30 $24.00 → close $23.76 -13.92; RRC×33 09:30 $41.44 → close $41.64 +6.60; CRK×97 09:30 $14.42 → close $14.62 +19.40; SLI×540 09:30 $2.60 → close $2.64 +21.60; ACMR×17 09:30 $81.65 → close $80.49 -19.72; GGB×307 09:30 $4.57 → close $4.70 +39.91; MT×18 09:30 $74.54 → close $74.63 +1.62; MU×1 09:30 $967.01 → close $935.39 -31.62 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $566.80 | ▲ 09:30 equity $11,257.22 vs yday $11,252.70 (+4.52) | 09:30 open · cash $566.80 (unchanged overnight, no fees) · equity $11,257.22 vs prior close $11,252.70 (+4.52) · 8 name(s) re-marked at the open (per-name table). RRC×33 yday $41.64 → 09:30 $41.74 +3.30; CRK×97 yday $14.62 → 09:30 $14.63 +0.97; MOS×58 yday $23.76 → 09:30 $23.95 +11.02; SLI×540 yday $2.64 → 09:30 $2.68 +21.60; ACMR×17 yday $80.49 → 09:30 $79.27 -20.74; GGB×307 yday $4.70 → 09:30 $4.67 -9.21; MT×18 yday $74.63 → 09:30 $75.39 +13.68; MU×1 yday $935.39 → 09:30 $919.29 -16.10 | — |
| 2026-08-28 09:30 ET | **SELL** | `RRC` | 33 | $41.74 | $2.11 | $+5.70 | $1,942.11 | ▲ +5.70 after sell → book $11,255.11; vs 09:30 mark -2.11 | time-stop after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `CRK` | 97 | $14.63 | $2.31 | $+15.78 | $3,358.91 | ▲ +15.78 after sell → book $11,252.80; vs 09:30 mark -2.31 | time-stop after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 58 | $23.95 | $2.19 | $-7.25 | $4,745.82 | ▼ -7.25 after sell → book $11,250.61; vs 09:30 mark -2.19 | time-stop after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 540 | $2.68 | $7.07 | $+29.17 | $6,185.96 | ▲ +29.17 after sell → book $11,243.55; vs 09:30 mark -7.06 | time-stop after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 17 | $79.27 | $2.06 | $-44.56 | $7,531.48 | ▼ -44.56 after sell → book $11,241.48; vs 09:30 mark -2.07 | time-stop after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 307 | $4.67 | $4.02 | $+22.72 | $8,961.15 | ▲ +22.72 after sell → book $11,237.46; vs 09:30 mark -4.02 | time-stop after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 18 | $75.39 | $2.06 | $+11.19 | $10,316.11 | ▲ +11.19 after sell → book $11,235.40; vs 09:30 mark -2.06 | time-stop after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $11,233.38 | ▼ -51.73 after sell → book $11,233.38; vs 09:30 mark -2.02 | time-stop after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 33 | $41.74 | $2.09 | — | $9,853.87 | — | sell at min-hold even if still listed; list flatten; ret5=+2.4; leftover $1404.17 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CRK` | 95 | $14.63 | $2.27 | — | $8,461.75 | — | sell at min-hold even if still listed; list flatten; ret5=+5.8; leftover $1404.17 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `MOS` | 58 | $23.95 | $2.16 | — | $7,070.49 | — | sell at min-hold even if still listed; list flatten; ret5=+1.8; leftover $1404.17 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `SLI` | 523 | $2.68 | $6.75 | — | $5,662.10 | — | sell at min-hold even if still listed; list flatten,ohlc_hot; ret5=+16.3; leftover $1404.17 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 42 | $32.90 | $2.12 | — | $4,278.18 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1404.17 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 89 | $15.66 | $2.26 | — | $2,882.19 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1404.17 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 17 | $79.42 | $2.04 | — | $1,530.00 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1404.17 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `PYXS` | 422 | $3.32 | $5.44 | — | $123.52 | — | sell at min-hold even if still listed; list probable,yday_gainer; ret5=+6.4; leftover $1404.17 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $123.52 | ▼ close $10,895.00 vs 09:30 $11,257.22 (session -313.25) | 16:00 close · cash $123.52 · equity $10,895.00 vs 09:30 $11,257.22 (-362.22; session marks -313.25) · 8 name(s) marked open→close (per-name table). RRC×33 09:30 $41.74 → close $41.46 -9.24; CRK×95 09:30 $14.63 → close $14.29 -32.30; MOS×58 09:30 $23.95 → close $23.60 -20.30; SLI×523 09:30 $2.68 → close $2.55 -67.99; SEDG×42 09:30 $32.90 → close $31.41 -62.58; GRRR×89 09:30 $15.66 → close $14.41 -111.25; URBN×17 09:30 $79.42 → close $81.09 +28.39; PYXS×422 09:30 $3.32 → close $3.23 -37.98 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $123.52 | ▲ 09:30 equity $10,924.94 vs yday $10,895.00 (+29.94) | 09:30 open · cash $123.52 (unchanged overnight, no fees) · equity $10,924.94 vs prior close $10,895.00 (+29.94) · 8 name(s) re-marked at the open (per-name table). RRC×33 yday $41.46 → 09:30 $42.00 +17.82; CRK×95 yday $14.29 → 09:30 $14.54 +23.75; MOS×58 yday $23.60 → 09:30 $23.68 +4.64; SLI×523 yday $2.55 → 09:30 $2.58 +15.69; SEDG×42 yday $31.41 → 09:30 $31.15 -10.92; GRRR×89 yday $14.41 → 09:30 $14.44 +2.67; URBN×17 yday $81.09 → 09:30 $80.44 -11.05; PYXS×422 yday $3.23 → 09:30 $3.20 -12.66 | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 33 | $42.00 | $2.11 | $+4.38 | $1,507.41 | ▲ +4.38 after sell → book $10,922.83; vs 09:30 mark -2.11 | time-stop after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 95 | $14.54 | $2.30 | $-13.13 | $2,886.41 | ▼ -13.13 after sell → book $10,920.53; vs 09:30 mark -2.30 | time-stop after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 58 | $23.68 | $2.19 | $-20.01 | $4,257.66 | ▼ -20.01 after sell → book $10,918.34; vs 09:30 mark -2.19 | time-stop after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 523 | $2.58 | $6.84 | $-65.89 | $5,600.16 | ▼ -65.89 after sell → book $10,911.50; vs 09:30 mark -6.84 | time-stop after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 42 | $31.15 | $2.14 | $-77.75 | $6,906.32 | ▼ -77.75 after sell → book $10,909.36; vs 09:30 mark -2.14 | time-stop after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 89 | $14.44 | $2.28 | $-113.12 | $8,189.20 | ▼ -113.12 after sell → book $10,907.08; vs 09:30 mark -2.28 | time-stop after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 17 | $80.44 | $2.06 | $+13.24 | $9,554.62 | ▲ +13.24 after sell → book $10,905.02; vs 09:30 mark -2.06 | time-stop after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PYXS` | 422 | $3.20 | $5.52 | $-61.61 | $10,899.49 | ▼ -61.61 after sell → book $10,899.49; vs 09:30 mark -5.53 | time-stop after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,899.49 | ▲ close $10,899.49 vs 09:30 $10,924.94 (session +0.00) | 16:00 close · cash $10,899.49 · no lots left · equity $10,899.49. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,899.49 | ▲ 09:30 equity $10,899.49 vs yday $10,899.49 (+0.00) | 09:30 open · cash $10,899.49 · no holdings · equity $10,899.49 vs prior close $10,899.49 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,899.49 | ▲ close $10,899.49 vs 09:30 $10,899.49 (session +0.00) | 16:00 close · cash $10,899.49 · no lots left · equity $10,899.49. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,899.49 | ▲ 09:30 equity $10,899.49 vs yday $10,899.49 (+0.00) | 09:30 open · cash $10,899.49 · no holdings · equity $10,899.49 vs prior close $10,899.49 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,899.49 | ▲ close $10,899.49 vs 09:30 $10,899.49 (session +0.00) | 16:00 close · cash $10,899.49 · no lots left · equity $10,899.49. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,899.49 | ▲ 09:30 equity $10,899.49 vs yday $10,899.49 (+0.00) | 09:30 open · cash $10,899.49 · no holdings · equity $10,899.49 vs prior close $10,899.49 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 25 | $52.88 | $2.06 | — | $9,575.43 | — | sell at min-hold even if still listed; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1362.44 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 31 | $42.93 | $2.08 | — | $8,242.52 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1362.44 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 375 | $3.63 | $4.84 | — | $6,876.43 | — | sell at min-hold even if still listed; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1362.44 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 169 | $8.03 | $2.50 | — | $5,516.86 | — | sell at min-hold even if still listed; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1362.44 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $4,190.34 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1362.44 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 88 | $15.45 | $2.25 | — | $2,828.49 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1362.44 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 9 | $145.94 | $2.02 | — | $1,512.97 | — | sell at min-hold even if still listed; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1362.44 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 81 | $16.77 | $2.23 | — | $152.36 | — | sell at min-hold even if still listed; list probable,yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1362.44 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $152.36 | ▼ close $10,637.23 vs 09:30 $10,899.49 (session -242.25) | 16:00 close · cash $152.36 · equity $10,637.23 vs 09:30 $10,899.49 (-262.26; session marks -242.25) · 8 name(s) marked open→close (per-name table). ATRC×25 09:30 $52.88 → close $52.46 -10.50; HRMY×31 09:30 $42.93 → close $41.86 -33.17; CABA×375 09:30 $3.63 → close $3.48 -56.25; VSTM×169 09:30 $8.03 → close $7.98 -8.45; RVTY×10 09:30 $132.45 → close $130.63 -18.20; CRK×88 09:30 $15.45 → close $14.95 -44.00; MRNA×9 09:30 $145.94 → close $148.87 +26.33; ARCT×81 09:30 $16.77 → close $15.56 -98.01 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $152.36 | ▲ 09:30 equity $10,641.19 vs yday $10,637.23 (+3.96) | 09:30 open · cash $152.36 (unchanged overnight, no fees) · equity $10,641.19 vs prior close $10,637.23 (+3.96) · 8 name(s) re-marked at the open (per-name table). ATRC×25 yday $52.46 → 09:30 $52.03 -10.75; HRMY×31 yday $41.86 → 09:30 $41.50 -11.16; CABA×375 yday $3.48 → 09:30 $3.46 -7.50; VSTM×169 yday $7.98 → 09:30 $7.91 -11.83; RVTY×10 yday $130.63 → 09:30 $130.03 -6.00; CRK×88 yday $14.95 → 09:30 $15.00 +4.40; MRNA×9 yday $148.87 → 09:30 $153.62 +42.75; ARCT×81 yday $15.56 → 09:30 $15.61 +4.05 | — |
| 2026-09-04 09:30 ET | **SELL** | `ATRC` | 25 | $52.03 | $2.09 | $-25.40 | $1,451.03 | ▼ -25.40 after sell → book $10,639.11; vs 09:30 mark -2.08 | time-stop after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 31 | $41.50 | $2.10 | $-48.52 | $2,735.42 | ▼ -48.52 after sell → book $10,637.00; vs 09:30 mark -2.11 | time-stop after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `CABA` | 375 | $3.46 | $4.91 | $-73.50 | $4,028.01 | ▼ -73.50 after sell → book $10,632.09; vs 09:30 mark -4.91 | time-stop after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 169 | $7.91 | $2.54 | $-25.31 | $5,362.27 | ▼ -25.31 after sell → book $10,629.56; vs 09:30 mark -2.53 | time-stop after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $130.03 | $2.04 | $-28.26 | $6,660.53 | ▼ -28.26 after sell → book $10,627.52; vs 09:30 mark -2.04 | time-stop after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 88 | $15.00 | $2.28 | $-44.13 | $7,978.25 | ▼ -44.13 after sell → book $10,625.24; vs 09:30 mark -2.28 | time-stop after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 9 | $153.62 | $2.04 | $+65.02 | $9,358.79 | ▲ +65.02 after sell → book $10,623.20; vs 09:30 mark -2.04 | time-stop after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 81 | $15.61 | $2.26 | $-98.45 | $10,620.94 | ▼ -98.45 after sell → book $10,620.94; vs 09:30 mark -2.26 | time-stop after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 383 | $3.46 | $4.94 | — | $9,290.82 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1327.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 526 | $2.52 | $6.79 | — | $7,958.52 | — | sell at min-hold even if still listed; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1327.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 197 | $6.71 | $2.58 | — | $6,634.07 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1327.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 698 | $1.90 | $9.00 | — | $5,298.86 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1327.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 277 | $4.78 | $3.57 | — | $3,971.23 | — | sell at min-hold even if still listed; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1327.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 834 | $1.59 | $10.76 | — | $2,634.41 | — | sell at min-hold even if still listed; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1327.62 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 117 | $11.31 | $2.34 | — | $1,308.80 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1327.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ATRC` | 25 | $52.03 | $2.06 | — | $5.99 | — | sell at min-hold even if still listed; list flatten,mover_buy; 🔵; ⚪; ret5=+6.5; leftover $1327.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.99 | ▼ close $10,525.43 vs 09:30 $10,641.19 (session -53.47) | 16:00 close · cash $5.99 · equity $10,525.43 vs 09:30 $10,641.19 (-115.76; session marks -53.47) · 8 name(s) marked open→close (per-name table). ATRC×25 09:30 $52.03 → close $51.52 -12.75; CABA×383 09:30 $3.46 → close $3.47 +3.83; ALEC×526 09:30 $2.52 → close $2.46 -31.56; BHC×197 09:30 $6.71 → close $6.56 -29.55; BMEA×698 09:30 $1.90 → close $2.03 +90.74; OABI×277 09:30 $4.78 → close $4.33 -124.65; OPK×834 09:30 $1.59 → close $1.64 +41.70; VIR×117 09:30 $11.31 → close $11.38 +8.77 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.99 | ▼ 09:30 equity $10,482.86 vs yday $10,525.43 (-42.57) | 09:30 open · cash $5.99 (unchanged overnight, no fees) · equity $10,482.86 vs prior close $10,525.43 (-42.57) · 8 name(s) re-marked at the open (per-name table). CABA×383 yday $3.47 → 09:30 $3.43 -15.32; ALEC×526 yday $2.46 → 09:30 $2.38 -42.08; BHC×197 yday $6.56 → 09:30 $6.57 +1.97; BMEA×698 yday $2.03 → 09:30 $2.00 -20.94; OABI×277 yday $4.33 → 09:30 $4.30 -8.31; OPK×834 yday $1.64 → 09:30 $1.63 -8.34; VIR×117 yday $11.38 → 09:30 $11.22 -19.30; ATRC×25 yday $51.52 → 09:30 $54.31 +69.75 | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 383 | $3.43 | $5.01 | $-21.45 | $1,314.66 | ▼ -21.45 after sell → book $10,477.84; vs 09:30 mark -5.02 | time-stop after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 526 | $2.38 | $6.88 | $-87.31 | $2,559.66 | ▼ -87.31 after sell → book $10,470.96; vs 09:30 mark -6.88 | time-stop after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 197 | $6.57 | $2.62 | $-32.79 | $3,851.32 | ▼ -32.79 after sell → book $10,468.33; vs 09:30 mark -2.63 | time-stop after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 698 | $2.00 | $9.13 | $+51.66 | $5,238.19 | ▲ +51.66 after sell → book $10,459.20; vs 09:30 mark -9.13 | time-stop after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 277 | $4.30 | $3.63 | $-140.16 | $6,425.66 | ▼ -140.16 after sell → book $10,455.57; vs 09:30 mark -3.63 | time-stop after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 834 | $1.63 | $10.91 | $+11.69 | $7,774.18 | ▲ +11.69 after sell → book $10,444.67; vs 09:30 mark -10.90 | time-stop after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 117 | $11.22 | $2.37 | $-15.24 | $9,084.54 | ▼ -15.24 after sell → book $10,442.29; vs 09:30 mark -2.38 | time-stop after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 25 | $54.31 | $2.09 | $+52.85 | $10,440.21 | ▲ +52.85 after sell → book $10,440.21; vs 09:30 mark -2.08 | time-stop after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,440.21 | ▲ close $10,440.21 vs 09:30 $10,482.86 (session +0.00) | 16:00 close · cash $10,440.21 · no lots left · equity $10,440.21. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,440.21 | ▲ 09:30 equity $10,440.21 vs yday $10,440.21 (-0.00) | 09:30 open · cash $10,440.21 · no holdings · equity $10,440.21 vs prior close $10,440.21 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,440.21 | ▲ close $10,440.21 vs 09:30 $10,440.21 (session +0.00) | 16:00 close · cash $10,440.21 · no lots left · equity $10,440.21. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
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
| 2026-09-02 | `PRQR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SID` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `PCG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VENU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CABA` | hard_red | hard-red S=-13.95 sit; no new buys |
