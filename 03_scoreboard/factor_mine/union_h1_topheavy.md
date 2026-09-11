# Factor mine action — `union_h1_topheavy`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `topheavy` · sell `list` · S-boost `none` · 40% to #1, rest split

Cash book **+8.47%** ($10,847) · signal-only (no cash/fees) was +4.90%. Starts YES **7/20**. Fills 148 · skips 73 · realized $+846.90.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Give about 40% of leftover cash to the first name; split the rest.
- Skip a name if the slice cannot buy 1 share after fees.
- This is a LONG sleeve: it buys shares and wants the price to go up.

### When it sells

- Sell first, then buy. Never sell a ticker we do not hold.
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `topheavy` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,846.92.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `BTSG` | 66 | — | $59.80 | +0.00 | $60.23 | +28.38 | +28.38 | +0.00 | +28.38 |
| 2026-08-13 | `IREN` | 18 | — | $45.98 | +0.00 | $44.76 | -21.96 | -21.96 | +0.00 | -21.96 |
| 2026-08-13 | `TPG` | 16 | — | $50.62 | +0.00 | $54.62 | +63.95 | +63.95 | +0.00 | +63.95 |
| 2026-08-13 | `TGTX` | 17 | — | $49.70 | +0.00 | $47.94 | -29.92 | -29.92 | +0.00 | -29.92 |
| 2026-08-13 | `SLS` | 73 | — | $11.70 | +0.00 | $12.36 | +48.18 | +48.18 | +0.00 | +48.18 |
| 2026-08-13 | `HIMS` | 28 | — | $29.74 | +0.00 | $28.77 | -27.16 | -27.16 | +0.00 | -27.16 |
| 2026-08-13 | `INO` | 1058 | — | $0.81 | +0.00 | $0.90 | +95.22 | +95.22 | +0.00 | +95.22 |
| 2026-08-13 | `TNDM` | 36 | — | $23.33 | +0.00 | $23.13 | -7.20 | -7.20 | +0.00 | -7.20 |
| 2026-08-14 | `BTSG` | 66 | $60.23 | $59.65 | -38.28 | — | +0.00 | -38.28 | -9.90 | — |
| 2026-08-14 | `IREN` | 18 | $44.76 | $44.09 | -12.06 | — | +0.00 | -12.06 | -34.02 | — |
| 2026-08-14 | `TPG` | 16 | $54.62 | $55.29 | +10.72 | — | +0.00 | +10.72 | +74.67 | — |
| 2026-08-14 | `TGTX` | 17 | $47.94 | $47.27 | -11.39 | — | +0.00 | -11.39 | -41.31 | — |
| 2026-08-14 | `SLS` | 73 | $12.36 | $12.40 | +2.92 | — | +0.00 | +2.92 | +51.10 | — |
| 2026-08-14 | `HIMS` | 28 | $28.77 | $29.15 | +10.64 | — | +0.00 | +10.64 | -16.52 | — |
| 2026-08-14 | `INO` | 1058 | $0.90 | $0.93 | +31.74 | — | +0.00 | +31.74 | +126.96 | — |
| 2026-08-14 | `TNDM` | 36 | $23.13 | $22.92 | -7.56 | — | +0.00 | -7.56 | -14.76 | — |
| 2026-08-14 | `TLN` | 11 | — | $359.83 | +0.00 | $362.74 | +32.01 | +32.01 | +0.00 | +32.01 |
| 2026-08-14 | `VST` | 5 | — | $146.90 | +0.00 | $148.13 | +6.15 | +6.15 | +0.00 | +6.15 |
| 2026-08-14 | `NRG` | 7 | — | $120.00 | +0.00 | $126.24 | +43.68 | +43.68 | +0.00 | +43.68 |
| 2026-08-14 | `DAVE` | 2 | — | $330.91 | +0.00 | $334.57 | +7.32 | +7.32 | +0.00 | +7.32 |
| 2026-08-14 | `SLG` | 14 | — | $57.61 | +0.00 | $56.09 | -21.28 | -21.28 | +0.00 | -21.28 |
| 2026-08-14 | `MARA` | 95 | — | $9.01 | +0.00 | $9.20 | +18.05 | +18.05 | +0.00 | +18.05 |
| 2026-08-14 | `LDI` | 922 | — | $0.94 | +0.00 | $0.90 | -36.88 | -36.88 | +0.00 | -36.88 |
| 2026-08-14 | `BTBT` | 576 | — | $1.50 | +0.00 | $1.57 | +40.32 | +40.32 | +0.00 | +40.32 |
| 2026-08-17 | `TLN` | 11 | $362.74 | $367.88 | +56.54 | — | +0.00 | +56.54 | +88.55 | — |
| 2026-08-17 | `VST` | 5 | $148.13 | $149.37 | +6.20 | — | +0.00 | +6.20 | +12.35 | — |
| 2026-08-17 | `NRG` | 7 | $126.24 | $127.40 | +8.12 | — | +0.00 | +8.12 | +51.80 | — |
| 2026-08-17 | `DAVE` | 2 | $334.57 | $336.94 | +4.74 | — | +0.00 | +4.74 | +12.06 | — |
| 2026-08-17 | `SLG` | 14 | $56.09 | $55.37 | -10.08 | — | +0.00 | -10.08 | -31.36 | — |
| 2026-08-17 | `MARA` | 95 | $9.20 | $9.22 | +1.90 | — | +0.00 | +1.90 | +19.95 | — |
| 2026-08-17 | `LDI` | 922 | $0.90 | $0.91 | +9.22 | — | +0.00 | +9.22 | -27.66 | — |
| 2026-08-17 | `BTBT` | 576 | $1.57 | $1.52 | -28.80 | — | +0.00 | -28.80 | +11.52 | — |
| 2026-08-17 | `DVN` | 87 | — | $46.18 | +0.00 | $47.57 | +120.93 | +120.93 | +0.00 | +120.93 |
| 2026-08-17 | `EOG` | 6 | — | $142.77 | +0.00 | $146.15 | +20.28 | +20.28 | +0.00 | +20.28 |
| 2026-08-17 | `FANG` | 4 | — | $202.70 | +0.00 | $206.29 | +14.36 | +14.36 | +0.00 | +14.36 |
| 2026-08-17 | `TMC` | 214 | — | $4.05 | +0.00 | $3.77 | -59.92 | -59.92 | +0.00 | -59.92 |
| 2026-08-17 | `TGB` | 102 | — | $8.46 | +0.00 | $8.77 | +31.62 | +31.62 | +0.00 | +31.62 |
| 2026-08-17 | `ELF` | 9 | — | $90.54 | +0.00 | $93.66 | +28.08 | +28.08 | +0.00 | +28.08 |
| 2026-08-17 | `DNN` | 268 | — | $3.24 | +0.00 | $3.19 | -13.40 | -13.40 | +0.00 | -13.40 |
| 2026-08-17 | `HNST` | 180 | — | $4.81 | +0.00 | $4.70 | -19.80 | -19.80 | +0.00 | -19.80 |
| 2026-08-18 | `DVN` | 87 | $47.57 | $48.00 | +37.41 | — | +0.00 | +37.41 | +158.34 | — |
| 2026-08-18 | `EOG` | 6 | $146.15 | $148.04 | +11.34 | — | +0.00 | +11.34 | +31.62 | — |
| 2026-08-18 | `FANG` | 4 | $206.29 | $208.93 | +10.56 | — | +0.00 | +10.56 | +24.92 | — |
| 2026-08-18 | `TMC` | 214 | $3.77 | $3.72 | -10.70 | — | +0.00 | -10.70 | -70.62 | — |
| 2026-08-18 | `TGB` | 102 | $8.77 | $8.55 | -22.44 | — | +0.00 | -22.44 | +9.18 | — |
| 2026-08-18 | `ELF` | 9 | $93.66 | $93.44 | -1.98 | — | +0.00 | -1.98 | +26.10 | — |
| 2026-08-18 | `DNN` | 268 | $3.19 | $3.11 | -21.44 | — | +0.00 | -21.44 | -34.84 | — |
| 2026-08-18 | `HNST` | 180 | $4.70 | $4.67 | -5.40 | — | +0.00 | -5.40 | -25.20 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 199 | — | $20.55 | +0.00 | $21.19 | +127.36 | +127.36 | +0.00 | +127.36 |
| 2026-08-20 | `BHP` | 9 | — | $91.01 | +0.00 | $93.63 | +23.58 | +23.58 | +0.00 | +23.58 |
| 2026-08-20 | `CDE` | 42 | — | $20.65 | +0.00 | $21.11 | +19.32 | +19.32 | +0.00 | +19.32 |
| 2026-08-20 | `HDSN` | 152 | — | $5.77 | +0.00 | $5.57 | -30.40 | -30.40 | +0.00 | -30.40 |
| 2026-08-20 | `IAG` | 44 | — | $19.63 | +0.00 | $20.50 | +38.28 | +38.28 | +0.00 | +38.28 |
| 2026-08-20 | `KGC` | 29 | — | $29.63 | +0.00 | $31.43 | +52.20 | +52.20 | +0.00 | +52.20 |
| 2026-08-20 | `NFGC` | 501 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 6 | — | $144.54 | +0.00 | $150.25 | +34.26 | +34.26 | +0.00 | +34.26 |
| 2026-08-21 | `AG` | 199 | $21.19 | $21.90 | +141.29 | — | +0.00 | +141.29 | +268.65 | — |
| 2026-08-21 | `BHP` | 9 | $93.63 | $95.72 | +18.81 | — | +0.00 | +18.81 | +42.39 | — |
| 2026-08-21 | `CDE` | 42 | $21.11 | $21.75 | +26.88 | — | +0.00 | +26.88 | +46.20 | — |
| 2026-08-21 | `HDSN` | 152 | $5.57 | $5.67 | +15.20 | — | +0.00 | +15.20 | -15.20 | — |
| 2026-08-21 | `IAG` | 44 | $20.50 | $21.17 | +29.48 | — | +0.00 | +29.48 | +67.76 | — |
| 2026-08-21 | `KGC` | 29 | $31.43 | $32.17 | +21.46 | — | +0.00 | +21.46 | +73.66 | — |
| 2026-08-21 | `NFGC` | 501 | $1.75 | $1.79 | +20.04 | — | +0.00 | +20.04 | +20.04 | — |
| 2026-08-21 | `WPM` | 6 | $150.25 | $154.70 | +26.70 | — | +0.00 | +26.70 | +60.96 | — |
| 2026-08-21 | `AU` | 36 | — | $119.43 | +0.00 | $121.22 | +64.44 | +64.44 | +0.00 | +64.44 |
| 2026-08-21 | `AUPH` | 53 | — | $17.20 | +0.00 | $16.65 | -29.15 | -29.15 | +0.00 | -29.15 |
| 2026-08-21 | `AEM` | 4 | — | $216.30 | +0.00 | $216.06 | -0.96 | -0.96 | +0.00 | -0.96 |
| 2026-08-21 | `ARCT` | 82 | — | $11.13 | +0.00 | $13.45 | +190.24 | +190.24 | +0.00 | +190.24 |
| 2026-08-21 | `AUTL` | 373 | — | $2.47 | +0.00 | $2.41 | -22.38 | -22.38 | +0.00 | -22.38 |
| 2026-08-21 | `CRDL` | 477 | — | $1.93 | +0.00 | $1.86 | -33.39 | -33.39 | +0.00 | -33.39 |
| 2026-08-21 | `CRSP` | 15 | — | $59.72 | +0.00 | $59.50 | -3.30 | -3.30 | +0.00 | -3.30 |
| 2026-08-21 | `CYPH` | 698 | — | $1.32 | +0.00 | $1.42 | +69.80 | +69.80 | +0.00 | +69.80 |
| 2026-08-24 | `AU` | 36 | $121.22 | $120.51 | -25.56 | — | +0.00 | -25.56 | +38.88 | — |
| 2026-08-24 | `AUPH` | 53 | $16.65 | $16.57 | -4.24 | — | +0.00 | -4.24 | -33.39 | — |
| 2026-08-24 | `AEM` | 4 | $216.06 | $217.03 | +3.88 | — | +0.00 | +3.88 | +2.92 | — |
| 2026-08-24 | `ARCT` | 82 | $13.45 | $13.33 | -9.84 | — | +0.00 | -9.84 | +180.40 | — |
| 2026-08-24 | `AUTL` | 373 | $2.41 | $2.40 | -3.73 | — | +0.00 | -3.73 | -26.11 | — |
| 2026-08-24 | `CRDL` | 477 | $1.86 | $1.88 | +9.54 | — | +0.00 | +9.54 | -23.85 | — |
| 2026-08-24 | `CRSP` | 15 | $59.50 | $58.75 | -11.25 | — | +0.00 | -11.25 | -14.55 | — |
| 2026-08-24 | `CYPH` | 698 | $1.42 | $1.83 | +286.18 | — | +0.00 | +286.18 | +355.98 | — |
| 2026-08-25 | `MOS` | 188 | — | $23.77 | +0.00 | $24.27 | +94.00 | +94.00 | +0.00 | +94.00 |
| 2026-08-25 | `OCUL` | 87 | — | $10.98 | +0.00 | $10.88 | -8.70 | -8.70 | +0.00 | -8.70 |
| 2026-08-25 | `INSP` | 15 | — | $61.19 | +0.00 | $61.07 | -1.80 | -1.80 | +0.00 | -1.80 |
| 2026-08-25 | `CRMD` | 114 | — | $8.35 | +0.00 | $8.56 | +23.94 | +23.94 | +0.00 | +23.94 |
| 2026-08-25 | `RZLT` | 193 | — | $4.94 | +0.00 | $5.01 | +13.51 | +13.51 | +0.00 | +13.51 |
| 2026-08-25 | `HCA` | 2 | — | $426.97 | +0.00 | $428.76 | +3.58 | +3.58 | +0.00 | +3.58 |
| 2026-08-25 | `CAPR` | 132 | — | $7.25 | +0.00 | $8.29 | +137.28 | +137.28 | +0.00 | +137.28 |
| 2026-08-25 | `SAFX` | 2675 | — | $0.36 | +0.00 | $0.35 | -10.70 | -10.70 | +0.00 | -10.70 |
| 2026-08-26 | `MOS` | 188 | $24.27 | $24.84 | +107.16 | $24.16 | -127.84 | -20.68 | +201.16 | +73.32 |
| 2026-08-26 | `OCUL` | 87 | $10.88 | $10.79 | -7.83 | $10.77 | -1.74 | -9.57 | -16.53 | -18.27 |
| 2026-08-26 | `INSP` | 15 | $61.07 | $60.07 | -15.00 | $61.80 | +25.95 | +10.95 | -16.80 | +9.15 |
| 2026-08-26 | `CRMD` | 114 | $8.56 | $8.60 | +4.56 | $8.39 | -23.94 | -19.38 | +28.50 | +4.56 |
| 2026-08-26 | `RZLT` | 193 | $5.01 | $5.01 | +0.00 | $5.04 | +5.79 | +5.79 | +13.51 | +19.30 |
| 2026-08-26 | `HCA` | 2 | $428.76 | $427.50 | -2.52 | $427.16 | -0.68 | -3.20 | +1.06 | +0.38 |
| 2026-08-26 | `CAPR` | 132 | $8.29 | $8.29 | +0.00 | — | +0.00 | +0.00 | +137.28 | — |
| 2026-08-26 | `SAFX` | 2675 | $0.35 | $0.35 | -2.68 | — | +0.00 | -2.68 | -13.38 | — |
| 2026-08-26 | `AVBP` | 27 | — | $31.21 | +0.00 | $31.14 | -1.89 | -1.89 | +0.00 | -1.89 |
| 2026-08-26 | `FLNC` | 115 | — | $11.12 | +0.00 | $11.08 | -4.60 | -4.60 | +0.00 | -4.60 |
| 2026-08-27 | `MOS` | 188 | $24.16 | $24.00 | -30.08 | $23.76 | -45.12 | -75.20 | +43.24 | -1.88 |
| 2026-08-27 | `OCUL` | 87 | $10.77 | $10.63 | -12.18 | — | +0.00 | -12.18 | -30.45 | — |
| 2026-08-27 | `INSP` | 15 | $61.80 | $62.10 | +4.50 | — | +0.00 | +4.50 | +13.65 | — |
| 2026-08-27 | `CRMD` | 114 | $8.39 | $8.49 | +11.40 | — | +0.00 | +11.40 | +15.96 | — |
| 2026-08-27 | `RZLT` | 193 | $5.04 | $5.07 | +5.79 | — | +0.00 | +5.79 | +25.09 | — |
| 2026-08-27 | `HCA` | 2 | $427.16 | $424.61 | -5.10 | — | +0.00 | -5.10 | -4.72 | — |
| 2026-08-27 | `AVBP` | 27 | $31.14 | $30.79 | -9.45 | — | +0.00 | -9.45 | -11.34 | — |
| 2026-08-27 | `FLNC` | 115 | $11.08 | $11.52 | +50.60 | — | +0.00 | +50.60 | +46.00 | — |
| 2026-08-27 | `RRC` | 65 | — | $41.44 | +0.00 | $41.64 | +13.00 | +13.00 | +0.00 | +13.00 |
| 2026-08-27 | `CRK` | 47 | — | $14.42 | +0.00 | $14.62 | +9.40 | +9.40 | +0.00 | +9.40 |
| 2026-08-27 | `SLI` | 261 | — | $2.60 | +0.00 | $2.64 | +10.44 | +10.44 | +0.00 | +10.44 |
| 2026-08-27 | `ACMR` | 8 | — | $81.65 | +0.00 | $80.49 | -9.28 | -9.28 | +0.00 | -9.28 |
| 2026-08-27 | `GGB` | 149 | — | $4.57 | +0.00 | $4.70 | +19.37 | +19.37 | +0.00 | +19.37 |
| 2026-08-27 | `MT` | 9 | — | $74.54 | +0.00 | $74.63 | +0.81 | +0.81 | +0.00 | +0.81 |
| 2026-08-28 | `MOS` | 188 | $23.76 | $23.95 | +35.72 | $23.60 | -65.80 | -30.08 | +33.84 | -31.96 |
| 2026-08-28 | `RRC` | 65 | $41.64 | $41.74 | +6.50 | $41.46 | -18.20 | -11.70 | +19.50 | +1.30 |
| 2026-08-28 | `CRK` | 47 | $14.62 | $14.63 | +0.47 | $14.29 | -15.98 | -15.51 | +9.87 | -6.11 |
| 2026-08-28 | `SLI` | 261 | $2.64 | $2.68 | +10.44 | $2.55 | -33.93 | -23.49 | +20.88 | -13.05 |
| 2026-08-28 | `ACMR` | 8 | $80.49 | $79.27 | -9.76 | — | +0.00 | -9.76 | -19.04 | — |
| 2026-08-28 | `GGB` | 149 | $4.70 | $4.67 | -4.47 | — | +0.00 | -4.47 | +14.90 | — |
| 2026-08-28 | `MT` | 9 | $74.63 | $75.39 | +6.84 | — | +0.00 | +6.84 | +7.65 | — |
| 2026-08-28 | `SEDG` | 33 | — | $32.90 | +0.00 | $31.41 | -49.17 | -49.17 | +0.00 | -49.17 |
| 2026-08-28 | `GRRR` | 35 | — | $15.66 | +0.00 | $14.41 | -43.75 | -43.75 | +0.00 | -43.75 |
| 2026-08-28 | `URBN` | 6 | — | $79.42 | +0.00 | $81.09 | +10.02 | +10.02 | +0.00 | +10.02 |
| 2026-08-28 | `PYXS` | 165 | — | $3.32 | +0.00 | $3.23 | -14.85 | -14.85 | +0.00 | -14.85 |
| 2026-08-31 | `MOS` | 188 | $23.60 | $23.68 | +15.04 | — | +0.00 | +15.04 | -16.92 | — |
| 2026-08-31 | `RRC` | 65 | $41.46 | $42.00 | +35.10 | — | +0.00 | +35.10 | +36.40 | — |
| 2026-08-31 | `CRK` | 47 | $14.29 | $14.54 | +11.75 | — | +0.00 | +11.75 | +5.64 | — |
| 2026-08-31 | `SLI` | 261 | $2.55 | $2.58 | +7.83 | — | +0.00 | +7.83 | -5.22 | — |
| 2026-08-31 | `SEDG` | 33 | $31.41 | $31.15 | -8.58 | — | +0.00 | -8.58 | -57.75 | — |
| 2026-08-31 | `GRRR` | 35 | $14.41 | $14.44 | +1.05 | — | +0.00 | +1.05 | -42.70 | — |
| 2026-08-31 | `URBN` | 6 | $81.09 | $80.44 | -3.90 | — | +0.00 | -3.90 | +6.12 | — |
| 2026-08-31 | `PYXS` | 165 | $3.23 | $3.20 | -4.95 | — | +0.00 | -4.95 | -19.80 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 84 | — | $52.88 | +0.00 | $52.46 | -35.28 | -35.28 | +0.00 | -35.28 |
| 2026-09-03 | `HRMY` | 22 | — | $42.93 | +0.00 | $41.86 | -23.54 | -23.54 | +0.00 | -23.54 |
| 2026-09-03 | `CABA` | 263 | — | $3.63 | +0.00 | $3.48 | -39.45 | -39.45 | +0.00 | -39.45 |
| 2026-09-03 | `VSTM` | 118 | — | $8.03 | +0.00 | $7.98 | -5.90 | -5.90 | +0.00 | -5.90 |
| 2026-09-03 | `RVTY` | 7 | — | $132.45 | +0.00 | $130.63 | -12.74 | -12.74 | +0.00 | -12.74 |
| 2026-09-03 | `CRK` | 61 | — | $15.45 | +0.00 | $14.95 | -30.50 | -30.50 | +0.00 | -30.50 |
| 2026-09-03 | `MRNA` | 6 | — | $145.94 | +0.00 | $148.87 | +17.55 | +17.55 | +0.00 | +17.55 |
| 2026-09-03 | `ARCT` | 56 | — | $16.77 | +0.00 | $15.56 | -67.76 | -67.76 | +0.00 | -67.76 |
| 2026-09-04 | `ATRC` | 84 | $52.46 | $52.03 | -36.12 | $51.52 | -42.84 | -78.96 | -71.40 | -114.24 |
| 2026-09-04 | `HRMY` | 22 | $41.86 | $41.50 | -7.92 | — | +0.00 | -7.92 | -31.46 | — |
| 2026-09-04 | `CABA` | 263 | $3.48 | $3.46 | -5.26 | $3.47 | +2.63 | -2.63 | -44.71 | -42.08 |
| 2026-09-04 | `VSTM` | 118 | $7.98 | $7.91 | -8.26 | — | +0.00 | -8.26 | -14.16 | — |
| 2026-09-04 | `RVTY` | 7 | $130.63 | $130.03 | -4.20 | — | +0.00 | -4.20 | -16.94 | — |
| 2026-09-04 | `CRK` | 61 | $14.95 | $15.00 | +3.05 | — | +0.00 | +3.05 | -27.45 | — |
| 2026-09-04 | `MRNA` | 6 | $148.87 | $153.62 | +28.50 | — | +0.00 | +28.50 | +46.05 | — |
| 2026-09-04 | `ARCT` | 56 | $15.56 | $15.61 | +2.80 | — | +0.00 | +2.80 | -64.96 | — |
| 2026-09-04 | `ALEC` | 889 | — | $2.52 | +0.00 | $2.46 | -53.34 | -53.34 | +0.00 | -53.34 |
| 2026-09-04 | `BHC` | 100 | — | $6.71 | +0.00 | $6.56 | -15.00 | -15.00 | +0.00 | -15.00 |
| 2026-09-04 | `BMEA` | 353 | — | $1.90 | +0.00 | $2.03 | +45.89 | +45.89 | +0.00 | +45.89 |
| 2026-09-04 | `OABI` | 140 | — | $4.78 | +0.00 | $4.33 | -63.00 | -63.00 | +0.00 | -63.00 |
| 2026-09-04 | `OPK` | 422 | — | $1.59 | +0.00 | $1.64 | +21.10 | +21.10 | +0.00 | +21.10 |
| 2026-09-04 | `VIR` | 57 | — | $11.31 | +0.00 | $11.38 | +4.27 | +4.27 | +0.00 | +4.27 |
| 2026-09-08 | `ATRC` | 84 | $51.52 | $54.31 | +234.36 | — | +0.00 | +234.36 | +120.12 | — |
| 2026-09-08 | `CABA` | 263 | $3.47 | $3.43 | -10.52 | — | +0.00 | -10.52 | -52.60 | — |
| 2026-09-08 | `ALEC` | 889 | $2.46 | $2.38 | -71.12 | — | +0.00 | -71.12 | -124.46 | — |
| 2026-09-08 | `BHC` | 100 | $6.56 | $6.57 | +1.00 | — | +0.00 | +1.00 | -14.00 | — |
| 2026-09-08 | `BMEA` | 353 | $2.03 | $2.00 | -10.59 | — | +0.00 | -10.59 | +35.30 | — |
| 2026-09-08 | `OABI` | 140 | $4.33 | $4.30 | -4.20 | — | +0.00 | -4.20 | -67.20 | — |
| 2026-09-08 | `OPK` | 422 | $1.64 | $1.63 | -4.22 | — | +0.00 | -4.22 | +16.88 | — |
| 2026-09-08 | `VIR` | 57 | $11.38 | $11.22 | -9.40 | — | +0.00 | -9.40 | -5.13 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +149.49 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $160.57 | $10,123.05 | BTSG×66, IREN×18, TPG×16, TGTX×17, SLS×73, HIMS×28, INO×1058, TNDM×36 |
| 2026-08-14 | +5.50 | $160.57 | BTSG×66, IREN×18, TPG×16, TGTX×17, SLS×73, HIMS×28, INO×1058, TNDM×36 | $10,109.78 | -13.27 | +89.37 | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $465.70 | $10,139.92 | TLN×11, VST×5, NRG×7, DAVE×2, SLG×14, MARA×95, LDI×922, BTBT×576 |
| 2026-08-17 | +2.25 | $465.70 | TLN×11, VST×5, NRG×7, DAVE×2, SLG×14, MARA×95, LDI×922, BTBT×576 | $10,187.76 | +47.84 | +122.15 | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | $173.44 | $10,259.27 | DVN×87, EOG×6, FANG×4, TMC×214, TGB×102, ELF×9, DNN×268, HNST×180 |
| 2026-08-18 | -6.20 | $173.44 | DVN×87, EOG×6, FANG×4, TMC×214, TGB×102, ELF×9, DNN×268, HNST×180 | $10,256.62 | -2.65 | +0.00 | — | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST | $10,237.02 | $10,237.02 | — |
| 2026-08-19 | -7.20 | $10,237.02 | — | $10,237.02 | +0.00 | +0.00 | — | — | $10,237.02 | $10,237.02 | — |
| 2026-08-20 | +1.12 | $10,237.02 | — | $10,237.02 | +0.00 | +264.60 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $95.33 | $10,479.79 | AG×199, BHP×9, CDE×42, HDSN×152, IAG×44, KGC×29, NFGC×501, WPM×6 |
| 2026-08-21 | +3.25 | $95.33 | AG×199, BHP×9, CDE×42, HDSN×152, IAG×44, KGC×29, NFGC×501, WPM×6 | $10,779.65 | +299.86 | +235.30 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $79.01 | $10,962.33 | AU×36, AUPH×53, AEM×4, ARCT×82, AUTL×373, CRDL×477, CRSP×15, CYPH×698 |
| 2026-08-24 | -5.17 | $79.01 | AU×36, AUPH×53, AEM×4, ARCT×82, AUTL×373, CRDL×477, CRSP×15, CYPH×698 | $11,207.31 | +244.98 | +0.00 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $11,176.40 | $11,176.40 | — |
| 2026-08-25 | +1.80 | $11,176.40 | — | $11,176.40 | +0.00 | +251.11 | MOS, OCUL, INSP, CRMD, RZLT, HCA, CAPR, SAFX | — | $126.90 | $11,393.79 | MOS×188, OCUL×87, INSP×15, CRMD×114, RZLT×193, HCA×2, CAPR×132, SAFX×2675 |
| 2026-08-26 | +2.02 | $126.90 | MOS×188, OCUL×87, INSP×15, CRMD×114, RZLT×193, HCA×2, CAPR×132, SAFX×2675 | $11,477.48 | +83.69 | -128.95 | AVBP, FLNC | CAPR, SAFX | $19.24 | $11,323.79 | MOS×188, OCUL×87, INSP×15, CRMD×114, RZLT×193, HCA×2, AVBP×27, FLNC×115 |
| 2026-08-27 | — | $19.24 | MOS×188, OCUL×87, INSP×15, CRMD×114, RZLT×193, HCA×2, AVBP×27, FLNC×115 | $11,339.27 | +15.48 | -1.38 | RRC, CRK, SLI, ACMR, GGB, MT | OCUL, INSP, CRMD, RZLT, HCA, AVBP, FLNC | $742.41 | $11,307.96 | MOS×188, RRC×65, CRK×47, SLI×261, ACMR×8, GGB×149, MT×9 |
| 2026-08-28 | +0.75 | $742.41 | MOS×188, RRC×65, CRK×47, SLI×261, ACMR×8, GGB×149, MT×9 | $11,353.70 | +45.74 | -231.66 | SEDG, GRRR, URBN, PYXS | ACMR, GGB, MT | $77.57 | $11,106.82 | MOS×188, RRC×65, CRK×47, SLI×261, SEDG×33, GRRR×35, URBN×6, PYXS×165 |
| 2026-08-31 | -5.85 | $77.57 | MOS×188, RRC×65, CRK×47, SLI×261, SEDG×33, GRRR×35, URBN×6, PYXS×165 | $11,160.16 | +53.34 | +0.00 | — | MOS, RRC, CRK, SLI, SEDG, GRRR, URBN, PYXS | $11,140.98 | $11,140.98 | — |
| 2026-09-01 | -6.30 | $11,140.98 | — | $11,140.98 | -0.00 | +0.00 | — | — | $11,140.98 | $11,140.98 | — |
| 2026-09-02 | -3.83 | $11,140.98 | — | $11,140.98 | -0.00 | +0.00 | — | — | $11,140.98 | $11,140.98 | — |
| 2026-09-03 | -0.90 | $11,140.98 | — | $11,140.98 | -0.00 | -197.62 | ATRC, HRMY, CABA, VSTM, RVTY, CRK, MRNA, ARCT | — | $149.59 | $10,924.97 | ATRC×84, HRMY×22, CABA×263, VSTM×118, RVTY×7, CRK×61, MRNA×6, ARCT×56 |
| 2026-09-04 | +2.25 | $149.59 | ATRC×84, HRMY×22, CABA×263, VSTM×118, RVTY×7, CRK×61, MRNA×6, ARCT×56 | $10,897.56 | -27.41 | -100.29 | ALEC, BHC, BMEA, OABI, OPK, VIR | HRMY, VSTM, RVTY, CRK, MRNA, ARCT | $9.03 | $10,756.07 | ATRC×84, CABA×263, ALEC×889, BHC×100, BMEA×353, OABI×140, OPK×422, VIR×57 |
| 2026-09-08 | -11.47 | $9.03 | ATRC×84, CABA×263, ALEC×889, BHC×100, BMEA×353, OABI×140, OPK×422, VIR×57 | $10,881.38 | +125.31 | +0.00 | — | ATRC, CABA, ALEC, BHC, BMEA, OABI, OPK, VIR | $10,846.92 | $10,846.92 | — |
| 2026-09-09 | -13.95 | $10,846.92 | — | $10,846.92 | -0.00 | +0.00 | — | — | $10,846.92 | $10,846.92 | — |
| 2026-09-10 | -13.28 | $10,846.92 | — | $10,846.92 | -0.00 | +0.00 | — | — | $10,846.92 | $10,846.92 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 66 | $59.80 | $2.19 | — | $6,051.01 | — | 40% to #1, rest split; list flatten; ⚪; ret5=-5.3; leftover $4000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 18 | $45.98 | $2.04 | — | $5,221.33 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+12.3; leftover $857.14 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 16 | $50.62 | $2.04 | — | $4,409.32 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+6.2; leftover $857.14 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 17 | $49.70 | $2.04 | — | $3,562.38 | — | 40% to #1, rest split; list flatten; ⚪; ret5=-0.8; leftover $857.14 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 73 | $11.70 | $2.21 | — | $2,706.07 | — | 40% to #1, rest split; list flatten; ⚪; ret5=-0.8; leftover $857.14 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 28 | $29.74 | $2.07 | — | $1,871.27 | — | 40% to #1, rest split; list flatten; ⚪; ret5=-5.3; leftover $857.14 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1058 | $0.81 | $11.74 | — | $1,002.55 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+13.2; leftover $857.14 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 36 | $23.33 | $2.10 | — | $160.57 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+19.7; leftover $857.14 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $160.57 | ▲ close $10,123.05 vs 09:30 $10,000.00 (session +149.49) | 16:00 close · cash $160.57 · equity $10,123.05 vs 09:30 $10,000.00 (+123.05; session marks +149.49) · 8 name(s) marked open→close (per-name table). BTSG×66 09:30 $59.80 → close $60.23 +28.38; IREN×18 09:30 $45.98 → close $44.76 -21.96; TPG×16 09:30 $50.62 → close $54.62 +63.95; TGTX×17 09:30 $49.70 → close $47.94 -29.92; SLS×73 09:30 $11.70 → close $12.36 +48.18; HIMS×28 09:30 $29.74 → close $28.77 -27.16; INO×1058 09:30 $0.81 → close $0.90 +95.22; TNDM×36 09:30 $23.33 → close $23.13 -7.20 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $160.57 | ▼ 09:30 equity $10,109.78 vs yday $10,123.05 (-13.27) | 09:30 open · cash $160.57 (unchanged overnight, no fees) · equity $10,109.78 vs prior close $10,123.05 (-13.27) · 8 name(s) re-marked at the open (per-name table). BTSG×66 yday $60.23 → 09:30 $59.65 -38.28; IREN×18 yday $44.76 → 09:30 $44.09 -12.06; TPG×16 yday $54.62 → 09:30 $55.29 +10.72; TGTX×17 yday $47.94 → 09:30 $47.27 -11.39; SLS×73 yday $12.36 → 09:30 $12.40 +2.92; HIMS×28 yday $28.77 → 09:30 $29.15 +10.64; INO×1058 yday $0.90 → 09:30 $0.93 +31.74; TNDM×36 yday $23.13 → 09:30 $22.92 -7.56 | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 66 | $59.65 | $2.23 | $-14.32 | $4,095.24 | ▼ -14.32 after sell → book $10,107.55; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 18 | $44.09 | $2.06 | $-38.13 | $4,886.80 | ▼ -38.13 after sell → book $10,105.49; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 16 | $55.29 | $2.06 | $+70.57 | $5,769.38 | ▲ +70.57 after sell → book $10,103.43; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 17 | $47.27 | $2.06 | $-45.41 | $6,570.91 | ▼ -45.41 after sell → book $10,101.37; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 73 | $12.40 | $2.23 | $+46.66 | $7,473.88 | ▲ +46.66 after sell → book $10,099.14; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 28 | $29.15 | $2.09 | $-20.69 | $8,287.98 | ▼ -20.69 after sell → book $10,097.04; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1058 | $0.93 | $13.20 | $+102.02 | $9,258.73 | ▲ +102.02 after sell → book $10,083.85; vs 09:30 mark -13.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 36 | $22.92 | $2.12 | $-18.98 | $10,081.73 | ▼ -18.98 after sell → book $10,081.73; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 11 | $359.83 | $2.02 | — | $6,121.57 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+5.9; leftover $4032.69 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 5 | $146.90 | $2.00 | — | $5,385.07 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+3.6; leftover $864.15 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 7 | $120.00 | $2.01 | — | $4,543.06 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+0.6; leftover $864.15 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 2 | $330.91 | $2.00 | — | $3,879.24 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=-8.6; leftover $864.15 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 14 | $57.61 | $2.03 | — | $3,070.67 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+5.7; leftover $864.15 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 95 | $9.01 | $2.27 | — | $2,212.45 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=-13.5; leftover $864.15 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 922 | $0.94 | $11.41 | — | $1,337.13 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+0.5; leftover $864.15 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 576 | $1.50 | $7.43 | — | $465.70 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+9.2; leftover $864.15 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $465.70 | ▲ close $10,139.92 vs 09:30 $10,109.78 (session +89.37) | 16:00 close · cash $465.70 · equity $10,139.92 vs 09:30 $10,109.78 (+30.14; session marks +89.37) · 8 name(s) marked open→close (per-name table). TLN×11 09:30 $359.83 → close $362.74 +32.01; VST×5 09:30 $146.90 → close $148.13 +6.15; NRG×7 09:30 $120.00 → close $126.24 +43.68; DAVE×2 09:30 $330.91 → close $334.57 +7.32; SLG×14 09:30 $57.61 → close $56.09 -21.28; MARA×95 09:30 $9.01 → close $9.20 +18.05; LDI×922 09:30 $0.94 → close $0.90 -36.88; BTBT×576 09:30 $1.50 → close $1.57 +40.32 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $465.70 | ▲ 09:30 equity $10,187.76 vs yday $10,139.92 (+47.84) | 09:30 open · cash $465.70 (unchanged overnight, no fees) · equity $10,187.76 vs prior close $10,139.92 (+47.84) · 8 name(s) re-marked at the open (per-name table). TLN×11 yday $362.74 → 09:30 $367.88 +56.54; VST×5 yday $148.13 → 09:30 $149.37 +6.20; NRG×7 yday $126.24 → 09:30 $127.40 +8.12; DAVE×2 yday $334.57 → 09:30 $336.94 +4.74; SLG×14 yday $56.09 → 09:30 $55.37 -10.08; MARA×95 yday $9.20 → 09:30 $9.22 +1.90; LDI×922 yday $0.90 → 09:30 $0.91 +9.22; BTBT×576 yday $1.57 → 09:30 $1.52 -28.80 | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 11 | $367.88 | $2.07 | $+84.46 | $4,510.31 | ▲ +84.46 after sell → book $10,185.69; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 5 | $149.37 | $2.02 | $+8.32 | $5,255.14 | ▲ +8.32 after sell → book $10,183.67; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 7 | $127.40 | $2.03 | $+47.76 | $6,144.90 | ▲ +47.76 after sell → book $10,181.64; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 2 | $336.94 | $2.02 | $+8.05 | $6,816.77 | ▲ +8.05 after sell → book $10,179.62; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 14 | $55.37 | $2.05 | $-35.44 | $7,589.90 | ▼ -35.44 after sell → book $10,177.57; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 95 | $9.22 | $2.30 | $+15.37 | $8,463.50 | ▲ +15.37 after sell → book $10,175.27; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 922 | $0.91 | $11.29 | $-50.36 | $9,288.46 | ▼ -50.36 after sell → book $10,163.98; vs 09:30 mark -11.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 576 | $1.52 | $7.54 | $-3.45 | $10,156.44 | ▼ -3.45 after sell → book $10,156.44; vs 09:30 mark -7.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 87 | $46.18 | $2.25 | — | $6,136.53 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+6.7; leftover $4062.58 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 6 | $142.77 | $2.01 | — | $5,277.90 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+5.8; leftover $870.55 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 4 | $202.70 | $2.00 | — | $4,465.10 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+8.3; leftover $870.55 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 214 | $4.05 | $2.76 | — | $3,595.64 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=-12.3; leftover $870.55 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 102 | $8.46 | $2.30 | — | $2,730.42 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+0.4; leftover $870.55 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 9 | $90.54 | $2.02 | — | $1,913.55 | — | 40% to #1, rest split; list flatten; ret5=-7.2; leftover $870.55 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 268 | $3.24 | $3.46 | — | $1,041.77 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+0.3; leftover $870.55 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 180 | $4.81 | $2.53 | — | $173.44 | — | 40% to #1, rest split; list flatten; ⚪; ret5=-11.4; leftover $870.55 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $173.44 | ▲ close $10,259.27 vs 09:30 $10,187.76 (session +122.15) | 16:00 close · cash $173.44 · equity $10,259.27 vs 09:30 $10,187.76 (+71.51; session marks +122.15) · 8 name(s) marked open→close (per-name table). DVN×87 09:30 $46.18 → close $47.57 +120.93; EOG×6 09:30 $142.77 → close $146.15 +20.28; FANG×4 09:30 $202.70 → close $206.29 +14.36; TMC×214 09:30 $4.05 → close $3.77 -59.92; TGB×102 09:30 $8.46 → close $8.77 +31.62; ELF×9 09:30 $90.54 → close $93.66 +28.08; DNN×268 09:30 $3.24 → close $3.19 -13.40; HNST×180 09:30 $4.81 → close $4.70 -19.80 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $173.44 | ▼ 09:30 equity $10,256.62 vs yday $10,259.27 (-2.65) | 09:30 open · cash $173.44 (unchanged overnight, no fees) · equity $10,256.62 vs prior close $10,259.27 (-2.65) · 8 name(s) re-marked at the open (per-name table). DVN×87 yday $47.57 → 09:30 $48.00 +37.41; EOG×6 yday $146.15 → 09:30 $148.04 +11.34; FANG×4 yday $206.29 → 09:30 $208.93 +10.56; TMC×214 yday $3.77 → 09:30 $3.72 -10.70; TGB×102 yday $8.77 → 09:30 $8.55 -22.44; ELF×9 yday $93.66 → 09:30 $93.44 -1.98; DNN×268 yday $3.19 → 09:30 $3.11 -21.44; HNST×180 yday $4.70 → 09:30 $4.67 -5.40 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 87 | $48.00 | $2.30 | $+153.79 | $4,347.14 | ▲ +153.79 after sell → book $10,254.32; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 6 | $148.04 | $2.03 | $+27.58 | $5,233.35 | ▲ +27.58 after sell → book $10,252.29; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 4 | $208.93 | $2.02 | $+20.90 | $6,067.05 | ▲ +20.90 after sell → book $10,250.27; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 214 | $3.72 | $2.81 | $-76.19 | $6,860.33 | ▼ -76.19 after sell → book $10,247.47; vs 09:30 mark -2.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 102 | $8.55 | $2.32 | $+4.56 | $7,730.10 | ▲ +4.56 after sell → book $10,245.14; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 9 | $93.44 | $2.04 | $+22.05 | $8,569.03 | ▲ +22.05 after sell → book $10,243.11; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 268 | $3.11 | $3.51 | $-41.81 | $9,398.99 | ▼ -41.81 after sell → book $10,239.59; vs 09:30 mark -3.52 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `HNST` | 180 | $4.67 | $2.57 | $-30.30 | $10,237.02 | ▼ -30.30 after sell → book $10,237.02; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,237.02 | ▲ close $10,237.02 vs 09:30 $10,256.62 (session +0.00) | 16:00 close · cash $10,237.02 · no lots left · equity $10,237.02. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,237.02 | ▲ 09:30 equity $10,237.02 vs yday $10,237.02 (+0.00) | 09:30 open · cash $10,237.02 · no holdings · equity $10,237.02 vs prior close $10,237.02 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,237.02 | ▲ close $10,237.02 vs 09:30 $10,237.02 (session +0.00) | 16:00 close · cash $10,237.02 · no lots left · equity $10,237.02. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,237.02 | ▲ 09:30 equity $10,237.02 vs yday $10,237.02 (+0.00) | 09:30 open · cash $10,237.02 · no holdings · equity $10,237.02 vs prior close $10,237.02 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 199 | $20.55 | $2.59 | — | $6,144.99 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $4094.81 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 9 | $91.01 | $2.02 | — | $5,323.88 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $877.46 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 42 | $20.65 | $2.12 | — | $4,454.46 | — | 40% to #1, rest split; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $877.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 152 | $5.77 | $2.45 | — | $3,574.98 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $877.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 44 | $19.63 | $2.12 | — | $2,709.14 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $877.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 29 | $29.63 | $2.08 | — | $1,847.79 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $877.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 501 | $1.75 | $6.46 | — | $964.58 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $877.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 6 | $144.54 | $2.01 | — | $95.33 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $877.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $95.33 | ▲ close $10,479.79 vs 09:30 $10,237.02 (session +264.60) | 16:00 close · cash $95.33 · equity $10,479.79 vs 09:30 $10,237.02 (+242.77; session marks +264.60) · 8 name(s) marked open→close (per-name table). AG×199 09:30 $20.55 → close $21.19 +127.36; BHP×9 09:30 $91.01 → close $93.63 +23.58; CDE×42 09:30 $20.65 → close $21.11 +19.32; HDSN×152 09:30 $5.77 → close $5.57 -30.40; IAG×44 09:30 $19.63 → close $20.50 +38.28; KGC×29 09:30 $29.63 → close $31.43 +52.20; NFGC×501 09:30 $1.75 → close $1.75 +0.00; WPM×6 09:30 $144.54 → close $150.25 +34.26 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.33 | ▲ 09:30 equity $10,779.65 vs yday $10,479.79 (+299.86) | 09:30 open · cash $95.33 (unchanged overnight, no fees) · equity $10,779.65 vs prior close $10,479.79 (+299.86) · 8 name(s) re-marked at the open (per-name table). AG×199 yday $21.19 → 09:30 $21.90 +141.29; BHP×9 yday $93.63 → 09:30 $95.72 +18.81; CDE×42 yday $21.11 → 09:30 $21.75 +26.88; HDSN×152 yday $5.57 → 09:30 $5.67 +15.20; IAG×44 yday $20.50 → 09:30 $21.17 +29.48; KGC×29 yday $31.43 → 09:30 $32.17 +21.46; NFGC×501 yday $1.75 → 09:30 $1.79 +20.04; WPM×6 yday $150.25 → 09:30 $154.70 +26.70 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 199 | $21.90 | $2.65 | $+263.41 | $4,450.77 | ▲ +263.41 after sell → book $10,776.99; vs 09:30 mark -2.66 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 9 | $95.72 | $2.04 | $+38.34 | $5,310.22 | ▲ +38.34 after sell → book $10,774.96; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 42 | $21.75 | $2.14 | $+41.95 | $6,221.58 | ▲ +41.95 after sell → book $10,772.82; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 152 | $5.67 | $2.48 | $-20.13 | $7,080.94 | ▼ -20.13 after sell → book $10,770.34; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 44 | $21.17 | $2.14 | $+63.50 | $8,010.28 | ▲ +63.50 after sell → book $10,768.20; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 29 | $32.17 | $2.10 | $+69.49 | $8,941.11 | ▲ +69.49 after sell → book $10,766.10; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 501 | $1.79 | $6.56 | $+7.02 | $9,831.34 | ▲ +7.02 after sell → book $10,759.54; vs 09:30 mark -6.56 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 6 | $154.70 | $2.03 | $+56.92 | $10,757.52 | ▲ +56.92 after sell → book $10,757.52; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 36 | $119.43 | $2.10 | — | $6,455.94 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $4303.01 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 53 | $17.20 | $2.15 | — | $5,542.19 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $922.07 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 4 | $216.30 | $2.00 | — | $4,674.99 | — | 40% to #1, rest split; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $922.07 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 82 | $11.13 | $2.24 | — | $3,760.09 | — | 40% to #1, rest split; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $922.07 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 373 | $2.47 | $4.81 | — | $2,833.97 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $922.07 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 477 | $1.93 | $6.15 | — | $1,907.21 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $922.07 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 15 | $59.72 | $2.04 | — | $1,009.37 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $922.07 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 698 | $1.32 | $9.00 | — | $79.01 | — | 40% to #1, rest split; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $922.07 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $79.01 | ▲ close $10,962.33 vs 09:30 $10,779.65 (session +235.30) | 16:00 close · cash $79.01 · equity $10,962.33 vs 09:30 $10,779.65 (+182.68; session marks +235.30) · 8 name(s) marked open→close (per-name table). AU×36 09:30 $119.43 → close $121.22 +64.44; AUPH×53 09:30 $17.20 → close $16.65 -29.15; AEM×4 09:30 $216.30 → close $216.06 -0.96; ARCT×82 09:30 $11.13 → close $13.45 +190.24; AUTL×373 09:30 $2.47 → close $2.41 -22.38; CRDL×477 09:30 $1.93 → close $1.86 -33.39; CRSP×15 09:30 $59.72 → close $59.50 -3.30; CYPH×698 09:30 $1.32 → close $1.42 +69.80 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.01 | ▲ 09:30 equity $11,207.31 vs yday $10,962.33 (+244.98) | 09:30 open · cash $79.01 (unchanged overnight, no fees) · equity $11,207.31 vs prior close $10,962.33 (+244.98) · 8 name(s) re-marked at the open (per-name table). AU×36 yday $121.22 → 09:30 $120.51 -25.56; AUPH×53 yday $16.65 → 09:30 $16.57 -4.24; AEM×4 yday $216.06 → 09:30 $217.03 +3.88; ARCT×82 yday $13.45 → 09:30 $13.33 -9.84; AUTL×373 yday $2.41 → 09:30 $2.40 -3.73; CRDL×477 yday $1.86 → 09:30 $1.88 +9.54; CRSP×15 yday $59.50 → 09:30 $58.75 -11.25; CYPH×698 yday $1.42 → 09:30 $1.83 +286.18 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 36 | $120.51 | $2.14 | $+34.64 | $4,415.22 | ▲ +34.64 after sell → book $11,205.16; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 53 | $16.57 | $2.17 | $-37.71 | $5,291.26 | ▼ -37.71 after sell → book $11,202.99; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 4 | $217.03 | $2.02 | $-1.10 | $6,157.36 | ▼ -1.10 after sell → book $11,200.97; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 82 | $13.33 | $2.26 | $+175.90 | $7,248.16 | ▲ +175.90 after sell → book $11,198.71; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 373 | $2.40 | $4.88 | $-35.81 | $8,138.48 | ▼ -35.81 after sell → book $11,193.83; vs 09:30 mark -4.88 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 477 | $1.88 | $6.24 | $-36.25 | $9,029.00 | ▼ -36.25 after sell → book $11,187.59; vs 09:30 mark -6.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 15 | $58.75 | $2.06 | $-18.64 | $9,908.19 | ▼ -18.64 after sell → book $11,185.53; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 698 | $1.83 | $9.13 | $+337.85 | $11,176.40 | ▲ +337.85 after sell → book $11,176.40; vs 09:30 mark -9.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,176.40 | ▲ close $11,176.40 vs 09:30 $11,207.31 (session +0.00) | 16:00 close · cash $11,176.40 · no lots left · equity $11,176.40. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,176.40 | ▲ 09:30 equity $11,176.40 vs yday $11,176.40 (+0.00) | 09:30 open · cash $11,176.40 · no holdings · equity $11,176.40 vs prior close $11,176.40 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 188 | $23.77 | $2.55 | — | $6,705.09 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+13.0; leftover $4470.56 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 87 | $10.98 | $2.25 | — | $5,747.58 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+1.2; leftover $957.98 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 15 | $61.19 | $2.04 | — | $4,827.69 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+7.4; leftover $957.98 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 114 | $8.35 | $2.33 | — | $3,873.46 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+8.0; leftover $957.98 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 193 | $4.94 | $2.57 | — | $2,917.47 | — | 40% to #1, rest split; list flatten; ret5=+7.1; leftover $957.98 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 2 | $426.97 | $2.00 | — | $2,061.53 | — | 40% to #1, rest split; list flatten; ret5=+6.0; leftover $957.98 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 132 | $7.25 | $2.39 | — | $1,102.15 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $957.98 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 2675 | $0.36 | $17.60 | — | $126.90 | — | 40% to #1, rest split; list probable,yday_gainer; ret5=-15.6; leftover $957.98 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $126.90 | ▲ close $11,393.79 vs 09:30 $11,176.40 (session +251.11) | 16:00 close · cash $126.90 · equity $11,393.79 vs 09:30 $11,176.40 (+217.39; session marks +251.11) · 8 name(s) marked open→close (per-name table). MOS×188 09:30 $23.77 → close $24.27 +94.00; OCUL×87 09:30 $10.98 → close $10.88 -8.70; INSP×15 09:30 $61.19 → close $61.07 -1.80; CRMD×114 09:30 $8.35 → close $8.56 +23.94; RZLT×193 09:30 $4.94 → close $5.01 +13.51; HCA×2 09:30 $426.97 → close $428.76 +3.58; CAPR×132 09:30 $7.25 → close $8.29 +137.28; SAFX×2675 09:30 $0.36 → close $0.35 -10.70 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $126.90 | ▲ 09:30 equity $11,477.48 vs yday $11,393.79 (+83.69) | 09:30 open · cash $126.90 (unchanged overnight, no fees) · equity $11,477.48 vs prior close $11,393.79 (+83.69) · 8 name(s) re-marked at the open (per-name table). MOS×188 yday $24.27 → 09:30 $24.84 +107.16; OCUL×87 yday $10.88 → 09:30 $10.79 -7.83; INSP×15 yday $61.07 → 09:30 $60.07 -15.00; CRMD×114 yday $8.56 → 09:30 $8.60 +4.56; RZLT×193 yday $5.01 → 09:30 $5.01 +0.00; HCA×2 yday $428.76 → 09:30 $427.50 -2.52; CAPR×132 yday $8.29 → 09:30 $8.29 +0.00; SAFX×2675 yday $0.35 → 09:30 $0.35 -2.68 | — |
| 2026-08-26 09:30 ET | **SELL** | `CAPR` | 132 | $8.29 | $2.42 | $+132.48 | $1,218.76 | ▲ +132.48 after sell → book $11,475.06; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `SAFX` | 2675 | $0.35 | $17.92 | $-48.90 | $2,145.11 | ▼ -48.90 after sell → book $11,457.14; vs 09:30 mark -17.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 27 | $31.21 | $2.07 | — | $1,300.37 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $858.05 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 115 | $11.12 | $2.33 | — | $19.24 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $1287.07 | join🔴 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.24 | ▼ close $11,323.79 vs 09:30 $11,477.48 (session -128.95) | 16:00 close · cash $19.24 · equity $11,323.79 vs 09:30 $11,477.48 (-153.69; session marks -128.95) · 8 name(s) marked open→close (per-name table). MOS×188 09:30 $24.84 → close $24.16 -127.84; OCUL×87 09:30 $10.79 → close $10.77 -1.74; INSP×15 09:30 $60.07 → close $61.80 +25.95; CRMD×114 09:30 $8.60 → close $8.39 -23.94; RZLT×193 09:30 $5.01 → close $5.04 +5.79; HCA×2 09:30 $427.50 → close $427.16 -0.68; AVBP×27 09:30 $31.21 → close $31.14 -1.89; FLNC×115 09:30 $11.12 → close $11.08 -4.60 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.24 | ▲ 09:30 equity $11,339.27 vs yday $11,323.79 (+15.48) | 09:30 open · cash $19.24 (unchanged overnight, no fees) · equity $11,339.27 vs prior close $11,323.79 (+15.48) · 8 name(s) re-marked at the open (per-name table). MOS×188 yday $24.16 → 09:30 $24.00 -30.08; OCUL×87 yday $10.77 → 09:30 $10.63 -12.18; INSP×15 yday $61.80 → 09:30 $62.10 +4.50; CRMD×114 yday $8.39 → 09:30 $8.49 +11.40; RZLT×193 yday $5.04 → 09:30 $5.07 +5.79; HCA×2 yday $427.16 → 09:30 $424.61 -5.10; AVBP×27 yday $31.14 → 09:30 $30.79 -9.45; FLNC×115 yday $11.08 → 09:30 $11.52 +50.60 | — |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 87 | $10.63 | $2.28 | $-34.98 | $941.77 | ▼ -34.98 after sell → book $11,336.99; vs 09:30 mark -2.28 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 15 | $62.10 | $2.06 | $+9.56 | $1,871.22 | ▲ +9.56 after sell → book $11,334.94; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 114 | $8.49 | $2.36 | $+11.27 | $2,836.72 | ▲ +11.27 after sell → book $11,332.58; vs 09:30 mark -2.36 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 193 | $5.07 | $2.61 | $+19.91 | $3,812.61 | ▲ +19.91 after sell → book $11,329.96; vs 09:30 mark -2.62 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 2 | $424.61 | $2.02 | $-8.73 | $4,659.82 | ▼ -8.73 after sell → book $11,327.95; vs 09:30 mark -2.01 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AVBP` | 27 | $30.79 | $2.09 | $-15.50 | $5,489.06 | ▼ -15.50 after sell → book $11,325.86; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FLNC` | 115 | $11.52 | $2.36 | $+41.30 | $6,811.49 | ▲ +41.30 after sell → book $11,323.49; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 65 | $41.44 | $2.19 | — | $4,115.71 | — | 40% to #1, rest split; list flatten; ret5=+3.1; leftover $2724.60 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 47 | $14.42 | $2.13 | — | $3,435.84 | — | 40% to #1, rest split; list flatten; ret5=+7.1; leftover $681.15 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 261 | $2.60 | $3.37 | — | $2,753.87 | — | 40% to #1, rest split; list flatten; ret5=+13.0; leftover $681.15 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 8 | $81.65 | $2.01 | — | $2,098.66 | — | 40% to #1, rest split; list mover_buy; 🔵; ret5=+2.0; leftover $681.15 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 149 | $4.57 | $2.44 | — | $1,415.29 | — | 40% to #1, rest split; list mover_buy; 🔵; ret5=+1.1; leftover $681.15 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 9 | $74.54 | $2.02 | — | $742.41 | — | 40% to #1, rest split; list mover_buy; 🔵; ret5=-0.1; leftover $681.15 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $742.41 | ▼ close $11,307.96 vs 09:30 $11,339.27 (session -1.38) | 16:00 close · cash $742.41 · equity $11,307.96 vs 09:30 $11,339.27 (-31.31; session marks -1.38) · 7 name(s) marked open→close (per-name table). MOS×188 09:30 $24.00 → close $23.76 -45.12; RRC×65 09:30 $41.44 → close $41.64 +13.00; CRK×47 09:30 $14.42 → close $14.62 +9.40; SLI×261 09:30 $2.60 → close $2.64 +10.44; ACMR×8 09:30 $81.65 → close $80.49 -9.28; GGB×149 09:30 $4.57 → close $4.70 +19.37; MT×9 09:30 $74.54 → close $74.63 +0.81 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $742.41 | ▲ 09:30 equity $11,353.70 vs yday $11,307.96 (+45.74) | 09:30 open · cash $742.41 (unchanged overnight, no fees) · equity $11,353.70 vs prior close $11,307.96 (+45.74) · 7 name(s) re-marked at the open (per-name table). MOS×188 yday $23.76 → 09:30 $23.95 +35.72; RRC×65 yday $41.64 → 09:30 $41.74 +6.50; CRK×47 yday $14.62 → 09:30 $14.63 +0.47; SLI×261 yday $2.64 → 09:30 $2.68 +10.44; ACMR×8 yday $80.49 → 09:30 $79.27 -9.76; GGB×149 yday $4.70 → 09:30 $4.67 -4.47; MT×9 yday $74.63 → 09:30 $75.39 +6.84 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 8 | $79.27 | $2.03 | $-23.09 | $1,374.54 | ▼ -23.09 after sell → book $11,351.67; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 149 | $4.67 | $2.47 | $+9.99 | $2,067.90 | ▲ +9.99 after sell → book $11,349.20; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 9 | $75.39 | $2.04 | $+3.60 | $2,744.37 | ▲ +3.60 after sell → book $11,347.16; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 33 | $32.90 | $2.09 | — | $1,656.58 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1097.75 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 35 | $15.66 | $2.10 | — | $1,106.39 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $548.87 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 6 | $79.42 | $2.01 | — | $627.86 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $548.87 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `PYXS` | 165 | $3.32 | $2.48 | — | $77.57 | — | 40% to #1, rest split; list probable,yday_gainer; ret5=+6.4; leftover $548.87 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $77.57 | ▼ close $11,106.82 vs 09:30 $11,353.70 (session -231.66) | 16:00 close · cash $77.57 · equity $11,106.82 vs 09:30 $11,353.70 (-246.88; session marks -231.66) · 8 name(s) marked open→close (per-name table). MOS×188 09:30 $23.95 → close $23.60 -65.80; RRC×65 09:30 $41.74 → close $41.46 -18.20; CRK×47 09:30 $14.63 → close $14.29 -15.98; SLI×261 09:30 $2.68 → close $2.55 -33.93; SEDG×33 09:30 $32.90 → close $31.41 -49.17; GRRR×35 09:30 $15.66 → close $14.41 -43.75; URBN×6 09:30 $79.42 → close $81.09 +10.02; PYXS×165 09:30 $3.32 → close $3.23 -14.85 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $77.57 | ▲ 09:30 equity $11,160.16 vs yday $11,106.82 (+53.34) | 09:30 open · cash $77.57 (unchanged overnight, no fees) · equity $11,160.16 vs prior close $11,106.82 (+53.34) · 8 name(s) re-marked at the open (per-name table). MOS×188 yday $23.60 → 09:30 $23.68 +15.04; RRC×65 yday $41.46 → 09:30 $42.00 +35.10; CRK×47 yday $14.29 → 09:30 $14.54 +11.75; SLI×261 yday $2.55 → 09:30 $2.58 +7.83; SEDG×33 yday $31.41 → 09:30 $31.15 -8.58; GRRR×35 yday $14.41 → 09:30 $14.44 +1.05; URBN×6 yday $81.09 → 09:30 $80.44 -3.90; PYXS×165 yday $3.23 → 09:30 $3.20 -4.95 | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 188 | $23.68 | $2.62 | $-22.09 | $4,526.79 | ▼ -22.09 after sell → book $11,157.54; vs 09:30 mark -2.62 | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 65 | $42.00 | $2.22 | $+32.00 | $7,254.57 | ▲ +32.00 after sell → book $11,155.32; vs 09:30 mark -2.22 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 47 | $14.54 | $2.15 | $+1.36 | $7,935.80 | ▲ +1.36 after sell → book $11,153.17; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 261 | $2.58 | $3.42 | $-12.01 | $8,605.76 | ▼ -12.01 after sell → book $11,149.75; vs 09:30 mark -3.42 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 33 | $31.15 | $2.11 | $-61.95 | $9,631.60 | ▼ -61.95 after sell → book $11,147.64; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 35 | $14.44 | $2.12 | $-46.91 | $10,134.89 | ▼ -46.91 after sell → book $11,145.53; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 6 | $80.44 | $2.03 | $+2.08 | $10,615.50 | ▲ +2.08 after sell → book $11,143.50; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PYXS` | 165 | $3.20 | $2.52 | $-24.81 | $11,140.98 | ▼ -24.81 after sell → book $11,140.98; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,140.98 | ▲ close $11,140.98 vs 09:30 $11,160.16 (session +0.00) | 16:00 close · cash $11,140.98 · no lots left · equity $11,140.98. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,140.98 | ▲ 09:30 equity $11,140.98 vs yday $11,140.98 (-0.00) | 09:30 open · cash $11,140.98 · no holdings · equity $11,140.98 vs prior close $11,140.98 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,140.98 | ▲ close $11,140.98 vs 09:30 $11,140.98 (session +0.00) | 16:00 close · cash $11,140.98 · no lots left · equity $11,140.98. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,140.98 | ▲ 09:30 equity $11,140.98 vs yday $11,140.98 (-0.00) | 09:30 open · cash $11,140.98 · no holdings · equity $11,140.98 vs prior close $11,140.98 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,140.98 | ▲ close $11,140.98 vs 09:30 $11,140.98 (session +0.00) | 16:00 close · cash $11,140.98 · no lots left · equity $11,140.98. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,140.98 | ▲ 09:30 equity $11,140.98 vs yday $11,140.98 (-0.00) | 09:30 open · cash $11,140.98 · no holdings · equity $11,140.98 vs prior close $11,140.98 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 84 | $52.88 | $2.24 | — | $6,696.82 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+9.2; leftover $4456.39 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 22 | $42.93 | $2.06 | — | $5,750.30 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $954.94 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 263 | $3.63 | $3.39 | — | $4,792.22 | — | 40% to #1, rest split; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $954.94 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 118 | $8.03 | $2.34 | — | $3,842.33 | — | 40% to #1, rest split; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $954.94 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 7 | $132.45 | $2.01 | — | $2,913.17 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $954.94 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 61 | $15.45 | $2.17 | — | $1,968.55 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $954.94 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 6 | $145.94 | $2.01 | — | $1,090.87 | — | 40% to #1, rest split; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $954.94 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 56 | $16.77 | $2.16 | — | $149.59 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $954.94 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $149.59 | ▼ close $10,924.97 vs 09:30 $11,140.98 (session -197.62) | 16:00 close · cash $149.59 · equity $10,924.97 vs 09:30 $11,140.98 (-216.01; session marks -197.62) · 8 name(s) marked open→close (per-name table). ATRC×84 09:30 $52.88 → close $52.46 -35.28; HRMY×22 09:30 $42.93 → close $41.86 -23.54; CABA×263 09:30 $3.63 → close $3.48 -39.45; VSTM×118 09:30 $8.03 → close $7.98 -5.90; RVTY×7 09:30 $132.45 → close $130.63 -12.74; CRK×61 09:30 $15.45 → close $14.95 -30.50; MRNA×6 09:30 $145.94 → close $148.87 +17.55; ARCT×56 09:30 $16.77 → close $15.56 -67.76 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $149.59 | ▼ 09:30 equity $10,897.56 vs yday $10,924.97 (-27.41) | 09:30 open · cash $149.59 (unchanged overnight, no fees) · equity $10,897.56 vs prior close $10,924.97 (-27.41) · 8 name(s) re-marked at the open (per-name table). ATRC×84 yday $52.46 → 09:30 $52.03 -36.12; HRMY×22 yday $41.86 → 09:30 $41.50 -7.92; CABA×263 yday $3.48 → 09:30 $3.46 -5.26; VSTM×118 yday $7.98 → 09:30 $7.91 -8.26; RVTY×7 yday $130.63 → 09:30 $130.03 -4.20; CRK×61 yday $14.95 → 09:30 $15.00 +3.05; MRNA×6 yday $148.87 → 09:30 $153.62 +28.50; ARCT×56 yday $15.56 → 09:30 $15.61 +2.80 | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 22 | $41.50 | $2.08 | $-35.59 | $1,060.52 | ▼ -35.59 after sell → book $10,895.49; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 118 | $7.91 | $2.37 | $-18.88 | $1,991.52 | ▼ -18.88 after sell → book $10,893.11; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 7 | $130.03 | $2.03 | $-20.98 | $2,899.70 | ▼ -20.98 after sell → book $10,891.08; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 61 | $15.00 | $2.19 | $-31.82 | $3,812.51 | ▼ -31.82 after sell → book $10,888.89; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 6 | $153.62 | $2.03 | $+42.01 | $4,732.20 | ▲ +42.01 after sell → book $10,886.86; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 56 | $15.61 | $2.18 | $-69.30 | $5,604.18 | ▼ -69.30 after sell → book $10,884.68; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 889 | $2.52 | $11.47 | — | $3,352.44 | — | 40% to #1, rest split; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $2241.67 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 100 | $6.71 | $2.29 | — | $2,679.15 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $672.50 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 353 | $1.90 | $4.55 | — | $2,003.89 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $672.50 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 140 | $4.78 | $2.41 | — | $1,332.28 | — | 40% to #1, rest split; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $672.50 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 422 | $1.59 | $5.44 | — | $655.86 | — | 40% to #1, rest split; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $672.50 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 57 | $11.31 | $2.16 | — | $9.03 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $672.50 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.03 | ▼ close $10,756.07 vs 09:30 $10,897.56 (session -100.29) | 16:00 close · cash $9.03 · equity $10,756.07 vs 09:30 $10,897.56 (-141.49; session marks -100.29) · 8 name(s) marked open→close (per-name table). ATRC×84 09:30 $52.03 → close $51.52 -42.84; CABA×263 09:30 $3.46 → close $3.47 +2.63; ALEC×889 09:30 $2.52 → close $2.46 -53.34; BHC×100 09:30 $6.71 → close $6.56 -15.00; BMEA×353 09:30 $1.90 → close $2.03 +45.89; OABI×140 09:30 $4.78 → close $4.33 -63.00; OPK×422 09:30 $1.59 → close $1.64 +21.10; VIR×57 09:30 $11.31 → close $11.38 +4.27 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.03 | ▲ 09:30 equity $10,881.38 vs yday $10,756.07 (+125.31) | 09:30 open · cash $9.03 (unchanged overnight, no fees) · equity $10,881.38 vs prior close $10,756.07 (+125.31) · 8 name(s) re-marked at the open (per-name table). ATRC×84 yday $51.52 → 09:30 $54.31 +234.36; CABA×263 yday $3.47 → 09:30 $3.43 -10.52; ALEC×889 yday $2.46 → 09:30 $2.38 -71.12; BHC×100 yday $6.56 → 09:30 $6.57 +1.00; BMEA×353 yday $2.03 → 09:30 $2.00 -10.59; OABI×140 yday $4.33 → 09:30 $4.30 -4.20; OPK×422 yday $1.64 → 09:30 $1.63 -4.22; VIR×57 yday $11.38 → 09:30 $11.22 -9.40 | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 84 | $54.31 | $2.29 | $+115.59 | $4,568.77 | ▲ +115.59 after sell → book $10,879.08; vs 09:30 mark -2.30 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 263 | $3.43 | $3.45 | $-59.44 | $5,467.42 | ▼ -59.44 after sell → book $10,875.64; vs 09:30 mark -3.44 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 889 | $2.38 | $11.63 | $-147.56 | $7,571.61 | ▼ -147.56 after sell → book $10,864.01; vs 09:30 mark -11.63 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 100 | $6.57 | $2.32 | $-18.61 | $8,226.29 | ▼ -18.61 after sell → book $10,861.69; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 353 | $2.00 | $4.62 | $+26.12 | $8,927.67 | ▲ +26.12 after sell → book $10,857.07; vs 09:30 mark -4.62 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 140 | $4.30 | $2.44 | $-72.05 | $9,527.22 | ▼ -72.05 after sell → book $10,854.62; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 422 | $1.63 | $5.52 | $+5.91 | $10,209.56 | ▲ +5.91 after sell → book $10,849.10; vs 09:30 mark -5.52 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 57 | $11.22 | $2.18 | $-9.47 | $10,846.92 | ▼ -9.47 after sell → book $10,846.92; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,846.92 | ▲ close $10,846.92 vs 09:30 $10,881.38 (session +0.00) | 16:00 close · cash $10,846.92 · no lots left · equity $10,846.92. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,846.92 | ▲ 09:30 equity $10,846.92 vs yday $10,846.92 (-0.00) | 09:30 open · cash $10,846.92 · no holdings · equity $10,846.92 vs prior close $10,846.92 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,846.92 | ▲ close $10,846.92 vs 09:30 $10,846.92 (session +0.00) | 16:00 close · cash $10,846.92 · no lots left · equity $10,846.92. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,846.92 | ▲ 09:30 equity $10,846.92 vs yday $10,846.92 (-0.00) | 09:30 open · cash $10,846.92 · no holdings · equity $10,846.92 vs prior close $10,846.92 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,846.92 | ▲ close $10,846.92 vs 09:30 $10,846.92 (session +0.00) | 16:00 close · cash $10,846.92 · no lots left · equity $10,846.92. | — |

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
| 2026-08-27 | `MU` | cash | leftover split 681.15 < 1 share @ 967.01 |
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
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OIS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
