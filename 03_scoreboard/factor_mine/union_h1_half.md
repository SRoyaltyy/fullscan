# Factor mine action — `union_h1_half`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `half` · sell `list` · S-boost `none` · deploy half leftover

Cash book **+1.16%** ($10,115) · signal-only (no cash/fees) was +12.08%. Starts YES **6/18**. Fills 148 · skips 57 · realized $+115.50.

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
- Only spend half of leftover cash; the rest stays cash.
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
- **Size** `half` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,115.48.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `BTSG` | 10 | — | $59.80 | +0.00 | $60.23 | +4.30 | +4.30 | +0.00 | +4.30 |
| 2026-08-13 | `IREN` | 13 | — | $45.98 | +0.00 | $44.76 | -15.86 | -15.86 | +0.00 | -15.86 |
| 2026-08-13 | `TPG` | 12 | — | $50.62 | +0.00 | $54.62 | +47.96 | +47.96 | +0.00 | +47.96 |
| 2026-08-13 | `TGTX` | 12 | — | $49.70 | +0.00 | $47.94 | -21.12 | -21.12 | +0.00 | -21.12 |
| 2026-08-13 | `SLS` | 53 | — | $11.70 | +0.00 | $12.36 | +34.98 | +34.98 | +0.00 | +34.98 |
| 2026-08-13 | `HIMS` | 21 | — | $29.74 | +0.00 | $28.77 | -20.37 | -20.37 | +0.00 | -20.37 |
| 2026-08-13 | `INO` | 771 | — | $0.81 | +0.00 | $0.90 | +69.39 | +69.39 | +0.00 | +69.39 |
| 2026-08-13 | `TNDM` | 26 | — | $23.33 | +0.00 | $23.13 | -5.20 | -5.20 | +0.00 | -5.20 |
| 2026-08-14 | `BTSG` | 10 | $60.23 | $59.65 | -5.80 | — | +0.00 | -5.80 | -1.50 | — |
| 2026-08-14 | `IREN` | 13 | $44.76 | $44.09 | -8.71 | — | +0.00 | -8.71 | -24.57 | — |
| 2026-08-14 | `TPG` | 12 | $54.62 | $55.29 | +8.04 | — | +0.00 | +8.04 | +56.00 | — |
| 2026-08-14 | `TGTX` | 12 | $47.94 | $47.27 | -8.04 | — | +0.00 | -8.04 | -29.16 | — |
| 2026-08-14 | `SLS` | 53 | $12.36 | $12.40 | +2.12 | — | +0.00 | +2.12 | +37.10 | — |
| 2026-08-14 | `HIMS` | 21 | $28.77 | $29.15 | +7.98 | — | +0.00 | +7.98 | -12.39 | — |
| 2026-08-14 | `INO` | 771 | $0.90 | $0.93 | +23.13 | — | +0.00 | +23.13 | +92.52 | — |
| 2026-08-14 | `TNDM` | 26 | $23.13 | $22.92 | -5.46 | — | +0.00 | -5.46 | -10.66 | — |
| 2026-08-14 | `TLN` | 1 | — | $359.83 | +0.00 | $362.74 | +2.91 | +2.91 | +0.00 | +2.91 |
| 2026-08-14 | `VST` | 4 | — | $146.90 | +0.00 | $148.13 | +4.92 | +4.92 | +0.00 | +4.92 |
| 2026-08-14 | `NRG` | 5 | — | $120.00 | +0.00 | $126.24 | +31.20 | +31.20 | +0.00 | +31.20 |
| 2026-08-14 | `DAVE` | 1 | — | $330.91 | +0.00 | $334.57 | +3.66 | +3.66 | +0.00 | +3.66 |
| 2026-08-14 | `SLG` | 10 | — | $57.61 | +0.00 | $56.09 | -15.20 | -15.20 | +0.00 | -15.20 |
| 2026-08-14 | `MARA` | 69 | — | $9.01 | +0.00 | $9.20 | +13.11 | +13.11 | +0.00 | +13.11 |
| 2026-08-14 | `LDI` | 671 | — | $0.94 | +0.00 | $0.90 | -26.84 | -26.84 | +0.00 | -26.84 |
| 2026-08-14 | `BTBT` | 419 | — | $1.50 | +0.00 | $1.57 | +29.33 | +29.33 | +0.00 | +29.33 |
| 2026-08-17 | `TLN` | 1 | $362.74 | $367.88 | +5.14 | — | +0.00 | +5.14 | +8.05 | — |
| 2026-08-17 | `VST` | 4 | $148.13 | $149.37 | +4.96 | — | +0.00 | +4.96 | +9.88 | — |
| 2026-08-17 | `NRG` | 5 | $126.24 | $127.40 | +5.80 | — | +0.00 | +5.80 | +37.00 | — |
| 2026-08-17 | `DAVE` | 1 | $334.57 | $336.94 | +2.37 | — | +0.00 | +2.37 | +6.03 | — |
| 2026-08-17 | `SLG` | 10 | $56.09 | $55.37 | -7.20 | — | +0.00 | -7.20 | -22.40 | — |
| 2026-08-17 | `MARA` | 69 | $9.20 | $9.22 | +1.38 | — | +0.00 | +1.38 | +14.49 | — |
| 2026-08-17 | `LDI` | 671 | $0.90 | $0.91 | +6.71 | — | +0.00 | +6.71 | -20.13 | — |
| 2026-08-17 | `BTBT` | 419 | $1.57 | $1.52 | -20.95 | — | +0.00 | -20.95 | +8.38 | — |
| 2026-08-17 | `DVN` | 13 | — | $46.18 | +0.00 | $47.57 | +18.07 | +18.07 | +0.00 | +18.07 |
| 2026-08-17 | `EOG` | 4 | — | $142.77 | +0.00 | $146.15 | +13.52 | +13.52 | +0.00 | +13.52 |
| 2026-08-17 | `FANG` | 3 | — | $202.70 | +0.00 | $206.29 | +10.77 | +10.77 | +0.00 | +10.77 |
| 2026-08-17 | `TMC` | 155 | — | $4.05 | +0.00 | $3.77 | -43.40 | -43.40 | +0.00 | -43.40 |
| 2026-08-17 | `TGB` | 74 | — | $8.46 | +0.00 | $8.77 | +22.94 | +22.94 | +0.00 | +22.94 |
| 2026-08-17 | `ELF` | 6 | — | $90.54 | +0.00 | $93.66 | +18.72 | +18.72 | +0.00 | +18.72 |
| 2026-08-17 | `DNN` | 193 | — | $3.24 | +0.00 | $3.19 | -9.65 | -9.65 | +0.00 | -9.65 |
| 2026-08-17 | `HNST` | 130 | — | $4.81 | +0.00 | $4.70 | -14.30 | -14.30 | +0.00 | -14.30 |
| 2026-08-18 | `DVN` | 13 | $47.57 | $48.00 | +5.59 | — | +0.00 | +5.59 | +23.66 | — |
| 2026-08-18 | `EOG` | 4 | $146.15 | $148.04 | +7.56 | — | +0.00 | +7.56 | +21.08 | — |
| 2026-08-18 | `FANG` | 3 | $206.29 | $208.93 | +7.92 | — | +0.00 | +7.92 | +18.69 | — |
| 2026-08-18 | `TMC` | 155 | $3.77 | $3.72 | -7.75 | — | +0.00 | -7.75 | -51.15 | — |
| 2026-08-18 | `TGB` | 74 | $8.77 | $8.55 | -16.28 | — | +0.00 | -16.28 | +6.66 | — |
| 2026-08-18 | `ELF` | 6 | $93.66 | $93.44 | -1.32 | — | +0.00 | -1.32 | +17.40 | — |
| 2026-08-18 | `DNN` | 193 | $3.19 | $3.11 | -15.44 | — | +0.00 | -15.44 | -25.09 | — |
| 2026-08-18 | `HNST` | 130 | $4.70 | $4.67 | -3.90 | — | +0.00 | -3.90 | -18.20 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 30 | — | $20.55 | +0.00 | $21.19 | +19.20 | +19.20 | +0.00 | +19.20 |
| 2026-08-20 | `BHP` | 6 | — | $91.01 | +0.00 | $93.63 | +15.72 | +15.72 | +0.00 | +15.72 |
| 2026-08-20 | `CDE` | 30 | — | $20.65 | +0.00 | $21.11 | +13.80 | +13.80 | +0.00 | +13.80 |
| 2026-08-20 | `HDSN` | 108 | — | $5.77 | +0.00 | $5.57 | -21.60 | -21.60 | +0.00 | -21.60 |
| 2026-08-20 | `IAG` | 31 | — | $19.63 | +0.00 | $20.50 | +26.97 | +26.97 | +0.00 | +26.97 |
| 2026-08-20 | `KGC` | 21 | — | $29.63 | +0.00 | $31.43 | +37.80 | +37.80 | +0.00 | +37.80 |
| 2026-08-20 | `NFGC` | 357 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 4 | — | $144.54 | +0.00 | $150.25 | +22.84 | +22.84 | +0.00 | +22.84 |
| 2026-08-21 | `AG` | 30 | $21.19 | $21.90 | +21.30 | — | +0.00 | +21.30 | +40.50 | — |
| 2026-08-21 | `BHP` | 6 | $93.63 | $95.72 | +12.54 | — | +0.00 | +12.54 | +28.26 | — |
| 2026-08-21 | `CDE` | 30 | $21.11 | $21.75 | +19.20 | — | +0.00 | +19.20 | +33.00 | — |
| 2026-08-21 | `HDSN` | 108 | $5.57 | $5.67 | +10.80 | — | +0.00 | +10.80 | -10.80 | — |
| 2026-08-21 | `IAG` | 31 | $20.50 | $21.17 | +20.77 | — | +0.00 | +20.77 | +47.74 | — |
| 2026-08-21 | `KGC` | 21 | $31.43 | $32.17 | +15.54 | — | +0.00 | +15.54 | +53.34 | — |
| 2026-08-21 | `NFGC` | 357 | $1.75 | $1.79 | +14.28 | — | +0.00 | +14.28 | +14.28 | — |
| 2026-08-21 | `WPM` | 4 | $150.25 | $154.70 | +17.80 | — | +0.00 | +17.80 | +40.64 | — |
| 2026-08-21 | `AU` | 5 | — | $119.43 | +0.00 | $121.22 | +8.95 | +8.95 | +0.00 | +8.95 |
| 2026-08-21 | `AUPH` | 37 | — | $17.20 | +0.00 | $16.65 | -20.35 | -20.35 | +0.00 | -20.35 |
| 2026-08-21 | `AEM` | 2 | — | $216.30 | +0.00 | $216.06 | -0.48 | -0.48 | +0.00 | -0.48 |
| 2026-08-21 | `ARCT` | 57 | — | $11.13 | +0.00 | $13.45 | +132.24 | +132.24 | +0.00 | +132.24 |
| 2026-08-21 | `AUTL` | 258 | — | $2.47 | +0.00 | $2.41 | -15.48 | -15.48 | +0.00 | -15.48 |
| 2026-08-21 | `CRDL` | 330 | — | $1.93 | +0.00 | $1.86 | -23.10 | -23.10 | +0.00 | -23.10 |
| 2026-08-21 | `CRSP` | 10 | — | $59.72 | +0.00 | $59.50 | -2.20 | -2.20 | +0.00 | -2.20 |
| 2026-08-21 | `CYPH` | 483 | — | $1.32 | +0.00 | $1.42 | +48.30 | +48.30 | +0.00 | +48.30 |
| 2026-08-24 | `AU` | 5 | $121.22 | $120.51 | -3.55 | — | +0.00 | -3.55 | +5.40 | — |
| 2026-08-24 | `AUPH` | 37 | $16.65 | $16.57 | -2.96 | — | +0.00 | -2.96 | -23.31 | — |
| 2026-08-24 | `AEM` | 2 | $216.06 | $217.03 | +1.94 | — | +0.00 | +1.94 | +1.46 | — |
| 2026-08-24 | `ARCT` | 57 | $13.45 | $13.33 | -6.84 | — | +0.00 | -6.84 | +125.40 | — |
| 2026-08-24 | `AUTL` | 258 | $2.41 | $2.40 | -2.58 | — | +0.00 | -2.58 | -18.06 | — |
| 2026-08-24 | `CRDL` | 330 | $1.86 | $1.88 | +6.60 | — | +0.00 | +6.60 | -16.50 | — |
| 2026-08-24 | `CRSP` | 10 | $59.50 | $58.75 | -7.50 | — | +0.00 | -7.50 | -9.70 | — |
| 2026-08-24 | `CYPH` | 483 | $1.42 | $1.83 | +198.03 | — | +0.00 | +198.03 | +246.33 | — |
| 2026-08-25 | `MOS` | 27 | — | $23.77 | +0.00 | $24.27 | +13.50 | +13.50 | +0.00 | +13.50 |
| 2026-08-25 | `OCUL` | 59 | — | $10.98 | +0.00 | $10.88 | -5.90 | -5.90 | +0.00 | -5.90 |
| 2026-08-25 | `INSP` | 10 | — | $61.19 | +0.00 | $61.07 | -1.20 | -1.20 | +0.00 | -1.20 |
| 2026-08-25 | `CRMD` | 78 | — | $8.35 | +0.00 | $8.56 | +16.38 | +16.38 | +0.00 | +16.38 |
| 2026-08-25 | `RZLT` | 132 | — | $4.94 | +0.00 | $5.01 | +9.24 | +9.24 | +0.00 | +9.24 |
| 2026-08-25 | `HCA` | 1 | — | $426.97 | +0.00 | $428.76 | +1.79 | +1.79 | +0.00 | +1.79 |
| 2026-08-25 | `CAPR` | 90 | — | $7.25 | +0.00 | $8.29 | +93.60 | +93.60 | +0.00 | +93.60 |
| 2026-08-25 | `SAFX` | 1829 | — | $0.36 | +0.00 | $0.35 | -7.32 | -7.32 | +0.00 | -7.32 |
| 2026-08-26 | `MOS` | 27 | $24.27 | $24.84 | +15.39 | $24.16 | -18.36 | -2.97 | +28.89 | +10.53 |
| 2026-08-26 | `OCUL` | 59 | $10.88 | $10.79 | -5.31 | $10.77 | -1.18 | -6.49 | -11.21 | -12.39 |
| 2026-08-26 | `INSP` | 10 | $61.07 | $60.07 | -10.00 | $61.80 | +17.30 | +7.30 | -11.20 | +6.10 |
| 2026-08-26 | `CRMD` | 78 | $8.56 | $8.60 | +3.12 | $8.39 | -16.38 | -13.26 | +19.50 | +3.12 |
| 2026-08-26 | `RZLT` | 132 | $5.01 | $5.01 | +0.00 | $5.04 | +3.96 | +3.96 | +9.24 | +13.20 |
| 2026-08-26 | `HCA` | 1 | $428.76 | $427.50 | -1.26 | $427.16 | -0.34 | -1.60 | +0.53 | +0.19 |
| 2026-08-26 | `CAPR` | 90 | $8.29 | $8.29 | +0.00 | — | +0.00 | +0.00 | +93.60 | — |
| 2026-08-26 | `SAFX` | 1829 | $0.35 | $0.35 | -1.83 | — | +0.00 | -1.83 | -9.15 | — |
| 2026-08-26 | `AVBP` | 55 | — | $31.21 | +0.00 | $31.14 | -3.85 | -3.85 | +0.00 | -3.85 |
| 2026-08-26 | `FLNC` | 154 | — | $11.12 | +0.00 | $11.08 | -6.16 | -6.16 | +0.00 | -6.16 |
| 2026-08-27 | `MOS` | 27 | $24.16 | $24.00 | -4.32 | $23.76 | -6.48 | -10.80 | +6.21 | -0.27 |
| 2026-08-27 | `OCUL` | 59 | $10.77 | $10.63 | -8.26 | — | +0.00 | -8.26 | -20.65 | — |
| 2026-08-27 | `INSP` | 10 | $61.80 | $62.10 | +3.00 | — | +0.00 | +3.00 | +9.10 | — |
| 2026-08-27 | `CRMD` | 78 | $8.39 | $8.49 | +7.80 | — | +0.00 | +7.80 | +10.92 | — |
| 2026-08-27 | `RZLT` | 132 | $5.04 | $5.07 | +3.96 | — | +0.00 | +3.96 | +17.16 | — |
| 2026-08-27 | `HCA` | 1 | $427.16 | $424.61 | -2.55 | — | +0.00 | -2.55 | -2.36 | — |
| 2026-08-27 | `AVBP` | 55 | $31.14 | $30.79 | -19.25 | — | +0.00 | -19.25 | -23.10 | — |
| 2026-08-27 | `FLNC` | 154 | $11.08 | $11.52 | +67.76 | — | +0.00 | +67.76 | +61.60 | — |
| 2026-08-27 | `RRC` | 17 | — | $41.44 | +0.00 | $41.64 | +3.40 | +3.40 | +0.00 | +3.40 |
| 2026-08-27 | `CRK` | 49 | — | $14.42 | +0.00 | $14.62 | +9.80 | +9.80 | +0.00 | +9.80 |
| 2026-08-27 | `SLI` | 272 | — | $2.60 | +0.00 | $2.64 | +10.88 | +10.88 | +0.00 | +10.88 |
| 2026-08-27 | `ACMR` | 8 | — | $81.65 | +0.00 | $80.49 | -9.28 | -9.28 | +0.00 | -9.28 |
| 2026-08-27 | `GGB` | 154 | — | $4.57 | +0.00 | $4.70 | +20.02 | +20.02 | +0.00 | +20.02 |
| 2026-08-27 | `MT` | 9 | — | $74.54 | +0.00 | $74.63 | +0.81 | +0.81 | +0.00 | +0.81 |
| 2026-08-28 | `MOS` | 27 | $23.76 | $23.95 | +5.13 | $23.60 | -9.45 | -4.32 | +4.86 | -4.59 |
| 2026-08-28 | `RRC` | 17 | $41.64 | $41.74 | +1.70 | $41.46 | -4.76 | -3.06 | +5.10 | +0.34 |
| 2026-08-28 | `CRK` | 49 | $14.62 | $14.63 | +0.49 | $14.29 | -16.66 | -16.17 | +10.29 | -6.37 |
| 2026-08-28 | `SLI` | 272 | $2.64 | $2.68 | +10.88 | $2.55 | -35.36 | -24.48 | +21.76 | -13.60 |
| 2026-08-28 | `ACMR` | 8 | $80.49 | $79.27 | -9.76 | — | +0.00 | -9.76 | -19.04 | — |
| 2026-08-28 | `GGB` | 154 | $4.70 | $4.67 | -4.62 | — | +0.00 | -4.62 | +15.40 | — |
| 2026-08-28 | `MT` | 9 | $74.63 | $75.39 | +6.84 | — | +0.00 | +6.84 | +7.65 | — |
| 2026-08-28 | `SEDG` | 29 | — | $32.90 | +0.00 | $31.41 | -43.21 | -43.21 | +0.00 | -43.21 |
| 2026-08-28 | `GRRR` | 62 | — | $15.66 | +0.00 | $14.41 | -77.50 | -77.50 | +0.00 | -77.50 |
| 2026-08-28 | `URBN` | 12 | — | $79.42 | +0.00 | $81.09 | +20.04 | +20.04 | +0.00 | +20.04 |
| 2026-08-28 | `PYXS` | 292 | — | $3.32 | +0.00 | $3.23 | -26.28 | -26.28 | +0.00 | -26.28 |
| 2026-08-31 | `MOS` | 27 | $23.60 | $23.68 | +2.16 | — | +0.00 | +2.16 | -2.43 | — |
| 2026-08-31 | `RRC` | 17 | $41.46 | $42.00 | +9.18 | — | +0.00 | +9.18 | +9.52 | — |
| 2026-08-31 | `CRK` | 49 | $14.29 | $14.54 | +12.25 | — | +0.00 | +12.25 | +5.88 | — |
| 2026-08-31 | `SLI` | 272 | $2.55 | $2.58 | +8.16 | — | +0.00 | +8.16 | -5.44 | — |
| 2026-08-31 | `SEDG` | 29 | $31.41 | $31.15 | -7.54 | — | +0.00 | -7.54 | -50.75 | — |
| 2026-08-31 | `GRRR` | 62 | $14.41 | $14.44 | +1.86 | — | +0.00 | +1.86 | -75.64 | — |
| 2026-08-31 | `URBN` | 12 | $81.09 | $80.44 | -7.80 | — | +0.00 | -7.80 | +12.24 | — |
| 2026-08-31 | `PYXS` | 292 | $3.23 | $3.20 | -8.76 | — | +0.00 | -8.76 | -35.04 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 12 | — | $52.88 | +0.00 | $52.46 | -5.04 | -5.04 | +0.00 | -5.04 |
| 2026-09-03 | `HRMY` | 15 | — | $42.93 | +0.00 | $41.86 | -16.05 | -16.05 | +0.00 | -16.05 |
| 2026-09-03 | `CABA` | 178 | — | $3.63 | +0.00 | $3.48 | -26.70 | -26.70 | +0.00 | -26.70 |
| 2026-09-03 | `VSTM` | 80 | — | $8.03 | +0.00 | $7.98 | -4.00 | -4.00 | +0.00 | -4.00 |
| 2026-09-03 | `RVTY` | 4 | — | $132.45 | +0.00 | $130.63 | -7.28 | -7.28 | +0.00 | -7.28 |
| 2026-09-03 | `CRK` | 41 | — | $15.45 | +0.00 | $14.95 | -20.50 | -20.50 | +0.00 | -20.50 |
| 2026-09-03 | `MRNA` | 4 | — | $145.94 | +0.00 | $148.87 | +11.70 | +11.70 | +0.00 | +11.70 |
| 2026-09-03 | `ARCT` | 38 | — | $16.77 | +0.00 | $15.56 | -45.98 | -45.98 | +0.00 | -45.98 |
| 2026-09-04 | `ATRC` | 12 | $52.46 | $52.03 | -5.16 | $51.52 | -6.12 | -11.28 | -10.20 | -16.32 |
| 2026-09-04 | `HRMY` | 15 | $41.86 | $41.50 | -5.40 | — | +0.00 | -5.40 | -21.45 | — |
| 2026-09-04 | `CABA` | 178 | $3.48 | $3.46 | -3.56 | $3.47 | +1.78 | -1.78 | -30.26 | -28.48 |
| 2026-09-04 | `VSTM` | 80 | $7.98 | $7.91 | -5.60 | — | +0.00 | -5.60 | -9.60 | — |
| 2026-09-04 | `RVTY` | 4 | $130.63 | $130.03 | -2.40 | — | +0.00 | -2.40 | -9.68 | — |
| 2026-09-04 | `CRK` | 41 | $14.95 | $15.00 | +2.05 | — | +0.00 | +2.05 | -18.45 | — |
| 2026-09-04 | `MRNA` | 4 | $148.87 | $153.62 | +19.00 | — | +0.00 | +19.00 | +30.70 | — |
| 2026-09-04 | `ARCT` | 38 | $15.56 | $15.61 | +1.90 | — | +0.00 | +1.90 | -44.08 | — |
| 2026-09-04 | `ALEC` | 297 | — | $2.52 | +0.00 | $2.46 | -17.82 | -17.82 | +0.00 | -17.82 |
| 2026-09-04 | `BHC` | 111 | — | $6.71 | +0.00 | $6.56 | -16.65 | -16.65 | +0.00 | -16.65 |
| 2026-09-04 | `BMEA` | 393 | — | $1.90 | +0.00 | $2.03 | +51.09 | +51.09 | +0.00 | +51.09 |
| 2026-09-04 | `OABI` | 156 | — | $4.78 | +0.00 | $4.33 | -70.20 | -70.20 | +0.00 | -70.20 |
| 2026-09-04 | `OPK` | 470 | — | $1.59 | +0.00 | $1.64 | +23.50 | +23.50 | +0.00 | +23.50 |
| 2026-09-04 | `VIR` | 66 | — | $11.31 | +0.00 | $11.38 | +4.95 | +4.95 | +0.00 | +4.95 |
| 2026-09-08 | `ATRC` | 12 | $51.52 | $54.31 | +33.48 | — | +0.00 | +33.48 | +17.16 | — |
| 2026-09-08 | `CABA` | 178 | $3.47 | $3.43 | -7.12 | — | +0.00 | -7.12 | -35.60 | — |
| 2026-09-08 | `ALEC` | 297 | $2.46 | $2.38 | -23.76 | — | +0.00 | -23.76 | -41.58 | — |
| 2026-09-08 | `BHC` | 111 | $6.56 | $6.57 | +1.11 | — | +0.00 | +1.11 | -15.54 | — |
| 2026-09-08 | `BMEA` | 393 | $2.03 | $2.00 | -11.79 | — | +0.00 | -11.79 | +39.30 | — |
| 2026-09-08 | `OABI` | 156 | $4.33 | $4.30 | -4.68 | — | +0.00 | -4.68 | -74.88 | — |
| 2026-09-08 | `OPK` | 470 | $1.64 | $1.63 | -4.70 | — | +0.00 | -4.70 | +18.80 | — |
| 2026-09-08 | `VIR` | 66 | $11.38 | $11.22 | -10.89 | — | +0.00 | -10.89 | -5.94 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +94.08 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $5,101.72 | $10,071.15 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26 |
| 2026-08-14 | +5.50 | $5,101.72 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26 | $10,084.41 | +13.26 | +43.09 | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $5,701.01 | $10,077.45 | TLN×1, VST×4, NRG×5, DAVE×1, SLG×10, MARA×69, LDI×671, BTBT×419 |
| 2026-08-17 | +2.25 | $5,701.01 | TLN×1, VST×4, NRG×5, DAVE×1, SLG×10, MARA×69, LDI×671, BTBT×419 | $10,075.66 | -1.79 | +16.67 | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | $5,204.80 | $10,048.64 | DVN×13, EOG×4, FANG×3, TMC×155, TGB×74, ELF×6, DNN×193, HNST×130 |
| 2026-08-18 | -6.20 | $5,204.80 | DVN×13, EOG×4, FANG×3, TMC×155, TGB×74, ELF×6, DNN×193, HNST×130 | $10,025.02 | -23.62 | +0.00 | — | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST | $10,007.16 | $10,007.16 | — |
| 2026-08-19 | -7.20 | $10,007.16 | — | $10,007.16 | -0.00 | +0.00 | — | — | $10,007.16 | $10,007.16 | — |
| 2026-08-20 | +1.12 | $10,007.16 | — | $10,007.16 | -0.00 | +114.73 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $5,149.04 | $10,102.66 | AG×30, BHP×6, CDE×30, HDSN×108, IAG×31, KGC×21, NFGC×357, WPM×4 |
| 2026-08-21 | +3.25 | $5,149.04 | AG×30, BHP×6, CDE×30, HDSN×108, IAG×31, KGC×21, NFGC×357, WPM×4 | $10,234.89 | +132.23 | +127.88 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $5,381.87 | $10,319.23 | AU×5, AUPH×37, AEM×2, ARCT×57, AUTL×258, CRDL×330, CRSP×10, CYPH×483 |
| 2026-08-24 | -5.17 | $5,381.87 | AU×5, AUPH×37, AEM×2, ARCT×57, AUTL×258, CRDL×330, CRSP×10, CYPH×483 | $10,502.37 | +183.14 | +0.00 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $10,477.97 | $10,477.97 | — |
| 2026-08-25 | +1.80 | $10,477.97 | — | $10,477.97 | -0.00 | +120.09 | MOS, OCUL, INSP, CRMD, RZLT, HCA, CAPR, SAFX | — | $5,511.67 | $10,570.90 | MOS×27, OCUL×59, INSP×10, CRMD×78, RZLT×132, HCA×1, CAPR×90, SAFX×1829 |
| 2026-08-26 | +2.02 | $5,511.67 | MOS×27, OCUL×59, INSP×10, CRMD×78, RZLT×132, HCA×1, CAPR×90, SAFX×1829 | $10,571.01 | +0.11 | -25.01 | AVBP, FLNC | CAPR, SAFX | $3,455.23 | $10,526.86 | MOS×27, OCUL×59, INSP×10, CRMD×78, RZLT×132, HCA×1, AVBP×55, FLNC×154 |
| 2026-08-27 | — | $3,455.23 | MOS×27, OCUL×59, INSP×10, CRMD×78, RZLT×132, HCA×1, AVBP×55, FLNC×154 | $10,575.00 | +48.14 | +29.15 | RRC, CRK, SLI, ACMR, GGB, MT | OCUL, INSP, CRMD, RZLT, HCA, AVBP, FLNC | $5,751.15 | $10,574.40 | MOS×27, RRC×17, CRK×49, SLI×272, ACMR×8, GGB×154, MT×9 |
| 2026-08-28 | +0.75 | $5,751.15 | MOS×27, RRC×17, CRK×49, SLI×272, ACMR×8, GGB×154, MT×9 | $10,585.06 | +10.66 | -193.18 | SEDG, GRRR, URBN, PYXS | ACMR, GGB, MT | $3,918.90 | $10,375.28 | MOS×27, RRC×17, CRK×49, SLI×272, SEDG×29, GRRR×62, URBN×12, PYXS×292 |
| 2026-08-31 | -5.85 | $3,918.90 | MOS×27, RRC×17, CRK×49, SLI×272, SEDG×29, GRRR×62, URBN×12, PYXS×292 | $10,384.79 | +9.51 | +0.00 | — | MOS, RRC, CRK, SLI, SEDG, GRRR, URBN, PYXS | $10,364.75 | $10,364.75 | — |
| 2026-09-01 | -6.30 | $10,364.75 | — | $10,364.75 | -0.00 | +0.00 | — | — | $10,364.75 | $10,364.75 | — |
| 2026-09-02 | -3.83 | $10,364.75 | — | $10,364.75 | -0.00 | +0.00 | — | — | $10,364.75 | $10,364.75 | — |
| 2026-09-03 | -0.90 | $10,364.75 | — | $10,364.75 | -0.00 | -113.85 | ATRC, HRMY, CABA, VSTM, RVTY, CRK, MRNA, ARCT | — | $5,396.37 | $10,233.86 | ATRC×12, HRMY×15, CABA×178, VSTM×80, RVTY×4, CRK×41, MRNA×4, ARCT×38 |
| 2026-09-04 | +2.25 | $5,396.37 | ATRC×12, HRMY×15, CABA×178, VSTM×80, RVTY×4, CRK×41, MRNA×4, ARCT×38 | $10,234.69 | +0.83 | -29.47 | ALEC, BHC, BMEA, OABI, OPK, VIR | HRMY, VSTM, RVTY, CRK, MRNA, ARCT | $4,480.52 | $10,170.68 | ATRC×12, CABA×178, ALEC×297, BHC×111, BMEA×393, OABI×156, OPK×470, VIR×66 |
| 2026-09-08 | -11.47 | $4,480.52 | ATRC×12, CABA×178, ALEC×297, BHC×111, BMEA×393, OABI×156, OPK×470, VIR×66 | $10,142.33 | -28.35 | +0.00 | — | ATRC, CABA, ALEC, BHC, BMEA, OABI, OPK, VIR | $10,115.48 | $10,115.48 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 10 | $59.80 | $2.02 | — | $9,399.98 | — | deploy half leftover; list flatten; ⚪; ret5=-5.3; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 13 | $45.98 | $2.03 | — | $8,800.21 | — | deploy half leftover; list flatten; ⚪; ret5=+12.3; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 12 | $50.62 | $2.03 | — | $8,190.71 | — | deploy half leftover; list flatten; ⚪; ret5=+6.2; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 12 | $49.70 | $2.03 | — | $7,592.28 | — | deploy half leftover; list flatten; ⚪; ret5=-0.8; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 53 | $11.70 | $2.15 | — | $6,970.03 | — | deploy half leftover; list flatten; ⚪; ret5=-0.8; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 21 | $29.74 | $2.05 | — | $6,343.44 | — | deploy half leftover; list flatten; ⚪; ret5=-5.3; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 771 | $0.81 | $8.56 | — | $5,710.37 | — | deploy half leftover; list flatten; ⚪; ret5=+13.2; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 26 | $23.33 | $2.07 | — | $5,101.72 | — | deploy half leftover; list flatten; ⚪; ret5=+19.7; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,101.72 | ▲ close $10,071.15 vs 09:30 $10,000.00 (session +94.08) | 16:00 close · cash $5,101.72 · equity $10,071.15 vs 09:30 $10,000.00 (+71.15; session marks +94.08) · 8 name(s) marked open→close (per-name table). BTSG×10 09:30 $59.80 → close $60.23 +4.30; IREN×13 09:30 $45.98 → close $44.76 -15.86; TPG×12 09:30 $50.62 → close $54.62 +47.96; TGTX×12 09:30 $49.70 → close $47.94 -21.12; SLS×53 09:30 $11.70 → close $12.36 +34.98; HIMS×21 09:30 $29.74 → close $28.77 -20.37; INO×771 09:30 $0.81 → close $0.90 +69.39; TNDM×26 09:30 $23.33 → close $23.13 -5.20 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,101.72 | ▲ 09:30 equity $10,084.41 vs yday $10,071.15 (+13.26) | 09:30 open · cash $5,101.72 (unchanged overnight, no fees) · equity $10,084.41 vs prior close $10,071.15 (+13.26) · 8 name(s) re-marked at the open (per-name table). BTSG×10 yday $60.23 → 09:30 $59.65 -5.80; IREN×13 yday $44.76 → 09:30 $44.09 -8.71; TPG×12 yday $54.62 → 09:30 $55.29 +8.04; TGTX×12 yday $47.94 → 09:30 $47.27 -8.04; SLS×53 yday $12.36 → 09:30 $12.40 +2.12; HIMS×21 yday $28.77 → 09:30 $29.15 +7.98; INO×771 yday $0.90 → 09:30 $0.93 +23.13; TNDM×26 yday $23.13 → 09:30 $22.92 -5.46 | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 10 | $59.65 | $2.04 | $-5.56 | $5,696.18 | ▼ -5.56 after sell → book $10,082.37; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 13 | $44.09 | $2.05 | $-28.65 | $6,267.30 | ▼ -28.65 after sell → book $10,080.32; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 12 | $55.29 | $2.05 | $+51.93 | $6,928.74 | ▲ +51.93 after sell → book $10,078.28; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 12 | $47.27 | $2.05 | $-33.23 | $7,493.93 | ▼ -33.23 after sell → book $10,076.23; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 53 | $12.40 | $2.17 | $+32.78 | $8,148.96 | ▲ +32.78 after sell → book $10,074.06; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 21 | $29.15 | $2.07 | $-16.52 | $8,759.04 | ▼ -16.52 after sell → book $10,071.99; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 771 | $0.93 | $9.62 | $+74.34 | $9,466.45 | ▲ +74.34 after sell → book $10,062.37; vs 09:30 mark -9.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 26 | $22.92 | $2.09 | $-14.82 | $10,060.28 | ▼ -14.82 after sell → book $10,060.28; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 1 | $359.83 | $1.99 | — | $9,698.46 | — | deploy half leftover; list flatten; 🔵; ret5=+5.9; leftover $628.77 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 4 | $146.90 | $2.00 | — | $9,108.86 | — | deploy half leftover; list flatten; 🔵; ret5=+3.6; leftover $628.77 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 5 | $120.00 | $2.00 | — | $8,506.85 | — | deploy half leftover; list flatten; 🔵; ret5=+0.6; leftover $628.77 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 1 | $330.91 | $1.99 | — | $8,173.95 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=-8.6; leftover $628.77 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 10 | $57.61 | $2.02 | — | $7,595.83 | — | deploy half leftover; list flatten; 🔵; ret5=+5.7; leftover $628.77 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 69 | $9.01 | $2.20 | — | $6,971.94 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=-13.5; leftover $628.77 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 671 | $0.94 | $8.30 | — | $6,334.91 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+0.5; leftover $628.77 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 419 | $1.50 | $5.41 | — | $5,701.01 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+9.2; leftover $628.77 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,701.01 | ▲ close $10,077.45 vs 09:30 $10,084.41 (session +43.09) | 16:00 close · cash $5,701.01 · equity $10,077.45 vs 09:30 $10,084.41 (-6.96; session marks +43.09) · 8 name(s) marked open→close (per-name table). TLN×1 09:30 $359.83 → close $362.74 +2.91; VST×4 09:30 $146.90 → close $148.13 +4.92; NRG×5 09:30 $120.00 → close $126.24 +31.20; DAVE×1 09:30 $330.91 → close $334.57 +3.66; SLG×10 09:30 $57.61 → close $56.09 -15.20; MARA×69 09:30 $9.01 → close $9.20 +13.11; LDI×671 09:30 $0.94 → close $0.90 -26.84; BTBT×419 09:30 $1.50 → close $1.57 +29.33 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,701.01 | ▼ 09:30 equity $10,075.66 vs yday $10,077.45 (-1.79) | 09:30 open · cash $5,701.01 (unchanged overnight, no fees) · equity $10,075.66 vs prior close $10,077.45 (-1.79) · 8 name(s) re-marked at the open (per-name table). TLN×1 yday $362.74 → 09:30 $367.88 +5.14; VST×4 yday $148.13 → 09:30 $149.37 +4.96; NRG×5 yday $126.24 → 09:30 $127.40 +5.80; DAVE×1 yday $334.57 → 09:30 $336.94 +2.37; SLG×10 yday $56.09 → 09:30 $55.37 -7.20; MARA×69 yday $9.20 → 09:30 $9.22 +1.38; LDI×671 yday $0.90 → 09:30 $0.91 +6.71; BTBT×419 yday $1.57 → 09:30 $1.52 -20.95 | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 1 | $367.88 | $2.01 | $+4.04 | $6,066.87 | ▲ +4.04 after sell → book $10,073.65; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 4 | $149.37 | $2.02 | $+5.86 | $6,662.33 | ▲ +5.86 after sell → book $10,071.63; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 5 | $127.40 | $2.02 | $+32.97 | $7,297.31 | ▲ +32.97 after sell → book $10,069.60; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 1 | $336.94 | $2.01 | $+2.02 | $7,632.23 | ▲ +2.02 after sell → book $10,067.59; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 10 | $55.37 | $2.04 | $-26.46 | $8,183.89 | ▼ -26.46 after sell → book $10,065.55; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 69 | $9.22 | $2.22 | $+10.07 | $8,817.86 | ▲ +10.07 after sell → book $10,063.33; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 671 | $0.91 | $8.22 | $-36.65 | $9,418.23 | ▼ -36.65 after sell → book $10,055.11; vs 09:30 mark -8.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 419 | $1.52 | $5.48 | $-2.51 | $10,049.63 | ▼ -2.51 after sell → book $10,049.63; vs 09:30 mark -5.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 13 | $46.18 | $2.03 | — | $9,447.26 | — | deploy half leftover; list flatten; 🔵; ret5=+6.7; leftover $628.10 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 4 | $142.77 | $2.00 | — | $8,874.18 | — | deploy half leftover; list flatten; 🔵; ret5=+5.8; leftover $628.10 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 3 | $202.70 | $2.00 | — | $8,264.08 | — | deploy half leftover; list flatten; 🔵; ret5=+8.3; leftover $628.10 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 155 | $4.05 | $2.46 | — | $7,633.87 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=-12.3; leftover $628.10 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 74 | $8.46 | $2.21 | — | $7,005.62 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+0.4; leftover $628.10 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 6 | $90.54 | $2.01 | — | $6,460.37 | — | deploy half leftover; list flatten; ret5=-7.2; leftover $628.10 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 193 | $3.24 | $2.57 | — | $5,832.48 | — | deploy half leftover; list flatten; ⚪; ret5=+0.3; leftover $628.10 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 130 | $4.81 | $2.38 | — | $5,204.80 | — | deploy half leftover; list flatten; ⚪; ret5=-11.4; leftover $628.10 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,204.80 | ▲ close $10,048.64 vs 09:30 $10,075.66 (session +16.67) | 16:00 close · cash $5,204.80 · equity $10,048.64 vs 09:30 $10,075.66 (-27.02; session marks +16.67) · 8 name(s) marked open→close (per-name table). DVN×13 09:30 $46.18 → close $47.57 +18.07; EOG×4 09:30 $142.77 → close $146.15 +13.52; FANG×3 09:30 $202.70 → close $206.29 +10.77; TMC×155 09:30 $4.05 → close $3.77 -43.40; TGB×74 09:30 $8.46 → close $8.77 +22.94; ELF×6 09:30 $90.54 → close $93.66 +18.72; DNN×193 09:30 $3.24 → close $3.19 -9.65; HNST×130 09:30 $4.81 → close $4.70 -14.30 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,204.80 | ▼ 09:30 equity $10,025.02 vs yday $10,048.64 (-23.62) | 09:30 open · cash $5,204.80 (unchanged overnight, no fees) · equity $10,025.02 vs prior close $10,048.64 (-23.62) · 8 name(s) re-marked at the open (per-name table). DVN×13 yday $47.57 → 09:30 $48.00 +5.59; EOG×4 yday $146.15 → 09:30 $148.04 +7.56; FANG×3 yday $206.29 → 09:30 $208.93 +7.92; TMC×155 yday $3.77 → 09:30 $3.72 -7.75; TGB×74 yday $8.77 → 09:30 $8.55 -16.28; ELF×6 yday $93.66 → 09:30 $93.44 -1.32; DNN×193 yday $3.19 → 09:30 $3.11 -15.44; HNST×130 yday $4.70 → 09:30 $4.67 -3.90 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 13 | $48.00 | $2.05 | $+19.58 | $5,826.76 | ▲ +19.58 after sell → book $10,022.98; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 4 | $148.04 | $2.02 | $+17.06 | $6,416.89 | ▲ +17.06 after sell → book $10,020.95; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 3 | $208.93 | $2.02 | $+14.67 | $7,041.66 | ▲ +14.67 after sell → book $10,018.93; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 155 | $3.72 | $2.49 | $-56.10 | $7,615.77 | ▼ -56.10 after sell → book $10,016.44; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 74 | $8.55 | $2.23 | $+2.21 | $8,246.24 | ▲ +2.21 after sell → book $10,014.21; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 6 | $93.44 | $2.03 | $+13.36 | $8,804.85 | ▲ +13.36 after sell → book $10,012.18; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 193 | $3.11 | $2.61 | $-30.27 | $9,402.47 | ▼ -30.27 after sell → book $10,009.57; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `HNST` | 130 | $4.67 | $2.41 | $-22.99 | $10,007.16 | ▼ -22.99 after sell → book $10,007.16; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,007.16 | ▲ close $10,007.16 vs 09:30 $10,025.02 (session +0.00) | 16:00 close · cash $10,007.16 · no lots left · equity $10,007.16. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,007.16 | ▲ 09:30 equity $10,007.16 vs yday $10,007.16 (-0.00) | 09:30 open · cash $10,007.16 · no holdings · equity $10,007.16 vs prior close $10,007.16 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,007.16 | ▲ close $10,007.16 vs 09:30 $10,007.16 (session +0.00) | 16:00 close · cash $10,007.16 · no lots left · equity $10,007.16. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,007.16 | ▲ 09:30 equity $10,007.16 vs yday $10,007.16 (-0.00) | 09:30 open · cash $10,007.16 · no holdings · equity $10,007.16 vs prior close $10,007.16 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 30 | $20.55 | $2.08 | — | $9,388.58 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $625.45 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 6 | $91.01 | $2.01 | — | $8,840.51 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $625.45 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 30 | $20.65 | $2.08 | — | $8,218.93 | — | deploy half leftover; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $625.45 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 108 | $5.77 | $2.31 | — | $7,593.46 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $625.45 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 31 | $19.63 | $2.08 | — | $6,982.84 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $625.45 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 21 | $29.63 | $2.05 | — | $6,358.56 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $625.45 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 357 | $1.75 | $4.61 | — | $5,729.21 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $625.45 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 4 | $144.54 | $2.00 | — | $5,149.04 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $625.45 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,149.04 | ▲ close $10,102.66 vs 09:30 $10,007.16 (session +114.73) | 16:00 close · cash $5,149.04 · equity $10,102.66 vs 09:30 $10,007.16 (+95.50; session marks +114.73) · 8 name(s) marked open→close (per-name table). AG×30 09:30 $20.55 → close $21.19 +19.20; BHP×6 09:30 $91.01 → close $93.63 +15.72; CDE×30 09:30 $20.65 → close $21.11 +13.80; HDSN×108 09:30 $5.77 → close $5.57 -21.60; IAG×31 09:30 $19.63 → close $20.50 +26.97; KGC×21 09:30 $29.63 → close $31.43 +37.80; NFGC×357 09:30 $1.75 → close $1.75 +0.00; WPM×4 09:30 $144.54 → close $150.25 +22.84 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,149.04 | ▲ 09:30 equity $10,234.89 vs yday $10,102.66 (+132.23) | 09:30 open · cash $5,149.04 (unchanged overnight, no fees) · equity $10,234.89 vs prior close $10,102.66 (+132.23) · 8 name(s) re-marked at the open (per-name table). AG×30 yday $21.19 → 09:30 $21.90 +21.30; BHP×6 yday $93.63 → 09:30 $95.72 +12.54; CDE×30 yday $21.11 → 09:30 $21.75 +19.20; HDSN×108 yday $5.57 → 09:30 $5.67 +10.80; IAG×31 yday $20.50 → 09:30 $21.17 +20.77; KGC×21 yday $31.43 → 09:30 $32.17 +15.54; NFGC×357 yday $1.75 → 09:30 $1.79 +14.28; WPM×4 yday $150.25 → 09:30 $154.70 +17.80 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 30 | $21.90 | $2.10 | $+36.32 | $5,803.94 | ▲ +36.32 after sell → book $10,232.79; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 6 | $95.72 | $2.03 | $+24.22 | $6,376.24 | ▲ +24.22 after sell → book $10,230.77; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 30 | $21.75 | $2.10 | $+28.82 | $7,026.64 | ▲ +28.82 after sell → book $10,228.67; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 108 | $5.67 | $2.34 | $-15.46 | $7,636.65 | ▼ -15.46 after sell → book $10,226.32; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 31 | $21.17 | $2.10 | $+43.55 | $8,290.82 | ▲ +43.55 after sell → book $10,224.22; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 21 | $32.17 | $2.07 | $+49.21 | $8,964.32 | ▲ +49.21 after sell → book $10,222.15; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 357 | $1.79 | $4.67 | $+5.00 | $9,598.67 | ▲ +5.00 after sell → book $10,217.47; vs 09:30 mark -4.68 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 4 | $154.70 | $2.02 | $+36.62 | $10,215.45 | ▲ +36.62 after sell → book $10,215.45; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 5 | $119.43 | $2.00 | — | $9,616.30 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $638.47 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 37 | $17.20 | $2.10 | — | $8,977.79 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $638.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 2 | $216.30 | $2.00 | — | $8,543.20 | — | deploy half leftover; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $638.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 57 | $11.13 | $2.16 | — | $7,906.63 | — | deploy half leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $638.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 258 | $2.47 | $3.33 | — | $7,266.04 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $638.47 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 330 | $1.93 | $4.26 | — | $6,624.88 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $638.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 10 | $59.72 | $2.02 | — | $6,025.66 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $638.47 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 483 | $1.32 | $6.23 | — | $5,381.87 | — | deploy half leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $638.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,381.87 | ▲ close $10,319.23 vs 09:30 $10,234.89 (session +127.88) | 16:00 close · cash $5,381.87 · equity $10,319.23 vs 09:30 $10,234.89 (+84.34; session marks +127.88) · 8 name(s) marked open→close (per-name table). AU×5 09:30 $119.43 → close $121.22 +8.95; AUPH×37 09:30 $17.20 → close $16.65 -20.35; AEM×2 09:30 $216.30 → close $216.06 -0.48; ARCT×57 09:30 $11.13 → close $13.45 +132.24; AUTL×258 09:30 $2.47 → close $2.41 -15.48; CRDL×330 09:30 $1.93 → close $1.86 -23.10; CRSP×10 09:30 $59.72 → close $59.50 -2.20; CYPH×483 09:30 $1.32 → close $1.42 +48.30 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,381.87 | ▲ 09:30 equity $10,502.37 vs yday $10,319.23 (+183.14) | 09:30 open · cash $5,381.87 (unchanged overnight, no fees) · equity $10,502.37 vs prior close $10,319.23 (+183.14) · 8 name(s) re-marked at the open (per-name table). AU×5 yday $121.22 → 09:30 $120.51 -3.55; AUPH×37 yday $16.65 → 09:30 $16.57 -2.96; AEM×2 yday $216.06 → 09:30 $217.03 +1.94; ARCT×57 yday $13.45 → 09:30 $13.33 -6.84; AUTL×258 yday $2.41 → 09:30 $2.40 -2.58; CRDL×330 yday $1.86 → 09:30 $1.88 +6.60; CRSP×10 yday $59.50 → 09:30 $58.75 -7.50; CYPH×483 yday $1.42 → 09:30 $1.83 +198.03 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 5 | $120.51 | $2.02 | $+1.37 | $5,982.40 | ▲ +1.37 after sell → book $10,500.35; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 37 | $16.57 | $2.12 | $-27.53 | $6,593.37 | ▼ -27.53 after sell → book $10,498.23; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 2 | $217.03 | $2.02 | $-2.55 | $7,025.41 | ▼ -2.55 after sell → book $10,496.21; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 57 | $13.33 | $2.18 | $+121.06 | $7,783.04 | ▲ +121.06 after sell → book $10,494.03; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 258 | $2.40 | $3.38 | $-24.77 | $8,398.86 | ▼ -24.77 after sell → book $10,490.65; vs 09:30 mark -3.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 330 | $1.88 | $4.32 | $-25.08 | $9,014.94 | ▼ -25.08 after sell → book $10,486.33; vs 09:30 mark -4.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 10 | $58.75 | $2.04 | $-13.76 | $9,600.40 | ▼ -13.76 after sell → book $10,484.29; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 483 | $1.83 | $6.32 | $+233.78 | $10,477.97 | ▲ +233.78 after sell → book $10,477.97; vs 09:30 mark -6.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,477.97 | ▲ close $10,477.97 vs 09:30 $10,502.37 (session +0.00) | 16:00 close · cash $10,477.97 · no lots left · equity $10,477.97. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,477.97 | ▲ 09:30 equity $10,477.97 vs yday $10,477.97 (-0.00) | 09:30 open · cash $10,477.97 · no holdings · equity $10,477.97 vs prior close $10,477.97 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 27 | $23.77 | $2.07 | — | $9,834.10 | — | deploy half leftover; list flatten; ⚪; ret5=+13.0; leftover $654.87 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 59 | $10.98 | $2.17 | — | $9,184.12 | — | deploy half leftover; list flatten; 🔵; ret5=+1.2; leftover $654.87 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 10 | $61.19 | $2.02 | — | $8,570.20 | — | deploy half leftover; list flatten; 🔵; ret5=+7.4; leftover $654.87 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 78 | $8.35 | $2.22 | — | $7,916.67 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+8.0; leftover $654.87 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 132 | $4.94 | $2.39 | — | $7,262.21 | — | deploy half leftover; list flatten; ret5=+7.1; leftover $654.87 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 1 | $426.97 | $1.99 | — | $6,833.24 | — | deploy half leftover; list flatten; ret5=+6.0; leftover $654.87 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 90 | $7.25 | $2.26 | — | $6,178.48 | — | deploy half leftover; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $654.87 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 1829 | $0.36 | $12.03 | — | $5,511.67 | — | deploy half leftover; list probable,yday_gainer; ret5=-15.6; leftover $654.87 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,511.67 | ▲ close $10,570.90 vs 09:30 $10,477.97 (session +120.09) | 16:00 close · cash $5,511.67 · equity $10,570.90 vs 09:30 $10,477.97 (+92.93; session marks +120.09) · 8 name(s) marked open→close (per-name table). MOS×27 09:30 $23.77 → close $24.27 +13.50; OCUL×59 09:30 $10.98 → close $10.88 -5.90; INSP×10 09:30 $61.19 → close $61.07 -1.20; CRMD×78 09:30 $8.35 → close $8.56 +16.38; RZLT×132 09:30 $4.94 → close $5.01 +9.24; HCA×1 09:30 $426.97 → close $428.76 +1.79; CAPR×90 09:30 $7.25 → close $8.29 +93.60; SAFX×1829 09:30 $0.36 → close $0.35 -7.32 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,511.67 | ▲ 09:30 equity $10,571.01 vs yday $10,570.90 (+0.11) | 09:30 open · cash $5,511.67 (unchanged overnight, no fees) · equity $10,571.01 vs prior close $10,570.90 (+0.11) · 8 name(s) re-marked at the open (per-name table). MOS×27 yday $24.27 → 09:30 $24.84 +15.39; OCUL×59 yday $10.88 → 09:30 $10.79 -5.31; INSP×10 yday $61.07 → 09:30 $60.07 -10.00; CRMD×78 yday $8.56 → 09:30 $8.60 +3.12; RZLT×132 yday $5.01 → 09:30 $5.01 +0.00; HCA×1 yday $428.76 → 09:30 $427.50 -1.26; CAPR×90 yday $8.29 → 09:30 $8.29 +0.00; SAFX×1829 yday $0.35 → 09:30 $0.35 -1.83 | — |
| 2026-08-26 09:30 ET | **SELL** | `CAPR` | 90 | $8.29 | $2.28 | $+89.06 | $6,255.48 | ▲ +89.06 after sell → book $10,568.73; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `SAFX` | 1829 | $0.35 | $12.26 | $-33.44 | $6,888.86 | ▼ -33.44 after sell → book $10,556.47; vs 09:30 mark -12.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 55 | $31.21 | $2.15 | — | $5,170.16 | — | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $1722.22 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 154 | $11.12 | $2.45 | — | $3,455.23 | — | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $1722.22 | join🔴 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,455.23 | ▼ close $10,526.86 vs 09:30 $10,571.01 (session -25.01) | 16:00 close · cash $3,455.23 · equity $10,526.86 vs 09:30 $10,571.01 (-44.15; session marks -25.01) · 8 name(s) marked open→close (per-name table). MOS×27 09:30 $24.84 → close $24.16 -18.36; OCUL×59 09:30 $10.79 → close $10.77 -1.18; INSP×10 09:30 $60.07 → close $61.80 +17.30; CRMD×78 09:30 $8.60 → close $8.39 -16.38; RZLT×132 09:30 $5.01 → close $5.04 +3.96; HCA×1 09:30 $427.50 → close $427.16 -0.34; AVBP×55 09:30 $31.21 → close $31.14 -3.85; FLNC×154 09:30 $11.12 → close $11.08 -6.16 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,455.23 | ▲ 09:30 equity $10,575.00 vs yday $10,526.86 (+48.14) | 09:30 open · cash $3,455.23 (unchanged overnight, no fees) · equity $10,575.00 vs prior close $10,526.86 (+48.14) · 8 name(s) re-marked at the open (per-name table). MOS×27 yday $24.16 → 09:30 $24.00 -4.32; OCUL×59 yday $10.77 → 09:30 $10.63 -8.26; INSP×10 yday $61.80 → 09:30 $62.10 +3.00; CRMD×78 yday $8.39 → 09:30 $8.49 +7.80; RZLT×132 yday $5.04 → 09:30 $5.07 +3.96; HCA×1 yday $427.16 → 09:30 $424.61 -2.55; AVBP×55 yday $31.14 → 09:30 $30.79 -19.25; FLNC×154 yday $11.08 → 09:30 $11.52 +67.76 | — |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 59 | $10.63 | $2.19 | $-25.00 | $4,080.21 | ▼ -25.00 after sell → book $10,572.81; vs 09:30 mark -2.19 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 10 | $62.10 | $2.04 | $+5.04 | $4,699.17 | ▲ +5.04 after sell → book $10,570.77; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 78 | $8.49 | $2.25 | $+6.45 | $5,359.14 | ▲ +6.45 after sell → book $10,568.52; vs 09:30 mark -2.25 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 132 | $5.07 | $2.42 | $+12.36 | $6,025.96 | ▲ +12.36 after sell → book $10,566.10; vs 09:30 mark -2.42 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 1 | $424.61 | $2.01 | $-6.37 | $6,448.56 | ▼ -6.37 after sell → book $10,564.09; vs 09:30 mark -2.01 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AVBP` | 55 | $30.79 | $2.18 | $-27.43 | $8,139.83 | ▼ -27.43 after sell → book $10,561.91; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FLNC` | 154 | $11.52 | $2.49 | $+56.66 | $9,911.42 | ▲ +56.66 after sell → book $10,559.42; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 17 | $41.44 | $2.04 | — | $9,204.90 | — | deploy half leftover; list flatten; ret5=+3.1; leftover $707.96 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 49 | $14.42 | $2.14 | — | $8,496.18 | — | deploy half leftover; list flatten; ret5=+7.1; leftover $707.96 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 272 | $2.60 | $3.51 | — | $7,785.47 | — | deploy half leftover; list flatten; ret5=+13.0; leftover $707.96 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 8 | $81.65 | $2.01 | — | $7,130.26 | — | deploy half leftover; list mover_buy; 🔵; ret5=+2.0; leftover $707.96 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 154 | $4.57 | $2.45 | — | $6,424.03 | — | deploy half leftover; list mover_buy; 🔵; ret5=+1.1; leftover $707.96 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 9 | $74.54 | $2.02 | — | $5,751.15 | — | deploy half leftover; list mover_buy; 🔵; ret5=-0.1; leftover $707.96 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,751.15 | ▲ close $10,574.40 vs 09:30 $10,575.00 (session +29.15) | 16:00 close · cash $5,751.15 · equity $10,574.40 vs 09:30 $10,575.00 (-0.60; session marks +29.15) · 7 name(s) marked open→close (per-name table). MOS×27 09:30 $24.00 → close $23.76 -6.48; RRC×17 09:30 $41.44 → close $41.64 +3.40; CRK×49 09:30 $14.42 → close $14.62 +9.80; SLI×272 09:30 $2.60 → close $2.64 +10.88; ACMR×8 09:30 $81.65 → close $80.49 -9.28; GGB×154 09:30 $4.57 → close $4.70 +20.02; MT×9 09:30 $74.54 → close $74.63 +0.81 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,751.15 | ▲ 09:30 equity $10,585.06 vs yday $10,574.40 (+10.66) | 09:30 open · cash $5,751.15 (unchanged overnight, no fees) · equity $10,585.06 vs prior close $10,574.40 (+10.66) · 7 name(s) re-marked at the open (per-name table). MOS×27 yday $23.76 → 09:30 $23.95 +5.13; RRC×17 yday $41.64 → 09:30 $41.74 +1.70; CRK×49 yday $14.62 → 09:30 $14.63 +0.49; SLI×272 yday $2.64 → 09:30 $2.68 +10.88; ACMR×8 yday $80.49 → 09:30 $79.27 -9.76; GGB×154 yday $4.70 → 09:30 $4.67 -4.62; MT×9 yday $74.63 → 09:30 $75.39 +6.84 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 8 | $79.27 | $2.03 | $-23.09 | $6,383.28 | ▼ -23.09 after sell → book $10,583.03; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 154 | $4.67 | $2.49 | $+10.46 | $7,099.97 | ▲ +10.46 after sell → book $10,580.54; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 9 | $75.39 | $2.04 | $+3.60 | $7,776.44 | ▲ +3.60 after sell → book $10,578.50; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 29 | $32.90 | $2.08 | — | $6,820.26 | — | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $972.06 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 62 | $15.66 | $2.18 | — | $5,847.17 | — | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $972.06 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 12 | $79.42 | $2.03 | — | $4,892.10 | — | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $972.06 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `PYXS` | 292 | $3.32 | $3.77 | — | $3,918.90 | — | deploy half leftover; list probable,yday_gainer; ret5=+6.4; leftover $972.06 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,918.90 | ▼ close $10,375.28 vs 09:30 $10,585.06 (session -193.18) | 16:00 close · cash $3,918.90 · equity $10,375.28 vs 09:30 $10,585.06 (-209.78; session marks -193.18) · 8 name(s) marked open→close (per-name table). MOS×27 09:30 $23.95 → close $23.60 -9.45; RRC×17 09:30 $41.74 → close $41.46 -4.76; CRK×49 09:30 $14.63 → close $14.29 -16.66; SLI×272 09:30 $2.68 → close $2.55 -35.36; SEDG×29 09:30 $32.90 → close $31.41 -43.21; GRRR×62 09:30 $15.66 → close $14.41 -77.50; URBN×12 09:30 $79.42 → close $81.09 +20.04; PYXS×292 09:30 $3.32 → close $3.23 -26.28 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,918.90 | ▲ 09:30 equity $10,384.79 vs yday $10,375.28 (+9.51) | 09:30 open · cash $3,918.90 (unchanged overnight, no fees) · equity $10,384.79 vs prior close $10,375.28 (+9.51) · 8 name(s) re-marked at the open (per-name table). MOS×27 yday $23.60 → 09:30 $23.68 +2.16; RRC×17 yday $41.46 → 09:30 $42.00 +9.18; CRK×49 yday $14.29 → 09:30 $14.54 +12.25; SLI×272 yday $2.55 → 09:30 $2.58 +8.16; SEDG×29 yday $31.41 → 09:30 $31.15 -7.54; GRRR×62 yday $14.41 → 09:30 $14.44 +1.86; URBN×12 yday $81.09 → 09:30 $80.44 -7.80; PYXS×292 yday $3.23 → 09:30 $3.20 -8.76 | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 27 | $23.68 | $2.09 | $-6.59 | $4,556.17 | ▼ -6.59 after sell → book $10,382.70; vs 09:30 mark -2.09 | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 17 | $42.00 | $2.06 | $+5.42 | $5,268.10 | ▲ +5.42 after sell → book $10,380.63; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 49 | $14.54 | $2.16 | $+1.59 | $5,978.41 | ▲ +1.59 after sell → book $10,378.48; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 272 | $2.58 | $3.56 | $-12.51 | $6,676.60 | ▼ -12.51 after sell → book $10,374.91; vs 09:30 mark -3.57 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 29 | $31.15 | $2.10 | $-54.92 | $7,577.86 | ▼ -54.92 after sell → book $10,372.82; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 62 | $14.44 | $2.20 | $-80.01 | $8,470.94 | ▼ -80.01 after sell → book $10,370.62; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 12 | $80.44 | $2.05 | $+8.17 | $9,434.17 | ▲ +8.17 after sell → book $10,368.57; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PYXS` | 292 | $3.20 | $3.83 | $-42.63 | $10,364.75 | ▼ -42.63 after sell → book $10,364.75; vs 09:30 mark -3.82 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,364.75 | ▲ close $10,364.75 vs 09:30 $10,384.79 (session +0.00) | 16:00 close · cash $10,364.75 · no lots left · equity $10,364.75. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,364.75 | ▲ 09:30 equity $10,364.75 vs yday $10,364.75 (-0.00) | 09:30 open · cash $10,364.75 · no holdings · equity $10,364.75 vs prior close $10,364.75 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,364.75 | ▲ close $10,364.75 vs 09:30 $10,364.75 (session +0.00) | 16:00 close · cash $10,364.75 · no lots left · equity $10,364.75. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,364.75 | ▲ 09:30 equity $10,364.75 vs yday $10,364.75 (-0.00) | 09:30 open · cash $10,364.75 · no holdings · equity $10,364.75 vs prior close $10,364.75 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,364.75 | ▲ close $10,364.75 vs 09:30 $10,364.75 (session +0.00) | 16:00 close · cash $10,364.75 · no lots left · equity $10,364.75. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,364.75 | ▲ 09:30 equity $10,364.75 vs yday $10,364.75 (-0.00) | 09:30 open · cash $10,364.75 · no holdings · equity $10,364.75 vs prior close $10,364.75 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 12 | $52.88 | $2.03 | — | $9,728.16 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+9.2; leftover $647.80 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 15 | $42.93 | $2.04 | — | $9,082.18 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $647.80 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 178 | $3.63 | $2.52 | — | $8,433.51 | — | deploy half leftover; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $647.80 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 80 | $8.03 | $2.23 | — | $7,788.88 | — | deploy half leftover; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $647.80 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 4 | $132.45 | $2.00 | — | $7,257.08 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $647.80 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 41 | $15.45 | $2.11 | — | $6,621.52 | — | deploy half leftover; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $647.80 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 4 | $145.94 | $2.00 | — | $6,035.74 | — | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $647.80 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 38 | $16.77 | $2.10 | — | $5,396.37 | — | deploy half leftover; list probable,yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $647.80 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,396.37 | ▼ close $10,233.86 vs 09:30 $10,364.75 (session -113.85) | 16:00 close · cash $5,396.37 · equity $10,233.86 vs 09:30 $10,364.75 (-130.89; session marks -113.85) · 8 name(s) marked open→close (per-name table). ATRC×12 09:30 $52.88 → close $52.46 -5.04; HRMY×15 09:30 $42.93 → close $41.86 -16.05; CABA×178 09:30 $3.63 → close $3.48 -26.70; VSTM×80 09:30 $8.03 → close $7.98 -4.00; RVTY×4 09:30 $132.45 → close $130.63 -7.28; CRK×41 09:30 $15.45 → close $14.95 -20.50; MRNA×4 09:30 $145.94 → close $148.87 +11.70; ARCT×38 09:30 $16.77 → close $15.56 -45.98 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,396.37 | ▲ 09:30 equity $10,234.69 vs yday $10,233.86 (+0.83) | 09:30 open · cash $5,396.37 (unchanged overnight, no fees) · equity $10,234.69 vs prior close $10,233.86 (+0.83) · 8 name(s) re-marked at the open (per-name table). ATRC×12 yday $52.46 → 09:30 $52.03 -5.16; HRMY×15 yday $41.86 → 09:30 $41.50 -5.40; CABA×178 yday $3.48 → 09:30 $3.46 -3.56; VSTM×80 yday $7.98 → 09:30 $7.91 -5.60; RVTY×4 yday $130.63 → 09:30 $130.03 -2.40; CRK×41 yday $14.95 → 09:30 $15.00 +2.05; MRNA×4 yday $148.87 → 09:30 $153.62 +19.00; ARCT×38 yday $15.56 → 09:30 $15.61 +1.90 | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 15 | $41.50 | $2.06 | $-25.54 | $6,016.82 | ▼ -25.54 after sell → book $10,232.64; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 80 | $7.91 | $2.25 | $-14.08 | $6,647.36 | ▼ -14.08 after sell → book $10,230.38; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 4 | $130.03 | $2.02 | $-13.70 | $7,165.46 | ▼ -13.70 after sell → book $10,228.36; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 41 | $15.00 | $2.13 | $-22.70 | $7,778.33 | ▼ -22.70 after sell → book $10,226.23; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 4 | $153.62 | $2.02 | $+26.68 | $8,390.79 | ▲ +26.68 after sell → book $10,224.21; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 38 | $15.61 | $2.12 | $-48.31 | $8,981.84 | ▼ -48.31 after sell → book $10,222.08; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 297 | $2.52 | $3.83 | — | $8,229.57 | — | deploy half leftover; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $748.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 111 | $6.71 | $2.32 | — | $7,482.44 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $748.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 393 | $1.90 | $5.07 | — | $6,730.67 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $748.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 156 | $4.78 | $2.46 | — | $5,982.53 | — | deploy half leftover; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $748.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 470 | $1.59 | $6.06 | — | $5,229.17 | — | deploy half leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $748.49 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 66 | $11.31 | $2.19 | — | $4,480.52 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $748.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,480.52 | ▼ close $10,170.68 vs 09:30 $10,234.69 (session -29.47) | 16:00 close · cash $4,480.52 · equity $10,170.68 vs 09:30 $10,234.69 (-64.01; session marks -29.47) · 8 name(s) marked open→close (per-name table). ATRC×12 09:30 $52.03 → close $51.52 -6.12; CABA×178 09:30 $3.46 → close $3.47 +1.78; ALEC×297 09:30 $2.52 → close $2.46 -17.82; BHC×111 09:30 $6.71 → close $6.56 -16.65; BMEA×393 09:30 $1.90 → close $2.03 +51.09; OABI×156 09:30 $4.78 → close $4.33 -70.20; OPK×470 09:30 $1.59 → close $1.64 +23.50; VIR×66 09:30 $11.31 → close $11.38 +4.95 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,480.52 | ▼ 09:30 equity $10,142.33 vs yday $10,170.68 (-28.35) | 09:30 open · cash $4,480.52 (unchanged overnight, no fees) · equity $10,142.33 vs prior close $10,170.68 (-28.35) · 8 name(s) re-marked at the open (per-name table). ATRC×12 yday $51.52 → 09:30 $54.31 +33.48; CABA×178 yday $3.47 → 09:30 $3.43 -7.12; ALEC×297 yday $2.46 → 09:30 $2.38 -23.76; BHC×111 yday $6.56 → 09:30 $6.57 +1.11; BMEA×393 yday $2.03 → 09:30 $2.00 -11.79; OABI×156 yday $4.33 → 09:30 $4.30 -4.68; OPK×470 yday $1.64 → 09:30 $1.63 -4.70; VIR×66 yday $11.38 → 09:30 $11.22 -10.89 | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 12 | $54.31 | $2.05 | $+13.09 | $5,130.19 | ▲ +13.09 after sell → book $10,140.28; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 178 | $3.43 | $2.56 | $-40.69 | $5,738.17 | ▼ -40.69 after sell → book $10,137.72; vs 09:30 mark -2.56 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 297 | $2.38 | $3.89 | $-49.30 | $6,441.14 | ▼ -49.30 after sell → book $10,133.83; vs 09:30 mark -3.89 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 111 | $6.57 | $2.35 | $-20.21 | $7,168.06 | ▼ -20.21 after sell → book $10,131.48; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 393 | $2.00 | $5.14 | $+29.09 | $7,948.91 | ▲ +29.09 after sell → book $10,126.33; vs 09:30 mark -5.15 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 156 | $4.30 | $2.49 | $-79.83 | $8,617.22 | ▼ -79.83 after sell → book $10,123.84; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 470 | $1.63 | $6.15 | $+6.59 | $9,377.17 | ▲ +6.59 after sell → book $10,117.69; vs 09:30 mark -6.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 66 | $11.22 | $2.21 | $-10.34 | $10,115.48 | ▼ -10.34 after sell → book $10,115.48; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,115.48 | ▲ close $10,115.48 vs 09:30 $10,142.33 (session +0.00) | 16:00 close · cash $10,115.48 · no lots left · equity $10,115.48. | — |

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
| 2026-08-27 | `MU` | cash | leftover split 707.96 < 1 share @ 967.01 |
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
