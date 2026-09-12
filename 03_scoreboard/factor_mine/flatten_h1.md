# Factor mine action — `flatten_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

Side **long** · universe `flatten` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **+6.53%** ($10,653) · signal-only (no cash/fees) was +1.72%. Starts YES **7/21**. Fills 126 · skips 55 · realized $+704.66.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the flatten wish-list (names the flatten board wanted that morning) and only buy names that pass the list as written. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the flatten wish-list (names the flatten board wanted that morning).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).

### When it buys

- At 09:30, take names on the flatten wish-list (names the flatten board wanted that morning) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- Keep the first 8 names in list order.
- Split leftover cash equally across *new* names (not ones we already hold).
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

- **Universe** `flatten` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $355.18.

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
| 2026-08-25 | `MOS` | 77 | — | $23.77 | +0.00 | $24.27 | +38.50 | +38.50 | +0.00 | +38.50 |
| 2026-08-25 | `OCUL` | 168 | — | $10.98 | +0.00 | $10.88 | -16.80 | -16.80 | +0.00 | -16.80 |
| 2026-08-25 | `INSP` | 30 | — | $61.19 | +0.00 | $61.07 | -3.60 | -3.60 | +0.00 | -3.60 |
| 2026-08-25 | `CRMD` | 221 | — | $8.35 | +0.00 | $8.56 | +46.41 | +46.41 | +0.00 | +46.41 |
| 2026-08-25 | `RZLT` | 375 | — | $4.94 | +0.00 | $5.01 | +26.25 | +26.25 | +0.00 | +26.25 |
| 2026-08-25 | `HCA` | 4 | — | $426.97 | +0.00 | $428.76 | +7.16 | +7.16 | +0.00 | +7.16 |
| 2026-08-26 | `MOS` | 77 | $24.27 | $24.84 | +43.89 | $24.16 | -52.36 | -8.47 | +82.39 | +30.03 |
| 2026-08-26 | `OCUL` | 168 | $10.88 | $10.79 | -15.12 | $10.77 | -3.36 | -18.48 | -31.92 | -35.28 |
| 2026-08-26 | `INSP` | 30 | $61.07 | $60.07 | -30.00 | $61.80 | +51.90 | +21.90 | -33.60 | +18.30 |
| 2026-08-26 | `CRMD` | 221 | $8.56 | $8.60 | +8.84 | $8.39 | -46.41 | -37.57 | +55.25 | +8.84 |
| 2026-08-26 | `RZLT` | 375 | $5.01 | $5.01 | +0.00 | $5.04 | +11.25 | +11.25 | +26.25 | +37.50 |
| 2026-08-26 | `HCA` | 4 | $428.76 | $427.50 | -5.04 | $427.16 | -1.36 | -6.40 | +2.12 | +0.76 |
| 2026-08-27 | `MOS` | 77 | $24.16 | $24.00 | -12.32 | $23.76 | -18.48 | -30.80 | +17.71 | -0.77 |
| 2026-08-27 | `OCUL` | 168 | $10.77 | $10.63 | -23.52 | — | +0.00 | -23.52 | -58.80 | — |
| 2026-08-27 | `INSP` | 30 | $61.80 | $62.10 | +9.00 | — | +0.00 | +9.00 | +27.30 | — |
| 2026-08-27 | `CRMD` | 221 | $8.39 | $8.49 | +22.10 | — | +0.00 | +22.10 | +30.94 | — |
| 2026-08-27 | `RZLT` | 375 | $5.04 | $5.07 | +11.25 | — | +0.00 | +11.25 | +48.75 | — |
| 2026-08-27 | `HCA` | 4 | $427.16 | $424.61 | -10.20 | — | +0.00 | -10.20 | -9.44 | — |
| 2026-08-27 | `RRC` | 74 | — | $41.44 | +0.00 | $41.64 | +14.80 | +14.80 | +0.00 | +14.80 |
| 2026-08-27 | `CRK` | 214 | — | $14.42 | +0.00 | $14.62 | +42.80 | +42.80 | +0.00 | +42.80 |
| 2026-08-27 | `SLI` | 1192 | — | $2.60 | +0.00 | $2.64 | +47.68 | +47.68 | +0.00 | +47.68 |
| 2026-08-28 | `MOS` | 77 | $23.76 | $23.95 | +14.63 | $23.60 | -26.95 | -12.32 | +13.86 | -13.09 |
| 2026-08-28 | `RRC` | 74 | $41.64 | $41.74 | +7.40 | $41.46 | -20.72 | -13.32 | +22.20 | +1.48 |
| 2026-08-28 | `CRK` | 214 | $14.62 | $14.63 | +2.14 | $14.29 | -72.76 | -70.62 | +44.94 | -27.82 |
| 2026-08-28 | `SLI` | 1192 | $2.64 | $2.68 | +47.68 | $2.55 | -154.96 | -107.28 | +95.36 | -59.60 |
| 2026-08-31 | `MOS` | 77 | $23.60 | $23.68 | +6.16 | — | +0.00 | +6.16 | -6.93 | — |
| 2026-08-31 | `RRC` | 74 | $41.46 | $42.00 | +39.96 | — | +0.00 | +39.96 | +41.44 | — |
| 2026-08-31 | `CRK` | 214 | $14.29 | $14.54 | +53.50 | — | +0.00 | +53.50 | +25.68 | — |
| 2026-08-31 | `SLI` | 1192 | $2.55 | $2.58 | +35.76 | — | +0.00 | +35.76 | -23.84 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 42 | — | $52.88 | +0.00 | $52.46 | -17.64 | -17.64 | +0.00 | -17.64 |
| 2026-09-03 | `HRMY` | 51 | — | $42.93 | +0.00 | $41.86 | -54.57 | -54.57 | +0.00 | -54.57 |
| 2026-09-03 | `CABA` | 612 | — | $3.63 | +0.00 | $3.48 | -91.80 | -91.80 | +0.00 | -91.80 |
| 2026-09-03 | `VSTM` | 277 | — | $8.03 | +0.00 | $7.98 | -13.85 | -13.85 | +0.00 | -13.85 |
| 2026-09-03 | `RVTY` | 16 | — | $132.45 | +0.00 | $130.63 | -29.12 | -29.12 | +0.00 | -29.12 |
| 2026-09-04 | `ATRC` | 42 | $52.46 | $52.03 | -18.06 | $51.52 | -21.42 | -39.48 | -35.70 | -57.12 |
| 2026-09-04 | `HRMY` | 51 | $41.86 | $41.50 | -18.36 | — | +0.00 | -18.36 | -72.93 | — |
| 2026-09-04 | `CABA` | 612 | $3.48 | $3.46 | -12.24 | $3.47 | +6.12 | -6.12 | -104.04 | -97.92 |
| 2026-09-04 | `VSTM` | 277 | $7.98 | $7.91 | -19.39 | — | +0.00 | -19.39 | -33.24 | — |
| 2026-09-04 | `RVTY` | 16 | $130.63 | $130.03 | -9.60 | — | +0.00 | -9.60 | -38.72 | — |
| 2026-09-04 | `ALEC` | 430 | — | $2.52 | +0.00 | $2.46 | -25.80 | -25.80 | +0.00 | -25.80 |
| 2026-09-04 | `BHC` | 161 | — | $6.71 | +0.00 | $6.56 | -24.15 | -24.15 | +0.00 | -24.15 |
| 2026-09-04 | `BMEA` | 570 | — | $1.90 | +0.00 | $2.03 | +74.10 | +74.10 | +0.00 | +74.10 |
| 2026-09-04 | `OABI` | 226 | — | $4.78 | +0.00 | $4.33 | -101.70 | -101.70 | +0.00 | -101.70 |
| 2026-09-04 | `OPK` | 682 | — | $1.59 | +0.00 | $1.64 | +34.10 | +34.10 | +0.00 | +34.10 |
| 2026-09-04 | `VIR` | 94 | — | $11.31 | +0.00 | $11.38 | +7.05 | +7.05 | +0.00 | +7.05 |
| 2026-09-08 | `ATRC` | 42 | $51.52 | $54.31 | +117.18 | — | +0.00 | +117.18 | +60.06 | — |
| 2026-09-08 | `CABA` | 612 | $3.47 | $3.43 | -24.48 | — | +0.00 | -24.48 | -122.40 | — |
| 2026-09-08 | `ALEC` | 430 | $2.46 | $2.38 | -34.40 | — | +0.00 | -34.40 | -60.20 | — |
| 2026-09-08 | `BHC` | 161 | $6.56 | $6.57 | +1.61 | — | +0.00 | +1.61 | -22.54 | — |
| 2026-09-08 | `BMEA` | 570 | $2.03 | $2.00 | -17.10 | — | +0.00 | -17.10 | +57.00 | — |
| 2026-09-08 | `OABI` | 226 | $4.33 | $4.30 | -6.78 | — | +0.00 | -6.78 | -108.48 | — |
| 2026-09-08 | `OPK` | 682 | $1.64 | $1.63 | -6.82 | — | +0.00 | -6.82 | +27.28 | — |
| 2026-09-08 | `VIR` | 94 | $11.38 | $11.22 | -15.51 | — | +0.00 | -15.51 | -8.46 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `AUPH` | 109 | — | $16.28 | +0.00 | $16.10 | -19.62 | -19.62 | +0.00 | -19.62 |
| 2026-09-11 | `OVID` | 653 | — | $2.73 | +0.00 | $2.69 | -26.12 | -26.12 | +0.00 | -26.12 |
| 2026-09-11 | `SANM` | 8 | — | $206.84 | +0.00 | $216.00 | +73.28 | +73.28 | +0.00 | +73.28 |
| 2026-09-11 | `ORCL` | 10 | — | $164.43 | +0.00 | $150.28 | -141.50 | -141.50 | +0.00 | -141.50 |
| 2026-09-11 | `NVT` | 11 | — | $157.78 | +0.00 | $162.38 | +50.60 | +50.60 | +0.00 | +50.60 |
| 2026-09-11 | `COHU` | 31 | — | $56.09 | +0.00 | $57.08 | +30.69 | +30.69 | +0.00 | +30.69 |

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
| 2026-08-25 | +1.80 | $11,121.36 | — | $11,121.36 | -0.00 | +97.92 | MOS, OCUL, INSP, CRMD, RZLT, HCA | — | $188.51 | $11,202.79 | MOS×77, OCUL×168, INSP×30, CRMD×221, RZLT×375, HCA×4 |
| 2026-08-26 | +2.02 | $188.51 | MOS×77, OCUL×168, INSP×30, CRMD×221, RZLT×375, HCA×4 | $11,205.36 | +2.57 | -40.34 | — | — | $188.51 | $11,165.02 | MOS×77, OCUL×168, INSP×30, CRMD×221, RZLT×375, HCA×4 |
| 2026-08-27 | — | $188.51 | MOS×77, OCUL×168, INSP×30, CRMD×221, RZLT×375, HCA×4 | $11,161.33 | -3.69 | +86.80 | RRC, CRK, SLI | OCUL, INSP, CRMD, RZLT, HCA | $26.86 | $11,213.30 | MOS×77, RRC×74, CRK×214, SLI×1192 |
| 2026-08-28 | +0.75 | $26.86 | MOS×77, RRC×74, CRK×214, SLI×1192 | $11,285.15 | +71.85 | -275.39 | — | — | $26.86 | $11,009.76 | MOS×77, RRC×74, CRK×214, SLI×1192 |
| 2026-08-31 | -5.85 | $26.86 | MOS×77, RRC×74, CRK×214, SLI×1192 | $11,145.14 | +135.38 | +0.00 | — | MOS, RRC, CRK, SLI | $11,122.22 | $11,122.22 | — |
| 2026-09-01 | -6.30 | $11,122.22 | — | $11,122.22 | +0.00 | +0.00 | — | — | $11,122.22 | $11,122.22 | — |
| 2026-09-02 | -3.83 | $11,122.22 | — | $11,122.22 | +0.00 | +0.00 | — | — | $11,122.22 | $11,122.22 | — |
| 2026-09-03 | -0.90 | $11,122.22 | — | $11,122.22 | +0.00 | -206.98 | ATRC, HRMY, CABA, VSTM, RVTY | — | $129.00 | $10,897.48 | ATRC×42, HRMY×51, CABA×612, VSTM×277, RVTY×16 |
| 2026-09-04 | +2.25 | $129.00 | ATRC×42, HRMY×51, CABA×612, VSTM×277, RVTY×16 | $10,819.83 | -77.65 | -51.70 | ALEC, BHC, BMEA, OABI, OPK, VIR | HRMY, VSTM, RVTY | $5.11 | $10,730.90 | ATRC×42, CABA×612, ALEC×430, BHC×161, BMEA×570, OABI×226, OPK×682, VIR×94 |
| 2026-09-08 | -11.47 | $5.11 | ATRC×42, CABA×612, ALEC×430, BHC×161, BMEA×570, OABI×226, OPK×682, VIR×94 | $10,744.60 | +13.70 | +0.00 | — | ATRC, CABA, ALEC, BHC, BMEA, OABI, OPK, VIR | $10,704.66 | $10,704.66 | — |
| 2026-09-09 | -13.95 | $10,704.66 | — | $10,704.66 | +0.00 | +0.00 | — | — | $10,704.66 | $10,704.66 | — |
| 2026-09-10 | -13.28 | $10,704.66 | — | $10,704.66 | +0.00 | +0.00 | — | — | $10,704.66 | $10,704.66 | — |
| 2026-09-11 | +0.50 | $10,704.66 | — | $10,704.66 | +0.00 | -32.67 | AUPH, OVID, SANM, ORCL, NVT, COHU | — | $355.18 | $10,653.11 | AUPH×109, OVID×653, SANM×8, ORCL×10, NVT×11, COHU×31 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.53 | ▲ close $10,153.12 vs 09:30 $10,000.00 (session +185.07) | 16:00 close · cash $97.53 · equity $10,153.12 vs 09:30 $10,000.00 (+153.12; session marks +185.07) · 8 name(s) marked open→close (per-name table). BTSG×20 09:30 $59.80 → close $60.23 +8.60; IREN×27 09:30 $45.98 → close $44.76 -32.94; TPG×24 09:30 $50.62 → close $54.62 +95.92; TGTX×25 09:30 $49.70 → close $47.94 -44.00; SLS×106 09:30 $11.70 → close $12.36 +69.96; HIMS×42 09:30 $29.74 → close $28.77 -40.74; INO×1543 09:30 $0.81 → close $0.90 +138.87; TNDM×53 09:30 $23.33 → close $23.13 -10.60 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,178.12 vs prior close $10,153.12 (+25.00) · 8 name(s) re-marked at the open (per-name table). BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13 | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $1,288.46 | ▼ -7.12 after sell → book $10,176.05; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $2,476.80 | ▼ -55.19 after sell → book $10,173.96; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $3,801.68 | ▲ +107.86 after sell → book $10,171.88; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 25 | $47.27 | $2.08 | $-64.90 | $4,981.35 | ▼ -64.90 after sell → book $10,169.80; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $6,293.41 | ▲ +69.56 after sell → book $10,167.46; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $7,515.57 | ▼ -29.03 after sell → book $10,165.32; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $8,931.32 | ▲ +148.79 after sell → book $10,146.08; vs 09:30 mark -19.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $10,143.91 | ▼ -26.05 after sell → book $10,143.91; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $9,062.42 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+5.9; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $7,885.21 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+3.6; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $6,683.19 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+0.6; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $5,688.46 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-8.6; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 22 | $57.61 | $2.06 | — | $4,418.98 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.7; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 140 | $9.01 | $2.41 | — | $3,155.17 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-13.5; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1353 | $0.94 | $16.74 | — | $1,870.67 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.5; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 845 | $1.50 | $10.90 | — | $592.27 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $592.27 | ▲ close $10,193.91 vs 09:30 $10,178.12 (session +90.14) | 16:00 close · cash $592.27 · equity $10,193.91 vs 09:30 $10,178.12 (+15.79; session marks +90.14) · 8 name(s) marked open→close (per-name table). TLN×3 09:30 $359.83 → close $362.74 +8.73; VST×8 09:30 $146.90 → close $148.13 +9.84; NRG×10 09:30 $120.00 → close $126.24 +62.40; DAVE×3 09:30 $330.91 → close $334.57 +10.98; SLG×22 09:30 $57.61 → close $56.09 -33.44; MARA×140 09:30 $9.01 → close $9.20 +26.60; LDI×1353 09:30 $0.94 → close $0.90 -54.12; BTBT×845 09:30 $1.50 → close $1.57 +59.15 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $592.27 | ▲ 09:30 equity $10,196.20 vs yday $10,193.91 (+2.29) | 09:30 open · cash $592.27 (unchanged overnight, no fees) · equity $10,196.20 vs prior close $10,193.91 (+2.29) · 8 name(s) re-marked at the open (per-name table). TLN×3 yday $362.74 → 09:30 $367.88 +15.42; VST×8 yday $148.13 → 09:30 $149.37 +9.92; NRG×10 yday $126.24 → 09:30 $127.40 +11.60; DAVE×3 yday $334.57 → 09:30 $336.94 +7.11; SLG×22 yday $56.09 → 09:30 $55.37 -15.84; MARA×140 yday $9.20 → 09:30 $9.22 +2.80; LDI×1353 yday $0.90 → 09:30 $0.91 +13.53; BTBT×845 yday $1.57 → 09:30 $1.52 -42.25 | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 3 | $367.88 | $2.02 | $+20.13 | $1,693.89 | ▲ +20.13 after sell → book $10,194.18; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 8 | $149.37 | $2.03 | $+15.71 | $2,886.82 | ▲ +15.71 after sell → book $10,192.15; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $4,158.78 | ▲ +69.94 after sell → book $10,190.11; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 3 | $336.94 | $2.02 | $+14.07 | $5,167.58 | ▲ +14.07 after sell → book $10,188.09; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 22 | $55.37 | $2.08 | $-53.41 | $6,383.64 | ▼ -53.41 after sell → book $10,186.02; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 140 | $9.22 | $2.44 | $+24.55 | $7,672.00 | ▲ +24.55 after sell → book $10,183.57; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 1353 | $0.91 | $16.57 | $-73.89 | $8,882.61 | ▼ -73.89 after sell → book $10,167.01; vs 09:30 mark -16.56 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 845 | $1.52 | $11.05 | $-5.05 | $10,155.96 | ▼ -5.05 after sell → book $10,155.96; vs 09:30 mark -11.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 27 | $46.18 | $2.07 | — | $8,907.02 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+6.7; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $7,762.85 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+5.8; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $6,544.64 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+8.3; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 313 | $4.05 | $4.04 | — | $5,272.95 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 150 | $8.46 | $2.44 | — | $4,001.51 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.4; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 14 | $90.54 | $2.03 | — | $2,731.92 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=-7.2; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 391 | $3.24 | $5.04 | — | $1,460.04 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 263 | $4.81 | $3.39 | — | $191.62 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-11.4; leftover $1269.49 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $191.62 | ▲ close $10,173.09 vs 09:30 $10,196.20 (session +40.17) | 16:00 close · cash $191.62 · equity $10,173.09 vs 09:30 $10,196.20 (-23.11; session marks +40.17) · 8 name(s) marked open→close (per-name table). DVN×27 09:30 $46.18 → close $47.57 +37.53; EOG×8 09:30 $142.77 → close $146.15 +27.04; FANG×6 09:30 $202.70 → close $206.29 +21.54; TMC×313 09:30 $4.05 → close $3.77 -87.64; TGB×150 09:30 $8.46 → close $8.77 +46.50; ELF×14 09:30 $90.54 → close $93.66 +43.68; DNN×391 09:30 $3.24 → close $3.19 -19.55; HNST×263 09:30 $4.81 → close $4.70 -28.93 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $191.62 | ▼ 09:30 equity $10,124.76 vs yday $10,173.09 (-48.33) | 09:30 open · cash $191.62 (unchanged overnight, no fees) · equity $10,124.76 vs prior close $10,173.09 (-48.33) · 8 name(s) re-marked at the open (per-name table). DVN×27 yday $47.57 → 09:30 $48.00 +11.61; EOG×8 yday $146.15 → 09:30 $148.04 +15.12; FANG×6 yday $206.29 → 09:30 $208.93 +15.84; TMC×313 yday $3.77 → 09:30 $3.72 -15.65; TGB×150 yday $8.77 → 09:30 $8.55 -33.00; ELF×14 yday $93.66 → 09:30 $93.44 -3.08; DNN×391 yday $3.19 → 09:30 $3.11 -31.28; HNST×263 yday $4.70 → 09:30 $4.67 -7.89 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 27 | $48.00 | $2.09 | $+44.98 | $1,485.52 | ▲ +44.98 after sell → book $10,122.66; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 8 | $148.04 | $2.03 | $+38.11 | $2,667.81 | ▲ +38.11 after sell → book $10,120.63; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $3,919.36 | ▲ +33.34 after sell → book $10,118.60; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 313 | $3.72 | $4.10 | $-111.43 | $5,079.62 | ▼ -111.43 after sell → book $10,114.50; vs 09:30 mark -4.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 150 | $8.55 | $2.48 | $+8.58 | $6,359.65 | ▲ +8.58 after sell → book $10,112.03; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 14 | $93.44 | $2.05 | $+36.52 | $7,665.76 | ▲ +36.52 after sell → book $10,109.98; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 391 | $3.11 | $5.12 | $-60.99 | $8,876.65 | ▼ -60.99 after sell → book $10,104.86; vs 09:30 mark -5.12 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `HNST` | 263 | $4.67 | $3.45 | $-43.66 | $10,101.41 | ▼ -43.66 after sell → book $10,101.41; vs 09:30 mark -3.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,101.41 | ▲ close $10,101.41 vs 09:30 $10,124.76 (session +0.00) | 16:00 close · cash $10,101.41 · no lots left · equity $10,101.41. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,101.41 | ▲ 09:30 equity $10,101.41 vs yday $10,101.41 (+0.00) | 09:30 open · cash $10,101.41 · no holdings · equity $10,101.41 vs prior close $10,101.41 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,101.41 | ▲ close $10,101.41 vs 09:30 $10,101.41 (session +0.00) | 16:00 close · cash $10,101.41 · no lots left · equity $10,101.41. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,101.41 | ▲ 09:30 equity $10,101.41 vs yday $10,101.41 (+0.00) | 09:30 open · cash $10,101.41 · no holdings · equity $10,101.41 vs prior close $10,101.41 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 61 | $20.55 | $2.17 | — | $8,845.69 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $1262.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,660.53 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $1262.68 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 61 | $20.65 | $2.17 | — | $6,398.71 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $1262.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 218 | $5.77 | $2.81 | — | $5,138.03 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $1262.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 64 | $19.63 | $2.18 | — | $3,879.53 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $1262.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $2,632.96 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $1262.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 721 | $1.75 | $9.30 | — | $1,361.90 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $1262.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $203.57 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $1262.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $203.57 | ▲ close $10,311.13 vs 09:30 $10,101.41 (session +234.52) | 16:00 close · cash $203.57 · equity $10,311.13 vs 09:30 $10,101.41 (+209.72; session marks +234.52) · 8 name(s) marked open→close (per-name table). AG×61 09:30 $20.55 → close $21.19 +39.04; BHP×13 09:30 $91.01 → close $93.63 +34.06; CDE×61 09:30 $20.65 → close $21.11 +28.06; HDSN×218 09:30 $5.77 → close $5.57 -43.60; IAG×64 09:30 $19.63 → close $20.50 +55.68; KGC×42 09:30 $29.63 → close $31.43 +75.60; NFGC×721 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $203.57 | ▲ 09:30 equity $10,580.85 vs yday $10,311.13 (+269.72) | 09:30 open · cash $203.57 (unchanged overnight, no fees) · equity $10,580.85 vs prior close $10,311.13 (+269.72) · 8 name(s) re-marked at the open (per-name table). AG×61 yday $21.19 → 09:30 $21.90 +43.31; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×61 yday $21.11 → 09:30 $21.75 +39.04; HDSN×218 yday $5.57 → 09:30 $5.67 +21.80; IAG×64 yday $20.50 → 09:30 $21.17 +42.88; KGC×42 yday $31.43 → 09:30 $32.17 +31.08; NFGC×721 yday $1.75 → 09:30 $1.79 +28.84; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 61 | $21.90 | $2.19 | $+77.98 | $1,537.28 | ▲ +77.98 after sell → book $10,578.66; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,779.59 | ▲ +57.15 after sell → book $10,576.61; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 61 | $21.75 | $2.19 | $+62.73 | $4,104.14 | ▲ +62.73 after sell → book $10,574.41; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 218 | $5.67 | $2.86 | $-27.47 | $5,337.35 | ▼ -27.47 after sell → book $10,571.56; vs 09:30 mark -2.85 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 64 | $21.17 | $2.20 | $+94.17 | $6,690.02 | ▲ +94.17 after sell → book $10,569.35; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 42 | $32.17 | $2.14 | $+102.43 | $8,039.02 | ▲ +102.43 after sell → book $10,567.21; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 721 | $1.79 | $9.43 | $+10.11 | $9,320.18 | ▲ +10.11 after sell → book $10,557.78; vs 09:30 mark -9.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $10,555.75 | ▲ +77.23 after sell → book $10,555.75; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 11 | $119.43 | $2.02 | — | $9,240.00 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+21.1; leftover $1319.47 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 76 | $17.20 | $2.22 | — | $7,930.58 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $1319.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 6 | $216.30 | $2.01 | — | $6,630.77 | — | baseline list, no extra gate; list flatten,ohlc_hot,mover_buy; live flatten mover; 🔵; ⚪; ret5=+17.6; leftover $1319.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 118 | $11.13 | $2.34 | — | $5,315.09 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $1319.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 534 | $2.47 | $6.89 | — | $3,989.22 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $1319.47 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 683 | $1.93 | $8.81 | — | $2,662.22 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $1319.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 22 | $59.72 | $2.06 | — | $1,346.32 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.6; leftover $1319.47 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 999 | $1.32 | $12.89 | — | $14.75 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $1319.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.75 | ▲ close $10,781.93 vs 09:30 $10,580.85 (session +265.42) | 16:00 close · cash $14.75 · equity $10,781.93 vs 09:30 $10,580.85 (+201.08; session marks +265.42) · 8 name(s) marked open→close (per-name table). AU×11 09:30 $119.43 → close $121.22 +19.69; AUPH×76 09:30 $17.20 → close $16.65 -41.80; AEM×6 09:30 $216.30 → close $216.06 -1.44; ARCT×118 09:30 $11.13 → close $13.45 +273.76; AUTL×534 09:30 $2.47 → close $2.41 -32.04; CRDL×683 09:30 $1.93 → close $1.86 -47.81; CRSP×22 09:30 $59.72 → close $59.50 -4.84; CYPH×999 09:30 $1.32 → close $1.42 +99.90 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.75 | ▲ 09:30 equity $11,161.11 vs yday $10,781.93 (+379.18) | 09:30 open · cash $14.75 (unchanged overnight, no fees) · equity $11,161.11 vs prior close $10,781.93 (+379.18) · 8 name(s) re-marked at the open (per-name table). AU×11 yday $121.22 → 09:30 $120.51 -7.81; AUPH×76 yday $16.65 → 09:30 $16.57 -6.08; AEM×6 yday $216.06 → 09:30 $217.03 +5.82; ARCT×118 yday $13.45 → 09:30 $13.33 -14.16; AUTL×534 yday $2.41 → 09:30 $2.40 -5.34; CRDL×683 yday $1.86 → 09:30 $1.88 +13.66; CRSP×22 yday $59.50 → 09:30 $58.75 -16.50; CYPH×999 yday $1.42 → 09:30 $1.83 +409.59 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 11 | $120.51 | $2.04 | $+7.81 | $1,338.32 | ▲ +7.81 after sell → book $11,159.07; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 76 | $16.57 | $2.24 | $-52.34 | $2,595.40 | ▼ -52.34 after sell → book $11,156.83; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 6 | $217.03 | $2.03 | $+0.34 | $3,895.55 | ▲ +0.34 after sell → book $11,154.80; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 118 | $13.33 | $2.38 | $+254.88 | $5,466.12 | ▲ +254.88 after sell → book $11,152.43; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 534 | $2.40 | $6.99 | $-51.26 | $6,740.73 | ▼ -51.26 after sell → book $11,145.44; vs 09:30 mark -6.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 683 | $1.88 | $8.93 | $-51.90 | $8,015.83 | ▼ -51.90 after sell → book $11,136.50; vs 09:30 mark -8.94 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 22 | $58.75 | $2.08 | $-25.47 | $9,306.26 | ▼ -25.47 after sell → book $11,134.43; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 999 | $1.83 | $13.07 | $+483.54 | $11,121.36 | ▲ +483.54 after sell → book $11,121.36; vs 09:30 mark -13.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,121.36 | ▲ close $11,121.36 vs 09:30 $11,161.11 (session +0.00) | 16:00 close · cash $11,121.36 · no lots left · equity $11,121.36. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,121.36 | ▲ 09:30 equity $11,121.36 vs yday $11,121.36 (-0.00) | 09:30 open · cash $11,121.36 · no holdings · equity $11,121.36 vs prior close $11,121.36 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 77 | $23.77 | $2.22 | — | $9,288.85 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.0; leftover $1853.56 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 168 | $10.98 | $2.49 | — | $7,441.71 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+1.2; leftover $1853.56 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 30 | $61.19 | $2.08 | — | $5,603.93 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+7.4; leftover $1853.56 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 221 | $8.35 | $2.85 | — | $3,755.73 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.0; leftover $1853.56 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 375 | $4.94 | $4.84 | — | $1,898.40 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; leftover $1853.56 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 4 | $426.97 | $2.00 | — | $188.51 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.0; leftover $1853.56 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $188.51 | ▲ close $11,202.79 vs 09:30 $11,121.36 (session +97.92) | 16:00 close · cash $188.51 · equity $11,202.79 vs 09:30 $11,121.36 (+81.43; session marks +97.92) · 6 name(s) marked open→close (per-name table). MOS×77 09:30 $23.77 → close $24.27 +38.50; OCUL×168 09:30 $10.98 → close $10.88 -16.80; INSP×30 09:30 $61.19 → close $61.07 -3.60; CRMD×221 09:30 $8.35 → close $8.56 +46.41; RZLT×375 09:30 $4.94 → close $5.01 +26.25; HCA×4 09:30 $426.97 → close $428.76 +7.16 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $188.51 | ▲ 09:30 equity $11,205.36 vs yday $11,202.79 (+2.57) | 09:30 open · cash $188.51 (unchanged overnight, no fees) · equity $11,205.36 vs prior close $11,202.79 (+2.57) · 6 name(s) re-marked at the open (per-name table). MOS×77 yday $24.27 → 09:30 $24.84 +43.89; OCUL×168 yday $10.88 → 09:30 $10.79 -15.12; INSP×30 yday $61.07 → 09:30 $60.07 -30.00; CRMD×221 yday $8.56 → 09:30 $8.60 +8.84; RZLT×375 yday $5.01 → 09:30 $5.01 +0.00; HCA×4 yday $428.76 → 09:30 $427.50 -5.04 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $188.51 | ▼ close $11,165.02 vs 09:30 $11,205.36 (session -40.34) | 16:00 close · cash $188.51 · equity $11,165.02 vs 09:30 $11,205.36 (-40.34; session marks -40.34) · 6 name(s) marked open→close (per-name table). MOS×77 09:30 $24.84 → close $24.16 -52.36; OCUL×168 09:30 $10.79 → close $10.77 -3.36; INSP×30 09:30 $60.07 → close $61.80 +51.90; CRMD×221 09:30 $8.60 → close $8.39 -46.41; RZLT×375 09:30 $5.01 → close $5.04 +11.25; HCA×4 09:30 $427.50 → close $427.16 -1.36 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $188.51 | ▼ 09:30 equity $11,161.33 vs yday $11,165.02 (-3.69) | 09:30 open · cash $188.51 (unchanged overnight, no fees) · equity $11,161.33 vs prior close $11,165.02 (-3.69) · 6 name(s) re-marked at the open (per-name table). MOS×77 yday $24.16 → 09:30 $24.00 -12.32; OCUL×168 yday $10.77 → 09:30 $10.63 -23.52; INSP×30 yday $61.80 → 09:30 $62.10 +9.00; CRMD×221 yday $8.39 → 09:30 $8.49 +22.10; RZLT×375 yday $5.04 → 09:30 $5.07 +11.25; HCA×4 yday $427.16 → 09:30 $424.61 -10.20 | — |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 168 | $10.63 | $2.54 | $-63.83 | $1,971.82 | ▼ -63.83 after sell → book $11,158.80; vs 09:30 mark -2.53 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 30 | $62.10 | $2.10 | $+23.12 | $3,832.71 | ▲ +23.12 after sell → book $11,156.69; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 221 | $8.49 | $2.90 | $+25.19 | $5,706.10 | ▲ +25.19 after sell → book $11,153.79; vs 09:30 mark -2.90 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 375 | $5.07 | $4.92 | $+39.00 | $7,602.44 | ▲ +39.00 after sell → book $11,148.88; vs 09:30 mark -4.91 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 4 | $424.61 | $2.03 | $-13.47 | $9,298.85 | ▼ -13.47 after sell → book $11,146.85; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 74 | $41.44 | $2.21 | — | $6,230.08 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.1; leftover $3099.62 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 214 | $14.42 | $2.76 | — | $3,141.44 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; leftover $3099.62 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 1192 | $2.60 | $15.38 | — | $26.86 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+13.0; leftover $3099.62 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26.86 | ▲ close $11,213.30 vs 09:30 $11,161.33 (session +86.80) | 16:00 close · cash $26.86 · equity $11,213.30 vs 09:30 $11,161.33 (+51.97; session marks +86.80) · 4 name(s) marked open→close (per-name table). MOS×77 09:30 $24.00 → close $23.76 -18.48; RRC×74 09:30 $41.44 → close $41.64 +14.80; CRK×214 09:30 $14.42 → close $14.62 +42.80; SLI×1192 09:30 $2.60 → close $2.64 +47.68 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26.86 | ▲ 09:30 equity $11,285.15 vs yday $11,213.30 (+71.85) | 09:30 open · cash $26.86 (unchanged overnight, no fees) · equity $11,285.15 vs prior close $11,213.30 (+71.85) · 4 name(s) re-marked at the open (per-name table). MOS×77 yday $23.76 → 09:30 $23.95 +14.63; RRC×74 yday $41.64 → 09:30 $41.74 +7.40; CRK×214 yday $14.62 → 09:30 $14.63 +2.14; SLI×1192 yday $2.64 → 09:30 $2.68 +47.68 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26.86 | ▼ close $11,009.76 vs 09:30 $11,285.15 (session -275.39) | 16:00 close · cash $26.86 · equity $11,009.76 vs 09:30 $11,285.15 (-275.39; session marks -275.39) · 4 name(s) marked open→close (per-name table). MOS×77 09:30 $23.95 → close $23.60 -26.95; RRC×74 09:30 $41.74 → close $41.46 -20.72; CRK×214 09:30 $14.63 → close $14.29 -72.76; SLI×1192 09:30 $2.68 → close $2.55 -154.96 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26.86 | ▲ 09:30 equity $11,145.14 vs yday $11,009.76 (+135.38) | 09:30 open · cash $26.86 (unchanged overnight, no fees) · equity $11,145.14 vs prior close $11,009.76 (+135.38) · 4 name(s) re-marked at the open (per-name table). MOS×77 yday $23.60 → 09:30 $23.68 +6.16; RRC×74 yday $41.46 → 09:30 $42.00 +39.96; CRK×214 yday $14.29 → 09:30 $14.54 +53.50; SLI×1192 yday $2.55 → 09:30 $2.58 +35.76 | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 77 | $23.68 | $2.25 | $-11.40 | $1,847.97 | ▼ -11.40 after sell → book $11,142.89; vs 09:30 mark -2.25 | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 74 | $42.00 | $2.25 | $+36.98 | $4,953.72 | ▲ +36.98 after sell → book $11,140.64; vs 09:30 mark -2.25 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 214 | $14.54 | $2.82 | $+20.10 | $8,062.46 | ▲ +20.10 after sell → book $11,137.82; vs 09:30 mark -2.82 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 1192 | $2.58 | $15.60 | $-54.82 | $11,122.22 | ▼ -54.82 after sell → book $11,122.22; vs 09:30 mark -15.60 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,122.22 | ▲ close $11,122.22 vs 09:30 $11,145.14 (session +0.00) | 16:00 close · cash $11,122.22 · no lots left · equity $11,122.22. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,122.22 | ▲ 09:30 equity $11,122.22 vs yday $11,122.22 (+0.00) | 09:30 open · cash $11,122.22 · no holdings · equity $11,122.22 vs prior close $11,122.22 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,122.22 | ▲ close $11,122.22 vs 09:30 $11,122.22 (session +0.00) | 16:00 close · cash $11,122.22 · no lots left · equity $11,122.22. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,122.22 | ▲ 09:30 equity $11,122.22 vs yday $11,122.22 (+0.00) | 09:30 open · cash $11,122.22 · no holdings · equity $11,122.22 vs prior close $11,122.22 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,122.22 | ▲ close $11,122.22 vs 09:30 $11,122.22 (session +0.00) | 16:00 close · cash $11,122.22 · no lots left · equity $11,122.22. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,122.22 | ▲ 09:30 equity $11,122.22 vs yday $11,122.22 (+0.00) | 09:30 open · cash $11,122.22 · no holdings · equity $11,122.22 vs prior close $11,122.22 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 42 | $52.88 | $2.12 | — | $8,899.15 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $2224.44 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 51 | $42.93 | $2.14 | — | $6,707.57 | — | baseline list, no extra gate; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.1; leftover $2224.44 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 612 | $3.63 | $7.89 | — | $4,478.12 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.8; leftover $2224.44 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 277 | $8.03 | $3.57 | — | $2,250.24 | — | baseline list, no extra gate; list flatten,ohlc_hot,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.5; leftover $2224.44 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 16 | $132.45 | $2.04 | — | $129.00 | — | baseline list, no extra gate; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.6; leftover $2224.44 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $129.00 | ▼ close $10,897.48 vs 09:30 $11,122.22 (session -206.98) | 16:00 close · cash $129.00 · equity $10,897.48 vs 09:30 $11,122.22 (-224.74; session marks -206.98) · 5 name(s) marked open→close (per-name table). ATRC×42 09:30 $52.88 → close $52.46 -17.64; HRMY×51 09:30 $42.93 → close $41.86 -54.57; CABA×612 09:30 $3.63 → close $3.48 -91.80; VSTM×277 09:30 $8.03 → close $7.98 -13.85; RVTY×16 09:30 $132.45 → close $130.63 -29.12 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.00 | ▼ 09:30 equity $10,819.83 vs yday $10,897.48 (-77.65) | 09:30 open · cash $129.00 (unchanged overnight, no fees) · equity $10,819.83 vs prior close $10,897.48 (-77.65) · 5 name(s) re-marked at the open (per-name table). ATRC×42 yday $52.46 → 09:30 $52.03 -18.06; HRMY×51 yday $41.86 → 09:30 $41.50 -18.36; CABA×612 yday $3.48 → 09:30 $3.46 -12.24; VSTM×277 yday $7.98 → 09:30 $7.91 -19.39; RVTY×16 yday $130.63 → 09:30 $130.03 -9.60 | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 51 | $41.50 | $2.17 | $-77.24 | $2,243.33 | ▼ -77.24 after sell → book $10,817.66; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 277 | $7.91 | $3.64 | $-40.45 | $4,430.76 | ▼ -40.45 after sell → book $10,814.02; vs 09:30 mark -3.64 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 16 | $130.03 | $2.06 | $-42.82 | $6,509.18 | ▼ -42.82 after sell → book $10,811.96; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 430 | $2.52 | $5.55 | — | $5,420.03 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+5.0; leftover $1084.86 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 161 | $6.71 | $2.47 | — | $4,337.25 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $1084.86 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 570 | $1.90 | $7.35 | — | $3,246.89 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.7; leftover $1084.86 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 226 | $4.78 | $2.92 | — | $2,163.70 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $1084.86 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 682 | $1.59 | $8.80 | — | $1,070.52 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $1084.86 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 94 | $11.31 | $2.27 | — | $5.11 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+1.2; leftover $1084.86 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.11 | ▼ close $10,730.90 vs 09:30 $10,819.83 (session -51.70) | 16:00 close · cash $5.11 · equity $10,730.90 vs 09:30 $10,819.83 (-88.93; session marks -51.70) · 8 name(s) marked open→close (per-name table). ATRC×42 09:30 $52.03 → close $51.52 -21.42; CABA×612 09:30 $3.46 → close $3.47 +6.12; ALEC×430 09:30 $2.52 → close $2.46 -25.80; BHC×161 09:30 $6.71 → close $6.56 -24.15; BMEA×570 09:30 $1.90 → close $2.03 +74.10; OABI×226 09:30 $4.78 → close $4.33 -101.70; OPK×682 09:30 $1.59 → close $1.64 +34.10; VIR×94 09:30 $11.31 → close $11.38 +7.05 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.11 | ▲ 09:30 equity $10,744.60 vs yday $10,730.90 (+13.70) | 09:30 open · cash $5.11 (unchanged overnight, no fees) · equity $10,744.60 vs prior close $10,730.90 (+13.70) · 8 name(s) re-marked at the open (per-name table). ATRC×42 yday $51.52 → 09:30 $54.31 +117.18; CABA×612 yday $3.47 → 09:30 $3.43 -24.48; ALEC×430 yday $2.46 → 09:30 $2.38 -34.40; BHC×161 yday $6.56 → 09:30 $6.57 +1.61; BMEA×570 yday $2.03 → 09:30 $2.00 -17.10; OABI×226 yday $4.33 → 09:30 $4.30 -6.78; OPK×682 yday $1.64 → 09:30 $1.63 -6.82; VIR×94 yday $11.38 → 09:30 $11.22 -15.51 | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 42 | $54.31 | $2.14 | $+55.80 | $2,283.98 | ▲ +55.80 after sell → book $10,742.45; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 612 | $3.43 | $8.01 | $-138.31 | $4,375.13 | ▼ -138.31 after sell → book $10,734.44; vs 09:30 mark -8.01 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 430 | $2.38 | $5.63 | $-71.38 | $5,392.90 | ▼ -71.38 after sell → book $10,728.81; vs 09:30 mark -5.63 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 161 | $6.57 | $2.51 | $-27.52 | $6,448.16 | ▼ -27.52 after sell → book $10,726.30; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 570 | $2.00 | $7.46 | $+42.19 | $7,580.71 | ▲ +42.19 after sell → book $10,718.85; vs 09:30 mark -7.45 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 226 | $4.30 | $2.96 | $-114.36 | $8,549.54 | ▼ -114.36 after sell → book $10,715.88; vs 09:30 mark -2.97 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 682 | $1.63 | $8.92 | $+9.56 | $9,652.28 | ▲ +9.56 after sell → book $10,706.96; vs 09:30 mark -8.92 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 94 | $11.22 | $2.30 | $-13.03 | $10,704.66 | ▼ -13.03 after sell → book $10,704.66; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,704.66 | ▲ close $10,704.66 vs 09:30 $10,744.60 (session +0.00) | 16:00 close · cash $10,704.66 · no lots left · equity $10,704.66. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,704.66 | ▲ 09:30 equity $10,704.66 vs yday $10,704.66 (+0.00) | 09:30 open · cash $10,704.66 · no holdings · equity $10,704.66 vs prior close $10,704.66 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,704.66 | ▲ close $10,704.66 vs 09:30 $10,704.66 (session +0.00) | 16:00 close · cash $10,704.66 · no lots left · equity $10,704.66. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,704.66 | ▲ 09:30 equity $10,704.66 vs yday $10,704.66 (+0.00) | 09:30 open · cash $10,704.66 · no holdings · equity $10,704.66 vs prior close $10,704.66 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,704.66 | ▲ close $10,704.66 vs 09:30 $10,704.66 (session +0.00) | 16:00 close · cash $10,704.66 · no lots left · equity $10,704.66. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,704.66 | ▲ 09:30 equity $10,704.66 vs yday $10,704.66 (+0.00) | 09:30 open · cash $10,704.66 · no holdings · equity $10,704.66 vs prior close $10,704.66 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 109 | $16.28 | $2.32 | — | $8,927.83 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-1.1; leftover $1784.11 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 653 | $2.73 | $8.42 | — | $7,136.71 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+3.0; leftover $1784.11 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 8 | $206.84 | $2.01 | — | $5,479.98 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.4; leftover $1784.11 | join🟢 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 10 | $164.43 | $2.02 | — | $3,833.66 | — | baseline list, no extra gate; list flatten,earn_react; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.0; leftover $1784.11 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 11 | $157.78 | $2.02 | — | $2,096.06 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+7.8; leftover $1784.11 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 31 | $56.09 | $2.08 | — | $355.18 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.3; leftover $1784.11 | join🟢 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $355.18 | ▼ close $10,653.11 vs 09:30 $10,704.66 (session -32.67) | 16:00 close · cash $355.18 · equity $10,653.11 vs 09:30 $10,704.66 (-51.55; session marks -32.67) · 6 name(s) marked open→close (per-name table). AUPH×109 09:30 $16.28 → close $16.10 -19.62; OVID×653 09:30 $2.73 → close $2.69 -26.12; SANM×8 09:30 $206.84 → close $216.00 +73.28; ORCL×10 09:30 $164.43 → close $150.28 -141.50; NVT×11 09:30 $157.78 → close $162.38 +50.60; COHU×31 09:30 $56.09 → close $57.08 +30.69 | — |

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
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
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
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SID` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `PCG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OIS` | hard_red | hard-red S=-13.28 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `AUPH` | 109 | 2026-09-11 @ $16.28 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-1.1; leftover $1784.11 |
| `OVID` | 653 | 2026-09-11 @ $2.73 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+3.0; leftover $1784.11 |
| `SANM` | 8 | 2026-09-11 @ $206.84 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.4; leftover $1784.11 |
| `ORCL` | 10 | 2026-09-11 @ $164.43 | baseline list, no extra gate; list flatten,earn_react; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.0; leftover $1784.11 |
| `NVT` | 11 | 2026-09-11 @ $157.78 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+7.8; leftover $1784.11 |
| `COHU` | 31 | 2026-09-11 @ $56.09 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.3; leftover $1784.11 |
