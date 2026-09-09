# Factor mine action — `union_w_hot_candle_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `w_hot_candle` · size `leftover` · sell `list` · S-boost `none` · rank by w_hot_candle

Cash book **+0.06%** ($10,006) · signal-only (no cash/fees) was +16.19%. Starts YES **7/18**. Fills 162 · skips 54 · realized $+5.70.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: a mix of tape-heat and prior candles.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is thrown out if any must-not is true.
- Sort the keepers by a mix of tape-heat and prior candles and keep the top 8.
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

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `w_hot_candle` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,005.63.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `TNDM` | 53 | — | $23.33 | +0.00 | $23.13 | -10.60 | -10.60 | +0.00 | -10.60 |
| 2026-08-13 | `IREN` | 27 | — | $45.98 | +0.00 | $44.76 | -32.94 | -32.94 | +0.00 | -32.94 |
| 2026-08-13 | `TPG` | 24 | — | $50.62 | +0.00 | $54.62 | +95.92 | +95.92 | +0.00 | +95.92 |
| 2026-08-13 | `HIMS` | 42 | — | $29.74 | +0.00 | $28.77 | -40.74 | -40.74 | +0.00 | -40.74 |
| 2026-08-13 | `INO` | 1543 | — | $0.81 | +0.00 | $0.90 | +138.87 | +138.87 | +0.00 | +138.87 |
| 2026-08-13 | `VOR` | 56 | — | $22.01 | +0.00 | $23.29 | +71.68 | +71.68 | +0.00 | +71.68 |
| 2026-08-13 | `SLS` | 106 | — | $11.70 | +0.00 | $12.36 | +69.96 | +69.96 | +0.00 | +69.96 |
| 2026-08-13 | `BTSG` | 20 | — | $59.80 | +0.00 | $60.23 | +8.60 | +8.60 | +0.00 | +8.60 |
| 2026-08-14 | `TNDM` | 53 | $23.13 | $22.92 | -11.13 | — | +0.00 | -11.13 | -21.73 | — |
| 2026-08-14 | `IREN` | 27 | $44.76 | $44.09 | -18.09 | — | +0.00 | -18.09 | -51.03 | — |
| 2026-08-14 | `TPG` | 24 | $54.62 | $55.29 | +16.08 | — | +0.00 | +16.08 | +112.00 | — |
| 2026-08-14 | `HIMS` | 42 | $28.77 | $29.15 | +15.96 | — | +0.00 | +15.96 | -24.78 | — |
| 2026-08-14 | `INO` | 1543 | $0.90 | $0.93 | +46.29 | — | +0.00 | +46.29 | +185.16 | — |
| 2026-08-14 | `VOR` | 56 | $23.29 | $23.33 | +2.24 | — | +0.00 | +2.24 | +73.92 | — |
| 2026-08-14 | `SLS` | 106 | $12.36 | $12.40 | +4.24 | — | +0.00 | +4.24 | +74.20 | — |
| 2026-08-14 | `BTSG` | 20 | $60.23 | $59.65 | -11.60 | — | +0.00 | -11.60 | -3.00 | — |
| 2026-08-14 | `QMCO` | 52 | — | $24.68 | +0.00 | $26.11 | +74.36 | +74.36 | +0.00 | +74.36 |
| 2026-08-14 | `ZENA` | 583 | — | $2.20 | +0.00 | $2.14 | -34.98 | -34.98 | +0.00 | -34.98 |
| 2026-08-14 | `AIRO` | 115 | — | $11.12 | +0.00 | $9.57 | -178.25 | -178.25 | +0.00 | -178.25 |
| 2026-08-14 | `ARX` | 65 | — | $19.57 | +0.00 | $19.58 | +0.65 | +0.65 | +0.00 | +0.65 |
| 2026-08-14 | `LIFE` | 36 | — | $35.04 | +0.00 | $34.02 | -36.72 | -36.72 | +0.00 | -36.72 |
| 2026-08-14 | `BETA` | 50 | — | $25.21 | +0.00 | $24.86 | -17.50 | -17.50 | +0.00 | -17.50 |
| 2026-08-14 | `LUNR` | 67 | — | $19.17 | +0.00 | $19.01 | -10.72 | -10.72 | +0.00 | -10.72 |
| 2026-08-14 | `VOYG` | 28 | — | $44.49 | +0.00 | $42.98 | -42.28 | -42.28 | +0.00 | -42.28 |
| 2026-08-17 | `QMCO` | 52 | $26.11 | $24.83 | -66.56 | — | +0.00 | -66.56 | +7.80 | — |
| 2026-08-17 | `ZENA` | 583 | $2.14 | $2.08 | -32.07 | — | +0.00 | -32.07 | -67.05 | — |
| 2026-08-17 | `AIRO` | 115 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -178.25 | — |
| 2026-08-17 | `ARX` | 65 | $19.58 | $19.57 | -0.65 | — | +0.00 | -0.65 | +0.00 | — |
| 2026-08-17 | `LIFE` | 36 | $34.02 | $34.03 | +0.36 | — | +0.00 | +0.36 | -36.36 | — |
| 2026-08-17 | `BETA` | 50 | $24.86 | $24.61 | -12.50 | — | +0.00 | -12.50 | -30.00 | — |
| 2026-08-17 | `LUNR` | 67 | $19.01 | $20.25 | +83.08 | — | +0.00 | +83.08 | +72.36 | — |
| 2026-08-17 | `VOYG` | 28 | $42.98 | $42.12 | -24.08 | — | +0.00 | -24.08 | -66.36 | — |
| 2026-08-17 | `XHG` | 296 | — | $4.19 | +0.00 | $3.91 | -82.88 | -82.88 | +0.00 | -82.88 |
| 2026-08-17 | `STDN` | 91 | — | $13.64 | +0.00 | $13.31 | -30.03 | -30.03 | +0.00 | -30.03 |
| 2026-08-17 | `HTFL` | 30 | — | $41.23 | +0.00 | $41.94 | +21.30 | +21.30 | +0.00 | +21.30 |
| 2026-08-17 | `SMJF` | 122 | — | $10.10 | +0.00 | $10.45 | +42.70 | +42.70 | +0.00 | +42.70 |
| 2026-08-17 | `NPWR` | 646 | — | $1.92 | +0.00 | $1.73 | -122.74 | -122.74 | +0.00 | -122.74 |
| 2026-08-17 | `NMAX` | 113 | — | $10.97 | +0.00 | $10.36 | -68.93 | -68.93 | +0.00 | -68.93 |
| 2026-08-17 | `CAPR` | 180 | — | $6.87 | +0.00 | $7.45 | +104.40 | +104.40 | +0.00 | +104.40 |
| 2026-08-17 | `UMAC` | 38 | — | $32.55 | +0.00 | $30.15 | -91.20 | -91.20 | +0.00 | -91.20 |
| 2026-08-18 | `XHG` | 296 | $3.91 | $3.94 | +8.88 | — | +0.00 | +8.88 | -74.00 | — |
| 2026-08-18 | `STDN` | 91 | $13.31 | $13.31 | +0.00 | — | +0.00 | +0.00 | -30.03 | — |
| 2026-08-18 | `HTFL` | 30 | $41.94 | $41.50 | -13.20 | — | +0.00 | -13.20 | +8.10 | — |
| 2026-08-18 | `SMJF` | 122 | $10.45 | $10.45 | +0.00 | — | +0.00 | +0.00 | +42.70 | — |
| 2026-08-18 | `NPWR` | 646 | $1.73 | $1.70 | -19.38 | — | +0.00 | -19.38 | -142.12 | — |
| 2026-08-18 | `NMAX` | 113 | $10.36 | $10.31 | -5.65 | — | +0.00 | -5.65 | -74.58 | — |
| 2026-08-18 | `CAPR` | 180 | $7.45 | $7.50 | +9.00 | $7.08 | -75.60 | -66.60 | +113.40 | +37.80 |
| 2026-08-18 | `UMAC` | 38 | $30.15 | $28.59 | -59.28 | — | +0.00 | -59.28 | -150.48 | — |
| 2026-08-19 | `CAPR` | 180 | $7.08 | $7.19 | +19.80 | — | +0.00 | +19.80 | +57.60 | — |
| 2026-08-20 | `MRNA` | 7 | — | $150.14 | +0.00 | $133.32 | -117.74 | -117.74 | +0.00 | -117.74 |
| 2026-08-20 | `CYPH` | 1034 | — | $1.15 | +0.00 | $1.19 | +41.36 | +41.36 | +0.00 | +41.36 |
| 2026-08-20 | `ABCL` | 100 | — | $11.81 | +0.00 | $11.57 | -24.50 | -24.50 | +0.00 | -24.50 |
| 2026-08-20 | `SENS` | 133 | — | $8.91 | +0.00 | $8.82 | -11.97 | -11.97 | +0.00 | -11.97 |
| 2026-08-20 | `ALEC` | 495 | — | $2.40 | +0.00 | $2.26 | -69.30 | -69.30 | +0.00 | -69.30 |
| 2026-08-20 | `BTGO` | 180 | — | $6.61 | +0.00 | $6.60 | -0.90 | -0.90 | +0.00 | -0.90 |
| 2026-08-20 | `IMMX` | 91 | — | $12.98 | +0.00 | $13.16 | +16.38 | +16.38 | +0.00 | +16.38 |
| 2026-08-20 | `BBNX` | 59 | — | $20.00 | +0.00 | $19.48 | -30.68 | -30.68 | +0.00 | -30.68 |
| 2026-08-21 | `MRNA` | 7 | $133.32 | $133.11 | -1.47 | $145.13 | +84.14 | +82.67 | -119.21 | -35.07 |
| 2026-08-21 | `CYPH` | 1034 | $1.19 | $1.32 | +134.42 | $1.42 | +103.40 | +237.82 | +175.78 | +279.18 |
| 2026-08-21 | `ABCL` | 100 | $11.57 | $11.57 | +0.00 | — | +0.00 | +0.00 | -24.50 | — |
| 2026-08-21 | `SENS` | 133 | $8.82 | $9.24 | +55.86 | — | +0.00 | +55.86 | +43.89 | — |
| 2026-08-21 | `ALEC` | 495 | $2.26 | $2.28 | +9.90 | — | +0.00 | +9.90 | -59.40 | — |
| 2026-08-21 | `BTGO` | 180 | $6.60 | $6.95 | +63.00 | — | +0.00 | +63.00 | +62.10 | — |
| 2026-08-21 | `IMMX` | 91 | $13.16 | $13.36 | +18.20 | — | +0.00 | +18.20 | +34.58 | — |
| 2026-08-21 | `BBNX` | 59 | $19.48 | $19.50 | +1.18 | — | +0.00 | +1.18 | -29.50 | — |
| 2026-08-21 | `XHG` | 269 | — | $4.49 | +0.00 | $4.41 | -21.52 | -21.52 | +0.00 | -21.52 |
| 2026-08-21 | `ARCT` | 108 | — | $11.13 | +0.00 | $13.45 | +250.56 | +250.56 | +0.00 | +250.56 |
| 2026-08-21 | `IOVA` | 133 | — | $9.08 | +0.00 | $8.29 | -105.07 | -105.07 | +0.00 | -105.07 |
| 2026-08-21 | `DFDV` | 299 | — | $4.04 | +0.00 | $3.94 | -29.90 | -29.90 | +0.00 | -29.90 |
| 2026-08-21 | `MRVI` | 146 | — | $8.28 | +0.00 | $8.64 | +52.56 | +52.56 | +0.00 | +52.56 |
| 2026-08-21 | `XXI` | 187 | — | $6.42 | +0.00 | $6.49 | +13.09 | +13.09 | +0.00 | +13.09 |
| 2026-08-24 | `MRNA` | 7 | $145.13 | $142.70 | -17.01 | — | +0.00 | -17.01 | -52.08 | — |
| 2026-08-24 | `CYPH` | 1034 | $1.42 | $1.83 | +423.94 | — | +0.00 | +423.94 | +703.12 | — |
| 2026-08-24 | `XHG` | 269 | $4.41 | $4.32 | -24.21 | — | +0.00 | -24.21 | -45.73 | — |
| 2026-08-24 | `ARCT` | 108 | $13.45 | $13.33 | -12.96 | — | +0.00 | -12.96 | +237.60 | — |
| 2026-08-24 | `IOVA` | 133 | $8.29 | $8.08 | -27.93 | — | +0.00 | -27.93 | -133.00 | — |
| 2026-08-24 | `DFDV` | 299 | $3.94 | $4.16 | +65.78 | — | +0.00 | +65.78 | +35.88 | — |
| 2026-08-24 | `MRVI` | 146 | $8.64 | $8.59 | -7.30 | — | +0.00 | -7.30 | +45.26 | — |
| 2026-08-24 | `XXI` | 187 | $6.49 | $6.64 | +28.98 | — | +0.00 | +28.98 | +42.07 | — |
| 2026-08-25 | `REAX` | 53 | — | $24.11 | +0.00 | $28.43 | +228.96 | +228.96 | +0.00 | +228.96 |
| 2026-08-25 | `CYPH` | 823 | — | $1.56 | +0.00 | $1.64 | +65.84 | +65.84 | +0.00 | +65.84 |
| 2026-08-25 | `XHG` | 315 | — | $4.07 | +0.00 | $4.02 | -15.75 | -15.75 | +0.00 | -15.75 |
| 2026-08-25 | `ASST` | 67 | — | $19.04 | +0.00 | $21.39 | +157.45 | +157.45 | +0.00 | +157.45 |
| 2026-08-25 | `ALVO` | 245 | — | $5.24 | +0.00 | $5.05 | -46.55 | -46.55 | +0.00 | -46.55 |
| 2026-08-25 | `SUJA` | 146 | — | $8.79 | +0.00 | $9.33 | +78.84 | +78.84 | +0.00 | +78.84 |
| 2026-08-25 | `BMNR` | 53 | — | $23.80 | +0.00 | $24.82 | +54.06 | +54.06 | +0.00 | +54.06 |
| 2026-08-25 | `GORO` | 361 | — | $3.55 | +0.00 | $3.87 | +115.52 | +115.52 | +0.00 | +115.52 |
| 2026-08-26 | `REAX` | 53 | $28.43 | $26.61 | -96.46 | — | +0.00 | -96.46 | +132.50 | — |
| 2026-08-26 | `CYPH` | 823 | $1.64 | $1.60 | -32.92 | — | +0.00 | -32.92 | +32.92 | — |
| 2026-08-26 | `XHG` | 315 | $4.02 | $3.81 | -66.15 | $4.06 | +78.75 | +12.60 | -81.90 | -3.15 |
| 2026-08-26 | `ASST` | 67 | $21.39 | $20.72 | -44.89 | — | +0.00 | -44.89 | +112.56 | — |
| 2026-08-26 | `ALVO` | 245 | $5.05 | $4.98 | -17.15 | — | +0.00 | -17.15 | -63.70 | — |
| 2026-08-26 | `SUJA` | 146 | $9.33 | $9.39 | +8.76 | $9.44 | +7.30 | +16.06 | +87.60 | +94.90 |
| 2026-08-26 | `BMNR` | 53 | $24.82 | $24.24 | -30.74 | — | +0.00 | -30.74 | +23.32 | — |
| 2026-08-26 | `GORO` | 361 | $3.87 | $3.77 | -36.10 | — | +0.00 | -36.10 | +79.42 | — |
| 2026-08-26 | `BYND` | 94 | — | $14.11 | +0.00 | $14.25 | +13.16 | +13.16 | +0.00 | +13.16 |
| 2026-08-26 | `USDE` | 228 | — | $5.81 | +0.00 | $5.98 | +38.76 | +38.76 | +0.00 | +38.76 |
| 2026-08-26 | `MNRO` | 94 | — | $14.00 | +0.00 | $12.61 | -130.66 | -130.66 | +0.00 | -130.66 |
| 2026-08-26 | `FIGR` | 32 | — | $40.50 | +0.00 | $37.08 | -109.44 | -109.44 | +0.00 | -109.44 |
| 2026-08-26 | `TRLV` | 118 | — | $11.22 | +0.00 | $11.43 | +24.78 | +24.78 | +0.00 | +24.78 |
| 2026-08-26 | `VIR` | 125 | — | $10.60 | +0.00 | $11.08 | +60.00 | +60.00 | +0.00 | +60.00 |
| 2026-08-27 | `XHG` | 315 | $4.06 | $4.06 | +0.00 | — | +0.00 | +0.00 | -3.15 | — |
| 2026-08-27 | `SUJA` | 146 | $9.44 | $9.41 | -4.38 | — | +0.00 | -4.38 | +90.52 | — |
| 2026-08-27 | `BYND` | 94 | $14.25 | $14.20 | -4.70 | — | +0.00 | -4.70 | +8.46 | — |
| 2026-08-27 | `USDE` | 228 | $5.98 | $6.50 | +118.56 | — | +0.00 | +118.56 | +157.32 | — |
| 2026-08-27 | `MNRO` | 94 | $12.61 | $12.56 | -4.70 | — | +0.00 | -4.70 | -135.36 | — |
| 2026-08-27 | `FIGR` | 32 | $37.08 | $37.42 | +10.88 | — | +0.00 | +10.88 | -98.56 | — |
| 2026-08-27 | `TRLV` | 118 | $11.43 | $11.38 | -5.90 | — | +0.00 | -5.90 | +18.88 | — |
| 2026-08-27 | `VIR` | 125 | $11.08 | $11.00 | -10.00 | — | +0.00 | -10.00 | +50.00 | — |
| 2026-08-27 | `GEN` | 44 | — | $29.83 | +0.00 | $30.50 | +29.48 | +29.48 | +0.00 | +29.48 |
| 2026-08-27 | `SLI` | 509 | — | $2.60 | +0.00 | $2.64 | +20.36 | +20.36 | +0.00 | +20.36 |
| 2026-08-27 | `RRC` | 31 | — | $41.44 | +0.00 | $41.64 | +6.20 | +6.20 | +0.00 | +6.20 |
| 2026-08-27 | `PGY` | 57 | — | $22.93 | +0.00 | $23.26 | +18.81 | +18.81 | +0.00 | +18.81 |
| 2026-08-27 | `DLO` | 86 | — | $15.33 | +0.00 | $15.14 | -16.34 | -16.34 | +0.00 | -16.34 |
| 2026-08-27 | `ANET` | 6 | — | $205.90 | +0.00 | $201.09 | -28.86 | -28.86 | +0.00 | -28.86 |
| 2026-08-27 | `CRK` | 91 | — | $14.42 | +0.00 | $14.62 | +18.20 | +18.20 | +0.00 | +18.20 |
| 2026-08-27 | `PLTR` | 7 | — | $178.75 | +0.00 | $185.93 | +50.26 | +50.26 | +0.00 | +50.26 |
| 2026-08-28 | `GEN` | 44 | $30.50 | $30.50 | +0.00 | — | +0.00 | +0.00 | +29.48 | — |
| 2026-08-28 | `SLI` | 509 | $2.64 | $2.68 | +20.36 | — | +0.00 | +20.36 | +40.72 | — |
| 2026-08-28 | `RRC` | 31 | $41.64 | $41.74 | +3.10 | — | +0.00 | +3.10 | +9.30 | — |
| 2026-08-28 | `PGY` | 57 | $23.26 | $23.21 | -2.85 | — | +0.00 | -2.85 | +15.96 | — |
| 2026-08-28 | `DLO` | 86 | $15.14 | $15.19 | +4.30 | — | +0.00 | +4.30 | -12.04 | — |
| 2026-08-28 | `ANET` | 6 | $201.09 | $200.00 | -6.54 | — | +0.00 | -6.54 | -35.40 | — |
| 2026-08-28 | `CRK` | 91 | $14.62 | $14.63 | +0.91 | — | +0.00 | +0.91 | +19.11 | — |
| 2026-08-28 | `PLTR` | 7 | $185.93 | $184.95 | -6.86 | — | +0.00 | -6.86 | +43.40 | — |
| 2026-08-28 | `BYND` | 95 | — | $14.00 | +0.00 | $13.86 | -13.30 | -13.30 | +0.00 | -13.30 |
| 2026-08-28 | `CAPR` | 136 | — | $9.73 | +0.00 | $9.59 | -19.04 | -19.04 | +0.00 | -19.04 |
| 2026-08-28 | `ANF` | 9 | — | $146.07 | +0.00 | $148.42 | +21.15 | +21.15 | +0.00 | +21.15 |
| 2026-08-28 | `NEO` | 72 | — | $18.36 | +0.00 | $18.05 | -22.32 | -22.32 | +0.00 | -22.32 |
| 2026-08-28 | `EL` | 12 | — | $106.99 | +0.00 | $103.39 | -43.20 | -43.20 | +0.00 | -43.20 |
| 2026-08-28 | `VYX` | 145 | — | $9.13 | +0.00 | $8.78 | -50.75 | -50.75 | +0.00 | -50.75 |
| 2026-08-28 | `FIG` | 44 | — | $30.18 | +0.00 | $28.82 | -59.84 | -59.84 | +0.00 | -59.84 |
| 2026-08-28 | `NCNO` | 57 | — | $23.30 | +0.00 | $22.99 | -17.67 | -17.67 | +0.00 | -17.67 |
| 2026-08-31 | `BYND` | 95 | $13.86 | $13.81 | -4.75 | $13.30 | -48.45 | -53.20 | -18.05 | -66.50 |
| 2026-08-31 | `CAPR` | 136 | $9.59 | $9.50 | -12.24 | — | +0.00 | -12.24 | -31.28 | — |
| 2026-08-31 | `ANF` | 9 | $148.42 | $148.03 | -3.51 | — | +0.00 | -3.51 | +17.64 | — |
| 2026-08-31 | `NEO` | 72 | $18.05 | $17.77 | -20.16 | — | +0.00 | -20.16 | -42.48 | — |
| 2026-08-31 | `EL` | 12 | $103.39 | $102.70 | -8.28 | — | +0.00 | -8.28 | -51.48 | — |
| 2026-08-31 | `VYX` | 145 | $8.78 | $8.66 | -17.40 | — | +0.00 | -17.40 | -68.15 | — |
| 2026-08-31 | `FIG` | 44 | $28.82 | $27.60 | -53.68 | — | +0.00 | -53.68 | -113.52 | — |
| 2026-08-31 | `NCNO` | 57 | $22.99 | $22.66 | -18.81 | — | +0.00 | -18.81 | -36.48 | — |
| 2026-09-01 | `BYND` | 95 | $13.30 | $13.04 | -24.70 | — | +0.00 | -24.70 | -91.20 | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `GPRO` | 716 | — | $1.78 | +0.00 | $1.39 | -279.24 | -279.24 | +0.00 | -279.24 |
| 2026-09-03 | `REAX` | 69 | — | $18.40 | +0.00 | $18.40 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `AGCO` | 9 | — | $127.91 | +0.00 | $125.82 | -18.81 | -18.81 | +0.00 | -18.81 |
| 2026-09-03 | `ASST` | 49 | — | $25.62 | +0.00 | $26.82 | +58.56 | +58.56 | +0.00 | +58.56 |
| 2026-09-03 | `SID` | 938 | — | $1.36 | +0.00 | $1.26 | -93.80 | -93.80 | +0.00 | -93.80 |
| 2026-09-03 | `TARS` | 15 | — | $82.76 | +0.00 | $83.20 | +6.67 | +6.67 | +0.00 | +6.67 |
| 2026-09-03 | `CNDT` | 671 | — | $1.90 | +0.00 | $1.89 | -6.71 | -6.71 | +0.00 | -6.71 |
| 2026-09-03 | `RSKD` | 190 | — | $6.68 | +0.00 | $6.93 | +47.50 | +47.50 | +0.00 | +47.50 |
| 2026-09-04 | `GPRO` | 716 | $1.39 | $1.48 | +64.44 | $1.70 | +157.52 | +221.96 | -214.80 | -57.28 |
| 2026-09-04 | `REAX` | 69 | $18.40 | $18.15 | -17.25 | — | +0.00 | -17.25 | -17.25 | — |
| 2026-09-04 | `AGCO` | 9 | $125.82 | $125.22 | -5.40 | — | +0.00 | -5.40 | -24.21 | — |
| 2026-09-04 | `ASST` | 49 | $26.82 | $25.18 | -80.36 | $27.14 | +96.04 | +15.68 | -21.81 | +74.24 |
| 2026-09-04 | `SID` | 938 | $1.26 | $1.23 | -28.14 | — | +0.00 | -28.14 | -121.94 | — |
| 2026-09-04 | `TARS` | 15 | $83.20 | $82.70 | -7.57 | $90.78 | +121.20 | +113.63 | -0.90 | +120.30 |
| 2026-09-04 | `CNDT` | 671 | $1.89 | $1.90 | +6.71 | — | +0.00 | +6.71 | +0.00 | — |
| 2026-09-04 | `RSKD` | 190 | $6.93 | $6.84 | -17.10 | — | +0.00 | -17.10 | +30.40 | — |
| 2026-09-04 | `DFDV` | 215 | — | $5.79 | +0.00 | $5.87 | +17.20 | +17.20 | +0.00 | +17.20 |
| 2026-09-04 | `USDE` | 158 | — | $7.87 | +0.00 | $7.93 | +9.48 | +9.48 | +0.00 | +9.48 |
| 2026-09-04 | `FRNM` | 76 | — | $16.40 | +0.00 | $16.31 | -6.84 | -6.84 | +0.00 | -6.84 |
| 2026-09-04 | `LENZ` | 216 | — | $5.75 | +0.00 | $5.96 | +45.36 | +45.36 | +0.00 | +45.36 |
| 2026-09-04 | `PAGS` | 124 | — | $9.96 | +0.00 | $9.73 | -28.52 | -28.52 | +0.00 | -28.52 |
| 2026-09-08 | `GPRO` | 716 | $1.70 | $1.56 | -96.66 | — | +0.00 | -96.66 | -153.94 | — |
| 2026-09-08 | `ASST` | 49 | $27.14 | $26.44 | -34.30 | — | +0.00 | -34.30 | +39.94 | — |
| 2026-09-08 | `TARS` | 15 | $90.78 | $89.67 | -16.65 | — | +0.00 | -16.65 | +103.65 | — |
| 2026-09-08 | `DFDV` | 215 | $5.87 | $5.81 | -12.90 | — | +0.00 | -12.90 | +4.30 | — |
| 2026-09-08 | `USDE` | 158 | $7.93 | $7.76 | -26.86 | — | +0.00 | -26.86 | -17.38 | — |
| 2026-09-08 | `FRNM` | 76 | $16.31 | $16.74 | +32.68 | — | +0.00 | +32.68 | +25.84 | — |
| 2026-09-08 | `LENZ` | 216 | $5.96 | $5.95 | -2.16 | — | +0.00 | -2.16 | +43.20 | — |
| 2026-09-08 | `PAGS` | 124 | $9.73 | $9.91 | +22.32 | — | +0.00 | +22.32 | -6.20 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +300.75 | TNDM, IREN, TPG, HIMS, INO, VOR, SLS, BTSG | — | $107.38 | $10,268.71 | TNDM×53, IREN×27, TPG×24, HIMS×42, INO×1543, VOR×56, SLS×106, BTSG×20 |
| 2026-08-14 | +5.50 | $107.38 | TNDM×53, IREN×27, TPG×24, HIMS×42, INO×1543, VOR×56, SLS×106, BTSG×20 | $10,312.70 | +43.99 | -245.44 | QMCO, ZENA, AIRO, ARX, LIFE, BETA, LUNR, VOYG | TNDM, IREN, TPG, HIMS, INO, VOR, SLS, BTSG | $86.84 | $10,010.26 | QMCO×52, ZENA×583, AIRO×115, ARX×65, LIFE×36, BETA×50, LUNR×67, VOYG×28 |
| 2026-08-17 | +2.25 | $86.84 | QMCO×52, ZENA×583, AIRO×115, ARX×65, LIFE×36, BETA×50, LUNR×67, VOYG×28 | $9,957.85 | -52.41 | -227.38 | XHG, STDN, HTFL, SMJF, NPWR, NMAX, CAPR, UMAC | QMCO, ZENA, AIRO, ARX, LIFE, BETA, LUNR, VOYG | $5.07 | $9,681.70 | XHG×296, STDN×91, HTFL×30, SMJF×122, NPWR×646, NMAX×113, CAPR×180, UMAC×38 |
| 2026-08-18 | -6.20 | $5.07 | XHG×296, STDN×91, HTFL×30, SMJF×122, NPWR×646, NMAX×113, CAPR×180, UMAC×38 | $9,602.07 | -79.63 | -75.60 | — | XHG, STDN, HTFL, SMJF, NPWR, NMAX, UMAC | $8,228.49 | $9,502.89 | CAPR×180 |
| 2026-08-19 | -7.20 | $8,228.49 | CAPR×180 | $9,522.69 | +19.80 | +0.00 | — | CAPR | $9,520.12 | $9,520.12 | — |
| 2026-08-20 | +1.12 | $9,520.12 | — | $9,520.12 | +0.00 | -197.35 | MRNA, CYPH, ABCL, SENS, ALEC, BTGO, IMMX, BBNX | — | $142.06 | $9,289.40 | MRNA×7, CYPH×1034, ABCL×100, SENS×133, ALEC×495, BTGO×180, IMMX×91, BBNX×59 |
| 2026-08-21 | +3.25 | $142.06 | MRNA×7, CYPH×1034, ABCL×100, SENS×133, ALEC×495, BTGO×180, IMMX×91, BBNX×59 | $9,570.49 | +281.09 | +347.26 | XHG, ARCT, IOVA, DFDV, MRVI, XXI | ABCL, SENS, ALEC, BTGO, IMMX, BBNX | $3.70 | $9,882.48 | MRNA×7, CYPH×1034, XHG×269, ARCT×108, IOVA×133, DFDV×299, MRVI×146, XXI×187 |
| 2026-08-24 | -5.17 | $3.70 | MRNA×7, CYPH×1034, XHG×269, ARCT×108, IOVA×133, DFDV×299, MRVI×146, XXI×187 | $10,311.77 | +429.29 | +0.00 | — | MRNA, CYPH, XHG, ARCT, IOVA, DFDV, MRVI, XXI | $10,278.95 | $10,278.95 | — |
| 2026-08-25 | +1.80 | $10,278.95 | — | $10,278.95 | +0.00 | +638.37 | REAX, CYPH, XHG, ASST, ALVO, SUJA, BMNR, GORO | — | $18.01 | $10,885.91 | REAX×53, CYPH×823, XHG×315, ASST×67, ALVO×245, SUJA×146, BMNR×53, GORO×361 |
| 2026-08-26 | +2.02 | $18.01 | REAX×53, CYPH×823, XHG×315, ASST×67, ALVO×245, SUJA×146, BMNR×53, GORO×361 | $10,570.26 | -315.65 | -17.35 | BYND, USDE, MNRO, FIGR, TRLV, VIR | REAX, CYPH, ASST, ALVO, BMNR, GORO | $47.65 | $10,513.37 | XHG×315, SUJA×146, BYND×94, USDE×228, MNRO×94, FIGR×32, TRLV×118, VIR×125 |
| 2026-08-27 | — | $47.65 | XHG×315, SUJA×146, BYND×94, USDE×228, MNRO×94, FIGR×32, TRLV×118, VIR×125 | $10,613.13 | +99.76 | +98.11 | GEN, SLI, RRC, PGY, DLO, ANET, CRK, PLTR | XHG, SUJA, BYND, USDE, MNRO, FIGR, TRLV, VIR | $225.80 | $10,668.73 | GEN×44, SLI×509, RRC×31, PGY×57, DLO×86, ANET×6, CRK×91, PLTR×7 |
| 2026-08-28 | +0.75 | $225.80 | GEN×44, SLI×509, RRC×31, PGY×57, DLO×86, ANET×6, CRK×91, PLTR×7 | $10,681.15 | +12.42 | -204.97 | BYND, CAPR, ANF, NEO, EL, VYX, FIG, NCNO | GEN, SLI, RRC, PGY, DLO, ANET, CRK, PLTR | $88.23 | $10,436.84 | BYND×95, CAPR×136, ANF×9, NEO×72, EL×12, VYX×145, FIG×44, NCNO×57 |
| 2026-08-31 | -5.85 | $88.23 | BYND×95, CAPR×136, ANF×9, NEO×72, EL×12, VYX×145, FIG×44, NCNO×57 | $10,298.01 | -138.83 | -48.45 | — | CAPR, ANF, NEO, EL, VYX, FIG, NCNO | $8,970.53 | $10,234.03 | BYND×95 |
| 2026-09-01 | -6.30 | $8,970.53 | BYND×95 | $10,209.33 | -24.70 | +0.00 | — | BYND | $10,207.03 | $10,207.03 | — |
| 2026-09-02 | -3.83 | $10,207.03 | — | $10,207.03 | +0.00 | +0.00 | — | — | $10,207.03 | $10,207.03 | — |
| 2026-09-03 | -0.90 | $10,207.03 | — | $10,207.03 | +0.00 | -285.83 | GPRO, REAX, AGCO, ASST, SID, TARS, CNDT, RSKD | — | $154.02 | $9,880.26 | GPRO×716, REAX×69, AGCO×9, ASST×49, SID×938, TARS×15, CNDT×671, RSKD×190 |
| 2026-09-04 | +2.25 | $154.02 | GPRO×716, REAX×69, AGCO×9, ASST×49, SID×938, TARS×15, CNDT×671, RSKD×190 | $9,795.59 | -84.67 | +411.44 | DFDV, USDE, FRNM, LENZ, PAGS | REAX, AGCO, SID, CNDT, RSKD | $9.33 | $10,166.52 | GPRO×716, ASST×49, TARS×15, DFDV×215, USDE×158, FRNM×76, LENZ×216, PAGS×124 |
| 2026-09-08 | -11.47 | $9.33 | GPRO×716, ASST×49, TARS×15, DFDV×215, USDE×158, FRNM×76, LENZ×216, PAGS×124 | $10,031.99 | -134.53 | +0.00 | — | GPRO, ASST, TARS, DFDV, USDE, FRNM, LENZ, PAGS | $10,005.63 | $10,005.63 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $8,761.36 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,517.83 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,300.81 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $5,049.62 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $3,782.66 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 56 | $22.01 | $2.16 | — | $2,547.94 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $1,305.43 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $107.38 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $107.38 | ▲ close $10,268.71 vs 09:30 $10,000.00 (session +300.75) | 16:00 close · cash $107.38 · equity $10,268.71 vs 09:30 $10,000.00 (+268.71; session marks +300.75) · 8 name(s) marked open→close (per-name table). TNDM×53 09:30 $23.33 → close $23.13 -10.60; IREN×27 09:30 $45.98 → close $44.76 -32.94; TPG×24 09:30 $50.62 → close $54.62 +95.92; HIMS×42 09:30 $29.74 → close $28.77 -40.74; INO×1543 09:30 $0.81 → close $0.90 +138.87; VOR×56 09:30 $22.01 → close $23.29 +71.68; SLS×106 09:30 $11.70 → close $12.36 +69.96; BTSG×20 09:30 $59.80 → close $60.23 +8.60 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $107.38 | ▲ 09:30 equity $10,312.70 vs yday $10,268.71 (+43.99) | 09:30 open · cash $107.38 (unchanged overnight, no fees) · equity $10,312.70 vs prior close $10,268.71 (+43.99) · 8 name(s) re-marked at the open (per-name table). TNDM×53 yday $23.13 → 09:30 $22.92 -11.13; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; VOR×56 yday $23.29 → 09:30 $23.33 +2.24; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; BTSG×20 yday $60.23 → 09:30 $59.65 -11.60 | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $1,319.97 | ▼ -26.05 after sell → book $10,310.53; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $2,508.31 | ▼ -55.19 after sell → book $10,308.44; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $3,833.19 | ▲ +107.86 after sell → book $10,306.36; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $5,055.35 | ▼ -29.03 after sell → book $10,304.22; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $6,471.10 | ▲ +148.79 after sell → book $10,284.98; vs 09:30 mark -19.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 56 | $23.33 | $2.18 | $+69.58 | $7,775.40 | ▲ +69.58 after sell → book $10,282.80; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $9,087.46 | ▲ +69.56 after sell → book $10,280.46; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $10,278.39 | ▼ -7.12 after sell → book $10,278.39; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 52 | $24.68 | $2.15 | — | $8,992.89 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ⚪; ret5=+111.3; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 583 | $2.20 | $7.52 | — | $7,702.77 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 115 | $11.12 | $2.33 | — | $6,421.63 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 65 | $19.57 | $2.19 | — | $5,147.40 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LIFE` | 36 | $35.04 | $2.10 | — | $3,883.86 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+16.7; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETA` | 50 | $25.21 | $2.14 | — | $2,621.22 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+15.5; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 67 | $19.17 | $2.19 | — | $1,334.64 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VOYG` | 28 | $44.49 | $2.07 | — | $86.84 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+15.6; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $86.84 | ▼ close $10,010.26 vs 09:30 $10,312.70 (session -245.44) | 16:00 close · cash $86.84 · equity $10,010.26 vs 09:30 $10,312.70 (-302.44; session marks -245.44) · 8 name(s) marked open→close (per-name table). QMCO×52 09:30 $24.68 → close $26.11 +74.36; ZENA×583 09:30 $2.20 → close $2.14 -34.98; AIRO×115 09:30 $11.12 → close $9.57 -178.25; ARX×65 09:30 $19.57 → close $19.58 +0.65; LIFE×36 09:30 $35.04 → close $34.02 -36.72; BETA×50 09:30 $25.21 → close $24.86 -17.50; LUNR×67 09:30 $19.17 → close $19.01 -10.72; VOYG×28 09:30 $44.49 → close $42.98 -42.28 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $86.84 | ▼ 09:30 equity $9,957.85 vs yday $10,010.26 (-52.41) | 09:30 open · cash $86.84 (unchanged overnight, no fees) · equity $9,957.85 vs prior close $10,010.26 (-52.41) · 8 name(s) re-marked at the open (per-name table). QMCO×52 yday $26.11 → 09:30 $24.83 -66.56; ZENA×583 yday $2.14 → 09:30 $2.08 -32.07; AIRO×115 yday $9.57 → 09:30 $9.57 +0.00; ARX×65 yday $19.58 → 09:30 $19.57 -0.65; LIFE×36 yday $34.02 → 09:30 $34.03 +0.36; BETA×50 yday $24.86 → 09:30 $24.61 -12.50; LUNR×67 yday $19.01 → 09:30 $20.25 +83.08; VOYG×28 yday $42.98 → 09:30 $42.12 -24.08 | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 52 | $24.83 | $2.17 | $+3.49 | $1,375.84 | ▲ +3.49 after sell → book $9,955.68; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 583 | $2.08 | $7.63 | $-82.19 | $2,583.76 | ▼ -82.19 after sell → book $9,948.05; vs 09:30 mark -7.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 115 | $9.57 | $2.36 | $-182.95 | $3,681.95 | ▼ -182.95 after sell → book $9,945.69; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 65 | $19.57 | $2.21 | $-4.39 | $4,951.79 | ▼ -4.39 after sell → book $9,943.48; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LIFE` | 36 | $34.03 | $2.12 | $-40.58 | $6,174.76 | ▼ -40.58 after sell → book $9,941.37; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETA` | 50 | $24.61 | $2.16 | $-34.30 | $7,403.10 | ▼ -34.30 after sell → book $9,939.21; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LUNR` | 67 | $20.25 | $2.21 | $+67.96 | $8,757.63 | ▲ +67.96 after sell → book $9,936.99; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `VOYG` | 28 | $42.12 | $2.09 | $-70.53 | $9,934.90 | ▼ -70.53 after sell → book $9,934.90; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 296 | $4.19 | $3.82 | — | $8,690.84 | — | rank by w_hot_candle; rank w_hot_candle; list yday_mover; ⚪; ret5=+291.8; leftover $1241.86 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 91 | $13.64 | $2.26 | — | $7,447.34 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ⚪; ret5=+57.7; leftover $1241.86 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 30 | $41.23 | $2.08 | — | $6,208.36 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+46.0; leftover $1241.86 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `SMJF` | 122 | $10.10 | $2.36 | — | $4,973.80 | — | rank by w_hot_candle; rank w_hot_candle; list mover_buy; ret5=+22.8; leftover $1241.86 | join🔴 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 646 | $1.92 | $8.33 | — | $3,725.15 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1241.86 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NMAX` | 113 | $10.97 | $2.33 | — | $2,483.21 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ⚪; ret5=+21.2; leftover $1241.86 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 180 | $6.87 | $2.53 | — | $1,244.08 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+62.6; leftover $1241.86 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 38 | $32.55 | $2.10 | — | $5.07 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1241.86 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.07 | ▼ close $9,681.70 vs 09:30 $9,957.85 (session -227.38) | 16:00 close · cash $5.07 · equity $9,681.70 vs 09:30 $9,957.85 (-276.15; session marks -227.38) · 8 name(s) marked open→close (per-name table). XHG×296 09:30 $4.19 → close $3.91 -82.88; STDN×91 09:30 $13.64 → close $13.31 -30.03; HTFL×30 09:30 $41.23 → close $41.94 +21.30; SMJF×122 09:30 $10.10 → close $10.45 +42.70; NPWR×646 09:30 $1.92 → close $1.73 -122.74; NMAX×113 09:30 $10.97 → close $10.36 -68.93; CAPR×180 09:30 $6.87 → close $7.45 +104.40; UMAC×38 09:30 $32.55 → close $30.15 -91.20 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.07 | ▼ 09:30 equity $9,602.07 vs yday $9,681.70 (-79.63) | 09:30 open · cash $5.07 (unchanged overnight, no fees) · equity $9,602.07 vs prior close $9,681.70 (-79.63) · 8 name(s) re-marked at the open (per-name table). XHG×296 yday $3.91 → 09:30 $3.94 +8.88; STDN×91 yday $13.31 → 09:30 $13.31 +0.00; HTFL×30 yday $41.94 → 09:30 $41.50 -13.20; SMJF×122 yday $10.45 → 09:30 $10.45 +0.00; NPWR×646 yday $1.73 → 09:30 $1.70 -19.38; NMAX×113 yday $10.36 → 09:30 $10.31 -5.65; CAPR×180 yday $7.45 → 09:30 $7.50 +9.00; UMAC×38 yday $30.15 → 09:30 $28.59 -59.28 | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 296 | $3.94 | $3.88 | $-81.70 | $1,167.44 | ▼ -81.70 after sell → book $9,598.20; vs 09:30 mark -3.87 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 91 | $13.31 | $2.29 | $-34.58 | $2,376.36 | ▼ -34.58 after sell → book $9,595.91; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 30 | $41.50 | $2.10 | $+3.92 | $3,619.26 | ▲ +3.92 after sell → book $9,593.81; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `SMJF` | 122 | $10.45 | $2.39 | $+37.96 | $4,891.77 | ▲ +37.96 after sell → book $9,591.42; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 646 | $1.70 | $8.45 | $-158.90 | $5,981.52 | ▼ -158.90 after sell → book $9,582.97; vs 09:30 mark -8.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NMAX` | 113 | $10.31 | $2.36 | $-79.27 | $7,144.19 | ▼ -79.27 after sell → book $9,580.61; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 38 | $28.59 | $2.12 | $-154.71 | $8,228.49 | ▼ -154.71 after sell → book $9,578.49; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,228.49 | ▼ close $9,502.89 vs 09:30 $9,602.07 (session -75.60) | 16:00 close · cash $8,228.49 · equity $9,502.89 vs 09:30 $9,602.07 (-99.18; session marks -75.60) · 1 name(s) marked open→close (per-name table). CAPR×180 09:30 $7.50 → close $7.08 -75.60 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,228.49 | ▲ 09:30 equity $9,522.69 vs yday $9,502.89 (+19.80) | 09:30 open · cash $8,228.49 (unchanged overnight, no fees) · equity $9,522.69 vs prior close $9,502.89 (+19.80) · 1 name(s) re-marked at the open (per-name table). CAPR×180 yday $7.08 → 09:30 $7.19 +19.80 | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 180 | $7.19 | $2.57 | $+52.50 | $9,520.12 | ▲ +52.50 after sell → book $9,520.12; vs 09:30 mark -2.57 | dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,520.12 | ▲ close $9,520.12 vs 09:30 $9,522.69 (session +0.00) | 16:00 close · cash $9,520.12 · no lots left · equity $9,520.12. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,520.12 | ▲ 09:30 equity $9,520.12 vs yday $9,520.12 (+0.00) | 09:30 open · cash $9,520.12 · no holdings · equity $9,520.12 vs prior close $9,520.12 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 7 | $150.14 | $2.01 | — | $8,467.13 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1190.02 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1034 | $1.15 | $13.34 | — | $7,264.69 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1190.02 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 100 | $11.81 | $2.29 | — | $6,080.90 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1190.02 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SENS` | 133 | $8.91 | $2.39 | — | $4,893.48 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1190.02 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ALEC` | 495 | $2.40 | $6.39 | — | $3,699.10 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+13.0; leftover $1190.02 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 180 | $6.61 | $2.53 | — | $2,507.67 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1190.02 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IMMX` | 91 | $12.98 | $2.26 | — | $1,324.22 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1190.02 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BBNX` | 59 | $20.00 | $2.17 | — | $142.06 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+15.5; leftover $1190.02 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $142.06 | ▼ close $9,289.40 vs 09:30 $9,520.12 (session -197.35) | 16:00 close · cash $142.06 · equity $9,289.40 vs 09:30 $9,520.12 (-230.72; session marks -197.35) · 8 name(s) marked open→close (per-name table). MRNA×7 09:30 $150.14 → close $133.32 -117.74; CYPH×1034 09:30 $1.15 → close $1.19 +41.36; ABCL×100 09:30 $11.81 → close $11.57 -24.50; SENS×133 09:30 $8.91 → close $8.82 -11.97; ALEC×495 09:30 $2.40 → close $2.26 -69.30; BTGO×180 09:30 $6.61 → close $6.60 -0.90; IMMX×91 09:30 $12.98 → close $13.16 +16.38; BBNX×59 09:30 $20.00 → close $19.48 -30.68 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $142.06 | ▲ 09:30 equity $9,570.49 vs yday $9,289.40 (+281.09) | 09:30 open · cash $142.06 (unchanged overnight, no fees) · equity $9,570.49 vs prior close $9,289.40 (+281.09) · 8 name(s) re-marked at the open (per-name table). MRNA×7 yday $133.32 → 09:30 $133.11 -1.47; CYPH×1034 yday $1.19 → 09:30 $1.32 +134.42; ABCL×100 yday $11.57 → 09:30 $11.57 +0.00; SENS×133 yday $8.82 → 09:30 $9.24 +55.86; ALEC×495 yday $2.26 → 09:30 $2.28 +9.90; BTGO×180 yday $6.60 → 09:30 $6.95 +63.00; IMMX×91 yday $13.16 → 09:30 $13.36 +18.20; BBNX×59 yday $19.48 → 09:30 $19.50 +1.18 | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 100 | $11.57 | $2.32 | $-29.11 | $1,296.74 | ▼ -29.11 after sell → book $9,568.17; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SENS` | 133 | $9.24 | $2.42 | $+39.08 | $2,523.24 | ▲ +39.08 after sell → book $9,565.75; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ALEC` | 495 | $2.28 | $6.48 | $-72.26 | $3,645.36 | ▼ -72.26 after sell → book $9,559.27; vs 09:30 mark -6.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 180 | $6.95 | $2.57 | $+57.00 | $4,893.79 | ▲ +57.00 after sell → book $9,556.70; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IMMX` | 91 | $13.36 | $2.29 | $+30.03 | $6,107.26 | ▲ +30.03 after sell → book $9,554.41; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BBNX` | 59 | $19.50 | $2.19 | $-33.85 | $7,255.58 | ▼ -33.85 after sell → book $9,552.23; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 269 | $4.49 | $3.47 | — | $6,044.30 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+12.7; leftover $1209.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 108 | $11.13 | $2.31 | — | $4,839.94 | — | rank by w_hot_candle; rank w_hot_candle; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1209.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 133 | $9.08 | $2.39 | — | $3,629.91 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $1209.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DFDV` | 299 | $4.04 | $3.86 | — | $2,418.10 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+23.6; leftover $1209.26 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 146 | $8.28 | $2.43 | — | $1,206.79 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; leftover $1209.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `XXI` | 187 | $6.42 | $2.55 | — | $3.70 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; ret5=+23.8; leftover $1209.26 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.70 | ▲ close $9,882.48 vs 09:30 $9,570.49 (session +347.26) | 16:00 close · cash $3.70 · equity $9,882.48 vs 09:30 $9,570.49 (+311.99; session marks +347.26) · 8 name(s) marked open→close (per-name table). MRNA×7 09:30 $133.11 → close $145.13 +84.14; CYPH×1034 09:30 $1.32 → close $1.42 +103.40; XHG×269 09:30 $4.49 → close $4.41 -21.52; ARCT×108 09:30 $11.13 → close $13.45 +250.56; IOVA×133 09:30 $9.08 → close $8.29 -105.07; DFDV×299 09:30 $4.04 → close $3.94 -29.90; MRVI×146 09:30 $8.28 → close $8.64 +52.56; XXI×187 09:30 $6.42 → close $6.49 +13.09 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.70 | ▲ 09:30 equity $10,311.77 vs yday $9,882.48 (+429.29) | 09:30 open · cash $3.70 (unchanged overnight, no fees) · equity $10,311.77 vs prior close $9,882.48 (+429.29) · 8 name(s) re-marked at the open (per-name table). MRNA×7 yday $145.13 → 09:30 $142.70 -17.01; CYPH×1034 yday $1.42 → 09:30 $1.83 +423.94; XHG×269 yday $4.41 → 09:30 $4.32 -24.21; ARCT×108 yday $13.45 → 09:30 $13.33 -12.96; IOVA×133 yday $8.29 → 09:30 $8.08 -27.93; DFDV×299 yday $3.94 → 09:30 $4.16 +65.78; MRVI×146 yday $8.64 → 09:30 $8.59 -7.30; XXI×187 yday $6.49 → 09:30 $6.64 +28.98 | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 7 | $142.70 | $2.03 | $-56.12 | $1,000.57 | ▼ -56.12 after sell → book $10,309.74; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1034 | $1.83 | $13.53 | $+676.26 | $2,879.26 | ▲ +676.26 after sell → book $10,296.22; vs 09:30 mark -13.52 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 269 | $4.32 | $3.52 | $-52.72 | $4,037.82 | ▼ -52.72 after sell → book $10,292.69; vs 09:30 mark -3.53 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 108 | $13.33 | $2.34 | $+232.94 | $5,475.11 | ▲ +232.94 after sell → book $10,290.35; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `IOVA` | 133 | $8.08 | $2.42 | $-137.81 | $6,547.33 | ▼ -137.81 after sell → book $10,287.93; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DFDV` | 299 | $4.16 | $3.92 | $+28.11 | $7,787.25 | ▲ +28.11 after sell → book $10,284.01; vs 09:30 mark -3.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 146 | $8.59 | $2.46 | $+40.37 | $9,038.93 | ▲ +40.37 after sell → book $10,281.55; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XXI` | 187 | $6.64 | $2.59 | $+36.93 | $10,278.95 | ▲ +36.93 after sell → book $10,278.95; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,278.95 | ▲ close $10,278.95 vs 09:30 $10,311.77 (session +0.00) | 16:00 close · cash $10,278.95 · no lots left · equity $10,278.95. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,278.95 | ▲ 09:30 equity $10,278.95 vs yday $10,278.95 (+0.00) | 09:30 open · cash $10,278.95 · no holdings · equity $10,278.95 vs prior close $10,278.95 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 53 | $24.11 | $2.15 | — | $8,998.98 | — | rank by w_hot_candle; rank w_hot_candle; list yday_mover; ret5=+891.7; leftover $1284.87 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 823 | $1.56 | $10.62 | — | $7,704.48 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $1284.87 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 315 | $4.07 | $4.06 | — | $6,418.37 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+4.9; leftover $1284.87 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 67 | $19.04 | $2.19 | — | $5,140.49 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ret5=+49.5; leftover $1284.87 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 245 | $5.24 | $3.16 | — | $3,853.53 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1284.87 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 146 | $8.79 | $2.43 | — | $2,567.77 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $1284.87 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMNR` | 53 | $23.80 | $2.15 | — | $1,304.22 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; ret5=+28.9; leftover $1284.87 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 361 | $3.55 | $4.66 | — | $18.01 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+27.9; leftover $1284.87 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18.01 | ▲ close $10,885.91 vs 09:30 $10,278.95 (session +638.37) | 16:00 close · cash $18.01 · equity $10,885.91 vs 09:30 $10,278.95 (+606.96; session marks +638.37) · 8 name(s) marked open→close (per-name table). REAX×53 09:30 $24.11 → close $28.43 +228.96; CYPH×823 09:30 $1.56 → close $1.64 +65.84; XHG×315 09:30 $4.07 → close $4.02 -15.75; ASST×67 09:30 $19.04 → close $21.39 +157.45; ALVO×245 09:30 $5.24 → close $5.05 -46.55; SUJA×146 09:30 $8.79 → close $9.33 +78.84; BMNR×53 09:30 $23.80 → close $24.82 +54.06; GORO×361 09:30 $3.55 → close $3.87 +115.52 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18.01 | ▼ 09:30 equity $10,570.26 vs yday $10,885.91 (-315.65) | 09:30 open · cash $18.01 (unchanged overnight, no fees) · equity $10,570.26 vs prior close $10,885.91 (-315.65) · 8 name(s) re-marked at the open (per-name table). REAX×53 yday $28.43 → 09:30 $26.61 -96.46; CYPH×823 yday $1.64 → 09:30 $1.60 -32.92; XHG×315 yday $4.02 → 09:30 $3.81 -66.15; ASST×67 yday $21.39 → 09:30 $20.72 -44.89; ALVO×245 yday $5.05 → 09:30 $4.98 -17.15; SUJA×146 yday $9.33 → 09:30 $9.39 +8.76; BMNR×53 yday $24.82 → 09:30 $24.24 -30.74; GORO×361 yday $3.87 → 09:30 $3.77 -36.10 | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 53 | $26.61 | $2.17 | $+128.18 | $1,426.17 | ▲ +128.18 after sell → book $10,568.09; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 823 | $1.60 | $10.76 | $+11.54 | $2,732.21 | ▲ +11.54 after sell → book $10,557.33; vs 09:30 mark -10.76 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 67 | $20.72 | $2.21 | $+108.16 | $4,118.23 | ▲ +108.16 after sell → book $10,555.11; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ALVO` | 245 | $4.98 | $3.21 | $-70.07 | $5,335.12 | ▼ -70.07 after sell → book $10,551.90; vs 09:30 mark -3.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMNR` | 53 | $24.24 | $2.17 | $+19.00 | $6,617.67 | ▲ +19.00 after sell → book $10,549.73; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GORO` | 361 | $3.77 | $4.73 | $+70.04 | $7,973.91 | ▲ +70.04 after sell → book $10,545.00; vs 09:30 mark -4.73 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 94 | $14.11 | $2.27 | — | $6,645.30 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+11.4; leftover $1328.99 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 228 | $5.81 | $2.94 | — | $5,317.68 | — | rank by w_hot_candle; rank w_hot_candle; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $1328.99 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `MNRO` | 94 | $14.00 | $2.27 | — | $3,999.41 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+17.8; leftover $1328.99 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FIGR` | 32 | $40.50 | $2.09 | — | $2,701.32 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+15.8; leftover $1328.99 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 118 | $11.22 | $2.34 | — | $1,375.02 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+16.8; leftover $1328.99 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `VIR` | 125 | $10.60 | $2.37 | — | $47.65 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+12.9; leftover $1328.99 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $47.65 | ▼ close $10,513.37 vs 09:30 $10,570.26 (session -17.35) | 16:00 close · cash $47.65 · equity $10,513.37 vs 09:30 $10,570.26 (-56.89; session marks -17.35) · 8 name(s) marked open→close (per-name table). XHG×315 09:30 $3.81 → close $4.06 +78.75; SUJA×146 09:30 $9.39 → close $9.44 +7.30; BYND×94 09:30 $14.11 → close $14.25 +13.16; USDE×228 09:30 $5.81 → close $5.98 +38.76; MNRO×94 09:30 $14.00 → close $12.61 -130.66; FIGR×32 09:30 $40.50 → close $37.08 -109.44; TRLV×118 09:30 $11.22 → close $11.43 +24.78; VIR×125 09:30 $10.60 → close $11.08 +60.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.65 | ▲ 09:30 equity $10,613.13 vs yday $10,513.37 (+99.76) | 09:30 open · cash $47.65 (unchanged overnight, no fees) · equity $10,613.13 vs prior close $10,513.37 (+99.76) · 8 name(s) re-marked at the open (per-name table). XHG×315 yday $4.06 → 09:30 $4.06 +0.00; SUJA×146 yday $9.44 → 09:30 $9.41 -4.38; BYND×94 yday $14.25 → 09:30 $14.20 -4.70; USDE×228 yday $5.98 → 09:30 $6.50 +118.56; MNRO×94 yday $12.61 → 09:30 $12.56 -4.70; FIGR×32 yday $37.08 → 09:30 $37.42 +10.88; TRLV×118 yday $11.43 → 09:30 $11.38 -5.90; VIR×125 yday $11.08 → 09:30 $11.00 -10.00 | — |
| 2026-08-27 09:30 ET | **SELL** | `XHG` | 315 | $4.06 | $4.13 | $-11.34 | $1,322.43 | ▼ -11.34 after sell → book $10,609.01; vs 09:30 mark -4.12 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SUJA` | 146 | $9.41 | $2.46 | $+85.63 | $2,693.83 | ▲ +85.63 after sell → book $10,606.55; vs 09:30 mark -2.46 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 94 | $14.20 | $2.30 | $+3.89 | $4,026.33 | ▲ +3.89 after sell → book $10,604.25; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 228 | $6.50 | $2.99 | $+151.39 | $5,505.34 | ▲ +151.39 after sell → book $10,601.26; vs 09:30 mark -2.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `MNRO` | 94 | $12.56 | $2.30 | $-139.93 | $6,683.68 | ▼ -139.93 after sell → book $10,598.96; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FIGR` | 32 | $37.42 | $2.11 | $-102.75 | $7,879.01 | ▼ -102.75 after sell → book $10,596.85; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TRLV` | 118 | $11.38 | $2.37 | $+14.16 | $9,219.48 | ▲ +14.16 after sell → book $10,594.48; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `VIR` | 125 | $11.00 | $2.40 | $+45.24 | $10,592.08 | ▲ +45.24 after sell → book $10,592.08; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 44 | $29.83 | $2.12 | — | $9,277.44 | — | rank by w_hot_candle; rank w_hot_candle; list mover_buy; 🔵; ret5=+7.6; leftover $1324.01 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 509 | $2.60 | $6.57 | — | $7,947.47 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ret5=+13.0; leftover $1324.01 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 31 | $41.44 | $2.08 | — | $6,660.75 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ret5=+3.1; leftover $1324.01 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PGY` | 57 | $22.93 | $2.16 | — | $5,351.58 | — | rank by w_hot_candle; rank w_hot_candle; list mover_buy; 🔵; ret5=+9.5; leftover $1324.01 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 86 | $15.33 | $2.25 | — | $4,030.95 | — | rank by w_hot_candle; rank w_hot_candle; list mover_buy; 🔵; ret5=+7.4; leftover $1324.01 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ANET` | 6 | $205.90 | $2.01 | — | $2,793.54 | — | rank by w_hot_candle; rank w_hot_candle; list mover_buy; 🔵; ret5=+8.5; leftover $1324.01 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 91 | $14.42 | $2.26 | — | $1,479.06 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ret5=+7.1; leftover $1324.01 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PLTR` | 7 | $178.75 | $2.01 | — | $225.80 | — | rank by w_hot_candle; rank w_hot_candle; list mover_buy; 🔵; ret5=+1.3; leftover $1324.01 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $225.80 | ▲ close $10,668.73 vs 09:30 $10,613.13 (session +98.11) | 16:00 close · cash $225.80 · equity $10,668.73 vs 09:30 $10,613.13 (+55.60; session marks +98.11) · 8 name(s) marked open→close (per-name table). GEN×44 09:30 $29.83 → close $30.50 +29.48; SLI×509 09:30 $2.60 → close $2.64 +20.36; RRC×31 09:30 $41.44 → close $41.64 +6.20; PGY×57 09:30 $22.93 → close $23.26 +18.81; DLO×86 09:30 $15.33 → close $15.14 -16.34; ANET×6 09:30 $205.90 → close $201.09 -28.86; CRK×91 09:30 $14.42 → close $14.62 +18.20; PLTR×7 09:30 $178.75 → close $185.93 +50.26 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $225.80 | ▲ 09:30 equity $10,681.15 vs yday $10,668.73 (+12.42) | 09:30 open · cash $225.80 (unchanged overnight, no fees) · equity $10,681.15 vs prior close $10,668.73 (+12.42) · 8 name(s) re-marked at the open (per-name table). GEN×44 yday $30.50 → 09:30 $30.50 +0.00; SLI×509 yday $2.64 → 09:30 $2.68 +20.36; RRC×31 yday $41.64 → 09:30 $41.74 +3.10; PGY×57 yday $23.26 → 09:30 $23.21 -2.85; DLO×86 yday $15.14 → 09:30 $15.19 +4.30; ANET×6 yday $201.09 → 09:30 $200.00 -6.54; CRK×91 yday $14.62 → 09:30 $14.63 +0.91; PLTR×7 yday $185.93 → 09:30 $184.95 -6.86 | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 44 | $30.50 | $2.14 | $+25.22 | $1,565.66 | ▲ +25.22 after sell → book $10,679.01; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 509 | $2.68 | $6.66 | $+27.49 | $2,923.11 | ▲ +27.49 after sell → book $10,672.34; vs 09:30 mark -6.67 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `RRC` | 31 | $41.74 | $2.10 | $+5.11 | $4,214.95 | ▲ +5.11 after sell → book $10,670.24; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `PGY` | 57 | $23.21 | $2.18 | $+11.62 | $5,535.74 | ▲ +11.62 after sell → book $10,668.06; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DLO` | 86 | $15.19 | $2.27 | $-16.56 | $6,839.81 | ▼ -16.56 after sell → book $10,665.79; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ANET` | 6 | $200.00 | $2.03 | $-39.44 | $8,037.78 | ▼ -39.44 after sell → book $10,663.76; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRK` | 91 | $14.63 | $2.29 | $+14.56 | $9,366.82 | ▲ +14.56 after sell → book $10,661.47; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `PLTR` | 7 | $184.95 | $2.03 | $+39.36 | $10,659.44 | ▲ +39.36 after sell → book $10,659.44; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 95 | $14.00 | $2.27 | — | $9,327.16 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=-3.3; leftover $1332.43 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 136 | $9.73 | $2.40 | — | $8,001.49 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+47.1; leftover $1332.43 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $146.07 | $2.02 | — | $6,684.84 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1332.43 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NEO` | 72 | $18.36 | $2.21 | — | $5,360.71 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+12.8; leftover $1332.43 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `EL` | 12 | $106.99 | $2.03 | — | $4,074.81 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+10.5; leftover $1332.43 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VYX` | 145 | $9.13 | $2.42 | — | $2,748.53 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ret5=+20.0; leftover $1332.43 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FIG` | 44 | $30.18 | $2.12 | — | $1,418.49 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+12.1; leftover $1332.43 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 57 | $23.30 | $2.16 | — | $88.23 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+14.5; leftover $1332.43 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $88.23 | ▼ close $10,436.84 vs 09:30 $10,681.15 (session -204.97) | 16:00 close · cash $88.23 · equity $10,436.84 vs 09:30 $10,681.15 (-244.31; session marks -204.97) · 8 name(s) marked open→close (per-name table). BYND×95 09:30 $14.00 → close $13.86 -13.30; CAPR×136 09:30 $9.73 → close $9.59 -19.04; ANF×9 09:30 $146.07 → close $148.42 +21.15; NEO×72 09:30 $18.36 → close $18.05 -22.32; EL×12 09:30 $106.99 → close $103.39 -43.20; VYX×145 09:30 $9.13 → close $8.78 -50.75; FIG×44 09:30 $30.18 → close $28.82 -59.84; NCNO×57 09:30 $23.30 → close $22.99 -17.67 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $88.23 | ▼ 09:30 equity $10,298.01 vs yday $10,436.84 (-138.83) | 09:30 open · cash $88.23 (unchanged overnight, no fees) · equity $10,298.01 vs prior close $10,436.84 (-138.83) · 8 name(s) re-marked at the open (per-name table). BYND×95 yday $13.86 → 09:30 $13.81 -4.75; CAPR×136 yday $9.59 → 09:30 $9.50 -12.24; ANF×9 yday $148.42 → 09:30 $148.03 -3.51; NEO×72 yday $18.05 → 09:30 $17.77 -20.16; EL×12 yday $103.39 → 09:30 $102.70 -8.28; VYX×145 yday $8.78 → 09:30 $8.66 -17.40; FIG×44 yday $28.82 → 09:30 $27.60 -53.68; NCNO×57 yday $22.99 → 09:30 $22.66 -18.81 | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 136 | $9.50 | $2.43 | $-36.11 | $1,377.80 | ▼ -36.11 after sell → book $10,295.58; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.03 | $2.04 | $+13.59 | $2,708.03 | ▲ +13.59 after sell → book $10,293.54; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NEO` | 72 | $17.77 | $2.23 | $-46.91 | $3,985.24 | ▼ -46.91 after sell → book $10,291.31; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `EL` | 12 | $102.70 | $2.05 | $-55.55 | $5,215.60 | ▼ -55.55 after sell → book $10,289.27; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `VYX` | 145 | $8.66 | $2.46 | $-73.03 | $6,468.84 | ▼ -73.03 after sell → book $10,286.81; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FIG` | 44 | $27.60 | $2.14 | $-117.78 | $7,681.10 | ▼ -117.78 after sell → book $10,284.67; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `NCNO` | 57 | $22.66 | $2.18 | $-40.82 | $8,970.53 | ▼ -40.82 after sell → book $10,282.48; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,970.53 | ▼ close $10,234.03 vs 09:30 $10,298.01 (session -48.45) | 16:00 close · cash $8,970.53 · equity $10,234.03 vs 09:30 $10,298.01 (-63.98; session marks -48.45) · 1 name(s) marked open→close (per-name table). BYND×95 09:30 $13.81 → close $13.30 -48.45 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,970.53 | ▼ 09:30 equity $10,209.33 vs yday $10,234.03 (-24.70) | 09:30 open · cash $8,970.53 (unchanged overnight, no fees) · equity $10,209.33 vs prior close $10,234.03 (-24.70) · 1 name(s) re-marked at the open (per-name table). BYND×95 yday $13.30 → 09:30 $13.04 -24.70 | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 95 | $13.04 | $2.30 | $-95.78 | $10,207.03 | ▼ -95.78 after sell → book $10,207.03; vs 09:30 mark -2.30 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,207.03 | ▲ close $10,207.03 vs 09:30 $10,209.33 (session +0.00) | 16:00 close · cash $10,207.03 · no lots left · equity $10,207.03. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,207.03 | ▲ 09:30 equity $10,207.03 vs yday $10,207.03 (+0.00) | 09:30 open · cash $10,207.03 · no holdings · equity $10,207.03 vs prior close $10,207.03 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,207.03 | ▲ close $10,207.03 vs 09:30 $10,207.03 (session +0.00) | 16:00 close · cash $10,207.03 · no lots left · equity $10,207.03. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,207.03 | ▲ 09:30 equity $10,207.03 vs yday $10,207.03 (+0.00) | 09:30 open · cash $10,207.03 · no holdings · equity $10,207.03 vs prior close $10,207.03 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 716 | $1.78 | $9.24 | — | $8,923.32 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+183.1; leftover $1275.88 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 69 | $18.40 | $2.20 | — | $7,651.52 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=-32.2; leftover $1275.88 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AGCO` | 9 | $127.91 | $2.02 | — | $6,498.31 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.6; leftover $1275.88 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ASST` | 49 | $25.62 | $2.14 | — | $5,240.55 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+13.1; leftover $1275.88 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SID` | 938 | $1.36 | $12.10 | — | $3,952.77 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1275.88 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `TARS` | 15 | $82.76 | $2.04 | — | $2,709.34 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+17.1; leftover $1275.88 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNDT` | 671 | $1.90 | $8.66 | — | $1,425.78 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+12.5; leftover $1275.88 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `RSKD` | 190 | $6.68 | $2.56 | — | $154.02 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+11.4; leftover $1275.88 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $154.02 | ▼ close $9,880.26 vs 09:30 $10,207.03 (session -285.83) | 16:00 close · cash $154.02 · equity $9,880.26 vs 09:30 $10,207.03 (-326.77; session marks -285.83) · 8 name(s) marked open→close (per-name table). GPRO×716 09:30 $1.78 → close $1.39 -279.24; REAX×69 09:30 $18.40 → close $18.40 +0.00; AGCO×9 09:30 $127.91 → close $125.82 -18.81; ASST×49 09:30 $25.62 → close $26.82 +58.56; SID×938 09:30 $1.36 → close $1.26 -93.80; TARS×15 09:30 $82.76 → close $83.20 +6.67; CNDT×671 09:30 $1.90 → close $1.89 -6.71; RSKD×190 09:30 $6.68 → close $6.93 +47.50 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $154.02 | ▼ 09:30 equity $9,795.59 vs yday $9,880.26 (-84.67) | 09:30 open · cash $154.02 (unchanged overnight, no fees) · equity $9,795.59 vs prior close $9,880.26 (-84.67) · 8 name(s) re-marked at the open (per-name table). GPRO×716 yday $1.39 → 09:30 $1.48 +64.44; REAX×69 yday $18.40 → 09:30 $18.15 -17.25; AGCO×9 yday $125.82 → 09:30 $125.22 -5.40; ASST×49 yday $26.82 → 09:30 $25.18 -80.36; SID×938 yday $1.26 → 09:30 $1.23 -28.14; TARS×15 yday $83.20 → 09:30 $82.70 -7.57; CNDT×671 yday $1.89 → 09:30 $1.90 +6.71; RSKD×190 yday $6.93 → 09:30 $6.84 -17.10 | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 69 | $18.15 | $2.22 | $-21.67 | $1,404.15 | ▼ -21.67 after sell → book $9,793.37; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AGCO` | 9 | $125.22 | $2.04 | $-28.26 | $2,529.09 | ▼ -28.26 after sell → book $9,791.33; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SID` | 938 | $1.23 | $12.27 | $-146.31 | $3,670.57 | ▼ -146.31 after sell → book $9,779.07; vs 09:30 mark -12.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNDT` | 671 | $1.90 | $8.78 | $-17.43 | $4,936.69 | ▼ -17.43 after sell → book $9,770.29; vs 09:30 mark -8.78 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RSKD` | 190 | $6.84 | $2.60 | $+25.24 | $6,233.69 | ▲ +25.24 after sell → book $9,767.69; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 215 | $5.79 | $2.77 | — | $4,986.07 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $1246.74 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 158 | $7.87 | $2.46 | — | $3,740.14 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+8.7; leftover $1246.74 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `FRNM` | 76 | $16.40 | $2.22 | — | $2,491.52 | — | rank by w_hot_candle; rank w_hot_candle; list mover_buy; 🔵; ⚪; ret5=+21.2; leftover $1246.74 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `LENZ` | 216 | $5.75 | $2.79 | — | $1,246.74 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ⚪; ret5=+21.2; leftover $1246.74 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `PAGS` | 124 | $9.96 | $2.36 | — | $9.33 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+11.5; leftover $1246.74 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.33 | ▲ close $10,166.52 vs 09:30 $9,795.59 (session +411.44) | 16:00 close · cash $9.33 · equity $10,166.52 vs 09:30 $9,795.59 (+370.93; session marks +411.44) · 8 name(s) marked open→close (per-name table). GPRO×716 09:30 $1.48 → close $1.70 +157.52; ASST×49 09:30 $25.18 → close $27.14 +96.04; TARS×15 09:30 $82.70 → close $90.78 +121.20; DFDV×215 09:30 $5.79 → close $5.87 +17.20; USDE×158 09:30 $7.87 → close $7.93 +9.48; FRNM×76 09:30 $16.40 → close $16.31 -6.84; LENZ×216 09:30 $5.75 → close $5.96 +45.36; PAGS×124 09:30 $9.96 → close $9.73 -28.52 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.33 | ▼ 09:30 equity $10,031.99 vs yday $10,166.52 (-134.53) | 09:30 open · cash $9.33 (unchanged overnight, no fees) · equity $10,031.99 vs prior close $10,166.52 (-134.53) · 8 name(s) re-marked at the open (per-name table). GPRO×716 yday $1.70 → 09:30 $1.56 -96.66; ASST×49 yday $27.14 → 09:30 $26.44 -34.30; TARS×15 yday $90.78 → 09:30 $89.67 -16.65; DFDV×215 yday $5.87 → 09:30 $5.81 -12.90; USDE×158 yday $7.93 → 09:30 $7.76 -26.86; FRNM×76 yday $16.31 → 09:30 $16.74 +32.68; LENZ×216 yday $5.96 → 09:30 $5.95 -2.16; PAGS×124 yday $9.73 → 09:30 $9.91 +22.32 | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 716 | $1.56 | $9.37 | $-172.54 | $1,120.51 | ▼ -172.54 after sell → book $10,022.63; vs 09:30 mark -9.36 | dropped from list after 2 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 49 | $26.44 | $2.16 | $+35.64 | $2,413.91 | ▲ +35.64 after sell → book $10,020.47; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TARS` | 15 | $89.67 | $2.06 | $+99.56 | $3,756.91 | ▲ +99.56 after sell → book $10,018.42; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 215 | $5.81 | $2.82 | $-1.29 | $5,003.24 | ▼ -1.29 after sell → book $10,015.60; vs 09:30 mark -2.82 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 158 | $7.76 | $2.50 | $-22.34 | $6,226.82 | ▼ -22.34 after sell → book $10,013.10; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 76 | $16.74 | $2.24 | $+21.38 | $7,496.82 | ▲ +21.38 after sell → book $10,010.86; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LENZ` | 216 | $5.95 | $2.83 | $+37.58 | $8,779.18 | ▲ +37.58 after sell → book $10,008.02; vs 09:30 mark -2.84 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `PAGS` | 124 | $9.91 | $2.39 | $-10.95 | $10,005.63 | ▼ -10.95 after sell → book $10,005.63; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,005.63 | ▲ close $10,005.63 vs 09:30 $10,031.99 (session +0.00) | 16:00 close · cash $10,005.63 · no lots left · equity $10,005.63. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CRDL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KYTX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OVID` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NMAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KURA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SG` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ZYME` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `KGC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SPT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DFDV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SUJA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KVYO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `REAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MRNA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ASST` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ARCT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SUJA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SID` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AGCO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TWI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HAFN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `LAND` | hard_red | hard-red S=-11.47 sit; no new buys |
