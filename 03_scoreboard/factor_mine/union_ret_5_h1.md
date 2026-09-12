# Factor mine action — `union_ret_5_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `ret_5` · size `leftover` · sell `list` · S-boost `none` · rank by ret_5

Cash book **+0.34%** ($10,034) · signal-only (no cash/fees) was +12.07%. Starts YES **6/21**. Fills 174 · skips 72 · realized $+150.28.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: the prior 5-session return (bigger first).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is thrown out if any must-not is true.
- Sort the keepers by the prior 5-session return (bigger first) and keep the top 8.
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
- **Gate** `none (list as ranked)` · **rank** `ret_5` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $117.83.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `TNDM` | 53 | — | $23.33 | +0.00 | $23.13 | -10.60 | -10.60 | +0.00 | -10.60 |
| 2026-08-13 | `INO` | 1543 | — | $0.81 | +0.00 | $0.90 | +138.87 | +138.87 | +0.00 | +138.87 |
| 2026-08-13 | `IREN` | 27 | — | $45.98 | +0.00 | $44.76 | -32.94 | -32.94 | +0.00 | -32.94 |
| 2026-08-13 | `TPG` | 24 | — | $50.62 | +0.00 | $54.62 | +95.92 | +95.92 | +0.00 | +95.92 |
| 2026-08-13 | `VOR` | 56 | — | $22.01 | +0.00 | $23.29 | +71.68 | +71.68 | +0.00 | +71.68 |
| 2026-08-13 | `SLS` | 106 | — | $11.70 | +0.00 | $12.36 | +69.96 | +69.96 | +0.00 | +69.96 |
| 2026-08-13 | `TGTX` | 25 | — | $49.70 | +0.00 | $47.94 | -44.00 | -44.00 | +0.00 | -44.00 |
| 2026-08-13 | `BTSG` | 20 | — | $59.80 | +0.00 | $60.23 | +8.60 | +8.60 | +0.00 | +8.60 |
| 2026-08-14 | `TNDM` | 53 | $23.13 | $22.92 | -11.13 | — | +0.00 | -11.13 | -21.73 | — |
| 2026-08-14 | `INO` | 1543 | $0.90 | $0.93 | +46.29 | — | +0.00 | +46.29 | +185.16 | — |
| 2026-08-14 | `IREN` | 27 | $44.76 | $44.09 | -18.09 | — | +0.00 | -18.09 | -51.03 | — |
| 2026-08-14 | `TPG` | 24 | $54.62 | $55.29 | +16.08 | — | +0.00 | +16.08 | +112.00 | — |
| 2026-08-14 | `VOR` | 56 | $23.29 | $23.33 | +2.24 | — | +0.00 | +2.24 | +73.92 | — |
| 2026-08-14 | `SLS` | 106 | $12.36 | $12.40 | +4.24 | — | +0.00 | +4.24 | +74.20 | — |
| 2026-08-14 | `TGTX` | 25 | $47.94 | $47.27 | -16.75 | — | +0.00 | -16.75 | -60.75 | — |
| 2026-08-14 | `BTSG` | 20 | $60.23 | $59.65 | -11.60 | — | +0.00 | -11.60 | -3.00 | — |
| 2026-08-14 | `QMCO` | 51 | — | $24.68 | +0.00 | $26.11 | +72.93 | +72.93 | +0.00 | +72.93 |
| 2026-08-14 | `ARX` | 65 | — | $19.57 | +0.00 | $19.58 | +0.65 | +0.65 | +0.00 | +0.65 |
| 2026-08-14 | `ZENA` | 581 | — | $2.20 | +0.00 | $2.14 | -34.86 | -34.86 | +0.00 | -34.86 |
| 2026-08-14 | `AIRO` | 115 | — | $11.12 | +0.00 | $9.57 | -178.25 | -178.25 | +0.00 | -178.25 |
| 2026-08-14 | `BRUN` | 48 | — | $26.25 | +0.00 | $22.93 | -159.12 | -159.12 | +0.00 | -159.12 |
| 2026-08-14 | `BCAR` | 210 | — | $6.09 | +0.00 | $5.83 | -54.60 | -54.60 | +0.00 | -54.60 |
| 2026-08-14 | `TBBB` | 26 | — | $48.82 | +0.00 | $47.79 | -26.78 | -26.78 | +0.00 | -26.78 |
| 2026-08-17 | `QMCO` | 51 | $26.11 | $24.83 | -65.28 | — | +0.00 | -65.28 | +7.65 | — |
| 2026-08-17 | `ARX` | 65 | $19.58 | $19.57 | -0.65 | — | +0.00 | -0.65 | +0.00 | — |
| 2026-08-17 | `ZENA` | 581 | $2.14 | $2.08 | -31.96 | — | +0.00 | -31.96 | -66.82 | — |
| 2026-08-17 | `AIRO` | 115 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -178.25 | — |
| 2026-08-17 | `BRUN` | 48 | $22.93 | $23.00 | +3.36 | — | +0.00 | +3.36 | -155.76 | — |
| 2026-08-17 | `BCAR` | 210 | $5.83 | $5.99 | +33.60 | — | +0.00 | +33.60 | -21.00 | — |
| 2026-08-17 | `TBBB` | 26 | $47.79 | $47.39 | -10.40 | — | +0.00 | -10.40 | -37.18 | — |
| 2026-08-17 | `XHG` | 290 | — | $4.19 | +0.00 | $3.91 | -81.20 | -81.20 | +0.00 | -81.20 |
| 2026-08-17 | `CAPR` | 177 | — | $6.87 | +0.00 | $7.45 | +102.66 | +102.66 | +0.00 | +102.66 |
| 2026-08-17 | `STDN` | 89 | — | $13.64 | +0.00 | $13.31 | -29.37 | -29.37 | +0.00 | -29.37 |
| 2026-08-17 | `HTFL` | 29 | — | $41.23 | +0.00 | $41.94 | +20.59 | +20.59 | +0.00 | +20.59 |
| 2026-08-17 | `UMAC` | 37 | — | $32.55 | +0.00 | $30.15 | -88.80 | -88.80 | +0.00 | -88.80 |
| 2026-08-17 | `KOPN` | 224 | — | $5.43 | +0.00 | $5.32 | -24.64 | -24.64 | +0.00 | -24.64 |
| 2026-08-17 | `NPWR` | 634 | — | $1.92 | +0.00 | $1.73 | -120.46 | -120.46 | +0.00 | -120.46 |
| 2026-08-17 | `SMJF` | 120 | — | $10.10 | +0.00 | $10.45 | +42.00 | +42.00 | +0.00 | +42.00 |
| 2026-08-18 | `XHG` | 290 | $3.91 | $3.94 | +8.70 | — | +0.00 | +8.70 | -72.50 | — |
| 2026-08-18 | `CAPR` | 177 | $7.45 | $7.50 | +8.85 | $7.08 | -74.34 | -65.49 | +111.51 | +37.17 |
| 2026-08-18 | `STDN` | 89 | $13.31 | $13.31 | +0.00 | — | +0.00 | +0.00 | -29.37 | — |
| 2026-08-18 | `HTFL` | 29 | $41.94 | $41.50 | -12.76 | — | +0.00 | -12.76 | +7.83 | — |
| 2026-08-18 | `UMAC` | 37 | $30.15 | $28.59 | -57.72 | — | +0.00 | -57.72 | -146.52 | — |
| 2026-08-18 | `KOPN` | 224 | $5.32 | $5.03 | -64.96 | — | +0.00 | -64.96 | -89.60 | — |
| 2026-08-18 | `NPWR` | 634 | $1.73 | $1.70 | -19.02 | — | +0.00 | -19.02 | -139.48 | — |
| 2026-08-18 | `SMJF` | 120 | $10.45 | $10.45 | +0.00 | — | +0.00 | +0.00 | +42.00 | — |
| 2026-08-19 | `CAPR` | 177 | $7.08 | $7.19 | +19.47 | — | +0.00 | +19.47 | +56.64 | — |
| 2026-08-20 | `MRNA` | 7 | — | $150.14 | +0.00 | $133.32 | -117.74 | -117.74 | +0.00 | -117.74 |
| 2026-08-20 | `CYPH` | 1013 | — | $1.15 | +0.00 | $1.19 | +40.52 | +40.52 | +0.00 | +40.52 |
| 2026-08-20 | `AZI` | 850 | — | $1.37 | +0.00 | $1.44 | +59.50 | +59.50 | +0.00 | +59.50 |
| 2026-08-20 | `BTGO` | 176 | — | $6.61 | +0.00 | $6.60 | -0.88 | -0.88 | +0.00 | -0.88 |
| 2026-08-20 | `BNTX` | 10 | — | $109.06 | +0.00 | $110.89 | +18.30 | +18.30 | +0.00 | +18.30 |
| 2026-08-20 | `AUTL` | 471 | — | $2.47 | +0.00 | $2.46 | -4.71 | -4.71 | +0.00 | -4.71 |
| 2026-08-20 | `ASST` | 72 | — | $16.00 | +0.00 | $16.13 | +9.36 | +9.36 | +0.00 | +9.36 |
| 2026-08-20 | `BRR` | 560 | — | $2.08 | +0.00 | $2.24 | +89.60 | +89.60 | +0.00 | +89.60 |
| 2026-08-21 | `MRNA` | 7 | $133.32 | $133.11 | -1.47 | $145.13 | +84.14 | +82.67 | -119.21 | -35.07 |
| 2026-08-21 | `CYPH` | 1013 | $1.19 | $1.32 | +131.69 | $1.42 | +101.30 | +232.99 | +172.21 | +273.51 |
| 2026-08-21 | `AZI` | 850 | $1.44 | $1.46 | +17.00 | — | +0.00 | +17.00 | +76.50 | — |
| 2026-08-21 | `BTGO` | 176 | $6.60 | $6.95 | +61.60 | — | +0.00 | +61.60 | +60.72 | — |
| 2026-08-21 | `BNTX` | 10 | $110.89 | $110.92 | +0.30 | — | +0.00 | +0.30 | +18.60 | — |
| 2026-08-21 | `AUTL` | 471 | $2.46 | $2.47 | +4.71 | — | +0.00 | +4.71 | +0.00 | — |
| 2026-08-21 | `ASST` | 72 | $16.13 | $17.66 | +110.16 | — | +0.00 | +110.16 | +119.52 | — |
| 2026-08-21 | `BRR` | 560 | $2.24 | $2.25 | +5.60 | — | +0.00 | +5.60 | +95.20 | — |
| 2026-08-21 | `CAPR` | 181 | — | $6.81 | +0.00 | $6.29 | -94.12 | -94.12 | +0.00 | -94.12 |
| 2026-08-21 | `ARCT` | 110 | — | $11.13 | +0.00 | $13.45 | +255.20 | +255.20 | +0.00 | +255.20 |
| 2026-08-21 | `IOVA` | 135 | — | $9.08 | +0.00 | $8.29 | -106.65 | -106.65 | +0.00 | -106.65 |
| 2026-08-21 | `MRVI` | 148 | — | $8.28 | +0.00 | $8.64 | +53.28 | +53.28 | +0.00 | +53.28 |
| 2026-08-21 | `INO` | 1003 | — | $1.23 | +0.00 | $1.18 | -50.15 | -50.15 | +0.00 | -50.15 |
| 2026-08-21 | `CAN` | 4125 | — | $0.29 | +0.00 | $0.35 | +251.62 | +251.62 | +0.00 | +251.62 |
| 2026-08-24 | `MRNA` | 7 | $145.13 | $142.70 | -17.01 | — | +0.00 | -17.01 | -52.08 | — |
| 2026-08-24 | `CYPH` | 1013 | $1.42 | $1.83 | +415.33 | — | +0.00 | +415.33 | +688.84 | — |
| 2026-08-24 | `CAPR` | 181 | $6.29 | $8.03 | +314.94 | — | +0.00 | +314.94 | +220.82 | — |
| 2026-08-24 | `ARCT` | 110 | $13.45 | $13.33 | -13.20 | — | +0.00 | -13.20 | +242.00 | — |
| 2026-08-24 | `IOVA` | 135 | $8.29 | $8.08 | -28.35 | — | +0.00 | -28.35 | -135.00 | — |
| 2026-08-24 | `MRVI` | 148 | $8.64 | $8.59 | -7.40 | — | +0.00 | -7.40 | +45.88 | — |
| 2026-08-24 | `INO` | 1003 | $1.18 | $1.19 | +10.03 | — | +0.00 | +10.03 | -40.12 | — |
| 2026-08-24 | `CAN` | 4125 | $0.35 | $0.38 | +115.50 | — | +0.00 | +115.50 | +367.13 | — |
| 2026-08-25 | `REAX` | 56 | — | $24.11 | +0.00 | $28.43 | +241.92 | +241.92 | +0.00 | +241.92 |
| 2026-08-25 | `CYPH` | 868 | — | $1.56 | +0.00 | $1.64 | +69.44 | +69.44 | +0.00 | +69.44 |
| 2026-08-25 | `ASST` | 71 | — | $19.04 | +0.00 | $21.39 | +166.85 | +166.85 | +0.00 | +166.85 |
| 2026-08-25 | `SUJA` | 154 | — | $8.79 | +0.00 | $9.33 | +83.16 | +83.16 | +0.00 | +83.16 |
| 2026-08-25 | `ALVO` | 258 | — | $5.24 | +0.00 | $5.05 | -49.02 | -49.02 | +0.00 | -49.02 |
| 2026-08-25 | `FWDI` | 237 | — | $5.71 | +0.00 | $6.05 | +80.58 | +80.58 | +0.00 | +80.58 |
| 2026-08-25 | `DEFT` | 2185 | — | $0.62 | +0.00 | $0.60 | -34.96 | -34.96 | +0.00 | -34.96 |
| 2026-08-25 | `DFDV` | 325 | — | $4.06 | +0.00 | $4.58 | +169.00 | +169.00 | +0.00 | +169.00 |
| 2026-08-26 | `REAX` | 56 | $28.43 | $26.61 | -101.92 | — | +0.00 | -101.92 | +140.00 | — |
| 2026-08-26 | `CYPH` | 868 | $1.64 | $1.60 | -34.72 | — | +0.00 | -34.72 | +34.72 | — |
| 2026-08-26 | `ASST` | 71 | $21.39 | $20.72 | -47.57 | — | +0.00 | -47.57 | +119.28 | — |
| 2026-08-26 | `SUJA` | 154 | $9.33 | $9.39 | +9.24 | $9.44 | +7.70 | +16.94 | +92.40 | +100.10 |
| 2026-08-26 | `ALVO` | 258 | $5.05 | $4.98 | -18.06 | — | +0.00 | -18.06 | -67.08 | — |
| 2026-08-26 | `FWDI` | 237 | $6.05 | $5.97 | -18.96 | — | +0.00 | -18.96 | +61.62 | — |
| 2026-08-26 | `DEFT` | 2185 | $0.60 | $0.60 | -13.11 | — | +0.00 | -13.11 | -48.07 | — |
| 2026-08-26 | `DFDV` | 325 | $4.58 | $4.35 | -74.75 | — | +0.00 | -74.75 | +94.25 | — |
| 2026-08-26 | `USDE` | 239 | — | $5.81 | +0.00 | $5.98 | +40.63 | +40.63 | +0.00 | +40.63 |
| 2026-08-26 | `BTG` | 241 | — | $5.75 | +0.00 | $5.74 | -2.41 | -2.41 | +0.00 | -2.41 |
| 2026-08-26 | `MNRO` | 99 | — | $14.00 | +0.00 | $12.61 | -137.61 | -137.61 | +0.00 | -137.61 |
| 2026-08-26 | `BRR` | 631 | — | $2.20 | +0.00 | $2.17 | -18.93 | -18.93 | +0.00 | -18.93 |
| 2026-08-26 | `PEPG` | 407 | — | $3.41 | +0.00 | $3.22 | -77.33 | -77.33 | +0.00 | -77.33 |
| 2026-08-26 | `AQST` | 273 | — | $5.08 | +0.00 | $5.39 | +84.63 | +84.63 | +0.00 | +84.63 |
| 2026-08-26 | `NEXA` | 88 | — | $15.64 | +0.00 | $15.58 | -5.28 | -5.28 | +0.00 | -5.28 |
| 2026-08-27 | `SUJA` | 154 | $9.44 | $9.41 | -4.62 | — | +0.00 | -4.62 | +95.48 | — |
| 2026-08-27 | `USDE` | 239 | $5.98 | $6.50 | +124.28 | — | +0.00 | +124.28 | +164.91 | — |
| 2026-08-27 | `BTG` | 241 | $5.74 | $5.73 | -2.41 | — | +0.00 | -2.41 | -4.82 | — |
| 2026-08-27 | `MNRO` | 99 | $12.61 | $12.56 | -4.95 | — | +0.00 | -4.95 | -142.56 | — |
| 2026-08-27 | `BRR` | 631 | $2.17 | $2.19 | +12.62 | — | +0.00 | +12.62 | -6.31 | — |
| 2026-08-27 | `PEPG` | 407 | $3.22 | $3.22 | +0.00 | — | +0.00 | +0.00 | -77.33 | — |
| 2026-08-27 | `AQST` | 273 | $5.39 | $5.39 | +0.00 | — | +0.00 | +0.00 | +84.63 | — |
| 2026-08-27 | `NEXA` | 88 | $15.58 | $14.90 | -59.84 | — | +0.00 | -59.84 | -65.12 | — |
| 2026-08-27 | `SLI` | 532 | — | $2.60 | +0.00 | $2.64 | +21.28 | +21.28 | +0.00 | +21.28 |
| 2026-08-27 | `PGY` | 60 | — | $22.93 | +0.00 | $23.26 | +19.80 | +19.80 | +0.00 | +19.80 |
| 2026-08-27 | `MOS` | 57 | — | $24.00 | +0.00 | $23.76 | -13.68 | -13.68 | +0.00 | -13.68 |
| 2026-08-27 | `ANET` | 6 | — | $205.90 | +0.00 | $201.09 | -28.86 | -28.86 | +0.00 | -28.86 |
| 2026-08-27 | `GEN` | 46 | — | $29.83 | +0.00 | $30.50 | +30.82 | +30.82 | +0.00 | +30.82 |
| 2026-08-27 | `DLO` | 90 | — | $15.33 | +0.00 | $15.14 | -17.10 | -17.10 | +0.00 | -17.10 |
| 2026-08-27 | `CRK` | 95 | — | $14.42 | +0.00 | $14.62 | +19.00 | +19.00 | +0.00 | +19.00 |
| 2026-08-27 | `MRVL` | 5 | — | $253.44 | +0.00 | $241.45 | -59.95 | -59.95 | +0.00 | -59.95 |
| 2026-08-28 | `SLI` | 532 | $2.64 | $2.68 | +21.28 | — | +0.00 | +21.28 | +42.56 | — |
| 2026-08-28 | `PGY` | 60 | $23.26 | $23.21 | -3.00 | — | +0.00 | -3.00 | +16.80 | — |
| 2026-08-28 | `MOS` | 57 | $23.76 | $23.95 | +10.83 | — | +0.00 | +10.83 | -2.85 | — |
| 2026-08-28 | `ANET` | 6 | $201.09 | $200.00 | -6.54 | — | +0.00 | -6.54 | -35.40 | — |
| 2026-08-28 | `GEN` | 46 | $30.50 | $30.50 | +0.00 | — | +0.00 | +0.00 | +30.82 | — |
| 2026-08-28 | `DLO` | 90 | $15.14 | $15.19 | +4.50 | — | +0.00 | +4.50 | -12.60 | — |
| 2026-08-28 | `CRK` | 95 | $14.62 | $14.63 | +0.95 | — | +0.00 | +0.95 | +19.95 | — |
| 2026-08-28 | `MRVL` | 5 | $241.45 | $225.26 | -80.95 | — | +0.00 | -80.95 | -140.90 | — |
| 2026-08-28 | `CAPR` | 140 | — | $9.73 | +0.00 | $9.59 | -19.60 | -19.60 | +0.00 | -19.60 |
| 2026-08-28 | `ANF` | 9 | — | $146.07 | +0.00 | $148.42 | +21.15 | +21.15 | +0.00 | +21.15 |
| 2026-08-28 | `MEI` | 76 | — | $17.78 | +0.00 | $18.21 | +32.68 | +32.68 | +0.00 | +32.68 |
| 2026-08-28 | `LVWR` | 984 | — | $1.39 | +0.00 | $1.35 | -39.36 | -39.36 | +0.00 | -39.36 |
| 2026-08-28 | `VYX` | 149 | — | $9.13 | +0.00 | $8.78 | -52.15 | -52.15 | +0.00 | -52.15 |
| 2026-08-28 | `BHVN` | 86 | — | $15.88 | +0.00 | $15.41 | -40.42 | -40.42 | +0.00 | -40.42 |
| 2026-08-28 | `SNPS` | 2 | — | $461.85 | +0.00 | $442.61 | -38.48 | -38.48 | +0.00 | -38.48 |
| 2026-08-28 | `ADCT` | 1232 | — | $1.11 | +0.00 | $1.14 | +36.96 | +36.96 | +0.00 | +36.96 |
| 2026-08-31 | `CAPR` | 140 | $9.59 | $9.50 | -12.60 | — | +0.00 | -12.60 | -32.20 | — |
| 2026-08-31 | `ANF` | 9 | $148.42 | $148.03 | -3.51 | — | +0.00 | -3.51 | +17.64 | — |
| 2026-08-31 | `MEI` | 76 | $18.21 | $18.15 | -4.56 | — | +0.00 | -4.56 | +28.12 | — |
| 2026-08-31 | `LVWR` | 984 | $1.35 | $1.30 | -49.20 | — | +0.00 | -49.20 | -88.56 | — |
| 2026-08-31 | `VYX` | 149 | $8.78 | $8.66 | -17.88 | — | +0.00 | -17.88 | -70.03 | — |
| 2026-08-31 | `BHVN` | 86 | $15.41 | $15.46 | +4.30 | — | +0.00 | +4.30 | -36.12 | — |
| 2026-08-31 | `SNPS` | 2 | $442.61 | $437.95 | -9.32 | — | +0.00 | -9.32 | -47.80 | — |
| 2026-08-31 | `ADCT` | 1232 | $1.14 | $1.13 | -12.32 | — | +0.00 | -12.32 | +24.64 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `GPRO` | 748 | — | $1.78 | +0.00 | $1.39 | -291.72 | -291.72 | +0.00 | -291.72 |
| 2026-09-03 | `MMED` | 55 | — | $23.88 | +0.00 | $23.84 | -2.20 | -2.20 | +0.00 | -2.20 |
| 2026-09-03 | `SION` | 182 | — | $7.31 | +0.00 | $6.75 | -101.92 | -101.92 | +0.00 | -101.92 |
| 2026-09-03 | `CNH` | 97 | — | $13.71 | +0.00 | $13.84 | +12.61 | +12.61 | +0.00 | +12.61 |
| 2026-09-03 | `PBR` | 62 | — | $21.18 | +0.00 | $20.51 | -41.54 | -41.54 | +0.00 | -41.54 |
| 2026-09-03 | `PBR-A` | 69 | — | $19.16 | +0.00 | $18.58 | -40.02 | -40.02 | +0.00 | -40.02 |
| 2026-09-03 | `TARS` | 16 | — | $82.76 | +0.00 | $83.20 | +7.12 | +7.12 | +0.00 | +7.12 |
| 2026-09-03 | `FRVO` | 72 | — | $18.28 | +0.00 | $17.16 | -80.64 | -80.64 | +0.00 | -80.64 |
| 2026-09-04 | `GPRO` | 748 | $1.39 | $1.48 | +67.32 | $1.70 | +164.56 | +231.88 | -224.40 | -59.84 |
| 2026-09-04 | `MMED` | 55 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -2.20 | — |
| 2026-09-04 | `SION` | 182 | $6.75 | $6.68 | -12.74 | — | +0.00 | -12.74 | -114.66 | — |
| 2026-09-04 | `CNH` | 97 | $13.84 | $13.89 | +4.85 | — | +0.00 | +4.85 | +17.46 | — |
| 2026-09-04 | `PBR` | 62 | $20.51 | $20.25 | -16.12 | — | +0.00 | -16.12 | -57.66 | — |
| 2026-09-04 | `PBR-A` | 69 | $18.58 | $18.36 | -15.18 | — | +0.00 | -15.18 | -55.20 | — |
| 2026-09-04 | `TARS` | 16 | $83.20 | $82.70 | -8.08 | — | +0.00 | -8.08 | -0.96 | — |
| 2026-09-04 | `FRVO` | 72 | $17.16 | $17.27 | +7.92 | — | +0.00 | +7.92 | -72.72 | — |
| 2026-09-04 | `FMC` | 99 | — | $12.95 | +0.00 | $12.97 | +1.98 | +1.98 | +0.00 | +1.98 |
| 2026-09-04 | `BRR` | 512 | — | $2.51 | +0.00 | $2.66 | +76.80 | +76.80 | +0.00 | +76.80 |
| 2026-09-04 | `FRNM` | 78 | — | $16.40 | +0.00 | $16.31 | -7.02 | -7.02 | +0.00 | -7.02 |
| 2026-09-04 | `LENZ` | 223 | — | $5.75 | +0.00 | $5.96 | +46.83 | +46.83 | +0.00 | +46.83 |
| 2026-09-04 | `IRD` | 283 | — | $4.53 | +0.00 | $4.67 | +39.62 | +39.62 | +0.00 | +39.62 |
| 2026-09-04 | `SLBT` | 408 | — | $3.15 | +0.00 | $2.88 | -110.16 | -110.16 | +0.00 | -110.16 |
| 2026-09-04 | `BAK` | 655 | — | $1.94 | +0.00 | $1.89 | -32.75 | -32.75 | +0.00 | -32.75 |
| 2026-09-08 | `GPRO` | 748 | $1.70 | $1.56 | -100.98 | — | +0.00 | -100.98 | -160.82 | — |
| 2026-09-08 | `FMC` | 99 | $12.97 | $13.11 | +13.86 | — | +0.00 | +13.86 | +15.84 | — |
| 2026-09-08 | `BRR` | 512 | $2.66 | $2.66 | +0.00 | — | +0.00 | +0.00 | +76.80 | — |
| 2026-09-08 | `FRNM` | 78 | $16.31 | $16.74 | +33.54 | — | +0.00 | +33.54 | +26.52 | — |
| 2026-09-08 | `LENZ` | 223 | $5.96 | $5.95 | -2.23 | — | +0.00 | -2.23 | +44.60 | — |
| 2026-09-08 | `IRD` | 283 | $4.67 | $4.53 | -39.62 | — | +0.00 | -39.62 | +0.00 | — |
| 2026-09-08 | `SLBT` | 408 | $2.88 | $2.88 | +0.00 | — | +0.00 | +0.00 | -110.16 | — |
| 2026-09-08 | `BAK` | 655 | $1.89 | $1.94 | +32.75 | — | +0.00 | +32.75 | +0.00 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `INDP` | 469 | — | $2.70 | +0.00 | $2.77 | +32.83 | +32.83 | +0.00 | +32.83 |
| 2026-09-11 | `BNC` | 258 | — | $4.91 | +0.00 | $4.80 | -28.38 | -28.38 | +0.00 | -28.38 |
| 2026-09-11 | `IRD` | 205 | — | $6.16 | +0.00 | $6.04 | -24.60 | -24.60 | +0.00 | -24.60 |
| 2026-09-11 | `CYPH` | 530 | — | $2.39 | +0.00 | $2.27 | -66.25 | -66.25 | +0.00 | -66.25 |
| 2026-09-11 | `ANGX` | 235 | — | $5.38 | +0.00 | $5.45 | +16.45 | +16.45 | +0.00 | +16.45 |
| 2026-09-11 | `CRWV` | 13 | — | $91.08 | +0.00 | $88.99 | -27.17 | -27.17 | +0.00 | -27.17 |
| 2026-09-11 | `RIOT` | 59 | — | $21.42 | +0.00 | $21.47 | +2.95 | +2.95 | +0.00 | +2.95 |
| 2026-09-11 | `INTC` | 12 | — | $102.47 | +0.00 | $102.94 | +5.64 | +5.64 | +0.00 | +5.64 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +297.49 | TNDM, INO, IREN, TPG, VOR, SLS, TGTX, BTSG | — | $114.01 | $10,265.50 | TNDM×53, INO×1543, IREN×27, TPG×24, VOR×56, SLS×106, TGTX×25, BTSG×20 |
| 2026-08-14 | +5.50 | $114.01 | TNDM×53, INO×1543, IREN×27, TPG×24, VOR×56, SLS×106, TGTX×25, BTSG×20 | $10,276.78 | +11.28 | -380.03 | QMCO, ARX, ZENA, AIRO, BRUN, BCAR, TBBB | TNDM, INO, IREN, TPG, VOR, SLS, TGTX, BTSG | $1,325.75 | $9,841.43 | QMCO×51, ARX×65, ZENA×581, AIRO×115, BRUN×48, BCAR×210, TBBB×26 |
| 2026-08-17 | +2.25 | $1,325.75 | QMCO×51, ARX×65, ZENA×581, AIRO×115, BRUN×48, BCAR×210, TBBB×26 | $9,770.10 | -71.33 | -179.22 | XHG, CAPR, STDN, HTFL, UMAC, KOPN, NPWR, SMJF | QMCO, ARX, ZENA, AIRO, BRUN, BCAR, TBBB | $31.98 | $9,543.43 | XHG×290, CAPR×177, STDN×89, HTFL×29, UMAC×37, KOPN×224, NPWR×634, SMJF×120 |
| 2026-08-18 | -6.20 | $31.98 | XHG×290, CAPR×177, STDN×89, HTFL×29, UMAC×37, KOPN×224, NPWR×634, SMJF×120 | $9,406.52 | -136.91 | -74.34 | — | XHG, STDN, HTFL, UMAC, KOPN, NPWR, SMJF | $8,055.12 | $9,308.28 | CAPR×177 |
| 2026-08-19 | -7.20 | $8,055.12 | CAPR×177 | $9,327.75 | +19.47 | +0.00 | — | CAPR | $9,325.18 | $9,325.18 | — |
| 2026-08-20 | +1.12 | $9,325.18 | — | $9,325.18 | +0.00 | +93.95 | MRNA, CYPH, AZI, BTGO, BNTX, AUTL, ASST, BRR | — | $165.42 | $9,373.05 | MRNA×7, CYPH×1013, AZI×850, BTGO×176, BNTX×10, AUTL×471, ASST×72, BRR×560 |
| 2026-08-21 | +3.25 | $165.42 | MRNA×7, CYPH×1013, AZI×850, BTGO×176, BNTX×10, AUTL×471, ASST×72, BRR×560 | $9,702.64 | +329.59 | +494.62 | CAPR, ARCT, IOVA, MRVI, INO, CAN | AZI, BTGO, BNTX, AUTL, ASST, BRR | $0.56 | $10,118.71 | MRNA×7, CYPH×1013, CAPR×181, ARCT×110, IOVA×135, MRVI×148, INO×1003, CAN×4125 |
| 2026-08-24 | -5.17 | $0.56 | MRNA×7, CYPH×1013, CAPR×181, ARCT×110, IOVA×135, MRVI×148, INO×1003, CAN×4125 | $10,908.55 | +789.84 | +0.00 | — | MRNA, CYPH, CAPR, ARCT, IOVA, MRVI, INO, CAN | $10,841.46 | $10,841.46 | — |
| 2026-08-25 | +1.80 | $10,841.46 | — | $10,841.46 | -0.00 | +726.97 | REAX, CYPH, ASST, SUJA, ALVO, FWDI, DEFT, DFDV | — | $3.64 | $11,519.74 | REAX×56, CYPH×868, ASST×71, SUJA×154, ALVO×258, FWDI×237, DEFT×2185, DFDV×325 |
| 2026-08-26 | +2.02 | $3.64 | REAX×56, CYPH×868, ASST×71, SUJA×154, ALVO×258, FWDI×237, DEFT×2185, DFDV×325 | $11,219.89 | -299.85 | -108.60 | USDE, BTG, MNRO, BRR, PEPG, AQST, NEXA | REAX, CYPH, ASST, ALVO, FWDI, DEFT, DFDV | $0.11 | $11,037.14 | SUJA×154, USDE×239, BTG×241, MNRO×99, BRR×631, PEPG×407, AQST×273, NEXA×88 |
| 2026-08-27 | — | $0.11 | SUJA×154, USDE×239, BTG×241, MNRO×99, BRR×631, PEPG×407, AQST×273, NEXA×88 | $11,102.22 | +65.08 | -28.69 | SLI, PGY, MOS, ANET, GEN, DLO, CRK, MRVL | SUJA, USDE, BTG, MNRO, BRR, PEPG, AQST, NEXA | $298.43 | $11,021.12 | SLI×532, PGY×60, MOS×57, ANET×6, GEN×46, DLO×90, CRK×95, MRVL×5 |
| 2026-08-28 | +0.75 | $298.43 | SLI×532, PGY×60, MOS×57, ANET×6, GEN×46, DLO×90, CRK×95, MRVL×5 | $10,968.19 | -52.93 | -99.22 | CAPR, ANF, MEI, LVWR, VYX, BHVN, SNPS, ADCT | SLI, PGY, MOS, ANET, GEN, DLO, CRK, MRVL | $491.01 | $10,804.93 | CAPR×140, ANF×9, MEI×76, LVWR×984, VYX×149, BHVN×86, SNPS×2, ADCT×1232 |
| 2026-08-31 | -5.85 | $491.01 | CAPR×140, ANF×9, MEI×76, LVWR×984, VYX×149, BHVN×86, SNPS×2, ADCT×1232 | $10,699.84 | -105.09 | +0.00 | — | CAPR, ANF, MEI, LVWR, VYX, BHVN, SNPS, ADCT | $10,657.38 | $10,657.38 | — |
| 2026-09-01 | -6.30 | $10,657.38 | — | $10,657.38 | +0.00 | +0.00 | — | — | $10,657.38 | $10,657.38 | — |
| 2026-09-02 | -3.83 | $10,657.38 | — | $10,657.38 | +0.00 | +0.00 | — | — | $10,657.38 | $10,657.38 | — |
| 2026-09-03 | -0.90 | $10,657.38 | — | $10,657.38 | +0.00 | -538.31 | GPRO, MMED, SION, CNH, PBR, PBR-A, TARS, FRVO | — | $51.50 | $10,093.84 | GPRO×748, MMED×55, SION×182, CNH×97, PBR×62, PBR-A×69, TARS×16, FRVO×72 |
| 2026-09-04 | +2.25 | $51.50 | GPRO×748, MMED×55, SION×182, CNH×97, PBR×62, PBR-A×69, TARS×16, FRVO×72 | $10,121.81 | +27.97 | +179.86 | FMC, BRR, FRNM, LENZ, IRD, SLBT, BAK | MMED, SION, CNH, PBR, PBR-A, TARS, FRVO | $1.14 | $10,254.55 | GPRO×748, FMC×99, BRR×512, FRNM×78, LENZ×223, IRD×283, SLBT×408, BAK×655 |
| 2026-09-08 | -11.47 | $1.14 | GPRO×748, FMC×99, BRR×512, FRNM×78, LENZ×223, IRD×283, SLBT×408, BAK×655 | $10,191.87 | -62.68 | +0.00 | — | GPRO, FMC, BRR, FRNM, LENZ, IRD, SLBT, BAK | $10,150.28 | $10,150.28 | — |
| 2026-09-09 | -13.95 | $10,150.28 | — | $10,150.28 | +0.00 | +0.00 | — | — | $10,150.28 | $10,150.28 | — |
| 2026-09-10 | -13.28 | $10,150.28 | — | $10,150.28 | +0.00 | +0.00 | — | — | $10,150.28 | $10,150.28 | — |
| 2026-09-11 | +0.50 | $10,150.28 | — | $10,150.28 | +0.00 | -88.53 | INDP, BNC, IRD, CYPH, ANGX, CRWV, RIOT, INTC | — | $117.83 | $10,033.64 | INDP×469, BNC×258, IRD×205, CYPH×530, ANGX×235, CRWV×13, RIOT×59, INTC×12 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $8,761.36 | — | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $7,494.40 | — | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $6,250.87 | — | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $5,033.85 | — | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 56 | $22.01 | $2.16 | — | $3,799.14 | — | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $2,556.63 | — | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $1,312.06 | — | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $114.01 | — | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $114.01 | ▲ close $10,265.50 vs 09:30 $10,000.00 (session +297.49) | 16:00 close · cash $114.01 · equity $10,265.50 vs 09:30 $10,000.00 (+265.50; session marks +297.49) · 8 name(s) marked open→close (per-name table). TNDM×53 09:30 $23.33 → close $23.13 -10.60; INO×1543 09:30 $0.81 → close $0.90 +138.87; IREN×27 09:30 $45.98 → close $44.76 -32.94; TPG×24 09:30 $50.62 → close $54.62 +95.92; VOR×56 09:30 $22.01 → close $23.29 +71.68; SLS×106 09:30 $11.70 → close $12.36 +69.96; TGTX×25 09:30 $49.70 → close $47.94 -44.00; BTSG×20 09:30 $59.80 → close $60.23 +8.60 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $114.01 | ▲ 09:30 equity $10,276.78 vs yday $10,265.50 (+11.28) | 09:30 open · cash $114.01 (unchanged overnight, no fees) · equity $10,276.78 vs prior close $10,265.50 (+11.28) · 8 name(s) re-marked at the open (per-name table). TNDM×53 yday $23.13 → 09:30 $22.92 -11.13; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; VOR×56 yday $23.29 → 09:30 $23.33 +2.24; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; BTSG×20 yday $60.23 → 09:30 $59.65 -11.60 | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $1,326.60 | ▼ -26.05 after sell → book $10,274.61; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $2,742.35 | ▲ +148.79 after sell → book $10,255.37; vs 09:30 mark -19.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $3,930.69 | ▼ -55.19 after sell → book $10,253.28; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $5,255.56 | ▲ +107.86 after sell → book $10,251.19; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 56 | $23.33 | $2.18 | $+69.58 | $6,559.87 | ▲ +69.58 after sell → book $10,249.02; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $7,871.93 | ▲ +69.56 after sell → book $10,246.68; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 25 | $47.27 | $2.08 | $-64.90 | $9,051.59 | ▼ -64.90 after sell → book $10,244.59; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $10,242.52 | ▼ -7.12 after sell → book $10,242.52; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 51 | $24.68 | $2.14 | — | $8,981.70 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+111.3; leftover $1280.32 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 65 | $19.57 | $2.19 | — | $7,707.47 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1280.32 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 581 | $2.20 | $7.49 | — | $6,421.77 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $1280.32 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 115 | $11.12 | $2.33 | — | $5,140.64 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1280.32 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BRUN` | 48 | $26.25 | $2.13 | — | $3,878.74 | — | rank by ret_5; rank ret_5; list earn_react; 🔵; ⚪; ret5=+31.2; leftover $1280.32 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BCAR` | 210 | $6.09 | $2.71 | — | $2,597.13 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+27.6; leftover $1280.32 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `TBBB` | 26 | $48.82 | $2.07 | — | $1,325.75 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.0; leftover $1280.32 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,325.75 | ▼ close $9,841.43 vs 09:30 $10,276.78 (session -380.03) | 16:00 close · cash $1,325.75 · equity $9,841.43 vs 09:30 $10,276.78 (-435.35; session marks -380.03) · 7 name(s) marked open→close (per-name table). QMCO×51 09:30 $24.68 → close $26.11 +72.93; ARX×65 09:30 $19.57 → close $19.58 +0.65; ZENA×581 09:30 $2.20 → close $2.14 -34.86; AIRO×115 09:30 $11.12 → close $9.57 -178.25; BRUN×48 09:30 $26.25 → close $22.93 -159.12; BCAR×210 09:30 $6.09 → close $5.83 -54.60; TBBB×26 09:30 $48.82 → close $47.79 -26.78 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,325.75 | ▼ 09:30 equity $9,770.10 vs yday $9,841.43 (-71.33) | 09:30 open · cash $1,325.75 (unchanged overnight, no fees) · equity $9,770.10 vs prior close $9,841.43 (-71.33) · 7 name(s) re-marked at the open (per-name table). QMCO×51 yday $26.11 → 09:30 $24.83 -65.28; ARX×65 yday $19.58 → 09:30 $19.57 -0.65; ZENA×581 yday $2.14 → 09:30 $2.08 -31.96; AIRO×115 yday $9.57 → 09:30 $9.57 +0.00; BRUN×48 yday $22.93 → 09:30 $23.00 +3.36; BCAR×210 yday $5.83 → 09:30 $5.99 +33.60; TBBB×26 yday $47.79 → 09:30 $47.39 -10.40 | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 51 | $24.83 | $2.16 | $+3.34 | $2,589.91 | ▲ +3.34 after sell → book $9,767.94; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 65 | $19.57 | $2.21 | $-4.39 | $3,859.76 | ▼ -4.39 after sell → book $9,765.73; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 581 | $2.08 | $7.60 | $-81.91 | $5,063.54 | ▼ -81.91 after sell → book $9,758.13; vs 09:30 mark -7.60 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 115 | $9.57 | $2.36 | $-182.95 | $6,161.73 | ▼ -182.95 after sell → book $9,755.77; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BRUN` | 48 | $23.00 | $2.15 | $-160.05 | $7,263.57 | ▼ -160.05 after sell → book $9,753.61; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BCAR` | 210 | $5.99 | $2.75 | $-26.46 | $8,518.72 | ▼ -26.46 after sell → book $9,750.86; vs 09:30 mark -2.75 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TBBB` | 26 | $47.39 | $2.09 | $-41.34 | $9,748.77 | ▼ -41.34 after sell → book $9,748.77; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 290 | $4.19 | $3.74 | — | $8,529.93 | — | rank by ret_5; rank ret_5; list yday_mover; ⚪; ret5=+291.8; leftover $1218.60 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 177 | $6.87 | $2.52 | — | $7,311.42 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+62.6; leftover $1218.60 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 89 | $13.64 | $2.26 | — | $6,095.20 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+57.7; leftover $1218.60 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 29 | $41.23 | $2.08 | — | $4,897.45 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+46.0; leftover $1218.60 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 37 | $32.55 | $2.10 | — | $3,691.00 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1218.60 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `KOPN` | 224 | $5.43 | $2.89 | — | $2,471.79 | — | rank by ret_5; rank ret_5; list yday_gainer; ⚪; ret5=+28.8; leftover $1218.60 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 634 | $1.92 | $8.18 | — | $1,246.33 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1218.60 | join🟡 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `SMJF` | 120 | $10.10 | $2.35 | — | $31.98 | — | rank by ret_5; rank ret_5; list mover_buy; ret5=+22.8; leftover $1218.60 | join🔴 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $31.98 | ▼ close $9,543.43 vs 09:30 $9,770.10 (session -179.22) | 16:00 close · cash $31.98 · equity $9,543.43 vs 09:30 $9,770.10 (-226.67; session marks -179.22) · 8 name(s) marked open→close (per-name table). XHG×290 09:30 $4.19 → close $3.91 -81.20; CAPR×177 09:30 $6.87 → close $7.45 +102.66; STDN×89 09:30 $13.64 → close $13.31 -29.37; HTFL×29 09:30 $41.23 → close $41.94 +20.59; UMAC×37 09:30 $32.55 → close $30.15 -88.80; KOPN×224 09:30 $5.43 → close $5.32 -24.64; NPWR×634 09:30 $1.92 → close $1.73 -120.46; SMJF×120 09:30 $10.10 → close $10.45 +42.00 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $31.98 | ▼ 09:30 equity $9,406.52 vs yday $9,543.43 (-136.91) | 09:30 open · cash $31.98 (unchanged overnight, no fees) · equity $9,406.52 vs prior close $9,543.43 (-136.91) · 8 name(s) re-marked at the open (per-name table). XHG×290 yday $3.91 → 09:30 $3.94 +8.70; CAPR×177 yday $7.45 → 09:30 $7.50 +8.85; STDN×89 yday $13.31 → 09:30 $13.31 +0.00; HTFL×29 yday $41.94 → 09:30 $41.50 -12.76; UMAC×37 yday $30.15 → 09:30 $28.59 -57.72; KOPN×224 yday $5.32 → 09:30 $5.03 -64.96; NPWR×634 yday $1.73 → 09:30 $1.70 -19.02; SMJF×120 yday $10.45 → 09:30 $10.45 +0.00 | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 290 | $3.94 | $3.80 | $-80.04 | $1,170.79 | ▼ -80.04 after sell → book $9,402.73; vs 09:30 mark -3.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 89 | $13.31 | $2.28 | $-33.91 | $2,353.09 | ▼ -33.91 after sell → book $9,400.44; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 29 | $41.50 | $2.10 | $+3.66 | $3,554.50 | ▲ +3.66 after sell → book $9,398.35; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 37 | $28.59 | $2.12 | $-150.74 | $4,610.21 | ▼ -150.74 after sell → book $9,396.23; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `KOPN` | 224 | $5.03 | $2.94 | $-95.43 | $5,733.99 | ▼ -95.43 after sell → book $9,393.29; vs 09:30 mark -2.94 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 634 | $1.70 | $8.29 | $-155.95 | $6,803.50 | ▼ -155.95 after sell → book $9,385.00; vs 09:30 mark -8.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `SMJF` | 120 | $10.45 | $2.38 | $+37.27 | $8,055.12 | ▲ +37.27 after sell → book $9,382.62; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,055.12 | ▼ close $9,308.28 vs 09:30 $9,406.52 (session -74.34) | 16:00 close · cash $8,055.12 · equity $9,308.28 vs 09:30 $9,406.52 (-98.24; session marks -74.34) · 1 name(s) marked open→close (per-name table). CAPR×177 09:30 $7.50 → close $7.08 -74.34 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,055.12 | ▲ 09:30 equity $9,327.75 vs yday $9,308.28 (+19.47) | 09:30 open · cash $8,055.12 (unchanged overnight, no fees) · equity $9,327.75 vs prior close $9,308.28 (+19.47) · 1 name(s) re-marked at the open (per-name table). CAPR×177 yday $7.08 → 09:30 $7.19 +19.47 | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 177 | $7.19 | $2.56 | $+51.56 | $9,325.18 | ▲ +51.56 after sell → book $9,325.18; vs 09:30 mark -2.57 | dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,325.18 | ▲ close $9,325.18 vs 09:30 $9,327.75 (session +0.00) | 16:00 close · cash $9,325.18 · no lots left · equity $9,325.18. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,325.18 | ▲ 09:30 equity $9,325.18 vs yday $9,325.18 (+0.00) | 09:30 open · cash $9,325.18 · no holdings · equity $9,325.18 vs prior close $9,325.18 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 7 | $150.14 | $2.01 | — | $8,272.19 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1165.65 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1013 | $1.15 | $13.07 | — | $7,094.18 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1165.65 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 850 | $1.37 | $10.96 | — | $5,918.71 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $1165.65 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 176 | $6.61 | $2.52 | — | $4,753.71 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1165.65 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BNTX` | 10 | $109.06 | $2.02 | — | $3,661.09 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+22.0; leftover $1165.65 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 471 | $2.47 | $6.08 | — | $2,491.65 | — | rank by ret_5; rank ret_5; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1165.65 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 72 | $16.00 | $2.21 | — | $1,337.44 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1165.65 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BRR` | 560 | $2.08 | $7.22 | — | $165.42 | — | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+18.0; leftover $1165.65 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $165.42 | ▲ close $9,373.05 vs 09:30 $9,325.18 (session +93.95) | 16:00 close · cash $165.42 · equity $9,373.05 vs 09:30 $9,325.18 (+47.87; session marks +93.95) · 8 name(s) marked open→close (per-name table). MRNA×7 09:30 $150.14 → close $133.32 -117.74; CYPH×1013 09:30 $1.15 → close $1.19 +40.52; AZI×850 09:30 $1.37 → close $1.44 +59.50; BTGO×176 09:30 $6.61 → close $6.60 -0.88; BNTX×10 09:30 $109.06 → close $110.89 +18.30; AUTL×471 09:30 $2.47 → close $2.46 -4.71; ASST×72 09:30 $16.00 → close $16.13 +9.36; BRR×560 09:30 $2.08 → close $2.24 +89.60 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $165.42 | ▲ 09:30 equity $9,702.64 vs yday $9,373.05 (+329.59) | 09:30 open · cash $165.42 (unchanged overnight, no fees) · equity $9,702.64 vs prior close $9,373.05 (+329.59) · 8 name(s) re-marked at the open (per-name table). MRNA×7 yday $133.32 → 09:30 $133.11 -1.47; CYPH×1013 yday $1.19 → 09:30 $1.32 +131.69; AZI×850 yday $1.44 → 09:30 $1.46 +17.00; BTGO×176 yday $6.60 → 09:30 $6.95 +61.60; BNTX×10 yday $110.89 → 09:30 $110.92 +0.30; AUTL×471 yday $2.46 → 09:30 $2.47 +4.71; ASST×72 yday $16.13 → 09:30 $17.66 +110.16; BRR×560 yday $2.24 → 09:30 $2.25 +5.60 | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 850 | $1.46 | $11.12 | $+54.42 | $1,395.30 | ▲ +54.42 after sell → book $9,691.52; vs 09:30 mark -11.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 176 | $6.95 | $2.56 | $+55.64 | $2,615.94 | ▲ +55.64 after sell → book $9,688.96; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BNTX` | 10 | $110.92 | $2.04 | $+14.54 | $3,723.10 | ▲ +14.54 after sell → book $9,686.92; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AUTL` | 471 | $2.47 | $6.16 | $-12.24 | $4,880.31 | ▼ -12.24 after sell → book $9,680.76; vs 09:30 mark -6.16 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 72 | $17.66 | $2.23 | $+115.09 | $6,149.60 | ▲ +115.09 after sell → book $9,678.53; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BRR` | 560 | $2.25 | $7.33 | $+80.65 | $7,402.27 | ▲ +80.65 after sell → book $9,671.20; vs 09:30 mark -7.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 181 | $6.81 | $2.53 | — | $6,167.13 | — | rank by ret_5; rank ret_5; list yday_mover; 🔵; ret5=+62.5; leftover $1233.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 110 | $11.13 | $2.32 | — | $4,940.51 | — | rank by ret_5; rank ret_5; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1233.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 135 | $9.08 | $2.40 | — | $3,712.32 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $1233.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 148 | $8.28 | $2.43 | — | $2,484.44 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; leftover $1233.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `INO` | 1003 | $1.23 | $12.94 | — | $1,237.81 | — | rank by ret_5; rank ret_5; list mover_buy; 🔵; ⚪; ret5=+34.4; leftover $1233.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAN` | 4125 | $0.29 | $24.50 | — | $0.56 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $1233.71 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.56 | ▲ close $10,118.71 vs 09:30 $9,702.64 (session +494.62) | 16:00 close · cash $0.56 · equity $10,118.71 vs 09:30 $9,702.64 (+416.07; session marks +494.62) · 8 name(s) marked open→close (per-name table). MRNA×7 09:30 $133.11 → close $145.13 +84.14; CYPH×1013 09:30 $1.32 → close $1.42 +101.30; CAPR×181 09:30 $6.81 → close $6.29 -94.12; ARCT×110 09:30 $11.13 → close $13.45 +255.20; IOVA×135 09:30 $9.08 → close $8.29 -106.65; MRVI×148 09:30 $8.28 → close $8.64 +53.28; INO×1003 09:30 $1.23 → close $1.18 -50.15; CAN×4125 09:30 $0.29 → close $0.35 +251.62 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.56 | ▲ 09:30 equity $10,908.55 vs yday $10,118.71 (+789.84) | 09:30 open · cash $0.56 (unchanged overnight, no fees) · equity $10,908.55 vs prior close $10,118.71 (+789.84) · 8 name(s) re-marked at the open (per-name table). MRNA×7 yday $145.13 → 09:30 $142.70 -17.01; CYPH×1013 yday $1.42 → 09:30 $1.83 +415.33; CAPR×181 yday $6.29 → 09:30 $8.03 +314.94; ARCT×110 yday $13.45 → 09:30 $13.33 -13.20; IOVA×135 yday $8.29 → 09:30 $8.08 -28.35; MRVI×148 yday $8.64 → 09:30 $8.59 -7.40; INO×1003 yday $1.18 → 09:30 $1.19 +10.03; CAN×4125 yday $0.35 → 09:30 $0.38 +115.50 | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 7 | $142.70 | $2.03 | $-56.12 | $997.43 | ▼ -56.12 after sell → book $10,906.52; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1013 | $1.83 | $13.25 | $+662.52 | $2,837.97 | ▲ +662.52 after sell → book $10,893.26; vs 09:30 mark -13.26 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 181 | $8.03 | $2.57 | $+215.71 | $4,288.82 | ▲ +215.71 after sell → book $10,890.69; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 110 | $13.33 | $2.35 | $+237.33 | $5,752.77 | ▲ +237.33 after sell → book $10,888.34; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `IOVA` | 135 | $8.08 | $2.43 | $-139.82 | $6,841.15 | ▼ -139.82 after sell → book $10,885.91; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 148 | $8.59 | $2.47 | $+40.98 | $8,110.00 | ▲ +40.98 after sell → book $10,883.44; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `INO` | 1003 | $1.19 | $13.12 | $-66.17 | $9,290.45 | ▼ -66.17 after sell → book $10,870.33; vs 09:30 mark -13.11 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟡 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CAN` | 4125 | $0.38 | $28.87 | $+313.75 | $10,841.46 | ▲ +313.75 after sell → book $10,841.46; vs 09:30 mark -28.87 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,841.46 | ▲ close $10,841.46 vs 09:30 $10,908.55 (session +0.00) | 16:00 close · cash $10,841.46 · no lots left · equity $10,841.46. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,841.46 | ▲ 09:30 equity $10,841.46 vs yday $10,841.46 (-0.00) | 09:30 open · cash $10,841.46 · no holdings · equity $10,841.46 vs prior close $10,841.46 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 56 | $24.11 | $2.16 | — | $9,489.14 | — | rank by ret_5; rank ret_5; list yday_mover; ret5=+891.7; leftover $1355.18 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 868 | $1.56 | $11.20 | — | $8,123.86 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $1355.18 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 71 | $19.04 | $2.20 | — | $6,769.82 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+49.5; leftover $1355.18 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 154 | $8.79 | $2.45 | — | $5,413.71 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $1355.18 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 258 | $5.24 | $3.33 | — | $4,058.46 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1355.18 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FWDI` | 237 | $5.71 | $3.06 | — | $2,702.13 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+33.9; leftover $1355.18 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 2185 | $0.62 | $20.10 | — | $1,327.33 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+33.3; leftover $1355.18 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DFDV` | 325 | $4.06 | $4.19 | — | $3.64 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+29.4; leftover $1355.18 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.64 | ▲ close $11,519.74 vs 09:30 $10,841.46 (session +726.97) | 16:00 close · cash $3.64 · equity $11,519.74 vs 09:30 $10,841.46 (+678.28; session marks +726.97) · 8 name(s) marked open→close (per-name table). REAX×56 09:30 $24.11 → close $28.43 +241.92; CYPH×868 09:30 $1.56 → close $1.64 +69.44; ASST×71 09:30 $19.04 → close $21.39 +166.85; SUJA×154 09:30 $8.79 → close $9.33 +83.16; ALVO×258 09:30 $5.24 → close $5.05 -49.02; FWDI×237 09:30 $5.71 → close $6.05 +80.58; DEFT×2185 09:30 $0.62 → close $0.60 -34.96; DFDV×325 09:30 $4.06 → close $4.58 +169.00 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.64 | ▼ 09:30 equity $11,219.89 vs yday $11,519.74 (-299.85) | 09:30 open · cash $3.64 (unchanged overnight, no fees) · equity $11,219.89 vs prior close $11,519.74 (-299.85) · 8 name(s) re-marked at the open (per-name table). REAX×56 yday $28.43 → 09:30 $26.61 -101.92; CYPH×868 yday $1.64 → 09:30 $1.60 -34.72; ASST×71 yday $21.39 → 09:30 $20.72 -47.57; SUJA×154 yday $9.33 → 09:30 $9.39 +9.24; ALVO×258 yday $5.05 → 09:30 $4.98 -18.06; FWDI×237 yday $6.05 → 09:30 $5.97 -18.96; DEFT×2185 yday $0.60 → 09:30 $0.60 -13.11; DFDV×325 yday $4.58 → 09:30 $4.35 -74.75 | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 56 | $26.61 | $2.18 | $+135.66 | $1,491.62 | ▲ +135.66 after sell → book $11,217.71; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 868 | $1.60 | $11.35 | $+12.17 | $2,869.07 | ▲ +12.17 after sell → book $11,206.35; vs 09:30 mark -11.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 71 | $20.72 | $2.23 | $+114.85 | $4,337.96 | ▲ +114.85 after sell → book $11,204.13; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ALVO` | 258 | $4.98 | $3.38 | $-73.79 | $5,619.42 | ▼ -73.79 after sell → book $11,200.75; vs 09:30 mark -3.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FWDI` | 237 | $5.97 | $3.11 | $+55.45 | $7,031.20 | ▲ +55.45 after sell → book $11,197.64; vs 09:30 mark -3.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `DEFT` | 2185 | $0.60 | $19.99 | $-88.17 | $8,317.83 | ▼ -88.17 after sell → book $11,177.64; vs 09:30 mark -20.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `DFDV` | 325 | $4.35 | $4.26 | $+85.80 | $9,727.33 | ▲ +85.80 after sell → book $11,173.39; vs 09:30 mark -4.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 239 | $5.81 | $3.08 | — | $8,335.65 | — | rank by ret_5; rank ret_5; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $1389.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BTG` | 241 | $5.75 | $3.11 | — | $6,946.79 | — | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+17.9; leftover $1389.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `MNRO` | 99 | $14.00 | $2.29 | — | $5,558.51 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+17.8; leftover $1389.62 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BRR` | 631 | $2.20 | $8.14 | — | $4,162.17 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+17.8; leftover $1389.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `PEPG` | 407 | $3.41 | $5.25 | — | $2,769.05 | — | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+17.7; leftover $1389.62 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `AQST` | 273 | $5.08 | $3.52 | — | $1,378.69 | — | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+17.6; leftover $1389.62 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `NEXA` | 88 | $15.64 | $2.25 | — | $0.11 | — | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+17.3; leftover $1389.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.11 | ▼ close $11,037.14 vs 09:30 $11,219.89 (session -108.60) | 16:00 close · cash $0.11 · equity $11,037.14 vs 09:30 $11,219.89 (-182.75; session marks -108.60) · 8 name(s) marked open→close (per-name table). SUJA×154 09:30 $9.39 → close $9.44 +7.70; USDE×239 09:30 $5.81 → close $5.98 +40.63; BTG×241 09:30 $5.75 → close $5.74 -2.41; MNRO×99 09:30 $14.00 → close $12.61 -137.61; BRR×631 09:30 $2.20 → close $2.17 -18.93; PEPG×407 09:30 $3.41 → close $3.22 -77.33; AQST×273 09:30 $5.08 → close $5.39 +84.63; NEXA×88 09:30 $15.64 → close $15.58 -5.28 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.11 | ▲ 09:30 equity $11,102.22 vs yday $11,037.14 (+65.08) | 09:30 open · cash $0.11 (unchanged overnight, no fees) · equity $11,102.22 vs prior close $11,037.14 (+65.08) · 8 name(s) re-marked at the open (per-name table). SUJA×154 yday $9.44 → 09:30 $9.41 -4.62; USDE×239 yday $5.98 → 09:30 $6.50 +124.28; BTG×241 yday $5.74 → 09:30 $5.73 -2.41; MNRO×99 yday $12.61 → 09:30 $12.56 -4.95; BRR×631 yday $2.17 → 09:30 $2.19 +12.62; PEPG×407 yday $3.22 → 09:30 $3.22 +0.00; AQST×273 yday $5.39 → 09:30 $5.39 +0.00; NEXA×88 yday $15.58 → 09:30 $14.90 -59.84 | — |
| 2026-08-27 09:30 ET | **SELL** | `SUJA` | 154 | $9.41 | $2.49 | $+90.54 | $1,446.76 | ▲ +90.54 after sell → book $11,099.73; vs 09:30 mark -2.49 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 239 | $6.50 | $3.14 | $+158.69 | $2,997.13 | ▲ +158.69 after sell → book $11,096.60; vs 09:30 mark -3.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BTG` | 241 | $5.73 | $3.16 | $-11.09 | $4,374.90 | ▼ -11.09 after sell → book $11,093.44; vs 09:30 mark -3.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `MNRO` | 99 | $12.56 | $2.31 | $-147.16 | $5,616.02 | ▼ -147.16 after sell → book $11,091.12; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BRR` | 631 | $2.19 | $8.26 | $-22.71 | $6,989.66 | ▼ -22.71 after sell → book $11,082.87; vs 09:30 mark -8.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PEPG` | 407 | $3.22 | $5.33 | $-87.91 | $8,294.87 | ▼ -87.91 after sell → book $11,077.54; vs 09:30 mark -5.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AQST` | 273 | $5.39 | $3.58 | $+77.53 | $9,762.76 | ▲ +77.53 after sell → book $11,073.96; vs 09:30 mark -3.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NEXA` | 88 | $14.90 | $2.28 | $-69.65 | $11,071.68 | ▼ -69.65 after sell → book $11,071.68; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 532 | $2.60 | $6.86 | — | $9,681.62 | — | rank by ret_5; rank ret_5; list flatten; ret5=+13.0; leftover $1383.96 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PGY` | 60 | $22.93 | $2.17 | — | $8,303.65 | — | rank by ret_5; rank ret_5; list mover_buy; 🔵; ret5=+9.5; leftover $1383.96 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 57 | $24.00 | $2.16 | — | $6,933.49 | — | rank by ret_5; rank ret_5; list flatten; ret5=+8.7; leftover $1383.96 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `ANET` | 6 | $205.90 | $2.01 | — | $5,696.08 | — | rank by ret_5; rank ret_5; list mover_buy; 🔵; ret5=+8.5; leftover $1383.96 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 46 | $29.83 | $2.13 | — | $4,321.77 | — | rank by ret_5; rank ret_5; list mover_buy; 🔵; ret5=+7.6; leftover $1383.96 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 90 | $15.33 | $2.26 | — | $2,939.81 | — | rank by ret_5; rank ret_5; list mover_buy; 🔵; ret5=+7.4; leftover $1383.96 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 95 | $14.42 | $2.27 | — | $1,567.64 | — | rank by ret_5; rank ret_5; list flatten; ret5=+7.1; leftover $1383.96 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 5 | $253.44 | $2.00 | — | $298.43 | — | rank by ret_5; rank ret_5; list mover_buy; 🔵; ret5=+3.3; leftover $1383.96 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $298.43 | ▼ close $11,021.12 vs 09:30 $11,102.22 (session -28.69) | 16:00 close · cash $298.43 · equity $11,021.12 vs 09:30 $11,102.22 (-81.10; session marks -28.69) · 8 name(s) marked open→close (per-name table). SLI×532 09:30 $2.60 → close $2.64 +21.28; PGY×60 09:30 $22.93 → close $23.26 +19.80; MOS×57 09:30 $24.00 → close $23.76 -13.68; ANET×6 09:30 $205.90 → close $201.09 -28.86; GEN×46 09:30 $29.83 → close $30.50 +30.82; DLO×90 09:30 $15.33 → close $15.14 -17.10; CRK×95 09:30 $14.42 → close $14.62 +19.00; MRVL×5 09:30 $253.44 → close $241.45 -59.95 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $298.43 | ▼ 09:30 equity $10,968.19 vs yday $11,021.12 (-52.93) | 09:30 open · cash $298.43 (unchanged overnight, no fees) · equity $10,968.19 vs prior close $11,021.12 (-52.93) · 8 name(s) re-marked at the open (per-name table). SLI×532 yday $2.64 → 09:30 $2.68 +21.28; PGY×60 yday $23.26 → 09:30 $23.21 -3.00; MOS×57 yday $23.76 → 09:30 $23.95 +10.83; ANET×6 yday $201.09 → 09:30 $200.00 -6.54; GEN×46 yday $30.50 → 09:30 $30.50 +0.00; DLO×90 yday $15.14 → 09:30 $15.19 +4.50; CRK×95 yday $14.62 → 09:30 $14.63 +0.95; MRVL×5 yday $241.45 → 09:30 $225.26 -80.95 | — |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 532 | $2.68 | $6.96 | $+28.73 | $1,717.23 | ▲ +28.73 after sell → book $10,961.23; vs 09:30 mark -6.96 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `PGY` | 60 | $23.21 | $2.19 | $+12.44 | $3,107.64 | ▲ +12.44 after sell → book $10,959.04; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 57 | $23.95 | $2.18 | $-7.19 | $4,470.61 | ▼ -7.19 after sell → book $10,956.86; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `ANET` | 6 | $200.00 | $2.03 | $-39.44 | $5,668.58 | ▼ -39.44 after sell → book $10,954.83; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 46 | $30.50 | $2.15 | $+26.54 | $7,069.43 | ▲ +26.54 after sell → book $10,952.68; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DLO` | 90 | $15.19 | $2.29 | $-17.15 | $8,434.24 | ▼ -17.15 after sell → book $10,950.39; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRK` | 95 | $14.63 | $2.30 | $+15.37 | $9,821.79 | ▲ +15.37 after sell → book $10,948.09; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `MRVL` | 5 | $225.26 | $2.02 | $-144.93 | $10,946.07 | ▼ -144.93 after sell → book $10,946.07; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 140 | $9.73 | $2.41 | — | $9,581.46 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+47.1; leftover $1368.26 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $146.07 | $2.02 | — | $8,264.81 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1368.26 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MEI` | 76 | $17.78 | $2.22 | — | $6,911.31 | — | rank by ret_5; rank ret_5; list mover_buy; 🔵; ⚪; ret5=+22.9; leftover $1368.26 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 984 | $1.39 | $12.69 | — | $5,530.86 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+20.4; leftover $1368.26 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VYX` | 149 | $9.13 | $2.44 | — | $4,168.05 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+20.0; leftover $1368.26 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 86 | $15.88 | $2.25 | — | $2,800.12 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+19.4; leftover $1368.26 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SNPS` | 2 | $461.85 | $2.00 | — | $1,874.43 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+16.8; leftover $1368.26 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADCT` | 1232 | $1.11 | $15.89 | — | $491.01 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+16.7; leftover $1368.26 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $491.01 | ▼ close $10,804.93 vs 09:30 $10,968.19 (session -99.22) | 16:00 close · cash $491.01 · equity $10,804.93 vs 09:30 $10,968.19 (-163.26; session marks -99.22) · 8 name(s) marked open→close (per-name table). CAPR×140 09:30 $9.73 → close $9.59 -19.60; ANF×9 09:30 $146.07 → close $148.42 +21.15; MEI×76 09:30 $17.78 → close $18.21 +32.68; LVWR×984 09:30 $1.39 → close $1.35 -39.36; VYX×149 09:30 $9.13 → close $8.78 -52.15; BHVN×86 09:30 $15.88 → close $15.41 -40.42; SNPS×2 09:30 $461.85 → close $442.61 -38.48; ADCT×1232 09:30 $1.11 → close $1.14 +36.96 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $491.01 | ▼ 09:30 equity $10,699.84 vs yday $10,804.93 (-105.09) | 09:30 open · cash $491.01 (unchanged overnight, no fees) · equity $10,699.84 vs prior close $10,804.93 (-105.09) · 8 name(s) re-marked at the open (per-name table). CAPR×140 yday $9.59 → 09:30 $9.50 -12.60; ANF×9 yday $148.42 → 09:30 $148.03 -3.51; MEI×76 yday $18.21 → 09:30 $18.15 -4.56; LVWR×984 yday $1.35 → 09:30 $1.30 -49.20; VYX×149 yday $8.78 → 09:30 $8.66 -17.88; BHVN×86 yday $15.41 → 09:30 $15.46 +4.30; SNPS×2 yday $442.61 → 09:30 $437.95 -9.32; ADCT×1232 yday $1.14 → 09:30 $1.13 -12.32 | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 140 | $9.50 | $2.44 | $-37.05 | $1,818.57 | ▼ -37.05 after sell → book $10,697.40; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.03 | $2.04 | $+13.59 | $3,148.80 | ▲ +13.59 after sell → book $10,695.36; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MEI` | 76 | $18.15 | $2.24 | $+23.66 | $4,525.96 | ▲ +23.66 after sell → book $10,693.12; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LVWR` | 984 | $1.30 | $12.87 | $-114.12 | $5,792.29 | ▼ -114.12 after sell → book $10,680.25; vs 09:30 mark -12.87 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `VYX` | 149 | $8.66 | $2.47 | $-74.94 | $7,080.16 | ▼ -74.94 after sell → book $10,677.78; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BHVN` | 86 | $15.46 | $2.27 | $-40.64 | $8,407.45 | ▼ -40.64 after sell → book $10,675.51; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SNPS` | 2 | $437.95 | $2.02 | $-51.81 | $9,281.33 | ▼ -51.81 after sell → book $10,673.49; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `ADCT` | 1232 | $1.13 | $16.11 | $-7.36 | $10,657.38 | ▼ -7.36 after sell → book $10,657.38; vs 09:30 mark -16.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,657.38 | ▲ close $10,657.38 vs 09:30 $10,699.84 (session +0.00) | 16:00 close · cash $10,657.38 · no lots left · equity $10,657.38. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,657.38 | ▲ 09:30 equity $10,657.38 vs yday $10,657.38 (+0.00) | 09:30 open · cash $10,657.38 · no holdings · equity $10,657.38 vs prior close $10,657.38 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,657.38 | ▲ close $10,657.38 vs 09:30 $10,657.38 (session +0.00) | 16:00 close · cash $10,657.38 · no lots left · equity $10,657.38. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,657.38 | ▲ 09:30 equity $10,657.38 vs yday $10,657.38 (+0.00) | 09:30 open · cash $10,657.38 · no holdings · equity $10,657.38 vs prior close $10,657.38 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,657.38 | ▲ close $10,657.38 vs 09:30 $10,657.38 (session +0.00) | 16:00 close · cash $10,657.38 · no lots left · equity $10,657.38. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,657.38 | ▲ 09:30 equity $10,657.38 vs yday $10,657.38 (+0.00) | 09:30 open · cash $10,657.38 · no holdings · equity $10,657.38 vs prior close $10,657.38 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 748 | $1.78 | $9.65 | — | $9,316.30 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+183.1; leftover $1332.17 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 55 | $23.88 | $2.15 | — | $8,000.74 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1332.17 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SION` | 182 | $7.31 | $2.54 | — | $6,667.78 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+18.5; leftover $1332.17 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 97 | $13.71 | $2.28 | — | $5,335.63 | — | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+17.5; leftover $1332.17 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PBR` | 62 | $21.18 | $2.18 | — | $4,020.30 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+17.5; leftover $1332.17 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `PBR-A` | 69 | $19.16 | $2.20 | — | $2,696.06 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+17.4; leftover $1332.17 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `TARS` | 16 | $82.76 | $2.04 | — | $1,369.86 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+17.1; leftover $1332.17 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 72 | $18.28 | $2.21 | — | $51.50 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+16.5; leftover $1332.17 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $51.50 | ▼ close $10,093.84 vs 09:30 $10,657.38 (session -538.31) | 16:00 close · cash $51.50 · equity $10,093.84 vs 09:30 $10,657.38 (-563.54; session marks -538.31) · 8 name(s) marked open→close (per-name table). GPRO×748 09:30 $1.78 → close $1.39 -291.72; MMED×55 09:30 $23.88 → close $23.84 -2.20; SION×182 09:30 $7.31 → close $6.75 -101.92; CNH×97 09:30 $13.71 → close $13.84 +12.61; PBR×62 09:30 $21.18 → close $20.51 -41.54; PBR-A×69 09:30 $19.16 → close $18.58 -40.02; TARS×16 09:30 $82.76 → close $83.20 +7.12; FRVO×72 09:30 $18.28 → close $17.16 -80.64 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.50 | ▲ 09:30 equity $10,121.81 vs yday $10,093.84 (+27.97) | 09:30 open · cash $51.50 (unchanged overnight, no fees) · equity $10,121.81 vs prior close $10,093.84 (+27.97) · 8 name(s) re-marked at the open (per-name table). GPRO×748 yday $1.39 → 09:30 $1.48 +67.32; MMED×55 yday $23.84 → 09:30 $23.84 +0.00; SION×182 yday $6.75 → 09:30 $6.68 -12.74; CNH×97 yday $13.84 → 09:30 $13.89 +4.85; PBR×62 yday $20.51 → 09:30 $20.25 -16.12; PBR-A×69 yday $18.58 → 09:30 $18.36 -15.18; TARS×16 yday $83.20 → 09:30 $82.70 -8.08; FRVO×72 yday $17.16 → 09:30 $17.27 +7.92 | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 55 | $23.84 | $2.18 | $-6.53 | $1,360.52 | ▼ -6.53 after sell → book $10,119.63; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SION` | 182 | $6.68 | $2.58 | $-119.77 | $2,573.70 | ▼ -119.77 after sell → book $10,117.05; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 97 | $13.89 | $2.31 | $+12.87 | $3,918.73 | ▲ +12.87 after sell → book $10,114.75; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PBR` | 62 | $20.25 | $2.20 | $-62.03 | $5,172.03 | ▼ -62.03 after sell → book $10,112.55; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PBR-A` | 69 | $18.36 | $2.22 | $-59.62 | $6,436.65 | ▼ -59.62 after sell → book $10,110.33; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `TARS` | 16 | $82.70 | $2.06 | $-5.06 | $7,757.79 | ▼ -5.06 after sell → book $10,108.27; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `FRVO` | 72 | $17.27 | $2.23 | $-77.15 | $8,999.01 | ▼ -77.15 after sell → book $10,106.05; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `FMC` | 99 | $12.95 | $2.29 | — | $7,714.67 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+21.8; leftover $1285.57 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 512 | $2.51 | $6.60 | — | $6,422.94 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+21.8; leftover $1285.57 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `FRNM` | 78 | $16.40 | $2.22 | — | $5,141.52 | — | rank by ret_5; rank ret_5; list mover_buy; 🔵; ⚪; ret5=+21.2; leftover $1285.57 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `LENZ` | 223 | $5.75 | $2.88 | — | $3,856.39 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+21.2; leftover $1285.57 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 283 | $4.53 | $3.65 | — | $2,570.75 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.5; leftover $1285.57 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 408 | $3.15 | $5.26 | — | $1,280.29 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+19.3; leftover $1285.57 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 655 | $1.94 | $8.45 | — | $1.14 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+18.3; leftover $1285.57 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.14 | ▲ close $10,254.55 vs 09:30 $10,121.81 (session +179.86) | 16:00 close · cash $1.14 · equity $10,254.55 vs 09:30 $10,121.81 (+132.74; session marks +179.86) · 8 name(s) marked open→close (per-name table). GPRO×748 09:30 $1.48 → close $1.70 +164.56; FMC×99 09:30 $12.95 → close $12.97 +1.98; BRR×512 09:30 $2.51 → close $2.66 +76.80; FRNM×78 09:30 $16.40 → close $16.31 -7.02; LENZ×223 09:30 $5.75 → close $5.96 +46.83; IRD×283 09:30 $4.53 → close $4.67 +39.62; SLBT×408 09:30 $3.15 → close $2.88 -110.16; BAK×655 09:30 $1.94 → close $1.89 -32.75 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.14 | ▼ 09:30 equity $10,191.87 vs yday $10,254.55 (-62.68) | 09:30 open · cash $1.14 (unchanged overnight, no fees) · equity $10,191.87 vs prior close $10,254.55 (-62.68) · 8 name(s) re-marked at the open (per-name table). GPRO×748 yday $1.70 → 09:30 $1.56 -100.98; FMC×99 yday $12.97 → 09:30 $13.11 +13.86; BRR×512 yday $2.66 → 09:30 $2.66 +0.00; FRNM×78 yday $16.31 → 09:30 $16.74 +33.54; LENZ×223 yday $5.96 → 09:30 $5.95 -2.23; IRD×283 yday $4.67 → 09:30 $4.53 -39.62; SLBT×408 yday $2.88 → 09:30 $2.88 +0.00; BAK×655 yday $1.89 → 09:30 $1.94 +32.75 | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 748 | $1.56 | $9.78 | $-180.25 | $1,161.98 | ▼ -180.25 after sell → book $10,182.09; vs 09:30 mark -9.78 | dropped from list after 2 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `FMC` | 99 | $13.11 | $2.31 | $+11.24 | $2,457.55 | ▲ +11.24 after sell → book $10,179.77; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BRR` | 512 | $2.66 | $6.70 | $+63.49 | $3,812.77 | ▲ +63.49 after sell → book $10,173.07; vs 09:30 mark -6.70 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 78 | $16.74 | $2.25 | $+22.05 | $5,116.24 | ▲ +22.05 after sell → book $10,170.82; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LENZ` | 223 | $5.95 | $2.92 | $+38.80 | $6,440.17 | ▲ +38.80 after sell → book $10,167.90; vs 09:30 mark -2.92 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `IRD` | 283 | $4.53 | $3.71 | $-7.36 | $7,718.45 | ▼ -7.36 after sell → book $10,164.19; vs 09:30 mark -3.71 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `SLBT` | 408 | $2.88 | $5.34 | $-120.76 | $8,888.15 | ▼ -120.76 after sell → book $10,158.85; vs 09:30 mark -5.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 655 | $1.94 | $8.57 | $-17.02 | $10,150.28 | ▼ -17.02 after sell → book $10,150.28; vs 09:30 mark -8.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,150.28 | ▲ close $10,150.28 vs 09:30 $10,191.87 (session +0.00) | 16:00 close · cash $10,150.28 · no lots left · equity $10,150.28. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,150.28 | ▲ 09:30 equity $10,150.28 vs yday $10,150.28 (+0.00) | 09:30 open · cash $10,150.28 · no holdings · equity $10,150.28 vs prior close $10,150.28 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,150.28 | ▲ close $10,150.28 vs 09:30 $10,150.28 (session +0.00) | 16:00 close · cash $10,150.28 · no lots left · equity $10,150.28. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,150.28 | ▲ 09:30 equity $10,150.28 vs yday $10,150.28 (+0.00) | 09:30 open · cash $10,150.28 · no holdings · equity $10,150.28 vs prior close $10,150.28 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,150.28 | ▲ close $10,150.28 vs 09:30 $10,150.28 (session +0.00) | 16:00 close · cash $10,150.28 · no lots left · equity $10,150.28. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,150.28 | ▲ 09:30 equity $10,150.28 vs yday $10,150.28 (+0.00) | 09:30 open · cash $10,150.28 · no holdings · equity $10,150.28 vs prior close $10,150.28 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 469 | $2.70 | $6.05 | — | $8,877.93 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $1268.79 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 258 | $4.91 | $3.33 | — | $7,607.82 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+69.9; leftover $1268.79 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 205 | $6.16 | $2.64 | — | $6,342.38 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+36.4; leftover $1268.79 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CYPH` | 530 | $2.39 | $6.84 | — | $5,068.84 | — | rank by ret_5; rank ret_5; list yday_mover; 🔵; ret5=+31.0; leftover $1268.79 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ANGX` | 235 | $5.38 | $3.03 | — | $3,801.51 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+19.8; leftover $1268.79 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CRWV` | 13 | $91.08 | $2.03 | — | $2,615.44 | — | rank by ret_5; rank ret_5; list ohlc_hot; ⚪; ret5=+17.6; leftover $1268.79 | join🟡 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `RIOT` | 59 | $21.42 | $2.17 | — | $1,349.50 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+17.2; leftover $1268.79 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `INTC` | 12 | $102.47 | $2.03 | — | $117.83 | — | rank by ret_5; rank ret_5; list ohlc_hot; ⚪; ret5=+16.7; leftover $1268.79 | join🟢 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $117.83 | ▼ close $10,033.64 vs 09:30 $10,150.28 (session -88.53) | 16:00 close · cash $117.83 · equity $10,033.64 vs 09:30 $10,150.28 (-116.64; session marks -88.53) · 8 name(s) marked open→close (per-name table). INDP×469 09:30 $2.70 → close $2.77 +32.83; BNC×258 09:30 $4.91 → close $4.80 -28.38; IRD×205 09:30 $6.16 → close $6.04 -24.60; CYPH×530 09:30 $2.39 → close $2.27 -66.25; ANGX×235 09:30 $5.38 → close $5.45 +16.45; CRWV×13 09:30 $91.08 → close $88.99 -27.17; RIOT×59 09:30 $21.42 → close $21.47 +2.95; INTC×12 09:30 $102.47 → close $102.94 +5.64 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1280.32 < 1 share @ 1646.93 |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CRDL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ALM` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NMAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ANGX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `KGC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SBSW` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `DFDV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SPT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FWDI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PROK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SKYX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `METC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `LENZ` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `XRX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ZETA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ASST` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SUJA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TWI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SECZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AGCO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `USDE` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `RIOT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `BTDR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AUR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VSAT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ANGX` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SID` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CRWV` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `RIOT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIMO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INTC` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `PAYP` | hard_red | hard-red S=-13.28 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `INDP` | 469 | 2026-09-11 @ $2.70 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $1268.79 |
| `BNC` | 258 | 2026-09-11 @ $4.91 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+69.9; leftover $1268.79 |
| `IRD` | 205 | 2026-09-11 @ $6.16 | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+36.4; leftover $1268.79 |
| `CYPH` | 530 | 2026-09-11 @ $2.39 | rank by ret_5; rank ret_5; list yday_mover; 🔵; ret5=+31.0; leftover $1268.79 |
| `ANGX` | 235 | 2026-09-11 @ $5.38 | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+19.8; leftover $1268.79 |
| `CRWV` | 13 | 2026-09-11 @ $91.08 | rank by ret_5; rank ret_5; list ohlc_hot; ⚪; ret5=+17.6; leftover $1268.79 |
| `RIOT` | 59 | 2026-09-11 @ $21.42 | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+17.2; leftover $1268.79 |
| `INTC` | 12 | 2026-09-11 @ $102.47 | rank by ret_5; rank ret_5; list ohlc_hot; ⚪; ret5=+16.7; leftover $1268.79 |
