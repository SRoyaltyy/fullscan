# Factor mine action — `union_hot_score_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `hot_score` · size `leftover` · sell `list` · S-boost `none` · rank by hot_score

Cash book **+0.39%** ($10,039) · signal-only (no cash/fees) was +16.15%. Starts YES **7/18**. Fills 164 · skips 51 · realized $+38.81.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how hot the prior tape looked.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is thrown out if any must-not is true.
- Sort the keepers by how hot the prior tape looked and keep the top 8.
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
- **Gate** `none (list as ranked)` · **rank** `hot_score` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,038.78.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `IREN` | 27 | — | $45.98 | +0.00 | $44.76 | -32.94 | -32.94 | +0.00 | -32.94 |
| 2026-08-13 | `TNDM` | 53 | — | $23.33 | +0.00 | $23.13 | -10.60 | -10.60 | +0.00 | -10.60 |
| 2026-08-13 | `TPG` | 24 | — | $50.62 | +0.00 | $54.62 | +95.92 | +95.92 | +0.00 | +95.92 |
| 2026-08-13 | `INO` | 1543 | — | $0.81 | +0.00 | $0.90 | +138.87 | +138.87 | +0.00 | +138.87 |
| 2026-08-13 | `HIMS` | 42 | — | $29.74 | +0.00 | $28.77 | -40.74 | -40.74 | +0.00 | -40.74 |
| 2026-08-13 | `SLS` | 106 | — | $11.70 | +0.00 | $12.36 | +69.96 | +69.96 | +0.00 | +69.96 |
| 2026-08-13 | `VOR` | 56 | — | $22.01 | +0.00 | $23.29 | +71.68 | +71.68 | +0.00 | +71.68 |
| 2026-08-13 | `BTSG` | 20 | — | $59.80 | +0.00 | $60.23 | +8.60 | +8.60 | +0.00 | +8.60 |
| 2026-08-14 | `IREN` | 27 | $44.76 | $44.09 | -18.09 | — | +0.00 | -18.09 | -51.03 | — |
| 2026-08-14 | `TNDM` | 53 | $23.13 | $22.92 | -11.13 | — | +0.00 | -11.13 | -21.73 | — |
| 2026-08-14 | `TPG` | 24 | $54.62 | $55.29 | +16.08 | — | +0.00 | +16.08 | +112.00 | — |
| 2026-08-14 | `INO` | 1543 | $0.90 | $0.93 | +46.29 | — | +0.00 | +46.29 | +185.16 | — |
| 2026-08-14 | `HIMS` | 42 | $28.77 | $29.15 | +15.96 | — | +0.00 | +15.96 | -24.78 | — |
| 2026-08-14 | `SLS` | 106 | $12.36 | $12.40 | +4.24 | — | +0.00 | +4.24 | +74.20 | — |
| 2026-08-14 | `VOR` | 56 | $23.29 | $23.33 | +2.24 | — | +0.00 | +2.24 | +73.92 | — |
| 2026-08-14 | `BTSG` | 20 | $60.23 | $59.65 | -11.60 | — | +0.00 | -11.60 | -3.00 | — |
| 2026-08-14 | `QMCO` | 52 | — | $24.68 | +0.00 | $26.11 | +74.36 | +74.36 | +0.00 | +74.36 |
| 2026-08-14 | `ARX` | 65 | — | $19.57 | +0.00 | $19.58 | +0.65 | +0.65 | +0.00 | +0.65 |
| 2026-08-14 | `ZENA` | 583 | — | $2.20 | +0.00 | $2.14 | -34.98 | -34.98 | +0.00 | -34.98 |
| 2026-08-14 | `AIRO` | 115 | — | $11.12 | +0.00 | $9.57 | -178.25 | -178.25 | +0.00 | -178.25 |
| 2026-08-14 | `LIFE` | 36 | — | $35.04 | +0.00 | $34.02 | -36.72 | -36.72 | +0.00 | -36.72 |
| 2026-08-14 | `BZAI` | 1677 | — | $0.77 | +0.00 | $0.59 | -290.12 | -290.12 | +0.00 | -290.12 |
| 2026-08-14 | `VOYG` | 28 | — | $44.49 | +0.00 | $42.98 | -42.28 | -42.28 | +0.00 | -42.28 |
| 2026-08-14 | `LUNR` | 67 | — | $19.17 | +0.00 | $19.01 | -10.72 | -10.72 | +0.00 | -10.72 |
| 2026-08-17 | `QMCO` | 52 | $26.11 | $24.83 | -66.56 | — | +0.00 | -66.56 | +7.80 | — |
| 2026-08-17 | `ARX` | 65 | $19.58 | $19.57 | -0.65 | — | +0.00 | -0.65 | +0.00 | — |
| 2026-08-17 | `ZENA` | 583 | $2.14 | $2.08 | -32.07 | — | +0.00 | -32.07 | -67.05 | — |
| 2026-08-17 | `AIRO` | 115 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -178.25 | — |
| 2026-08-17 | `LIFE` | 36 | $34.02 | $34.03 | +0.36 | — | +0.00 | +0.36 | -36.36 | — |
| 2026-08-17 | `BZAI` | 1677 | $0.59 | $0.55 | -68.76 | — | +0.00 | -68.76 | -358.88 | — |
| 2026-08-17 | `VOYG` | 28 | $42.98 | $42.12 | -24.08 | — | +0.00 | -24.08 | -66.36 | — |
| 2026-08-17 | `LUNR` | 67 | $19.01 | $20.25 | +83.08 | — | +0.00 | +83.08 | +72.36 | — |
| 2026-08-17 | `XHG` | 285 | — | $4.19 | +0.00 | $3.91 | -79.80 | -79.80 | +0.00 | -79.80 |
| 2026-08-17 | `CAPR` | 174 | — | $6.87 | +0.00 | $7.45 | +100.92 | +100.92 | +0.00 | +100.92 |
| 2026-08-17 | `STDN` | 87 | — | $13.64 | +0.00 | $13.31 | -28.71 | -28.71 | +0.00 | -28.71 |
| 2026-08-17 | `HTFL` | 29 | — | $41.23 | +0.00 | $41.94 | +20.59 | +20.59 | +0.00 | +20.59 |
| 2026-08-17 | `UMAC` | 36 | — | $32.55 | +0.00 | $30.15 | -86.40 | -86.40 | +0.00 | -86.40 |
| 2026-08-17 | `SMJF` | 118 | — | $10.10 | +0.00 | $10.45 | +41.30 | +41.30 | +0.00 | +41.30 |
| 2026-08-17 | `ALOY` | 81 | — | $14.66 | +0.00 | $13.86 | -65.20 | -65.20 | +0.00 | -65.20 |
| 2026-08-17 | `NPWR` | 623 | — | $1.92 | +0.00 | $1.73 | -118.37 | -118.37 | +0.00 | -118.37 |
| 2026-08-18 | `XHG` | 285 | $3.91 | $3.94 | +8.55 | — | +0.00 | +8.55 | -71.25 | — |
| 2026-08-18 | `CAPR` | 174 | $7.45 | $7.50 | +8.70 | $7.08 | -73.08 | -64.38 | +109.62 | +36.54 |
| 2026-08-18 | `STDN` | 87 | $13.31 | $13.31 | +0.00 | — | +0.00 | +0.00 | -28.71 | — |
| 2026-08-18 | `HTFL` | 29 | $41.94 | $41.50 | -12.76 | — | +0.00 | -12.76 | +7.83 | — |
| 2026-08-18 | `UMAC` | 36 | $30.15 | $28.59 | -56.16 | — | +0.00 | -56.16 | -142.56 | — |
| 2026-08-18 | `SMJF` | 118 | $10.45 | $10.45 | +0.00 | — | +0.00 | +0.00 | +41.30 | — |
| 2026-08-18 | `ALOY` | 81 | $13.86 | $13.19 | -53.87 | — | +0.00 | -53.87 | -119.07 | — |
| 2026-08-18 | `NPWR` | 623 | $1.73 | $1.70 | -18.69 | — | +0.00 | -18.69 | -137.06 | — |
| 2026-08-19 | `CAPR` | 174 | $7.08 | $7.19 | +19.14 | — | +0.00 | +19.14 | +55.68 | — |
| 2026-08-20 | `MRNA` | 7 | — | $150.14 | +0.00 | $133.32 | -117.74 | -117.74 | +0.00 | -117.74 |
| 2026-08-20 | `CYPH` | 992 | — | $1.15 | +0.00 | $1.19 | +39.68 | +39.68 | +0.00 | +39.68 |
| 2026-08-20 | `ABCL` | 96 | — | $11.81 | +0.00 | $11.57 | -23.52 | -23.52 | +0.00 | -23.52 |
| 2026-08-20 | `AZI` | 833 | — | $1.37 | +0.00 | $1.44 | +58.31 | +58.31 | +0.00 | +58.31 |
| 2026-08-20 | `SENS` | 128 | — | $8.91 | +0.00 | $8.82 | -11.52 | -11.52 | +0.00 | -11.52 |
| 2026-08-20 | `ALEC` | 475 | — | $2.40 | +0.00 | $2.26 | -66.50 | -66.50 | +0.00 | -66.50 |
| 2026-08-20 | `BTGO` | 172 | — | $6.61 | +0.00 | $6.60 | -0.86 | -0.86 | +0.00 | -0.86 |
| 2026-08-20 | `AUTL` | 462 | — | $2.47 | +0.00 | $2.46 | -4.62 | -4.62 | +0.00 | -4.62 |
| 2026-08-21 | `MRNA` | 7 | $133.32 | $133.11 | -1.47 | $145.13 | +84.14 | +82.67 | -119.21 | -35.07 |
| 2026-08-21 | `CYPH` | 992 | $1.19 | $1.32 | +128.96 | $1.42 | +99.20 | +228.16 | +168.64 | +267.84 |
| 2026-08-21 | `ABCL` | 96 | $11.57 | $11.57 | +0.00 | — | +0.00 | +0.00 | -23.52 | — |
| 2026-08-21 | `AZI` | 833 | $1.44 | $1.46 | +16.66 | — | +0.00 | +16.66 | +74.97 | — |
| 2026-08-21 | `SENS` | 128 | $8.82 | $9.24 | +53.76 | — | +0.00 | +53.76 | +42.24 | — |
| 2026-08-21 | `ALEC` | 475 | $2.26 | $2.28 | +9.50 | — | +0.00 | +9.50 | -57.00 | — |
| 2026-08-21 | `BTGO` | 172 | $6.60 | $6.95 | +60.20 | — | +0.00 | +60.20 | +59.34 | — |
| 2026-08-21 | `AUTL` | 462 | $2.46 | $2.47 | +4.62 | — | +0.00 | +4.62 | +0.00 | — |
| 2026-08-21 | `XHG` | 258 | — | $4.49 | +0.00 | $4.41 | -20.64 | -20.64 | +0.00 | -20.64 |
| 2026-08-21 | `CAPR` | 170 | — | $6.81 | +0.00 | $6.29 | -88.40 | -88.40 | +0.00 | -88.40 |
| 2026-08-21 | `ARCT` | 104 | — | $11.13 | +0.00 | $13.45 | +241.28 | +241.28 | +0.00 | +241.28 |
| 2026-08-21 | `IOVA` | 127 | — | $9.08 | +0.00 | $8.29 | -100.33 | -100.33 | +0.00 | -100.33 |
| 2026-08-21 | `MRVI` | 140 | — | $8.28 | +0.00 | $8.64 | +50.40 | +50.40 | +0.00 | +50.40 |
| 2026-08-21 | `CAN` | 3877 | — | $0.29 | +0.00 | $0.35 | +236.50 | +236.50 | +0.00 | +236.50 |
| 2026-08-24 | `MRNA` | 7 | $145.13 | $142.70 | -17.01 | — | +0.00 | -17.01 | -52.08 | — |
| 2026-08-24 | `CYPH` | 992 | $1.42 | $1.83 | +406.72 | — | +0.00 | +406.72 | +674.56 | — |
| 2026-08-24 | `XHG` | 258 | $4.41 | $4.32 | -23.22 | — | +0.00 | -23.22 | -43.86 | — |
| 2026-08-24 | `CAPR` | 170 | $6.29 | $8.03 | +295.80 | — | +0.00 | +295.80 | +207.40 | — |
| 2026-08-24 | `ARCT` | 104 | $13.45 | $13.33 | -12.48 | — | +0.00 | -12.48 | +228.80 | — |
| 2026-08-24 | `IOVA` | 127 | $8.29 | $8.08 | -26.67 | — | +0.00 | -26.67 | -127.00 | — |
| 2026-08-24 | `MRVI` | 140 | $8.64 | $8.59 | -7.00 | — | +0.00 | -7.00 | +43.40 | — |
| 2026-08-24 | `CAN` | 3877 | $0.35 | $0.38 | +108.56 | — | +0.00 | +108.56 | +345.05 | — |
| 2026-08-25 | `REAX` | 53 | — | $24.11 | +0.00 | $28.43 | +228.96 | +228.96 | +0.00 | +228.96 |
| 2026-08-25 | `CYPH` | 828 | — | $1.56 | +0.00 | $1.64 | +66.24 | +66.24 | +0.00 | +66.24 |
| 2026-08-25 | `XHG` | 317 | — | $4.07 | +0.00 | $4.02 | -15.85 | -15.85 | +0.00 | -15.85 |
| 2026-08-25 | `ASST` | 67 | — | $19.04 | +0.00 | $21.39 | +157.45 | +157.45 | +0.00 | +157.45 |
| 2026-08-25 | `ALVO` | 246 | — | $5.24 | +0.00 | $5.05 | -46.74 | -46.74 | +0.00 | -46.74 |
| 2026-08-25 | `FWDI` | 226 | — | $5.71 | +0.00 | $6.05 | +76.84 | +76.84 | +0.00 | +76.84 |
| 2026-08-25 | `SUJA` | 147 | — | $8.79 | +0.00 | $9.33 | +79.38 | +79.38 | +0.00 | +79.38 |
| 2026-08-25 | `GORO` | 364 | — | $3.55 | +0.00 | $3.87 | +116.48 | +116.48 | +0.00 | +116.48 |
| 2026-08-26 | `REAX` | 53 | $28.43 | $26.61 | -96.46 | — | +0.00 | -96.46 | +132.50 | — |
| 2026-08-26 | `CYPH` | 828 | $1.64 | $1.60 | -33.12 | — | +0.00 | -33.12 | +33.12 | — |
| 2026-08-26 | `XHG` | 317 | $4.02 | $3.81 | -66.57 | $4.06 | +79.25 | +12.68 | -82.42 | -3.17 |
| 2026-08-26 | `ASST` | 67 | $21.39 | $20.72 | -44.89 | — | +0.00 | -44.89 | +112.56 | — |
| 2026-08-26 | `ALVO` | 246 | $5.05 | $4.98 | -17.22 | — | +0.00 | -17.22 | -63.96 | — |
| 2026-08-26 | `FWDI` | 226 | $6.05 | $5.97 | -18.08 | — | +0.00 | -18.08 | +58.76 | — |
| 2026-08-26 | `SUJA` | 147 | $9.33 | $9.39 | +8.82 | $9.44 | +7.35 | +16.17 | +88.20 | +95.55 |
| 2026-08-26 | `GORO` | 364 | $3.87 | $3.77 | -36.40 | — | +0.00 | -36.40 | +80.08 | — |
| 2026-08-26 | `BYND` | 95 | — | $14.11 | +0.00 | $14.25 | +13.30 | +13.30 | +0.00 | +13.30 |
| 2026-08-26 | `USDE` | 230 | — | $5.81 | +0.00 | $5.98 | +39.10 | +39.10 | +0.00 | +39.10 |
| 2026-08-26 | `CAPR` | 161 | — | $8.29 | +0.00 | $9.36 | +172.27 | +172.27 | +0.00 | +172.27 |
| 2026-08-26 | `FIGR` | 33 | — | $40.50 | +0.00 | $37.08 | -112.86 | -112.86 | +0.00 | -112.86 |
| 2026-08-26 | `KURA` | 98 | — | $13.63 | +0.00 | $13.06 | -55.86 | -55.86 | +0.00 | -55.86 |
| 2026-08-26 | `MNRO` | 95 | — | $14.00 | +0.00 | $12.61 | -132.05 | -132.05 | +0.00 | -132.05 |
| 2026-08-27 | `XHG` | 317 | $4.06 | $4.06 | +0.00 | — | +0.00 | +0.00 | -3.17 | — |
| 2026-08-27 | `SUJA` | 147 | $9.44 | $9.41 | -4.41 | — | +0.00 | -4.41 | +91.14 | — |
| 2026-08-27 | `BYND` | 95 | $14.25 | $14.20 | -4.75 | — | +0.00 | -4.75 | +8.55 | — |
| 2026-08-27 | `USDE` | 230 | $5.98 | $6.50 | +119.60 | — | +0.00 | +119.60 | +158.70 | — |
| 2026-08-27 | `CAPR` | 161 | $9.36 | $9.19 | -27.37 | — | +0.00 | -27.37 | +144.90 | — |
| 2026-08-27 | `FIGR` | 33 | $37.08 | $37.42 | +11.22 | — | +0.00 | +11.22 | -101.64 | — |
| 2026-08-27 | `KURA` | 98 | $13.06 | $12.98 | -7.84 | — | +0.00 | -7.84 | -63.70 | — |
| 2026-08-27 | `MNRO` | 95 | $12.61 | $12.56 | -4.75 | — | +0.00 | -4.75 | -136.80 | — |
| 2026-08-27 | `SLI` | 514 | — | $2.60 | +0.00 | $2.64 | +20.56 | +20.56 | +0.00 | +20.56 |
| 2026-08-27 | `RRC` | 32 | — | $41.44 | +0.00 | $41.64 | +6.40 | +6.40 | +0.00 | +6.40 |
| 2026-08-27 | `PGY` | 58 | — | $22.93 | +0.00 | $23.26 | +19.14 | +19.14 | +0.00 | +19.14 |
| 2026-08-27 | `CRK` | 92 | — | $14.42 | +0.00 | $14.62 | +18.40 | +18.40 | +0.00 | +18.40 |
| 2026-08-27 | `GEN` | 44 | — | $29.83 | +0.00 | $30.50 | +29.48 | +29.48 | +0.00 | +29.48 |
| 2026-08-27 | `MRVL` | 5 | — | $253.44 | +0.00 | $241.45 | -59.95 | -59.95 | +0.00 | -59.95 |
| 2026-08-27 | `ANET` | 6 | — | $205.90 | +0.00 | $201.09 | -28.86 | -28.86 | +0.00 | -28.86 |
| 2026-08-27 | `MOS` | 55 | — | $24.00 | +0.00 | $23.76 | -13.20 | -13.20 | +0.00 | -13.20 |
| 2026-08-28 | `SLI` | 514 | $2.64 | $2.68 | +20.56 | — | +0.00 | +20.56 | +41.12 | — |
| 2026-08-28 | `RRC` | 32 | $41.64 | $41.74 | +3.20 | — | +0.00 | +3.20 | +9.60 | — |
| 2026-08-28 | `PGY` | 58 | $23.26 | $23.21 | -2.90 | — | +0.00 | -2.90 | +16.24 | — |
| 2026-08-28 | `CRK` | 92 | $14.62 | $14.63 | +0.92 | — | +0.00 | +0.92 | +19.32 | — |
| 2026-08-28 | `GEN` | 44 | $30.50 | $30.50 | +0.00 | — | +0.00 | +0.00 | +29.48 | — |
| 2026-08-28 | `MRVL` | 5 | $241.45 | $225.26 | -80.95 | — | +0.00 | -80.95 | -140.90 | — |
| 2026-08-28 | `ANET` | 6 | $201.09 | $200.00 | -6.54 | — | +0.00 | -6.54 | -35.40 | — |
| 2026-08-28 | `MOS` | 55 | $23.76 | $23.95 | +10.45 | — | +0.00 | +10.45 | -2.75 | — |
| 2026-08-28 | `BYND` | 94 | — | $14.00 | +0.00 | $13.86 | -13.16 | -13.16 | +0.00 | -13.16 |
| 2026-08-28 | `CAPR` | 136 | — | $9.73 | +0.00 | $9.59 | -19.04 | -19.04 | +0.00 | -19.04 |
| 2026-08-28 | `MRNA` | 9 | — | $137.19 | +0.00 | $137.99 | +7.20 | +7.20 | +0.00 | +7.20 |
| 2026-08-28 | `ANF` | 9 | — | $146.07 | +0.00 | $148.42 | +21.15 | +21.15 | +0.00 | +21.15 |
| 2026-08-28 | `SNPS` | 2 | — | $461.85 | +0.00 | $442.61 | -38.48 | -38.48 | +0.00 | -38.48 |
| 2026-08-28 | `SRPT` | 61 | — | $21.49 | +0.00 | $20.86 | -38.43 | -38.43 | +0.00 | -38.43 |
| 2026-08-28 | `NEO` | 72 | — | $18.36 | +0.00 | $18.05 | -22.32 | -22.32 | +0.00 | -22.32 |
| 2026-08-28 | `VYX` | 144 | — | $9.13 | +0.00 | $8.78 | -50.40 | -50.40 | +0.00 | -50.40 |
| 2026-08-31 | `BYND` | 94 | $13.86 | $13.81 | -4.70 | $13.30 | -47.94 | -52.64 | -17.86 | -65.80 |
| 2026-08-31 | `CAPR` | 136 | $9.59 | $9.50 | -12.24 | — | +0.00 | -12.24 | -31.28 | — |
| 2026-08-31 | `MRNA` | 9 | $137.99 | $134.10 | -35.01 | $140.34 | +56.16 | +21.15 | -27.81 | +28.35 |
| 2026-08-31 | `ANF` | 9 | $148.42 | $148.03 | -3.51 | — | +0.00 | -3.51 | +17.64 | — |
| 2026-08-31 | `SNPS` | 2 | $442.61 | $437.95 | -9.32 | — | +0.00 | -9.32 | -47.80 | — |
| 2026-08-31 | `SRPT` | 61 | $20.86 | $20.56 | -18.30 | — | +0.00 | -18.30 | -56.73 | — |
| 2026-08-31 | `NEO` | 72 | $18.05 | $17.77 | -20.16 | — | +0.00 | -20.16 | -42.48 | — |
| 2026-08-31 | `VYX` | 144 | $8.78 | $8.66 | -17.28 | — | +0.00 | -17.28 | -67.68 | — |
| 2026-09-01 | `BYND` | 94 | $13.30 | $13.04 | -24.44 | — | +0.00 | -24.44 | -90.24 | — |
| 2026-09-01 | `MRNA` | 9 | $140.34 | $140.25 | -0.81 | $154.27 | +126.18 | +125.37 | +27.54 | +153.72 |
| 2026-09-02 | `MRNA` | 9 | $154.27 | $151.40 | -25.83 | $150.81 | -5.31 | -31.14 | +127.89 | +122.58 |
| 2026-09-03 | `MRNA` | 9 | $150.81 | $145.94 | -43.79 | — | +0.00 | -43.79 | +78.79 | — |
| 2026-09-03 | `GPRO` | 724 | — | $1.78 | +0.00 | $1.39 | -282.36 | -282.36 | +0.00 | -282.36 |
| 2026-09-03 | `REAX` | 70 | — | $18.40 | +0.00 | $18.40 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `CNH` | 94 | — | $13.71 | +0.00 | $13.84 | +12.22 | +12.22 | +0.00 | +12.22 |
| 2026-09-03 | `MMED` | 53 | — | $23.88 | +0.00 | $23.84 | -2.12 | -2.12 | +0.00 | -2.12 |
| 2026-09-03 | `SID` | 948 | — | $1.36 | +0.00 | $1.26 | -94.80 | -94.80 | +0.00 | -94.80 |
| 2026-09-03 | `BMEA` | 668 | — | $1.93 | +0.00 | $1.91 | -13.36 | -13.36 | +0.00 | -13.36 |
| 2026-09-03 | `AGCO` | 10 | — | $127.91 | +0.00 | $125.82 | -20.90 | -20.90 | +0.00 | -20.90 |
| 2026-09-03 | `ASST` | 50 | — | $25.62 | +0.00 | $26.82 | +59.75 | +59.75 | +0.00 | +59.75 |
| 2026-09-04 | `GPRO` | 724 | $1.39 | $1.48 | +65.16 | $1.70 | +159.28 | +224.44 | -217.20 | -57.92 |
| 2026-09-04 | `REAX` | 70 | $18.40 | $18.15 | -17.50 | — | +0.00 | -17.50 | -17.50 | — |
| 2026-09-04 | `CNH` | 94 | $13.84 | $13.89 | +4.70 | — | +0.00 | +4.70 | +16.92 | — |
| 2026-09-04 | `MMED` | 53 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -2.12 | — |
| 2026-09-04 | `SID` | 948 | $1.26 | $1.23 | -28.44 | — | +0.00 | -28.44 | -123.24 | — |
| 2026-09-04 | `BMEA` | 668 | $1.91 | $1.90 | -6.68 | — | +0.00 | -6.68 | -20.04 | — |
| 2026-09-04 | `AGCO` | 10 | $125.82 | $125.22 | -6.00 | — | +0.00 | -6.00 | -26.90 | — |
| 2026-09-04 | `ASST` | 50 | $26.82 | $25.18 | -82.00 | $27.14 | +98.00 | +16.00 | -22.25 | +75.75 |
| 2026-09-04 | `USDE` | 158 | — | $7.87 | +0.00 | $7.93 | +9.48 | +9.48 | +0.00 | +9.48 |
| 2026-09-04 | `DFDV` | 215 | — | $5.79 | +0.00 | $5.87 | +17.20 | +17.20 | +0.00 | +17.20 |
| 2026-09-04 | `BRR` | 498 | — | $2.51 | +0.00 | $2.66 | +74.70 | +74.70 | +0.00 | +74.70 |
| 2026-09-04 | `HOOD` | 10 | — | $120.47 | +0.00 | $122.11 | +16.35 | +16.35 | +0.00 | +16.35 |
| 2026-09-04 | `FRNM` | 76 | — | $16.40 | +0.00 | $16.31 | -6.84 | -6.84 | +0.00 | -6.84 |
| 2026-09-04 | `IRD` | 275 | — | $4.53 | +0.00 | $4.67 | +38.50 | +38.50 | +0.00 | +38.50 |
| 2026-09-08 | `GPRO` | 724 | $1.70 | $1.56 | -97.74 | — | +0.00 | -97.74 | -155.66 | — |
| 2026-09-08 | `ASST` | 50 | $27.14 | $26.44 | -35.00 | — | +0.00 | -35.00 | +40.75 | — |
| 2026-09-08 | `USDE` | 158 | $7.93 | $7.76 | -26.86 | — | +0.00 | -26.86 | -17.38 | — |
| 2026-09-08 | `DFDV` | 215 | $5.87 | $5.81 | -12.90 | — | +0.00 | -12.90 | +4.30 | — |
| 2026-09-08 | `BRR` | 498 | $2.66 | $2.66 | +0.00 | — | +0.00 | +0.00 | +74.70 | — |
| 2026-09-08 | `HOOD` | 10 | $122.11 | $125.07 | +29.60 | — | +0.00 | +29.60 | +45.95 | — |
| 2026-09-08 | `FRNM` | 76 | $16.31 | $16.74 | +32.68 | — | +0.00 | +32.68 | +25.84 | — |
| 2026-09-08 | `IRD` | 275 | $4.67 | $4.53 | -38.50 | — | +0.00 | -38.50 | +0.00 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +300.75 | IREN, TNDM, TPG, INO, HIMS, SLS, VOR, BTSG | — | $107.38 | $10,268.71 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20 |
| 2026-08-14 | +5.50 | $107.38 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20 | $10,312.70 | +43.99 | -518.06 | QMCO, ARX, ZENA, AIRO, LIFE, BZAI, VOYG, LUNR | IREN, TNDM, TPG, INO, HIMS, SLS, VOR, BTSG | $47.02 | $9,721.90 | QMCO×52, ARX×65, ZENA×583, AIRO×115, LIFE×36, BZAI×1677, VOYG×28, LUNR×67 |
| 2026-08-17 | +2.25 | $47.02 | QMCO×52, ARX×65, ZENA×583, AIRO×115, LIFE×36, BZAI×1677, VOYG×28, LUNR×67 | $9,613.23 | -108.67 | -215.67 | XHG, CAPR, STDN, HTFL, UMAC, SMJF, ALOY, NPWR | QMCO, ARX, ZENA, AIRO, LIFE, BZAI, VOYG, LUNR | $33.54 | $9,336.96 | XHG×285, CAPR×174, STDN×87, HTFL×29, UMAC×36, SMJF×118, ALOY×81, NPWR×623 |
| 2026-08-18 | -6.20 | $33.54 | XHG×285, CAPR×174, STDN×87, HTFL×29, UMAC×36, SMJF×118, ALOY×81, NPWR×623 | $9,212.74 | -124.22 | -73.08 | — | XHG, STDN, HTFL, UMAC, SMJF, ALOY, NPWR | $7,884.73 | $9,116.65 | CAPR×174 |
| 2026-08-19 | -7.20 | $7,884.73 | CAPR×174 | $9,135.79 | +19.14 | +0.00 | — | CAPR | $9,133.24 | $9,133.24 | — |
| 2026-08-20 | +1.12 | $9,133.24 | — | $9,133.24 | +0.00 | -126.77 | MRNA, CYPH, ABCL, AZI, SENS, ALEC, BTGO, AUTL | — | $63.54 | $8,961.68 | MRNA×7, CYPH×992, ABCL×96, AZI×833, SENS×128, ALEC×475, BTGO×172, AUTL×462 |
| 2026-08-21 | +3.25 | $63.54 | MRNA×7, CYPH×992, ABCL×96, AZI×833, SENS×128, ALEC×475, BTGO×172, AUTL×462 | $9,233.91 | +272.23 | +502.15 | XHG, CAPR, ARCT, IOVA, MRVI, CAN | ABCL, AZI, SENS, ALEC, BTGO, AUTL | $0.51 | $9,669.70 | MRNA×7, CYPH×992, XHG×258, CAPR×170, ARCT×104, IOVA×127, MRVI×140, CAN×3877 |
| 2026-08-24 | -5.17 | $0.51 | MRNA×7, CYPH×992, XHG×258, CAPR×170, ARCT×104, IOVA×127, MRVI×140, CAN×3877 | $10,394.40 | +724.70 | +0.00 | — | MRNA, CYPH, XHG, CAPR, ARCT, IOVA, MRVI, CAN | $10,339.16 | $10,339.16 | — |
| 2026-08-25 | +1.80 | $10,339.16 | — | $10,339.16 | -0.00 | +662.76 | REAX, CYPH, XHG, ASST, ALVO, FWDI, SUJA, GORO | — | $7.62 | $10,969.59 | REAX×53, CYPH×828, XHG×317, ASST×67, ALVO×246, FWDI×226, SUJA×147, GORO×364 |
| 2026-08-26 | +2.02 | $7.62 | REAX×53, CYPH×828, XHG×317, ASST×67, ALVO×246, FWDI×226, SUJA×147, GORO×364 | $10,665.67 | -303.92 | +10.50 | BYND, USDE, CAPR, FIGR, KURA, MNRO | REAX, CYPH, ASST, ALVO, FWDI, GORO | $23.36 | $10,635.64 | XHG×317, SUJA×147, BYND×95, USDE×230, CAPR×161, FIGR×33, KURA×98, MNRO×95 |
| 2026-08-27 | — | $23.36 | XHG×317, SUJA×147, BYND×95, USDE×230, CAPR×161, FIGR×33, KURA×98, MNRO×95 | $10,717.34 | +81.70 | -8.03 | SLI, RRC, PGY, CRK, GEN, MRVL, ANET, MOS | XHG, SUJA, BYND, USDE, CAPR, FIGR, KURA, MNRO | $220.56 | $10,666.71 | SLI×514, RRC×32, PGY×58, CRK×92, GEN×44, MRVL×5, ANET×6, MOS×55 |
| 2026-08-28 | +0.75 | $220.56 | SLI×514, RRC×32, PGY×58, CRK×92, GEN×44, MRVL×5, ANET×6, MOS×55 | $10,611.45 | -55.26 | -153.48 | BYND, CAPR, MRNA, ANF, SNPS, SRPT, NEO, VYX | SLI, RRC, PGY, CRK, GEN, MRVL, ANET, MOS | $512.41 | $10,418.78 | BYND×94, CAPR×136, MRNA×9, ANF×9, SNPS×2, SRPT×61, NEO×72, VYX×144 |
| 2026-08-31 | -5.85 | $512.41 | BYND×94, CAPR×136, MRNA×9, ANF×9, SNPS×2, SRPT×61, NEO×72, VYX×144 | $10,298.26 | -120.52 | +8.22 | — | CAPR, ANF, SNPS, SRPT, NEO, VYX | $7,779.86 | $10,293.12 | BYND×94, MRNA×9 |
| 2026-09-01 | -6.30 | $7,779.86 | BYND×94, MRNA×9 | $10,267.87 | -25.25 | +126.18 | — | BYND | $9,003.32 | $10,391.75 | MRNA×9 |
| 2026-09-02 | -3.83 | $9,003.32 | MRNA×9 | $10,365.92 | -25.83 | -5.31 | — | — | $9,003.32 | $10,360.61 | MRNA×9 |
| 2026-09-03 | -0.90 | $9,003.32 | MRNA×9 | $10,316.83 | -43.78 | -341.57 | GPRO, REAX, CNH, MMED, SID, BMEA, AGCO, ASST | MRNA | $3.85 | $9,932.25 | GPRO×724, REAX×70, CNH×94, MMED×53, SID×948, BMEA×668, AGCO×10, ASST×50 |
| 2026-09-04 | +2.25 | $3.85 | GPRO×724, REAX×70, CNH×94, MMED×53, SID×948, BMEA×668, AGCO×10, ASST×50 | $9,861.49 | -70.76 | +406.67 | USDE, DFDV, BRR, HOOD, FRNM, IRD | REAX, CNH, MMED, SID, BMEA, AGCO | $46.47 | $10,218.85 | GPRO×724, ASST×50, USDE×158, DFDV×215, BRR×498, HOOD×10, FRNM×76, IRD×275 |
| 2026-09-08 | -11.47 | $46.47 | GPRO×724, ASST×50, USDE×158, DFDV×215, BRR×498, HOOD×10, FRNM×76, IRD×275 | $10,070.13 | -148.72 | +0.00 | — | GPRO, ASST, USDE, DFDV, BRR, HOOD, FRNM, IRD | $10,038.78 | $10,038.78 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $8,756.47 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $7,517.83 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,300.81 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $5,033.85 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $3,782.66 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $2,540.15 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 56 | $22.01 | $2.16 | — | $1,305.43 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $107.38 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $107.38 | ▲ close $10,268.71 vs 09:30 $10,000.00 (session +300.75) | 16:00 close · cash $107.38 · equity $10,268.71 vs 09:30 $10,000.00 (+268.71; session marks +300.75) · 8 name(s) marked open→close (per-name table). IREN×27 09:30 $45.98 → close $44.76 -32.94; TNDM×53 09:30 $23.33 → close $23.13 -10.60; TPG×24 09:30 $50.62 → close $54.62 +95.92; INO×1543 09:30 $0.81 → close $0.90 +138.87; HIMS×42 09:30 $29.74 → close $28.77 -40.74; SLS×106 09:30 $11.70 → close $12.36 +69.96; VOR×56 09:30 $22.01 → close $23.29 +71.68; BTSG×20 09:30 $59.80 → close $60.23 +8.60 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $107.38 | ▲ 09:30 equity $10,312.70 vs yday $10,268.71 (+43.99) | 09:30 open · cash $107.38 (unchanged overnight, no fees) · equity $10,312.70 vs prior close $10,268.71 (+43.99) · 8 name(s) re-marked at the open (per-name table). IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; VOR×56 yday $23.29 → 09:30 $23.33 +2.24; BTSG×20 yday $60.23 → 09:30 $59.65 -11.60 | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $1,295.72 | ▼ -55.19 after sell → book $10,310.61; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $2,508.31 | ▼ -26.05 after sell → book $10,308.44; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $3,833.19 | ▲ +107.86 after sell → book $10,306.36; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $5,248.93 | ▲ +148.79 after sell → book $10,287.11; vs 09:30 mark -19.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $6,471.10 | ▼ -29.03 after sell → book $10,284.98; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $7,783.16 | ▲ +69.56 after sell → book $10,282.64; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 56 | $23.33 | $2.18 | $+69.58 | $9,087.46 | ▲ +69.58 after sell → book $10,280.46; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $10,278.39 | ▼ -7.12 after sell → book $10,278.39; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 52 | $24.68 | $2.15 | — | $8,992.89 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+111.3; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 65 | $19.57 | $2.19 | — | $7,718.65 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 583 | $2.20 | $7.52 | — | $6,428.53 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 115 | $11.12 | $2.33 | — | $5,147.40 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LIFE` | 36 | $35.04 | $2.10 | — | $3,883.86 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.7; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BZAI` | 1677 | $0.77 | $17.88 | — | $2,581.40 | — | rank by hot_score; rank hot_score; list earn_react; 🔵; ⚪; ret5=+20.4; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VOYG` | 28 | $44.49 | $2.07 | — | $1,333.60 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+15.6; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 67 | $19.17 | $2.19 | — | $47.02 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $47.02 | ▼ close $9,721.90 vs 09:30 $10,312.70 (session -518.06) | 16:00 close · cash $47.02 · equity $9,721.90 vs 09:30 $10,312.70 (-590.80; session marks -518.06) · 8 name(s) marked open→close (per-name table). QMCO×52 09:30 $24.68 → close $26.11 +74.36; ARX×65 09:30 $19.57 → close $19.58 +0.65; ZENA×583 09:30 $2.20 → close $2.14 -34.98; AIRO×115 09:30 $11.12 → close $9.57 -178.25; LIFE×36 09:30 $35.04 → close $34.02 -36.72; BZAI×1677 09:30 $0.77 → close $0.59 -290.12; VOYG×28 09:30 $44.49 → close $42.98 -42.28; LUNR×67 09:30 $19.17 → close $19.01 -10.72 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.02 | ▼ 09:30 equity $9,613.23 vs yday $9,721.90 (-108.67) | 09:30 open · cash $47.02 (unchanged overnight, no fees) · equity $9,613.23 vs prior close $9,721.90 (-108.67) · 8 name(s) re-marked at the open (per-name table). QMCO×52 yday $26.11 → 09:30 $24.83 -66.56; ARX×65 yday $19.58 → 09:30 $19.57 -0.65; ZENA×583 yday $2.14 → 09:30 $2.08 -32.07; AIRO×115 yday $9.57 → 09:30 $9.57 +0.00; LIFE×36 yday $34.02 → 09:30 $34.03 +0.36; BZAI×1677 yday $0.59 → 09:30 $0.55 -68.76; VOYG×28 yday $42.98 → 09:30 $42.12 -24.08; LUNR×67 yday $19.01 → 09:30 $20.25 +83.08 | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 52 | $24.83 | $2.17 | $+3.49 | $1,336.02 | ▲ +3.49 after sell → book $9,611.07; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 65 | $19.57 | $2.21 | $-4.39 | $2,605.86 | ▼ -4.39 after sell → book $9,608.86; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 583 | $2.08 | $7.63 | $-82.19 | $3,813.79 | ▼ -82.19 after sell → book $9,601.23; vs 09:30 mark -7.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 115 | $9.57 | $2.36 | $-182.95 | $4,911.97 | ▼ -182.95 after sell → book $9,598.87; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LIFE` | 36 | $34.03 | $2.12 | $-40.58 | $6,134.94 | ▼ -40.58 after sell → book $9,596.75; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BZAI` | 1677 | $0.55 | $14.58 | $-391.33 | $7,046.06 | ▼ -391.33 after sell → book $9,582.17; vs 09:30 mark -14.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VOYG` | 28 | $42.12 | $2.09 | $-70.53 | $8,223.33 | ▼ -70.53 after sell → book $9,580.08; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `LUNR` | 67 | $20.25 | $2.21 | $+67.96 | $9,577.87 | ▲ +67.96 after sell → book $9,577.87; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 285 | $4.19 | $3.68 | — | $8,380.04 | — | rank by hot_score; rank hot_score; list yday_mover; ⚪; ret5=+291.8; leftover $1197.23 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 174 | $6.87 | $2.51 | — | $7,182.15 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; leftover $1197.23 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 87 | $13.64 | $2.25 | — | $5,993.22 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; leftover $1197.23 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 29 | $41.23 | $2.08 | — | $4,795.47 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; leftover $1197.23 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 36 | $32.55 | $2.10 | — | $3,621.57 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1197.23 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `SMJF` | 118 | $10.10 | $2.34 | — | $2,427.43 | — | rank by hot_score; rank hot_score; list mover_buy; ret5=+22.8; leftover $1197.23 | join🔴 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 81 | $14.66 | $2.23 | — | $1,237.74 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $1197.23 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 623 | $1.92 | $8.04 | — | $33.54 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1197.23 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $33.54 | ▼ close $9,336.96 vs 09:30 $9,613.23 (session -215.67) | 16:00 close · cash $33.54 · equity $9,336.96 vs 09:30 $9,613.23 (-276.27; session marks -215.67) · 8 name(s) marked open→close (per-name table). XHG×285 09:30 $4.19 → close $3.91 -79.80; CAPR×174 09:30 $6.87 → close $7.45 +100.92; STDN×87 09:30 $13.64 → close $13.31 -28.71; HTFL×29 09:30 $41.23 → close $41.94 +20.59; UMAC×36 09:30 $32.55 → close $30.15 -86.40; SMJF×118 09:30 $10.10 → close $10.45 +41.30; ALOY×81 09:30 $14.66 → close $13.86 -65.20; NPWR×623 09:30 $1.92 → close $1.73 -118.37 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $33.54 | ▼ 09:30 equity $9,212.74 vs yday $9,336.96 (-124.22) | 09:30 open · cash $33.54 (unchanged overnight, no fees) · equity $9,212.74 vs prior close $9,336.96 (-124.22) · 8 name(s) re-marked at the open (per-name table). XHG×285 yday $3.91 → 09:30 $3.94 +8.55; CAPR×174 yday $7.45 → 09:30 $7.50 +8.70; STDN×87 yday $13.31 → 09:30 $13.31 +0.00; HTFL×29 yday $41.94 → 09:30 $41.50 -12.76; UMAC×36 yday $30.15 → 09:30 $28.59 -56.16; SMJF×118 yday $10.45 → 09:30 $10.45 +0.00; ALOY×81 yday $13.86 → 09:30 $13.19 -53.87; NPWR×623 yday $1.73 → 09:30 $1.70 -18.69 | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 285 | $3.94 | $3.73 | $-78.66 | $1,152.71 | ▼ -78.66 after sell → book $9,209.01; vs 09:30 mark -3.73 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 87 | $13.31 | $2.28 | $-33.24 | $2,308.40 | ▼ -33.24 after sell → book $9,206.73; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 29 | $41.50 | $2.10 | $+3.66 | $3,509.80 | ▲ +3.66 after sell → book $9,204.63; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 36 | $28.59 | $2.12 | $-146.78 | $4,536.93 | ▼ -146.78 after sell → book $9,202.51; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `SMJF` | 118 | $10.45 | $2.37 | $+36.58 | $5,767.65 | ▲ +36.58 after sell → book $9,200.14; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 81 | $13.19 | $2.26 | $-123.56 | $6,833.79 | ▼ -123.56 after sell → book $9,197.89; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 623 | $1.70 | $8.15 | $-153.25 | $7,884.73 | ▼ -153.25 after sell → book $9,189.73; vs 09:30 mark -8.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,884.73 | ▼ close $9,116.65 vs 09:30 $9,212.74 (session -73.08) | 16:00 close · cash $7,884.73 · equity $9,116.65 vs 09:30 $9,212.74 (-96.09; session marks -73.08) · 1 name(s) marked open→close (per-name table). CAPR×174 09:30 $7.50 → close $7.08 -73.08 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,884.73 | ▲ 09:30 equity $9,135.79 vs yday $9,116.65 (+19.14) | 09:30 open · cash $7,884.73 (unchanged overnight, no fees) · equity $9,135.79 vs prior close $9,116.65 (+19.14) · 1 name(s) re-marked at the open (per-name table). CAPR×174 yday $7.08 → 09:30 $7.19 +19.14 | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 174 | $7.19 | $2.55 | $+50.62 | $9,133.24 | ▲ +50.62 after sell → book $9,133.24; vs 09:30 mark -2.55 | dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,133.24 | ▲ close $9,133.24 vs 09:30 $9,135.79 (session +0.00) | 16:00 close · cash $9,133.24 · no lots left · equity $9,133.24. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,133.24 | ▲ 09:30 equity $9,133.24 vs yday $9,133.24 (+0.00) | 09:30 open · cash $9,133.24 · no holdings · equity $9,133.24 vs prior close $9,133.24 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 7 | $150.14 | $2.01 | — | $8,080.25 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1141.66 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 992 | $1.15 | $12.80 | — | $6,926.66 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1141.66 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 96 | $11.81 | $2.28 | — | $5,790.14 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1141.66 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 833 | $1.37 | $10.75 | — | $4,638.18 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $1141.66 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SENS` | 128 | $8.91 | $2.37 | — | $3,495.33 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1141.66 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ALEC` | 475 | $2.40 | $6.13 | — | $2,349.20 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+13.0; leftover $1141.66 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 172 | $6.61 | $2.51 | — | $1,210.64 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1141.66 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 462 | $2.47 | $5.96 | — | $63.54 | — | rank by hot_score; rank hot_score; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1141.66 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.54 | ▼ close $8,961.68 vs 09:30 $9,133.24 (session -126.77) | 16:00 close · cash $63.54 · equity $8,961.68 vs 09:30 $9,133.24 (-171.56; session marks -126.77) · 8 name(s) marked open→close (per-name table). MRNA×7 09:30 $150.14 → close $133.32 -117.74; CYPH×992 09:30 $1.15 → close $1.19 +39.68; ABCL×96 09:30 $11.81 → close $11.57 -23.52; AZI×833 09:30 $1.37 → close $1.44 +58.31; SENS×128 09:30 $8.91 → close $8.82 -11.52; ALEC×475 09:30 $2.40 → close $2.26 -66.50; BTGO×172 09:30 $6.61 → close $6.60 -0.86; AUTL×462 09:30 $2.47 → close $2.46 -4.62 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.54 | ▲ 09:30 equity $9,233.91 vs yday $8,961.68 (+272.23) | 09:30 open · cash $63.54 (unchanged overnight, no fees) · equity $9,233.91 vs prior close $8,961.68 (+272.23) · 8 name(s) re-marked at the open (per-name table). MRNA×7 yday $133.32 → 09:30 $133.11 -1.47; CYPH×992 yday $1.19 → 09:30 $1.32 +128.96; ABCL×96 yday $11.57 → 09:30 $11.57 +0.00; AZI×833 yday $1.44 → 09:30 $1.46 +16.66; SENS×128 yday $8.82 → 09:30 $9.24 +53.76; ALEC×475 yday $2.26 → 09:30 $2.28 +9.50; BTGO×172 yday $6.60 → 09:30 $6.95 +60.20; AUTL×462 yday $2.46 → 09:30 $2.47 +4.62 | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 96 | $11.57 | $2.30 | $-28.10 | $1,171.95 | ▼ -28.10 after sell → book $9,231.60; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 833 | $1.46 | $10.89 | $+53.33 | $2,377.24 | ▲ +53.33 after sell → book $9,220.71; vs 09:30 mark -10.89 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SENS` | 128 | $9.24 | $2.41 | $+37.46 | $3,557.55 | ▲ +37.46 after sell → book $9,218.30; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ALEC` | 475 | $2.28 | $6.22 | $-69.34 | $4,634.34 | ▼ -69.34 after sell → book $9,212.09; vs 09:30 mark -6.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 172 | $6.95 | $2.54 | $+54.29 | $5,827.19 | ▲ +54.29 after sell → book $9,209.54; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AUTL` | 462 | $2.47 | $6.05 | $-12.01 | $6,962.28 | ▼ -12.01 after sell → book $9,203.49; vs 09:30 mark -6.05 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 258 | $4.49 | $3.33 | — | $5,800.54 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; leftover $1160.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 170 | $6.81 | $2.50 | — | $4,640.34 | — | rank by hot_score; rank hot_score; list yday_mover; 🔵; ret5=+62.5; leftover $1160.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 104 | $11.13 | $2.30 | — | $3,480.51 | — | rank by hot_score; rank hot_score; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1160.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 127 | $9.08 | $2.37 | — | $2,324.98 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $1160.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 140 | $8.28 | $2.41 | — | $1,163.37 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; leftover $1160.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAN` | 3877 | $0.29 | $23.03 | — | $0.51 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $1160.38 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.51 | ▲ close $9,669.70 vs 09:30 $9,233.91 (session +502.15) | 16:00 close · cash $0.51 · equity $9,669.70 vs 09:30 $9,233.91 (+435.79; session marks +502.15) · 8 name(s) marked open→close (per-name table). MRNA×7 09:30 $133.11 → close $145.13 +84.14; CYPH×992 09:30 $1.32 → close $1.42 +99.20; XHG×258 09:30 $4.49 → close $4.41 -20.64; CAPR×170 09:30 $6.81 → close $6.29 -88.40; ARCT×104 09:30 $11.13 → close $13.45 +241.28; IOVA×127 09:30 $9.08 → close $8.29 -100.33; MRVI×140 09:30 $8.28 → close $8.64 +50.40; CAN×3877 09:30 $0.29 → close $0.35 +236.50 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.51 | ▲ 09:30 equity $10,394.40 vs yday $9,669.70 (+724.70) | 09:30 open · cash $0.51 (unchanged overnight, no fees) · equity $10,394.40 vs prior close $9,669.70 (+724.70) · 8 name(s) re-marked at the open (per-name table). MRNA×7 yday $145.13 → 09:30 $142.70 -17.01; CYPH×992 yday $1.42 → 09:30 $1.83 +406.72; XHG×258 yday $4.41 → 09:30 $4.32 -23.22; CAPR×170 yday $6.29 → 09:30 $8.03 +295.80; ARCT×104 yday $13.45 → 09:30 $13.33 -12.48; IOVA×127 yday $8.29 → 09:30 $8.08 -26.67; MRVI×140 yday $8.64 → 09:30 $8.59 -7.00; CAN×3877 yday $0.35 → 09:30 $0.38 +108.56 | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 7 | $142.70 | $2.03 | $-56.12 | $997.38 | ▼ -56.12 after sell → book $10,392.37; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 992 | $1.83 | $12.98 | $+648.79 | $2,799.76 | ▲ +648.79 after sell → book $10,379.39; vs 09:30 mark -12.98 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 258 | $4.32 | $3.38 | $-50.57 | $3,910.94 | ▼ -50.57 after sell → book $10,376.01; vs 09:30 mark -3.38 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 170 | $8.03 | $2.54 | $+202.36 | $5,273.50 | ▲ +202.36 after sell → book $10,373.47; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 104 | $13.33 | $2.33 | $+224.17 | $6,657.49 | ▲ +224.17 after sell → book $10,371.14; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `IOVA` | 127 | $8.08 | $2.40 | $-131.77 | $7,681.25 | ▼ -131.77 after sell → book $10,368.74; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 140 | $8.59 | $2.44 | $+38.55 | $8,881.40 | ▲ +38.55 after sell → book $10,366.29; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAN` | 3877 | $0.38 | $27.14 | $+294.89 | $10,339.16 | ▲ +294.89 after sell → book $10,339.16; vs 09:30 mark -27.13 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,339.16 | ▲ close $10,339.16 vs 09:30 $10,394.40 (session +0.00) | 16:00 close · cash $10,339.16 · no lots left · equity $10,339.16. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,339.16 | ▲ 09:30 equity $10,339.16 vs yday $10,339.16 (-0.00) | 09:30 open · cash $10,339.16 · no holdings · equity $10,339.16 vs prior close $10,339.16 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 53 | $24.11 | $2.15 | — | $9,059.18 | — | rank by hot_score; rank hot_score; list yday_mover; ret5=+891.7; leftover $1292.39 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 828 | $1.56 | $10.68 | — | $7,756.82 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $1292.39 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 317 | $4.07 | $4.09 | — | $6,462.54 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; leftover $1292.39 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 67 | $19.04 | $2.19 | — | $5,184.67 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; leftover $1292.39 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 246 | $5.24 | $3.17 | — | $3,892.45 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1292.39 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FWDI` | 226 | $5.71 | $2.92 | — | $2,599.08 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+33.9; leftover $1292.39 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 147 | $8.79 | $2.43 | — | $1,304.52 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $1292.39 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 364 | $3.55 | $4.70 | — | $7.62 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+27.9; leftover $1292.39 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.62 | ▲ close $10,969.59 vs 09:30 $10,339.16 (session +662.76) | 16:00 close · cash $7.62 · equity $10,969.59 vs 09:30 $10,339.16 (+630.43; session marks +662.76) · 8 name(s) marked open→close (per-name table). REAX×53 09:30 $24.11 → close $28.43 +228.96; CYPH×828 09:30 $1.56 → close $1.64 +66.24; XHG×317 09:30 $4.07 → close $4.02 -15.85; ASST×67 09:30 $19.04 → close $21.39 +157.45; ALVO×246 09:30 $5.24 → close $5.05 -46.74; FWDI×226 09:30 $5.71 → close $6.05 +76.84; SUJA×147 09:30 $8.79 → close $9.33 +79.38; GORO×364 09:30 $3.55 → close $3.87 +116.48 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.62 | ▼ 09:30 equity $10,665.67 vs yday $10,969.59 (-303.92) | 09:30 open · cash $7.62 (unchanged overnight, no fees) · equity $10,665.67 vs prior close $10,969.59 (-303.92) · 8 name(s) re-marked at the open (per-name table). REAX×53 yday $28.43 → 09:30 $26.61 -96.46; CYPH×828 yday $1.64 → 09:30 $1.60 -33.12; XHG×317 yday $4.02 → 09:30 $3.81 -66.57; ASST×67 yday $21.39 → 09:30 $20.72 -44.89; ALVO×246 yday $5.05 → 09:30 $4.98 -17.22; FWDI×226 yday $6.05 → 09:30 $5.97 -18.08; SUJA×147 yday $9.33 → 09:30 $9.39 +8.82; GORO×364 yday $3.87 → 09:30 $3.77 -36.40 | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 53 | $26.61 | $2.17 | $+128.18 | $1,415.78 | ▲ +128.18 after sell → book $10,663.50; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 828 | $1.60 | $10.83 | $+11.61 | $2,729.75 | ▲ +11.61 after sell → book $10,652.67; vs 09:30 mark -10.83 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 67 | $20.72 | $2.21 | $+108.16 | $4,115.78 | ▲ +108.16 after sell → book $10,650.46; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ALVO` | 246 | $4.98 | $3.22 | $-70.36 | $5,337.64 | ▼ -70.36 after sell → book $10,647.24; vs 09:30 mark -3.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FWDI` | 226 | $5.97 | $2.96 | $+52.88 | $6,683.89 | ▲ +52.88 after sell → book $10,644.27; vs 09:30 mark -2.97 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GORO` | 364 | $3.77 | $4.77 | $+70.62 | $8,051.41 | ▲ +70.62 after sell → book $10,639.51; vs 09:30 mark -4.76 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 95 | $14.11 | $2.27 | — | $6,708.68 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+11.4; leftover $1341.90 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 230 | $5.81 | $2.97 | — | $5,369.41 | — | rank by hot_score; rank hot_score; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $1341.90 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 161 | $8.29 | $2.47 | — | $4,032.25 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; leftover $1341.90 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FIGR` | 33 | $40.50 | $2.09 | — | $2,693.66 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+15.8; leftover $1341.90 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **BUY** | `KURA` | 98 | $13.63 | $2.28 | — | $1,355.64 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+10.2; leftover $1341.90 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `MNRO` | 95 | $14.00 | $2.27 | — | $23.36 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+17.8; leftover $1341.90 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.36 | ▲ close $10,635.64 vs 09:30 $10,665.67 (session +10.50) | 16:00 close · cash $23.36 · equity $10,635.64 vs 09:30 $10,665.67 (-30.03; session marks +10.50) · 8 name(s) marked open→close (per-name table). XHG×317 09:30 $3.81 → close $4.06 +79.25; SUJA×147 09:30 $9.39 → close $9.44 +7.35; BYND×95 09:30 $14.11 → close $14.25 +13.30; USDE×230 09:30 $5.81 → close $5.98 +39.10; CAPR×161 09:30 $8.29 → close $9.36 +172.27; FIGR×33 09:30 $40.50 → close $37.08 -112.86; KURA×98 09:30 $13.63 → close $13.06 -55.86; MNRO×95 09:30 $14.00 → close $12.61 -132.05 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.36 | ▲ 09:30 equity $10,717.34 vs yday $10,635.64 (+81.70) | 09:30 open · cash $23.36 (unchanged overnight, no fees) · equity $10,717.34 vs prior close $10,635.64 (+81.70) · 8 name(s) re-marked at the open (per-name table). XHG×317 yday $4.06 → 09:30 $4.06 +0.00; SUJA×147 yday $9.44 → 09:30 $9.41 -4.41; BYND×95 yday $14.25 → 09:30 $14.20 -4.75; USDE×230 yday $5.98 → 09:30 $6.50 +119.60; CAPR×161 yday $9.36 → 09:30 $9.19 -27.37; FIGR×33 yday $37.08 → 09:30 $37.42 +11.22; KURA×98 yday $13.06 → 09:30 $12.98 -7.84; MNRO×95 yday $12.61 → 09:30 $12.56 -4.75 | — |
| 2026-08-27 09:30 ET | **SELL** | `XHG` | 317 | $4.06 | $4.15 | $-11.41 | $1,306.23 | ▼ -11.41 after sell → book $10,713.19; vs 09:30 mark -4.15 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SUJA` | 147 | $9.41 | $2.47 | $+86.24 | $2,687.03 | ▲ +86.24 after sell → book $10,710.72; vs 09:30 mark -2.47 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 95 | $14.20 | $2.30 | $+3.97 | $4,033.73 | ▲ +3.97 after sell → book $10,708.42; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 230 | $6.50 | $3.02 | $+152.72 | $5,525.71 | ▲ +152.72 after sell → book $10,705.40; vs 09:30 mark -3.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 161 | $9.19 | $2.51 | $+139.92 | $7,002.79 | ▲ +139.92 after sell → book $10,702.89; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FIGR` | 33 | $37.42 | $2.11 | $-105.84 | $8,235.54 | ▼ -105.84 after sell → book $10,700.78; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `KURA` | 98 | $12.98 | $2.31 | $-68.29 | $9,505.27 | ▼ -68.29 after sell → book $10,698.47; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `MNRO` | 95 | $12.56 | $2.30 | $-141.38 | $10,696.17 | ▼ -141.38 after sell → book $10,696.17; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 514 | $2.60 | $6.63 | — | $9,353.14 | — | rank by hot_score; rank hot_score; list flatten; ret5=+13.0; leftover $1337.02 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 32 | $41.44 | $2.09 | — | $8,024.98 | — | rank by hot_score; rank hot_score; list flatten; ret5=+3.1; leftover $1337.02 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PGY` | 58 | $22.93 | $2.16 | — | $6,692.87 | — | rank by hot_score; rank hot_score; list mover_buy; 🔵; ret5=+9.5; leftover $1337.02 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 92 | $14.42 | $2.27 | — | $5,363.97 | — | rank by hot_score; rank hot_score; list flatten; ret5=+7.1; leftover $1337.02 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 44 | $29.83 | $2.12 | — | $4,049.32 | — | rank by hot_score; rank hot_score; list mover_buy; 🔵; ret5=+7.6; leftover $1337.02 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 5 | $253.44 | $2.00 | — | $2,780.12 | — | rank by hot_score; rank hot_score; list mover_buy; 🔵; ret5=+3.3; leftover $1337.02 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ANET` | 6 | $205.90 | $2.01 | — | $1,542.71 | — | rank by hot_score; rank hot_score; list mover_buy; 🔵; ret5=+8.5; leftover $1337.02 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 55 | $24.00 | $2.15 | — | $220.56 | — | rank by hot_score; rank hot_score; list flatten; ret5=+8.7; leftover $1337.02 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $220.56 | ▼ close $10,666.71 vs 09:30 $10,717.34 (session -8.03) | 16:00 close · cash $220.56 · equity $10,666.71 vs 09:30 $10,717.34 (-50.63; session marks -8.03) · 8 name(s) marked open→close (per-name table). SLI×514 09:30 $2.60 → close $2.64 +20.56; RRC×32 09:30 $41.44 → close $41.64 +6.40; PGY×58 09:30 $22.93 → close $23.26 +19.14; CRK×92 09:30 $14.42 → close $14.62 +18.40; GEN×44 09:30 $29.83 → close $30.50 +29.48; MRVL×5 09:30 $253.44 → close $241.45 -59.95; ANET×6 09:30 $205.90 → close $201.09 -28.86; MOS×55 09:30 $24.00 → close $23.76 -13.20 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $220.56 | ▼ 09:30 equity $10,611.45 vs yday $10,666.71 (-55.26) | 09:30 open · cash $220.56 (unchanged overnight, no fees) · equity $10,611.45 vs prior close $10,666.71 (-55.26) · 8 name(s) re-marked at the open (per-name table). SLI×514 yday $2.64 → 09:30 $2.68 +20.56; RRC×32 yday $41.64 → 09:30 $41.74 +3.20; PGY×58 yday $23.26 → 09:30 $23.21 -2.90; CRK×92 yday $14.62 → 09:30 $14.63 +0.92; GEN×44 yday $30.50 → 09:30 $30.50 +0.00; MRVL×5 yday $241.45 → 09:30 $225.26 -80.95; ANET×6 yday $201.09 → 09:30 $200.00 -6.54; MOS×55 yday $23.76 → 09:30 $23.95 +10.45 | — |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 514 | $2.68 | $6.73 | $+27.76 | $1,591.35 | ▲ +27.76 after sell → book $10,604.72; vs 09:30 mark -6.73 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `RRC` | 32 | $41.74 | $2.11 | $+5.41 | $2,924.92 | ▲ +5.41 after sell → book $10,602.61; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `PGY` | 58 | $23.21 | $2.18 | $+11.89 | $4,268.92 | ▲ +11.89 after sell → book $10,600.43; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRK` | 92 | $14.63 | $2.29 | $+14.76 | $5,612.59 | ▲ +14.76 after sell → book $10,598.14; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 44 | $30.50 | $2.14 | $+25.22 | $6,952.44 | ▲ +25.22 after sell → book $10,595.99; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MRVL` | 5 | $225.26 | $2.02 | $-144.93 | $8,076.72 | ▼ -144.93 after sell → book $10,593.97; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `ANET` | 6 | $200.00 | $2.03 | $-39.44 | $9,274.69 | ▼ -39.44 after sell → book $10,591.94; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 55 | $23.95 | $2.18 | $-7.08 | $10,589.76 | ▼ -7.08 after sell → book $10,589.76; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 94 | $14.00 | $2.27 | — | $9,271.49 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=-3.3; leftover $1323.72 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 136 | $9.73 | $2.40 | — | $7,945.81 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+47.1; leftover $1323.72 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MRNA` | 9 | $137.19 | $2.02 | — | $6,709.09 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+7.1; leftover $1323.72 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $146.07 | $2.02 | — | $5,392.44 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1323.72 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SNPS` | 2 | $461.85 | $2.00 | — | $4,466.74 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+16.8; leftover $1323.72 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SRPT` | 61 | $21.49 | $2.17 | — | $3,153.68 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+12.3; leftover $1323.72 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NEO` | 72 | $18.36 | $2.21 | — | $1,829.56 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+12.8; leftover $1323.72 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VYX` | 144 | $9.13 | $2.42 | — | $512.41 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ret5=+20.0; leftover $1323.72 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $512.41 | ▼ close $10,418.78 vs 09:30 $10,611.45 (session -153.48) | 16:00 close · cash $512.41 · equity $10,418.78 vs 09:30 $10,611.45 (-192.67; session marks -153.48) · 8 name(s) marked open→close (per-name table). BYND×94 09:30 $14.00 → close $13.86 -13.16; CAPR×136 09:30 $9.73 → close $9.59 -19.04; MRNA×9 09:30 $137.19 → close $137.99 +7.20; ANF×9 09:30 $146.07 → close $148.42 +21.15; SNPS×2 09:30 $461.85 → close $442.61 -38.48; SRPT×61 09:30 $21.49 → close $20.86 -38.43; NEO×72 09:30 $18.36 → close $18.05 -22.32; VYX×144 09:30 $9.13 → close $8.78 -50.40 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $512.41 | ▼ 09:30 equity $10,298.26 vs yday $10,418.78 (-120.52) | 09:30 open · cash $512.41 (unchanged overnight, no fees) · equity $10,298.26 vs prior close $10,418.78 (-120.52) · 8 name(s) re-marked at the open (per-name table). BYND×94 yday $13.86 → 09:30 $13.81 -4.70; CAPR×136 yday $9.59 → 09:30 $9.50 -12.24; MRNA×9 yday $137.99 → 09:30 $134.10 -35.01; ANF×9 yday $148.42 → 09:30 $148.03 -3.51; SNPS×2 yday $442.61 → 09:30 $437.95 -9.32; SRPT×61 yday $20.86 → 09:30 $20.56 -18.30; NEO×72 yday $18.05 → 09:30 $17.77 -20.16; VYX×144 yday $8.78 → 09:30 $8.66 -17.28 | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 136 | $9.50 | $2.43 | $-36.11 | $1,801.98 | ▼ -36.11 after sell → book $10,295.83; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.03 | $2.04 | $+13.59 | $3,132.22 | ▲ +13.59 after sell → book $10,293.80; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SNPS` | 2 | $437.95 | $2.02 | $-51.81 | $4,006.10 | ▼ -51.81 after sell → book $10,291.78; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `SRPT` | 61 | $20.56 | $2.19 | $-61.10 | $5,258.07 | ▼ -61.10 after sell → book $10,289.59; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `NEO` | 72 | $17.77 | $2.23 | $-46.91 | $6,535.28 | ▼ -46.91 after sell → book $10,287.36; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `VYX` | 144 | $8.66 | $2.46 | $-72.56 | $7,779.86 | ▼ -72.56 after sell → book $10,284.90; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,779.86 | ▲ close $10,293.12 vs 09:30 $10,298.26 (session +8.22) | 16:00 close · cash $7,779.86 · equity $10,293.12 vs 09:30 $10,298.26 (-5.14; session marks +8.22) · 2 name(s) marked open→close (per-name table). BYND×94 09:30 $13.81 → close $13.30 -47.94; MRNA×9 09:30 $134.10 → close $140.34 +56.16 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,779.86 | ▼ 09:30 equity $10,267.87 vs yday $10,293.12 (-25.25) | 09:30 open · cash $7,779.86 (unchanged overnight, no fees) · equity $10,267.87 vs prior close $10,293.12 (-25.25) · 2 name(s) re-marked at the open (per-name table). BYND×94 yday $13.30 → 09:30 $13.04 -24.44; MRNA×9 yday $140.34 → 09:30 $140.25 -0.81 | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 94 | $13.04 | $2.30 | $-94.81 | $9,003.32 | ▼ -94.81 after sell → book $10,265.57; vs 09:30 mark -2.30 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,003.32 | ▲ close $10,391.75 vs 09:30 $10,267.87 (session +126.18) | 16:00 close · cash $9,003.32 · equity $10,391.75 vs 09:30 $10,267.87 (+123.88; session marks +126.18) · 1 name(s) marked open→close (per-name table). MRNA×9 09:30 $140.25 → close $154.27 +126.18 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,003.32 | ▼ 09:30 equity $10,365.92 vs yday $10,391.75 (-25.83) | 09:30 open · cash $9,003.32 (unchanged overnight, no fees) · equity $10,365.92 vs prior close $10,391.75 (-25.83) · 1 name(s) re-marked at the open (per-name table). MRNA×9 yday $154.27 → 09:30 $151.40 -25.83 | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,003.32 | ▼ close $10,360.61 vs 09:30 $10,365.92 (session -5.31) | 16:00 close · cash $9,003.32 · equity $10,360.61 vs 09:30 $10,365.92 (-5.31; session marks -5.31) · 1 name(s) marked open→close (per-name table). MRNA×9 09:30 $151.40 → close $150.81 -5.31 | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,003.32 | ▼ 09:30 equity $10,316.83 vs yday $10,360.61 (-43.78) | 09:30 open · cash $9,003.32 (unchanged overnight, no fees) · equity $10,316.83 vs prior close $10,360.61 (-43.78) · 1 name(s) re-marked at the open (per-name table). MRNA×9 yday $150.81 → 09:30 $145.94 -43.79 | — |
| 2026-09-03 09:30 ET | **SELL** | `MRNA` | 9 | $145.94 | $2.04 | $+74.74 | $10,314.79 | ▲ +74.74 after sell → book $10,314.79; vs 09:30 mark -2.04 | dropped from list after 4 sess (min 1) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 724 | $1.78 | $9.34 | — | $9,016.73 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; leftover $1289.35 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 70 | $18.40 | $2.20 | — | $7,726.53 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; leftover $1289.35 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 94 | $13.71 | $2.27 | — | $6,435.52 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; leftover $1289.35 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 53 | $23.88 | $2.15 | — | $5,167.73 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1289.35 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SID` | 948 | $1.36 | $12.23 | — | $3,866.22 | — | rank by hot_score; rank hot_score; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1289.35 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 668 | $1.93 | $8.62 | — | $2,568.36 | — | rank by hot_score; rank hot_score; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1289.35 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AGCO` | 10 | $127.91 | $2.02 | — | $1,287.24 | — | rank by hot_score; rank hot_score; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.6; leftover $1289.35 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ASST` | 50 | $25.62 | $2.14 | — | $3.85 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+13.1; leftover $1289.35 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.85 | ▼ close $9,932.25 vs 09:30 $10,316.83 (session -341.57) | 16:00 close · cash $3.85 · equity $9,932.25 vs 09:30 $10,316.83 (-384.58; session marks -341.57) · 8 name(s) marked open→close (per-name table). GPRO×724 09:30 $1.78 → close $1.39 -282.36; REAX×70 09:30 $18.40 → close $18.40 +0.00; CNH×94 09:30 $13.71 → close $13.84 +12.22; MMED×53 09:30 $23.88 → close $23.84 -2.12; SID×948 09:30 $1.36 → close $1.26 -94.80; BMEA×668 09:30 $1.93 → close $1.91 -13.36; AGCO×10 09:30 $127.91 → close $125.82 -20.90; ASST×50 09:30 $25.62 → close $26.82 +59.75 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.85 | ▼ 09:30 equity $9,861.49 vs yday $9,932.25 (-70.76) | 09:30 open · cash $3.85 (unchanged overnight, no fees) · equity $9,861.49 vs prior close $9,932.25 (-70.76) · 8 name(s) re-marked at the open (per-name table). GPRO×724 yday $1.39 → 09:30 $1.48 +65.16; REAX×70 yday $18.40 → 09:30 $18.15 -17.50; CNH×94 yday $13.84 → 09:30 $13.89 +4.70; MMED×53 yday $23.84 → 09:30 $23.84 +0.00; SID×948 yday $1.26 → 09:30 $1.23 -28.44; BMEA×668 yday $1.91 → 09:30 $1.90 -6.68; AGCO×10 yday $125.82 → 09:30 $125.22 -6.00; ASST×50 yday $26.82 → 09:30 $25.18 -82.00 | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 70 | $18.15 | $2.22 | $-21.92 | $1,272.13 | ▼ -21.92 after sell → book $9,859.27; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 94 | $13.89 | $2.30 | $+12.35 | $2,575.50 | ▲ +12.35 after sell → book $9,856.98; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 53 | $23.84 | $2.17 | $-6.44 | $3,836.85 | ▼ -6.44 after sell → book $9,854.81; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SID` | 948 | $1.23 | $12.40 | $-147.87 | $4,990.49 | ▼ -147.87 after sell → book $9,842.41; vs 09:30 mark -12.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `BMEA` | 668 | $1.90 | $8.74 | $-37.40 | $6,250.95 | ▼ -37.40 after sell → book $9,833.67; vs 09:30 mark -8.74 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `AGCO` | 10 | $125.22 | $2.04 | $-30.96 | $7,501.11 | ▼ -30.96 after sell → book $9,831.63; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 158 | $7.87 | $2.46 | — | $6,255.19 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; leftover $1250.19 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 215 | $5.79 | $2.77 | — | $5,007.56 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $1250.19 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 498 | $2.51 | $6.42 | — | $3,751.16 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+21.8; leftover $1250.19 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `HOOD` | 10 | $120.47 | $2.02 | — | $2,544.39 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+13.6; leftover $1250.19 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `FRNM` | 76 | $16.40 | $2.22 | — | $1,295.77 | — | rank by hot_score; rank hot_score; list mover_buy; 🔵; ⚪; ret5=+21.2; leftover $1250.19 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 275 | $4.53 | $3.55 | — | $46.47 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.5; leftover $1250.19 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $46.47 | ▲ close $10,218.85 vs 09:30 $9,861.49 (session +406.67) | 16:00 close · cash $46.47 · equity $10,218.85 vs 09:30 $9,861.49 (+357.36; session marks +406.67) · 8 name(s) marked open→close (per-name table). GPRO×724 09:30 $1.48 → close $1.70 +159.28; ASST×50 09:30 $25.18 → close $27.14 +98.00; USDE×158 09:30 $7.87 → close $7.93 +9.48; DFDV×215 09:30 $5.79 → close $5.87 +17.20; BRR×498 09:30 $2.51 → close $2.66 +74.70; HOOD×10 09:30 $120.47 → close $122.11 +16.35; FRNM×76 09:30 $16.40 → close $16.31 -6.84; IRD×275 09:30 $4.53 → close $4.67 +38.50 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $46.47 | ▼ 09:30 equity $10,070.13 vs yday $10,218.85 (-148.72) | 09:30 open · cash $46.47 (unchanged overnight, no fees) · equity $10,070.13 vs prior close $10,218.85 (-148.72) · 8 name(s) re-marked at the open (per-name table). GPRO×724 yday $1.70 → 09:30 $1.56 -97.74; ASST×50 yday $27.14 → 09:30 $26.44 -35.00; USDE×158 yday $7.93 → 09:30 $7.76 -26.86; DFDV×215 yday $5.87 → 09:30 $5.81 -12.90; BRR×498 yday $2.66 → 09:30 $2.66 +0.00; HOOD×10 yday $122.11 → 09:30 $125.07 +29.60; FRNM×76 yday $16.31 → 09:30 $16.74 +32.68; IRD×275 yday $4.67 → 09:30 $4.53 -38.50 | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 724 | $1.56 | $9.47 | $-174.47 | $1,170.06 | ▼ -174.47 after sell → book $10,060.66; vs 09:30 mark -9.47 | dropped from list after 2 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 50 | $26.44 | $2.16 | $+36.45 | $2,489.90 | ▲ +36.45 after sell → book $10,058.50; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 158 | $7.76 | $2.50 | $-22.34 | $3,713.48 | ▼ -22.34 after sell → book $10,056.00; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 215 | $5.81 | $2.82 | $-1.29 | $4,959.81 | ▼ -1.29 after sell → book $10,053.18; vs 09:30 mark -2.82 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `BRR` | 498 | $2.66 | $6.52 | $+61.76 | $6,277.98 | ▲ +61.76 after sell → book $10,046.67; vs 09:30 mark -6.51 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `HOOD` | 10 | $125.07 | $2.04 | $+41.89 | $7,526.64 | ▲ +41.89 after sell → book $10,044.63; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 76 | $16.74 | $2.24 | $+21.38 | $8,796.64 | ▲ +21.38 after sell → book $10,042.39; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `IRD` | 275 | $4.53 | $3.60 | $-7.15 | $10,038.78 | ▼ -7.15 after sell → book $10,038.78; vs 09:30 mark -3.61 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,038.78 | ▲ close $10,038.78 vs 09:30 $10,070.13 (session +0.00) | 16:00 close · cash $10,038.78 · no lots left · equity $10,038.78. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ALM` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KURA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `KGC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DFDV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SUJA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SKYX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `REAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ASST` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CYPH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ARCT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SUJA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AGCO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TWI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SECZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SKYX` | hard_red | hard-red S=-11.47 sit; no new buys |
