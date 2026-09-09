# Factor mine action — `union_news_vol_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **+7.15%** ($10,715) · signal-only (no cash/fees) was +6.32%. Starts YES **18/19**. Fills 69 · skips 92 · realized $+716.31.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the news camera (does the morning packet like the headline?) is green.
- Must-have: the volume camera (is this name unusually active?) is green.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).
- Must-not: the news camera (does the morning packet like the headline?) is red.

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Keep the first 8 names in list order.
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- This is a LONG sleeve: it buys shares and wants the price to go up.

### When it sells

- Sell first, then buy. Never sell a ticker we do not hold.
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news=good,vol=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,454.56.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `ANGX` | 464 | — | $4.31 | +0.00 | $4.37 | +27.84 | +27.84 | +0.00 | +27.84 |
| 2026-08-14 | `ARX` | 102 | — | $19.57 | +0.00 | $19.58 | +1.02 | +1.02 | +0.00 | +1.02 |
| 2026-08-14 | `SNDK` | 1 | — | $1646.93 | +0.00 | $1641.11 | -5.82 | -5.82 | +0.00 | -5.82 |
| 2026-08-14 | `MH` | 147 | — | $13.55 | +0.00 | $13.10 | -66.15 | -66.15 | +0.00 | -66.15 |
| 2026-08-14 | `HLIT` | 151 | — | $13.18 | +0.00 | $13.92 | +111.74 | +111.74 | +0.00 | +111.74 |
| 2026-08-17 | `ANGX` | 464 | $4.37 | $4.60 | +106.72 | $4.71 | +51.04 | +157.76 | +134.56 | +185.60 |
| 2026-08-17 | `ARX` | 102 | $19.58 | $19.57 | -1.02 | $19.54 | -3.06 | -4.08 | +0.00 | -3.06 |
| 2026-08-17 | `SNDK` | 1 | $1641.11 | $1700.74 | +59.63 | $1786.85 | +86.11 | +145.74 | +53.81 | +139.92 |
| 2026-08-17 | `MH` | 147 | $13.10 | $13.16 | +8.82 | $12.77 | -57.33 | -48.51 | -57.33 | -114.66 |
| 2026-08-17 | `HLIT` | 151 | $13.92 | $13.84 | -12.08 | $13.43 | -61.91 | -73.99 | +99.66 | +37.75 |
| 2026-08-18 | `ANGX` | 464 | $4.71 | $4.79 | +37.12 | $4.85 | +27.84 | +64.96 | +222.72 | +250.56 |
| 2026-08-18 | `ARX` | 102 | $19.54 | $19.57 | +3.06 | $19.56 | -1.02 | +2.04 | +0.00 | -1.02 |
| 2026-08-18 | `SNDK` | 1 | $1786.85 | $1677.54 | -109.31 | $1625.78 | -51.76 | -161.07 | +30.61 | -21.15 |
| 2026-08-18 | `MH` | 147 | $12.77 | $13.00 | +33.81 | $13.12 | +17.64 | +51.45 | -80.85 | -63.21 |
| 2026-08-18 | `HLIT` | 151 | $13.43 | $12.93 | -75.50 | $12.73 | -30.20 | -105.70 | -37.75 | -67.95 |
| 2026-08-19 | `ANGX` | 464 | $4.85 | $4.79 | -27.84 | $4.60 | -88.16 | -116.00 | +222.72 | +134.56 |
| 2026-08-19 | `ARX` | 102 | $19.56 | $19.58 | +2.04 | — | +0.00 | +2.04 | +1.02 | — |
| 2026-08-19 | `SNDK` | 1 | $1625.78 | $1682.40 | +56.62 | — | +0.00 | +56.62 | +35.47 | — |
| 2026-08-19 | `MH` | 147 | $13.12 | $13.01 | -16.17 | — | +0.00 | -16.17 | -79.38 | — |
| 2026-08-19 | `HLIT` | 151 | $12.73 | $12.90 | +25.67 | — | +0.00 | +25.67 | -42.28 | — |
| 2026-08-20 | `ANGX` | 464 | $4.60 | $4.57 | -13.92 | — | +0.00 | -13.92 | +120.64 | — |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `MRNA` | 8 | — | $150.14 | +0.00 | $133.32 | -134.56 | -134.56 | +0.00 | -134.56 |
| 2026-08-20 | `HUMA` | 1768 | — | $0.71 | +0.00 | $0.68 | -45.97 | -45.97 | +0.00 | -45.97 |
| 2026-08-20 | `BTGO` | 189 | — | $6.61 | +0.00 | $6.60 | -0.95 | -0.95 | +0.00 | -0.95 |
| 2026-08-20 | `ASST` | 78 | — | $16.00 | +0.00 | $16.13 | +10.14 | +10.14 | +0.00 | +10.14 |
| 2026-08-20 | `ZLAB` | 47 | — | $26.57 | +0.00 | $26.02 | -25.85 | -25.85 | +0.00 | -25.85 |
| 2026-08-20 | `CRSP` | 21 | — | $58.73 | +0.00 | $58.12 | -12.81 | -12.81 | +0.00 | -12.81 |
| 2026-08-20 | `APA` | 27 | — | $44.76 | +0.00 | $44.39 | -9.99 | -9.99 | +0.00 | -9.99 |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | $97.03 | +17.03 | +44.20 | +61.23 | +78.26 |
| 2026-08-21 | `MRNA` | 8 | $133.32 | $133.11 | -1.68 | $145.13 | +96.16 | +94.48 | -136.24 | -40.08 |
| 2026-08-21 | `HUMA` | 1768 | $0.68 | $0.67 | -12.38 | $0.64 | -56.58 | -68.96 | -58.34 | -114.92 |
| 2026-08-21 | `BTGO` | 189 | $6.60 | $6.95 | +66.15 | $6.84 | -20.79 | +45.36 | +65.20 | +44.41 |
| 2026-08-21 | `ASST` | 78 | $16.13 | $17.66 | +119.34 | $18.22 | +43.68 | +163.02 | +129.48 | +173.16 |
| 2026-08-21 | `ZLAB` | 47 | $26.02 | $26.25 | +10.81 | $26.01 | -11.28 | -0.47 | -15.04 | -26.32 |
| 2026-08-21 | `CRSP` | 21 | $58.12 | $59.72 | +33.60 | $59.50 | -4.62 | +28.98 | +20.79 | +16.17 |
| 2026-08-21 | `APA` | 27 | $44.39 | $44.52 | +3.51 | $43.39 | -30.51 | -27.00 | -6.48 | -36.99 |
| 2026-08-21 | `AUTL` | 8 | — | $2.47 | +0.00 | $2.41 | -0.48 | -0.48 | +0.00 | -0.48 |
| 2026-08-21 | `MARA` | 1 | — | $11.70 | +0.00 | $11.26 | -0.44 | -0.44 | +0.00 | -0.44 |
| 2026-08-21 | `BTDR` | 1 | — | $11.10 | +0.00 | $11.37 | +0.27 | +0.27 | +0.00 | +0.27 |
| 2026-08-21 | `HIVE` | 6 | — | $3.24 | +0.00 | $3.03 | -1.26 | -1.26 | +0.00 | -1.26 |
| 2026-08-24 | `BHP` | 13 | $97.03 | $97.34 | +4.03 | $96.66 | -8.84 | -4.81 | +82.29 | +73.45 |
| 2026-08-24 | `MRNA` | 8 | $145.13 | $142.70 | -19.44 | $139.27 | -27.44 | -46.88 | -59.52 | -86.96 |
| 2026-08-24 | `HUMA` | 1768 | $0.64 | $0.68 | +67.18 | $0.67 | -17.68 | +49.50 | -47.74 | -65.42 |
| 2026-08-24 | `BTGO` | 189 | $6.84 | $6.87 | +5.67 | $6.97 | +18.90 | +24.57 | +50.08 | +68.98 |
| 2026-08-24 | `ASST` | 78 | $18.22 | $18.76 | +42.12 | $19.82 | +82.68 | +124.80 | +215.28 | +297.96 |
| 2026-08-24 | `ZLAB` | 47 | $26.01 | $25.59 | -19.74 | $25.51 | -3.76 | -23.50 | -46.06 | -49.82 |
| 2026-08-24 | `CRSP` | 21 | $59.50 | $58.79 | -14.91 | $56.91 | -39.48 | -54.39 | +1.26 | -38.22 |
| 2026-08-24 | `APA` | 27 | $43.39 | $42.93 | -12.42 | $42.10 | -22.41 | -34.83 | -49.41 | -71.82 |
| 2026-08-24 | `AUTL` | 8 | $2.41 | $2.36 | -0.40 | $2.38 | +0.16 | -0.24 | -0.88 | -0.72 |
| 2026-08-24 | `MARA` | 1 | $11.26 | $11.18 | -0.08 | $11.44 | +0.26 | +0.18 | -0.52 | -0.26 |
| 2026-08-24 | `BTDR` | 1 | $11.37 | $11.49 | +0.12 | $11.30 | -0.19 | -0.07 | +0.39 | +0.21 |
| 2026-08-24 | `HIVE` | 6 | $3.03 | $2.98 | -0.30 | $2.94 | -0.24 | -0.54 | -1.56 | -1.80 |
| 2026-08-25 | `BHP` | 13 | $96.66 | $95.95 | -9.23 | — | +0.00 | -9.23 | +64.22 | — |
| 2026-08-25 | `MRNA` | 8 | $139.27 | $141.19 | +15.36 | — | +0.00 | +15.36 | -71.60 | — |
| 2026-08-25 | `HUMA` | 1768 | $0.67 | $0.67 | +0.00 | — | +0.00 | +0.00 | -65.42 | — |
| 2026-08-25 | `BTGO` | 189 | $6.97 | $6.89 | -15.12 | — | +0.00 | -15.12 | +53.86 | — |
| 2026-08-25 | `ASST` | 78 | $19.82 | $20.90 | +84.24 | — | +0.00 | +84.24 | +382.20 | — |
| 2026-08-25 | `ZLAB` | 47 | $25.51 | $25.93 | +19.74 | — | +0.00 | +19.74 | -30.08 | — |
| 2026-08-25 | `CRSP` | 21 | $56.91 | $57.00 | +1.89 | — | +0.00 | +1.89 | -36.33 | — |
| 2026-08-25 | `APA` | 27 | $42.10 | $42.70 | +16.20 | — | +0.00 | +16.20 | -55.62 | — |
| 2026-08-25 | `AUTL` | 8 | $2.38 | $2.32 | -0.48 | $2.34 | +0.16 | -0.32 | -1.20 | -1.04 |
| 2026-08-25 | `MARA` | 1 | $11.44 | $11.28 | -0.16 | $11.29 | +0.01 | -0.15 | -0.42 | -0.41 |
| 2026-08-25 | `BTDR` | 1 | $11.30 | $11.19 | -0.11 | $11.28 | +0.09 | -0.02 | +0.09 | +0.18 |
| 2026-08-25 | `HIVE` | 6 | $2.94 | $2.82 | -0.72 | $2.89 | +0.42 | -0.30 | -2.52 | -2.10 |
| 2026-08-25 | `RUM` | 154 | — | $9.36 | +0.00 | $9.35 | -1.54 | -1.54 | +0.00 | -1.54 |
| 2026-08-25 | `EZPW` | 41 | — | $34.48 | +0.00 | $34.69 | +8.61 | +8.61 | +0.00 | +8.61 |
| 2026-08-25 | `REAX` | 60 | — | $24.00 | +0.00 | $24.00 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `BKKT` | 174 | — | $8.28 | +0.00 | $8.38 | +17.40 | +17.40 | +0.00 | +17.40 |
| 2026-08-25 | `FCX` | 18 | — | $77.90 | +0.00 | $77.49 | -7.38 | -7.38 | +0.00 | -7.38 |
| 2026-08-25 | `NVAX` | 162 | — | $8.88 | +0.00 | $8.93 | +8.10 | +8.10 | +0.00 | +8.10 |
| 2026-08-25 | `AU` | 12 | — | $119.46 | +0.00 | $118.55 | -10.92 | -10.92 | +0.00 | -10.92 |
| 2026-08-26 | `AUTL` | 8 | $2.34 | $2.34 | +0.00 | $2.34 | +0.00 | +0.00 | -1.04 | -1.04 |
| 2026-08-26 | `MARA` | 1 | $11.29 | $11.29 | +0.00 | $11.29 | +0.00 | +0.00 | -0.41 | -0.41 |
| 2026-08-26 | `BTDR` | 1 | $11.28 | $11.28 | +0.00 | $11.28 | +0.00 | +0.00 | +0.18 | +0.18 |
| 2026-08-26 | `HIVE` | 6 | $2.89 | $2.89 | +0.00 | $2.89 | +0.00 | +0.00 | -2.10 | -2.10 |
| 2026-08-26 | `RUM` | 154 | $9.35 | $9.35 | +0.00 | $9.35 | +0.00 | +0.00 | -1.54 | -1.54 |
| 2026-08-26 | `EZPW` | 41 | $34.69 | $34.69 | +0.00 | $34.69 | +0.00 | +0.00 | +8.61 | +8.61 |
| 2026-08-26 | `REAX` | 60 | $24.00 | $24.00 | +0.00 | $24.00 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `BKKT` | 174 | $8.38 | $8.38 | +0.00 | $8.38 | +0.00 | +0.00 | +17.40 | +17.40 |
| 2026-08-26 | `FCX` | 18 | $77.49 | $77.49 | +0.00 | $77.49 | +0.00 | +0.00 | -7.38 | -7.38 |
| 2026-08-26 | `NVAX` | 162 | $8.93 | $8.93 | +0.00 | $8.93 | +0.00 | +0.00 | +8.10 | +8.10 |
| 2026-08-26 | `AU` | 12 | $118.55 | $118.55 | +0.00 | $118.55 | +0.00 | +0.00 | -10.92 | -10.92 |
| 2026-08-27 | `AUTL` | 8 | $2.34 | $2.41 | +0.56 | — | +0.00 | +0.56 | -0.48 | — |
| 2026-08-27 | `MARA` | 1 | $11.29 | $11.56 | +0.27 | — | +0.00 | +0.27 | -0.14 | — |
| 2026-08-27 | `BTDR` | 1 | $11.28 | $11.05 | -0.23 | — | +0.00 | -0.23 | -0.04 | — |
| 2026-08-27 | `HIVE` | 6 | $2.89 | $2.95 | +0.36 | — | +0.00 | +0.36 | -1.74 | — |
| 2026-08-27 | `RUM` | 154 | $9.35 | $10.07 | +110.88 | $9.38 | -106.26 | +4.62 | +109.34 | +3.08 |
| 2026-08-27 | `EZPW` | 41 | $34.69 | $35.70 | +41.41 | $33.90 | -73.80 | -32.39 | +50.02 | -23.78 |
| 2026-08-27 | `REAX` | 60 | $24.00 | $26.61 | +156.60 | $26.59 | -1.20 | +155.40 | +156.60 | +155.40 |
| 2026-08-27 | `BKKT` | 174 | $8.38 | $8.38 | +0.00 | $8.23 | -26.10 | -26.10 | +17.40 | -8.70 |
| 2026-08-27 | `FCX` | 18 | $77.49 | $79.34 | +33.30 | $79.00 | -6.12 | +27.18 | +25.92 | +19.80 |
| 2026-08-27 | `NVAX` | 162 | $8.93 | $9.33 | +64.80 | $9.21 | -19.44 | +45.36 | +72.90 | +53.46 |
| 2026-08-27 | `AU` | 12 | $118.55 | $119.80 | +15.00 | $118.11 | -20.28 | -5.28 | +4.08 | -16.20 |
| 2026-08-28 | `RUM` | 154 | $9.38 | $9.51 | +20.02 | — | +0.00 | +20.02 | +23.10 | — |
| 2026-08-28 | `EZPW` | 41 | $33.90 | $33.50 | -16.40 | — | +0.00 | -16.40 | -40.18 | — |
| 2026-08-28 | `REAX` | 60 | $26.59 | $25.91 | -40.80 | — | +0.00 | -40.80 | +114.60 | — |
| 2026-08-28 | `BKKT` | 174 | $8.23 | $8.50 | +46.98 | — | +0.00 | +46.98 | +38.28 | — |
| 2026-08-28 | `FCX` | 18 | $79.00 | $78.83 | -3.06 | — | +0.00 | -3.06 | +16.74 | — |
| 2026-08-28 | `NVAX` | 162 | $9.21 | $9.12 | -14.58 | — | +0.00 | -14.58 | +38.88 | — |
| 2026-08-28 | `AU` | 12 | $118.11 | $117.41 | -8.40 | — | +0.00 | -8.40 | -24.60 | — |
| 2026-08-28 | `CAPR` | 187 | — | $9.19 | +0.00 | $10.06 | +162.69 | +162.69 | +0.00 | +162.69 |
| 2026-08-28 | `SEDG` | 50 | — | $33.78 | +0.00 | $33.51 | -13.50 | -13.50 | +0.00 | -13.50 |
| 2026-08-28 | `SMTC` | 11 | — | $149.40 | +0.00 | $142.43 | -76.67 | -76.67 | +0.00 | -76.67 |
| 2026-08-28 | `ERAS` | 89 | — | $19.30 | +0.00 | $19.49 | +16.91 | +16.91 | +0.00 | +16.91 |
| 2026-08-28 | `BBWI` | 92 | — | $18.68 | +0.00 | $18.65 | -2.76 | -2.76 | +0.00 | -2.76 |
| 2026-08-28 | `ZYME` | 58 | — | $29.33 | +0.00 | $29.01 | -18.56 | -18.56 | +0.00 | -18.56 |
| 2026-08-31 | `CAPR` | 187 | $10.06 | $9.44 | -115.94 | $9.36 | -14.96 | -130.90 | +46.75 | +31.79 |
| 2026-08-31 | `SEDG` | 50 | $33.51 | $31.50 | -100.50 | $31.27 | -11.50 | -112.00 | -114.00 | -125.50 |
| 2026-08-31 | `SMTC` | 11 | $142.43 | $133.04 | -103.29 | $132.54 | -5.50 | -108.79 | -179.96 | -185.46 |
| 2026-08-31 | `ERAS` | 89 | $19.49 | $17.90 | -141.51 | $17.90 | +0.00 | -141.51 | -124.60 | -124.60 |
| 2026-08-31 | `BBWI` | 92 | $18.65 | $19.30 | +59.80 | $19.22 | -7.36 | +52.44 | +57.04 | +49.68 |
| 2026-08-31 | `ZYME` | 58 | $29.01 | $28.27 | -42.92 | $28.27 | +0.00 | -42.92 | -61.48 | -61.48 |
| 2026-09-01 | `CAPR` | 187 | $9.36 | $10.43 | +200.09 | $10.19 | -44.88 | +155.21 | +231.88 | +187.00 |
| 2026-09-01 | `SEDG` | 50 | $31.27 | $32.22 | +47.50 | $31.80 | -21.00 | +26.50 | -78.00 | -99.00 |
| 2026-09-01 | `SMTC` | 11 | $132.54 | $131.65 | -9.79 | $129.50 | -23.65 | -33.44 | -195.25 | -218.90 |
| 2026-09-01 | `ERAS` | 89 | $17.90 | $18.00 | +8.90 | $17.70 | -26.70 | -17.80 | -115.70 | -142.40 |
| 2026-09-01 | `BBWI` | 92 | $19.22 | $19.10 | -11.04 | $19.10 | +0.00 | -11.04 | +38.64 | +38.64 |
| 2026-09-01 | `ZYME` | 58 | $28.27 | $29.32 | +60.90 | $29.33 | +0.58 | +61.48 | -0.58 | +0.00 |
| 2026-09-02 | `CAPR` | 187 | $10.19 | $10.77 | +108.46 | — | +0.00 | +108.46 | +295.46 | — |
| 2026-09-02 | `SEDG` | 50 | $31.80 | $31.87 | +3.50 | — | +0.00 | +3.50 | -95.50 | — |
| 2026-09-02 | `SMTC` | 11 | $129.50 | $127.63 | -20.57 | — | +0.00 | -20.57 | -239.47 | — |
| 2026-09-02 | `ERAS` | 89 | $17.70 | $17.58 | -10.68 | — | +0.00 | -10.68 | -153.08 | — |
| 2026-09-02 | `BBWI` | 92 | $19.10 | $18.77 | -30.36 | — | +0.00 | -30.36 | +8.28 | — |
| 2026-09-02 | `ZYME` | 58 | $29.33 | $29.32 | -0.58 | — | +0.00 | -0.58 | -0.58 | — |
| 2026-09-03 | `MMED` | 110 | — | $22.78 | +0.00 | $23.76 | +107.80 | +107.80 | +0.00 | +107.80 |
| 2026-09-03 | `DELL` | 5 | — | $462.05 | +0.00 | $492.20 | +150.75 | +150.75 | +0.00 | +150.75 |
| 2026-09-03 | `FRNM` | 165 | — | $15.24 | +0.00 | $15.95 | +117.15 | +117.15 | +0.00 | +117.15 |
| 2026-09-03 | `CXW` | 76 | — | $32.87 | +0.00 | $32.61 | -19.76 | -19.76 | +0.00 | -19.76 |
| 2026-09-04 | `MMED` | 110 | $23.76 | $23.88 | +13.20 | $23.84 | -4.40 | +8.80 | +121.00 | +116.60 |
| 2026-09-04 | `DELL` | 5 | $492.20 | $486.31 | -29.45 | $516.39 | +150.40 | +120.95 | +121.30 | +271.70 |
| 2026-09-04 | `FRNM` | 165 | $15.95 | $15.87 | -13.20 | $16.90 | +169.95 | +156.75 | +103.95 | +273.90 |
| 2026-09-04 | `CXW` | 76 | $32.61 | $32.31 | -22.80 | $33.66 | +102.60 | +79.80 | -42.56 | +60.04 |
| 2026-09-04 | `BAK` | 133 | — | $1.95 | +0.00 | $1.94 | -1.33 | -1.33 | +0.00 | -1.33 |
| 2026-09-07 | `MMED` | 110 | $23.84 | $23.84 | +0.00 | $23.28 | -61.60 | -61.60 | +116.60 | +55.00 |
| 2026-09-07 | `DELL` | 5 | $516.39 | $513.78 | -13.05 | $524.14 | +51.80 | +38.75 | +258.65 | +310.45 |
| 2026-09-07 | `FRNM` | 165 | $16.90 | $16.40 | -82.50 | $16.31 | -14.85 | -97.35 | +191.40 | +176.55 |
| 2026-09-07 | `CXW` | 76 | $33.66 | $33.46 | -15.20 | $34.71 | +95.00 | +79.80 | +44.84 | +139.84 |
| 2026-09-07 | `BAK` | 133 | $1.94 | $1.94 | +0.00 | $1.89 | -6.65 | -6.65 | -1.33 | -7.98 |
| 2026-09-08 | `MMED` | 110 | $23.28 | $23.00 | -30.80 | — | +0.00 | -30.80 | +24.20 | — |
| 2026-09-08 | `DELL` | 5 | $524.14 | $519.47 | -23.35 | — | +0.00 | -23.35 | +287.10 | — |
| 2026-09-08 | `FRNM` | 165 | $16.31 | $16.31 | +0.00 | — | +0.00 | +0.00 | +176.55 | — |
| 2026-09-08 | `CXW` | 76 | $34.71 | $34.80 | +6.84 | — | +0.00 | +6.84 | +146.68 | — |
| 2026-09-08 | `BAK` | 133 | $1.89 | $1.96 | +9.31 | $1.96 | +0.00 | +9.31 | +1.33 | +1.33 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +68.63 | ANGX, ARX, SNDK, MH, HLIT | — | $359.91 | $10,053.48 | ANGX×464, ARX×102, SNDK×1, MH×147, HLIT×151 |
| 2026-08-17 | +2.25 | $359.91 | ANGX×464, ARX×102, SNDK×1, MH×147, HLIT×151 | $10,215.56 | +162.08 | +14.85 | — | — | $359.91 | $10,230.40 | ANGX×464, ARX×102, SNDK×1, MH×147, HLIT×151 |
| 2026-08-18 | -6.20 | $359.91 | ANGX×464, ARX×102, SNDK×1, MH×147, HLIT×151 | $10,119.58 | -110.82 | -37.50 | — | — | $359.91 | $10,082.08 | ANGX×464, ARX×102, SNDK×1, MH×147, HLIT×151 |
| 2026-08-19 | -7.20 | $359.91 | ANGX×464, ARX×102, SNDK×1, MH×147, HLIT×151 | $10,122.41 | +40.33 | -88.16 | — | ARX, SNDK, MH, HLIT | $7,890.55 | $10,024.95 | ANGX×464 |
| 2026-08-20 | +1.12 | $7,890.55 | ANGX×464 | $10,011.03 | -13.92 | -185.93 | BHP, MRNA, HUMA, BTGO, ASST, ZLAB, CRSP, APA | ANGX | $150.85 | $9,786.14 | BHP×13, MRNA×8, HUMA×1768, BTGO×189, ASST×78, ZLAB×47, CRSP×21, APA×27 |
| 2026-08-21 | +3.25 | $150.85 | BHP×13, MRNA×8, HUMA×1768, BTGO×189, ASST×78, ZLAB×47, CRSP×21, APA×27 | $10,032.67 | +246.53 | +31.18 | AUTL, MARA, BTDR, HIVE | — | $88.19 | $10,063.19 | BHP×13, MRNA×8, HUMA×1768, BTGO×189, ASST×78, ZLAB×47, CRSP×21, APA×27, AUTL×8, MARA×1, BTDR×1, HIVE×6 |
| 2026-08-24 | -5.17 | $88.19 | BHP×13, MRNA×8, HUMA×1768, BTGO×189, ASST×78, ZLAB×47, CRSP×21, APA×27, AUTL×8, MARA×1, BTDR×1, HIVE×6 | $10,115.02 | +51.83 | -18.04 | — | — | $88.19 | $10,096.98 | BHP×13, MRNA×8, HUMA×1768, BTGO×189, ASST×78, ZLAB×47, CRSP×21, APA×27, AUTL×8, MARA×1, BTDR×1, HIVE×6 |
| 2026-08-25 | +1.80 | $88.19 | BHP×13, MRNA×8, HUMA×1768, BTGO×189, ASST×78, ZLAB×47, CRSP×21, APA×27, AUTL×8, MARA×1, BTDR×1, HIVE×6 | $10,208.59 | +111.61 | +14.95 | RUM, EZPW, REAX, BKKT, FCX, NVAX, AU | BHP, MRNA, HUMA, BTGO, ASST, ZLAB, CRSP, APA | $92.03 | $10,175.05 | AUTL×8, MARA×1, BTDR×1, HIVE×6, RUM×154, EZPW×41, REAX×60, BKKT×174, FCX×18, NVAX×162, AU×12 |
| 2026-08-26 | +2.02 | $92.03 | AUTL×8, MARA×1, BTDR×1, HIVE×6, RUM×154, EZPW×41, REAX×60, BKKT×174, FCX×18, NVAX×162, AU×12 | $10,175.05 | -0.00 | +0.00 | — | — | $92.03 | $10,175.05 | AUTL×8, MARA×1, BTDR×1, HIVE×6, RUM×154, EZPW×41, REAX×60, BKKT×174, FCX×18, NVAX×162, AU×12 |
| 2026-08-27 | — | $92.03 | AUTL×8, MARA×1, BTDR×1, HIVE×6, RUM×154, EZPW×41, REAX×60, BKKT×174, FCX×18, NVAX×162, AU×12 | $10,598.00 | +422.95 | -253.20 | — | AUTL, MARA, BTDR, HIVE | $150.89 | $10,344.07 | RUM×154, EZPW×41, REAX×60, BKKT×174, FCX×18, NVAX×162, AU×12 |
| 2026-08-28 | +0.75 | $150.89 | RUM×154, EZPW×41, REAX×60, BKKT×174, FCX×18, NVAX×162, AU×12 | $10,327.83 | -16.24 | +68.11 | CAPR, SEDG, SMTC, ERAS, BBWI, ZYME | RUM, EZPW, REAX, BKKT, FCX, NVAX, AU | $110.11 | $10,366.55 | CAPR×187, SEDG×50, SMTC×11, ERAS×89, BBWI×92, ZYME×58 |
| 2026-08-31 | -5.85 | $110.11 | CAPR×187, SEDG×50, SMTC×11, ERAS×89, BBWI×92, ZYME×58 | $9,922.19 | -444.36 | -39.32 | — | — | $110.11 | $9,882.87 | CAPR×187, SEDG×50, SMTC×11, ERAS×89, BBWI×92, ZYME×58 |
| 2026-09-01 | -6.30 | $110.11 | CAPR×187, SEDG×50, SMTC×11, ERAS×89, BBWI×92, ZYME×58 | $10,179.43 | +296.56 | -115.65 | — | — | $110.11 | $10,063.78 | CAPR×187, SEDG×50, SMTC×11, ERAS×89, BBWI×92, ZYME×58 |
| 2026-09-02 | -3.83 | $110.11 | CAPR×187, SEDG×50, SMTC×11, ERAS×89, BBWI×92, ZYME×58 | $10,113.55 | +49.77 | +0.00 | — | CAPR, SEDG, SMTC, ERAS, BBWI, ZYME | $10,099.97 | $10,099.97 | — |
| 2026-09-03 | -0.90 | $10,099.97 | — | $10,099.97 | +0.00 | +355.94 | MMED, DELL, FRNM, CXW | — | $262.18 | $10,446.89 | MMED×110, DELL×5, FRNM×165, CXW×76 |
| 2026-09-04 | +2.25 | $262.18 | MMED×110, DELL×5, FRNM×165, CXW×76 | $10,394.64 | -52.25 | +417.22 | BAK | — | $0.44 | $10,809.47 | MMED×110, DELL×5, FRNM×165, CXW×76, BAK×133 |
| 2026-09-07 | — | $0.44 | MMED×110, DELL×5, FRNM×165, CXW×76, BAK×133 | $10,698.72 | -110.75 | +63.70 | — | — | $0.44 | $10,762.42 | MMED×110, DELL×5, FRNM×165, CXW×76, BAK×133 |
| 2026-09-08 | -11.47 | $0.44 | MMED×110, DELL×5, FRNM×165, CXW×76, BAK×133 | $10,724.42 | -38.00 | +0.00 | — | MMED, DELL, FRNM, CXW | $10,454.56 | $10,715.24 | BAK×133 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 464 | $4.31 | $5.99 | — | $7,994.17 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 102 | $19.57 | $2.30 | — | $5,995.74 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SNDK` | 1 | $1646.93 | $1.99 | — | $4,346.82 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.4; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 147 | $13.55 | $2.43 | — | $2,352.53 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 151 | $13.18 | $2.44 | — | $359.91 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $359.91 | ▲ close $10,053.48 vs 09:30 $10,000.00 (session +68.63) | 16:00 close · cash $359.91 · equity $10,053.48 vs 09:30 $10,000.00 (+53.48; session marks +68.63) · 5 name(s) marked open→close (per-name table). ANGX×464 09:30 $4.31 → close $4.37 +27.84; ARX×102 09:30 $19.57 → close $19.58 +1.02; SNDK×1 09:30 $1646.93 → close $1641.11 -5.82; MH×147 09:30 $13.55 → close $13.10 -66.15; HLIT×151 09:30 $13.18 → close $13.92 +111.74 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $359.91 | ▲ 09:30 equity $10,215.56 vs yday $10,053.48 (+162.08) | 09:30 open · cash $359.91 (unchanged overnight, no fees) · equity $10,215.56 vs prior close $10,053.48 (+162.08) · 5 name(s) re-marked at the open (per-name table). ANGX×464 yday $4.37 → 09:30 $4.60 +106.72; ARX×102 yday $19.58 → 09:30 $19.57 -1.02; SNDK×1 yday $1641.11 → 09:30 $1700.74 +59.63; MH×147 yday $13.10 → 09:30 $13.16 +8.82; HLIT×151 yday $13.92 → 09:30 $13.84 -12.08 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $359.91 | ▲ close $10,230.40 vs 09:30 $10,215.56 (session +14.85) | 16:00 close · cash $359.91 · equity $10,230.40 vs 09:30 $10,215.56 (+14.84; session marks +14.85) · 5 name(s) marked open→close (per-name table). ANGX×464 09:30 $4.60 → close $4.71 +51.04; ARX×102 09:30 $19.57 → close $19.54 -3.06; SNDK×1 09:30 $1700.74 → close $1786.85 +86.11; MH×147 09:30 $13.16 → close $12.77 -57.33; HLIT×151 09:30 $13.84 → close $13.43 -61.91 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $359.91 | ▼ 09:30 equity $10,119.58 vs yday $10,230.40 (-110.82) | 09:30 open · cash $359.91 (unchanged overnight, no fees) · equity $10,119.58 vs prior close $10,230.40 (-110.82) · 5 name(s) re-marked at the open (per-name table). ANGX×464 yday $4.71 → 09:30 $4.79 +37.12; ARX×102 yday $19.54 → 09:30 $19.57 +3.06; SNDK×1 yday $1786.85 → 09:30 $1677.54 -109.31; MH×147 yday $12.77 → 09:30 $13.00 +33.81; HLIT×151 yday $13.43 → 09:30 $12.93 -75.50 | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $359.91 | ▼ close $10,082.08 vs 09:30 $10,119.58 (session -37.50) | 16:00 close · cash $359.91 · equity $10,082.08 vs 09:30 $10,119.58 (-37.50; session marks -37.50) · 5 name(s) marked open→close (per-name table). ANGX×464 09:30 $4.79 → close $4.85 +27.84; ARX×102 09:30 $19.57 → close $19.56 -1.02; SNDK×1 09:30 $1677.54 → close $1625.78 -51.76; MH×147 09:30 $13.00 → close $13.12 +17.64; HLIT×151 09:30 $12.93 → close $12.73 -30.20 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $359.91 | ▲ 09:30 equity $10,122.41 vs yday $10,082.08 (+40.33) | 09:30 open · cash $359.91 (unchanged overnight, no fees) · equity $10,122.41 vs prior close $10,082.08 (+40.33) · 5 name(s) re-marked at the open (per-name table). ANGX×464 yday $4.85 → 09:30 $4.79 -27.84; ARX×102 yday $19.56 → 09:30 $19.58 +2.04; SNDK×1 yday $1625.78 → 09:30 $1682.40 +56.62; MH×147 yday $13.12 → 09:30 $13.01 -16.17; HLIT×151 yday $12.73 → 09:30 $12.90 +25.67 | — |
| 2026-08-19 09:30 ET | **SELL** | `ARX` | 102 | $19.58 | $2.33 | $-3.60 | $2,354.74 | ▼ -3.60 after sell → book $10,120.08; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | join🟡 sector🟡 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `SNDK` | 1 | $1682.40 | $2.02 | $+31.47 | $4,035.13 | ▲ +31.47 after sell → book $10,118.06; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `MH` | 147 | $13.01 | $2.47 | $-84.28 | $5,945.13 | ▼ -84.28 after sell → book $10,115.59; vs 09:30 mark -2.47 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `HLIT` | 151 | $12.90 | $2.48 | $-47.21 | $7,890.55 | ▼ -47.21 after sell → book $10,113.11; vs 09:30 mark -2.48 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,890.55 | ▼ close $10,024.95 vs 09:30 $10,122.41 (session -88.16) | 16:00 close · cash $7,890.55 · equity $10,024.95 vs 09:30 $10,122.41 (-97.46; session marks -88.16) · 1 name(s) marked open→close (per-name table). ANGX×464 09:30 $4.79 → close $4.60 -88.16 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,890.55 | ▼ 09:30 equity $10,011.03 vs yday $10,024.95 (-13.92) | 09:30 open · cash $7,890.55 (unchanged overnight, no fees) · equity $10,011.03 vs prior close $10,024.95 (-13.92) · 1 name(s) re-marked at the open (per-name table). ANGX×464 yday $4.60 → 09:30 $4.57 -13.92 | — |
| 2026-08-20 09:30 ET | **SELL** | `ANGX` | 464 | $4.57 | $6.08 | $+108.57 | $10,004.95 | ▲ +108.57 after sell → book $10,004.95; vs 09:30 mark -6.08 | dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,819.79 | — | combo gate; gate news=good,vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1250.62 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $7,616.65 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1250.62 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 1768 | $0.71 | $17.80 | — | $6,348.87 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+10.5; leftover $1250.62 | join🟡 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 189 | $6.61 | $2.56 | — | $5,097.97 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1250.62 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 78 | $16.00 | $2.22 | — | $3,847.75 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1250.62 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 47 | $26.57 | $2.13 | — | $2,596.83 | — | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ret5=+4.8; leftover $1250.62 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 21 | $58.73 | $2.05 | — | $1,361.44 | — | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1250.62 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 27 | $44.76 | $2.07 | — | $150.85 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ret5=+8.7; leftover $1250.62 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $150.85 | ▼ close $9,786.14 vs 09:30 $10,011.03 (session -185.93) | 16:00 close · cash $150.85 · equity $9,786.14 vs 09:30 $10,011.03 (-224.89; session marks -185.93) · 8 name(s) marked open→close (per-name table). BHP×13 09:30 $91.01 → close $93.63 +34.06; MRNA×8 09:30 $150.14 → close $133.32 -134.56; HUMA×1768 09:30 $0.71 → close $0.68 -45.97; BTGO×189 09:30 $6.61 → close $6.60 -0.95; ASST×78 09:30 $16.00 → close $16.13 +10.14; ZLAB×47 09:30 $26.57 → close $26.02 -25.85; CRSP×21 09:30 $58.73 → close $58.12 -12.81; APA×27 09:30 $44.76 → close $44.39 -9.99 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $150.85 | ▲ 09:30 equity $10,032.67 vs yday $9,786.14 (+246.53) | 09:30 open · cash $150.85 (unchanged overnight, no fees) · equity $10,032.67 vs prior close $9,786.14 (+246.53) · 8 name(s) re-marked at the open (per-name table). BHP×13 yday $93.63 → 09:30 $95.72 +27.17; MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; HUMA×1768 yday $0.68 → 09:30 $0.67 -12.38; BTGO×189 yday $6.60 → 09:30 $6.95 +66.15; ASST×78 yday $16.13 → 09:30 $17.66 +119.34; ZLAB×47 yday $26.02 → 09:30 $26.25 +10.81; CRSP×21 yday $58.12 → 09:30 $59.72 +33.60; APA×27 yday $44.39 → 09:30 $44.52 +3.51 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 8 | $2.47 | $0.22 | — | $130.87 | — | combo gate; gate news=good,vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $21.55 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 1 | $11.70 | $0.12 | — | $119.05 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $21.55 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 1 | $11.10 | $0.11 | — | $107.84 | — | combo gate; gate news=good,vol=good; list yday_gainer; ret5=+19.1; leftover $21.55 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 6 | $3.24 | $0.21 | — | $88.19 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ret5=+21.3; leftover $21.55 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $88.19 | ▲ close $10,063.19 vs 09:30 $10,032.67 (session +31.18) | 16:00 close · cash $88.19 · equity $10,063.19 vs 09:30 $10,032.67 (+30.52; session marks +31.18) · 12 name(s) marked open→close (per-name table). BHP×13 09:30 $95.72 → close $97.03 +17.03; MRNA×8 09:30 $133.11 → close $145.13 +96.16; HUMA×1768 09:30 $0.67 → close $0.64 -56.58; BTGO×189 09:30 $6.95 → close $6.84 -20.79; ASST×78 09:30 $17.66 → close $18.22 +43.68; ZLAB×47 09:30 $26.25 → close $26.01 -11.28; CRSP×21 09:30 $59.72 → close $59.50 -4.62; APA×27 09:30 $44.52 → close $43.39 -30.51; AUTL×8 09:30 $2.47 → close $2.41 -0.48; MARA×1 09:30 $11.70 → close $11.26 -0.44; BTDR×1 09:30 $11.10 → close $11.37 +0.27; HIVE×6 09:30 $3.24 → close $3.03 -1.26 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $88.19 | ▲ 09:30 equity $10,115.02 vs yday $10,063.19 (+51.83) | 09:30 open · cash $88.19 (unchanged overnight, no fees) · equity $10,115.02 vs prior close $10,063.19 (+51.83) · 12 name(s) re-marked at the open (per-name table). BHP×13 yday $97.03 → 09:30 $97.34 +4.03; MRNA×8 yday $145.13 → 09:30 $142.70 -19.44; HUMA×1768 yday $0.64 → 09:30 $0.68 +67.18; BTGO×189 yday $6.84 → 09:30 $6.87 +5.67; ASST×78 yday $18.22 → 09:30 $18.76 +42.12; ZLAB×47 yday $26.01 → 09:30 $25.59 -19.74; CRSP×21 yday $59.50 → 09:30 $58.79 -14.91; APA×27 yday $43.39 → 09:30 $42.93 -12.42; AUTL×8 yday $2.41 → 09:30 $2.36 -0.40; MARA×1 yday $11.26 → 09:30 $11.18 -0.08; BTDR×1 yday $11.37 → 09:30 $11.49 +0.12; HIVE×6 yday $3.03 → 09:30 $2.98 -0.30 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $88.19 | ▼ close $10,096.98 vs 09:30 $10,115.02 (session -18.04) | 16:00 close · cash $88.19 · equity $10,096.98 vs 09:30 $10,115.02 (-18.04; session marks -18.04) · 12 name(s) marked open→close (per-name table). BHP×13 09:30 $97.34 → close $96.66 -8.84; MRNA×8 09:30 $142.70 → close $139.27 -27.44; HUMA×1768 09:30 $0.68 → close $0.67 -17.68; BTGO×189 09:30 $6.87 → close $6.97 +18.90; ASST×78 09:30 $18.76 → close $19.82 +82.68; ZLAB×47 09:30 $25.59 → close $25.51 -3.76; CRSP×21 09:30 $58.79 → close $56.91 -39.48; APA×27 09:30 $42.93 → close $42.10 -22.41; AUTL×8 09:30 $2.36 → close $2.38 +0.16; MARA×1 09:30 $11.18 → close $11.44 +0.26; BTDR×1 09:30 $11.49 → close $11.30 -0.19; HIVE×6 09:30 $2.98 → close $2.94 -0.24 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $88.19 | ▲ 09:30 equity $10,208.59 vs yday $10,096.98 (+111.61) | 09:30 open · cash $88.19 (unchanged overnight, no fees) · equity $10,208.59 vs prior close $10,096.98 (+111.61) · 12 name(s) re-marked at the open (per-name table). BHP×13 yday $96.66 → 09:30 $95.95 -9.23; MRNA×8 yday $139.27 → 09:30 $141.19 +15.36; HUMA×1768 yday $0.67 → 09:30 $0.67 +0.00; BTGO×189 yday $6.97 → 09:30 $6.89 -15.12; ASST×78 yday $19.82 → 09:30 $20.90 +84.24; ZLAB×47 yday $25.51 → 09:30 $25.93 +19.74; CRSP×21 yday $56.91 → 09:30 $57.00 +1.89; APA×27 yday $42.10 → 09:30 $42.70 +16.20; AUTL×8 yday $2.38 → 09:30 $2.32 -0.48; MARA×1 yday $11.44 → 09:30 $11.28 -0.16; BTDR×1 yday $11.30 → 09:30 $11.19 -0.11; HIVE×6 yday $2.94 → 09:30 $2.82 -0.72 | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.95 | $2.05 | $+60.14 | $1,333.49 | ▲ +60.14 after sell → book $10,206.54; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MRNA` | 8 | $141.19 | $2.03 | $-75.65 | $2,460.98 | ▼ -75.65 after sell → book $10,204.51; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HUMA` | 1768 | $0.67 | $17.45 | $-100.67 | $3,628.08 | ▼ -100.67 after sell → book $10,187.05; vs 09:30 mark -17.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BTGO` | 189 | $6.89 | $2.60 | $+48.71 | $4,927.70 | ▲ +48.71 after sell → book $10,184.46; vs 09:30 mark -2.59 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ASST` | 78 | $20.90 | $2.25 | $+377.73 | $6,555.65 | ▲ +377.73 after sell → book $10,182.21; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `ZLAB` | 47 | $25.93 | $2.15 | $-34.36 | $7,772.20 | ▼ -34.36 after sell → book $10,180.05; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 21 | $57.00 | $2.07 | $-40.46 | $8,967.13 | ▼ -40.46 after sell → book $10,177.98; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `APA` | 27 | $42.70 | $2.09 | $-59.78 | $10,117.94 | ▼ -59.78 after sell → book $10,175.89; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 154 | $9.36 | $2.45 | — | $8,674.05 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ret5=+21.3; leftover $1445.42 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 41 | $34.48 | $2.11 | — | $7,258.26 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1445.42 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 60 | $24.00 | $2.17 | — | $5,816.09 | — | combo gate; gate news=good,vol=good; list yday_mover; ret5=+10.0; leftover $1445.42 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BKKT` | 174 | $8.28 | $2.51 | — | $4,372.85 | — | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ret5=+12.3; leftover $1445.42 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 18 | $77.90 | $2.04 | — | $2,968.61 | — | combo gate; gate news=good,vol=good; list ohlc_hot,mover_buy; ⚪; ret5=+15.3; leftover $1445.42 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NVAX` | 162 | $8.88 | $2.48 | — | $1,527.57 | — | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ret5=+11.1; leftover $1445.42 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 12 | $119.46 | $2.03 | — | $92.03 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ⚪; ret5=+25.9; leftover $1445.42 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $92.03 | ▲ close $10,175.05 vs 09:30 $10,208.59 (session +14.95) | 16:00 close · cash $92.03 · equity $10,175.05 vs 09:30 $10,208.59 (-33.54; session marks +14.95) · 11 name(s) marked open→close (per-name table). AUTL×8 09:30 $2.32 → close $2.34 +0.16; MARA×1 09:30 $11.28 → close $11.29 +0.01; BTDR×1 09:30 $11.19 → close $11.28 +0.09; HIVE×6 09:30 $2.82 → close $2.89 +0.42; RUM×154 09:30 $9.36 → close $9.35 -1.54; EZPW×41 09:30 $34.48 → close $34.69 +8.61; REAX×60 09:30 $24.00 → close $24.00 +0.00; BKKT×174 09:30 $8.28 → close $8.38 +17.40; FCX×18 09:30 $77.90 → close $77.49 -7.38; NVAX×162 09:30 $8.88 → close $8.93 +8.10; AU×12 09:30 $119.46 → close $118.55 -10.92 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $92.03 | ▲ 09:30 equity $10,175.05 vs yday $10,175.05 (-0.00) | 09:30 open · cash $92.03 (unchanged overnight, no fees) · equity $10,175.05 vs prior close $10,175.05 (-0.00) · 11 name(s) re-marked at the open (per-name table). AUTL×8 yday $2.34 → 09:30 $2.34 +0.00; MARA×1 yday $11.29 → 09:30 $11.29 +0.00; BTDR×1 yday $11.28 → 09:30 $11.28 +0.00; HIVE×6 yday $2.89 → 09:30 $2.89 +0.00; RUM×154 yday $9.35 → 09:30 $9.35 +0.00; EZPW×41 yday $34.69 → 09:30 $34.69 +0.00; REAX×60 yday $24.00 → 09:30 $24.00 +0.00; BKKT×174 yday $8.38 → 09:30 $8.38 +0.00; FCX×18 yday $77.49 → 09:30 $77.49 +0.00; NVAX×162 yday $8.93 → 09:30 $8.93 +0.00; AU×12 yday $118.55 → 09:30 $118.55 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $92.03 | ▲ close $10,175.05 vs 09:30 $10,175.05 (session +0.00) | 16:00 close · cash $92.03 · equity $10,175.05 vs 09:30 $10,175.05 (-0.00; session marks +0.00) · 11 name(s) marked open→close (per-name table). AUTL×8 09:30 $2.34 → close $2.34 +0.00; MARA×1 09:30 $11.29 → close $11.29 +0.00; BTDR×1 09:30 $11.28 → close $11.28 +0.00; HIVE×6 09:30 $2.89 → close $2.89 +0.00; RUM×154 09:30 $9.35 → close $9.35 +0.00; EZPW×41 09:30 $34.69 → close $34.69 +0.00; REAX×60 09:30 $24.00 → close $24.00 +0.00; BKKT×174 09:30 $8.38 → close $8.38 +0.00; FCX×18 09:30 $77.49 → close $77.49 +0.00; NVAX×162 09:30 $8.93 → close $8.93 +0.00; AU×12 09:30 $118.55 → close $118.55 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $92.03 | ▲ 09:30 equity $10,598.00 vs yday $10,175.05 (+422.95) | 09:30 open · cash $92.03 (unchanged overnight, no fees) · equity $10,598.00 vs prior close $10,175.05 (+422.95) · 11 name(s) re-marked at the open (per-name table). AUTL×8 yday $2.34 → 09:30 $2.41 +0.56; MARA×1 yday $11.29 → 09:30 $11.56 +0.27; BTDR×1 yday $11.28 → 09:30 $11.05 -0.23; HIVE×6 yday $2.89 → 09:30 $2.95 +0.36; RUM×154 yday $9.35 → 09:30 $10.07 +110.88; EZPW×41 yday $34.69 → 09:30 $35.70 +41.41; REAX×60 yday $24.00 → 09:30 $26.61 +156.60; BKKT×174 yday $8.38 → 09:30 $8.38 +0.00; FCX×18 yday $77.49 → 09:30 $79.34 +33.30; NVAX×162 yday $8.93 → 09:30 $9.33 +64.80; AU×12 yday $118.55 → 09:30 $119.80 +15.00 | — |
| 2026-08-27 09:30 ET | **SELL** | `AUTL` | 8 | $2.41 | $0.24 | $-0.94 | $111.07 | ▼ -0.94 after sell → book $10,597.76; vs 09:30 mark -0.24 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `MARA` | 1 | $11.56 | $0.14 | $-0.40 | $122.49 | ▼ -0.40 after sell → book $10,597.62; vs 09:30 mark -0.14 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `BTDR` | 1 | $11.05 | $0.13 | $-0.29 | $133.41 | ▼ -0.29 after sell → book $10,597.49; vs 09:30 mark -0.13 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `HIVE` | 6 | $2.95 | $0.21 | $-2.17 | $150.89 | ▼ -2.17 after sell → book $10,597.27; vs 09:30 mark -0.22 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $150.89 | ▼ close $10,344.07 vs 09:30 $10,598.00 (session -253.20) | 16:00 close · cash $150.89 · equity $10,344.07 vs 09:30 $10,598.00 (-253.93; session marks -253.20) · 7 name(s) marked open→close (per-name table). RUM×154 09:30 $10.07 → close $9.38 -106.26; EZPW×41 09:30 $35.70 → close $33.90 -73.80; REAX×60 09:30 $26.61 → close $26.59 -1.20; BKKT×174 09:30 $8.38 → close $8.23 -26.10; FCX×18 09:30 $79.34 → close $79.00 -6.12; NVAX×162 09:30 $9.33 → close $9.21 -19.44; AU×12 09:30 $119.80 → close $118.11 -20.28 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $150.89 | ▼ 09:30 equity $10,327.83 vs yday $10,344.07 (-16.24) | 09:30 open · cash $150.89 (unchanged overnight, no fees) · equity $10,327.83 vs prior close $10,344.07 (-16.24) · 7 name(s) re-marked at the open (per-name table). RUM×154 yday $9.38 → 09:30 $9.51 +20.02; EZPW×41 yday $33.90 → 09:30 $33.50 -16.40; REAX×60 yday $26.59 → 09:30 $25.91 -40.80; BKKT×174 yday $8.23 → 09:30 $8.50 +46.98; FCX×18 yday $79.00 → 09:30 $78.83 -3.06; NVAX×162 yday $9.21 → 09:30 $9.12 -14.58; AU×12 yday $118.11 → 09:30 $117.41 -8.40 | — |
| 2026-08-28 09:30 ET | **SELL** | `RUM` | 154 | $9.51 | $2.49 | $+18.16 | $1,612.94 | ▲ +18.16 after sell → book $10,325.34; vs 09:30 mark -2.49 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EZPW` | 41 | $33.50 | $2.13 | $-44.43 | $2,984.31 | ▼ -44.43 after sell → book $10,323.21; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `REAX` | 60 | $25.91 | $2.19 | $+110.24 | $4,536.72 | ▲ +110.24 after sell → book $10,321.02; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BKKT` | 174 | $8.50 | $2.55 | $+33.22 | $6,013.17 | ▲ +33.22 after sell → book $10,318.47; vs 09:30 mark -2.55 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 18 | $78.83 | $2.07 | $+12.63 | $7,430.04 | ▲ +12.63 after sell → book $10,316.40; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 catal🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `NVAX` | 162 | $9.12 | $2.51 | $+33.89 | $8,904.96 | ▲ +33.89 after sell → book $10,313.88; vs 09:30 mark -2.52 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 12 | $117.41 | $2.05 | $-28.67 | $10,311.84 | ▼ -28.67 after sell → book $10,311.84; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 187 | $9.19 | $2.55 | — | $8,590.76 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1718.64 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 50 | $33.78 | $2.14 | — | $6,899.62 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1718.64 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 11 | $149.40 | $2.02 | — | $5,254.19 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1718.64 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 89 | $19.30 | $2.26 | — | $3,534.24 | — | combo gate; gate news=good,vol=good; list yday_gainer; ret5=-4.1; leftover $1718.64 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 92 | $18.68 | $2.27 | — | $1,813.41 | — | combo gate; gate news=good,vol=good; list yday_gainer; ret5=+0.2; leftover $1718.64 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 58 | $29.33 | $2.16 | — | $110.11 | — | combo gate; gate news=good,vol=good; list yday_gainer,ohlc_hot; ret5=+14.1; leftover $1718.64 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $110.11 | ▲ close $10,366.55 vs 09:30 $10,327.83 (session +68.11) | 16:00 close · cash $110.11 · equity $10,366.55 vs 09:30 $10,327.83 (+38.72; session marks +68.11) · 6 name(s) marked open→close (per-name table). CAPR×187 09:30 $9.19 → close $10.06 +162.69; SEDG×50 09:30 $33.78 → close $33.51 -13.50; SMTC×11 09:30 $149.40 → close $142.43 -76.67; ERAS×89 09:30 $19.30 → close $19.49 +16.91; BBWI×92 09:30 $18.68 → close $18.65 -2.76; ZYME×58 09:30 $29.33 → close $29.01 -18.56 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $110.11 | ▼ 09:30 equity $9,922.19 vs yday $10,366.55 (-444.36) | 09:30 open · cash $110.11 (unchanged overnight, no fees) · equity $9,922.19 vs prior close $10,366.55 (-444.36) · 6 name(s) re-marked at the open (per-name table). CAPR×187 yday $10.06 → 09:30 $9.44 -115.94; SEDG×50 yday $33.51 → 09:30 $31.50 -100.50; SMTC×11 yday $142.43 → 09:30 $133.04 -103.29; ERAS×89 yday $19.49 → 09:30 $17.90 -141.51; BBWI×92 yday $18.65 → 09:30 $19.30 +59.80; ZYME×58 yday $29.01 → 09:30 $28.27 -42.92 | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $110.11 | ▼ close $9,882.87 vs 09:30 $9,922.19 (session -39.32) | 16:00 close · cash $110.11 · equity $9,882.87 vs 09:30 $9,922.19 (-39.32; session marks -39.32) · 6 name(s) marked open→close (per-name table). CAPR×187 09:30 $9.44 → close $9.36 -14.96; SEDG×50 09:30 $31.50 → close $31.27 -11.50; SMTC×11 09:30 $133.04 → close $132.54 -5.50; ERAS×89 09:30 $17.90 → close $17.90 +0.00; BBWI×92 09:30 $19.30 → close $19.22 -7.36; ZYME×58 09:30 $28.27 → close $28.27 +0.00 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $110.11 | ▲ 09:30 equity $10,179.43 vs yday $9,882.87 (+296.56) | 09:30 open · cash $110.11 (unchanged overnight, no fees) · equity $10,179.43 vs prior close $9,882.87 (+296.56) · 6 name(s) re-marked at the open (per-name table). CAPR×187 yday $9.36 → 09:30 $10.43 +200.09; SEDG×50 yday $31.27 → 09:30 $32.22 +47.50; SMTC×11 yday $132.54 → 09:30 $131.65 -9.79; ERAS×89 yday $17.90 → 09:30 $18.00 +8.90; BBWI×92 yday $19.22 → 09:30 $19.10 -11.04; ZYME×58 yday $28.27 → 09:30 $29.32 +60.90 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $110.11 | ▼ close $10,063.78 vs 09:30 $10,179.43 (session -115.65) | 16:00 close · cash $110.11 · equity $10,063.78 vs 09:30 $10,179.43 (-115.65; session marks -115.65) · 6 name(s) marked open→close (per-name table). CAPR×187 09:30 $10.43 → close $10.19 -44.88; SEDG×50 09:30 $32.22 → close $31.80 -21.00; SMTC×11 09:30 $131.65 → close $129.50 -23.65; ERAS×89 09:30 $18.00 → close $17.70 -26.70; BBWI×92 09:30 $19.10 → close $19.10 +0.00; ZYME×58 09:30 $29.32 → close $29.33 +0.58 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $110.11 | ▲ 09:30 equity $10,113.55 vs yday $10,063.78 (+49.77) | 09:30 open · cash $110.11 (unchanged overnight, no fees) · equity $10,113.55 vs prior close $10,063.78 (+49.77) · 6 name(s) re-marked at the open (per-name table). CAPR×187 yday $10.19 → 09:30 $10.77 +108.46; SEDG×50 yday $31.80 → 09:30 $31.87 +3.50; SMTC×11 yday $129.50 → 09:30 $127.63 -20.57; ERAS×89 yday $17.70 → 09:30 $17.58 -10.68; BBWI×92 yday $19.10 → 09:30 $18.77 -30.36; ZYME×58 yday $29.33 → 09:30 $29.32 -0.58 | — |
| 2026-09-02 09:30 ET | **SELL** | `CAPR` | 187 | $10.77 | $2.60 | $+290.31 | $2,121.50 | ▲ +290.31 after sell → book $10,110.95; vs 09:30 mark -2.60 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 50 | $31.87 | $2.16 | $-99.80 | $3,712.84 | ▼ -99.80 after sell → book $10,108.79; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 11 | $127.63 | $2.04 | $-243.54 | $5,114.72 | ▼ -243.54 after sell → book $10,106.74; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ERAS` | 89 | $17.58 | $2.28 | $-157.62 | $6,677.06 | ▼ -157.62 after sell → book $10,104.46; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBWI` | 92 | $18.77 | $2.30 | $+3.72 | $8,401.60 | ▲ +3.72 after sell → book $10,102.16; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ZYME` | 58 | $29.32 | $2.19 | $-4.93 | $10,099.97 | ▼ -4.93 after sell → book $10,099.97; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,099.97 | ▲ close $10,099.97 vs 09:30 $10,113.55 (session +0.00) | 16:00 close · cash $10,099.97 · no lots left · equity $10,099.97. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,099.97 | ▲ 09:30 equity $10,099.97 vs yday $10,099.97 (+0.00) | 09:30 open · cash $10,099.97 · no holdings · equity $10,099.97 vs prior close $10,099.97 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 110 | $22.78 | $2.32 | — | $7,591.85 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $2524.99 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 5 | $462.05 | $2.00 | — | $5,279.60 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ret5=-9.9; leftover $2524.99 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 165 | $15.24 | $2.48 | — | $2,762.51 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ⚪; ret5=+19.5; leftover $2524.99 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 76 | $32.87 | $2.22 | — | $262.18 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ⚪; ret5=+3.6; leftover $2524.99 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $262.18 | ▲ close $10,446.89 vs 09:30 $10,099.97 (session +355.94) | 16:00 close · cash $262.18 · equity $10,446.89 vs 09:30 $10,099.97 (+346.92; session marks +355.94) · 4 name(s) marked open→close (per-name table). MMED×110 09:30 $22.78 → close $23.76 +107.80; DELL×5 09:30 $462.05 → close $492.20 +150.75; FRNM×165 09:30 $15.24 → close $15.95 +117.15; CXW×76 09:30 $32.87 → close $32.61 -19.76 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $262.18 | ▼ 09:30 equity $10,394.64 vs yday $10,446.89 (-52.25) | 09:30 open · cash $262.18 (unchanged overnight, no fees) · equity $10,394.64 vs prior close $10,446.89 (-52.25) · 4 name(s) re-marked at the open (per-name table). MMED×110 yday $23.76 → 09:30 $23.88 +13.20; DELL×5 yday $492.20 → 09:30 $486.31 -29.45; FRNM×165 yday $15.95 → 09:30 $15.87 -13.20; CXW×76 yday $32.61 → 09:30 $32.31 -22.80 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 133 | $1.95 | $2.39 | — | $0.44 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $262.18 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.44 | ▲ close $10,809.47 vs 09:30 $10,394.64 (session +417.22) | 16:00 close · cash $0.44 · equity $10,809.47 vs 09:30 $10,394.64 (+414.83; session marks +417.22) · 5 name(s) marked open→close (per-name table). MMED×110 09:30 $23.88 → close $23.84 -4.40; DELL×5 09:30 $486.31 → close $516.39 +150.40; FRNM×165 09:30 $15.87 → close $16.90 +169.95; CXW×76 09:30 $32.31 → close $33.66 +102.60; BAK×133 09:30 $1.95 → close $1.94 -1.33 | — |
| 2026-09-07 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.44 | ▼ 09:30 equity $10,698.72 vs yday $10,809.47 (-110.75) | 09:30 open · cash $0.44 (unchanged overnight, no fees) · equity $10,698.72 vs prior close $10,809.47 (-110.75) · 5 name(s) re-marked at the open (per-name table). MMED×110 yday $23.84 → 09:30 $23.84 +0.00; DELL×5 yday $516.39 → 09:30 $513.78 -13.05; FRNM×165 yday $16.90 → 09:30 $16.40 -82.50; CXW×76 yday $33.66 → 09:30 $33.46 -15.20; BAK×133 yday $1.94 → 09:30 $1.94 +0.00 | — |
| 2026-09-07 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.44 | ▲ close $10,762.42 vs 09:30 $10,698.72 (session +63.70) | 16:00 close · cash $0.44 · equity $10,762.42 vs 09:30 $10,698.72 (+63.70; session marks +63.70) · 5 name(s) marked open→close (per-name table). MMED×110 09:30 $23.84 → close $23.28 -61.60; DELL×5 09:30 $513.78 → close $524.14 +51.80; FRNM×165 09:30 $16.40 → close $16.31 -14.85; CXW×76 09:30 $33.46 → close $34.71 +95.00; BAK×133 09:30 $1.94 → close $1.89 -6.65 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.44 | ▼ 09:30 equity $10,724.42 vs yday $10,762.42 (-38.00) | 09:30 open · cash $0.44 (unchanged overnight, no fees) · equity $10,724.42 vs prior close $10,762.42 (-38.00) · 5 name(s) re-marked at the open (per-name table). MMED×110 yday $23.28 → 09:30 $23.00 -30.80; DELL×5 yday $524.14 → 09:30 $519.47 -23.35; FRNM×165 yday $16.31 → 09:30 $16.31 +0.00; CXW×76 yday $34.71 → 09:30 $34.80 +6.84; BAK×133 yday $1.89 → 09:30 $1.96 +9.31 | — |
| 2026-09-08 09:30 ET | **SELL** | `MMED` | 110 | $23.00 | $2.36 | $+19.52 | $2,528.08 | ▲ +19.52 after sell → book $10,722.06; vs 09:30 mark -2.36 | dropped from list after 3 sess (min 3) | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 5 | $519.47 | $2.04 | $+283.06 | $5,123.39 | ▲ +283.06 after sell → book $10,720.02; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 165 | $16.31 | $2.53 | $+171.53 | $7,812.01 | ▲ +171.53 after sell → book $10,717.49; vs 09:30 mark -2.53 | dropped from list after 3 sess (min 3) | — |
| 2026-09-08 09:30 ET | **SELL** | `CXW` | 76 | $34.80 | $2.25 | $+142.21 | $10,454.56 | ▲ +142.21 after sell → book $10,715.24; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,454.56 | ▲ close $10,715.24 vs 09:30 $10,724.42 (session +0.00) | 16:00 close · cash $10,454.56 · equity $10,715.24 vs 09:30 $10,724.42 (-9.18; session marks +0.00) · 1 name(s) marked open→close (per-name table). BAK×133 09:30 $1.96 → close $1.96 +0.00 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `SNDK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `MH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HLIT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `SNDK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `MH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HLIT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AUTL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HUMA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BTGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ZLAB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `APA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 21.55 < 1 share @ 119.43 |
| 2026-08-21 | `FUTU` | cash | leftover split 21.55 < 1 share @ 115.18 |
| 2026-08-21 | `DE` | cash | leftover split 21.55 < 1 share @ 623.26 |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HUMA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BTGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ZLAB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `APA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `BTDR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `HIVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `BTDR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `HIVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AUTL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `MARA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BTDR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `HIVE` | no_price | no 09:30 open — carry |
| 2026-08-26 | `RUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EZPW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `REAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BKKT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `NVAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `RUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EZPW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `REAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BKKT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `NVAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ERAS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BBWI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ZYME` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ERAS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BBWI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ZYME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CXW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-07 | `DELL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-07 | `CXW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-07 | `BAK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-07 | `CHPT` | cash | leftover split 0.09 < 1 share @ 9.28 |
| 2026-09-07 | `SMMT` | cash | leftover split 0.09 < 1 share @ 16.93 |
| 2026-09-07 | `SNOW` | cash | leftover split 0.09 < 1 share @ 353.63 |
| 2026-09-07 | `MSTR` | cash | leftover split 0.09 < 1 share @ 137.35 |
| 2026-09-07 | `MRX` | cash | leftover split 0.09 < 1 share @ 75.65 |
| 2026-09-08 | `BAK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `AIRS` | hard_red | hard-red S=-11.47 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `BAK` | 133 | 2026-09-04 @ $1.95 | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $262.18 |
