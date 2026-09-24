# Factor mine action — `union_hot_n4_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `hot_score` · size `leftover` · sell `list` · S-boost `none` · top 4 by hot

Cash book **+62.80%** ($16,280) · signal-only (no cash/fees) was +89.81%. Starts YES **29/30**. Fills 111 · skips 47 · realized $+2978.17.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written and skip anything on the must-not list. They take up to 4 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Sort the keepers by how hot the prior tape looked and keep the top 4.
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
- **Gate** `none (list as ranked)` · **rank** `hot_score` · **top_n** 4.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,581.64.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `IREN` | 54 | — | $45.98 | +0.00 | $44.76 | -65.88 | -65.88 | +0.00 | -65.88 |
| 2026-08-13 | `TNDM` | 107 | — | $23.33 | +0.00 | $23.13 | -21.40 | -21.40 | +0.00 | -21.40 |
| 2026-08-13 | `TPG` | 49 | — | $50.62 | +0.00 | $54.62 | +195.84 | +195.84 | +0.00 | +195.84 |
| 2026-08-13 | `INO` | 3085 | — | $0.81 | +0.00 | $0.90 | +277.65 | +277.65 | +0.00 | +277.65 |
| 2026-08-14 | `IREN` | 54 | $44.76 | $44.09 | -36.18 | — | +0.00 | -36.18 | -102.06 | — |
| 2026-08-14 | `TNDM` | 107 | $23.13 | $22.92 | -22.47 | — | +0.00 | -22.47 | -43.87 | — |
| 2026-08-14 | `TPG` | 49 | $54.62 | $55.29 | +32.83 | — | +0.00 | +32.83 | +228.67 | — |
| 2026-08-14 | `INO` | 3085 | $0.90 | $0.93 | +92.55 | — | +0.00 | +92.55 | +370.20 | — |
| 2026-08-14 | `QMCO` | 105 | — | $24.68 | +0.00 | $26.11 | +150.15 | +150.15 | +0.00 | +150.15 |
| 2026-08-14 | `ARX` | 132 | — | $19.57 | +0.00 | $19.58 | +1.32 | +1.32 | +0.00 | +1.32 |
| 2026-08-14 | `ZENA` | 1178 | — | $2.20 | +0.00 | $2.14 | -70.68 | -70.68 | +0.00 | -70.68 |
| 2026-08-14 | `AIRO` | 231 | — | $11.12 | +0.00 | $9.57 | -358.05 | -358.05 | +0.00 | -358.05 |
| 2026-08-17 | `QMCO` | 105 | $26.11 | $24.83 | -134.40 | — | +0.00 | -134.40 | +15.75 | — |
| 2026-08-17 | `ARX` | 132 | $19.58 | $19.57 | -1.32 | — | +0.00 | -1.32 | +0.00 | — |
| 2026-08-17 | `ZENA` | 1178 | $2.14 | $2.08 | -64.79 | — | +0.00 | -64.79 | -135.47 | — |
| 2026-08-17 | `AIRO` | 231 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -358.05 | — |
| 2026-08-17 | `XHG` | 587 | — | $4.19 | +0.00 | $3.91 | -164.36 | -164.36 | +0.00 | -164.36 |
| 2026-08-17 | `CAPR` | 358 | — | $6.87 | +0.00 | $7.45 | +207.64 | +207.64 | +0.00 | +207.64 |
| 2026-08-17 | `STDN` | 180 | — | $13.64 | +0.00 | $13.31 | -59.40 | -59.40 | +0.00 | -59.40 |
| 2026-08-17 | `HTFL` | 59 | — | $41.23 | +0.00 | $41.94 | +41.89 | +41.89 | +0.00 | +41.89 |
| 2026-08-18 | `XHG` | 587 | $3.91 | $3.94 | +17.61 | — | +0.00 | +17.61 | -146.75 | — |
| 2026-08-18 | `CAPR` | 358 | $7.45 | $7.50 | +17.90 | $7.08 | -150.36 | -132.46 | +225.54 | +75.18 |
| 2026-08-18 | `STDN` | 180 | $13.31 | $13.31 | +0.00 | — | +0.00 | +0.00 | -59.40 | — |
| 2026-08-18 | `HTFL` | 59 | $41.94 | $41.50 | -25.96 | — | +0.00 | -25.96 | +15.93 | — |
| 2026-08-19 | `CAPR` | 358 | $7.08 | $7.19 | +39.38 | — | +0.00 | +39.38 | +114.56 | — |
| 2026-08-20 | `MRNA` | 16 | — | $150.14 | +0.00 | $133.32 | -269.12 | -269.12 | +0.00 | -269.12 |
| 2026-08-20 | `CYPH` | 2115 | — | $1.15 | +0.00 | $1.19 | +84.60 | +84.60 | +0.00 | +84.60 |
| 2026-08-20 | `ABCL` | 205 | — | $11.81 | +0.00 | $11.57 | -50.22 | -50.22 | +0.00 | -50.22 |
| 2026-08-20 | `AZI` | 1767 | — | $1.37 | +0.00 | $1.44 | +123.69 | +123.69 | +0.00 | +123.69 |
| 2026-08-21 | `MRNA` | 16 | $133.32 | $133.11 | -3.36 | $145.13 | +192.32 | +188.96 | -272.48 | -80.16 |
| 2026-08-21 | `CYPH` | 2115 | $1.19 | $1.32 | +274.95 | $1.42 | +211.50 | +486.45 | +359.55 | +571.05 |
| 2026-08-21 | `ABCL` | 205 | $11.57 | $11.57 | +0.00 | — | +0.00 | +0.00 | -50.22 | — |
| 2026-08-21 | `AZI` | 1767 | $1.44 | $1.46 | +35.34 | — | +0.00 | +35.34 | +159.03 | — |
| 2026-08-21 | `XHG` | 548 | — | $4.49 | +0.00 | $4.41 | -43.84 | -43.84 | +0.00 | -43.84 |
| 2026-08-21 | `CAPR` | 360 | — | $6.81 | +0.00 | $6.29 | -187.20 | -187.20 | +0.00 | -187.20 |
| 2026-08-24 | `MRNA` | 16 | $145.13 | $142.70 | -38.88 | — | +0.00 | -38.88 | -119.04 | — |
| 2026-08-24 | `CYPH` | 2115 | $1.42 | $1.83 | +867.15 | — | +0.00 | +867.15 | +1438.20 | — |
| 2026-08-24 | `XHG` | 548 | $4.41 | $4.32 | -49.32 | — | +0.00 | -49.32 | -93.16 | — |
| 2026-08-24 | `CAPR` | 360 | $6.29 | $8.03 | +626.40 | — | +0.00 | +626.40 | +439.20 | — |
| 2026-08-25 | `REAX` | 117 | — | $24.11 | +0.00 | $28.43 | +505.44 | +505.44 | +0.00 | +505.44 |
| 2026-08-25 | `CYPH` | 1822 | — | $1.56 | +0.00 | $1.64 | +145.76 | +145.76 | +0.00 | +145.76 |
| 2026-08-25 | `XHG` | 698 | — | $4.07 | +0.00 | $4.02 | -34.90 | -34.90 | +0.00 | -34.90 |
| 2026-08-25 | `ASST` | 148 | — | $19.04 | +0.00 | $21.39 | +347.80 | +347.80 | +0.00 | +347.80 |
| 2026-08-26 | `REAX` | 117 | $28.43 | $26.61 | -212.94 | — | +0.00 | -212.94 | +292.50 | — |
| 2026-08-26 | `CYPH` | 1822 | $1.64 | $1.60 | -72.88 | — | +0.00 | -72.88 | +72.88 | — |
| 2026-08-26 | `XHG` | 698 | $4.02 | $3.81 | -146.58 | $4.06 | +174.50 | +27.92 | -181.48 | -6.98 |
| 2026-08-26 | `ASST` | 148 | $21.39 | $20.72 | -99.16 | — | +0.00 | -99.16 | +248.64 | — |
| 2026-08-26 | `BYND` | 214 | — | $14.11 | +0.00 | $14.25 | +29.96 | +29.96 | +0.00 | +29.96 |
| 2026-08-26 | `USDE` | 520 | — | $5.81 | +0.00 | $5.98 | +88.40 | +88.40 | +0.00 | +88.40 |
| 2026-08-26 | `PURR` | 261 | — | $11.59 | +0.00 | $11.56 | -6.53 | -6.53 | +0.00 | -6.53 |
| 2026-08-27 | `XHG` | 698 | $4.06 | $4.06 | +0.00 | $3.80 | -181.48 | -181.48 | -6.98 | -188.46 |
| 2026-08-27 | `BYND` | 214 | $14.25 | $14.20 | -10.70 | — | +0.00 | -10.70 | +19.26 | — |
| 2026-08-27 | `USDE` | 520 | $5.98 | $6.50 | +270.40 | — | +0.00 | +270.40 | +358.80 | — |
| 2026-08-27 | `PURR` | 261 | $11.56 | $12.18 | +161.82 | — | +0.00 | +161.82 | +155.29 | — |
| 2026-08-27 | `CAPR` | 347 | — | $9.19 | +0.00 | $10.06 | +301.89 | +301.89 | +0.00 | +301.89 |
| 2026-08-27 | `MRNA` | 22 | — | $144.18 | +0.00 | $142.77 | -31.02 | -31.02 | +0.00 | -31.02 |
| 2026-08-27 | `BZ` | 172 | — | $18.50 | +0.00 | $18.00 | -86.00 | -86.00 | +0.00 | -86.00 |
| 2026-08-28 | `XHG` | 698 | $3.80 | $3.69 | -76.78 | — | +0.00 | -76.78 | -265.24 | — |
| 2026-08-28 | `CAPR` | 347 | $10.06 | $9.73 | -114.51 | $9.59 | -48.58 | -163.09 | +187.38 | +138.80 |
| 2026-08-28 | `MRNA` | 22 | $142.77 | $137.19 | -122.76 | $137.99 | +17.60 | -105.16 | -153.78 | -136.18 |
| 2026-08-28 | `BZ` | 172 | $18.00 | $18.15 | +25.80 | — | +0.00 | +25.80 | -60.20 | — |
| 2026-08-28 | `BYND` | 204 | — | $14.00 | +0.00 | $13.86 | -28.56 | -28.56 | +0.00 | -28.56 |
| 2026-08-28 | `ANF` | 19 | — | $146.07 | +0.00 | $148.42 | +44.65 | +44.65 | +0.00 | +44.65 |
| 2026-08-31 | `CAPR` | 347 | $9.59 | $9.50 | -31.23 | — | +0.00 | -31.23 | +107.57 | — |
| 2026-08-31 | `MRNA` | 22 | $137.99 | $134.10 | -85.58 | — | +0.00 | -85.58 | -221.76 | — |
| 2026-08-31 | `BYND` | 204 | $13.86 | $13.81 | -10.20 | $13.30 | -104.04 | -114.24 | -38.76 | -142.80 |
| 2026-08-31 | `ANF` | 19 | $148.42 | $148.03 | -7.41 | — | +0.00 | -7.41 | +37.24 | — |
| 2026-09-01 | `BYND` | 204 | $13.30 | $13.04 | -53.04 | — | +0.00 | -53.04 | -195.84 | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `GPRO` | 1656 | — | $1.78 | +0.00 | $1.39 | -645.84 | -645.84 | +0.00 | -645.84 |
| 2026-09-03 | `REAX` | 160 | — | $18.40 | +0.00 | $18.40 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `CNH` | 215 | — | $13.71 | +0.00 | $13.84 | +27.95 | +27.95 | +0.00 | +27.95 |
| 2026-09-03 | `MMED` | 122 | — | $23.88 | +0.00 | $23.84 | -4.88 | -4.88 | +0.00 | -4.88 |
| 2026-09-04 | `GPRO` | 1656 | $1.39 | $1.48 | +149.04 | $1.70 | +364.32 | +513.36 | -496.80 | -132.48 |
| 2026-09-04 | `REAX` | 160 | $18.40 | $18.15 | -40.00 | — | +0.00 | -40.00 | -40.00 | — |
| 2026-09-04 | `CNH` | 215 | $13.84 | $13.89 | +10.75 | — | +0.00 | +10.75 | +38.70 | — |
| 2026-09-04 | `MMED` | 122 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -4.88 | — |
| 2026-09-04 | `ASST` | 116 | — | $25.18 | +0.00 | $27.14 | +227.36 | +227.36 | +0.00 | +227.36 |
| 2026-09-04 | `USDE` | 372 | — | $7.87 | +0.00 | $7.93 | +22.32 | +22.32 | +0.00 | +22.32 |
| 2026-09-04 | `DFDV` | 506 | — | $5.79 | +0.00 | $5.87 | +40.48 | +40.48 | +0.00 | +40.48 |
| 2026-09-08 | `GPRO` | 1656 | $1.70 | $1.56 | -223.56 | — | +0.00 | -223.56 | -356.04 | — |
| 2026-09-08 | `ASST` | 116 | $27.14 | $26.44 | -81.20 | — | +0.00 | -81.20 | +146.16 | — |
| 2026-09-08 | `USDE` | 372 | $7.93 | $7.76 | -63.24 | — | +0.00 | -63.24 | -40.92 | — |
| 2026-09-08 | `DFDV` | 506 | $5.87 | $5.81 | -30.36 | — | +0.00 | -30.36 | +10.12 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `INDP` | 1061 | — | $2.70 | +0.00 | $2.77 | +74.27 | +74.27 | +0.00 | +74.27 |
| 2026-09-11 | `BNC` | 583 | — | $4.91 | +0.00 | $4.80 | -64.13 | -64.13 | +0.00 | -64.13 |
| 2026-09-11 | `IRD` | 465 | — | $6.16 | +0.00 | $6.04 | -55.80 | -55.80 | +0.00 | -55.80 |
| 2026-09-11 | `CMRC` | 904 | — | $3.13 | +0.00 | $3.50 | +339.00 | +339.00 | +0.00 | +339.00 |
| 2026-09-14 | `INDP` | 1061 | $2.77 | $2.80 | +31.83 | $3.14 | +360.74 | +392.57 | +106.10 | +466.84 |
| 2026-09-14 | `BNC` | 583 | $4.80 | $5.03 | +134.09 | $5.27 | +139.92 | +274.01 | +69.96 | +209.88 |
| 2026-09-14 | `IRD` | 465 | $6.04 | $6.02 | -9.30 | — | +0.00 | -9.30 | -65.10 | — |
| 2026-09-14 | `CMRC` | 904 | $3.50 | $3.51 | +4.52 | $3.64 | +117.52 | +122.04 | +343.52 | +461.04 |
| 2026-09-15 | `INDP` | 1061 | $3.14 | $3.40 | +275.86 | $3.64 | +254.64 | +530.50 | +742.70 | +997.34 |
| 2026-09-15 | `BNC` | 583 | $5.27 | $5.11 | -93.28 | — | +0.00 | -93.28 | +116.60 | — |
| 2026-09-15 | `CMRC` | 904 | $3.64 | $3.64 | +0.00 | — | +0.00 | +0.00 | +461.04 | — |
| 2026-09-16 | `INDP` | 1061 | $3.64 | $3.66 | +21.22 | $3.21 | -477.45 | -456.23 | +1018.56 | +541.11 |
| 2026-09-16 | `HLP` | 1674 | — | $1.80 | +0.00 | $2.07 | +451.98 | +451.98 | +0.00 | +451.98 |
| 2026-09-16 | `SDGR` | 129 | — | $23.29 | +0.00 | $23.93 | +82.56 | +82.56 | +0.00 | +82.56 |
| 2026-09-16 | `SSL` | 205 | — | $14.62 | +0.00 | $14.29 | -67.65 | -67.65 | +0.00 | -67.65 |
| 2026-09-17 | `INDP` | 1061 | $3.21 | $3.30 | +95.49 | $3.93 | +668.43 | +763.92 | +636.60 | +1305.03 |
| 2026-09-17 | `HLP` | 1674 | $2.07 | $2.10 | +50.22 | $2.02 | -133.92 | -83.70 | +502.20 | +368.28 |
| 2026-09-17 | `SDGR` | 129 | $23.93 | $24.09 | +20.64 | — | +0.00 | +20.64 | +103.20 | — |
| 2026-09-17 | `SSL` | 205 | $14.29 | $13.77 | -106.60 | — | +0.00 | -106.60 | -174.25 | — |
| 2026-09-17 | `BBNX` | 131 | — | $22.46 | +0.00 | $21.43 | -134.93 | -134.93 | +0.00 | -134.93 |
| 2026-09-17 | `FPS` | 80 | — | $36.76 | +0.00 | $38.06 | +104.00 | +104.00 | +0.00 | +104.00 |
| 2026-09-18 | `INDP` | 1061 | $3.93 | $3.85 | -84.88 | $3.55 | -318.30 | -403.18 | +1220.15 | +901.85 |
| 2026-09-18 | `HLP` | 1674 | $2.02 | $1.96 | -100.44 | — | +0.00 | -100.44 | +267.84 | — |
| 2026-09-18 | `BBNX` | 131 | $21.43 | $21.30 | -17.03 | — | +0.00 | -17.03 | -151.96 | — |
| 2026-09-18 | `FPS` | 80 | $38.06 | $39.50 | +115.20 | — | +0.00 | +115.20 | +219.20 | — |
| 2026-09-18 | `SDGR` | 105 | — | $29.32 | +0.00 | $29.02 | -31.50 | -31.50 | +0.00 | -31.50 |
| 2026-09-18 | `CYPH` | 1015 | — | $3.04 | +0.00 | $3.60 | +573.47 | +573.47 | +0.00 | +573.47 |
| 2026-09-18 | `TEM` | 37 | — | $81.40 | +0.00 | $77.84 | -131.72 | -131.72 | +0.00 | -131.72 |
| 2026-09-21 | `INDP` | 1061 | $3.55 | $3.55 | +0.00 | — | +0.00 | +0.00 | +901.85 | — |
| 2026-09-21 | `SDGR` | 105 | $29.02 | $29.43 | +43.05 | — | +0.00 | +43.05 | +11.55 | — |
| 2026-09-21 | `CYPH` | 1015 | $3.60 | $4.00 | +406.00 | — | +0.00 | +406.00 | +979.47 | — |
| 2026-09-21 | `TEM` | 37 | $77.84 | $79.08 | +45.88 | — | +0.00 | +45.88 | -85.84 | — |
| 2026-09-21 | `FEAM` | 1403 | — | $2.47 | +0.00 | $2.48 | +14.03 | +14.03 | +0.00 | +14.03 |
| 2026-09-21 | `TJGC` | 205 | — | $16.91 | +0.00 | $17.58 | +137.35 | +137.35 | +0.00 | +137.35 |
| 2026-09-21 | `LVWR` | 2101 | — | $1.65 | +0.00 | $1.53 | -252.12 | -252.12 | +0.00 | -252.12 |
| 2026-09-21 | `SECZ` | 292 | — | $11.67 | +0.00 | $13.50 | +534.36 | +534.36 | +0.00 | +534.36 |
| 2026-09-22 | `FEAM` | 1403 | $2.48 | $2.48 | +0.00 | $2.48 | +0.00 | +0.00 | +14.03 | +14.03 |
| 2026-09-22 | `TJGC` | 205 | $17.58 | $17.58 | +0.00 | $17.58 | +0.00 | +0.00 | +137.35 | +137.35 |
| 2026-09-22 | `LVWR` | 2101 | $1.53 | $1.53 | +0.00 | $1.53 | +0.00 | +0.00 | -252.12 | -252.12 |
| 2026-09-22 | `SECZ` | 292 | $13.50 | $12.96 | -157.68 | $13.00 | +11.68 | -146.00 | +376.68 | +388.36 |
| 2026-09-23 | `FEAM` | 1403 | $2.48 | $2.92 | +617.32 | $2.64 | -392.84 | +224.48 | +631.35 | +238.51 |
| 2026-09-23 | `TJGC` | 205 | $17.58 | $16.92 | -135.30 | — | +0.00 | -135.30 | +2.05 | — |
| 2026-09-23 | `LVWR` | 2101 | $1.53 | $1.41 | -252.12 | — | +0.00 | -252.12 | -504.24 | — |
| 2026-09-23 | `SECZ` | 292 | $13.00 | $12.80 | -58.40 | — | +0.00 | -58.40 | +329.96 | — |
| 2026-09-23 | `GLND` | 1252 | — | $2.70 | +0.00 | $2.91 | +262.92 | +262.92 | +0.00 | +262.92 |
| 2026-09-23 | `VKTX` | 80 | — | $41.76 | +0.00 | $41.65 | -8.80 | -8.80 | +0.00 | -8.80 |
| 2026-09-23 | `SVIA` | 753 | — | $4.49 | +0.00 | $4.03 | -346.38 | -346.38 | +0.00 | -346.38 |
| 2026-09-24 | `FEAM` | 1403 | $2.64 | $2.68 | +56.12 | — | +0.00 | +56.12 | +294.63 | — |
| 2026-09-24 | `GLND` | 1252 | $2.91 | $3.22 | +385.62 | $5.35 | +2669.26 | +3054.88 | +648.54 | +3317.80 |
| 2026-09-24 | `VKTX` | 80 | $41.65 | $36.02 | -450.00 | — | +0.00 | -450.00 | -458.80 | — |
| 2026-09-24 | `SVIA` | 753 | $4.03 | $3.92 | -79.07 | — | +0.00 | -79.07 | -425.45 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +386.21 | IREN, TNDM, TPG, INO | — | $0.54 | $10,345.37 | IREN×54, TNDM×107, TPG×49, INO×3085 |
| 2026-08-14 | +5.50 | $0.54 | IREN×54, TNDM×107, TPG×49, INO×3085 | $10,412.10 | +66.73 | -277.26 | QMCO, ARX, ZENA, AIRO | IREN, TNDM, TPG, INO | $9.09 | $10,066.79 | QMCO×105, ARX×132, ZENA×1178, AIRO×231 |
| 2026-08-17 | +2.25 | $9.09 | QMCO×105, ARX×132, ZENA×1178, AIRO×231 | $9,866.28 | -200.51 | +25.77 | XHG, CAPR, STDN, HTFL | QMCO, ARX, ZENA, AIRO | $19.42 | $9,851.95 | XHG×587, CAPR×358, STDN×180, HTFL×59 |
| 2026-08-18 | -6.20 | $19.42 | XHG×587, CAPR×358, STDN×180, HTFL×59 | $9,861.50 | +9.55 | -150.36 | — | XHG, STDN, HTFL | $7,164.03 | $9,698.67 | CAPR×358 |
| 2026-08-19 | -7.20 | $7,164.03 | CAPR×358 | $9,738.05 | +39.38 | +0.00 | — | CAPR | $9,733.36 | $9,733.36 | — |
| 2026-08-20 | +1.12 | $9,733.36 | — | $9,733.36 | -0.00 | -111.05 | MRNA, CYPH, ABCL, AZI | — | $1.24 | $9,567.54 | MRNA×16, CYPH×2115, ABCL×205, AZI×1767 |
| 2026-08-21 | +3.25 | $1.24 | MRNA×16, CYPH×2115, ABCL×205, AZI×1767 | $9,874.47 | +306.93 | +172.78 | XHG, CAPR | ABCL, AZI | $3.27 | $10,009.73 | MRNA×16, CYPH×2115, XHG×548, CAPR×360 |
| 2026-08-24 | -5.17 | $3.27 | MRNA×16, CYPH×2115, XHG×548, CAPR×360 | $11,415.08 | +1,405.35 | +0.00 | — | MRNA, CYPH, XHG, CAPR | $11,373.44 | $11,373.44 | — |
| 2026-08-25 | +1.80 | $11,373.44 | — | $11,373.44 | +0.00 | +964.10 | REAX, CYPH, XHG, ASST | — | $14.19 | $12,300.26 | REAX×117, CYPH×1822, XHG×698, ASST×148 |
| 2026-08-26 | +2.02 | $14.19 | REAX×117, CYPH×1822, XHG×698, ASST×148 | $11,768.70 | -531.56 | +286.33 | BYND, USDE, PURR | REAX, CYPH, ASST | $3.36 | $12,013.50 | XHG×698, BYND×214, USDE×520, PURR×261 |
| 2026-08-27 | — | $3.36 | XHG×698, BYND×214, USDE×520, PURR×261 | $12,435.02 | +421.52 | +3.39 | CAPR, MRNA, BZ | BYND, USDE, PURR | $36.14 | $12,416.30 | XHG×698, CAPR×347, MRNA×22, BZ×172 |
| 2026-08-28 | +0.75 | $36.14 | XHG×698, CAPR×347, MRNA×22, BZ×172 | $12,128.05 | -288.25 | -14.89 | BYND, ANF | XHG, BZ | $85.85 | $12,096.78 | CAPR×347, MRNA×22, BYND×204, ANF×19 |
| 2026-08-31 | -5.85 | $85.85 | CAPR×347, MRNA×22, BYND×204, ANF×19 | $11,962.36 | -134.42 | -104.04 | — | CAPR, MRNA, ANF | $9,136.39 | $11,849.59 | BYND×204 |
| 2026-09-01 | -6.30 | $9,136.39 | BYND×204 | $11,796.55 | -53.04 | +0.00 | — | BYND | $11,793.86 | $11,793.86 | — |
| 2026-09-02 | -3.83 | $11,793.86 | — | $11,793.86 | +0.00 | +0.00 | — | — | $11,793.86 | $11,793.86 | — |
| 2026-09-03 | -0.90 | $11,793.86 | — | $11,793.86 | +0.00 | -622.77 | GPRO, REAX, CNH, MMED | — | $12.21 | $11,142.13 | GPRO×1656, REAX×160, CNH×215, MMED×122 |
| 2026-09-04 | +2.25 | $12.21 | GPRO×1656, REAX×160, CNH×215, MMED×122 | $11,261.92 | +119.79 | +654.48 | ASST, USDE, DFDV | REAX, CNH, MMED | $11.36 | $11,894.98 | GPRO×1656, ASST×116, USDE×372, DFDV×506 |
| 2026-09-08 | -11.47 | $11.36 | GPRO×1656, ASST×116, USDE×372, DFDV×506 | $11,496.62 | -398.36 | +0.00 | — | GPRO, ASST, USDE, DFDV | $11,461.06 | $11,461.06 | — |
| 2026-09-09 | -13.95 | $11,461.06 | — | $11,461.06 | +0.00 | +0.00 | — | — | $11,461.06 | $11,461.06 | — |
| 2026-09-10 | -13.28 | $11,461.06 | — | $11,461.06 | +0.00 | +0.00 | — | — | $11,461.06 | $11,461.06 | — |
| 2026-09-11 | +0.50 | $11,461.06 | — | $11,461.06 | +0.00 | +293.34 | INDP, BNC, IRD, CMRC | — | $1.05 | $11,715.54 | INDP×1061, BNC×583, IRD×465, CMRC×904 |
| 2026-09-14 | -11.00 | $1.05 | INDP×1061, BNC×583, IRD×465, CMRC×904 | $11,876.68 | +161.14 | +618.18 | — | IRD | $2,794.25 | $12,488.76 | INDP×1061, BNC×583, CMRC×904 |
| 2026-09-15 | -3.84 | $2,794.25 | INDP×1061, BNC×583, CMRC×904 | $12,671.34 | +182.58 | +254.64 | — | BNC, CMRC | $9,044.46 | $12,906.50 | INDP×1061 |
| 2026-09-16 | +5.30 | $9,044.46 | INDP×1061 | $12,927.72 | +21.22 | -10.56 | HLP, SDGR, SSL | — | $3.13 | $12,890.54 | INDP×1061, HLP×1674, SDGR×129, SSL×205 |
| 2026-09-17 | +7.38 | $3.13 | INDP×1061, HLP×1674, SDGR×129, SSL×205 | $12,950.29 | +59.75 | +503.58 | BBNX, FPS | SDGR, SSL | $40.80 | $13,444.14 | INDP×1061, HLP×1674, BBNX×131, FPS×80 |
| 2026-09-18 | +4.86 | $40.80 | INDP×1061, HLP×1674, BBNX×131, FPS×80 | $13,356.99 | -87.15 | +91.95 | SDGR, CYPH, TEM | HLP, BBNX, FPS | $57.12 | $13,404.85 | INDP×1061, SDGR×105, CYPH×1015, TEM×37 |
| 2026-09-21 | +12.87 | $57.12 | INDP×1061, SDGR×105, CYPH×1015, TEM×37 | $13,899.78 | +494.93 | +433.62 | FEAM, TJGC, LVWR, SECZ | INDP, SDGR, CYPH, TEM | $10.24 | $14,250.11 | FEAM×1403, TJGC×205, LVWR×2101, SECZ×292 |
| 2026-09-22 | -0.50 | $10.24 | FEAM×1403, TJGC×205, LVWR×2101, SECZ×292 | $14,092.43 | -157.68 | +11.68 | — | — | $10.24 | $14,104.11 | FEAM×1403, TJGC×205, LVWR×2101, SECZ×292 |
| 2026-09-23 | +2.29 | $10.24 | FEAM×1403, TJGC×205, LVWR×2101, SECZ×292 | $14,275.61 | +171.50 | -485.10 | GLND, VKTX, SVIA | TJGC, LVWR, SECZ | $14.56 | $13,728.39 | FEAM×1403, GLND×1252, VKTX×80, SVIA×753 |
| 2026-09-24 | -7.66 | $14.56 | FEAM×1403, GLND×1252, VKTX×80, SVIA×753 | $13,641.06 | -87.33 | +2,669.26 | — | FEAM, VKTX, SVIA | $9,581.64 | $16,279.84 | GLND×1252 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 54 | $45.98 | $2.15 | — | $7,514.93 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+12.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 107 | $23.33 | $2.31 | — | $5,016.31 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+19.7; leftover $2500.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 49 | $50.62 | $2.14 | — | $2,533.63 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+6.2; leftover $2500.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 3085 | $0.81 | $34.24 | — | $0.54 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+13.2; leftover $2500.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.54 | ▲ close $10,345.37 vs 09:30 $10,000.00 (session +386.21) | 16:00 close · cash $0.54 · equity $10,345.37 vs 09:30 $10,000.00 (+345.37; session marks +386.21) · 4 name(s) marked open→close (per-name table). IREN×54 09:30 $45.98 → close $44.76 -65.88; TNDM×107 09:30 $23.33 → close $23.13 -21.40; TPG×49 09:30 $50.62 → close $54.62 +195.84; INO×3085 09:30 $0.81 → close $0.90 +277.65 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.54 | ▲ 09:30 equity $10,412.10 vs yday $10,345.37 (+66.73) | 09:30 open · cash $0.54 (unchanged overnight, no fees) · equity $10,412.10 vs prior close $10,345.37 (+66.73) · 4 name(s) re-marked at the open (per-name table). IREN×54 yday $44.76 → 09:30 $44.09 -36.18; TNDM×107 yday $23.13 → 09:30 $22.92 -22.47; TPG×49 yday $54.62 → 09:30 $55.29 +32.83; INO×3085 yday $0.90 → 09:30 $0.93 +92.55 | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 54 | $44.09 | $2.18 | $-106.39 | $2,379.22 | ▼ -106.39 after sell → book $10,409.92; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 107 | $22.92 | $2.35 | $-48.53 | $4,829.31 | ▼ -48.53 after sell → book $10,407.57; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 49 | $55.29 | $2.17 | $+224.37 | $7,536.35 | ▲ +224.37 after sell → book $10,405.40; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 3085 | $0.93 | $38.48 | $+297.48 | $10,366.92 | ▲ +297.48 after sell → book $10,366.92; vs 09:30 mark -38.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 105 | $24.68 | $2.31 | — | $7,773.22 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+111.3; leftover $2591.73 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 132 | $19.57 | $2.39 | — | $5,187.59 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $2591.73 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 1178 | $2.20 | $15.20 | — | $2,580.79 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $2591.73 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 231 | $11.12 | $2.98 | — | $9.09 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $2591.73 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.09 | ▼ close $10,066.79 vs 09:30 $10,412.10 (session -277.26) | 16:00 close · cash $9.09 · equity $10,066.79 vs 09:30 $10,412.10 (-345.31; session marks -277.26) · 4 name(s) marked open→close (per-name table). QMCO×105 09:30 $24.68 → close $26.11 +150.15; ARX×132 09:30 $19.57 → close $19.58 +1.32; ZENA×1178 09:30 $2.20 → close $2.14 -70.68; AIRO×231 09:30 $11.12 → close $9.57 -358.05 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.09 | ▼ 09:30 equity $9,866.28 vs yday $10,066.79 (-200.51) | 09:30 open · cash $9.09 (unchanged overnight, no fees) · equity $9,866.28 vs prior close $10,066.79 (-200.51) · 4 name(s) re-marked at the open (per-name table). QMCO×105 yday $26.11 → 09:30 $24.83 -134.40; ARX×132 yday $19.58 → 09:30 $19.57 -1.32; ZENA×1178 yday $2.14 → 09:30 $2.08 -64.79; AIRO×231 yday $9.57 → 09:30 $9.57 +0.00 | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 105 | $24.83 | $2.34 | $+11.10 | $2,613.90 | ▲ +11.10 after sell → book $9,863.94; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 132 | $19.57 | $2.43 | $-4.81 | $5,194.71 | ▼ -4.81 after sell → book $9,861.51; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 1178 | $2.08 | $15.41 | $-166.08 | $7,635.43 | ▼ -166.08 after sell → book $9,846.10; vs 09:30 mark -15.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 231 | $9.57 | $3.04 | $-364.07 | $9,843.06 | ▼ -364.07 after sell → book $9,843.06; vs 09:30 mark -3.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 587 | $4.19 | $7.57 | — | $7,375.96 | — | top 4 by hot; rank hot_score; list yday_mover; ⚪; ret5=+291.8; leftover $2460.77 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 358 | $6.87 | $4.62 | — | $4,911.88 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; leftover $2460.77 | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 180 | $13.64 | $2.53 | — | $2,454.15 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; leftover $2460.77 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 59 | $41.23 | $2.17 | — | $19.42 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; leftover $2460.77 | join🟡 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.42 | ▲ close $9,851.95 vs 09:30 $9,866.28 (session +25.77) | 16:00 close · cash $19.42 · equity $9,851.95 vs 09:30 $9,866.28 (-14.33; session marks +25.77) · 4 name(s) marked open→close (per-name table). XHG×587 09:30 $4.19 → close $3.91 -164.36; CAPR×358 09:30 $6.87 → close $7.45 +207.64; STDN×180 09:30 $13.64 → close $13.31 -59.40; HTFL×59 09:30 $41.23 → close $41.94 +41.89 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.42 | ▲ 09:30 equity $9,861.50 vs yday $9,851.95 (+9.55) | 09:30 open · cash $19.42 (unchanged overnight, no fees) · equity $9,861.50 vs prior close $9,851.95 (+9.55) · 4 name(s) re-marked at the open (per-name table). XHG×587 yday $3.91 → 09:30 $3.94 +17.61; CAPR×358 yday $7.45 → 09:30 $7.50 +17.90; STDN×180 yday $13.31 → 09:30 $13.31 +0.00; HTFL×59 yday $41.94 → 09:30 $41.50 -25.96 | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 587 | $3.94 | $7.69 | $-162.01 | $2,324.51 | ▼ -162.01 after sell → book $9,853.81; vs 09:30 mark -7.69 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 180 | $13.31 | $2.58 | $-64.51 | $4,717.73 | ▼ -64.51 after sell → book $9,851.23; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 59 | $41.50 | $2.20 | $+11.57 | $7,164.03 | ▲ +11.57 after sell → book $9,849.03; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,164.03 | ▼ close $9,698.67 vs 09:30 $9,861.50 (session -150.36) | 16:00 close · cash $7,164.03 · equity $9,698.67 vs 09:30 $9,861.50 (-162.83; session marks -150.36) · 1 name(s) marked open→close (per-name table). CAPR×358 09:30 $7.50 → close $7.08 -150.36 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,164.03 | ▲ 09:30 equity $9,738.05 vs yday $9,698.67 (+39.38) | 09:30 open · cash $7,164.03 (unchanged overnight, no fees) · equity $9,738.05 vs prior close $9,698.67 (+39.38) · 1 name(s) re-marked at the open (per-name table). CAPR×358 yday $7.08 → 09:30 $7.19 +39.38 | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 358 | $7.19 | $4.70 | $+105.24 | $9,733.36 | ▲ +105.24 after sell → book $9,733.36; vs 09:30 mark -4.69 | dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,733.36 | ▲ close $9,733.36 vs 09:30 $9,738.05 (session +0.00) | 16:00 close · cash $9,733.36 · no lots left · equity $9,733.36. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,733.36 | ▲ 09:30 equity $9,733.36 vs yday $9,733.36 (-0.00) | 09:30 open · cash $9,733.36 · no holdings · equity $9,733.36 vs prior close $9,733.36 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 16 | $150.14 | $2.04 | — | $7,329.08 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $2433.34 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 2115 | $1.15 | $27.28 | — | $4,869.54 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $2433.34 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 205 | $11.81 | $2.64 | — | $2,444.82 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $2433.34 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 1767 | $1.37 | $22.79 | — | $1.24 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $2433.34 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.24 | ▼ close $9,567.54 vs 09:30 $9,733.36 (session -111.05) | 16:00 close · cash $1.24 · equity $9,567.54 vs 09:30 $9,733.36 (-165.82; session marks -111.05) · 4 name(s) marked open→close (per-name table). MRNA×16 09:30 $150.14 → close $133.32 -269.12; CYPH×2115 09:30 $1.15 → close $1.19 +84.60; ABCL×205 09:30 $11.81 → close $11.57 -50.22; AZI×1767 09:30 $1.37 → close $1.44 +123.69 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.24 | ▲ 09:30 equity $9,874.47 vs yday $9,567.54 (+306.93) | 09:30 open · cash $1.24 (unchanged overnight, no fees) · equity $9,874.47 vs prior close $9,567.54 (+306.93) · 4 name(s) re-marked at the open (per-name table). MRNA×16 yday $133.32 → 09:30 $133.11 -3.36; CYPH×2115 yday $1.19 → 09:30 $1.32 +274.95; ABCL×205 yday $11.57 → 09:30 $11.57 +0.00; AZI×1767 yday $1.44 → 09:30 $1.46 +35.34 | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 205 | $11.57 | $2.70 | $-55.57 | $2,370.39 | ▼ -55.57 after sell → book $9,871.77; vs 09:30 mark -2.70 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 1767 | $1.46 | $23.11 | $+113.13 | $4,927.10 | ▲ +113.13 after sell → book $9,848.66; vs 09:30 mark -23.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 548 | $4.49 | $7.07 | — | $2,459.51 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; leftover $2463.55 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 360 | $6.81 | $4.64 | — | $3.27 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+62.5; leftover $2463.55 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.27 | ▲ close $10,009.73 vs 09:30 $9,874.47 (session +172.78) | 16:00 close · cash $3.27 · equity $10,009.73 vs 09:30 $9,874.47 (+135.26; session marks +172.78) · 4 name(s) marked open→close (per-name table). MRNA×16 09:30 $133.11 → close $145.13 +192.32; CYPH×2115 09:30 $1.32 → close $1.42 +211.50; XHG×548 09:30 $4.49 → close $4.41 -43.84; CAPR×360 09:30 $6.81 → close $6.29 -187.20 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.27 | ▲ 09:30 equity $11,415.08 vs yday $10,009.73 (+1,405.35) | 09:30 open · cash $3.27 (unchanged overnight, no fees) · equity $11,415.08 vs prior close $10,009.73 (+1405.35) · 4 name(s) re-marked at the open (per-name table). MRNA×16 yday $145.13 → 09:30 $142.70 -38.88; CYPH×2115 yday $1.42 → 09:30 $1.83 +867.15; XHG×548 yday $4.41 → 09:30 $4.32 -49.32; CAPR×360 yday $6.29 → 09:30 $8.03 +626.40 | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 16 | $142.70 | $2.07 | $-123.14 | $2,284.40 | ▼ -123.14 after sell → book $11,413.01; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 2115 | $1.83 | $27.67 | $+1383.25 | $6,127.19 | ▲ +1,383.25 after sell → book $11,385.35; vs 09:30 mark -27.66 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 548 | $4.32 | $7.18 | $-107.41 | $8,487.37 | ▼ -107.41 after sell → book $11,378.17; vs 09:30 mark -7.18 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 360 | $8.03 | $4.73 | $+429.83 | $11,373.44 | ▲ +429.83 after sell → book $11,373.44; vs 09:30 mark -4.73 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,373.44 | ▲ close $11,373.44 vs 09:30 $11,415.08 (session +0.00) | 16:00 close · cash $11,373.44 · no lots left · equity $11,373.44. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,373.44 | ▲ 09:30 equity $11,373.44 vs yday $11,373.44 (+0.00) | 09:30 open · cash $11,373.44 · no holdings · equity $11,373.44 vs prior close $11,373.44 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 117 | $24.11 | $2.34 | — | $8,550.23 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+891.7; leftover $2843.36 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 1822 | $1.56 | $23.50 | — | $5,684.41 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $2843.36 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 698 | $4.07 | $9.00 | — | $2,834.54 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; leftover $2843.36 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 148 | $19.04 | $2.43 | — | $14.19 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; leftover $2843.36 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.19 | ▲ close $12,300.26 vs 09:30 $11,373.44 (session +964.10) | 16:00 close · cash $14.19 · equity $12,300.26 vs 09:30 $11,373.44 (+926.82; session marks +964.10) · 4 name(s) marked open→close (per-name table). REAX×117 09:30 $24.11 → close $28.43 +505.44; CYPH×1822 09:30 $1.56 → close $1.64 +145.76; XHG×698 09:30 $4.07 → close $4.02 -34.90; ASST×148 09:30 $19.04 → close $21.39 +347.80 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.19 | ▼ 09:30 equity $11,768.70 vs yday $12,300.26 (-531.56) | 09:30 open · cash $14.19 (unchanged overnight, no fees) · equity $11,768.70 vs prior close $12,300.26 (-531.56) · 4 name(s) re-marked at the open (per-name table). REAX×117 yday $28.43 → 09:30 $26.61 -212.94; CYPH×1822 yday $1.64 → 09:30 $1.60 -72.88; XHG×698 yday $4.02 → 09:30 $3.81 -146.58; ASST×148 yday $21.39 → 09:30 $20.72 -99.16 | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 117 | $26.61 | $2.39 | $+287.77 | $3,125.17 | ▲ +287.77 after sell → book $11,766.31; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 1822 | $1.60 | $23.83 | $+25.55 | $6,016.55 | ▲ +25.55 after sell → book $11,742.49; vs 09:30 mark -23.82 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 148 | $20.72 | $2.48 | $+243.72 | $9,080.62 | ▲ +243.72 after sell → book $11,740.00; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 214 | $14.11 | $2.76 | — | $6,058.32 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+11.4; leftover $3026.87 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 520 | $5.81 | $6.71 | — | $3,030.41 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $3026.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `PURR` | 261 | $11.59 | $3.37 | — | $3.36 | — | top 4 by hot; rank hot_score; list overnight; 🔵; ret5=+64.9; leftover $3026.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.36 | ▲ close $12,013.50 vs 09:30 $11,768.70 (session +286.33) | 16:00 close · cash $3.36 · equity $12,013.50 vs 09:30 $11,768.70 (+244.80; session marks +286.33) · 4 name(s) marked open→close (per-name table). XHG×698 09:30 $3.81 → close $4.06 +174.50; BYND×214 09:30 $14.11 → close $14.25 +29.96; USDE×520 09:30 $5.81 → close $5.98 +88.40; PURR×261 09:30 $11.59 → close $11.56 -6.53 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.36 | ▲ 09:30 equity $12,435.02 vs yday $12,013.50 (+421.52) | 09:30 open · cash $3.36 (unchanged overnight, no fees) · equity $12,435.02 vs prior close $12,013.50 (+421.52) · 4 name(s) re-marked at the open (per-name table). XHG×698 yday $4.06 → 09:30 $4.06 +0.00; BYND×214 yday $14.25 → 09:30 $14.20 -10.70; USDE×520 yday $5.98 → 09:30 $6.50 +270.40; PURR×261 yday $11.56 → 09:30 $12.18 +161.82 | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 214 | $14.20 | $2.82 | $+13.68 | $3,039.34 | ▲ +13.68 after sell → book $12,432.20; vs 09:30 mark -2.82 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 520 | $6.50 | $6.82 | $+345.27 | $6,412.52 | ▲ +345.27 after sell → book $12,425.38; vs 09:30 mark -6.82 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟡 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `PURR` | 261 | $12.18 | $3.44 | $+148.49 | $9,588.06 | ▲ +148.49 after sell → book $12,421.94; vs 09:30 mark -3.44 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CAPR` | 347 | $9.19 | $4.48 | — | $6,394.66 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.3; leftover $3196.02 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRNA` | 22 | $144.18 | $2.06 | — | $3,220.64 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-14.2; leftover $3196.02 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `BZ` | 172 | $18.50 | $2.51 | — | $36.14 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+17.2; leftover $3196.02 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $36.14 | ▲ close $12,416.30 vs 09:30 $12,435.02 (session +3.39) | 16:00 close · cash $36.14 · equity $12,416.30 vs 09:30 $12,435.02 (-18.72; session marks +3.39) · 4 name(s) marked open→close (per-name table). XHG×698 09:30 $4.06 → close $3.80 -181.48; CAPR×347 09:30 $9.19 → close $10.06 +301.89; MRNA×22 09:30 $144.18 → close $142.77 -31.02; BZ×172 09:30 $18.50 → close $18.00 -86.00 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $36.14 | ▼ 09:30 equity $12,128.05 vs yday $12,416.30 (-288.25) | 09:30 open · cash $36.14 (unchanged overnight, no fees) · equity $12,128.05 vs prior close $12,416.30 (-288.25) · 4 name(s) re-marked at the open (per-name table). XHG×698 yday $3.80 → 09:30 $3.69 -76.78; CAPR×347 yday $10.06 → 09:30 $9.73 -114.51; MRNA×22 yday $142.77 → 09:30 $137.19 -122.76; BZ×172 yday $18.00 → 09:30 $18.15 +25.80 | — |
| 2026-08-28 09:30 ET | **SELL** | `XHG` | 698 | $3.69 | $9.14 | $-283.38 | $2,602.61 | ▼ -283.38 after sell → book $12,118.90; vs 09:30 mark -9.15 | dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 172 | $18.15 | $2.56 | $-65.27 | $5,721.86 | ▼ -65.27 after sell → book $12,116.35; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 204 | $14.00 | $2.63 | — | $2,863.22 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-3.3; leftover $2860.93 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 19 | $146.07 | $2.05 | — | $85.85 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $2860.93 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $85.85 | ▼ close $12,096.78 vs 09:30 $12,128.05 (session -14.89) | 16:00 close · cash $85.85 · equity $12,096.78 vs 09:30 $12,128.05 (-31.27; session marks -14.89) · 4 name(s) marked open→close (per-name table). CAPR×347 09:30 $9.73 → close $9.59 -48.58; MRNA×22 09:30 $137.19 → close $137.99 +17.60; BYND×204 09:30 $14.00 → close $13.86 -28.56; ANF×19 09:30 $146.07 → close $148.42 +44.65 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.85 | ▼ 09:30 equity $11,962.36 vs yday $12,096.78 (-134.42) | 09:30 open · cash $85.85 (unchanged overnight, no fees) · equity $11,962.36 vs prior close $12,096.78 (-134.42) · 4 name(s) re-marked at the open (per-name table). CAPR×347 yday $9.59 → 09:30 $9.50 -31.23; MRNA×22 yday $137.99 → 09:30 $134.10 -85.58; BYND×204 yday $13.86 → 09:30 $13.81 -10.20; ANF×19 yday $148.42 → 09:30 $148.03 -7.41 | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 347 | $9.50 | $4.56 | $+98.53 | $3,377.79 | ▲ +98.53 after sell → book $11,957.80; vs 09:30 mark -4.56 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 22 | $134.10 | $2.09 | $-225.91 | $6,325.90 | ▼ -225.91 after sell → book $11,955.71; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 19 | $148.03 | $2.08 | $+33.11 | $9,136.39 | ▲ +33.11 after sell → book $11,953.63; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,136.39 | ▼ close $11,849.59 vs 09:30 $11,962.36 (session -104.04) | 16:00 close · cash $9,136.39 · equity $11,849.59 vs 09:30 $11,962.36 (-112.77; session marks -104.04) · 1 name(s) marked open→close (per-name table). BYND×204 09:30 $13.81 → close $13.30 -104.04 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,136.39 | ▼ 09:30 equity $11,796.55 vs yday $11,849.59 (-53.04) | 09:30 open · cash $9,136.39 (unchanged overnight, no fees) · equity $11,796.55 vs prior close $11,849.59 (-53.04) · 1 name(s) re-marked at the open (per-name table). BYND×204 yday $13.30 → 09:30 $13.04 -53.04 | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 204 | $13.04 | $2.69 | $-201.16 | $11,793.86 | ▼ -201.16 after sell → book $11,793.86; vs 09:30 mark -2.69 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,793.86 | ▲ close $11,793.86 vs 09:30 $11,796.55 (session +0.00) | 16:00 close · cash $11,793.86 · no lots left · equity $11,793.86. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,793.86 | ▲ 09:30 equity $11,793.86 vs yday $11,793.86 (+0.00) | 09:30 open · cash $11,793.86 · no holdings · equity $11,793.86 vs prior close $11,793.86 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,793.86 | ▲ close $11,793.86 vs 09:30 $11,793.86 (session +0.00) | 16:00 close · cash $11,793.86 · no lots left · equity $11,793.86. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,793.86 | ▲ 09:30 equity $11,793.86 vs yday $11,793.86 (+0.00) | 09:30 open · cash $11,793.86 · no holdings · equity $11,793.86 vs prior close $11,793.86 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1656 | $1.78 | $21.36 | — | $8,824.82 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; leftover $2948.47 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 160 | $18.40 | $2.47 | — | $5,878.35 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; leftover $2948.47 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 215 | $13.71 | $2.77 | — | $2,927.92 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; leftover $2948.47 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 122 | $23.88 | $2.36 | — | $12.21 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $2948.47 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.21 | ▼ close $11,142.13 vs 09:30 $11,793.86 (session -622.77) | 16:00 close · cash $12.21 · equity $11,142.13 vs 09:30 $11,793.86 (-651.73; session marks -622.77) · 4 name(s) marked open→close (per-name table). GPRO×1656 09:30 $1.78 → close $1.39 -645.84; REAX×160 09:30 $18.40 → close $18.40 +0.00; CNH×215 09:30 $13.71 → close $13.84 +27.95; MMED×122 09:30 $23.88 → close $23.84 -4.88 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.21 | ▲ 09:30 equity $11,261.92 vs yday $11,142.13 (+119.79) | 09:30 open · cash $12.21 (unchanged overnight, no fees) · equity $11,261.92 vs prior close $11,142.13 (+119.79) · 4 name(s) re-marked at the open (per-name table). GPRO×1656 yday $1.39 → 09:30 $1.48 +149.04; REAX×160 yday $18.40 → 09:30 $18.15 -40.00; CNH×215 yday $13.84 → 09:30 $13.89 +10.75; MMED×122 yday $23.84 → 09:30 $23.84 +0.00 | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 160 | $18.15 | $2.52 | $-44.99 | $2,913.69 | ▼ -44.99 after sell → book $11,259.40; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 215 | $13.89 | $2.83 | $+33.09 | $5,897.21 | ▲ +33.09 after sell → book $11,256.57; vs 09:30 mark -2.83 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 122 | $23.84 | $2.40 | $-9.64 | $8,803.29 | ▼ -9.64 after sell → book $11,254.17; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 116 | $25.18 | $2.34 | — | $5,880.07 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+16.0; leftover $2934.43 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 372 | $7.87 | $4.80 | — | $2,947.63 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; leftover $2934.43 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 506 | $5.79 | $6.53 | — | $11.36 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $2934.43 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.36 | ▲ close $11,894.98 vs 09:30 $11,261.92 (session +654.48) | 16:00 close · cash $11.36 · equity $11,894.98 vs 09:30 $11,261.92 (+633.06; session marks +654.48) · 4 name(s) marked open→close (per-name table). GPRO×1656 09:30 $1.48 → close $1.70 +364.32; ASST×116 09:30 $25.18 → close $27.14 +227.36; USDE×372 09:30 $7.87 → close $7.93 +22.32; DFDV×506 09:30 $5.79 → close $5.87 +40.48 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.36 | ▼ 09:30 equity $11,496.62 vs yday $11,894.98 (-398.36) | 09:30 open · cash $11.36 (unchanged overnight, no fees) · equity $11,496.62 vs prior close $11,894.98 (-398.36) · 4 name(s) re-marked at the open (per-name table). GPRO×1656 yday $1.70 → 09:30 $1.56 -223.56; ASST×116 yday $27.14 → 09:30 $26.44 -81.20; USDE×372 yday $7.93 → 09:30 $7.76 -63.24; DFDV×506 yday $5.87 → 09:30 $5.81 -30.36 | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 1656 | $1.56 | $21.66 | $-399.06 | $2,581.34 | ▼ -399.06 after sell → book $11,474.96; vs 09:30 mark -21.66 | dropped from list after 2 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 116 | $26.44 | $2.38 | $+141.44 | $5,646.00 | ▲ +141.44 after sell → book $11,472.58; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 372 | $7.76 | $4.88 | $-50.60 | $8,527.84 | ▼ -50.60 after sell → book $11,467.70; vs 09:30 mark -4.88 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 506 | $5.81 | $6.63 | $-3.04 | $11,461.06 | ▼ -3.04 after sell → book $11,461.06; vs 09:30 mark -6.64 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,461.06 | ▲ close $11,461.06 vs 09:30 $11,496.62 (session +0.00) | 16:00 close · cash $11,461.06 · no lots left · equity $11,461.06. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,461.06 | ▲ 09:30 equity $11,461.06 vs yday $11,461.06 (+0.00) | 09:30 open · cash $11,461.06 · no holdings · equity $11,461.06 vs prior close $11,461.06 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,461.06 | ▲ close $11,461.06 vs 09:30 $11,461.06 (session +0.00) | 16:00 close · cash $11,461.06 · no lots left · equity $11,461.06. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,461.06 | ▲ 09:30 equity $11,461.06 vs yday $11,461.06 (+0.00) | 09:30 open · cash $11,461.06 · no holdings · equity $11,461.06 vs prior close $11,461.06 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,461.06 | ▲ close $11,461.06 vs 09:30 $11,461.06 (session +0.00) | 16:00 close · cash $11,461.06 · no lots left · equity $11,461.06. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,461.06 | ▲ 09:30 equity $11,461.06 vs yday $11,461.06 (+0.00) | 09:30 open · cash $11,461.06 · no holdings · equity $11,461.06 vs prior close $11,461.06 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 1061 | $2.70 | $13.69 | — | $8,582.68 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $2865.27 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 583 | $4.91 | $7.52 | — | $5,712.63 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $2865.27 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 465 | $6.16 | $6.00 | — | $2,842.23 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+36.4; leftover $2865.27 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 904 | $3.13 | $11.66 | — | $1.05 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+24.2; leftover $2865.27 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.05 | ▲ close $11,715.54 vs 09:30 $11,461.06 (session +293.34) | 16:00 close · cash $1.05 · equity $11,715.54 vs 09:30 $11,461.06 (+254.48; session marks +293.34) · 4 name(s) marked open→close (per-name table). INDP×1061 09:30 $2.70 → close $2.77 +74.27; BNC×583 09:30 $4.91 → close $4.80 -64.13; IRD×465 09:30 $6.16 → close $6.04 -55.80; CMRC×904 09:30 $3.13 → close $3.50 +339.00 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.05 | ▲ 09:30 equity $11,876.68 vs yday $11,715.54 (+161.14) | 09:30 open · cash $1.05 (unchanged overnight, no fees) · equity $11,876.68 vs prior close $11,715.54 (+161.14) · 4 name(s) re-marked at the open (per-name table). INDP×1061 yday $2.77 → 09:30 $2.80 +31.83; BNC×583 yday $4.80 → 09:30 $5.03 +134.09; IRD×465 yday $6.04 → 09:30 $6.02 -9.30; CMRC×904 yday $3.50 → 09:30 $3.51 +4.52 | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 465 | $6.02 | $6.10 | $-77.20 | $2,794.25 | ▼ -77.20 after sell → book $11,870.58; vs 09:30 mark -6.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,794.25 | ▲ close $12,488.76 vs 09:30 $11,876.68 (session +618.18) | 16:00 close · cash $2,794.25 · equity $12,488.76 vs 09:30 $11,876.68 (+612.08; session marks +618.18) · 3 name(s) marked open→close (per-name table). INDP×1061 09:30 $2.80 → close $3.14 +360.74; BNC×583 09:30 $5.03 → close $5.27 +139.92; CMRC×904 09:30 $3.51 → close $3.64 +117.52 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,794.25 | ▲ 09:30 equity $12,671.34 vs yday $12,488.76 (+182.58) | 09:30 open · cash $2,794.25 (unchanged overnight, no fees) · equity $12,671.34 vs prior close $12,488.76 (+182.58) · 3 name(s) re-marked at the open (per-name table). INDP×1061 yday $3.14 → 09:30 $3.40 +275.86; BNC×583 yday $5.27 → 09:30 $5.11 -93.28; CMRC×904 yday $3.64 → 09:30 $3.64 +0.00 | — |
| 2026-09-15 09:30 ET | **SELL** | `BNC` | 583 | $5.11 | $7.64 | $+101.44 | $5,765.74 | ▲ +101.44 after sell → book $12,663.70; vs 09:30 mark -7.64 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 09:30 ET | **SELL** | `CMRC` | 904 | $3.64 | $11.84 | $+437.54 | $9,044.46 | ▲ +437.54 after sell → book $12,651.86; vs 09:30 mark -11.84 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,044.46 | ▲ close $12,906.50 vs 09:30 $12,671.34 (session +254.64) | 16:00 close · cash $9,044.46 · equity $12,906.50 vs 09:30 $12,671.34 (+235.16; session marks +254.64) · 1 name(s) marked open→close (per-name table). INDP×1061 09:30 $3.40 → close $3.64 +254.64 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,044.46 | ▲ 09:30 equity $12,927.72 vs yday $12,906.50 (+21.22) | 09:30 open · cash $9,044.46 (unchanged overnight, no fees) · equity $12,927.72 vs prior close $12,906.50 (+21.22) · 1 name(s) re-marked at the open (per-name table). INDP×1061 yday $3.64 → 09:30 $3.66 +21.22 | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 1674 | $1.80 | $21.59 | — | $6,009.66 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.5; leftover $3014.82 | join🟡 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 129 | $23.29 | $2.38 | — | $3,002.88 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+16.1; leftover $3014.82 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SSL` | 205 | $14.62 | $2.64 | — | $3.13 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+13.6; leftover $3014.82 | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.13 | ▼ close $12,890.54 vs 09:30 $12,927.72 (session -10.56) | 16:00 close · cash $3.13 · equity $12,890.54 vs 09:30 $12,927.72 (-37.18; session marks -10.56) · 4 name(s) marked open→close (per-name table). INDP×1061 09:30 $3.66 → close $3.21 -477.45; HLP×1674 09:30 $1.80 → close $2.07 +451.98; SDGR×129 09:30 $23.29 → close $23.93 +82.56; SSL×205 09:30 $14.62 → close $14.29 -67.65 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.13 | ▲ 09:30 equity $12,950.29 vs yday $12,890.54 (+59.75) | 09:30 open · cash $3.13 (unchanged overnight, no fees) · equity $12,950.29 vs prior close $12,890.54 (+59.75) · 4 name(s) re-marked at the open (per-name table). INDP×1061 yday $3.21 → 09:30 $3.30 +95.49; HLP×1674 yday $2.07 → 09:30 $2.10 +50.22; SDGR×129 yday $23.93 → 09:30 $24.09 +20.64; SSL×205 yday $14.29 → 09:30 $13.77 -106.60 | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 129 | $24.09 | $2.42 | $+98.40 | $3,108.32 | ▲ +98.40 after sell → book $12,947.87; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SSL` | 205 | $13.77 | $2.70 | $-179.60 | $5,928.47 | ▼ -179.60 after sell → book $12,945.17; vs 09:30 mark -2.70 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `BBNX` | 131 | $22.46 | $2.38 | — | $2,983.83 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+27.3; leftover $2964.23 | join🟢 sector🟢 gen🟢 news🔴 digest🔴 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `FPS` | 80 | $36.76 | $2.23 | — | $40.80 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+12.4; leftover $2964.23 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $40.80 | ▲ close $13,444.14 vs 09:30 $12,950.29 (session +503.58) | 16:00 close · cash $40.80 · equity $13,444.14 vs 09:30 $12,950.29 (+493.85; session marks +503.58) · 4 name(s) marked open→close (per-name table). INDP×1061 09:30 $3.30 → close $3.93 +668.43; HLP×1674 09:30 $2.10 → close $2.02 -133.92; BBNX×131 09:30 $22.46 → close $21.43 -134.93; FPS×80 09:30 $36.76 → close $38.06 +104.00 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.80 | ▼ 09:30 equity $13,356.99 vs yday $13,444.14 (-87.15) | 09:30 open · cash $40.80 (unchanged overnight, no fees) · equity $13,356.99 vs prior close $13,444.14 (-87.15) · 4 name(s) re-marked at the open (per-name table). INDP×1061 yday $3.93 → 09:30 $3.85 -84.88; HLP×1674 yday $2.02 → 09:30 $1.96 -100.44; BBNX×131 yday $21.43 → 09:30 $21.30 -17.03; FPS×80 yday $38.06 → 09:30 $39.50 +115.20 | — |
| 2026-09-18 09:30 ET | **SELL** | `HLP` | 1674 | $1.96 | $21.90 | $+224.35 | $3,299.94 | ▲ +224.35 after sell → book $13,335.09; vs 09:30 mark -21.90 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BBNX` | 131 | $21.30 | $2.43 | $-156.77 | $6,087.81 | ▼ -156.77 after sell → book $13,332.66; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 80 | $39.50 | $2.27 | $+214.70 | $9,245.54 | ▲ +214.70 after sell → book $13,330.39; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 105 | $29.32 | $2.31 | — | $6,164.64 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; leftover $3081.85 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 1015 | $3.04 | $13.09 | — | $3,071.02 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $3081.85 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 37 | $81.40 | $2.10 | — | $57.12 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; leftover $3081.85 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.12 | ▲ close $13,404.85 vs 09:30 $13,356.99 (session +91.95) | 16:00 close · cash $57.12 · equity $13,404.85 vs 09:30 $13,356.99 (+47.86; session marks +91.95) · 4 name(s) marked open→close (per-name table). INDP×1061 09:30 $3.85 → close $3.55 -318.30; SDGR×105 09:30 $29.32 → close $29.02 -31.50; CYPH×1015 09:30 $3.04 → close $3.60 +573.47; TEM×37 09:30 $81.40 → close $77.84 -131.72 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.12 | ▲ 09:30 equity $13,899.78 vs yday $13,404.85 (+494.93) | 09:30 open · cash $57.12 (unchanged overnight, no fees) · equity $13,899.78 vs prior close $13,404.85 (+494.93) · 4 name(s) re-marked at the open (per-name table). INDP×1061 yday $3.55 → 09:30 $3.55 +0.00; SDGR×105 yday $29.02 → 09:30 $29.43 +43.05; CYPH×1015 yday $3.60 → 09:30 $4.00 +406.00; TEM×37 yday $77.84 → 09:30 $79.08 +45.88 | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 1061 | $3.55 | $13.89 | $+874.27 | $3,809.77 | ▲ +874.27 after sell → book $13,885.88; vs 09:30 mark -13.90 | dropped from list after 6 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 105 | $29.43 | $2.35 | $+6.90 | $6,897.58 | ▲ +6.90 after sell → book $13,883.54; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 1015 | $4.00 | $13.29 | $+953.09 | $10,944.28 | ▲ +953.09 after sell → book $13,870.24; vs 09:30 mark -13.30 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 37 | $79.08 | $2.13 | $-90.08 | $13,868.11 | ▼ -90.08 after sell → book $13,868.11; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 1403 | $2.47 | $18.10 | — | $10,384.60 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+73.6; leftover $3467.03 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 205 | $16.91 | $2.64 | — | $6,915.40 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+50.5; leftover $3467.03 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟡 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `LVWR` | 2101 | $1.65 | $27.10 | — | $3,421.65 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+43.0; leftover $3467.03 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 292 | $11.67 | $3.77 | — | $10.24 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+31.3; leftover $3467.03 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.24 | ▲ close $14,250.11 vs 09:30 $13,899.78 (session +433.62) | 16:00 close · cash $10.24 · equity $14,250.11 vs 09:30 $13,899.78 (+350.33; session marks +433.62) · 4 name(s) marked open→close (per-name table). FEAM×1403 09:30 $2.47 → close $2.48 +14.03; TJGC×205 09:30 $16.91 → close $17.58 +137.35; LVWR×2101 09:30 $1.65 → close $1.53 -252.12; SECZ×292 09:30 $11.67 → close $13.50 +534.36 | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.24 | ▼ 09:30 equity $14,092.43 vs yday $14,250.11 (-157.68) | 09:30 open · cash $10.24 (unchanged overnight, no fees) · equity $14,092.43 vs prior close $14,250.11 (-157.68) · 4 name(s) re-marked at the open (per-name table). FEAM×1403 yday $2.48 → 09:30 $2.48 +0.00; TJGC×205 yday $17.58 → 09:30 $17.58 +0.00; LVWR×2101 yday $1.53 → 09:30 $1.53 +0.00; SECZ×292 yday $13.50 → 09:30 $12.96 -157.68 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.24 | ▲ close $14,104.11 vs 09:30 $14,092.43 (session +11.68) | 16:00 close · cash $10.24 · equity $14,104.11 vs 09:30 $14,092.43 (+11.68; session marks +11.68) · 4 name(s) marked open→close (per-name table). FEAM×1403 09:30 $2.48 → close $2.48 +0.00; TJGC×205 09:30 $17.58 → close $17.58 +0.00; LVWR×2101 09:30 $1.53 → close $1.53 +0.00; SECZ×292 09:30 $12.96 → close $13.00 +11.68 | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.24 | ▲ 09:30 equity $14,275.61 vs yday $14,104.11 (+171.50) | 09:30 open · cash $10.24 (unchanged overnight, no fees) · equity $14,275.61 vs prior close $14,104.11 (+171.50) · 4 name(s) re-marked at the open (per-name table). FEAM×1403 yday $2.48 → 09:30 $2.92 +617.32; TJGC×205 yday $17.58 → 09:30 $16.92 -135.30; LVWR×2101 yday $1.53 → 09:30 $1.41 -252.12; SECZ×292 yday $13.00 → 09:30 $12.80 -58.40 | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 205 | $16.92 | $2.71 | $-3.30 | $3,476.14 | ▼ -3.30 after sell → book $14,272.91; vs 09:30 mark -2.70 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `LVWR` | 2101 | $1.41 | $27.48 | $-558.82 | $6,411.07 | ▼ -558.82 after sell → book $14,245.43; vs 09:30 mark -27.48 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SECZ` | 292 | $12.80 | $3.85 | $+322.35 | $10,144.83 | ▲ +322.35 after sell → book $14,241.59; vs 09:30 mark -3.84 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `GLND` | 1252 | $2.70 | $16.15 | — | $6,748.28 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; leftover $3381.61 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 80 | $41.76 | $2.23 | — | $3,405.25 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $3381.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-23 09:30 ET | **BUY** | `SVIA` | 753 | $4.49 | $9.71 | — | $14.56 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+26.4; leftover $3381.61 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.56 | ▼ close $13,728.39 vs 09:30 $14,275.61 (session -485.10) | 16:00 close · cash $14.56 · equity $13,728.39 vs 09:30 $14,275.61 (-547.22; session marks -485.10) · 4 name(s) marked open→close (per-name table). FEAM×1403 09:30 $2.92 → close $2.64 -392.84; GLND×1252 09:30 $2.70 → close $2.91 +262.92; VKTX×80 09:30 $41.76 → close $41.65 -8.80; SVIA×753 09:30 $4.49 → close $4.03 -346.38 | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.56 | ▼ 09:30 equity $13,641.06 vs yday $13,728.39 (-87.33) | 09:30 open · cash $14.56 (unchanged overnight, no fees) · equity $13,641.06 vs prior close $13,728.39 (-87.33) · 4 name(s) re-marked at the open (per-name table). FEAM×1403 yday $2.64 → 09:30 $2.68 +56.12; GLND×1252 yday $2.91 → 09:30 $3.22 +385.62; VKTX×80 yday $41.65 → 09:30 $36.02 -450.00; SVIA×753 yday $4.03 → 09:30 $3.92 -79.07 | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 1403 | $2.68 | $18.36 | $+258.17 | $3,756.24 | ▲ +258.17 after sell → book $13,622.70; vs 09:30 mark -18.36 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 80 | $36.02 | $2.27 | $-463.30 | $6,635.97 | ▼ -463.30 after sell → book $13,620.44; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SVIA` | 753 | $3.92 | $9.86 | $-445.02 | $9,581.64 | ▼ -445.02 after sell → book $13,610.57; vs 09:30 mark -9.87 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,581.64 | ▲ close $16,279.84 vs 09:30 $13,641.06 (session +2,669.26) | 16:00 close · cash $9,581.64 · equity $16,279.84 vs 09:30 $13,641.06 (+2638.78; session marks +2669.26) · 1 name(s) marked open→close (per-name table). GLND×1252 09:30 $3.22 → close $5.35 +2669.26 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `REAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MRNA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `GPRO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `GPRO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SES` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `FEAM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TJGC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `LVWR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `CRML` | cash | leftover split 3.41 < 1 share @ 9.11 |
| 2026-09-22 | `NUAI` | cash | leftover split 3.41 < 1 share @ 7.23 |
| 2026-09-24 | `TJGC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SECZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `VICR` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GLND` | 1252 | 2026-09-23 @ $2.70 | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; leftover $3381.61 |
