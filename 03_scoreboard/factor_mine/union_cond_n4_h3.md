# Factor mine action — `union_cond_n4_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · top 4 by cond

Cash book **-9.40%** ($9,060) · signal-only (no cash/fees) was -4.86%. Starts YES **3/26**. Fills 70 · skips 135 · realized $-971.38.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written and skip anything on the must-not list. They take up to 4 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how many morning cameras are green vs red.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is thrown out if any must-not is true.
- Sort the keepers by how many morning cameras are green vs red and keep the top 4.
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
- **Gate** `none (list as ranked)` · **rank** `cond` · **top_n** 4.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $13.21.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `BTSG` | 41 | — | $59.80 | +0.00 | $60.23 | +17.63 | +17.63 | +0.00 | +17.63 |
| 2026-08-13 | `HIMS` | 84 | — | $29.74 | +0.00 | $28.77 | -81.48 | -81.48 | +0.00 | -81.48 |
| 2026-08-13 | `INO` | 3086 | — | $0.81 | +0.00 | $0.90 | +277.74 | +277.74 | +0.00 | +277.74 |
| 2026-08-13 | `IREN` | 54 | — | $45.98 | +0.00 | $44.76 | -65.88 | -65.88 | +0.00 | -65.88 |
| 2026-08-14 | `BTSG` | 41 | $60.23 | $59.65 | -23.78 | $61.71 | +84.46 | +60.68 | -6.15 | +78.31 |
| 2026-08-14 | `HIMS` | 84 | $28.77 | $29.15 | +31.92 | $28.15 | -84.00 | -52.08 | -49.56 | -133.56 |
| 2026-08-14 | `INO` | 3086 | $0.90 | $0.93 | +92.58 | $1.09 | +493.76 | +586.34 | +370.32 | +864.08 |
| 2026-08-14 | `IREN` | 54 | $44.76 | $44.09 | -36.18 | $44.06 | -1.62 | -37.80 | -102.06 | -103.68 |
| 2026-08-17 | `BTSG` | 41 | $61.71 | $61.69 | -0.82 | $60.38 | -53.71 | -54.53 | +77.49 | +23.78 |
| 2026-08-17 | `HIMS` | 84 | $28.15 | $28.14 | -0.84 | $28.61 | +39.48 | +38.64 | -134.40 | -94.92 |
| 2026-08-17 | `INO` | 3086 | $1.09 | $1.07 | -61.72 | $1.15 | +246.88 | +185.16 | +802.36 | +1049.24 |
| 2026-08-17 | `IREN` | 54 | $44.06 | $45.23 | +63.18 | $44.90 | -17.82 | +45.36 | -40.50 | -58.32 |
| 2026-08-17 | `VERI` | 5 | — | $1.15 | +0.00 | $1.08 | -0.32 | -0.32 | +0.00 | -0.32 |
| 2026-08-18 | `BTSG` | 41 | $60.38 | $60.00 | -15.58 | — | +0.00 | -15.58 | +8.20 | — |
| 2026-08-18 | `HIMS` | 84 | $28.61 | $27.85 | -63.84 | — | +0.00 | -63.84 | -158.76 | — |
| 2026-08-18 | `INO` | 3086 | $1.15 | $1.14 | -30.86 | — | +0.00 | -30.86 | +1018.38 | — |
| 2026-08-18 | `IREN` | 54 | $44.90 | $43.56 | -72.36 | — | +0.00 | -72.36 | -130.68 | — |
| 2026-08-18 | `VERI` | 5 | $1.08 | $1.05 | -0.17 | $0.99 | -0.28 | -0.45 | -0.50 | -0.77 |
| 2026-08-19 | `VERI` | 5 | $0.99 | $1.00 | +0.03 | $0.97 | -0.17 | -0.14 | -0.75 | -0.92 |
| 2026-08-20 | `VERI` | 5 | $0.97 | $0.96 | -0.02 | — | +0.00 | -0.02 | -0.93 | — |
| 2026-08-20 | `AG` | 129 | — | $20.55 | +0.00 | $21.19 | +82.56 | +82.56 | +0.00 | +82.56 |
| 2026-08-20 | `BHP` | 29 | — | $91.01 | +0.00 | $93.63 | +75.98 | +75.98 | +0.00 | +75.98 |
| 2026-08-20 | `CDE` | 128 | — | $20.65 | +0.00 | $21.11 | +58.88 | +58.88 | +0.00 | +58.88 |
| 2026-08-20 | `HDSN` | 461 | — | $5.77 | +0.00 | $5.57 | -92.20 | -92.20 | +0.00 | -92.20 |
| 2026-08-21 | `AG` | 129 | $21.19 | $21.90 | +91.59 | $21.09 | -104.49 | -12.90 | +174.15 | +69.66 |
| 2026-08-21 | `BHP` | 29 | $93.63 | $95.72 | +60.61 | $97.03 | +37.99 | +98.60 | +136.59 | +174.58 |
| 2026-08-21 | `CDE` | 128 | $21.11 | $21.75 | +81.92 | $20.97 | -99.84 | -17.92 | +140.80 | +40.96 |
| 2026-08-21 | `HDSN` | 461 | $5.57 | $5.67 | +46.10 | $5.63 | -18.44 | +27.66 | -46.10 | -64.54 |
| 2026-08-24 | `AG` | 129 | $21.09 | $21.30 | +27.09 | $20.83 | -60.63 | -33.54 | +96.75 | +36.12 |
| 2026-08-24 | `BHP` | 29 | $97.03 | $97.31 | +8.12 | $97.13 | -5.22 | +2.90 | +182.70 | +177.48 |
| 2026-08-24 | `CDE` | 128 | $20.97 | $21.26 | +37.12 | $20.88 | -48.64 | -11.52 | +78.08 | +29.44 |
| 2026-08-24 | `HDSN` | 461 | $5.63 | $5.69 | +27.66 | $5.52 | -78.37 | -50.71 | -36.88 | -115.25 |
| 2026-08-25 | `AG` | 129 | $20.83 | $20.32 | -65.79 | — | +0.00 | -65.79 | -29.67 | — |
| 2026-08-25 | `BHP` | 29 | $97.13 | $95.86 | -36.83 | — | +0.00 | -36.83 | +140.65 | — |
| 2026-08-25 | `CDE` | 128 | $20.88 | $20.47 | -52.48 | — | +0.00 | -52.48 | -23.04 | — |
| 2026-08-25 | `HDSN` | 461 | $5.52 | $5.53 | +4.61 | — | +0.00 | +4.61 | -110.64 | — |
| 2026-08-25 | `AU` | 22 | — | $118.52 | +0.00 | $123.39 | +107.14 | +107.14 | +0.00 | +107.14 |
| 2026-08-25 | `ERO` | 69 | — | $38.01 | +0.00 | $40.40 | +164.91 | +164.91 | +0.00 | +164.91 |
| 2026-08-25 | `FCX` | 34 | — | $77.13 | +0.00 | $79.91 | +94.52 | +94.52 | +0.00 | +94.52 |
| 2026-08-25 | `CNH` | 222 | — | $11.90 | +0.00 | $11.56 | -75.48 | -75.48 | +0.00 | -75.48 |
| 2026-08-26 | `AU` | 22 | $123.39 | $119.80 | -78.98 | $118.11 | -37.18 | -116.16 | +28.16 | -9.02 |
| 2026-08-26 | `ERO` | 69 | $40.40 | $40.51 | +7.59 | $39.24 | -87.63 | -80.04 | +172.50 | +84.87 |
| 2026-08-26 | `FCX` | 34 | $79.91 | $79.34 | -19.38 | $79.00 | -11.56 | -30.94 | +75.14 | +63.58 |
| 2026-08-26 | `CNH` | 222 | $11.56 | $11.54 | -4.44 | $11.62 | +17.76 | +13.32 | -79.92 | -62.16 |
| 2026-08-26 | `ASST` | 1 | — | $20.72 | +0.00 | $21.50 | +0.78 | +0.78 | +0.00 | +0.78 |
| 2026-08-26 | `DBRG` | 1 | — | $15.97 | +0.00 | $15.97 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-27 | `AU` | 22 | $118.11 | $117.41 | -15.40 | $118.40 | +21.78 | +6.38 | -24.42 | -2.64 |
| 2026-08-27 | `ERO` | 69 | $39.24 | $39.20 | -2.76 | $39.82 | +42.78 | +40.02 | +82.11 | +124.89 |
| 2026-08-27 | `FCX` | 34 | $79.00 | $78.83 | -5.78 | $78.42 | -13.94 | -19.72 | +57.80 | +43.86 |
| 2026-08-27 | `CNH` | 222 | $11.62 | $11.62 | +0.00 | $11.43 | -42.18 | -42.18 | -62.16 | -104.34 |
| 2026-08-27 | `ASST` | 1 | $21.50 | $22.45 | +0.95 | $23.12 | +0.67 | +1.62 | +1.73 | +2.40 |
| 2026-08-27 | `DBRG` | 1 | $15.97 | $15.96 | -0.01 | $15.96 | +0.00 | -0.01 | -0.01 | -0.01 |
| 2026-08-27 | `GGB` | 3 | — | $4.57 | +0.00 | $4.70 | +0.39 | +0.39 | +0.00 | +0.39 |
| 2026-08-28 | `AU` | 22 | $118.40 | $119.19 | +17.38 | — | +0.00 | +17.38 | +14.74 | — |
| 2026-08-28 | `ERO` | 69 | $39.82 | $40.12 | +20.70 | — | +0.00 | +20.70 | +145.59 | — |
| 2026-08-28 | `FCX` | 34 | $78.42 | $78.57 | +5.10 | — | +0.00 | +5.10 | +48.96 | — |
| 2026-08-28 | `CNH` | 222 | $11.43 | $11.55 | +26.64 | — | +0.00 | +26.64 | -77.70 | — |
| 2026-08-28 | `ASST` | 1 | $23.12 | $22.50 | -0.62 | $21.74 | -0.76 | -1.38 | +1.78 | +1.02 |
| 2026-08-28 | `DBRG` | 1 | $15.96 | $15.97 | +0.01 | $15.93 | -0.04 | -0.03 | +0.00 | -0.04 |
| 2026-08-28 | `GGB` | 3 | $4.70 | $4.67 | -0.09 | $4.59 | -0.24 | -0.33 | +0.30 | +0.06 |
| 2026-08-28 | `KEYS` | 8 | — | $324.41 | +0.00 | $319.97 | -35.52 | -35.52 | +0.00 | -35.52 |
| 2026-08-28 | `SMTC` | 18 | — | $141.76 | +0.00 | $131.17 | -190.62 | -190.62 | +0.00 | -190.62 |
| 2026-08-28 | `CIEN` | 6 | — | $400.42 | +0.00 | $378.44 | -131.88 | -131.88 | +0.00 | -131.88 |
| 2026-08-28 | `MPWR` | 2 | — | $1306.03 | +0.00 | $1256.26 | -99.54 | -99.54 | +0.00 | -99.54 |
| 2026-08-31 | `ASST` | 1 | $21.74 | $22.54 | +0.80 | — | +0.00 | +0.80 | +1.82 | — |
| 2026-08-31 | `DBRG` | 1 | $15.93 | $15.93 | +0.00 | — | +0.00 | +0.00 | -0.04 | — |
| 2026-08-31 | `GGB` | 3 | $4.59 | $4.67 | +0.24 | $4.61 | -0.18 | +0.06 | +0.30 | +0.12 |
| 2026-08-31 | `KEYS` | 8 | $319.97 | $322.49 | +20.16 | $322.70 | +1.68 | +21.84 | -15.36 | -13.68 |
| 2026-08-31 | `SMTC` | 18 | $131.17 | $132.30 | +20.34 | $132.96 | +11.88 | +32.22 | -170.28 | -158.40 |
| 2026-08-31 | `CIEN` | 6 | $378.44 | $378.44 | +0.00 | $382.80 | +26.16 | +26.16 | -131.88 | -105.72 |
| 2026-08-31 | `MPWR` | 2 | $1256.26 | $1261.90 | +11.28 | $1267.77 | +11.74 | +23.02 | -88.26 | -76.52 |
| 2026-09-01 | `GGB` | 3 | $4.61 | $4.57 | -0.12 | — | +0.00 | -0.12 | +0.00 | — |
| 2026-09-01 | `KEYS` | 8 | $322.70 | $321.47 | -9.84 | $319.27 | -17.60 | -27.44 | -23.52 | -41.12 |
| 2026-09-01 | `SMTC` | 18 | $132.96 | $127.63 | -95.94 | $132.27 | +83.52 | -12.42 | -254.34 | -170.82 |
| 2026-09-01 | `CIEN` | 6 | $382.80 | $376.89 | -35.46 | $360.33 | -99.36 | -134.82 | -141.18 | -240.54 |
| 2026-09-01 | `MPWR` | 2 | $1267.77 | $1245.11 | -45.32 | $1225.96 | -38.30 | -83.62 | -121.84 | -160.14 |
| 2026-09-02 | `KEYS` | 8 | $319.27 | $318.04 | -9.84 | — | +0.00 | -9.84 | -50.96 | — |
| 2026-09-02 | `SMTC` | 18 | $132.27 | $133.00 | +13.14 | — | +0.00 | +13.14 | -157.68 | — |
| 2026-09-02 | `CIEN` | 6 | $360.33 | $357.25 | -18.48 | — | +0.00 | -18.48 | -259.02 | — |
| 2026-09-02 | `MPWR` | 2 | $1225.96 | $1224.92 | -2.08 | — | +0.00 | -2.08 | -162.22 | — |
| 2026-09-03 | `ARCT` | 150 | — | $16.77 | +0.00 | $15.56 | -181.50 | -181.50 | +0.00 | -181.50 |
| 2026-09-03 | `BMEA` | 1304 | — | $1.93 | +0.00 | $1.91 | -26.08 | -26.08 | +0.00 | -26.08 |
| 2026-09-03 | `CRDL` | 1154 | — | $2.18 | +0.00 | $2.16 | -23.08 | -23.08 | +0.00 | -23.08 |
| 2026-09-03 | `HRMY` | 57 | — | $42.93 | +0.00 | $41.86 | -60.99 | -60.99 | +0.00 | -60.99 |
| 2026-09-04 | `ARCT` | 150 | $15.56 | $15.61 | +7.50 | $15.82 | +31.50 | +39.00 | -174.00 | -142.50 |
| 2026-09-04 | `BMEA` | 1304 | $1.91 | $1.90 | -13.04 | $2.03 | +169.52 | +156.48 | -39.12 | +130.40 |
| 2026-09-04 | `CRDL` | 1154 | $2.16 | $2.16 | +0.00 | $2.20 | +46.16 | +46.16 | -23.08 | +23.08 |
| 2026-09-04 | `HRMY` | 57 | $41.86 | $41.50 | -20.52 | $42.25 | +42.75 | +22.23 | -81.51 | -38.76 |
| 2026-09-04 | `CABA` | 2 | — | $3.46 | +0.00 | $3.47 | +0.02 | +0.02 | +0.00 | +0.02 |
| 2026-09-04 | `ALEC` | 3 | — | $2.52 | +0.00 | $2.46 | -0.18 | -0.18 | +0.00 | -0.18 |
| 2026-09-04 | `BHC` | 1 | — | $6.71 | +0.00 | $6.56 | -0.15 | -0.15 | +0.00 | -0.15 |
| 2026-09-08 | `ARCT` | 150 | $15.82 | $15.47 | -52.50 | $15.63 | +24.00 | -28.50 | -195.00 | -171.00 |
| 2026-09-08 | `BMEA` | 1304 | $2.03 | $2.00 | -39.12 | $1.93 | -91.28 | -130.40 | +91.28 | +0.00 |
| 2026-09-08 | `CRDL` | 1154 | $2.20 | $2.20 | +0.00 | $2.22 | +23.08 | +23.08 | +23.08 | +46.16 |
| 2026-09-08 | `HRMY` | 57 | $42.25 | $42.20 | -2.85 | $42.07 | -7.41 | -10.26 | -41.61 | -49.02 |
| 2026-09-08 | `CABA` | 2 | $3.47 | $3.43 | -0.08 | $3.27 | -0.32 | -0.40 | -0.06 | -0.38 |
| 2026-09-08 | `ALEC` | 3 | $2.46 | $2.38 | -0.24 | $2.47 | +0.27 | +0.03 | -0.42 | -0.15 |
| 2026-09-08 | `BHC` | 1 | $6.56 | $6.57 | +0.01 | $6.43 | -0.14 | -0.13 | -0.14 | -0.28 |
| 2026-09-09 | `ARCT` | 150 | $15.63 | $15.46 | -25.50 | — | +0.00 | -25.50 | -196.50 | — |
| 2026-09-09 | `BMEA` | 1304 | $1.93 | $1.94 | +13.04 | — | +0.00 | +13.04 | +13.04 | — |
| 2026-09-09 | `CRDL` | 1154 | $2.22 | $2.22 | +0.00 | — | +0.00 | +0.00 | +46.16 | — |
| 2026-09-09 | `HRMY` | 57 | $42.07 | $42.01 | -3.42 | — | +0.00 | -3.42 | -52.44 | — |
| 2026-09-09 | `CABA` | 2 | $3.27 | $3.28 | +0.02 | $2.91 | -0.74 | -0.72 | -0.36 | -1.10 |
| 2026-09-09 | `ALEC` | 3 | $2.47 | $2.47 | +0.00 | $2.27 | -0.60 | -0.60 | -0.15 | -0.75 |
| 2026-09-09 | `BHC` | 1 | $6.43 | $6.38 | -0.05 | $6.16 | -0.22 | -0.27 | -0.33 | -0.55 |
| 2026-09-10 | `CABA` | 2 | $2.91 | $2.85 | -0.12 | — | +0.00 | -0.12 | -1.22 | — |
| 2026-09-10 | `ALEC` | 3 | $2.27 | $2.22 | -0.15 | — | +0.00 | -0.15 | -0.90 | — |
| 2026-09-10 | `BHC` | 1 | $6.16 | $6.11 | -0.05 | — | +0.00 | -0.05 | -0.60 | — |
| 2026-09-11 | `SEDG` | 66 | — | $36.78 | +0.00 | $34.68 | -138.60 | -138.60 | +0.00 | -138.60 |
| 2026-09-11 | `BAND` | 46 | — | $52.55 | +0.00 | $56.87 | +198.72 | +198.72 | +0.00 | +198.72 |
| 2026-09-11 | `ORCL` | 14 | — | $164.43 | +0.00 | $150.28 | -198.10 | -198.10 | +0.00 | -198.10 |
| 2026-09-11 | `PAGS` | 242 | — | $10.11 | +0.00 | $10.12 | +2.42 | +2.42 | +0.00 | +2.42 |
| 2026-09-14 | `SEDG` | 66 | $34.68 | $33.64 | -68.64 | $35.33 | +111.54 | +42.90 | -207.24 | -95.70 |
| 2026-09-14 | `BAND` | 46 | $56.87 | $56.90 | +1.38 | $48.97 | -364.78 | -363.40 | +200.10 | -164.68 |
| 2026-09-14 | `ORCL` | 14 | $150.28 | $141.42 | -124.04 | $144.79 | +47.18 | -76.86 | -322.14 | -274.96 |
| 2026-09-14 | `PAGS` | 242 | $10.12 | $10.00 | -29.04 | $9.93 | -16.94 | -45.98 | -26.62 | -43.56 |
| 2026-09-15 | `SEDG` | 66 | $35.33 | $35.24 | -5.94 | $35.26 | +1.32 | -4.62 | -101.64 | -100.32 |
| 2026-09-15 | `BAND` | 46 | $48.97 | $49.51 | +24.84 | $50.08 | +26.22 | +51.06 | -139.84 | -113.62 |
| 2026-09-15 | `ORCL` | 14 | $144.79 | $143.46 | -18.62 | $140.35 | -43.54 | -62.16 | -293.58 | -337.12 |
| 2026-09-15 | `PAGS` | 242 | $9.93 | $9.94 | +2.42 | $9.62 | -77.44 | -75.02 | -41.14 | -118.58 |
| 2026-09-16 | `SEDG` | 66 | $35.26 | $35.93 | +44.22 | — | +0.00 | +44.22 | -56.10 | — |
| 2026-09-16 | `BAND` | 46 | $50.08 | $48.60 | -68.08 | — | +0.00 | -68.08 | -181.70 | — |
| 2026-09-16 | `ORCL` | 14 | $140.35 | $140.03 | -4.48 | — | +0.00 | -4.48 | -341.60 | — |
| 2026-09-16 | `PAGS` | 242 | $9.62 | $9.39 | -55.66 | — | +0.00 | -55.66 | -174.24 | — |
| 2026-09-16 | `QRVO` | 19 | — | $118.18 | +0.00 | $113.97 | -79.99 | -79.99 | +0.00 | -79.99 |
| 2026-09-16 | `SWKS` | 25 | — | $89.38 | +0.00 | $85.59 | -94.75 | -94.75 | +0.00 | -94.75 |
| 2026-09-16 | `ATRC` | 40 | — | $55.66 | +0.00 | $57.14 | +59.20 | +59.20 | +0.00 | +59.20 |
| 2026-09-16 | `BWIN` | 69 | — | $32.25 | +0.00 | $32.04 | -14.49 | -14.49 | +0.00 | -14.49 |
| 2026-09-17 | `QRVO` | 19 | $113.97 | $114.90 | +17.67 | $119.51 | +87.59 | +105.26 | -62.32 | +25.27 |
| 2026-09-17 | `SWKS` | 25 | $85.59 | $86.76 | +29.25 | $91.32 | +114.00 | +143.25 | -65.50 | +48.50 |
| 2026-09-17 | `ATRC` | 40 | $57.14 | $57.96 | +32.80 | $59.12 | +46.40 | +79.20 | +92.00 | +138.40 |
| 2026-09-17 | `BWIN` | 69 | $32.04 | $32.06 | +1.38 | $31.95 | -7.59 | -6.21 | -13.11 | -20.70 |
| 2026-09-17 | `ASAN` | 2 | — | $9.55 | +0.00 | $10.09 | +1.08 | +1.08 | +0.00 | +1.08 |
| 2026-09-17 | `BULL` | 2 | — | $7.95 | +0.00 | $7.71 | -0.48 | -0.48 | +0.00 | -0.48 |
| 2026-09-17 | `CIFR` | 1 | — | $18.04 | +0.00 | $16.94 | -1.09 | -1.09 | +0.00 | -1.09 |
| 2026-09-17 | `SABR` | 9 | — | $2.40 | +0.00 | $2.32 | -0.72 | -0.72 | +0.00 | -0.72 |
| 2026-09-18 | `QRVO` | 19 | $119.51 | $120.76 | +23.75 | $117.18 | -68.02 | -44.27 | +49.02 | -19.00 |
| 2026-09-18 | `SWKS` | 25 | $91.32 | $92.05 | +18.25 | $88.76 | -82.25 | -64.00 | +66.75 | -15.50 |
| 2026-09-18 | `ATRC` | 40 | $59.12 | $58.51 | -24.40 | $58.10 | -16.40 | -40.80 | +114.00 | +97.60 |
| 2026-09-18 | `BWIN` | 69 | $31.95 | $31.98 | +2.07 | $31.93 | -3.45 | -1.38 | -18.63 | -22.08 |
| 2026-09-18 | `ASAN` | 2 | $10.09 | $10.09 | +0.00 | $9.51 | -1.16 | -1.16 | +1.08 | -0.08 |
| 2026-09-18 | `BULL` | 2 | $7.71 | $7.85 | +0.28 | $8.25 | +0.80 | +1.08 | -0.20 | +0.60 |
| 2026-09-18 | `CIFR` | 1 | $16.94 | $17.80 | +0.86 | $18.34 | +0.54 | +1.40 | -0.23 | +0.30 |
| 2026-09-18 | `SABR` | 9 | $2.32 | $2.29 | -0.27 | $2.26 | -0.27 | -0.54 | -0.99 | -1.26 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +148.01 | BTSG, HIMS, INO, IREN | — | $26.70 | $10,107.25 | BTSG×41, HIMS×84, INO×3086, IREN×54 |
| 2026-08-14 | +5.50 | $26.70 | BTSG×41, HIMS×84, INO×3086, IREN×54 | $10,171.79 | +64.54 | +492.60 | — | — | $26.70 | $10,664.39 | BTSG×41, HIMS×84, INO×3086, IREN×54 |
| 2026-08-17 | +2.25 | $26.70 | BTSG×41, HIMS×84, INO×3086, IREN×54 | $10,664.19 | -0.20 | +214.51 | VERI | — | $20.88 | $10,878.62 | BTSG×41, HIMS×84, INO×3086, IREN×54, VERI×5 |
| 2026-08-18 | -6.20 | $20.88 | BTSG×41, HIMS×84, INO×3086, IREN×54, VERI×5 | $10,695.81 | -182.81 | -0.28 | — | BTSG, HIMS, INO, IREN | $10,643.61 | $10,648.58 | VERI×5 |
| 2026-08-19 | -7.20 | $10,643.61 | VERI×5 | $10,648.61 | +0.03 | -0.17 | — | — | $10,643.61 | $10,648.44 | VERI×5 |
| 2026-08-20 | +1.12 | $10,643.61 | VERI×5 | $10,648.42 | -0.02 | +125.22 | AG, BHP, CDE, HDSN | VERI | $42.15 | $10,760.78 | AG×129, BHP×29, CDE×128, HDSN×461 |
| 2026-08-21 | +3.25 | $42.15 | AG×129, BHP×29, CDE×128, HDSN×461 | $11,041.00 | +280.22 | -184.78 | — | — | $42.15 | $10,856.22 | AG×129, BHP×29, CDE×128, HDSN×461 |
| 2026-08-24 | -5.17 | $42.15 | AG×129, BHP×29, CDE×128, HDSN×461 | $10,956.21 | +99.99 | -192.86 | — | — | $42.15 | $10,763.35 | AG×129, BHP×29, CDE×128, HDSN×461 |
| 2026-08-25 | +1.80 | $42.15 | AG×129, BHP×29, CDE×128, HDSN×461 | $10,612.86 | -150.49 | +291.09 | AU, ERO, FCX, CNH | AG, BHP, CDE, HDSN | $96.32 | $10,881.76 | AU×22, ERO×69, FCX×34, CNH×222 |
| 2026-08-26 | +2.02 | $96.32 | AU×22, ERO×69, FCX×34, CNH×222 | $10,786.55 | -95.21 | -117.83 | ASST, DBRG | — | $59.25 | $10,668.34 | AU×22, ERO×69, FCX×34, CNH×222, ASST×1, DBRG×1 |
| 2026-08-27 | — | $59.25 | AU×22, ERO×69, FCX×34, CNH×222, ASST×1, DBRG×1 | $10,645.34 | -23.00 | +9.50 | GGB | — | $45.40 | $10,654.70 | AU×22, ERO×69, FCX×34, CNH×222, ASST×1, DBRG×1, GGB×3 |
| 2026-08-28 | +0.75 | $45.40 | AU×22, ERO×69, FCX×34, CNH×222, ASST×1, DBRG×1, GGB×3 | $10,723.82 | +69.12 | -458.60 | KEYS, SMTC, CIEN, MPWR | AU, ERO, FCX, CNH | $492.37 | $10,247.79 | ASST×1, DBRG×1, GGB×3, KEYS×8, SMTC×18, CIEN×6, MPWR×2 |
| 2026-08-31 | -5.85 | $492.37 | ASST×1, DBRG×1, GGB×3, KEYS×8, SMTC×18, CIEN×6, MPWR×2 | $10,300.61 | +52.82 | +51.28 | — | ASST, DBRG | $530.41 | $10,351.46 | GGB×3, KEYS×8, SMTC×18, CIEN×6, MPWR×2 |
| 2026-09-01 | -6.30 | $530.41 | GGB×3, KEYS×8, SMTC×18, CIEN×6, MPWR×2 | $10,164.78 | -186.68 | -71.74 | — | GGB | $543.96 | $10,092.88 | KEYS×8, SMTC×18, CIEN×6, MPWR×2 |
| 2026-09-02 | -3.83 | $543.96 | KEYS×8, SMTC×18, CIEN×6, MPWR×2 | $10,075.62 | -17.26 | +0.00 | — | KEYS, SMTC, CIEN, MPWR | $10,067.44 | $10,067.44 | — |
| 2026-09-03 | -0.90 | $10,067.44 | — | $10,067.44 | -0.00 | -291.65 | ARCT, BMEA, CRDL, HRMY | — | $36.18 | $9,739.48 | ARCT×150, BMEA×1304, CRDL×1154, HRMY×57 |
| 2026-09-04 | +2.25 | $36.18 | ARCT×150, BMEA×1304, CRDL×1154, HRMY×57 | $9,713.42 | -26.06 | +289.62 | CABA, ALEC, BHC | — | $14.76 | $10,002.81 | ARCT×150, BMEA×1304, CRDL×1154, HRMY×57, CABA×2, ALEC×3, BHC×1 |
| 2026-09-08 | -11.47 | $14.76 | ARCT×150, BMEA×1304, CRDL×1154, HRMY×57, CABA×2, ALEC×3, BHC×1 | $9,908.03 | -94.78 | -51.80 | — | — | $14.76 | $9,856.23 | ARCT×150, BMEA×1304, CRDL×1154, HRMY×57, CABA×2, ALEC×3, BHC×1 |
| 2026-09-09 | -13.95 | $14.76 | ARCT×150, BMEA×1304, CRDL×1154, HRMY×57, CABA×2, ALEC×3, BHC×1 | $9,840.32 | -15.91 | -1.56 | — | ARCT, BMEA, CRDL, HRMY | $9,783.14 | $9,801.93 | CABA×2, ALEC×3, BHC×1 |
| 2026-09-10 | -13.28 | $9,783.14 | CABA×2, ALEC×3, BHC×1 | $9,801.61 | -0.32 | +0.00 | — | CABA, ALEC, BHC | $9,801.35 | $9,801.35 | — |
| 2026-09-11 | +0.50 | $9,801.35 | — | $9,801.35 | -0.00 | -135.56 | SEDG, BAND, ORCL, PAGS | — | $198.46 | $9,656.32 | SEDG×66, BAND×46, ORCL×14, PAGS×242 |
| 2026-09-14 | -11.00 | $198.46 | SEDG×66, BAND×46, ORCL×14, PAGS×242 | $9,435.98 | -220.34 | -223.00 | — | — | $198.46 | $9,212.98 | SEDG×66, BAND×46, ORCL×14, PAGS×242 |
| 2026-09-15 | -3.84 | $198.46 | SEDG×66, BAND×46, ORCL×14, PAGS×242 | $9,215.68 | +2.70 | -93.44 | — | — | $198.46 | $9,122.24 | SEDG×66, BAND×46, ORCL×14, PAGS×242 |
| 2026-09-16 | +5.30 | $198.46 | SEDG×66, BAND×46, ORCL×14, PAGS×242 | $9,038.24 | -84.00 | -130.03 | QRVO, SWKS, ATRC, BWIN | SEDG, BAND, ORCL, PAGS | $88.64 | $8,890.18 | QRVO×19, SWKS×25, ATRC×40, BWIN×69 |
| 2026-09-17 | +7.38 | $88.64 | QRVO×19, SWKS×25, ATRC×40, BWIN×69 | $8,971.28 | +81.10 | +239.19 | ASAN, BULL, CIFR, SABR | — | $13.21 | $9,209.67 | QRVO×19, SWKS×25, ATRC×40, BWIN×69, ASAN×2, BULL×2, CIFR×1, SABR×9 |
| 2026-09-18 | +4.86 | $13.21 | QRVO×19, SWKS×25, ATRC×40, BWIN×69, ASAN×2, BULL×2, CIFR×1, SABR×9 | $9,230.21 | +20.54 | -170.21 | — | — | $13.21 | $9,060.00 | QRVO×19, SWKS×25, ATRC×40, BWIN×69, ASAN×2, BULL×2, CIFR×1, SABR×9 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 41 | $59.80 | $2.11 | — | $7,546.09 | — | top 4 by cond; rank cond; list flatten; ⚪; ret5=-5.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 84 | $29.74 | $2.24 | — | $5,045.69 | — | top 4 by cond; rank cond; list flatten; ⚪; ret5=-5.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 3086 | $0.81 | $34.25 | — | $2,511.77 | — | top 4 by cond; rank cond; list flatten; ⚪; ret5=+13.2; leftover $2500.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 54 | $45.98 | $2.15 | — | $26.70 | — | top 4 by cond; rank cond; list flatten; ⚪; ret5=+12.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26.70 | ▲ close $10,107.25 vs 09:30 $10,000.00 (session +148.01) | 16:00 close · cash $26.70 · equity $10,107.25 vs 09:30 $10,000.00 (+107.25; session marks +148.01) · 4 name(s) marked open→close (per-name table). BTSG×41 09:30 $59.80 → close $60.23 +17.63; HIMS×84 09:30 $29.74 → close $28.77 -81.48; INO×3086 09:30 $0.81 → close $0.90 +277.74; IREN×54 09:30 $45.98 → close $44.76 -65.88 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26.70 | ▲ 09:30 equity $10,171.79 vs yday $10,107.25 (+64.54) | 09:30 open · cash $26.70 (unchanged overnight, no fees) · equity $10,171.79 vs prior close $10,107.25 (+64.54) · 4 name(s) re-marked at the open (per-name table). BTSG×41 yday $60.23 → 09:30 $59.65 -23.78; HIMS×84 yday $28.77 → 09:30 $29.15 +31.92; INO×3086 yday $0.90 → 09:30 $0.93 +92.58; IREN×54 yday $44.76 → 09:30 $44.09 -36.18 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26.70 | ▲ close $10,664.39 vs 09:30 $10,171.79 (session +492.60) | 16:00 close · cash $26.70 · equity $10,664.39 vs 09:30 $10,171.79 (+492.60; session marks +492.60) · 4 name(s) marked open→close (per-name table). BTSG×41 09:30 $59.65 → close $61.71 +84.46; HIMS×84 09:30 $29.15 → close $28.15 -84.00; INO×3086 09:30 $0.93 → close $1.09 +493.76; IREN×54 09:30 $44.09 → close $44.06 -1.62 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26.70 | ▼ 09:30 equity $10,664.19 vs yday $10,664.39 (-0.20) | 09:30 open · cash $26.70 (unchanged overnight, no fees) · equity $10,664.19 vs prior close $10,664.39 (-0.20) · 4 name(s) re-marked at the open (per-name table). BTSG×41 yday $61.71 → 09:30 $61.69 -0.82; HIMS×84 yday $28.15 → 09:30 $28.14 -0.84; INO×3086 yday $1.09 → 09:30 $1.07 -61.72; IREN×54 yday $44.06 → 09:30 $45.23 +63.18 | — |
| 2026-08-17 09:30 ET | **BUY** | `VERI` | 5 | $1.15 | $0.07 | — | $20.88 | — | top 4 by cond; rank cond; list yday_mover; ⚪; ret5=-12.2; leftover $6.67 | join🟡 sector🟢 gen🟢 news🔴 judge🟢 vol🟢 buy🟢 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.88 | ▲ close $10,878.62 vs 09:30 $10,664.19 (session +214.51) | 16:00 close · cash $20.88 · equity $10,878.62 vs 09:30 $10,664.19 (+214.43; session marks +214.51) · 5 name(s) marked open→close (per-name table). BTSG×41 09:30 $61.69 → close $60.38 -53.71; HIMS×84 09:30 $28.14 → close $28.61 +39.48; INO×3086 09:30 $1.07 → close $1.15 +246.88; IREN×54 09:30 $45.23 → close $44.90 -17.82; VERI×5 09:30 $1.15 → close $1.08 -0.32 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.88 | ▼ 09:30 equity $10,695.81 vs yday $10,878.62 (-182.81) | 09:30 open · cash $20.88 (unchanged overnight, no fees) · equity $10,695.81 vs prior close $10,878.62 (-182.81) · 5 name(s) re-marked at the open (per-name table). BTSG×41 yday $60.38 → 09:30 $60.00 -15.58; HIMS×84 yday $28.61 → 09:30 $27.85 -63.84; INO×3086 yday $1.15 → 09:30 $1.14 -30.86; IREN×54 yday $44.90 → 09:30 $43.56 -72.36; VERI×5 yday $1.08 → 09:30 $1.05 -0.17 | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 41 | $60.00 | $2.14 | $+3.94 | $2,478.73 | ▲ +3.94 after sell → book $10,693.66; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 84 | $27.85 | $2.27 | $-163.28 | $4,815.86 | ▼ -163.28 after sell → book $10,691.39; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 3086 | $1.14 | $40.35 | $+943.78 | $8,293.55 | ▲ +943.78 after sell → book $10,651.04; vs 09:30 mark -40.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 54 | $43.56 | $2.18 | $-135.01 | $10,643.61 | ▼ -135.01 after sell → book $10,648.86; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,643.61 | ▼ close $10,648.58 vs 09:30 $10,695.81 (session -0.28) | 16:00 close · cash $10,643.61 · equity $10,648.58 vs 09:30 $10,695.81 (-47.23; session marks -0.28) · 1 name(s) marked open→close (per-name table). VERI×5 09:30 $1.05 → close $0.99 -0.28 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,643.61 | ▲ 09:30 equity $10,648.61 vs yday $10,648.58 (+0.03) | 09:30 open · cash $10,643.61 (unchanged overnight, no fees) · equity $10,648.61 vs prior close $10,648.58 (+0.03) · 1 name(s) re-marked at the open (per-name table). VERI×5 yday $0.99 → 09:30 $1.00 +0.03 | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,643.61 | ▼ close $10,648.44 vs 09:30 $10,648.61 (session -0.17) | 16:00 close · cash $10,643.61 · equity $10,648.44 vs 09:30 $10,648.61 (-0.17; session marks -0.17) · 1 name(s) marked open→close (per-name table). VERI×5 09:30 $1.00 → close $0.97 -0.17 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,643.61 | ▼ 09:30 equity $10,648.42 vs yday $10,648.44 (-0.02) | 09:30 open · cash $10,643.61 (unchanged overnight, no fees) · equity $10,648.42 vs prior close $10,648.44 (-0.02) · 1 name(s) re-marked at the open (per-name table). VERI×5 yday $0.97 → 09:30 $0.96 -0.02 | — |
| 2026-08-20 09:30 ET | **SELL** | `VERI` | 5 | $0.96 | $0.08 | $-1.09 | $10,648.34 | ▼ -1.09 after sell → book $10,648.34; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 129 | $20.55 | $2.38 | — | $7,995.01 | — | top 4 by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $2662.08 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 29 | $91.01 | $2.08 | — | $5,353.65 | — | top 4 by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $2662.08 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 128 | $20.65 | $2.37 | — | $2,708.07 | — | top 4 by cond; rank cond; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $2662.08 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 461 | $5.77 | $5.95 | — | $42.15 | — | top 4 by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $2662.08 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $42.15 | ▲ close $10,760.78 vs 09:30 $10,648.42 (session +125.22) | 16:00 close · cash $42.15 · equity $10,760.78 vs 09:30 $10,648.42 (+112.36; session marks +125.22) · 4 name(s) marked open→close (per-name table). AG×129 09:30 $20.55 → close $21.19 +82.56; BHP×29 09:30 $91.01 → close $93.63 +75.98; CDE×128 09:30 $20.65 → close $21.11 +58.88; HDSN×461 09:30 $5.77 → close $5.57 -92.20 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $42.15 | ▲ 09:30 equity $11,041.00 vs yday $10,760.78 (+280.22) | 09:30 open · cash $42.15 (unchanged overnight, no fees) · equity $11,041.00 vs prior close $10,760.78 (+280.22) · 4 name(s) re-marked at the open (per-name table). AG×129 yday $21.19 → 09:30 $21.90 +91.59; BHP×29 yday $93.63 → 09:30 $95.72 +60.61; CDE×128 yday $21.11 → 09:30 $21.75 +81.92; HDSN×461 yday $5.57 → 09:30 $5.67 +46.10 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $42.15 | ▼ close $10,856.22 vs 09:30 $11,041.00 (session -184.78) | 16:00 close · cash $42.15 · equity $10,856.22 vs 09:30 $11,041.00 (-184.78; session marks -184.78) · 4 name(s) marked open→close (per-name table). AG×129 09:30 $21.90 → close $21.09 -104.49; BHP×29 09:30 $95.72 → close $97.03 +37.99; CDE×128 09:30 $21.75 → close $20.97 -99.84; HDSN×461 09:30 $5.67 → close $5.63 -18.44 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $42.15 | ▲ 09:30 equity $10,956.21 vs yday $10,856.22 (+99.99) | 09:30 open · cash $42.15 (unchanged overnight, no fees) · equity $10,956.21 vs prior close $10,856.22 (+99.99) · 4 name(s) re-marked at the open (per-name table). AG×129 yday $21.09 → 09:30 $21.30 +27.09; BHP×29 yday $97.03 → 09:30 $97.31 +8.12; CDE×128 yday $20.97 → 09:30 $21.26 +37.12; HDSN×461 yday $5.63 → 09:30 $5.69 +27.66 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $42.15 | ▼ close $10,763.35 vs 09:30 $10,956.21 (session -192.86) | 16:00 close · cash $42.15 · equity $10,763.35 vs 09:30 $10,956.21 (-192.86; session marks -192.86) · 4 name(s) marked open→close (per-name table). AG×129 09:30 $21.30 → close $20.83 -60.63; BHP×29 09:30 $97.31 → close $97.13 -5.22; CDE×128 09:30 $21.26 → close $20.88 -48.64; HDSN×461 09:30 $5.69 → close $5.52 -78.37 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $42.15 | ▼ 09:30 equity $10,612.86 vs yday $10,763.35 (-150.49) | 09:30 open · cash $42.15 (unchanged overnight, no fees) · equity $10,612.86 vs prior close $10,763.35 (-150.49) · 4 name(s) re-marked at the open (per-name table). AG×129 yday $20.83 → 09:30 $20.32 -65.79; BHP×29 yday $97.13 → 09:30 $95.86 -36.83; CDE×128 yday $20.88 → 09:30 $20.47 -52.48; HDSN×461 yday $5.52 → 09:30 $5.53 +4.61 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 129 | $20.32 | $2.42 | $-34.47 | $2,661.02 | ▼ -34.47 after sell → book $10,610.45; vs 09:30 mark -2.41 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 29 | $95.86 | $2.11 | $+136.46 | $5,438.85 | ▲ +136.46 after sell → book $10,608.34; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 128 | $20.47 | $2.42 | $-27.83 | $8,056.59 | ▼ -27.83 after sell → book $10,605.92; vs 09:30 mark -2.42 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 461 | $5.53 | $6.04 | $-122.63 | $10,599.88 | ▼ -122.63 after sell → book $10,599.88; vs 09:30 mark -6.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 22 | $118.52 | $2.06 | — | $7,990.38 | — | top 4 by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $2649.97 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ERO` | 69 | $38.01 | $2.20 | — | $5,365.49 | — | top 4 by cond; rank cond; list mover_buy; ⚪; ret5=+10.4; leftover $2649.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 34 | $77.13 | $2.09 | — | $2,740.98 | — | top 4 by cond; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $2649.97 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CNH` | 222 | $11.90 | $2.86 | — | $96.32 | — | top 4 by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+14.3; leftover $2649.97 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $96.32 | ▲ close $10,881.76 vs 09:30 $10,612.86 (session +291.09) | 16:00 close · cash $96.32 · equity $10,881.76 vs 09:30 $10,612.86 (+268.90; session marks +291.09) · 4 name(s) marked open→close (per-name table). AU×22 09:30 $118.52 → close $123.39 +107.14; ERO×69 09:30 $38.01 → close $40.40 +164.91; FCX×34 09:30 $77.13 → close $79.91 +94.52; CNH×222 09:30 $11.90 → close $11.56 -75.48 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $96.32 | ▼ 09:30 equity $10,786.55 vs yday $10,881.76 (-95.21) | 09:30 open · cash $96.32 (unchanged overnight, no fees) · equity $10,786.55 vs prior close $10,881.76 (-95.21) · 4 name(s) re-marked at the open (per-name table). AU×22 yday $123.39 → 09:30 $119.80 -78.98; ERO×69 yday $40.40 → 09:30 $40.51 +7.59; FCX×34 yday $79.91 → 09:30 $79.34 -19.38; CNH×222 yday $11.56 → 09:30 $11.54 -4.44 | — |
| 2026-08-26 09:30 ET | **BUY** | `ASST` | 1 | $20.72 | $0.21 | — | $75.39 | — | top 4 by cond; rank cond; list oppset; 🔵; ret5=+67.1; leftover $24.08 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `DBRG` | 1 | $15.97 | $0.16 | — | $59.25 | — | top 4 by cond; rank cond; list oppset; 🔵; ret5=+0.4; leftover $24.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $59.25 | ▼ close $10,668.34 vs 09:30 $10,786.55 (session -117.83) | 16:00 close · cash $59.25 · equity $10,668.34 vs 09:30 $10,786.55 (-118.21; session marks -117.83) · 6 name(s) marked open→close (per-name table). AU×22 09:30 $119.80 → close $118.11 -37.18; ERO×69 09:30 $40.51 → close $39.24 -87.63; FCX×34 09:30 $79.34 → close $79.00 -11.56; CNH×222 09:30 $11.54 → close $11.62 +17.76; ASST×1 09:30 $20.72 → close $21.50 +0.78; DBRG×1 09:30 $15.97 → close $15.97 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.25 | ▼ 09:30 equity $10,645.34 vs yday $10,668.34 (-23.00) | 09:30 open · cash $59.25 (unchanged overnight, no fees) · equity $10,645.34 vs prior close $10,668.34 (-23.00) · 6 name(s) re-marked at the open (per-name table). AU×22 yday $118.11 → 09:30 $117.41 -15.40; ERO×69 yday $39.24 → 09:30 $39.20 -2.76; FCX×34 yday $79.00 → 09:30 $78.83 -5.78; CNH×222 yday $11.62 → 09:30 $11.62 +0.00; ASST×1 yday $21.50 → 09:30 $22.45 +0.95; DBRG×1 yday $15.97 → 09:30 $15.96 -0.01 | — |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 3 | $4.57 | $0.15 | — | $45.40 | — | top 4 by cond; rank cond; list mover_buy; 🔵; ret5=+1.1; leftover $14.81 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $45.40 | ▲ close $10,654.70 vs 09:30 $10,645.34 (session +9.50) | 16:00 close · cash $45.40 · equity $10,654.70 vs 09:30 $10,645.34 (+9.36; session marks +9.50) · 7 name(s) marked open→close (per-name table). AU×22 09:30 $117.41 → close $118.40 +21.78; ERO×69 09:30 $39.20 → close $39.82 +42.78; FCX×34 09:30 $78.83 → close $78.42 -13.94; CNH×222 09:30 $11.62 → close $11.43 -42.18; ASST×1 09:30 $22.45 → close $23.12 +0.67; DBRG×1 09:30 $15.96 → close $15.96 +0.00; GGB×3 09:30 $4.57 → close $4.70 +0.39 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $45.40 | ▲ 09:30 equity $10,723.82 vs yday $10,654.70 (+69.12) | 09:30 open · cash $45.40 (unchanged overnight, no fees) · equity $10,723.82 vs prior close $10,654.70 (+69.12) · 7 name(s) re-marked at the open (per-name table). AU×22 yday $118.40 → 09:30 $119.19 +17.38; ERO×69 yday $39.82 → 09:30 $40.12 +20.70; FCX×34 yday $78.42 → 09:30 $78.57 +5.10; CNH×222 yday $11.43 → 09:30 $11.55 +26.64; ASST×1 yday $23.12 → 09:30 $22.50 -0.62; DBRG×1 yday $15.96 → 09:30 $15.97 +0.01; GGB×3 yday $4.70 → 09:30 $4.67 -0.09 | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 22 | $119.19 | $2.09 | $+10.60 | $2,665.49 | ▲ +10.60 after sell → book $10,721.73; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ERO` | 69 | $40.12 | $2.23 | $+141.16 | $5,431.54 | ▲ +141.16 after sell → book $10,719.50; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 34 | $78.57 | $2.12 | $+44.74 | $8,100.80 | ▲ +44.74 after sell → book $10,717.38; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CNH` | 222 | $11.55 | $2.92 | $-83.49 | $10,661.98 | ▼ -83.49 after sell → book $10,714.46; vs 09:30 mark -2.92 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 8 | $324.41 | $2.01 | — | $8,064.68 | — | top 4 by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $2665.49 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 18 | $141.76 | $2.04 | — | $5,510.96 | — | top 4 by cond; rank cond; list yday_gainer,yday_mover,mover_buy,oppset; 🔵; ⚪; ret5=+14.1; leftover $2665.49 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 6 | $400.42 | $2.01 | — | $3,106.43 | — | top 4 by cond; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $2665.49 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 2 | $1306.03 | $2.00 | — | $492.37 | — | top 4 by cond; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $2665.49 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $492.37 | ▼ close $10,247.79 vs 09:30 $10,723.82 (session -458.60) | 16:00 close · cash $492.37 · equity $10,247.79 vs 09:30 $10,723.82 (-476.03; session marks -458.60) · 7 name(s) marked open→close (per-name table). ASST×1 09:30 $22.50 → close $21.74 -0.76; DBRG×1 09:30 $15.97 → close $15.93 -0.04; GGB×3 09:30 $4.67 → close $4.59 -0.24; KEYS×8 09:30 $324.41 → close $319.97 -35.52; SMTC×18 09:30 $141.76 → close $131.17 -190.62; CIEN×6 09:30 $400.42 → close $378.44 -131.88; MPWR×2 09:30 $1306.03 → close $1256.26 -99.54 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $492.37 | ▲ 09:30 equity $10,300.61 vs yday $10,247.79 (+52.82) | 09:30 open · cash $492.37 (unchanged overnight, no fees) · equity $10,300.61 vs prior close $10,247.79 (+52.82) · 7 name(s) re-marked at the open (per-name table). ASST×1 yday $21.74 → 09:30 $22.54 +0.80; DBRG×1 yday $15.93 → 09:30 $15.93 +0.00; GGB×3 yday $4.59 → 09:30 $4.67 +0.24; KEYS×8 yday $319.97 → 09:30 $322.49 +20.16; SMTC×18 yday $131.17 → 09:30 $132.30 +20.34; CIEN×6 yday $378.44 → 09:30 $378.44 +0.00; MPWR×2 yday $1256.26 → 09:30 $1261.90 +11.28 | — |
| 2026-08-31 09:30 ET | **SELL** | `ASST` | 1 | $22.54 | $0.25 | $+1.36 | $514.67 | ▲ +1.36 after sell → book $10,300.37; vs 09:30 mark -0.24 | dropped from list after 3 sess (min 3) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `DBRG` | 1 | $15.93 | $0.18 | $-0.38 | $530.41 | ▼ -0.38 after sell → book $10,300.18; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $530.41 | ▲ close $10,351.46 vs 09:30 $10,300.61 (session +51.28) | 16:00 close · cash $530.41 · equity $10,351.46 vs 09:30 $10,300.61 (+50.85; session marks +51.28) · 5 name(s) marked open→close (per-name table). GGB×3 09:30 $4.67 → close $4.61 -0.18; KEYS×8 09:30 $322.49 → close $322.70 +1.68; SMTC×18 09:30 $132.30 → close $132.96 +11.88; CIEN×6 09:30 $378.44 → close $382.80 +26.16; MPWR×2 09:30 $1261.90 → close $1267.77 +11.74 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $530.41 | ▼ 09:30 equity $10,164.78 vs yday $10,351.46 (-186.68) | 09:30 open · cash $530.41 (unchanged overnight, no fees) · equity $10,164.78 vs prior close $10,351.46 (-186.68) · 5 name(s) re-marked at the open (per-name table). GGB×3 yday $4.61 → 09:30 $4.57 -0.12; KEYS×8 yday $322.70 → 09:30 $321.47 -9.84; SMTC×18 yday $132.96 → 09:30 $127.63 -95.94; CIEN×6 yday $382.80 → 09:30 $376.89 -35.46; MPWR×2 yday $1267.77 → 09:30 $1245.11 -45.32 | — |
| 2026-09-01 09:30 ET | **SELL** | `GGB` | 3 | $4.57 | $0.17 | $-0.31 | $543.96 | ▼ -0.31 after sell → book $10,164.62; vs 09:30 mark -0.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $543.96 | ▼ close $10,092.88 vs 09:30 $10,164.78 (session -71.74) | 16:00 close · cash $543.96 · equity $10,092.88 vs 09:30 $10,164.78 (-71.90; session marks -71.74) · 4 name(s) marked open→close (per-name table). KEYS×8 09:30 $321.47 → close $319.27 -17.60; SMTC×18 09:30 $127.63 → close $132.27 +83.52; CIEN×6 09:30 $376.89 → close $360.33 -99.36; MPWR×2 09:30 $1245.11 → close $1225.96 -38.30 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $543.96 | ▼ 09:30 equity $10,075.62 vs yday $10,092.88 (-17.26) | 09:30 open · cash $543.96 (unchanged overnight, no fees) · equity $10,075.62 vs prior close $10,092.88 (-17.26) · 4 name(s) re-marked at the open (per-name table). KEYS×8 yday $319.27 → 09:30 $318.04 -9.84; SMTC×18 yday $132.27 → 09:30 $133.00 +13.14; CIEN×6 yday $360.33 → 09:30 $357.25 -18.48; MPWR×2 yday $1225.96 → 09:30 $1224.92 -2.08 | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 8 | $318.04 | $2.04 | $-55.02 | $3,086.23 | ▼ -55.02 after sell → book $10,073.57; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 18 | $133.00 | $2.07 | $-161.80 | $5,478.16 | ▼ -161.80 after sell → book $10,071.50; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CIEN` | 6 | $357.25 | $2.04 | $-263.06 | $7,619.62 | ▼ -263.06 after sell → book $10,069.46; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `MPWR` | 2 | $1224.92 | $2.03 | $-166.24 | $10,067.44 | ▼ -166.24 after sell → book $10,067.44; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,067.44 | ▲ close $10,067.44 vs 09:30 $10,075.62 (session +0.00) | 16:00 close · cash $10,067.44 · no lots left · equity $10,067.44. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,067.44 | ▲ 09:30 equity $10,067.44 vs yday $10,067.44 (-0.00) | 09:30 open · cash $10,067.44 · no holdings · equity $10,067.44 vs prior close $10,067.44 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 150 | $16.77 | $2.44 | — | $7,549.50 | — | top 4 by cond; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy,oppset; 🔵; ⚪; ret5=+5.7; leftover $2516.86 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 1304 | $1.93 | $16.82 | — | $5,015.96 | — | top 4 by cond; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $2516.86 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 1154 | $2.18 | $14.89 | — | $2,485.35 | — | top 4 by cond; rank cond; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $2516.86 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 57 | $42.93 | $2.16 | — | $36.18 | — | top 4 by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $2516.86 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $36.18 | ▼ close $9,739.48 vs 09:30 $10,067.44 (session -291.65) | 16:00 close · cash $36.18 · equity $9,739.48 vs 09:30 $10,067.44 (-327.96; session marks -291.65) · 4 name(s) marked open→close (per-name table). ARCT×150 09:30 $16.77 → close $15.56 -181.50; BMEA×1304 09:30 $1.93 → close $1.91 -26.08; CRDL×1154 09:30 $2.18 → close $2.16 -23.08; HRMY×57 09:30 $42.93 → close $41.86 -60.99 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $36.18 | ▼ 09:30 equity $9,713.42 vs yday $9,739.48 (-26.06) | 09:30 open · cash $36.18 (unchanged overnight, no fees) · equity $9,713.42 vs prior close $9,739.48 (-26.06) · 4 name(s) re-marked at the open (per-name table). ARCT×150 yday $15.56 → 09:30 $15.61 +7.50; BMEA×1304 yday $1.91 → 09:30 $1.90 -13.04; CRDL×1154 yday $2.16 → 09:30 $2.16 +0.00; HRMY×57 yday $41.86 → 09:30 $41.50 -20.52 | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 2 | $3.46 | $0.08 | — | $29.18 | — | top 4 by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $9.04 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 3 | $2.52 | $0.08 | — | $21.54 | — | top 4 by cond; rank cond; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $9.04 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 1 | $6.71 | $0.07 | — | $14.76 | — | top 4 by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $9.04 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.76 | ▲ close $10,002.81 vs 09:30 $9,713.42 (session +289.62) | 16:00 close · cash $14.76 · equity $10,002.81 vs 09:30 $9,713.42 (+289.39; session marks +289.62) · 7 name(s) marked open→close (per-name table). ARCT×150 09:30 $15.61 → close $15.82 +31.50; BMEA×1304 09:30 $1.90 → close $2.03 +169.52; CRDL×1154 09:30 $2.16 → close $2.20 +46.16; HRMY×57 09:30 $41.50 → close $42.25 +42.75; CABA×2 09:30 $3.46 → close $3.47 +0.02; ALEC×3 09:30 $2.52 → close $2.46 -0.18; BHC×1 09:30 $6.71 → close $6.56 -0.15 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.76 | ▼ 09:30 equity $9,908.03 vs yday $10,002.81 (-94.78) | 09:30 open · cash $14.76 (unchanged overnight, no fees) · equity $9,908.03 vs prior close $10,002.81 (-94.78) · 7 name(s) re-marked at the open (per-name table). ARCT×150 yday $15.82 → 09:30 $15.47 -52.50; BMEA×1304 yday $2.03 → 09:30 $2.00 -39.12; CRDL×1154 yday $2.20 → 09:30 $2.20 +0.00; HRMY×57 yday $42.25 → 09:30 $42.20 -2.85; CABA×2 yday $3.47 → 09:30 $3.43 -0.08; ALEC×3 yday $2.46 → 09:30 $2.38 -0.24; BHC×1 yday $6.56 → 09:30 $6.57 +0.01 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.76 | ▼ close $9,856.23 vs 09:30 $9,908.03 (session -51.80) | 16:00 close · cash $14.76 · equity $9,856.23 vs 09:30 $9,908.03 (-51.80; session marks -51.80) · 7 name(s) marked open→close (per-name table). ARCT×150 09:30 $15.47 → close $15.63 +24.00; BMEA×1304 09:30 $2.00 → close $1.93 -91.28; CRDL×1154 09:30 $2.20 → close $2.22 +23.08; HRMY×57 09:30 $42.20 → close $42.07 -7.41; CABA×2 09:30 $3.43 → close $3.27 -0.32; ALEC×3 09:30 $2.38 → close $2.47 +0.27; BHC×1 09:30 $6.57 → close $6.43 -0.14 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.76 | ▼ 09:30 equity $9,840.32 vs yday $9,856.23 (-15.91) | 09:30 open · cash $14.76 (unchanged overnight, no fees) · equity $9,840.32 vs prior close $9,856.23 (-15.91) · 7 name(s) re-marked at the open (per-name table). ARCT×150 yday $15.63 → 09:30 $15.46 -25.50; BMEA×1304 yday $1.93 → 09:30 $1.94 +13.04; CRDL×1154 yday $2.22 → 09:30 $2.22 +0.00; HRMY×57 yday $42.07 → 09:30 $42.01 -3.42; CABA×2 yday $3.27 → 09:30 $3.28 +0.02; ALEC×3 yday $2.47 → 09:30 $2.47 +0.00; BHC×1 yday $6.43 → 09:30 $6.38 -0.05 | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 150 | $15.46 | $2.48 | $-201.42 | $2,331.28 | ▼ -201.42 after sell → book $9,837.84; vs 09:30 mark -2.48 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `BMEA` | 1304 | $1.94 | $17.06 | $-20.84 | $4,843.98 | ▼ -20.84 after sell → book $9,820.78; vs 09:30 mark -17.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRDL` | 1154 | $2.22 | $15.10 | $+16.17 | $7,390.76 | ▲ +16.17 after sell → book $9,805.68; vs 09:30 mark -15.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 57 | $42.01 | $2.19 | $-56.79 | $9,783.14 | ▼ -56.79 after sell → book $9,803.49; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,783.14 | ▼ close $9,801.93 vs 09:30 $9,840.32 (session -1.56) | 16:00 close · cash $9,783.14 · equity $9,801.93 vs 09:30 $9,840.32 (-38.39; session marks -1.56) · 3 name(s) marked open→close (per-name table). CABA×2 09:30 $3.28 → close $2.91 -0.74; ALEC×3 09:30 $2.47 → close $2.27 -0.60; BHC×1 09:30 $6.38 → close $6.16 -0.22 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,783.14 | ▼ 09:30 equity $9,801.61 vs yday $9,801.93 (-0.32) | 09:30 open · cash $9,783.14 (unchanged overnight, no fees) · equity $9,801.61 vs prior close $9,801.93 (-0.32) · 3 name(s) re-marked at the open (per-name table). CABA×2 yday $2.91 → 09:30 $2.85 -0.12; ALEC×3 yday $2.27 → 09:30 $2.22 -0.15; BHC×1 yday $6.16 → 09:30 $6.11 -0.05 | — |
| 2026-09-10 09:30 ET | **SELL** | `CABA` | 2 | $2.85 | $0.08 | $-1.38 | $9,788.76 | ▼ -1.38 after sell → book $9,801.53; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `ALEC` | 3 | $2.22 | $0.10 | $-1.08 | $9,795.32 | ▼ -1.08 after sell → book $9,801.43; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BHC` | 1 | $6.11 | $0.08 | $-0.75 | $9,801.35 | ▼ -0.75 after sell → book $9,801.35; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,801.35 | ▲ close $9,801.35 vs 09:30 $9,801.61 (session +0.00) | 16:00 close · cash $9,801.35 · no lots left · equity $9,801.35. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,801.35 | ▲ 09:30 equity $9,801.35 vs yday $9,801.35 (-0.00) | 09:30 open · cash $9,801.35 · no holdings · equity $9,801.35 vs prior close $9,801.35 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `SEDG` | 66 | $36.78 | $2.19 | — | $7,371.68 | — | top 4 by cond; rank cond; list oppset; 🔵; ⚪; ret5=+8.2; leftover $2450.34 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 46 | $52.55 | $2.13 | — | $4,952.25 | — | top 4 by cond; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $2450.34 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 14 | $164.43 | $2.03 | — | $2,648.20 | — | top 4 by cond; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $2450.34 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `PAGS` | 242 | $10.11 | $3.12 | — | $198.46 | — | top 4 by cond; rank cond; list yday_gainer; 🔵; ⚪; ret5=+2.9; leftover $2450.34 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $198.46 | ▼ close $9,656.32 vs 09:30 $9,801.35 (session -135.56) | 16:00 close · cash $198.46 · equity $9,656.32 vs 09:30 $9,801.35 (-145.03; session marks -135.56) · 4 name(s) marked open→close (per-name table). SEDG×66 09:30 $36.78 → close $34.68 -138.60; BAND×46 09:30 $52.55 → close $56.87 +198.72; ORCL×14 09:30 $164.43 → close $150.28 -198.10; PAGS×242 09:30 $10.11 → close $10.12 +2.42 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $198.46 | ▼ 09:30 equity $9,435.98 vs yday $9,656.32 (-220.34) | 09:30 open · cash $198.46 (unchanged overnight, no fees) · equity $9,435.98 vs prior close $9,656.32 (-220.34) · 4 name(s) re-marked at the open (per-name table). SEDG×66 yday $34.68 → 09:30 $33.64 -68.64; BAND×46 yday $56.87 → 09:30 $56.90 +1.38; ORCL×14 yday $150.28 → 09:30 $141.42 -124.04; PAGS×242 yday $10.12 → 09:30 $10.00 -29.04 | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $198.46 | ▼ close $9,212.98 vs 09:30 $9,435.98 (session -223.00) | 16:00 close · cash $198.46 · equity $9,212.98 vs 09:30 $9,435.98 (-223.00; session marks -223.00) · 4 name(s) marked open→close (per-name table). SEDG×66 09:30 $33.64 → close $35.33 +111.54; BAND×46 09:30 $56.90 → close $48.97 -364.78; ORCL×14 09:30 $141.42 → close $144.79 +47.18; PAGS×242 09:30 $10.00 → close $9.93 -16.94 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $198.46 | ▲ 09:30 equity $9,215.68 vs yday $9,212.98 (+2.70) | 09:30 open · cash $198.46 (unchanged overnight, no fees) · equity $9,215.68 vs prior close $9,212.98 (+2.70) · 4 name(s) re-marked at the open (per-name table). SEDG×66 yday $35.33 → 09:30 $35.24 -5.94; BAND×46 yday $48.97 → 09:30 $49.51 +24.84; ORCL×14 yday $144.79 → 09:30 $143.46 -18.62; PAGS×242 yday $9.93 → 09:30 $9.94 +2.42 | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $198.46 | ▼ close $9,122.24 vs 09:30 $9,215.68 (session -93.44) | 16:00 close · cash $198.46 · equity $9,122.24 vs 09:30 $9,215.68 (-93.44; session marks -93.44) · 4 name(s) marked open→close (per-name table). SEDG×66 09:30 $35.24 → close $35.26 +1.32; BAND×46 09:30 $49.51 → close $50.08 +26.22; ORCL×14 09:30 $143.46 → close $140.35 -43.54; PAGS×242 09:30 $9.94 → close $9.62 -77.44 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $198.46 | ▼ 09:30 equity $9,038.24 vs yday $9,122.24 (-84.00) | 09:30 open · cash $198.46 (unchanged overnight, no fees) · equity $9,038.24 vs prior close $9,122.24 (-84.00) · 4 name(s) re-marked at the open (per-name table). SEDG×66 yday $35.26 → 09:30 $35.93 +44.22; BAND×46 yday $50.08 → 09:30 $48.60 -68.08; ORCL×14 yday $140.35 → 09:30 $140.03 -4.48; PAGS×242 yday $9.62 → 09:30 $9.39 -55.66 | — |
| 2026-09-16 09:30 ET | **SELL** | `SEDG` | 66 | $35.93 | $2.22 | $-60.51 | $2,567.62 | ▼ -60.51 after sell → book $9,036.02; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BAND` | 46 | $48.60 | $2.16 | $-185.98 | $4,801.06 | ▼ -185.98 after sell → book $9,033.86; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 14 | $140.03 | $2.06 | $-345.69 | $6,759.43 | ▼ -345.69 after sell → book $9,031.81; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `PAGS` | 242 | $9.39 | $3.18 | $-180.54 | $9,028.63 | ▼ -180.54 after sell → book $9,028.63; vs 09:30 mark -3.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `QRVO` | 19 | $118.18 | $2.05 | — | $6,781.16 | — | top 4 by cond; rank cond; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $2257.16 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 25 | $89.38 | $2.06 | — | $4,544.59 | — | top 4 by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $2257.16 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `ATRC` | 40 | $55.66 | $2.11 | — | $2,316.08 | — | top 4 by cond; rank cond; list ohlc_hot; 🔵; ret5=+4.6; leftover $2257.16 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `BWIN` | 69 | $32.25 | $2.20 | — | $88.64 | — | top 4 by cond; rank cond; list oppset; 🔵; ret5=-3.7; leftover $2257.16 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $88.64 | ▼ close $8,890.18 vs 09:30 $9,038.24 (session -130.03) | 16:00 close · cash $88.64 · equity $8,890.18 vs 09:30 $9,038.24 (-148.06; session marks -130.03) · 4 name(s) marked open→close (per-name table). QRVO×19 09:30 $118.18 → close $113.97 -79.99; SWKS×25 09:30 $89.38 → close $85.59 -94.75; ATRC×40 09:30 $55.66 → close $57.14 +59.20; BWIN×69 09:30 $32.25 → close $32.04 -14.49 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $88.64 | ▲ 09:30 equity $8,971.28 vs yday $8,890.18 (+81.10) | 09:30 open · cash $88.64 (unchanged overnight, no fees) · equity $8,971.28 vs prior close $8,890.18 (+81.10) · 4 name(s) re-marked at the open (per-name table). QRVO×19 yday $113.97 → 09:30 $114.90 +17.67; SWKS×25 yday $85.59 → 09:30 $86.76 +29.25; ATRC×40 yday $57.14 → 09:30 $57.96 +32.80; BWIN×69 yday $32.04 → 09:30 $32.06 +1.38 | — |
| 2026-09-17 09:30 ET | **BUY** | `ASAN` | 2 | $9.55 | $0.20 | — | $69.34 | — | top 4 by cond; rank cond; list oppset; 🔵; ⚪; ret5=+17.0; leftover $22.16 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `BULL` | 2 | $7.95 | $0.17 | — | $53.27 | — | top 4 by cond; rank cond; list yday_mover; 🔵; ⚪; ret5=-18.4; leftover $22.16 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `CIFR` | 1 | $18.04 | $0.18 | — | $35.06 | — | top 4 by cond; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.1; leftover $22.16 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 9 | $2.40 | $0.24 | — | $13.21 | — | top 4 by cond; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $22.16 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.21 | ▲ close $9,209.67 vs 09:30 $8,971.28 (session +239.19) | 16:00 close · cash $13.21 · equity $9,209.67 vs 09:30 $8,971.28 (+238.39; session marks +239.19) · 8 name(s) marked open→close (per-name table). QRVO×19 09:30 $114.90 → close $119.51 +87.59; SWKS×25 09:30 $86.76 → close $91.32 +114.00; ATRC×40 09:30 $57.96 → close $59.12 +46.40; BWIN×69 09:30 $32.06 → close $31.95 -7.59; ASAN×2 09:30 $9.55 → close $10.09 +1.08; BULL×2 09:30 $7.95 → close $7.71 -0.48; CIFR×1 09:30 $18.04 → close $16.94 -1.09; SABR×9 09:30 $2.40 → close $2.32 -0.72 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.21 | ▲ 09:30 equity $9,230.21 vs yday $9,209.67 (+20.54) | 09:30 open · cash $13.21 (unchanged overnight, no fees) · equity $9,230.21 vs prior close $9,209.67 (+20.54) · 8 name(s) re-marked at the open (per-name table). QRVO×19 yday $119.51 → 09:30 $120.76 +23.75; SWKS×25 yday $91.32 → 09:30 $92.05 +18.25; ATRC×40 yday $59.12 → 09:30 $58.51 -24.40; BWIN×69 yday $31.95 → 09:30 $31.98 +2.07; ASAN×2 yday $10.09 → 09:30 $10.09 +0.00; BULL×2 yday $7.71 → 09:30 $7.85 +0.28; CIFR×1 yday $16.94 → 09:30 $17.80 +0.86; SABR×9 yday $2.32 → 09:30 $2.29 -0.27 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.21 | ▼ close $9,060.00 vs 09:30 $9,230.21 (session -170.21) | 16:00 close · cash $13.21 · equity $9,060.00 vs 09:30 $9,230.21 (-170.21; session marks -170.21) · 8 name(s) marked open→close (per-name table). QRVO×19 09:30 $120.76 → close $117.18 -68.02; SWKS×25 09:30 $92.05 → close $88.76 -82.25; ATRC×40 09:30 $58.51 → close $58.10 -16.40; BWIN×69 09:30 $31.98 → close $31.93 -3.45; ASAN×2 09:30 $10.09 → close $9.51 -1.16; BULL×2 09:30 $7.85 → close $8.25 +0.80; CIFR×1 09:30 $17.80 → close $18.34 +0.54; SABR×9 09:30 $2.29 → close $2.26 -0.27 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `BRUN` | cash | leftover split 6.67 < 1 share @ 26.25 |
| 2026-08-14 | `CLBT` | cash | leftover split 6.67 < 1 share @ 10.83 |
| 2026-08-14 | `HLIT` | cash | leftover split 6.67 < 1 share @ 13.18 |
| 2026-08-14 | `MNTN` | cash | leftover split 6.67 < 1 share @ 12.50 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `LPTH` | cash | leftover split 6.67 < 1 share @ 14.94 |
| 2026-08-17 | `DVN` | cash | leftover split 6.67 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 6.67 < 1 share @ 142.77 |
| 2026-08-18 | `VERI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `RLX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `VERI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BHP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BSBR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 10.54 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 10.54 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 10.54 < 1 share @ 216.30 |
| 2026-08-21 | `ARCT` | cash | leftover split 10.54 < 1 share @ 11.13 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ERO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CNH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FNV` | cash | leftover split 24.08 < 1 share @ 267.02 |
| 2026-08-26 | `MOS` | cash | leftover split 24.08 < 1 share @ 24.84 |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ERO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CNH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `DBRG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `ACMR` | cash | leftover split 14.81 < 1 share @ 81.65 |
| 2026-08-27 | `MT` | cash | leftover split 14.81 < 1 share @ 74.54 |
| 2026-08-27 | `MU` | cash | leftover split 14.81 < 1 share @ 967.01 |
| 2026-08-28 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `DBRG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `GGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CIEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACIW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CIEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `MPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ALAB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ACB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ADM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ASND` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ATRC` | cash | leftover split 9.04 < 1 share @ 52.03 |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BHC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `LOGI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `VNT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BHC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `GLW` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SWKS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `BKV` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CVI` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BAND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `PAGS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `AVT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DBRG` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BAND` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `PAGS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PANW` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PBF` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `QRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SWKS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ATRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `BWIN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `QRVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SWKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BWIN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BULL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `CIFR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `SABR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `S` | cash | leftover split 6.61 < 1 share @ 23.13 |
| 2026-09-18 | `CRWV` | cash | leftover split 6.61 < 1 share @ 79.83 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `QRVO` | 19 | 2026-09-16 @ $118.18 | top 4 by cond; rank cond; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $2257.16 |
| `SWKS` | 25 | 2026-09-16 @ $89.38 | top 4 by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $2257.16 |
| `ATRC` | 40 | 2026-09-16 @ $55.66 | top 4 by cond; rank cond; list ohlc_hot; 🔵; ret5=+4.6; leftover $2257.16 |
| `BWIN` | 69 | 2026-09-16 @ $32.25 | top 4 by cond; rank cond; list oppset; 🔵; ret5=-3.7; leftover $2257.16 |
| `ASAN` | 2 | 2026-09-17 @ $9.55 | top 4 by cond; rank cond; list oppset; 🔵; ⚪; ret5=+17.0; leftover $22.16 |
| `BULL` | 2 | 2026-09-17 @ $7.95 | top 4 by cond; rank cond; list yday_mover; 🔵; ⚪; ret5=-18.4; leftover $22.16 |
| `CIFR` | 1 | 2026-09-17 @ $18.04 | top 4 by cond; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.1; leftover $22.16 |
| `SABR` | 9 | 2026-09-17 @ $2.40 | top 4 by cond; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $22.16 |
