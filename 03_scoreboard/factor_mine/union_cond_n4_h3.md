# Factor mine action — `union_cond_n4_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · top 4 by cond

Cash book **+2.29%** ($10,229) · signal-only (no cash/fees) was +8.26%. Starts YES **6/21**. Fills 46 · skips 104 · realized $+124.02.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $5,138.37.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `BTSG` | 41 | — | $59.80 | +0.00 | $60.23 | +17.63 | +17.63 | +0.00 | +17.63 |
| 2026-08-13 | `HIMS` | 84 | — | $29.74 | +0.00 | $28.77 | -81.48 | -81.48 | +0.00 | -81.48 |
| 2026-08-13 | `INO` | 3086 | — | $0.81 | +0.00 | $0.90 | +277.74 | +277.74 | +0.00 | +277.74 |
| 2026-08-13 | `SLS` | 213 | — | $11.70 | +0.00 | $12.36 | +140.58 | +140.58 | +0.00 | +140.58 |
| 2026-08-14 | `BTSG` | 41 | $60.23 | $59.65 | -23.78 | $61.71 | +84.46 | +60.68 | -6.15 | +78.31 |
| 2026-08-14 | `HIMS` | 84 | $28.77 | $29.15 | +31.92 | $28.15 | -84.00 | -52.08 | -49.56 | -133.56 |
| 2026-08-14 | `INO` | 3086 | $0.90 | $0.93 | +92.58 | $1.09 | +493.76 | +586.34 | +370.32 | +864.08 |
| 2026-08-14 | `SLS` | 213 | $12.36 | $12.40 | +8.52 | $12.78 | +80.94 | +89.46 | +149.10 | +230.04 |
| 2026-08-17 | `BTSG` | 41 | $61.71 | $61.69 | -0.82 | $60.38 | -53.71 | -54.53 | +77.49 | +23.78 |
| 2026-08-17 | `HIMS` | 84 | $28.15 | $28.14 | -0.84 | $28.61 | +39.48 | +38.64 | -134.40 | -94.92 |
| 2026-08-17 | `INO` | 3086 | $1.09 | $1.07 | -61.72 | $1.15 | +246.88 | +185.16 | +802.36 | +1049.24 |
| 2026-08-17 | `SLS` | 213 | $12.78 | $12.78 | +0.00 | $13.00 | +46.86 | +46.86 | +230.04 | +276.90 |
| 2026-08-17 | `VERI` | 3 | — | $1.15 | +0.00 | $1.08 | -0.19 | -0.19 | +0.00 | -0.19 |
| 2026-08-18 | `BTSG` | 41 | $60.38 | $60.00 | -15.58 | — | +0.00 | -15.58 | +8.20 | — |
| 2026-08-18 | `HIMS` | 84 | $28.61 | $27.85 | -63.84 | — | +0.00 | -63.84 | -158.76 | — |
| 2026-08-18 | `INO` | 3086 | $1.15 | $1.14 | -30.86 | — | +0.00 | -30.86 | +1018.38 | — |
| 2026-08-18 | `SLS` | 213 | $13.00 | $12.66 | -72.42 | — | +0.00 | -72.42 | +204.48 | — |
| 2026-08-18 | `VERI` | 3 | $1.08 | $1.05 | -0.10 | $0.99 | -0.17 | -0.27 | -0.30 | -0.46 |
| 2026-08-19 | `VERI` | 3 | $0.99 | $1.00 | +0.02 | $0.97 | -0.10 | -0.08 | -0.45 | -0.55 |
| 2026-08-20 | `VERI` | 3 | $0.97 | $0.96 | -0.01 | — | +0.00 | -0.01 | -0.56 | — |
| 2026-08-20 | `AG` | 133 | — | $20.55 | +0.00 | $21.19 | +85.12 | +85.12 | +0.00 | +85.12 |
| 2026-08-20 | `BHP` | 30 | — | $91.01 | +0.00 | $93.63 | +78.60 | +78.60 | +0.00 | +78.60 |
| 2026-08-20 | `CDE` | 132 | — | $20.65 | +0.00 | $21.11 | +60.72 | +60.72 | +0.00 | +60.72 |
| 2026-08-20 | `HDSN` | 475 | — | $5.77 | +0.00 | $5.57 | -95.00 | -95.00 | +0.00 | -95.00 |
| 2026-08-21 | `AG` | 133 | $21.19 | $21.90 | +94.43 | $21.09 | -107.73 | -13.30 | +179.55 | +71.82 |
| 2026-08-21 | `BHP` | 30 | $93.63 | $95.72 | +62.70 | $97.03 | +39.30 | +102.00 | +141.30 | +180.60 |
| 2026-08-21 | `CDE` | 132 | $21.11 | $21.75 | +84.48 | $20.97 | -102.96 | -18.48 | +145.20 | +42.24 |
| 2026-08-21 | `HDSN` | 475 | $5.57 | $5.67 | +47.50 | $5.63 | -19.00 | +28.50 | -47.50 | -66.50 |
| 2026-08-24 | `AG` | 133 | $21.09 | $21.30 | +27.93 | $20.83 | -62.51 | -34.58 | +99.75 | +37.24 |
| 2026-08-24 | `BHP` | 30 | $97.03 | $97.31 | +8.40 | $97.13 | -5.40 | +3.00 | +189.00 | +183.60 |
| 2026-08-24 | `CDE` | 132 | $20.97 | $21.26 | +38.28 | $20.88 | -50.16 | -11.88 | +80.52 | +30.36 |
| 2026-08-24 | `HDSN` | 475 | $5.63 | $5.69 | +28.50 | $5.52 | -80.75 | -52.25 | -38.00 | -118.75 |
| 2026-08-25 | `AG` | 133 | $20.83 | $20.32 | -67.83 | — | +0.00 | -67.83 | -30.59 | — |
| 2026-08-25 | `BHP` | 30 | $97.13 | $95.86 | -38.10 | — | +0.00 | -38.10 | +145.50 | — |
| 2026-08-25 | `CDE` | 132 | $20.88 | $20.47 | -54.12 | — | +0.00 | -54.12 | -23.76 | — |
| 2026-08-25 | `HDSN` | 475 | $5.52 | $5.53 | +4.75 | — | +0.00 | +4.75 | -114.00 | — |
| 2026-08-25 | `AU` | 23 | — | $118.52 | +0.00 | $123.39 | +112.01 | +112.01 | +0.00 | +112.01 |
| 2026-08-25 | `ERO` | 71 | — | $38.01 | +0.00 | $40.40 | +169.69 | +169.69 | +0.00 | +169.69 |
| 2026-08-25 | `FCX` | 35 | — | $77.13 | +0.00 | $79.91 | +97.30 | +97.30 | +0.00 | +97.30 |
| 2026-08-25 | `CNH` | 229 | — | $11.90 | +0.00 | $11.56 | -77.86 | -77.86 | +0.00 | -77.86 |
| 2026-08-26 | `AU` | 23 | $123.39 | $119.80 | -82.57 | $118.11 | -38.87 | -121.44 | +29.44 | -9.43 |
| 2026-08-26 | `ERO` | 71 | $40.40 | $40.51 | +7.81 | $39.24 | -90.17 | -82.36 | +177.50 | +87.33 |
| 2026-08-26 | `FCX` | 35 | $79.91 | $79.34 | -19.95 | $79.00 | -11.90 | -31.85 | +77.35 | +65.45 |
| 2026-08-26 | `CNH` | 229 | $11.56 | $11.54 | -4.58 | $11.62 | +18.32 | +13.74 | -82.44 | -64.12 |
| 2026-08-27 | `AU` | 23 | $118.11 | $117.41 | -16.10 | $118.40 | +22.77 | +6.67 | -25.53 | -2.76 |
| 2026-08-27 | `ERO` | 71 | $39.24 | $39.20 | -2.84 | $39.82 | +44.02 | +41.18 | +84.49 | +128.51 |
| 2026-08-27 | `FCX` | 35 | $79.00 | $78.83 | -5.95 | $78.42 | -14.35 | -20.30 | +59.50 | +45.15 |
| 2026-08-27 | `CNH` | 229 | $11.62 | $11.62 | +0.00 | $11.43 | -43.51 | -43.51 | -64.12 | -107.63 |
| 2026-08-27 | `GGB` | 4 | — | $4.57 | +0.00 | $4.70 | +0.52 | +0.52 | +0.00 | +0.52 |
| 2026-08-28 | `AU` | 23 | $118.40 | $119.19 | +18.17 | — | +0.00 | +18.17 | +15.41 | — |
| 2026-08-28 | `ERO` | 71 | $39.82 | $40.12 | +21.30 | — | +0.00 | +21.30 | +149.81 | — |
| 2026-08-28 | `FCX` | 35 | $78.42 | $78.57 | +5.25 | — | +0.00 | +5.25 | +50.40 | — |
| 2026-08-28 | `CNH` | 229 | $11.43 | $11.55 | +27.48 | — | +0.00 | +27.48 | -80.15 | — |
| 2026-08-28 | `GGB` | 4 | $4.70 | $4.67 | -0.12 | $4.59 | -0.32 | -0.44 | +0.40 | +0.08 |
| 2026-08-28 | `KEYS` | 8 | — | $324.41 | +0.00 | $319.97 | -35.52 | -35.52 | +0.00 | -35.52 |
| 2026-08-28 | `SMTC` | 19 | — | $141.76 | +0.00 | $131.17 | -201.21 | -201.21 | +0.00 | -201.21 |
| 2026-08-28 | `CIEN` | 6 | — | $400.42 | +0.00 | $378.44 | -131.88 | -131.88 | +0.00 | -131.88 |
| 2026-08-28 | `MPWR` | 2 | — | $1306.03 | +0.00 | $1256.26 | -99.54 | -99.54 | +0.00 | -99.54 |
| 2026-08-31 | `GGB` | 4 | $4.59 | $4.67 | +0.32 | $4.61 | -0.24 | +0.08 | +0.40 | +0.16 |
| 2026-08-31 | `KEYS` | 8 | $319.97 | $322.49 | +20.16 | $322.70 | +1.68 | +21.84 | -15.36 | -13.68 |
| 2026-08-31 | `SMTC` | 19 | $131.17 | $132.30 | +21.47 | $132.96 | +12.54 | +34.01 | -179.74 | -167.20 |
| 2026-08-31 | `CIEN` | 6 | $378.44 | $378.44 | +0.00 | $382.80 | +26.16 | +26.16 | -131.88 | -105.72 |
| 2026-08-31 | `MPWR` | 2 | $1256.26 | $1261.90 | +11.28 | $1267.77 | +11.74 | +23.02 | -88.26 | -76.52 |
| 2026-09-01 | `GGB` | 4 | $4.61 | $4.57 | -0.16 | — | +0.00 | -0.16 | +0.00 | — |
| 2026-09-01 | `KEYS` | 8 | $322.70 | $321.47 | -9.84 | $319.27 | -17.60 | -27.44 | -23.52 | -41.12 |
| 2026-09-01 | `SMTC` | 19 | $132.96 | $127.63 | -101.27 | $132.27 | +88.16 | -13.11 | -268.47 | -180.31 |
| 2026-09-01 | `CIEN` | 6 | $382.80 | $376.89 | -35.46 | $360.33 | -99.36 | -134.82 | -141.18 | -240.54 |
| 2026-09-01 | `MPWR` | 2 | $1267.77 | $1245.11 | -45.32 | $1225.96 | -38.30 | -83.62 | -121.84 | -160.14 |
| 2026-09-02 | `KEYS` | 8 | $319.27 | $318.04 | -9.84 | — | +0.00 | -9.84 | -50.96 | — |
| 2026-09-02 | `SMTC` | 19 | $132.27 | $133.00 | +13.87 | — | +0.00 | +13.87 | -166.44 | — |
| 2026-09-02 | `CIEN` | 6 | $360.33 | $357.25 | -18.48 | — | +0.00 | -18.48 | -259.02 | — |
| 2026-09-02 | `MPWR` | 2 | $1225.96 | $1224.92 | -2.08 | — | +0.00 | -2.08 | -162.22 | — |
| 2026-09-03 | `ARCT` | 154 | — | $16.77 | +0.00 | $15.56 | -186.34 | -186.34 | +0.00 | -186.34 |
| 2026-09-03 | `BMEA` | 1346 | — | $1.93 | +0.00 | $1.91 | -26.92 | -26.92 | +0.00 | -26.92 |
| 2026-09-03 | `CRDL` | 1192 | — | $2.18 | +0.00 | $2.16 | -23.84 | -23.84 | +0.00 | -23.84 |
| 2026-09-03 | `HRMY` | 60 | — | $42.93 | +0.00 | $41.86 | -64.20 | -64.20 | +0.00 | -64.20 |
| 2026-09-04 | `ARCT` | 154 | $15.56 | $15.61 | +7.70 | $15.82 | +32.34 | +40.04 | -178.64 | -146.30 |
| 2026-09-04 | `BMEA` | 1346 | $1.91 | $1.90 | -13.46 | $2.03 | +174.98 | +161.52 | -40.38 | +134.60 |
| 2026-09-04 | `CRDL` | 1192 | $2.16 | $2.16 | +0.00 | $2.20 | +47.68 | +47.68 | -23.84 | +23.84 |
| 2026-09-04 | `HRMY` | 60 | $41.86 | $41.50 | -21.60 | $42.25 | +45.00 | +23.40 | -85.80 | -40.80 |
| 2026-09-08 | `ARCT` | 154 | $15.82 | $15.47 | -53.90 | $15.63 | +24.64 | -29.26 | -200.20 | -175.56 |
| 2026-09-08 | `BMEA` | 1346 | $2.03 | $2.00 | -40.38 | $1.93 | -94.22 | -134.60 | +94.22 | +0.00 |
| 2026-09-08 | `CRDL` | 1192 | $2.20 | $2.20 | +0.00 | $2.22 | +23.84 | +23.84 | +23.84 | +47.68 |
| 2026-09-08 | `HRMY` | 60 | $42.25 | $42.20 | -3.00 | $42.07 | -7.80 | -10.80 | -43.80 | -51.60 |
| 2026-09-09 | `ARCT` | 154 | $15.63 | $15.46 | -26.18 | — | +0.00 | -26.18 | -201.74 | — |
| 2026-09-09 | `BMEA` | 1346 | $1.93 | $1.94 | +13.46 | — | +0.00 | +13.46 | +13.46 | — |
| 2026-09-09 | `CRDL` | 1192 | $2.22 | $2.22 | +0.00 | — | +0.00 | +0.00 | +47.68 | — |
| 2026-09-09 | `HRMY` | 60 | $42.07 | $42.01 | -3.60 | — | +0.00 | -3.60 | -55.20 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `HPE` | 44 | — | $56.37 | +0.00 | $62.09 | +251.68 | +251.68 | +0.00 | +251.68 |
| 2026-09-11 | `SEDG` | 68 | — | $36.78 | +0.00 | $34.68 | -142.80 | -142.80 | +0.00 | -142.80 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +354.47 | BTSG, HIMS, INO, SLS | — | $16.92 | $10,313.11 | BTSG×41, HIMS×84, INO×3086, SLS×213 |
| 2026-08-14 | +5.50 | $16.92 | BTSG×41, HIMS×84, INO×3086, SLS×213 | $10,422.35 | +109.24 | +575.16 | — | — | $16.92 | $10,997.51 | BTSG×41, HIMS×84, INO×3086, SLS×213 |
| 2026-08-17 | +2.25 | $16.92 | BTSG×41, HIMS×84, INO×3086, SLS×213 | $10,934.13 | -63.38 | +279.32 | VERI | — | $13.43 | $11,213.40 | BTSG×41, HIMS×84, INO×3086, SLS×213, VERI×3 |
| 2026-08-18 | -6.20 | $13.43 | BTSG×41, HIMS×84, INO×3086, SLS×213, VERI×3 | $11,030.60 | -182.80 | -0.17 | — | BTSG, HIMS, INO, SLS | $10,979.88 | $10,982.86 | VERI×3 |
| 2026-08-19 | -7.20 | $10,979.88 | VERI×3 | $10,982.88 | +0.02 | -0.10 | — | — | $10,979.88 | $10,982.78 | VERI×3 |
| 2026-08-20 | +1.12 | $10,979.88 | VERI×3 | $10,982.77 | -0.01 | +129.44 | AG, BHP, CDE, HDSN | VERI | $39.73 | $11,099.17 | AG×133, BHP×30, CDE×132, HDSN×475 |
| 2026-08-21 | +3.25 | $39.73 | AG×133, BHP×30, CDE×132, HDSN×475 | $11,388.28 | +289.11 | -190.39 | — | — | $39.73 | $11,197.89 | AG×133, BHP×30, CDE×132, HDSN×475 |
| 2026-08-24 | -5.17 | $39.73 | AG×133, BHP×30, CDE×132, HDSN×475 | $11,301.00 | +103.11 | -198.82 | — | — | $39.73 | $11,102.18 | AG×133, BHP×30, CDE×132, HDSN×475 |
| 2026-08-25 | +1.80 | $39.73 | AG×133, BHP×30, CDE×132, HDSN×475 | $10,946.88 | -155.30 | +301.14 | AU, ERO, FCX, CNH | AG, BHP, CDE, HDSN | $75.04 | $11,225.50 | AU×23, ERO×71, FCX×35, CNH×229 |
| 2026-08-26 | +2.02 | $75.04 | AU×23, ERO×71, FCX×35, CNH×229 | $11,126.21 | -99.29 | -122.62 | — | — | $75.04 | $11,003.59 | AU×23, ERO×71, FCX×35, CNH×229 |
| 2026-08-27 | — | $75.04 | AU×23, ERO×71, FCX×35, CNH×229 | $10,978.70 | -24.89 | +9.45 | GGB | — | $56.57 | $10,987.96 | AU×23, ERO×71, FCX×35, CNH×229, GGB×4 |
| 2026-08-28 | +0.75 | $56.57 | AU×23, ERO×71, FCX×35, CNH×229, GGB×4 | $11,060.04 | +72.08 | -468.47 | KEYS, SMTC, CIEN, MPWR | AU, ERO, FCX, CNH | $720.52 | $10,574.03 | GGB×4, KEYS×8, SMTC×19, CIEN×6, MPWR×2 |
| 2026-08-31 | -5.85 | $720.52 | GGB×4, KEYS×8, SMTC×19, CIEN×6, MPWR×2 | $10,627.26 | +53.23 | +51.88 | — | — | $720.52 | $10,679.14 | GGB×4, KEYS×8, SMTC×19, CIEN×6, MPWR×2 |
| 2026-09-01 | -6.30 | $720.52 | GGB×4, KEYS×8, SMTC×19, CIEN×6, MPWR×2 | $10,487.09 | -192.05 | -67.10 | — | GGB | $738.59 | $10,419.78 | KEYS×8, SMTC×19, CIEN×6, MPWR×2 |
| 2026-09-02 | -3.83 | $738.59 | KEYS×8, SMTC×19, CIEN×6, MPWR×2 | $10,403.25 | -16.53 | +0.00 | — | KEYS, SMTC, CIEN, MPWR | $10,395.07 | $10,395.07 | — |
| 2026-09-03 | -0.90 | $10,395.07 | — | $10,395.07 | -0.00 | -301.30 | ARCT, BMEA, CRDL, HRMY | — | $2.98 | $10,056.40 | ARCT×154, BMEA×1346, CRDL×1192, HRMY×60 |
| 2026-09-04 | +2.25 | $2.98 | ARCT×154, BMEA×1346, CRDL×1192, HRMY×60 | $10,029.04 | -27.36 | +300.00 | — | — | $2.98 | $10,329.04 | ARCT×154, BMEA×1346, CRDL×1192, HRMY×60 |
| 2026-09-08 | -11.47 | $2.98 | ARCT×154, BMEA×1346, CRDL×1192, HRMY×60 | $10,231.76 | -97.28 | -53.54 | — | — | $2.98 | $10,178.22 | ARCT×154, BMEA×1346, CRDL×1192, HRMY×60 |
| 2026-09-09 | -13.95 | $2.98 | ARCT×154, BMEA×1346, CRDL×1192, HRMY×60 | $10,161.90 | -16.32 | +0.00 | — | ARCT, BMEA, CRDL, HRMY | $10,124.00 | $10,124.00 | — |
| 2026-09-10 | -13.28 | $10,124.00 | — | $10,124.00 | +0.00 | +0.00 | — | — | $10,124.00 | $10,124.00 | — |
| 2026-09-11 | +0.50 | $10,124.00 | — | $10,124.00 | +0.00 | +108.88 | HPE, SEDG | — | $5,138.37 | $10,228.57 | HPE×44, SEDG×68 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 41 | $59.80 | $2.11 | — | $7,546.09 | — | top 4 by cond; rank cond; list flatten; ⚪; ret5=-5.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 84 | $29.74 | $2.24 | — | $5,045.69 | — | top 4 by cond; rank cond; list flatten; ⚪; ret5=-5.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 3086 | $0.81 | $34.25 | — | $2,511.77 | — | top 4 by cond; rank cond; list flatten; ⚪; ret5=+13.2; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 213 | $11.70 | $2.75 | — | $16.92 | — | top 4 by cond; rank cond; list flatten; ⚪; ret5=-0.8; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.92 | ▲ close $10,313.11 vs 09:30 $10,000.00 (session +354.47) | 16:00 close · cash $16.92 · equity $10,313.11 vs 09:30 $10,000.00 (+313.11; session marks +354.47) · 4 name(s) marked open→close (per-name table). BTSG×41 09:30 $59.80 → close $60.23 +17.63; HIMS×84 09:30 $29.74 → close $28.77 -81.48; INO×3086 09:30 $0.81 → close $0.90 +277.74; SLS×213 09:30 $11.70 → close $12.36 +140.58 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.92 | ▲ 09:30 equity $10,422.35 vs yday $10,313.11 (+109.24) | 09:30 open · cash $16.92 (unchanged overnight, no fees) · equity $10,422.35 vs prior close $10,313.11 (+109.24) · 4 name(s) re-marked at the open (per-name table). BTSG×41 yday $60.23 → 09:30 $59.65 -23.78; HIMS×84 yday $28.77 → 09:30 $29.15 +31.92; INO×3086 yday $0.90 → 09:30 $0.93 +92.58; SLS×213 yday $12.36 → 09:30 $12.40 +8.52 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.92 | ▲ close $10,997.51 vs 09:30 $10,422.35 (session +575.16) | 16:00 close · cash $16.92 · equity $10,997.51 vs 09:30 $10,422.35 (+575.16; session marks +575.16) · 4 name(s) marked open→close (per-name table). BTSG×41 09:30 $59.65 → close $61.71 +84.46; HIMS×84 09:30 $29.15 → close $28.15 -84.00; INO×3086 09:30 $0.93 → close $1.09 +493.76; SLS×213 09:30 $12.40 → close $12.78 +80.94 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.92 | ▼ 09:30 equity $10,934.13 vs yday $10,997.51 (-63.38) | 09:30 open · cash $16.92 (unchanged overnight, no fees) · equity $10,934.13 vs prior close $10,997.51 (-63.38) · 4 name(s) re-marked at the open (per-name table). BTSG×41 yday $61.71 → 09:30 $61.69 -0.82; HIMS×84 yday $28.15 → 09:30 $28.14 -0.84; INO×3086 yday $1.09 → 09:30 $1.07 -61.72; SLS×213 yday $12.78 → 09:30 $12.78 +0.00 | — |
| 2026-08-17 09:30 ET | **BUY** | `VERI` | 3 | $1.15 | $0.04 | — | $13.43 | — | top 4 by cond; rank cond; list yday_mover; ⚪; ret5=-12.2; leftover $4.23 | join🟡 sector🟢 gen🟢 news🔴 judge🟢 vol🟢 buy🟢 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.43 | ▲ close $11,213.40 vs 09:30 $10,934.13 (session +279.32) | 16:00 close · cash $13.43 · equity $11,213.40 vs 09:30 $10,934.13 (+279.27; session marks +279.32) · 5 name(s) marked open→close (per-name table). BTSG×41 09:30 $61.69 → close $60.38 -53.71; HIMS×84 09:30 $28.14 → close $28.61 +39.48; INO×3086 09:30 $1.07 → close $1.15 +246.88; SLS×213 09:30 $12.78 → close $13.00 +46.86; VERI×3 09:30 $1.15 → close $1.08 -0.19 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.43 | ▼ 09:30 equity $11,030.60 vs yday $11,213.40 (-182.80) | 09:30 open · cash $13.43 (unchanged overnight, no fees) · equity $11,030.60 vs prior close $11,213.40 (-182.80) · 5 name(s) re-marked at the open (per-name table). BTSG×41 yday $60.38 → 09:30 $60.00 -15.58; HIMS×84 yday $28.61 → 09:30 $27.85 -63.84; INO×3086 yday $1.15 → 09:30 $1.14 -30.86; SLS×213 yday $13.00 → 09:30 $12.66 -72.42; VERI×3 yday $1.08 → 09:30 $1.05 -0.10 | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 41 | $60.00 | $2.14 | $+3.94 | $2,471.29 | ▲ +3.94 after sell → book $11,028.46; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 84 | $27.85 | $2.27 | $-163.28 | $4,808.41 | ▼ -163.28 after sell → book $11,026.18; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 3086 | $1.14 | $40.35 | $+943.78 | $8,286.10 | ▲ +943.78 after sell → book $10,985.83; vs 09:30 mark -40.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 213 | $12.66 | $2.80 | $+198.93 | $10,979.88 | ▲ +198.93 after sell → book $10,983.03; vs 09:30 mark -2.80 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,979.88 | ▼ close $10,982.86 vs 09:30 $11,030.60 (session -0.17) | 16:00 close · cash $10,979.88 · equity $10,982.86 vs 09:30 $11,030.60 (-47.74; session marks -0.17) · 1 name(s) marked open→close (per-name table). VERI×3 09:30 $1.05 → close $0.99 -0.17 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,979.88 | ▲ 09:30 equity $10,982.88 vs yday $10,982.86 (+0.02) | 09:30 open · cash $10,979.88 (unchanged overnight, no fees) · equity $10,982.88 vs prior close $10,982.86 (+0.02) · 1 name(s) re-marked at the open (per-name table). VERI×3 yday $0.99 → 09:30 $1.00 +0.02 | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,979.88 | ▼ close $10,982.78 vs 09:30 $10,982.88 (session -0.10) | 16:00 close · cash $10,979.88 · equity $10,982.78 vs 09:30 $10,982.88 (-0.10; session marks -0.10) · 1 name(s) marked open→close (per-name table). VERI×3 09:30 $1.00 → close $0.97 -0.10 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,979.88 | ▼ 09:30 equity $10,982.77 vs yday $10,982.78 (-0.01) | 09:30 open · cash $10,979.88 (unchanged overnight, no fees) · equity $10,982.77 vs prior close $10,982.78 (-0.01) · 1 name(s) re-marked at the open (per-name table). VERI×3 yday $0.97 → 09:30 $0.96 -0.01 | — |
| 2026-08-20 09:30 ET | **SELL** | `VERI` | 3 | $0.96 | $0.06 | $-0.66 | $10,982.71 | ▼ -0.66 after sell → book $10,982.71; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 133 | $20.55 | $2.39 | — | $8,247.17 | — | top 4 by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $2745.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 30 | $91.01 | $2.08 | — | $5,514.79 | — | top 4 by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $2745.68 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 132 | $20.65 | $2.39 | — | $2,786.60 | — | top 4 by cond; rank cond; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $2745.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 475 | $5.77 | $6.13 | — | $39.73 | — | top 4 by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $2745.68 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $39.73 | ▲ close $11,099.17 vs 09:30 $10,982.77 (session +129.44) | 16:00 close · cash $39.73 · equity $11,099.17 vs 09:30 $10,982.77 (+116.40; session marks +129.44) · 4 name(s) marked open→close (per-name table). AG×133 09:30 $20.55 → close $21.19 +85.12; BHP×30 09:30 $91.01 → close $93.63 +78.60; CDE×132 09:30 $20.65 → close $21.11 +60.72; HDSN×475 09:30 $5.77 → close $5.57 -95.00 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.73 | ▲ 09:30 equity $11,388.28 vs yday $11,099.17 (+289.11) | 09:30 open · cash $39.73 (unchanged overnight, no fees) · equity $11,388.28 vs prior close $11,099.17 (+289.11) · 4 name(s) re-marked at the open (per-name table). AG×133 yday $21.19 → 09:30 $21.90 +94.43; BHP×30 yday $93.63 → 09:30 $95.72 +62.70; CDE×132 yday $21.11 → 09:30 $21.75 +84.48; HDSN×475 yday $5.57 → 09:30 $5.67 +47.50 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $39.73 | ▼ close $11,197.89 vs 09:30 $11,388.28 (session -190.39) | 16:00 close · cash $39.73 · equity $11,197.89 vs 09:30 $11,388.28 (-190.39; session marks -190.39) · 4 name(s) marked open→close (per-name table). AG×133 09:30 $21.90 → close $21.09 -107.73; BHP×30 09:30 $95.72 → close $97.03 +39.30; CDE×132 09:30 $21.75 → close $20.97 -102.96; HDSN×475 09:30 $5.67 → close $5.63 -19.00 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.73 | ▲ 09:30 equity $11,301.00 vs yday $11,197.89 (+103.11) | 09:30 open · cash $39.73 (unchanged overnight, no fees) · equity $11,301.00 vs prior close $11,197.89 (+103.11) · 4 name(s) re-marked at the open (per-name table). AG×133 yday $21.09 → 09:30 $21.30 +27.93; BHP×30 yday $97.03 → 09:30 $97.31 +8.40; CDE×132 yday $20.97 → 09:30 $21.26 +38.28; HDSN×475 yday $5.63 → 09:30 $5.69 +28.50 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $39.73 | ▼ close $11,102.18 vs 09:30 $11,301.00 (session -198.82) | 16:00 close · cash $39.73 · equity $11,102.18 vs 09:30 $11,301.00 (-198.82; session marks -198.82) · 4 name(s) marked open→close (per-name table). AG×133 09:30 $21.30 → close $20.83 -62.51; BHP×30 09:30 $97.31 → close $97.13 -5.40; CDE×132 09:30 $21.26 → close $20.88 -50.16; HDSN×475 09:30 $5.69 → close $5.52 -80.75 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.73 | ▼ 09:30 equity $10,946.88 vs yday $11,102.18 (-155.30) | 09:30 open · cash $39.73 (unchanged overnight, no fees) · equity $10,946.88 vs prior close $11,102.18 (-155.30) · 4 name(s) re-marked at the open (per-name table). AG×133 yday $20.83 → 09:30 $20.32 -67.83; BHP×30 yday $97.13 → 09:30 $95.86 -38.10; CDE×132 yday $20.88 → 09:30 $20.47 -54.12; HDSN×475 yday $5.52 → 09:30 $5.53 +4.75 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 133 | $20.32 | $2.43 | $-35.41 | $2,739.85 | ▼ -35.41 after sell → book $10,944.44; vs 09:30 mark -2.44 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 30 | $95.86 | $2.11 | $+141.31 | $5,613.54 | ▲ +141.31 after sell → book $10,942.33; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 132 | $20.47 | $2.43 | $-28.58 | $8,313.15 | ▼ -28.58 after sell → book $10,939.90; vs 09:30 mark -2.43 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 475 | $5.53 | $6.23 | $-126.35 | $10,933.67 | ▼ -126.35 after sell → book $10,933.67; vs 09:30 mark -6.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 23 | $118.52 | $2.06 | — | $8,205.65 | — | top 4 by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $2733.42 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ERO` | 71 | $38.01 | $2.20 | — | $5,504.74 | — | top 4 by cond; rank cond; list mover_buy; ⚪; ret5=+10.4; leftover $2733.42 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 35 | $77.13 | $2.10 | — | $2,803.10 | — | top 4 by cond; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $2733.42 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CNH` | 229 | $11.90 | $2.95 | — | $75.04 | — | top 4 by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+14.3; leftover $2733.42 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $75.04 | ▲ close $11,225.50 vs 09:30 $10,946.88 (session +301.14) | 16:00 close · cash $75.04 · equity $11,225.50 vs 09:30 $10,946.88 (+278.62; session marks +301.14) · 4 name(s) marked open→close (per-name table). AU×23 09:30 $118.52 → close $123.39 +112.01; ERO×71 09:30 $38.01 → close $40.40 +169.69; FCX×35 09:30 $77.13 → close $79.91 +97.30; CNH×229 09:30 $11.90 → close $11.56 -77.86 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $75.04 | ▼ 09:30 equity $11,126.21 vs yday $11,225.50 (-99.29) | 09:30 open · cash $75.04 (unchanged overnight, no fees) · equity $11,126.21 vs prior close $11,225.50 (-99.29) · 4 name(s) re-marked at the open (per-name table). AU×23 yday $123.39 → 09:30 $119.80 -82.57; ERO×71 yday $40.40 → 09:30 $40.51 +7.81; FCX×35 yday $79.91 → 09:30 $79.34 -19.95; CNH×229 yday $11.56 → 09:30 $11.54 -4.58 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $75.04 | ▼ close $11,003.59 vs 09:30 $11,126.21 (session -122.62) | 16:00 close · cash $75.04 · equity $11,003.59 vs 09:30 $11,126.21 (-122.62; session marks -122.62) · 4 name(s) marked open→close (per-name table). AU×23 09:30 $119.80 → close $118.11 -38.87; ERO×71 09:30 $40.51 → close $39.24 -90.17; FCX×35 09:30 $79.34 → close $79.00 -11.90; CNH×229 09:30 $11.54 → close $11.62 +18.32 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $75.04 | ▼ 09:30 equity $10,978.70 vs yday $11,003.59 (-24.89) | 09:30 open · cash $75.04 (unchanged overnight, no fees) · equity $10,978.70 vs prior close $11,003.59 (-24.89) · 4 name(s) re-marked at the open (per-name table). AU×23 yday $118.11 → 09:30 $117.41 -16.10; ERO×71 yday $39.24 → 09:30 $39.20 -2.84; FCX×35 yday $79.00 → 09:30 $78.83 -5.95; CNH×229 yday $11.62 → 09:30 $11.62 +0.00 | — |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 4 | $4.57 | $0.19 | — | $56.57 | — | top 4 by cond; rank cond; list mover_buy; 🔵; ret5=+1.1; leftover $18.76 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $56.57 | ▲ close $10,987.96 vs 09:30 $10,978.70 (session +9.45) | 16:00 close · cash $56.57 · equity $10,987.96 vs 09:30 $10,978.70 (+9.26; session marks +9.45) · 5 name(s) marked open→close (per-name table). AU×23 09:30 $117.41 → close $118.40 +22.77; ERO×71 09:30 $39.20 → close $39.82 +44.02; FCX×35 09:30 $78.83 → close $78.42 -14.35; CNH×229 09:30 $11.62 → close $11.43 -43.51; GGB×4 09:30 $4.57 → close $4.70 +0.52 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $56.57 | ▲ 09:30 equity $11,060.04 vs yday $10,987.96 (+72.08) | 09:30 open · cash $56.57 (unchanged overnight, no fees) · equity $11,060.04 vs prior close $10,987.96 (+72.08) · 5 name(s) re-marked at the open (per-name table). AU×23 yday $118.40 → 09:30 $119.19 +18.17; ERO×71 yday $39.82 → 09:30 $40.12 +21.30; FCX×35 yday $78.42 → 09:30 $78.57 +5.25; CNH×229 yday $11.43 → 09:30 $11.55 +27.48; GGB×4 yday $4.70 → 09:30 $4.67 -0.12 | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 23 | $119.19 | $2.09 | $+11.26 | $2,795.85 | ▲ +11.26 after sell → book $11,057.95; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ERO` | 71 | $40.12 | $2.24 | $+145.37 | $5,642.13 | ▲ +145.37 after sell → book $11,055.71; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 35 | $78.57 | $2.13 | $+46.18 | $8,389.95 | ▲ +46.18 after sell → book $11,053.58; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CNH` | 229 | $11.55 | $3.01 | $-86.12 | $11,031.89 | ▼ -86.12 after sell → book $11,050.57; vs 09:30 mark -3.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 8 | $324.41 | $2.01 | — | $8,434.59 | — | top 4 by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $2757.97 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 19 | $141.76 | $2.05 | — | $5,739.11 | — | top 4 by cond; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $2757.97 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 6 | $400.42 | $2.01 | — | $3,334.58 | — | top 4 by cond; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $2757.97 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 2 | $1306.03 | $2.00 | — | $720.52 | — | top 4 by cond; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $2757.97 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $720.52 | ▼ close $10,574.03 vs 09:30 $11,060.04 (session -468.47) | 16:00 close · cash $720.52 · equity $10,574.03 vs 09:30 $11,060.04 (-486.01; session marks -468.47) · 5 name(s) marked open→close (per-name table). GGB×4 09:30 $4.67 → close $4.59 -0.32; KEYS×8 09:30 $324.41 → close $319.97 -35.52; SMTC×19 09:30 $141.76 → close $131.17 -201.21; CIEN×6 09:30 $400.42 → close $378.44 -131.88; MPWR×2 09:30 $1306.03 → close $1256.26 -99.54 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $720.52 | ▲ 09:30 equity $10,627.26 vs yday $10,574.03 (+53.23) | 09:30 open · cash $720.52 (unchanged overnight, no fees) · equity $10,627.26 vs prior close $10,574.03 (+53.23) · 5 name(s) re-marked at the open (per-name table). GGB×4 yday $4.59 → 09:30 $4.67 +0.32; KEYS×8 yday $319.97 → 09:30 $322.49 +20.16; SMTC×19 yday $131.17 → 09:30 $132.30 +21.47; CIEN×6 yday $378.44 → 09:30 $378.44 +0.00; MPWR×2 yday $1256.26 → 09:30 $1261.90 +11.28 | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $720.52 | ▲ close $10,679.14 vs 09:30 $10,627.26 (session +51.88) | 16:00 close · cash $720.52 · equity $10,679.14 vs 09:30 $10,627.26 (+51.88; session marks +51.88) · 5 name(s) marked open→close (per-name table). GGB×4 09:30 $4.67 → close $4.61 -0.24; KEYS×8 09:30 $322.49 → close $322.70 +1.68; SMTC×19 09:30 $132.30 → close $132.96 +12.54; CIEN×6 09:30 $378.44 → close $382.80 +26.16; MPWR×2 09:30 $1261.90 → close $1267.77 +11.74 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $720.52 | ▼ 09:30 equity $10,487.09 vs yday $10,679.14 (-192.05) | 09:30 open · cash $720.52 (unchanged overnight, no fees) · equity $10,487.09 vs prior close $10,679.14 (-192.05) · 5 name(s) re-marked at the open (per-name table). GGB×4 yday $4.61 → 09:30 $4.57 -0.16; KEYS×8 yday $322.70 → 09:30 $321.47 -9.84; SMTC×19 yday $132.96 → 09:30 $127.63 -101.27; CIEN×6 yday $382.80 → 09:30 $376.89 -35.46; MPWR×2 yday $1267.77 → 09:30 $1245.11 -45.32 | — |
| 2026-09-01 09:30 ET | **SELL** | `GGB` | 4 | $4.57 | $0.21 | $-0.41 | $738.59 | ▼ -0.41 after sell → book $10,486.88; vs 09:30 mark -0.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $738.59 | ▼ close $10,419.78 vs 09:30 $10,487.09 (session -67.10) | 16:00 close · cash $738.59 · equity $10,419.78 vs 09:30 $10,487.09 (-67.31; session marks -67.10) · 4 name(s) marked open→close (per-name table). KEYS×8 09:30 $321.47 → close $319.27 -17.60; SMTC×19 09:30 $127.63 → close $132.27 +88.16; CIEN×6 09:30 $376.89 → close $360.33 -99.36; MPWR×2 09:30 $1245.11 → close $1225.96 -38.30 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $738.59 | ▼ 09:30 equity $10,403.25 vs yday $10,419.78 (-16.53) | 09:30 open · cash $738.59 (unchanged overnight, no fees) · equity $10,403.25 vs prior close $10,419.78 (-16.53) · 4 name(s) re-marked at the open (per-name table). KEYS×8 yday $319.27 → 09:30 $318.04 -9.84; SMTC×19 yday $132.27 → 09:30 $133.00 +13.87; CIEN×6 yday $360.33 → 09:30 $357.25 -18.48; MPWR×2 yday $1225.96 → 09:30 $1224.92 -2.08 | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 8 | $318.04 | $2.04 | $-55.02 | $3,280.86 | ▼ -55.02 after sell → book $10,401.20; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 19 | $133.00 | $2.08 | $-170.56 | $5,805.79 | ▼ -170.56 after sell → book $10,399.13; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CIEN` | 6 | $357.25 | $2.04 | $-263.06 | $7,947.25 | ▼ -263.06 after sell → book $10,397.09; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `MPWR` | 2 | $1224.92 | $2.03 | $-166.24 | $10,395.07 | ▼ -166.24 after sell → book $10,395.07; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,395.07 | ▲ close $10,395.07 vs 09:30 $10,403.25 (session +0.00) | 16:00 close · cash $10,395.07 · no lots left · equity $10,395.07. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,395.07 | ▲ 09:30 equity $10,395.07 vs yday $10,395.07 (-0.00) | 09:30 open · cash $10,395.07 · no holdings · equity $10,395.07 vs prior close $10,395.07 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 154 | $16.77 | $2.45 | — | $7,810.03 | — | top 4 by cond; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $2598.77 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 1346 | $1.93 | $17.36 | — | $5,194.89 | — | top 4 by cond; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $2598.77 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 1192 | $2.18 | $15.38 | — | $2,580.95 | — | top 4 by cond; rank cond; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $2598.77 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 60 | $42.93 | $2.17 | — | $2.98 | — | top 4 by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $2598.77 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.98 | ▼ close $10,056.40 vs 09:30 $10,395.07 (session -301.30) | 16:00 close · cash $2.98 · equity $10,056.40 vs 09:30 $10,395.07 (-338.67; session marks -301.30) · 4 name(s) marked open→close (per-name table). ARCT×154 09:30 $16.77 → close $15.56 -186.34; BMEA×1346 09:30 $1.93 → close $1.91 -26.92; CRDL×1192 09:30 $2.18 → close $2.16 -23.84; HRMY×60 09:30 $42.93 → close $41.86 -64.20 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.98 | ▼ 09:30 equity $10,029.04 vs yday $10,056.40 (-27.36) | 09:30 open · cash $2.98 (unchanged overnight, no fees) · equity $10,029.04 vs prior close $10,056.40 (-27.36) · 4 name(s) re-marked at the open (per-name table). ARCT×154 yday $15.56 → 09:30 $15.61 +7.70; BMEA×1346 yday $1.91 → 09:30 $1.90 -13.46; CRDL×1192 yday $2.16 → 09:30 $2.16 +0.00; HRMY×60 yday $41.86 → 09:30 $41.50 -21.60 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.98 | ▲ close $10,329.04 vs 09:30 $10,029.04 (session +300.00) | 16:00 close · cash $2.98 · equity $10,329.04 vs 09:30 $10,029.04 (+300.00; session marks +300.00) · 4 name(s) marked open→close (per-name table). ARCT×154 09:30 $15.61 → close $15.82 +32.34; BMEA×1346 09:30 $1.90 → close $2.03 +174.98; CRDL×1192 09:30 $2.16 → close $2.20 +47.68; HRMY×60 09:30 $41.50 → close $42.25 +45.00 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.98 | ▼ 09:30 equity $10,231.76 vs yday $10,329.04 (-97.28) | 09:30 open · cash $2.98 (unchanged overnight, no fees) · equity $10,231.76 vs prior close $10,329.04 (-97.28) · 4 name(s) re-marked at the open (per-name table). ARCT×154 yday $15.82 → 09:30 $15.47 -53.90; BMEA×1346 yday $2.03 → 09:30 $2.00 -40.38; CRDL×1192 yday $2.20 → 09:30 $2.20 +0.00; HRMY×60 yday $42.25 → 09:30 $42.20 -3.00 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.98 | ▼ close $10,178.22 vs 09:30 $10,231.76 (session -53.54) | 16:00 close · cash $2.98 · equity $10,178.22 vs 09:30 $10,231.76 (-53.54; session marks -53.54) · 4 name(s) marked open→close (per-name table). ARCT×154 09:30 $15.47 → close $15.63 +24.64; BMEA×1346 09:30 $2.00 → close $1.93 -94.22; CRDL×1192 09:30 $2.20 → close $2.22 +23.84; HRMY×60 09:30 $42.20 → close $42.07 -7.80 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.98 | ▼ 09:30 equity $10,161.90 vs yday $10,178.22 (-16.32) | 09:30 open · cash $2.98 (unchanged overnight, no fees) · equity $10,161.90 vs prior close $10,178.22 (-16.32) · 4 name(s) re-marked at the open (per-name table). ARCT×154 yday $15.63 → 09:30 $15.46 -26.18; BMEA×1346 yday $1.93 → 09:30 $1.94 +13.46; CRDL×1192 yday $2.22 → 09:30 $2.22 +0.00; HRMY×60 yday $42.07 → 09:30 $42.01 -3.60 | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 154 | $15.46 | $2.50 | $-206.69 | $2,381.33 | ▼ -206.69 after sell → book $10,159.41; vs 09:30 mark -2.49 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `BMEA` | 1346 | $1.94 | $17.61 | $-21.51 | $4,974.96 | ▼ -21.51 after sell → book $10,141.80; vs 09:30 mark -17.61 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRDL` | 1192 | $2.22 | $15.60 | $+16.71 | $7,605.60 | ▲ +16.71 after sell → book $10,126.20; vs 09:30 mark -15.60 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 60 | $42.01 | $2.20 | $-59.57 | $10,124.00 | ▼ -59.57 after sell → book $10,124.00; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,124.00 | ▲ close $10,124.00 vs 09:30 $10,161.90 (session +0.00) | 16:00 close · cash $10,124.00 · no lots left · equity $10,124.00. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,124.00 | ▲ 09:30 equity $10,124.00 vs yday $10,124.00 (+0.00) | 09:30 open · cash $10,124.00 · no holdings · equity $10,124.00 vs prior close $10,124.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,124.00 | ▲ close $10,124.00 vs 09:30 $10,124.00 (session +0.00) | 16:00 close · cash $10,124.00 · no lots left · equity $10,124.00. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,124.00 | ▲ 09:30 equity $10,124.00 vs yday $10,124.00 (+0.00) | 09:30 open · cash $10,124.00 · no holdings · equity $10,124.00 vs prior close $10,124.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `HPE` | 44 | $56.37 | $2.12 | — | $7,641.60 | — | top 4 by cond; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+15.8; leftover $2531.00 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `SEDG` | 68 | $36.78 | $2.19 | — | $5,138.37 | — | top 4 by cond; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+13.1; leftover $2531.00 | join🟢 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,138.37 | ▲ close $10,228.57 vs 09:30 $10,124.00 (session +108.88) | 16:00 close · cash $5,138.37 · equity $10,228.57 vs 09:30 $10,124.00 (+104.57; session marks +108.88) · 2 name(s) marked open→close (per-name table). HPE×44 09:30 $56.37 → close $62.09 +251.68; SEDG×68 09:30 $36.78 → close $34.68 -142.80 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `BRUN` | cash | leftover split 4.23 < 1 share @ 26.25 |
| 2026-08-14 | `CLBT` | cash | leftover split 4.23 < 1 share @ 10.83 |
| 2026-08-14 | `HLIT` | cash | leftover split 4.23 < 1 share @ 13.18 |
| 2026-08-14 | `MNTN` | cash | leftover split 4.23 < 1 share @ 12.50 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `LPTH` | cash | leftover split 4.23 < 1 share @ 14.94 |
| 2026-08-17 | `DVN` | cash | leftover split 4.23 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 4.23 < 1 share @ 142.77 |
| 2026-08-18 | `VERI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AURA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `VERI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BHP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BSBR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 9.93 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 9.93 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 9.93 < 1 share @ 216.30 |
| 2026-08-21 | `ARCT` | cash | leftover split 9.93 < 1 share @ 11.13 |
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
| 2026-08-26 | `FNV` | cash | leftover split 18.76 < 1 share @ 267.02 |
| 2026-08-26 | `MOS` | cash | leftover split 18.76 < 1 share @ 24.84 |
| 2026-08-26 | `FIGR` | cash | leftover split 18.76 < 1 share @ 40.50 |
| 2026-08-26 | `FUTU` | cash | leftover split 18.76 < 1 share @ 124.67 |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ERO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CNH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ACMR` | cash | leftover split 18.76 < 1 share @ 81.65 |
| 2026-08-27 | `MT` | cash | leftover split 18.76 < 1 share @ 74.54 |
| 2026-08-27 | `MU` | cash | leftover split 18.76 < 1 share @ 967.01 |
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
| 2026-09-04 | `CABA` | cash | leftover split 0.75 < 1 share @ 3.46 |
| 2026-09-04 | `ALEC` | cash | leftover split 0.75 < 1 share @ 2.52 |
| 2026-09-04 | `ATRC` | cash | leftover split 0.75 < 1 share @ 52.03 |
| 2026-09-04 | `BHC` | cash | leftover split 0.75 < 1 share @ 6.71 |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `LOGI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `VNT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `GLW` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SWKS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `CVI` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INTC` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-11 | `ORCL` | no_price | no 09:30 open |
| 2026-09-11 | `SWKS` | no_price | no 09:30 open |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `HPE` | 44 | 2026-09-11 @ $56.37 | top 4 by cond; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+15.8; leftover $2531.00 |
| `SEDG` | 68 | 2026-09-11 @ $36.78 | top 4 by cond; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+13.1; leftover $2531.00 |
