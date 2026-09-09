# Factor mine action — `union_cond_n4_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · top 4 by cond

Cash book **-1.45%** ($9,855) · signal-only (no cash/fees) was +4.10%. Starts YES **1/18**. Fills 47 · skips 96 · realized $+66.32.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $15.60.

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
| 2026-08-14 | `BTBT` | 4 | — | $1.50 | +0.00 | $1.57 | +0.28 | +0.28 | +0.00 | +0.28 |
| 2026-08-17 | `BTSG` | 41 | $61.71 | $61.69 | -0.82 | $60.38 | -53.71 | -54.53 | +77.49 | +23.78 |
| 2026-08-17 | `HIMS` | 84 | $28.15 | $28.14 | -0.84 | $28.61 | +39.48 | +38.64 | -134.40 | -94.92 |
| 2026-08-17 | `INO` | 3086 | $1.09 | $1.07 | -61.72 | $1.15 | +246.88 | +185.16 | +802.36 | +1049.24 |
| 2026-08-17 | `IREN` | 54 | $44.06 | $45.23 | +63.18 | $44.90 | -17.82 | +45.36 | -40.50 | -58.32 |
| 2026-08-17 | `BTBT` | 4 | $1.57 | $1.52 | -0.20 | $1.60 | +0.32 | +0.12 | +0.08 | +0.40 |
| 2026-08-17 | `INV` | 3 | — | $1.62 | +0.00 | $1.39 | -0.71 | -0.71 | +0.00 | -0.71 |
| 2026-08-17 | `XHG` | 1 | — | $4.19 | +0.00 | $3.91 | -0.28 | -0.28 | +0.00 | -0.28 |
| 2026-08-18 | `BTSG` | 41 | $60.38 | $60.00 | -15.58 | — | +0.00 | -15.58 | +8.20 | — |
| 2026-08-18 | `HIMS` | 84 | $28.61 | $27.85 | -63.84 | — | +0.00 | -63.84 | -158.76 | — |
| 2026-08-18 | `INO` | 3086 | $1.15 | $1.14 | -30.86 | — | +0.00 | -30.86 | +1018.38 | — |
| 2026-08-18 | `IREN` | 54 | $44.90 | $43.56 | -72.36 | — | +0.00 | -72.36 | -130.68 | — |
| 2026-08-18 | `BTBT` | 4 | $1.60 | $1.54 | -0.24 | $1.45 | -0.36 | -0.60 | +0.16 | -0.20 |
| 2026-08-18 | `INV` | 3 | $1.39 | $1.32 | -0.18 | $1.32 | +0.00 | -0.18 | -0.89 | -0.89 |
| 2026-08-18 | `XHG` | 1 | $3.91 | $3.94 | +0.03 | $4.28 | +0.34 | +0.37 | -0.25 | +0.09 |
| 2026-08-19 | `BTBT` | 4 | $1.45 | $1.42 | -0.12 | — | +0.00 | -0.12 | -0.32 | — |
| 2026-08-19 | `INV` | 3 | $1.32 | $1.39 | +0.19 | $1.54 | +0.45 | +0.64 | -0.69 | -0.24 |
| 2026-08-19 | `XHG` | 1 | $4.28 | $4.32 | +0.04 | $4.33 | +0.01 | +0.05 | +0.13 | +0.14 |
| 2026-08-20 | `INV` | 3 | $1.54 | $1.55 | +0.03 | — | +0.00 | +0.03 | -0.21 | — |
| 2026-08-20 | `XHG` | 1 | $4.33 | $4.10 | -0.23 | — | +0.00 | -0.23 | -0.09 | — |
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
| 2026-08-27 | `AU` | 22 | $118.11 | $117.41 | -15.40 | $118.40 | +21.78 | +6.38 | -24.42 | -2.64 |
| 2026-08-27 | `ERO` | 69 | $39.24 | $39.20 | -2.76 | $39.82 | +42.78 | +40.02 | +82.11 | +124.89 |
| 2026-08-27 | `FCX` | 34 | $79.00 | $78.83 | -5.78 | $78.42 | -13.94 | -19.72 | +57.80 | +43.86 |
| 2026-08-27 | `CNH` | 222 | $11.62 | $11.62 | +0.00 | $11.43 | -42.18 | -42.18 | -62.16 | -104.34 |
| 2026-08-27 | `GGB` | 5 | — | $4.57 | +0.00 | $4.70 | +0.65 | +0.65 | +0.00 | +0.65 |
| 2026-08-28 | `AU` | 22 | $118.40 | $119.19 | +17.38 | — | +0.00 | +17.38 | +14.74 | — |
| 2026-08-28 | `ERO` | 69 | $39.82 | $40.12 | +20.70 | — | +0.00 | +20.70 | +145.59 | — |
| 2026-08-28 | `FCX` | 34 | $78.42 | $78.57 | +5.10 | — | +0.00 | +5.10 | +48.96 | — |
| 2026-08-28 | `CNH` | 222 | $11.43 | $11.55 | +26.64 | — | +0.00 | +26.64 | -77.70 | — |
| 2026-08-28 | `GGB` | 5 | $4.70 | $4.67 | -0.15 | $4.59 | -0.40 | -0.55 | +0.50 | +0.10 |
| 2026-08-28 | `KEYS` | 8 | — | $324.41 | +0.00 | $319.97 | -35.52 | -35.52 | +0.00 | -35.52 |
| 2026-08-28 | `SMTC` | 18 | — | $141.76 | +0.00 | $131.17 | -190.62 | -190.62 | +0.00 | -190.62 |
| 2026-08-28 | `CIEN` | 6 | — | $400.42 | +0.00 | $378.44 | -131.88 | -131.88 | +0.00 | -131.88 |
| 2026-08-28 | `MPWR` | 2 | — | $1306.03 | +0.00 | $1256.26 | -99.54 | -99.54 | +0.00 | -99.54 |
| 2026-08-31 | `GGB` | 5 | $4.59 | $4.67 | +0.40 | $4.61 | -0.30 | +0.10 | +0.50 | +0.20 |
| 2026-08-31 | `KEYS` | 8 | $319.97 | $322.49 | +20.16 | $322.70 | +1.68 | +21.84 | -15.36 | -13.68 |
| 2026-08-31 | `SMTC` | 18 | $131.17 | $132.30 | +20.34 | $132.96 | +11.88 | +32.22 | -170.28 | -158.40 |
| 2026-08-31 | `CIEN` | 6 | $378.44 | $378.44 | +0.00 | $382.80 | +26.16 | +26.16 | -131.88 | -105.72 |
| 2026-08-31 | `MPWR` | 2 | $1256.26 | $1261.90 | +11.28 | $1267.77 | +11.74 | +23.02 | -88.26 | -76.52 |
| 2026-09-01 | `GGB` | 5 | $4.61 | $4.57 | -0.20 | — | +0.00 | -0.20 | +0.00 | — |
| 2026-09-01 | `KEYS` | 8 | $322.70 | $321.47 | -9.84 | $319.27 | -17.60 | -27.44 | -23.52 | -41.12 |
| 2026-09-01 | `SMTC` | 18 | $132.96 | $127.63 | -95.94 | $132.27 | +83.52 | -12.42 | -254.34 | -170.82 |
| 2026-09-01 | `CIEN` | 6 | $382.80 | $376.89 | -35.46 | $360.33 | -99.36 | -134.82 | -141.18 | -240.54 |
| 2026-09-01 | `MPWR` | 2 | $1267.77 | $1245.11 | -45.32 | $1225.96 | -38.30 | -83.62 | -121.84 | -160.14 |
| 2026-09-02 | `KEYS` | 8 | $319.27 | $318.04 | -9.84 | — | +0.00 | -9.84 | -50.96 | — |
| 2026-09-02 | `SMTC` | 18 | $132.27 | $133.00 | +13.14 | — | +0.00 | +13.14 | -157.68 | — |
| 2026-09-02 | `CIEN` | 6 | $360.33 | $357.25 | -18.48 | — | +0.00 | -18.48 | -259.02 | — |
| 2026-09-02 | `MPWR` | 2 | $1225.96 | $1224.92 | -2.08 | — | +0.00 | -2.08 | -162.22 | — |
| 2026-09-03 | `ARCT` | 150 | — | $16.77 | +0.00 | $15.56 | -181.50 | -181.50 | +0.00 | -181.50 |
| 2026-09-03 | `BMEA` | 1303 | — | $1.93 | +0.00 | $1.91 | -26.06 | -26.06 | +0.00 | -26.06 |
| 2026-09-03 | `CRDL` | 1154 | — | $2.18 | +0.00 | $2.16 | -23.08 | -23.08 | +0.00 | -23.08 |
| 2026-09-03 | `HRMY` | 57 | — | $42.93 | +0.00 | $41.86 | -60.99 | -60.99 | +0.00 | -60.99 |
| 2026-09-04 | `ARCT` | 150 | $15.56 | $15.61 | +7.50 | $15.82 | +31.50 | +39.00 | -174.00 | -142.50 |
| 2026-09-04 | `BMEA` | 1303 | $1.91 | $1.90 | -13.03 | $2.03 | +169.39 | +156.36 | -39.09 | +130.30 |
| 2026-09-04 | `CRDL` | 1154 | $2.16 | $2.16 | +0.00 | $2.20 | +46.16 | +46.16 | -23.08 | +23.08 |
| 2026-09-04 | `HRMY` | 57 | $41.86 | $41.50 | -20.52 | $42.25 | +42.75 | +22.23 | -81.51 | -38.76 |
| 2026-09-04 | `CABA` | 2 | — | $3.46 | +0.00 | $3.47 | +0.02 | +0.02 | +0.00 | +0.02 |
| 2026-09-04 | `ALEC` | 3 | — | $2.52 | +0.00 | $2.46 | -0.18 | -0.18 | +0.00 | -0.18 |
| 2026-09-04 | `BHC` | 1 | — | $6.71 | +0.00 | $6.56 | -0.15 | -0.15 | +0.00 | -0.15 |
| 2026-09-08 | `ARCT` | 150 | $15.82 | $15.47 | -52.50 | $15.63 | +24.00 | -28.50 | -195.00 | -171.00 |
| 2026-09-08 | `BMEA` | 1303 | $2.03 | $2.00 | -39.09 | $1.93 | -91.21 | -130.30 | +91.21 | +0.00 |
| 2026-09-08 | `CRDL` | 1154 | $2.20 | $2.20 | +0.00 | $2.22 | +23.08 | +23.08 | +23.08 | +46.16 |
| 2026-09-08 | `HRMY` | 57 | $42.25 | $42.20 | -2.85 | $42.07 | -7.41 | -10.26 | -41.61 | -49.02 |
| 2026-09-08 | `CABA` | 2 | $3.47 | $3.43 | -0.08 | $3.27 | -0.32 | -0.40 | -0.06 | -0.38 |
| 2026-09-08 | `ALEC` | 3 | $2.46 | $2.38 | -0.24 | $2.47 | +0.27 | +0.03 | -0.42 | -0.15 |
| 2026-09-08 | `BHC` | 1 | $6.56 | $6.57 | +0.01 | $6.43 | -0.14 | -0.13 | -0.14 | -0.28 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +148.01 | BTSG, HIMS, INO, IREN | — | $26.70 | $10,107.25 | BTSG×41, HIMS×84, INO×3086, IREN×54 |
| 2026-08-14 | +5.50 | $26.70 | BTSG×41, HIMS×84, INO×3086, IREN×54 | $10,171.79 | +64.54 | +492.88 | BTBT | — | $20.63 | $10,664.60 | BTSG×41, HIMS×84, INO×3086, IREN×54, BTBT×4 |
| 2026-08-17 | +2.25 | $20.63 | BTSG×41, HIMS×84, INO×3086, IREN×54, BTBT×4 | $10,664.20 | -0.40 | +214.16 | INV, XHG | — | $11.47 | $10,878.26 | BTSG×41, HIMS×84, INO×3086, IREN×54, BTBT×4, INV×3, XHG×1 |
| 2026-08-18 | -6.20 | $11.47 | BTSG×41, HIMS×84, INO×3086, IREN×54, BTBT×4, INV×3, XHG×1 | $10,695.23 | -183.03 | -0.02 | — | BTSG, HIMS, INO, IREN | $10,634.21 | $10,648.26 | BTBT×4, INV×3, XHG×1 |
| 2026-08-19 | -7.20 | $10,634.21 | BTBT×4, INV×3, XHG×1 | $10,648.38 | +0.12 | +0.46 | — | BTBT | $10,639.80 | $10,648.75 | INV×3, XHG×1 |
| 2026-08-20 | +1.12 | $10,639.80 | INV×3, XHG×1 | $10,648.55 | -0.20 | +125.22 | AG, BHP, CDE, HDSN | INV, XHG | $42.22 | $10,760.85 | AG×129, BHP×29, CDE×128, HDSN×461 |
| 2026-08-21 | +3.25 | $42.22 | AG×129, BHP×29, CDE×128, HDSN×461 | $11,041.07 | +280.22 | -184.78 | — | — | $42.22 | $10,856.29 | AG×129, BHP×29, CDE×128, HDSN×461 |
| 2026-08-24 | -5.17 | $42.22 | AG×129, BHP×29, CDE×128, HDSN×461 | $10,956.28 | +99.99 | -192.86 | — | — | $42.22 | $10,763.42 | AG×129, BHP×29, CDE×128, HDSN×461 |
| 2026-08-25 | +1.80 | $42.22 | AG×129, BHP×29, CDE×128, HDSN×461 | $10,612.93 | -150.49 | +291.09 | AU, ERO, FCX, CNH | AG, BHP, CDE, HDSN | $96.39 | $10,881.83 | AU×22, ERO×69, FCX×34, CNH×222 |
| 2026-08-26 | +2.02 | $96.39 | AU×22, ERO×69, FCX×34, CNH×222 | $10,786.62 | -95.21 | -118.61 | — | — | $96.39 | $10,668.01 | AU×22, ERO×69, FCX×34, CNH×222 |
| 2026-08-27 | — | $96.39 | AU×22, ERO×69, FCX×34, CNH×222 | $10,644.07 | -23.94 | +9.09 | GGB | — | $73.29 | $10,652.91 | AU×22, ERO×69, FCX×34, CNH×222, GGB×5 |
| 2026-08-28 | +0.75 | $73.29 | AU×22, ERO×69, FCX×34, CNH×222, GGB×5 | $10,722.58 | +69.67 | -457.96 | KEYS, SMTC, CIEN, MPWR | AU, ERO, FCX, CNH | $520.27 | $10,247.20 | GGB×5, KEYS×8, SMTC×18, CIEN×6, MPWR×2 |
| 2026-08-31 | -5.85 | $520.27 | GGB×5, KEYS×8, SMTC×18, CIEN×6, MPWR×2 | $10,299.38 | +52.18 | +51.16 | — | — | $520.27 | $10,350.54 | GGB×5, KEYS×8, SMTC×18, CIEN×6, MPWR×2 |
| 2026-09-01 | -6.30 | $520.27 | GGB×5, KEYS×8, SMTC×18, CIEN×6, MPWR×2 | $10,163.78 | -186.76 | -71.74 | — | GGB | $542.85 | $10,091.77 | KEYS×8, SMTC×18, CIEN×6, MPWR×2 |
| 2026-09-02 | -3.83 | $542.85 | KEYS×8, SMTC×18, CIEN×6, MPWR×2 | $10,074.51 | -17.26 | +0.00 | — | KEYS, SMTC, CIEN, MPWR | $10,066.34 | $10,066.34 | — |
| 2026-09-03 | -0.90 | $10,066.34 | — | $10,066.34 | -0.00 | -291.63 | ARCT, BMEA, CRDL, HRMY | — | $37.02 | $9,738.41 | ARCT×150, BMEA×1303, CRDL×1154, HRMY×57 |
| 2026-09-04 | +2.25 | $37.02 | ARCT×150, BMEA×1303, CRDL×1154, HRMY×57 | $9,712.36 | -26.05 | +289.49 | CABA, ALEC, BHC | — | $15.60 | $10,001.62 | ARCT×150, BMEA×1303, CRDL×1154, HRMY×57, CABA×2, ALEC×3, BHC×1 |
| 2026-09-08 | -11.47 | $15.60 | ARCT×150, BMEA×1303, CRDL×1154, HRMY×57, CABA×2, ALEC×3, BHC×1 | $9,906.87 | -94.75 | -51.73 | — | — | $15.60 | $9,855.14 | ARCT×150, BMEA×1303, CRDL×1154, HRMY×57, CABA×2, ALEC×3, BHC×1 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 41 | $59.80 | $2.11 | — | $7,546.09 | — | top 4 by cond; rank cond; list flatten; ⚪; ret5=-5.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 84 | $29.74 | $2.24 | — | $5,045.69 | — | top 4 by cond; rank cond; list flatten; ⚪; ret5=-5.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 3086 | $0.81 | $34.25 | — | $2,511.77 | — | top 4 by cond; rank cond; list flatten; ⚪; ret5=+13.2; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 54 | $45.98 | $2.15 | — | $26.70 | — | top 4 by cond; rank cond; list flatten; ⚪; ret5=+12.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26.70 | ▲ close $10,107.25 vs 09:30 $10,000.00 (session +148.01) | 16:00 close · cash $26.70 · equity $10,107.25 vs 09:30 $10,000.00 (+107.25; session marks +148.01) · 4 name(s) marked open→close (per-name table). BTSG×41 09:30 $59.80 → close $60.23 +17.63; HIMS×84 09:30 $29.74 → close $28.77 -81.48; INO×3086 09:30 $0.81 → close $0.90 +277.74; IREN×54 09:30 $45.98 → close $44.76 -65.88 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26.70 | ▲ 09:30 equity $10,171.79 vs yday $10,107.25 (+64.54) | 09:30 open · cash $26.70 (unchanged overnight, no fees) · equity $10,171.79 vs prior close $10,107.25 (+64.54) · 4 name(s) re-marked at the open (per-name table). BTSG×41 yday $60.23 → 09:30 $59.65 -23.78; HIMS×84 yday $28.77 → 09:30 $29.15 +31.92; INO×3086 yday $0.90 → 09:30 $0.93 +92.58; IREN×54 yday $44.76 → 09:30 $44.09 -36.18 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 4 | $1.50 | $0.07 | — | $20.63 | — | top 4 by cond; rank cond; list flatten; 🔵; ⚪; ret5=+9.2; leftover $6.67 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.63 | ▲ close $10,664.60 vs 09:30 $10,171.79 (session +492.88) | 16:00 close · cash $20.63 · equity $10,664.60 vs 09:30 $10,171.79 (+492.81; session marks +492.88) · 5 name(s) marked open→close (per-name table). BTSG×41 09:30 $59.65 → close $61.71 +84.46; HIMS×84 09:30 $29.15 → close $28.15 -84.00; INO×3086 09:30 $0.93 → close $1.09 +493.76; IREN×54 09:30 $44.09 → close $44.06 -1.62; BTBT×4 09:30 $1.50 → close $1.57 +0.28 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.63 | ▼ 09:30 equity $10,664.20 vs yday $10,664.60 (-0.40) | 09:30 open · cash $20.63 (unchanged overnight, no fees) · equity $10,664.20 vs prior close $10,664.60 (-0.40) · 5 name(s) re-marked at the open (per-name table). BTSG×41 yday $61.71 → 09:30 $61.69 -0.82; HIMS×84 yday $28.15 → 09:30 $28.14 -0.84; INO×3086 yday $1.09 → 09:30 $1.07 -61.72; IREN×54 yday $44.06 → 09:30 $45.23 +63.18; BTBT×4 yday $1.57 → 09:30 $1.52 -0.20 | — |
| 2026-08-17 09:30 ET | **BUY** | `INV` | 3 | $1.62 | $0.06 | — | $15.71 | — | top 4 by cond; rank cond; list yday_mover; 🔵; ⚪; ret5=-53.0; leftover $5.16 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 1 | $4.19 | $0.04 | — | $11.47 | — | top 4 by cond; rank cond; list yday_mover; ⚪; ret5=+291.8; leftover $5.16 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.47 | ▲ close $10,878.26 vs 09:30 $10,664.20 (session +214.16) | 16:00 close · cash $11.47 · equity $10,878.26 vs 09:30 $10,664.20 (+214.06; session marks +214.16) · 7 name(s) marked open→close (per-name table). BTSG×41 09:30 $61.69 → close $60.38 -53.71; HIMS×84 09:30 $28.14 → close $28.61 +39.48; INO×3086 09:30 $1.07 → close $1.15 +246.88; IREN×54 09:30 $45.23 → close $44.90 -17.82; BTBT×4 09:30 $1.52 → close $1.60 +0.32; INV×3 09:30 $1.62 → close $1.39 -0.71; XHG×1 09:30 $4.19 → close $3.91 -0.28 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.47 | ▼ 09:30 equity $10,695.23 vs yday $10,878.26 (-183.03) | 09:30 open · cash $11.47 (unchanged overnight, no fees) · equity $10,695.23 vs prior close $10,878.26 (-183.03) · 7 name(s) re-marked at the open (per-name table). BTSG×41 yday $60.38 → 09:30 $60.00 -15.58; HIMS×84 yday $28.61 → 09:30 $27.85 -63.84; INO×3086 yday $1.15 → 09:30 $1.14 -30.86; IREN×54 yday $44.90 → 09:30 $43.56 -72.36; BTBT×4 yday $1.60 → 09:30 $1.54 -0.24; INV×3 yday $1.39 → 09:30 $1.32 -0.18; XHG×1 yday $3.91 → 09:30 $3.94 +0.03 | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 41 | $60.00 | $2.14 | $+3.94 | $2,469.33 | ▲ +3.94 after sell → book $10,693.09; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 84 | $27.85 | $2.27 | $-163.28 | $4,806.46 | ▼ -163.28 after sell → book $10,690.81; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 3086 | $1.14 | $40.35 | $+943.78 | $8,284.15 | ▲ +943.78 after sell → book $10,650.46; vs 09:30 mark -40.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 54 | $43.56 | $2.18 | $-135.01 | $10,634.21 | ▼ -135.01 after sell → book $10,648.28; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,634.21 | ▼ close $10,648.26 vs 09:30 $10,695.23 (session -0.02) | 16:00 close · cash $10,634.21 · equity $10,648.26 vs 09:30 $10,695.23 (-46.97; session marks -0.02) · 3 name(s) marked open→close (per-name table). BTBT×4 09:30 $1.54 → close $1.45 -0.36; INV×3 09:30 $1.32 → close $1.32 +0.00; XHG×1 09:30 $3.94 → close $4.28 +0.34 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,634.21 | ▲ 09:30 equity $10,648.38 vs yday $10,648.26 (+0.12) | 09:30 open · cash $10,634.21 (unchanged overnight, no fees) · equity $10,648.38 vs prior close $10,648.26 (+0.12) · 3 name(s) re-marked at the open (per-name table). BTBT×4 yday $1.45 → 09:30 $1.42 -0.12; INV×3 yday $1.32 → 09:30 $1.39 +0.19; XHG×1 yday $4.28 → 09:30 $4.32 +0.04 | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 4 | $1.42 | $0.09 | $-0.48 | $10,639.80 | ▼ -0.48 after sell → book $10,648.29; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,639.80 | ▲ close $10,648.75 vs 09:30 $10,648.38 (session +0.46) | 16:00 close · cash $10,639.80 · equity $10,648.75 vs 09:30 $10,648.38 (+0.37; session marks +0.46) · 2 name(s) marked open→close (per-name table). INV×3 09:30 $1.39 → close $1.54 +0.45; XHG×1 09:30 $4.32 → close $4.33 +0.01 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,639.80 | ▼ 09:30 equity $10,648.55 vs yday $10,648.75 (-0.20) | 09:30 open · cash $10,639.80 (unchanged overnight, no fees) · equity $10,648.55 vs prior close $10,648.75 (-0.20) · 2 name(s) re-marked at the open (per-name table). INV×3 yday $1.54 → 09:30 $1.55 +0.03; XHG×1 yday $4.33 → 09:30 $4.10 -0.23 | — |
| 2026-08-20 09:30 ET | **SELL** | `INV` | 3 | $1.55 | $0.08 | $-0.34 | $10,644.37 | ▼ -0.34 after sell → book $10,648.47; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `XHG` | 1 | $4.10 | $0.06 | $-0.20 | $10,648.41 | ▼ -0.20 after sell → book $10,648.41; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 129 | $20.55 | $2.38 | — | $7,995.08 | — | top 4 by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $2662.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 29 | $91.01 | $2.08 | — | $5,353.71 | — | top 4 by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $2662.10 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 128 | $20.65 | $2.37 | — | $2,708.14 | — | top 4 by cond; rank cond; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $2662.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 461 | $5.77 | $5.95 | — | $42.22 | — | top 4 by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $2662.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $42.22 | ▲ close $10,760.85 vs 09:30 $10,648.55 (session +125.22) | 16:00 close · cash $42.22 · equity $10,760.85 vs 09:30 $10,648.55 (+112.30; session marks +125.22) · 4 name(s) marked open→close (per-name table). AG×129 09:30 $20.55 → close $21.19 +82.56; BHP×29 09:30 $91.01 → close $93.63 +75.98; CDE×128 09:30 $20.65 → close $21.11 +58.88; HDSN×461 09:30 $5.77 → close $5.57 -92.20 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $42.22 | ▲ 09:30 equity $11,041.07 vs yday $10,760.85 (+280.22) | 09:30 open · cash $42.22 (unchanged overnight, no fees) · equity $11,041.07 vs prior close $10,760.85 (+280.22) · 4 name(s) re-marked at the open (per-name table). AG×129 yday $21.19 → 09:30 $21.90 +91.59; BHP×29 yday $93.63 → 09:30 $95.72 +60.61; CDE×128 yday $21.11 → 09:30 $21.75 +81.92; HDSN×461 yday $5.57 → 09:30 $5.67 +46.10 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $42.22 | ▼ close $10,856.29 vs 09:30 $11,041.07 (session -184.78) | 16:00 close · cash $42.22 · equity $10,856.29 vs 09:30 $11,041.07 (-184.78; session marks -184.78) · 4 name(s) marked open→close (per-name table). AG×129 09:30 $21.90 → close $21.09 -104.49; BHP×29 09:30 $95.72 → close $97.03 +37.99; CDE×128 09:30 $21.75 → close $20.97 -99.84; HDSN×461 09:30 $5.67 → close $5.63 -18.44 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $42.22 | ▲ 09:30 equity $10,956.28 vs yday $10,856.29 (+99.99) | 09:30 open · cash $42.22 (unchanged overnight, no fees) · equity $10,956.28 vs prior close $10,856.29 (+99.99) · 4 name(s) re-marked at the open (per-name table). AG×129 yday $21.09 → 09:30 $21.30 +27.09; BHP×29 yday $97.03 → 09:30 $97.31 +8.12; CDE×128 yday $20.97 → 09:30 $21.26 +37.12; HDSN×461 yday $5.63 → 09:30 $5.69 +27.66 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $42.22 | ▼ close $10,763.42 vs 09:30 $10,956.28 (session -192.86) | 16:00 close · cash $42.22 · equity $10,763.42 vs 09:30 $10,956.28 (-192.86; session marks -192.86) · 4 name(s) marked open→close (per-name table). AG×129 09:30 $21.30 → close $20.83 -60.63; BHP×29 09:30 $97.31 → close $97.13 -5.22; CDE×128 09:30 $21.26 → close $20.88 -48.64; HDSN×461 09:30 $5.69 → close $5.52 -78.37 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $42.22 | ▼ 09:30 equity $10,612.93 vs yday $10,763.42 (-150.49) | 09:30 open · cash $42.22 (unchanged overnight, no fees) · equity $10,612.93 vs prior close $10,763.42 (-150.49) · 4 name(s) re-marked at the open (per-name table). AG×129 yday $20.83 → 09:30 $20.32 -65.79; BHP×29 yday $97.13 → 09:30 $95.86 -36.83; CDE×128 yday $20.88 → 09:30 $20.47 -52.48; HDSN×461 yday $5.52 → 09:30 $5.53 +4.61 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 129 | $20.32 | $2.42 | $-34.47 | $2,661.08 | ▼ -34.47 after sell → book $10,610.51; vs 09:30 mark -2.42 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 29 | $95.86 | $2.11 | $+136.46 | $5,438.91 | ▲ +136.46 after sell → book $10,608.40; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 128 | $20.47 | $2.42 | $-27.83 | $8,056.66 | ▼ -27.83 after sell → book $10,605.99; vs 09:30 mark -2.41 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 461 | $5.53 | $6.04 | $-122.63 | $10,599.94 | ▼ -122.63 after sell → book $10,599.94; vs 09:30 mark -6.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 22 | $118.52 | $2.06 | — | $7,990.45 | — | top 4 by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $2649.99 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ERO` | 69 | $38.01 | $2.20 | — | $5,365.56 | — | top 4 by cond; rank cond; list mover_buy; ⚪; ret5=+10.4; leftover $2649.99 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 34 | $77.13 | $2.09 | — | $2,741.05 | — | top 4 by cond; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $2649.99 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CNH` | 222 | $11.90 | $2.86 | — | $96.39 | — | top 4 by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+14.3; leftover $2649.99 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $96.39 | ▲ close $10,881.83 vs 09:30 $10,612.93 (session +291.09) | 16:00 close · cash $96.39 · equity $10,881.83 vs 09:30 $10,612.93 (+268.90; session marks +291.09) · 4 name(s) marked open→close (per-name table). AU×22 09:30 $118.52 → close $123.39 +107.14; ERO×69 09:30 $38.01 → close $40.40 +164.91; FCX×34 09:30 $77.13 → close $79.91 +94.52; CNH×222 09:30 $11.90 → close $11.56 -75.48 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $96.39 | ▼ 09:30 equity $10,786.62 vs yday $10,881.83 (-95.21) | 09:30 open · cash $96.39 (unchanged overnight, no fees) · equity $10,786.62 vs prior close $10,881.83 (-95.21) · 4 name(s) re-marked at the open (per-name table). AU×22 yday $123.39 → 09:30 $119.80 -78.98; ERO×69 yday $40.40 → 09:30 $40.51 +7.59; FCX×34 yday $79.91 → 09:30 $79.34 -19.38; CNH×222 yday $11.56 → 09:30 $11.54 -4.44 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $96.39 | ▼ close $10,668.01 vs 09:30 $10,786.62 (session -118.61) | 16:00 close · cash $96.39 · equity $10,668.01 vs 09:30 $10,786.62 (-118.61; session marks -118.61) · 4 name(s) marked open→close (per-name table). AU×22 09:30 $119.80 → close $118.11 -37.18; ERO×69 09:30 $40.51 → close $39.24 -87.63; FCX×34 09:30 $79.34 → close $79.00 -11.56; CNH×222 09:30 $11.54 → close $11.62 +17.76 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $96.39 | ▼ 09:30 equity $10,644.07 vs yday $10,668.01 (-23.94) | 09:30 open · cash $96.39 (unchanged overnight, no fees) · equity $10,644.07 vs prior close $10,668.01 (-23.94) · 4 name(s) re-marked at the open (per-name table). AU×22 yday $118.11 → 09:30 $117.41 -15.40; ERO×69 yday $39.24 → 09:30 $39.20 -2.76; FCX×34 yday $79.00 → 09:30 $78.83 -5.78; CNH×222 yday $11.62 → 09:30 $11.62 +0.00 | — |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 5 | $4.57 | $0.24 | — | $73.29 | — | top 4 by cond; rank cond; list mover_buy; 🔵; ret5=+1.1; leftover $24.10 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $73.29 | ▲ close $10,652.91 vs 09:30 $10,644.07 (session +9.09) | 16:00 close · cash $73.29 · equity $10,652.91 vs 09:30 $10,644.07 (+8.84; session marks +9.09) · 5 name(s) marked open→close (per-name table). AU×22 09:30 $117.41 → close $118.40 +21.78; ERO×69 09:30 $39.20 → close $39.82 +42.78; FCX×34 09:30 $78.83 → close $78.42 -13.94; CNH×222 09:30 $11.62 → close $11.43 -42.18; GGB×5 09:30 $4.57 → close $4.70 +0.65 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $73.29 | ▲ 09:30 equity $10,722.58 vs yday $10,652.91 (+69.67) | 09:30 open · cash $73.29 (unchanged overnight, no fees) · equity $10,722.58 vs prior close $10,652.91 (+69.67) · 5 name(s) re-marked at the open (per-name table). AU×22 yday $118.40 → 09:30 $119.19 +17.38; ERO×69 yday $39.82 → 09:30 $40.12 +20.70; FCX×34 yday $78.42 → 09:30 $78.57 +5.10; CNH×222 yday $11.43 → 09:30 $11.55 +26.64; GGB×5 yday $4.70 → 09:30 $4.67 -0.15 | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 22 | $119.19 | $2.09 | $+10.60 | $2,693.38 | ▲ +10.60 after sell → book $10,720.49; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ERO` | 69 | $40.12 | $2.23 | $+141.16 | $5,459.43 | ▲ +141.16 after sell → book $10,718.26; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 34 | $78.57 | $2.12 | $+44.74 | $8,128.69 | ▲ +44.74 after sell → book $10,716.14; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CNH` | 222 | $11.55 | $2.92 | $-83.49 | $10,689.87 | ▼ -83.49 after sell → book $10,713.22; vs 09:30 mark -2.92 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 8 | $324.41 | $2.01 | — | $8,092.58 | — | top 4 by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $2672.47 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 18 | $141.76 | $2.04 | — | $5,538.85 | — | top 4 by cond; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $2672.47 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 6 | $400.42 | $2.01 | — | $3,134.32 | — | top 4 by cond; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $2672.47 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 2 | $1306.03 | $2.00 | — | $520.27 | — | top 4 by cond; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $2672.47 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $520.27 | ▼ close $10,247.20 vs 09:30 $10,722.58 (session -457.96) | 16:00 close · cash $520.27 · equity $10,247.20 vs 09:30 $10,722.58 (-475.38; session marks -457.96) · 5 name(s) marked open→close (per-name table). GGB×5 09:30 $4.67 → close $4.59 -0.40; KEYS×8 09:30 $324.41 → close $319.97 -35.52; SMTC×18 09:30 $141.76 → close $131.17 -190.62; CIEN×6 09:30 $400.42 → close $378.44 -131.88; MPWR×2 09:30 $1306.03 → close $1256.26 -99.54 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $520.27 | ▲ 09:30 equity $10,299.38 vs yday $10,247.20 (+52.18) | 09:30 open · cash $520.27 (unchanged overnight, no fees) · equity $10,299.38 vs prior close $10,247.20 (+52.18) · 5 name(s) re-marked at the open (per-name table). GGB×5 yday $4.59 → 09:30 $4.67 +0.40; KEYS×8 yday $319.97 → 09:30 $322.49 +20.16; SMTC×18 yday $131.17 → 09:30 $132.30 +20.34; CIEN×6 yday $378.44 → 09:30 $378.44 +0.00; MPWR×2 yday $1256.26 → 09:30 $1261.90 +11.28 | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $520.27 | ▲ close $10,350.54 vs 09:30 $10,299.38 (session +51.16) | 16:00 close · cash $520.27 · equity $10,350.54 vs 09:30 $10,299.38 (+51.16; session marks +51.16) · 5 name(s) marked open→close (per-name table). GGB×5 09:30 $4.67 → close $4.61 -0.30; KEYS×8 09:30 $322.49 → close $322.70 +1.68; SMTC×18 09:30 $132.30 → close $132.96 +11.88; CIEN×6 09:30 $378.44 → close $382.80 +26.16; MPWR×2 09:30 $1261.90 → close $1267.77 +11.74 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $520.27 | ▼ 09:30 equity $10,163.78 vs yday $10,350.54 (-186.76) | 09:30 open · cash $520.27 (unchanged overnight, no fees) · equity $10,163.78 vs prior close $10,350.54 (-186.76) · 5 name(s) re-marked at the open (per-name table). GGB×5 yday $4.61 → 09:30 $4.57 -0.20; KEYS×8 yday $322.70 → 09:30 $321.47 -9.84; SMTC×18 yday $132.96 → 09:30 $127.63 -95.94; CIEN×6 yday $382.80 → 09:30 $376.89 -35.46; MPWR×2 yday $1267.77 → 09:30 $1245.11 -45.32 | — |
| 2026-09-01 09:30 ET | **SELL** | `GGB` | 5 | $4.57 | $0.26 | $-0.51 | $542.85 | ▼ -0.51 after sell → book $10,163.51; vs 09:30 mark -0.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $542.85 | ▼ close $10,091.77 vs 09:30 $10,163.78 (session -71.74) | 16:00 close · cash $542.85 · equity $10,091.77 vs 09:30 $10,163.78 (-72.01; session marks -71.74) · 4 name(s) marked open→close (per-name table). KEYS×8 09:30 $321.47 → close $319.27 -17.60; SMTC×18 09:30 $127.63 → close $132.27 +83.52; CIEN×6 09:30 $376.89 → close $360.33 -99.36; MPWR×2 09:30 $1245.11 → close $1225.96 -38.30 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $542.85 | ▼ 09:30 equity $10,074.51 vs yday $10,091.77 (-17.26) | 09:30 open · cash $542.85 (unchanged overnight, no fees) · equity $10,074.51 vs prior close $10,091.77 (-17.26) · 4 name(s) re-marked at the open (per-name table). KEYS×8 yday $319.27 → 09:30 $318.04 -9.84; SMTC×18 yday $132.27 → 09:30 $133.00 +13.14; CIEN×6 yday $360.33 → 09:30 $357.25 -18.48; MPWR×2 yday $1225.96 → 09:30 $1224.92 -2.08 | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 8 | $318.04 | $2.04 | $-55.02 | $3,085.13 | ▼ -55.02 after sell → book $10,072.47; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 18 | $133.00 | $2.07 | $-161.80 | $5,477.06 | ▼ -161.80 after sell → book $10,070.40; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CIEN` | 6 | $357.25 | $2.04 | $-263.06 | $7,618.52 | ▼ -263.06 after sell → book $10,068.36; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `MPWR` | 2 | $1224.92 | $2.03 | $-166.24 | $10,066.34 | ▼ -166.24 after sell → book $10,066.34; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,066.34 | ▲ close $10,066.34 vs 09:30 $10,074.51 (session +0.00) | 16:00 close · cash $10,066.34 · no lots left · equity $10,066.34. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,066.34 | ▲ 09:30 equity $10,066.34 vs yday $10,066.34 (-0.00) | 09:30 open · cash $10,066.34 · no holdings · equity $10,066.34 vs prior close $10,066.34 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 150 | $16.77 | $2.44 | — | $7,548.40 | — | top 4 by cond; rank cond; list probable,yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $2516.58 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 1303 | $1.93 | $16.81 | — | $5,016.80 | — | top 4 by cond; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $2516.58 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 1154 | $2.18 | $14.89 | — | $2,486.19 | — | top 4 by cond; rank cond; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $2516.58 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 57 | $42.93 | $2.16 | — | $37.02 | — | top 4 by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $2516.58 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.02 | ▼ close $9,738.41 vs 09:30 $10,066.34 (session -291.63) | 16:00 close · cash $37.02 · equity $9,738.41 vs 09:30 $10,066.34 (-327.93; session marks -291.63) · 4 name(s) marked open→close (per-name table). ARCT×150 09:30 $16.77 → close $15.56 -181.50; BMEA×1303 09:30 $1.93 → close $1.91 -26.06; CRDL×1154 09:30 $2.18 → close $2.16 -23.08; HRMY×57 09:30 $42.93 → close $41.86 -60.99 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.02 | ▼ 09:30 equity $9,712.36 vs yday $9,738.41 (-26.05) | 09:30 open · cash $37.02 (unchanged overnight, no fees) · equity $9,712.36 vs prior close $9,738.41 (-26.05) · 4 name(s) re-marked at the open (per-name table). ARCT×150 yday $15.56 → 09:30 $15.61 +7.50; BMEA×1303 yday $1.91 → 09:30 $1.90 -13.03; CRDL×1154 yday $2.16 → 09:30 $2.16 +0.00; HRMY×57 yday $41.86 → 09:30 $41.50 -20.52 | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 2 | $3.46 | $0.08 | — | $30.02 | — | top 4 by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $9.25 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 3 | $2.52 | $0.08 | — | $22.38 | — | top 4 by cond; rank cond; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $9.25 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 1 | $6.71 | $0.07 | — | $15.60 | — | top 4 by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $9.25 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.60 | ▲ close $10,001.62 vs 09:30 $9,712.36 (session +289.49) | 16:00 close · cash $15.60 · equity $10,001.62 vs 09:30 $9,712.36 (+289.26; session marks +289.49) · 7 name(s) marked open→close (per-name table). ARCT×150 09:30 $15.61 → close $15.82 +31.50; BMEA×1303 09:30 $1.90 → close $2.03 +169.39; CRDL×1154 09:30 $2.16 → close $2.20 +46.16; HRMY×57 09:30 $41.50 → close $42.25 +42.75; CABA×2 09:30 $3.46 → close $3.47 +0.02; ALEC×3 09:30 $2.52 → close $2.46 -0.18; BHC×1 09:30 $6.71 → close $6.56 -0.15 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.60 | ▼ 09:30 equity $9,906.87 vs yday $10,001.62 (-94.75) | 09:30 open · cash $15.60 (unchanged overnight, no fees) · equity $9,906.87 vs prior close $10,001.62 (-94.75) · 7 name(s) re-marked at the open (per-name table). ARCT×150 yday $15.82 → 09:30 $15.47 -52.50; BMEA×1303 yday $2.03 → 09:30 $2.00 -39.09; CRDL×1154 yday $2.20 → 09:30 $2.20 +0.00; HRMY×57 yday $42.25 → 09:30 $42.20 -2.85; CABA×2 yday $3.47 → 09:30 $3.43 -0.08; ALEC×3 yday $2.46 → 09:30 $2.38 -0.24; BHC×1 yday $6.56 → 09:30 $6.57 +0.01 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.60 | ▼ close $9,855.14 vs 09:30 $9,906.87 (session -51.73) | 16:00 close · cash $15.60 · equity $9,855.14 vs 09:30 $9,906.87 (-51.73; session marks -51.73) · 7 name(s) marked open→close (per-name table). ARCT×150 09:30 $15.47 → close $15.63 +24.00; BMEA×1303 09:30 $2.00 → close $1.93 -91.21; CRDL×1154 09:30 $2.20 → close $2.22 +23.08; HRMY×57 09:30 $42.20 → close $42.07 -7.41; CABA×2 09:30 $3.43 → close $3.27 -0.32; ALEC×3 09:30 $2.38 → close $2.47 +0.27; BHC×1 09:30 $6.57 → close $6.43 -0.14 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `ARX` | cash | leftover split 6.67 < 1 share @ 19.57 |
| 2026-08-14 | `BETR` | cash | leftover split 6.67 < 1 share @ 14.80 |
| 2026-08-14 | `FIGR` | cash | leftover split 6.67 < 1 share @ 32.12 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ABX` | cash | leftover split 5.16 < 1 share @ 9.12 |
| 2026-08-17 | `NU` | cash | leftover split 5.16 < 1 share @ 15.40 |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `INV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `XHG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AURA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `INV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `XHG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BHP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BSBR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 10.56 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 10.56 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 10.56 < 1 share @ 216.30 |
| 2026-08-21 | `ARCT` | cash | leftover split 10.56 < 1 share @ 11.13 |
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
| 2026-08-26 | `FNV` | cash | leftover split 24.10 < 1 share @ 267.02 |
| 2026-08-26 | `MOS` | cash | leftover split 24.10 < 1 share @ 24.84 |
| 2026-08-26 | `FIGR` | cash | leftover split 24.10 < 1 share @ 40.50 |
| 2026-08-26 | `FUTU` | cash | leftover split 24.10 < 1 share @ 124.67 |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ERO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CNH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ACMR` | cash | leftover split 24.10 < 1 share @ 81.65 |
| 2026-08-27 | `MT` | cash | leftover split 24.10 < 1 share @ 74.54 |
| 2026-08-27 | `MU` | cash | leftover split 24.10 < 1 share @ 967.01 |
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
| 2026-09-04 | `ATRC` | cash | leftover split 9.25 < 1 share @ 52.03 |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BHC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `LOGI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `VNT` | hard_red | hard-red S=-11.47 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ARCT` | 150 | 2026-09-03 @ $16.77 | top 4 by cond; rank cond; list probable,yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $2516.58 |
| `BMEA` | 1303 | 2026-09-03 @ $1.93 | top 4 by cond; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $2516.58 |
| `CRDL` | 1154 | 2026-09-03 @ $2.18 | top 4 by cond; rank cond; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $2516.58 |
| `HRMY` | 57 | 2026-09-03 @ $42.93 | top 4 by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $2516.58 |
| `CABA` | 2 | 2026-09-04 @ $3.46 | top 4 by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $9.25 |
| `ALEC` | 3 | 2026-09-04 @ $2.52 | top 4 by cond; rank cond; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $9.25 |
| `BHC` | 1 | 2026-09-04 @ $6.71 | top 4 by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $9.25 |
