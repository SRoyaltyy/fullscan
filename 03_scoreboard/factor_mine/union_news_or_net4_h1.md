# Factor mine action — `union_news_or_net4_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · packet🟢 OR headline🟢 and camera net ≥ 4

Cash book **-11.74%** ($8,826) · signal-only (no cash/fees) was -9.27%. Starts YES **0/22**. Fills 92 · skips 21 · realized $-1174.31.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how many morning cameras are green vs red.
- Must-have: the morning news packet OR the prior-export headline is green.
- Must-have: camera net (+G −R) is at least 4.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Sort the keepers by how many morning cameras are green vs red and keep the top 8.
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
- **Gate** `news_or_headline=True,cam_net_min=4` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,825.72.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `ANGX` | 464 | — | $4.31 | +0.00 | $4.37 | +27.84 | +27.84 | +0.00 | +27.84 |
| 2026-08-14 | `ARX` | 102 | — | $19.57 | +0.00 | $19.58 | +1.02 | +1.02 | +0.00 | +1.02 |
| 2026-08-14 | `HLIT` | 151 | — | $13.18 | +0.00 | $13.92 | +111.74 | +111.74 | +0.00 | +111.74 |
| 2026-08-14 | `MH` | 147 | — | $13.55 | +0.00 | $13.10 | -66.15 | -66.15 | +0.00 | -66.15 |
| 2026-08-14 | `SNDK` | 1 | — | $1646.93 | +0.00 | $1641.11 | -5.82 | -5.82 | +0.00 | -5.82 |
| 2026-08-17 | `ANGX` | 464 | $4.37 | $4.60 | +106.72 | — | +0.00 | +106.72 | +134.56 | — |
| 2026-08-17 | `ARX` | 102 | $19.58 | $19.57 | -1.02 | — | +0.00 | -1.02 | +0.00 | — |
| 2026-08-17 | `HLIT` | 151 | $13.92 | $13.84 | -12.08 | — | +0.00 | -12.08 | +99.66 | — |
| 2026-08-17 | `MH` | 147 | $13.10 | $13.16 | +8.82 | — | +0.00 | +8.82 | -57.33 | — |
| 2026-08-17 | `SNDK` | 1 | $1641.11 | $1700.74 | +59.63 | — | +0.00 | +59.63 | +53.81 | — |
| 2026-08-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `BHP` | 16 | — | $91.01 | +0.00 | $93.63 | +41.92 | +41.92 | +0.00 | +41.92 |
| 2026-08-20 | `APA` | 32 | — | $44.76 | +0.00 | $44.39 | -11.84 | -11.84 | +0.00 | -11.84 |
| 2026-08-20 | `AUTL` | 589 | — | $2.47 | +0.00 | $2.46 | -5.89 | -5.89 | +0.00 | -5.89 |
| 2026-08-20 | `CRSP` | 24 | — | $58.73 | +0.00 | $58.12 | -14.64 | -14.64 | +0.00 | -14.64 |
| 2026-08-20 | `ASST` | 91 | — | $16.00 | +0.00 | $16.13 | +11.83 | +11.83 | +0.00 | +11.83 |
| 2026-08-20 | `MRNA` | 9 | — | $150.14 | +0.00 | $133.32 | -151.38 | -151.38 | +0.00 | -151.38 |
| 2026-08-20 | `ZLAB` | 54 | — | $26.57 | +0.00 | $26.02 | -29.70 | -29.70 | +0.00 | -29.70 |
| 2026-08-21 | `BHP` | 16 | $93.63 | $95.72 | +33.44 | — | +0.00 | +33.44 | +75.36 | — |
| 2026-08-21 | `APA` | 32 | $44.39 | $44.52 | +4.16 | — | +0.00 | +4.16 | -7.68 | — |
| 2026-08-21 | `AUTL` | 589 | $2.46 | $2.47 | +5.89 | $2.41 | -35.34 | -29.45 | +0.00 | -35.34 |
| 2026-08-21 | `CRSP` | 24 | $58.12 | $59.72 | +38.40 | $59.50 | -5.28 | +33.12 | +23.76 | +18.48 |
| 2026-08-21 | `ASST` | 91 | $16.13 | $17.66 | +139.23 | — | +0.00 | +139.23 | +151.06 | — |
| 2026-08-21 | `MRNA` | 9 | $133.32 | $133.11 | -1.89 | — | +0.00 | -1.89 | -153.27 | — |
| 2026-08-21 | `ZLAB` | 54 | $26.02 | $26.25 | +12.42 | — | +0.00 | +12.42 | -17.28 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `FUTU` | 10 | — | $115.18 | +0.00 | $123.64 | +84.60 | +84.60 | +0.00 | +84.60 |
| 2026-08-21 | `GRAL` | 15 | — | $78.88 | +0.00 | $79.54 | +9.90 | +9.90 | +0.00 | +9.90 |
| 2026-08-21 | `ABTC` | 141 | — | $8.66 | +0.00 | $7.93 | -102.93 | -102.93 | +0.00 | -102.93 |
| 2026-08-21 | `HIVE` | 378 | — | $3.24 | +0.00 | $3.03 | -79.38 | -79.38 | +0.00 | -79.38 |
| 2026-08-21 | `MARA` | 104 | — | $11.70 | +0.00 | $11.26 | -45.76 | -45.76 | +0.00 | -45.76 |
| 2026-08-24 | `AUTL` | 589 | $2.41 | $2.40 | -5.89 | — | +0.00 | -5.89 | -41.23 | — |
| 2026-08-24 | `CRSP` | 24 | $59.50 | $58.75 | -18.00 | $57.08 | -40.20 | -58.20 | +0.48 | -39.72 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.51 | -7.10 | — | +0.00 | -7.10 | +10.80 | — |
| 2026-08-24 | `FUTU` | 10 | $123.64 | $121.00 | -26.40 | — | +0.00 | -26.40 | +58.20 | — |
| 2026-08-24 | `GRAL` | 15 | $79.54 | $81.87 | +34.95 | — | +0.00 | +34.95 | +44.85 | — |
| 2026-08-24 | `ABTC` | 141 | $7.93 | $8.00 | +9.87 | — | +0.00 | +9.87 | -93.06 | — |
| 2026-08-24 | `HIVE` | 378 | $3.03 | $2.99 | -15.12 | — | +0.00 | -15.12 | -94.50 | — |
| 2026-08-24 | `MARA` | 104 | $11.26 | $11.17 | -9.36 | — | +0.00 | -9.36 | -55.12 | — |
| 2026-08-25 | `CRSP` | 24 | $57.08 | $57.93 | +20.52 | — | +0.00 | +20.52 | -19.20 | — |
| 2026-08-25 | `AU` | 28 | — | $118.52 | +0.00 | $123.39 | +136.36 | +136.36 | +0.00 | +136.36 |
| 2026-08-25 | `FCX` | 43 | — | $77.13 | +0.00 | $79.91 | +119.54 | +119.54 | +0.00 | +119.54 |
| 2026-08-25 | `EZPW` | 94 | — | $35.05 | +0.00 | $35.23 | +16.92 | +16.92 | +0.00 | +16.92 |
| 2026-08-26 | `AU` | 28 | $123.39 | $119.80 | -100.52 | — | +0.00 | -100.52 | +35.84 | — |
| 2026-08-26 | `FCX` | 43 | $79.91 | $79.34 | -24.51 | — | +0.00 | -24.51 | +95.03 | — |
| 2026-08-26 | `EZPW` | 94 | $35.23 | $35.70 | +44.18 | — | +0.00 | +44.18 | +61.10 | — |
| 2026-08-26 | `FNV` | 38 | — | $267.02 | +0.00 | $267.37 | +13.30 | +13.30 | +0.00 | +13.30 |
| 2026-08-27 | `FNV` | 38 | $267.37 | $267.23 | -5.32 | — | +0.00 | -5.32 | +7.98 | — |
| 2026-08-27 | `ACMR` | 24 | — | $81.65 | +0.00 | $80.49 | -27.84 | -27.84 | +0.00 | -27.84 |
| 2026-08-27 | `MU` | 2 | — | $967.01 | +0.00 | $935.39 | -63.24 | -63.24 | +0.00 | -63.24 |
| 2026-08-27 | `ASML` | 1 | — | $1746.53 | +0.00 | $1735.01 | -11.52 | -11.52 | +0.00 | -11.52 |
| 2026-08-27 | `LRCX` | 6 | — | $318.88 | +0.00 | $318.58 | -1.80 | -1.80 | +0.00 | -1.80 |
| 2026-08-27 | `NVDA` | 9 | — | $222.86 | +0.00 | $227.98 | +46.08 | +46.08 | +0.00 | +46.08 |
| 2026-08-28 | `ACMR` | 24 | $80.49 | $79.27 | -29.28 | — | +0.00 | -29.28 | -57.12 | — |
| 2026-08-28 | `MU` | 2 | $935.39 | $919.29 | -32.20 | — | +0.00 | -32.20 | -95.44 | — |
| 2026-08-28 | `ASML` | 1 | $1735.01 | $1734.75 | -0.26 | — | +0.00 | -0.26 | -11.78 | — |
| 2026-08-28 | `LRCX` | 6 | $318.58 | $318.03 | -3.30 | — | +0.00 | -3.30 | -5.10 | — |
| 2026-08-28 | `NVDA` | 9 | $227.98 | $227.36 | -5.58 | — | +0.00 | -5.58 | +40.50 | — |
| 2026-08-28 | `KEYS` | 3 | — | $324.41 | +0.00 | $319.97 | -13.32 | -13.32 | +0.00 | -13.32 |
| 2026-08-28 | `SMTC` | 8 | — | $141.76 | +0.00 | $131.17 | -84.72 | -84.72 | +0.00 | -84.72 |
| 2026-08-28 | `CIEN` | 3 | — | $400.42 | +0.00 | $378.44 | -65.94 | -65.94 | +0.00 | -65.94 |
| 2026-08-28 | `DDOG` | 5 | — | $240.22 | +0.00 | $236.98 | -16.20 | -16.20 | +0.00 | -16.20 |
| 2026-08-28 | `ADSK` | 4 | — | $261.16 | +0.00 | $260.66 | -2.00 | -2.00 | +0.00 | -2.00 |
| 2026-08-28 | `SEDG` | 38 | — | $32.90 | +0.00 | $31.41 | -56.62 | -56.62 | +0.00 | -56.62 |
| 2026-08-28 | `TLS` | 259 | — | $4.82 | +0.00 | $4.79 | -7.77 | -7.77 | +0.00 | -7.77 |
| 2026-08-31 | `KEYS` | 3 | $319.97 | $322.49 | +7.56 | — | +0.00 | +7.56 | -5.76 | — |
| 2026-08-31 | `SMTC` | 8 | $131.17 | $132.30 | +9.04 | — | +0.00 | +9.04 | -75.68 | — |
| 2026-08-31 | `CIEN` | 3 | $378.44 | $378.44 | +0.00 | — | +0.00 | +0.00 | -65.94 | — |
| 2026-08-31 | `DDOG` | 5 | $236.98 | $233.97 | -15.07 | — | +0.00 | -15.07 | -31.27 | — |
| 2026-08-31 | `ADSK` | 4 | $260.66 | $257.71 | -11.80 | — | +0.00 | -11.80 | -13.80 | — |
| 2026-08-31 | `SEDG` | 38 | $31.41 | $31.15 | -9.88 | — | +0.00 | -9.88 | -66.50 | — |
| 2026-08-31 | `TLS` | 259 | $4.79 | $4.81 | +5.18 | — | +0.00 | +5.18 | -2.59 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AVGO` | 3 | — | $351.74 | +0.00 | $357.16 | +16.26 | +16.26 | +0.00 | +16.26 |
| 2026-09-03 | `DELL` | 2 | — | $486.31 | +0.00 | $516.39 | +60.16 | +60.16 | +0.00 | +60.16 |
| 2026-09-03 | `CXW` | 43 | — | $32.31 | +0.00 | $33.66 | +58.05 | +58.05 | +0.00 | +58.05 |
| 2026-09-03 | `FRNM` | 87 | — | $15.87 | +0.00 | $16.90 | +89.61 | +89.61 | +0.00 | +89.61 |
| 2026-09-03 | `MMED` | 58 | — | $23.88 | +0.00 | $23.84 | -2.32 | -2.32 | +0.00 | -2.32 |
| 2026-09-03 | `DE` | 1 | — | $703.25 | +0.00 | $694.41 | -8.84 | -8.84 | +0.00 | -8.84 |
| 2026-09-03 | `HPE` | 29 | — | $47.60 | +0.00 | $54.44 | +198.36 | +198.36 | +0.00 | +198.36 |
| 2026-09-04 | `AVGO` | 3 | $357.16 | $359.70 | +7.62 | — | +0.00 | +7.62 | +23.88 | — |
| 2026-09-04 | `DELL` | 2 | $516.39 | $513.78 | -5.22 | — | +0.00 | -5.22 | +54.94 | — |
| 2026-09-04 | `CXW` | 43 | $33.66 | $33.46 | -8.60 | — | +0.00 | -8.60 | +49.45 | — |
| 2026-09-04 | `FRNM` | 87 | $16.90 | $16.40 | -43.50 | $16.31 | -7.83 | -51.33 | +46.11 | +38.28 |
| 2026-09-04 | `MMED` | 58 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -2.32 | — |
| 2026-09-04 | `DE` | 1 | $694.41 | $692.03 | -2.38 | — | +0.00 | -2.38 | -11.22 | — |
| 2026-09-04 | `HPE` | 29 | $54.44 | $53.85 | -17.11 | — | +0.00 | -17.11 | +181.25 | — |
| 2026-09-04 | `CRM` | 8 | — | $263.36 | +0.00 | $259.23 | -33.04 | -33.04 | +0.00 | -33.04 |
| 2026-09-04 | `MRX` | 28 | — | $75.65 | +0.00 | $78.27 | +73.36 | +73.36 | +0.00 | +73.36 |
| 2026-09-04 | `BE` | 9 | — | $236.82 | +0.00 | $252.87 | +144.45 | +144.45 | +0.00 | +144.45 |
| 2026-09-04 | `BAK` | 1110 | — | $1.94 | +0.00 | $1.89 | -55.50 | -55.50 | +0.00 | -55.50 |
| 2026-09-08 | `FRNM` | 87 | $16.31 | $16.74 | +37.41 | — | +0.00 | +37.41 | +75.69 | — |
| 2026-09-08 | `CRM` | 8 | $259.23 | $253.72 | -44.08 | — | +0.00 | -44.08 | -77.12 | — |
| 2026-09-08 | `MRX` | 28 | $78.27 | $78.84 | +15.96 | $76.71 | -59.64 | -43.68 | +89.32 | +29.68 |
| 2026-09-08 | `BE` | 9 | $252.87 | $267.76 | +134.01 | — | +0.00 | +134.01 | +278.46 | — |
| 2026-09-08 | `BAK` | 1110 | $1.89 | $1.94 | +55.50 | — | +0.00 | +55.50 | +0.00 | — |
| 2026-09-09 | `MRX` | 28 | $76.71 | $76.60 | -3.08 | — | +0.00 | -3.08 | +26.60 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 62 | — | $164.43 | +0.00 | $150.28 | -877.30 | -877.30 | +0.00 | -877.30 |
| 2026-09-14 | `ORCL` | 62 | $150.28 | $141.42 | -549.32 | — | +0.00 | -549.32 | -1426.62 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +68.63 | ANGX, ARX, HLIT, MH, SNDK | — | $359.91 | $10,053.48 | ANGX×464, ARX×102, HLIT×151, MH×147, SNDK×1 |
| 2026-08-17 | +2.25 | $359.91 | ANGX×464, ARX×102, HLIT×151, MH×147, SNDK×1 | $10,215.56 | +162.08 | +0.00 | — | ANGX, ARX, HLIT, MH, SNDK | $10,200.18 | $10,200.18 | — |
| 2026-08-18 | -6.20 | $10,200.18 | — | $10,200.18 | -0.00 | +0.00 | — | — | $10,200.18 | $10,200.18 | — |
| 2026-08-19 | -7.20 | $10,200.18 | — | $10,200.18 | -0.00 | +0.00 | — | — | $10,200.18 | $10,200.18 | — |
| 2026-08-20 | +1.12 | $10,200.18 | — | $10,200.18 | -0.00 | -159.70 | BHP, APA, AUTL, CRSP, ASST, MRNA, ZLAB | — | $185.09 | $10,020.26 | BHP×16, APA×32, AUTL×589, CRSP×24, ASST×91, MRNA×9, ZLAB×54 |
| 2026-08-21 | +3.25 | $185.09 | BHP×16, APA×32, AUTL×589, CRSP×24, ASST×91, MRNA×9, ZLAB×54 | $10,251.91 | +231.65 | -156.29 | AU, FUTU, GRAL, ABTC, HIVE, MARA | BHP, APA, ASST, MRNA, ZLAB | $145.58 | $10,069.28 | AUTL×589, CRSP×24, AU×10, FUTU×10, GRAL×15, ABTC×141, HIVE×378, MARA×104 |
| 2026-08-24 | -5.17 | $145.58 | AUTL×589, CRSP×24, AU×10, FUTU×10, GRAL×15, ABTC×141, HIVE×378, MARA×104 | $10,032.23 | -37.05 | -40.20 | — | AUTL, AU, FUTU, GRAL, ABTC, HIVE, MARA | $8,598.67 | $9,968.47 | CRSP×24 |
| 2026-08-25 | +1.80 | $8,598.67 | CRSP×24 | $9,988.99 | +20.52 | +272.82 | AU, FCX, EZPW | CRSP | $50.59 | $10,253.26 | AU×28, FCX×43, EZPW×94 |
| 2026-08-26 | +2.02 | $50.59 | AU×28, FCX×43, EZPW×94 | $10,172.41 | -80.85 | +13.30 | FNV | AU, FCX, EZPW | $16.96 | $10,177.02 | FNV×38 |
| 2026-08-27 | — | $16.96 | FNV×38 | $10,171.70 | -5.32 | -58.32 | ACMR, MU, ASML, LRCX, NVDA | FNV | $600.26 | $10,101.11 | ACMR×24, MU×2, ASML×1, LRCX×6, NVDA×9 |
| 2026-08-28 | +0.75 | $600.26 | ACMR×24, MU×2, ASML×1, LRCX×6, NVDA×9 | $10,030.49 | -70.62 | -246.57 | KEYS, SMTC, CIEN, DDOG, ADSK, SEDG, TLS | ACMR, MU, ASML, LRCX, NVDA | $1,951.94 | $9,758.26 | KEYS×3, SMTC×8, CIEN×3, DDOG×5, ADSK×4, SEDG×38, TLS×259 |
| 2026-08-31 | -5.85 | $1,951.94 | KEYS×3, SMTC×8, CIEN×3, DDOG×5, ADSK×4, SEDG×38, TLS×259 | $9,743.28 | -14.98 | +0.00 | — | KEYS, SMTC, CIEN, DDOG, ADSK, SEDG, TLS | $9,727.64 | $9,727.64 | — |
| 2026-09-01 | -6.30 | $9,727.64 | — | $9,727.64 | +0.00 | +0.00 | — | — | $9,727.64 | $9,727.64 | — |
| 2026-09-02 | -3.83 | $9,727.64 | — | $9,727.64 | +0.00 | +0.00 | — | — | $9,727.64 | $9,727.64 | — |
| 2026-09-03 | -0.90 | $9,727.64 | — | $9,727.64 | +0.00 | +411.28 | AVGO, DELL, CXW, FRNM, MMED, DE, HPE | — | $1,446.50 | $10,124.33 | AVGO×3, DELL×2, CXW×43, FRNM×87, MMED×58, DE×1, HPE×29 |
| 2026-09-04 | +2.25 | $1,446.50 | AVGO×3, DELL×2, CXW×43, FRNM×87, MMED×58, DE×1, HPE×29 | $10,055.14 | -69.19 | +121.44 | CRM, MRX, BE, BAK | AVGO, DELL, CXW, MMED, DE, HPE | $85.58 | $10,143.68 | FRNM×87, CRM×8, MRX×28, BE×9, BAK×1110 |
| 2026-09-08 | -11.47 | $85.58 | FRNM×87, CRM×8, MRX×28, BE×9, BAK×1110 | $10,342.48 | +198.80 | -59.64 | — | FRNM, CRM, BE, BAK | $8,114.07 | $10,261.95 | MRX×28 |
| 2026-09-09 | -13.95 | $8,114.07 | MRX×28 | $10,258.87 | -3.08 | +0.00 | — | MRX | $10,256.77 | $10,256.77 | — |
| 2026-09-10 | -13.28 | $10,256.77 | — | $10,256.77 | +0.00 | +0.00 | — | — | $10,256.77 | $10,256.77 | — |
| 2026-09-11 | +0.50 | $10,256.77 | — | $10,256.77 | +0.00 | -877.30 | ORCL | — | $59.94 | $9,377.30 | ORCL×62 |
| 2026-09-14 | -11.00 | $59.94 | ORCL×62 | $8,827.98 | -549.32 | +0.00 | — | ORCL | $8,825.72 | $8,825.72 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 464 | $4.31 | $5.99 | — | $7,994.17 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 102 | $19.57 | $2.30 | — | $5,995.74 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 151 | $13.18 | $2.44 | — | $4,003.12 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 147 | $13.55 | $2.43 | — | $2,008.83 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SNDK` | 1 | $1646.93 | $1.99 | — | $359.91 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.4; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $359.91 | ▲ close $10,053.48 vs 09:30 $10,000.00 (session +68.63) | 16:00 close · cash $359.91 · equity $10,053.48 vs 09:30 $10,000.00 (+53.48; session marks +68.63) · 5 name(s) marked open→close (per-name table). ANGX×464 09:30 $4.31 → close $4.37 +27.84; ARX×102 09:30 $19.57 → close $19.58 +1.02; HLIT×151 09:30 $13.18 → close $13.92 +111.74; MH×147 09:30 $13.55 → close $13.10 -66.15; SNDK×1 09:30 $1646.93 → close $1641.11 -5.82 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $359.91 | ▲ 09:30 equity $10,215.56 vs yday $10,053.48 (+162.08) | 09:30 open · cash $359.91 (unchanged overnight, no fees) · equity $10,215.56 vs prior close $10,053.48 (+162.08) · 5 name(s) re-marked at the open (per-name table). ANGX×464 yday $4.37 → 09:30 $4.60 +106.72; ARX×102 yday $19.58 → 09:30 $19.57 -1.02; HLIT×151 yday $13.92 → 09:30 $13.84 -12.08; MH×147 yday $13.10 → 09:30 $13.16 +8.82; SNDK×1 yday $1641.11 → 09:30 $1700.74 +59.63 | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 464 | $4.60 | $6.08 | $+122.49 | $2,488.23 | ▲ +122.49 after sell → book $10,209.48; vs 09:30 mark -6.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 102 | $19.57 | $2.33 | $-4.62 | $4,482.04 | ▼ -4.62 after sell → book $10,207.15; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 151 | $13.84 | $2.48 | $+94.73 | $6,569.40 | ▲ +94.73 after sell → book $10,204.66; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 147 | $13.16 | $2.47 | $-62.23 | $8,501.45 | ▼ -62.23 after sell → book $10,202.19; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SNDK` | 1 | $1700.74 | $2.02 | $+49.81 | $10,200.18 | ▲ +49.81 after sell → book $10,200.18; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,200.18 | ▲ close $10,200.18 vs 09:30 $10,215.56 (session +0.00) | 16:00 close · cash $10,200.18 · no lots left · equity $10,200.18. | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,200.18 | ▲ 09:30 equity $10,200.18 vs yday $10,200.18 (-0.00) | 09:30 open · cash $10,200.18 · no holdings · equity $10,200.18 vs prior close $10,200.18 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,200.18 | ▲ close $10,200.18 vs 09:30 $10,200.18 (session +0.00) | 16:00 close · cash $10,200.18 · no lots left · equity $10,200.18. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,200.18 | ▲ 09:30 equity $10,200.18 vs yday $10,200.18 (-0.00) | 09:30 open · cash $10,200.18 · no holdings · equity $10,200.18 vs prior close $10,200.18 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,200.18 | ▲ close $10,200.18 vs 09:30 $10,200.18 (session +0.00) | 16:00 close · cash $10,200.18 · no lots left · equity $10,200.18. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,200.18 | ▲ 09:30 equity $10,200.18 vs yday $10,200.18 (-0.00) | 09:30 open · cash $10,200.18 · no holdings · equity $10,200.18 vs prior close $10,200.18 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 16 | $91.01 | $2.04 | — | $8,741.98 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1457.17 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 32 | $44.76 | $2.09 | — | $7,307.57 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $1457.17 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 589 | $2.47 | $7.60 | — | $5,845.14 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1457.17 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 24 | $58.73 | $2.06 | — | $4,433.56 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1457.17 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 91 | $16.00 | $2.26 | — | $2,975.30 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1457.17 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 9 | $150.14 | $2.02 | — | $1,622.02 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1457.17 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 54 | $26.57 | $2.15 | — | $185.09 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+4.8; leftover $1457.17 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $185.09 | ▼ close $10,020.26 vs 09:30 $10,200.18 (session -159.70) | 16:00 close · cash $185.09 · equity $10,020.26 vs 09:30 $10,200.18 (-179.92; session marks -159.70) · 7 name(s) marked open→close (per-name table). BHP×16 09:30 $91.01 → close $93.63 +41.92; APA×32 09:30 $44.76 → close $44.39 -11.84; AUTL×589 09:30 $2.47 → close $2.46 -5.89; CRSP×24 09:30 $58.73 → close $58.12 -14.64; ASST×91 09:30 $16.00 → close $16.13 +11.83; MRNA×9 09:30 $150.14 → close $133.32 -151.38; ZLAB×54 09:30 $26.57 → close $26.02 -29.70 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $185.09 | ▲ 09:30 equity $10,251.91 vs yday $10,020.26 (+231.65) | 09:30 open · cash $185.09 (unchanged overnight, no fees) · equity $10,251.91 vs prior close $10,020.26 (+231.65) · 7 name(s) re-marked at the open (per-name table). BHP×16 yday $93.63 → 09:30 $95.72 +33.44; APA×32 yday $44.39 → 09:30 $44.52 +4.16; AUTL×589 yday $2.46 → 09:30 $2.47 +5.89; CRSP×24 yday $58.12 → 09:30 $59.72 +38.40; ASST×91 yday $16.13 → 09:30 $17.66 +139.23; MRNA×9 yday $133.32 → 09:30 $133.11 -1.89; ZLAB×54 yday $26.02 → 09:30 $26.25 +12.42 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 16 | $95.72 | $2.06 | $+71.26 | $1,714.55 | ▲ +71.26 after sell → book $10,249.85; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 32 | $44.52 | $2.11 | $-11.87 | $3,137.08 | ▼ -11.87 after sell → book $10,247.74; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 91 | $17.66 | $2.29 | $+146.51 | $4,741.85 | ▲ +146.51 after sell → book $10,245.45; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 9 | $133.11 | $2.04 | $-157.32 | $5,937.80 | ▼ -157.32 after sell → book $10,243.41; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 54 | $26.25 | $2.17 | $-21.61 | $7,353.13 | ▼ -21.61 after sell → book $10,241.24; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $6,156.81 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1225.52 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 10 | $115.18 | $2.02 | — | $5,002.99 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1225.52 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 15 | $78.88 | $2.04 | — | $3,817.76 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1225.52 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 141 | $8.66 | $2.41 | — | $2,594.28 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $1225.52 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 378 | $3.24 | $4.88 | — | $1,364.69 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ret5=+21.3; leftover $1225.52 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 104 | $11.70 | $2.30 | — | $145.58 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $1225.52 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $145.58 | ▼ close $10,069.28 vs 09:30 $10,251.91 (session -156.29) | 16:00 close · cash $145.58 · equity $10,069.28 vs 09:30 $10,251.91 (-182.63; session marks -156.29) · 8 name(s) marked open→close (per-name table). AUTL×589 09:30 $2.47 → close $2.41 -35.34; CRSP×24 09:30 $59.72 → close $59.50 -5.28; AU×10 09:30 $119.43 → close $121.22 +17.90; FUTU×10 09:30 $115.18 → close $123.64 +84.60; GRAL×15 09:30 $78.88 → close $79.54 +9.90; ABTC×141 09:30 $8.66 → close $7.93 -102.93; HIVE×378 09:30 $3.24 → close $3.03 -79.38; MARA×104 09:30 $11.70 → close $11.26 -45.76 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $145.58 | ▼ 09:30 equity $10,032.23 vs yday $10,069.28 (-37.05) | 09:30 open · cash $145.58 (unchanged overnight, no fees) · equity $10,032.23 vs prior close $10,069.28 (-37.05) · 8 name(s) re-marked at the open (per-name table). AUTL×589 yday $2.41 → 09:30 $2.40 -5.89; CRSP×24 yday $59.50 → 09:30 $58.75 -18.00; AU×10 yday $121.22 → 09:30 $120.51 -7.10; FUTU×10 yday $123.64 → 09:30 $121.00 -26.40; GRAL×15 yday $79.54 → 09:30 $81.87 +34.95; ABTC×141 yday $7.93 → 09:30 $8.00 +9.87; HIVE×378 yday $3.03 → 09:30 $2.99 -15.12; MARA×104 yday $11.26 → 09:30 $11.17 -9.36 | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 589 | $2.40 | $7.71 | $-56.54 | $1,551.48 | ▼ -56.54 after sell → book $10,024.53; vs 09:30 mark -7.70 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $2,754.54 | ▲ +6.74 after sell → book $10,022.49; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 10 | $121.00 | $2.04 | $+54.14 | $3,962.50 | ▲ +54.14 after sell → book $10,020.45; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 15 | $81.87 | $2.06 | $+40.76 | $5,188.49 | ▲ +40.76 after sell → book $10,018.39; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ABTC` | 141 | $8.00 | $2.45 | $-97.92 | $6,314.05 | ▼ -97.92 after sell → book $10,015.95; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 378 | $2.99 | $4.95 | $-104.33 | $7,439.32 | ▼ -104.33 after sell → book $10,011.00; vs 09:30 mark -4.95 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 104 | $11.17 | $2.33 | $-59.75 | $8,598.67 | ▼ -59.75 after sell → book $10,008.67; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,598.67 | ▼ close $9,968.47 vs 09:30 $10,032.23 (session -40.20) | 16:00 close · cash $8,598.67 · equity $9,968.47 vs 09:30 $10,032.23 (-63.76; session marks -40.20) · 1 name(s) marked open→close (per-name table). CRSP×24 09:30 $58.75 → close $57.08 -40.20 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,598.67 | ▲ 09:30 equity $9,988.99 vs yday $9,968.47 (+20.52) | 09:30 open · cash $8,598.67 (unchanged overnight, no fees) · equity $9,988.99 vs prior close $9,968.47 (+20.52) · 1 name(s) re-marked at the open (per-name table). CRSP×24 yday $57.08 → 09:30 $57.93 +20.52 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 24 | $57.93 | $2.08 | $-23.35 | $9,986.90 | ▼ -23.35 after sell → book $9,986.90; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 28 | $118.52 | $2.07 | — | $6,666.27 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $3328.97 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 43 | $77.13 | $2.12 | — | $3,347.56 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $3328.97 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 94 | $35.05 | $2.27 | — | $50.59 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $3328.97 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $50.59 | ▲ close $10,253.26 vs 09:30 $9,988.99 (session +272.82) | 16:00 close · cash $50.59 · equity $10,253.26 vs 09:30 $9,988.99 (+264.27; session marks +272.82) · 3 name(s) marked open→close (per-name table). AU×28 09:30 $118.52 → close $123.39 +136.36; FCX×43 09:30 $77.13 → close $79.91 +119.54; EZPW×94 09:30 $35.05 → close $35.23 +16.92 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $50.59 | ▼ 09:30 equity $10,172.41 vs yday $10,253.26 (-80.85) | 09:30 open · cash $50.59 (unchanged overnight, no fees) · equity $10,172.41 vs prior close $10,253.26 (-80.85) · 3 name(s) re-marked at the open (per-name table). AU×28 yday $123.39 → 09:30 $119.80 -100.52; FCX×43 yday $79.91 → 09:30 $79.34 -24.51; EZPW×94 yday $35.23 → 09:30 $35.70 +44.18 | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 28 | $119.80 | $2.11 | $+31.66 | $3,402.88 | ▲ +31.66 after sell → book $10,170.30; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 43 | $79.34 | $2.16 | $+90.75 | $6,812.34 | ▲ +90.75 after sell → book $10,168.14; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 94 | $35.70 | $2.31 | $+56.51 | $10,165.83 | ▲ +56.51 after sell → book $10,165.83; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 38 | $267.02 | $2.10 | — | $16.96 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $10165.83 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.96 | ▲ close $10,177.02 vs 09:30 $10,172.41 (session +13.30) | 16:00 close · cash $16.96 · equity $10,177.02 vs 09:30 $10,172.41 (+4.61; session marks +13.30) · 1 name(s) marked open→close (per-name table). FNV×38 09:30 $267.02 → close $267.37 +13.30 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.96 | ▼ 09:30 equity $10,171.70 vs yday $10,177.02 (-5.32) | 09:30 open · cash $16.96 (unchanged overnight, no fees) · equity $10,171.70 vs prior close $10,177.02 (-5.32) · 1 name(s) re-marked at the open (per-name table). FNV×38 yday $267.37 → 09:30 $267.23 -5.32 | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 38 | $267.23 | $2.20 | $+3.68 | $10,169.51 | ▲ +3.68 after sell → book $10,169.51; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 24 | $81.65 | $2.06 | — | $8,207.85 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $2033.90 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 2 | $967.01 | $2.00 | — | $6,271.83 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $2033.90 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ASML` | 1 | $1746.53 | $1.99 | — | $4,523.31 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=-0.3; leftover $2033.90 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 6 | $318.88 | $2.01 | — | $2,608.02 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $2033.90 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 9 | $222.86 | $2.02 | — | $600.26 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=-3.6; leftover $2033.90 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $600.26 | ▼ close $10,101.11 vs 09:30 $10,171.70 (session -58.32) | 16:00 close · cash $600.26 · equity $10,101.11 vs 09:30 $10,171.70 (-70.59; session marks -58.32) · 5 name(s) marked open→close (per-name table). ACMR×24 09:30 $81.65 → close $80.49 -27.84; MU×2 09:30 $967.01 → close $935.39 -63.24; ASML×1 09:30 $1746.53 → close $1735.01 -11.52; LRCX×6 09:30 $318.88 → close $318.58 -1.80; NVDA×9 09:30 $222.86 → close $227.98 +46.08 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $600.26 | ▼ 09:30 equity $10,030.49 vs yday $10,101.11 (-70.62) | 09:30 open · cash $600.26 (unchanged overnight, no fees) · equity $10,030.49 vs prior close $10,101.11 (-70.62) · 5 name(s) re-marked at the open (per-name table). ACMR×24 yday $80.49 → 09:30 $79.27 -29.28; MU×2 yday $935.39 → 09:30 $919.29 -32.20; ASML×1 yday $1735.01 → 09:30 $1734.75 -0.26; LRCX×6 yday $318.58 → 09:30 $318.03 -3.30; NVDA×9 yday $227.98 → 09:30 $227.36 -5.58 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 24 | $79.27 | $2.09 | $-61.27 | $2,500.66 | ▼ -61.27 after sell → book $10,028.41; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 2 | $919.29 | $2.02 | $-99.46 | $4,337.21 | ▼ -99.46 after sell → book $10,026.38; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ASML` | 1 | $1734.75 | $2.02 | $-15.79 | $6,069.95 | ▼ -15.79 after sell → book $10,024.37; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 6 | $318.03 | $2.03 | $-9.14 | $7,976.09 | ▼ -9.14 after sell → book $10,022.33; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 9 | $227.36 | $2.04 | $+36.44 | $10,020.29 | ▲ +36.44 after sell → book $10,020.29; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 3 | $324.41 | $2.00 | — | $9,045.06 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1252.54 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $141.76 | $2.01 | — | $7,908.97 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1252.54 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $6,705.71 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1252.54 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 5 | $240.22 | $2.00 | — | $5,502.60 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1252.54 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 4 | $261.16 | $2.00 | — | $4,455.96 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list earn_react; ret5=+7.8; leftover $1252.54 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 38 | $32.90 | $2.10 | — | $3,203.66 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1252.54 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TLS` | 259 | $4.82 | $3.34 | — | $1,951.94 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+8.8; leftover $1252.54 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,951.94 | ▼ close $9,758.26 vs 09:30 $10,030.49 (session -246.57) | 16:00 close · cash $1,951.94 · equity $9,758.26 vs 09:30 $10,030.49 (-272.23; session marks -246.57) · 7 name(s) marked open→close (per-name table). KEYS×3 09:30 $324.41 → close $319.97 -13.32; SMTC×8 09:30 $141.76 → close $131.17 -84.72; CIEN×3 09:30 $400.42 → close $378.44 -65.94; DDOG×5 09:30 $240.22 → close $236.98 -16.20; ADSK×4 09:30 $261.16 → close $260.66 -2.00; SEDG×38 09:30 $32.90 → close $31.41 -56.62; TLS×259 09:30 $4.82 → close $4.79 -7.77 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,951.94 | ▼ 09:30 equity $9,743.28 vs yday $9,758.26 (-14.98) | 09:30 open · cash $1,951.94 (unchanged overnight, no fees) · equity $9,743.28 vs prior close $9,758.26 (-14.98) · 7 name(s) re-marked at the open (per-name table). KEYS×3 yday $319.97 → 09:30 $322.49 +7.56; SMTC×8 yday $131.17 → 09:30 $132.30 +9.04; CIEN×3 yday $378.44 → 09:30 $378.44 +0.00; DDOG×5 yday $236.98 → 09:30 $233.97 -15.07; ADSK×4 yday $260.66 → 09:30 $257.71 -11.80; SEDG×38 yday $31.41 → 09:30 $31.15 -9.88; TLS×259 yday $4.79 → 09:30 $4.81 +5.18 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 3 | $322.49 | $2.02 | $-9.78 | $2,917.39 | ▼ -9.78 after sell → book $9,741.26; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 8 | $132.30 | $2.03 | $-79.73 | $3,973.75 | ▼ -79.73 after sell → book $9,739.23; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 3 | $378.44 | $2.02 | $-69.96 | $5,107.05 | ▼ -69.96 after sell → book $9,737.21; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DDOG` | 5 | $233.97 | $2.02 | $-35.31 | $6,274.85 | ▼ -35.31 after sell → book $9,735.18; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 4 | $257.71 | $2.02 | $-17.82 | $7,303.67 | ▼ -17.82 after sell → book $9,733.16; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 38 | $31.15 | $2.12 | $-70.73 | $8,485.25 | ▼ -70.73 after sell → book $9,731.04; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TLS` | 259 | $4.81 | $3.39 | $-9.33 | $9,727.64 | ▼ -9.33 after sell → book $9,727.64; vs 09:30 mark -3.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,727.64 | ▲ close $9,727.64 vs 09:30 $9,743.28 (session +0.00) | 16:00 close · cash $9,727.64 · no lots left · equity $9,727.64. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,727.64 | ▲ 09:30 equity $9,727.64 vs yday $9,727.64 (+0.00) | 09:30 open · cash $9,727.64 · no holdings · equity $9,727.64 vs prior close $9,727.64 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,727.64 | ▲ close $9,727.64 vs 09:30 $9,727.64 (session +0.00) | 16:00 close · cash $9,727.64 · no lots left · equity $9,727.64. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,727.64 | ▲ 09:30 equity $9,727.64 vs yday $9,727.64 (+0.00) | 09:30 open · cash $9,727.64 · no holdings · equity $9,727.64 vs prior close $9,727.64 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,727.64 | ▲ close $9,727.64 vs 09:30 $9,727.64 (session +0.00) | 16:00 close · cash $9,727.64 · no lots left · equity $9,727.64. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,727.64 | ▲ 09:30 equity $9,727.64 vs yday $9,727.64 (+0.00) | 09:30 open · cash $9,727.64 · no holdings · equity $9,727.64 vs prior close $9,727.64 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $8,670.43 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1389.66 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $7,695.81 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1389.66 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 43 | $32.31 | $2.12 | — | $6,304.36 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1389.66 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 87 | $15.87 | $2.25 | — | $4,921.42 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1389.66 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 58 | $23.88 | $2.16 | — | $3,534.22 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1389.66 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 1 | $703.25 | $1.99 | — | $2,828.97 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1389.66 | join🟡 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 29 | $47.60 | $2.08 | — | $1,446.50 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list earn_react; 🔵; ret5=-6.2; leftover $1389.66 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,446.50 | ▲ close $10,124.33 vs 09:30 $9,727.64 (session +411.28) | 16:00 close · cash $1,446.50 · equity $10,124.33 vs 09:30 $9,727.64 (+396.69; session marks +411.28) · 7 name(s) marked open→close (per-name table). AVGO×3 09:30 $351.74 → close $357.16 +16.26; DELL×2 09:30 $486.31 → close $516.39 +60.16; CXW×43 09:30 $32.31 → close $33.66 +58.05; FRNM×87 09:30 $15.87 → close $16.90 +89.61; MMED×58 09:30 $23.88 → close $23.84 -2.32; DE×1 09:30 $703.25 → close $694.41 -8.84; HPE×29 09:30 $47.60 → close $54.44 +198.36 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,446.50 | ▼ 09:30 equity $10,055.14 vs yday $10,124.33 (-69.19) | 09:30 open · cash $1,446.50 (unchanged overnight, no fees) · equity $10,055.14 vs prior close $10,124.33 (-69.19) · 7 name(s) re-marked at the open (per-name table). AVGO×3 yday $357.16 → 09:30 $359.70 +7.62; DELL×2 yday $516.39 → 09:30 $513.78 -5.22; CXW×43 yday $33.66 → 09:30 $33.46 -8.60; FRNM×87 yday $16.90 → 09:30 $16.40 -43.50; MMED×58 yday $23.84 → 09:30 $23.84 +0.00; DE×1 yday $694.41 → 09:30 $692.03 -2.38; HPE×29 yday $54.44 → 09:30 $53.85 -17.11 | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 3 | $359.70 | $2.02 | $+19.86 | $2,523.58 | ▲ +19.86 after sell → book $10,053.12; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 2 | $513.78 | $2.02 | $+50.93 | $3,549.12 | ▲ +50.93 after sell → book $10,051.10; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 43 | $33.46 | $2.14 | $+45.19 | $4,985.76 | ▲ +45.19 after sell → book $10,048.96; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 58 | $23.84 | $2.19 | $-6.67 | $6,366.30 | ▼ -6.67 after sell → book $10,046.78; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 1 | $692.03 | $2.01 | $-15.23 | $7,056.31 | ▼ -15.23 after sell → book $10,044.76; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 29 | $53.85 | $2.10 | $+177.07 | $8,615.86 | ▲ +177.07 after sell → book $10,042.66; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 8 | $263.36 | $2.01 | — | $6,506.97 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $2153.97 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 28 | $75.65 | $2.07 | — | $4,386.69 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $2153.97 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 9 | $236.82 | $2.02 | — | $2,253.30 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; ret5=+8.1; leftover $2153.97 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 1110 | $1.94 | $14.32 | — | $85.58 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; ret5=+18.3; leftover $2153.97 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $85.58 | ▲ close $10,143.68 vs 09:30 $10,055.14 (session +121.44) | 16:00 close · cash $85.58 · equity $10,143.68 vs 09:30 $10,055.14 (+88.54; session marks +121.44) · 5 name(s) marked open→close (per-name table). FRNM×87 09:30 $16.40 → close $16.31 -7.83; CRM×8 09:30 $263.36 → close $259.23 -33.04; MRX×28 09:30 $75.65 → close $78.27 +73.36; BE×9 09:30 $236.82 → close $252.87 +144.45; BAK×1110 09:30 $1.94 → close $1.89 -55.50 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.58 | ▲ 09:30 equity $10,342.48 vs yday $10,143.68 (+198.80) | 09:30 open · cash $85.58 (unchanged overnight, no fees) · equity $10,342.48 vs prior close $10,143.68 (+198.80) · 5 name(s) re-marked at the open (per-name table). FRNM×87 yday $16.31 → 09:30 $16.74 +37.41; CRM×8 yday $259.23 → 09:30 $253.72 -44.08; MRX×28 yday $78.27 → 09:30 $78.84 +15.96; BE×9 yday $252.87 → 09:30 $267.76 +134.01; BAK×1110 yday $1.89 → 09:30 $1.94 +55.50 | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 87 | $16.74 | $2.28 | $+71.16 | $1,539.68 | ▲ +71.16 after sell → book $10,340.20; vs 09:30 mark -2.28 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 8 | $253.72 | $2.04 | $-81.17 | $3,567.40 | ▼ -81.17 after sell → book $10,338.16; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 9 | $267.76 | $2.05 | $+274.40 | $5,975.20 | ▲ +274.40 after sell → book $10,336.12; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 1110 | $1.94 | $14.52 | $-28.84 | $8,114.07 | ▼ -28.84 after sell → book $10,321.59; vs 09:30 mark -14.53 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,114.07 | ▼ close $10,261.95 vs 09:30 $10,342.48 (session -59.64) | 16:00 close · cash $8,114.07 · equity $10,261.95 vs 09:30 $10,342.48 (-80.53; session marks -59.64) · 1 name(s) marked open→close (per-name table). MRX×28 09:30 $78.84 → close $76.71 -59.64 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,114.07 | ▼ 09:30 equity $10,258.87 vs yday $10,261.95 (-3.08) | 09:30 open · cash $8,114.07 (unchanged overnight, no fees) · equity $10,258.87 vs prior close $10,261.95 (-3.08) · 1 name(s) re-marked at the open (per-name table). MRX×28 yday $76.71 → 09:30 $76.60 -3.08 | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 28 | $76.60 | $2.10 | $+22.42 | $10,256.77 | ▲ +22.42 after sell → book $10,256.77; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,256.77 | ▲ close $10,256.77 vs 09:30 $10,258.87 (session +0.00) | 16:00 close · cash $10,256.77 · no lots left · equity $10,256.77. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,256.77 | ▲ 09:30 equity $10,256.77 vs yday $10,256.77 (+0.00) | 09:30 open · cash $10,256.77 · no holdings · equity $10,256.77 vs prior close $10,256.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,256.77 | ▲ close $10,256.77 vs 09:30 $10,256.77 (session +0.00) | 16:00 close · cash $10,256.77 · no lots left · equity $10,256.77. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,256.77 | ▲ 09:30 equity $10,256.77 vs yday $10,256.77 (+0.00) | 09:30 open · cash $10,256.77 · no holdings · equity $10,256.77 vs prior close $10,256.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 62 | $164.43 | $2.18 | — | $59.94 | — | packet🟢 OR headline🟢 and camera net ≥ 4; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $10256.77 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $59.94 | ▼ close $9,377.30 vs 09:30 $10,256.77 (session -877.30) | 16:00 close · cash $59.94 · equity $9,377.30 vs 09:30 $10,256.77 (-879.47; session marks -877.30) · 1 name(s) marked open→close (per-name table). ORCL×62 09:30 $164.43 → close $150.28 -877.30 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.94 | ▼ 09:30 equity $8,827.98 vs yday $9,377.30 (-549.32) | 09:30 open · cash $59.94 (unchanged overnight, no fees) · equity $8,827.98 vs prior close $9,377.30 (-549.32) · 1 name(s) re-marked at the open (per-name table). ORCL×62 yday $150.28 → 09:30 $141.42 -549.32 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 62 | $141.42 | $2.26 | $-1431.05 | $8,825.72 | ▼ -1,431.05 after sell → book $8,825.72; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,825.72 | ▲ close $8,825.72 vs 09:30 $8,827.98 (session +0.00) | 16:00 close · cash $8,825.72 · no lots left · equity $8,825.72. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-28 | `MPWR` | cash | leftover split 1252.54 < 1 share @ 1306.03 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DVN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EOLS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
