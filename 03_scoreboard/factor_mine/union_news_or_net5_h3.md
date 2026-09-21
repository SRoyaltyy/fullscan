# Factor mine action — `union_news_or_net5_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · packet🟢 OR headline🟢 and camera net ≥ 5

Cash book **-26.18%** ($7,382) · signal-only (no cash/fees) was -20.43%. Starts YES **2/27**. Fills 64 · skips 89 · realized $-2353.92.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how many morning cameras are green vs red.
- Must-have: the morning news packet OR the prior-export headline is green.
- Must-have: camera net (+G −R) is at least 5.
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
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news_or_headline=True,cam_net_min=5` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $67.81.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `HLIT` | 379 | — | $13.18 | +0.00 | $13.92 | +280.46 | +280.46 | +0.00 | +280.46 |
| 2026-08-14 | `SNDK` | 3 | — | $1646.93 | +0.00 | $1641.11 | -17.46 | -17.46 | +0.00 | -17.46 |
| 2026-08-17 | `HLIT` | 379 | $13.92 | $13.84 | -30.32 | $13.43 | -155.39 | -185.71 | +250.14 | +94.75 |
| 2026-08-17 | `SNDK` | 3 | $1641.11 | $1700.74 | +178.90 | $1786.85 | +258.32 | +437.22 | +161.44 | +419.76 |
| 2026-08-18 | `HLIT` | 379 | $13.43 | $12.93 | -189.50 | $12.73 | -75.80 | -265.30 | -94.75 | -170.55 |
| 2026-08-18 | `SNDK` | 3 | $1786.85 | $1677.54 | -327.93 | $1625.78 | -155.28 | -483.21 | +91.83 | -63.45 |
| 2026-08-19 | `HLIT` | 379 | $12.73 | $12.90 | +64.43 | — | +0.00 | +64.43 | -106.12 | — |
| 2026-08-19 | `SNDK` | 3 | $1625.78 | $1682.40 | +169.88 | — | +0.00 | +169.88 | +106.42 | — |
| 2026-08-20 | `BHP` | 27 | — | $91.01 | +0.00 | $93.63 | +70.74 | +70.74 | +0.00 | +70.74 |
| 2026-08-20 | `APA` | 55 | — | $44.76 | +0.00 | $44.39 | -20.35 | -20.35 | +0.00 | -20.35 |
| 2026-08-20 | `AUTL` | 1010 | — | $2.47 | +0.00 | $2.46 | -10.10 | -10.10 | +0.00 | -10.10 |
| 2026-08-20 | `CRSP` | 42 | — | $58.73 | +0.00 | $58.12 | -25.62 | -25.62 | +0.00 | -25.62 |
| 2026-08-21 | `BHP` | 27 | $93.63 | $95.72 | +56.43 | $97.03 | +35.37 | +91.80 | +127.17 | +162.54 |
| 2026-08-21 | `APA` | 55 | $44.39 | $44.52 | +7.15 | $43.39 | -62.15 | -55.00 | -13.20 | -75.35 |
| 2026-08-21 | `AUTL` | 1010 | $2.46 | $2.47 | +10.10 | $2.41 | -60.60 | -50.50 | +0.00 | -60.60 |
| 2026-08-21 | `CRSP` | 42 | $58.12 | $59.72 | +67.20 | $59.50 | -9.24 | +57.96 | +41.58 | +32.34 |
| 2026-08-21 | `ABTC` | 1 | — | $8.66 | +0.00 | $7.93 | -0.73 | -0.73 | +0.00 | -0.73 |
| 2026-08-21 | `HIVE` | 4 | — | $3.24 | +0.00 | $3.03 | -0.84 | -0.84 | +0.00 | -0.84 |
| 2026-08-21 | `MARA` | 1 | — | $11.70 | +0.00 | $11.26 | -0.44 | -0.44 | +0.00 | -0.44 |
| 2026-08-24 | `BHP` | 27 | $97.03 | $97.31 | +7.56 | $97.13 | -4.86 | +2.70 | +170.10 | +165.24 |
| 2026-08-24 | `APA` | 55 | $43.39 | $42.93 | -25.30 | $42.96 | +1.65 | -23.65 | -100.65 | -99.00 |
| 2026-08-24 | `AUTL` | 1010 | $2.41 | $2.40 | -10.10 | $2.34 | -60.60 | -70.70 | -70.70 | -131.30 |
| 2026-08-24 | `CRSP` | 42 | $59.50 | $58.75 | -31.50 | $57.08 | -70.35 | -101.85 | +0.84 | -69.51 |
| 2026-08-24 | `ABTC` | 1 | $7.93 | $8.00 | +0.07 | $8.64 | +0.64 | +0.71 | -0.66 | -0.02 |
| 2026-08-24 | `HIVE` | 4 | $3.03 | $2.99 | -0.16 | $2.86 | -0.52 | -0.68 | -1.00 | -1.52 |
| 2026-08-24 | `MARA` | 1 | $11.26 | $11.17 | -0.09 | $11.18 | +0.01 | -0.08 | -0.53 | -0.52 |
| 2026-08-25 | `BHP` | 27 | $97.13 | $95.86 | -34.29 | — | +0.00 | -34.29 | +130.95 | — |
| 2026-08-25 | `APA` | 55 | $42.96 | $41.38 | -86.90 | — | +0.00 | -86.90 | -185.90 | — |
| 2026-08-25 | `AUTL` | 1010 | $2.34 | $2.38 | +40.40 | — | +0.00 | +40.40 | -90.90 | — |
| 2026-08-25 | `CRSP` | 42 | $57.08 | $57.93 | +35.91 | — | +0.00 | +35.91 | -33.60 | — |
| 2026-08-25 | `ABTC` | 1 | $8.64 | $8.62 | -0.02 | $9.24 | +0.62 | +0.60 | -0.04 | +0.58 |
| 2026-08-25 | `HIVE` | 4 | $2.86 | $2.87 | +0.04 | $3.02 | +0.60 | +0.64 | -1.48 | -0.88 |
| 2026-08-25 | `MARA` | 1 | $11.18 | $11.07 | -0.11 | $11.83 | +0.76 | +0.65 | -0.63 | +0.13 |
| 2026-08-25 | `AU` | 27 | — | $118.52 | +0.00 | $123.39 | +131.49 | +131.49 | +0.00 | +131.49 |
| 2026-08-25 | `FCX` | 42 | — | $77.13 | +0.00 | $79.91 | +116.76 | +116.76 | +0.00 | +116.76 |
| 2026-08-25 | `EZPW` | 92 | — | $35.05 | +0.00 | $35.23 | +16.56 | +16.56 | +0.00 | +16.56 |
| 2026-08-26 | `ABTC` | 1 | $9.24 | $8.84 | -0.40 | — | +0.00 | -0.40 | +0.18 | — |
| 2026-08-26 | `HIVE` | 4 | $3.02 | $2.95 | -0.28 | — | +0.00 | -0.28 | -1.16 | — |
| 2026-08-26 | `MARA` | 1 | $11.83 | $11.56 | -0.27 | — | +0.00 | -0.27 | -0.14 | — |
| 2026-08-26 | `AU` | 27 | $123.39 | $119.80 | -96.93 | $118.11 | -45.63 | -142.56 | +34.56 | -11.07 |
| 2026-08-26 | `FCX` | 42 | $79.91 | $79.34 | -23.94 | $79.00 | -14.28 | -38.22 | +92.82 | +78.54 |
| 2026-08-26 | `EZPW` | 92 | $35.23 | $35.70 | +43.24 | $33.90 | -165.60 | -122.36 | +59.80 | -105.80 |
| 2026-08-27 | `AU` | 27 | $118.11 | $117.41 | -18.90 | $118.40 | +26.73 | +7.83 | -29.97 | -3.24 |
| 2026-08-27 | `FCX` | 42 | $79.00 | $78.83 | -7.14 | $78.42 | -17.22 | -24.36 | +71.40 | +54.18 |
| 2026-08-27 | `EZPW` | 92 | $33.90 | $33.50 | -36.80 | $34.41 | +83.72 | +46.92 | -142.60 | -58.88 |
| 2026-08-28 | `AU` | 27 | $118.40 | $119.19 | +21.33 | — | +0.00 | +21.33 | +18.09 | — |
| 2026-08-28 | `FCX` | 42 | $78.42 | $78.57 | +6.30 | — | +0.00 | +6.30 | +60.48 | — |
| 2026-08-28 | `EZPW` | 92 | $34.41 | $34.50 | +8.28 | — | +0.00 | +8.28 | -50.60 | — |
| 2026-08-28 | `KEYS` | 4 | — | $324.41 | +0.00 | $319.97 | -17.76 | -17.76 | +0.00 | -17.76 |
| 2026-08-28 | `SMTC` | 9 | — | $141.76 | +0.00 | $131.17 | -95.31 | -95.31 | +0.00 | -95.31 |
| 2026-08-28 | `CIEN` | 3 | — | $400.42 | +0.00 | $378.44 | -65.94 | -65.94 | +0.00 | -65.94 |
| 2026-08-28 | `MPWR` | 1 | — | $1306.03 | +0.00 | $1256.26 | -49.77 | -49.77 | +0.00 | -49.77 |
| 2026-08-28 | `DDOG` | 5 | — | $240.22 | +0.00 | $236.98 | -16.20 | -16.20 | +0.00 | -16.20 |
| 2026-08-28 | `ADSK` | 5 | — | $261.16 | +0.00 | $260.66 | -2.50 | -2.50 | +0.00 | -2.50 |
| 2026-08-28 | `SEDG` | 42 | — | $32.90 | +0.00 | $31.41 | -62.58 | -62.58 | +0.00 | -62.58 |
| 2026-08-31 | `KEYS` | 4 | $319.97 | $322.49 | +10.08 | $322.70 | +0.84 | +10.92 | -7.68 | -6.84 |
| 2026-08-31 | `SMTC` | 9 | $131.17 | $132.30 | +10.17 | $132.96 | +5.94 | +16.11 | -85.14 | -79.20 |
| 2026-08-31 | `CIEN` | 3 | $378.44 | $378.44 | +0.00 | $382.80 | +13.08 | +13.08 | -65.94 | -52.86 |
| 2026-08-31 | `MPWR` | 1 | $1256.26 | $1261.90 | +5.64 | $1267.77 | +5.87 | +11.51 | -44.13 | -38.26 |
| 2026-08-31 | `DDOG` | 5 | $236.98 | $233.97 | -15.07 | $237.04 | +15.37 | +0.30 | -31.27 | -15.90 |
| 2026-08-31 | `ADSK` | 5 | $260.66 | $257.71 | -14.75 | $258.53 | +4.10 | -10.65 | -17.25 | -13.15 |
| 2026-08-31 | `SEDG` | 42 | $31.41 | $31.15 | -10.92 | $32.20 | +44.10 | +33.18 | -73.50 | -29.40 |
| 2026-09-01 | `KEYS` | 4 | $322.70 | $321.47 | -4.92 | $319.27 | -8.80 | -13.72 | -11.76 | -20.56 |
| 2026-09-01 | `SMTC` | 9 | $132.96 | $127.63 | -47.97 | $132.27 | +41.76 | -6.21 | -127.17 | -85.41 |
| 2026-09-01 | `CIEN` | 3 | $382.80 | $376.89 | -17.73 | $360.33 | -49.68 | -67.41 | -70.59 | -120.27 |
| 2026-09-01 | `MPWR` | 1 | $1267.77 | $1245.11 | -22.66 | $1225.96 | -19.15 | -41.81 | -60.92 | -80.07 |
| 2026-09-01 | `DDOG` | 5 | $237.04 | $232.88 | -20.80 | $223.84 | -45.20 | -66.00 | -36.70 | -81.90 |
| 2026-09-01 | `ADSK` | 5 | $258.53 | $253.48 | -25.25 | $247.69 | -28.95 | -54.20 | -38.40 | -67.35 |
| 2026-09-01 | `SEDG` | 42 | $32.20 | $31.87 | -13.86 | $32.49 | +26.04 | +12.18 | -43.26 | -17.22 |
| 2026-09-02 | `KEYS` | 4 | $319.27 | $318.04 | -4.92 | — | +0.00 | -4.92 | -25.48 | — |
| 2026-09-02 | `SMTC` | 9 | $132.27 | $133.00 | +6.57 | — | +0.00 | +6.57 | -78.84 | — |
| 2026-09-02 | `CIEN` | 3 | $360.33 | $357.25 | -9.24 | — | +0.00 | -9.24 | -129.51 | — |
| 2026-09-02 | `MPWR` | 1 | $1225.96 | $1224.92 | -1.04 | — | +0.00 | -1.04 | -81.11 | — |
| 2026-09-02 | `DDOG` | 5 | $223.84 | $219.46 | -21.90 | — | +0.00 | -21.90 | -103.80 | — |
| 2026-09-02 | `ADSK` | 5 | $247.69 | $246.70 | -4.95 | — | +0.00 | -4.95 | -72.30 | — |
| 2026-09-02 | `SEDG` | 42 | $32.49 | $32.42 | -2.94 | — | +0.00 | -2.94 | -20.16 | — |
| 2026-09-03 | `AVGO` | 4 | — | $351.74 | +0.00 | $357.16 | +21.68 | +21.68 | +0.00 | +21.68 |
| 2026-09-03 | `DELL` | 3 | — | $486.31 | +0.00 | $516.39 | +90.24 | +90.24 | +0.00 | +90.24 |
| 2026-09-03 | `CXW` | 47 | — | $32.31 | +0.00 | $33.66 | +63.45 | +63.45 | +0.00 | +63.45 |
| 2026-09-03 | `FRNM` | 97 | — | $15.87 | +0.00 | $16.90 | +99.91 | +99.91 | +0.00 | +99.91 |
| 2026-09-03 | `MMED` | 64 | — | $23.88 | +0.00 | $23.84 | -2.56 | -2.56 | +0.00 | -2.56 |
| 2026-09-03 | `DE` | 2 | — | $703.25 | +0.00 | $694.41 | -17.68 | -17.68 | +0.00 | -17.68 |
| 2026-09-04 | `AVGO` | 4 | $357.16 | $359.70 | +10.16 | $357.90 | -7.20 | +2.96 | +31.84 | +24.64 |
| 2026-09-04 | `DELL` | 3 | $516.39 | $513.78 | -7.83 | $524.14 | +31.08 | +23.25 | +82.41 | +113.49 |
| 2026-09-04 | `CXW` | 47 | $33.66 | $33.46 | -9.40 | $34.71 | +58.75 | +49.35 | +54.05 | +112.80 |
| 2026-09-04 | `FRNM` | 97 | $16.90 | $16.40 | -48.50 | $16.31 | -8.73 | -57.23 | +51.41 | +42.68 |
| 2026-09-04 | `MMED` | 64 | $23.84 | $23.84 | +0.00 | $23.29 | -35.20 | -35.20 | -2.56 | -37.76 |
| 2026-09-04 | `DE` | 2 | $694.41 | $692.03 | -4.76 | $693.53 | +3.00 | -1.76 | -22.44 | -19.44 |
| 2026-09-04 | `MRX` | 1 | — | $75.65 | +0.00 | $78.27 | +2.62 | +2.62 | +0.00 | +2.62 |
| 2026-09-08 | `AVGO` | 4 | $357.90 | $363.68 | +23.12 | $368.56 | +19.52 | +42.64 | +47.76 | +67.28 |
| 2026-09-08 | `DELL` | 3 | $524.14 | $521.15 | -8.97 | $533.88 | +38.19 | +29.22 | +104.52 | +142.71 |
| 2026-09-08 | `CXW` | 47 | $34.71 | $34.49 | -10.34 | $35.05 | +26.32 | +15.98 | +102.46 | +128.78 |
| 2026-09-08 | `FRNM` | 97 | $16.31 | $16.74 | +41.71 | $15.99 | -72.75 | -31.04 | +84.39 | +11.64 |
| 2026-09-08 | `MMED` | 64 | $23.29 | $23.16 | -8.32 | $23.32 | +10.24 | +1.92 | -46.08 | -35.84 |
| 2026-09-08 | `DE` | 2 | $693.53 | $687.21 | -12.64 | $680.73 | -12.96 | -25.60 | -32.08 | -45.04 |
| 2026-09-08 | `MRX` | 1 | $78.27 | $78.84 | +0.57 | $76.71 | -2.13 | -1.56 | +3.19 | +1.06 |
| 2026-09-09 | `AVGO` | 4 | $368.56 | $366.23 | -9.32 | — | +0.00 | -9.32 | +57.96 | — |
| 2026-09-09 | `DELL` | 3 | $533.88 | $538.47 | +13.77 | — | +0.00 | +13.77 | +156.48 | — |
| 2026-09-09 | `CXW` | 47 | $35.05 | $35.09 | +1.88 | — | +0.00 | +1.88 | +130.66 | — |
| 2026-09-09 | `FRNM` | 97 | $15.99 | $15.96 | -2.91 | — | +0.00 | -2.91 | +8.73 | — |
| 2026-09-09 | `MMED` | 64 | $23.32 | $23.22 | -6.40 | — | +0.00 | -6.40 | -42.24 | — |
| 2026-09-09 | `DE` | 2 | $680.73 | $681.32 | +1.18 | — | +0.00 | +1.18 | -43.86 | — |
| 2026-09-09 | `MRX` | 1 | $76.71 | $76.60 | -0.11 | $75.72 | -0.88 | -0.99 | +0.95 | +0.07 |
| 2026-09-10 | `MRX` | 1 | $75.72 | $75.00 | -0.72 | — | +0.00 | -0.72 | -0.65 | — |
| 2026-09-11 | `ORCL` | 57 | — | $164.43 | +0.00 | $150.28 | -806.55 | -806.55 | +0.00 | -806.55 |
| 2026-09-14 | `ORCL` | 57 | $150.28 | $141.42 | -505.02 | $144.79 | +192.09 | -312.93 | -1311.57 | -1119.48 |
| 2026-09-15 | `ORCL` | 57 | $144.79 | $143.46 | -75.81 | $140.35 | -177.27 | -253.08 | -1195.29 | -1372.56 |
| 2026-09-16 | `ORCL` | 57 | $140.35 | $140.03 | -18.24 | — | +0.00 | -18.24 | -1390.80 | — |
| 2026-09-16 | `WAY` | 102 | — | $26.27 | +0.00 | $26.59 | +32.64 | +32.64 | +0.00 | +32.64 |
| 2026-09-16 | `QCOM` | 14 | — | $189.17 | +0.00 | $184.84 | -60.62 | -60.62 | +0.00 | -60.62 |
| 2026-09-16 | `SM` | 67 | — | $39.99 | +0.00 | $38.16 | -122.61 | -122.61 | +0.00 | -122.61 |
| 2026-09-17 | `WAY` | 102 | $26.59 | $26.51 | -8.16 | $26.51 | +0.00 | -8.16 | +24.48 | +24.48 |
| 2026-09-17 | `QCOM` | 14 | $184.84 | $190.35 | +77.14 | $188.71 | -22.96 | +54.18 | +16.52 | -6.44 |
| 2026-09-17 | `SM` | 67 | $38.16 | $37.57 | -39.53 | $36.97 | -40.20 | -79.73 | -162.14 | -202.34 |
| 2026-09-17 | `GME` | 1 | — | $22.12 | +0.00 | $22.77 | +0.65 | +0.65 | +0.00 | +0.65 |
| 2026-09-18 | `WAY` | 102 | $26.51 | $26.95 | +44.88 | $25.66 | -131.58 | -86.70 | +69.36 | -62.22 |
| 2026-09-18 | `QCOM` | 14 | $188.71 | $191.34 | +36.82 | $177.72 | -190.68 | -153.86 | +30.38 | -160.30 |
| 2026-09-18 | `SM` | 67 | $36.97 | $36.87 | -6.70 | $36.97 | +6.70 | +0.00 | -209.04 | -202.34 |
| 2026-09-18 | `GME` | 1 | $22.77 | $22.90 | +0.13 | $22.64 | -0.26 | -0.13 | +0.78 | +0.52 |
| 2026-09-21 | `WAY` | 102 | $25.66 | $25.94 | +28.56 | — | +0.00 | +28.56 | -33.66 | — |
| 2026-09-21 | `QCOM` | 14 | $177.72 | $180.61 | +40.46 | — | +0.00 | +40.46 | -119.84 | — |
| 2026-09-21 | `SM` | 67 | $36.97 | $35.91 | -71.35 | — | +0.00 | -71.35 | -273.70 | — |
| 2026-09-21 | `GME` | 1 | $22.64 | $22.78 | +0.14 | $22.76 | -0.02 | +0.12 | +0.66 | +0.64 |
| 2026-09-21 | `VICR` | 11 | — | $230.25 | +0.00 | $223.90 | -69.85 | -69.85 | +0.00 | -69.85 |
| 2026-09-21 | `SMTC` | 13 | — | $190.30 | +0.00 | $177.37 | -168.09 | -168.09 | +0.00 | -168.09 |
| 2026-09-21 | `ALVO` | 429 | — | $5.92 | +0.00 | $5.88 | -17.16 | -17.16 | +0.00 | -17.16 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +263.00 | HLIT, SNDK | — | $57.10 | $10,256.11 | HLIT×379, SNDK×3 |
| 2026-08-17 | +2.25 | $57.10 | HLIT×379, SNDK×3 | $10,404.70 | +148.59 | +102.93 | — | — | $57.10 | $10,507.62 | HLIT×379, SNDK×3 |
| 2026-08-18 | -6.20 | $57.10 | HLIT×379, SNDK×3 | $9,990.19 | -517.43 | -231.08 | — | — | $57.10 | $9,759.11 | HLIT×379, SNDK×3 |
| 2026-08-19 | -7.20 | $57.10 | HLIT×379, SNDK×3 | $9,993.42 | +234.31 | +0.00 | — | HLIT, SNDK | $9,986.38 | $9,986.38 | — |
| 2026-08-20 | +1.12 | $9,986.38 | — | $9,986.38 | -0.00 | +14.67 | BHP, APA, AUTL, CRSP | — | $86.58 | $9,981.68 | BHP×27, APA×55, AUTL×1010, CRSP×42 |
| 2026-08-21 | +3.25 | $86.58 | BHP×27, APA×55, AUTL×1010, CRSP×42 | $10,122.56 | +140.88 | -98.63 | ABTC, HIVE, MARA | — | $52.90 | $10,023.57 | BHP×27, APA×55, AUTL×1010, CRSP×42, ABTC×1, HIVE×4, MARA×1 |
| 2026-08-24 | -5.17 | $52.90 | BHP×27, APA×55, AUTL×1010, CRSP×42, ABTC×1, HIVE×4, MARA×1 | $9,964.05 | -59.52 | -134.03 | — | — | $52.90 | $9,830.02 | BHP×27, APA×55, AUTL×1010, CRSP×42, ABTC×1, HIVE×4, MARA×1 |
| 2026-08-25 | +1.80 | $52.90 | BHP×27, APA×55, AUTL×1010, CRSP×42, ABTC×1, HIVE×4, MARA×1 | $9,785.05 | -44.97 | +266.79 | AU, FCX, EZPW | BHP, APA, AUTL, CRSP | $63.68 | $10,025.74 | ABTC×1, HIVE×4, MARA×1, AU×27, FCX×42, EZPW×92 |
| 2026-08-26 | +2.02 | $63.68 | ABTC×1, HIVE×4, MARA×1, AU×27, FCX×42, EZPW×92 | $9,947.16 | -78.58 | -225.51 | — | ABTC, HIVE, MARA | $95.48 | $9,721.25 | AU×27, FCX×42, EZPW×92 |
| 2026-08-27 | — | $95.48 | AU×27, FCX×42, EZPW×92 | $9,658.41 | -62.84 | +93.23 | — | — | $95.48 | $9,751.64 | AU×27, FCX×42, EZPW×92 |
| 2026-08-28 | +0.75 | $95.48 | AU×27, FCX×42, EZPW×92 | $9,787.55 | +35.91 | -310.06 | KEYS, SMTC, CIEN, MPWR, DDOG, ADSK, SEDG | AU, FCX, EZPW | $797.38 | $9,456.79 | KEYS×4, SMTC×9, CIEN×3, MPWR×1, DDOG×5, ADSK×5, SEDG×42 |
| 2026-08-31 | -5.85 | $797.38 | KEYS×4, SMTC×9, CIEN×3, MPWR×1, DDOG×5, ADSK×5, SEDG×42 | $9,441.94 | -14.85 | +89.30 | — | — | $797.38 | $9,531.24 | KEYS×4, SMTC×9, CIEN×3, MPWR×1, DDOG×5, ADSK×5, SEDG×42 |
| 2026-09-01 | -6.30 | $797.38 | KEYS×4, SMTC×9, CIEN×3, MPWR×1, DDOG×5, ADSK×5, SEDG×42 | $9,378.05 | -153.19 | -83.98 | — | — | $797.38 | $9,294.07 | KEYS×4, SMTC×9, CIEN×3, MPWR×1, DDOG×5, ADSK×5, SEDG×42 |
| 2026-09-02 | -3.83 | $797.38 | KEYS×4, SMTC×9, CIEN×3, MPWR×1, DDOG×5, ADSK×5, SEDG×42 | $9,255.65 | -38.42 | +0.00 | — | KEYS, SMTC, CIEN, MPWR, DDOG, ADSK, SEDG | $9,241.37 | $9,241.37 | — |
| 2026-09-03 | -0.90 | $9,241.37 | — | $9,241.37 | +0.00 | +255.04 | AVGO, DELL, CXW, FRNM, MMED, DE | — | $370.11 | $9,483.82 | AVGO×4, DELL×3, CXW×47, FRNM×97, MMED×64, DE×2 |
| 2026-09-04 | +2.25 | $370.11 | AVGO×4, DELL×3, CXW×47, FRNM×97, MMED×64, DE×2 | $9,423.49 | -60.33 | +44.32 | MRX | — | $293.70 | $9,467.05 | AVGO×4, DELL×3, CXW×47, FRNM×97, MMED×64, DE×2, MRX×1 |
| 2026-09-08 | -11.47 | $293.70 | AVGO×4, DELL×3, CXW×47, FRNM×97, MMED×64, DE×2, MRX×1 | $9,492.18 | +25.13 | +6.43 | — | — | $293.70 | $9,498.61 | AVGO×4, DELL×3, CXW×47, FRNM×97, MMED×64, DE×2, MRX×1 |
| 2026-09-09 | -13.95 | $293.70 | AVGO×4, DELL×3, CXW×47, FRNM×97, MMED×64, DE×2, MRX×1 | $9,496.70 | -1.91 | -0.88 | — | AVGO, DELL, CXW, FRNM, MMED, DE | $9,407.37 | $9,483.09 | MRX×1 |
| 2026-09-10 | -13.28 | $9,407.37 | MRX×1 | $9,482.37 | -0.72 | +0.00 | — | MRX | $9,481.60 | $9,481.60 | — |
| 2026-09-11 | +0.50 | $9,481.60 | — | $9,481.60 | -0.00 | -806.55 | ORCL | — | $106.93 | $8,672.89 | ORCL×57 |
| 2026-09-14 | -11.00 | $106.93 | ORCL×57 | $8,167.87 | -505.02 | +192.09 | — | — | $106.93 | $8,359.96 | ORCL×57 |
| 2026-09-15 | -3.84 | $106.93 | ORCL×57 | $8,284.15 | -75.81 | -177.27 | — | — | $106.93 | $8,106.88 | ORCL×57 |
| 2026-09-16 | +5.30 | $106.93 | ORCL×57 | $8,088.64 | -18.24 | -150.59 | WAY, QCOM, SM | ORCL | $72.63 | $7,929.29 | WAY×102, QCOM×14, SM×67 |
| 2026-09-17 | +7.38 | $72.63 | WAY×102, QCOM×14, SM×67 | $7,958.74 | +29.45 | -62.51 | GME | — | $50.29 | $7,896.01 | WAY×102, QCOM×14, SM×67, GME×1 |
| 2026-09-18 | +4.86 | $50.29 | WAY×102, QCOM×14, SM×67, GME×1 | $7,971.14 | +75.13 | -315.82 | — | — | $50.29 | $7,655.32 | WAY×102, QCOM×14, SM×67, GME×1 |
| 2026-09-21 | +12.87 | $50.29 | WAY×102, QCOM×14, SM×67, GME×1 | $7,653.13 | -2.19 | -255.12 | VICR, SMTC, ALVO | WAY, QCOM, SM | $67.81 | $7,381.80 | GME×1, VICR×11, SMTC×13, ALVO×429 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 379 | $13.18 | $4.89 | — | $4,999.89 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $5000.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SNDK` | 3 | $1646.93 | $2.00 | — | $57.10 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.4; leftover $5000.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.10 | ▲ close $10,256.11 vs 09:30 $10,000.00 (session +263.00) | 16:00 close · cash $57.10 · equity $10,256.11 vs 09:30 $10,000.00 (+256.11; session marks +263.00) · 2 name(s) marked open→close (per-name table). HLIT×379 09:30 $13.18 → close $13.92 +280.46; SNDK×3 09:30 $1646.93 → close $1641.11 -17.46 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.10 | ▲ 09:30 equity $10,404.70 vs yday $10,256.11 (+148.59) | 09:30 open · cash $57.10 (unchanged overnight, no fees) · equity $10,404.70 vs prior close $10,256.11 (+148.59) · 2 name(s) re-marked at the open (per-name table). HLIT×379 yday $13.92 → 09:30 $13.84 -30.32; SNDK×3 yday $1641.11 → 09:30 $1700.74 +178.90 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.10 | ▲ close $10,507.62 vs 09:30 $10,404.70 (session +102.93) | 16:00 close · cash $57.10 · equity $10,507.62 vs 09:30 $10,404.70 (+102.92; session marks +102.93) · 2 name(s) marked open→close (per-name table). HLIT×379 09:30 $13.84 → close $13.43 -155.39; SNDK×3 09:30 $1700.74 → close $1786.85 +258.32 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.10 | ▼ 09:30 equity $9,990.19 vs yday $10,507.62 (-517.43) | 09:30 open · cash $57.10 (unchanged overnight, no fees) · equity $9,990.19 vs prior close $10,507.62 (-517.43) · 2 name(s) re-marked at the open (per-name table). HLIT×379 yday $13.43 → 09:30 $12.93 -189.50; SNDK×3 yday $1786.85 → 09:30 $1677.54 -327.93 | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.10 | ▼ close $9,759.11 vs 09:30 $9,990.19 (session -231.08) | 16:00 close · cash $57.10 · equity $9,759.11 vs 09:30 $9,990.19 (-231.08; session marks -231.08) · 2 name(s) marked open→close (per-name table). HLIT×379 09:30 $12.93 → close $12.73 -75.80; SNDK×3 09:30 $1677.54 → close $1625.78 -155.28 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.10 | ▲ 09:30 equity $9,993.42 vs yday $9,759.11 (+234.31) | 09:30 open · cash $57.10 (unchanged overnight, no fees) · equity $9,993.42 vs prior close $9,759.11 (+234.31) · 2 name(s) re-marked at the open (per-name table). HLIT×379 yday $12.73 → 09:30 $12.90 +64.43; SNDK×3 yday $1625.78 → 09:30 $1682.40 +169.88 | — |
| 2026-08-19 09:30 ET | **SELL** | `HLIT` | 379 | $12.90 | $4.99 | $-116.00 | $4,941.21 | ▼ -116.00 after sell → book $9,988.43; vs 09:30 mark -4.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `SNDK` | 3 | $1682.40 | $2.05 | $+102.38 | $9,986.38 | ▲ +102.38 after sell → book $9,986.38; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,986.38 | ▲ close $9,986.38 vs 09:30 $9,993.42 (session +0.00) | 16:00 close · cash $9,986.38 · no lots left · equity $9,986.38. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,986.38 | ▲ 09:30 equity $9,986.38 vs yday $9,986.38 (-0.00) | 09:30 open · cash $9,986.38 · no holdings · equity $9,986.38 vs prior close $9,986.38 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 27 | $91.01 | $2.07 | — | $7,527.04 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $2496.59 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 55 | $44.76 | $2.15 | — | $5,063.08 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $2496.59 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 1010 | $2.47 | $13.03 | — | $2,555.35 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $2496.59 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 42 | $58.73 | $2.12 | — | $86.58 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $2496.59 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $86.58 | ▲ close $9,981.68 vs 09:30 $9,986.38 (session +14.67) | 16:00 close · cash $86.58 · equity $9,981.68 vs 09:30 $9,986.38 (-4.70; session marks +14.67) · 4 name(s) marked open→close (per-name table). BHP×27 09:30 $91.01 → close $93.63 +70.74; APA×55 09:30 $44.76 → close $44.39 -20.35; AUTL×1010 09:30 $2.47 → close $2.46 -10.10; CRSP×42 09:30 $58.73 → close $58.12 -25.62 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $86.58 | ▲ 09:30 equity $10,122.56 vs yday $9,981.68 (+140.88) | 09:30 open · cash $86.58 (unchanged overnight, no fees) · equity $10,122.56 vs prior close $9,981.68 (+140.88) · 4 name(s) re-marked at the open (per-name table). BHP×27 yday $93.63 → 09:30 $95.72 +56.43; APA×55 yday $44.39 → 09:30 $44.52 +7.15; AUTL×1010 yday $2.46 → 09:30 $2.47 +10.10; CRSP×42 yday $58.12 → 09:30 $59.72 +67.20 | — |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 1 | $8.66 | $0.09 | — | $77.83 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $14.43 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 4 | $3.24 | $0.14 | — | $64.72 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer; 🔵; ret5=+21.3; leftover $14.43 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 1 | $11.70 | $0.12 | — | $52.90 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $14.43 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $52.90 | ▼ close $10,023.57 vs 09:30 $10,122.56 (session -98.63) | 16:00 close · cash $52.90 · equity $10,023.57 vs 09:30 $10,122.56 (-98.99; session marks -98.63) · 7 name(s) marked open→close (per-name table). BHP×27 09:30 $95.72 → close $97.03 +35.37; APA×55 09:30 $44.52 → close $43.39 -62.15; AUTL×1010 09:30 $2.47 → close $2.41 -60.60; CRSP×42 09:30 $59.72 → close $59.50 -9.24; ABTC×1 09:30 $8.66 → close $7.93 -0.73; HIVE×4 09:30 $3.24 → close $3.03 -0.84; MARA×1 09:30 $11.70 → close $11.26 -0.44 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $52.90 | ▼ 09:30 equity $9,964.05 vs yday $10,023.57 (-59.52) | 09:30 open · cash $52.90 (unchanged overnight, no fees) · equity $9,964.05 vs prior close $10,023.57 (-59.52) · 7 name(s) re-marked at the open (per-name table). BHP×27 yday $97.03 → 09:30 $97.31 +7.56; APA×55 yday $43.39 → 09:30 $42.93 -25.30; AUTL×1010 yday $2.41 → 09:30 $2.40 -10.10; CRSP×42 yday $59.50 → 09:30 $58.75 -31.50; ABTC×1 yday $7.93 → 09:30 $8.00 +0.07; HIVE×4 yday $3.03 → 09:30 $2.99 -0.16; MARA×1 yday $11.26 → 09:30 $11.17 -0.09 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $52.90 | ▼ close $9,830.02 vs 09:30 $9,964.05 (session -134.03) | 16:00 close · cash $52.90 · equity $9,830.02 vs 09:30 $9,964.05 (-134.03; session marks -134.03) · 7 name(s) marked open→close (per-name table). BHP×27 09:30 $97.31 → close $97.13 -4.86; APA×55 09:30 $42.93 → close $42.96 +1.65; AUTL×1010 09:30 $2.40 → close $2.34 -60.60; CRSP×42 09:30 $58.75 → close $57.08 -70.35; ABTC×1 09:30 $8.00 → close $8.64 +0.64; HIVE×4 09:30 $2.99 → close $2.86 -0.52; MARA×1 09:30 $11.17 → close $11.18 +0.01 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $52.90 | ▼ 09:30 equity $9,785.05 vs yday $9,830.02 (-44.97) | 09:30 open · cash $52.90 (unchanged overnight, no fees) · equity $9,785.05 vs prior close $9,830.02 (-44.97) · 7 name(s) re-marked at the open (per-name table). BHP×27 yday $97.13 → 09:30 $95.86 -34.29; APA×55 yday $42.96 → 09:30 $41.38 -86.90; AUTL×1010 yday $2.34 → 09:30 $2.38 +40.40; CRSP×42 yday $57.08 → 09:30 $57.93 +35.91; ABTC×1 yday $8.64 → 09:30 $8.62 -0.02; HIVE×4 yday $2.86 → 09:30 $2.87 +0.04; MARA×1 yday $11.18 → 09:30 $11.07 -0.11 | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 27 | $95.86 | $2.10 | $+126.78 | $2,639.02 | ▲ +126.78 after sell → book $9,782.95; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `APA` | 55 | $41.38 | $2.18 | $-190.24 | $4,912.74 | ▼ -190.24 after sell → book $9,780.77; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AUTL` | 1010 | $2.38 | $13.22 | $-117.14 | $7,303.32 | ▼ -117.14 after sell → book $9,767.55; vs 09:30 mark -13.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 42 | $57.93 | $2.15 | $-37.86 | $9,734.24 | ▼ -37.86 after sell → book $9,765.41; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 27 | $118.52 | $2.07 | — | $6,532.13 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $3244.75 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 42 | $77.13 | $2.12 | — | $3,290.55 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $3244.75 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 92 | $35.05 | $2.27 | — | $63.68 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $3244.75 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.68 | ▲ close $10,025.74 vs 09:30 $9,785.05 (session +266.79) | 16:00 close · cash $63.68 · equity $10,025.74 vs 09:30 $9,785.05 (+240.69; session marks +266.79) · 6 name(s) marked open→close (per-name table). ABTC×1 09:30 $8.62 → close $9.24 +0.62; HIVE×4 09:30 $2.87 → close $3.02 +0.60; MARA×1 09:30 $11.07 → close $11.83 +0.76; AU×27 09:30 $118.52 → close $123.39 +131.49; FCX×42 09:30 $77.13 → close $79.91 +116.76; EZPW×92 09:30 $35.05 → close $35.23 +16.56 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.68 | ▼ 09:30 equity $9,947.16 vs yday $10,025.74 (-78.58) | 09:30 open · cash $63.68 (unchanged overnight, no fees) · equity $9,947.16 vs prior close $10,025.74 (-78.58) · 6 name(s) re-marked at the open (per-name table). ABTC×1 yday $9.24 → 09:30 $8.84 -0.40; HIVE×4 yday $3.02 → 09:30 $2.95 -0.28; MARA×1 yday $11.83 → 09:30 $11.56 -0.27; AU×27 yday $123.39 → 09:30 $119.80 -96.93; FCX×42 yday $79.91 → 09:30 $79.34 -23.94; EZPW×92 yday $35.23 → 09:30 $35.70 +43.24 | — |
| 2026-08-26 09:30 ET | **SELL** | `ABTC` | 1 | $8.84 | $0.11 | $-0.02 | $72.41 | ▼ -0.02 after sell → book $9,947.05; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `HIVE` | 4 | $2.95 | $0.15 | $-1.45 | $84.06 | ▼ -1.45 after sell → book $9,946.90; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `MARA` | 1 | $11.56 | $0.14 | $-0.40 | $95.48 | ▼ -0.40 after sell → book $9,946.76; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $95.48 | ▼ close $9,721.25 vs 09:30 $9,947.16 (session -225.51) | 16:00 close · cash $95.48 · equity $9,721.25 vs 09:30 $9,947.16 (-225.91; session marks -225.51) · 3 name(s) marked open→close (per-name table). AU×27 09:30 $119.80 → close $118.11 -45.63; FCX×42 09:30 $79.34 → close $79.00 -14.28; EZPW×92 09:30 $35.70 → close $33.90 -165.60 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.48 | ▼ 09:30 equity $9,658.41 vs yday $9,721.25 (-62.84) | 09:30 open · cash $95.48 (unchanged overnight, no fees) · equity $9,658.41 vs prior close $9,721.25 (-62.84) · 3 name(s) re-marked at the open (per-name table). AU×27 yday $118.11 → 09:30 $117.41 -18.90; FCX×42 yday $79.00 → 09:30 $78.83 -7.14; EZPW×92 yday $33.90 → 09:30 $33.50 -36.80 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $95.48 | ▲ close $9,751.64 vs 09:30 $9,658.41 (session +93.23) | 16:00 close · cash $95.48 · equity $9,751.64 vs 09:30 $9,658.41 (+93.23; session marks +93.23) · 3 name(s) marked open→close (per-name table). AU×27 09:30 $117.41 → close $118.40 +26.73; FCX×42 09:30 $78.83 → close $78.42 -17.22; EZPW×92 09:30 $33.50 → close $34.41 +83.72 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.48 | ▲ 09:30 equity $9,787.55 vs yday $9,751.64 (+35.91) | 09:30 open · cash $95.48 (unchanged overnight, no fees) · equity $9,787.55 vs prior close $9,751.64 (+35.91) · 3 name(s) re-marked at the open (per-name table). AU×27 yday $118.40 → 09:30 $119.19 +21.33; FCX×42 yday $78.42 → 09:30 $78.57 +6.30; EZPW×92 yday $34.41 → 09:30 $34.50 +8.28 | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 27 | $119.19 | $2.11 | $+13.91 | $3,311.51 | ▲ +13.91 after sell → book $9,785.45; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 42 | $78.57 | $2.15 | $+56.21 | $6,609.30 | ▲ +56.21 after sell → book $9,783.30; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EZPW` | 92 | $34.50 | $2.31 | $-55.17 | $9,780.99 | ▼ -55.17 after sell → book $9,780.99; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $8,481.35 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1397.28 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $7,203.49 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1397.28 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $6,000.23 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1397.28 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $4,692.21 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $1397.28 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 5 | $240.22 | $2.00 | — | $3,489.10 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1397.28 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 5 | $261.16 | $2.00 | — | $2,181.30 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list earn_react; ret5=+7.8; leftover $1397.28 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 42 | $32.90 | $2.12 | — | $797.38 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1397.28 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $797.38 | ▼ close $9,456.79 vs 09:30 $9,787.55 (session -310.06) | 16:00 close · cash $797.38 · equity $9,456.79 vs 09:30 $9,787.55 (-330.76; session marks -310.06) · 7 name(s) marked open→close (per-name table). KEYS×4 09:30 $324.41 → close $319.97 -17.76; SMTC×9 09:30 $141.76 → close $131.17 -95.31; CIEN×3 09:30 $400.42 → close $378.44 -65.94; MPWR×1 09:30 $1306.03 → close $1256.26 -49.77; DDOG×5 09:30 $240.22 → close $236.98 -16.20; ADSK×5 09:30 $261.16 → close $260.66 -2.50; SEDG×42 09:30 $32.90 → close $31.41 -62.58 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $797.38 | ▼ 09:30 equity $9,441.94 vs yday $9,456.79 (-14.85) | 09:30 open · cash $797.38 (unchanged overnight, no fees) · equity $9,441.94 vs prior close $9,456.79 (-14.85) · 7 name(s) re-marked at the open (per-name table). KEYS×4 yday $319.97 → 09:30 $322.49 +10.08; SMTC×9 yday $131.17 → 09:30 $132.30 +10.17; CIEN×3 yday $378.44 → 09:30 $378.44 +0.00; MPWR×1 yday $1256.26 → 09:30 $1261.90 +5.64; DDOG×5 yday $236.98 → 09:30 $233.97 -15.07; ADSK×5 yday $260.66 → 09:30 $257.71 -14.75; SEDG×42 yday $31.41 → 09:30 $31.15 -10.92 | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $797.38 | ▲ close $9,531.24 vs 09:30 $9,441.94 (session +89.30) | 16:00 close · cash $797.38 · equity $9,531.24 vs 09:30 $9,441.94 (+89.30; session marks +89.30) · 7 name(s) marked open→close (per-name table). KEYS×4 09:30 $322.49 → close $322.70 +0.84; SMTC×9 09:30 $132.30 → close $132.96 +5.94; CIEN×3 09:30 $378.44 → close $382.80 +13.08; MPWR×1 09:30 $1261.90 → close $1267.77 +5.87; DDOG×5 09:30 $233.97 → close $237.04 +15.37; ADSK×5 09:30 $257.71 → close $258.53 +4.10; SEDG×42 09:30 $31.15 → close $32.20 +44.10 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $797.38 | ▼ 09:30 equity $9,378.05 vs yday $9,531.24 (-153.19) | 09:30 open · cash $797.38 (unchanged overnight, no fees) · equity $9,378.05 vs prior close $9,531.24 (-153.19) · 7 name(s) re-marked at the open (per-name table). KEYS×4 yday $322.70 → 09:30 $321.47 -4.92; SMTC×9 yday $132.96 → 09:30 $127.63 -47.97; CIEN×3 yday $382.80 → 09:30 $376.89 -17.73; MPWR×1 yday $1267.77 → 09:30 $1245.11 -22.66; DDOG×5 yday $237.04 → 09:30 $232.88 -20.80; ADSK×5 yday $258.53 → 09:30 $253.48 -25.25; SEDG×42 yday $32.20 → 09:30 $31.87 -13.86 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $797.38 | ▼ close $9,294.07 vs 09:30 $9,378.05 (session -83.98) | 16:00 close · cash $797.38 · equity $9,294.07 vs 09:30 $9,378.05 (-83.98; session marks -83.98) · 7 name(s) marked open→close (per-name table). KEYS×4 09:30 $321.47 → close $319.27 -8.80; SMTC×9 09:30 $127.63 → close $132.27 +41.76; CIEN×3 09:30 $376.89 → close $360.33 -49.68; MPWR×1 09:30 $1245.11 → close $1225.96 -19.15; DDOG×5 09:30 $232.88 → close $223.84 -45.20; ADSK×5 09:30 $253.48 → close $247.69 -28.95; SEDG×42 09:30 $31.87 → close $32.49 +26.04 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $797.38 | ▼ 09:30 equity $9,255.65 vs yday $9,294.07 (-38.42) | 09:30 open · cash $797.38 (unchanged overnight, no fees) · equity $9,255.65 vs prior close $9,294.07 (-38.42) · 7 name(s) re-marked at the open (per-name table). KEYS×4 yday $319.27 → 09:30 $318.04 -4.92; SMTC×9 yday $132.27 → 09:30 $133.00 +6.57; CIEN×3 yday $360.33 → 09:30 $357.25 -9.24; MPWR×1 yday $1225.96 → 09:30 $1224.92 -1.04; DDOG×5 yday $223.84 → 09:30 $219.46 -21.90; ADSK×5 yday $247.69 → 09:30 $246.70 -4.95; SEDG×42 yday $32.49 → 09:30 $32.42 -2.94 | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 4 | $318.04 | $2.02 | $-29.50 | $2,067.52 | ▼ -29.50 after sell → book $9,253.63; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 9 | $133.00 | $2.04 | $-82.89 | $3,262.48 | ▼ -82.89 after sell → book $9,251.59; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CIEN` | 3 | $357.25 | $2.02 | $-133.53 | $4,332.21 | ▼ -133.53 after sell → book $9,249.57; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | join🔴 sector🟢 gen🔴 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-02 09:30 ET | **SELL** | `MPWR` | 1 | $1224.92 | $2.01 | $-85.12 | $5,555.12 | ▼ -85.12 after sell → book $9,247.56; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `DDOG` | 5 | $219.46 | $2.02 | $-107.83 | $6,650.40 | ▼ -107.83 after sell → book $9,245.54; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 5 | $246.70 | $2.02 | $-76.33 | $7,881.87 | ▼ -76.33 after sell → book $9,243.51; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 42 | $32.42 | $2.14 | $-24.41 | $9,241.37 | ▼ -24.41 after sell → book $9,241.37; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,241.37 | ▲ close $9,241.37 vs 09:30 $9,255.65 (session +0.00) | 16:00 close · cash $9,241.37 · no lots left · equity $9,241.37. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,241.37 | ▲ 09:30 equity $9,241.37 vs yday $9,241.37 (+0.00) | 09:30 open · cash $9,241.37 · no holdings · equity $9,241.37 vs prior close $9,241.37 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 4 | $351.74 | $2.00 | — | $7,832.41 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1540.23 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 3 | $486.31 | $2.00 | — | $6,371.48 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1540.23 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 47 | $32.31 | $2.13 | — | $4,850.78 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1540.23 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 97 | $15.87 | $2.28 | — | $3,309.11 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1540.23 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 64 | $23.88 | $2.18 | — | $1,778.61 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1540.23 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 2 | $703.25 | $2.00 | — | $370.11 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1540.23 | join🟡 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $370.11 | ▲ close $9,483.82 vs 09:30 $9,241.37 (session +255.04) | 16:00 close · cash $370.11 · equity $9,483.82 vs 09:30 $9,241.37 (+242.45; session marks +255.04) · 6 name(s) marked open→close (per-name table). AVGO×4 09:30 $351.74 → close $357.16 +21.68; DELL×3 09:30 $486.31 → close $516.39 +90.24; CXW×47 09:30 $32.31 → close $33.66 +63.45; FRNM×97 09:30 $15.87 → close $16.90 +99.91; MMED×64 09:30 $23.88 → close $23.84 -2.56; DE×2 09:30 $703.25 → close $694.41 -17.68 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $370.11 | ▼ 09:30 equity $9,423.49 vs yday $9,483.82 (-60.33) | 09:30 open · cash $370.11 (unchanged overnight, no fees) · equity $9,423.49 vs prior close $9,483.82 (-60.33) · 6 name(s) re-marked at the open (per-name table). AVGO×4 yday $357.16 → 09:30 $359.70 +10.16; DELL×3 yday $516.39 → 09:30 $513.78 -7.83; CXW×47 yday $33.66 → 09:30 $33.46 -9.40; FRNM×97 yday $16.90 → 09:30 $16.40 -48.50; MMED×64 yday $23.84 → 09:30 $23.84 +0.00; DE×2 yday $694.41 → 09:30 $692.03 -4.76 | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 1 | $75.65 | $0.76 | — | $293.70 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $123.37 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $293.70 | ▲ close $9,467.05 vs 09:30 $9,423.49 (session +44.32) | 16:00 close · cash $293.70 · equity $9,467.05 vs 09:30 $9,423.49 (+43.56; session marks +44.32) · 7 name(s) marked open→close (per-name table). AVGO×4 09:30 $359.70 → close $357.90 -7.20; DELL×3 09:30 $513.78 → close $524.14 +31.08; CXW×47 09:30 $33.46 → close $34.71 +58.75; FRNM×97 09:30 $16.40 → close $16.31 -8.73; MMED×64 09:30 $23.84 → close $23.29 -35.20; DE×2 09:30 $692.03 → close $693.53 +3.00; MRX×1 09:30 $75.65 → close $78.27 +2.62 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $293.70 | ▲ 09:30 equity $9,492.18 vs yday $9,467.05 (+25.13) | 09:30 open · cash $293.70 (unchanged overnight, no fees) · equity $9,492.18 vs prior close $9,467.05 (+25.13) · 7 name(s) re-marked at the open (per-name table). AVGO×4 yday $357.90 → 09:30 $363.68 +23.12; DELL×3 yday $524.14 → 09:30 $521.15 -8.97; CXW×47 yday $34.71 → 09:30 $34.49 -10.34; FRNM×97 yday $16.31 → 09:30 $16.74 +41.71; MMED×64 yday $23.29 → 09:30 $23.16 -8.32; DE×2 yday $693.53 → 09:30 $687.21 -12.64; MRX×1 yday $78.27 → 09:30 $78.84 +0.57 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $293.70 | ▲ close $9,498.61 vs 09:30 $9,492.18 (session +6.43) | 16:00 close · cash $293.70 · equity $9,498.61 vs 09:30 $9,492.18 (+6.43; session marks +6.43) · 7 name(s) marked open→close (per-name table). AVGO×4 09:30 $363.68 → close $368.56 +19.52; DELL×3 09:30 $521.15 → close $533.88 +38.19; CXW×47 09:30 $34.49 → close $35.05 +26.32; FRNM×97 09:30 $16.74 → close $15.99 -72.75; MMED×64 09:30 $23.16 → close $23.32 +10.24; DE×2 09:30 $687.21 → close $680.73 -12.96; MRX×1 09:30 $78.84 → close $76.71 -2.13 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $293.70 | ▼ 09:30 equity $9,496.70 vs yday $9,498.61 (-1.91) | 09:30 open · cash $293.70 (unchanged overnight, no fees) · equity $9,496.70 vs prior close $9,498.61 (-1.91) · 7 name(s) re-marked at the open (per-name table). AVGO×4 yday $368.56 → 09:30 $366.23 -9.32; DELL×3 yday $533.88 → 09:30 $538.47 +13.77; CXW×47 yday $35.05 → 09:30 $35.09 +1.88; FRNM×97 yday $15.99 → 09:30 $15.96 -2.91; MMED×64 yday $23.32 → 09:30 $23.22 -6.40; DE×2 yday $680.73 → 09:30 $681.32 +1.18; MRX×1 yday $76.71 → 09:30 $76.60 -0.11 | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 4 | $366.23 | $2.02 | $+53.93 | $1,756.60 | ▲ +53.93 after sell → book $9,494.68; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DELL` | 3 | $538.47 | $2.02 | $+152.46 | $3,369.99 | ▲ +152.46 after sell → book $9,492.66; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CXW` | 47 | $35.09 | $2.15 | $+126.37 | $5,017.06 | ▲ +126.37 after sell → book $9,490.50; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FRNM` | 97 | $15.96 | $2.31 | $+4.14 | $6,562.87 | ▲ +4.14 after sell → book $9,488.19; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MMED` | 64 | $23.22 | $2.20 | $-46.63 | $8,046.75 | ▼ -46.63 after sell → book $9,485.99; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DE` | 2 | $681.32 | $2.02 | $-47.87 | $9,407.37 | ▼ -47.87 after sell → book $9,483.97; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,407.37 | ▼ close $9,483.09 vs 09:30 $9,496.70 (session -0.88) | 16:00 close · cash $9,407.37 · equity $9,483.09 vs 09:30 $9,496.70 (-13.61; session marks -0.88) · 1 name(s) marked open→close (per-name table). MRX×1 09:30 $76.60 → close $75.72 -0.88 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,407.37 | ▼ 09:30 equity $9,482.37 vs yday $9,483.09 (-0.72) | 09:30 open · cash $9,407.37 (unchanged overnight, no fees) · equity $9,482.37 vs prior close $9,483.09 (-0.72) · 1 name(s) re-marked at the open (per-name table). MRX×1 yday $75.72 → 09:30 $75.00 -0.72 | — |
| 2026-09-10 09:30 ET | **SELL** | `MRX` | 1 | $75.00 | $0.77 | $-2.18 | $9,481.60 | ▼ -2.18 after sell → book $9,481.60; vs 09:30 mark -0.77 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,481.60 | ▲ close $9,481.60 vs 09:30 $9,482.37 (session +0.00) | 16:00 close · cash $9,481.60 · no lots left · equity $9,481.60. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,481.60 | ▲ 09:30 equity $9,481.60 vs yday $9,481.60 (-0.00) | 09:30 open · cash $9,481.60 · no holdings · equity $9,481.60 vs prior close $9,481.60 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 57 | $164.43 | $2.16 | — | $106.93 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $9481.60 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $106.93 | ▼ close $8,672.89 vs 09:30 $9,481.60 (session -806.55) | 16:00 close · cash $106.93 · equity $8,672.89 vs 09:30 $9,481.60 (-808.71; session marks -806.55) · 1 name(s) marked open→close (per-name table). ORCL×57 09:30 $164.43 → close $150.28 -806.55 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $106.93 | ▼ 09:30 equity $8,167.87 vs yday $8,672.89 (-505.02) | 09:30 open · cash $106.93 (unchanged overnight, no fees) · equity $8,167.87 vs prior close $8,672.89 (-505.02) · 1 name(s) re-marked at the open (per-name table). ORCL×57 yday $150.28 → 09:30 $141.42 -505.02 | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $106.93 | ▲ close $8,359.96 vs 09:30 $8,167.87 (session +192.09) | 16:00 close · cash $106.93 · equity $8,359.96 vs 09:30 $8,167.87 (+192.09; session marks +192.09) · 1 name(s) marked open→close (per-name table). ORCL×57 09:30 $141.42 → close $144.79 +192.09 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $106.93 | ▼ 09:30 equity $8,284.15 vs yday $8,359.96 (-75.81) | 09:30 open · cash $106.93 (unchanged overnight, no fees) · equity $8,284.15 vs prior close $8,359.96 (-75.81) · 1 name(s) re-marked at the open (per-name table). ORCL×57 yday $144.79 → 09:30 $143.46 -75.81 | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $106.93 | ▼ close $8,106.88 vs 09:30 $8,284.15 (session -177.27) | 16:00 close · cash $106.93 · equity $8,106.88 vs 09:30 $8,284.15 (-177.27; session marks -177.27) · 1 name(s) marked open→close (per-name table). ORCL×57 09:30 $143.46 → close $140.35 -177.27 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $106.93 | ▼ 09:30 equity $8,088.64 vs yday $8,106.88 (-18.24) | 09:30 open · cash $106.93 (unchanged overnight, no fees) · equity $8,088.64 vs prior close $8,106.88 (-18.24) · 1 name(s) re-marked at the open (per-name table). ORCL×57 yday $140.35 → 09:30 $140.03 -18.24 | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 57 | $140.03 | $2.23 | $-1395.20 | $8,086.40 | ▼ -1,395.20 after sell → book $8,086.40; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 102 | $26.27 | $2.30 | — | $5,404.57 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $2695.47 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 14 | $189.17 | $2.03 | — | $2,754.16 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $2695.47 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 67 | $39.99 | $2.19 | — | $72.63 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $2695.47 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $72.63 | ▼ close $7,929.29 vs 09:30 $8,088.64 (session -150.59) | 16:00 close · cash $72.63 · equity $7,929.29 vs 09:30 $8,088.64 (-159.35; session marks -150.59) · 3 name(s) marked open→close (per-name table). WAY×102 09:30 $26.27 → close $26.59 +32.64; QCOM×14 09:30 $189.17 → close $184.84 -60.62; SM×67 09:30 $39.99 → close $38.16 -122.61 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $72.63 | ▲ 09:30 equity $7,958.74 vs yday $7,929.29 (+29.45) | 09:30 open · cash $72.63 (unchanged overnight, no fees) · equity $7,958.74 vs prior close $7,929.29 (+29.45) · 3 name(s) re-marked at the open (per-name table). WAY×102 yday $26.59 → 09:30 $26.51 -8.16; QCOM×14 yday $184.84 → 09:30 $190.35 +77.14; SM×67 yday $38.16 → 09:30 $37.57 -39.53 | — |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 1 | $22.12 | $0.22 | — | $50.29 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $24.21 | join🟢 sector🟢 gen🟢 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $50.29 | ▼ close $7,896.01 vs 09:30 $7,958.74 (session -62.51) | 16:00 close · cash $50.29 · equity $7,896.01 vs 09:30 $7,958.74 (-62.73; session marks -62.51) · 4 name(s) marked open→close (per-name table). WAY×102 09:30 $26.51 → close $26.51 +0.00; QCOM×14 09:30 $190.35 → close $188.71 -22.96; SM×67 09:30 $37.57 → close $36.97 -40.20; GME×1 09:30 $22.12 → close $22.77 +0.65 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $50.29 | ▲ 09:30 equity $7,971.14 vs yday $7,896.01 (+75.13) | 09:30 open · cash $50.29 (unchanged overnight, no fees) · equity $7,971.14 vs prior close $7,896.01 (+75.13) · 4 name(s) re-marked at the open (per-name table). WAY×102 yday $26.51 → 09:30 $26.95 +44.88; QCOM×14 yday $188.71 → 09:30 $191.34 +36.82; SM×67 yday $36.97 → 09:30 $36.87 -6.70; GME×1 yday $22.77 → 09:30 $22.90 +0.13 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $50.29 | ▼ close $7,655.32 vs 09:30 $7,971.14 (session -315.82) | 16:00 close · cash $50.29 · equity $7,655.32 vs 09:30 $7,971.14 (-315.82; session marks -315.82) · 4 name(s) marked open→close (per-name table). WAY×102 09:30 $26.95 → close $25.66 -131.58; QCOM×14 09:30 $191.34 → close $177.72 -190.68; SM×67 09:30 $36.87 → close $36.97 +6.70; GME×1 09:30 $22.90 → close $22.64 -0.26 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $50.29 | ▼ 09:30 equity $7,653.13 vs yday $7,655.32 (-2.19) | 09:30 open · cash $50.29 (unchanged overnight, no fees) · equity $7,653.13 vs prior close $7,655.32 (-2.19) · 4 name(s) re-marked at the open (per-name table). WAY×102 yday $25.66 → 09:30 $25.94 +28.56; QCOM×14 yday $177.72 → 09:30 $180.61 +40.46; SM×67 yday $36.97 → 09:30 $35.91 -71.35; GME×1 yday $22.64 → 09:30 $22.78 +0.14 | — |
| 2026-09-21 09:30 ET | **SELL** | `WAY` | 102 | $25.94 | $2.33 | $-38.29 | $2,693.84 | ▼ -38.29 after sell → book $7,650.79; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `QCOM` | 14 | $180.61 | $2.06 | $-123.93 | $5,220.31 | ▼ -123.93 after sell → book $7,648.73; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SM` | 67 | $35.91 | $2.22 | $-278.11 | $7,623.73 | ▼ -278.11 after sell → book $7,646.51; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 11 | $230.25 | $2.02 | — | $5,088.95 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; ret5=+12.5; leftover $2541.24 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 13 | $190.30 | $2.03 | — | $2,613.03 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; ret5=+10.6; leftover $2541.24 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `ALVO` | 429 | $5.92 | $5.53 | — | $67.81 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; ret5=+11.0; leftover $2541.24 | join🔴 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $67.81 | ▼ close $7,381.80 vs 09:30 $7,653.13 (session -255.12) | 16:00 close · cash $67.81 · equity $7,381.80 vs 09:30 $7,653.13 (-271.33; session marks -255.12) · 4 name(s) marked open→close (per-name table). GME×1 09:30 $22.78 → close $22.76 -0.02; VICR×11 09:30 $230.25 → close $223.90 -69.85; SMTC×13 09:30 $190.30 → close $177.37 -168.09; ALVO×429 09:30 $5.92 → close $5.88 -17.16 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `HLIT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `SNDK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `HLIT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `SNDK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `APA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 14.43 < 1 share @ 119.43 |
| 2026-08-21 | `FUTU` | cash | leftover split 14.43 < 1 share @ 115.18 |
| 2026-08-21 | `GRAL` | cash | leftover split 14.43 < 1 share @ 78.88 |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `APA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `HIVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ABTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `HIVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EZPW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FNV` | cash | leftover split 47.74 < 1 share @ 267.02 |
| 2026-08-26 | `CM` | cash | leftover split 47.74 < 1 share @ 118.50 |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EZPW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ACMR` | cash | leftover split 11.94 < 1 share @ 81.65 |
| 2026-08-27 | `MU` | cash | leftover split 11.94 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 11.94 < 1 share @ 1746.53 |
| 2026-08-27 | `CM` | cash | leftover split 11.94 < 1 share @ 118.77 |
| 2026-08-27 | `GEN` | cash | leftover split 11.94 < 1 share @ 29.83 |
| 2026-08-27 | `LRCX` | cash | leftover split 11.94 < 1 share @ 318.88 |
| 2026-08-27 | `NVDA` | cash | leftover split 11.94 < 1 share @ 222.86 |
| 2026-08-27 | `ADSK` | cash | leftover split 11.94 < 1 share @ 261.47 |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CIEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `DDOG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ADSK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CIEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `MPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DDOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ADSK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `AVGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CXW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRM` | cash | leftover split 123.37 < 1 share @ 263.36 |
| 2026-09-04 | `BE` | cash | leftover split 123.37 < 1 share @ 236.82 |
| 2026-09-08 | `AVGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DELL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CXW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FRNM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MMED` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-09 | `MRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `WAY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `QCOM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SMTC` | cash | leftover split 24.21 < 1 share @ 170.85 |
| 2026-09-17 | `CLS` | cash | leftover split 24.21 < 1 share @ 337.75 |
| 2026-09-18 | `WAY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QCOM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TH` | cash | leftover split 12.57 < 1 share @ 20.91 |
| 2026-09-18 | `RARE` | cash | leftover split 12.57 < 1 share @ 14.79 |
| 2026-09-18 | `CLS` | cash | leftover split 12.57 < 1 share @ 332.06 |
| 2026-09-18 | `BHVN` | cash | leftover split 12.57 < 1 share @ 14.07 |
| 2026-09-21 | `GME` | min_hold | dropped but min-hold 2/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GME` | 1 | 2026-09-17 @ $22.12 | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $24.21 |
| `VICR` | 11 | 2026-09-21 @ $230.25 | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; ret5=+12.5; leftover $2541.24 |
| `SMTC` | 13 | 2026-09-21 @ $190.30 | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; ret5=+10.6; leftover $2541.24 |
| `ALVO` | 429 | 2026-09-21 @ $5.92 | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; ret5=+11.0; leftover $2541.24 |
