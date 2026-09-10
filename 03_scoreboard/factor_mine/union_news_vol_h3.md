# Factor mine action — `union_news_vol_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-1.65%** ($9,835) · signal-only (no cash/fees) was +176.90%. Starts YES **4/19**. Fills 69 · skips 88 · realized $-173.83.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,826.28.

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
| 2026-08-24 | `BHP` | 13 | $97.03 | $97.31 | +3.64 | $97.13 | -2.34 | +1.30 | +81.90 | +79.56 |
| 2026-08-24 | `MRNA` | 8 | $145.13 | $142.70 | -19.44 | $138.89 | -30.48 | -49.92 | -59.52 | -90.00 |
| 2026-08-24 | `HUMA` | 1768 | $0.64 | $0.68 | +58.34 | $0.66 | -22.98 | +35.36 | -56.58 | -79.56 |
| 2026-08-24 | `BTGO` | 189 | $6.84 | $6.85 | +1.89 | $6.80 | -9.45 | -7.56 | +46.30 | +36.85 |
| 2026-08-24 | `ASST` | 78 | $18.22 | $18.76 | +42.12 | $19.73 | +75.66 | +117.78 | +215.28 | +290.94 |
| 2026-08-24 | `ZLAB` | 47 | $26.01 | $25.43 | -27.26 | $25.64 | +9.87 | -17.39 | -53.58 | -43.71 |
| 2026-08-24 | `CRSP` | 21 | $59.50 | $58.75 | -15.75 | $57.08 | -35.17 | -50.92 | +0.42 | -34.75 |
| 2026-08-24 | `APA` | 27 | $43.39 | $42.93 | -12.42 | $42.96 | +0.81 | -11.61 | -49.41 | -48.60 |
| 2026-08-24 | `AUTL` | 8 | $2.41 | $2.40 | -0.08 | $2.34 | -0.48 | -0.56 | -0.56 | -1.04 |
| 2026-08-24 | `MARA` | 1 | $11.26 | $11.17 | -0.09 | $11.18 | +0.01 | -0.08 | -0.53 | -0.52 |
| 2026-08-24 | `BTDR` | 1 | $11.37 | $11.48 | +0.11 | $10.91 | -0.57 | -0.46 | +0.38 | -0.19 |
| 2026-08-24 | `HIVE` | 6 | $3.03 | $2.99 | -0.24 | $2.86 | -0.78 | -1.02 | -1.50 | -2.28 |
| 2026-08-25 | `BHP` | 13 | $97.13 | $95.86 | -16.51 | — | +0.00 | -16.51 | +63.05 | — |
| 2026-08-25 | `MRNA` | 8 | $138.89 | $143.50 | +36.88 | — | +0.00 | +36.88 | -53.12 | — |
| 2026-08-25 | `HUMA` | 1768 | $0.66 | $0.66 | +1.77 | — | +0.00 | +1.77 | -77.79 | — |
| 2026-08-25 | `BTGO` | 189 | $6.80 | $6.75 | -9.45 | — | +0.00 | -9.45 | +27.40 | — |
| 2026-08-25 | `ASST` | 78 | $19.73 | $19.04 | -53.82 | — | +0.00 | -53.82 | +237.12 | — |
| 2026-08-25 | `ZLAB` | 47 | $25.64 | $26.04 | +18.80 | — | +0.00 | +18.80 | -24.91 | — |
| 2026-08-25 | `CRSP` | 21 | $57.08 | $57.93 | +17.95 | — | +0.00 | +17.95 | -16.80 | — |
| 2026-08-25 | `APA` | 27 | $42.96 | $41.38 | -42.66 | — | +0.00 | -42.66 | -91.26 | — |
| 2026-08-25 | `AUTL` | 8 | $2.34 | $2.38 | +0.32 | $2.44 | +0.48 | +0.80 | -0.72 | -0.24 |
| 2026-08-25 | `MARA` | 1 | $11.18 | $11.07 | -0.11 | $11.83 | +0.76 | +0.65 | -0.63 | +0.13 |
| 2026-08-25 | `BTDR` | 1 | $10.91 | $10.70 | -0.21 | $11.29 | +0.59 | +0.38 | -0.40 | +0.19 |
| 2026-08-25 | `HIVE` | 6 | $2.86 | $2.87 | +0.06 | $3.02 | +0.90 | +0.96 | -2.22 | -1.32 |
| 2026-08-25 | `RUM` | 211 | — | $9.42 | +0.00 | $10.23 | +170.91 | +170.91 | +0.00 | +170.91 |
| 2026-08-25 | `EZPW` | 56 | — | $35.05 | +0.00 | $35.23 | +10.08 | +10.08 | +0.00 | +10.08 |
| 2026-08-25 | `REAX` | 82 | — | $24.11 | +0.00 | $28.43 | +354.24 | +354.24 | +0.00 | +354.24 |
| 2026-08-25 | `AU` | 16 | — | $118.52 | +0.00 | $123.39 | +77.92 | +77.92 | +0.00 | +77.92 |
| 2026-08-25 | `FCX` | 25 | — | $77.13 | +0.00 | $79.91 | +69.50 | +69.50 | +0.00 | +69.50 |
| 2026-08-26 | `AUTL` | 8 | $2.44 | $2.41 | -0.24 | — | +0.00 | -0.24 | -0.48 | — |
| 2026-08-26 | `MARA` | 1 | $11.83 | $11.56 | -0.27 | — | +0.00 | -0.27 | -0.14 | — |
| 2026-08-26 | `BTDR` | 1 | $11.29 | $11.05 | -0.24 | — | +0.00 | -0.24 | -0.04 | — |
| 2026-08-26 | `HIVE` | 6 | $3.02 | $2.95 | -0.42 | — | +0.00 | -0.42 | -1.74 | — |
| 2026-08-26 | `RUM` | 211 | $10.23 | $10.07 | -33.76 | $9.38 | -146.65 | -180.41 | +137.15 | -9.49 |
| 2026-08-26 | `EZPW` | 56 | $35.23 | $35.70 | +26.32 | $33.90 | -100.80 | -74.48 | +36.40 | -64.40 |
| 2026-08-26 | `REAX` | 82 | $28.43 | $26.61 | -149.24 | $26.59 | -1.64 | -150.88 | +205.00 | +203.36 |
| 2026-08-26 | `AU` | 16 | $123.39 | $119.80 | -57.44 | $118.11 | -27.04 | -84.48 | +20.48 | -6.56 |
| 2026-08-26 | `FCX` | 25 | $79.91 | $79.34 | -14.25 | $79.00 | -8.50 | -22.75 | +55.25 | +46.75 |
| 2026-08-27 | `RUM` | 211 | $9.38 | $9.51 | +28.48 | $9.43 | -16.88 | +11.60 | +18.99 | +2.11 |
| 2026-08-27 | `EZPW` | 56 | $33.90 | $33.50 | -22.40 | $34.41 | +50.96 | +28.56 | -86.80 | -35.84 |
| 2026-08-27 | `REAX` | 82 | $26.59 | $25.91 | -55.76 | $23.63 | -186.96 | -242.72 | +147.60 | -39.36 |
| 2026-08-27 | `AU` | 16 | $118.11 | $117.41 | -11.20 | $118.40 | +15.84 | +4.64 | -17.76 | -1.92 |
| 2026-08-27 | `FCX` | 25 | $79.00 | $78.83 | -4.25 | $78.42 | -10.25 | -14.50 | +42.50 | +32.25 |
| 2026-08-28 | `RUM` | 211 | $9.43 | $9.30 | -27.43 | — | +0.00 | -27.43 | -25.32 | — |
| 2026-08-28 | `EZPW` | 56 | $34.41 | $34.50 | +5.04 | — | +0.00 | +5.04 | -30.80 | — |
| 2026-08-28 | `REAX` | 82 | $23.63 | $23.40 | -18.86 | — | +0.00 | -18.86 | -58.22 | — |
| 2026-08-28 | `AU` | 16 | $118.40 | $119.19 | +12.64 | — | +0.00 | +12.64 | +10.72 | — |
| 2026-08-28 | `FCX` | 25 | $78.42 | $78.57 | +3.75 | — | +0.00 | +3.75 | +36.00 | — |
| 2026-08-28 | `SEDG` | 43 | — | $32.90 | +0.00 | $31.41 | -64.07 | -64.07 | +0.00 | -64.07 |
| 2026-08-28 | `CAPR` | 145 | — | $9.73 | +0.00 | $9.59 | -20.30 | -20.30 | +0.00 | -20.30 |
| 2026-08-28 | `SMTC` | 9 | — | $141.76 | +0.00 | $131.17 | -95.31 | -95.31 | +0.00 | -95.31 |
| 2026-08-28 | `ERAS` | 73 | — | $19.25 | +0.00 | $18.03 | -89.06 | -89.06 | +0.00 | -89.06 |
| 2026-08-28 | `BBWI` | 75 | — | $18.75 | +0.00 | $19.22 | +35.25 | +35.25 | +0.00 | +35.25 |
| 2026-08-28 | `ZYME` | 48 | — | $28.91 | +0.00 | $28.27 | -30.72 | -30.72 | +0.00 | -30.72 |
| 2026-08-28 | `TH` | 74 | — | $19.00 | +0.00 | $18.55 | -33.30 | -33.30 | +0.00 | -33.30 |
| 2026-08-31 | `SEDG` | 43 | $31.41 | $31.15 | -11.18 | $32.20 | +45.15 | +33.97 | -75.25 | -30.10 |
| 2026-08-31 | `CAPR` | 145 | $9.59 | $9.50 | -13.05 | $9.91 | +59.45 | +46.40 | -33.35 | +26.10 |
| 2026-08-31 | `SMTC` | 9 | $131.17 | $132.30 | +10.17 | $132.96 | +5.94 | +16.11 | -85.14 | -79.20 |
| 2026-08-31 | `ERAS` | 73 | $18.03 | $17.87 | -11.68 | $17.88 | +0.73 | -10.95 | -100.74 | -100.01 |
| 2026-08-31 | `BBWI` | 75 | $19.22 | $19.25 | +2.25 | $19.25 | +0.00 | +2.25 | +37.50 | +37.50 |
| 2026-08-31 | `ZYME` | 48 | $28.27 | $28.06 | -10.08 | $29.41 | +64.80 | +54.72 | -40.80 | +24.00 |
| 2026-08-31 | `TH` | 74 | $18.55 | $18.12 | -31.45 | $18.52 | +29.23 | -2.22 | -64.75 | -35.52 |
| 2026-09-01 | `SEDG` | 43 | $32.20 | $31.87 | -14.19 | $32.49 | +26.66 | +12.47 | -44.29 | -17.63 |
| 2026-09-01 | `CAPR` | 145 | $9.91 | $10.77 | +124.70 | $10.01 | -110.20 | +14.50 | +150.80 | +40.60 |
| 2026-09-01 | `SMTC` | 9 | $132.96 | $127.63 | -47.97 | $132.27 | +41.76 | -6.21 | -127.17 | -85.41 |
| 2026-09-01 | `ERAS` | 73 | $17.88 | $17.58 | -21.90 | $16.76 | -59.86 | -81.76 | -121.91 | -181.77 |
| 2026-09-01 | `BBWI` | 75 | $19.25 | $18.77 | -36.00 | $18.61 | -12.00 | -48.00 | +1.50 | -10.50 |
| 2026-09-01 | `ZYME` | 48 | $29.41 | $29.32 | -4.32 | $29.67 | +16.80 | +12.48 | +19.68 | +36.48 |
| 2026-09-01 | `TH` | 74 | $18.52 | $18.45 | -5.18 | $18.07 | -28.12 | -33.30 | -40.70 | -68.82 |
| 2026-09-02 | `SEDG` | 43 | $32.49 | $32.42 | -3.01 | — | +0.00 | -3.01 | -20.64 | — |
| 2026-09-02 | `CAPR` | 145 | $10.01 | $10.07 | +8.70 | — | +0.00 | +8.70 | +49.30 | — |
| 2026-09-02 | `SMTC` | 9 | $132.27 | $133.00 | +6.57 | — | +0.00 | +6.57 | -78.84 | — |
| 2026-09-02 | `ERAS` | 73 | $16.76 | $16.97 | +15.33 | — | +0.00 | +15.33 | -166.44 | — |
| 2026-09-02 | `BBWI` | 75 | $18.61 | $18.41 | -15.00 | — | +0.00 | -15.00 | -25.50 | — |
| 2026-09-02 | `ZYME` | 48 | $29.67 | $30.00 | +15.84 | — | +0.00 | +15.84 | +52.32 | — |
| 2026-09-02 | `TH` | 74 | $18.07 | $17.98 | -6.66 | — | +0.00 | -6.66 | -75.48 | — |
| 2026-09-03 | `MMED` | 80 | — | $23.88 | +0.00 | $23.84 | -3.20 | -3.20 | +0.00 | -3.20 |
| 2026-09-03 | `DE` | 2 | — | $703.25 | +0.00 | $694.41 | -17.68 | -17.68 | +0.00 | -17.68 |
| 2026-09-03 | `FRNM` | 121 | — | $15.87 | +0.00 | $16.90 | +124.63 | +124.63 | +0.00 | +124.63 |
| 2026-09-03 | `DELL` | 3 | — | $486.31 | +0.00 | $516.39 | +90.24 | +90.24 | +0.00 | +90.24 |
| 2026-09-03 | `CXW` | 59 | — | $32.31 | +0.00 | $33.66 | +79.65 | +79.65 | +0.00 | +79.65 |
| 2026-09-04 | `MMED` | 80 | $23.84 | $23.84 | +0.00 | $23.29 | -44.00 | -44.00 | -3.20 | -47.20 |
| 2026-09-04 | `DE` | 2 | $694.41 | $692.03 | -4.76 | $693.53 | +3.00 | -1.76 | -22.44 | -19.44 |
| 2026-09-04 | `FRNM` | 121 | $16.90 | $16.40 | -60.50 | $16.31 | -10.89 | -71.39 | +64.13 | +53.24 |
| 2026-09-04 | `DELL` | 3 | $516.39 | $513.78 | -7.83 | $524.14 | +31.08 | +23.25 | +82.41 | +113.49 |
| 2026-09-04 | `CXW` | 59 | $33.66 | $33.46 | -11.80 | $34.71 | +73.75 | +61.95 | +67.85 | +141.60 |
| 2026-09-04 | `BAK` | 512 | — | $1.94 | +0.00 | $1.89 | -25.60 | -25.60 | +0.00 | -25.60 |
| 2026-09-08 | `MMED` | 80 | $23.29 | $23.16 | -10.40 | $23.32 | +12.80 | +2.40 | -57.60 | -44.80 |
| 2026-09-08 | `DE` | 2 | $693.53 | $687.21 | -12.64 | $680.73 | -12.96 | -25.60 | -32.08 | -45.04 |
| 2026-09-08 | `FRNM` | 121 | $16.31 | $16.74 | +52.03 | $15.99 | -90.75 | -38.72 | +105.27 | +14.52 |
| 2026-09-08 | `DELL` | 3 | $524.14 | $521.15 | -8.97 | $533.88 | +38.19 | +29.22 | +104.52 | +142.71 |
| 2026-09-08 | `CXW` | 59 | $34.71 | $34.49 | -12.98 | $35.05 | +33.04 | +20.06 | +128.62 | +161.66 |
| 2026-09-08 | `BAK` | 512 | $1.89 | $1.94 | +25.60 | $1.92 | -10.24 | +15.36 | +0.00 | -10.24 |
| 2026-09-09 | `MMED` | 80 | $23.32 | $23.22 | -8.00 | — | +0.00 | -8.00 | -52.80 | — |
| 2026-09-09 | `DE` | 2 | $680.73 | $681.32 | +1.18 | — | +0.00 | +1.18 | -43.86 | — |
| 2026-09-09 | `FRNM` | 121 | $15.99 | $15.96 | -3.63 | — | +0.00 | -3.63 | +10.89 | — |
| 2026-09-09 | `DELL` | 3 | $533.88 | $538.47 | +13.77 | — | +0.00 | +13.77 | +156.48 | — |
| 2026-09-09 | `CXW` | 59 | $35.05 | $35.09 | +2.36 | — | +0.00 | +2.36 | +164.02 | — |
| 2026-09-09 | `BAK` | 512 | $1.92 | $2.02 | +48.64 | $1.97 | -23.04 | +25.60 | +38.40 | +15.36 |

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
| 2026-08-24 | -5.17 | $88.19 | BHP×13, MRNA×8, HUMA×1768, BTGO×189, ASST×78, ZLAB×47, CRSP×21, APA×27, AUTL×8, MARA×1, BTDR×1, HIVE×6 | $10,094.01 | +30.82 | -15.90 | — | — | $88.19 | $10,078.10 | BHP×13, MRNA×8, HUMA×1768, BTGO×189, ASST×78, ZLAB×47, CRSP×21, APA×27, AUTL×8, MARA×1, BTDR×1, HIVE×6 |
| 2026-08-25 | +1.80 | $88.19 | BHP×13, MRNA×8, HUMA×1768, BTGO×189, ASST×78, ZLAB×47, CRSP×21, APA×27, AUTL×8, MARA×1, BTDR×1, HIVE×6 | $10,031.12 | -46.98 | +685.38 | RUM, EZPW, REAX, AU, FCX | BHP, MRNA, HUMA, BTGO, ASST, ZLAB, CRSP, APA | $177.29 | $10,672.71 | AUTL×8, MARA×1, BTDR×1, HIVE×6, RUM×211, EZPW×56, REAX×82, AU×16, FCX×25 |
| 2026-08-26 | +2.02 | $177.29 | AUTL×8, MARA×1, BTDR×1, HIVE×6, RUM×211, EZPW×56, REAX×82, AU×16, FCX×25 | $10,443.17 | -229.54 | -284.63 | — | AUTL, MARA, BTDR, HIVE | $236.16 | $10,157.82 | RUM×211, EZPW×56, REAX×82, AU×16, FCX×25 |
| 2026-08-27 | — | $236.16 | RUM×211, EZPW×56, REAX×82, AU×16, FCX×25 | $10,092.70 | -65.12 | -147.29 | — | — | $236.16 | $9,945.41 | RUM×211, EZPW×56, REAX×82, AU×16, FCX×25 |
| 2026-08-28 | +0.75 | $236.16 | RUM×211, EZPW×56, REAX×82, AU×16, FCX×25 | $9,920.55 | -24.86 | -297.51 | SEDG, CAPR, SMTC, ERAS, BBWI, ZYME, TH | RUM, EZPW, REAX, AU, FCX | $187.27 | $9,596.33 | SEDG×43, CAPR×145, SMTC×9, ERAS×73, BBWI×75, ZYME×48, TH×74 |
| 2026-08-31 | -5.85 | $187.27 | SEDG×43, CAPR×145, SMTC×9, ERAS×73, BBWI×75, ZYME×48, TH×74 | $9,531.31 | -65.02 | +205.30 | — | — | $187.27 | $9,736.61 | SEDG×43, CAPR×145, SMTC×9, ERAS×73, BBWI×75, ZYME×48, TH×74 |
| 2026-09-01 | -6.30 | $187.27 | SEDG×43, CAPR×145, SMTC×9, ERAS×73, BBWI×75, ZYME×48, TH×74 | $9,731.75 | -4.86 | -124.96 | — | — | $187.27 | $9,606.79 | SEDG×43, CAPR×145, SMTC×9, ERAS×73, BBWI×75, ZYME×48, TH×74 |
| 2026-09-02 | -3.83 | $187.27 | SEDG×43, CAPR×145, SMTC×9, ERAS×73, BBWI×75, ZYME×48, TH×74 | $9,628.56 | +21.77 | +0.00 | — | SEDG, CAPR, SMTC, ERAS, BBWI, ZYME, TH | $9,613.06 | $9,613.06 | — |
| 2026-09-03 | -0.90 | $9,613.06 | — | $9,613.06 | +0.00 | +273.64 | MMED, DE, FRNM, DELL, CXW | — | $999.93 | $9,875.96 | MMED×80, DE×2, FRNM×121, DELL×3, CXW×59 |
| 2026-09-04 | +2.25 | $999.93 | MMED×80, DE×2, FRNM×121, DELL×3, CXW×59 | $9,791.07 | -84.89 | +27.34 | BAK | — | $0.04 | $9,811.80 | MMED×80, DE×2, FRNM×121, DELL×3, CXW×59, BAK×512 |
| 2026-09-08 | -11.47 | $0.04 | MMED×80, DE×2, FRNM×121, DELL×3, CXW×59, BAK×512 | $9,844.44 | +32.64 | -29.92 | — | — | $0.04 | $9,814.52 | MMED×80, DE×2, FRNM×121, DELL×3, CXW×59, BAK×512 |
| 2026-09-09 | -13.95 | $0.04 | MMED×80, DE×2, FRNM×121, DELL×3, CXW×59, BAK×512 | $9,868.84 | +54.32 | -23.04 | — | MMED, DE, FRNM, DELL, CXW | $8,826.28 | $9,834.92 | BAK×512 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 464 | $4.31 | $5.99 | — | $7,994.17 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 102 | $19.57 | $2.30 | — | $5,995.74 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SNDK` | 1 | $1646.93 | $1.99 | — | $4,346.82 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.4; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 147 | $13.55 | $2.43 | — | $2,352.53 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 151 | $13.18 | $2.44 | — | $359.91 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
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
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $88.19 | ▲ 09:30 equity $10,094.01 vs yday $10,063.19 (+30.82) | 09:30 open · cash $88.19 (unchanged overnight, no fees) · equity $10,094.01 vs prior close $10,063.19 (+30.82) · 12 name(s) re-marked at the open (per-name table). BHP×13 yday $97.03 → 09:30 $97.31 +3.64; MRNA×8 yday $145.13 → 09:30 $142.70 -19.44; HUMA×1768 yday $0.64 → 09:30 $0.68 +58.34; BTGO×189 yday $6.84 → 09:30 $6.85 +1.89; ASST×78 yday $18.22 → 09:30 $18.76 +42.12; ZLAB×47 yday $26.01 → 09:30 $25.43 -27.26; CRSP×21 yday $59.50 → 09:30 $58.75 -15.75; APA×27 yday $43.39 → 09:30 $42.93 -12.42; AUTL×8 yday $2.41 → 09:30 $2.40 -0.08; MARA×1 yday $11.26 → 09:30 $11.17 -0.09; BTDR×1 yday $11.37 → 09:30 $11.48 +0.11; HIVE×6 yday $3.03 → 09:30 $2.99 -0.24 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $88.19 | ▼ close $10,078.10 vs 09:30 $10,094.01 (session -15.90) | 16:00 close · cash $88.19 · equity $10,078.10 vs 09:30 $10,094.01 (-15.91; session marks -15.90) · 12 name(s) marked open→close (per-name table). BHP×13 09:30 $97.31 → close $97.13 -2.34; MRNA×8 09:30 $142.70 → close $138.89 -30.48; HUMA×1768 09:30 $0.68 → close $0.66 -22.98; BTGO×189 09:30 $6.85 → close $6.80 -9.45; ASST×78 09:30 $18.76 → close $19.73 +75.66; ZLAB×47 09:30 $25.43 → close $25.64 +9.87; CRSP×21 09:30 $58.75 → close $57.08 -35.17; APA×27 09:30 $42.93 → close $42.96 +0.81; AUTL×8 09:30 $2.40 → close $2.34 -0.48; MARA×1 09:30 $11.17 → close $11.18 +0.01; BTDR×1 09:30 $11.48 → close $10.91 -0.57; HIVE×6 09:30 $2.99 → close $2.86 -0.78 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $88.19 | ▼ 09:30 equity $10,031.12 vs yday $10,078.10 (-46.98) | 09:30 open · cash $88.19 (unchanged overnight, no fees) · equity $10,031.12 vs prior close $10,078.10 (-46.98) · 12 name(s) re-marked at the open (per-name table). BHP×13 yday $97.13 → 09:30 $95.86 -16.51; MRNA×8 yday $138.89 → 09:30 $143.50 +36.88; HUMA×1768 yday $0.66 → 09:30 $0.66 +1.77; BTGO×189 yday $6.80 → 09:30 $6.75 -9.45; ASST×78 yday $19.73 → 09:30 $19.04 -53.82; ZLAB×47 yday $25.64 → 09:30 $26.04 +18.80; CRSP×21 yday $57.08 → 09:30 $57.93 +17.95; APA×27 yday $42.96 → 09:30 $41.38 -42.66; AUTL×8 yday $2.34 → 09:30 $2.38 +0.32; MARA×1 yday $11.18 → 09:30 $11.07 -0.11; BTDR×1 yday $10.91 → 09:30 $10.70 -0.21; HIVE×6 yday $2.86 → 09:30 $2.87 +0.06 | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.86 | $2.05 | $+58.97 | $1,332.32 | ▲ +58.97 after sell → book $10,029.08; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MRNA` | 8 | $143.50 | $2.03 | $-57.17 | $2,478.29 | ▼ -57.17 after sell → book $10,027.04; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HUMA` | 1768 | $0.66 | $17.33 | $-112.93 | $3,633.14 | ▼ -112.93 after sell → book $10,009.71; vs 09:30 mark -17.33 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BTGO` | 189 | $6.75 | $2.60 | $+22.25 | $4,906.29 | ▲ +22.25 after sell → book $10,007.11; vs 09:30 mark -2.60 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ASST` | 78 | $19.04 | $2.25 | $+232.65 | $6,389.16 | ▲ +232.65 after sell → book $10,004.86; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `ZLAB` | 47 | $26.04 | $2.15 | $-29.19 | $7,610.89 | ▼ -29.19 after sell → book $10,002.71; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 21 | $57.93 | $2.07 | $-20.93 | $8,825.35 | ▼ -20.93 after sell → book $10,000.64; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `APA` | 27 | $41.38 | $2.09 | $-95.42 | $9,940.52 | ▼ -95.42 after sell → book $9,998.55; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 211 | $9.42 | $2.72 | — | $7,950.18 | — | combo gate; gate news=good,vol=good; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1988.10 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 56 | $35.05 | $2.16 | — | $5,985.22 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1988.10 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 82 | $24.11 | $2.24 | — | $4,005.96 | — | combo gate; gate news=good,vol=good; list yday_mover; ret5=+891.7; leftover $1988.10 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 16 | $118.52 | $2.04 | — | $2,107.61 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $1988.10 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 25 | $77.13 | $2.06 | — | $177.29 | — | combo gate; gate news=good,vol=good; list mover_buy; ⚪; ret5=+13.8; leftover $1988.10 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $177.29 | ▲ close $10,672.71 vs 09:30 $10,031.12 (session +685.38) | 16:00 close · cash $177.29 · equity $10,672.71 vs 09:30 $10,031.12 (+641.59; session marks +685.38) · 9 name(s) marked open→close (per-name table). AUTL×8 09:30 $2.38 → close $2.44 +0.48; MARA×1 09:30 $11.07 → close $11.83 +0.76; BTDR×1 09:30 $10.70 → close $11.29 +0.59; HIVE×6 09:30 $2.87 → close $3.02 +0.90; RUM×211 09:30 $9.42 → close $10.23 +170.91; EZPW×56 09:30 $35.05 → close $35.23 +10.08; REAX×82 09:30 $24.11 → close $28.43 +354.24; AU×16 09:30 $118.52 → close $123.39 +77.92; FCX×25 09:30 $77.13 → close $79.91 +69.50 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $177.29 | ▼ 09:30 equity $10,443.17 vs yday $10,672.71 (-229.54) | 09:30 open · cash $177.29 (unchanged overnight, no fees) · equity $10,443.17 vs prior close $10,672.71 (-229.54) · 9 name(s) re-marked at the open (per-name table). AUTL×8 yday $2.44 → 09:30 $2.41 -0.24; MARA×1 yday $11.83 → 09:30 $11.56 -0.27; BTDR×1 yday $11.29 → 09:30 $11.05 -0.24; HIVE×6 yday $3.02 → 09:30 $2.95 -0.42; RUM×211 yday $10.23 → 09:30 $10.07 -33.76; EZPW×56 yday $35.23 → 09:30 $35.70 +26.32; REAX×82 yday $28.43 → 09:30 $26.61 -149.24; AU×16 yday $123.39 → 09:30 $119.80 -57.44; FCX×25 yday $79.91 → 09:30 $79.34 -14.25 | — |
| 2026-08-26 09:30 ET | **SELL** | `AUTL` | 8 | $2.41 | $0.24 | $-0.94 | $196.33 | ▼ -0.94 after sell → book $10,442.93; vs 09:30 mark -0.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `MARA` | 1 | $11.56 | $0.14 | $-0.40 | $207.76 | ▼ -0.40 after sell → book $10,442.80; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BTDR` | 1 | $11.05 | $0.13 | $-0.29 | $218.67 | ▼ -0.29 after sell → book $10,442.66; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `HIVE` | 6 | $2.95 | $0.21 | $-2.17 | $236.16 | ▼ -2.17 after sell → book $10,442.45; vs 09:30 mark -0.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $236.16 | ▼ close $10,157.82 vs 09:30 $10,443.17 (session -284.63) | 16:00 close · cash $236.16 · equity $10,157.82 vs 09:30 $10,443.17 (-285.35; session marks -284.63) · 5 name(s) marked open→close (per-name table). RUM×211 09:30 $10.07 → close $9.38 -146.65; EZPW×56 09:30 $35.70 → close $33.90 -100.80; REAX×82 09:30 $26.61 → close $26.59 -1.64; AU×16 09:30 $119.80 → close $118.11 -27.04; FCX×25 09:30 $79.34 → close $79.00 -8.50 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $236.16 | ▼ 09:30 equity $10,092.70 vs yday $10,157.82 (-65.12) | 09:30 open · cash $236.16 (unchanged overnight, no fees) · equity $10,092.70 vs prior close $10,157.82 (-65.12) · 5 name(s) re-marked at the open (per-name table). RUM×211 yday $9.38 → 09:30 $9.51 +28.48; EZPW×56 yday $33.90 → 09:30 $33.50 -22.40; REAX×82 yday $26.59 → 09:30 $25.91 -55.76; AU×16 yday $118.11 → 09:30 $117.41 -11.20; FCX×25 yday $79.00 → 09:30 $78.83 -4.25 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $236.16 | ▼ close $9,945.41 vs 09:30 $10,092.70 (session -147.29) | 16:00 close · cash $236.16 · equity $9,945.41 vs 09:30 $10,092.70 (-147.29; session marks -147.29) · 5 name(s) marked open→close (per-name table). RUM×211 09:30 $9.51 → close $9.43 -16.88; EZPW×56 09:30 $33.50 → close $34.41 +50.96; REAX×82 09:30 $25.91 → close $23.63 -186.96; AU×16 09:30 $117.41 → close $118.40 +15.84; FCX×25 09:30 $78.83 → close $78.42 -10.25 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $236.16 | ▼ 09:30 equity $9,920.55 vs yday $9,945.41 (-24.86) | 09:30 open · cash $236.16 (unchanged overnight, no fees) · equity $9,920.55 vs prior close $9,945.41 (-24.86) · 5 name(s) re-marked at the open (per-name table). RUM×211 yday $9.43 → 09:30 $9.30 -27.43; EZPW×56 yday $34.41 → 09:30 $34.50 +5.04; REAX×82 yday $23.63 → 09:30 $23.40 -18.86; AU×16 yday $118.40 → 09:30 $119.19 +12.64; FCX×25 yday $78.42 → 09:30 $78.57 +3.75 | — |
| 2026-08-28 09:30 ET | **SELL** | `RUM` | 211 | $9.30 | $2.77 | $-30.81 | $2,195.68 | ▼ -30.81 after sell → book $9,917.77; vs 09:30 mark -2.78 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EZPW` | 56 | $34.50 | $2.18 | $-35.14 | $4,125.50 | ▼ -35.14 after sell → book $9,915.59; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `REAX` | 82 | $23.40 | $2.27 | $-62.72 | $6,042.04 | ▼ -62.72 after sell → book $9,913.33; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 16 | $119.19 | $2.06 | $+6.62 | $7,947.01 | ▲ +6.62 after sell → book $9,911.26; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 25 | $78.57 | $2.09 | $+31.84 | $9,909.17 | ▲ +31.84 after sell → book $9,909.17; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 43 | $32.90 | $2.12 | — | $8,492.35 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1415.60 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 145 | $9.73 | $2.42 | — | $7,079.08 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; ret5=+47.1; leftover $1415.60 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $5,801.22 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1415.60 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 73 | $19.25 | $2.21 | — | $4,393.76 | — | combo gate; gate news=good,vol=good; list yday_gainer; ret5=+14.1; leftover $1415.60 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 75 | $18.75 | $2.21 | — | $2,985.30 | — | combo gate; gate news=good,vol=good; list yday_gainer; ret5=-5.0; leftover $1415.60 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 48 | $28.91 | $2.13 | — | $1,595.48 | — | combo gate; gate news=good,vol=good; list yday_gainer; ret5=+9.2; leftover $1415.60 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 74 | $19.00 | $2.21 | — | $187.27 | — | combo gate; gate news=good,vol=good; list ohlc_hot; ret5=+7.5; leftover $1415.60 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $187.27 | ▼ close $9,596.33 vs 09:30 $9,920.55 (session -297.51) | 16:00 close · cash $187.27 · equity $9,596.33 vs 09:30 $9,920.55 (-324.22; session marks -297.51) · 7 name(s) marked open→close (per-name table). SEDG×43 09:30 $32.90 → close $31.41 -64.07; CAPR×145 09:30 $9.73 → close $9.59 -20.30; SMTC×9 09:30 $141.76 → close $131.17 -95.31; ERAS×73 09:30 $19.25 → close $18.03 -89.06; BBWI×75 09:30 $18.75 → close $19.22 +35.25; ZYME×48 09:30 $28.91 → close $28.27 -30.72; TH×74 09:30 $19.00 → close $18.55 -33.30 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $187.27 | ▼ 09:30 equity $9,531.31 vs yday $9,596.33 (-65.02) | 09:30 open · cash $187.27 (unchanged overnight, no fees) · equity $9,531.31 vs prior close $9,596.33 (-65.02) · 7 name(s) re-marked at the open (per-name table). SEDG×43 yday $31.41 → 09:30 $31.15 -11.18; CAPR×145 yday $9.59 → 09:30 $9.50 -13.05; SMTC×9 yday $131.17 → 09:30 $132.30 +10.17; ERAS×73 yday $18.03 → 09:30 $17.87 -11.68; BBWI×75 yday $19.22 → 09:30 $19.25 +2.25; ZYME×48 yday $28.27 → 09:30 $28.06 -10.08; TH×74 yday $18.55 → 09:30 $18.12 -31.45 | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $187.27 | ▲ close $9,736.61 vs 09:30 $9,531.31 (session +205.30) | 16:00 close · cash $187.27 · equity $9,736.61 vs 09:30 $9,531.31 (+205.30; session marks +205.30) · 7 name(s) marked open→close (per-name table). SEDG×43 09:30 $31.15 → close $32.20 +45.15; CAPR×145 09:30 $9.50 → close $9.91 +59.45; SMTC×9 09:30 $132.30 → close $132.96 +5.94; ERAS×73 09:30 $17.87 → close $17.88 +0.73; BBWI×75 09:30 $19.25 → close $19.25 +0.00; ZYME×48 09:30 $28.06 → close $29.41 +64.80; TH×74 09:30 $18.12 → close $18.52 +29.23 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $187.27 | ▼ 09:30 equity $9,731.75 vs yday $9,736.61 (-4.86) | 09:30 open · cash $187.27 (unchanged overnight, no fees) · equity $9,731.75 vs prior close $9,736.61 (-4.86) · 7 name(s) re-marked at the open (per-name table). SEDG×43 yday $32.20 → 09:30 $31.87 -14.19; CAPR×145 yday $9.91 → 09:30 $10.77 +124.70; SMTC×9 yday $132.96 → 09:30 $127.63 -47.97; ERAS×73 yday $17.88 → 09:30 $17.58 -21.90; BBWI×75 yday $19.25 → 09:30 $18.77 -36.00; ZYME×48 yday $29.41 → 09:30 $29.32 -4.32; TH×74 yday $18.52 → 09:30 $18.45 -5.18 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $187.27 | ▼ close $9,606.79 vs 09:30 $9,731.75 (session -124.96) | 16:00 close · cash $187.27 · equity $9,606.79 vs 09:30 $9,731.75 (-124.96; session marks -124.96) · 7 name(s) marked open→close (per-name table). SEDG×43 09:30 $31.87 → close $32.49 +26.66; CAPR×145 09:30 $10.77 → close $10.01 -110.20; SMTC×9 09:30 $127.63 → close $132.27 +41.76; ERAS×73 09:30 $17.58 → close $16.76 -59.86; BBWI×75 09:30 $18.77 → close $18.61 -12.00; ZYME×48 09:30 $29.32 → close $29.67 +16.80; TH×74 09:30 $18.45 → close $18.07 -28.12 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $187.27 | ▲ 09:30 equity $9,628.56 vs yday $9,606.79 (+21.77) | 09:30 open · cash $187.27 (unchanged overnight, no fees) · equity $9,628.56 vs prior close $9,606.79 (+21.77) · 7 name(s) re-marked at the open (per-name table). SEDG×43 yday $32.49 → 09:30 $32.42 -3.01; CAPR×145 yday $10.01 → 09:30 $10.07 +8.70; SMTC×9 yday $132.27 → 09:30 $133.00 +6.57; ERAS×73 yday $16.76 → 09:30 $16.97 +15.33; BBWI×75 yday $18.61 → 09:30 $18.41 -15.00; ZYME×48 yday $29.67 → 09:30 $30.00 +15.84; TH×74 yday $18.07 → 09:30 $17.98 -6.66 | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 43 | $32.42 | $2.14 | $-24.90 | $1,579.19 | ▼ -24.90 after sell → book $9,626.42; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CAPR` | 145 | $10.07 | $2.46 | $+44.41 | $3,036.88 | ▲ +44.41 after sell → book $9,623.96; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 9 | $133.00 | $2.04 | $-82.89 | $4,231.84 | ▼ -82.89 after sell → book $9,621.92; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ERAS` | 73 | $16.97 | $2.23 | $-170.88 | $5,468.42 | ▼ -170.88 after sell → book $9,619.69; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBWI` | 75 | $18.41 | $2.24 | $-29.95 | $6,846.93 | ▼ -29.95 after sell → book $9,617.45; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ZYME` | 48 | $30.00 | $2.16 | $+48.03 | $8,284.78 | ▲ +48.03 after sell → book $9,615.30; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TH` | 74 | $17.98 | $2.23 | $-79.93 | $9,613.06 | ▼ -79.93 after sell → book $9,613.06; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,613.06 | ▲ close $9,613.06 vs 09:30 $9,628.56 (session +0.00) | 16:00 close · cash $9,613.06 · no lots left · equity $9,613.06. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,613.06 | ▲ 09:30 equity $9,613.06 vs yday $9,613.06 (+0.00) | 09:30 open · cash $9,613.06 · no holdings · equity $9,613.06 vs prior close $9,613.06 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 80 | $23.88 | $2.23 | — | $7,700.43 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1922.61 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 2 | $703.25 | $2.00 | — | $6,291.94 | — | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1922.61 | join🟡 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 121 | $15.87 | $2.35 | — | $4,369.31 | — | combo gate; gate news=good,vol=good; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1922.61 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 3 | $486.31 | $2.00 | — | $2,908.38 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ret5=+6.1; leftover $1922.61 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 59 | $32.31 | $2.17 | — | $999.93 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1922.61 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $999.93 | ▲ close $9,875.96 vs 09:30 $9,613.06 (session +273.64) | 16:00 close · cash $999.93 · equity $9,875.96 vs 09:30 $9,613.06 (+262.90; session marks +273.64) · 5 name(s) marked open→close (per-name table). MMED×80 09:30 $23.88 → close $23.84 -3.20; DE×2 09:30 $703.25 → close $694.41 -17.68; FRNM×121 09:30 $15.87 → close $16.90 +124.63; DELL×3 09:30 $486.31 → close $516.39 +90.24; CXW×59 09:30 $32.31 → close $33.66 +79.65 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $999.93 | ▼ 09:30 equity $9,791.07 vs yday $9,875.96 (-84.89) | 09:30 open · cash $999.93 (unchanged overnight, no fees) · equity $9,791.07 vs prior close $9,875.96 (-84.89) · 5 name(s) re-marked at the open (per-name table). MMED×80 yday $23.84 → 09:30 $23.84 +0.00; DE×2 yday $694.41 → 09:30 $692.03 -4.76; FRNM×121 yday $16.90 → 09:30 $16.40 -60.50; DELL×3 yday $516.39 → 09:30 $513.78 -7.83; CXW×59 yday $33.66 → 09:30 $33.46 -11.80 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 512 | $1.94 | $6.60 | — | $0.04 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; ret5=+18.3; leftover $999.93 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.04 | ▲ close $9,811.80 vs 09:30 $9,791.07 (session +27.34) | 16:00 close · cash $0.04 · equity $9,811.80 vs 09:30 $9,791.07 (+20.73; session marks +27.34) · 6 name(s) marked open→close (per-name table). MMED×80 09:30 $23.84 → close $23.29 -44.00; DE×2 09:30 $692.03 → close $693.53 +3.00; FRNM×121 09:30 $16.40 → close $16.31 -10.89; DELL×3 09:30 $513.78 → close $524.14 +31.08; CXW×59 09:30 $33.46 → close $34.71 +73.75; BAK×512 09:30 $1.94 → close $1.89 -25.60 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.04 | ▲ 09:30 equity $9,844.44 vs yday $9,811.80 (+32.64) | 09:30 open · cash $0.04 (unchanged overnight, no fees) · equity $9,844.44 vs prior close $9,811.80 (+32.64) · 6 name(s) re-marked at the open (per-name table). MMED×80 yday $23.29 → 09:30 $23.16 -10.40; DE×2 yday $693.53 → 09:30 $687.21 -12.64; FRNM×121 yday $16.31 → 09:30 $16.74 +52.03; DELL×3 yday $524.14 → 09:30 $521.15 -8.97; CXW×59 yday $34.71 → 09:30 $34.49 -12.98; BAK×512 yday $1.89 → 09:30 $1.94 +25.60 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.04 | ▼ close $9,814.52 vs 09:30 $9,844.44 (session -29.92) | 16:00 close · cash $0.04 · equity $9,814.52 vs 09:30 $9,844.44 (-29.92; session marks -29.92) · 6 name(s) marked open→close (per-name table). MMED×80 09:30 $23.16 → close $23.32 +12.80; DE×2 09:30 $687.21 → close $680.73 -12.96; FRNM×121 09:30 $16.74 → close $15.99 -90.75; DELL×3 09:30 $521.15 → close $533.88 +38.19; CXW×59 09:30 $34.49 → close $35.05 +33.04; BAK×512 09:30 $1.94 → close $1.92 -10.24 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.04 | ▲ 09:30 equity $9,868.84 vs yday $9,814.52 (+54.32) | 09:30 open · cash $0.04 (unchanged overnight, no fees) · equity $9,868.84 vs prior close $9,814.52 (+54.32) · 6 name(s) re-marked at the open (per-name table). MMED×80 yday $23.32 → 09:30 $23.22 -8.00; DE×2 yday $680.73 → 09:30 $681.32 +1.18; FRNM×121 yday $15.99 → 09:30 $15.96 -3.63; DELL×3 yday $533.88 → 09:30 $538.47 +13.77; CXW×59 yday $35.05 → 09:30 $35.09 +2.36; BAK×512 yday $1.92 → 09:30 $2.02 +48.64 | — |
| 2026-09-09 09:30 ET | **SELL** | `MMED` | 80 | $23.22 | $2.26 | $-57.29 | $1,855.38 | ▼ -57.29 after sell → book $9,866.58; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DE` | 2 | $681.32 | $2.02 | $-47.87 | $3,216.01 | ▼ -47.87 after sell → book $9,864.57; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FRNM` | 121 | $15.96 | $2.39 | $+6.15 | $5,144.78 | ▲ +6.15 after sell → book $9,862.18; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DELL` | 3 | $538.47 | $2.02 | $+152.46 | $6,758.17 | ▲ +152.46 after sell → book $9,860.16; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CXW` | 59 | $35.09 | $2.19 | $+159.66 | $8,826.28 | ▲ +159.66 after sell → book $9,857.96; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,826.28 | ▼ close $9,834.92 vs 09:30 $9,868.84 (session -23.04) | 16:00 close · cash $8,826.28 · equity $9,834.92 vs 09:30 $9,868.84 (-33.92; session marks -23.04) · 1 name(s) marked open→close (per-name table). BAK×512 09:30 $2.02 → close $1.97 -23.04 | — |

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
| 2026-08-26 | `RUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EZPW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `REAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `RUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EZPW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `REAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ERAS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BBWI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ZYME` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ERAS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BBWI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ZYME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CXW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `MMED` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FRNM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DELL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CXW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `BAK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MSTR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `BAK` | min_hold | dropped but min-hold 2/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `BAK` | 512 | 2026-09-04 @ $1.94 | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; ret5=+18.3; leftover $999.93 |
