# Factor mine action — `union_news_vol_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **+3.68%** ($10,368) · signal-only (no cash/fees) was +7.11%. Starts YES **15/19**. Fills 88 · skips 25 · realized $+368.01.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news=good,vol=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,368.03.

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
| 2026-08-17 | `ANGX` | 464 | $4.37 | $4.60 | +106.72 | — | +0.00 | +106.72 | +134.56 | — |
| 2026-08-17 | `ARX` | 102 | $19.58 | $19.57 | -1.02 | — | +0.00 | -1.02 | +0.00 | — |
| 2026-08-17 | `SNDK` | 1 | $1641.11 | $1700.74 | +59.63 | — | +0.00 | +59.63 | +53.81 | — |
| 2026-08-17 | `MH` | 147 | $13.10 | $13.16 | +8.82 | — | +0.00 | +8.82 | -57.33 | — |
| 2026-08-17 | `HLIT` | 151 | $13.92 | $13.84 | -12.08 | — | +0.00 | -12.08 | +99.66 | — |
| 2026-08-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `BHP` | 14 | — | $91.01 | +0.00 | $93.63 | +36.68 | +36.68 | +0.00 | +36.68 |
| 2026-08-20 | `MRNA` | 8 | — | $150.14 | +0.00 | $133.32 | -134.56 | -134.56 | +0.00 | -134.56 |
| 2026-08-20 | `HUMA` | 1803 | — | $0.71 | +0.00 | $0.68 | -46.88 | -46.88 | +0.00 | -46.88 |
| 2026-08-20 | `BTGO` | 193 | — | $6.61 | +0.00 | $6.60 | -0.97 | -0.97 | +0.00 | -0.97 |
| 2026-08-20 | `ASST` | 79 | — | $16.00 | +0.00 | $16.13 | +10.27 | +10.27 | +0.00 | +10.27 |
| 2026-08-20 | `ZLAB` | 47 | — | $26.57 | +0.00 | $26.02 | -25.85 | -25.85 | +0.00 | -25.85 |
| 2026-08-20 | `CRSP` | 21 | — | $58.73 | +0.00 | $58.12 | -12.81 | -12.81 | +0.00 | -12.81 |
| 2026-08-20 | `APA` | 28 | — | $44.76 | +0.00 | $44.39 | -10.36 | -10.36 | +0.00 | -10.36 |
| 2026-08-21 | `BHP` | 14 | $93.63 | $95.72 | +29.26 | — | +0.00 | +29.26 | +65.94 | — |
| 2026-08-21 | `MRNA` | 8 | $133.32 | $133.11 | -1.68 | — | +0.00 | -1.68 | -136.24 | — |
| 2026-08-21 | `HUMA` | 1803 | $0.68 | $0.67 | -12.62 | — | +0.00 | -12.62 | -59.50 | — |
| 2026-08-21 | `BTGO` | 193 | $6.60 | $6.95 | +67.55 | — | +0.00 | +67.55 | +66.58 | — |
| 2026-08-21 | `ASST` | 79 | $16.13 | $17.66 | +120.87 | — | +0.00 | +120.87 | +131.14 | — |
| 2026-08-21 | `ZLAB` | 47 | $26.02 | $26.25 | +10.81 | — | +0.00 | +10.81 | -15.04 | — |
| 2026-08-21 | `CRSP` | 21 | $58.12 | $59.72 | +33.60 | $59.50 | -4.62 | +28.98 | +20.79 | +16.17 |
| 2026-08-21 | `APA` | 28 | $44.39 | $44.52 | +3.64 | — | +0.00 | +3.64 | -6.72 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `AUTL` | 517 | — | $2.47 | +0.00 | $2.41 | -31.02 | -31.02 | +0.00 | -31.02 |
| 2026-08-21 | `FUTU` | 11 | — | $115.18 | +0.00 | $123.64 | +93.06 | +93.06 | +0.00 | +93.06 |
| 2026-08-21 | `DE` | 2 | — | $623.26 | +0.00 | $647.47 | +48.42 | +48.42 | +0.00 | +48.42 |
| 2026-08-21 | `MARA` | 109 | — | $11.70 | +0.00 | $11.26 | -47.96 | -47.96 | +0.00 | -47.96 |
| 2026-08-21 | `BTDR` | 115 | — | $11.10 | +0.00 | $11.37 | +31.62 | +31.62 | +0.00 | +31.62 |
| 2026-08-21 | `HIVE` | 394 | — | $3.24 | +0.00 | $3.03 | -82.74 | -82.74 | +0.00 | -82.74 |
| 2026-08-24 | `CRSP` | 21 | $59.50 | $58.79 | -14.91 | $56.91 | -39.48 | -54.39 | +1.26 | -38.22 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.50 | -7.20 | — | +0.00 | -7.20 | +10.70 | — |
| 2026-08-24 | `AUTL` | 517 | $2.41 | $2.36 | -25.85 | — | +0.00 | -25.85 | -56.87 | — |
| 2026-08-24 | `FUTU` | 11 | $123.64 | $120.87 | -30.47 | — | +0.00 | -30.47 | +62.59 | — |
| 2026-08-24 | `DE` | 2 | $647.47 | $653.62 | +12.30 | — | +0.00 | +12.30 | +60.72 | — |
| 2026-08-24 | `MARA` | 109 | $11.26 | $11.18 | -8.72 | — | +0.00 | -8.72 | -56.68 | — |
| 2026-08-24 | `BTDR` | 115 | $11.37 | $11.49 | +13.80 | — | +0.00 | +13.80 | +45.42 | — |
| 2026-08-24 | `HIVE` | 394 | $3.03 | $2.98 | -19.70 | — | +0.00 | -19.70 | -102.44 | — |
| 2026-08-25 | `CRSP` | 21 | $56.91 | $57.00 | +1.89 | — | +0.00 | +1.89 | -36.33 | — |
| 2026-08-25 | `RUM` | 153 | — | $9.36 | +0.00 | $9.35 | -1.53 | -1.53 | +0.00 | -1.53 |
| 2026-08-25 | `EZPW` | 41 | — | $34.48 | +0.00 | $34.69 | +8.61 | +8.61 | +0.00 | +8.61 |
| 2026-08-25 | `REAX` | 59 | — | $24.00 | +0.00 | $24.00 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `BKKT` | 173 | — | $8.28 | +0.00 | $8.38 | +17.30 | +17.30 | +0.00 | +17.30 |
| 2026-08-25 | `FCX` | 18 | — | $77.90 | +0.00 | $77.49 | -7.38 | -7.38 | +0.00 | -7.38 |
| 2026-08-25 | `NVAX` | 161 | — | $8.88 | +0.00 | $8.93 | +8.05 | +8.05 | +0.00 | +8.05 |
| 2026-08-25 | `AU` | 12 | — | $119.46 | +0.00 | $118.55 | -10.92 | -10.92 | +0.00 | -10.92 |
| 2026-08-26 | `RUM` | 153 | $9.35 | $9.35 | +0.00 | $9.35 | +0.00 | +0.00 | -1.53 | -1.53 |
| 2026-08-26 | `EZPW` | 41 | $34.69 | $34.69 | +0.00 | $34.69 | +0.00 | +0.00 | +8.61 | +8.61 |
| 2026-08-26 | `REAX` | 59 | $24.00 | $24.00 | +0.00 | $24.00 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `BKKT` | 173 | $8.38 | $8.38 | +0.00 | $8.38 | +0.00 | +0.00 | +17.30 | +17.30 |
| 2026-08-26 | `FCX` | 18 | $77.49 | $77.49 | +0.00 | $77.49 | +0.00 | +0.00 | -7.38 | -7.38 |
| 2026-08-26 | `NVAX` | 161 | $8.93 | $8.93 | +0.00 | $8.93 | +0.00 | +0.00 | +8.05 | +8.05 |
| 2026-08-26 | `AU` | 12 | $118.55 | $118.55 | +0.00 | $118.55 | +0.00 | +0.00 | -10.92 | -10.92 |
| 2026-08-27 | `RUM` | 153 | $9.35 | $10.07 | +110.16 | — | +0.00 | +110.16 | +108.63 | — |
| 2026-08-27 | `EZPW` | 41 | $34.69 | $35.70 | +41.41 | — | +0.00 | +41.41 | +50.02 | — |
| 2026-08-27 | `REAX` | 59 | $24.00 | $26.61 | +153.99 | — | +0.00 | +153.99 | +153.99 | — |
| 2026-08-27 | `BKKT` | 173 | $8.38 | $8.38 | +0.00 | — | +0.00 | +0.00 | +17.30 | — |
| 2026-08-27 | `FCX` | 18 | $77.49 | $79.34 | +33.30 | — | +0.00 | +33.30 | +25.92 | — |
| 2026-08-27 | `NVAX` | 161 | $8.93 | $9.33 | +64.40 | — | +0.00 | +64.40 | +72.45 | — |
| 2026-08-27 | `AU` | 12 | $118.55 | $119.80 | +15.00 | — | +0.00 | +15.00 | +4.08 | — |
| 2026-08-28 | `CAPR` | 189 | — | $9.19 | +0.00 | $10.06 | +164.43 | +164.43 | +0.00 | +164.43 |
| 2026-08-28 | `SEDG` | 51 | — | $33.78 | +0.00 | $33.51 | -13.77 | -13.77 | +0.00 | -13.77 |
| 2026-08-28 | `SMTC` | 11 | — | $149.40 | +0.00 | $142.43 | -76.67 | -76.67 | +0.00 | -76.67 |
| 2026-08-28 | `ERAS` | 90 | — | $19.30 | +0.00 | $19.49 | +17.10 | +17.10 | +0.00 | +17.10 |
| 2026-08-28 | `BBWI` | 93 | — | $18.68 | +0.00 | $18.65 | -2.79 | -2.79 | +0.00 | -2.79 |
| 2026-08-28 | `ZYME` | 59 | — | $29.33 | +0.00 | $29.01 | -18.88 | -18.88 | +0.00 | -18.88 |
| 2026-08-31 | `CAPR` | 189 | $10.06 | $9.44 | -117.18 | — | +0.00 | -117.18 | +47.25 | — |
| 2026-08-31 | `SEDG` | 51 | $33.51 | $31.50 | -102.51 | — | +0.00 | -102.51 | -116.28 | — |
| 2026-08-31 | `SMTC` | 11 | $142.43 | $133.04 | -103.29 | — | +0.00 | -103.29 | -179.96 | — |
| 2026-08-31 | `ERAS` | 90 | $19.49 | $17.90 | -143.10 | — | +0.00 | -143.10 | -126.00 | — |
| 2026-08-31 | `BBWI` | 93 | $18.65 | $19.30 | +60.45 | — | +0.00 | +60.45 | +57.66 | — |
| 2026-08-31 | `ZYME` | 59 | $29.01 | $28.27 | -43.66 | — | +0.00 | -43.66 | -62.54 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `MMED` | 110 | — | $22.78 | +0.00 | $23.76 | +107.80 | +107.80 | +0.00 | +107.80 |
| 2026-09-03 | `DELL` | 5 | — | $462.05 | +0.00 | $492.20 | +150.75 | +150.75 | +0.00 | +150.75 |
| 2026-09-03 | `FRNM` | 164 | — | $15.24 | +0.00 | $15.95 | +116.44 | +116.44 | +0.00 | +116.44 |
| 2026-09-03 | `CXW` | 76 | — | $32.87 | +0.00 | $32.61 | -19.76 | -19.76 | +0.00 | -19.76 |
| 2026-09-04 | `MMED` | 110 | $23.76 | $23.88 | +13.20 | — | +0.00 | +13.20 | +121.00 | — |
| 2026-09-04 | `DELL` | 5 | $492.20 | $486.31 | -29.45 | — | +0.00 | -29.45 | +121.30 | — |
| 2026-09-04 | `FRNM` | 164 | $15.95 | $15.87 | -13.12 | $16.90 | +168.92 | +155.80 | +103.32 | +272.24 |
| 2026-09-04 | `CXW` | 76 | $32.61 | $32.31 | -22.80 | — | +0.00 | -22.80 | -42.56 | — |
| 2026-09-04 | `BAK` | 3943 | — | $1.95 | +0.00 | $1.94 | -39.43 | -39.43 | +0.00 | -39.43 |
| 2026-09-07 | `FRNM` | 164 | $16.90 | $16.40 | -82.00 | $16.31 | -14.76 | -96.76 | +190.24 | +175.48 |
| 2026-09-07 | `BAK` | 3943 | $1.94 | $1.94 | +0.00 | — | +0.00 | +0.00 | -39.43 | — |
| 2026-09-07 | `CHPT` | 136 | — | $9.28 | +0.00 | $9.89 | +82.96 | +82.96 | +0.00 | +82.96 |
| 2026-09-07 | `SMMT` | 74 | — | $16.93 | +0.00 | $17.60 | +49.58 | +49.58 | +0.00 | +49.58 |
| 2026-09-07 | `SNOW` | 3 | — | $353.63 | +0.00 | $337.18 | -49.35 | -49.35 | +0.00 | -49.35 |
| 2026-09-07 | `MSTR` | 9 | — | $137.35 | +0.00 | $142.80 | +49.05 | +49.05 | +0.00 | +49.05 |
| 2026-09-07 | `MRX` | 16 | — | $75.65 | +0.00 | $78.27 | +41.92 | +41.92 | +0.00 | +41.92 |
| 2026-09-07 | `MMED` | 53 | — | $23.84 | +0.00 | $23.28 | -29.68 | -29.68 | +0.00 | -29.68 |
| 2026-09-08 | `FRNM` | 164 | $16.31 | $16.31 | +0.00 | — | +0.00 | +0.00 | +175.48 | — |
| 2026-09-08 | `CHPT` | 136 | $9.89 | $9.98 | +12.24 | — | +0.00 | +12.24 | +95.20 | — |
| 2026-09-08 | `SMMT` | 74 | $17.60 | $17.38 | -16.28 | — | +0.00 | -16.28 | +33.30 | — |
| 2026-09-08 | `SNOW` | 3 | $337.18 | $336.00 | -3.54 | — | +0.00 | -3.54 | -52.89 | — |
| 2026-09-08 | `MSTR` | 9 | $142.80 | $139.92 | -25.92 | — | +0.00 | -25.92 | +23.13 | — |
| 2026-09-08 | `MRX` | 16 | $78.27 | $79.95 | +26.88 | — | +0.00 | +26.88 | +68.80 | — |
| 2026-09-08 | `MMED` | 53 | $23.28 | $23.00 | -14.84 | — | +0.00 | -14.84 | -44.52 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +68.63 | ANGX, ARX, SNDK, MH, HLIT | — | $359.91 | $10,053.48 | ANGX×464, ARX×102, SNDK×1, MH×147, HLIT×151 |
| 2026-08-17 | +2.25 | $359.91 | ANGX×464, ARX×102, SNDK×1, MH×147, HLIT×151 | $10,215.56 | +162.08 | +0.00 | — | ANGX, ARX, SNDK, MH, HLIT | $10,200.18 | $10,200.18 | — |
| 2026-08-18 | -6.20 | $10,200.18 | — | $10,200.18 | -0.00 | +0.00 | — | — | $10,200.18 | $10,200.18 | — |
| 2026-08-19 | -7.20 | $10,200.18 | — | $10,200.18 | -0.00 | +0.00 | — | — | $10,200.18 | $10,200.18 | — |
| 2026-08-20 | +1.12 | $10,200.18 | — | $10,200.18 | -0.00 | -184.48 | BHP, MRNA, HUMA, BTGO, ASST, ZLAB, CRSP, APA | — | $142.77 | $9,982.45 | BHP×14, MRNA×8, HUMA×1803, BTGO×193, ASST×79, ZLAB×47, CRSP×21, APA×28 |
| 2026-08-21 | +3.25 | $142.77 | BHP×14, MRNA×8, HUMA×1803, BTGO×193, ASST×79, ZLAB×47, CRSP×21, APA×28 | $10,233.88 | +251.43 | +24.66 | AU, AUTL, FUTU, DE, MARA, BTDR, HIVE | BHP, MRNA, HUMA, BTGO, ASST, ZLAB, APA | $113.67 | $10,205.03 | CRSP×21, AU×10, AUTL×517, FUTU×11, DE×2, MARA×109, BTDR×115, HIVE×394 |
| 2026-08-24 | -5.17 | $113.67 | CRSP×21, AU×10, AUTL×517, FUTU×11, DE×2, MARA×109, BTDR×115, HIVE×394 | $10,124.28 | -80.75 | -39.48 | — | AU, AUTL, FUTU, DE, MARA, BTDR, HIVE | $8,866.96 | $10,062.07 | CRSP×21 |
| 2026-08-25 | +1.80 | $8,866.96 | CRSP×21 | $10,063.96 | +1.89 | +14.13 | RUM, EZPW, REAX, BKKT, FCX, NVAX, AU | CRSP | $86.50 | $10,060.23 | RUM×153, EZPW×41, REAX×59, BKKT×173, FCX×18, NVAX×161, AU×12 |
| 2026-08-26 | +2.02 | $86.50 | RUM×153, EZPW×41, REAX×59, BKKT×173, FCX×18, NVAX×161, AU×12 | $10,060.23 | +0.00 | +0.00 | — | — | $86.50 | $10,060.23 | RUM×153, EZPW×41, REAX×59, BKKT×173, FCX×18, NVAX×161, AU×12 |
| 2026-08-27 | — | $86.50 | RUM×153, EZPW×41, REAX×59, BKKT×173, FCX×18, NVAX×161, AU×12 | $10,478.49 | +418.26 | +0.00 | — | RUM, EZPW, REAX, BKKT, FCX, NVAX, AU | $10,462.51 | $10,462.51 | — |
| 2026-08-28 | +0.75 | $10,462.51 | — | $10,462.51 | +0.00 | +69.42 | CAPR, SEDG, SMTC, ERAS, BBWI, ZYME | — | $141.29 | $10,518.51 | CAPR×189, SEDG×51, SMTC×11, ERAS×90, BBWI×93, ZYME×59 |
| 2026-08-31 | -5.85 | $141.29 | CAPR×189, SEDG×51, SMTC×11, ERAS×90, BBWI×93, ZYME×59 | $10,069.22 | -449.29 | +0.00 | — | CAPR, SEDG, SMTC, ERAS, BBWI, ZYME | $10,055.63 | $10,055.63 | — |
| 2026-09-01 | -6.30 | $10,055.63 | — | $10,055.63 | +0.00 | +0.00 | — | — | $10,055.63 | $10,055.63 | — |
| 2026-09-02 | -3.83 | $10,055.63 | — | $10,055.63 | +0.00 | +0.00 | — | — | $10,055.63 | $10,055.63 | — |
| 2026-09-03 | -0.90 | $10,055.63 | — | $10,055.63 | +0.00 | +355.23 | MMED, DELL, FRNM, CXW | — | $233.08 | $10,401.84 | MMED×110, DELL×5, FRNM×164, CXW×76 |
| 2026-09-04 | +2.25 | $233.08 | MMED×110, DELL×5, FRNM×164, CXW×76 | $10,349.67 | -52.17 | +129.49 | BAK | MMED, DELL, CXW | $0.63 | $10,421.65 | FRNM×164, BAK×3943 |
| 2026-09-07 | — | $0.63 | FRNM×164, BAK×3943 | $10,339.65 | -82.00 | +129.72 | CHPT, SMMT, SNOW, MSTR, MRX, MMED | BAK | $299.79 | $10,404.97 | FRNM×164, CHPT×136, SMMT×74, SNOW×3, MSTR×9, MRX×16, MMED×53 |
| 2026-09-08 | -11.47 | $299.79 | FRNM×164, CHPT×136, SMMT×74, SNOW×3, MSTR×9, MRX×16, MMED×53 | $10,383.51 | -21.46 | +0.00 | — | FRNM, CHPT, SMMT, SNOW, MSTR, MRX, MMED | $10,368.03 | $10,368.03 | — |

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
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 464 | $4.60 | $6.08 | $+122.49 | $2,488.23 | ▲ +122.49 after sell → book $10,209.48; vs 09:30 mark -6.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 102 | $19.57 | $2.33 | $-4.62 | $4,482.04 | ▼ -4.62 after sell → book $10,207.15; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SNDK` | 1 | $1700.74 | $2.02 | $+49.81 | $6,180.77 | ▲ +49.81 after sell → book $10,205.13; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 147 | $13.16 | $2.47 | $-62.23 | $8,112.82 | ▼ -62.23 after sell → book $10,202.66; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 151 | $13.84 | $2.48 | $+94.73 | $10,200.18 | ▲ +94.73 after sell → book $10,200.18; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,200.18 | ▲ close $10,200.18 vs 09:30 $10,215.56 (session +0.00) | 16:00 close · cash $10,200.18 · no lots left · equity $10,200.18. | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,200.18 | ▲ 09:30 equity $10,200.18 vs yday $10,200.18 (-0.00) | 09:30 open · cash $10,200.18 · no holdings · equity $10,200.18 vs prior close $10,200.18 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,200.18 | ▲ close $10,200.18 vs 09:30 $10,200.18 (session +0.00) | 16:00 close · cash $10,200.18 · no lots left · equity $10,200.18. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,200.18 | ▲ 09:30 equity $10,200.18 vs yday $10,200.18 (-0.00) | 09:30 open · cash $10,200.18 · no holdings · equity $10,200.18 vs prior close $10,200.18 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,200.18 | ▲ close $10,200.18 vs 09:30 $10,200.18 (session +0.00) | 16:00 close · cash $10,200.18 · no lots left · equity $10,200.18. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,200.18 | ▲ 09:30 equity $10,200.18 vs yday $10,200.18 (-0.00) | 09:30 open · cash $10,200.18 · no holdings · equity $10,200.18 vs prior close $10,200.18 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $8,924.00 | — | combo gate; gate news=good,vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1275.02 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $7,720.87 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1275.02 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 1803 | $0.71 | $18.16 | — | $6,427.99 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+10.5; leftover $1275.02 | join🟡 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 193 | $6.61 | $2.57 | — | $5,150.66 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1275.02 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 79 | $16.00 | $2.23 | — | $3,884.43 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1275.02 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 47 | $26.57 | $2.13 | — | $2,633.51 | — | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ret5=+4.8; leftover $1275.02 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 21 | $58.73 | $2.05 | — | $1,398.13 | — | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1275.02 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 28 | $44.76 | $2.07 | — | $142.77 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ret5=+8.7; leftover $1275.02 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $142.77 | ▼ close $9,982.45 vs 09:30 $10,200.18 (session -184.48) | 16:00 close · cash $142.77 · equity $9,982.45 vs 09:30 $10,200.18 (-217.73; session marks -184.48) · 8 name(s) marked open→close (per-name table). BHP×14 09:30 $91.01 → close $93.63 +36.68; MRNA×8 09:30 $150.14 → close $133.32 -134.56; HUMA×1803 09:30 $0.71 → close $0.68 -46.88; BTGO×193 09:30 $6.61 → close $6.60 -0.97; ASST×79 09:30 $16.00 → close $16.13 +10.27; ZLAB×47 09:30 $26.57 → close $26.02 -25.85; CRSP×21 09:30 $58.73 → close $58.12 -12.81; APA×28 09:30 $44.76 → close $44.39 -10.36 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $142.77 | ▲ 09:30 equity $10,233.88 vs yday $9,982.45 (+251.43) | 09:30 open · cash $142.77 (unchanged overnight, no fees) · equity $10,233.88 vs prior close $9,982.45 (+251.43) · 8 name(s) re-marked at the open (per-name table). BHP×14 yday $93.63 → 09:30 $95.72 +29.26; MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; HUMA×1803 yday $0.68 → 09:30 $0.67 -12.62; BTGO×193 yday $6.60 → 09:30 $6.95 +67.55; ASST×79 yday $16.13 → 09:30 $17.66 +120.87; ZLAB×47 yday $26.02 → 09:30 $26.25 +10.81; CRSP×21 yday $58.12 → 09:30 $59.72 +33.60; APA×28 yday $44.39 → 09:30 $44.52 +3.64 | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 14 | $95.72 | $2.05 | $+61.86 | $1,480.80 | ▲ +61.86 after sell → book $10,231.82; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 8 | $133.11 | $2.03 | $-140.29 | $2,543.65 | ▼ -140.29 after sell → book $10,229.79; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `HUMA` | 1803 | $0.67 | $17.87 | $-95.53 | $3,741.00 | ▼ -95.53 after sell → book $10,211.92; vs 09:30 mark -17.87 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 193 | $6.95 | $2.61 | $+61.40 | $5,079.74 | ▲ +61.40 after sell → book $10,209.31; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 79 | $17.66 | $2.25 | $+126.66 | $6,472.62 | ▲ +126.66 after sell → book $10,207.05; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 47 | $26.25 | $2.15 | $-19.32 | $7,704.22 | ▼ -19.32 after sell → book $10,204.90; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 28 | $44.52 | $2.09 | $-10.89 | $8,948.69 | ▼ -10.89 after sell → book $10,202.81; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $7,752.37 | — | combo gate; gate news=good,vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $1278.38 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 517 | $2.47 | $6.67 | — | $6,468.71 | — | combo gate; gate news=good,vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1278.38 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 11 | $115.18 | $2.02 | — | $5,199.71 | — | combo gate; gate news=good,vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1278.38 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $3,951.19 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1278.38 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 109 | $11.70 | $2.32 | — | $2,673.57 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $1278.38 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 115 | $11.10 | $2.33 | — | $1,395.31 | — | combo gate; gate news=good,vol=good; list yday_gainer; ret5=+19.1; leftover $1278.38 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 394 | $3.24 | $5.08 | — | $113.67 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ret5=+21.3; leftover $1278.38 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $113.67 | ▲ close $10,205.03 vs 09:30 $10,233.88 (session +24.66) | 16:00 close · cash $113.67 · equity $10,205.03 vs 09:30 $10,233.88 (-28.85; session marks +24.66) · 8 name(s) marked open→close (per-name table). CRSP×21 09:30 $59.72 → close $59.50 -4.62; AU×10 09:30 $119.43 → close $121.22 +17.90; AUTL×517 09:30 $2.47 → close $2.41 -31.02; FUTU×11 09:30 $115.18 → close $123.64 +93.06; DE×2 09:30 $623.26 → close $647.47 +48.42; MARA×109 09:30 $11.70 → close $11.26 -47.96; BTDR×115 09:30 $11.10 → close $11.37 +31.62; HIVE×394 09:30 $3.24 → close $3.03 -82.74 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $113.67 | ▼ 09:30 equity $10,124.28 vs yday $10,205.03 (-80.75) | 09:30 open · cash $113.67 (unchanged overnight, no fees) · equity $10,124.28 vs prior close $10,205.03 (-80.75) · 8 name(s) re-marked at the open (per-name table). CRSP×21 yday $59.50 → 09:30 $58.79 -14.91; AU×10 yday $121.22 → 09:30 $120.50 -7.20; AUTL×517 yday $2.41 → 09:30 $2.36 -25.85; FUTU×11 yday $123.64 → 09:30 $120.87 -30.47; DE×2 yday $647.47 → 09:30 $653.62 +12.30; MARA×109 yday $11.26 → 09:30 $11.18 -8.72; BTDR×115 yday $11.37 → 09:30 $11.49 +13.80; HIVE×394 yday $3.03 → 09:30 $2.98 -19.70 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.50 | $2.04 | $+6.64 | $1,316.63 | ▲ +6.64 after sell → book $10,122.24; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 517 | $2.36 | $6.77 | $-70.30 | $2,529.99 | ▼ -70.30 after sell → book $10,115.48; vs 09:30 mark -6.76 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 11 | $120.87 | $2.04 | $+58.52 | $3,857.51 | ▲ +58.52 after sell → book $10,113.43; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.62 | $2.02 | $+56.71 | $5,162.74 | ▲ +56.71 after sell → book $10,111.42; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 109 | $11.18 | $2.35 | $-61.34 | $6,379.01 | ▼ -61.34 after sell → book $10,109.07; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTDR` | 115 | $11.49 | $2.36 | $+40.73 | $7,698.00 | ▲ +40.73 after sell → book $10,106.71; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 394 | $2.98 | $5.16 | $-112.68 | $8,866.96 | ▼ -112.68 after sell → book $10,101.55; vs 09:30 mark -5.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,866.96 | ▼ close $10,062.07 vs 09:30 $10,124.28 (session -39.48) | 16:00 close · cash $8,866.96 · equity $10,062.07 vs 09:30 $10,124.28 (-62.21; session marks -39.48) · 1 name(s) marked open→close (per-name table). CRSP×21 09:30 $58.79 → close $56.91 -39.48 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,866.96 | ▲ 09:30 equity $10,063.96 vs yday $10,062.07 (+1.89) | 09:30 open · cash $8,866.96 (unchanged overnight, no fees) · equity $10,063.96 vs prior close $10,062.07 (+1.89) · 1 name(s) re-marked at the open (per-name table). CRSP×21 yday $56.91 → 09:30 $57.00 +1.89 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 21 | $57.00 | $2.07 | $-40.46 | $10,061.89 | ▼ -40.46 after sell → book $10,061.89; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 153 | $9.36 | $2.45 | — | $8,627.36 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ret5=+21.3; leftover $1437.41 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 41 | $34.48 | $2.11 | — | $7,211.56 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1437.41 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 59 | $24.00 | $2.17 | — | $5,793.40 | — | combo gate; gate news=good,vol=good; list yday_mover; ret5=+10.0; leftover $1437.41 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BKKT` | 173 | $8.28 | $2.51 | — | $4,358.45 | — | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ret5=+12.3; leftover $1437.41 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 18 | $77.90 | $2.04 | — | $2,954.20 | — | combo gate; gate news=good,vol=good; list ohlc_hot,mover_buy; ⚪; ret5=+15.3; leftover $1437.41 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NVAX` | 161 | $8.88 | $2.47 | — | $1,522.05 | — | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ret5=+11.1; leftover $1437.41 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 12 | $119.46 | $2.03 | — | $86.50 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ⚪; ret5=+25.9; leftover $1437.41 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $86.50 | ▲ close $10,060.23 vs 09:30 $10,063.96 (session +14.13) | 16:00 close · cash $86.50 · equity $10,060.23 vs 09:30 $10,063.96 (-3.73; session marks +14.13) · 7 name(s) marked open→close (per-name table). RUM×153 09:30 $9.36 → close $9.35 -1.53; EZPW×41 09:30 $34.48 → close $34.69 +8.61; REAX×59 09:30 $24.00 → close $24.00 +0.00; BKKT×173 09:30 $8.28 → close $8.38 +17.30; FCX×18 09:30 $77.90 → close $77.49 -7.38; NVAX×161 09:30 $8.88 → close $8.93 +8.05; AU×12 09:30 $119.46 → close $118.55 -10.92 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $86.50 | ▲ 09:30 equity $10,060.23 vs yday $10,060.23 (+0.00) | 09:30 open · cash $86.50 (unchanged overnight, no fees) · equity $10,060.23 vs prior close $10,060.23 (+0.00) · 7 name(s) re-marked at the open (per-name table). RUM×153 yday $9.35 → 09:30 $9.35 +0.00; EZPW×41 yday $34.69 → 09:30 $34.69 +0.00; REAX×59 yday $24.00 → 09:30 $24.00 +0.00; BKKT×173 yday $8.38 → 09:30 $8.38 +0.00; FCX×18 yday $77.49 → 09:30 $77.49 +0.00; NVAX×161 yday $8.93 → 09:30 $8.93 +0.00; AU×12 yday $118.55 → 09:30 $118.55 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $86.50 | ▲ close $10,060.23 vs 09:30 $10,060.23 (session +0.00) | 16:00 close · cash $86.50 · equity $10,060.23 vs 09:30 $10,060.23 (+0.00; session marks +0.00) · 7 name(s) marked open→close (per-name table). RUM×153 09:30 $9.35 → close $9.35 +0.00; EZPW×41 09:30 $34.69 → close $34.69 +0.00; REAX×59 09:30 $24.00 → close $24.00 +0.00; BKKT×173 09:30 $8.38 → close $8.38 +0.00; FCX×18 09:30 $77.49 → close $77.49 +0.00; NVAX×161 09:30 $8.93 → close $8.93 +0.00; AU×12 09:30 $118.55 → close $118.55 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $86.50 | ▲ 09:30 equity $10,478.49 vs yday $10,060.23 (+418.26) | 09:30 open · cash $86.50 (unchanged overnight, no fees) · equity $10,478.49 vs prior close $10,060.23 (+418.26) · 7 name(s) re-marked at the open (per-name table). RUM×153 yday $9.35 → 09:30 $10.07 +110.16; EZPW×41 yday $34.69 → 09:30 $35.70 +41.41; REAX×59 yday $24.00 → 09:30 $26.61 +153.99; BKKT×173 yday $8.38 → 09:30 $8.38 +0.00; FCX×18 yday $77.49 → 09:30 $79.34 +33.30; NVAX×161 yday $8.93 → 09:30 $9.33 +64.40; AU×12 yday $118.55 → 09:30 $119.80 +15.00 | — |
| 2026-08-27 09:30 ET | **SELL** | `RUM` | 153 | $10.07 | $2.49 | $+103.69 | $1,624.73 | ▲ +103.69 after sell → book $10,476.01; vs 09:30 mark -2.48 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `EZPW` | 41 | $35.70 | $2.13 | $+45.77 | $3,086.29 | ▲ +45.77 after sell → book $10,473.87; vs 09:30 mark -2.14 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `REAX` | 59 | $26.61 | $2.19 | $+149.63 | $4,654.09 | ▲ +149.63 after sell → book $10,471.68; vs 09:30 mark -2.19 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BKKT` | 173 | $8.38 | $2.55 | $+12.24 | $6,101.28 | ▲ +12.24 after sell → book $10,469.13; vs 09:30 mark -2.55 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FCX` | 18 | $79.34 | $2.07 | $+21.81 | $7,527.34 | ▲ +21.81 after sell → book $10,467.07; vs 09:30 mark -2.06 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NVAX` | 161 | $9.33 | $2.51 | $+67.47 | $9,026.96 | ▲ +67.47 after sell → book $10,464.56; vs 09:30 mark -2.51 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AU` | 12 | $119.80 | $2.05 | $+0.01 | $10,462.51 | ▲ +0.01 after sell → book $10,462.51; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,462.51 | ▲ close $10,462.51 vs 09:30 $10,478.49 (session +0.00) | 16:00 close · cash $10,462.51 · no lots left · equity $10,462.51. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,462.51 | ▲ 09:30 equity $10,462.51 vs yday $10,462.51 (+0.00) | 09:30 open · cash $10,462.51 · no holdings · equity $10,462.51 vs prior close $10,462.51 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 189 | $9.19 | $2.56 | — | $8,723.04 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1743.75 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 51 | $33.78 | $2.14 | — | $6,998.12 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1743.75 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 11 | $149.40 | $2.02 | — | $5,352.70 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1743.75 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 90 | $19.30 | $2.26 | — | $3,613.44 | — | combo gate; gate news=good,vol=good; list yday_gainer; ret5=-4.1; leftover $1743.75 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 93 | $18.68 | $2.27 | — | $1,873.93 | — | combo gate; gate news=good,vol=good; list yday_gainer; ret5=+0.2; leftover $1743.75 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 59 | $29.33 | $2.17 | — | $141.29 | — | combo gate; gate news=good,vol=good; list yday_gainer,ohlc_hot; ret5=+14.1; leftover $1743.75 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $141.29 | ▲ close $10,518.51 vs 09:30 $10,462.51 (session +69.42) | 16:00 close · cash $141.29 · equity $10,518.51 vs 09:30 $10,462.51 (+56.00; session marks +69.42) · 6 name(s) marked open→close (per-name table). CAPR×189 09:30 $9.19 → close $10.06 +164.43; SEDG×51 09:30 $33.78 → close $33.51 -13.77; SMTC×11 09:30 $149.40 → close $142.43 -76.67; ERAS×90 09:30 $19.30 → close $19.49 +17.10; BBWI×93 09:30 $18.68 → close $18.65 -2.79; ZYME×59 09:30 $29.33 → close $29.01 -18.88 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $141.29 | ▼ 09:30 equity $10,069.22 vs yday $10,518.51 (-449.29) | 09:30 open · cash $141.29 (unchanged overnight, no fees) · equity $10,069.22 vs prior close $10,518.51 (-449.29) · 6 name(s) re-marked at the open (per-name table). CAPR×189 yday $10.06 → 09:30 $9.44 -117.18; SEDG×51 yday $33.51 → 09:30 $31.50 -102.51; SMTC×11 yday $142.43 → 09:30 $133.04 -103.29; ERAS×90 yday $19.49 → 09:30 $17.90 -143.10; BBWI×93 yday $18.65 → 09:30 $19.30 +60.45; ZYME×59 yday $29.01 → 09:30 $28.27 -43.66 | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 189 | $9.44 | $2.60 | $+42.09 | $1,922.85 | ▲ +42.09 after sell → book $10,066.62; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 51 | $31.50 | $2.17 | $-120.59 | $3,527.18 | ▼ -120.59 after sell → book $10,064.45; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 11 | $133.04 | $2.04 | $-184.03 | $4,988.58 | ▼ -184.03 after sell → book $10,062.41; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 90 | $17.90 | $2.29 | $-130.55 | $6,597.29 | ▼ -130.55 after sell → book $10,060.12; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 93 | $19.30 | $2.30 | $+53.09 | $8,389.89 | ▲ +53.09 after sell → book $10,057.82; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 59 | $28.27 | $2.19 | $-66.90 | $10,055.63 | ▼ -66.90 after sell → book $10,055.63; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,055.63 | ▲ close $10,055.63 vs 09:30 $10,069.22 (session +0.00) | 16:00 close · cash $10,055.63 · no lots left · equity $10,055.63. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,055.63 | ▲ 09:30 equity $10,055.63 vs yday $10,055.63 (+0.00) | 09:30 open · cash $10,055.63 · no holdings · equity $10,055.63 vs prior close $10,055.63 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,055.63 | ▲ close $10,055.63 vs 09:30 $10,055.63 (session +0.00) | 16:00 close · cash $10,055.63 · no lots left · equity $10,055.63. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,055.63 | ▲ 09:30 equity $10,055.63 vs yday $10,055.63 (+0.00) | 09:30 open · cash $10,055.63 · no holdings · equity $10,055.63 vs prior close $10,055.63 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,055.63 | ▲ close $10,055.63 vs 09:30 $10,055.63 (session +0.00) | 16:00 close · cash $10,055.63 · no lots left · equity $10,055.63. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,055.63 | ▲ 09:30 equity $10,055.63 vs yday $10,055.63 (+0.00) | 09:30 open · cash $10,055.63 · no holdings · equity $10,055.63 vs prior close $10,055.63 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 110 | $22.78 | $2.32 | — | $7,547.51 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $2513.91 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 5 | $462.05 | $2.00 | — | $5,235.26 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ret5=-9.9; leftover $2513.91 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 164 | $15.24 | $2.48 | — | $2,733.41 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ⚪; ret5=+19.5; leftover $2513.91 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 76 | $32.87 | $2.22 | — | $233.08 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ⚪; ret5=+3.6; leftover $2513.91 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $233.08 | ▲ close $10,401.84 vs 09:30 $10,055.63 (session +355.23) | 16:00 close · cash $233.08 · equity $10,401.84 vs 09:30 $10,055.63 (+346.21; session marks +355.23) · 4 name(s) marked open→close (per-name table). MMED×110 09:30 $22.78 → close $23.76 +107.80; DELL×5 09:30 $462.05 → close $492.20 +150.75; FRNM×164 09:30 $15.24 → close $15.95 +116.44; CXW×76 09:30 $32.87 → close $32.61 -19.76 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $233.08 | ▼ 09:30 equity $10,349.67 vs yday $10,401.84 (-52.17) | 09:30 open · cash $233.08 (unchanged overnight, no fees) · equity $10,349.67 vs prior close $10,401.84 (-52.17) · 4 name(s) re-marked at the open (per-name table). MMED×110 yday $23.76 → 09:30 $23.88 +13.20; DELL×5 yday $492.20 → 09:30 $486.31 -29.45; FRNM×164 yday $15.95 → 09:30 $15.87 -13.12; CXW×76 yday $32.61 → 09:30 $32.31 -22.80 | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 110 | $23.88 | $2.36 | $+116.32 | $2,857.52 | ▲ +116.32 after sell → book $10,347.31; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 5 | $486.31 | $2.03 | $+117.26 | $5,287.03 | ▲ +117.26 after sell → book $10,345.27; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 76 | $32.31 | $2.25 | $-47.03 | $7,740.34 | ▼ -47.03 after sell → book $10,343.02; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 3943 | $1.95 | $50.86 | — | $0.63 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $7740.34 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.63 | ▲ close $10,421.65 vs 09:30 $10,349.67 (session +129.49) | 16:00 close · cash $0.63 · equity $10,421.65 vs 09:30 $10,349.67 (+71.98; session marks +129.49) · 2 name(s) marked open→close (per-name table). FRNM×164 09:30 $15.87 → close $16.90 +168.92; BAK×3943 09:30 $1.95 → close $1.94 -39.43 | — |
| 2026-09-07 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.63 | ▼ 09:30 equity $10,339.65 vs yday $10,421.65 (-82.00) | 09:30 open · cash $0.63 (unchanged overnight, no fees) · equity $10,339.65 vs prior close $10,421.65 (-82.00) · 2 name(s) re-marked at the open (per-name table). FRNM×164 yday $16.90 → 09:30 $16.40 -82.00; BAK×3943 yday $1.94 → 09:30 $1.94 +0.00 | — |
| 2026-09-07 09:30 ET | **SELL** | `BAK` | 3943 | $1.94 | $51.58 | $-141.88 | $7,598.47 | ▼ -141.88 after sell → book $10,288.07; vs 09:30 mark -51.58 | dropped from list after 1 sess (min 1) | — |
| 2026-09-07 09:30 ET | **BUY** | `CHPT` | 136 | $9.28 | $2.40 | — | $6,333.99 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.1; leftover $1266.41 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **BUY** | `SMMT` | 74 | $16.93 | $2.21 | — | $5,078.96 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; ret5=-1.4; leftover $1266.41 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **BUY** | `SNOW` | 3 | $353.63 | $2.00 | — | $4,016.07 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; ret5=+1.2; leftover $1266.41 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **BUY** | `MSTR` | 9 | $137.35 | $2.02 | — | $2,777.90 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+28.2; leftover $1266.41 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-07 09:30 ET | **BUY** | `MRX` | 16 | $75.65 | $2.04 | — | $1,565.46 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ⚪; ret5=+2.7; leftover $1266.41 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-07 09:30 ET | **BUY** | `MMED` | 53 | $23.84 | $2.15 | — | $299.79 | — | combo gate; gate news=good,vol=good; list mover_buy; ⚪; ret5=+1.5; leftover $1266.41 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-07 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $299.79 | ▲ close $10,404.97 vs 09:30 $10,339.65 (session +129.72) | 16:00 close · cash $299.79 · equity $10,404.97 vs 09:30 $10,339.65 (+65.32; session marks +129.72) · 7 name(s) marked open→close (per-name table). FRNM×164 09:30 $16.40 → close $16.31 -14.76; CHPT×136 09:30 $9.28 → close $9.89 +82.96; SMMT×74 09:30 $16.93 → close $17.60 +49.58; SNOW×3 09:30 $353.63 → close $337.18 -49.35; MSTR×9 09:30 $137.35 → close $142.80 +49.05; MRX×16 09:30 $75.65 → close $78.27 +41.92; MMED×53 09:30 $23.84 → close $23.28 -29.68 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $299.79 | ▼ 09:30 equity $10,383.51 vs yday $10,404.97 (-21.46) | 09:30 open · cash $299.79 (unchanged overnight, no fees) · equity $10,383.51 vs prior close $10,404.97 (-21.46) · 7 name(s) re-marked at the open (per-name table). FRNM×164 yday $16.31 → 09:30 $16.31 +0.00; CHPT×136 yday $9.89 → 09:30 $9.98 +12.24; SMMT×74 yday $17.60 → 09:30 $17.38 -16.28; SNOW×3 yday $337.18 → 09:30 $336.00 -3.54; MSTR×9 yday $142.80 → 09:30 $139.92 -25.92; MRX×16 yday $78.27 → 09:30 $79.95 +26.88; MMED×53 yday $23.28 → 09:30 $23.00 -14.84 | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 164 | $16.31 | $2.53 | $+170.47 | $2,972.10 | ▲ +170.47 after sell → book $10,380.98; vs 09:30 mark -2.53 | dropped from list after 3 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CHPT` | 136 | $9.98 | $2.43 | $+90.37 | $4,326.95 | ▲ +90.37 after sell → book $10,378.55; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `SMMT` | 74 | $17.38 | $2.23 | $+28.85 | $5,610.84 | ▲ +28.85 after sell → book $10,376.32; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `SNOW` | 3 | $336.00 | $2.02 | $-56.91 | $6,616.82 | ▼ -56.91 after sell → book $10,374.30; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MSTR` | 9 | $139.92 | $2.04 | $+19.08 | $7,874.06 | ▲ +19.08 after sell → book $10,372.26; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MRX` | 16 | $79.95 | $2.06 | $+64.70 | $9,151.20 | ▲ +64.70 after sell → book $10,370.20; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MMED` | 53 | $23.00 | $2.17 | $-48.84 | $10,368.03 | ▼ -48.84 after sell → book $10,368.03; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,368.03 | ▲ close $10,368.03 vs 09:30 $10,383.51 (session +0.00) | 16:00 close · cash $10,368.03 · no lots left · equity $10,368.03. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AUTL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ANGX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `RUM` | no_price | no 09:30 open — carry |
| 2026-08-26 | `EZPW` | no_price | no 09:30 open — carry |
| 2026-08-26 | `REAX` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BKKT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `FCX` | no_price | no 09:30 open — carry |
| 2026-08-26 | `NVAX` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AU` | no_price | no 09:30 open — carry |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-08 | `AIRS` | hard_red | hard-red S=-11.47 sit; no new buys |
