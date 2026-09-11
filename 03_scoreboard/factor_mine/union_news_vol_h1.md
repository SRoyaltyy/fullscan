# Factor mine action — `union_news_vol_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **+2.83%** ($10,283) · signal-only (no cash/fees) was +5.80%. Starts YES **14/20**. Fills 76 · skips 28 · realized $+283.36.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,283.35.

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
| 2026-08-24 | `CRSP` | 21 | $59.50 | $58.75 | -15.75 | $57.08 | -35.17 | -50.92 | +0.42 | -34.75 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.51 | -7.10 | — | +0.00 | -7.10 | +10.80 | — |
| 2026-08-24 | `AUTL` | 517 | $2.41 | $2.40 | -5.17 | — | +0.00 | -5.17 | -36.19 | — |
| 2026-08-24 | `FUTU` | 11 | $123.64 | $121.00 | -29.04 | — | +0.00 | -29.04 | +64.02 | — |
| 2026-08-24 | `DE` | 2 | $647.47 | $653.04 | +11.14 | — | +0.00 | +11.14 | +59.56 | — |
| 2026-08-24 | `MARA` | 109 | $11.26 | $11.17 | -9.81 | — | +0.00 | -9.81 | -57.77 | — |
| 2026-08-24 | `BTDR` | 115 | $11.37 | $11.48 | +12.65 | — | +0.00 | +12.65 | +44.27 | — |
| 2026-08-24 | `HIVE` | 394 | $3.03 | $2.99 | -15.76 | — | +0.00 | -15.76 | -98.50 | — |
| 2026-08-25 | `CRSP` | 21 | $57.08 | $57.93 | +17.95 | — | +0.00 | +17.95 | -16.80 | — |
| 2026-08-25 | `RUM` | 214 | — | $9.42 | +0.00 | $10.23 | +173.34 | +173.34 | +0.00 | +173.34 |
| 2026-08-25 | `EZPW` | 57 | — | $35.05 | +0.00 | $35.23 | +10.26 | +10.26 | +0.00 | +10.26 |
| 2026-08-25 | `REAX` | 83 | — | $24.11 | +0.00 | $28.43 | +358.56 | +358.56 | +0.00 | +358.56 |
| 2026-08-25 | `AU` | 17 | — | $118.52 | +0.00 | $123.39 | +82.79 | +82.79 | +0.00 | +82.79 |
| 2026-08-25 | `FCX` | 26 | — | $77.13 | +0.00 | $79.91 | +72.28 | +72.28 | +0.00 | +72.28 |
| 2026-08-26 | `RUM` | 214 | $10.23 | $10.07 | -34.24 | — | +0.00 | -34.24 | +139.10 | — |
| 2026-08-26 | `EZPW` | 57 | $35.23 | $35.70 | +26.79 | — | +0.00 | +26.79 | +37.05 | — |
| 2026-08-26 | `REAX` | 83 | $28.43 | $26.61 | -151.06 | — | +0.00 | -151.06 | +207.50 | — |
| 2026-08-26 | `AU` | 17 | $123.39 | $119.80 | -61.03 | — | +0.00 | -61.03 | +21.76 | — |
| 2026-08-26 | `FCX` | 26 | $79.91 | $79.34 | -14.82 | — | +0.00 | -14.82 | +57.46 | — |
| 2026-08-27 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-28 | `SEDG` | 45 | — | $32.90 | +0.00 | $31.41 | -67.05 | -67.05 | +0.00 | -67.05 |
| 2026-08-28 | `CAPR` | 154 | — | $9.73 | +0.00 | $9.59 | -21.56 | -21.56 | +0.00 | -21.56 |
| 2026-08-28 | `SMTC` | 10 | — | $141.76 | +0.00 | $131.17 | -105.90 | -105.90 | +0.00 | -105.90 |
| 2026-08-28 | `ERAS` | 78 | — | $19.25 | +0.00 | $18.03 | -95.16 | -95.16 | +0.00 | -95.16 |
| 2026-08-28 | `BBWI` | 80 | — | $18.75 | +0.00 | $19.22 | +37.60 | +37.60 | +0.00 | +37.60 |
| 2026-08-28 | `ZYME` | 52 | — | $28.91 | +0.00 | $28.27 | -33.28 | -33.28 | +0.00 | -33.28 |
| 2026-08-28 | `TH` | 79 | — | $19.00 | +0.00 | $18.55 | -35.55 | -35.55 | +0.00 | -35.55 |
| 2026-08-31 | `SEDG` | 45 | $31.41 | $31.15 | -11.70 | — | +0.00 | -11.70 | -78.75 | — |
| 2026-08-31 | `CAPR` | 154 | $9.59 | $9.50 | -13.86 | — | +0.00 | -13.86 | -35.42 | — |
| 2026-08-31 | `SMTC` | 10 | $131.17 | $132.30 | +11.30 | — | +0.00 | +11.30 | -94.60 | — |
| 2026-08-31 | `ERAS` | 78 | $18.03 | $17.87 | -12.48 | — | +0.00 | -12.48 | -107.64 | — |
| 2026-08-31 | `BBWI` | 80 | $19.22 | $19.25 | +2.40 | — | +0.00 | +2.40 | +40.00 | — |
| 2026-08-31 | `ZYME` | 52 | $28.27 | $28.06 | -10.92 | — | +0.00 | -10.92 | -44.20 | — |
| 2026-08-31 | `TH` | 79 | $18.55 | $18.12 | -33.58 | $18.52 | +31.20 | -2.38 | -69.12 | -37.92 |
| 2026-09-01 | `TH` | 79 | $18.52 | $18.45 | -5.53 | — | +0.00 | -5.53 | -43.45 | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `MMED` | 85 | — | $23.88 | +0.00 | $23.84 | -3.40 | -3.40 | +0.00 | -3.40 |
| 2026-09-03 | `DE` | 2 | — | $703.25 | +0.00 | $694.41 | -17.68 | -17.68 | +0.00 | -17.68 |
| 2026-09-03 | `FRNM` | 127 | — | $15.87 | +0.00 | $16.90 | +130.81 | +130.81 | +0.00 | +130.81 |
| 2026-09-03 | `DELL` | 4 | — | $486.31 | +0.00 | $516.39 | +120.32 | +120.32 | +0.00 | +120.32 |
| 2026-09-03 | `CXW` | 62 | — | $32.31 | +0.00 | $33.66 | +83.70 | +83.70 | +0.00 | +83.70 |
| 2026-09-04 | `MMED` | 85 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -3.40 | — |
| 2026-09-04 | `DE` | 2 | $694.41 | $692.03 | -4.76 | — | +0.00 | -4.76 | -22.44 | — |
| 2026-09-04 | `FRNM` | 127 | $16.90 | $16.40 | -63.50 | $16.31 | -11.43 | -74.93 | +67.31 | +55.88 |
| 2026-09-04 | `DELL` | 4 | $516.39 | $513.78 | -10.44 | — | +0.00 | -10.44 | +109.88 | — |
| 2026-09-04 | `CXW` | 62 | $33.66 | $33.46 | -12.40 | — | +0.00 | -12.40 | +71.30 | — |
| 2026-09-04 | `BAK` | 4234 | — | $1.94 | +0.00 | $1.89 | -211.70 | -211.70 | +0.00 | -211.70 |
| 2026-09-08 | `FRNM` | 127 | $16.31 | $16.74 | +54.61 | — | +0.00 | +54.61 | +110.49 | — |
| 2026-09-08 | `BAK` | 4234 | $1.89 | $1.94 | +211.70 | — | +0.00 | +211.70 | +0.00 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

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
| 2026-08-24 | -5.17 | $113.67 | CRSP×21, AU×10, AUTL×517, FUTU×11, DE×2, MARA×109, BTDR×115, HIVE×394 | $10,146.19 | -58.84 | -35.17 | — | AU, AUTL, FUTU, DE, MARA, BTDR, HIVE | $8,889.71 | $10,088.28 | CRSP×21 |
| 2026-08-25 | +1.80 | $8,889.71 | CRSP×21 | $10,106.24 | +17.96 | +697.23 | RUM, EZPW, REAX, AU, FCX | CRSP | $57.82 | $10,790.13 | RUM×214, EZPW×57, REAX×83, AU×17, FCX×26 |
| 2026-08-26 | +2.02 | $57.82 | RUM×214, EZPW×57, REAX×83, AU×17, FCX×26 | $10,555.77 | -234.36 | +0.00 | — | RUM, EZPW, REAX, AU, FCX | $10,544.33 | $10,544.33 | — |
| 2026-08-27 | — | $10,544.33 | — | $10,544.33 | +0.00 | +0.00 | — | — | $10,544.33 | $10,544.33 | — |
| 2026-08-28 | +0.75 | $10,544.33 | — | $10,544.33 | +0.00 | -320.90 | SEDG, CAPR, SMTC, ERAS, BBWI, ZYME, TH | — | $126.57 | $10,208.01 | SEDG×45, CAPR×154, SMTC×10, ERAS×78, BBWI×80, ZYME×52, TH×79 |
| 2026-08-31 | -5.85 | $126.57 | SEDG×45, CAPR×154, SMTC×10, ERAS×78, BBWI×80, ZYME×52, TH×79 | $10,139.17 | -68.84 | +31.20 | — | SEDG, CAPR, SMTC, ERAS, BBWI, ZYME | $8,693.95 | $10,157.03 | TH×79 |
| 2026-09-01 | -6.30 | $8,693.95 | TH×79 | $10,151.50 | -5.53 | +0.00 | — | TH | $10,149.25 | $10,149.25 | — |
| 2026-09-02 | -3.83 | $10,149.25 | — | $10,149.25 | +0.00 | +0.00 | — | — | $10,149.25 | $10,149.25 | — |
| 2026-09-03 | -0.90 | $10,149.25 | — | $10,149.25 | +0.00 | +313.75 | MMED, DE, FRNM, DELL, CXW | — | $738.21 | $10,452.21 | MMED×85, DE×2, FRNM×127, DELL×4, CXW×62 |
| 2026-09-04 | +2.25 | $738.21 | MMED×85, DE×2, FRNM×127, DELL×4, CXW×62 | $10,361.11 | -91.10 | -223.13 | BAK | MMED, DE, DELL, CXW | $1.21 | $10,074.84 | FRNM×127, BAK×4234 |
| 2026-09-08 | -11.47 | $1.21 | FRNM×127, BAK×4234 | $10,341.15 | +266.31 | +0.00 | — | FRNM, BAK | $10,283.35 | $10,283.35 | — |
| 2026-09-09 | -13.95 | $10,283.35 | — | $10,283.35 | +0.00 | +0.00 | — | — | $10,283.35 | $10,283.35 | — |
| 2026-09-10 | -13.28 | $10,283.35 | — | $10,283.35 | +0.00 | +0.00 | — | — | $10,283.35 | $10,283.35 | — |

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
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $7,752.37 | — | combo gate; gate news=good,vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1278.38 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 517 | $2.47 | $6.67 | — | $6,468.71 | — | combo gate; gate news=good,vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1278.38 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 11 | $115.18 | $2.02 | — | $5,199.71 | — | combo gate; gate news=good,vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1278.38 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $3,951.19 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1278.38 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 109 | $11.70 | $2.32 | — | $2,673.57 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $1278.38 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 115 | $11.10 | $2.33 | — | $1,395.31 | — | combo gate; gate news=good,vol=good; list yday_gainer; ret5=+19.1; leftover $1278.38 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 394 | $3.24 | $5.08 | — | $113.67 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ret5=+21.3; leftover $1278.38 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $113.67 | ▲ close $10,205.03 vs 09:30 $10,233.88 (session +24.66) | 16:00 close · cash $113.67 · equity $10,205.03 vs 09:30 $10,233.88 (-28.85; session marks +24.66) · 8 name(s) marked open→close (per-name table). CRSP×21 09:30 $59.72 → close $59.50 -4.62; AU×10 09:30 $119.43 → close $121.22 +17.90; AUTL×517 09:30 $2.47 → close $2.41 -31.02; FUTU×11 09:30 $115.18 → close $123.64 +93.06; DE×2 09:30 $623.26 → close $647.47 +48.42; MARA×109 09:30 $11.70 → close $11.26 -47.96; BTDR×115 09:30 $11.10 → close $11.37 +31.62; HIVE×394 09:30 $3.24 → close $3.03 -82.74 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $113.67 | ▼ 09:30 equity $10,146.19 vs yday $10,205.03 (-58.84) | 09:30 open · cash $113.67 (unchanged overnight, no fees) · equity $10,146.19 vs prior close $10,205.03 (-58.84) · 8 name(s) re-marked at the open (per-name table). CRSP×21 yday $59.50 → 09:30 $58.75 -15.75; AU×10 yday $121.22 → 09:30 $120.51 -7.10; AUTL×517 yday $2.41 → 09:30 $2.40 -5.17; FUTU×11 yday $123.64 → 09:30 $121.00 -29.04; DE×2 yday $647.47 → 09:30 $653.04 +11.14; MARA×109 yday $11.26 → 09:30 $11.17 -9.81; BTDR×115 yday $11.37 → 09:30 $11.48 +12.65; HIVE×394 yday $3.03 → 09:30 $2.99 -15.76 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,316.73 | ▲ +6.74 after sell → book $10,144.15; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 517 | $2.40 | $6.77 | $-49.62 | $2,550.77 | ▼ -49.62 after sell → book $10,137.39; vs 09:30 mark -6.76 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 11 | $121.00 | $2.04 | $+59.95 | $3,879.72 | ▲ +59.95 after sell → book $10,135.34; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.04 | $2.02 | $+55.55 | $5,183.79 | ▲ +55.55 after sell → book $10,133.33; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 109 | $11.17 | $2.35 | $-62.43 | $6,398.97 | ▼ -62.43 after sell → book $10,130.98; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTDR` | 115 | $11.48 | $2.36 | $+39.58 | $7,716.81 | ▲ +39.58 after sell → book $10,128.62; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 394 | $2.99 | $5.16 | $-108.74 | $8,889.71 | ▼ -108.74 after sell → book $10,123.46; vs 09:30 mark -5.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,889.71 | ▼ close $10,088.28 vs 09:30 $10,146.19 (session -35.17) | 16:00 close · cash $8,889.71 · equity $10,088.28 vs 09:30 $10,146.19 (-57.91; session marks -35.17) · 1 name(s) marked open→close (per-name table). CRSP×21 09:30 $58.75 → close $57.08 -35.17 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,889.71 | ▲ 09:30 equity $10,106.24 vs yday $10,088.28 (+17.96) | 09:30 open · cash $8,889.71 (unchanged overnight, no fees) · equity $10,106.24 vs prior close $10,088.28 (+17.96) · 1 name(s) re-marked at the open (per-name table). CRSP×21 yday $57.08 → 09:30 $57.93 +17.95 | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 21 | $57.93 | $2.07 | $-20.93 | $10,104.17 | ▼ -20.93 after sell → book $10,104.17; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 214 | $9.42 | $2.76 | — | $8,085.53 | — | combo gate; gate news=good,vol=good; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $2020.83 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 57 | $35.05 | $2.16 | — | $6,085.51 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $2020.83 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 83 | $24.11 | $2.24 | — | $4,082.15 | — | combo gate; gate news=good,vol=good; list yday_mover; ret5=+891.7; leftover $2020.83 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 17 | $118.52 | $2.04 | — | $2,065.26 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $2020.83 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 26 | $77.13 | $2.07 | — | $57.82 | — | combo gate; gate news=good,vol=good; list mover_buy; ⚪; ret5=+13.8; leftover $2020.83 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.82 | ▲ close $10,790.13 vs 09:30 $10,106.24 (session +697.23) | 16:00 close · cash $57.82 · equity $10,790.13 vs 09:30 $10,106.24 (+683.89; session marks +697.23) · 5 name(s) marked open→close (per-name table). RUM×214 09:30 $9.42 → close $10.23 +173.34; EZPW×57 09:30 $35.05 → close $35.23 +10.26; REAX×83 09:30 $24.11 → close $28.43 +358.56; AU×17 09:30 $118.52 → close $123.39 +82.79; FCX×26 09:30 $77.13 → close $79.91 +72.28 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.82 | ▼ 09:30 equity $10,555.77 vs yday $10,790.13 (-234.36) | 09:30 open · cash $57.82 (unchanged overnight, no fees) · equity $10,555.77 vs prior close $10,790.13 (-234.36) · 5 name(s) re-marked at the open (per-name table). RUM×214 yday $10.23 → 09:30 $10.07 -34.24; EZPW×57 yday $35.23 → 09:30 $35.70 +26.79; REAX×83 yday $28.43 → 09:30 $26.61 -151.06; AU×17 yday $123.39 → 09:30 $119.80 -61.03; FCX×26 yday $79.91 → 09:30 $79.34 -14.82 | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 214 | $10.07 | $2.81 | $+133.53 | $2,209.98 | ▲ +133.53 after sell → book $10,552.95; vs 09:30 mark -2.82 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 57 | $35.70 | $2.19 | $+32.70 | $4,242.70 | ▲ +32.70 after sell → book $10,550.77; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 83 | $26.61 | $2.27 | $+202.99 | $6,449.06 | ▲ +202.99 after sell → book $10,548.50; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 17 | $119.80 | $2.07 | $+17.65 | $8,483.59 | ▲ +17.65 after sell → book $10,546.43; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 26 | $79.34 | $2.09 | $+53.30 | $10,544.33 | ▲ +53.30 after sell → book $10,544.33; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,544.33 | ▲ close $10,544.33 vs 09:30 $10,555.77 (session +0.00) | 16:00 close · cash $10,544.33 · no lots left · equity $10,544.33. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,544.33 | ▲ 09:30 equity $10,544.33 vs yday $10,544.33 (+0.00) | 09:30 open · cash $10,544.33 · no holdings · equity $10,544.33 vs prior close $10,544.33 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,544.33 | ▲ close $10,544.33 vs 09:30 $10,544.33 (session +0.00) | 16:00 close · cash $10,544.33 · no lots left · equity $10,544.33. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,544.33 | ▲ 09:30 equity $10,544.33 vs yday $10,544.33 (+0.00) | 09:30 open · cash $10,544.33 · no holdings · equity $10,544.33 vs prior close $10,544.33 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 45 | $32.90 | $2.12 | — | $9,061.71 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1506.33 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 154 | $9.73 | $2.45 | — | $7,560.84 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; ret5=+47.1; leftover $1506.33 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 10 | $141.76 | $2.02 | — | $6,141.22 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1506.33 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 78 | $19.25 | $2.22 | — | $4,637.49 | — | combo gate; gate news=good,vol=good; list yday_gainer; ret5=+14.1; leftover $1506.33 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 80 | $18.75 | $2.23 | — | $3,135.26 | — | combo gate; gate news=good,vol=good; list yday_gainer; ret5=-5.0; leftover $1506.33 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 52 | $28.91 | $2.15 | — | $1,629.80 | — | combo gate; gate news=good,vol=good; list yday_gainer; ret5=+9.2; leftover $1506.33 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 79 | $19.00 | $2.23 | — | $126.57 | — | combo gate; gate news=good,vol=good; list ohlc_hot; ret5=+7.5; leftover $1506.33 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $126.57 | ▼ close $10,208.01 vs 09:30 $10,544.33 (session -320.90) | 16:00 close · cash $126.57 · equity $10,208.01 vs 09:30 $10,544.33 (-336.32; session marks -320.90) · 7 name(s) marked open→close (per-name table). SEDG×45 09:30 $32.90 → close $31.41 -67.05; CAPR×154 09:30 $9.73 → close $9.59 -21.56; SMTC×10 09:30 $141.76 → close $131.17 -105.90; ERAS×78 09:30 $19.25 → close $18.03 -95.16; BBWI×80 09:30 $18.75 → close $19.22 +37.60; ZYME×52 09:30 $28.91 → close $28.27 -33.28; TH×79 09:30 $19.00 → close $18.55 -35.55 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $126.57 | ▼ 09:30 equity $10,139.17 vs yday $10,208.01 (-68.84) | 09:30 open · cash $126.57 (unchanged overnight, no fees) · equity $10,139.17 vs prior close $10,208.01 (-68.84) · 7 name(s) re-marked at the open (per-name table). SEDG×45 yday $31.41 → 09:30 $31.15 -11.70; CAPR×154 yday $9.59 → 09:30 $9.50 -13.86; SMTC×10 yday $131.17 → 09:30 $132.30 +11.30; ERAS×78 yday $18.03 → 09:30 $17.87 -12.48; BBWI×80 yday $19.22 → 09:30 $19.25 +2.40; ZYME×52 yday $28.27 → 09:30 $28.06 -10.92; TH×79 yday $18.55 → 09:30 $18.12 -33.58 | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 45 | $31.15 | $2.15 | $-83.02 | $1,526.17 | ▼ -83.02 after sell → book $10,137.03; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 154 | $9.50 | $2.49 | $-40.36 | $2,986.68 | ▼ -40.36 after sell → book $10,134.54; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 10 | $132.30 | $2.04 | $-98.66 | $4,307.64 | ▼ -98.66 after sell → book $10,132.50; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 78 | $17.87 | $2.25 | $-112.11 | $5,699.26 | ▼ -112.11 after sell → book $10,130.25; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 80 | $19.25 | $2.26 | $+35.51 | $7,237.00 | ▲ +35.51 after sell → book $10,127.99; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 52 | $28.06 | $2.17 | $-48.51 | $8,693.95 | ▼ -48.51 after sell → book $10,125.83; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,693.95 | ▲ close $10,157.03 vs 09:30 $10,139.17 (session +31.20) | 16:00 close · cash $8,693.95 · equity $10,157.03 vs 09:30 $10,139.17 (+17.86; session marks +31.20) · 1 name(s) marked open→close (per-name table). TH×79 09:30 $18.12 → close $18.52 +31.20 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,693.95 | ▼ 09:30 equity $10,151.50 vs yday $10,157.03 (-5.53) | 09:30 open · cash $8,693.95 (unchanged overnight, no fees) · equity $10,151.50 vs prior close $10,157.03 (-5.53) · 1 name(s) re-marked at the open (per-name table). TH×79 yday $18.52 → 09:30 $18.45 -5.53 | — |
| 2026-09-01 09:30 ET | **SELL** | `TH` | 79 | $18.45 | $2.25 | $-47.93 | $10,149.25 | ▼ -47.93 after sell → book $10,149.25; vs 09:30 mark -2.25 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,149.25 | ▲ close $10,149.25 vs 09:30 $10,151.50 (session +0.00) | 16:00 close · cash $10,149.25 · no lots left · equity $10,149.25. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,149.25 | ▲ 09:30 equity $10,149.25 vs yday $10,149.25 (+0.00) | 09:30 open · cash $10,149.25 · no holdings · equity $10,149.25 vs prior close $10,149.25 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,149.25 | ▲ close $10,149.25 vs 09:30 $10,149.25 (session +0.00) | 16:00 close · cash $10,149.25 · no lots left · equity $10,149.25. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,149.25 | ▲ 09:30 equity $10,149.25 vs yday $10,149.25 (+0.00) | 09:30 open · cash $10,149.25 · no holdings · equity $10,149.25 vs prior close $10,149.25 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 85 | $23.88 | $2.25 | — | $8,117.21 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $2029.85 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 2 | $703.25 | $2.00 | — | $6,708.71 | — | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $2029.85 | join🟡 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 127 | $15.87 | $2.37 | — | $4,690.85 | — | combo gate; gate news=good,vol=good; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $2029.85 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 4 | $486.31 | $2.00 | — | $2,743.61 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ret5=+6.1; leftover $2029.85 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 62 | $32.31 | $2.18 | — | $738.21 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $2029.85 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $738.21 | ▲ close $10,452.21 vs 09:30 $10,149.25 (session +313.75) | 16:00 close · cash $738.21 · equity $10,452.21 vs 09:30 $10,149.25 (+302.96; session marks +313.75) · 5 name(s) marked open→close (per-name table). MMED×85 09:30 $23.88 → close $23.84 -3.40; DE×2 09:30 $703.25 → close $694.41 -17.68; FRNM×127 09:30 $15.87 → close $16.90 +130.81; DELL×4 09:30 $486.31 → close $516.39 +120.32; CXW×62 09:30 $32.31 → close $33.66 +83.70 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $738.21 | ▼ 09:30 equity $10,361.11 vs yday $10,452.21 (-91.10) | 09:30 open · cash $738.21 (unchanged overnight, no fees) · equity $10,361.11 vs prior close $10,452.21 (-91.10) · 5 name(s) re-marked at the open (per-name table). MMED×85 yday $23.84 → 09:30 $23.84 +0.00; DE×2 yday $694.41 → 09:30 $692.03 -4.76; FRNM×127 yday $16.90 → 09:30 $16.40 -63.50; DELL×4 yday $516.39 → 09:30 $513.78 -10.44; CXW×62 yday $33.66 → 09:30 $33.46 -12.40 | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 85 | $23.84 | $2.28 | $-7.92 | $2,762.34 | ▼ -7.92 after sell → book $10,358.84; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 2 | $692.03 | $2.02 | $-26.45 | $4,144.38 | ▼ -26.45 after sell → book $10,356.82; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 4 | $513.78 | $2.03 | $+105.85 | $6,197.47 | ▲ +105.85 after sell → book $10,354.79; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 62 | $33.46 | $2.20 | $+66.92 | $8,269.79 | ▲ +66.92 after sell → book $10,352.59; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 4234 | $1.94 | $54.62 | — | $1.21 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; ret5=+18.3; leftover $8269.79 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.21 | ▼ close $10,074.84 vs 09:30 $10,361.11 (session -223.13) | 16:00 close · cash $1.21 · equity $10,074.84 vs 09:30 $10,361.11 (-286.27; session marks -223.13) · 2 name(s) marked open→close (per-name table). FRNM×127 09:30 $16.40 → close $16.31 -11.43; BAK×4234 09:30 $1.94 → close $1.89 -211.70 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.21 | ▲ 09:30 equity $10,341.15 vs yday $10,074.84 (+266.31) | 09:30 open · cash $1.21 (unchanged overnight, no fees) · equity $10,341.15 vs prior close $10,074.84 (+266.31) · 2 name(s) re-marked at the open (per-name table). FRNM×127 yday $16.31 → 09:30 $16.74 +54.61; BAK×4234 yday $1.89 → 09:30 $1.94 +211.70 | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 127 | $16.74 | $2.41 | $+105.71 | $2,124.78 | ▲ +105.71 after sell → book $10,338.74; vs 09:30 mark -2.41 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 4234 | $1.94 | $55.39 | $-110.01 | $10,283.35 | ▼ -110.01 after sell → book $10,283.35; vs 09:30 mark -55.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,283.35 | ▲ close $10,283.35 vs 09:30 $10,341.15 (session +0.00) | 16:00 close · cash $10,283.35 · no lots left · equity $10,283.35. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,283.35 | ▲ 09:30 equity $10,283.35 vs yday $10,283.35 (+0.00) | 09:30 open · cash $10,283.35 · no holdings · equity $10,283.35 vs prior close $10,283.35 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,283.35 | ▲ close $10,283.35 vs 09:30 $10,283.35 (session +0.00) | 16:00 close · cash $10,283.35 · no lots left · equity $10,283.35. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,283.35 | ▲ 09:30 equity $10,283.35 vs yday $10,283.35 (+0.00) | 09:30 open · cash $10,283.35 · no holdings · equity $10,283.35 vs prior close $10,283.35 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,283.35 | ▲ close $10,283.35 vs 09:30 $10,283.35 (session +0.00) | 16:00 close · cash $10,283.35 · no lots left · equity $10,283.35. | — |

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
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MSTR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OCGN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `WLTH` | hard_red | hard-red S=-13.28 sit; no new buys |
