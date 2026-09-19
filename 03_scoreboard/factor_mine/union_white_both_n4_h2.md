# Factor mine action — `union_white_both_n4_h2`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **2** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `list` · size `leftover` · sell `list` · S-boost `none` · −0 red + yday up AND catalyst, top 4 by Score

Cash book **+15.86%** ($11,586) · signal-only (no cash/fees) was +14.76%. Starts YES **10/26**. Fills 68 · skips 39 · realized $+1842.30.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 4 names, spend leftover cash on whole shares, and hold at least 2 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: the morning-board Score (100 minus list rank) — only after the pool is chosen.
- Must-have: at most 0 red cameras (the −R half of +G −R; 🚨 is not counted here).
- Must-have: yesterday's session was up AND a major good catalyst.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Sort the keepers by the morning-board Score (100 minus list rank) — only after the pool is chosen and keep the top 4.
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- This is a LONG sleeve: it buys shares and wants the price to go up.

### When it sells

- Sell first, then buy. Never sell a ticker we do not hold.
- Minimum hold is 2 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 2 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `cam_bad_max=0,yday_and_catalyst=True` · **rank** `list` · **top_n** 4.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **2**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $225.94.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `BTSG` | 83 | — | $59.80 | +0.00 | $60.23 | +35.69 | +35.69 | +0.00 | +35.69 |
| 2026-08-13 | `TPG` | 98 | — | $50.62 | +0.00 | $54.62 | +391.69 | +391.69 | +0.00 | +391.69 |
| 2026-08-14 | `BTSG` | 83 | $60.23 | $59.65 | -48.14 | $61.71 | +170.98 | +122.84 | -12.45 | +158.53 |
| 2026-08-14 | `TPG` | 98 | $54.62 | $55.29 | +65.66 | $53.03 | -221.48 | -155.82 | +457.35 | +235.87 |
| 2026-08-14 | `BETR` | 1 | — | $14.80 | +0.00 | $13.73 | -1.07 | -1.07 | +0.00 | -1.07 |
| 2026-08-14 | `ANGX` | 4 | — | $4.31 | +0.00 | $4.37 | +0.24 | +0.24 | +0.00 | +0.24 |
| 2026-08-17 | `BTSG` | 83 | $61.71 | $61.69 | -1.66 | — | +0.00 | -1.66 | +156.87 | — |
| 2026-08-17 | `TPG` | 98 | $53.03 | $52.67 | -35.28 | — | +0.00 | -35.28 | +200.59 | — |
| 2026-08-17 | `BETR` | 1 | $13.73 | $13.67 | -0.06 | $13.54 | -0.13 | -0.19 | -1.13 | -1.26 |
| 2026-08-17 | `ANGX` | 4 | $4.37 | $4.60 | +0.92 | $4.71 | +0.44 | +1.36 | +1.16 | +1.60 |
| 2026-08-17 | `ABX` | 282 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `ALM` | 159 | — | $16.20 | +0.00 | $16.36 | +25.44 | +25.44 | +0.00 | +25.44 |
| 2026-08-17 | `NMAX` | 235 | — | $10.97 | +0.00 | $10.36 | -143.35 | -143.35 | +0.00 | -143.35 |
| 2026-08-17 | `AAOI` | 16 | — | $152.64 | +0.00 | $154.89 | +36.00 | +36.00 | +0.00 | +36.00 |
| 2026-08-18 | `BETR` | 1 | $13.54 | $13.21 | -0.33 | — | +0.00 | -0.33 | -1.59 | — |
| 2026-08-18 | `ANGX` | 4 | $4.71 | $4.79 | +0.32 | — | +0.00 | +0.32 | +1.92 | — |
| 2026-08-18 | `ABX` | 282 | $9.12 | $9.03 | -25.38 | $9.01 | -5.64 | -31.02 | -25.38 | -31.02 |
| 2026-08-18 | `ALM` | 159 | $16.36 | $15.78 | -92.22 | $15.60 | -28.62 | -120.84 | -66.78 | -95.40 |
| 2026-08-18 | `NMAX` | 235 | $10.36 | $10.31 | -11.75 | $11.43 | +263.20 | +251.45 | -155.10 | +108.10 |
| 2026-08-18 | `AAOI` | 16 | $154.89 | $146.20 | -139.04 | $131.41 | -236.64 | -375.68 | -103.04 | -339.68 |
| 2026-08-19 | `ABX` | 282 | $9.01 | $9.08 | +19.74 | — | +0.00 | +19.74 | -11.28 | — |
| 2026-08-19 | `ALM` | 159 | $15.60 | $16.05 | +71.55 | — | +0.00 | +71.55 | -23.85 | — |
| 2026-08-19 | `NMAX` | 235 | $11.43 | $11.50 | +16.45 | — | +0.00 | +16.45 | +124.55 | — |
| 2026-08-19 | `AAOI` | 16 | $131.41 | $135.85 | +71.04 | — | +0.00 | +71.04 | -268.64 | — |
| 2026-08-20 | `BHP` | 27 | — | $91.01 | +0.00 | $93.63 | +70.74 | +70.74 | +0.00 | +70.74 |
| 2026-08-20 | `KGC` | 85 | — | $29.63 | +0.00 | $31.43 | +153.00 | +153.00 | +0.00 | +153.00 |
| 2026-08-20 | `WPM` | 17 | — | $144.54 | +0.00 | $150.25 | +97.07 | +97.07 | +0.00 | +97.07 |
| 2026-08-20 | `CYPH` | 2205 | — | $1.15 | +0.00 | $1.19 | +88.20 | +88.20 | +0.00 | +88.20 |
| 2026-08-21 | `BHP` | 27 | $93.63 | $95.72 | +56.43 | $97.03 | +35.37 | +91.80 | +127.17 | +162.54 |
| 2026-08-21 | `KGC` | 85 | $31.43 | $32.17 | +62.90 | $32.76 | +50.15 | +113.05 | +215.90 | +266.05 |
| 2026-08-21 | `WPM` | 17 | $150.25 | $154.70 | +75.65 | $157.78 | +52.36 | +128.01 | +172.72 | +225.08 |
| 2026-08-21 | `CYPH` | 2205 | $1.19 | $1.32 | +286.65 | $1.42 | +220.50 | +507.15 | +374.85 | +595.35 |
| 2026-08-21 | `AUPH` | 2 | — | $17.20 | +0.00 | $16.65 | -1.10 | -1.10 | +0.00 | -1.10 |
| 2026-08-21 | `ARCT` | 4 | — | $11.13 | +0.00 | $13.45 | +9.28 | +9.28 | +0.00 | +9.28 |
| 2026-08-24 | `BHP` | 27 | $97.03 | $97.31 | +7.56 | — | +0.00 | +7.56 | +170.10 | — |
| 2026-08-24 | `KGC` | 85 | $32.76 | $33.03 | +22.95 | — | +0.00 | +22.95 | +289.00 | — |
| 2026-08-24 | `WPM` | 17 | $157.78 | $159.50 | +29.24 | — | +0.00 | +29.24 | +254.32 | — |
| 2026-08-24 | `CYPH` | 2205 | $1.42 | $1.83 | +904.05 | — | +0.00 | +904.05 | +1499.40 | — |
| 2026-08-24 | `AUPH` | 2 | $16.65 | $16.57 | -0.16 | $16.57 | +0.00 | -0.16 | -1.26 | -1.26 |
| 2026-08-24 | `ARCT` | 4 | $13.45 | $13.33 | -0.48 | $14.34 | +4.04 | +3.56 | +8.80 | +12.84 |
| 2026-08-25 | `AUPH` | 2 | $16.57 | $16.63 | +0.12 | — | +0.00 | +0.12 | -1.14 | — |
| 2026-08-25 | `ARCT` | 4 | $14.34 | $14.12 | -0.88 | — | +0.00 | -0.88 | +11.96 | — |
| 2026-08-25 | `CRMD` | 368 | — | $8.35 | +0.00 | $8.56 | +77.28 | +77.28 | +0.00 | +77.28 |
| 2026-08-25 | `BMEA` | 1886 | — | $1.63 | +0.00 | $1.73 | +188.60 | +188.60 | +0.00 | +188.60 |
| 2026-08-25 | `CYPH` | 1970 | — | $1.56 | +0.00 | $1.64 | +157.60 | +157.60 | +0.00 | +157.60 |
| 2026-08-25 | `EZPW` | 86 | — | $35.05 | +0.00 | $35.23 | +15.48 | +15.48 | +0.00 | +15.48 |
| 2026-08-26 | `CRMD` | 368 | $8.56 | $8.60 | +14.72 | $8.39 | -77.28 | -62.56 | +92.00 | +14.72 |
| 2026-08-26 | `BMEA` | 1886 | $1.73 | $1.75 | +47.15 | $1.71 | -84.87 | -37.72 | +235.75 | +150.88 |
| 2026-08-26 | `CYPH` | 1970 | $1.64 | $1.60 | -78.80 | $1.63 | +59.10 | -19.70 | +78.80 | +137.90 |
| 2026-08-26 | `EZPW` | 86 | $35.23 | $35.70 | +40.42 | $33.90 | -154.80 | -114.38 | +55.90 | -98.90 |
| 2026-08-27 | `CRMD` | 368 | $8.39 | $8.49 | +36.80 | — | +0.00 | +36.80 | +51.52 | — |
| 2026-08-27 | `BMEA` | 1886 | $1.71 | $1.74 | +56.58 | — | +0.00 | +56.58 | +207.46 | — |
| 2026-08-27 | `CYPH` | 1970 | $1.63 | $1.75 | +236.40 | — | +0.00 | +236.40 | +374.30 | — |
| 2026-08-27 | `EZPW` | 86 | $33.90 | $33.50 | -34.40 | — | +0.00 | -34.40 | -133.30 | — |
| 2026-08-28 | `SMTC` | 22 | — | $141.76 | +0.00 | $131.17 | -232.98 | -232.98 | +0.00 | -232.98 |
| 2026-08-28 | `TTMI` | 25 | — | $122.81 | +0.00 | $118.65 | -104.00 | -104.00 | +0.00 | -104.00 |
| 2026-08-28 | `KEYS` | 9 | — | $324.41 | +0.00 | $319.97 | -39.96 | -39.96 | +0.00 | -39.96 |
| 2026-08-28 | `AVT` | 34 | — | $91.49 | +0.00 | $88.63 | -97.24 | -97.24 | +0.00 | -97.24 |
| 2026-08-31 | `SMTC` | 22 | $131.17 | $132.30 | +24.86 | $132.96 | +14.52 | +39.38 | -208.12 | -193.60 |
| 2026-08-31 | `TTMI` | 25 | $118.65 | $118.83 | +4.50 | $118.92 | +2.25 | +6.75 | -99.50 | -97.25 |
| 2026-08-31 | `KEYS` | 9 | $319.97 | $322.49 | +22.68 | $322.70 | +1.89 | +24.57 | -17.28 | -15.39 |
| 2026-08-31 | `AVT` | 34 | $88.63 | $89.39 | +25.84 | $89.56 | +5.78 | +31.62 | -71.40 | -65.62 |
| 2026-09-01 | `SMTC` | 22 | $132.96 | $127.63 | -117.26 | — | +0.00 | -117.26 | -310.86 | — |
| 2026-09-01 | `TTMI` | 25 | $118.92 | $116.68 | -56.00 | — | +0.00 | -56.00 | -153.25 | — |
| 2026-09-01 | `KEYS` | 9 | $322.70 | $321.47 | -11.07 | — | +0.00 | -11.07 | -26.46 | — |
| 2026-09-01 | `AVT` | 34 | $89.56 | $88.58 | -33.32 | — | +0.00 | -33.32 | -98.94 | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 57 | — | $52.88 | +0.00 | $52.46 | -23.94 | -23.94 | +0.00 | -23.94 |
| 2026-09-03 | `HRMY` | 70 | — | $42.93 | +0.00 | $41.86 | -74.90 | -74.90 | +0.00 | -74.90 |
| 2026-09-03 | `CABA` | 831 | — | $3.63 | +0.00 | $3.48 | -124.65 | -124.65 | +0.00 | -124.65 |
| 2026-09-03 | `VSTM` | 376 | — | $8.03 | +0.00 | $7.98 | -18.80 | -18.80 | +0.00 | -18.80 |
| 2026-09-04 | `ATRC` | 57 | $52.46 | $52.03 | -24.51 | $51.52 | -29.07 | -53.58 | -48.45 | -77.52 |
| 2026-09-04 | `HRMY` | 70 | $41.86 | $41.50 | -25.20 | $42.25 | +52.50 | +27.30 | -100.10 | -47.60 |
| 2026-09-04 | `CABA` | 831 | $3.48 | $3.46 | -16.62 | $3.47 | +8.31 | -8.31 | -141.27 | -132.96 |
| 2026-09-04 | `VSTM` | 376 | $7.98 | $7.91 | -26.32 | $8.20 | +109.04 | +82.72 | -45.12 | +63.92 |
| 2026-09-08 | `ATRC` | 57 | $51.52 | $54.31 | +159.03 | — | +0.00 | +159.03 | +81.51 | — |
| 2026-09-08 | `HRMY` | 70 | $42.25 | $42.20 | -3.50 | — | +0.00 | -3.50 | -51.10 | — |
| 2026-09-08 | `CABA` | 831 | $3.47 | $3.43 | -33.24 | — | +0.00 | -33.24 | -166.20 | — |
| 2026-09-08 | `VSTM` | 376 | $8.20 | $8.20 | +0.00 | — | +0.00 | +0.00 | +63.92 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `BAND` | 75 | — | $52.55 | +0.00 | $56.87 | +324.00 | +324.00 | +0.00 | +324.00 |
| 2026-09-11 | `PAYP` | 217 | — | $18.30 | +0.00 | $18.45 | +32.55 | +32.55 | +0.00 | +32.55 |
| 2026-09-11 | `SEDG` | 108 | — | $36.78 | +0.00 | $34.68 | -226.80 | -226.80 | +0.00 | -226.80 |
| 2026-09-14 | `BAND` | 75 | $56.87 | $56.90 | +2.25 | $48.97 | -594.75 | -592.50 | +326.25 | -268.50 |
| 2026-09-14 | `PAYP` | 217 | $18.45 | $18.28 | -36.89 | $18.68 | +86.80 | +49.91 | -4.34 | +82.46 |
| 2026-09-14 | `SEDG` | 108 | $34.68 | $33.64 | -112.32 | $35.33 | +182.52 | +70.20 | -339.12 | -156.60 |
| 2026-09-15 | `BAND` | 75 | $48.97 | $49.51 | +40.50 | — | +0.00 | +40.50 | -228.00 | — |
| 2026-09-15 | `PAYP` | 217 | $18.68 | $18.30 | -82.46 | — | +0.00 | -82.46 | +0.00 | — |
| 2026-09-15 | `SEDG` | 108 | $35.33 | $35.24 | -9.72 | — | +0.00 | -9.72 | -166.32 | — |
| 2026-09-16 | `SWKS` | 64 | — | $89.38 | +0.00 | $85.59 | -242.56 | -242.56 | +0.00 | -242.56 |
| 2026-09-16 | `QRVO` | 48 | — | $118.18 | +0.00 | $113.97 | -202.08 | -202.08 | +0.00 | -202.08 |
| 2026-09-17 | `SWKS` | 64 | $85.59 | $86.76 | +74.88 | $91.32 | +291.84 | +366.72 | -167.68 | +124.16 |
| 2026-09-17 | `QRVO` | 48 | $113.97 | $114.90 | +44.64 | $119.51 | +221.28 | +265.92 | -157.44 | +63.84 |
| 2026-09-17 | `VOD` | 4 | — | $17.56 | +0.00 | $17.52 | -0.16 | -0.16 | +0.00 | -0.16 |
| 2026-09-17 | `ASAN` | 8 | — | $9.55 | +0.00 | $10.09 | +4.32 | +4.32 | +0.00 | +4.32 |
| 2026-09-18 | `SWKS` | 64 | $91.32 | $92.05 | +46.72 | — | +0.00 | +46.72 | +170.88 | — |
| 2026-09-18 | `QRVO` | 48 | $119.51 | $120.76 | +60.00 | — | +0.00 | +60.00 | +123.84 | — |
| 2026-09-18 | `VOD` | 4 | $17.52 | $16.73 | -3.16 | $16.95 | +0.88 | -2.28 | -3.32 | -2.44 |
| 2026-09-18 | `ASAN` | 8 | $10.09 | $10.09 | +0.00 | $9.51 | -4.64 | -4.64 | +4.32 | -0.32 |
| 2026-09-18 | `ILMN` | 11 | — | $249.13 | +0.00 | $239.62 | -104.61 | -104.61 | +0.00 | -104.61 |
| 2026-09-18 | `SDGR` | 99 | — | $29.32 | +0.00 | $29.02 | -29.70 | -29.70 | +0.00 | -29.70 |
| 2026-09-18 | `ARQT` | 111 | — | $26.14 | +0.00 | $25.38 | -84.36 | -84.36 | +0.00 | -84.36 |
| 2026-09-18 | `FTRE` | 145 | — | $20.10 | +0.00 | $19.93 | -24.65 | -24.65 | +0.00 | -24.65 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +427.38 | BTSG, TPG | — | $71.00 | $10,422.85 | BTSG×83, TPG×98 |
| 2026-08-14 | +5.50 | $71.00 | BTSG×83, TPG×98 | $10,440.37 | +17.52 | -51.33 | BETR, ANGX | — | $38.63 | $10,388.71 | BTSG×83, TPG×98, BETR×1, ANGX×4 |
| 2026-08-17 | +2.25 | $38.63 | BTSG×83, TPG×98, BETR×1, ANGX×4 | $10,352.63 | -36.08 | -81.60 | ABX, ALM, NMAX, AAOI | BTSG, TPG | $136.92 | $10,255.22 | BETR×1, ANGX×4, ABX×282, ALM×159, NMAX×235, AAOI×16 |
| 2026-08-18 | -6.20 | $136.92 | BETR×1, ANGX×4, ABX×282, ALM×159, NMAX×235, AAOI×16 | $9,986.82 | -268.40 | -7.70 | — | BETR, ANGX | $168.91 | $9,978.74 | ABX×282, ALM×159, NMAX×235, AAOI×16 |
| 2026-08-19 | -7.20 | $168.91 | ABX×282, ALM×159, NMAX×235, AAOI×16 | $10,157.52 | +178.78 | +0.00 | — | ABX, ALM, NMAX, AAOI | $10,146.14 | $10,146.14 | — |
| 2026-08-20 | +1.12 | $10,146.14 | — | $10,146.14 | +0.00 | +409.01 | BHP, KGC, WPM, CYPH | — | $142.59 | $10,520.35 | BHP×27, KGC×85, WPM×17, CYPH×2205 |
| 2026-08-21 | +3.25 | $142.59 | BHP×27, KGC×85, WPM×17, CYPH×2205 | $11,001.98 | +481.63 | +366.56 | AUPH, ARCT | — | $62.86 | $11,367.73 | BHP×27, KGC×85, WPM×17, CYPH×2205, AUPH×2, ARCT×4 |
| 2026-08-24 | -5.17 | $62.86 | BHP×27, KGC×85, WPM×17, CYPH×2205, AUPH×2, ARCT×4 | $12,330.89 | +963.16 | +4.04 | — | BHP, KGC, WPM, CYPH | $12,209.14 | $12,299.64 | AUPH×2, ARCT×4 |
| 2026-08-25 | +1.80 | $12,209.14 | AUPH×2, ARCT×4 | $12,298.88 | -0.76 | +438.96 | CRMD, BMEA, CYPH, EZPW | AUPH, ARCT | $6.70 | $12,680.14 | CRMD×368, BMEA×1886, CYPH×1970, EZPW×86 |
| 2026-08-26 | +2.02 | $6.70 | CRMD×368, BMEA×1886, CYPH×1970, EZPW×86 | $12,703.63 | +23.49 | -257.85 | — | — | $6.70 | $12,445.78 | CRMD×368, BMEA×1886, CYPH×1970, EZPW×86 |
| 2026-08-27 | — | $6.70 | CRMD×368, BMEA×1886, CYPH×1970, EZPW×86 | $12,741.16 | +295.38 | +0.00 | — | CRMD, BMEA, CYPH, EZPW | $12,683.61 | $12,683.61 | — |
| 2026-08-28 | +0.75 | $12,683.61 | — | $12,683.61 | -0.00 | -474.18 | SMTC, TTMI, KEYS, AVT | — | $456.06 | $12,201.20 | SMTC×22, TTMI×25, KEYS×9, AVT×34 |
| 2026-08-31 | -5.85 | $456.06 | SMTC×22, TTMI×25, KEYS×9, AVT×34 | $12,279.08 | +77.88 | +24.44 | — | — | $456.06 | $12,303.52 | SMTC×22, TTMI×25, KEYS×9, AVT×34 |
| 2026-09-01 | -6.30 | $456.06 | SMTC×22, TTMI×25, KEYS×9, AVT×34 | $12,085.87 | -217.65 | +0.00 | — | SMTC, TTMI, KEYS, AVT | $12,077.50 | $12,077.50 | — |
| 2026-09-02 | -3.83 | $12,077.50 | — | $12,077.50 | +0.00 | +0.00 | — | — | $12,077.50 | $12,077.50 | — |
| 2026-09-03 | -0.90 | $12,077.50 | — | $12,077.50 | +0.00 | -242.29 | ATRC, HRMY, CABA, VSTM | — | $2.50 | $11,815.28 | ATRC×57, HRMY×70, CABA×831, VSTM×376 |
| 2026-09-04 | +2.25 | $2.50 | ATRC×57, HRMY×70, CABA×831, VSTM×376 | $11,722.63 | -92.65 | +140.78 | — | — | $2.50 | $11,863.41 | ATRC×57, HRMY×70, CABA×831, VSTM×376 |
| 2026-09-08 | -11.47 | $2.50 | ATRC×57, HRMY×70, CABA×831, VSTM×376 | $11,985.70 | +122.29 | +0.00 | — | ATRC, HRMY, CABA, VSTM | $11,965.45 | $11,965.45 | — |
| 2026-09-09 | -13.95 | $11,965.45 | — | $11,965.45 | +0.00 | +0.00 | — | — | $11,965.45 | $11,965.45 | — |
| 2026-09-10 | -13.28 | $11,965.45 | — | $11,965.45 | +0.00 | +0.00 | — | — | $11,965.45 | $11,965.45 | — |
| 2026-09-11 | +0.50 | $11,965.45 | — | $11,965.45 | +0.00 | +129.75 | BAND, PAYP, SEDG | — | $73.54 | $12,087.88 | BAND×75, PAYP×217, SEDG×108 |
| 2026-09-14 | -11.00 | $73.54 | BAND×75, PAYP×217, SEDG×108 | $11,940.92 | -146.96 | -325.43 | — | — | $73.54 | $11,615.49 | BAND×75, PAYP×217, SEDG×108 |
| 2026-09-15 | -3.84 | $73.54 | BAND×75, PAYP×217, SEDG×108 | $11,563.81 | -51.68 | +0.00 | — | BAND, PAYP, SEDG | $11,556.32 | $11,556.32 | — |
| 2026-09-16 | +5.30 | $11,556.32 | — | $11,556.32 | -0.00 | -444.64 | SWKS, QRVO | — | $159.04 | $11,107.36 | SWKS×64, QRVO×48 |
| 2026-09-17 | +7.38 | $159.04 | SWKS×64, QRVO×48 | $11,226.88 | +119.52 | +517.28 | VOD, ASAN | — | $10.90 | $11,742.66 | SWKS×64, QRVO×48, VOD×4, ASAN×8 |
| 2026-09-18 | +4.86 | $10.90 | SWKS×64, QRVO×48, VOD×4, ASAN×8 | $11,846.22 | +103.56 | -247.08 | ILMN, SDGR, ARQT, FTRE | SWKS, QRVO | $225.94 | $11,585.65 | VOD×4, ASAN×8, ILMN×11, SDGR×99, ARQT×111, FTRE×145 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 83 | $59.80 | $2.24 | — | $5,034.36 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; ⚪; ret5=-5.3; leftover $5000.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 98 | $50.62 | $2.28 | — | $71.00 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; ⚪; ret5=+6.2; leftover $5000.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $71.00 | ▲ close $10,422.85 vs 09:30 $10,000.00 (session +427.38) | 16:00 close · cash $71.00 · equity $10,422.85 vs 09:30 $10,000.00 (+422.85; session marks +427.38) · 2 name(s) marked open→close (per-name table). BTSG×83 09:30 $59.80 → close $60.23 +35.69; TPG×98 09:30 $50.62 → close $54.62 +391.69 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $71.00 | ▲ 09:30 equity $10,440.37 vs yday $10,422.85 (+17.52) | 09:30 open · cash $71.00 (unchanged overnight, no fees) · equity $10,440.37 vs prior close $10,422.85 (+17.52) · 2 name(s) re-marked at the open (per-name table). BTSG×83 yday $60.23 → 09:30 $59.65 -48.14; TPG×98 yday $54.62 → 09:30 $55.29 +65.66 | — |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 1 | $14.80 | $0.15 | — | $56.05 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=-9.9; leftover $17.75 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 4 | $4.31 | $0.18 | — | $38.63 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $17.75 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $38.63 | ▼ close $10,388.71 vs 09:30 $10,440.37 (session -51.33) | 16:00 close · cash $38.63 · equity $10,388.71 vs 09:30 $10,440.37 (-51.66; session marks -51.33) · 4 name(s) marked open→close (per-name table). BTSG×83 09:30 $59.65 → close $61.71 +170.98; TPG×98 09:30 $55.29 → close $53.03 -221.48; BETR×1 09:30 $14.80 → close $13.73 -1.07; ANGX×4 09:30 $4.31 → close $4.37 +0.24 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $38.63 | ▼ 09:30 equity $10,352.63 vs yday $10,388.71 (-36.08) | 09:30 open · cash $38.63 (unchanged overnight, no fees) · equity $10,352.63 vs prior close $10,388.71 (-36.08) · 4 name(s) re-marked at the open (per-name table). BTSG×83 yday $61.71 → 09:30 $61.69 -1.66; TPG×98 yday $53.03 → 09:30 $52.67 -35.28; BETR×1 yday $13.73 → 09:30 $13.67 -0.06; ANGX×4 yday $4.37 → 09:30 $4.60 +0.92 | — |
| 2026-08-17 09:30 ET | **SELL** | `BTSG` | 83 | $61.69 | $2.29 | $+152.34 | $5,156.60 | ▲ +152.34 after sell → book $10,350.33; vs 09:30 mark -2.30 | dropped from list after 2 sess (min 2) | — |
| 2026-08-17 09:30 ET | **SELL** | `TPG` | 98 | $52.67 | $2.34 | $+195.96 | $10,315.92 | ▲ +195.96 after sell → book $10,347.99; vs 09:30 mark -2.34 | dropped from list after 2 sess (min 2) | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 282 | $9.12 | $3.64 | — | $7,740.44 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $2578.98 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 159 | $16.20 | $2.47 | — | $5,162.18 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $2578.98 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NMAX` | 235 | $10.97 | $3.03 | — | $2,581.20 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover,oppset; ⚪; ret5=+21.2; leftover $2578.98 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `AAOI` | 16 | $152.64 | $2.04 | — | $136.92 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+10.8; leftover $2578.98 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $136.92 | ▼ close $10,255.22 vs 09:30 $10,352.63 (session -81.60) | 16:00 close · cash $136.92 · equity $10,255.22 vs 09:30 $10,352.63 (-97.41; session marks -81.60) · 6 name(s) marked open→close (per-name table). BETR×1 09:30 $13.67 → close $13.54 -0.13; ANGX×4 09:30 $4.60 → close $4.71 +0.44; ABX×282 09:30 $9.12 → close $9.12 +0.00; ALM×159 09:30 $16.20 → close $16.36 +25.44; NMAX×235 09:30 $10.97 → close $10.36 -143.35; AAOI×16 09:30 $152.64 → close $154.89 +36.00 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $136.92 | ▼ 09:30 equity $9,986.82 vs yday $10,255.22 (-268.40) | 09:30 open · cash $136.92 (unchanged overnight, no fees) · equity $9,986.82 vs prior close $10,255.22 (-268.40) · 6 name(s) re-marked at the open (per-name table). BETR×1 yday $13.54 → 09:30 $13.21 -0.33; ANGX×4 yday $4.71 → 09:30 $4.79 +0.32; ABX×282 yday $9.12 → 09:30 $9.03 -25.38; ALM×159 yday $16.36 → 09:30 $15.78 -92.22; NMAX×235 yday $10.36 → 09:30 $10.31 -11.75; AAOI×16 yday $154.89 → 09:30 $146.20 -139.04 | — |
| 2026-08-18 09:30 ET | **SELL** | `BETR` | 1 | $13.21 | $0.16 | $-1.90 | $149.97 | ▼ -1.90 after sell → book $9,986.66; vs 09:30 mark -0.16 | dropped from list after 2 sess (min 2) | — |
| 2026-08-18 09:30 ET | **SELL** | `ANGX` | 4 | $4.79 | $0.22 | $+1.51 | $168.91 | ▲ +1.51 after sell → book $9,986.44; vs 09:30 mark -0.22 | dropped from list after 2 sess (min 2) | join🔴 sector🔴 gen🔴 news🟢 vol🟢 buy🟡 |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $168.91 | ▼ close $9,978.74 vs 09:30 $9,986.82 (session -7.70) | 16:00 close · cash $168.91 · equity $9,978.74 vs 09:30 $9,986.82 (-8.08; session marks -7.70) · 4 name(s) marked open→close (per-name table). ABX×282 09:30 $9.03 → close $9.01 -5.64; ALM×159 09:30 $15.78 → close $15.60 -28.62; NMAX×235 09:30 $10.31 → close $11.43 +263.20; AAOI×16 09:30 $146.20 → close $131.41 -236.64 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $168.91 | ▲ 09:30 equity $10,157.52 vs yday $9,978.74 (+178.78) | 09:30 open · cash $168.91 (unchanged overnight, no fees) · equity $10,157.52 vs prior close $9,978.74 (+178.78) · 4 name(s) re-marked at the open (per-name table). ABX×282 yday $9.01 → 09:30 $9.08 +19.74; ALM×159 yday $15.60 → 09:30 $16.05 +71.55; NMAX×235 yday $11.43 → 09:30 $11.50 +16.45; AAOI×16 yday $131.41 → 09:30 $135.85 +71.04 | — |
| 2026-08-19 09:30 ET | **SELL** | `ABX` | 282 | $9.08 | $3.71 | $-18.62 | $2,725.76 | ▼ -18.62 after sell → book $10,153.81; vs 09:30 mark -3.71 | dropped from list after 2 sess (min 2) | — |
| 2026-08-19 09:30 ET | **SELL** | `ALM` | 159 | $16.05 | $2.51 | $-28.83 | $5,275.20 | ▼ -28.83 after sell → book $10,151.30; vs 09:30 mark -2.51 | dropped from list after 2 sess (min 2) | — |
| 2026-08-19 09:30 ET | **SELL** | `NMAX` | 235 | $11.50 | $3.09 | $+118.43 | $7,974.61 | ▲ +118.43 after sell → book $10,148.21; vs 09:30 mark -3.09 | dropped from list after 2 sess (min 2) | join🔴 sector🟡 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `AAOI` | 16 | $135.85 | $2.07 | $-272.74 | $10,146.14 | ▼ -272.74 after sell → book $10,146.14; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 2) | join🔴 sector🟡 gen🔴 news🟡 peer🔴 vol🟢 buy🟡 |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,146.14 | ▲ close $10,146.14 vs 09:30 $10,157.52 (session +0.00) | 16:00 close · cash $10,146.14 · no lots left · equity $10,146.14. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,146.14 | ▲ 09:30 equity $10,146.14 vs yday $10,146.14 (+0.00) | 09:30 open · cash $10,146.14 · no holdings · equity $10,146.14 vs prior close $10,146.14 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 27 | $91.01 | $2.07 | — | $7,686.80 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $2536.54 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 85 | $29.63 | $2.25 | — | $5,166.01 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $2536.54 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 17 | $144.54 | $2.04 | — | $2,706.79 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy,oppset; 🔵; ⚪; ret5=+9.2; leftover $2536.54 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 2205 | $1.15 | $28.44 | — | $142.59 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $2536.54 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $142.59 | ▲ close $10,520.35 vs 09:30 $10,146.14 (session +409.01) | 16:00 close · cash $142.59 · equity $10,520.35 vs 09:30 $10,146.14 (+374.21; session marks +409.01) · 4 name(s) marked open→close (per-name table). BHP×27 09:30 $91.01 → close $93.63 +70.74; KGC×85 09:30 $29.63 → close $31.43 +153.00; WPM×17 09:30 $144.54 → close $150.25 +97.07; CYPH×2205 09:30 $1.15 → close $1.19 +88.20 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $142.59 | ▲ 09:30 equity $11,001.98 vs yday $10,520.35 (+481.63) | 09:30 open · cash $142.59 (unchanged overnight, no fees) · equity $11,001.98 vs prior close $10,520.35 (+481.63) · 4 name(s) re-marked at the open (per-name table). BHP×27 yday $93.63 → 09:30 $95.72 +56.43; KGC×85 yday $31.43 → 09:30 $32.17 +62.90; WPM×17 yday $150.25 → 09:30 $154.70 +75.65; CYPH×2205 yday $1.19 → 09:30 $1.32 +286.65 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 2 | $17.20 | $0.35 | — | $107.84 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $47.53 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 4 | $11.13 | $0.46 | — | $62.86 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,yday_gainer,mover_buy,oppset; 🔵; ⚪; ret5=+39.8; leftover $47.53 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $62.86 | ▲ close $11,367.73 vs 09:30 $11,001.98 (session +366.56) | 16:00 close · cash $62.86 · equity $11,367.73 vs 09:30 $11,001.98 (+365.75; session marks +366.56) · 6 name(s) marked open→close (per-name table). BHP×27 09:30 $95.72 → close $97.03 +35.37; KGC×85 09:30 $32.17 → close $32.76 +50.15; WPM×17 09:30 $154.70 → close $157.78 +52.36; CYPH×2205 09:30 $1.32 → close $1.42 +220.50; AUPH×2 09:30 $17.20 → close $16.65 -1.10; ARCT×4 09:30 $11.13 → close $13.45 +9.28 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $62.86 | ▲ 09:30 equity $12,330.89 vs yday $11,367.73 (+963.16) | 09:30 open · cash $62.86 (unchanged overnight, no fees) · equity $12,330.89 vs prior close $11,367.73 (+963.16) · 6 name(s) re-marked at the open (per-name table). BHP×27 yday $97.03 → 09:30 $97.31 +7.56; KGC×85 yday $32.76 → 09:30 $33.03 +22.95; WPM×17 yday $157.78 → 09:30 $159.50 +29.24; CYPH×2205 yday $1.42 → 09:30 $1.83 +904.05; AUPH×2 yday $16.65 → 09:30 $16.57 -0.16; ARCT×4 yday $13.45 → 09:30 $13.33 -0.48 | — |
| 2026-08-24 09:30 ET | **SELL** | `BHP` | 27 | $97.31 | $2.10 | $+165.93 | $2,688.13 | ▲ +165.93 after sell → book $12,328.79; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 2) | — |
| 2026-08-24 09:30 ET | **SELL** | `KGC` | 85 | $33.03 | $2.28 | $+284.47 | $5,493.40 | ▲ +284.47 after sell → book $12,326.51; vs 09:30 mark -2.28 | dropped from list after 2 sess (min 2) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `WPM` | 17 | $159.50 | $2.07 | $+250.21 | $8,202.83 | ▲ +250.21 after sell → book $12,324.44; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 2) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 2205 | $1.83 | $28.84 | $+1442.11 | $12,209.14 | ▲ +1,442.11 after sell → book $12,295.60; vs 09:30 mark -28.84 | dropped from list after 2 sess (min 2) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,209.14 | ▲ close $12,299.64 vs 09:30 $12,330.89 (session +4.04) | 16:00 close · cash $12,209.14 · equity $12,299.64 vs 09:30 $12,330.89 (-31.25; session marks +4.04) · 2 name(s) marked open→close (per-name table). AUPH×2 09:30 $16.57 → close $16.57 +0.00; ARCT×4 09:30 $13.33 → close $14.34 +4.04 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,209.14 | ▼ 09:30 equity $12,298.88 vs yday $12,299.64 (-0.76) | 09:30 open · cash $12,209.14 (unchanged overnight, no fees) · equity $12,298.88 vs prior close $12,299.64 (-0.76) · 2 name(s) re-marked at the open (per-name table). AUPH×2 yday $16.57 → 09:30 $16.63 +0.12; ARCT×4 yday $14.34 → 09:30 $14.12 -0.88 | — |
| 2026-08-25 09:30 ET | **SELL** | `AUPH` | 2 | $16.63 | $0.36 | $-1.85 | $12,242.04 | ▼ -1.85 after sell → book $12,298.52; vs 09:30 mark -0.36 | dropped from list after 2 sess (min 2) | — |
| 2026-08-25 09:30 ET | **SELL** | `ARCT` | 4 | $14.12 | $0.60 | $+10.91 | $12,297.92 | ▲ +10.91 after sell → book $12,297.92; vs 09:30 mark -0.60 | dropped from list after 2 sess (min 2) | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 368 | $8.35 | $4.75 | — | $9,220.37 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+8.0; leftover $3074.48 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 1886 | $1.63 | $24.33 | — | $6,121.86 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $3074.48 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 1970 | $1.56 | $25.41 | — | $3,023.25 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $3074.48 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 86 | $35.05 | $2.25 | — | $6.70 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,oppset; 🔵; ⚪; ret5=+19.7; leftover $3074.48 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.70 | ▲ close $12,680.14 vs 09:30 $12,298.88 (session +438.96) | 16:00 close · cash $6.70 · equity $12,680.14 vs 09:30 $12,298.88 (+381.26; session marks +438.96) · 4 name(s) marked open→close (per-name table). CRMD×368 09:30 $8.35 → close $8.56 +77.28; BMEA×1886 09:30 $1.63 → close $1.73 +188.60; CYPH×1970 09:30 $1.56 → close $1.64 +157.60; EZPW×86 09:30 $35.05 → close $35.23 +15.48 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.70 | ▲ 09:30 equity $12,703.63 vs yday $12,680.14 (+23.49) | 09:30 open · cash $6.70 (unchanged overnight, no fees) · equity $12,703.63 vs prior close $12,680.14 (+23.49) · 4 name(s) re-marked at the open (per-name table). CRMD×368 yday $8.56 → 09:30 $8.60 +14.72; BMEA×1886 yday $1.73 → 09:30 $1.75 +47.15; CYPH×1970 yday $1.64 → 09:30 $1.60 -78.80; EZPW×86 yday $35.23 → 09:30 $35.70 +40.42 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.70 | ▼ close $12,445.78 vs 09:30 $12,703.63 (session -257.85) | 16:00 close · cash $6.70 · equity $12,445.78 vs 09:30 $12,703.63 (-257.85; session marks -257.85) · 4 name(s) marked open→close (per-name table). CRMD×368 09:30 $8.60 → close $8.39 -77.28; BMEA×1886 09:30 $1.75 → close $1.71 -84.87; CYPH×1970 09:30 $1.60 → close $1.63 +59.10; EZPW×86 09:30 $35.70 → close $33.90 -154.80 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.70 | ▲ 09:30 equity $12,741.16 vs yday $12,445.78 (+295.38) | 09:30 open · cash $6.70 (unchanged overnight, no fees) · equity $12,741.16 vs prior close $12,445.78 (+295.38) · 4 name(s) re-marked at the open (per-name table). CRMD×368 yday $8.39 → 09:30 $8.49 +36.80; BMEA×1886 yday $1.71 → 09:30 $1.74 +56.58; CYPH×1970 yday $1.63 → 09:30 $1.75 +236.40; EZPW×86 yday $33.90 → 09:30 $33.50 -34.40 | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 368 | $8.49 | $4.83 | $+41.94 | $3,126.19 | ▲ +41.94 after sell → book $12,736.33; vs 09:30 mark -4.83 | dropped from list after 2 sess (min 2) | — |
| 2026-08-27 09:30 ET | **SELL** | `BMEA` | 1886 | $1.74 | $24.67 | $+158.46 | $6,383.16 | ▲ +158.46 after sell → book $12,711.66; vs 09:30 mark -24.67 | dropped from list after 2 sess (min 2) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 1970 | $1.75 | $25.77 | $+323.12 | $9,804.89 | ▲ +323.12 after sell → book $12,685.89; vs 09:30 mark -25.77 | dropped from list after 2 sess (min 2) | — |
| 2026-08-27 09:30 ET | **SELL** | `EZPW` | 86 | $33.50 | $2.29 | $-137.83 | $12,683.61 | ▼ -137.83 after sell → book $12,683.61; vs 09:30 mark -2.28 | dropped from list after 2 sess (min 2) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,683.61 | ▲ close $12,683.61 vs 09:30 $12,741.16 (session +0.00) | 16:00 close · cash $12,683.61 · no lots left · equity $12,683.61. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,683.61 | ▲ 09:30 equity $12,683.61 vs yday $12,683.61 (-0.00) | 09:30 open · cash $12,683.61 · no holdings · equity $12,683.61 vs prior close $12,683.61 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 22 | $141.76 | $2.06 | — | $9,562.83 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover,mover_buy,oppset; 🔵; ⚪; ret5=+14.1; leftover $3170.90 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 25 | $122.81 | $2.06 | — | $6,490.52 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $3170.90 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 9 | $324.41 | $2.02 | — | $3,568.81 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $3170.90 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 34 | $91.49 | $2.09 | — | $456.06 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $3170.90 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $456.06 | ▼ close $12,201.20 vs 09:30 $12,683.61 (session -474.18) | 16:00 close · cash $456.06 · equity $12,201.20 vs 09:30 $12,683.61 (-482.41; session marks -474.18) · 4 name(s) marked open→close (per-name table). SMTC×22 09:30 $141.76 → close $131.17 -232.98; TTMI×25 09:30 $122.81 → close $118.65 -104.00; KEYS×9 09:30 $324.41 → close $319.97 -39.96; AVT×34 09:30 $91.49 → close $88.63 -97.24 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $456.06 | ▲ 09:30 equity $12,279.08 vs yday $12,201.20 (+77.88) | 09:30 open · cash $456.06 (unchanged overnight, no fees) · equity $12,279.08 vs prior close $12,201.20 (+77.88) · 4 name(s) re-marked at the open (per-name table). SMTC×22 yday $131.17 → 09:30 $132.30 +24.86; TTMI×25 yday $118.65 → 09:30 $118.83 +4.50; KEYS×9 yday $319.97 → 09:30 $322.49 +22.68; AVT×34 yday $88.63 → 09:30 $89.39 +25.84 | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $456.06 | ▲ close $12,303.52 vs 09:30 $12,279.08 (session +24.44) | 16:00 close · cash $456.06 · equity $12,303.52 vs 09:30 $12,279.08 (+24.44; session marks +24.44) · 4 name(s) marked open→close (per-name table). SMTC×22 09:30 $132.30 → close $132.96 +14.52; TTMI×25 09:30 $118.83 → close $118.92 +2.25; KEYS×9 09:30 $322.49 → close $322.70 +1.89; AVT×34 09:30 $89.39 → close $89.56 +5.78 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $456.06 | ▼ 09:30 equity $12,085.87 vs yday $12,303.52 (-217.65) | 09:30 open · cash $456.06 (unchanged overnight, no fees) · equity $12,085.87 vs prior close $12,303.52 (-217.65) · 4 name(s) re-marked at the open (per-name table). SMTC×22 yday $132.96 → 09:30 $127.63 -117.26; TTMI×25 yday $118.92 → 09:30 $116.68 -56.00; KEYS×9 yday $322.70 → 09:30 $321.47 -11.07; AVT×34 yday $89.56 → 09:30 $88.58 -33.32 | — |
| 2026-09-01 09:30 ET | **SELL** | `SMTC` | 22 | $127.63 | $2.09 | $-315.00 | $3,261.83 | ▼ -315.00 after sell → book $12,083.78; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 2) | — |
| 2026-09-01 09:30 ET | **SELL** | `TTMI` | 25 | $116.68 | $2.10 | $-157.41 | $6,176.73 | ▼ -157.41 after sell → book $12,081.68; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 2) | — |
| 2026-09-01 09:30 ET | **SELL** | `KEYS` | 9 | $321.47 | $2.05 | $-30.53 | $9,067.91 | ▼ -30.53 after sell → book $12,079.63; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 2) | — |
| 2026-09-01 09:30 ET | **SELL** | `AVT` | 34 | $88.58 | $2.13 | $-103.16 | $12,077.50 | ▼ -103.16 after sell → book $12,077.50; vs 09:30 mark -2.13 | dropped from list after 2 sess (min 2) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,077.50 | ▲ close $12,077.50 vs 09:30 $12,085.87 (session +0.00) | 16:00 close · cash $12,077.50 · no lots left · equity $12,077.50. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,077.50 | ▲ 09:30 equity $12,077.50 vs yday $12,077.50 (+0.00) | 09:30 open · cash $12,077.50 · no holdings · equity $12,077.50 vs prior close $12,077.50 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,077.50 | ▲ close $12,077.50 vs 09:30 $12,077.50 (session +0.00) | 16:00 close · cash $12,077.50 · no lots left · equity $12,077.50. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,077.50 | ▲ 09:30 equity $12,077.50 vs yday $12,077.50 (+0.00) | 09:30 open · cash $12,077.50 · no holdings · equity $12,077.50 vs prior close $12,077.50 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 57 | $52.88 | $2.16 | — | $9,061.18 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+9.2; leftover $3019.38 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 70 | $42.93 | $2.20 | — | $6,053.88 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $3019.38 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 831 | $3.63 | $10.72 | — | $3,026.63 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $3019.38 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 376 | $8.03 | $4.85 | — | $2.50 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $3019.38 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.50 | ▼ close $11,815.28 vs 09:30 $12,077.50 (session -242.29) | 16:00 close · cash $2.50 · equity $11,815.28 vs 09:30 $12,077.50 (-262.22; session marks -242.29) · 4 name(s) marked open→close (per-name table). ATRC×57 09:30 $52.88 → close $52.46 -23.94; HRMY×70 09:30 $42.93 → close $41.86 -74.90; CABA×831 09:30 $3.63 → close $3.48 -124.65; VSTM×376 09:30 $8.03 → close $7.98 -18.80 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.50 | ▼ 09:30 equity $11,722.63 vs yday $11,815.28 (-92.65) | 09:30 open · cash $2.50 (unchanged overnight, no fees) · equity $11,722.63 vs prior close $11,815.28 (-92.65) · 4 name(s) re-marked at the open (per-name table). ATRC×57 yday $52.46 → 09:30 $52.03 -24.51; HRMY×70 yday $41.86 → 09:30 $41.50 -25.20; CABA×831 yday $3.48 → 09:30 $3.46 -16.62; VSTM×376 yday $7.98 → 09:30 $7.91 -26.32 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.50 | ▲ close $11,863.41 vs 09:30 $11,722.63 (session +140.78) | 16:00 close · cash $2.50 · equity $11,863.41 vs 09:30 $11,722.63 (+140.78; session marks +140.78) · 4 name(s) marked open→close (per-name table). ATRC×57 09:30 $52.03 → close $51.52 -29.07; HRMY×70 09:30 $41.50 → close $42.25 +52.50; CABA×831 09:30 $3.46 → close $3.47 +8.31; VSTM×376 09:30 $7.91 → close $8.20 +109.04 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.50 | ▲ 09:30 equity $11,985.70 vs yday $11,863.41 (+122.29) | 09:30 open · cash $2.50 (unchanged overnight, no fees) · equity $11,985.70 vs prior close $11,863.41 (+122.29) · 4 name(s) re-marked at the open (per-name table). ATRC×57 yday $51.52 → 09:30 $54.31 +159.03; HRMY×70 yday $42.25 → 09:30 $42.20 -3.50; CABA×831 yday $3.47 → 09:30 $3.43 -33.24; VSTM×376 yday $8.20 → 09:30 $8.20 +0.00 | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 57 | $54.31 | $2.20 | $+77.15 | $3,095.98 | ▲ +77.15 after sell → book $11,983.51; vs 09:30 mark -2.19 | dropped from list after 2 sess (min 2) | — |
| 2026-09-08 09:30 ET | **SELL** | `HRMY` | 70 | $42.20 | $2.24 | $-55.54 | $6,047.74 | ▼ -55.54 after sell → book $11,981.27; vs 09:30 mark -2.24 | dropped from list after 2 sess (min 2) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 831 | $3.43 | $10.88 | $-187.80 | $8,887.19 | ▼ -187.80 after sell → book $11,970.39; vs 09:30 mark -10.88 | dropped from list after 2 sess (min 2) | — |
| 2026-09-08 09:30 ET | **SELL** | `VSTM` | 376 | $8.20 | $4.94 | $+54.13 | $11,965.45 | ▲ +54.13 after sell → book $11,965.45; vs 09:30 mark -4.94 | dropped from list after 2 sess (min 2) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,965.45 | ▲ close $11,965.45 vs 09:30 $11,985.70 (session +0.00) | 16:00 close · cash $11,965.45 · no lots left · equity $11,965.45. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,965.45 | ▲ 09:30 equity $11,965.45 vs yday $11,965.45 (+0.00) | 09:30 open · cash $11,965.45 · no holdings · equity $11,965.45 vs prior close $11,965.45 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,965.45 | ▲ close $11,965.45 vs 09:30 $11,965.45 (session +0.00) | 16:00 close · cash $11,965.45 · no lots left · equity $11,965.45. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,965.45 | ▲ 09:30 equity $11,965.45 vs yday $11,965.45 (+0.00) | 09:30 open · cash $11,965.45 · no holdings · equity $11,965.45 vs prior close $11,965.45 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,965.45 | ▲ close $11,965.45 vs 09:30 $11,965.45 (session +0.00) | 16:00 close · cash $11,965.45 · no lots left · equity $11,965.45. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,965.45 | ▲ 09:30 equity $11,965.45 vs yday $11,965.45 (+0.00) | 09:30 open · cash $11,965.45 · no holdings · equity $11,965.45 vs prior close $11,965.45 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 75 | $52.55 | $2.21 | — | $8,021.99 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $3988.48 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `PAYP` | 217 | $18.30 | $2.80 | — | $4,048.09 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $3988.48 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `SEDG` | 108 | $36.78 | $2.31 | — | $73.54 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list oppset; 🔵; ⚪; ret5=+8.2; leftover $3988.48 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $73.54 | ▲ close $12,087.88 vs 09:30 $11,965.45 (session +129.75) | 16:00 close · cash $73.54 · equity $12,087.88 vs 09:30 $11,965.45 (+122.43; session marks +129.75) · 3 name(s) marked open→close (per-name table). BAND×75 09:30 $52.55 → close $56.87 +324.00; PAYP×217 09:30 $18.30 → close $18.45 +32.55; SEDG×108 09:30 $36.78 → close $34.68 -226.80 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $73.54 | ▼ 09:30 equity $11,940.92 vs yday $12,087.88 (-146.96) | 09:30 open · cash $73.54 (unchanged overnight, no fees) · equity $11,940.92 vs prior close $12,087.88 (-146.96) · 3 name(s) re-marked at the open (per-name table). BAND×75 yday $56.87 → 09:30 $56.90 +2.25; PAYP×217 yday $18.45 → 09:30 $18.28 -36.89; SEDG×108 yday $34.68 → 09:30 $33.64 -112.32 | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $73.54 | ▼ close $11,615.49 vs 09:30 $11,940.92 (session -325.43) | 16:00 close · cash $73.54 · equity $11,615.49 vs 09:30 $11,940.92 (-325.43; session marks -325.43) · 3 name(s) marked open→close (per-name table). BAND×75 09:30 $56.90 → close $48.97 -594.75; PAYP×217 09:30 $18.28 → close $18.68 +86.80; SEDG×108 09:30 $33.64 → close $35.33 +182.52 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $73.54 | ▼ 09:30 equity $11,563.81 vs yday $11,615.49 (-51.68) | 09:30 open · cash $73.54 (unchanged overnight, no fees) · equity $11,563.81 vs prior close $11,615.49 (-51.68) · 3 name(s) re-marked at the open (per-name table). BAND×75 yday $48.97 → 09:30 $49.51 +40.50; PAYP×217 yday $18.68 → 09:30 $18.30 -82.46; SEDG×108 yday $35.33 → 09:30 $35.24 -9.72 | — |
| 2026-09-15 09:30 ET | **SELL** | `BAND` | 75 | $49.51 | $2.26 | $-232.47 | $3,784.53 | ▼ -232.47 after sell → book $11,561.55; vs 09:30 mark -2.26 | dropped from list after 2 sess (min 2) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-15 09:30 ET | **SELL** | `PAYP` | 217 | $18.30 | $2.87 | $-5.67 | $7,752.76 | ▼ -5.67 after sell → book $11,558.68; vs 09:30 mark -2.87 | dropped from list after 2 sess (min 2) | — |
| 2026-09-15 09:30 ET | **SELL** | `SEDG` | 108 | $35.24 | $2.36 | $-171.00 | $11,556.32 | ▼ -171.00 after sell → book $11,556.32; vs 09:30 mark -2.36 | dropped from list after 2 sess (min 2) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,556.32 | ▲ close $11,556.32 vs 09:30 $11,563.81 (session +0.00) | 16:00 close · cash $11,556.32 · no lots left · equity $11,556.32. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,556.32 | ▲ 09:30 equity $11,556.32 vs yday $11,556.32 (-0.00) | 09:30 open · cash $11,556.32 · no holdings · equity $11,556.32 vs prior close $11,556.32 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 64 | $89.38 | $2.18 | — | $5,833.82 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $5778.16 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `QRVO` | 48 | $118.18 | $2.13 | — | $159.04 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $5778.16 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $159.04 | ▼ close $11,107.36 vs 09:30 $11,556.32 (session -444.64) | 16:00 close · cash $159.04 · equity $11,107.36 vs 09:30 $11,556.32 (-448.96; session marks -444.64) · 2 name(s) marked open→close (per-name table). SWKS×64 09:30 $89.38 → close $85.59 -242.56; QRVO×48 09:30 $118.18 → close $113.97 -202.08 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $159.04 | ▲ 09:30 equity $11,226.88 vs yday $11,107.36 (+119.52) | 09:30 open · cash $159.04 (unchanged overnight, no fees) · equity $11,226.88 vs prior close $11,107.36 (+119.52) · 2 name(s) re-marked at the open (per-name table). SWKS×64 yday $85.59 → 09:30 $86.76 +74.88; QRVO×48 yday $113.97 → 09:30 $114.90 +44.64 | — |
| 2026-09-17 09:30 ET | **BUY** | `VOD` | 4 | $17.56 | $0.71 | — | $88.09 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list ohlc_hot; 🔵; ⚪; ret5=+7.9; leftover $79.52 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `ASAN` | 8 | $9.55 | $0.79 | — | $10.90 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list oppset; 🔵; ⚪; ret5=+17.0; leftover $79.52 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.90 | ▲ close $11,742.66 vs 09:30 $11,226.88 (session +517.28) | 16:00 close · cash $10.90 · equity $11,742.66 vs 09:30 $11,226.88 (+515.78; session marks +517.28) · 4 name(s) marked open→close (per-name table). SWKS×64 09:30 $86.76 → close $91.32 +291.84; QRVO×48 09:30 $114.90 → close $119.51 +221.28; VOD×4 09:30 $17.56 → close $17.52 -0.16; ASAN×8 09:30 $9.55 → close $10.09 +4.32 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.90 | ▲ 09:30 equity $11,846.22 vs yday $11,742.66 (+103.56) | 09:30 open · cash $10.90 (unchanged overnight, no fees) · equity $11,846.22 vs prior close $11,742.66 (+103.56) · 4 name(s) re-marked at the open (per-name table). SWKS×64 yday $91.32 → 09:30 $92.05 +46.72; QRVO×48 yday $119.51 → 09:30 $120.76 +60.00; VOD×4 yday $17.52 → 09:30 $16.73 -3.16; ASAN×8 yday $10.09 → 09:30 $10.09 +0.00 | — |
| 2026-09-18 09:30 ET | **SELL** | `SWKS` | 64 | $92.05 | $2.24 | $+166.46 | $5,899.86 | ▲ +166.46 after sell → book $11,843.98; vs 09:30 mark -2.24 | dropped from list after 2 sess (min 2) | — |
| 2026-09-18 09:30 ET | **SELL** | `QRVO` | 48 | $120.76 | $2.19 | $+119.52 | $11,694.15 | ▲ +119.52 after sell → book $11,841.79; vs 09:30 mark -2.19 | dropped from list after 2 sess (min 2) | — |
| 2026-09-18 09:30 ET | **BUY** | `ILMN` | 11 | $249.13 | $2.02 | — | $8,951.70 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+21.8; leftover $2923.54 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 99 | $29.32 | $2.29 | — | $6,046.73 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+60.9; leftover $2923.54 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `ARQT` | 111 | $26.14 | $2.32 | — | $3,142.87 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,ohlc_hot,oppset; 🔵; ⚪; ret5=+13.2; leftover $2923.54 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `FTRE` | 145 | $20.10 | $2.42 | — | $225.94 | — | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+19.2; leftover $2923.54 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $225.94 | ▼ close $11,585.65 vs 09:30 $11,846.22 (session -247.08) | 16:00 close · cash $225.94 · equity $11,585.65 vs 09:30 $11,846.22 (-260.57; session marks -247.08) · 6 name(s) marked open→close (per-name table). VOD×4 09:30 $16.73 → close $16.95 +0.88; ASAN×8 09:30 $10.09 → close $9.51 -4.64; ILMN×11 09:30 $249.13 → close $239.62 -104.61; SDGR×99 09:30 $29.32 → close $29.02 -29.70; ARQT×111 09:30 $26.14 → close $25.38 -84.36; FTRE×145 09:30 $20.10 → close $19.93 -24.65 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-14 | `SLG` | cash | leftover split 17.75 < 1 share @ 57.61 |
| 2026-08-14 | `WDC` | cash | leftover split 17.75 < 1 share @ 503.50 |
| 2026-08-17 | `BETR` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-18 | `ABX` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-18 | `ALM` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-18 | `NMAX` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-18 | `AAOI` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-21 | `AEM` | cash | leftover split 47.53 < 1 share @ 216.30 |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-26 | `CRMD` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-26 | `EZPW` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-31 | `TTMI` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-31 | `AVT` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-04 | `ATRC` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-04 | `CABA` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-04 | `CRM` | cash | leftover split 0.63 < 1 share @ 263.36 |
| 2026-09-04 | `DELL` | cash | leftover split 0.63 < 1 share @ 513.78 |
| 2026-09-04 | `IRD` | cash | leftover split 0.63 < 1 share @ 4.53 |
| 2026-09-04 | `LENZ` | cash | leftover split 0.63 < 1 share @ 5.75 |
| 2026-09-14 | `BAND` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-14 | `PAYP` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-14 | `SEDG` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-17 | `SWKS` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-17 | `QRVO` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-18 | `VOD` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-18 | `ASAN` | min_hold | dropped but min-hold 1/2 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `VOD` | 4 | 2026-09-17 @ $17.56 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list ohlc_hot; 🔵; ⚪; ret5=+7.9; leftover $79.52 |
| `ASAN` | 8 | 2026-09-17 @ $9.55 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list oppset; 🔵; ⚪; ret5=+17.0; leftover $79.52 |
| `ILMN` | 11 | 2026-09-18 @ $249.13 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+21.8; leftover $2923.54 |
| `SDGR` | 99 | 2026-09-18 @ $29.32 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+60.9; leftover $2923.54 |
| `ARQT` | 111 | 2026-09-18 @ $26.14 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten,ohlc_hot,oppset; 🔵; ⚪; ret5=+13.2; leftover $2923.54 |
| `FTRE` | 145 | 2026-09-18 @ $20.10 | −0 red + yday up AND catalyst, top 4 by Score; gate cam_bad_max=0,yday_and_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+19.2; leftover $2923.54 |
