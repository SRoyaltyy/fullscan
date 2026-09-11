# Factor mine action — `union_w_hot_cond_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `w_hot_cond` · size `leftover` · sell `list` · S-boost `none` · rank by w_hot_cond

Cash book **-1.54%** ($9,846) · signal-only (no cash/fees) was +4.20%. Starts YES **6/20**. Fills 168 · skips 70 · realized $-154.41.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: a mix of tape-heat and green cameras.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is thrown out if any must-not is true.
- Sort the keepers by a mix of tape-heat and green cameras and keep the top 8.
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
- **Gate** `none (list as ranked)` · **rank** `w_hot_cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,845.56.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `IREN` | 27 | — | $45.98 | +0.00 | $44.76 | -32.94 | -32.94 | +0.00 | -32.94 |
| 2026-08-13 | `TNDM` | 53 | — | $23.33 | +0.00 | $23.13 | -10.60 | -10.60 | +0.00 | -10.60 |
| 2026-08-13 | `INO` | 1543 | — | $0.81 | +0.00 | $0.90 | +138.87 | +138.87 | +0.00 | +138.87 |
| 2026-08-13 | `TPG` | 24 | — | $50.62 | +0.00 | $54.62 | +95.92 | +95.92 | +0.00 | +95.92 |
| 2026-08-13 | `HIMS` | 42 | — | $29.74 | +0.00 | $28.77 | -40.74 | -40.74 | +0.00 | -40.74 |
| 2026-08-13 | `SLS` | 106 | — | $11.70 | +0.00 | $12.36 | +69.96 | +69.96 | +0.00 | +69.96 |
| 2026-08-13 | `VOR` | 56 | — | $22.01 | +0.00 | $23.29 | +71.68 | +71.68 | +0.00 | +71.68 |
| 2026-08-13 | `BTSG` | 20 | — | $59.80 | +0.00 | $60.23 | +8.60 | +8.60 | +0.00 | +8.60 |
| 2026-08-14 | `IREN` | 27 | $44.76 | $44.09 | -18.09 | — | +0.00 | -18.09 | -51.03 | — |
| 2026-08-14 | `TNDM` | 53 | $23.13 | $22.92 | -11.13 | — | +0.00 | -11.13 | -21.73 | — |
| 2026-08-14 | `INO` | 1543 | $0.90 | $0.93 | +46.29 | — | +0.00 | +46.29 | +185.16 | — |
| 2026-08-14 | `TPG` | 24 | $54.62 | $55.29 | +16.08 | — | +0.00 | +16.08 | +112.00 | — |
| 2026-08-14 | `HIMS` | 42 | $28.77 | $29.15 | +15.96 | — | +0.00 | +15.96 | -24.78 | — |
| 2026-08-14 | `SLS` | 106 | $12.36 | $12.40 | +4.24 | — | +0.00 | +4.24 | +74.20 | — |
| 2026-08-14 | `VOR` | 56 | $23.29 | $23.33 | +2.24 | — | +0.00 | +2.24 | +73.92 | — |
| 2026-08-14 | `BTSG` | 20 | $60.23 | $59.65 | -11.60 | — | +0.00 | -11.60 | -3.00 | — |
| 2026-08-14 | `QMCO` | 52 | — | $24.68 | +0.00 | $26.11 | +74.36 | +74.36 | +0.00 | +74.36 |
| 2026-08-14 | `ZENA` | 583 | — | $2.20 | +0.00 | $2.14 | -34.98 | -34.98 | +0.00 | -34.98 |
| 2026-08-14 | `ARX` | 65 | — | $19.57 | +0.00 | $19.58 | +0.65 | +0.65 | +0.00 | +0.65 |
| 2026-08-14 | `AIRO` | 115 | — | $11.12 | +0.00 | $9.57 | -178.25 | -178.25 | +0.00 | -178.25 |
| 2026-08-14 | `BZAI` | 1677 | — | $0.77 | +0.00 | $0.59 | -290.12 | -290.12 | +0.00 | -290.12 |
| 2026-08-14 | `BRUN` | 48 | — | $26.25 | +0.00 | $22.93 | -159.12 | -159.12 | +0.00 | -159.12 |
| 2026-08-14 | `LIFE` | 36 | — | $35.04 | +0.00 | $34.02 | -36.72 | -36.72 | +0.00 | -36.72 |
| 2026-08-14 | `LUNR` | 67 | — | $19.17 | +0.00 | $19.01 | -10.72 | -10.72 | +0.00 | -10.72 |
| 2026-08-17 | `QMCO` | 52 | $26.11 | $24.83 | -66.56 | — | +0.00 | -66.56 | +7.80 | — |
| 2026-08-17 | `ZENA` | 583 | $2.14 | $2.08 | -32.07 | — | +0.00 | -32.07 | -67.05 | — |
| 2026-08-17 | `ARX` | 65 | $19.58 | $19.57 | -0.65 | — | +0.00 | -0.65 | +0.00 | — |
| 2026-08-17 | `AIRO` | 115 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -178.25 | — |
| 2026-08-17 | `BZAI` | 1677 | $0.59 | $0.55 | -68.76 | — | +0.00 | -68.76 | -358.88 | — |
| 2026-08-17 | `BRUN` | 48 | $22.93 | $23.00 | +3.36 | — | +0.00 | +3.36 | -155.76 | — |
| 2026-08-17 | `LIFE` | 36 | $34.02 | $34.03 | +0.36 | — | +0.00 | +0.36 | -36.36 | — |
| 2026-08-17 | `LUNR` | 67 | $19.01 | $20.25 | +83.08 | — | +0.00 | +83.08 | +72.36 | — |
| 2026-08-17 | `XHG` | 283 | — | $4.19 | +0.00 | $3.91 | -79.24 | -79.24 | +0.00 | -79.24 |
| 2026-08-17 | `CAPR` | 172 | — | $6.87 | +0.00 | $7.45 | +99.76 | +99.76 | +0.00 | +99.76 |
| 2026-08-17 | `STDN` | 86 | — | $13.64 | +0.00 | $13.31 | -28.38 | -28.38 | +0.00 | -28.38 |
| 2026-08-17 | `HTFL` | 28 | — | $41.23 | +0.00 | $41.94 | +19.88 | +19.88 | +0.00 | +19.88 |
| 2026-08-17 | `UMAC` | 36 | — | $32.55 | +0.00 | $30.15 | -86.40 | -86.40 | +0.00 | -86.40 |
| 2026-08-17 | `ALOY` | 80 | — | $14.66 | +0.00 | $13.86 | -64.40 | -64.40 | +0.00 | -64.40 |
| 2026-08-17 | `LPTH` | 79 | — | $14.94 | +0.00 | $14.80 | -11.06 | -11.06 | +0.00 | -11.06 |
| 2026-08-17 | `NPWR` | 617 | — | $1.92 | +0.00 | $1.73 | -117.23 | -117.23 | +0.00 | -117.23 |
| 2026-08-18 | `XHG` | 283 | $3.91 | $3.94 | +8.49 | — | +0.00 | +8.49 | -70.75 | — |
| 2026-08-18 | `CAPR` | 172 | $7.45 | $7.50 | +8.60 | $7.08 | -72.24 | -63.64 | +108.36 | +36.12 |
| 2026-08-18 | `STDN` | 86 | $13.31 | $13.31 | +0.00 | — | +0.00 | +0.00 | -28.38 | — |
| 2026-08-18 | `HTFL` | 28 | $41.94 | $41.50 | -12.32 | — | +0.00 | -12.32 | +7.56 | — |
| 2026-08-18 | `UMAC` | 36 | $30.15 | $28.59 | -56.16 | — | +0.00 | -56.16 | -142.56 | — |
| 2026-08-18 | `ALOY` | 80 | $13.86 | $13.19 | -53.20 | — | +0.00 | -53.20 | -117.60 | — |
| 2026-08-18 | `LPTH` | 79 | $14.80 | $14.01 | -62.41 | — | +0.00 | -62.41 | -73.47 | — |
| 2026-08-18 | `NPWR` | 617 | $1.73 | $1.70 | -18.51 | — | +0.00 | -18.51 | -135.74 | — |
| 2026-08-19 | `CAPR` | 172 | $7.08 | $7.19 | +18.92 | — | +0.00 | +18.92 | +55.04 | — |
| 2026-08-20 | `MRNA` | 7 | — | $150.14 | +0.00 | $133.32 | -117.74 | -117.74 | +0.00 | -117.74 |
| 2026-08-20 | `CYPH` | 970 | — | $1.15 | +0.00 | $1.19 | +38.80 | +38.80 | +0.00 | +38.80 |
| 2026-08-20 | `ABCL` | 94 | — | $11.81 | +0.00 | $11.57 | -23.03 | -23.03 | +0.00 | -23.03 |
| 2026-08-20 | `SENS` | 125 | — | $8.91 | +0.00 | $8.82 | -11.25 | -11.25 | +0.00 | -11.25 |
| 2026-08-20 | `AUTL` | 452 | — | $2.47 | +0.00 | $2.46 | -4.52 | -4.52 | +0.00 | -4.52 |
| 2026-08-20 | `TEM` | 18 | — | $61.83 | +0.00 | $66.65 | +86.76 | +86.76 | +0.00 | +86.76 |
| 2026-08-20 | `WPM` | 7 | — | $144.54 | +0.00 | $150.25 | +39.97 | +39.97 | +0.00 | +39.97 |
| 2026-08-20 | `IAG` | 56 | — | $19.63 | +0.00 | $20.50 | +48.72 | +48.72 | +0.00 | +48.72 |
| 2026-08-21 | `MRNA` | 7 | $133.32 | $133.11 | -1.47 | $145.13 | +84.14 | +82.67 | -119.21 | -35.07 |
| 2026-08-21 | `CYPH` | 970 | $1.19 | $1.32 | +126.10 | $1.42 | +97.00 | +223.10 | +164.90 | +261.90 |
| 2026-08-21 | `ABCL` | 94 | $11.57 | $11.57 | +0.00 | — | +0.00 | +0.00 | -23.03 | — |
| 2026-08-21 | `SENS` | 125 | $8.82 | $9.24 | +52.50 | — | +0.00 | +52.50 | +41.25 | — |
| 2026-08-21 | `AUTL` | 452 | $2.46 | $2.47 | +4.52 | — | +0.00 | +4.52 | +0.00 | — |
| 2026-08-21 | `TEM` | 18 | $66.65 | $65.60 | -18.90 | — | +0.00 | -18.90 | +67.86 | — |
| 2026-08-21 | `WPM` | 7 | $150.25 | $154.70 | +31.15 | — | +0.00 | +31.15 | +71.12 | — |
| 2026-08-21 | `IAG` | 56 | $20.50 | $21.17 | +37.52 | — | +0.00 | +37.52 | +86.24 | — |
| 2026-08-21 | `XHG` | 258 | — | $4.49 | +0.00 | $4.41 | -20.64 | -20.64 | +0.00 | -20.64 |
| 2026-08-21 | `ARCT` | 104 | — | $11.13 | +0.00 | $13.45 | +241.28 | +241.28 | +0.00 | +241.28 |
| 2026-08-21 | `IOVA` | 127 | — | $9.08 | +0.00 | $8.29 | -100.33 | -100.33 | +0.00 | -100.33 |
| 2026-08-21 | `MRVI` | 140 | — | $8.28 | +0.00 | $8.64 | +50.40 | +50.40 | +0.00 | +50.40 |
| 2026-08-21 | `AU` | 9 | — | $119.43 | +0.00 | $121.22 | +16.11 | +16.11 | +0.00 | +16.11 |
| 2026-08-21 | `CAPR` | 170 | — | $6.81 | +0.00 | $6.29 | -88.40 | -88.40 | +0.00 | -88.40 |
| 2026-08-24 | `MRNA` | 7 | $145.13 | $142.70 | -17.01 | — | +0.00 | -17.01 | -52.08 | — |
| 2026-08-24 | `CYPH` | 970 | $1.42 | $1.83 | +397.70 | — | +0.00 | +397.70 | +659.60 | — |
| 2026-08-24 | `XHG` | 258 | $4.41 | $4.32 | -23.22 | — | +0.00 | -23.22 | -43.86 | — |
| 2026-08-24 | `ARCT` | 104 | $13.45 | $13.33 | -12.48 | — | +0.00 | -12.48 | +228.80 | — |
| 2026-08-24 | `IOVA` | 127 | $8.29 | $8.08 | -26.67 | — | +0.00 | -26.67 | -127.00 | — |
| 2026-08-24 | `MRVI` | 140 | $8.64 | $8.59 | -7.00 | — | +0.00 | -7.00 | +43.40 | — |
| 2026-08-24 | `AU` | 9 | $121.22 | $120.51 | -6.39 | — | +0.00 | -6.39 | +9.72 | — |
| 2026-08-24 | `CAPR` | 170 | $6.29 | $8.03 | +295.80 | — | +0.00 | +295.80 | +207.40 | — |
| 2026-08-25 | `REAX` | 51 | — | $24.11 | +0.00 | $28.43 | +220.32 | +220.32 | +0.00 | +220.32 |
| 2026-08-25 | `CYPH` | 801 | — | $1.56 | +0.00 | $1.64 | +64.08 | +64.08 | +0.00 | +64.08 |
| 2026-08-25 | `XHG` | 307 | — | $4.07 | +0.00 | $4.02 | -15.35 | -15.35 | +0.00 | -15.35 |
| 2026-08-25 | `ALVO` | 238 | — | $5.24 | +0.00 | $5.05 | -45.22 | -45.22 | +0.00 | -45.22 |
| 2026-08-25 | `ASST` | 65 | — | $19.04 | +0.00 | $21.39 | +152.75 | +152.75 | +0.00 | +152.75 |
| 2026-08-25 | `GORO` | 352 | — | $3.55 | +0.00 | $3.87 | +112.64 | +112.64 | +0.00 | +112.64 |
| 2026-08-25 | `EZPW` | 35 | — | $35.05 | +0.00 | $35.23 | +6.30 | +6.30 | +0.00 | +6.30 |
| 2026-08-25 | `BMEA` | 767 | — | $1.63 | +0.00 | $1.73 | +76.70 | +76.70 | +0.00 | +76.70 |
| 2026-08-26 | `REAX` | 51 | $28.43 | $26.61 | -92.82 | — | +0.00 | -92.82 | +127.50 | — |
| 2026-08-26 | `CYPH` | 801 | $1.64 | $1.60 | -32.04 | — | +0.00 | -32.04 | +32.04 | — |
| 2026-08-26 | `XHG` | 307 | $4.02 | $3.81 | -64.47 | $4.06 | +76.75 | +12.28 | -79.82 | -3.07 |
| 2026-08-26 | `ALVO` | 238 | $5.05 | $4.98 | -16.66 | — | +0.00 | -16.66 | -61.88 | — |
| 2026-08-26 | `ASST` | 65 | $21.39 | $20.72 | -43.55 | — | +0.00 | -43.55 | +109.20 | — |
| 2026-08-26 | `GORO` | 352 | $3.87 | $3.77 | -35.20 | — | +0.00 | -35.20 | +77.44 | — |
| 2026-08-26 | `EZPW` | 35 | $35.23 | $35.70 | +16.45 | — | +0.00 | +16.45 | +22.75 | — |
| 2026-08-26 | `BMEA` | 767 | $1.73 | $1.75 | +19.17 | — | +0.00 | +19.17 | +95.88 | — |
| 2026-08-26 | `BYND` | 92 | — | $14.11 | +0.00 | $14.25 | +12.88 | +12.88 | +0.00 | +12.88 |
| 2026-08-26 | `USDE` | 223 | — | $5.81 | +0.00 | $5.98 | +37.91 | +37.91 | +0.00 | +37.91 |
| 2026-08-26 | `FIGR` | 32 | — | $40.50 | +0.00 | $37.08 | -109.44 | -109.44 | +0.00 | -109.44 |
| 2026-08-26 | `FUTU` | 10 | — | $124.67 | +0.00 | $127.34 | +26.70 | +26.70 | +0.00 | +26.70 |
| 2026-08-26 | `TIGR` | 249 | — | $5.21 | +0.00 | $5.46 | +62.25 | +62.25 | +0.00 | +62.25 |
| 2026-08-26 | `FNV` | 4 | — | $267.02 | +0.00 | $267.37 | +1.40 | +1.40 | +0.00 | +1.40 |
| 2026-08-26 | `BTG` | 225 | — | $5.75 | +0.00 | $5.74 | -2.25 | -2.25 | +0.00 | -2.25 |
| 2026-08-27 | `XHG` | 307 | $4.06 | $4.06 | +0.00 | — | +0.00 | +0.00 | -3.07 | — |
| 2026-08-27 | `BYND` | 92 | $14.25 | $14.20 | -4.60 | — | +0.00 | -4.60 | +8.28 | — |
| 2026-08-27 | `USDE` | 223 | $5.98 | $6.50 | +115.96 | — | +0.00 | +115.96 | +153.87 | — |
| 2026-08-27 | `FIGR` | 32 | $37.08 | $37.42 | +10.88 | — | +0.00 | +10.88 | -98.56 | — |
| 2026-08-27 | `FUTU` | 10 | $127.34 | $128.00 | +6.60 | — | +0.00 | +6.60 | +33.30 | — |
| 2026-08-27 | `TIGR` | 249 | $5.46 | $5.49 | +7.47 | — | +0.00 | +7.47 | +69.72 | — |
| 2026-08-27 | `FNV` | 4 | $267.37 | $267.23 | -0.56 | — | +0.00 | -0.56 | +0.84 | — |
| 2026-08-27 | `BTG` | 225 | $5.74 | $5.73 | -2.25 | — | +0.00 | -2.25 | -4.50 | — |
| 2026-08-27 | `SLI` | 502 | — | $2.60 | +0.00 | $2.64 | +20.08 | +20.08 | +0.00 | +20.08 |
| 2026-08-27 | `PGY` | 57 | — | $22.93 | +0.00 | $23.26 | +18.81 | +18.81 | +0.00 | +18.81 |
| 2026-08-27 | `GEN` | 43 | — | $29.83 | +0.00 | $30.50 | +28.81 | +28.81 | +0.00 | +28.81 |
| 2026-08-27 | `MRVL` | 5 | — | $253.44 | +0.00 | $241.45 | -59.95 | -59.95 | +0.00 | -59.95 |
| 2026-08-27 | `ANET` | 6 | — | $205.90 | +0.00 | $201.09 | -28.86 | -28.86 | +0.00 | -28.86 |
| 2026-08-27 | `MOS` | 54 | — | $24.00 | +0.00 | $23.76 | -12.96 | -12.96 | +0.00 | -12.96 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-27 | `GGB` | 286 | — | $4.57 | +0.00 | $4.70 | +37.18 | +37.18 | +0.00 | +37.18 |
| 2026-08-28 | `SLI` | 502 | $2.64 | $2.68 | +20.08 | — | +0.00 | +20.08 | +40.16 | — |
| 2026-08-28 | `PGY` | 57 | $23.26 | $23.21 | -2.85 | — | +0.00 | -2.85 | +15.96 | — |
| 2026-08-28 | `GEN` | 43 | $30.50 | $30.50 | +0.00 | — | +0.00 | +0.00 | +28.81 | — |
| 2026-08-28 | `MRVL` | 5 | $241.45 | $225.26 | -80.95 | — | +0.00 | -80.95 | -140.90 | — |
| 2026-08-28 | `ANET` | 6 | $201.09 | $200.00 | -6.54 | — | +0.00 | -6.54 | -35.40 | — |
| 2026-08-28 | `MOS` | 54 | $23.76 | $23.95 | +10.26 | — | +0.00 | +10.26 | -2.70 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `GGB` | 286 | $4.70 | $4.67 | -8.58 | — | +0.00 | -8.58 | +28.60 | — |
| 2026-08-28 | `BYND` | 91 | — | $14.00 | +0.00 | $13.86 | -12.74 | -12.74 | +0.00 | -12.74 |
| 2026-08-28 | `CAPR` | 132 | — | $9.73 | +0.00 | $9.59 | -18.48 | -18.48 | +0.00 | -18.48 |
| 2026-08-28 | `ANF` | 8 | — | $146.07 | +0.00 | $148.42 | +18.80 | +18.80 | +0.00 | +18.80 |
| 2026-08-28 | `NCNO` | 55 | — | $23.30 | +0.00 | $22.99 | -17.05 | -17.05 | +0.00 | -17.05 |
| 2026-08-28 | `MEI` | 72 | — | $17.78 | +0.00 | $18.21 | +30.96 | +30.96 | +0.00 | +30.96 |
| 2026-08-28 | `VYX` | 141 | — | $9.13 | +0.00 | $8.78 | -49.35 | -49.35 | +0.00 | -49.35 |
| 2026-08-28 | `SMTC` | 9 | — | $141.76 | +0.00 | $131.17 | -95.31 | -95.31 | +0.00 | -95.31 |
| 2026-08-28 | `SNPS` | 2 | — | $461.85 | +0.00 | $442.61 | -38.48 | -38.48 | +0.00 | -38.48 |
| 2026-08-31 | `BYND` | 91 | $13.86 | $13.81 | -4.55 | $13.30 | -46.41 | -50.96 | -17.29 | -63.70 |
| 2026-08-31 | `CAPR` | 132 | $9.59 | $9.50 | -11.88 | — | +0.00 | -11.88 | -30.36 | — |
| 2026-08-31 | `ANF` | 8 | $148.42 | $148.03 | -3.12 | — | +0.00 | -3.12 | +15.68 | — |
| 2026-08-31 | `NCNO` | 55 | $22.99 | $22.66 | -18.15 | — | +0.00 | -18.15 | -35.20 | — |
| 2026-08-31 | `MEI` | 72 | $18.21 | $18.15 | -4.32 | — | +0.00 | -4.32 | +26.64 | — |
| 2026-08-31 | `VYX` | 141 | $8.78 | $8.66 | -16.92 | — | +0.00 | -16.92 | -66.27 | — |
| 2026-08-31 | `SMTC` | 9 | $131.17 | $132.30 | +10.17 | — | +0.00 | +10.17 | -85.14 | — |
| 2026-08-31 | `SNPS` | 2 | $442.61 | $437.95 | -9.32 | — | +0.00 | -9.32 | -47.80 | — |
| 2026-09-01 | `BYND` | 91 | $13.30 | $13.04 | -23.66 | — | +0.00 | -23.66 | -87.36 | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `GPRO` | 699 | — | $1.78 | +0.00 | $1.39 | -272.61 | -272.61 | +0.00 | -272.61 |
| 2026-09-03 | `REAX` | 67 | — | $18.40 | +0.00 | $18.40 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `BMEA` | 644 | — | $1.93 | +0.00 | $1.91 | -12.88 | -12.88 | +0.00 | -12.88 |
| 2026-09-03 | `MMED` | 52 | — | $23.88 | +0.00 | $23.84 | -2.08 | -2.08 | +0.00 | -2.08 |
| 2026-09-03 | `AGCO` | 9 | — | $127.91 | +0.00 | $125.82 | -18.81 | -18.81 | +0.00 | -18.81 |
| 2026-09-03 | `ARCT` | 74 | — | $16.77 | +0.00 | $15.56 | -89.54 | -89.54 | +0.00 | -89.54 |
| 2026-09-03 | `NVAX` | 119 | — | $10.42 | +0.00 | $10.34 | -9.52 | -9.52 | +0.00 | -9.52 |
| 2026-09-03 | `VSTM` | 154 | — | $8.03 | +0.00 | $7.98 | -7.70 | -7.70 | +0.00 | -7.70 |
| 2026-09-04 | `GPRO` | 699 | $1.39 | $1.48 | +62.91 | $1.70 | +153.78 | +216.69 | -209.70 | -55.92 |
| 2026-09-04 | `REAX` | 67 | $18.40 | $18.15 | -16.75 | — | +0.00 | -16.75 | -16.75 | — |
| 2026-09-04 | `BMEA` | 644 | $1.91 | $1.90 | -6.44 | — | +0.00 | -6.44 | -19.32 | — |
| 2026-09-04 | `MMED` | 52 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -2.08 | — |
| 2026-09-04 | `AGCO` | 9 | $125.82 | $125.22 | -5.40 | — | +0.00 | -5.40 | -24.21 | — |
| 2026-09-04 | `ARCT` | 74 | $15.56 | $15.61 | +3.70 | — | +0.00 | +3.70 | -85.84 | — |
| 2026-09-04 | `NVAX` | 119 | $10.34 | $10.50 | +19.04 | — | +0.00 | +19.04 | +9.52 | — |
| 2026-09-04 | `VSTM` | 154 | $7.98 | $7.91 | -10.78 | — | +0.00 | -10.78 | -18.48 | — |
| 2026-09-04 | `DFDV` | 209 | — | $5.79 | +0.00 | $5.87 | +16.72 | +16.72 | +0.00 | +16.72 |
| 2026-09-04 | `FRNM` | 74 | — | $16.40 | +0.00 | $16.31 | -6.66 | -6.66 | +0.00 | -6.66 |
| 2026-09-04 | `TARS` | 14 | — | $82.70 | +0.00 | $90.78 | +113.12 | +113.12 | +0.00 | +113.12 |
| 2026-09-04 | `BRR` | 483 | — | $2.51 | +0.00 | $2.66 | +72.45 | +72.45 | +0.00 | +72.45 |
| 2026-09-04 | `IRD` | 268 | — | $4.53 | +0.00 | $4.67 | +37.52 | +37.52 | +0.00 | +37.52 |
| 2026-09-04 | `LENZ` | 211 | — | $5.75 | +0.00 | $5.96 | +44.31 | +44.31 | +0.00 | +44.31 |
| 2026-09-04 | `ASST` | 48 | — | $25.18 | +0.00 | $27.14 | +94.08 | +94.08 | +0.00 | +94.08 |
| 2026-09-08 | `GPRO` | 699 | $1.70 | $1.56 | -94.37 | — | +0.00 | -94.37 | -150.29 | — |
| 2026-09-08 | `DFDV` | 209 | $5.87 | $5.81 | -12.54 | — | +0.00 | -12.54 | +4.18 | — |
| 2026-09-08 | `FRNM` | 74 | $16.31 | $16.74 | +31.82 | — | +0.00 | +31.82 | +25.16 | — |
| 2026-09-08 | `TARS` | 14 | $90.78 | $89.67 | -15.54 | — | +0.00 | -15.54 | +97.58 | — |
| 2026-09-08 | `BRR` | 483 | $2.66 | $2.66 | +0.00 | — | +0.00 | +0.00 | +72.45 | — |
| 2026-09-08 | `IRD` | 268 | $4.67 | $4.53 | -37.52 | — | +0.00 | -37.52 | +0.00 | — |
| 2026-09-08 | `LENZ` | 211 | $5.96 | $5.95 | -2.11 | — | +0.00 | -2.11 | +42.20 | — |
| 2026-09-08 | `ASST` | 48 | $27.14 | $26.44 | -33.60 | — | +0.00 | -33.60 | +60.48 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +300.75 | IREN, TNDM, INO, TPG, HIMS, SLS, VOR, BTSG | — | $107.38 | $10,268.71 | IREN×27, TNDM×53, INO×1543, TPG×24, HIMS×42, SLS×106, VOR×56, BTSG×20 |
| 2026-08-14 | +5.50 | $107.38 | IREN×27, TNDM×53, INO×1543, TPG×24, HIMS×42, SLS×106, VOR×56, BTSG×20 | $10,312.70 | +43.99 | -634.90 | QMCO, ZENA, ARX, AIRO, BZAI, BRUN, LIFE, LUNR | IREN, TNDM, INO, TPG, HIMS, SLS, VOR, BTSG | $32.92 | $9,605.00 | QMCO×52, ZENA×583, ARX×65, AIRO×115, BZAI×1677, BRUN×48, LIFE×36, LUNR×67 |
| 2026-08-17 | +2.25 | $32.92 | QMCO×52, ZENA×583, ARX×65, AIRO×115, BZAI×1677, BRUN×48, LIFE×36, LUNR×67 | $9,523.77 | -81.23 | -267.07 | XHG, CAPR, STDN, HTFL, UMAC, ALOY, LPTH, NPWR | QMCO, ZENA, ARX, AIRO, BZAI, BRUN, LIFE, LUNR | $58.96 | $9,196.28 | XHG×283, CAPR×172, STDN×86, HTFL×28, UMAC×36, ALOY×80, LPTH×79, NPWR×617 |
| 2026-08-18 | -6.20 | $58.96 | XHG×283, CAPR×172, STDN×86, HTFL×28, UMAC×36, ALOY×80, LPTH×79, NPWR×617 | $9,010.77 | -185.51 | -72.24 | — | XHG, STDN, HTFL, UMAC, ALOY, LPTH, NPWR | $7,698.01 | $8,915.77 | CAPR×172 |
| 2026-08-19 | -7.20 | $7,698.01 | CAPR×172 | $8,934.69 | +18.92 | +0.00 | — | CAPR | $8,932.14 | $8,932.14 | — |
| 2026-08-20 | +1.12 | $8,932.14 | — | $8,932.14 | +0.00 | +57.71 | MRNA, CYPH, ABCL, SENS, AUTL, TEM, WPM, IAG | — | $169.66 | $8,958.65 | MRNA×7, CYPH×970, ABCL×94, SENS×125, AUTL×452, TEM×18, WPM×7, IAG×56 |
| 2026-08-21 | +3.25 | $169.66 | MRNA×7, CYPH×970, ABCL×94, SENS×125, AUTL×452, TEM×18, WPM×7, IAG×56 | $9,190.07 | +231.42 | +279.56 | XHG, ARCT, IOVA, MRVI, AU, CAPR | ABCL, SENS, AUTL, TEM, WPM, IAG | $85.22 | $9,437.82 | MRNA×7, CYPH×970, XHG×258, ARCT×104, IOVA×127, MRVI×140, AU×9, CAPR×170 |
| 2026-08-24 | -5.17 | $85.22 | MRNA×7, CYPH×970, XHG×258, ARCT×104, IOVA×127, MRVI×140, AU×9, CAPR×170 | $10,038.55 | +600.73 | +0.00 | — | MRNA, CYPH, XHG, ARCT, IOVA, MRVI, AU, CAPR | $10,008.70 | $10,008.70 | — |
| 2026-08-25 | +1.80 | $10,008.70 | — | $10,008.70 | -0.00 | +572.22 | REAX, CYPH, XHG, ALVO, ASST, GORO, EZPW, BMEA | — | $30.53 | $10,542.69 | REAX×51, CYPH×801, XHG×307, ALVO×238, ASST×65, GORO×352, EZPW×35, BMEA×767 |
| 2026-08-26 | +2.02 | $30.53 | REAX×51, CYPH×801, XHG×307, ALVO×238, ASST×65, GORO×352, EZPW×35, BMEA×767 | $10,293.58 | -249.11 | +106.20 | BYND, USDE, FIGR, FUTU, TIGR, FNV, BTG | REAX, CYPH, ALVO, ASST, GORO, EZPW, BMEA | $276.25 | $10,347.69 | XHG×307, BYND×92, USDE×223, FIGR×32, FUTU×10, TIGR×249, FNV×4, BTG×225 |
| 2026-08-27 | — | $276.25 | XHG×307, BYND×92, USDE×223, FIGR×32, FUTU×10, TIGR×249, FNV×4, BTG×225 | $10,481.19 | +133.50 | -28.51 | SLI, PGY, GEN, MRVL, ANET, MOS, MU, GGB | XHG, BYND, USDE, FIGR, FUTU, TIGR, FNV, BTG | $469.44 | $10,408.46 | SLI×502, PGY×57, GEN×43, MRVL×5, ANET×6, MOS×54, MU×1, GGB×286 |
| 2026-08-28 | +0.75 | $469.44 | SLI×502, PGY×57, GEN×43, MRVL×5, ANET×6, MOS×54, MU×1, GGB×286 | $10,323.78 | -84.68 | -181.65 | BYND, CAPR, ANF, NCNO, MEI, VYX, SMTC, SNPS | SLI, PGY, GEN, MRVL, ANET, MOS, MU, GGB | $508.00 | $10,101.80 | BYND×91, CAPR×132, ANF×8, NCNO×55, MEI×72, VYX×141, SMTC×9, SNPS×2 |
| 2026-08-31 | -5.85 | $508.00 | BYND×91, CAPR×132, ANF×8, NCNO×55, MEI×72, VYX×141, SMTC×9, SNPS×2 | $10,043.71 | -58.09 | -46.41 | — | CAPR, ANF, NCNO, MEI, VYX, SMTC, SNPS | $8,771.64 | $9,981.94 | BYND×91 |
| 2026-09-01 | -6.30 | $8,771.64 | BYND×91 | $9,958.28 | -23.66 | +0.00 | — | BYND | $9,956.00 | $9,956.00 | — |
| 2026-09-02 | -3.83 | $9,956.00 | — | $9,956.00 | -0.00 | +0.00 | — | — | $9,956.00 | $9,956.00 | — |
| 2026-09-03 | -0.90 | $9,956.00 | — | $9,956.00 | -0.00 | -413.14 | GPRO, REAX, BMEA, MMED, AGCO, ARCT, NVAX, VSTM | — | $94.84 | $9,512.17 | GPRO×699, REAX×67, BMEA×644, MMED×52, AGCO×9, ARCT×74, NVAX×119, VSTM×154 |
| 2026-09-04 | +2.25 | $94.84 | GPRO×699, REAX×67, BMEA×644, MMED×52, AGCO×9, ARCT×74, NVAX×119, VSTM×154 | $9,558.45 | +46.28 | +525.32 | DFDV, FRNM, TARS, BRR, IRD, LENZ, ASST | REAX, BMEA, MMED, AGCO, ARCT, NVAX, VSTM | $50.73 | $10,040.34 | GPRO×699, DFDV×209, FRNM×74, TARS×14, BRR×483, IRD×268, LENZ×211, ASST×48 |
| 2026-09-08 | -11.47 | $50.73 | GPRO×699, DFDV×209, FRNM×74, TARS×14, BRR×483, IRD×268, LENZ×211, ASST×48 | $9,876.49 | -163.85 | +0.00 | — | GPRO, DFDV, FRNM, TARS, BRR, IRD, LENZ, ASST | $9,845.56 | $9,845.56 | — |
| 2026-09-09 | -13.95 | $9,845.56 | — | $9,845.56 | +0.00 | +0.00 | — | — | $9,845.56 | $9,845.56 | — |
| 2026-09-10 | -13.28 | $9,845.56 | — | $9,845.56 | +0.00 | +0.00 | — | — | $9,845.56 | $9,845.56 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $8,756.47 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $7,517.83 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $6,250.87 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $5,033.85 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $3,782.66 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $2,540.15 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 56 | $22.01 | $2.16 | — | $1,305.43 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $107.38 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $107.38 | ▲ close $10,268.71 vs 09:30 $10,000.00 (session +300.75) | 16:00 close · cash $107.38 · equity $10,268.71 vs 09:30 $10,000.00 (+268.71; session marks +300.75) · 8 name(s) marked open→close (per-name table). IREN×27 09:30 $45.98 → close $44.76 -32.94; TNDM×53 09:30 $23.33 → close $23.13 -10.60; INO×1543 09:30 $0.81 → close $0.90 +138.87; TPG×24 09:30 $50.62 → close $54.62 +95.92; HIMS×42 09:30 $29.74 → close $28.77 -40.74; SLS×106 09:30 $11.70 → close $12.36 +69.96; VOR×56 09:30 $22.01 → close $23.29 +71.68; BTSG×20 09:30 $59.80 → close $60.23 +8.60 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $107.38 | ▲ 09:30 equity $10,312.70 vs yday $10,268.71 (+43.99) | 09:30 open · cash $107.38 (unchanged overnight, no fees) · equity $10,312.70 vs prior close $10,268.71 (+43.99) · 8 name(s) re-marked at the open (per-name table). IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; VOR×56 yday $23.29 → 09:30 $23.33 +2.24; BTSG×20 yday $60.23 → 09:30 $59.65 -11.60 | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $1,295.72 | ▼ -55.19 after sell → book $10,310.61; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $2,508.31 | ▼ -26.05 after sell → book $10,308.44; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $3,924.06 | ▲ +148.79 after sell → book $10,289.20; vs 09:30 mark -19.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $5,248.93 | ▲ +107.86 after sell → book $10,287.11; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $6,471.10 | ▼ -29.03 after sell → book $10,284.98; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $7,783.16 | ▲ +69.56 after sell → book $10,282.64; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 56 | $23.33 | $2.18 | $+69.58 | $9,087.46 | ▲ +69.58 after sell → book $10,280.46; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $10,278.39 | ▼ -7.12 after sell → book $10,278.39; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 52 | $24.68 | $2.15 | — | $8,992.89 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+111.3; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 583 | $2.20 | $7.52 | — | $7,702.77 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 65 | $19.57 | $2.19 | — | $6,428.53 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 115 | $11.12 | $2.33 | — | $5,147.40 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BZAI` | 1677 | $0.77 | $17.88 | — | $3,844.94 | — | rank by w_hot_cond; rank w_hot_cond; list earn_react; 🔵; ⚪; ret5=+20.4; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BRUN` | 48 | $26.25 | $2.13 | — | $2,583.04 | — | rank by w_hot_cond; rank w_hot_cond; list earn_react; 🔵; ⚪; ret5=+31.2; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LIFE` | 36 | $35.04 | $2.10 | — | $1,319.50 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+16.7; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 67 | $19.17 | $2.19 | — | $32.92 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🔴 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.92 | ▼ close $9,605.00 vs 09:30 $10,312.70 (session -634.90) | 16:00 close · cash $32.92 · equity $9,605.00 vs 09:30 $10,312.70 (-707.70; session marks -634.90) · 8 name(s) marked open→close (per-name table). QMCO×52 09:30 $24.68 → close $26.11 +74.36; ZENA×583 09:30 $2.20 → close $2.14 -34.98; ARX×65 09:30 $19.57 → close $19.58 +0.65; AIRO×115 09:30 $11.12 → close $9.57 -178.25; BZAI×1677 09:30 $0.77 → close $0.59 -290.12; BRUN×48 09:30 $26.25 → close $22.93 -159.12; LIFE×36 09:30 $35.04 → close $34.02 -36.72; LUNR×67 09:30 $19.17 → close $19.01 -10.72 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.92 | ▼ 09:30 equity $9,523.77 vs yday $9,605.00 (-81.23) | 09:30 open · cash $32.92 (unchanged overnight, no fees) · equity $9,523.77 vs prior close $9,605.00 (-81.23) · 8 name(s) re-marked at the open (per-name table). QMCO×52 yday $26.11 → 09:30 $24.83 -66.56; ZENA×583 yday $2.14 → 09:30 $2.08 -32.07; ARX×65 yday $19.58 → 09:30 $19.57 -0.65; AIRO×115 yday $9.57 → 09:30 $9.57 +0.00; BZAI×1677 yday $0.59 → 09:30 $0.55 -68.76; BRUN×48 yday $22.93 → 09:30 $23.00 +3.36; LIFE×36 yday $34.02 → 09:30 $34.03 +0.36; LUNR×67 yday $19.01 → 09:30 $20.25 +83.08 | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 52 | $24.83 | $2.17 | $+3.49 | $1,321.92 | ▲ +3.49 after sell → book $9,521.61; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 583 | $2.08 | $7.63 | $-82.19 | $2,529.84 | ▼ -82.19 after sell → book $9,513.98; vs 09:30 mark -7.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 65 | $19.57 | $2.21 | $-4.39 | $3,799.69 | ▼ -4.39 after sell → book $9,511.77; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 115 | $9.57 | $2.36 | $-182.95 | $4,897.87 | ▼ -182.95 after sell → book $9,509.41; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BZAI` | 1677 | $0.55 | $14.58 | $-391.33 | $5,809.00 | ▼ -391.33 after sell → book $9,494.83; vs 09:30 mark -14.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BRUN` | 48 | $23.00 | $2.15 | $-160.05 | $6,910.85 | ▼ -160.05 after sell → book $9,492.68; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LIFE` | 36 | $34.03 | $2.12 | $-40.58 | $8,133.81 | ▼ -40.58 after sell → book $9,490.56; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LUNR` | 67 | $20.25 | $2.21 | $+67.96 | $9,488.35 | ▲ +67.96 after sell → book $9,488.35; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 283 | $4.19 | $3.65 | — | $8,298.93 | — | rank by w_hot_cond; rank w_hot_cond; list yday_mover; ⚪; ret5=+291.8; leftover $1186.04 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 172 | $6.87 | $2.51 | — | $7,114.78 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+62.6; leftover $1186.04 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 86 | $13.64 | $2.25 | — | $5,939.49 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+57.7; leftover $1186.04 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 28 | $41.23 | $2.07 | — | $4,782.98 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+46.0; leftover $1186.04 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 36 | $32.55 | $2.10 | — | $3,609.08 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1186.04 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 80 | $14.66 | $2.23 | — | $2,434.05 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $1186.04 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `LPTH` | 79 | $14.94 | $2.23 | — | $1,251.56 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $1186.04 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 617 | $1.92 | $7.96 | — | $58.96 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1186.04 | join🟡 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $58.96 | ▼ close $9,196.28 vs 09:30 $9,523.77 (session -267.07) | 16:00 close · cash $58.96 · equity $9,196.28 vs 09:30 $9,523.77 (-327.49; session marks -267.07) · 8 name(s) marked open→close (per-name table). XHG×283 09:30 $4.19 → close $3.91 -79.24; CAPR×172 09:30 $6.87 → close $7.45 +99.76; STDN×86 09:30 $13.64 → close $13.31 -28.38; HTFL×28 09:30 $41.23 → close $41.94 +19.88; UMAC×36 09:30 $32.55 → close $30.15 -86.40; ALOY×80 09:30 $14.66 → close $13.86 -64.40; LPTH×79 09:30 $14.94 → close $14.80 -11.06; NPWR×617 09:30 $1.92 → close $1.73 -117.23 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $58.96 | ▼ 09:30 equity $9,010.77 vs yday $9,196.28 (-185.51) | 09:30 open · cash $58.96 (unchanged overnight, no fees) · equity $9,010.77 vs prior close $9,196.28 (-185.51) · 8 name(s) re-marked at the open (per-name table). XHG×283 yday $3.91 → 09:30 $3.94 +8.49; CAPR×172 yday $7.45 → 09:30 $7.50 +8.60; STDN×86 yday $13.31 → 09:30 $13.31 +0.00; HTFL×28 yday $41.94 → 09:30 $41.50 -12.32; UMAC×36 yday $30.15 → 09:30 $28.59 -56.16; ALOY×80 yday $13.86 → 09:30 $13.19 -53.20; LPTH×79 yday $14.80 → 09:30 $14.01 -62.41; NPWR×617 yday $1.73 → 09:30 $1.70 -18.51 | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 283 | $3.94 | $3.71 | $-78.11 | $1,170.28 | ▼ -78.11 after sell → book $9,007.07; vs 09:30 mark -3.70 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 86 | $13.31 | $2.27 | $-32.90 | $2,312.66 | ▼ -32.90 after sell → book $9,004.79; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 28 | $41.50 | $2.09 | $+3.39 | $3,472.57 | ▲ +3.39 after sell → book $9,002.70; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 36 | $28.59 | $2.12 | $-146.78 | $4,499.69 | ▼ -146.78 after sell → book $9,000.58; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 80 | $13.19 | $2.25 | $-122.08 | $5,552.64 | ▼ -122.08 after sell → book $8,998.33; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `LPTH` | 79 | $14.01 | $2.25 | $-77.95 | $6,657.18 | ▼ -77.95 after sell → book $8,996.08; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 617 | $1.70 | $8.07 | $-151.77 | $7,698.01 | ▼ -151.77 after sell → book $8,988.01; vs 09:30 mark -8.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,698.01 | ▼ close $8,915.77 vs 09:30 $9,010.77 (session -72.24) | 16:00 close · cash $7,698.01 · equity $8,915.77 vs 09:30 $9,010.77 (-95.00; session marks -72.24) · 1 name(s) marked open→close (per-name table). CAPR×172 09:30 $7.50 → close $7.08 -72.24 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,698.01 | ▲ 09:30 equity $8,934.69 vs yday $8,915.77 (+18.92) | 09:30 open · cash $7,698.01 (unchanged overnight, no fees) · equity $8,934.69 vs prior close $8,915.77 (+18.92) · 1 name(s) re-marked at the open (per-name table). CAPR×172 yday $7.08 → 09:30 $7.19 +18.92 | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 172 | $7.19 | $2.54 | $+49.99 | $8,932.14 | ▲ +49.99 after sell → book $8,932.14; vs 09:30 mark -2.55 | dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,932.14 | ▲ close $8,932.14 vs 09:30 $8,934.69 (session +0.00) | 16:00 close · cash $8,932.14 · no lots left · equity $8,932.14. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,932.14 | ▲ 09:30 equity $8,932.14 vs yday $8,932.14 (+0.00) | 09:30 open · cash $8,932.14 · no holdings · equity $8,932.14 vs prior close $8,932.14 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 7 | $150.14 | $2.01 | — | $7,879.15 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1116.52 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 970 | $1.15 | $12.51 | — | $6,751.14 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1116.52 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 94 | $11.81 | $2.27 | — | $5,638.26 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1116.52 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SENS` | 125 | $8.91 | $2.37 | — | $4,522.14 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1116.52 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 452 | $2.47 | $5.83 | — | $3,399.87 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1116.52 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TEM` | 18 | $61.83 | $2.04 | — | $2,284.89 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $1116.52 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 7 | $144.54 | $2.01 | — | $1,271.10 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1116.52 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 56 | $19.63 | $2.16 | — | $169.66 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1116.52 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $169.66 | ▲ close $8,958.65 vs 09:30 $8,932.14 (session +57.71) | 16:00 close · cash $169.66 · equity $8,958.65 vs 09:30 $8,932.14 (+26.51; session marks +57.71) · 8 name(s) marked open→close (per-name table). MRNA×7 09:30 $150.14 → close $133.32 -117.74; CYPH×970 09:30 $1.15 → close $1.19 +38.80; ABCL×94 09:30 $11.81 → close $11.57 -23.03; SENS×125 09:30 $8.91 → close $8.82 -11.25; AUTL×452 09:30 $2.47 → close $2.46 -4.52; TEM×18 09:30 $61.83 → close $66.65 +86.76; WPM×7 09:30 $144.54 → close $150.25 +39.97; IAG×56 09:30 $19.63 → close $20.50 +48.72 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $169.66 | ▲ 09:30 equity $9,190.07 vs yday $8,958.65 (+231.42) | 09:30 open · cash $169.66 (unchanged overnight, no fees) · equity $9,190.07 vs prior close $8,958.65 (+231.42) · 8 name(s) re-marked at the open (per-name table). MRNA×7 yday $133.32 → 09:30 $133.11 -1.47; CYPH×970 yday $1.19 → 09:30 $1.32 +126.10; ABCL×94 yday $11.57 → 09:30 $11.57 +0.00; SENS×125 yday $8.82 → 09:30 $9.24 +52.50; AUTL×452 yday $2.46 → 09:30 $2.47 +4.52; TEM×18 yday $66.65 → 09:30 $65.60 -18.90; WPM×7 yday $150.25 → 09:30 $154.70 +31.15; IAG×56 yday $20.50 → 09:30 $21.17 +37.52 | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 94 | $11.57 | $2.30 | $-27.60 | $1,254.94 | ▼ -27.60 after sell → book $9,187.77; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SENS` | 125 | $9.24 | $2.40 | $+36.49 | $2,407.54 | ▲ +36.49 after sell → book $9,185.37; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `AUTL` | 452 | $2.47 | $5.92 | $-11.75 | $3,518.07 | ▼ -11.75 after sell → book $9,179.46; vs 09:30 mark -5.91 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `TEM` | 18 | $65.60 | $2.06 | $+63.75 | $4,696.80 | ▲ +63.75 after sell → book $9,177.39; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 7 | $154.70 | $2.03 | $+67.08 | $5,777.67 | ▲ +67.08 after sell → book $9,175.36; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 56 | $21.17 | $2.18 | $+81.90 | $6,961.02 | ▲ +81.90 after sell → book $9,173.19; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 258 | $4.49 | $3.33 | — | $5,799.27 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+12.7; leftover $1160.17 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 104 | $11.13 | $2.30 | — | $4,639.45 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1160.17 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 127 | $9.08 | $2.37 | — | $3,483.91 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $1160.17 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 140 | $8.28 | $2.41 | — | $2,322.30 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; leftover $1160.17 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 9 | $119.43 | $2.02 | — | $1,245.42 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1160.17 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 170 | $6.81 | $2.50 | — | $85.22 | — | rank by w_hot_cond; rank w_hot_cond; list yday_mover; 🔵; ret5=+62.5; leftover $1160.17 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $85.22 | ▲ close $9,437.82 vs 09:30 $9,190.07 (session +279.56) | 16:00 close · cash $85.22 · equity $9,437.82 vs 09:30 $9,190.07 (+247.75; session marks +279.56) · 8 name(s) marked open→close (per-name table). MRNA×7 09:30 $133.11 → close $145.13 +84.14; CYPH×970 09:30 $1.32 → close $1.42 +97.00; XHG×258 09:30 $4.49 → close $4.41 -20.64; ARCT×104 09:30 $11.13 → close $13.45 +241.28; IOVA×127 09:30 $9.08 → close $8.29 -100.33; MRVI×140 09:30 $8.28 → close $8.64 +50.40; AU×9 09:30 $119.43 → close $121.22 +16.11; CAPR×170 09:30 $6.81 → close $6.29 -88.40 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.22 | ▲ 09:30 equity $10,038.55 vs yday $9,437.82 (+600.73) | 09:30 open · cash $85.22 (unchanged overnight, no fees) · equity $10,038.55 vs prior close $9,437.82 (+600.73) · 8 name(s) re-marked at the open (per-name table). MRNA×7 yday $145.13 → 09:30 $142.70 -17.01; CYPH×970 yday $1.42 → 09:30 $1.83 +397.70; XHG×258 yday $4.41 → 09:30 $4.32 -23.22; ARCT×104 yday $13.45 → 09:30 $13.33 -12.48; IOVA×127 yday $8.29 → 09:30 $8.08 -26.67; MRVI×140 yday $8.64 → 09:30 $8.59 -7.00; AU×9 yday $121.22 → 09:30 $120.51 -6.39; CAPR×170 yday $6.29 → 09:30 $8.03 +295.80 | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 7 | $142.70 | $2.03 | $-56.12 | $1,082.09 | ▼ -56.12 after sell → book $10,036.52; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 970 | $1.83 | $12.69 | $+634.40 | $2,844.50 | ▲ +634.40 after sell → book $10,023.83; vs 09:30 mark -12.69 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 258 | $4.32 | $3.38 | $-50.57 | $3,955.68 | ▼ -50.57 after sell → book $10,020.45; vs 09:30 mark -3.38 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 104 | $13.33 | $2.33 | $+224.17 | $5,339.67 | ▲ +224.17 after sell → book $10,018.12; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `IOVA` | 127 | $8.08 | $2.40 | $-131.77 | $6,363.42 | ▼ -131.77 after sell → book $10,015.71; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 140 | $8.59 | $2.44 | $+38.55 | $7,563.58 | ▲ +38.55 after sell → book $10,013.27; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 9 | $120.51 | $2.04 | $+5.67 | $8,646.13 | ▲ +5.67 after sell → book $10,011.23; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 170 | $8.03 | $2.54 | $+202.36 | $10,008.70 | ▲ +202.36 after sell → book $10,008.70; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,008.70 | ▲ close $10,008.70 vs 09:30 $10,038.55 (session +0.00) | 16:00 close · cash $10,008.70 · no lots left · equity $10,008.70. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,008.70 | ▲ 09:30 equity $10,008.70 vs yday $10,008.70 (-0.00) | 09:30 open · cash $10,008.70 · no holdings · equity $10,008.70 vs prior close $10,008.70 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 51 | $24.11 | $2.14 | — | $8,776.94 | — | rank by w_hot_cond; rank w_hot_cond; list yday_mover; ret5=+891.7; leftover $1251.09 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 801 | $1.56 | $10.33 | — | $7,517.05 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $1251.09 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 307 | $4.07 | $3.96 | — | $6,263.60 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $1251.09 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 238 | $5.24 | $3.07 | — | $5,013.41 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1251.09 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 65 | $19.04 | $2.19 | — | $3,773.62 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ret5=+49.5; leftover $1251.09 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 352 | $3.55 | $4.54 | — | $2,519.48 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+27.9; leftover $1251.09 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 35 | $35.05 | $2.10 | — | $1,290.64 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1251.09 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 767 | $1.63 | $9.89 | — | $30.53 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1251.09 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.53 | ▲ close $10,542.69 vs 09:30 $10,008.70 (session +572.22) | 16:00 close · cash $30.53 · equity $10,542.69 vs 09:30 $10,008.70 (+533.99; session marks +572.22) · 8 name(s) marked open→close (per-name table). REAX×51 09:30 $24.11 → close $28.43 +220.32; CYPH×801 09:30 $1.56 → close $1.64 +64.08; XHG×307 09:30 $4.07 → close $4.02 -15.35; ALVO×238 09:30 $5.24 → close $5.05 -45.22; ASST×65 09:30 $19.04 → close $21.39 +152.75; GORO×352 09:30 $3.55 → close $3.87 +112.64; EZPW×35 09:30 $35.05 → close $35.23 +6.30; BMEA×767 09:30 $1.63 → close $1.73 +76.70 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.53 | ▼ 09:30 equity $10,293.58 vs yday $10,542.69 (-249.11) | 09:30 open · cash $30.53 (unchanged overnight, no fees) · equity $10,293.58 vs prior close $10,542.69 (-249.11) · 8 name(s) re-marked at the open (per-name table). REAX×51 yday $28.43 → 09:30 $26.61 -92.82; CYPH×801 yday $1.64 → 09:30 $1.60 -32.04; XHG×307 yday $4.02 → 09:30 $3.81 -64.47; ALVO×238 yday $5.05 → 09:30 $4.98 -16.66; ASST×65 yday $21.39 → 09:30 $20.72 -43.55; GORO×352 yday $3.87 → 09:30 $3.77 -35.20; EZPW×35 yday $35.23 → 09:30 $35.70 +16.45; BMEA×767 yday $1.73 → 09:30 $1.75 +19.17 | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 51 | $26.61 | $2.16 | $+123.19 | $1,385.48 | ▲ +123.19 after sell → book $10,291.42; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 801 | $1.60 | $10.48 | $+11.23 | $2,656.60 | ▲ +11.23 after sell → book $10,280.94; vs 09:30 mark -10.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ALVO` | 238 | $4.98 | $3.12 | $-68.07 | $3,838.72 | ▼ -68.07 after sell → book $10,277.82; vs 09:30 mark -3.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 65 | $20.72 | $2.21 | $+104.81 | $5,183.32 | ▲ +104.81 after sell → book $10,275.61; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GORO` | 352 | $3.77 | $4.61 | $+68.29 | $6,505.75 | ▲ +68.29 after sell → book $10,271.00; vs 09:30 mark -4.61 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 35 | $35.70 | $2.12 | $+18.54 | $7,753.13 | ▲ +18.54 after sell → book $10,268.89; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 767 | $1.75 | $10.03 | $+75.95 | $9,089.19 | ▲ +75.95 after sell → book $10,258.86; vs 09:30 mark -10.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 92 | $14.11 | $2.27 | — | $7,788.80 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+11.4; leftover $1298.46 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 223 | $5.81 | $2.88 | — | $6,490.29 | — | rank by w_hot_cond; rank w_hot_cond; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $1298.46 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FIGR` | 32 | $40.50 | $2.09 | — | $5,192.21 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+15.8; leftover $1298.46 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **BUY** | `FUTU` | 10 | $124.67 | $2.02 | — | $3,943.49 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+15.7; leftover $1298.46 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 249 | $5.21 | $3.21 | — | $2,642.98 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot,earn_react; 🔵; ret5=+14.3; leftover $1298.46 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 4 | $267.02 | $2.00 | — | $1,572.90 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $1298.46 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BTG` | 225 | $5.75 | $2.90 | — | $276.25 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+17.9; leftover $1298.46 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $276.25 | ▲ close $10,347.69 vs 09:30 $10,293.58 (session +106.20) | 16:00 close · cash $276.25 · equity $10,347.69 vs 09:30 $10,293.58 (+54.11; session marks +106.20) · 8 name(s) marked open→close (per-name table). XHG×307 09:30 $3.81 → close $4.06 +76.75; BYND×92 09:30 $14.11 → close $14.25 +12.88; USDE×223 09:30 $5.81 → close $5.98 +37.91; FIGR×32 09:30 $40.50 → close $37.08 -109.44; FUTU×10 09:30 $124.67 → close $127.34 +26.70; TIGR×249 09:30 $5.21 → close $5.46 +62.25; FNV×4 09:30 $267.02 → close $267.37 +1.40; BTG×225 09:30 $5.75 → close $5.74 -2.25 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $276.25 | ▲ 09:30 equity $10,481.19 vs yday $10,347.69 (+133.50) | 09:30 open · cash $276.25 (unchanged overnight, no fees) · equity $10,481.19 vs prior close $10,347.69 (+133.50) · 8 name(s) re-marked at the open (per-name table). XHG×307 yday $4.06 → 09:30 $4.06 +0.00; BYND×92 yday $14.25 → 09:30 $14.20 -4.60; USDE×223 yday $5.98 → 09:30 $6.50 +115.96; FIGR×32 yday $37.08 → 09:30 $37.42 +10.88; FUTU×10 yday $127.34 → 09:30 $128.00 +6.60; TIGR×249 yday $5.46 → 09:30 $5.49 +7.47; FNV×4 yday $267.37 → 09:30 $267.23 -0.56; BTG×225 yday $5.74 → 09:30 $5.73 -2.25 | — |
| 2026-08-27 09:30 ET | **SELL** | `XHG` | 307 | $4.06 | $4.02 | $-11.05 | $1,518.65 | ▼ -11.05 after sell → book $10,477.17; vs 09:30 mark -4.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 92 | $14.20 | $2.29 | $+3.72 | $2,822.76 | ▲ +3.72 after sell → book $10,474.88; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 223 | $6.50 | $2.93 | $+148.07 | $4,269.33 | ▲ +148.07 after sell → book $10,471.95; vs 09:30 mark -2.93 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FIGR` | 32 | $37.42 | $2.11 | $-102.75 | $5,464.67 | ▼ -102.75 after sell → book $10,469.85; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FUTU` | 10 | $128.00 | $2.04 | $+29.24 | $6,742.63 | ▲ +29.24 after sell → book $10,467.81; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TIGR` | 249 | $5.49 | $3.26 | $+63.24 | $8,106.37 | ▲ +63.24 after sell → book $10,464.54; vs 09:30 mark -3.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 4 | $267.23 | $2.02 | $-3.18 | $9,173.27 | ▼ -3.18 after sell → book $10,462.52; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BTG` | 225 | $5.73 | $2.95 | $-10.35 | $10,459.57 | ▼ -10.35 after sell → book $10,459.57; vs 09:30 mark -2.95 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 502 | $2.60 | $6.48 | — | $9,147.89 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ret5=+13.0; leftover $1307.45 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PGY` | 57 | $22.93 | $2.16 | — | $7,838.72 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=+9.5; leftover $1307.45 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 43 | $29.83 | $2.12 | — | $6,553.91 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=+7.6; leftover $1307.45 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 5 | $253.44 | $2.00 | — | $5,284.71 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=+3.3; leftover $1307.45 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ANET` | 6 | $205.90 | $2.01 | — | $4,047.30 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=+8.5; leftover $1307.45 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 54 | $24.00 | $2.15 | — | $2,749.15 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ret5=+8.7; leftover $1307.45 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $1,780.15 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=+0.1; leftover $1307.45 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 286 | $4.57 | $3.69 | — | $469.44 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=+1.1; leftover $1307.45 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $469.44 | ▼ close $10,408.46 vs 09:30 $10,481.19 (session -28.51) | 16:00 close · cash $469.44 · equity $10,408.46 vs 09:30 $10,481.19 (-72.73; session marks -28.51) · 8 name(s) marked open→close (per-name table). SLI×502 09:30 $2.60 → close $2.64 +20.08; PGY×57 09:30 $22.93 → close $23.26 +18.81; GEN×43 09:30 $29.83 → close $30.50 +28.81; MRVL×5 09:30 $253.44 → close $241.45 -59.95; ANET×6 09:30 $205.90 → close $201.09 -28.86; MOS×54 09:30 $24.00 → close $23.76 -12.96; MU×1 09:30 $967.01 → close $935.39 -31.62; GGB×286 09:30 $4.57 → close $4.70 +37.18 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $469.44 | ▼ 09:30 equity $10,323.78 vs yday $10,408.46 (-84.68) | 09:30 open · cash $469.44 (unchanged overnight, no fees) · equity $10,323.78 vs prior close $10,408.46 (-84.68) · 8 name(s) re-marked at the open (per-name table). SLI×502 yday $2.64 → 09:30 $2.68 +20.08; PGY×57 yday $23.26 → 09:30 $23.21 -2.85; GEN×43 yday $30.50 → 09:30 $30.50 +0.00; MRVL×5 yday $241.45 → 09:30 $225.26 -80.95; ANET×6 yday $201.09 → 09:30 $200.00 -6.54; MOS×54 yday $23.76 → 09:30 $23.95 +10.26; MU×1 yday $935.39 → 09:30 $919.29 -16.10; GGB×286 yday $4.70 → 09:30 $4.67 -8.58 | — |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 502 | $2.68 | $6.57 | $+27.11 | $1,808.23 | ▲ +27.11 after sell → book $10,317.21; vs 09:30 mark -6.57 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `PGY` | 57 | $23.21 | $2.18 | $+11.62 | $3,129.01 | ▲ +11.62 after sell → book $10,315.02; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 43 | $30.50 | $2.14 | $+24.55 | $4,438.37 | ▲ +24.55 after sell → book $10,312.88; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MRVL` | 5 | $225.26 | $2.02 | $-144.93 | $5,562.65 | ▼ -144.93 after sell → book $10,310.86; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `ANET` | 6 | $200.00 | $2.03 | $-39.44 | $6,760.62 | ▼ -39.44 after sell → book $10,308.83; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 54 | $23.95 | $2.17 | $-7.02 | $8,051.75 | ▼ -7.02 after sell → book $10,306.66; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $8,969.03 | ▼ -51.73 after sell → book $10,304.65; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 286 | $4.67 | $3.75 | $+21.16 | $10,300.90 | ▲ +21.16 after sell → book $10,300.90; vs 09:30 mark -3.75 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 91 | $14.00 | $2.26 | — | $9,024.64 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=-3.3; leftover $1287.61 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 132 | $9.73 | $2.39 | — | $7,737.89 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+47.1; leftover $1287.61 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $146.07 | $2.01 | — | $6,567.32 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1287.61 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 55 | $23.30 | $2.15 | — | $5,283.66 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+14.5; leftover $1287.61 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MEI` | 72 | $17.78 | $2.21 | — | $4,001.29 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ⚪; ret5=+22.9; leftover $1287.61 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VYX` | 141 | $9.13 | $2.41 | — | $2,711.55 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ret5=+20.0; leftover $1287.61 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $1,433.69 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1287.61 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SNPS` | 2 | $461.85 | $2.00 | — | $508.00 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+16.8; leftover $1287.61 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $508.00 | ▼ close $10,101.80 vs 09:30 $10,323.78 (session -181.65) | 16:00 close · cash $508.00 · equity $10,101.80 vs 09:30 $10,323.78 (-221.98; session marks -181.65) · 8 name(s) marked open→close (per-name table). BYND×91 09:30 $14.00 → close $13.86 -12.74; CAPR×132 09:30 $9.73 → close $9.59 -18.48; ANF×8 09:30 $146.07 → close $148.42 +18.80; NCNO×55 09:30 $23.30 → close $22.99 -17.05; MEI×72 09:30 $17.78 → close $18.21 +30.96; VYX×141 09:30 $9.13 → close $8.78 -49.35; SMTC×9 09:30 $141.76 → close $131.17 -95.31; SNPS×2 09:30 $461.85 → close $442.61 -38.48 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $508.00 | ▼ 09:30 equity $10,043.71 vs yday $10,101.80 (-58.09) | 09:30 open · cash $508.00 (unchanged overnight, no fees) · equity $10,043.71 vs prior close $10,101.80 (-58.09) · 8 name(s) re-marked at the open (per-name table). BYND×91 yday $13.86 → 09:30 $13.81 -4.55; CAPR×132 yday $9.59 → 09:30 $9.50 -11.88; ANF×8 yday $148.42 → 09:30 $148.03 -3.12; NCNO×55 yday $22.99 → 09:30 $22.66 -18.15; MEI×72 yday $18.21 → 09:30 $18.15 -4.32; VYX×141 yday $8.78 → 09:30 $8.66 -16.92; SMTC×9 yday $131.17 → 09:30 $132.30 +10.17; SNPS×2 yday $442.61 → 09:30 $437.95 -9.32 | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 132 | $9.50 | $2.42 | $-35.16 | $1,759.58 | ▼ -35.16 after sell → book $10,041.29; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 8 | $148.03 | $2.03 | $+11.63 | $2,941.79 | ▲ +11.63 after sell → book $10,039.26; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NCNO` | 55 | $22.66 | $2.17 | $-39.53 | $4,185.91 | ▼ -39.53 after sell → book $10,037.08; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `MEI` | 72 | $18.15 | $2.23 | $+22.21 | $5,490.48 | ▲ +22.21 after sell → book $10,034.85; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `VYX` | 141 | $8.66 | $2.45 | $-71.13 | $6,709.10 | ▼ -71.13 after sell → book $10,032.41; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $7,897.76 | ▼ -89.19 after sell → book $10,030.37; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SNPS` | 2 | $437.95 | $2.02 | $-51.81 | $8,771.64 | ▼ -51.81 after sell → book $10,028.35; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,771.64 | ▼ close $9,981.94 vs 09:30 $10,043.71 (session -46.41) | 16:00 close · cash $8,771.64 · equity $9,981.94 vs 09:30 $10,043.71 (-61.77; session marks -46.41) · 1 name(s) marked open→close (per-name table). BYND×91 09:30 $13.81 → close $13.30 -46.41 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,771.64 | ▼ 09:30 equity $9,958.28 vs yday $9,981.94 (-23.66) | 09:30 open · cash $8,771.64 (unchanged overnight, no fees) · equity $9,958.28 vs prior close $9,981.94 (-23.66) · 1 name(s) re-marked at the open (per-name table). BYND×91 yday $13.30 → 09:30 $13.04 -23.66 | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 91 | $13.04 | $2.29 | $-91.91 | $9,956.00 | ▼ -91.91 after sell → book $9,956.00; vs 09:30 mark -2.28 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,956.00 | ▲ close $9,956.00 vs 09:30 $9,958.28 (session +0.00) | 16:00 close · cash $9,956.00 · no lots left · equity $9,956.00. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,956.00 | ▲ 09:30 equity $9,956.00 vs yday $9,956.00 (-0.00) | 09:30 open · cash $9,956.00 · no holdings · equity $9,956.00 vs prior close $9,956.00 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,956.00 | ▲ close $9,956.00 vs 09:30 $9,956.00 (session +0.00) | 16:00 close · cash $9,956.00 · no lots left · equity $9,956.00. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,956.00 | ▲ 09:30 equity $9,956.00 vs yday $9,956.00 (-0.00) | 09:30 open · cash $9,956.00 · no holdings · equity $9,956.00 vs prior close $9,956.00 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 699 | $1.78 | $9.02 | — | $8,702.76 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+183.1; leftover $1244.50 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 67 | $18.40 | $2.19 | — | $7,467.77 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=-32.2; leftover $1244.50 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 644 | $1.93 | $8.31 | — | $6,216.54 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1244.50 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 52 | $23.88 | $2.15 | — | $4,972.63 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1244.50 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AGCO` | 9 | $127.91 | $2.02 | — | $3,819.43 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.6; leftover $1244.50 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 74 | $16.77 | $2.21 | — | $2,576.24 | — | rank by w_hot_cond; rank w_hot_cond; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1244.50 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 119 | $10.42 | $2.35 | — | $1,333.91 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; leftover $1244.50 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 154 | $8.03 | $2.45 | — | $94.84 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1244.50 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $94.84 | ▼ close $9,512.17 vs 09:30 $9,956.00 (session -413.14) | 16:00 close · cash $94.84 · equity $9,512.17 vs 09:30 $9,956.00 (-443.83; session marks -413.14) · 8 name(s) marked open→close (per-name table). GPRO×699 09:30 $1.78 → close $1.39 -272.61; REAX×67 09:30 $18.40 → close $18.40 +0.00; BMEA×644 09:30 $1.93 → close $1.91 -12.88; MMED×52 09:30 $23.88 → close $23.84 -2.08; AGCO×9 09:30 $127.91 → close $125.82 -18.81; ARCT×74 09:30 $16.77 → close $15.56 -89.54; NVAX×119 09:30 $10.42 → close $10.34 -9.52; VSTM×154 09:30 $8.03 → close $7.98 -7.70 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $94.84 | ▲ 09:30 equity $9,558.45 vs yday $9,512.17 (+46.28) | 09:30 open · cash $94.84 (unchanged overnight, no fees) · equity $9,558.45 vs prior close $9,512.17 (+46.28) · 8 name(s) re-marked at the open (per-name table). GPRO×699 yday $1.39 → 09:30 $1.48 +62.91; REAX×67 yday $18.40 → 09:30 $18.15 -16.75; BMEA×644 yday $1.91 → 09:30 $1.90 -6.44; MMED×52 yday $23.84 → 09:30 $23.84 +0.00; AGCO×9 yday $125.82 → 09:30 $125.22 -5.40; ARCT×74 yday $15.56 → 09:30 $15.61 +3.70; NVAX×119 yday $10.34 → 09:30 $10.50 +19.04; VSTM×154 yday $7.98 → 09:30 $7.91 -10.78 | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 67 | $18.15 | $2.21 | $-21.15 | $1,308.67 | ▼ -21.15 after sell → book $9,556.23; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `BMEA` | 644 | $1.90 | $8.42 | $-36.05 | $2,523.85 | ▼ -36.05 after sell → book $9,547.81; vs 09:30 mark -8.42 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 52 | $23.84 | $2.17 | $-6.39 | $3,761.36 | ▼ -6.39 after sell → book $9,545.64; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AGCO` | 9 | $125.22 | $2.04 | $-28.26 | $4,886.31 | ▼ -28.26 after sell → book $9,543.61; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 74 | $15.61 | $2.23 | $-90.29 | $6,039.21 | ▼ -90.29 after sell → book $9,541.37; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 119 | $10.50 | $2.38 | $+4.80 | $7,286.34 | ▲ +4.80 after sell → book $9,539.00; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 154 | $7.91 | $2.49 | $-23.42 | $8,501.99 | ▼ -23.42 after sell → book $9,536.51; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 209 | $5.79 | $2.70 | — | $7,289.18 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $1214.57 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `FRNM` | 74 | $16.40 | $2.21 | — | $6,073.37 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ⚪; ret5=+21.2; leftover $1214.57 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 14 | $82.70 | $2.03 | — | $4,913.54 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+15.7; leftover $1214.57 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 483 | $2.51 | $6.23 | — | $3,694.98 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+21.8; leftover $1214.57 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 268 | $4.53 | $3.46 | — | $2,477.48 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.5; leftover $1214.57 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `LENZ` | 211 | $5.75 | $2.72 | — | $1,261.51 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+21.2; leftover $1214.57 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 48 | $25.18 | $2.13 | — | $50.73 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+16.0; leftover $1214.57 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $50.73 | ▲ close $10,040.34 vs 09:30 $9,558.45 (session +525.32) | 16:00 close · cash $50.73 · equity $10,040.34 vs 09:30 $9,558.45 (+481.89; session marks +525.32) · 8 name(s) marked open→close (per-name table). GPRO×699 09:30 $1.48 → close $1.70 +153.78; DFDV×209 09:30 $5.79 → close $5.87 +16.72; FRNM×74 09:30 $16.40 → close $16.31 -6.66; TARS×14 09:30 $82.70 → close $90.78 +113.12; BRR×483 09:30 $2.51 → close $2.66 +72.45; IRD×268 09:30 $4.53 → close $4.67 +37.52; LENZ×211 09:30 $5.75 → close $5.96 +44.31; ASST×48 09:30 $25.18 → close $27.14 +94.08 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $50.73 | ▼ 09:30 equity $9,876.49 vs yday $10,040.34 (-163.85) | 09:30 open · cash $50.73 (unchanged overnight, no fees) · equity $9,876.49 vs prior close $10,040.34 (-163.85) · 8 name(s) re-marked at the open (per-name table). GPRO×699 yday $1.70 → 09:30 $1.56 -94.37; DFDV×209 yday $5.87 → 09:30 $5.81 -12.54; FRNM×74 yday $16.31 → 09:30 $16.74 +31.82; TARS×14 yday $90.78 → 09:30 $89.67 -15.54; BRR×483 yday $2.66 → 09:30 $2.66 +0.00; IRD×268 yday $4.67 → 09:30 $4.53 -37.52; LENZ×211 yday $5.96 → 09:30 $5.95 -2.11; ASST×48 yday $27.14 → 09:30 $26.44 -33.60 | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 699 | $1.56 | $9.14 | $-168.45 | $1,135.53 | ▼ -168.45 after sell → book $9,867.35; vs 09:30 mark -9.14 | dropped from list after 2 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 209 | $5.81 | $2.74 | $-1.26 | $2,347.08 | ▼ -1.26 after sell → book $9,864.61; vs 09:30 mark -2.74 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 74 | $16.74 | $2.23 | $+20.71 | $3,583.60 | ▲ +20.71 after sell → book $9,862.37; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TARS` | 14 | $89.67 | $2.05 | $+93.50 | $4,836.93 | ▲ +93.50 after sell → book $9,860.32; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BRR` | 483 | $2.66 | $6.32 | $+59.90 | $6,115.39 | ▲ +59.90 after sell → book $9,854.00; vs 09:30 mark -6.32 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `IRD` | 268 | $4.53 | $3.51 | $-6.97 | $7,325.92 | ▼ -6.97 after sell → book $9,850.49; vs 09:30 mark -3.51 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LENZ` | 211 | $5.95 | $2.77 | $+36.71 | $8,578.60 | ▲ +36.71 after sell → book $9,847.72; vs 09:30 mark -2.77 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 48 | $26.44 | $2.15 | $+56.19 | $9,845.56 | ▲ +56.19 after sell → book $9,845.56; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,845.56 | ▲ close $9,845.56 vs 09:30 $9,876.49 (session +0.00) | 16:00 close · cash $9,845.56 · no lots left · equity $9,845.56. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,845.56 | ▲ 09:30 equity $9,845.56 vs yday $9,845.56 (+0.00) | 09:30 open · cash $9,845.56 · no holdings · equity $9,845.56 vs prior close $9,845.56 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,845.56 | ▲ close $9,845.56 vs 09:30 $9,845.56 (session +0.00) | 16:00 close · cash $9,845.56 · no lots left · equity $9,845.56. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,845.56 | ▲ 09:30 equity $9,845.56 vs yday $9,845.56 (+0.00) | 09:30 open · cash $9,845.56 · no holdings · equity $9,845.56 vs prior close $9,845.56 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,845.56 | ▲ close $9,845.56 vs 09:30 $9,845.56 (session +0.00) | 16:00 close · cash $9,845.56 · no lots left · equity $9,845.56. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OCUL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KURA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `KGC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CVI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `WFRD` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `LENZ` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `REAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MRNA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EBS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CYPH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VIR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AGCO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HAFN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `LOGI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GLW` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SWKS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SKHY` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DOCN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CVI` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INTC` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SWKS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `UROY` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DOCN` | hard_red | hard-red S=-13.28 sit; no new buys |
