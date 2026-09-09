# Factor mine action — `union_w_hot_cond_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `w_hot_cond` · size `leftover` · sell `list` · S-boost `none` · rank by w_hot_cond

Cash book **-0.24%** ($9,976) · signal-only (no cash/fees) was +5.48%. Starts YES **6/18**. Fills 168 · skips 54 · realized $-23.95.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,976.02.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `IREN` | 27 | — | $45.98 | +0.00 | $44.76 | -32.94 | -32.94 | +0.00 | -32.94 |
| 2026-08-13 | `TNDM` | 53 | — | $23.33 | +0.00 | $23.13 | -10.60 | -10.60 | +0.00 | -10.60 |
| 2026-08-13 | `TPG` | 24 | — | $50.62 | +0.00 | $54.62 | +95.92 | +95.92 | +0.00 | +95.92 |
| 2026-08-13 | `INO` | 1543 | — | $0.81 | +0.00 | $0.90 | +138.87 | +138.87 | +0.00 | +138.87 |
| 2026-08-13 | `HIMS` | 42 | — | $29.74 | +0.00 | $28.77 | -40.74 | -40.74 | +0.00 | -40.74 |
| 2026-08-13 | `SLS` | 106 | — | $11.70 | +0.00 | $12.36 | +69.96 | +69.96 | +0.00 | +69.96 |
| 2026-08-13 | `VOR` | 56 | — | $22.01 | +0.00 | $23.29 | +71.68 | +71.68 | +0.00 | +71.68 |
| 2026-08-13 | `BTSG` | 20 | — | $59.80 | +0.00 | $60.23 | +8.60 | +8.60 | +0.00 | +8.60 |
| 2026-08-14 | `IREN` | 27 | $44.76 | $44.09 | -18.09 | — | +0.00 | -18.09 | -51.03 | — |
| 2026-08-14 | `TNDM` | 53 | $23.13 | $22.92 | -11.13 | — | +0.00 | -11.13 | -21.73 | — |
| 2026-08-14 | `TPG` | 24 | $54.62 | $55.29 | +16.08 | — | +0.00 | +16.08 | +112.00 | — |
| 2026-08-14 | `INO` | 1543 | $0.90 | $0.93 | +46.29 | — | +0.00 | +46.29 | +185.16 | — |
| 2026-08-14 | `HIMS` | 42 | $28.77 | $29.15 | +15.96 | — | +0.00 | +15.96 | -24.78 | — |
| 2026-08-14 | `SLS` | 106 | $12.36 | $12.40 | +4.24 | — | +0.00 | +4.24 | +74.20 | — |
| 2026-08-14 | `VOR` | 56 | $23.29 | $23.33 | +2.24 | — | +0.00 | +2.24 | +73.92 | — |
| 2026-08-14 | `BTSG` | 20 | $60.23 | $59.65 | -11.60 | — | +0.00 | -11.60 | -3.00 | — |
| 2026-08-14 | `QMCO` | 52 | — | $24.68 | +0.00 | $26.11 | +74.36 | +74.36 | +0.00 | +74.36 |
| 2026-08-14 | `ARX` | 65 | — | $19.57 | +0.00 | $19.58 | +0.65 | +0.65 | +0.00 | +0.65 |
| 2026-08-14 | `ZENA` | 583 | — | $2.20 | +0.00 | $2.14 | -34.98 | -34.98 | +0.00 | -34.98 |
| 2026-08-14 | `AIRO` | 115 | — | $11.12 | +0.00 | $9.57 | -178.25 | -178.25 | +0.00 | -178.25 |
| 2026-08-14 | `LIFE` | 36 | — | $35.04 | +0.00 | $34.02 | -36.72 | -36.72 | +0.00 | -36.72 |
| 2026-08-14 | `LUNR` | 67 | — | $19.17 | +0.00 | $19.01 | -10.72 | -10.72 | +0.00 | -10.72 |
| 2026-08-14 | `TBBB` | 26 | — | $48.82 | +0.00 | $47.79 | -26.78 | -26.78 | +0.00 | -26.78 |
| 2026-08-14 | `BZAI` | 1677 | — | $0.77 | +0.00 | $0.59 | -290.12 | -290.12 | +0.00 | -290.12 |
| 2026-08-17 | `QMCO` | 52 | $26.11 | $24.83 | -66.56 | — | +0.00 | -66.56 | +7.80 | — |
| 2026-08-17 | `ARX` | 65 | $19.58 | $19.57 | -0.65 | — | +0.00 | -0.65 | +0.00 | — |
| 2026-08-17 | `ZENA` | 583 | $2.14 | $2.08 | -32.07 | — | +0.00 | -32.07 | -67.05 | — |
| 2026-08-17 | `AIRO` | 115 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -178.25 | — |
| 2026-08-17 | `LIFE` | 36 | $34.02 | $34.03 | +0.36 | — | +0.00 | +0.36 | -36.36 | — |
| 2026-08-17 | `LUNR` | 67 | $19.01 | $20.25 | +83.08 | — | +0.00 | +83.08 | +72.36 | — |
| 2026-08-17 | `TBBB` | 26 | $47.79 | $47.39 | -10.40 | — | +0.00 | -10.40 | -37.18 | — |
| 2026-08-17 | `BZAI` | 1677 | $0.59 | $0.55 | -68.76 | — | +0.00 | -68.76 | -358.88 | — |
| 2026-08-17 | `XHG` | 286 | — | $4.19 | +0.00 | $3.91 | -80.08 | -80.08 | +0.00 | -80.08 |
| 2026-08-17 | `CAPR` | 174 | — | $6.87 | +0.00 | $7.45 | +100.92 | +100.92 | +0.00 | +100.92 |
| 2026-08-17 | `STDN` | 88 | — | $13.64 | +0.00 | $13.31 | -29.04 | -29.04 | +0.00 | -29.04 |
| 2026-08-17 | `HTFL` | 29 | — | $41.23 | +0.00 | $41.94 | +20.59 | +20.59 | +0.00 | +20.59 |
| 2026-08-17 | `UMAC` | 36 | — | $32.55 | +0.00 | $30.15 | -86.40 | -86.40 | +0.00 | -86.40 |
| 2026-08-17 | `ALOY` | 81 | — | $14.66 | +0.00 | $13.86 | -65.20 | -65.20 | +0.00 | -65.20 |
| 2026-08-17 | `NPWR` | 625 | — | $1.92 | +0.00 | $1.73 | -118.75 | -118.75 | +0.00 | -118.75 |
| 2026-08-17 | `LPTH` | 80 | — | $14.94 | +0.00 | $14.80 | -11.20 | -11.20 | +0.00 | -11.20 |
| 2026-08-18 | `XHG` | 286 | $3.91 | $3.94 | +8.58 | — | +0.00 | +8.58 | -71.50 | — |
| 2026-08-18 | `CAPR` | 174 | $7.45 | $7.50 | +8.70 | $7.08 | -73.08 | -64.38 | +109.62 | +36.54 |
| 2026-08-18 | `STDN` | 88 | $13.31 | $13.31 | +0.00 | — | +0.00 | +0.00 | -29.04 | — |
| 2026-08-18 | `HTFL` | 29 | $41.94 | $41.50 | -12.76 | — | +0.00 | -12.76 | +7.83 | — |
| 2026-08-18 | `UMAC` | 36 | $30.15 | $28.59 | -56.16 | — | +0.00 | -56.16 | -142.56 | — |
| 2026-08-18 | `ALOY` | 81 | $13.86 | $13.19 | -53.87 | — | +0.00 | -53.87 | -119.07 | — |
| 2026-08-18 | `NPWR` | 625 | $1.73 | $1.70 | -18.75 | — | +0.00 | -18.75 | -137.50 | — |
| 2026-08-18 | `LPTH` | 80 | $14.80 | $14.01 | -63.20 | — | +0.00 | -63.20 | -74.40 | — |
| 2026-08-19 | `CAPR` | 174 | $7.08 | $7.19 | +19.14 | — | +0.00 | +19.14 | +55.68 | — |
| 2026-08-20 | `MRNA` | 7 | — | $150.14 | +0.00 | $133.32 | -117.74 | -117.74 | +0.00 | -117.74 |
| 2026-08-20 | `CYPH` | 983 | — | $1.15 | +0.00 | $1.19 | +39.32 | +39.32 | +0.00 | +39.32 |
| 2026-08-20 | `ABCL` | 95 | — | $11.81 | +0.00 | $11.57 | -23.27 | -23.27 | +0.00 | -23.27 |
| 2026-08-20 | `SENS` | 126 | — | $8.91 | +0.00 | $8.82 | -11.34 | -11.34 | +0.00 | -11.34 |
| 2026-08-20 | `AUTL` | 457 | — | $2.47 | +0.00 | $2.46 | -4.57 | -4.57 | +0.00 | -4.57 |
| 2026-08-20 | `TEM` | 18 | — | $61.83 | +0.00 | $66.65 | +86.76 | +86.76 | +0.00 | +86.76 |
| 2026-08-20 | `WPM` | 7 | — | $144.54 | +0.00 | $150.25 | +39.97 | +39.97 | +0.00 | +39.97 |
| 2026-08-20 | `IAG` | 57 | — | $19.63 | +0.00 | $20.50 | +49.59 | +49.59 | +0.00 | +49.59 |
| 2026-08-21 | `MRNA` | 7 | $133.32 | $133.11 | -1.47 | $145.13 | +84.14 | +82.67 | -119.21 | -35.07 |
| 2026-08-21 | `CYPH` | 983 | $1.19 | $1.32 | +127.79 | $1.42 | +98.30 | +226.09 | +167.11 | +265.41 |
| 2026-08-21 | `ABCL` | 95 | $11.57 | $11.57 | +0.00 | — | +0.00 | +0.00 | -23.27 | — |
| 2026-08-21 | `SENS` | 126 | $8.82 | $9.24 | +52.92 | — | +0.00 | +52.92 | +41.58 | — |
| 2026-08-21 | `AUTL` | 457 | $2.46 | $2.47 | +4.57 | — | +0.00 | +4.57 | +0.00 | — |
| 2026-08-21 | `TEM` | 18 | $66.65 | $65.60 | -18.90 | — | +0.00 | -18.90 | +67.86 | — |
| 2026-08-21 | `WPM` | 7 | $150.25 | $154.70 | +31.15 | — | +0.00 | +31.15 | +71.12 | — |
| 2026-08-21 | `IAG` | 57 | $20.50 | $21.17 | +38.19 | — | +0.00 | +38.19 | +87.78 | — |
| 2026-08-21 | `XHG` | 262 | — | $4.49 | +0.00 | $4.41 | -20.96 | -20.96 | +0.00 | -20.96 |
| 2026-08-21 | `ARCT` | 105 | — | $11.13 | +0.00 | $13.45 | +243.60 | +243.60 | +0.00 | +243.60 |
| 2026-08-21 | `IOVA` | 129 | — | $9.08 | +0.00 | $8.29 | -101.91 | -101.91 | +0.00 | -101.91 |
| 2026-08-21 | `MRVI` | 142 | — | $8.28 | +0.00 | $8.64 | +51.12 | +51.12 | +0.00 | +51.12 |
| 2026-08-21 | `AU` | 9 | — | $119.43 | +0.00 | $121.22 | +16.11 | +16.11 | +0.00 | +16.11 |
| 2026-08-21 | `CAPR` | 172 | — | $6.81 | +0.00 | $6.29 | -89.44 | -89.44 | +0.00 | -89.44 |
| 2026-08-24 | `MRNA` | 7 | $145.13 | $142.70 | -17.01 | — | +0.00 | -17.01 | -52.08 | — |
| 2026-08-24 | `CYPH` | 983 | $1.42 | $1.83 | +403.03 | — | +0.00 | +403.03 | +668.44 | — |
| 2026-08-24 | `XHG` | 262 | $4.41 | $4.32 | -23.58 | — | +0.00 | -23.58 | -44.54 | — |
| 2026-08-24 | `ARCT` | 105 | $13.45 | $13.33 | -12.60 | — | +0.00 | -12.60 | +231.00 | — |
| 2026-08-24 | `IOVA` | 129 | $8.29 | $8.08 | -27.09 | — | +0.00 | -27.09 | -129.00 | — |
| 2026-08-24 | `MRVI` | 142 | $8.64 | $8.59 | -7.10 | — | +0.00 | -7.10 | +44.02 | — |
| 2026-08-24 | `AU` | 9 | $121.22 | $120.51 | -6.39 | — | +0.00 | -6.39 | +9.72 | — |
| 2026-08-24 | `CAPR` | 172 | $6.29 | $8.03 | +299.28 | — | +0.00 | +299.28 | +209.84 | — |
| 2026-08-25 | `REAX` | 52 | — | $24.11 | +0.00 | $28.43 | +224.64 | +224.64 | +0.00 | +224.64 |
| 2026-08-25 | `CYPH` | 812 | — | $1.56 | +0.00 | $1.64 | +64.96 | +64.96 | +0.00 | +64.96 |
| 2026-08-25 | `XHG` | 311 | — | $4.07 | +0.00 | $4.02 | -15.55 | -15.55 | +0.00 | -15.55 |
| 2026-08-25 | `ALVO` | 241 | — | $5.24 | +0.00 | $5.05 | -45.79 | -45.79 | +0.00 | -45.79 |
| 2026-08-25 | `ASST` | 66 | — | $19.04 | +0.00 | $21.39 | +155.10 | +155.10 | +0.00 | +155.10 |
| 2026-08-25 | `GORO` | 356 | — | $3.55 | +0.00 | $3.87 | +113.92 | +113.92 | +0.00 | +113.92 |
| 2026-08-25 | `EZPW` | 36 | — | $35.05 | +0.00 | $35.23 | +6.48 | +6.48 | +0.00 | +6.48 |
| 2026-08-25 | `BMEA` | 776 | — | $1.63 | +0.00 | $1.73 | +77.60 | +77.60 | +0.00 | +77.60 |
| 2026-08-26 | `REAX` | 52 | $28.43 | $26.61 | -94.64 | — | +0.00 | -94.64 | +130.00 | — |
| 2026-08-26 | `CYPH` | 812 | $1.64 | $1.60 | -32.48 | — | +0.00 | -32.48 | +32.48 | — |
| 2026-08-26 | `XHG` | 311 | $4.02 | $3.81 | -65.31 | $4.06 | +77.75 | +12.44 | -80.86 | -3.11 |
| 2026-08-26 | `ALVO` | 241 | $5.05 | $4.98 | -16.87 | — | +0.00 | -16.87 | -62.66 | — |
| 2026-08-26 | `ASST` | 66 | $21.39 | $20.72 | -44.22 | — | +0.00 | -44.22 | +110.88 | — |
| 2026-08-26 | `GORO` | 356 | $3.87 | $3.77 | -35.60 | — | +0.00 | -35.60 | +78.32 | — |
| 2026-08-26 | `EZPW` | 36 | $35.23 | $35.70 | +16.92 | — | +0.00 | +16.92 | +23.40 | — |
| 2026-08-26 | `BMEA` | 776 | $1.73 | $1.75 | +19.40 | — | +0.00 | +19.40 | +97.00 | — |
| 2026-08-26 | `BYND` | 93 | — | $14.11 | +0.00 | $14.25 | +13.02 | +13.02 | +0.00 | +13.02 |
| 2026-08-26 | `USDE` | 226 | — | $5.81 | +0.00 | $5.98 | +38.42 | +38.42 | +0.00 | +38.42 |
| 2026-08-26 | `FIGR` | 32 | — | $40.50 | +0.00 | $37.08 | -109.44 | -109.44 | +0.00 | -109.44 |
| 2026-08-26 | `FUTU` | 10 | — | $124.67 | +0.00 | $127.34 | +26.70 | +26.70 | +0.00 | +26.70 |
| 2026-08-26 | `TIGR` | 252 | — | $5.21 | +0.00 | $5.46 | +63.00 | +63.00 | +0.00 | +63.00 |
| 2026-08-26 | `FNV` | 4 | — | $267.02 | +0.00 | $267.37 | +1.40 | +1.40 | +0.00 | +1.40 |
| 2026-08-26 | `BTG` | 228 | — | $5.75 | +0.00 | $5.74 | -2.28 | -2.28 | +0.00 | -2.28 |
| 2026-08-27 | `XHG` | 311 | $4.06 | $4.06 | +0.00 | — | +0.00 | +0.00 | -3.11 | — |
| 2026-08-27 | `BYND` | 93 | $14.25 | $14.20 | -4.65 | — | +0.00 | -4.65 | +8.37 | — |
| 2026-08-27 | `USDE` | 226 | $5.98 | $6.50 | +117.52 | — | +0.00 | +117.52 | +155.94 | — |
| 2026-08-27 | `FIGR` | 32 | $37.08 | $37.42 | +10.88 | — | +0.00 | +10.88 | -98.56 | — |
| 2026-08-27 | `FUTU` | 10 | $127.34 | $128.00 | +6.60 | — | +0.00 | +6.60 | +33.30 | — |
| 2026-08-27 | `TIGR` | 252 | $5.46 | $5.49 | +7.56 | — | +0.00 | +7.56 | +70.56 | — |
| 2026-08-27 | `FNV` | 4 | $267.37 | $267.23 | -0.56 | — | +0.00 | -0.56 | +0.84 | — |
| 2026-08-27 | `BTG` | 228 | $5.74 | $5.73 | -2.28 | — | +0.00 | -2.28 | -4.56 | — |
| 2026-08-27 | `SLI` | 509 | — | $2.60 | +0.00 | $2.64 | +20.36 | +20.36 | +0.00 | +20.36 |
| 2026-08-27 | `PGY` | 57 | — | $22.93 | +0.00 | $23.26 | +18.81 | +18.81 | +0.00 | +18.81 |
| 2026-08-27 | `GEN` | 44 | — | $29.83 | +0.00 | $30.50 | +29.48 | +29.48 | +0.00 | +29.48 |
| 2026-08-27 | `MRVL` | 5 | — | $253.44 | +0.00 | $241.45 | -59.95 | -59.95 | +0.00 | -59.95 |
| 2026-08-27 | `ANET` | 6 | — | $205.90 | +0.00 | $201.09 | -28.86 | -28.86 | +0.00 | -28.86 |
| 2026-08-27 | `MOS` | 55 | — | $24.00 | +0.00 | $23.76 | -13.20 | -13.20 | +0.00 | -13.20 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-27 | `GGB` | 289 | — | $4.57 | +0.00 | $4.70 | +37.57 | +37.57 | +0.00 | +37.57 |
| 2026-08-28 | `SLI` | 509 | $2.64 | $2.68 | +20.36 | — | +0.00 | +20.36 | +40.72 | — |
| 2026-08-28 | `PGY` | 57 | $23.26 | $23.21 | -2.85 | — | +0.00 | -2.85 | +15.96 | — |
| 2026-08-28 | `GEN` | 44 | $30.50 | $30.50 | +0.00 | — | +0.00 | +0.00 | +29.48 | — |
| 2026-08-28 | `MRVL` | 5 | $241.45 | $225.26 | -80.95 | — | +0.00 | -80.95 | -140.90 | — |
| 2026-08-28 | `ANET` | 6 | $201.09 | $200.00 | -6.54 | — | +0.00 | -6.54 | -35.40 | — |
| 2026-08-28 | `MOS` | 55 | $23.76 | $23.95 | +10.45 | — | +0.00 | +10.45 | -2.75 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `GGB` | 289 | $4.70 | $4.67 | -8.67 | — | +0.00 | -8.67 | +28.90 | — |
| 2026-08-28 | `BYND` | 93 | — | $14.00 | +0.00 | $13.86 | -13.02 | -13.02 | +0.00 | -13.02 |
| 2026-08-28 | `CAPR` | 134 | — | $9.73 | +0.00 | $9.59 | -18.76 | -18.76 | +0.00 | -18.76 |
| 2026-08-28 | `ANF` | 8 | — | $146.07 | +0.00 | $148.42 | +18.80 | +18.80 | +0.00 | +18.80 |
| 2026-08-28 | `NCNO` | 55 | — | $23.30 | +0.00 | $22.99 | -17.05 | -17.05 | +0.00 | -17.05 |
| 2026-08-28 | `MEI` | 73 | — | $17.78 | +0.00 | $18.21 | +31.39 | +31.39 | +0.00 | +31.39 |
| 2026-08-28 | `VYX` | 142 | — | $9.13 | +0.00 | $8.78 | -49.70 | -49.70 | +0.00 | -49.70 |
| 2026-08-28 | `SMTC` | 9 | — | $141.76 | +0.00 | $131.17 | -95.31 | -95.31 | +0.00 | -95.31 |
| 2026-08-28 | `SNPS` | 2 | — | $461.85 | +0.00 | $442.61 | -38.48 | -38.48 | +0.00 | -38.48 |
| 2026-08-31 | `BYND` | 93 | $13.86 | $13.81 | -4.65 | $13.30 | -47.43 | -52.08 | -17.67 | -65.10 |
| 2026-08-31 | `CAPR` | 134 | $9.59 | $9.50 | -12.06 | — | +0.00 | -12.06 | -30.82 | — |
| 2026-08-31 | `ANF` | 8 | $148.42 | $148.03 | -3.12 | — | +0.00 | -3.12 | +15.68 | — |
| 2026-08-31 | `NCNO` | 55 | $22.99 | $22.66 | -18.15 | — | +0.00 | -18.15 | -35.20 | — |
| 2026-08-31 | `MEI` | 73 | $18.21 | $18.15 | -4.38 | — | +0.00 | -4.38 | +27.01 | — |
| 2026-08-31 | `VYX` | 142 | $8.78 | $8.66 | -17.04 | — | +0.00 | -17.04 | -66.74 | — |
| 2026-08-31 | `SMTC` | 9 | $131.17 | $132.30 | +10.17 | — | +0.00 | +10.17 | -85.14 | — |
| 2026-08-31 | `SNPS` | 2 | $442.61 | $437.95 | -9.32 | — | +0.00 | -9.32 | -47.80 | — |
| 2026-09-01 | `BYND` | 93 | $13.30 | $13.04 | -24.18 | — | +0.00 | -24.18 | -89.28 | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `GPRO` | 708 | — | $1.78 | +0.00 | $1.39 | -276.12 | -276.12 | +0.00 | -276.12 |
| 2026-09-03 | `REAX` | 68 | — | $18.40 | +0.00 | $18.40 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `BMEA` | 653 | — | $1.93 | +0.00 | $1.91 | -13.06 | -13.06 | +0.00 | -13.06 |
| 2026-09-03 | `MMED` | 52 | — | $23.88 | +0.00 | $23.84 | -2.08 | -2.08 | +0.00 | -2.08 |
| 2026-09-03 | `AGCO` | 9 | — | $127.91 | +0.00 | $125.82 | -18.81 | -18.81 | +0.00 | -18.81 |
| 2026-09-03 | `ARCT` | 75 | — | $16.77 | +0.00 | $15.56 | -90.75 | -90.75 | +0.00 | -90.75 |
| 2026-09-03 | `NVAX` | 121 | — | $10.42 | +0.00 | $10.34 | -9.68 | -9.68 | +0.00 | -9.68 |
| 2026-09-03 | `VSTM` | 157 | — | $8.03 | +0.00 | $7.98 | -7.85 | -7.85 | +0.00 | -7.85 |
| 2026-09-04 | `GPRO` | 708 | $1.39 | $1.48 | +63.72 | $1.70 | +155.76 | +219.48 | -212.40 | -56.64 |
| 2026-09-04 | `REAX` | 68 | $18.40 | $18.15 | -17.00 | — | +0.00 | -17.00 | -17.00 | — |
| 2026-09-04 | `BMEA` | 653 | $1.91 | $1.90 | -6.53 | — | +0.00 | -6.53 | -19.59 | — |
| 2026-09-04 | `MMED` | 52 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -2.08 | — |
| 2026-09-04 | `AGCO` | 9 | $125.82 | $125.22 | -5.40 | — | +0.00 | -5.40 | -24.21 | — |
| 2026-09-04 | `ARCT` | 75 | $15.56 | $15.61 | +3.75 | — | +0.00 | +3.75 | -87.00 | — |
| 2026-09-04 | `NVAX` | 121 | $10.34 | $10.50 | +19.36 | — | +0.00 | +19.36 | +9.68 | — |
| 2026-09-04 | `VSTM` | 157 | $7.98 | $7.91 | -10.99 | — | +0.00 | -10.99 | -18.84 | — |
| 2026-09-04 | `DFDV` | 212 | — | $5.79 | +0.00 | $5.87 | +16.96 | +16.96 | +0.00 | +16.96 |
| 2026-09-04 | `FRNM` | 75 | — | $16.40 | +0.00 | $16.31 | -6.75 | -6.75 | +0.00 | -6.75 |
| 2026-09-04 | `TARS` | 14 | — | $82.70 | +0.00 | $90.78 | +113.12 | +113.12 | +0.00 | +113.12 |
| 2026-09-04 | `BRR` | 490 | — | $2.51 | +0.00 | $2.66 | +73.50 | +73.50 | +0.00 | +73.50 |
| 2026-09-04 | `IRD` | 271 | — | $4.53 | +0.00 | $4.67 | +37.94 | +37.94 | +0.00 | +37.94 |
| 2026-09-04 | `LENZ` | 214 | — | $5.75 | +0.00 | $5.96 | +44.94 | +44.94 | +0.00 | +44.94 |
| 2026-09-04 | `ASST` | 48 | — | $25.18 | +0.00 | $27.14 | +94.08 | +94.08 | +0.00 | +94.08 |
| 2026-09-08 | `GPRO` | 708 | $1.70 | $1.56 | -95.58 | — | +0.00 | -95.58 | -152.22 | — |
| 2026-09-08 | `DFDV` | 212 | $5.87 | $5.81 | -12.72 | — | +0.00 | -12.72 | +4.24 | — |
| 2026-09-08 | `FRNM` | 75 | $16.31 | $16.74 | +32.25 | — | +0.00 | +32.25 | +25.50 | — |
| 2026-09-08 | `TARS` | 14 | $90.78 | $89.67 | -15.54 | — | +0.00 | -15.54 | +97.58 | — |
| 2026-09-08 | `BRR` | 490 | $2.66 | $2.66 | +0.00 | — | +0.00 | +0.00 | +73.50 | — |
| 2026-09-08 | `IRD` | 271 | $4.67 | $4.53 | -37.94 | — | +0.00 | -37.94 | +0.00 | — |
| 2026-09-08 | `LENZ` | 214 | $5.96 | $5.95 | -2.14 | — | +0.00 | -2.14 | +42.80 | — |
| 2026-09-08 | `ASST` | 48 | $27.14 | $26.44 | -33.60 | — | +0.00 | -33.60 | +60.48 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +300.75 | IREN, TNDM, TPG, INO, HIMS, SLS, VOR, BTSG | — | $107.38 | $10,268.71 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20 |
| 2026-08-14 | +5.50 | $107.38 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20 | $10,312.70 | +43.99 | -502.56 | QMCO, ARX, ZENA, AIRO, LIFE, LUNR, TBBB, BZAI | IREN, TNDM, TPG, INO, HIMS, SLS, VOR, BTSG | $23.43 | $9,737.41 | QMCO×52, ARX×65, ZENA×583, AIRO×115, LIFE×36, LUNR×67, TBBB×26, BZAI×1677 |
| 2026-08-17 | +2.25 | $23.43 | QMCO×52, ARX×65, ZENA×583, AIRO×115, LIFE×36, LUNR×67, TBBB×26, BZAI×1677 | $9,642.42 | -94.99 | -269.16 | XHG, CAPR, STDN, HTFL, UMAC, ALOY, NPWR, LPTH | QMCO, ARX, ZENA, AIRO, LIFE, LUNR, TBBB, BZAI | $37.73 | $9,312.74 | XHG×286, CAPR×174, STDN×88, HTFL×29, UMAC×36, ALOY×81, NPWR×625, LPTH×80 |
| 2026-08-18 | -6.20 | $37.73 | XHG×286, CAPR×174, STDN×88, HTFL×29, UMAC×36, ALOY×81, NPWR×625, LPTH×80 | $9,125.28 | -187.46 | -73.08 | — | XHG, STDN, HTFL, UMAC, ALOY, NPWR, LPTH | $7,797.36 | $9,029.28 | CAPR×174 |
| 2026-08-19 | -7.20 | $7,797.36 | CAPR×174 | $9,048.42 | +19.14 | +0.00 | — | CAPR | $9,045.87 | $9,045.87 | — |
| 2026-08-20 | +1.12 | $9,045.87 | — | $9,045.87 | -0.00 | +58.72 | MRNA, CYPH, ABCL, SENS, AUTL, TEM, WPM, IAG | — | $215.49 | $9,073.14 | MRNA×7, CYPH×983, ABCL×95, SENS×126, AUTL×457, TEM×18, WPM×7, IAG×57 |
| 2026-08-21 | +3.25 | $215.49 | MRNA×7, CYPH×983, ABCL×95, SENS×126, AUTL×457, TEM×18, WPM×7, IAG×57 | $9,307.39 | +234.25 | +280.96 | XHG, ARCT, IOVA, MRVI, AU, CAPR | ABCL, SENS, AUTL, TEM, WPM, IAG | $107.80 | $9,556.39 | MRNA×7, CYPH×983, XHG×262, ARCT×105, IOVA×129, MRVI×142, AU×9, CAPR×172 |
| 2026-08-24 | -5.17 | $107.80 | MRNA×7, CYPH×983, XHG×262, ARCT×105, IOVA×129, MRVI×142, AU×9, CAPR×172 | $10,164.93 | +608.54 | +0.00 | — | MRNA, CYPH, XHG, ARCT, IOVA, MRVI, AU, CAPR | $10,134.83 | $10,134.83 | — |
| 2026-08-25 | +1.80 | $10,134.83 | — | $10,134.83 | +0.00 | +581.36 | REAX, CYPH, XHG, ALVO, ASST, GORO, EZPW, BMEA | — | $0.03 | $10,677.56 | REAX×52, CYPH×812, XHG×311, ALVO×241, ASST×66, GORO×356, EZPW×36, BMEA×776 |
| 2026-08-26 | +2.02 | $0.03 | REAX×52, CYPH×812, XHG×311, ALVO×241, ASST×66, GORO×356, EZPW×36, BMEA×776 | $10,424.76 | -252.80 | +108.57 | BYND, USDE, FIGR, FUTU, TIGR, FNV, BTG | REAX, CYPH, ALVO, ASST, GORO, EZPW, BMEA | $327.29 | $10,480.76 | XHG×311, BYND×93, USDE×226, FIGR×32, FUTU×10, TIGR×252, FNV×4, BTG×228 |
| 2026-08-27 | — | $327.29 | XHG×311, BYND×93, USDE×226, FIGR×32, FUTU×10, TIGR×252, FNV×4, BTG×228 | $10,615.83 | +135.07 | -27.41 | SLI, PGY, GEN, MRVL, ANET, MOS, MU, GGB | XHG, BYND, USDE, FIGR, FUTU, TIGR, FNV, BTG | $518.03 | $10,543.89 | SLI×509, PGY×57, GEN×44, MRVL×5, ANET×6, MOS×55, MU×1, GGB×289 |
| 2026-08-28 | +0.75 | $518.03 | SLI×509, PGY×57, GEN×44, MRVL×5, ANET×6, MOS×55, MU×1, GGB×289 | $10,459.59 | -84.30 | -182.13 | BYND, CAPR, ANF, NCNO, MEI, VYX, SMTC, SNPS | SLI, PGY, GEN, MRVL, ANET, MOS, MU, GGB | $569.28 | $10,236.97 | BYND×93, CAPR×134, ANF×8, NCNO×55, MEI×73, VYX×142, SMTC×9, SNPS×2 |
| 2026-08-31 | -5.85 | $569.28 | BYND×93, CAPR×134, ANF×8, NCNO×55, MEI×73, VYX×142, SMTC×9, SNPS×2 | $10,178.42 | -58.55 | -47.43 | — | CAPR, ANF, NCNO, MEI, VYX, SMTC, SNPS | $8,878.73 | $10,115.63 | BYND×93 |
| 2026-09-01 | -6.30 | $8,878.73 | BYND×93 | $10,091.45 | -24.18 | +0.00 | — | BYND | $10,089.15 | $10,089.15 | — |
| 2026-09-02 | -3.83 | $10,089.15 | — | $10,089.15 | +0.00 | +0.00 | — | — | $10,089.15 | $10,089.15 | — |
| 2026-09-03 | -0.90 | $10,089.15 | — | $10,089.15 | +0.00 | -418.35 | GPRO, REAX, BMEA, MMED, AGCO, ARCT, NVAX, VSTM | — | $114.25 | $9,639.86 | GPRO×708, REAX×68, BMEA×653, MMED×52, AGCO×9, ARCT×75, NVAX×121, VSTM×157 |
| 2026-09-04 | +2.25 | $114.25 | GPRO×708, REAX×68, BMEA×653, MMED×52, AGCO×9, ARCT×75, NVAX×121, VSTM×157 | $9,686.77 | +46.91 | +529.55 | DFDV, FRNM, TARS, BRR, IRD, LENZ, ASST | REAX, BMEA, MMED, AGCO, ARCT, NVAX, VSTM | $83.21 | $10,172.55 | GPRO×708, DFDV×212, FRNM×75, TARS×14, BRR×490, IRD×271, LENZ×214, ASST×48 |
| 2026-09-08 | -11.47 | $83.21 | GPRO×708, DFDV×212, FRNM×75, TARS×14, BRR×490, IRD×271, LENZ×214, ASST×48 | $10,007.28 | -165.27 | +0.00 | — | GPRO, DFDV, FRNM, TARS, BRR, IRD, LENZ, ASST | $9,976.02 | $9,976.02 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $8,756.47 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $7,517.83 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,300.81 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $5,033.85 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $3,782.66 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $2,540.15 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 56 | $22.01 | $2.16 | — | $1,305.43 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $107.38 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $107.38 | ▲ close $10,268.71 vs 09:30 $10,000.00 (session +300.75) | 16:00 close · cash $107.38 · equity $10,268.71 vs 09:30 $10,000.00 (+268.71; session marks +300.75) · 8 name(s) marked open→close (per-name table). IREN×27 09:30 $45.98 → close $44.76 -32.94; TNDM×53 09:30 $23.33 → close $23.13 -10.60; TPG×24 09:30 $50.62 → close $54.62 +95.92; INO×1543 09:30 $0.81 → close $0.90 +138.87; HIMS×42 09:30 $29.74 → close $28.77 -40.74; SLS×106 09:30 $11.70 → close $12.36 +69.96; VOR×56 09:30 $22.01 → close $23.29 +71.68; BTSG×20 09:30 $59.80 → close $60.23 +8.60 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $107.38 | ▲ 09:30 equity $10,312.70 vs yday $10,268.71 (+43.99) | 09:30 open · cash $107.38 (unchanged overnight, no fees) · equity $10,312.70 vs prior close $10,268.71 (+43.99) · 8 name(s) re-marked at the open (per-name table). IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; VOR×56 yday $23.29 → 09:30 $23.33 +2.24; BTSG×20 yday $60.23 → 09:30 $59.65 -11.60 | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $1,295.72 | ▼ -55.19 after sell → book $10,310.61; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $2,508.31 | ▼ -26.05 after sell → book $10,308.44; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $3,833.19 | ▲ +107.86 after sell → book $10,306.36; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $5,248.93 | ▲ +148.79 after sell → book $10,287.11; vs 09:30 mark -19.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $6,471.10 | ▼ -29.03 after sell → book $10,284.98; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $7,783.16 | ▲ +69.56 after sell → book $10,282.64; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 56 | $23.33 | $2.18 | $+69.58 | $9,087.46 | ▲ +69.58 after sell → book $10,280.46; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $10,278.39 | ▼ -7.12 after sell → book $10,278.39; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 52 | $24.68 | $2.15 | — | $8,992.89 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+111.3; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 65 | $19.57 | $2.19 | — | $7,718.65 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 583 | $2.20 | $7.52 | — | $6,428.53 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 115 | $11.12 | $2.33 | — | $5,147.40 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LIFE` | 36 | $35.04 | $2.10 | — | $3,883.86 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+16.7; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 67 | $19.17 | $2.19 | — | $2,597.28 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `TBBB` | 26 | $48.82 | $2.07 | — | $1,325.89 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.0; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BZAI` | 1677 | $0.77 | $17.88 | — | $23.43 | — | rank by w_hot_cond; rank w_hot_cond; list earn_react; 🔵; ⚪; ret5=+20.4; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.43 | ▼ close $9,737.41 vs 09:30 $10,312.70 (session -502.56) | 16:00 close · cash $23.43 · equity $9,737.41 vs 09:30 $10,312.70 (-575.29; session marks -502.56) · 8 name(s) marked open→close (per-name table). QMCO×52 09:30 $24.68 → close $26.11 +74.36; ARX×65 09:30 $19.57 → close $19.58 +0.65; ZENA×583 09:30 $2.20 → close $2.14 -34.98; AIRO×115 09:30 $11.12 → close $9.57 -178.25; LIFE×36 09:30 $35.04 → close $34.02 -36.72; LUNR×67 09:30 $19.17 → close $19.01 -10.72; TBBB×26 09:30 $48.82 → close $47.79 -26.78; BZAI×1677 09:30 $0.77 → close $0.59 -290.12 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.43 | ▼ 09:30 equity $9,642.42 vs yday $9,737.41 (-94.99) | 09:30 open · cash $23.43 (unchanged overnight, no fees) · equity $9,642.42 vs prior close $9,737.41 (-94.99) · 8 name(s) re-marked at the open (per-name table). QMCO×52 yday $26.11 → 09:30 $24.83 -66.56; ARX×65 yday $19.58 → 09:30 $19.57 -0.65; ZENA×583 yday $2.14 → 09:30 $2.08 -32.07; AIRO×115 yday $9.57 → 09:30 $9.57 +0.00; LIFE×36 yday $34.02 → 09:30 $34.03 +0.36; LUNR×67 yday $19.01 → 09:30 $20.25 +83.08; TBBB×26 yday $47.79 → 09:30 $47.39 -10.40; BZAI×1677 yday $0.59 → 09:30 $0.55 -68.76 | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 52 | $24.83 | $2.17 | $+3.49 | $1,312.42 | ▲ +3.49 after sell → book $9,640.25; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 65 | $19.57 | $2.21 | $-4.39 | $2,582.27 | ▼ -4.39 after sell → book $9,638.05; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 583 | $2.08 | $7.63 | $-82.19 | $3,790.19 | ▼ -82.19 after sell → book $9,630.42; vs 09:30 mark -7.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 115 | $9.57 | $2.36 | $-182.95 | $4,888.38 | ▼ -182.95 after sell → book $9,628.05; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LIFE` | 36 | $34.03 | $2.12 | $-40.58 | $6,111.34 | ▼ -40.58 after sell → book $9,625.94; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LUNR` | 67 | $20.25 | $2.21 | $+67.96 | $7,465.88 | ▲ +67.96 after sell → book $9,623.72; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `TBBB` | 26 | $47.39 | $2.09 | $-41.34 | $8,695.93 | ▼ -41.34 after sell → book $9,621.64; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BZAI` | 1677 | $0.55 | $14.58 | $-391.33 | $9,607.06 | ▼ -391.33 after sell → book $9,607.06; vs 09:30 mark -14.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 286 | $4.19 | $3.69 | — | $8,405.03 | — | rank by w_hot_cond; rank w_hot_cond; list yday_mover; ⚪; ret5=+291.8; leftover $1200.88 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 174 | $6.87 | $2.51 | — | $7,207.14 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+62.6; leftover $1200.88 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 88 | $13.64 | $2.25 | — | $6,004.56 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+57.7; leftover $1200.88 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 29 | $41.23 | $2.08 | — | $4,806.82 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+46.0; leftover $1200.88 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 36 | $32.55 | $2.10 | — | $3,632.92 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1200.88 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 81 | $14.66 | $2.23 | — | $2,443.23 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $1200.88 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 625 | $1.92 | $8.06 | — | $1,235.16 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1200.88 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `LPTH` | 80 | $14.94 | $2.23 | — | $37.73 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $1200.88 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟢 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.73 | ▼ close $9,312.74 vs 09:30 $9,642.42 (session -269.16) | 16:00 close · cash $37.73 · equity $9,312.74 vs 09:30 $9,642.42 (-329.68; session marks -269.16) · 8 name(s) marked open→close (per-name table). XHG×286 09:30 $4.19 → close $3.91 -80.08; CAPR×174 09:30 $6.87 → close $7.45 +100.92; STDN×88 09:30 $13.64 → close $13.31 -29.04; HTFL×29 09:30 $41.23 → close $41.94 +20.59; UMAC×36 09:30 $32.55 → close $30.15 -86.40; ALOY×81 09:30 $14.66 → close $13.86 -65.20; NPWR×625 09:30 $1.92 → close $1.73 -118.75; LPTH×80 09:30 $14.94 → close $14.80 -11.20 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.73 | ▼ 09:30 equity $9,125.28 vs yday $9,312.74 (-187.46) | 09:30 open · cash $37.73 (unchanged overnight, no fees) · equity $9,125.28 vs prior close $9,312.74 (-187.46) · 8 name(s) re-marked at the open (per-name table). XHG×286 yday $3.91 → 09:30 $3.94 +8.58; CAPR×174 yday $7.45 → 09:30 $7.50 +8.70; STDN×88 yday $13.31 → 09:30 $13.31 +0.00; HTFL×29 yday $41.94 → 09:30 $41.50 -12.76; UMAC×36 yday $30.15 → 09:30 $28.59 -56.16; ALOY×81 yday $13.86 → 09:30 $13.19 -53.87; NPWR×625 yday $1.73 → 09:30 $1.70 -18.75; LPTH×80 yday $14.80 → 09:30 $14.01 -63.20 | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 286 | $3.94 | $3.75 | $-78.94 | $1,160.83 | ▼ -78.94 after sell → book $9,121.54; vs 09:30 mark -3.74 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 88 | $13.31 | $2.28 | $-33.57 | $2,329.83 | ▼ -33.57 after sell → book $9,119.26; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 29 | $41.50 | $2.10 | $+3.66 | $3,531.23 | ▲ +3.66 after sell → book $9,117.16; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 36 | $28.59 | $2.12 | $-146.78 | $4,558.35 | ▼ -146.78 after sell → book $9,115.04; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 81 | $13.19 | $2.26 | $-123.56 | $5,624.49 | ▼ -123.56 after sell → book $9,112.79; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 625 | $1.70 | $8.18 | $-153.74 | $6,678.81 | ▼ -153.74 after sell → book $9,104.61; vs 09:30 mark -8.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `LPTH` | 80 | $14.01 | $2.25 | $-78.88 | $7,797.36 | ▼ -78.88 after sell → book $9,102.36; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,797.36 | ▼ close $9,029.28 vs 09:30 $9,125.28 (session -73.08) | 16:00 close · cash $7,797.36 · equity $9,029.28 vs 09:30 $9,125.28 (-96.00; session marks -73.08) · 1 name(s) marked open→close (per-name table). CAPR×174 09:30 $7.50 → close $7.08 -73.08 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,797.36 | ▲ 09:30 equity $9,048.42 vs yday $9,029.28 (+19.14) | 09:30 open · cash $7,797.36 (unchanged overnight, no fees) · equity $9,048.42 vs prior close $9,029.28 (+19.14) · 1 name(s) re-marked at the open (per-name table). CAPR×174 yday $7.08 → 09:30 $7.19 +19.14 | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 174 | $7.19 | $2.55 | $+50.62 | $9,045.87 | ▲ +50.62 after sell → book $9,045.87; vs 09:30 mark -2.55 | dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,045.87 | ▲ close $9,045.87 vs 09:30 $9,048.42 (session +0.00) | 16:00 close · cash $9,045.87 · no lots left · equity $9,045.87. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,045.87 | ▲ 09:30 equity $9,045.87 vs yday $9,045.87 (-0.00) | 09:30 open · cash $9,045.87 · no holdings · equity $9,045.87 vs prior close $9,045.87 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 7 | $150.14 | $2.01 | — | $7,992.88 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1130.73 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 983 | $1.15 | $12.68 | — | $6,849.74 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1130.73 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 95 | $11.81 | $2.27 | — | $5,725.04 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1130.73 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SENS` | 126 | $8.91 | $2.37 | — | $4,600.02 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1130.73 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 457 | $2.47 | $5.90 | — | $3,465.33 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1130.73 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TEM` | 18 | $61.83 | $2.04 | — | $2,350.35 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $1130.73 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 7 | $144.54 | $2.01 | — | $1,336.56 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1130.73 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 57 | $19.63 | $2.16 | — | $215.49 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1130.73 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $215.49 | ▲ close $9,073.14 vs 09:30 $9,045.87 (session +58.72) | 16:00 close · cash $215.49 · equity $9,073.14 vs 09:30 $9,045.87 (+27.27; session marks +58.72) · 8 name(s) marked open→close (per-name table). MRNA×7 09:30 $150.14 → close $133.32 -117.74; CYPH×983 09:30 $1.15 → close $1.19 +39.32; ABCL×95 09:30 $11.81 → close $11.57 -23.27; SENS×126 09:30 $8.91 → close $8.82 -11.34; AUTL×457 09:30 $2.47 → close $2.46 -4.57; TEM×18 09:30 $61.83 → close $66.65 +86.76; WPM×7 09:30 $144.54 → close $150.25 +39.97; IAG×57 09:30 $19.63 → close $20.50 +49.59 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $215.49 | ▲ 09:30 equity $9,307.39 vs yday $9,073.14 (+234.25) | 09:30 open · cash $215.49 (unchanged overnight, no fees) · equity $9,307.39 vs prior close $9,073.14 (+234.25) · 8 name(s) re-marked at the open (per-name table). MRNA×7 yday $133.32 → 09:30 $133.11 -1.47; CYPH×983 yday $1.19 → 09:30 $1.32 +127.79; ABCL×95 yday $11.57 → 09:30 $11.57 +0.00; SENS×126 yday $8.82 → 09:30 $9.24 +52.92; AUTL×457 yday $2.46 → 09:30 $2.47 +4.57; TEM×18 yday $66.65 → 09:30 $65.60 -18.90; WPM×7 yday $150.25 → 09:30 $154.70 +31.15; IAG×57 yday $20.50 → 09:30 $21.17 +38.19 | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 95 | $11.57 | $2.30 | $-27.85 | $1,312.33 | ▼ -27.85 after sell → book $9,305.08; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SENS` | 126 | $9.24 | $2.40 | $+36.81 | $2,474.18 | ▲ +36.81 after sell → book $9,302.69; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `AUTL` | 457 | $2.47 | $5.98 | $-11.88 | $3,596.98 | ▼ -11.88 after sell → book $9,296.70; vs 09:30 mark -5.99 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `TEM` | 18 | $65.60 | $2.06 | $+63.75 | $4,775.72 | ▲ +63.75 after sell → book $9,294.64; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 7 | $154.70 | $2.03 | $+67.08 | $5,856.59 | ▲ +67.08 after sell → book $9,292.61; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 57 | $21.17 | $2.18 | $+83.44 | $7,061.10 | ▲ +83.44 after sell → book $9,290.43; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 262 | $4.49 | $3.38 | — | $5,881.34 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+12.7; leftover $1176.85 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 105 | $11.13 | $2.31 | — | $4,710.38 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1176.85 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 129 | $9.08 | $2.38 | — | $3,536.69 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $1176.85 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 142 | $8.28 | $2.42 | — | $2,358.51 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; leftover $1176.85 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 9 | $119.43 | $2.02 | — | $1,281.62 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1176.85 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 172 | $6.81 | $2.51 | — | $107.80 | — | rank by w_hot_cond; rank w_hot_cond; list yday_mover; 🔵; ret5=+62.5; leftover $1176.85 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $107.80 | ▲ close $9,556.39 vs 09:30 $9,307.39 (session +280.96) | 16:00 close · cash $107.80 · equity $9,556.39 vs 09:30 $9,307.39 (+249.00; session marks +280.96) · 8 name(s) marked open→close (per-name table). MRNA×7 09:30 $133.11 → close $145.13 +84.14; CYPH×983 09:30 $1.32 → close $1.42 +98.30; XHG×262 09:30 $4.49 → close $4.41 -20.96; ARCT×105 09:30 $11.13 → close $13.45 +243.60; IOVA×129 09:30 $9.08 → close $8.29 -101.91; MRVI×142 09:30 $8.28 → close $8.64 +51.12; AU×9 09:30 $119.43 → close $121.22 +16.11; CAPR×172 09:30 $6.81 → close $6.29 -89.44 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $107.80 | ▲ 09:30 equity $10,164.93 vs yday $9,556.39 (+608.54) | 09:30 open · cash $107.80 (unchanged overnight, no fees) · equity $10,164.93 vs prior close $9,556.39 (+608.54) · 8 name(s) re-marked at the open (per-name table). MRNA×7 yday $145.13 → 09:30 $142.70 -17.01; CYPH×983 yday $1.42 → 09:30 $1.83 +403.03; XHG×262 yday $4.41 → 09:30 $4.32 -23.58; ARCT×105 yday $13.45 → 09:30 $13.33 -12.60; IOVA×129 yday $8.29 → 09:30 $8.08 -27.09; MRVI×142 yday $8.64 → 09:30 $8.59 -7.10; AU×9 yday $121.22 → 09:30 $120.51 -6.39; CAPR×172 yday $6.29 → 09:30 $8.03 +299.28 | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 7 | $142.70 | $2.03 | $-56.12 | $1,104.67 | ▼ -56.12 after sell → book $10,162.90; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 983 | $1.83 | $12.86 | $+642.90 | $2,890.70 | ▲ +642.90 after sell → book $10,150.04; vs 09:30 mark -12.86 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 262 | $4.32 | $3.43 | $-51.35 | $4,019.10 | ▼ -51.35 after sell → book $10,146.60; vs 09:30 mark -3.44 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 105 | $13.33 | $2.33 | $+226.36 | $5,416.42 | ▲ +226.36 after sell → book $10,144.27; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `IOVA` | 129 | $8.08 | $2.41 | $-133.79 | $6,456.33 | ▼ -133.79 after sell → book $10,141.86; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 142 | $8.59 | $2.45 | $+39.15 | $7,673.66 | ▲ +39.15 after sell → book $10,139.41; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 9 | $120.51 | $2.04 | $+5.67 | $8,756.22 | ▲ +5.67 after sell → book $10,137.38; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 172 | $8.03 | $2.55 | $+204.79 | $10,134.83 | ▲ +204.79 after sell → book $10,134.83; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,134.83 | ▲ close $10,134.83 vs 09:30 $10,164.93 (session +0.00) | 16:00 close · cash $10,134.83 · no lots left · equity $10,134.83. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,134.83 | ▲ 09:30 equity $10,134.83 vs yday $10,134.83 (+0.00) | 09:30 open · cash $10,134.83 · no holdings · equity $10,134.83 vs prior close $10,134.83 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 52 | $24.11 | $2.15 | — | $8,878.96 | — | rank by w_hot_cond; rank w_hot_cond; list yday_mover; ret5=+891.7; leftover $1266.85 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 812 | $1.56 | $10.47 | — | $7,601.77 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $1266.85 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 311 | $4.07 | $4.01 | — | $6,331.99 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $1266.85 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 241 | $5.24 | $3.11 | — | $5,066.04 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1266.85 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 66 | $19.04 | $2.19 | — | $3,807.21 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ret5=+49.5; leftover $1266.85 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 356 | $3.55 | $4.59 | — | $2,538.82 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+27.9; leftover $1266.85 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 36 | $35.05 | $2.10 | — | $1,274.92 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1266.85 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 776 | $1.63 | $10.01 | — | $0.03 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1266.85 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.03 | ▲ close $10,677.56 vs 09:30 $10,134.83 (session +581.36) | 16:00 close · cash $0.03 · equity $10,677.56 vs 09:30 $10,134.83 (+542.73; session marks +581.36) · 8 name(s) marked open→close (per-name table). REAX×52 09:30 $24.11 → close $28.43 +224.64; CYPH×812 09:30 $1.56 → close $1.64 +64.96; XHG×311 09:30 $4.07 → close $4.02 -15.55; ALVO×241 09:30 $5.24 → close $5.05 -45.79; ASST×66 09:30 $19.04 → close $21.39 +155.10; GORO×356 09:30 $3.55 → close $3.87 +113.92; EZPW×36 09:30 $35.05 → close $35.23 +6.48; BMEA×776 09:30 $1.63 → close $1.73 +77.60 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.03 | ▼ 09:30 equity $10,424.76 vs yday $10,677.56 (-252.80) | 09:30 open · cash $0.03 (unchanged overnight, no fees) · equity $10,424.76 vs prior close $10,677.56 (-252.80) · 8 name(s) re-marked at the open (per-name table). REAX×52 yday $28.43 → 09:30 $26.61 -94.64; CYPH×812 yday $1.64 → 09:30 $1.60 -32.48; XHG×311 yday $4.02 → 09:30 $3.81 -65.31; ALVO×241 yday $5.05 → 09:30 $4.98 -16.87; ASST×66 yday $21.39 → 09:30 $20.72 -44.22; GORO×356 yday $3.87 → 09:30 $3.77 -35.60; EZPW×36 yday $35.23 → 09:30 $35.70 +16.92; BMEA×776 yday $1.73 → 09:30 $1.75 +19.40 | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 52 | $26.61 | $2.17 | $+125.69 | $1,381.58 | ▲ +125.69 after sell → book $10,422.59; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 812 | $1.60 | $10.62 | $+11.39 | $2,670.16 | ▲ +11.39 after sell → book $10,411.97; vs 09:30 mark -10.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ALVO` | 241 | $4.98 | $3.16 | $-68.93 | $3,867.18 | ▼ -68.93 after sell → book $10,408.81; vs 09:30 mark -3.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 66 | $20.72 | $2.21 | $+106.48 | $5,232.49 | ▲ +106.48 after sell → book $10,406.60; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GORO` | 356 | $3.77 | $4.66 | $+69.07 | $6,569.95 | ▲ +69.07 after sell → book $10,401.94; vs 09:30 mark -4.66 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 36 | $35.70 | $2.12 | $+19.18 | $7,853.03 | ▲ +19.18 after sell → book $10,399.82; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 776 | $1.75 | $10.15 | $+76.84 | $9,204.76 | ▲ +76.84 after sell → book $10,389.67; vs 09:30 mark -10.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 93 | $14.11 | $2.27 | — | $7,890.26 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+11.4; leftover $1314.97 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 226 | $5.81 | $2.92 | — | $6,574.29 | — | rank by w_hot_cond; rank w_hot_cond; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $1314.97 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FIGR` | 32 | $40.50 | $2.09 | — | $5,276.20 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+15.8; leftover $1314.97 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **BUY** | `FUTU` | 10 | $124.67 | $2.02 | — | $4,027.48 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+15.7; leftover $1314.97 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 252 | $5.21 | $3.25 | — | $2,711.31 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot,earn_react; 🔵; ret5=+14.3; leftover $1314.97 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 4 | $267.02 | $2.00 | — | $1,641.23 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $1314.97 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BTG` | 228 | $5.75 | $2.94 | — | $327.29 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+17.9; leftover $1314.97 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $327.29 | ▲ close $10,480.76 vs 09:30 $10,424.76 (session +108.57) | 16:00 close · cash $327.29 · equity $10,480.76 vs 09:30 $10,424.76 (+56.00; session marks +108.57) · 8 name(s) marked open→close (per-name table). XHG×311 09:30 $3.81 → close $4.06 +77.75; BYND×93 09:30 $14.11 → close $14.25 +13.02; USDE×226 09:30 $5.81 → close $5.98 +38.42; FIGR×32 09:30 $40.50 → close $37.08 -109.44; FUTU×10 09:30 $124.67 → close $127.34 +26.70; TIGR×252 09:30 $5.21 → close $5.46 +63.00; FNV×4 09:30 $267.02 → close $267.37 +1.40; BTG×228 09:30 $5.75 → close $5.74 -2.28 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $327.29 | ▲ 09:30 equity $10,615.83 vs yday $10,480.76 (+135.07) | 09:30 open · cash $327.29 (unchanged overnight, no fees) · equity $10,615.83 vs prior close $10,480.76 (+135.07) · 8 name(s) re-marked at the open (per-name table). XHG×311 yday $4.06 → 09:30 $4.06 +0.00; BYND×93 yday $14.25 → 09:30 $14.20 -4.65; USDE×226 yday $5.98 → 09:30 $6.50 +117.52; FIGR×32 yday $37.08 → 09:30 $37.42 +10.88; FUTU×10 yday $127.34 → 09:30 $128.00 +6.60; TIGR×252 yday $5.46 → 09:30 $5.49 +7.56; FNV×4 yday $267.37 → 09:30 $267.23 -0.56; BTG×228 yday $5.74 → 09:30 $5.73 -2.28 | — |
| 2026-08-27 09:30 ET | **SELL** | `XHG` | 311 | $4.06 | $4.07 | $-11.20 | $1,585.88 | ▼ -11.20 after sell → book $10,611.76; vs 09:30 mark -4.07 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 93 | $14.20 | $2.29 | $+3.81 | $2,904.18 | ▲ +3.81 after sell → book $10,609.46; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 226 | $6.50 | $2.96 | $+150.06 | $4,370.22 | ▲ +150.06 after sell → book $10,606.50; vs 09:30 mark -2.96 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FIGR` | 32 | $37.42 | $2.11 | $-102.75 | $5,565.55 | ▼ -102.75 after sell → book $10,604.39; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FUTU` | 10 | $128.00 | $2.04 | $+29.24 | $6,843.51 | ▲ +29.24 after sell → book $10,602.35; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TIGR` | 252 | $5.49 | $3.30 | $+64.01 | $8,223.69 | ▲ +64.01 after sell → book $10,599.05; vs 09:30 mark -3.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 4 | $267.23 | $2.02 | $-3.18 | $9,290.58 | ▼ -3.18 after sell → book $10,597.02; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BTG` | 228 | $5.73 | $2.99 | $-10.49 | $10,594.03 | ▼ -10.49 after sell → book $10,594.03; vs 09:30 mark -2.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 509 | $2.60 | $6.57 | — | $9,264.07 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ret5=+13.0; leftover $1324.25 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PGY` | 57 | $22.93 | $2.16 | — | $7,954.90 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=+9.5; leftover $1324.25 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 44 | $29.83 | $2.12 | — | $6,640.26 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=+7.6; leftover $1324.25 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 5 | $253.44 | $2.00 | — | $5,371.05 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=+3.3; leftover $1324.25 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ANET` | 6 | $205.90 | $2.01 | — | $4,133.64 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=+8.5; leftover $1324.25 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 55 | $24.00 | $2.15 | — | $2,811.49 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ret5=+8.7; leftover $1324.25 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $1,842.48 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=+0.1; leftover $1324.25 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 289 | $4.57 | $3.73 | — | $518.03 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=+1.1; leftover $1324.25 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $518.03 | ▼ close $10,543.89 vs 09:30 $10,615.83 (session -27.41) | 16:00 close · cash $518.03 · equity $10,543.89 vs 09:30 $10,615.83 (-71.94; session marks -27.41) · 8 name(s) marked open→close (per-name table). SLI×509 09:30 $2.60 → close $2.64 +20.36; PGY×57 09:30 $22.93 → close $23.26 +18.81; GEN×44 09:30 $29.83 → close $30.50 +29.48; MRVL×5 09:30 $253.44 → close $241.45 -59.95; ANET×6 09:30 $205.90 → close $201.09 -28.86; MOS×55 09:30 $24.00 → close $23.76 -13.20; MU×1 09:30 $967.01 → close $935.39 -31.62; GGB×289 09:30 $4.57 → close $4.70 +37.57 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $518.03 | ▼ 09:30 equity $10,459.59 vs yday $10,543.89 (-84.30) | 09:30 open · cash $518.03 (unchanged overnight, no fees) · equity $10,459.59 vs prior close $10,543.89 (-84.30) · 8 name(s) re-marked at the open (per-name table). SLI×509 yday $2.64 → 09:30 $2.68 +20.36; PGY×57 yday $23.26 → 09:30 $23.21 -2.85; GEN×44 yday $30.50 → 09:30 $30.50 +0.00; MRVL×5 yday $241.45 → 09:30 $225.26 -80.95; ANET×6 yday $201.09 → 09:30 $200.00 -6.54; MOS×55 yday $23.76 → 09:30 $23.95 +10.45; MU×1 yday $935.39 → 09:30 $919.29 -16.10; GGB×289 yday $4.70 → 09:30 $4.67 -8.67 | — |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 509 | $2.68 | $6.66 | $+27.49 | $1,875.49 | ▲ +27.49 after sell → book $10,452.93; vs 09:30 mark -6.66 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `PGY` | 57 | $23.21 | $2.18 | $+11.62 | $3,196.27 | ▲ +11.62 after sell → book $10,450.74; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 44 | $30.50 | $2.14 | $+25.22 | $4,536.13 | ▲ +25.22 after sell → book $10,448.60; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MRVL` | 5 | $225.26 | $2.02 | $-144.93 | $5,660.41 | ▼ -144.93 after sell → book $10,446.58; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `ANET` | 6 | $200.00 | $2.03 | $-39.44 | $6,858.38 | ▼ -39.44 after sell → book $10,444.55; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 55 | $23.95 | $2.18 | $-7.08 | $8,173.45 | ▼ -7.08 after sell → book $10,442.37; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $9,090.73 | ▼ -51.73 after sell → book $10,440.36; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 289 | $4.67 | $3.79 | $+21.38 | $10,436.57 | ▲ +21.38 after sell → book $10,436.57; vs 09:30 mark -3.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 93 | $14.00 | $2.27 | — | $9,132.30 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=-3.3; leftover $1304.57 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 134 | $9.73 | $2.39 | — | $7,826.09 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+47.1; leftover $1304.57 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $146.07 | $2.01 | — | $6,655.52 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1304.57 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 55 | $23.30 | $2.15 | — | $5,371.86 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+14.5; leftover $1304.57 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MEI` | 73 | $17.78 | $2.21 | — | $4,071.71 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ⚪; ret5=+22.9; leftover $1304.57 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VYX` | 142 | $9.13 | $2.42 | — | $2,772.84 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ret5=+20.0; leftover $1304.57 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $1,494.98 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1304.57 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SNPS` | 2 | $461.85 | $2.00 | — | $569.28 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+16.8; leftover $1304.57 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $569.28 | ▼ close $10,236.97 vs 09:30 $10,459.59 (session -182.13) | 16:00 close · cash $569.28 · equity $10,236.97 vs 09:30 $10,459.59 (-222.62; session marks -182.13) · 8 name(s) marked open→close (per-name table). BYND×93 09:30 $14.00 → close $13.86 -13.02; CAPR×134 09:30 $9.73 → close $9.59 -18.76; ANF×8 09:30 $146.07 → close $148.42 +18.80; NCNO×55 09:30 $23.30 → close $22.99 -17.05; MEI×73 09:30 $17.78 → close $18.21 +31.39; VYX×142 09:30 $9.13 → close $8.78 -49.70; SMTC×9 09:30 $141.76 → close $131.17 -95.31; SNPS×2 09:30 $461.85 → close $442.61 -38.48 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $569.28 | ▼ 09:30 equity $10,178.42 vs yday $10,236.97 (-58.55) | 09:30 open · cash $569.28 (unchanged overnight, no fees) · equity $10,178.42 vs prior close $10,236.97 (-58.55) · 8 name(s) re-marked at the open (per-name table). BYND×93 yday $13.86 → 09:30 $13.81 -4.65; CAPR×134 yday $9.59 → 09:30 $9.50 -12.06; ANF×8 yday $148.42 → 09:30 $148.03 -3.12; NCNO×55 yday $22.99 → 09:30 $22.66 -18.15; MEI×73 yday $18.21 → 09:30 $18.15 -4.38; VYX×142 yday $8.78 → 09:30 $8.66 -17.04; SMTC×9 yday $131.17 → 09:30 $132.30 +10.17; SNPS×2 yday $442.61 → 09:30 $437.95 -9.32 | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 134 | $9.50 | $2.42 | $-35.64 | $1,839.86 | ▼ -35.64 after sell → book $10,176.00; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 8 | $148.03 | $2.03 | $+11.63 | $3,022.07 | ▲ +11.63 after sell → book $10,173.97; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NCNO` | 55 | $22.66 | $2.17 | $-39.53 | $4,266.19 | ▼ -39.53 after sell → book $10,171.79; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `MEI` | 73 | $18.15 | $2.23 | $+22.57 | $5,588.91 | ▲ +22.57 after sell → book $10,169.56; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `VYX` | 142 | $8.66 | $2.45 | $-71.61 | $6,816.18 | ▼ -71.61 after sell → book $10,167.11; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $8,004.84 | ▼ -89.19 after sell → book $10,165.07; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SNPS` | 2 | $437.95 | $2.02 | $-51.81 | $8,878.73 | ▼ -51.81 after sell → book $10,163.06; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,878.73 | ▼ close $10,115.63 vs 09:30 $10,178.42 (session -47.43) | 16:00 close · cash $8,878.73 · equity $10,115.63 vs 09:30 $10,178.42 (-62.79; session marks -47.43) · 1 name(s) marked open→close (per-name table). BYND×93 09:30 $13.81 → close $13.30 -47.43 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,878.73 | ▼ 09:30 equity $10,091.45 vs yday $10,115.63 (-24.18) | 09:30 open · cash $8,878.73 (unchanged overnight, no fees) · equity $10,091.45 vs prior close $10,115.63 (-24.18) · 1 name(s) re-marked at the open (per-name table). BYND×93 yday $13.30 → 09:30 $13.04 -24.18 | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 93 | $13.04 | $2.29 | $-93.84 | $10,089.15 | ▼ -93.84 after sell → book $10,089.15; vs 09:30 mark -2.30 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,089.15 | ▲ close $10,089.15 vs 09:30 $10,091.45 (session +0.00) | 16:00 close · cash $10,089.15 · no lots left · equity $10,089.15. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,089.15 | ▲ 09:30 equity $10,089.15 vs yday $10,089.15 (+0.00) | 09:30 open · cash $10,089.15 · no holdings · equity $10,089.15 vs prior close $10,089.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,089.15 | ▲ close $10,089.15 vs 09:30 $10,089.15 (session +0.00) | 16:00 close · cash $10,089.15 · no lots left · equity $10,089.15. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,089.15 | ▲ 09:30 equity $10,089.15 vs yday $10,089.15 (+0.00) | 09:30 open · cash $10,089.15 · no holdings · equity $10,089.15 vs prior close $10,089.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 708 | $1.78 | $9.13 | — | $8,819.78 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+183.1; leftover $1261.14 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 68 | $18.40 | $2.19 | — | $7,566.39 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=-32.2; leftover $1261.14 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 653 | $1.93 | $8.42 | — | $6,297.67 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1261.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 52 | $23.88 | $2.15 | — | $5,053.77 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1261.14 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AGCO` | 9 | $127.91 | $2.02 | — | $3,900.56 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.6; leftover $1261.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 75 | $16.77 | $2.21 | — | $2,640.59 | — | rank by w_hot_cond; rank w_hot_cond; list probable,yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1261.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 121 | $10.42 | $2.35 | — | $1,377.42 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; leftover $1261.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 157 | $8.03 | $2.46 | — | $114.25 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1261.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $114.25 | ▼ close $9,639.86 vs 09:30 $10,089.15 (session -418.35) | 16:00 close · cash $114.25 · equity $9,639.86 vs 09:30 $10,089.15 (-449.29; session marks -418.35) · 8 name(s) marked open→close (per-name table). GPRO×708 09:30 $1.78 → close $1.39 -276.12; REAX×68 09:30 $18.40 → close $18.40 +0.00; BMEA×653 09:30 $1.93 → close $1.91 -13.06; MMED×52 09:30 $23.88 → close $23.84 -2.08; AGCO×9 09:30 $127.91 → close $125.82 -18.81; ARCT×75 09:30 $16.77 → close $15.56 -90.75; NVAX×121 09:30 $10.42 → close $10.34 -9.68; VSTM×157 09:30 $8.03 → close $7.98 -7.85 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $114.25 | ▲ 09:30 equity $9,686.77 vs yday $9,639.86 (+46.91) | 09:30 open · cash $114.25 (unchanged overnight, no fees) · equity $9,686.77 vs prior close $9,639.86 (+46.91) · 8 name(s) re-marked at the open (per-name table). GPRO×708 yday $1.39 → 09:30 $1.48 +63.72; REAX×68 yday $18.40 → 09:30 $18.15 -17.00; BMEA×653 yday $1.91 → 09:30 $1.90 -6.53; MMED×52 yday $23.84 → 09:30 $23.84 +0.00; AGCO×9 yday $125.82 → 09:30 $125.22 -5.40; ARCT×75 yday $15.56 → 09:30 $15.61 +3.75; NVAX×121 yday $10.34 → 09:30 $10.50 +19.36; VSTM×157 yday $7.98 → 09:30 $7.91 -10.99 | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 68 | $18.15 | $2.22 | $-21.41 | $1,346.23 | ▼ -21.41 after sell → book $9,684.55; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `BMEA` | 653 | $1.90 | $8.54 | $-36.56 | $2,578.39 | ▼ -36.56 after sell → book $9,676.01; vs 09:30 mark -8.54 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 52 | $23.84 | $2.17 | $-6.39 | $3,815.91 | ▼ -6.39 after sell → book $9,673.85; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AGCO` | 9 | $125.22 | $2.04 | $-28.26 | $4,940.85 | ▼ -28.26 after sell → book $9,671.81; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 75 | $15.61 | $2.24 | $-91.45 | $6,109.36 | ▼ -91.45 after sell → book $9,669.57; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 121 | $10.50 | $2.38 | $+4.94 | $7,377.48 | ▲ +4.94 after sell → book $9,667.19; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 157 | $7.91 | $2.50 | $-23.80 | $8,616.85 | ▼ -23.80 after sell → book $9,664.69; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 212 | $5.79 | $2.73 | — | $7,386.64 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $1230.98 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `FRNM` | 75 | $16.40 | $2.21 | — | $6,154.42 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ⚪; ret5=+21.2; leftover $1230.98 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 14 | $82.70 | $2.03 | — | $4,994.59 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+15.7; leftover $1230.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 490 | $2.51 | $6.32 | — | $3,758.37 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+21.8; leftover $1230.98 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 271 | $4.53 | $3.50 | — | $2,527.24 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.5; leftover $1230.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `LENZ` | 214 | $5.75 | $2.76 | — | $1,293.98 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+21.2; leftover $1230.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 48 | $25.18 | $2.13 | — | $83.21 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+16.0; leftover $1230.98 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $83.21 | ▲ close $10,172.55 vs 09:30 $9,686.77 (session +529.55) | 16:00 close · cash $83.21 · equity $10,172.55 vs 09:30 $9,686.77 (+485.78; session marks +529.55) · 8 name(s) marked open→close (per-name table). GPRO×708 09:30 $1.48 → close $1.70 +155.76; DFDV×212 09:30 $5.79 → close $5.87 +16.96; FRNM×75 09:30 $16.40 → close $16.31 -6.75; TARS×14 09:30 $82.70 → close $90.78 +113.12; BRR×490 09:30 $2.51 → close $2.66 +73.50; IRD×271 09:30 $4.53 → close $4.67 +37.94; LENZ×214 09:30 $5.75 → close $5.96 +44.94; ASST×48 09:30 $25.18 → close $27.14 +94.08 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $83.21 | ▼ 09:30 equity $10,007.28 vs yday $10,172.55 (-165.27) | 09:30 open · cash $83.21 (unchanged overnight, no fees) · equity $10,007.28 vs prior close $10,172.55 (-165.27) · 8 name(s) re-marked at the open (per-name table). GPRO×708 yday $1.70 → 09:30 $1.56 -95.58; DFDV×212 yday $5.87 → 09:30 $5.81 -12.72; FRNM×75 yday $16.31 → 09:30 $16.74 +32.25; TARS×14 yday $90.78 → 09:30 $89.67 -15.54; BRR×490 yday $2.66 → 09:30 $2.66 +0.00; IRD×271 yday $4.67 → 09:30 $4.53 -37.94; LENZ×214 yday $5.96 → 09:30 $5.95 -2.14; ASST×48 yday $27.14 → 09:30 $26.44 -33.60 | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 708 | $1.56 | $9.26 | $-170.61 | $1,181.97 | ▼ -170.61 after sell → book $9,998.02; vs 09:30 mark -9.26 | dropped from list after 2 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 212 | $5.81 | $2.78 | $-1.27 | $2,410.91 | ▼ -1.27 after sell → book $9,995.24; vs 09:30 mark -2.78 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 75 | $16.74 | $2.24 | $+21.05 | $3,664.17 | ▲ +21.05 after sell → book $9,993.00; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TARS` | 14 | $89.67 | $2.05 | $+93.50 | $4,917.50 | ▲ +93.50 after sell → book $9,990.95; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BRR` | 490 | $2.66 | $6.41 | $+60.77 | $6,214.48 | ▲ +60.77 after sell → book $9,984.53; vs 09:30 mark -6.42 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `IRD` | 271 | $4.53 | $3.55 | $-7.05 | $7,438.56 | ▼ -7.05 after sell → book $9,980.98; vs 09:30 mark -3.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LENZ` | 214 | $5.95 | $2.81 | $+37.23 | $8,709.06 | ▲ +37.23 after sell → book $9,978.18; vs 09:30 mark -2.80 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 48 | $26.44 | $2.15 | $+56.19 | $9,976.02 | ▲ +56.19 after sell → book $9,976.02; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,976.02 | ▲ close $9,976.02 vs 09:30 $10,007.28 (session +0.00) | 16:00 close · cash $9,976.02 · no lots left · equity $9,976.02. | — |

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
