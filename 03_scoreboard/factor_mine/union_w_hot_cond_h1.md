# Factor mine action — `union_w_hot_cond_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `w_hot_cond` · size `leftover` · sell `list` · S-boost `none` · rank by w_hot_cond

Cash book **-2.52%** ($9,748) · signal-only (no cash/fees) was +3.69%. Starts YES **6/19**. Fills 168 · skips 62 · realized $-252.48.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,747.55.

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
| 2026-08-14 | `AMPY` | 260 | — | $4.94 | +0.00 | $4.78 | -41.60 | -41.60 | +0.00 | -41.60 |
| 2026-08-14 | `LIFE` | 36 | — | $35.04 | +0.00 | $34.02 | -36.72 | -36.72 | +0.00 | -36.72 |
| 2026-08-17 | `QMCO` | 52 | $26.11 | $24.83 | -66.56 | — | +0.00 | -66.56 | +7.80 | — |
| 2026-08-17 | `ZENA` | 583 | $2.14 | $2.08 | -32.07 | — | +0.00 | -32.07 | -67.05 | — |
| 2026-08-17 | `ARX` | 65 | $19.58 | $19.57 | -0.65 | — | +0.00 | -0.65 | +0.00 | — |
| 2026-08-17 | `AIRO` | 115 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -178.25 | — |
| 2026-08-17 | `BZAI` | 1677 | $0.59 | $0.55 | -68.76 | — | +0.00 | -68.76 | -358.88 | — |
| 2026-08-17 | `BRUN` | 48 | $22.93 | $23.00 | +3.36 | — | +0.00 | +3.36 | -155.76 | — |
| 2026-08-17 | `AMPY` | 260 | $4.78 | $4.86 | +20.80 | — | +0.00 | +20.80 | -20.80 | — |
| 2026-08-17 | `LIFE` | 36 | $34.02 | $34.03 | +0.36 | — | +0.00 | +0.36 | -36.36 | — |
| 2026-08-17 | `XHG` | 280 | — | $4.19 | +0.00 | $3.91 | -78.40 | -78.40 | +0.00 | -78.40 |
| 2026-08-17 | `CAPR` | 170 | — | $6.87 | +0.00 | $7.45 | +98.60 | +98.60 | +0.00 | +98.60 |
| 2026-08-17 | `STDN` | 86 | — | $13.64 | +0.00 | $13.31 | -28.38 | -28.38 | +0.00 | -28.38 |
| 2026-08-17 | `HTFL` | 28 | — | $41.23 | +0.00 | $41.94 | +19.88 | +19.88 | +0.00 | +19.88 |
| 2026-08-17 | `UMAC` | 36 | — | $32.55 | +0.00 | $30.15 | -86.40 | -86.40 | +0.00 | -86.40 |
| 2026-08-17 | `ALOY` | 80 | — | $14.66 | +0.00 | $13.86 | -64.40 | -64.40 | +0.00 | -64.40 |
| 2026-08-17 | `LPTH` | 78 | — | $14.94 | +0.00 | $14.80 | -10.92 | -10.92 | +0.00 | -10.92 |
| 2026-08-17 | `NPWR` | 611 | — | $1.92 | +0.00 | $1.73 | -116.09 | -116.09 | +0.00 | -116.09 |
| 2026-08-18 | `XHG` | 280 | $3.91 | $3.94 | +8.40 | — | +0.00 | +8.40 | -70.00 | — |
| 2026-08-18 | `CAPR` | 170 | $7.45 | $7.50 | +8.50 | $7.08 | -71.40 | -62.90 | +107.10 | +35.70 |
| 2026-08-18 | `STDN` | 86 | $13.31 | $13.31 | +0.00 | — | +0.00 | +0.00 | -28.38 | — |
| 2026-08-18 | `HTFL` | 28 | $41.94 | $41.50 | -12.32 | — | +0.00 | -12.32 | +7.56 | — |
| 2026-08-18 | `UMAC` | 36 | $30.15 | $28.59 | -56.16 | — | +0.00 | -56.16 | -142.56 | — |
| 2026-08-18 | `ALOY` | 80 | $13.86 | $13.19 | -53.20 | — | +0.00 | -53.20 | -117.60 | — |
| 2026-08-18 | `LPTH` | 78 | $14.80 | $14.01 | -61.62 | — | +0.00 | -61.62 | -72.54 | — |
| 2026-08-18 | `NPWR` | 611 | $1.73 | $1.70 | -18.33 | — | +0.00 | -18.33 | -134.42 | — |
| 2026-08-19 | `CAPR` | 170 | $7.08 | $7.19 | +18.70 | — | +0.00 | +18.70 | +54.40 | — |
| 2026-08-20 | `MRNA` | 7 | — | $150.14 | +0.00 | $133.32 | -117.74 | -117.74 | +0.00 | -117.74 |
| 2026-08-20 | `CYPH` | 960 | — | $1.15 | +0.00 | $1.19 | +38.40 | +38.40 | +0.00 | +38.40 |
| 2026-08-20 | `ABCL` | 93 | — | $11.81 | +0.00 | $11.57 | -22.78 | -22.78 | +0.00 | -22.78 |
| 2026-08-20 | `SENS` | 124 | — | $8.91 | +0.00 | $8.82 | -11.16 | -11.16 | +0.00 | -11.16 |
| 2026-08-20 | `AUTL` | 447 | — | $2.47 | +0.00 | $2.46 | -4.47 | -4.47 | +0.00 | -4.47 |
| 2026-08-20 | `TEM` | 17 | — | $61.83 | +0.00 | $66.65 | +81.94 | +81.94 | +0.00 | +81.94 |
| 2026-08-20 | `WPM` | 7 | — | $144.54 | +0.00 | $150.25 | +39.97 | +39.97 | +0.00 | +39.97 |
| 2026-08-20 | `IAG` | 56 | — | $19.63 | +0.00 | $20.50 | +48.72 | +48.72 | +0.00 | +48.72 |
| 2026-08-21 | `MRNA` | 7 | $133.32 | $133.11 | -1.47 | $145.13 | +84.14 | +82.67 | -119.21 | -35.07 |
| 2026-08-21 | `CYPH` | 960 | $1.19 | $1.32 | +124.80 | $1.42 | +96.00 | +220.80 | +163.20 | +259.20 |
| 2026-08-21 | `ABCL` | 93 | $11.57 | $11.57 | +0.00 | — | +0.00 | +0.00 | -22.78 | — |
| 2026-08-21 | `SENS` | 124 | $8.82 | $9.24 | +52.08 | — | +0.00 | +52.08 | +40.92 | — |
| 2026-08-21 | `AUTL` | 447 | $2.46 | $2.47 | +4.47 | — | +0.00 | +4.47 | +0.00 | — |
| 2026-08-21 | `TEM` | 17 | $66.65 | $65.60 | -17.85 | — | +0.00 | -17.85 | +64.09 | — |
| 2026-08-21 | `WPM` | 7 | $150.25 | $154.70 | +31.15 | — | +0.00 | +31.15 | +71.12 | — |
| 2026-08-21 | `IAG` | 56 | $20.50 | $21.17 | +37.52 | — | +0.00 | +37.52 | +86.24 | — |
| 2026-08-21 | `XHG` | 255 | — | $4.49 | +0.00 | $4.41 | -20.40 | -20.40 | +0.00 | -20.40 |
| 2026-08-21 | `ARCT` | 102 | — | $11.13 | +0.00 | $13.45 | +236.64 | +236.64 | +0.00 | +236.64 |
| 2026-08-21 | `IOVA` | 126 | — | $9.08 | +0.00 | $8.29 | -99.54 | -99.54 | +0.00 | -99.54 |
| 2026-08-21 | `MRVI` | 138 | — | $8.28 | +0.00 | $8.64 | +49.68 | +49.68 | +0.00 | +49.68 |
| 2026-08-21 | `AU` | 9 | — | $119.43 | +0.00 | $121.22 | +16.11 | +16.11 | +0.00 | +16.11 |
| 2026-08-21 | `CAPR` | 168 | — | $6.81 | +0.00 | $6.29 | -87.36 | -87.36 | +0.00 | -87.36 |
| 2026-08-24 | `MRNA` | 7 | $145.13 | $142.70 | -17.01 | — | +0.00 | -17.01 | -52.08 | — |
| 2026-08-24 | `CYPH` | 960 | $1.42 | $1.83 | +393.60 | — | +0.00 | +393.60 | +652.80 | — |
| 2026-08-24 | `XHG` | 255 | $4.41 | $4.32 | -22.95 | — | +0.00 | -22.95 | -43.35 | — |
| 2026-08-24 | `ARCT` | 102 | $13.45 | $13.33 | -12.24 | — | +0.00 | -12.24 | +224.40 | — |
| 2026-08-24 | `IOVA` | 126 | $8.29 | $8.08 | -26.46 | — | +0.00 | -26.46 | -126.00 | — |
| 2026-08-24 | `MRVI` | 138 | $8.64 | $8.59 | -6.90 | — | +0.00 | -6.90 | +42.78 | — |
| 2026-08-24 | `AU` | 9 | $121.22 | $120.51 | -6.39 | — | +0.00 | -6.39 | +9.72 | — |
| 2026-08-24 | `CAPR` | 168 | $6.29 | $8.03 | +292.32 | — | +0.00 | +292.32 | +204.96 | — |
| 2026-08-25 | `REAX` | 51 | — | $24.11 | +0.00 | $28.43 | +220.32 | +220.32 | +0.00 | +220.32 |
| 2026-08-25 | `CYPH` | 793 | — | $1.56 | +0.00 | $1.64 | +63.44 | +63.44 | +0.00 | +63.44 |
| 2026-08-25 | `XHG` | 304 | — | $4.07 | +0.00 | $4.02 | -15.20 | -15.20 | +0.00 | -15.20 |
| 2026-08-25 | `ALVO` | 236 | — | $5.24 | +0.00 | $5.05 | -44.84 | -44.84 | +0.00 | -44.84 |
| 2026-08-25 | `ASST` | 64 | — | $19.04 | +0.00 | $21.39 | +150.40 | +150.40 | +0.00 | +150.40 |
| 2026-08-25 | `GORO` | 348 | — | $3.55 | +0.00 | $3.87 | +111.36 | +111.36 | +0.00 | +111.36 |
| 2026-08-25 | `EZPW` | 35 | — | $35.05 | +0.00 | $35.23 | +6.30 | +6.30 | +0.00 | +6.30 |
| 2026-08-25 | `BMEA` | 759 | — | $1.63 | +0.00 | $1.73 | +75.90 | +75.90 | +0.00 | +75.90 |
| 2026-08-26 | `REAX` | 51 | $28.43 | $26.61 | -92.82 | — | +0.00 | -92.82 | +127.50 | — |
| 2026-08-26 | `CYPH` | 793 | $1.64 | $1.60 | -31.72 | — | +0.00 | -31.72 | +31.72 | — |
| 2026-08-26 | `XHG` | 304 | $4.02 | $3.81 | -63.84 | $4.06 | +76.00 | +12.16 | -79.04 | -3.04 |
| 2026-08-26 | `ALVO` | 236 | $5.05 | $4.98 | -16.52 | — | +0.00 | -16.52 | -61.36 | — |
| 2026-08-26 | `ASST` | 64 | $21.39 | $20.72 | -42.88 | — | +0.00 | -42.88 | +107.52 | — |
| 2026-08-26 | `GORO` | 348 | $3.87 | $3.77 | -34.80 | — | +0.00 | -34.80 | +76.56 | — |
| 2026-08-26 | `EZPW` | 35 | $35.23 | $35.70 | +16.45 | — | +0.00 | +16.45 | +22.75 | — |
| 2026-08-26 | `BMEA` | 759 | $1.73 | $1.75 | +18.97 | — | +0.00 | +18.97 | +94.88 | — |
| 2026-08-26 | `BYND` | 91 | — | $14.11 | +0.00 | $14.25 | +12.74 | +12.74 | +0.00 | +12.74 |
| 2026-08-26 | `USDE` | 221 | — | $5.81 | +0.00 | $5.98 | +37.57 | +37.57 | +0.00 | +37.57 |
| 2026-08-26 | `FIGR` | 31 | — | $40.50 | +0.00 | $37.08 | -106.02 | -106.02 | +0.00 | -106.02 |
| 2026-08-26 | `FUTU` | 10 | — | $124.67 | +0.00 | $127.34 | +26.70 | +26.70 | +0.00 | +26.70 |
| 2026-08-26 | `TIGR` | 246 | — | $5.21 | +0.00 | $5.46 | +61.50 | +61.50 | +0.00 | +61.50 |
| 2026-08-26 | `FNV` | 4 | — | $267.02 | +0.00 | $267.37 | +1.40 | +1.40 | +0.00 | +1.40 |
| 2026-08-26 | `BTG` | 223 | — | $5.75 | +0.00 | $5.74 | -2.23 | -2.23 | +0.00 | -2.23 |
| 2026-08-27 | `XHG` | 304 | $4.06 | $4.06 | +0.00 | — | +0.00 | +0.00 | -3.04 | — |
| 2026-08-27 | `BYND` | 91 | $14.25 | $14.20 | -4.55 | — | +0.00 | -4.55 | +8.19 | — |
| 2026-08-27 | `USDE` | 221 | $5.98 | $6.50 | +114.92 | — | +0.00 | +114.92 | +152.49 | — |
| 2026-08-27 | `FIGR` | 31 | $37.08 | $37.42 | +10.54 | — | +0.00 | +10.54 | -95.48 | — |
| 2026-08-27 | `FUTU` | 10 | $127.34 | $128.00 | +6.60 | — | +0.00 | +6.60 | +33.30 | — |
| 2026-08-27 | `TIGR` | 246 | $5.46 | $5.49 | +7.38 | — | +0.00 | +7.38 | +68.88 | — |
| 2026-08-27 | `FNV` | 4 | $267.37 | $267.23 | -0.56 | — | +0.00 | -0.56 | +0.84 | — |
| 2026-08-27 | `BTG` | 223 | $5.74 | $5.73 | -2.23 | — | +0.00 | -2.23 | -4.46 | — |
| 2026-08-27 | `SLI` | 497 | — | $2.60 | +0.00 | $2.64 | +19.88 | +19.88 | +0.00 | +19.88 |
| 2026-08-27 | `PGY` | 56 | — | $22.93 | +0.00 | $23.26 | +18.48 | +18.48 | +0.00 | +18.48 |
| 2026-08-27 | `GEN` | 43 | — | $29.83 | +0.00 | $30.50 | +28.81 | +28.81 | +0.00 | +28.81 |
| 2026-08-27 | `MRVL` | 5 | — | $253.44 | +0.00 | $241.45 | -59.95 | -59.95 | +0.00 | -59.95 |
| 2026-08-27 | `ANET` | 6 | — | $205.90 | +0.00 | $201.09 | -28.86 | -28.86 | +0.00 | -28.86 |
| 2026-08-27 | `MOS` | 53 | — | $24.00 | +0.00 | $23.76 | -12.72 | -12.72 | +0.00 | -12.72 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-27 | `GGB` | 283 | — | $4.57 | +0.00 | $4.70 | +36.79 | +36.79 | +0.00 | +36.79 |
| 2026-08-28 | `SLI` | 497 | $2.64 | $2.68 | +19.88 | — | +0.00 | +19.88 | +39.76 | — |
| 2026-08-28 | `PGY` | 56 | $23.26 | $23.21 | -2.80 | — | +0.00 | -2.80 | +15.68 | — |
| 2026-08-28 | `GEN` | 43 | $30.50 | $30.50 | +0.00 | — | +0.00 | +0.00 | +28.81 | — |
| 2026-08-28 | `MRVL` | 5 | $241.45 | $225.26 | -80.95 | — | +0.00 | -80.95 | -140.90 | — |
| 2026-08-28 | `ANET` | 6 | $201.09 | $200.00 | -6.54 | — | +0.00 | -6.54 | -35.40 | — |
| 2026-08-28 | `MOS` | 53 | $23.76 | $23.95 | +10.07 | — | +0.00 | +10.07 | -2.65 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `GGB` | 283 | $4.70 | $4.67 | -8.49 | — | +0.00 | -8.49 | +28.30 | — |
| 2026-08-28 | `BYND` | 90 | — | $14.00 | +0.00 | $13.86 | -12.60 | -12.60 | +0.00 | -12.60 |
| 2026-08-28 | `CAPR` | 130 | — | $9.73 | +0.00 | $9.59 | -18.20 | -18.20 | +0.00 | -18.20 |
| 2026-08-28 | `ANF` | 8 | — | $146.07 | +0.00 | $148.42 | +18.80 | +18.80 | +0.00 | +18.80 |
| 2026-08-28 | `NCNO` | 54 | — | $23.30 | +0.00 | $22.99 | -16.74 | -16.74 | +0.00 | -16.74 |
| 2026-08-28 | `MEI` | 71 | — | $17.78 | +0.00 | $18.21 | +30.53 | +30.53 | +0.00 | +30.53 |
| 2026-08-28 | `VYX` | 139 | — | $9.13 | +0.00 | $8.78 | -48.65 | -48.65 | +0.00 | -48.65 |
| 2026-08-28 | `SMTC` | 8 | — | $141.76 | +0.00 | $131.17 | -84.72 | -84.72 | +0.00 | -84.72 |
| 2026-08-28 | `SNPS` | 2 | — | $461.85 | +0.00 | $442.61 | -38.48 | -38.48 | +0.00 | -38.48 |
| 2026-08-31 | `BYND` | 90 | $13.86 | $13.81 | -4.50 | $13.30 | -45.90 | -50.40 | -17.10 | -63.00 |
| 2026-08-31 | `CAPR` | 130 | $9.59 | $9.50 | -11.70 | — | +0.00 | -11.70 | -29.90 | — |
| 2026-08-31 | `ANF` | 8 | $148.42 | $148.03 | -3.12 | — | +0.00 | -3.12 | +15.68 | — |
| 2026-08-31 | `NCNO` | 54 | $22.99 | $22.66 | -17.82 | — | +0.00 | -17.82 | -34.56 | — |
| 2026-08-31 | `MEI` | 71 | $18.21 | $18.15 | -4.26 | — | +0.00 | -4.26 | +26.27 | — |
| 2026-08-31 | `VYX` | 139 | $8.78 | $8.66 | -16.68 | — | +0.00 | -16.68 | -65.33 | — |
| 2026-08-31 | `SMTC` | 8 | $131.17 | $132.30 | +9.04 | — | +0.00 | +9.04 | -75.68 | — |
| 2026-08-31 | `SNPS` | 2 | $442.61 | $437.95 | -9.32 | — | +0.00 | -9.32 | -47.80 | — |
| 2026-09-01 | `BYND` | 90 | $13.30 | $13.04 | -23.40 | — | +0.00 | -23.40 | -86.40 | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `GPRO` | 692 | — | $1.78 | +0.00 | $1.39 | -269.88 | -269.88 | +0.00 | -269.88 |
| 2026-09-03 | `REAX` | 66 | — | $18.40 | +0.00 | $18.40 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `BMEA` | 638 | — | $1.93 | +0.00 | $1.91 | -12.76 | -12.76 | +0.00 | -12.76 |
| 2026-09-03 | `MMED` | 51 | — | $23.88 | +0.00 | $23.84 | -2.04 | -2.04 | +0.00 | -2.04 |
| 2026-09-03 | `AGCO` | 9 | — | $127.91 | +0.00 | $125.82 | -18.81 | -18.81 | +0.00 | -18.81 |
| 2026-09-03 | `ARCT` | 73 | — | $16.77 | +0.00 | $15.56 | -88.33 | -88.33 | +0.00 | -88.33 |
| 2026-09-03 | `NVAX` | 118 | — | $10.42 | +0.00 | $10.34 | -9.44 | -9.44 | +0.00 | -9.44 |
| 2026-09-03 | `VSTM` | 153 | — | $8.03 | +0.00 | $7.98 | -7.65 | -7.65 | +0.00 | -7.65 |
| 2026-09-04 | `GPRO` | 692 | $1.39 | $1.48 | +62.28 | $1.70 | +152.24 | +214.52 | -207.60 | -55.36 |
| 2026-09-04 | `REAX` | 66 | $18.40 | $18.15 | -16.50 | — | +0.00 | -16.50 | -16.50 | — |
| 2026-09-04 | `BMEA` | 638 | $1.91 | $1.90 | -6.38 | — | +0.00 | -6.38 | -19.14 | — |
| 2026-09-04 | `MMED` | 51 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -2.04 | — |
| 2026-09-04 | `AGCO` | 9 | $125.82 | $125.22 | -5.40 | — | +0.00 | -5.40 | -24.21 | — |
| 2026-09-04 | `ARCT` | 73 | $15.56 | $15.61 | +3.65 | — | +0.00 | +3.65 | -84.68 | — |
| 2026-09-04 | `NVAX` | 118 | $10.34 | $10.50 | +18.88 | — | +0.00 | +18.88 | +9.44 | — |
| 2026-09-04 | `VSTM` | 153 | $7.98 | $7.91 | -10.71 | — | +0.00 | -10.71 | -18.36 | — |
| 2026-09-04 | `DFDV` | 207 | — | $5.79 | +0.00 | $5.87 | +16.56 | +16.56 | +0.00 | +16.56 |
| 2026-09-04 | `FRNM` | 73 | — | $16.40 | +0.00 | $16.31 | -6.57 | -6.57 | +0.00 | -6.57 |
| 2026-09-04 | `TARS` | 14 | — | $82.70 | +0.00 | $90.78 | +113.12 | +113.12 | +0.00 | +113.12 |
| 2026-09-04 | `BRR` | 479 | — | $2.51 | +0.00 | $2.66 | +71.85 | +71.85 | +0.00 | +71.85 |
| 2026-09-04 | `IRD` | 265 | — | $4.53 | +0.00 | $4.67 | +37.10 | +37.10 | +0.00 | +37.10 |
| 2026-09-04 | `LENZ` | 209 | — | $5.75 | +0.00 | $5.96 | +43.89 | +43.89 | +0.00 | +43.89 |
| 2026-09-04 | `ASST` | 47 | — | $25.18 | +0.00 | $27.14 | +92.12 | +92.12 | +0.00 | +92.12 |
| 2026-09-08 | `GPRO` | 692 | $1.70 | $1.56 | -93.42 | — | +0.00 | -93.42 | -148.78 | — |
| 2026-09-08 | `DFDV` | 207 | $5.87 | $5.81 | -12.42 | — | +0.00 | -12.42 | +4.14 | — |
| 2026-09-08 | `FRNM` | 73 | $16.31 | $16.74 | +31.39 | — | +0.00 | +31.39 | +24.82 | — |
| 2026-09-08 | `TARS` | 14 | $90.78 | $89.67 | -15.54 | — | +0.00 | -15.54 | +97.58 | — |
| 2026-09-08 | `BRR` | 479 | $2.66 | $2.66 | +0.00 | — | +0.00 | +0.00 | +71.85 | — |
| 2026-09-08 | `IRD` | 265 | $4.67 | $4.53 | -37.10 | — | +0.00 | -37.10 | +0.00 | — |
| 2026-09-08 | `LENZ` | 209 | $5.96 | $5.95 | -2.09 | — | +0.00 | -2.09 | +41.80 | — |
| 2026-09-08 | `ASST` | 47 | $27.14 | $26.44 | -32.90 | — | +0.00 | -32.90 | +59.22 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +300.75 | IREN, TNDM, INO, TPG, HIMS, SLS, VOR, BTSG | — | $107.38 | $10,268.71 | IREN×27, TNDM×53, INO×1543, TPG×24, HIMS×42, SLS×106, VOR×56, BTSG×20 |
| 2026-08-14 | +5.50 | $107.38 | IREN×27, TNDM×53, INO×1543, TPG×24, HIMS×42, SLS×106, VOR×56, BTSG×20 | $10,312.70 | +43.99 | -665.78 | QMCO, ZENA, ARX, AIRO, BZAI, BRUN, AMPY, LIFE | IREN, TNDM, INO, TPG, HIMS, SLS, VOR, BTSG | $31.75 | $9,572.96 | QMCO×52, ZENA×583, ARX×65, AIRO×115, BZAI×1677, BRUN×48, AMPY×260, LIFE×36 |
| 2026-08-17 | +2.25 | $31.75 | QMCO×52, ZENA×583, ARX×65, AIRO×115, BZAI×1677, BRUN×48, AMPY×260, LIFE×36 | $9,429.45 | -143.51 | -266.11 | XHG, CAPR, STDN, HTFL, UMAC, ALOY, LPTH, NPWR | QMCO, ZENA, ARX, AIRO, BZAI, BRUN, AMPY, LIFE | $16.34 | $9,101.85 | XHG×280, CAPR×170, STDN×86, HTFL×28, UMAC×36, ALOY×80, LPTH×78, NPWR×611 |
| 2026-08-18 | -6.20 | $16.34 | XHG×280, CAPR×170, STDN×86, HTFL×28, UMAC×36, ALOY×80, LPTH×78, NPWR×611 | $8,917.12 | -184.73 | -71.40 | — | XHG, STDN, HTFL, UMAC, ALOY, LPTH, NPWR | $7,619.48 | $8,823.08 | CAPR×170 |
| 2026-08-19 | -7.20 | $7,619.48 | CAPR×170 | $8,841.78 | +18.70 | +0.00 | — | CAPR | $8,839.24 | $8,839.24 | — |
| 2026-08-20 | +1.12 | $8,839.24 | — | $8,839.24 | -0.00 | +52.88 | MRNA, CYPH, ABCL, SENS, AUTL, TEM, WPM, IAG | — | $183.36 | $8,861.11 | MRNA×7, CYPH×960, ABCL×93, SENS×124, AUTL×447, TEM×17, WPM×7, IAG×56 |
| 2026-08-21 | +3.25 | $183.36 | MRNA×7, CYPH×960, ABCL×93, SENS×124, AUTL×447, TEM×17, WPM×7, IAG×56 | $9,091.81 | +230.70 | +275.27 | XHG, ARCT, IOVA, MRVI, AU, CAPR | ABCL, SENS, AUTL, TEM, WPM, IAG | $75.28 | $9,335.40 | MRNA×7, CYPH×960, XHG×255, ARCT×102, IOVA×126, MRVI×138, AU×9, CAPR×168 |
| 2026-08-24 | -5.17 | $75.28 | MRNA×7, CYPH×960, XHG×255, ARCT×102, IOVA×126, MRVI×138, AU×9, CAPR×168 | $9,929.37 | +593.97 | +0.00 | — | MRNA, CYPH, XHG, ARCT, IOVA, MRVI, AU, CAPR | $9,899.71 | $9,899.71 | — |
| 2026-08-25 | +1.80 | $9,899.71 | — | $9,899.71 | +0.00 | +567.68 | REAX, CYPH, XHG, ALVO, ASST, GORO, EZPW, BMEA | — | $3.33 | $10,429.50 | REAX×51, CYPH×793, XHG×304, ALVO×236, ASST×64, GORO×348, EZPW×35, BMEA×759 |
| 2026-08-26 | +2.02 | $3.33 | REAX×51, CYPH×793, XHG×304, ALVO×236, ASST×64, GORO×348, EZPW×35, BMEA×759 | $10,182.34 | -247.16 | +107.66 | BYND, USDE, FIGR, FUTU, TIGR, FNV, BTG | REAX, CYPH, ALVO, ASST, GORO, EZPW, BMEA | $270.19 | $10,238.30 | XHG×304, BYND×91, USDE×221, FIGR×31, FUTU×10, TIGR×246, FNV×4, BTG×223 |
| 2026-08-27 | — | $270.19 | XHG×304, BYND×91, USDE×221, FIGR×31, FUTU×10, TIGR×246, FNV×4, BTG×223 | $10,370.40 | +132.10 | -29.19 | SLI, PGY, GEN, MRVL, ANET, MOS, MU, GGB | XHG, BYND, USDE, FIGR, FUTU, TIGR, FNV, BTG | $432.53 | $10,297.23 | SLI×497, PGY×56, GEN×43, MRVL×5, ANET×6, MOS×53, MU×1, GGB×283 |
| 2026-08-28 | +0.75 | $432.53 | SLI×497, PGY×56, GEN×43, MRVL×5, ANET×6, MOS×53, MU×1, GGB×283 | $10,212.30 | -84.93 | -170.06 | BYND, CAPR, ANF, NCNO, MEI, VYX, SMTC, SNPS | SLI, PGY, GEN, MRVL, ANET, MOS, MU, GGB | $631.22 | $10,002.05 | BYND×90, CAPR×130, ANF×8, NCNO×54, MEI×71, VYX×139, SMTC×8, SNPS×2 |
| 2026-08-31 | -5.85 | $631.22 | BYND×90, CAPR×130, ANF×8, NCNO×54, MEI×71, VYX×139, SMTC×8, SNPS×2 | $9,943.69 | -58.36 | -45.90 | — | CAPR, ANF, NCNO, MEI, VYX, SMTC, SNPS | $8,685.46 | $9,882.46 | BYND×90 |
| 2026-09-01 | -6.30 | $8,685.46 | BYND×90 | $9,859.06 | -23.40 | +0.00 | — | BYND | $9,856.78 | $9,856.78 | — |
| 2026-09-02 | -3.83 | $9,856.78 | — | $9,856.78 | -0.00 | +0.00 | — | — | $9,856.78 | $9,856.78 | — |
| 2026-09-03 | -0.90 | $9,856.78 | — | $9,856.78 | -0.00 | -408.91 | GPRO, REAX, BMEA, MMED, AGCO, ARCT, NVAX, VSTM | — | $97.34 | $9,417.36 | GPRO×692, REAX×66, BMEA×638, MMED×51, AGCO×9, ARCT×73, NVAX×118, VSTM×153 |
| 2026-09-04 | +2.25 | $97.34 | GPRO×692, REAX×66, BMEA×638, MMED×51, AGCO×9, ARCT×73, NVAX×118, VSTM×153 | $9,463.18 | +45.82 | +520.31 | DFDV, FRNM, TARS, BRR, IRD, LENZ, ASST | REAX, BMEA, MMED, AGCO, ARCT, NVAX, VSTM | $54.36 | $9,940.31 | GPRO×692, DFDV×207, FRNM×73, TARS×14, BRR×479, IRD×265, LENZ×209, ASST×47 |
| 2026-09-08 | -11.47 | $54.36 | GPRO×692, DFDV×207, FRNM×73, TARS×14, BRR×479, IRD×265, LENZ×209, ASST×47 | $9,778.23 | -162.08 | +0.00 | — | GPRO, DFDV, FRNM, TARS, BRR, IRD, LENZ, ASST | $9,747.55 | $9,747.55 | — |
| 2026-09-09 | -13.95 | $9,747.55 | — | $9,747.55 | -0.00 | +0.00 | — | — | $9,747.55 | $9,747.55 | — |

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
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 115 | $11.12 | $2.33 | — | $5,147.40 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BZAI` | 1677 | $0.77 | $17.88 | — | $3,844.94 | — | rank by w_hot_cond; rank w_hot_cond; list earn_react; 🔵; ⚪; ret5=+20.4; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BRUN` | 48 | $26.25 | $2.13 | — | $2,583.04 | — | rank by w_hot_cond; rank w_hot_cond; list earn_react; 🔵; ⚪; ret5=+31.2; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AMPY` | 260 | $4.94 | $3.35 | — | $1,295.29 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.4; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LIFE` | 36 | $35.04 | $2.10 | — | $31.75 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+16.7; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $31.75 | ▼ close $9,572.96 vs 09:30 $10,312.70 (session -665.78) | 16:00 close · cash $31.75 · equity $9,572.96 vs 09:30 $10,312.70 (-739.74; session marks -665.78) · 8 name(s) marked open→close (per-name table). QMCO×52 09:30 $24.68 → close $26.11 +74.36; ZENA×583 09:30 $2.20 → close $2.14 -34.98; ARX×65 09:30 $19.57 → close $19.58 +0.65; AIRO×115 09:30 $11.12 → close $9.57 -178.25; BZAI×1677 09:30 $0.77 → close $0.59 -290.12; BRUN×48 09:30 $26.25 → close $22.93 -159.12; AMPY×260 09:30 $4.94 → close $4.78 -41.60; LIFE×36 09:30 $35.04 → close $34.02 -36.72 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $31.75 | ▼ 09:30 equity $9,429.45 vs yday $9,572.96 (-143.51) | 09:30 open · cash $31.75 (unchanged overnight, no fees) · equity $9,429.45 vs prior close $9,572.96 (-143.51) · 8 name(s) re-marked at the open (per-name table). QMCO×52 yday $26.11 → 09:30 $24.83 -66.56; ZENA×583 yday $2.14 → 09:30 $2.08 -32.07; ARX×65 yday $19.58 → 09:30 $19.57 -0.65; AIRO×115 yday $9.57 → 09:30 $9.57 +0.00; BZAI×1677 yday $0.59 → 09:30 $0.55 -68.76; BRUN×48 yday $22.93 → 09:30 $23.00 +3.36; AMPY×260 yday $4.78 → 09:30 $4.86 +20.80; LIFE×36 yday $34.02 → 09:30 $34.03 +0.36 | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 52 | $24.83 | $2.17 | $+3.49 | $1,320.74 | ▲ +3.49 after sell → book $9,427.28; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 583 | $2.08 | $7.63 | $-82.19 | $2,528.67 | ▼ -82.19 after sell → book $9,419.66; vs 09:30 mark -7.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 65 | $19.57 | $2.21 | $-4.39 | $3,798.52 | ▼ -4.39 after sell → book $9,417.45; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 115 | $9.57 | $2.36 | $-182.95 | $4,896.70 | ▼ -182.95 after sell → book $9,415.09; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BZAI` | 1677 | $0.55 | $14.58 | $-391.33 | $5,807.83 | ▼ -391.33 after sell → book $9,400.51; vs 09:30 mark -14.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BRUN` | 48 | $23.00 | $2.15 | $-160.05 | $6,909.68 | ▼ -160.05 after sell → book $9,398.36; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AMPY` | 260 | $4.86 | $3.41 | $-27.56 | $8,169.87 | ▼ -27.56 after sell → book $9,394.95; vs 09:30 mark -3.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LIFE` | 36 | $34.03 | $2.12 | $-40.58 | $9,392.83 | ▼ -40.58 after sell → book $9,392.83; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 280 | $4.19 | $3.61 | — | $8,216.02 | — | rank by w_hot_cond; rank w_hot_cond; list yday_mover; ⚪; ret5=+291.8; leftover $1174.10 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 170 | $6.87 | $2.50 | — | $7,045.62 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+62.6; leftover $1174.10 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 86 | $13.64 | $2.25 | — | $5,870.33 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+57.7; leftover $1174.10 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 28 | $41.23 | $2.07 | — | $4,713.82 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+46.0; leftover $1174.10 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 36 | $32.55 | $2.10 | — | $3,539.92 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1174.10 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 80 | $14.66 | $2.23 | — | $2,364.89 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $1174.10 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `LPTH` | 78 | $14.94 | $2.22 | — | $1,197.34 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $1174.10 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 611 | $1.92 | $7.88 | — | $16.34 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1174.10 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.34 | ▼ close $9,101.85 vs 09:30 $9,429.45 (session -266.11) | 16:00 close · cash $16.34 · equity $9,101.85 vs 09:30 $9,429.45 (-327.60; session marks -266.11) · 8 name(s) marked open→close (per-name table). XHG×280 09:30 $4.19 → close $3.91 -78.40; CAPR×170 09:30 $6.87 → close $7.45 +98.60; STDN×86 09:30 $13.64 → close $13.31 -28.38; HTFL×28 09:30 $41.23 → close $41.94 +19.88; UMAC×36 09:30 $32.55 → close $30.15 -86.40; ALOY×80 09:30 $14.66 → close $13.86 -64.40; LPTH×78 09:30 $14.94 → close $14.80 -10.92; NPWR×611 09:30 $1.92 → close $1.73 -116.09 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.34 | ▼ 09:30 equity $8,917.12 vs yday $9,101.85 (-184.73) | 09:30 open · cash $16.34 (unchanged overnight, no fees) · equity $8,917.12 vs prior close $9,101.85 (-184.73) · 8 name(s) re-marked at the open (per-name table). XHG×280 yday $3.91 → 09:30 $3.94 +8.40; CAPR×170 yday $7.45 → 09:30 $7.50 +8.50; STDN×86 yday $13.31 → 09:30 $13.31 +0.00; HTFL×28 yday $41.94 → 09:30 $41.50 -12.32; UMAC×36 yday $30.15 → 09:30 $28.59 -56.16; ALOY×80 yday $13.86 → 09:30 $13.19 -53.20; LPTH×78 yday $14.80 → 09:30 $14.01 -61.62; NPWR×611 yday $1.73 → 09:30 $1.70 -18.33 | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 280 | $3.94 | $3.67 | $-77.28 | $1,115.87 | ▼ -77.28 after sell → book $8,913.45; vs 09:30 mark -3.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 86 | $13.31 | $2.27 | $-32.90 | $2,258.26 | ▼ -32.90 after sell → book $8,911.18; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 judge🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 28 | $41.50 | $2.09 | $+3.39 | $3,418.17 | ▲ +3.39 after sell → book $8,909.09; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 36 | $28.59 | $2.12 | $-146.78 | $4,445.29 | ▼ -146.78 after sell → book $8,906.97; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 80 | $13.19 | $2.25 | $-122.08 | $5,498.24 | ▼ -122.08 after sell → book $8,904.72; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `LPTH` | 78 | $14.01 | $2.25 | $-77.01 | $6,588.77 | ▼ -77.01 after sell → book $8,902.47; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 611 | $1.70 | $7.99 | $-150.30 | $7,619.48 | ▼ -150.30 after sell → book $8,894.48; vs 09:30 mark -7.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,619.48 | ▼ close $8,823.08 vs 09:30 $8,917.12 (session -71.40) | 16:00 close · cash $7,619.48 · equity $8,823.08 vs 09:30 $8,917.12 (-94.04; session marks -71.40) · 1 name(s) marked open→close (per-name table). CAPR×170 09:30 $7.50 → close $7.08 -71.40 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,619.48 | ▲ 09:30 equity $8,841.78 vs yday $8,823.08 (+18.70) | 09:30 open · cash $7,619.48 (unchanged overnight, no fees) · equity $8,841.78 vs prior close $8,823.08 (+18.70) · 1 name(s) re-marked at the open (per-name table). CAPR×170 yday $7.08 → 09:30 $7.19 +18.70 | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 170 | $7.19 | $2.54 | $+49.36 | $8,839.24 | ▲ +49.36 after sell → book $8,839.24; vs 09:30 mark -2.54 | dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,839.24 | ▲ close $8,839.24 vs 09:30 $8,841.78 (session +0.00) | 16:00 close · cash $8,839.24 · no lots left · equity $8,839.24. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,839.24 | ▲ 09:30 equity $8,839.24 vs yday $8,839.24 (-0.00) | 09:30 open · cash $8,839.24 · no holdings · equity $8,839.24 vs prior close $8,839.24 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 7 | $150.14 | $2.01 | — | $7,786.25 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1104.90 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 960 | $1.15 | $12.38 | — | $6,669.86 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1104.90 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 93 | $11.81 | $2.27 | — | $5,568.80 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1104.90 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SENS` | 124 | $8.91 | $2.36 | — | $4,461.60 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1104.90 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 447 | $2.47 | $5.77 | — | $3,351.74 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1104.90 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TEM` | 17 | $61.83 | $2.04 | — | $2,298.59 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $1104.90 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 7 | $144.54 | $2.01 | — | $1,284.80 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1104.90 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 56 | $19.63 | $2.16 | — | $183.36 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1104.90 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $183.36 | ▲ close $8,861.11 vs 09:30 $8,839.24 (session +52.88) | 16:00 close · cash $183.36 · equity $8,861.11 vs 09:30 $8,839.24 (+21.87; session marks +52.88) · 8 name(s) marked open→close (per-name table). MRNA×7 09:30 $150.14 → close $133.32 -117.74; CYPH×960 09:30 $1.15 → close $1.19 +38.40; ABCL×93 09:30 $11.81 → close $11.57 -22.78; SENS×124 09:30 $8.91 → close $8.82 -11.16; AUTL×447 09:30 $2.47 → close $2.46 -4.47; TEM×17 09:30 $61.83 → close $66.65 +81.94; WPM×7 09:30 $144.54 → close $150.25 +39.97; IAG×56 09:30 $19.63 → close $20.50 +48.72 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $183.36 | ▲ 09:30 equity $9,091.81 vs yday $8,861.11 (+230.70) | 09:30 open · cash $183.36 (unchanged overnight, no fees) · equity $9,091.81 vs prior close $8,861.11 (+230.70) · 8 name(s) re-marked at the open (per-name table). MRNA×7 yday $133.32 → 09:30 $133.11 -1.47; CYPH×960 yday $1.19 → 09:30 $1.32 +124.80; ABCL×93 yday $11.57 → 09:30 $11.57 +0.00; SENS×124 yday $8.82 → 09:30 $9.24 +52.08; AUTL×447 yday $2.46 → 09:30 $2.47 +4.47; TEM×17 yday $66.65 → 09:30 $65.60 -17.85; WPM×7 yday $150.25 → 09:30 $154.70 +31.15; IAG×56 yday $20.50 → 09:30 $21.17 +37.52 | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 93 | $11.57 | $2.29 | $-27.35 | $1,257.08 | ▼ -27.35 after sell → book $9,089.52; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SENS` | 124 | $9.24 | $2.39 | $+36.17 | $2,400.44 | ▲ +36.17 after sell → book $9,087.12; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `AUTL` | 447 | $2.47 | $5.85 | $-11.62 | $3,498.68 | ▼ -11.62 after sell → book $9,081.27; vs 09:30 mark -5.85 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `TEM` | 17 | $65.60 | $2.06 | $+59.99 | $4,611.82 | ▲ +59.99 after sell → book $9,079.21; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 7 | $154.70 | $2.03 | $+67.08 | $5,692.69 | ▲ +67.08 after sell → book $9,077.18; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 56 | $21.17 | $2.18 | $+81.90 | $6,876.03 | ▲ +81.90 after sell → book $9,075.00; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 255 | $4.49 | $3.29 | — | $5,727.79 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+12.7; leftover $1146.01 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 102 | $11.13 | $2.30 | — | $4,590.24 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1146.01 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 126 | $9.08 | $2.37 | — | $3,443.79 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $1146.01 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 138 | $8.28 | $2.40 | — | $2,298.75 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; leftover $1146.01 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 9 | $119.43 | $2.02 | — | $1,221.86 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1146.01 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 168 | $6.81 | $2.49 | — | $75.28 | — | rank by w_hot_cond; rank w_hot_cond; list yday_mover; 🔵; ret5=+62.5; leftover $1146.01 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $75.28 | ▲ close $9,335.40 vs 09:30 $9,091.81 (session +275.27) | 16:00 close · cash $75.28 · equity $9,335.40 vs 09:30 $9,091.81 (+243.59; session marks +275.27) · 8 name(s) marked open→close (per-name table). MRNA×7 09:30 $133.11 → close $145.13 +84.14; CYPH×960 09:30 $1.32 → close $1.42 +96.00; XHG×255 09:30 $4.49 → close $4.41 -20.40; ARCT×102 09:30 $11.13 → close $13.45 +236.64; IOVA×126 09:30 $9.08 → close $8.29 -99.54; MRVI×138 09:30 $8.28 → close $8.64 +49.68; AU×9 09:30 $119.43 → close $121.22 +16.11; CAPR×168 09:30 $6.81 → close $6.29 -87.36 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $75.28 | ▲ 09:30 equity $9,929.37 vs yday $9,335.40 (+593.97) | 09:30 open · cash $75.28 (unchanged overnight, no fees) · equity $9,929.37 vs prior close $9,335.40 (+593.97) · 8 name(s) re-marked at the open (per-name table). MRNA×7 yday $145.13 → 09:30 $142.70 -17.01; CYPH×960 yday $1.42 → 09:30 $1.83 +393.60; XHG×255 yday $4.41 → 09:30 $4.32 -22.95; ARCT×102 yday $13.45 → 09:30 $13.33 -12.24; IOVA×126 yday $8.29 → 09:30 $8.08 -26.46; MRVI×138 yday $8.64 → 09:30 $8.59 -6.90; AU×9 yday $121.22 → 09:30 $120.51 -6.39; CAPR×168 yday $6.29 → 09:30 $8.03 +292.32 | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 7 | $142.70 | $2.03 | $-56.12 | $1,072.15 | ▼ -56.12 after sell → book $9,927.34; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 960 | $1.83 | $12.56 | $+627.86 | $2,816.40 | ▲ +627.86 after sell → book $9,914.79; vs 09:30 mark -12.55 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 255 | $4.32 | $3.34 | $-49.98 | $3,914.65 | ▼ -49.98 after sell → book $9,911.44; vs 09:30 mark -3.35 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 102 | $13.33 | $2.32 | $+219.78 | $5,271.99 | ▲ +219.78 after sell → book $9,909.12; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `IOVA` | 126 | $8.08 | $2.40 | $-130.77 | $6,287.67 | ▼ -130.77 after sell → book $9,906.72; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 138 | $8.59 | $2.44 | $+37.94 | $7,470.65 | ▲ +37.94 after sell → book $9,904.28; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 9 | $120.51 | $2.04 | $+5.67 | $8,553.21 | ▲ +5.67 after sell → book $9,902.25; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 168 | $8.03 | $2.53 | $+199.93 | $9,899.71 | ▲ +199.93 after sell → book $9,899.71; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,899.71 | ▲ close $9,899.71 vs 09:30 $9,929.37 (session +0.00) | 16:00 close · cash $9,899.71 · no lots left · equity $9,899.71. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,899.71 | ▲ 09:30 equity $9,899.71 vs yday $9,899.71 (+0.00) | 09:30 open · cash $9,899.71 · no holdings · equity $9,899.71 vs prior close $9,899.71 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 51 | $24.11 | $2.14 | — | $8,667.96 | — | rank by w_hot_cond; rank w_hot_cond; list yday_mover; ret5=+891.7; leftover $1237.46 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 793 | $1.56 | $10.23 | — | $7,420.65 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $1237.46 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 304 | $4.07 | $3.92 | — | $6,179.45 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $1237.46 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 236 | $5.24 | $3.04 | — | $4,939.77 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1237.46 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 64 | $19.04 | $2.18 | — | $3,719.02 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ret5=+49.5; leftover $1237.46 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 348 | $3.55 | $4.49 | — | $2,479.13 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+27.9; leftover $1237.46 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 35 | $35.05 | $2.10 | — | $1,250.29 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1237.46 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 759 | $1.63 | $9.79 | — | $3.33 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1237.46 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.33 | ▲ close $10,429.50 vs 09:30 $9,899.71 (session +567.68) | 16:00 close · cash $3.33 · equity $10,429.50 vs 09:30 $9,899.71 (+529.79; session marks +567.68) · 8 name(s) marked open→close (per-name table). REAX×51 09:30 $24.11 → close $28.43 +220.32; CYPH×793 09:30 $1.56 → close $1.64 +63.44; XHG×304 09:30 $4.07 → close $4.02 -15.20; ALVO×236 09:30 $5.24 → close $5.05 -44.84; ASST×64 09:30 $19.04 → close $21.39 +150.40; GORO×348 09:30 $3.55 → close $3.87 +111.36; EZPW×35 09:30 $35.05 → close $35.23 +6.30; BMEA×759 09:30 $1.63 → close $1.73 +75.90 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.33 | ▼ 09:30 equity $10,182.34 vs yday $10,429.50 (-247.16) | 09:30 open · cash $3.33 (unchanged overnight, no fees) · equity $10,182.34 vs prior close $10,429.50 (-247.16) · 8 name(s) re-marked at the open (per-name table). REAX×51 yday $28.43 → 09:30 $26.61 -92.82; CYPH×793 yday $1.64 → 09:30 $1.60 -31.72; XHG×304 yday $4.02 → 09:30 $3.81 -63.84; ALVO×236 yday $5.05 → 09:30 $4.98 -16.52; ASST×64 yday $21.39 → 09:30 $20.72 -42.88; GORO×348 yday $3.87 → 09:30 $3.77 -34.80; EZPW×35 yday $35.23 → 09:30 $35.70 +16.45; BMEA×759 yday $1.73 → 09:30 $1.75 +18.97 | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 51 | $26.61 | $2.16 | $+123.19 | $1,358.27 | ▲ +123.19 after sell → book $10,180.18; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 793 | $1.60 | $10.37 | $+11.12 | $2,616.70 | ▲ +11.12 after sell → book $10,169.81; vs 09:30 mark -10.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ALVO` | 236 | $4.98 | $3.09 | $-67.50 | $3,788.89 | ▼ -67.50 after sell → book $10,166.71; vs 09:30 mark -3.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 64 | $20.72 | $2.20 | $+103.13 | $5,112.77 | ▲ +103.13 after sell → book $10,164.51; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GORO` | 348 | $3.77 | $4.56 | $+67.51 | $6,420.17 | ▲ +67.51 after sell → book $10,159.95; vs 09:30 mark -4.56 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 35 | $35.70 | $2.12 | $+18.54 | $7,667.55 | ▲ +18.54 after sell → book $10,157.84; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 759 | $1.75 | $9.93 | $+75.16 | $8,989.67 | ▲ +75.16 after sell → book $10,147.91; vs 09:30 mark -9.93 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 91 | $14.11 | $2.26 | — | $7,703.40 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+11.4; leftover $1284.24 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 221 | $5.81 | $2.85 | — | $6,416.54 | — | rank by w_hot_cond; rank w_hot_cond; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $1284.24 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FIGR` | 31 | $40.50 | $2.08 | — | $5,158.95 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+15.8; leftover $1284.24 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **BUY** | `FUTU` | 10 | $124.67 | $2.02 | — | $3,910.23 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+15.7; leftover $1284.24 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 246 | $5.21 | $3.17 | — | $2,625.40 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot,earn_react; 🔵; ret5=+14.3; leftover $1284.24 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 4 | $267.02 | $2.00 | — | $1,555.32 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $1284.24 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BTG` | 223 | $5.75 | $2.88 | — | $270.19 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+17.9; leftover $1284.24 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $270.19 | ▲ close $10,238.30 vs 09:30 $10,182.34 (session +107.66) | 16:00 close · cash $270.19 · equity $10,238.30 vs 09:30 $10,182.34 (+55.96; session marks +107.66) · 8 name(s) marked open→close (per-name table). XHG×304 09:30 $3.81 → close $4.06 +76.00; BYND×91 09:30 $14.11 → close $14.25 +12.74; USDE×221 09:30 $5.81 → close $5.98 +37.57; FIGR×31 09:30 $40.50 → close $37.08 -106.02; FUTU×10 09:30 $124.67 → close $127.34 +26.70; TIGR×246 09:30 $5.21 → close $5.46 +61.50; FNV×4 09:30 $267.02 → close $267.37 +1.40; BTG×223 09:30 $5.75 → close $5.74 -2.23 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $270.19 | ▲ 09:30 equity $10,370.40 vs yday $10,238.30 (+132.10) | 09:30 open · cash $270.19 (unchanged overnight, no fees) · equity $10,370.40 vs prior close $10,238.30 (+132.10) · 8 name(s) re-marked at the open (per-name table). XHG×304 yday $4.06 → 09:30 $4.06 +0.00; BYND×91 yday $14.25 → 09:30 $14.20 -4.55; USDE×221 yday $5.98 → 09:30 $6.50 +114.92; FIGR×31 yday $37.08 → 09:30 $37.42 +10.54; FUTU×10 yday $127.34 → 09:30 $128.00 +6.60; TIGR×246 yday $5.46 → 09:30 $5.49 +7.38; FNV×4 yday $267.37 → 09:30 $267.23 -0.56; BTG×223 yday $5.74 → 09:30 $5.73 -2.23 | — |
| 2026-08-27 09:30 ET | **SELL** | `XHG` | 304 | $4.06 | $3.98 | $-10.94 | $1,500.45 | ▼ -10.94 after sell → book $10,366.42; vs 09:30 mark -3.98 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 91 | $14.20 | $2.29 | $+3.64 | $2,790.36 | ▲ +3.64 after sell → book $10,364.13; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 221 | $6.50 | $2.90 | $+146.74 | $4,223.96 | ▲ +146.74 after sell → book $10,361.23; vs 09:30 mark -2.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FIGR` | 31 | $37.42 | $2.10 | $-99.67 | $5,381.88 | ▼ -99.67 after sell → book $10,359.13; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FUTU` | 10 | $128.00 | $2.04 | $+29.24 | $6,659.84 | ▲ +29.24 after sell → book $10,357.09; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TIGR` | 246 | $5.49 | $3.23 | $+62.48 | $8,007.15 | ▲ +62.48 after sell → book $10,353.86; vs 09:30 mark -3.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 4 | $267.23 | $2.02 | $-3.18 | $9,074.05 | ▼ -3.18 after sell → book $10,351.84; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BTG` | 223 | $5.73 | $2.92 | $-10.26 | $10,348.92 | ▼ -10.26 after sell → book $10,348.92; vs 09:30 mark -2.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 497 | $2.60 | $6.41 | — | $9,050.31 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ret5=+13.0; leftover $1293.61 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PGY` | 56 | $22.93 | $2.16 | — | $7,764.07 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=+9.5; leftover $1293.61 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 43 | $29.83 | $2.12 | — | $6,479.26 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=+7.6; leftover $1293.61 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 5 | $253.44 | $2.00 | — | $5,210.06 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=+3.3; leftover $1293.61 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ANET` | 6 | $205.90 | $2.01 | — | $3,972.65 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=+8.5; leftover $1293.61 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 53 | $24.00 | $2.15 | — | $2,698.50 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ret5=+8.7; leftover $1293.61 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $1,729.50 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=+0.1; leftover $1293.61 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 283 | $4.57 | $3.65 | — | $432.53 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=+1.1; leftover $1293.61 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $432.53 | ▼ close $10,297.23 vs 09:30 $10,370.40 (session -29.19) | 16:00 close · cash $432.53 · equity $10,297.23 vs 09:30 $10,370.40 (-73.17; session marks -29.19) · 8 name(s) marked open→close (per-name table). SLI×497 09:30 $2.60 → close $2.64 +19.88; PGY×56 09:30 $22.93 → close $23.26 +18.48; GEN×43 09:30 $29.83 → close $30.50 +28.81; MRVL×5 09:30 $253.44 → close $241.45 -59.95; ANET×6 09:30 $205.90 → close $201.09 -28.86; MOS×53 09:30 $24.00 → close $23.76 -12.72; MU×1 09:30 $967.01 → close $935.39 -31.62; GGB×283 09:30 $4.57 → close $4.70 +36.79 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $432.53 | ▼ 09:30 equity $10,212.30 vs yday $10,297.23 (-84.93) | 09:30 open · cash $432.53 (unchanged overnight, no fees) · equity $10,212.30 vs prior close $10,297.23 (-84.93) · 8 name(s) re-marked at the open (per-name table). SLI×497 yday $2.64 → 09:30 $2.68 +19.88; PGY×56 yday $23.26 → 09:30 $23.21 -2.80; GEN×43 yday $30.50 → 09:30 $30.50 +0.00; MRVL×5 yday $241.45 → 09:30 $225.26 -80.95; ANET×6 yday $201.09 → 09:30 $200.00 -6.54; MOS×53 yday $23.76 → 09:30 $23.95 +10.07; MU×1 yday $935.39 → 09:30 $919.29 -16.10; GGB×283 yday $4.70 → 09:30 $4.67 -8.49 | — |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 497 | $2.68 | $6.50 | $+26.84 | $1,757.99 | ▲ +26.84 after sell → book $10,205.80; vs 09:30 mark -6.50 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `PGY` | 56 | $23.21 | $2.18 | $+11.34 | $3,055.57 | ▲ +11.34 after sell → book $10,203.62; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 43 | $30.50 | $2.14 | $+24.55 | $4,364.93 | ▲ +24.55 after sell → book $10,201.48; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MRVL` | 5 | $225.26 | $2.02 | $-144.93 | $5,489.21 | ▼ -144.93 after sell → book $10,199.46; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `ANET` | 6 | $200.00 | $2.03 | $-39.44 | $6,687.18 | ▼ -39.44 after sell → book $10,197.43; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 53 | $23.95 | $2.17 | $-6.97 | $7,954.36 | ▼ -6.97 after sell → book $10,195.26; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $8,871.64 | ▼ -51.73 after sell → book $10,193.25; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 283 | $4.67 | $3.71 | $+20.94 | $10,189.54 | ▲ +20.94 after sell → book $10,189.54; vs 09:30 mark -3.71 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 90 | $14.00 | $2.26 | — | $8,927.28 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=-3.3; leftover $1273.69 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 130 | $9.73 | $2.38 | — | $7,660.00 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+47.1; leftover $1273.69 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $146.07 | $2.01 | — | $6,489.42 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1273.69 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 54 | $23.30 | $2.15 | — | $5,229.07 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+14.5; leftover $1273.69 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MEI` | 71 | $17.78 | $2.20 | — | $3,964.49 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ⚪; ret5=+22.9; leftover $1273.69 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VYX` | 139 | $9.13 | $2.41 | — | $2,693.01 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ret5=+20.0; leftover $1273.69 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $141.76 | $2.01 | — | $1,556.92 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1273.69 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SNPS` | 2 | $461.85 | $2.00 | — | $631.22 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+16.8; leftover $1273.69 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $631.22 | ▼ close $10,002.05 vs 09:30 $10,212.30 (session -170.06) | 16:00 close · cash $631.22 · equity $10,002.05 vs 09:30 $10,212.30 (-210.25; session marks -170.06) · 8 name(s) marked open→close (per-name table). BYND×90 09:30 $14.00 → close $13.86 -12.60; CAPR×130 09:30 $9.73 → close $9.59 -18.20; ANF×8 09:30 $146.07 → close $148.42 +18.80; NCNO×54 09:30 $23.30 → close $22.99 -16.74; MEI×71 09:30 $17.78 → close $18.21 +30.53; VYX×139 09:30 $9.13 → close $8.78 -48.65; SMTC×8 09:30 $141.76 → close $131.17 -84.72; SNPS×2 09:30 $461.85 → close $442.61 -38.48 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $631.22 | ▼ 09:30 equity $9,943.69 vs yday $10,002.05 (-58.36) | 09:30 open · cash $631.22 (unchanged overnight, no fees) · equity $9,943.69 vs prior close $10,002.05 (-58.36) · 8 name(s) re-marked at the open (per-name table). BYND×90 yday $13.86 → 09:30 $13.81 -4.50; CAPR×130 yday $9.59 → 09:30 $9.50 -11.70; ANF×8 yday $148.42 → 09:30 $148.03 -3.12; NCNO×54 yday $22.99 → 09:30 $22.66 -17.82; MEI×71 yday $18.21 → 09:30 $18.15 -4.26; VYX×139 yday $8.78 → 09:30 $8.66 -16.68; SMTC×8 yday $131.17 → 09:30 $132.30 +9.04; SNPS×2 yday $442.61 → 09:30 $437.95 -9.32 | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 130 | $9.50 | $2.41 | $-34.69 | $1,863.81 | ▼ -34.69 after sell → book $9,941.28; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 8 | $148.03 | $2.03 | $+11.63 | $3,046.02 | ▲ +11.63 after sell → book $9,939.25; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NCNO` | 54 | $22.66 | $2.17 | $-38.88 | $4,267.49 | ▼ -38.88 after sell → book $9,937.08; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `MEI` | 71 | $18.15 | $2.23 | $+21.84 | $5,553.91 | ▲ +21.84 after sell → book $9,934.85; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `VYX` | 139 | $8.66 | $2.44 | $-70.18 | $6,755.21 | ▼ -70.18 after sell → book $9,932.41; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 8 | $132.30 | $2.03 | $-79.73 | $7,811.58 | ▼ -79.73 after sell → book $9,930.38; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SNPS` | 2 | $437.95 | $2.02 | $-51.81 | $8,685.46 | ▼ -51.81 after sell → book $9,928.36; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,685.46 | ▼ close $9,882.46 vs 09:30 $9,943.69 (session -45.90) | 16:00 close · cash $8,685.46 · equity $9,882.46 vs 09:30 $9,943.69 (-61.23; session marks -45.90) · 1 name(s) marked open→close (per-name table). BYND×90 09:30 $13.81 → close $13.30 -45.90 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,685.46 | ▼ 09:30 equity $9,859.06 vs yday $9,882.46 (-23.40) | 09:30 open · cash $8,685.46 (unchanged overnight, no fees) · equity $9,859.06 vs prior close $9,882.46 (-23.40) · 1 name(s) re-marked at the open (per-name table). BYND×90 yday $13.30 → 09:30 $13.04 -23.40 | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 90 | $13.04 | $2.28 | $-90.94 | $9,856.78 | ▼ -90.94 after sell → book $9,856.78; vs 09:30 mark -2.28 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,856.78 | ▲ close $9,856.78 vs 09:30 $9,859.06 (session +0.00) | 16:00 close · cash $9,856.78 · no lots left · equity $9,856.78. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,856.78 | ▲ 09:30 equity $9,856.78 vs yday $9,856.78 (-0.00) | 09:30 open · cash $9,856.78 · no holdings · equity $9,856.78 vs prior close $9,856.78 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,856.78 | ▲ close $9,856.78 vs 09:30 $9,856.78 (session +0.00) | 16:00 close · cash $9,856.78 · no lots left · equity $9,856.78. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,856.78 | ▲ 09:30 equity $9,856.78 vs yday $9,856.78 (-0.00) | 09:30 open · cash $9,856.78 · no holdings · equity $9,856.78 vs prior close $9,856.78 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 692 | $1.78 | $8.93 | — | $8,616.09 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+183.1; leftover $1232.10 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 66 | $18.40 | $2.19 | — | $7,399.50 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=-32.2; leftover $1232.10 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 638 | $1.93 | $8.23 | — | $6,159.93 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1232.10 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 51 | $23.88 | $2.14 | — | $4,939.91 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1232.10 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AGCO` | 9 | $127.91 | $2.02 | — | $3,786.70 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.6; leftover $1232.10 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 73 | $16.77 | $2.21 | — | $2,560.28 | — | rank by w_hot_cond; rank w_hot_cond; list probable,yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1232.10 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 118 | $10.42 | $2.34 | — | $1,328.38 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; leftover $1232.10 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 153 | $8.03 | $2.45 | — | $97.34 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1232.10 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.34 | ▼ close $9,417.36 vs 09:30 $9,856.78 (session -408.91) | 16:00 close · cash $97.34 · equity $9,417.36 vs 09:30 $9,856.78 (-439.42; session marks -408.91) · 8 name(s) marked open→close (per-name table). GPRO×692 09:30 $1.78 → close $1.39 -269.88; REAX×66 09:30 $18.40 → close $18.40 +0.00; BMEA×638 09:30 $1.93 → close $1.91 -12.76; MMED×51 09:30 $23.88 → close $23.84 -2.04; AGCO×9 09:30 $127.91 → close $125.82 -18.81; ARCT×73 09:30 $16.77 → close $15.56 -88.33; NVAX×118 09:30 $10.42 → close $10.34 -9.44; VSTM×153 09:30 $8.03 → close $7.98 -7.65 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.34 | ▲ 09:30 equity $9,463.18 vs yday $9,417.36 (+45.82) | 09:30 open · cash $97.34 (unchanged overnight, no fees) · equity $9,463.18 vs prior close $9,417.36 (+45.82) · 8 name(s) re-marked at the open (per-name table). GPRO×692 yday $1.39 → 09:30 $1.48 +62.28; REAX×66 yday $18.40 → 09:30 $18.15 -16.50; BMEA×638 yday $1.91 → 09:30 $1.90 -6.38; MMED×51 yday $23.84 → 09:30 $23.84 +0.00; AGCO×9 yday $125.82 → 09:30 $125.22 -5.40; ARCT×73 yday $15.56 → 09:30 $15.61 +3.65; NVAX×118 yday $10.34 → 09:30 $10.50 +18.88; VSTM×153 yday $7.98 → 09:30 $7.91 -10.71 | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 66 | $18.15 | $2.21 | $-20.90 | $1,293.03 | ▼ -20.90 after sell → book $9,460.97; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `BMEA` | 638 | $1.90 | $8.35 | $-35.72 | $2,496.88 | ▼ -35.72 after sell → book $9,452.62; vs 09:30 mark -8.35 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 51 | $23.84 | $2.16 | $-6.35 | $3,710.56 | ▼ -6.35 after sell → book $9,450.46; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AGCO` | 9 | $125.22 | $2.04 | $-28.26 | $4,835.50 | ▼ -28.26 after sell → book $9,448.42; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 73 | $15.61 | $2.23 | $-89.12 | $5,972.80 | ▼ -89.12 after sell → book $9,446.19; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 118 | $10.50 | $2.37 | $+4.72 | $7,209.43 | ▲ +4.72 after sell → book $9,443.82; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 153 | $7.91 | $2.48 | $-23.29 | $8,417.17 | ▼ -23.29 after sell → book $9,441.33; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 207 | $5.79 | $2.67 | — | $7,215.97 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $1202.45 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `FRNM` | 73 | $16.40 | $2.21 | — | $6,016.56 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ⚪; ret5=+21.2; leftover $1202.45 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 14 | $82.70 | $2.03 | — | $4,856.73 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+15.7; leftover $1202.45 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 479 | $2.51 | $6.18 | — | $3,648.26 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+21.8; leftover $1202.45 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 265 | $4.53 | $3.42 | — | $2,444.39 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.5; leftover $1202.45 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `LENZ` | 209 | $5.75 | $2.70 | — | $1,239.95 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+21.2; leftover $1202.45 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 47 | $25.18 | $2.13 | — | $54.36 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+16.0; leftover $1202.45 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $54.36 | ▲ close $9,940.31 vs 09:30 $9,463.18 (session +520.31) | 16:00 close · cash $54.36 · equity $9,940.31 vs 09:30 $9,463.18 (+477.13; session marks +520.31) · 8 name(s) marked open→close (per-name table). GPRO×692 09:30 $1.48 → close $1.70 +152.24; DFDV×207 09:30 $5.79 → close $5.87 +16.56; FRNM×73 09:30 $16.40 → close $16.31 -6.57; TARS×14 09:30 $82.70 → close $90.78 +113.12; BRR×479 09:30 $2.51 → close $2.66 +71.85; IRD×265 09:30 $4.53 → close $4.67 +37.10; LENZ×209 09:30 $5.75 → close $5.96 +43.89; ASST×47 09:30 $25.18 → close $27.14 +92.12 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $54.36 | ▼ 09:30 equity $9,778.23 vs yday $9,940.31 (-162.08) | 09:30 open · cash $54.36 (unchanged overnight, no fees) · equity $9,778.23 vs prior close $9,940.31 (-162.08) · 8 name(s) re-marked at the open (per-name table). GPRO×692 yday $1.70 → 09:30 $1.56 -93.42; DFDV×207 yday $5.87 → 09:30 $5.81 -12.42; FRNM×73 yday $16.31 → 09:30 $16.74 +31.39; TARS×14 yday $90.78 → 09:30 $89.67 -15.54; BRR×479 yday $2.66 → 09:30 $2.66 +0.00; IRD×265 yday $4.67 → 09:30 $4.53 -37.10; LENZ×209 yday $5.96 → 09:30 $5.95 -2.09; ASST×47 yday $27.14 → 09:30 $26.44 -32.90 | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 692 | $1.56 | $9.05 | $-166.76 | $1,128.29 | ▼ -166.76 after sell → book $9,769.18; vs 09:30 mark -9.05 | dropped from list after 2 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 207 | $5.81 | $2.71 | $-1.25 | $2,328.24 | ▼ -1.25 after sell → book $9,766.46; vs 09:30 mark -2.72 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 73 | $16.74 | $2.23 | $+20.38 | $3,548.03 | ▲ +20.38 after sell → book $9,764.23; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TARS` | 14 | $89.67 | $2.05 | $+93.50 | $4,801.36 | ▲ +93.50 after sell → book $9,762.18; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BRR` | 479 | $2.66 | $6.27 | $+59.40 | $6,069.23 | ▲ +59.40 after sell → book $9,755.91; vs 09:30 mark -6.27 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `IRD` | 265 | $4.53 | $3.47 | $-6.89 | $7,266.21 | ▼ -6.89 after sell → book $9,752.44; vs 09:30 mark -3.47 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LENZ` | 209 | $5.95 | $2.74 | $+36.36 | $8,507.02 | ▲ +36.36 after sell → book $9,749.70; vs 09:30 mark -2.74 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 47 | $26.44 | $2.15 | $+54.94 | $9,747.55 | ▲ +54.94 after sell → book $9,747.55; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,747.55 | ▲ close $9,747.55 vs 09:30 $9,778.23 (session +0.00) | 16:00 close · cash $9,747.55 · no lots left · equity $9,747.55. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,747.55 | ▲ 09:30 equity $9,747.55 vs yday $9,747.55 (-0.00) | 09:30 open · cash $9,747.55 · no holdings · equity $9,747.55 vs prior close $9,747.55 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,747.55 | ▲ close $9,747.55 vs 09:30 $9,747.55 (session +0.00) | 16:00 close · cash $9,747.55 · no lots left · equity $9,747.55. | — |

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
