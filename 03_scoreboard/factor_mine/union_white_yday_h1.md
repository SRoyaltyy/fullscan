# Factor mine action — `union_white_yday_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · union looker: 0 red cameras + yesterday up, rank +G−R

Cash book **-4.12%** ($9,588) · signal-only (no cash/fees) was +4.13%. Starts YES **3/26**. Fills 158 · skips 1 · realized $-277.22.

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
- Must-have: no morning camera is red (the 'white' / all-clear row).
- Must-have: yesterday's session was up (prior close-to-close Change% > 0, or last finished bar green if the % is missing).
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
- **Gate** `zero_red=True,yday_up=True` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $246.15.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `BTSG` | 41 | — | $59.80 | +0.00 | $60.23 | +17.63 | +17.63 | +0.00 | +17.63 |
| 2026-08-13 | `IREN` | 54 | — | $45.98 | +0.00 | $44.76 | -65.88 | -65.88 | +0.00 | -65.88 |
| 2026-08-13 | `SLS` | 213 | — | $11.70 | +0.00 | $12.36 | +140.58 | +140.58 | +0.00 | +140.58 |
| 2026-08-13 | `TPG` | 49 | — | $50.62 | +0.00 | $54.62 | +195.84 | +195.84 | +0.00 | +195.84 |
| 2026-08-14 | `BTSG` | 41 | $60.23 | $59.65 | -23.78 | — | +0.00 | -23.78 | -6.15 | — |
| 2026-08-14 | `IREN` | 54 | $44.76 | $44.09 | -36.18 | — | +0.00 | -36.18 | -102.06 | — |
| 2026-08-14 | `SLS` | 213 | $12.36 | $12.40 | +8.52 | — | +0.00 | +8.52 | +149.10 | — |
| 2026-08-14 | `TPG` | 49 | $54.62 | $55.29 | +32.83 | — | +0.00 | +32.83 | +228.67 | — |
| 2026-08-14 | `BRUN` | 48 | — | $26.25 | +0.00 | $22.93 | -159.12 | -159.12 | +0.00 | -159.12 |
| 2026-08-14 | `HLIT` | 97 | — | $13.18 | +0.00 | $13.92 | +71.78 | +71.78 | +0.00 | +71.78 |
| 2026-08-14 | `MNTN` | 102 | — | $12.50 | +0.00 | $12.52 | +2.04 | +2.04 | +0.00 | +2.04 |
| 2026-08-14 | `QMCO` | 51 | — | $24.68 | +0.00 | $26.11 | +72.93 | +72.93 | +0.00 | +72.93 |
| 2026-08-14 | `QMLS` | 175 | — | $7.29 | +0.00 | $7.32 | +5.25 | +5.25 | +0.00 | +5.25 |
| 2026-08-14 | `SMWB` | 142 | — | $9.01 | +0.00 | $8.89 | -17.04 | -17.04 | +0.00 | -17.04 |
| 2026-08-14 | `ZENA` | 582 | — | $2.20 | +0.00 | $2.14 | -34.92 | -34.92 | +0.00 | -34.92 |
| 2026-08-17 | `BRUN` | 48 | $22.93 | $23.00 | +3.36 | — | +0.00 | +3.36 | -155.76 | — |
| 2026-08-17 | `HLIT` | 97 | $13.92 | $13.84 | -7.76 | — | +0.00 | -7.76 | +64.02 | — |
| 2026-08-17 | `MNTN` | 102 | $12.52 | $12.40 | -12.24 | — | +0.00 | -12.24 | -10.20 | — |
| 2026-08-17 | `QMCO` | 51 | $26.11 | $24.83 | -65.28 | — | +0.00 | -65.28 | +7.65 | — |
| 2026-08-17 | `QMLS` | 175 | $7.32 | $7.24 | -14.00 | — | +0.00 | -14.00 | -8.75 | — |
| 2026-08-17 | `SMWB` | 142 | $8.89 | $8.96 | +9.94 | — | +0.00 | +9.94 | -7.10 | — |
| 2026-08-17 | `ZENA` | 582 | $2.14 | $2.08 | -32.01 | — | +0.00 | -32.01 | -66.93 | — |
| 2026-08-17 | `LPTH` | 83 | — | $14.94 | +0.00 | $14.80 | -11.62 | -11.62 | +0.00 | -11.62 |
| 2026-08-17 | `AAOI` | 8 | — | $152.64 | +0.00 | $154.89 | +18.00 | +18.00 | +0.00 | +18.00 |
| 2026-08-17 | `ABX` | 137 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `ALOY` | 85 | — | $14.66 | +0.00 | $13.86 | -68.42 | -68.42 | +0.00 | -68.42 |
| 2026-08-17 | `BORR` | 273 | — | $4.59 | +0.00 | $4.50 | -24.57 | -24.57 | +0.00 | -24.57 |
| 2026-08-17 | `DIOD` | 12 | — | $104.00 | +0.00 | $107.56 | +42.72 | +42.72 | +0.00 | +42.72 |
| 2026-08-17 | `INDI` | 269 | — | $4.65 | +0.00 | $4.71 | +16.14 | +16.14 | +0.00 | +16.14 |
| 2026-08-17 | `KOPN` | 230 | — | $5.43 | +0.00 | $5.32 | -25.30 | -25.30 | +0.00 | -25.30 |
| 2026-08-18 | `LPTH` | 83 | $14.80 | $14.01 | -65.57 | — | +0.00 | -65.57 | -77.19 | — |
| 2026-08-18 | `AAOI` | 8 | $154.89 | $146.20 | -69.52 | — | +0.00 | -69.52 | -51.52 | — |
| 2026-08-18 | `ABX` | 137 | $9.12 | $9.03 | -12.33 | — | +0.00 | -12.33 | -12.33 | — |
| 2026-08-18 | `ALOY` | 85 | $13.86 | $13.19 | -56.53 | — | +0.00 | -56.53 | -124.95 | — |
| 2026-08-18 | `BORR` | 273 | $4.50 | $4.56 | +16.38 | — | +0.00 | +16.38 | -8.19 | — |
| 2026-08-18 | `DIOD` | 12 | $107.56 | $103.01 | -54.60 | — | +0.00 | -54.60 | -11.88 | — |
| 2026-08-18 | `INDI` | 269 | $4.71 | $4.48 | -61.87 | — | +0.00 | -61.87 | -45.73 | — |
| 2026-08-18 | `KOPN` | 230 | $5.32 | $5.03 | -66.70 | — | +0.00 | -66.70 | -92.00 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 58 | — | $20.55 | +0.00 | $21.19 | +37.12 | +37.12 | +0.00 | +37.12 |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `CDE` | 57 | — | $20.65 | +0.00 | $21.11 | +26.22 | +26.22 | +0.00 | +26.22 |
| 2026-08-20 | `HDSN` | 207 | — | $5.77 | +0.00 | $5.57 | -41.40 | -41.40 | +0.00 | -41.40 |
| 2026-08-20 | `IAG` | 60 | — | $19.63 | +0.00 | $20.50 | +52.20 | +52.20 | +0.00 | +52.20 |
| 2026-08-20 | `KGC` | 40 | — | $29.63 | +0.00 | $31.43 | +72.00 | +72.00 | +0.00 | +72.00 |
| 2026-08-20 | `NFGC` | 683 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-21 | `AG` | 58 | $21.19 | $21.90 | +41.18 | — | +0.00 | +41.18 | +78.30 | — |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `CDE` | 57 | $21.11 | $21.75 | +36.48 | — | +0.00 | +36.48 | +62.70 | — |
| 2026-08-21 | `HDSN` | 207 | $5.57 | $5.67 | +20.70 | — | +0.00 | +20.70 | -20.70 | — |
| 2026-08-21 | `IAG` | 60 | $20.50 | $21.17 | +40.20 | — | +0.00 | +40.20 | +92.40 | — |
| 2026-08-21 | `KGC` | 40 | $31.43 | $32.17 | +29.60 | — | +0.00 | +29.60 | +101.60 | — |
| 2026-08-21 | `NFGC` | 683 | $1.75 | $1.79 | +27.32 | — | +0.00 | +27.32 | +27.32 | — |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | — | +0.00 | +35.60 | +81.28 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `AUPH` | 72 | — | $17.20 | +0.00 | $16.65 | -39.60 | -39.60 | +0.00 | -39.60 |
| 2026-08-21 | `AEM` | 5 | — | $216.30 | +0.00 | $216.06 | -1.20 | -1.20 | +0.00 | -1.20 |
| 2026-08-21 | `ARCT` | 112 | — | $11.13 | +0.00 | $13.45 | +259.84 | +259.84 | +0.00 | +259.84 |
| 2026-08-21 | `CYPH` | 947 | — | $1.32 | +0.00 | $1.42 | +94.70 | +94.70 | +0.00 | +94.70 |
| 2026-08-21 | `FUTU` | 10 | — | $115.18 | +0.00 | $123.64 | +84.60 | +84.60 | +0.00 | +84.60 |
| 2026-08-21 | `GRAL` | 15 | — | $78.88 | +0.00 | $79.54 | +9.90 | +9.90 | +0.00 | +9.90 |
| 2026-08-21 | `ILMN` | 5 | — | $212.40 | +0.00 | $219.40 | +35.00 | +35.00 | +0.00 | +35.00 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.51 | -7.10 | — | +0.00 | -7.10 | +10.80 | — |
| 2026-08-24 | `AUPH` | 72 | $16.65 | $16.57 | -5.76 | — | +0.00 | -5.76 | -45.36 | — |
| 2026-08-24 | `AEM` | 5 | $216.06 | $217.03 | +4.85 | — | +0.00 | +4.85 | +3.65 | — |
| 2026-08-24 | `ARCT` | 112 | $13.45 | $13.33 | -13.44 | — | +0.00 | -13.44 | +246.40 | — |
| 2026-08-24 | `CYPH` | 947 | $1.42 | $1.83 | +388.27 | — | +0.00 | +388.27 | +482.97 | — |
| 2026-08-24 | `FUTU` | 10 | $123.64 | $121.00 | -26.40 | — | +0.00 | -26.40 | +58.20 | — |
| 2026-08-24 | `GRAL` | 15 | $79.54 | $81.87 | +34.95 | — | +0.00 | +34.95 | +44.85 | — |
| 2026-08-24 | `ILMN` | 5 | $219.40 | $215.98 | -17.10 | — | +0.00 | -17.10 | +17.90 | — |
| 2026-08-25 | `FCX` | 17 | — | $77.13 | +0.00 | $79.91 | +47.26 | +47.26 | +0.00 | +47.26 |
| 2026-08-25 | `RHI` | 30 | — | $43.76 | +0.00 | $44.90 | +34.20 | +34.20 | +0.00 | +34.20 |
| 2026-08-25 | `SUZ` | 149 | — | $8.98 | +0.00 | $9.03 | +7.45 | +7.45 | +0.00 | +7.45 |
| 2026-08-25 | `VALE` | 89 | — | $15.01 | +0.00 | $15.33 | +28.48 | +28.48 | +0.00 | +28.48 |
| 2026-08-25 | `WPM` | 8 | — | $156.51 | +0.00 | $163.72 | +57.68 | +57.68 | +0.00 | +57.68 |
| 2026-08-25 | `AVAH` | 98 | — | $13.62 | +0.00 | $13.59 | -3.43 | -3.43 | +0.00 | -3.43 |
| 2026-08-25 | `BMEA` | 825 | — | $1.63 | +0.00 | $1.73 | +82.50 | +82.50 | +0.00 | +82.50 |
| 2026-08-25 | `CYPH` | 862 | — | $1.56 | +0.00 | $1.64 | +68.96 | +68.96 | +0.00 | +68.96 |
| 2026-08-26 | `FCX` | 17 | $79.91 | $79.34 | -9.69 | — | +0.00 | -9.69 | +37.57 | — |
| 2026-08-26 | `RHI` | 30 | $44.90 | $44.33 | -17.10 | — | +0.00 | -17.10 | +17.10 | — |
| 2026-08-26 | `SUZ` | 149 | $9.03 | $9.03 | +0.00 | — | +0.00 | +0.00 | +7.45 | — |
| 2026-08-26 | `VALE` | 89 | $15.33 | $15.37 | +3.56 | — | +0.00 | +3.56 | +32.04 | — |
| 2026-08-26 | `WPM` | 8 | $163.72 | $160.93 | -22.32 | — | +0.00 | -22.32 | +35.36 | — |
| 2026-08-26 | `AVAH` | 98 | $13.59 | $13.65 | +5.88 | — | +0.00 | +5.88 | +2.45 | — |
| 2026-08-26 | `BMEA` | 825 | $1.73 | $1.75 | +20.62 | — | +0.00 | +20.62 | +103.12 | — |
| 2026-08-26 | `CYPH` | 862 | $1.64 | $1.60 | -34.48 | — | +0.00 | -34.48 | +34.48 | — |
| 2026-08-27 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-28 | `KEYS` | 4 | — | $324.41 | +0.00 | $319.97 | -17.76 | -17.76 | +0.00 | -17.76 |
| 2026-08-28 | `SMTC` | 9 | — | $141.76 | +0.00 | $131.17 | -95.31 | -95.31 | +0.00 | -95.31 |
| 2026-08-28 | `AVT` | 14 | — | $91.49 | +0.00 | $88.63 | -40.04 | -40.04 | +0.00 | -40.04 |
| 2026-08-28 | `CGNX` | 21 | — | $62.82 | +0.00 | $60.46 | -49.56 | -49.56 | +0.00 | -49.56 |
| 2026-08-28 | `COHR` | 4 | — | $289.44 | +0.00 | $279.20 | -40.96 | -40.96 | +0.00 | -40.96 |
| 2026-08-28 | `LSCC` | 11 | — | $119.76 | +0.00 | $114.40 | -58.96 | -58.96 | +0.00 | -58.96 |
| 2026-08-28 | `MEI` | 77 | — | $17.78 | +0.00 | $18.21 | +33.11 | +33.11 | +0.00 | +33.11 |
| 2026-08-28 | `MTSI` | 4 | — | $275.20 | +0.00 | $265.27 | -39.72 | -39.72 | +0.00 | -39.72 |
| 2026-08-31 | `KEYS` | 4 | $319.97 | $322.49 | +10.08 | — | +0.00 | +10.08 | -7.68 | — |
| 2026-08-31 | `SMTC` | 9 | $131.17 | $132.30 | +10.17 | — | +0.00 | +10.17 | -85.14 | — |
| 2026-08-31 | `AVT` | 14 | $88.63 | $89.39 | +10.64 | — | +0.00 | +10.64 | -29.40 | — |
| 2026-08-31 | `CGNX` | 21 | $60.46 | $60.46 | +0.00 | — | +0.00 | +0.00 | -49.56 | — |
| 2026-08-31 | `COHR` | 4 | $279.20 | $280.25 | +4.20 | — | +0.00 | +4.20 | -36.76 | — |
| 2026-08-31 | `LSCC` | 11 | $114.40 | $115.56 | +12.76 | — | +0.00 | +12.76 | -46.20 | — |
| 2026-08-31 | `MEI` | 77 | $18.21 | $18.15 | -4.62 | — | +0.00 | -4.62 | +28.49 | — |
| 2026-08-31 | `MTSI` | 4 | $265.27 | $266.96 | +6.76 | — | +0.00 | +6.76 | -32.96 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ARCT` | 79 | — | $16.77 | +0.00 | $15.56 | -95.59 | -95.59 | +0.00 | -95.59 |
| 2026-09-03 | `BMEA` | 691 | — | $1.93 | +0.00 | $1.91 | -13.82 | -13.82 | +0.00 | -13.82 |
| 2026-09-03 | `CRDL` | 612 | — | $2.18 | +0.00 | $2.16 | -12.24 | -12.24 | +0.00 | -12.24 |
| 2026-09-03 | `HRMY` | 31 | — | $42.93 | +0.00 | $41.86 | -33.17 | -33.17 | +0.00 | -33.17 |
| 2026-09-03 | `NVAX` | 128 | — | $10.42 | +0.00 | $10.34 | -10.24 | -10.24 | +0.00 | -10.24 |
| 2026-09-03 | `PBH` | 24 | — | $53.45 | +0.00 | $52.56 | -21.36 | -21.36 | +0.00 | -21.36 |
| 2026-09-03 | `PCRX` | 49 | — | $26.74 | +0.00 | $26.60 | -6.86 | -6.86 | +0.00 | -6.86 |
| 2026-09-03 | `RVTY` | 10 | — | $132.45 | +0.00 | $130.63 | -18.20 | -18.20 | +0.00 | -18.20 |
| 2026-09-04 | `ARCT` | 79 | $15.56 | $15.61 | +3.95 | — | +0.00 | +3.95 | -91.64 | — |
| 2026-09-04 | `BMEA` | 691 | $1.91 | $1.90 | -6.91 | — | +0.00 | -6.91 | -20.73 | — |
| 2026-09-04 | `CRDL` | 612 | $2.16 | $2.16 | +0.00 | — | +0.00 | +0.00 | -12.24 | — |
| 2026-09-04 | `HRMY` | 31 | $41.86 | $41.50 | -11.16 | — | +0.00 | -11.16 | -44.33 | — |
| 2026-09-04 | `NVAX` | 128 | $10.34 | $10.50 | +20.48 | — | +0.00 | +20.48 | +10.24 | — |
| 2026-09-04 | `PBH` | 24 | $52.56 | $51.80 | -18.24 | — | +0.00 | -18.24 | -39.60 | — |
| 2026-09-04 | `PCRX` | 49 | $26.60 | $26.38 | -10.78 | — | +0.00 | -10.78 | -17.64 | — |
| 2026-09-04 | `RVTY` | 10 | $130.63 | $130.03 | -6.00 | — | +0.00 | -6.00 | -24.20 | — |
| 2026-09-04 | `CRM` | 4 | — | $263.36 | +0.00 | $259.23 | -16.52 | -16.52 | +0.00 | -16.52 |
| 2026-09-04 | `TARS` | 15 | — | $82.70 | +0.00 | $90.78 | +121.20 | +121.20 | +0.00 | +121.20 |
| 2026-09-04 | `DELL` | 2 | — | $513.78 | +0.00 | $524.14 | +20.72 | +20.72 | +0.00 | +20.72 |
| 2026-09-04 | `FRNM` | 79 | — | $16.40 | +0.00 | $16.31 | -7.11 | -7.11 | +0.00 | -7.11 |
| 2026-09-04 | `IRD` | 286 | — | $4.53 | +0.00 | $4.67 | +40.04 | +40.04 | +0.00 | +40.04 |
| 2026-09-04 | `LENZ` | 225 | — | $5.75 | +0.00 | $5.96 | +47.25 | +47.25 | +0.00 | +47.25 |
| 2026-09-04 | `MMED` | 54 | — | $23.84 | +0.00 | $23.29 | -29.70 | -29.70 | +0.00 | -29.70 |
| 2026-09-04 | `PIPR` | 16 | — | $76.55 | +0.00 | $77.04 | +7.84 | +7.84 | +0.00 | +7.84 |
| 2026-09-08 | `CRM` | 4 | $259.23 | $253.72 | -22.04 | — | +0.00 | -22.04 | -38.56 | — |
| 2026-09-08 | `TARS` | 15 | $90.78 | $89.67 | -16.65 | — | +0.00 | -16.65 | +104.55 | — |
| 2026-09-08 | `DELL` | 2 | $524.14 | $521.15 | -5.98 | — | +0.00 | -5.98 | +14.74 | — |
| 2026-09-08 | `FRNM` | 79 | $16.31 | $16.74 | +33.97 | — | +0.00 | +33.97 | +26.86 | — |
| 2026-09-08 | `IRD` | 286 | $4.67 | $4.53 | -40.04 | — | +0.00 | -40.04 | +0.00 | — |
| 2026-09-08 | `LENZ` | 225 | $5.96 | $5.95 | -2.25 | — | +0.00 | -2.25 | +45.00 | — |
| 2026-09-08 | `MMED` | 54 | $23.29 | $23.16 | -7.02 | — | +0.00 | -7.02 | -36.72 | — |
| 2026-09-08 | `PIPR` | 16 | $77.04 | $76.64 | -6.40 | — | +0.00 | -6.40 | +1.44 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `SEDG` | 56 | — | $36.78 | +0.00 | $34.68 | -117.60 | -117.60 | +0.00 | -117.60 |
| 2026-09-11 | `BAND` | 39 | — | $52.55 | +0.00 | $56.87 | +168.48 | +168.48 | +0.00 | +168.48 |
| 2026-09-11 | `PAGS` | 206 | — | $10.11 | +0.00 | $10.12 | +2.06 | +2.06 | +0.00 | +2.06 |
| 2026-09-11 | `PAYP` | 114 | — | $18.30 | +0.00 | $18.45 | +17.10 | +17.10 | +0.00 | +17.10 |
| 2026-09-11 | `ZSQR` | 643 | — | $3.25 | +0.00 | $3.07 | -115.74 | -115.74 | +0.00 | -115.74 |
| 2026-09-14 | `SEDG` | 56 | $34.68 | $33.64 | -58.24 | — | +0.00 | -58.24 | -175.84 | — |
| 2026-09-14 | `BAND` | 39 | $56.87 | $56.90 | +1.17 | — | +0.00 | +1.17 | +169.65 | — |
| 2026-09-14 | `PAGS` | 206 | $10.12 | $10.00 | -24.72 | — | +0.00 | -24.72 | -22.66 | — |
| 2026-09-14 | `PAYP` | 114 | $18.45 | $18.28 | -19.38 | — | +0.00 | -19.38 | -2.28 | — |
| 2026-09-14 | `ZSQR` | 643 | $3.07 | $3.06 | -6.43 | — | +0.00 | -6.43 | -122.17 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `QRVO` | 43 | — | $118.18 | +0.00 | $113.97 | -181.03 | -181.03 | +0.00 | -181.03 |
| 2026-09-16 | `SWKS` | 57 | — | $89.38 | +0.00 | $85.59 | -216.03 | -216.03 | +0.00 | -216.03 |
| 2026-09-17 | `QRVO` | 43 | $113.97 | $114.90 | +39.99 | — | +0.00 | +39.99 | -141.04 | — |
| 2026-09-17 | `SWKS` | 57 | $85.59 | $86.76 | +66.69 | — | +0.00 | +66.69 | -149.34 | — |
| 2026-09-17 | `ASAN` | 521 | — | $9.55 | +0.00 | $10.09 | +281.34 | +281.34 | +0.00 | +281.34 |
| 2026-09-17 | `VOD` | 283 | — | $17.56 | +0.00 | $17.52 | -11.32 | -11.32 | +0.00 | -11.32 |
| 2026-09-18 | `ASAN` | 521 | $10.09 | $10.09 | +0.00 | $9.51 | -302.18 | -302.18 | +281.34 | -20.84 |
| 2026-09-18 | `VOD` | 283 | $17.52 | $16.73 | -223.57 | — | +0.00 | -223.57 | -234.89 | — |
| 2026-09-18 | `S` | 29 | — | $23.13 | +0.00 | $22.51 | -17.98 | -17.98 | +0.00 | -17.98 |
| 2026-09-18 | `ATRC` | 11 | — | $58.51 | +0.00 | $58.10 | -4.51 | -4.51 | +0.00 | -4.51 |
| 2026-09-18 | `ILMN` | 2 | — | $249.13 | +0.00 | $239.62 | -19.02 | -19.02 | +0.00 | -19.02 |
| 2026-09-18 | `PGEN` | 84 | — | $7.98 | +0.00 | $7.78 | -16.80 | -16.80 | +0.00 | -16.80 |
| 2026-09-18 | `RXT` | 171 | — | $3.94 | +0.00 | $3.80 | -23.94 | -23.94 | +0.00 | -23.94 |
| 2026-09-18 | `TH` | 32 | — | $20.91 | +0.00 | $21.19 | +8.96 | +8.96 | +0.00 | +8.96 |
| 2026-09-18 | `ARQT` | 25 | — | $26.14 | +0.00 | $25.38 | -19.00 | -19.00 | +0.00 | -19.00 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +288.17 | BTSG, IREN, SLS, TPG | — | $83.49 | $10,279.02 | BTSG×41, IREN×54, SLS×213, TPG×49 |
| 2026-08-14 | +5.50 | $83.49 | BTSG×41, IREN×54, SLS×213, TPG×49 | $10,260.41 | -18.61 | -59.08 | BRUN, HLIT, MNTN, QMCO, QMLS, SMWB, ZENA | BTSG, IREN, SLS, TPG | $1,322.35 | $10,170.74 | BRUN×48, HLIT×97, MNTN×102, QMCO×51, QMLS×175, SMWB×142, ZENA×582 |
| 2026-08-17 | +2.25 | $1,322.35 | BRUN×48, HLIT×97, MNTN×102, QMCO×51, QMLS×175, SMWB×142, ZENA×582 | $10,052.75 | -117.99 | -53.05 | LPTH, AAOI, ABX, ALOY, BORR, DIOD, INDI, KOPN | BRUN, HLIT, MNTN, QMCO, QMLS, SMWB, ZENA | $52.80 | $9,957.25 | LPTH×83, AAOI×8, ABX×137, ALOY×85, BORR×273, DIOD×12, INDI×269, KOPN×230 |
| 2026-08-18 | -6.20 | $52.80 | LPTH×83, AAOI×8, ABX×137, ALOY×85, BORR×273, DIOD×12, INDI×269, KOPN×230 | $9,586.51 | -370.74 | +0.00 | — | LPTH, AAOI, ABX, ALOY, BORR, DIOD, INDI, KOPN | $9,565.35 | $9,565.35 | — |
| 2026-08-19 | -7.20 | $9,565.35 | — | $9,565.35 | +0.00 | +0.00 | — | — | $9,565.35 | $9,565.35 | — |
| 2026-08-20 | +1.12 | $9,565.35 | — | $9,565.35 | +0.00 | +225.88 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $80.18 | $9,767.10 | AG×58, BHP×13, CDE×57, HDSN×207, IAG×60, KGC×40, NFGC×683, WPM×8 |
| 2026-08-21 | +3.25 | $80.18 | AG×58, BHP×13, CDE×57, HDSN×207, IAG×60, KGC×40, NFGC×683, WPM×8 | $10,025.35 | +258.25 | +461.14 | AU, AUPH, AEM, ARCT, CYPH, FUTU, GRAL, ILMN | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $566.30 | $10,435.24 | AU×10, AUPH×72, AEM×5, ARCT×112, CYPH×947, FUTU×10, GRAL×15, ILMN×5 |
| 2026-08-24 | -5.17 | $566.30 | AU×10, AUPH×72, AEM×5, ARCT×112, CYPH×947, FUTU×10, GRAL×15, ILMN×5 | $10,793.51 | +358.27 | +0.00 | — | AU, AUPH, AEM, ARCT, CYPH, FUTU, GRAL, ILMN | $10,766.35 | $10,766.35 | — |
| 2026-08-25 | +1.80 | $10,766.35 | — | $10,766.35 | +0.00 | +323.10 | FCX, RHI, SUZ, VALE, WPM, AVAH, BMEA, CYPH | — | $156.76 | $11,054.58 | FCX×17, RHI×30, SUZ×149, VALE×89, WPM×8, AVAH×98, BMEA×825, CYPH×862 |
| 2026-08-26 | +2.02 | $156.76 | FCX×17, RHI×30, SUZ×149, VALE×89, WPM×8, AVAH×98, BMEA×825, CYPH×862 | $11,001.05 | -53.53 | +0.00 | — | FCX, RHI, SUZ, VALE, WPM, AVAH, BMEA, CYPH | $10,965.73 | $10,965.73 | — |
| 2026-08-27 | — | $10,965.73 | — | $10,965.73 | -0.00 | +0.00 | — | — | $10,965.73 | $10,965.73 | — |
| 2026-08-28 | +0.75 | $10,965.73 | — | $10,965.73 | -0.00 | -309.20 | KEYS, SMTC, AVT, CGNX, COHR, LSCC, MEI, MTSI | — | $830.83 | $10,640.17 | KEYS×4, SMTC×9, AVT×14, CGNX×21, COHR×4, LSCC×11, MEI×77, MTSI×4 |
| 2026-08-31 | -5.85 | $830.83 | KEYS×4, SMTC×9, AVT×14, CGNX×21, COHR×4, LSCC×11, MEI×77, MTSI×4 | $10,690.16 | +49.99 | +0.00 | — | KEYS, SMTC, AVT, CGNX, COHR, LSCC, MEI, MTSI | $10,673.65 | $10,673.65 | — |
| 2026-09-01 | -6.30 | $10,673.65 | — | $10,673.65 | -0.00 | +0.00 | — | — | $10,673.65 | $10,673.65 | — |
| 2026-09-02 | -3.83 | $10,673.65 | — | $10,673.65 | -0.00 | +0.00 | — | — | $10,673.65 | $10,673.65 | — |
| 2026-09-03 | -0.90 | $10,673.65 | — | $10,673.65 | -0.00 | -211.48 | ARCT, BMEA, CRDL, HRMY, NVAX, PBH, PCRX, RVTY | — | $69.17 | $10,432.46 | ARCT×79, BMEA×691, CRDL×612, HRMY×31, NVAX×128, PBH×24, PCRX×49, RVTY×10 |
| 2026-09-04 | +2.25 | $69.17 | ARCT×79, BMEA×691, CRDL×612, HRMY×31, NVAX×128, PBH×24, PCRX×49, RVTY×10 | $10,403.80 | -28.66 | +183.72 | CRM, TARS, DELL, FRNM, IRD, LENZ, MMED, PIPR | ARCT, BMEA, CRDL, HRMY, NVAX, PBH, PCRX, RVTY | $636.08 | $10,538.39 | CRM×4, TARS×15, DELL×2, FRNM×79, IRD×286, LENZ×225, MMED×54, PIPR×16 |
| 2026-09-08 | -11.47 | $636.08 | CRM×4, TARS×15, DELL×2, FRNM×79, IRD×286, LENZ×225, MMED×54, PIPR×16 | $10,471.98 | -66.41 | +0.00 | — | CRM, TARS, DELL, FRNM, IRD, LENZ, MMED, PIPR | $10,452.71 | $10,452.71 | — |
| 2026-09-09 | -13.95 | $10,452.71 | — | $10,452.71 | -0.00 | +0.00 | — | — | $10,452.71 | $10,452.71 | — |
| 2026-09-10 | -13.28 | $10,452.71 | — | $10,452.71 | -0.00 | +0.00 | — | — | $10,452.71 | $10,452.71 | — |
| 2026-09-11 | +0.50 | $10,452.71 | — | $10,452.71 | -0.00 | -45.70 | SEDG, BAND, PAGS, PAYP, ZSQR | — | $67.42 | $10,389.46 | SEDG×56, BAND×39, PAGS×206, PAYP×114, ZSQR×643 |
| 2026-09-14 | -11.00 | $67.42 | SEDG×56, BAND×39, PAGS×206, PAYP×114, ZSQR×643 | $10,281.86 | -107.60 | +0.00 | — | SEDG, BAND, PAGS, PAYP, ZSQR | $10,264.05 | $10,264.05 | — |
| 2026-09-15 | -3.84 | $10,264.05 | — | $10,264.05 | -0.00 | +0.00 | — | — | $10,264.05 | $10,264.05 | — |
| 2026-09-16 | +5.30 | $10,264.05 | — | $10,264.05 | -0.00 | -397.06 | QRVO, SWKS | — | $83.37 | $9,862.71 | QRVO×43, SWKS×57 |
| 2026-09-17 | +7.38 | $83.37 | QRVO×43, SWKS×57 | $9,969.39 | +106.68 | +270.02 | ASAN, VOD | QRVO, SWKS | $9.61 | $10,224.66 | ASAN×521, VOD×283 |
| 2026-09-18 | +4.86 | $9.61 | ASAN×521, VOD×283 | $10,001.09 | -223.57 | -394.47 | S, ATRC, ILMN, PGEN, RXT, TH, ARQT | VOD | $246.15 | $9,587.89 | ASAN×521, S×29, ATRC×11, ILMN×2, PGEN×84, RXT×171, TH×32, ARQT×25 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 41 | $59.80 | $2.11 | — | $7,546.09 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten; ⚪; ret5=-5.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 54 | $45.98 | $2.15 | — | $5,061.02 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten; ⚪; ret5=+12.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 213 | $11.70 | $2.75 | — | $2,566.17 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten; ⚪; ret5=-0.8; leftover $2500.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 49 | $50.62 | $2.14 | — | $83.49 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten; ⚪; ret5=+6.2; leftover $2500.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $83.49 | ▲ close $10,279.02 vs 09:30 $10,000.00 (session +288.17) | 16:00 close · cash $83.49 · equity $10,279.02 vs 09:30 $10,000.00 (+279.02; session marks +288.17) · 4 name(s) marked open→close (per-name table). BTSG×41 09:30 $59.80 → close $60.23 +17.63; IREN×54 09:30 $45.98 → close $44.76 -65.88; SLS×213 09:30 $11.70 → close $12.36 +140.58; TPG×49 09:30 $50.62 → close $54.62 +195.84 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $83.49 | ▼ 09:30 equity $10,260.41 vs yday $10,279.02 (-18.61) | 09:30 open · cash $83.49 (unchanged overnight, no fees) · equity $10,260.41 vs prior close $10,279.02 (-18.61) · 4 name(s) re-marked at the open (per-name table). BTSG×41 yday $60.23 → 09:30 $59.65 -23.78; IREN×54 yday $44.76 → 09:30 $44.09 -36.18; SLS×213 yday $12.36 → 09:30 $12.40 +8.52; TPG×49 yday $54.62 → 09:30 $55.29 +32.83 | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 41 | $59.65 | $2.14 | $-10.41 | $2,527.00 | ▼ -10.41 after sell → book $10,258.27; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 54 | $44.09 | $2.18 | $-106.39 | $4,905.68 | ▼ -106.39 after sell → book $10,256.09; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 213 | $12.40 | $2.80 | $+143.55 | $7,544.08 | ▲ +143.55 after sell → book $10,253.29; vs 09:30 mark -2.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 49 | $55.29 | $2.17 | $+224.37 | $10,251.12 | ▲ +224.37 after sell → book $10,251.12; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BRUN` | 48 | $26.25 | $2.13 | — | $8,989.22 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list earn_react; 🔵; ⚪; ret5=+31.2; leftover $1281.39 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 97 | $13.18 | $2.28 | — | $7,708.48 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1281.39 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MNTN` | 102 | $12.50 | $2.30 | — | $6,431.19 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $1281.39 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 51 | $24.68 | $2.14 | — | $5,170.36 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,oppset; 🔵; ⚪; ret5=+111.3; leftover $1281.39 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 175 | $7.29 | $2.52 | — | $3,892.10 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $1281.39 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SMWB` | 142 | $9.01 | $2.42 | — | $2,610.26 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list oppset; 🔵; ⚪; ret5=+22.1; leftover $1281.39 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 582 | $2.20 | $7.51 | — | $1,322.35 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $1281.39 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,322.35 | ▼ close $10,170.74 vs 09:30 $10,260.41 (session -59.08) | 16:00 close · cash $1,322.35 · equity $10,170.74 vs 09:30 $10,260.41 (-89.67; session marks -59.08) · 7 name(s) marked open→close (per-name table). BRUN×48 09:30 $26.25 → close $22.93 -159.12; HLIT×97 09:30 $13.18 → close $13.92 +71.78; MNTN×102 09:30 $12.50 → close $12.52 +2.04; QMCO×51 09:30 $24.68 → close $26.11 +72.93; QMLS×175 09:30 $7.29 → close $7.32 +5.25; SMWB×142 09:30 $9.01 → close $8.89 -17.04; ZENA×582 09:30 $2.20 → close $2.14 -34.92 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,322.35 | ▼ 09:30 equity $10,052.75 vs yday $10,170.74 (-117.99) | 09:30 open · cash $1,322.35 (unchanged overnight, no fees) · equity $10,052.75 vs prior close $10,170.74 (-117.99) · 7 name(s) re-marked at the open (per-name table). BRUN×48 yday $22.93 → 09:30 $23.00 +3.36; HLIT×97 yday $13.92 → 09:30 $13.84 -7.76; MNTN×102 yday $12.52 → 09:30 $12.40 -12.24; QMCO×51 yday $26.11 → 09:30 $24.83 -65.28; QMLS×175 yday $7.32 → 09:30 $7.24 -14.00; SMWB×142 yday $8.89 → 09:30 $8.96 +9.94; ZENA×582 yday $2.14 → 09:30 $2.08 -32.01 | — |
| 2026-08-17 09:30 ET | **SELL** | `BRUN` | 48 | $23.00 | $2.15 | $-160.05 | $2,424.20 | ▼ -160.05 after sell → book $10,050.60; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 97 | $13.84 | $2.31 | $+59.43 | $3,764.37 | ▲ +59.43 after sell → book $10,048.29; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MNTN` | 102 | $12.40 | $2.32 | $-14.82 | $5,026.85 | ▼ -14.82 after sell → book $10,045.97; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 51 | $24.83 | $2.16 | $+3.34 | $6,291.02 | ▲ +3.34 after sell → book $10,043.81; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `QMLS` | 175 | $7.24 | $2.55 | $-13.82 | $7,555.46 | ▼ -13.82 after sell → book $10,041.25; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SMWB` | 142 | $8.96 | $2.45 | $-11.97 | $8,825.33 | ▼ -11.97 after sell → book $10,038.80; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 582 | $2.08 | $7.61 | $-82.05 | $10,031.19 | ▼ -82.05 after sell → book $10,031.19; vs 09:30 mark -7.61 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `LPTH` | 83 | $14.94 | $2.24 | — | $8,788.93 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $1253.90 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `AAOI` | 8 | $152.64 | $2.01 | — | $7,565.79 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+10.8; leftover $1253.90 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 137 | $9.12 | $2.40 | — | $6,313.95 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1253.90 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 85 | $14.66 | $2.25 | — | $5,065.61 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $1253.90 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `BORR` | 273 | $4.59 | $3.52 | — | $3,809.02 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,ohlc_hot; ⚪; ret5=+14.8; leftover $1253.90 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DIOD` | 12 | $104.00 | $2.03 | — | $2,558.99 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list oppset; ⚪; ret5=-1.6; leftover $1253.90 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `INDI` | 269 | $4.65 | $3.47 | — | $1,304.67 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; ⚪; ret5=+16.6; leftover $1253.90 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `KOPN` | 230 | $5.43 | $2.97 | — | $52.80 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer; ⚪; ret5=+28.8; leftover $1253.90 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟢 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $52.80 | ▼ close $9,957.25 vs 09:30 $10,052.75 (session -53.05) | 16:00 close · cash $52.80 · equity $9,957.25 vs 09:30 $10,052.75 (-95.50; session marks -53.05) · 8 name(s) marked open→close (per-name table). LPTH×83 09:30 $14.94 → close $14.80 -11.62; AAOI×8 09:30 $152.64 → close $154.89 +18.00; ABX×137 09:30 $9.12 → close $9.12 +0.00; ALOY×85 09:30 $14.66 → close $13.86 -68.42; BORR×273 09:30 $4.59 → close $4.50 -24.57; DIOD×12 09:30 $104.00 → close $107.56 +42.72; INDI×269 09:30 $4.65 → close $4.71 +16.14; KOPN×230 09:30 $5.43 → close $5.32 -25.30 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $52.80 | ▼ 09:30 equity $9,586.51 vs yday $9,957.25 (-370.74) | 09:30 open · cash $52.80 (unchanged overnight, no fees) · equity $9,586.51 vs prior close $9,957.25 (-370.74) · 8 name(s) re-marked at the open (per-name table). LPTH×83 yday $14.80 → 09:30 $14.01 -65.57; AAOI×8 yday $154.89 → 09:30 $146.20 -69.52; ABX×137 yday $9.12 → 09:30 $9.03 -12.33; ALOY×85 yday $13.86 → 09:30 $13.19 -56.53; BORR×273 yday $4.50 → 09:30 $4.56 +16.38; DIOD×12 yday $107.56 → 09:30 $103.01 -54.60; INDI×269 yday $4.71 → 09:30 $4.48 -61.87; KOPN×230 yday $5.32 → 09:30 $5.03 -66.70 | — |
| 2026-08-18 09:30 ET | **SELL** | `LPTH` | 83 | $14.01 | $2.26 | $-81.69 | $1,213.37 | ▼ -81.69 after sell → book $9,584.25; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `AAOI` | 8 | $146.20 | $2.03 | $-55.57 | $2,380.94 | ▼ -55.57 after sell → book $9,582.22; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 137 | $9.03 | $2.43 | $-17.16 | $3,615.61 | ▼ -17.16 after sell → book $9,579.78; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 85 | $13.19 | $2.27 | $-129.46 | $4,734.49 | ▼ -129.46 after sell → book $9,577.51; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `BORR` | 273 | $4.56 | $3.58 | $-15.29 | $5,975.80 | ▼ -15.29 after sell → book $9,573.94; vs 09:30 mark -3.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DIOD` | 12 | $103.01 | $2.05 | $-15.95 | $7,209.87 | ▼ -15.95 after sell → book $9,571.89; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `INDI` | 269 | $4.48 | $3.52 | $-52.72 | $8,411.47 | ▼ -52.72 after sell → book $9,568.37; vs 09:30 mark -3.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `KOPN` | 230 | $5.03 | $3.02 | $-97.98 | $9,565.35 | ▼ -97.98 after sell → book $9,565.35; vs 09:30 mark -3.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,565.35 | ▲ close $9,565.35 vs 09:30 $9,586.51 (session +0.00) | 16:00 close · cash $9,565.35 · no lots left · equity $9,565.35. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,565.35 | ▲ 09:30 equity $9,565.35 vs yday $9,565.35 (+0.00) | 09:30 open · cash $9,565.35 · no holdings · equity $9,565.35 vs prior close $9,565.35 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,565.35 | ▲ close $9,565.35 vs 09:30 $9,565.35 (session +0.00) | 16:00 close · cash $9,565.35 · no lots left · equity $9,565.35. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,565.35 | ▲ 09:30 equity $9,565.35 vs yday $9,565.35 (+0.00) | 09:30 open · cash $9,565.35 · no holdings · equity $9,565.35 vs prior close $9,565.35 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 58 | $20.55 | $2.16 | — | $8,371.29 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1195.67 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,186.13 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1195.67 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 57 | $20.65 | $2.16 | — | $6,006.92 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1195.67 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 207 | $5.77 | $2.67 | — | $4,809.86 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1195.67 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 60 | $19.63 | $2.17 | — | $3,629.89 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1195.67 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 40 | $29.63 | $2.11 | — | $2,442.58 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1195.67 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 683 | $1.75 | $8.81 | — | $1,238.52 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1195.67 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $80.18 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy,oppset; 🔵; ⚪; ret5=+9.2; leftover $1195.67 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $80.18 | ▲ close $9,767.10 vs 09:30 $9,565.35 (session +225.88) | 16:00 close · cash $80.18 · equity $9,767.10 vs 09:30 $9,565.35 (+201.75; session marks +225.88) · 8 name(s) marked open→close (per-name table). AG×58 09:30 $20.55 → close $21.19 +37.12; BHP×13 09:30 $91.01 → close $93.63 +34.06; CDE×57 09:30 $20.65 → close $21.11 +26.22; HDSN×207 09:30 $5.77 → close $5.57 -41.40; IAG×60 09:30 $19.63 → close $20.50 +52.20; KGC×40 09:30 $29.63 → close $31.43 +72.00; NFGC×683 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $80.18 | ▲ 09:30 equity $10,025.35 vs yday $9,767.10 (+258.25) | 09:30 open · cash $80.18 (unchanged overnight, no fees) · equity $10,025.35 vs prior close $9,767.10 (+258.25) · 8 name(s) re-marked at the open (per-name table). AG×58 yday $21.19 → 09:30 $21.90 +41.18; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×57 yday $21.11 → 09:30 $21.75 +36.48; HDSN×207 yday $5.57 → 09:30 $5.67 +20.70; IAG×60 yday $20.50 → 09:30 $21.17 +40.20; KGC×40 yday $31.43 → 09:30 $32.17 +29.60; NFGC×683 yday $1.75 → 09:30 $1.79 +27.32; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 58 | $21.90 | $2.18 | $+73.95 | $1,348.20 | ▲ +73.95 after sell → book $10,023.17; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,590.51 | ▲ +57.15 after sell → book $10,021.12; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 57 | $21.75 | $2.18 | $+58.36 | $3,828.08 | ▲ +58.36 after sell → book $10,018.94; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 207 | $5.67 | $2.71 | $-26.08 | $4,999.05 | ▼ -26.08 after sell → book $10,016.22; vs 09:30 mark -2.72 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 60 | $21.17 | $2.19 | $+88.04 | $6,267.06 | ▲ +88.04 after sell → book $10,014.03; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 40 | $32.17 | $2.13 | $+97.36 | $7,551.73 | ▲ +97.36 after sell → book $10,011.90; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 683 | $1.79 | $8.93 | $+9.58 | $8,765.37 | ▲ +9.58 after sell → book $10,002.97; vs 09:30 mark -8.93 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $10,000.93 | ▲ +77.23 after sell → book $10,000.93; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $8,804.61 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1250.12 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 72 | $17.20 | $2.21 | — | $7,564.01 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1250.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $6,480.50 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1250.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 112 | $11.13 | $2.33 | — | $5,231.62 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,yday_gainer,mover_buy,oppset; 🔵; ⚪; ret5=+39.8; leftover $1250.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 947 | $1.32 | $12.22 | — | $3,969.36 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1250.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 10 | $115.18 | $2.02 | — | $2,815.54 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1250.12 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 15 | $78.88 | $2.04 | — | $1,630.31 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1250.12 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ILMN` | 5 | $212.40 | $2.00 | — | $566.30 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+10.7; leftover $1250.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $566.30 | ▲ close $10,435.24 vs 09:30 $10,025.35 (session +461.14) | 16:00 close · cash $566.30 · equity $10,435.24 vs 09:30 $10,025.35 (+409.89; session marks +461.14) · 8 name(s) marked open→close (per-name table). AU×10 09:30 $119.43 → close $121.22 +17.90; AUPH×72 09:30 $17.20 → close $16.65 -39.60; AEM×5 09:30 $216.30 → close $216.06 -1.20; ARCT×112 09:30 $11.13 → close $13.45 +259.84; CYPH×947 09:30 $1.32 → close $1.42 +94.70; FUTU×10 09:30 $115.18 → close $123.64 +84.60; GRAL×15 09:30 $78.88 → close $79.54 +9.90; ILMN×5 09:30 $212.40 → close $219.40 +35.00 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $566.30 | ▲ 09:30 equity $10,793.51 vs yday $10,435.24 (+358.27) | 09:30 open · cash $566.30 (unchanged overnight, no fees) · equity $10,793.51 vs prior close $10,435.24 (+358.27) · 8 name(s) re-marked at the open (per-name table). AU×10 yday $121.22 → 09:30 $120.51 -7.10; AUPH×72 yday $16.65 → 09:30 $16.57 -5.76; AEM×5 yday $216.06 → 09:30 $217.03 +4.85; ARCT×112 yday $13.45 → 09:30 $13.33 -13.44; CYPH×947 yday $1.42 → 09:30 $1.83 +388.27; FUTU×10 yday $123.64 → 09:30 $121.00 -26.40; GRAL×15 yday $79.54 → 09:30 $81.87 +34.95; ILMN×5 yday $219.40 → 09:30 $215.98 -17.10 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,769.36 | ▲ +6.74 after sell → book $10,791.47; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 72 | $16.57 | $2.23 | $-49.79 | $2,960.17 | ▼ -49.79 after sell → book $10,789.24; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $4,043.30 | ▼ -0.38 after sell → book $10,787.22; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 112 | $13.33 | $2.36 | $+241.72 | $5,533.90 | ▲ +241.72 after sell → book $10,784.86; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 947 | $1.83 | $12.39 | $+458.37 | $7,254.52 | ▲ +458.37 after sell → book $10,772.47; vs 09:30 mark -12.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 10 | $121.00 | $2.04 | $+54.14 | $8,462.48 | ▲ +54.14 after sell → book $10,770.43; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 15 | $81.87 | $2.06 | $+40.76 | $9,688.48 | ▲ +40.76 after sell → book $10,768.38; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ILMN` | 5 | $215.98 | $2.02 | $+13.87 | $10,766.35 | ▲ +13.87 after sell → book $10,766.35; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,766.35 | ▲ close $10,766.35 vs 09:30 $10,793.51 (session +0.00) | 16:00 close · cash $10,766.35 · no lots left · equity $10,766.35. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,766.35 | ▲ 09:30 equity $10,766.35 vs yday $10,766.35 (+0.00) | 09:30 open · cash $10,766.35 · no holdings · equity $10,766.35 vs prior close $10,766.35 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 17 | $77.13 | $2.04 | — | $9,453.10 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $1345.79 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 30 | $43.76 | $2.08 | — | $8,138.22 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+6.2; leftover $1345.79 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUZ` | 149 | $8.98 | $2.44 | — | $6,797.77 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot,mover_buy,oppset; ⚪; ret5=+15.4; leftover $1345.79 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `VALE` | 89 | $15.01 | $2.26 | — | $5,459.62 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; ⚪; ret5=+9.4; leftover $1345.79 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `WPM` | 8 | $156.51 | $2.01 | — | $4,205.53 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot,mover_buy; ⚪; ret5=+17.4; leftover $1345.79 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AVAH` | 98 | $13.62 | $2.28 | — | $2,867.99 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; leftover $1345.79 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 825 | $1.63 | $10.64 | — | $1,512.60 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1345.79 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 862 | $1.56 | $11.12 | — | $156.76 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $1345.79 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $156.76 | ▲ close $11,054.58 vs 09:30 $10,766.35 (session +323.10) | 16:00 close · cash $156.76 · equity $11,054.58 vs 09:30 $10,766.35 (+288.23; session marks +323.10) · 8 name(s) marked open→close (per-name table). FCX×17 09:30 $77.13 → close $79.91 +47.26; RHI×30 09:30 $43.76 → close $44.90 +34.20; SUZ×149 09:30 $8.98 → close $9.03 +7.45; VALE×89 09:30 $15.01 → close $15.33 +28.48; WPM×8 09:30 $156.51 → close $163.72 +57.68; AVAH×98 09:30 $13.62 → close $13.59 -3.43; BMEA×825 09:30 $1.63 → close $1.73 +82.50; CYPH×862 09:30 $1.56 → close $1.64 +68.96 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $156.76 | ▼ 09:30 equity $11,001.05 vs yday $11,054.58 (-53.53) | 09:30 open · cash $156.76 (unchanged overnight, no fees) · equity $11,001.05 vs prior close $11,054.58 (-53.53) · 8 name(s) re-marked at the open (per-name table). FCX×17 yday $79.91 → 09:30 $79.34 -9.69; RHI×30 yday $44.90 → 09:30 $44.33 -17.10; SUZ×149 yday $9.03 → 09:30 $9.03 +0.00; VALE×89 yday $15.33 → 09:30 $15.37 +3.56; WPM×8 yday $163.72 → 09:30 $160.93 -22.32; AVAH×98 yday $13.59 → 09:30 $13.65 +5.88; BMEA×825 yday $1.73 → 09:30 $1.75 +20.62; CYPH×862 yday $1.64 → 09:30 $1.60 -34.48 | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 17 | $79.34 | $2.06 | $+33.47 | $1,503.48 | ▲ +33.47 after sell → book $10,998.99; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RHI` | 30 | $44.33 | $2.10 | $+12.92 | $2,831.28 | ▲ +12.92 after sell → book $10,996.89; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SUZ` | 149 | $9.03 | $2.47 | $+2.54 | $4,174.27 | ▲ +2.54 after sell → book $10,994.42; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `VALE` | 89 | $15.37 | $2.28 | $+27.50 | $5,539.92 | ▲ +27.50 after sell → book $10,992.14; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `WPM` | 8 | $160.93 | $2.03 | $+31.31 | $6,825.33 | ▲ +31.31 after sell → book $10,990.10; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AVAH` | 98 | $13.65 | $2.31 | $-2.14 | $8,160.72 | ▼ -2.14 after sell → book $10,987.79; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 825 | $1.75 | $10.79 | $+81.69 | $9,597.80 | ▲ +81.69 after sell → book $10,977.00; vs 09:30 mark -10.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 862 | $1.60 | $11.27 | $+12.09 | $10,965.73 | ▲ +12.09 after sell → book $10,965.73; vs 09:30 mark -11.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,965.73 | ▲ close $10,965.73 vs 09:30 $11,001.05 (session +0.00) | 16:00 close · cash $10,965.73 · no lots left · equity $10,965.73. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,965.73 | ▲ 09:30 equity $10,965.73 vs yday $10,965.73 (-0.00) | 09:30 open · cash $10,965.73 · no holdings · equity $10,965.73 vs prior close $10,965.73 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,965.73 | ▲ close $10,965.73 vs 09:30 $10,965.73 (session +0.00) | 16:00 close · cash $10,965.73 · no lots left · equity $10,965.73. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,965.73 | ▲ 09:30 equity $10,965.73 vs yday $10,965.73 (-0.00) | 09:30 open · cash $10,965.73 · no holdings · equity $10,965.73 vs prior close $10,965.73 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $9,666.08 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1370.72 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $8,388.23 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover,mover_buy,oppset; 🔵; ⚪; ret5=+14.1; leftover $1370.72 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 14 | $91.49 | $2.03 | — | $7,105.34 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $1370.72 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 21 | $62.82 | $2.05 | — | $5,784.06 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+5.4; leftover $1370.72 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 4 | $289.44 | $2.00 | — | $4,624.30 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+1.8; leftover $1370.72 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 11 | $119.76 | $2.02 | — | $3,304.92 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.5; leftover $1370.72 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MEI` | 77 | $17.78 | $2.22 | — | $1,933.64 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+22.9; leftover $1370.72 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MTSI` | 4 | $275.20 | $2.00 | — | $830.83 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+4.1; leftover $1370.72 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $830.83 | ▼ close $10,640.17 vs 09:30 $10,965.73 (session -309.20) | 16:00 close · cash $830.83 · equity $10,640.17 vs 09:30 $10,965.73 (-325.56; session marks -309.20) · 8 name(s) marked open→close (per-name table). KEYS×4 09:30 $324.41 → close $319.97 -17.76; SMTC×9 09:30 $141.76 → close $131.17 -95.31; AVT×14 09:30 $91.49 → close $88.63 -40.04; CGNX×21 09:30 $62.82 → close $60.46 -49.56; COHR×4 09:30 $289.44 → close $279.20 -40.96; LSCC×11 09:30 $119.76 → close $114.40 -58.96; MEI×77 09:30 $17.78 → close $18.21 +33.11; MTSI×4 09:30 $275.20 → close $265.27 -39.72 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $830.83 | ▲ 09:30 equity $10,690.16 vs yday $10,640.17 (+49.99) | 09:30 open · cash $830.83 (unchanged overnight, no fees) · equity $10,690.16 vs prior close $10,640.17 (+49.99) · 8 name(s) re-marked at the open (per-name table). KEYS×4 yday $319.97 → 09:30 $322.49 +10.08; SMTC×9 yday $131.17 → 09:30 $132.30 +10.17; AVT×14 yday $88.63 → 09:30 $89.39 +10.64; CGNX×21 yday $60.46 → 09:30 $60.46 +0.00; COHR×4 yday $279.20 → 09:30 $280.25 +4.20; LSCC×11 yday $114.40 → 09:30 $115.56 +12.76; MEI×77 yday $18.21 → 09:30 $18.15 -4.62; MTSI×4 yday $265.27 → 09:30 $266.96 +6.76 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 4 | $322.49 | $2.02 | $-11.70 | $2,118.77 | ▼ -11.70 after sell → book $10,688.14; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $3,307.44 | ▼ -89.19 after sell → book $10,686.11; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVT` | 14 | $89.39 | $2.05 | $-33.48 | $4,556.84 | ▼ -33.48 after sell → book $10,684.05; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CGNX` | 21 | $60.46 | $2.07 | $-53.69 | $5,824.43 | ▼ -53.69 after sell → book $10,681.98; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `COHR` | 4 | $280.25 | $2.02 | $-40.78 | $6,943.41 | ▼ -40.78 after sell → book $10,679.96; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LSCC` | 11 | $115.56 | $2.04 | $-50.27 | $8,212.52 | ▼ -50.27 after sell → book $10,677.91; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MEI` | 77 | $18.15 | $2.25 | $+24.02 | $9,607.83 | ▲ +24.02 after sell → book $10,675.67; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MTSI` | 4 | $266.96 | $2.02 | $-36.98 | $10,673.65 | ▼ -36.98 after sell → book $10,673.65; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,673.65 | ▲ close $10,673.65 vs 09:30 $10,690.16 (session +0.00) | 16:00 close · cash $10,673.65 · no lots left · equity $10,673.65. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,673.65 | ▲ 09:30 equity $10,673.65 vs yday $10,673.65 (-0.00) | 09:30 open · cash $10,673.65 · no holdings · equity $10,673.65 vs prior close $10,673.65 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,673.65 | ▲ close $10,673.65 vs 09:30 $10,673.65 (session +0.00) | 16:00 close · cash $10,673.65 · no lots left · equity $10,673.65. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,673.65 | ▲ 09:30 equity $10,673.65 vs yday $10,673.65 (-0.00) | 09:30 open · cash $10,673.65 · no holdings · equity $10,673.65 vs prior close $10,673.65 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,673.65 | ▲ close $10,673.65 vs 09:30 $10,673.65 (session +0.00) | 16:00 close · cash $10,673.65 · no lots left · equity $10,673.65. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,673.65 | ▲ 09:30 equity $10,673.65 vs yday $10,673.65 (-0.00) | 09:30 open · cash $10,673.65 · no holdings · equity $10,673.65 vs prior close $10,673.65 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 79 | $16.77 | $2.23 | — | $9,346.59 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy,oppset; 🔵; ⚪; ret5=+5.7; leftover $1334.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 691 | $1.93 | $8.91 | — | $8,004.05 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1334.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 612 | $2.18 | $7.89 | — | $6,661.99 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1334.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 31 | $42.93 | $2.08 | — | $5,329.08 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1334.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 128 | $10.42 | $2.37 | — | $3,992.95 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; leftover $1334.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PBH` | 24 | $53.45 | $2.06 | — | $2,708.08 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+1.3; leftover $1334.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `PCRX` | 49 | $26.74 | $2.14 | — | $1,395.69 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.6; leftover $1334.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $69.17 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1334.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $69.17 | ▼ close $10,432.46 vs 09:30 $10,673.65 (session -211.48) | 16:00 close · cash $69.17 · equity $10,432.46 vs 09:30 $10,673.65 (-241.19; session marks -211.48) · 8 name(s) marked open→close (per-name table). ARCT×79 09:30 $16.77 → close $15.56 -95.59; BMEA×691 09:30 $1.93 → close $1.91 -13.82; CRDL×612 09:30 $2.18 → close $2.16 -12.24; HRMY×31 09:30 $42.93 → close $41.86 -33.17; NVAX×128 09:30 $10.42 → close $10.34 -10.24; PBH×24 09:30 $53.45 → close $52.56 -21.36; PCRX×49 09:30 $26.74 → close $26.60 -6.86; RVTY×10 09:30 $132.45 → close $130.63 -18.20 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $69.17 | ▼ 09:30 equity $10,403.80 vs yday $10,432.46 (-28.66) | 09:30 open · cash $69.17 (unchanged overnight, no fees) · equity $10,403.80 vs prior close $10,432.46 (-28.66) · 8 name(s) re-marked at the open (per-name table). ARCT×79 yday $15.56 → 09:30 $15.61 +3.95; BMEA×691 yday $1.91 → 09:30 $1.90 -6.91; CRDL×612 yday $2.16 → 09:30 $2.16 +0.00; HRMY×31 yday $41.86 → 09:30 $41.50 -11.16; NVAX×128 yday $10.34 → 09:30 $10.50 +20.48; PBH×24 yday $52.56 → 09:30 $51.80 -18.24; PCRX×49 yday $26.60 → 09:30 $26.38 -10.78; RVTY×10 yday $130.63 → 09:30 $130.03 -6.00 | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 79 | $15.61 | $2.25 | $-96.12 | $1,300.11 | ▼ -96.12 after sell → book $10,401.55; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `BMEA` | 691 | $1.90 | $9.04 | $-38.68 | $2,603.97 | ▼ -38.68 after sell → book $10,392.51; vs 09:30 mark -9.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 612 | $2.16 | $8.01 | $-28.14 | $3,917.88 | ▼ -28.14 after sell → book $10,384.50; vs 09:30 mark -8.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 31 | $41.50 | $2.10 | $-48.52 | $5,202.28 | ▼ -48.52 after sell → book $10,382.40; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 128 | $10.50 | $2.41 | $+5.46 | $6,543.87 | ▲ +5.46 after sell → book $10,379.99; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PBH` | 24 | $51.80 | $2.08 | $-43.74 | $7,784.99 | ▼ -43.74 after sell → book $10,377.91; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PCRX` | 49 | $26.38 | $2.16 | $-21.93 | $9,075.45 | ▼ -21.93 after sell → book $10,375.75; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $130.03 | $2.04 | $-28.26 | $10,373.71 | ▼ -28.26 after sell → book $10,373.71; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 4 | $263.36 | $2.00 | — | $9,318.27 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1296.71 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 15 | $82.70 | $2.04 | — | $8,075.73 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+15.7; leftover $1296.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $513.78 | $2.00 | — | $7,046.18 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy,oppset; 🔵; ⚪; ret5=+9.3; leftover $1296.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `FRNM` | 79 | $16.40 | $2.23 | — | $5,748.35 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.2; leftover $1296.71 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 286 | $4.53 | $3.69 | — | $4,449.08 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.5; leftover $1296.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `LENZ` | 225 | $5.75 | $2.90 | — | $3,152.43 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+21.2; leftover $1296.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MMED` | 54 | $23.84 | $2.15 | — | $1,862.92 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list oppset; 🔵; ⚪; ret5=+22.2; leftover $1296.71 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `PIPR` | 16 | $76.55 | $2.04 | — | $636.08 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+4.2; leftover $1296.71 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $636.08 | ▲ close $10,538.39 vs 09:30 $10,403.80 (session +183.72) | 16:00 close · cash $636.08 · equity $10,538.39 vs 09:30 $10,403.80 (+134.59; session marks +183.72) · 8 name(s) marked open→close (per-name table). CRM×4 09:30 $263.36 → close $259.23 -16.52; TARS×15 09:30 $82.70 → close $90.78 +121.20; DELL×2 09:30 $513.78 → close $524.14 +20.72; FRNM×79 09:30 $16.40 → close $16.31 -7.11; IRD×286 09:30 $4.53 → close $4.67 +40.04; LENZ×225 09:30 $5.75 → close $5.96 +47.25; MMED×54 09:30 $23.84 → close $23.29 -29.70; PIPR×16 09:30 $76.55 → close $77.04 +7.84 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $636.08 | ▼ 09:30 equity $10,471.98 vs yday $10,538.39 (-66.41) | 09:30 open · cash $636.08 (unchanged overnight, no fees) · equity $10,471.98 vs prior close $10,538.39 (-66.41) · 8 name(s) re-marked at the open (per-name table). CRM×4 yday $259.23 → 09:30 $253.72 -22.04; TARS×15 yday $90.78 → 09:30 $89.67 -16.65; DELL×2 yday $524.14 → 09:30 $521.15 -5.98; FRNM×79 yday $16.31 → 09:30 $16.74 +33.97; IRD×286 yday $4.67 → 09:30 $4.53 -40.04; LENZ×225 yday $5.96 → 09:30 $5.95 -2.25; MMED×54 yday $23.29 → 09:30 $23.16 -7.02; PIPR×16 yday $77.04 → 09:30 $76.64 -6.40 | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 4 | $253.72 | $2.02 | $-42.58 | $1,648.94 | ▼ -42.58 after sell → book $10,469.96; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TARS` | 15 | $89.67 | $2.06 | $+100.46 | $2,991.93 | ▲ +100.46 after sell → book $10,467.90; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 2 | $521.15 | $2.02 | $+10.73 | $4,032.22 | ▲ +10.73 after sell → book $10,465.89; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 79 | $16.74 | $2.25 | $+22.38 | $5,352.42 | ▲ +22.38 after sell → book $10,463.63; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `IRD` | 286 | $4.53 | $3.75 | $-7.44 | $6,644.26 | ▼ -7.44 after sell → book $10,459.89; vs 09:30 mark -3.74 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LENZ` | 225 | $5.95 | $2.95 | $+39.15 | $7,980.06 | ▲ +39.15 after sell → book $10,456.94; vs 09:30 mark -2.95 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `MMED` | 54 | $23.16 | $2.17 | $-41.04 | $9,228.52 | ▼ -41.04 after sell → book $10,454.76; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `PIPR` | 16 | $76.64 | $2.06 | $-2.66 | $10,452.71 | ▼ -2.66 after sell → book $10,452.71; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,452.71 | ▲ close $10,452.71 vs 09:30 $10,471.98 (session +0.00) | 16:00 close · cash $10,452.71 · no lots left · equity $10,452.71. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,452.71 | ▲ 09:30 equity $10,452.71 vs yday $10,452.71 (-0.00) | 09:30 open · cash $10,452.71 · no holdings · equity $10,452.71 vs prior close $10,452.71 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,452.71 | ▲ close $10,452.71 vs 09:30 $10,452.71 (session +0.00) | 16:00 close · cash $10,452.71 · no lots left · equity $10,452.71. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,452.71 | ▲ 09:30 equity $10,452.71 vs yday $10,452.71 (-0.00) | 09:30 open · cash $10,452.71 · no holdings · equity $10,452.71 vs prior close $10,452.71 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,452.71 | ▲ close $10,452.71 vs 09:30 $10,452.71 (session +0.00) | 16:00 close · cash $10,452.71 · no lots left · equity $10,452.71. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,452.71 | ▲ 09:30 equity $10,452.71 vs yday $10,452.71 (-0.00) | 09:30 open · cash $10,452.71 · no holdings · equity $10,452.71 vs prior close $10,452.71 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `SEDG` | 56 | $36.78 | $2.16 | — | $8,390.87 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list oppset; 🔵; ⚪; ret5=+8.2; leftover $2090.54 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 39 | $52.55 | $2.11 | — | $6,339.31 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $2090.54 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `PAGS` | 206 | $10.11 | $2.66 | — | $4,253.99 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+2.9; leftover $2090.54 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `PAYP` | 114 | $18.30 | $2.33 | — | $2,165.46 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $2090.54 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ZSQR` | 643 | $3.25 | $8.29 | — | $67.42 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+3.6; leftover $2090.54 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟡 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $67.42 | ▼ close $10,389.46 vs 09:30 $10,452.71 (session -45.70) | 16:00 close · cash $67.42 · equity $10,389.46 vs 09:30 $10,452.71 (-63.25; session marks -45.70) · 5 name(s) marked open→close (per-name table). SEDG×56 09:30 $36.78 → close $34.68 -117.60; BAND×39 09:30 $52.55 → close $56.87 +168.48; PAGS×206 09:30 $10.11 → close $10.12 +2.06; PAYP×114 09:30 $18.30 → close $18.45 +17.10; ZSQR×643 09:30 $3.25 → close $3.07 -115.74 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $67.42 | ▼ 09:30 equity $10,281.86 vs yday $10,389.46 (-107.60) | 09:30 open · cash $67.42 (unchanged overnight, no fees) · equity $10,281.86 vs prior close $10,389.46 (-107.60) · 5 name(s) re-marked at the open (per-name table). SEDG×56 yday $34.68 → 09:30 $33.64 -58.24; BAND×39 yday $56.87 → 09:30 $56.90 +1.17; PAGS×206 yday $10.12 → 09:30 $10.00 -24.72; PAYP×114 yday $18.45 → 09:30 $18.28 -19.38; ZSQR×643 yday $3.07 → 09:30 $3.06 -6.43 | — |
| 2026-09-14 09:30 ET | **SELL** | `SEDG` | 56 | $33.64 | $2.18 | $-180.18 | $1,949.07 | ▼ -180.18 after sell → book $10,279.67; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAND` | 39 | $56.90 | $2.13 | $+165.41 | $4,166.04 | ▲ +165.41 after sell → book $10,277.54; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `PAGS` | 206 | $10.00 | $2.71 | $-28.03 | $6,223.33 | ▼ -28.03 after sell → book $10,274.83; vs 09:30 mark -2.71 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `PAYP` | 114 | $18.28 | $2.37 | $-6.98 | $8,304.88 | ▼ -6.98 after sell → book $10,272.46; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ZSQR` | 643 | $3.06 | $8.42 | $-138.88 | $10,264.05 | ▼ -138.88 after sell → book $10,264.05; vs 09:30 mark -8.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,264.05 | ▲ close $10,264.05 vs 09:30 $10,281.86 (session +0.00) | 16:00 close · cash $10,264.05 · no lots left · equity $10,264.05. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,264.05 | ▲ 09:30 equity $10,264.05 vs yday $10,264.05 (-0.00) | 09:30 open · cash $10,264.05 · no holdings · equity $10,264.05 vs prior close $10,264.05 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,264.05 | ▲ close $10,264.05 vs 09:30 $10,264.05 (session +0.00) | 16:00 close · cash $10,264.05 · no lots left · equity $10,264.05. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,264.05 | ▲ 09:30 equity $10,264.05 vs yday $10,264.05 (-0.00) | 09:30 open · cash $10,264.05 · no holdings · equity $10,264.05 vs prior close $10,264.05 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `QRVO` | 43 | $118.18 | $2.12 | — | $5,180.19 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $5132.02 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 57 | $89.38 | $2.16 | — | $83.37 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $5132.02 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $83.37 | ▼ close $9,862.71 vs 09:30 $10,264.05 (session -397.06) | 16:00 close · cash $83.37 · equity $9,862.71 vs 09:30 $10,264.05 (-401.34; session marks -397.06) · 2 name(s) marked open→close (per-name table). QRVO×43 09:30 $118.18 → close $113.97 -181.03; SWKS×57 09:30 $89.38 → close $85.59 -216.03 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $83.37 | ▲ 09:30 equity $9,969.39 vs yday $9,862.71 (+106.68) | 09:30 open · cash $83.37 (unchanged overnight, no fees) · equity $9,969.39 vs prior close $9,862.71 (+106.68) · 2 name(s) re-marked at the open (per-name table). QRVO×43 yday $113.97 → 09:30 $114.90 +39.99; SWKS×57 yday $85.59 → 09:30 $86.76 +66.69 | — |
| 2026-09-17 09:30 ET | **SELL** | `QRVO` | 43 | $114.90 | $2.17 | $-145.33 | $5,021.90 | ▼ -145.33 after sell → book $9,967.22; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 57 | $86.76 | $2.21 | $-153.71 | $9,965.01 | ▼ -153.71 after sell → book $9,965.01; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ASAN` | 521 | $9.55 | $6.72 | — | $4,982.74 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list oppset; 🔵; ⚪; ret5=+17.0; leftover $4982.50 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `VOD` | 283 | $17.56 | $3.65 | — | $9.61 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+7.9; leftover $4982.50 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.61 | ▲ close $10,224.66 vs 09:30 $9,969.39 (session +270.02) | 16:00 close · cash $9.61 · equity $10,224.66 vs 09:30 $9,969.39 (+255.27; session marks +270.02) · 2 name(s) marked open→close (per-name table). ASAN×521 09:30 $9.55 → close $10.09 +281.34; VOD×283 09:30 $17.56 → close $17.52 -11.32 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.61 | ▼ 09:30 equity $10,001.09 vs yday $10,224.66 (-223.57) | 09:30 open · cash $9.61 (unchanged overnight, no fees) · equity $10,001.09 vs prior close $10,224.66 (-223.57) · 2 name(s) re-marked at the open (per-name table). ASAN×521 yday $10.09 → 09:30 $10.09 +0.00; VOD×283 yday $17.52 → 09:30 $16.73 -223.57 | — |
| 2026-09-18 09:30 ET | **SELL** | `VOD` | 283 | $16.73 | $3.74 | $-242.28 | $4,740.46 | ▼ -242.28 after sell → book $9,997.35; vs 09:30 mark -3.74 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `S` | 29 | $23.13 | $2.08 | — | $4,067.61 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+17.1; leftover $677.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `ATRC` | 11 | $58.51 | $2.02 | — | $3,421.98 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.6; leftover $677.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-18 09:30 ET | **BUY** | `ILMN` | 2 | $249.13 | $2.00 | — | $2,921.72 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten; 🔵; ⚪; ret5=+21.8; leftover $677.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-18 09:30 ET | **BUY** | `PGEN` | 84 | $7.98 | $2.24 | — | $2,249.16 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+17.8; leftover $677.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-18 09:30 ET | **BUY** | `RXT` | 171 | $3.94 | $2.50 | — | $1,572.92 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+25.2; leftover $677.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 32 | $20.91 | $2.09 | — | $901.71 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $677.21 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `ARQT` | 25 | $26.14 | $2.06 | — | $246.15 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,ohlc_hot,oppset; 🔵; ⚪; ret5=+13.2; leftover $677.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $246.15 | ▼ close $9,587.89 vs 09:30 $10,001.09 (session -394.47) | 16:00 close · cash $246.15 · equity $9,587.89 vs 09:30 $10,001.09 (-413.20; session marks -394.47) · 8 name(s) marked open→close (per-name table). ASAN×521 09:30 $10.09 → close $9.51 -302.18; S×29 09:30 $23.13 → close $22.51 -17.98; ATRC×11 09:30 $58.51 → close $58.10 -4.51; ILMN×2 09:30 $249.13 → close $239.62 -19.02; PGEN×84 09:30 $7.98 → close $7.78 -16.80; RXT×171 09:30 $3.94 → close $3.80 -23.94; TH×32 09:30 $20.91 → close $21.19 +8.96; ARQT×25 09:30 $26.14 → close $25.38 -19.00 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1281.39 < 1 share @ 1646.93 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ASAN` | 521 | 2026-09-17 @ $9.55 | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list oppset; 🔵; ⚪; ret5=+17.0; leftover $4982.50 |
| `S` | 29 | 2026-09-18 @ $23.13 | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+17.1; leftover $677.21 |
| `ATRC` | 11 | 2026-09-18 @ $58.51 | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.6; leftover $677.21 |
| `ILMN` | 2 | 2026-09-18 @ $249.13 | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten; 🔵; ⚪; ret5=+21.8; leftover $677.21 |
| `PGEN` | 84 | 2026-09-18 @ $7.98 | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+17.8; leftover $677.21 |
| `RXT` | 171 | 2026-09-18 @ $3.94 | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+25.2; leftover $677.21 |
| `TH` | 32 | 2026-09-18 @ $20.91 | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $677.21 |
| `ARQT` | 25 | 2026-09-18 @ $26.14 | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,ohlc_hot,oppset; 🔵; ⚪; ret5=+13.2; leftover $677.21 |
