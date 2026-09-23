# Factor mine action — `union_white_yday_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · union looker: 0 red cameras + yesterday up, rank +G−R

Cash book **-3.37%** ($9,663) · signal-only (no cash/fees) was +0.86%. Starts YES **3/29**. Fills 170 · skips 1 · realized $-93.82.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $229.96.

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
| 2026-08-14 | `ZENA` | 582 | — | $2.20 | +0.00 | $2.14 | -34.92 | -34.92 | +0.00 | -34.92 |
| 2026-08-14 | `ADUR` | 77 | — | $16.50 | +0.00 | $16.17 | -25.41 | -25.41 | +0.00 | -25.41 |
| 2026-08-17 | `BRUN` | 48 | $22.93 | $23.00 | +3.36 | — | +0.00 | +3.36 | -155.76 | — |
| 2026-08-17 | `HLIT` | 97 | $13.92 | $13.84 | -7.76 | — | +0.00 | -7.76 | +64.02 | — |
| 2026-08-17 | `MNTN` | 102 | $12.52 | $12.40 | -12.24 | — | +0.00 | -12.24 | -10.20 | — |
| 2026-08-17 | `QMCO` | 51 | $26.11 | $24.83 | -65.28 | — | +0.00 | -65.28 | +7.65 | — |
| 2026-08-17 | `QMLS` | 175 | $7.32 | $7.24 | -14.00 | — | +0.00 | -14.00 | -8.75 | — |
| 2026-08-17 | `ZENA` | 582 | $2.14 | $2.08 | -32.01 | — | +0.00 | -32.01 | -66.93 | — |
| 2026-08-17 | `ADUR` | 77 | $16.17 | $15.73 | -33.88 | — | +0.00 | -33.88 | -59.29 | — |
| 2026-08-17 | `LPTH` | 83 | — | $14.94 | +0.00 | $14.80 | -11.62 | -11.62 | +0.00 | -11.62 |
| 2026-08-17 | `AAOI` | 8 | — | $152.64 | +0.00 | $154.89 | +18.00 | +18.00 | +0.00 | +18.00 |
| 2026-08-17 | `ABX` | 136 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `ALOY` | 85 | — | $14.66 | +0.00 | $13.86 | -68.42 | -68.42 | +0.00 | -68.42 |
| 2026-08-17 | `BORR` | 271 | — | $4.59 | +0.00 | $4.50 | -24.39 | -24.39 | +0.00 | -24.39 |
| 2026-08-17 | `INDI` | 268 | — | $4.65 | +0.00 | $4.71 | +16.08 | +16.08 | +0.00 | +16.08 |
| 2026-08-17 | `KOPN` | 229 | — | $5.43 | +0.00 | $5.32 | -25.19 | -25.19 | +0.00 | -25.19 |
| 2026-08-17 | `MP` | 21 | — | $58.01 | +0.00 | $58.51 | +10.50 | +10.50 | +0.00 | +10.50 |
| 2026-08-18 | `LPTH` | 83 | $14.80 | $14.01 | -65.57 | — | +0.00 | -65.57 | -77.19 | — |
| 2026-08-18 | `AAOI` | 8 | $154.89 | $146.20 | -69.52 | — | +0.00 | -69.52 | -51.52 | — |
| 2026-08-18 | `ABX` | 136 | $9.12 | $9.03 | -12.24 | — | +0.00 | -12.24 | -12.24 | — |
| 2026-08-18 | `ALOY` | 85 | $13.86 | $13.19 | -56.53 | — | +0.00 | -56.53 | -124.95 | — |
| 2026-08-18 | `BORR` | 271 | $4.50 | $4.56 | +16.26 | — | +0.00 | +16.26 | -8.13 | — |
| 2026-08-18 | `INDI` | 268 | $4.71 | $4.48 | -61.64 | — | +0.00 | -61.64 | -45.56 | — |
| 2026-08-18 | `KOPN` | 229 | $5.32 | $5.03 | -66.41 | — | +0.00 | -66.41 | -91.60 | — |
| 2026-08-18 | `MP` | 21 | $58.51 | $56.35 | -45.36 | — | +0.00 | -45.36 | -34.86 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 57 | — | $20.55 | +0.00 | $21.19 | +36.48 | +36.48 | +0.00 | +36.48 |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `CDE` | 57 | — | $20.65 | +0.00 | $21.11 | +26.22 | +26.22 | +0.00 | +26.22 |
| 2026-08-20 | `HDSN` | 205 | — | $5.77 | +0.00 | $5.57 | -41.00 | -41.00 | +0.00 | -41.00 |
| 2026-08-20 | `IAG` | 60 | — | $19.63 | +0.00 | $20.50 | +52.20 | +52.20 | +0.00 | +52.20 |
| 2026-08-20 | `KGC` | 40 | — | $29.63 | +0.00 | $31.43 | +72.00 | +72.00 | +0.00 | +72.00 |
| 2026-08-20 | `NFGC` | 677 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-21 | `AG` | 57 | $21.19 | $21.90 | +40.47 | — | +0.00 | +40.47 | +76.95 | — |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `CDE` | 57 | $21.11 | $21.75 | +36.48 | — | +0.00 | +36.48 | +62.70 | — |
| 2026-08-21 | `HDSN` | 205 | $5.57 | $5.67 | +20.50 | — | +0.00 | +20.50 | -20.50 | — |
| 2026-08-21 | `IAG` | 60 | $20.50 | $21.17 | +40.20 | — | +0.00 | +40.20 | +92.40 | — |
| 2026-08-21 | `KGC` | 40 | $31.43 | $32.17 | +29.60 | — | +0.00 | +29.60 | +101.60 | — |
| 2026-08-21 | `NFGC` | 677 | $1.75 | $1.79 | +27.08 | — | +0.00 | +27.08 | +27.08 | — |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | — | +0.00 | +35.60 | +81.28 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `AUPH` | 72 | — | $17.20 | +0.00 | $16.65 | -39.60 | -39.60 | +0.00 | -39.60 |
| 2026-08-21 | `AEM` | 5 | — | $216.30 | +0.00 | $216.06 | -1.20 | -1.20 | +0.00 | -1.20 |
| 2026-08-21 | `ARCT` | 111 | — | $11.13 | +0.00 | $13.45 | +257.52 | +257.52 | +0.00 | +257.52 |
| 2026-08-21 | `CYPH` | 939 | — | $1.32 | +0.00 | $1.42 | +93.90 | +93.90 | +0.00 | +93.90 |
| 2026-08-21 | `FUTU` | 10 | — | $115.18 | +0.00 | $123.64 | +84.60 | +84.60 | +0.00 | +84.60 |
| 2026-08-21 | `GRAL` | 15 | — | $78.88 | +0.00 | $79.54 | +9.90 | +9.90 | +0.00 | +9.90 |
| 2026-08-21 | `ILMN` | 5 | — | $212.40 | +0.00 | $219.40 | +35.00 | +35.00 | +0.00 | +35.00 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.51 | -7.10 | — | +0.00 | -7.10 | +10.80 | — |
| 2026-08-24 | `AUPH` | 72 | $16.65 | $16.57 | -5.76 | — | +0.00 | -5.76 | -45.36 | — |
| 2026-08-24 | `AEM` | 5 | $216.06 | $217.03 | +4.85 | — | +0.00 | +4.85 | +3.65 | — |
| 2026-08-24 | `ARCT` | 111 | $13.45 | $13.33 | -13.32 | — | +0.00 | -13.32 | +244.20 | — |
| 2026-08-24 | `CYPH` | 939 | $1.42 | $1.83 | +384.99 | — | +0.00 | +384.99 | +478.89 | — |
| 2026-08-24 | `FUTU` | 10 | $123.64 | $121.00 | -26.40 | — | +0.00 | -26.40 | +58.20 | — |
| 2026-08-24 | `GRAL` | 15 | $79.54 | $81.87 | +34.95 | — | +0.00 | +34.95 | +44.85 | — |
| 2026-08-24 | `ILMN` | 5 | $219.40 | $215.98 | -17.10 | — | +0.00 | -17.10 | +17.90 | — |
| 2026-08-25 | `FCX` | 17 | — | $77.13 | +0.00 | $79.91 | +47.26 | +47.26 | +0.00 | +47.26 |
| 2026-08-25 | `RHI` | 30 | — | $43.76 | +0.00 | $44.90 | +34.20 | +34.20 | +0.00 | +34.20 |
| 2026-08-25 | `SUZ` | 148 | — | $8.98 | +0.00 | $9.03 | +7.40 | +7.40 | +0.00 | +7.40 |
| 2026-08-25 | `VALE` | 88 | — | $15.01 | +0.00 | $15.33 | +28.16 | +28.16 | +0.00 | +28.16 |
| 2026-08-25 | `WPM` | 8 | — | $156.51 | +0.00 | $163.72 | +57.68 | +57.68 | +0.00 | +57.68 |
| 2026-08-25 | `AVAH` | 98 | — | $13.62 | +0.00 | $13.59 | -3.43 | -3.43 | +0.00 | -3.43 |
| 2026-08-25 | `BMEA` | 819 | — | $1.63 | +0.00 | $1.73 | +81.90 | +81.90 | +0.00 | +81.90 |
| 2026-08-25 | `CYPH` | 856 | — | $1.56 | +0.00 | $1.64 | +68.48 | +68.48 | +0.00 | +68.48 |
| 2026-08-26 | `FCX` | 17 | $79.91 | $79.34 | -9.69 | — | +0.00 | -9.69 | +37.57 | — |
| 2026-08-26 | `RHI` | 30 | $44.90 | $44.33 | -17.10 | — | +0.00 | -17.10 | +17.10 | — |
| 2026-08-26 | `SUZ` | 148 | $9.03 | $9.03 | +0.00 | — | +0.00 | +0.00 | +7.40 | — |
| 2026-08-26 | `VALE` | 88 | $15.33 | $15.37 | +3.52 | — | +0.00 | +3.52 | +31.68 | — |
| 2026-08-26 | `WPM` | 8 | $163.72 | $160.93 | -22.32 | — | +0.00 | -22.32 | +35.36 | — |
| 2026-08-26 | `AVAH` | 98 | $13.59 | $13.65 | +5.88 | — | +0.00 | +5.88 | +2.45 | — |
| 2026-08-26 | `BMEA` | 819 | $1.73 | $1.75 | +20.47 | — | +0.00 | +20.47 | +102.38 | — |
| 2026-08-26 | `CYPH` | 856 | $1.64 | $1.60 | -34.24 | — | +0.00 | -34.24 | +34.24 | — |
| 2026-08-27 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-28 | `KEYS` | 4 | — | $324.41 | +0.00 | $319.97 | -17.76 | -17.76 | +0.00 | -17.76 |
| 2026-08-28 | `SMTC` | 9 | — | $141.76 | +0.00 | $131.17 | -95.31 | -95.31 | +0.00 | -95.31 |
| 2026-08-28 | `AVT` | 14 | — | $91.49 | +0.00 | $88.63 | -40.04 | -40.04 | +0.00 | -40.04 |
| 2026-08-28 | `CGNX` | 21 | — | $62.82 | +0.00 | $60.46 | -49.56 | -49.56 | +0.00 | -49.56 |
| 2026-08-28 | `COHR` | 4 | — | $289.44 | +0.00 | $279.20 | -40.96 | -40.96 | +0.00 | -40.96 |
| 2026-08-28 | `LSCC` | 11 | — | $119.76 | +0.00 | $114.40 | -58.96 | -58.96 | +0.00 | -58.96 |
| 2026-08-28 | `MEI` | 76 | — | $17.78 | +0.00 | $18.21 | +32.68 | +32.68 | +0.00 | +32.68 |
| 2026-08-28 | `MTSI` | 4 | — | $275.20 | +0.00 | $265.27 | -39.72 | -39.72 | +0.00 | -39.72 |
| 2026-08-31 | `KEYS` | 4 | $319.97 | $322.49 | +10.08 | — | +0.00 | +10.08 | -7.68 | — |
| 2026-08-31 | `SMTC` | 9 | $131.17 | $132.30 | +10.17 | — | +0.00 | +10.17 | -85.14 | — |
| 2026-08-31 | `AVT` | 14 | $88.63 | $89.39 | +10.64 | — | +0.00 | +10.64 | -29.40 | — |
| 2026-08-31 | `CGNX` | 21 | $60.46 | $60.46 | +0.00 | — | +0.00 | +0.00 | -49.56 | — |
| 2026-08-31 | `COHR` | 4 | $279.20 | $280.25 | +4.20 | — | +0.00 | +4.20 | -36.76 | — |
| 2026-08-31 | `LSCC` | 11 | $114.40 | $115.56 | +12.76 | — | +0.00 | +12.76 | -46.20 | — |
| 2026-08-31 | `MEI` | 76 | $18.21 | $18.15 | -4.56 | — | +0.00 | -4.56 | +28.12 | — |
| 2026-08-31 | `MTSI` | 4 | $265.27 | $266.96 | +6.76 | — | +0.00 | +6.76 | -32.96 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ARCT` | 78 | — | $16.77 | +0.00 | $15.56 | -94.38 | -94.38 | +0.00 | -94.38 |
| 2026-09-03 | `BMEA` | 685 | — | $1.93 | +0.00 | $1.91 | -13.70 | -13.70 | +0.00 | -13.70 |
| 2026-09-03 | `CRDL` | 607 | — | $2.18 | +0.00 | $2.16 | -12.14 | -12.14 | +0.00 | -12.14 |
| 2026-09-03 | `HRMY` | 30 | — | $42.93 | +0.00 | $41.86 | -32.10 | -32.10 | +0.00 | -32.10 |
| 2026-09-03 | `NVAX` | 127 | — | $10.42 | +0.00 | $10.34 | -10.16 | -10.16 | +0.00 | -10.16 |
| 2026-09-03 | `PBH` | 24 | — | $53.45 | +0.00 | $52.56 | -21.36 | -21.36 | +0.00 | -21.36 |
| 2026-09-03 | `PCRX` | 49 | — | $26.74 | +0.00 | $26.60 | -6.86 | -6.86 | +0.00 | -6.86 |
| 2026-09-03 | `RVTY` | 9 | — | $132.45 | +0.00 | $130.63 | -16.38 | -16.38 | +0.00 | -16.38 |
| 2026-09-04 | `ARCT` | 78 | $15.56 | $15.61 | +3.90 | — | +0.00 | +3.90 | -90.48 | — |
| 2026-09-04 | `BMEA` | 685 | $1.91 | $1.90 | -6.85 | — | +0.00 | -6.85 | -20.55 | — |
| 2026-09-04 | `CRDL` | 607 | $2.16 | $2.16 | +0.00 | — | +0.00 | +0.00 | -12.14 | — |
| 2026-09-04 | `HRMY` | 30 | $41.86 | $41.50 | -10.80 | — | +0.00 | -10.80 | -42.90 | — |
| 2026-09-04 | `NVAX` | 127 | $10.34 | $10.50 | +20.32 | — | +0.00 | +20.32 | +10.16 | — |
| 2026-09-04 | `PBH` | 24 | $52.56 | $51.80 | -18.24 | — | +0.00 | -18.24 | -39.60 | — |
| 2026-09-04 | `PCRX` | 49 | $26.60 | $26.38 | -10.78 | — | +0.00 | -10.78 | -17.64 | — |
| 2026-09-04 | `RVTY` | 9 | $130.63 | $130.03 | -5.40 | — | +0.00 | -5.40 | -21.78 | — |
| 2026-09-04 | `CRM` | 4 | — | $263.36 | +0.00 | $259.23 | -16.52 | -16.52 | +0.00 | -16.52 |
| 2026-09-04 | `TARS` | 15 | — | $82.70 | +0.00 | $90.78 | +121.20 | +121.20 | +0.00 | +121.20 |
| 2026-09-04 | `DELL` | 2 | — | $513.78 | +0.00 | $524.14 | +20.72 | +20.72 | +0.00 | +20.72 |
| 2026-09-04 | `FRNM` | 78 | — | $16.40 | +0.00 | $16.31 | -7.02 | -7.02 | +0.00 | -7.02 |
| 2026-09-04 | `IRD` | 284 | — | $4.53 | +0.00 | $4.67 | +39.76 | +39.76 | +0.00 | +39.76 |
| 2026-09-04 | `LENZ` | 223 | — | $5.75 | +0.00 | $5.96 | +46.83 | +46.83 | +0.00 | +46.83 |
| 2026-09-04 | `PIPR` | 16 | — | $76.55 | +0.00 | $77.04 | +7.84 | +7.84 | +0.00 | +7.84 |
| 2026-09-04 | `TDS` | 34 | — | $37.44 | +0.00 | $37.83 | +13.26 | +13.26 | +0.00 | +13.26 |
| 2026-09-08 | `CRM` | 4 | $259.23 | $253.72 | -22.04 | — | +0.00 | -22.04 | -38.56 | — |
| 2026-09-08 | `TARS` | 15 | $90.78 | $89.67 | -16.65 | — | +0.00 | -16.65 | +104.55 | — |
| 2026-09-08 | `DELL` | 2 | $524.14 | $521.15 | -5.98 | — | +0.00 | -5.98 | +14.74 | — |
| 2026-09-08 | `FRNM` | 78 | $16.31 | $16.74 | +33.54 | — | +0.00 | +33.54 | +26.52 | — |
| 2026-09-08 | `IRD` | 284 | $4.67 | $4.53 | -39.76 | — | +0.00 | -39.76 | +0.00 | — |
| 2026-09-08 | `LENZ` | 223 | $5.96 | $5.95 | -2.23 | — | +0.00 | -2.23 | +44.60 | — |
| 2026-09-08 | `PIPR` | 16 | $77.04 | $76.64 | -6.40 | — | +0.00 | -6.40 | +1.44 | — |
| 2026-09-08 | `TDS` | 34 | $37.83 | $37.75 | -2.72 | — | +0.00 | -2.72 | +10.54 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `BAND` | 49 | — | $52.55 | +0.00 | $56.87 | +211.68 | +211.68 | +0.00 | +211.68 |
| 2026-09-11 | `PAGS` | 257 | — | $10.11 | +0.00 | $10.12 | +2.57 | +2.57 | +0.00 | +2.57 |
| 2026-09-11 | `PAYP` | 142 | — | $18.30 | +0.00 | $18.45 | +21.30 | +21.30 | +0.00 | +21.30 |
| 2026-09-11 | `ZSQR` | 801 | — | $3.25 | +0.00 | $3.07 | -144.18 | -144.18 | +0.00 | -144.18 |
| 2026-09-14 | `BAND` | 49 | $56.87 | $56.90 | +1.47 | — | +0.00 | +1.47 | +213.15 | — |
| 2026-09-14 | `PAGS` | 257 | $10.12 | $10.00 | -30.84 | — | +0.00 | -30.84 | -28.27 | — |
| 2026-09-14 | `PAYP` | 142 | $18.45 | $18.28 | -24.14 | — | +0.00 | -24.14 | -2.84 | — |
| 2026-09-14 | `ZSQR` | 801 | $3.07 | $3.06 | -8.01 | — | +0.00 | -8.01 | -152.19 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `QRVO` | 44 | — | $118.18 | +0.00 | $113.97 | -185.24 | -185.24 | +0.00 | -185.24 |
| 2026-09-16 | `SWKS` | 58 | — | $89.38 | +0.00 | $85.59 | -219.82 | -219.82 | +0.00 | -219.82 |
| 2026-09-17 | `QRVO` | 44 | $113.97 | $114.90 | +40.92 | — | +0.00 | +40.92 | -144.32 | — |
| 2026-09-17 | `SWKS` | 58 | $85.59 | $86.76 | +67.86 | — | +0.00 | +67.86 | -151.96 | — |
| 2026-09-18 | `CRWD` | 5 | — | $246.98 | +0.00 | $237.65 | -46.65 | -46.65 | +0.00 | -46.65 |
| 2026-09-18 | `FIVN` | 36 | — | $34.44 | +0.00 | $32.47 | -70.92 | -70.92 | +0.00 | -70.92 |
| 2026-09-18 | `ATRC` | 21 | — | $58.51 | +0.00 | $58.10 | -8.61 | -8.61 | +0.00 | -8.61 |
| 2026-09-18 | `ECO` | 14 | — | $85.00 | +0.00 | $84.95 | -0.70 | -0.70 | +0.00 | -0.70 |
| 2026-09-18 | `PGEN` | 158 | — | $7.98 | +0.00 | $7.78 | -31.60 | -31.60 | +0.00 | -31.60 |
| 2026-09-18 | `RBRK` | 11 | — | $108.55 | +0.00 | $106.71 | -20.24 | -20.24 | +0.00 | -20.24 |
| 2026-09-18 | `RXT` | 320 | — | $3.94 | +0.00 | $3.80 | -44.80 | -44.80 | +0.00 | -44.80 |
| 2026-09-18 | `TH` | 60 | — | $20.91 | +0.00 | $21.19 | +16.80 | +16.80 | +0.00 | +16.80 |
| 2026-09-21 | `CRWD` | 5 | $237.65 | $231.62 | -30.15 | — | +0.00 | -30.15 | -76.80 | — |
| 2026-09-21 | `FIVN` | 36 | $32.47 | $33.00 | +19.08 | — | +0.00 | +19.08 | -51.84 | — |
| 2026-09-21 | `ATRC` | 21 | $58.10 | $58.23 | +2.73 | — | +0.00 | +2.73 | -5.88 | — |
| 2026-09-21 | `ECO` | 14 | $84.95 | $82.83 | -29.68 | — | +0.00 | -29.68 | -30.38 | — |
| 2026-09-21 | `PGEN` | 158 | $7.78 | $7.84 | +9.48 | — | +0.00 | +9.48 | -22.12 | — |
| 2026-09-21 | `RBRK` | 11 | $106.71 | $107.57 | +9.46 | — | +0.00 | +9.46 | -10.78 | — |
| 2026-09-21 | `RXT` | 320 | $3.80 | $3.90 | +32.00 | — | +0.00 | +32.00 | -12.80 | — |
| 2026-09-21 | `TH` | 60 | $21.19 | $21.65 | +27.60 | — | +0.00 | +27.60 | +44.40 | — |
| 2026-09-22 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-23 | `A` | 7 | — | $166.54 | +0.00 | $165.28 | -8.82 | -8.82 | +0.00 | -8.82 |
| 2026-09-23 | `BFLY` | 125 | — | $9.90 | +0.00 | $9.20 | -87.50 | -87.50 | +0.00 | -87.50 |
| 2026-09-23 | `OMER` | 59 | — | $20.65 | +0.00 | $20.74 | +5.55 | +5.55 | +0.00 | +5.55 |
| 2026-09-23 | `AMRX` | 62 | — | $19.70 | +0.00 | $19.33 | -22.94 | -22.94 | +0.00 | -22.94 |
| 2026-09-23 | `ARQT` | 44 | — | $27.79 | +0.00 | $26.38 | -62.04 | -62.04 | +0.00 | -62.04 |
| 2026-09-23 | `FIVN` | 31 | — | $38.99 | +0.00 | $37.68 | -40.61 | -40.61 | +0.00 | -40.61 |
| 2026-09-23 | `INOD` | 17 | — | $70.84 | +0.00 | $71.99 | +19.55 | +19.55 | +0.00 | +19.55 |
| 2026-09-23 | `CTAS` | 6 | — | $196.78 | +0.00 | $191.97 | -28.86 | -28.86 | +0.00 | -28.86 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +288.17 | BTSG, IREN, SLS, TPG | — | $83.49 | $10,279.02 | BTSG×41, IREN×54, SLS×213, TPG×49 |
| 2026-08-14 | +5.50 | $83.49 | BTSG×41, IREN×54, SLS×213, TPG×49 | $10,260.41 | -18.61 | -67.45 | BRUN, HLIT, MNTN, QMCO, QMLS, ZENA, ADUR | BTSG, IREN, SLS, TPG | $1,331.47 | $10,162.57 | BRUN×48, HLIT×97, MNTN×102, QMCO×51, QMLS×175, ZENA×582, ADUR×77 |
| 2026-08-17 | +2.25 | $1,331.47 | BRUN×48, HLIT×97, MNTN×102, QMCO×51, QMLS×175, ZENA×582, ADUR×77 | $10,000.76 | -161.81 | -85.04 | LPTH, AAOI, ABX, ALOY, BORR, INDI, KOPN, MP | BRUN, HLIT, MNTN, QMCO, QMLS, ZENA, ADUR | $59.21 | $9,873.50 | LPTH×83, AAOI×8, ABX×136, ALOY×85, BORR×271, INDI×268, KOPN×229, MP×21 |
| 2026-08-18 | -6.20 | $59.21 | LPTH×83, AAOI×8, ABX×136, ALOY×85, BORR×271, INDI×268, KOPN×229, MP×21 | $9,512.49 | -361.01 | +0.00 | — | LPTH, AAOI, ABX, ALOY, BORR, INDI, KOPN, MP | $9,491.36 | $9,491.36 | — |
| 2026-08-19 | -7.20 | $9,491.36 | — | $9,491.36 | -0.00 | +0.00 | — | — | $9,491.36 | $9,491.36 | — |
| 2026-08-20 | +1.12 | $9,491.36 | — | $9,491.36 | -0.00 | +225.64 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $48.89 | $9,692.98 | AG×57, BHP×13, CDE×57, HDSN×205, IAG×60, KGC×40, NFGC×677, WPM×8 |
| 2026-08-21 | +3.25 | $48.89 | AG×57, BHP×13, CDE×57, HDSN×205, IAG×60, KGC×40, NFGC×677, WPM×8 | $9,950.08 | +257.10 | +458.02 | AU, AUPH, AEM, ARCT, CYPH, FUTU, GRAL, ILMN | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $512.93 | $10,357.06 | AU×10, AUPH×72, AEM×5, ARCT×111, CYPH×939, FUTU×10, GRAL×15, ILMN×5 |
| 2026-08-24 | -5.17 | $512.93 | AU×10, AUPH×72, AEM×5, ARCT×111, CYPH×939, FUTU×10, GRAL×15, ILMN×5 | $10,712.17 | +355.11 | +0.00 | — | AU, AUPH, AEM, ARCT, CYPH, FUTU, GRAL, ILMN | $10,685.12 | $10,685.12 | — |
| 2026-08-25 | +1.80 | $10,685.12 | — | $10,685.12 | -0.00 | +321.65 | FCX, RHI, SUZ, VALE, WPM, AVAH, BMEA, CYPH | — | $118.82 | $10,972.06 | FCX×17, RHI×30, SUZ×148, VALE×88, WPM×8, AVAH×98, BMEA×819, CYPH×856 |
| 2026-08-26 | +2.02 | $118.82 | FCX×17, RHI×30, SUZ×148, VALE×88, WPM×8, AVAH×98, BMEA×819, CYPH×856 | $10,918.58 | -53.48 | +0.00 | — | FCX, RHI, SUZ, VALE, WPM, AVAH, BMEA, CYPH | $10,883.42 | $10,883.42 | — |
| 2026-08-27 | — | $10,883.42 | — | $10,883.42 | -0.00 | +0.00 | — | — | $10,883.42 | $10,883.42 | — |
| 2026-08-28 | +0.75 | $10,883.42 | — | $10,883.42 | -0.00 | -309.63 | KEYS, SMTC, AVT, CGNX, COHR, LSCC, MEI, MTSI | — | $766.31 | $10,557.44 | KEYS×4, SMTC×9, AVT×14, CGNX×21, COHR×4, LSCC×11, MEI×76, MTSI×4 |
| 2026-08-31 | -5.85 | $766.31 | KEYS×4, SMTC×9, AVT×14, CGNX×21, COHR×4, LSCC×11, MEI×76, MTSI×4 | $10,607.49 | +50.05 | +0.00 | — | KEYS, SMTC, AVT, CGNX, COHR, LSCC, MEI, MTSI | $10,590.97 | $10,590.97 | — |
| 2026-09-01 | -6.30 | $10,590.97 | — | $10,590.97 | +0.00 | +0.00 | — | — | $10,590.97 | $10,590.97 | — |
| 2026-09-02 | -3.83 | $10,590.97 | — | $10,590.97 | +0.00 | +0.00 | — | — | $10,590.97 | $10,590.97 | — |
| 2026-09-03 | -0.90 | $10,590.97 | — | $10,590.97 | +0.00 | -207.08 | ARCT, BMEA, CRDL, HRMY, NVAX, PBH, PCRX, RVTY | — | $211.70 | $10,354.34 | ARCT×78, BMEA×685, CRDL×607, HRMY×30, NVAX×127, PBH×24, PCRX×49, RVTY×9 |
| 2026-09-04 | +2.25 | $211.70 | ARCT×78, BMEA×685, CRDL×607, HRMY×30, NVAX×127, PBH×24, PCRX×49, RVTY×9 | $10,326.49 | -27.85 | +226.07 | CRM, TARS, DELL, FRNM, IRD, LENZ, PIPR, TDS | ARCT, BMEA, CRDL, HRMY, NVAX, PBH, PCRX, RVTY | $610.40 | $10,503.70 | CRM×4, TARS×15, DELL×2, FRNM×78, IRD×284, LENZ×223, PIPR×16, TDS×34 |
| 2026-09-08 | -11.47 | $610.40 | CRM×4, TARS×15, DELL×2, FRNM×78, IRD×284, LENZ×223, PIPR×16, TDS×34 | $10,441.46 | -62.24 | +0.00 | — | CRM, TARS, DELL, FRNM, IRD, LENZ, PIPR, TDS | $10,422.30 | $10,422.30 | — |
| 2026-09-09 | -13.95 | $10,422.30 | — | $10,422.30 | +0.00 | +0.00 | — | — | $10,422.30 | $10,422.30 | — |
| 2026-09-10 | -13.28 | $10,422.30 | — | $10,422.30 | +0.00 | +0.00 | — | — | $10,422.30 | $10,422.30 | — |
| 2026-09-11 | +0.50 | $10,422.30 | — | $10,422.30 | +0.00 | +91.37 | BAND, PAGS, PAYP, ZSQR | — | $29.03 | $10,495.47 | BAND×49, PAGS×257, PAYP×142, ZSQR×801 |
| 2026-09-14 | -11.00 | $29.03 | BAND×49, PAGS×257, PAYP×142, ZSQR×801 | $10,433.95 | -61.52 | +0.00 | — | BAND, PAGS, PAYP, ZSQR | $10,415.46 | $10,415.46 | — |
| 2026-09-15 | -3.84 | $10,415.46 | — | $10,415.46 | -0.00 | +0.00 | — | — | $10,415.46 | $10,415.46 | — |
| 2026-09-16 | +5.30 | $10,415.46 | — | $10,415.46 | -0.00 | -405.06 | QRVO, SWKS | — | $27.21 | $10,006.11 | QRVO×44, SWKS×58 |
| 2026-09-17 | +7.38 | $27.21 | QRVO×44, SWKS×58 | $10,114.89 | +108.78 | +0.00 | — | QRVO, SWKS | $10,110.51 | $10,110.51 | — |
| 2026-09-18 | +4.86 | $10,110.51 | — | $10,110.51 | -0.00 | -206.72 | CRWD, FIVN, ATRC, ECO, PGEN, RBRK, RXT, TH | — | $227.79 | $9,884.81 | CRWD×5, FIVN×36, ATRC×21, ECO×14, PGEN×158, RBRK×11, RXT×320, TH×60 |
| 2026-09-21 | +12.87 | $227.79 | CRWD×5, FIVN×36, ATRC×21, ECO×14, PGEN×158, RBRK×11, RXT×320, TH×60 | $9,925.33 | +40.52 | +0.00 | — | CRWD, FIVN, ATRC, ECO, PGEN, RBRK, RXT, TH | $9,906.14 | $9,906.14 | — |
| 2026-09-22 | -0.50 | $9,906.14 | — | $9,906.14 | +0.00 | +0.00 | — | — | $9,906.14 | $9,906.14 | — |
| 2026-09-23 | +2.29 | $9,906.14 | — | $9,906.14 | +0.00 | -225.67 | A, BFLY, OMER, AMRX, ARQT, FIVN, INOD, CTAS | — | $229.96 | $9,663.49 | A×7, BFLY×125, OMER×59, AMRX×62, ARQT×44, FIVN×31, INOD×17, CTAS×6 |

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
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 51 | $24.68 | $2.14 | — | $5,170.36 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+111.3; leftover $1281.39 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 175 | $7.29 | $2.52 | — | $3,892.10 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $1281.39 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 582 | $2.20 | $7.51 | — | $2,604.19 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $1281.39 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 77 | $16.50 | $2.22 | — | $1,331.47 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1281.39 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,331.47 | ▼ close $10,162.57 vs 09:30 $10,260.41 (session -67.45) | 16:00 close · cash $1,331.47 · equity $10,162.57 vs 09:30 $10,260.41 (-97.84; session marks -67.45) · 7 name(s) marked open→close (per-name table). BRUN×48 09:30 $26.25 → close $22.93 -159.12; HLIT×97 09:30 $13.18 → close $13.92 +71.78; MNTN×102 09:30 $12.50 → close $12.52 +2.04; QMCO×51 09:30 $24.68 → close $26.11 +72.93; QMLS×175 09:30 $7.29 → close $7.32 +5.25; ZENA×582 09:30 $2.20 → close $2.14 -34.92; ADUR×77 09:30 $16.50 → close $16.17 -25.41 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,331.47 | ▼ 09:30 equity $10,000.76 vs yday $10,162.57 (-161.81) | 09:30 open · cash $1,331.47 (unchanged overnight, no fees) · equity $10,000.76 vs prior close $10,162.57 (-161.81) · 7 name(s) re-marked at the open (per-name table). BRUN×48 yday $22.93 → 09:30 $23.00 +3.36; HLIT×97 yday $13.92 → 09:30 $13.84 -7.76; MNTN×102 yday $12.52 → 09:30 $12.40 -12.24; QMCO×51 yday $26.11 → 09:30 $24.83 -65.28; QMLS×175 yday $7.32 → 09:30 $7.24 -14.00; ZENA×582 yday $2.14 → 09:30 $2.08 -32.01; ADUR×77 yday $16.17 → 09:30 $15.73 -33.88 | — |
| 2026-08-17 09:30 ET | **SELL** | `BRUN` | 48 | $23.00 | $2.15 | $-160.05 | $2,433.32 | ▼ -160.05 after sell → book $9,998.61; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 97 | $13.84 | $2.31 | $+59.43 | $3,773.49 | ▲ +59.43 after sell → book $9,996.30; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MNTN` | 102 | $12.40 | $2.32 | $-14.82 | $5,035.96 | ▼ -14.82 after sell → book $9,993.97; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 51 | $24.83 | $2.16 | $+3.34 | $6,300.13 | ▲ +3.34 after sell → book $9,991.81; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMLS` | 175 | $7.24 | $2.55 | $-13.82 | $7,564.58 | ▼ -13.82 after sell → book $9,989.26; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 582 | $2.08 | $7.61 | $-82.05 | $8,770.43 | ▼ -82.05 after sell → book $9,981.64; vs 09:30 mark -7.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 77 | $15.73 | $2.24 | $-63.75 | $9,979.40 | ▼ -63.75 after sell → book $9,979.40; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `LPTH` | 83 | $14.94 | $2.24 | — | $8,737.14 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $1247.42 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `AAOI` | 8 | $152.64 | $2.01 | — | $7,514.01 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+10.8; leftover $1247.42 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 136 | $9.12 | $2.40 | — | $6,271.29 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1247.42 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 85 | $14.66 | $2.25 | — | $5,022.94 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $1247.42 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `BORR` | 271 | $4.59 | $3.50 | — | $3,775.56 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,ohlc_hot; ⚪; ret5=+14.8; leftover $1247.42 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `INDI` | 268 | $4.65 | $3.46 | — | $2,525.90 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; ⚪; ret5=+16.6; leftover $1247.42 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `KOPN` | 229 | $5.43 | $2.95 | — | $1,279.48 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer; ⚪; ret5=+28.8; leftover $1247.42 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `MP` | 21 | $58.01 | $2.05 | — | $59.21 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1247.42 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $59.21 | ▼ close $9,873.50 vs 09:30 $10,000.76 (session -85.04) | 16:00 close · cash $59.21 · equity $9,873.50 vs 09:30 $10,000.76 (-127.26; session marks -85.04) · 8 name(s) marked open→close (per-name table). LPTH×83 09:30 $14.94 → close $14.80 -11.62; AAOI×8 09:30 $152.64 → close $154.89 +18.00; ABX×136 09:30 $9.12 → close $9.12 +0.00; ALOY×85 09:30 $14.66 → close $13.86 -68.42; BORR×271 09:30 $4.59 → close $4.50 -24.39; INDI×268 09:30 $4.65 → close $4.71 +16.08; KOPN×229 09:30 $5.43 → close $5.32 -25.19; MP×21 09:30 $58.01 → close $58.51 +10.50 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.21 | ▼ 09:30 equity $9,512.49 vs yday $9,873.50 (-361.01) | 09:30 open · cash $59.21 (unchanged overnight, no fees) · equity $9,512.49 vs prior close $9,873.50 (-361.01) · 8 name(s) re-marked at the open (per-name table). LPTH×83 yday $14.80 → 09:30 $14.01 -65.57; AAOI×8 yday $154.89 → 09:30 $146.20 -69.52; ABX×136 yday $9.12 → 09:30 $9.03 -12.24; ALOY×85 yday $13.86 → 09:30 $13.19 -56.53; BORR×271 yday $4.50 → 09:30 $4.56 +16.26; INDI×268 yday $4.71 → 09:30 $4.48 -61.64; KOPN×229 yday $5.32 → 09:30 $5.03 -66.41; MP×21 yday $58.51 → 09:30 $56.35 -45.36 | — |
| 2026-08-18 09:30 ET | **SELL** | `LPTH` | 83 | $14.01 | $2.26 | $-81.69 | $1,219.78 | ▼ -81.69 after sell → book $9,510.23; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `AAOI` | 8 | $146.20 | $2.03 | $-55.57 | $2,387.35 | ▼ -55.57 after sell → book $9,508.20; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 136 | $9.03 | $2.43 | $-17.07 | $3,613.00 | ▼ -17.07 after sell → book $9,505.77; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 85 | $13.19 | $2.27 | $-129.46 | $4,731.88 | ▼ -129.46 after sell → book $9,503.50; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `BORR` | 271 | $4.56 | $3.55 | $-15.18 | $5,964.09 | ▼ -15.18 after sell → book $9,499.95; vs 09:30 mark -3.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `INDI` | 268 | $4.48 | $3.51 | $-52.53 | $7,161.21 | ▼ -52.53 after sell → book $9,496.43; vs 09:30 mark -3.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `KOPN` | 229 | $5.03 | $3.00 | $-97.56 | $8,310.08 | ▼ -97.56 after sell → book $9,493.43; vs 09:30 mark -3.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `MP` | 21 | $56.35 | $2.07 | $-38.99 | $9,491.36 | ▼ -38.99 after sell → book $9,491.36; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,491.36 | ▲ close $9,491.36 vs 09:30 $9,512.49 (session +0.00) | 16:00 close · cash $9,491.36 · no lots left · equity $9,491.36. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,491.36 | ▲ 09:30 equity $9,491.36 vs yday $9,491.36 (-0.00) | 09:30 open · cash $9,491.36 · no holdings · equity $9,491.36 vs prior close $9,491.36 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,491.36 | ▲ close $9,491.36 vs 09:30 $9,491.36 (session +0.00) | 16:00 close · cash $9,491.36 · no lots left · equity $9,491.36. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,491.36 | ▲ 09:30 equity $9,491.36 vs yday $9,491.36 (-0.00) | 09:30 open · cash $9,491.36 · no holdings · equity $9,491.36 vs prior close $9,491.36 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 57 | $20.55 | $2.16 | — | $8,317.85 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1186.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,132.69 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1186.42 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 57 | $20.65 | $2.16 | — | $5,953.48 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1186.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 205 | $5.77 | $2.64 | — | $4,767.98 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1186.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 60 | $19.63 | $2.17 | — | $3,588.01 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1186.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 40 | $29.63 | $2.11 | — | $2,400.70 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1186.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 677 | $1.75 | $8.73 | — | $1,207.22 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1186.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $48.89 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1186.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.89 | ▲ close $9,692.98 vs 09:30 $9,491.36 (session +225.64) | 16:00 close · cash $48.89 · equity $9,692.98 vs 09:30 $9,491.36 (+201.62; session marks +225.64) · 8 name(s) marked open→close (per-name table). AG×57 09:30 $20.55 → close $21.19 +36.48; BHP×13 09:30 $91.01 → close $93.63 +34.06; CDE×57 09:30 $20.65 → close $21.11 +26.22; HDSN×205 09:30 $5.77 → close $5.57 -41.00; IAG×60 09:30 $19.63 → close $20.50 +52.20; KGC×40 09:30 $29.63 → close $31.43 +72.00; NFGC×677 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.89 | ▲ 09:30 equity $9,950.08 vs yday $9,692.98 (+257.10) | 09:30 open · cash $48.89 (unchanged overnight, no fees) · equity $9,950.08 vs prior close $9,692.98 (+257.10) · 8 name(s) re-marked at the open (per-name table). AG×57 yday $21.19 → 09:30 $21.90 +40.47; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×57 yday $21.11 → 09:30 $21.75 +36.48; HDSN×205 yday $5.57 → 09:30 $5.67 +20.50; IAG×60 yday $20.50 → 09:30 $21.17 +40.20; KGC×40 yday $31.43 → 09:30 $32.17 +29.60; NFGC×677 yday $1.75 → 09:30 $1.79 +27.08; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 57 | $21.90 | $2.18 | $+72.61 | $1,295.00 | ▲ +72.61 after sell → book $9,947.89; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,537.32 | ▲ +57.15 after sell → book $9,945.85; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 57 | $21.75 | $2.18 | $+58.36 | $3,774.88 | ▲ +58.36 after sell → book $9,943.66; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 205 | $5.67 | $2.69 | $-25.83 | $4,934.55 | ▼ -25.83 after sell → book $9,940.98; vs 09:30 mark -2.68 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 60 | $21.17 | $2.19 | $+88.04 | $6,202.56 | ▲ +88.04 after sell → book $9,938.79; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 40 | $32.17 | $2.13 | $+97.36 | $7,487.23 | ▲ +97.36 after sell → book $9,936.66; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 677 | $1.79 | $8.86 | $+9.49 | $8,690.20 | ▲ +9.49 after sell → book $9,927.80; vs 09:30 mark -8.86 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $9,925.77 | ▲ +77.23 after sell → book $9,925.77; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $8,729.45 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1240.72 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 72 | $17.20 | $2.21 | — | $7,488.84 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1240.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $6,405.33 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1240.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 111 | $11.13 | $2.32 | — | $5,167.58 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1240.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 939 | $1.32 | $12.11 | — | $3,915.99 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1240.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 10 | $115.18 | $2.02 | — | $2,762.17 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1240.72 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 15 | $78.88 | $2.04 | — | $1,576.93 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1240.72 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ILMN` | 5 | $212.40 | $2.00 | — | $512.93 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+10.7; leftover $1240.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $512.93 | ▲ close $10,357.06 vs 09:30 $9,950.08 (session +458.02) | 16:00 close · cash $512.93 · equity $10,357.06 vs 09:30 $9,950.08 (+406.98; session marks +458.02) · 8 name(s) marked open→close (per-name table). AU×10 09:30 $119.43 → close $121.22 +17.90; AUPH×72 09:30 $17.20 → close $16.65 -39.60; AEM×5 09:30 $216.30 → close $216.06 -1.20; ARCT×111 09:30 $11.13 → close $13.45 +257.52; CYPH×939 09:30 $1.32 → close $1.42 +93.90; FUTU×10 09:30 $115.18 → close $123.64 +84.60; GRAL×15 09:30 $78.88 → close $79.54 +9.90; ILMN×5 09:30 $212.40 → close $219.40 +35.00 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $512.93 | ▲ 09:30 equity $10,712.17 vs yday $10,357.06 (+355.11) | 09:30 open · cash $512.93 (unchanged overnight, no fees) · equity $10,712.17 vs prior close $10,357.06 (+355.11) · 8 name(s) re-marked at the open (per-name table). AU×10 yday $121.22 → 09:30 $120.51 -7.10; AUPH×72 yday $16.65 → 09:30 $16.57 -5.76; AEM×5 yday $216.06 → 09:30 $217.03 +4.85; ARCT×111 yday $13.45 → 09:30 $13.33 -13.32; CYPH×939 yday $1.42 → 09:30 $1.83 +384.99; FUTU×10 yday $123.64 → 09:30 $121.00 -26.40; GRAL×15 yday $79.54 → 09:30 $81.87 +34.95; ILMN×5 yday $219.40 → 09:30 $215.98 -17.10 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,715.99 | ▲ +6.74 after sell → book $10,710.13; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 72 | $16.57 | $2.23 | $-49.79 | $2,906.80 | ▼ -49.79 after sell → book $10,707.90; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,989.93 | ▼ -0.38 after sell → book $10,705.88; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 111 | $13.33 | $2.35 | $+239.52 | $5,467.20 | ▲ +239.52 after sell → book $10,703.52; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 939 | $1.83 | $12.28 | $+454.49 | $7,173.29 | ▲ +454.49 after sell → book $10,691.24; vs 09:30 mark -12.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 10 | $121.00 | $2.04 | $+54.14 | $8,381.25 | ▲ +54.14 after sell → book $10,689.20; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 15 | $81.87 | $2.06 | $+40.76 | $9,607.24 | ▲ +40.76 after sell → book $10,687.14; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ILMN` | 5 | $215.98 | $2.02 | $+13.87 | $10,685.12 | ▲ +13.87 after sell → book $10,685.12; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,685.12 | ▲ close $10,685.12 vs 09:30 $10,712.17 (session +0.00) | 16:00 close · cash $10,685.12 · no lots left · equity $10,685.12. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,685.12 | ▲ 09:30 equity $10,685.12 vs yday $10,685.12 (-0.00) | 09:30 open · cash $10,685.12 · no holdings · equity $10,685.12 vs prior close $10,685.12 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 17 | $77.13 | $2.04 | — | $9,371.87 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $1335.64 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 30 | $43.76 | $2.08 | — | $8,056.99 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+6.2; leftover $1335.64 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUZ` | 148 | $8.98 | $2.43 | — | $6,725.51 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot,mover_buy; ⚪; ret5=+15.4; leftover $1335.64 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `VALE` | 88 | $15.01 | $2.25 | — | $5,402.38 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; ⚪; ret5=+9.4; leftover $1335.64 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `WPM` | 8 | $156.51 | $2.01 | — | $4,148.29 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot,mover_buy; ⚪; ret5=+17.4; leftover $1335.64 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AVAH` | 98 | $13.62 | $2.28 | — | $2,810.75 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; leftover $1335.64 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 819 | $1.63 | $10.57 | — | $1,465.22 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1335.64 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 856 | $1.56 | $11.04 | — | $118.82 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $1335.64 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $118.82 | ▲ close $10,972.06 vs 09:30 $10,685.12 (session +321.65) | 16:00 close · cash $118.82 · equity $10,972.06 vs 09:30 $10,685.12 (+286.94; session marks +321.65) · 8 name(s) marked open→close (per-name table). FCX×17 09:30 $77.13 → close $79.91 +47.26; RHI×30 09:30 $43.76 → close $44.90 +34.20; SUZ×148 09:30 $8.98 → close $9.03 +7.40; VALE×88 09:30 $15.01 → close $15.33 +28.16; WPM×8 09:30 $156.51 → close $163.72 +57.68; AVAH×98 09:30 $13.62 → close $13.59 -3.43; BMEA×819 09:30 $1.63 → close $1.73 +81.90; CYPH×856 09:30 $1.56 → close $1.64 +68.48 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $118.82 | ▼ 09:30 equity $10,918.58 vs yday $10,972.06 (-53.48) | 09:30 open · cash $118.82 (unchanged overnight, no fees) · equity $10,918.58 vs prior close $10,972.06 (-53.48) · 8 name(s) re-marked at the open (per-name table). FCX×17 yday $79.91 → 09:30 $79.34 -9.69; RHI×30 yday $44.90 → 09:30 $44.33 -17.10; SUZ×148 yday $9.03 → 09:30 $9.03 +0.00; VALE×88 yday $15.33 → 09:30 $15.37 +3.52; WPM×8 yday $163.72 → 09:30 $160.93 -22.32; AVAH×98 yday $13.59 → 09:30 $13.65 +5.88; BMEA×819 yday $1.73 → 09:30 $1.75 +20.47; CYPH×856 yday $1.64 → 09:30 $1.60 -34.24 | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 17 | $79.34 | $2.06 | $+33.47 | $1,465.53 | ▲ +33.47 after sell → book $10,916.52; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RHI` | 30 | $44.33 | $2.10 | $+12.92 | $2,793.33 | ▲ +12.92 after sell → book $10,914.42; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SUZ` | 148 | $9.03 | $2.47 | $+2.50 | $4,127.30 | ▲ +2.50 after sell → book $10,911.95; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `VALE` | 88 | $15.37 | $2.28 | $+27.15 | $5,477.58 | ▲ +27.15 after sell → book $10,909.67; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `WPM` | 8 | $160.93 | $2.03 | $+31.31 | $6,762.99 | ▲ +31.31 after sell → book $10,907.63; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AVAH` | 98 | $13.65 | $2.31 | $-2.14 | $8,098.38 | ▼ -2.14 after sell → book $10,905.32; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 819 | $1.75 | $10.71 | $+81.10 | $9,525.01 | ▲ +81.10 after sell → book $10,894.61; vs 09:30 mark -10.71 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 856 | $1.60 | $11.20 | $+12.00 | $10,883.42 | ▲ +12.00 after sell → book $10,883.42; vs 09:30 mark -11.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,883.42 | ▲ close $10,883.42 vs 09:30 $10,918.58 (session +0.00) | 16:00 close · cash $10,883.42 · no lots left · equity $10,883.42. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,883.42 | ▲ 09:30 equity $10,883.42 vs yday $10,883.42 (-0.00) | 09:30 open · cash $10,883.42 · no holdings · equity $10,883.42 vs prior close $10,883.42 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,883.42 | ▲ close $10,883.42 vs 09:30 $10,883.42 (session +0.00) | 16:00 close · cash $10,883.42 · no lots left · equity $10,883.42. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,883.42 | ▲ 09:30 equity $10,883.42 vs yday $10,883.42 (-0.00) | 09:30 open · cash $10,883.42 · no holdings · equity $10,883.42 vs prior close $10,883.42 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $9,583.77 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1360.43 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $8,305.92 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1360.43 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 14 | $91.49 | $2.03 | — | $7,023.02 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $1360.43 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 21 | $62.82 | $2.05 | — | $5,701.75 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+5.4; leftover $1360.43 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 4 | $289.44 | $2.00 | — | $4,541.99 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+1.8; leftover $1360.43 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 11 | $119.76 | $2.02 | — | $3,222.61 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.5; leftover $1360.43 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MEI` | 76 | $17.78 | $2.22 | — | $1,869.11 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+22.9; leftover $1360.43 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MTSI` | 4 | $275.20 | $2.00 | — | $766.31 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+4.1; leftover $1360.43 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $766.31 | ▼ close $10,557.44 vs 09:30 $10,883.42 (session -309.63) | 16:00 close · cash $766.31 · equity $10,557.44 vs 09:30 $10,883.42 (-325.98; session marks -309.63) · 8 name(s) marked open→close (per-name table). KEYS×4 09:30 $324.41 → close $319.97 -17.76; SMTC×9 09:30 $141.76 → close $131.17 -95.31; AVT×14 09:30 $91.49 → close $88.63 -40.04; CGNX×21 09:30 $62.82 → close $60.46 -49.56; COHR×4 09:30 $289.44 → close $279.20 -40.96; LSCC×11 09:30 $119.76 → close $114.40 -58.96; MEI×76 09:30 $17.78 → close $18.21 +32.68; MTSI×4 09:30 $275.20 → close $265.27 -39.72 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $766.31 | ▲ 09:30 equity $10,607.49 vs yday $10,557.44 (+50.05) | 09:30 open · cash $766.31 (unchanged overnight, no fees) · equity $10,607.49 vs prior close $10,557.44 (+50.05) · 8 name(s) re-marked at the open (per-name table). KEYS×4 yday $319.97 → 09:30 $322.49 +10.08; SMTC×9 yday $131.17 → 09:30 $132.30 +10.17; AVT×14 yday $88.63 → 09:30 $89.39 +10.64; CGNX×21 yday $60.46 → 09:30 $60.46 +0.00; COHR×4 yday $279.20 → 09:30 $280.25 +4.20; LSCC×11 yday $114.40 → 09:30 $115.56 +12.76; MEI×76 yday $18.21 → 09:30 $18.15 -4.56; MTSI×4 yday $265.27 → 09:30 $266.96 +6.76 | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 4 | $322.49 | $2.02 | $-11.70 | $2,054.24 | ▼ -11.70 after sell → book $10,605.46; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $3,242.91 | ▼ -89.19 after sell → book $10,603.43; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVT` | 14 | $89.39 | $2.05 | $-33.48 | $4,492.32 | ▼ -33.48 after sell → book $10,601.38; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CGNX` | 21 | $60.46 | $2.07 | $-53.69 | $5,759.90 | ▼ -53.69 after sell → book $10,599.30; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `COHR` | 4 | $280.25 | $2.02 | $-40.78 | $6,878.88 | ▼ -40.78 after sell → book $10,597.28; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LSCC` | 11 | $115.56 | $2.04 | $-50.27 | $8,148.00 | ▼ -50.27 after sell → book $10,595.24; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MEI` | 76 | $18.15 | $2.24 | $+23.66 | $9,525.16 | ▲ +23.66 after sell → book $10,593.00; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MTSI` | 4 | $266.96 | $2.02 | $-36.98 | $10,590.97 | ▼ -36.98 after sell → book $10,590.97; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,590.97 | ▲ close $10,590.97 vs 09:30 $10,607.49 (session +0.00) | 16:00 close · cash $10,590.97 · no lots left · equity $10,590.97. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,590.97 | ▲ 09:30 equity $10,590.97 vs yday $10,590.97 (+0.00) | 09:30 open · cash $10,590.97 · no holdings · equity $10,590.97 vs prior close $10,590.97 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,590.97 | ▲ close $10,590.97 vs 09:30 $10,590.97 (session +0.00) | 16:00 close · cash $10,590.97 · no lots left · equity $10,590.97. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,590.97 | ▲ 09:30 equity $10,590.97 vs yday $10,590.97 (+0.00) | 09:30 open · cash $10,590.97 · no holdings · equity $10,590.97 vs prior close $10,590.97 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,590.97 | ▲ close $10,590.97 vs 09:30 $10,590.97 (session +0.00) | 16:00 close · cash $10,590.97 · no lots left · equity $10,590.97. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,590.97 | ▲ 09:30 equity $10,590.97 vs yday $10,590.97 (+0.00) | 09:30 open · cash $10,590.97 · no holdings · equity $10,590.97 vs prior close $10,590.97 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 78 | $16.77 | $2.22 | — | $9,280.69 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1323.87 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 685 | $1.93 | $8.84 | — | $7,949.80 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1323.87 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 607 | $2.18 | $7.83 | — | $6,618.71 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1323.87 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 30 | $42.93 | $2.08 | — | $5,328.73 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1323.87 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 127 | $10.42 | $2.37 | — | $4,003.02 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; leftover $1323.87 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PBH` | 24 | $53.45 | $2.06 | — | $2,718.16 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+1.3; leftover $1323.87 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `PCRX` | 49 | $26.74 | $2.14 | — | $1,405.76 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.6; leftover $1323.87 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $211.70 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1323.87 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $211.70 | ▼ close $10,354.34 vs 09:30 $10,590.97 (session -207.08) | 16:00 close · cash $211.70 · equity $10,354.34 vs 09:30 $10,590.97 (-236.63; session marks -207.08) · 8 name(s) marked open→close (per-name table). ARCT×78 09:30 $16.77 → close $15.56 -94.38; BMEA×685 09:30 $1.93 → close $1.91 -13.70; CRDL×607 09:30 $2.18 → close $2.16 -12.14; HRMY×30 09:30 $42.93 → close $41.86 -32.10; NVAX×127 09:30 $10.42 → close $10.34 -10.16; PBH×24 09:30 $53.45 → close $52.56 -21.36; PCRX×49 09:30 $26.74 → close $26.60 -6.86; RVTY×9 09:30 $132.45 → close $130.63 -16.38 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $211.70 | ▼ 09:30 equity $10,326.49 vs yday $10,354.34 (-27.85) | 09:30 open · cash $211.70 (unchanged overnight, no fees) · equity $10,326.49 vs prior close $10,354.34 (-27.85) · 8 name(s) re-marked at the open (per-name table). ARCT×78 yday $15.56 → 09:30 $15.61 +3.90; BMEA×685 yday $1.91 → 09:30 $1.90 -6.85; CRDL×607 yday $2.16 → 09:30 $2.16 +0.00; HRMY×30 yday $41.86 → 09:30 $41.50 -10.80; NVAX×127 yday $10.34 → 09:30 $10.50 +20.32; PBH×24 yday $52.56 → 09:30 $51.80 -18.24; PCRX×49 yday $26.60 → 09:30 $26.38 -10.78; RVTY×9 yday $130.63 → 09:30 $130.03 -5.40 | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 78 | $15.61 | $2.25 | $-94.95 | $1,427.03 | ▼ -94.95 after sell → book $10,324.24; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `BMEA` | 685 | $1.90 | $8.96 | $-38.35 | $2,719.57 | ▼ -38.35 after sell → book $10,315.28; vs 09:30 mark -8.96 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 607 | $2.16 | $7.94 | $-27.91 | $4,022.75 | ▼ -27.91 after sell → book $10,307.34; vs 09:30 mark -7.94 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 30 | $41.50 | $2.10 | $-47.08 | $5,265.65 | ▼ -47.08 after sell → book $10,305.24; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 127 | $10.50 | $2.40 | $+5.39 | $6,596.74 | ▲ +5.39 after sell → book $10,302.83; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PBH` | 24 | $51.80 | $2.08 | $-43.74 | $7,837.86 | ▼ -43.74 after sell → book $10,300.75; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PCRX` | 49 | $26.38 | $2.16 | $-21.93 | $9,128.32 | ▼ -21.93 after sell → book $10,298.59; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $130.03 | $2.04 | $-25.83 | $10,296.56 | ▼ -25.83 after sell → book $10,296.56; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 4 | $263.36 | $2.00 | — | $9,241.12 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1287.07 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 15 | $82.70 | $2.04 | — | $7,998.58 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+15.7; leftover $1287.07 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $513.78 | $2.00 | — | $6,969.02 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; leftover $1287.07 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `FRNM` | 78 | $16.40 | $2.22 | — | $5,687.60 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.2; leftover $1287.07 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 284 | $4.53 | $3.66 | — | $4,397.42 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.5; leftover $1287.07 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `LENZ` | 223 | $5.75 | $2.88 | — | $3,112.29 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+21.2; leftover $1287.07 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `PIPR` | 16 | $76.55 | $2.04 | — | $1,885.45 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+4.2; leftover $1287.07 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TDS` | 34 | $37.44 | $2.09 | — | $610.40 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+14.1; leftover $1287.07 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $610.40 | ▲ close $10,503.70 vs 09:30 $10,326.49 (session +226.07) | 16:00 close · cash $610.40 · equity $10,503.70 vs 09:30 $10,326.49 (+177.21; session marks +226.07) · 8 name(s) marked open→close (per-name table). CRM×4 09:30 $263.36 → close $259.23 -16.52; TARS×15 09:30 $82.70 → close $90.78 +121.20; DELL×2 09:30 $513.78 → close $524.14 +20.72; FRNM×78 09:30 $16.40 → close $16.31 -7.02; IRD×284 09:30 $4.53 → close $4.67 +39.76; LENZ×223 09:30 $5.75 → close $5.96 +46.83; PIPR×16 09:30 $76.55 → close $77.04 +7.84; TDS×34 09:30 $37.44 → close $37.83 +13.26 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $610.40 | ▼ 09:30 equity $10,441.46 vs yday $10,503.70 (-62.24) | 09:30 open · cash $610.40 (unchanged overnight, no fees) · equity $10,441.46 vs prior close $10,503.70 (-62.24) · 8 name(s) re-marked at the open (per-name table). CRM×4 yday $259.23 → 09:30 $253.72 -22.04; TARS×15 yday $90.78 → 09:30 $89.67 -16.65; DELL×2 yday $524.14 → 09:30 $521.15 -5.98; FRNM×78 yday $16.31 → 09:30 $16.74 +33.54; IRD×284 yday $4.67 → 09:30 $4.53 -39.76; LENZ×223 yday $5.96 → 09:30 $5.95 -2.23; PIPR×16 yday $77.04 → 09:30 $76.64 -6.40; TDS×34 yday $37.83 → 09:30 $37.75 -2.72 | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 4 | $253.72 | $2.02 | $-42.58 | $1,623.26 | ▼ -42.58 after sell → book $10,439.44; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TARS` | 15 | $89.67 | $2.06 | $+100.46 | $2,966.25 | ▲ +100.46 after sell → book $10,437.38; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 2 | $521.15 | $2.02 | $+10.73 | $4,006.54 | ▲ +10.73 after sell → book $10,435.37; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 78 | $16.74 | $2.25 | $+22.05 | $5,310.01 | ▲ +22.05 after sell → book $10,433.12; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `IRD` | 284 | $4.53 | $3.72 | $-7.38 | $6,592.81 | ▼ -7.38 after sell → book $10,429.40; vs 09:30 mark -3.72 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LENZ` | 223 | $5.95 | $2.92 | $+38.80 | $7,916.73 | ▲ +38.80 after sell → book $10,426.47; vs 09:30 mark -2.93 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `PIPR` | 16 | $76.64 | $2.06 | $-2.66 | $9,140.92 | ▼ -2.66 after sell → book $10,424.42; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TDS` | 34 | $37.75 | $2.11 | $+6.34 | $10,422.30 | ▲ +6.34 after sell → book $10,422.30; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,422.30 | ▲ close $10,422.30 vs 09:30 $10,441.46 (session +0.00) | 16:00 close · cash $10,422.30 · no lots left · equity $10,422.30. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,422.30 | ▲ 09:30 equity $10,422.30 vs yday $10,422.30 (+0.00) | 09:30 open · cash $10,422.30 · no holdings · equity $10,422.30 vs prior close $10,422.30 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,422.30 | ▲ close $10,422.30 vs 09:30 $10,422.30 (session +0.00) | 16:00 close · cash $10,422.30 · no lots left · equity $10,422.30. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,422.30 | ▲ 09:30 equity $10,422.30 vs yday $10,422.30 (+0.00) | 09:30 open · cash $10,422.30 · no holdings · equity $10,422.30 vs prior close $10,422.30 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,422.30 | ▲ close $10,422.30 vs 09:30 $10,422.30 (session +0.00) | 16:00 close · cash $10,422.30 · no lots left · equity $10,422.30. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,422.30 | ▲ 09:30 equity $10,422.30 vs yday $10,422.30 (+0.00) | 09:30 open · cash $10,422.30 · no holdings · equity $10,422.30 vs prior close $10,422.30 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 49 | $52.55 | $2.14 | — | $7,845.22 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $2605.58 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `PAGS` | 257 | $10.11 | $3.32 | — | $5,243.63 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+2.9; leftover $2605.58 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `PAYP` | 142 | $18.30 | $2.42 | — | $2,642.62 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $2605.58 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ZSQR` | 801 | $3.25 | $10.33 | — | $29.03 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+3.6; leftover $2605.58 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟡 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $29.03 | ▲ close $10,495.47 vs 09:30 $10,422.30 (session +91.37) | 16:00 close · cash $29.03 · equity $10,495.47 vs 09:30 $10,422.30 (+73.17; session marks +91.37) · 4 name(s) marked open→close (per-name table). BAND×49 09:30 $52.55 → close $56.87 +211.68; PAGS×257 09:30 $10.11 → close $10.12 +2.57; PAYP×142 09:30 $18.30 → close $18.45 +21.30; ZSQR×801 09:30 $3.25 → close $3.07 -144.18 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29.03 | ▼ 09:30 equity $10,433.95 vs yday $10,495.47 (-61.52) | 09:30 open · cash $29.03 (unchanged overnight, no fees) · equity $10,433.95 vs prior close $10,495.47 (-61.52) · 4 name(s) re-marked at the open (per-name table). BAND×49 yday $56.87 → 09:30 $56.90 +1.47; PAGS×257 yday $10.12 → 09:30 $10.00 -30.84; PAYP×142 yday $18.45 → 09:30 $18.28 -24.14; ZSQR×801 yday $3.07 → 09:30 $3.06 -8.01 | — |
| 2026-09-14 09:30 ET | **SELL** | `BAND` | 49 | $56.90 | $2.17 | $+208.84 | $2,814.96 | ▲ +208.84 after sell → book $10,431.78; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `PAGS` | 257 | $10.00 | $3.38 | $-34.96 | $5,381.58 | ▼ -34.96 after sell → book $10,428.40; vs 09:30 mark -3.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `PAYP` | 142 | $18.28 | $2.46 | $-7.72 | $7,974.88 | ▼ -7.72 after sell → book $10,425.94; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ZSQR` | 801 | $3.06 | $10.49 | $-173.01 | $10,415.46 | ▼ -173.01 after sell → book $10,415.46; vs 09:30 mark -10.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,415.46 | ▲ close $10,415.46 vs 09:30 $10,433.95 (session +0.00) | 16:00 close · cash $10,415.46 · no lots left · equity $10,415.46. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,415.46 | ▲ 09:30 equity $10,415.46 vs yday $10,415.46 (-0.00) | 09:30 open · cash $10,415.46 · no holdings · equity $10,415.46 vs prior close $10,415.46 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,415.46 | ▲ close $10,415.46 vs 09:30 $10,415.46 (session +0.00) | 16:00 close · cash $10,415.46 · no lots left · equity $10,415.46. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,415.46 | ▲ 09:30 equity $10,415.46 vs yday $10,415.46 (-0.00) | 09:30 open · cash $10,415.46 · no holdings · equity $10,415.46 vs prior close $10,415.46 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `QRVO` | 44 | $118.18 | $2.12 | — | $5,213.42 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $5207.73 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 58 | $89.38 | $2.16 | — | $27.21 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $5207.73 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $27.21 | ▼ close $10,006.11 vs 09:30 $10,415.46 (session -405.06) | 16:00 close · cash $27.21 · equity $10,006.11 vs 09:30 $10,415.46 (-409.35; session marks -405.06) · 2 name(s) marked open→close (per-name table). QRVO×44 09:30 $118.18 → close $113.97 -185.24; SWKS×58 09:30 $89.38 → close $85.59 -219.82 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $27.21 | ▲ 09:30 equity $10,114.89 vs yday $10,006.11 (+108.78) | 09:30 open · cash $27.21 (unchanged overnight, no fees) · equity $10,114.89 vs prior close $10,006.11 (+108.78) · 2 name(s) re-marked at the open (per-name table). QRVO×44 yday $113.97 → 09:30 $114.90 +40.92; SWKS×58 yday $85.59 → 09:30 $86.76 +67.86 | — |
| 2026-09-17 09:30 ET | **SELL** | `QRVO` | 44 | $114.90 | $2.17 | $-148.61 | $5,080.64 | ▼ -148.61 after sell → book $10,112.72; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 58 | $86.76 | $2.21 | $-156.34 | $10,110.51 | ▼ -156.34 after sell → book $10,110.51; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,110.51 | ▲ close $10,110.51 vs 09:30 $10,114.89 (session +0.00) | 16:00 close · cash $10,110.51 · no lots left · equity $10,110.51. | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,110.51 | ▲ 09:30 equity $10,110.51 vs yday $10,110.51 (-0.00) | 09:30 open · cash $10,110.51 · no holdings · equity $10,110.51 vs prior close $10,110.51 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-18 09:30 ET | **BUY** | `CRWD` | 5 | $246.98 | $2.00 | — | $8,873.60 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1263.81 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 36 | $34.44 | $2.10 | — | $7,631.66 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten; 🔵; ⚪; ret5=+14.0; leftover $1263.81 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-18 09:30 ET | **BUY** | `ATRC` | 21 | $58.51 | $2.05 | — | $6,400.90 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.6; leftover $1263.81 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 14 | $85.00 | $2.03 | — | $5,208.87 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten; 🔵; ⚪; ret5=+18.3; leftover $1263.81 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `PGEN` | 158 | $7.98 | $2.46 | — | $3,945.56 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+17.8; leftover $1263.81 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 11 | $108.55 | $2.02 | — | $2,749.49 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten; ⚪; ret5=+21.3; leftover $1263.81 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `RXT` | 320 | $3.94 | $4.13 | — | $1,484.56 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+25.2; leftover $1263.81 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 60 | $20.91 | $2.17 | — | $227.79 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $1263.81 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $227.79 | ▼ close $9,884.81 vs 09:30 $10,110.51 (session -206.72) | 16:00 close · cash $227.79 · equity $9,884.81 vs 09:30 $10,110.51 (-225.70; session marks -206.72) · 8 name(s) marked open→close (per-name table). CRWD×5 09:30 $246.98 → close $237.65 -46.65; FIVN×36 09:30 $34.44 → close $32.47 -70.92; ATRC×21 09:30 $58.51 → close $58.10 -8.61; ECO×14 09:30 $85.00 → close $84.95 -0.70; PGEN×158 09:30 $7.98 → close $7.78 -31.60; RBRK×11 09:30 $108.55 → close $106.71 -20.24; RXT×320 09:30 $3.94 → close $3.80 -44.80; TH×60 09:30 $20.91 → close $21.19 +16.80 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $227.79 | ▲ 09:30 equity $9,925.33 vs yday $9,884.81 (+40.52) | 09:30 open · cash $227.79 (unchanged overnight, no fees) · equity $9,925.33 vs prior close $9,884.81 (+40.52) · 8 name(s) re-marked at the open (per-name table). CRWD×5 yday $237.65 → 09:30 $231.62 -30.15; FIVN×36 yday $32.47 → 09:30 $33.00 +19.08; ATRC×21 yday $58.10 → 09:30 $58.23 +2.73; ECO×14 yday $84.95 → 09:30 $82.83 -29.68; PGEN×158 yday $7.78 → 09:30 $7.84 +9.48; RBRK×11 yday $106.71 → 09:30 $107.57 +9.46; RXT×320 yday $3.80 → 09:30 $3.90 +32.00; TH×60 yday $21.19 → 09:30 $21.65 +27.60 | — |
| 2026-09-21 09:30 ET | **SELL** | `CRWD` | 5 | $231.62 | $2.02 | $-80.83 | $1,383.87 | ▼ -80.83 after sell → book $9,923.31; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FIVN` | 36 | $33.00 | $2.12 | $-56.06 | $2,569.75 | ▼ -56.06 after sell → book $9,921.19; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ATRC` | 21 | $58.23 | $2.07 | $-10.01 | $3,790.51 | ▼ -10.01 after sell → book $9,919.12; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ECO` | 14 | $82.83 | $2.05 | $-34.46 | $4,948.07 | ▼ -34.46 after sell → book $9,917.06; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `PGEN` | 158 | $7.84 | $2.50 | $-27.08 | $6,184.29 | ▼ -27.08 after sell → book $9,914.56; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **SELL** | `RBRK` | 11 | $107.57 | $2.04 | $-14.85 | $7,365.52 | ▼ -14.85 after sell → book $9,912.52; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RXT` | 320 | $3.90 | $4.19 | $-21.12 | $8,609.33 | ▼ -21.12 after sell → book $9,908.33; vs 09:30 mark -4.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 60 | $21.65 | $2.19 | $+40.04 | $9,906.14 | ▲ +40.04 after sell → book $9,906.14; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,906.14 | ▲ close $9,906.14 vs 09:30 $9,925.33 (session +0.00) | 16:00 close · cash $9,906.14 · no lots left · equity $9,906.14. | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,906.14 | ▲ 09:30 equity $9,906.14 vs yday $9,906.14 (+0.00) | 09:30 open · cash $9,906.14 · no holdings · equity $9,906.14 vs prior close $9,906.14 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,906.14 | ▲ close $9,906.14 vs 09:30 $9,906.14 (session +0.00) | 16:00 close · cash $9,906.14 · no lots left · equity $9,906.14. | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,906.14 | ▲ 09:30 equity $9,906.14 vs yday $9,906.14 (+0.00) | 09:30 open · cash $9,906.14 · no holdings · equity $9,906.14 vs prior close $9,906.14 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-23 09:30 ET | **BUY** | `A` | 7 | $166.54 | $2.01 | — | $8,738.35 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten; 🔵; ⚪; ret5=+10.3; leftover $1238.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-23 09:30 ET | **BUY** | `BFLY` | 125 | $9.90 | $2.37 | — | $7,498.48 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-4.7; leftover $1238.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 59 | $20.65 | $2.17 | — | $6,278.20 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1238.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-23 09:30 ET | **BUY** | `AMRX` | 62 | $19.70 | $2.18 | — | $5,054.63 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+17.1; leftover $1238.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 44 | $27.79 | $2.12 | — | $3,829.75 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten; 🔵; ⚪; ret5=+7.0; leftover $1238.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-23 09:30 ET | **BUY** | `FIVN` | 31 | $38.99 | $2.08 | — | $2,618.97 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+16.6; leftover $1238.27 | join🟢 sector🟡 gen🟢 news🔴 digest🟡 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-23 09:30 ET | **BUY** | `INOD` | 17 | $70.84 | $2.04 | — | $1,412.65 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-0.5; leftover $1238.27 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 6 | $196.78 | $2.01 | — | $229.96 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list earn_react; 🔵; ⚪; ret5=-0.6; leftover $1238.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟡 heat🟢 vol🟡 buy🟡 |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $229.96 | ▼ close $9,663.49 vs 09:30 $9,906.14 (session -225.67) | 16:00 close · cash $229.96 · equity $9,663.49 vs 09:30 $9,906.14 (-242.65; session marks -225.67) · 8 name(s) marked open→close (per-name table). A×7 09:30 $166.54 → close $165.28 -8.82; BFLY×125 09:30 $9.90 → close $9.20 -87.50; OMER×59 09:30 $20.65 → close $20.74 +5.55; AMRX×62 09:30 $19.70 → close $19.33 -22.94; ARQT×44 09:30 $27.79 → close $26.38 -62.04; FIVN×31 09:30 $38.99 → close $37.68 -40.61; INOD×17 09:30 $70.84 → close $71.99 +19.55; CTAS×6 09:30 $196.78 → close $191.97 -28.86 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1281.39 < 1 share @ 1646.93 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `A` | 7 | 2026-09-23 @ $166.54 | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten; 🔵; ⚪; ret5=+10.3; leftover $1238.27 |
| `BFLY` | 125 | 2026-09-23 @ $9.90 | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-4.7; leftover $1238.27 |
| `OMER` | 59 | 2026-09-23 @ $20.65 | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1238.27 |
| `AMRX` | 62 | 2026-09-23 @ $19.70 | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+17.1; leftover $1238.27 |
| `ARQT` | 44 | 2026-09-23 @ $27.79 | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten; 🔵; ⚪; ret5=+7.0; leftover $1238.27 |
| `FIVN` | 31 | 2026-09-23 @ $38.99 | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+16.6; leftover $1238.27 |
| `INOD` | 17 | 2026-09-23 @ $70.84 | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-0.5; leftover $1238.27 |
| `CTAS` | 6 | 2026-09-23 @ $196.78 | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list earn_react; 🔵; ⚪; ret5=-0.6; leftover $1238.27 |
