# Factor mine action — `union_white_any_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · −0 red + (yday up or major catalyst), then Score

Cash book **-9.25%** ($9,075) · signal-only (no cash/fees) was -5.52%. Starts YES **0/26**. Fills 166 · skips 0 · realized $-915.34.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: the morning-board Score (100 minus list rank) — only after the pool is chosen.
- Must-have: at most 0 red cameras (the −R half of +G −R; 🚨 is not counted here).
- Must-have: yesterday's session was up, or a major good catalyst (EPS beat / catal green / earnings-react that is not a miss).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Sort the keepers by the morning-board Score (100 minus list rank) — only after the pool is chosen and keep the top 8.
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
- **Gate** `cam_bad_max=0,yday_or_catalyst=True` · **rank** `list` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $251.13.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `BTSG` | 27 | — | $59.80 | +0.00 | $60.23 | +11.61 | +11.61 | +0.00 | +11.61 |
| 2026-08-13 | `IREN` | 36 | — | $45.98 | +0.00 | $44.76 | -43.92 | -43.92 | +0.00 | -43.92 |
| 2026-08-13 | `TPG` | 32 | — | $50.62 | +0.00 | $54.62 | +127.90 | +127.90 | +0.00 | +127.90 |
| 2026-08-13 | `SLS` | 142 | — | $11.70 | +0.00 | $12.36 | +93.72 | +93.72 | +0.00 | +93.72 |
| 2026-08-13 | `INO` | 2057 | — | $0.81 | +0.00 | $0.90 | +185.13 | +185.13 | +0.00 | +185.13 |
| 2026-08-13 | `TNDM` | 71 | — | $23.33 | +0.00 | $23.13 | -14.20 | -14.20 | +0.00 | -14.20 |
| 2026-08-14 | `BTSG` | 27 | $60.23 | $59.65 | -15.66 | — | +0.00 | -15.66 | -4.05 | — |
| 2026-08-14 | `IREN` | 36 | $44.76 | $44.09 | -24.12 | — | +0.00 | -24.12 | -68.04 | — |
| 2026-08-14 | `TPG` | 32 | $54.62 | $55.29 | +21.44 | — | +0.00 | +21.44 | +149.34 | — |
| 2026-08-14 | `SLS` | 142 | $12.36 | $12.40 | +5.68 | — | +0.00 | +5.68 | +99.40 | — |
| 2026-08-14 | `INO` | 2057 | $0.90 | $0.93 | +61.71 | — | +0.00 | +61.71 | +246.84 | — |
| 2026-08-14 | `TNDM` | 71 | $23.13 | $22.92 | -14.91 | — | +0.00 | -14.91 | -29.11 | — |
| 2026-08-14 | `DAVE` | 3 | — | $330.91 | +0.00 | $334.57 | +10.98 | +10.98 | +0.00 | +10.98 |
| 2026-08-14 | `SLG` | 22 | — | $57.61 | +0.00 | $56.09 | -33.44 | -33.44 | +0.00 | -33.44 |
| 2026-08-14 | `LDI` | 1377 | — | $0.94 | +0.00 | $0.90 | -55.08 | -55.08 | +0.00 | -55.08 |
| 2026-08-14 | `BTBT` | 860 | — | $1.50 | +0.00 | $1.57 | +60.20 | +60.20 | +0.00 | +60.20 |
| 2026-08-14 | `BETR` | 87 | — | $14.80 | +0.00 | $13.73 | -93.09 | -93.09 | +0.00 | -93.09 |
| 2026-08-14 | `ANGX` | 299 | — | $4.31 | +0.00 | $4.37 | +17.94 | +17.94 | +0.00 | +17.94 |
| 2026-08-14 | `HYLN` | 308 | — | $4.18 | +0.00 | $4.06 | -36.96 | -36.96 | +0.00 | -36.96 |
| 2026-08-14 | `WDC` | 2 | — | $503.50 | +0.00 | $508.80 | +10.60 | +10.60 | +0.00 | +10.60 |
| 2026-08-17 | `DAVE` | 3 | $334.57 | $336.94 | +7.11 | — | +0.00 | +7.11 | +18.09 | — |
| 2026-08-17 | `SLG` | 22 | $56.09 | $55.37 | -15.84 | — | +0.00 | -15.84 | -49.28 | — |
| 2026-08-17 | `LDI` | 1377 | $0.90 | $0.91 | +13.77 | — | +0.00 | +13.77 | -41.31 | — |
| 2026-08-17 | `BTBT` | 860 | $1.57 | $1.52 | -43.00 | — | +0.00 | -43.00 | +17.20 | — |
| 2026-08-17 | `BETR` | 87 | $13.73 | $13.67 | -5.22 | — | +0.00 | -5.22 | -98.31 | — |
| 2026-08-17 | `ANGX` | 299 | $4.37 | $4.60 | +68.77 | — | +0.00 | +68.77 | +86.71 | — |
| 2026-08-17 | `HYLN` | 308 | $4.06 | $4.10 | +12.32 | — | +0.00 | +12.32 | -24.64 | — |
| 2026-08-17 | `WDC` | 2 | $508.80 | $525.53 | +33.46 | — | +0.00 | +33.46 | +44.06 | — |
| 2026-08-17 | `DNN` | 393 | — | $3.24 | +0.00 | $3.19 | -19.65 | -19.65 | +0.00 | -19.65 |
| 2026-08-17 | `CDNL` | 31 | — | $39.85 | +0.00 | $39.23 | -19.22 | -19.22 | +0.00 | -19.22 |
| 2026-08-17 | `ABX` | 139 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `OCC` | 69 | — | $18.24 | +0.00 | $17.12 | -77.28 | -77.28 | +0.00 | -77.28 |
| 2026-08-17 | `ALM` | 78 | — | $16.20 | +0.00 | $16.36 | +12.48 | +12.48 | +0.00 | +12.48 |
| 2026-08-17 | `UMAC` | 39 | — | $32.55 | +0.00 | $30.15 | -93.60 | -93.60 | +0.00 | -93.60 |
| 2026-08-17 | `NPWR` | 663 | — | $1.92 | +0.00 | $1.73 | -125.97 | -125.97 | +0.00 | -125.97 |
| 2026-08-17 | `LPTH` | 85 | — | $14.94 | +0.00 | $14.80 | -11.90 | -11.90 | +0.00 | -11.90 |
| 2026-08-18 | `DNN` | 393 | $3.19 | $3.11 | -31.44 | — | +0.00 | -31.44 | -51.09 | — |
| 2026-08-18 | `CDNL` | 31 | $39.23 | $41.57 | +72.54 | — | +0.00 | +72.54 | +53.32 | — |
| 2026-08-18 | `ABX` | 139 | $9.12 | $9.03 | -12.51 | — | +0.00 | -12.51 | -12.51 | — |
| 2026-08-18 | `OCC` | 69 | $17.12 | $16.20 | -63.48 | — | +0.00 | -63.48 | -140.76 | — |
| 2026-08-18 | `ALM` | 78 | $16.36 | $15.78 | -45.24 | — | +0.00 | -45.24 | -32.76 | — |
| 2026-08-18 | `UMAC` | 39 | $30.15 | $28.59 | -60.84 | — | +0.00 | -60.84 | -154.44 | — |
| 2026-08-18 | `NPWR` | 663 | $1.73 | $1.70 | -19.89 | — | +0.00 | -19.89 | -145.86 | — |
| 2026-08-18 | `LPTH` | 85 | $14.80 | $14.01 | -67.15 | — | +0.00 | -67.15 | -79.05 | — |
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
| 2026-08-21 | `AUTL` | 506 | — | $2.47 | +0.00 | $2.41 | -30.36 | -30.36 | +0.00 | -30.36 |
| 2026-08-21 | `CRDL` | 648 | — | $1.93 | +0.00 | $1.86 | -45.36 | -45.36 | +0.00 | -45.36 |
| 2026-08-21 | `CRSP` | 20 | — | $59.72 | +0.00 | $59.50 | -4.40 | -4.40 | +0.00 | -4.40 |
| 2026-08-21 | `CYPH` | 947 | — | $1.32 | +0.00 | $1.42 | +94.70 | +94.70 | +0.00 | +94.70 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.51 | -7.10 | — | +0.00 | -7.10 | +10.80 | — |
| 2026-08-24 | `AUPH` | 72 | $16.65 | $16.57 | -5.76 | — | +0.00 | -5.76 | -45.36 | — |
| 2026-08-24 | `AEM` | 5 | $216.06 | $217.03 | +4.85 | — | +0.00 | +4.85 | +3.65 | — |
| 2026-08-24 | `ARCT` | 112 | $13.45 | $13.33 | -13.44 | — | +0.00 | -13.44 | +246.40 | — |
| 2026-08-24 | `AUTL` | 506 | $2.41 | $2.40 | -5.06 | — | +0.00 | -5.06 | -35.42 | — |
| 2026-08-24 | `CRDL` | 648 | $1.86 | $1.88 | +12.96 | — | +0.00 | +12.96 | -32.40 | — |
| 2026-08-24 | `CRSP` | 20 | $59.50 | $58.75 | -15.00 | — | +0.00 | -15.00 | -19.40 | — |
| 2026-08-24 | `CYPH` | 947 | $1.42 | $1.83 | +388.27 | — | +0.00 | +388.27 | +482.97 | — |
| 2026-08-25 | `MOS` | 55 | — | $23.77 | +0.00 | $24.27 | +27.50 | +27.50 | +0.00 | +27.50 |
| 2026-08-25 | `CRMD` | 157 | — | $8.35 | +0.00 | $8.56 | +32.97 | +32.97 | +0.00 | +32.97 |
| 2026-08-25 | `BMEA` | 808 | — | $1.63 | +0.00 | $1.73 | +80.80 | +80.80 | +0.00 | +80.80 |
| 2026-08-25 | `ALVO` | 251 | — | $5.24 | +0.00 | $5.05 | -47.69 | -47.69 | +0.00 | -47.69 |
| 2026-08-25 | `SUJA` | 149 | — | $8.79 | +0.00 | $9.33 | +80.46 | +80.46 | +0.00 | +80.46 |
| 2026-08-25 | `CYPH` | 844 | — | $1.56 | +0.00 | $1.64 | +67.52 | +67.52 | +0.00 | +67.52 |
| 2026-08-25 | `DEFT` | 2125 | — | $0.62 | +0.00 | $0.60 | -34.00 | -34.00 | +0.00 | -34.00 |
| 2026-08-25 | `ZURA` | 203 | — | $6.37 | +0.00 | $6.32 | -10.15 | -10.15 | +0.00 | -10.15 |
| 2026-08-26 | `MOS` | 55 | $24.27 | $24.84 | +31.35 | — | +0.00 | +31.35 | +58.85 | — |
| 2026-08-26 | `CRMD` | 157 | $8.56 | $8.60 | +6.28 | — | +0.00 | +6.28 | +39.25 | — |
| 2026-08-26 | `BMEA` | 808 | $1.73 | $1.75 | +20.20 | — | +0.00 | +20.20 | +101.00 | — |
| 2026-08-26 | `ALVO` | 251 | $5.05 | $4.98 | -17.57 | — | +0.00 | -17.57 | -65.26 | — |
| 2026-08-26 | `SUJA` | 149 | $9.33 | $9.39 | +8.94 | — | +0.00 | +8.94 | +89.40 | — |
| 2026-08-26 | `CYPH` | 844 | $1.64 | $1.60 | -33.76 | — | +0.00 | -33.76 | +33.76 | — |
| 2026-08-26 | `DEFT` | 2125 | $0.60 | $0.60 | -12.75 | — | +0.00 | -12.75 | -46.75 | — |
| 2026-08-26 | `ZURA` | 203 | $6.32 | $6.13 | -38.57 | — | +0.00 | -38.57 | -48.72 | — |
| 2026-08-27 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-28 | `SIMO` | 5 | — | $252.24 | +0.00 | $245.81 | -32.15 | -32.15 | +0.00 | -32.15 |
| 2026-08-28 | `SMTC` | 9 | — | $141.76 | +0.00 | $131.17 | -95.31 | -95.31 | +0.00 | -95.31 |
| 2026-08-28 | `TTMI` | 10 | — | $122.81 | +0.00 | $118.65 | -41.60 | -41.60 | +0.00 | -41.60 |
| 2026-08-28 | `KEYS` | 4 | — | $324.41 | +0.00 | $319.97 | -17.76 | -17.76 | +0.00 | -17.76 |
| 2026-08-28 | `AVT` | 14 | — | $91.49 | +0.00 | $88.63 | -40.04 | -40.04 | +0.00 | -40.04 |
| 2026-08-28 | `CGNX` | 21 | — | $62.82 | +0.00 | $60.46 | -49.56 | -49.56 | +0.00 | -49.56 |
| 2026-08-28 | `COHR` | 4 | — | $289.44 | +0.00 | $279.20 | -40.96 | -40.96 | +0.00 | -40.96 |
| 2026-08-28 | `LSCC` | 11 | — | $119.76 | +0.00 | $114.40 | -58.96 | -58.96 | +0.00 | -58.96 |
| 2026-08-31 | `SIMO` | 5 | $245.81 | $247.05 | +6.20 | — | +0.00 | +6.20 | -25.95 | — |
| 2026-08-31 | `SMTC` | 9 | $131.17 | $132.30 | +10.17 | — | +0.00 | +10.17 | -85.14 | — |
| 2026-08-31 | `TTMI` | 10 | $118.65 | $118.83 | +1.80 | — | +0.00 | +1.80 | -39.80 | — |
| 2026-08-31 | `KEYS` | 4 | $319.97 | $322.49 | +10.08 | — | +0.00 | +10.08 | -7.68 | — |
| 2026-08-31 | `AVT` | 14 | $88.63 | $89.39 | +10.64 | — | +0.00 | +10.64 | -29.40 | — |
| 2026-08-31 | `CGNX` | 21 | $60.46 | $60.46 | +0.00 | — | +0.00 | +0.00 | -49.56 | — |
| 2026-08-31 | `COHR` | 4 | $279.20 | $280.25 | +4.20 | — | +0.00 | +4.20 | -36.76 | — |
| 2026-08-31 | `LSCC` | 11 | $114.40 | $115.56 | +12.76 | — | +0.00 | +12.76 | -46.20 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 24 | — | $52.88 | +0.00 | $52.46 | -10.08 | -10.08 | +0.00 | -10.08 |
| 2026-09-03 | `HRMY` | 29 | — | $42.93 | +0.00 | $41.86 | -31.03 | -31.03 | +0.00 | -31.03 |
| 2026-09-03 | `CABA` | 352 | — | $3.63 | +0.00 | $3.48 | -52.80 | -52.80 | +0.00 | -52.80 |
| 2026-09-03 | `VSTM` | 159 | — | $8.03 | +0.00 | $7.98 | -7.95 | -7.95 | +0.00 | -7.95 |
| 2026-09-03 | `RVTY` | 9 | — | $132.45 | +0.00 | $130.63 | -16.38 | -16.38 | +0.00 | -16.38 |
| 2026-09-03 | `ARCT` | 76 | — | $16.77 | +0.00 | $15.56 | -91.96 | -91.96 | +0.00 | -91.96 |
| 2026-09-03 | `SLN` | 86 | — | $14.85 | +0.00 | $14.79 | -5.16 | -5.16 | +0.00 | -5.16 |
| 2026-09-03 | `CRDL` | 587 | — | $2.18 | +0.00 | $2.16 | -11.74 | -11.74 | +0.00 | -11.74 |
| 2026-09-04 | `ATRC` | 24 | $52.46 | $52.03 | -10.32 | $51.52 | -12.24 | -22.56 | -20.40 | -32.64 |
| 2026-09-04 | `HRMY` | 29 | $41.86 | $41.50 | -10.44 | — | +0.00 | -10.44 | -41.47 | — |
| 2026-09-04 | `CABA` | 352 | $3.48 | $3.46 | -7.04 | $3.47 | +3.52 | -3.52 | -59.84 | -56.32 |
| 2026-09-04 | `VSTM` | 159 | $7.98 | $7.91 | -11.13 | — | +0.00 | -11.13 | -19.08 | — |
| 2026-09-04 | `RVTY` | 9 | $130.63 | $130.03 | -5.40 | — | +0.00 | -5.40 | -21.78 | — |
| 2026-09-04 | `ARCT` | 76 | $15.56 | $15.61 | +3.80 | — | +0.00 | +3.80 | -88.16 | — |
| 2026-09-04 | `SLN` | 86 | $14.79 | $14.63 | -13.76 | — | +0.00 | -13.76 | -18.92 | — |
| 2026-09-04 | `CRDL` | 587 | $2.16 | $2.16 | +0.00 | — | +0.00 | +0.00 | -11.74 | — |
| 2026-09-04 | `ALEC` | 492 | — | $2.52 | +0.00 | $2.46 | -29.52 | -29.52 | +0.00 | -29.52 |
| 2026-09-04 | `BHC` | 185 | — | $6.71 | +0.00 | $6.56 | -27.75 | -27.75 | +0.00 | -27.75 |
| 2026-09-04 | `BMEA` | 653 | — | $1.90 | +0.00 | $2.03 | +84.89 | +84.89 | +0.00 | +84.89 |
| 2026-09-04 | `OABI` | 259 | — | $4.78 | +0.00 | $4.33 | -116.55 | -116.55 | +0.00 | -116.55 |
| 2026-09-04 | `OPK` | 780 | — | $1.59 | +0.00 | $1.64 | +39.00 | +39.00 | +0.00 | +39.00 |
| 2026-09-04 | `VIR` | 107 | — | $11.31 | +0.00 | $11.38 | +8.02 | +8.02 | +0.00 | +8.02 |
| 2026-09-08 | `ATRC` | 24 | $51.52 | $54.31 | +66.96 | — | +0.00 | +66.96 | +34.32 | — |
| 2026-09-08 | `CABA` | 352 | $3.47 | $3.43 | -14.08 | — | +0.00 | -14.08 | -70.40 | — |
| 2026-09-08 | `ALEC` | 492 | $2.46 | $2.38 | -39.36 | — | +0.00 | -39.36 | -68.88 | — |
| 2026-09-08 | `BHC` | 185 | $6.56 | $6.57 | +1.85 | — | +0.00 | +1.85 | -25.90 | — |
| 2026-09-08 | `BMEA` | 653 | $2.03 | $2.00 | -19.59 | — | +0.00 | -19.59 | +65.30 | — |
| 2026-09-08 | `OABI` | 259 | $4.33 | $4.30 | -7.77 | — | +0.00 | -7.77 | -124.32 | — |
| 2026-09-08 | `OPK` | 780 | $1.64 | $1.63 | -7.80 | — | +0.00 | -7.80 | +31.20 | — |
| 2026-09-08 | `VIR` | 107 | $11.38 | $11.22 | -17.65 | — | +0.00 | -17.65 | -9.63 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `ORCL` | 9 | — | $164.43 | +0.00 | $150.28 | -127.35 | -127.35 | +0.00 | -127.35 |
| 2026-09-11 | `BAND` | 30 | — | $52.55 | +0.00 | $56.87 | +129.60 | +129.60 | +0.00 | +129.60 |
| 2026-09-11 | `PAGS` | 160 | — | $10.11 | +0.00 | $10.12 | +1.60 | +1.60 | +0.00 | +1.60 |
| 2026-09-11 | `ZSQR` | 500 | — | $3.25 | +0.00 | $3.07 | -90.00 | -90.00 | +0.00 | -90.00 |
| 2026-09-11 | `PAYP` | 88 | — | $18.30 | +0.00 | $18.45 | +13.20 | +13.20 | +0.00 | +13.20 |
| 2026-09-11 | `SEDG` | 44 | — | $36.78 | +0.00 | $34.68 | -92.40 | -92.40 | +0.00 | -92.40 |
| 2026-09-14 | `ORCL` | 9 | $150.28 | $141.42 | -79.74 | — | +0.00 | -79.74 | -207.09 | — |
| 2026-09-14 | `BAND` | 30 | $56.87 | $56.90 | +0.90 | — | +0.00 | +0.90 | +130.50 | — |
| 2026-09-14 | `PAGS` | 160 | $10.12 | $10.00 | -19.20 | — | +0.00 | -19.20 | -17.60 | — |
| 2026-09-14 | `ZSQR` | 500 | $3.07 | $3.06 | -5.00 | — | +0.00 | -5.00 | -95.00 | — |
| 2026-09-14 | `PAYP` | 88 | $18.45 | $18.28 | -14.96 | — | +0.00 | -14.96 | -1.76 | — |
| 2026-09-14 | `SEDG` | 44 | $34.68 | $33.64 | -45.76 | — | +0.00 | -45.76 | -138.16 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `SWKS` | 52 | — | $89.38 | +0.00 | $85.59 | -197.08 | -197.08 | +0.00 | -197.08 |
| 2026-09-16 | `QRVO` | 39 | — | $118.18 | +0.00 | $113.97 | -164.19 | -164.19 | +0.00 | -164.19 |
| 2026-09-17 | `SWKS` | 52 | $85.59 | $86.76 | +60.84 | — | +0.00 | +60.84 | -136.24 | — |
| 2026-09-17 | `QRVO` | 39 | $113.97 | $114.90 | +36.27 | — | +0.00 | +36.27 | -127.92 | — |
| 2026-09-17 | `BULL` | 382 | — | $7.95 | +0.00 | $7.71 | -91.68 | -91.68 | +0.00 | -91.68 |
| 2026-09-17 | `VOD` | 173 | — | $17.56 | +0.00 | $17.52 | -6.92 | -6.92 | +0.00 | -6.92 |
| 2026-09-17 | `ASAN` | 317 | — | $9.55 | +0.00 | $10.09 | +171.18 | +171.18 | +0.00 | +171.18 |
| 2026-09-18 | `BULL` | 382 | $7.71 | $7.85 | +53.48 | — | +0.00 | +53.48 | -38.20 | — |
| 2026-09-18 | `VOD` | 173 | $17.52 | $16.73 | -136.67 | — | +0.00 | -136.67 | -143.59 | — |
| 2026-09-18 | `ASAN` | 317 | $10.09 | $10.09 | +0.00 | — | +0.00 | +0.00 | +171.18 | — |
| 2026-09-18 | `ILMN` | 4 | — | $249.13 | +0.00 | $239.62 | -38.04 | -38.04 | +0.00 | -38.04 |
| 2026-09-18 | `SDGR` | 38 | — | $29.32 | +0.00 | $29.02 | -11.40 | -11.40 | +0.00 | -11.40 |
| 2026-09-18 | `ARQT` | 43 | — | $26.14 | +0.00 | $25.38 | -32.68 | -32.68 | +0.00 | -32.68 |
| 2026-09-18 | `FTRE` | 56 | — | $20.10 | +0.00 | $19.93 | -9.52 | -9.52 | +0.00 | -9.52 |
| 2026-09-18 | `RARE` | 76 | — | $14.79 | +0.00 | $14.51 | -21.28 | -21.28 | +0.00 | -21.28 |
| 2026-09-18 | `CYPH` | 374 | — | $3.04 | +0.00 | $3.60 | +211.31 | +211.31 | +0.00 | +211.31 |
| 2026-09-18 | `TEM` | 13 | — | $81.40 | +0.00 | $77.84 | -46.28 | -46.28 | +0.00 | -46.28 |
| 2026-09-18 | `RXT` | 288 | — | $3.94 | +0.00 | $3.80 | -40.32 | -40.32 | +0.00 | -40.32 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +360.24 | BTSG, IREN, TPG, SLS, INO, TNDM | — | $92.47 | $10,326.53 | BTSG×27, IREN×36, TPG×32, SLS×142, INO×2057, TNDM×71 |
| 2026-08-14 | +5.50 | $92.47 | BTSG×27, IREN×36, TPG×32, SLS×142, INO×2057, TNDM×71 | $10,360.67 | +34.14 | -118.85 | DAVE, SLG, LDI, BTBT, BETR, ANGX, HYLN, WDC | BTSG, IREN, TPG, SLS, INO, TNDM | $568.62 | $10,160.90 | DAVE×3, SLG×22, LDI×1377, BTBT×860, BETR×87, ANGX×299, HYLN×308, WDC×2 |
| 2026-08-17 | +2.25 | $568.62 | DAVE×3, SLG×22, LDI×1377, BTBT×860, BETR×87, ANGX×299, HYLN×308, WDC×2 | $10,232.27 | +71.37 | -335.14 | DNN, CDNL, ABX, OCC, ALM, UMAC, NPWR, LPTH | DAVE, SLG, LDI, BTBT, BETR, ANGX, HYLN, WDC | $50.12 | $9,825.80 | DNN×393, CDNL×31, ABX×139, OCC×69, ALM×78, UMAC×39, NPWR×663, LPTH×85 |
| 2026-08-18 | -6.20 | $50.12 | DNN×393, CDNL×31, ABX×139, OCC×69, ALM×78, UMAC×39, NPWR×663, LPTH×85 | $9,597.79 | -228.01 | +0.00 | — | DNN, CDNL, ABX, OCC, ALM, UMAC, NPWR, LPTH | $9,570.56 | $9,570.56 | — |
| 2026-08-19 | -7.20 | $9,570.56 | — | $9,570.56 | +0.00 | +0.00 | — | — | $9,570.56 | $9,570.56 | — |
| 2026-08-20 | +1.12 | $9,570.56 | — | $9,570.56 | +0.00 | +225.88 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $85.40 | $9,772.32 | AG×58, BHP×13, CDE×57, HDSN×207, IAG×60, KGC×40, NFGC×683, WPM×8 |
| 2026-08-21 | +3.25 | $85.40 | AG×58, BHP×13, CDE×57, HDSN×207, IAG×60, KGC×40, NFGC×683, WPM×8 | $10,030.57 | +258.25 | +251.52 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $262.78 | $10,219.96 | AU×10, AUPH×72, AEM×5, ARCT×112, AUTL×506, CRDL×648, CRSP×20, CYPH×947 |
| 2026-08-24 | -5.17 | $262.78 | AU×10, AUPH×72, AEM×5, ARCT×112, AUTL×506, CRDL×648, CRSP×20, CYPH×947 | $10,579.68 | +359.72 | +0.00 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $10,541.47 | $10,541.47 | — |
| 2026-08-25 | +1.80 | $10,541.47 | — | $10,541.47 | +0.00 | +197.41 | MOS, CRMD, BMEA, ALVO, SUJA, CYPH, DEFT, ZURA | — | $0.16 | $10,685.11 | MOS×55, CRMD×157, BMEA×808, ALVO×251, SUJA×149, CYPH×844, DEFT×2125, ZURA×203 |
| 2026-08-26 | +2.02 | $0.16 | MOS×55, CRMD×157, BMEA×808, ALVO×251, SUJA×149, CYPH×844, DEFT×2125, ZURA×203 | $10,649.23 | -35.88 | +0.00 | — | MOS, CRMD, BMEA, ALVO, SUJA, CYPH, DEFT, ZURA | $10,595.08 | $10,595.08 | — |
| 2026-08-27 | — | $10,595.08 | — | $10,595.08 | +0.00 | +0.00 | — | — | $10,595.08 | $10,595.08 | — |
| 2026-08-28 | +0.75 | $10,595.08 | — | $10,595.08 | +0.00 | -376.34 | SIMO, SMTC, TTMI, KEYS, AVT, CGNX, COHR, LSCC | — | $440.95 | $10,202.59 | SIMO×5, SMTC×9, TTMI×10, KEYS×4, AVT×14, CGNX×21, COHR×4, LSCC×11 |
| 2026-08-31 | -5.85 | $440.95 | SIMO×5, SMTC×9, TTMI×10, KEYS×4, AVT×14, CGNX×21, COHR×4, LSCC×11 | $10,258.44 | +55.85 | +0.00 | — | SIMO, SMTC, TTMI, KEYS, AVT, CGNX, COHR, LSCC | $10,242.12 | $10,242.12 | — |
| 2026-09-01 | -6.30 | $10,242.12 | — | $10,242.12 | +0.00 | +0.00 | — | — | $10,242.12 | $10,242.12 | — |
| 2026-09-02 | -3.83 | $10,242.12 | — | $10,242.12 | +0.00 | +0.00 | — | — | $10,242.12 | $10,242.12 | — |
| 2026-09-03 | -0.90 | $10,242.12 | — | $10,242.12 | +0.00 | -227.10 | ATRC, HRMY, CABA, VSTM, RVTY, ARCT, SLN, CRDL | — | $124.97 | $9,989.82 | ATRC×24, HRMY×29, CABA×352, VSTM×159, RVTY×9, ARCT×76, SLN×86, CRDL×587 |
| 2026-09-04 | +2.25 | $124.97 | ATRC×24, HRMY×29, CABA×352, VSTM×159, RVTY×9, ARCT×76, SLN×86, CRDL×587 | $9,935.53 | -54.29 | -50.63 | ALEC, BHC, BMEA, OABI, OPK, VIR | HRMY, VSTM, RVTY, ARCT, SLN, CRDL | $6.75 | $9,833.05 | ATRC×24, CABA×352, ALEC×492, BHC×185, BMEA×653, OABI×259, OPK×780, VIR×107 |
| 2026-09-08 | -11.47 | $6.75 | ATRC×24, CABA×352, ALEC×492, BHC×185, BMEA×653, OABI×259, OPK×780, VIR×107 | $9,795.60 | -37.45 | +0.00 | — | ATRC, CABA, ALEC, BHC, BMEA, OABI, OPK, VIR | $9,755.41 | $9,755.41 | — |
| 2026-09-09 | -13.95 | $9,755.41 | — | $9,755.41 | -0.00 | +0.00 | — | — | $9,755.41 | $9,755.41 | — |
| 2026-09-10 | -13.28 | $9,755.41 | — | $9,755.41 | -0.00 | +0.00 | — | — | $9,755.41 | $9,755.41 | — |
| 2026-09-11 | +0.50 | $9,755.41 | — | $9,755.41 | -0.00 | -165.35 | ORCL, BAND, PAGS, ZSQR, PAYP, SEDG | — | $210.32 | $9,572.66 | ORCL×9, BAND×30, PAGS×160, ZSQR×500, PAYP×88, SEDG×44 |
| 2026-09-14 | -11.00 | $210.32 | ORCL×9, BAND×30, PAGS×160, ZSQR×500, PAYP×88, SEDG×44 | $9,408.90 | -163.76 | +0.00 | — | ORCL, BAND, PAGS, ZSQR, PAYP, SEDG | $9,391.28 | $9,391.28 | — |
| 2026-09-15 | -3.84 | $9,391.28 | — | $9,391.28 | +0.00 | +0.00 | — | — | $9,391.28 | $9,391.28 | — |
| 2026-09-16 | +5.30 | $9,391.28 | — | $9,391.28 | +0.00 | -361.27 | SWKS, QRVO | — | $130.25 | $9,025.76 | SWKS×52, QRVO×39 |
| 2026-09-17 | +7.38 | $130.25 | SWKS×52, QRVO×39 | $9,122.87 | +97.11 | +72.58 | BULL, VOD, ASAN | SWKS, QRVO | $4.87 | $9,179.58 | BULL×382, VOD×173, ASAN×317 |
| 2026-09-18 | +4.86 | $4.87 | BULL×382, VOD×173, ASAN×317 | $9,096.39 | -83.19 | +11.79 | ILMN, SDGR, ARQT, FTRE, RARE, CYPH, TEM, RXT | BULL, VOD, ASAN | $251.13 | $9,075.27 | ILMN×4, SDGR×38, ARQT×43, FTRE×56, RARE×76, CYPH×374, TEM×13, RXT×288 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 27 | $59.80 | $2.07 | — | $8,383.33 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; ⚪; ret5=-5.3; leftover $1666.67 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 36 | $45.98 | $2.10 | — | $6,725.95 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; ⚪; ret5=+12.3; leftover $1666.67 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 32 | $50.62 | $2.09 | — | $5,103.92 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; ⚪; ret5=+6.2; leftover $1666.67 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 142 | $11.70 | $2.42 | — | $3,440.11 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; ⚪; ret5=-0.8; leftover $1666.67 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 2057 | $0.81 | $22.83 | — | $1,751.10 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; ⚪; ret5=+13.2; leftover $1666.67 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 71 | $23.33 | $2.20 | — | $92.47 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; ⚪; ret5=+19.7; leftover $1666.67 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $92.47 | ▲ close $10,326.53 vs 09:30 $10,000.00 (session +360.24) | 16:00 close · cash $92.47 · equity $10,326.53 vs 09:30 $10,000.00 (+326.53; session marks +360.24) · 6 name(s) marked open→close (per-name table). BTSG×27 09:30 $59.80 → close $60.23 +11.61; IREN×36 09:30 $45.98 → close $44.76 -43.92; TPG×32 09:30 $50.62 → close $54.62 +127.90; SLS×142 09:30 $11.70 → close $12.36 +93.72; INO×2057 09:30 $0.81 → close $0.90 +185.13; TNDM×71 09:30 $23.33 → close $23.13 -14.20 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $92.47 | ▲ 09:30 equity $10,360.67 vs yday $10,326.53 (+34.14) | 09:30 open · cash $92.47 (unchanged overnight, no fees) · equity $10,360.67 vs prior close $10,326.53 (+34.14) · 6 name(s) re-marked at the open (per-name table). BTSG×27 yday $60.23 → 09:30 $59.65 -15.66; IREN×36 yday $44.76 → 09:30 $44.09 -24.12; TPG×32 yday $54.62 → 09:30 $55.29 +21.44; SLS×142 yday $12.36 → 09:30 $12.40 +5.68; INO×2057 yday $0.90 → 09:30 $0.93 +61.71; TNDM×71 yday $23.13 → 09:30 $22.92 -14.91 | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 27 | $59.65 | $2.09 | $-8.21 | $1,700.93 | ▼ -8.21 after sell → book $10,358.58; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 36 | $44.09 | $2.12 | $-72.26 | $3,286.05 | ▼ -72.26 after sell → book $10,356.46; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 32 | $55.29 | $2.11 | $+145.14 | $5,053.22 | ▲ +145.14 after sell → book $10,354.35; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 142 | $12.40 | $2.45 | $+94.53 | $6,811.56 | ▲ +94.53 after sell → book $10,351.89; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 2057 | $0.93 | $25.66 | $+198.35 | $8,698.91 | ▲ +198.35 after sell → book $10,326.23; vs 09:30 mark -25.66 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 71 | $22.92 | $2.23 | $-33.54 | $10,324.01 | ▼ -33.54 after sell → book $10,324.01; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $9,329.28 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1290.50 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 22 | $57.61 | $2.06 | — | $8,059.80 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+5.7; leftover $1290.50 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1377 | $0.94 | $17.03 | — | $6,752.52 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1290.50 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 860 | $1.50 | $11.09 | — | $5,451.43 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1290.50 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 87 | $14.80 | $2.25 | — | $4,161.57 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1290.50 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 299 | $4.31 | $3.86 | — | $2,869.03 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1290.50 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 308 | $4.18 | $3.97 | — | $1,577.61 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1290.50 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 2 | $503.50 | $2.00 | — | $568.62 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable; 🔵; ⚪; ret5=+7.9; leftover $1290.50 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $568.62 | ▼ close $10,160.90 vs 09:30 $10,360.67 (session -118.85) | 16:00 close · cash $568.62 · equity $10,160.90 vs 09:30 $10,360.67 (-199.77; session marks -118.85) · 8 name(s) marked open→close (per-name table). DAVE×3 09:30 $330.91 → close $334.57 +10.98; SLG×22 09:30 $57.61 → close $56.09 -33.44; LDI×1377 09:30 $0.94 → close $0.90 -55.08; BTBT×860 09:30 $1.50 → close $1.57 +60.20; BETR×87 09:30 $14.80 → close $13.73 -93.09; ANGX×299 09:30 $4.31 → close $4.37 +17.94; HYLN×308 09:30 $4.18 → close $4.06 -36.96; WDC×2 09:30 $503.50 → close $508.80 +10.60 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $568.62 | ▲ 09:30 equity $10,232.27 vs yday $10,160.90 (+71.37) | 09:30 open · cash $568.62 (unchanged overnight, no fees) · equity $10,232.27 vs prior close $10,160.90 (+71.37) · 8 name(s) re-marked at the open (per-name table). DAVE×3 yday $334.57 → 09:30 $336.94 +7.11; SLG×22 yday $56.09 → 09:30 $55.37 -15.84; LDI×1377 yday $0.90 → 09:30 $0.91 +13.77; BTBT×860 yday $1.57 → 09:30 $1.52 -43.00; BETR×87 yday $13.73 → 09:30 $13.67 -5.22; ANGX×299 yday $4.37 → 09:30 $4.60 +68.77; HYLN×308 yday $4.06 → 09:30 $4.10 +12.32; WDC×2 yday $508.80 → 09:30 $525.53 +33.46 | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 3 | $336.94 | $2.02 | $+14.07 | $1,577.42 | ▲ +14.07 after sell → book $10,230.25; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 22 | $55.37 | $2.08 | $-53.41 | $2,793.48 | ▼ -53.41 after sell → book $10,228.17; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 1377 | $0.91 | $16.86 | $-75.20 | $4,025.56 | ▼ -75.20 after sell → book $10,211.31; vs 09:30 mark -16.86 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 860 | $1.52 | $11.25 | $-5.14 | $5,321.52 | ▼ -5.14 after sell → book $10,200.07; vs 09:30 mark -11.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 87 | $13.67 | $2.28 | $-102.84 | $6,508.53 | ▼ -102.84 after sell → book $10,197.79; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 299 | $4.60 | $3.92 | $+78.94 | $7,880.01 | ▲ +78.94 after sell → book $10,193.87; vs 09:30 mark -3.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 308 | $4.10 | $4.03 | $-32.65 | $9,138.78 | ▼ -32.65 after sell → book $10,189.84; vs 09:30 mark -4.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 2 | $525.53 | $2.02 | $+40.05 | $10,187.82 | ▲ +40.05 after sell → book $10,187.82; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 393 | $3.24 | $5.07 | — | $8,909.43 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; ⚪; ret5=+0.3; leftover $1273.48 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 31 | $39.85 | $2.08 | — | $7,672.00 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1273.48 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 139 | $9.12 | $2.41 | — | $6,401.91 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1273.48 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 69 | $18.24 | $2.20 | — | $5,141.16 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $1273.48 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 78 | $16.20 | $2.22 | — | $3,875.33 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1273.48 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 39 | $32.55 | $2.11 | — | $2,603.77 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover,oppset; ⚪; ret5=+30.4; leftover $1273.48 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 663 | $1.92 | $8.55 | — | $1,322.26 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1273.48 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `LPTH` | 85 | $14.94 | $2.25 | — | $50.12 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $1273.48 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟢 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $50.12 | ▼ close $9,825.80 vs 09:30 $10,232.27 (session -335.14) | 16:00 close · cash $50.12 · equity $9,825.80 vs 09:30 $10,232.27 (-406.47; session marks -335.14) · 8 name(s) marked open→close (per-name table). DNN×393 09:30 $3.24 → close $3.19 -19.65; CDNL×31 09:30 $39.85 → close $39.23 -19.22; ABX×139 09:30 $9.12 → close $9.12 +0.00; OCC×69 09:30 $18.24 → close $17.12 -77.28; ALM×78 09:30 $16.20 → close $16.36 +12.48; UMAC×39 09:30 $32.55 → close $30.15 -93.60; NPWR×663 09:30 $1.92 → close $1.73 -125.97; LPTH×85 09:30 $14.94 → close $14.80 -11.90 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $50.12 | ▼ 09:30 equity $9,597.79 vs yday $9,825.80 (-228.01) | 09:30 open · cash $50.12 (unchanged overnight, no fees) · equity $9,597.79 vs prior close $9,825.80 (-228.01) · 8 name(s) re-marked at the open (per-name table). DNN×393 yday $3.19 → 09:30 $3.11 -31.44; CDNL×31 yday $39.23 → 09:30 $41.57 +72.54; ABX×139 yday $9.12 → 09:30 $9.03 -12.51; OCC×69 yday $17.12 → 09:30 $16.20 -63.48; ALM×78 yday $16.36 → 09:30 $15.78 -45.24; UMAC×39 yday $30.15 → 09:30 $28.59 -60.84; NPWR×663 yday $1.73 → 09:30 $1.70 -19.89; LPTH×85 yday $14.80 → 09:30 $14.01 -67.15 | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 393 | $3.11 | $5.14 | $-61.30 | $1,267.20 | ▼ -61.30 after sell → book $9,592.64; vs 09:30 mark -5.15 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 31 | $41.57 | $2.10 | $+49.13 | $2,553.77 | ▲ +49.13 after sell → book $9,590.54; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 139 | $9.03 | $2.44 | $-17.36 | $3,806.50 | ▼ -17.36 after sell → book $9,588.10; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 69 | $16.20 | $2.22 | $-145.18 | $4,922.08 | ▼ -145.18 after sell → book $9,585.88; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 78 | $15.78 | $2.25 | $-37.23 | $6,150.67 | ▼ -37.23 after sell → book $9,583.63; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 39 | $28.59 | $2.13 | $-158.67 | $7,263.56 | ▼ -158.67 after sell → book $9,581.51; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 663 | $1.70 | $8.67 | $-163.09 | $8,381.98 | ▼ -163.09 after sell → book $9,572.83; vs 09:30 mark -8.68 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `LPTH` | 85 | $14.01 | $2.27 | $-83.56 | $9,570.56 | ▼ -83.56 after sell → book $9,570.56; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,570.56 | ▲ close $9,570.56 vs 09:30 $9,597.79 (session +0.00) | 16:00 close · cash $9,570.56 · no lots left · equity $9,570.56. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,570.56 | ▲ 09:30 equity $9,570.56 vs yday $9,570.56 (+0.00) | 09:30 open · cash $9,570.56 · no holdings · equity $9,570.56 vs prior close $9,570.56 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,570.56 | ▲ close $9,570.56 vs 09:30 $9,570.56 (session +0.00) | 16:00 close · cash $9,570.56 · no lots left · equity $9,570.56. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,570.56 | ▲ 09:30 equity $9,570.56 vs yday $9,570.56 (+0.00) | 09:30 open · cash $9,570.56 · no holdings · equity $9,570.56 vs prior close $9,570.56 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 58 | $20.55 | $2.16 | — | $8,376.50 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1196.32 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,191.34 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1196.32 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 57 | $20.65 | $2.16 | — | $6,012.13 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1196.32 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 207 | $5.77 | $2.67 | — | $4,815.07 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1196.32 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 60 | $19.63 | $2.17 | — | $3,635.10 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1196.32 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 40 | $29.63 | $2.11 | — | $2,447.79 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1196.32 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 683 | $1.75 | $8.81 | — | $1,243.73 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1196.32 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $85.40 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy,oppset; 🔵; ⚪; ret5=+9.2; leftover $1196.32 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $85.40 | ▲ close $9,772.32 vs 09:30 $9,570.56 (session +225.88) | 16:00 close · cash $85.40 · equity $9,772.32 vs 09:30 $9,570.56 (+201.76; session marks +225.88) · 8 name(s) marked open→close (per-name table). AG×58 09:30 $20.55 → close $21.19 +37.12; BHP×13 09:30 $91.01 → close $93.63 +34.06; CDE×57 09:30 $20.65 → close $21.11 +26.22; HDSN×207 09:30 $5.77 → close $5.57 -41.40; IAG×60 09:30 $19.63 → close $20.50 +52.20; KGC×40 09:30 $29.63 → close $31.43 +72.00; NFGC×683 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.40 | ▲ 09:30 equity $10,030.57 vs yday $9,772.32 (+258.25) | 09:30 open · cash $85.40 (unchanged overnight, no fees) · equity $10,030.57 vs prior close $9,772.32 (+258.25) · 8 name(s) re-marked at the open (per-name table). AG×58 yday $21.19 → 09:30 $21.90 +41.18; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×57 yday $21.11 → 09:30 $21.75 +36.48; HDSN×207 yday $5.57 → 09:30 $5.67 +20.70; IAG×60 yday $20.50 → 09:30 $21.17 +40.20; KGC×40 yday $31.43 → 09:30 $32.17 +29.60; NFGC×683 yday $1.75 → 09:30 $1.79 +27.32; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 58 | $21.90 | $2.18 | $+73.95 | $1,353.41 | ▲ +73.95 after sell → book $10,028.38; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,595.72 | ▲ +57.15 after sell → book $10,026.33; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 57 | $21.75 | $2.18 | $+58.36 | $3,833.29 | ▲ +58.36 after sell → book $10,024.15; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 207 | $5.67 | $2.71 | $-26.08 | $5,004.27 | ▼ -26.08 after sell → book $10,021.44; vs 09:30 mark -2.71 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 60 | $21.17 | $2.19 | $+88.04 | $6,272.28 | ▲ +88.04 after sell → book $10,019.25; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 40 | $32.17 | $2.13 | $+97.36 | $7,556.95 | ▲ +97.36 after sell → book $10,017.12; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 683 | $1.79 | $8.93 | $+9.58 | $8,770.58 | ▲ +9.58 after sell → book $10,008.18; vs 09:30 mark -8.94 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $10,006.15 | ▲ +77.23 after sell → book $10,006.15; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $8,809.83 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1250.77 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 72 | $17.20 | $2.21 | — | $7,569.22 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1250.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $6,485.72 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1250.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 112 | $11.13 | $2.33 | — | $5,236.83 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,yday_gainer,mover_buy,oppset; 🔵; ⚪; ret5=+39.8; leftover $1250.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 506 | $2.47 | $6.53 | — | $3,980.48 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1250.77 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 648 | $1.93 | $8.36 | — | $2,721.48 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1250.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 20 | $59.72 | $2.05 | — | $1,525.03 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1250.77 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 947 | $1.32 | $12.22 | — | $262.78 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1250.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $262.78 | ▲ close $10,219.96 vs 09:30 $10,030.57 (session +251.52) | 16:00 close · cash $262.78 · equity $10,219.96 vs 09:30 $10,030.57 (+189.39; session marks +251.52) · 8 name(s) marked open→close (per-name table). AU×10 09:30 $119.43 → close $121.22 +17.90; AUPH×72 09:30 $17.20 → close $16.65 -39.60; AEM×5 09:30 $216.30 → close $216.06 -1.20; ARCT×112 09:30 $11.13 → close $13.45 +259.84; AUTL×506 09:30 $2.47 → close $2.41 -30.36; CRDL×648 09:30 $1.93 → close $1.86 -45.36; CRSP×20 09:30 $59.72 → close $59.50 -4.40; CYPH×947 09:30 $1.32 → close $1.42 +94.70 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $262.78 | ▲ 09:30 equity $10,579.68 vs yday $10,219.96 (+359.72) | 09:30 open · cash $262.78 (unchanged overnight, no fees) · equity $10,579.68 vs prior close $10,219.96 (+359.72) · 8 name(s) re-marked at the open (per-name table). AU×10 yday $121.22 → 09:30 $120.51 -7.10; AUPH×72 yday $16.65 → 09:30 $16.57 -5.76; AEM×5 yday $216.06 → 09:30 $217.03 +4.85; ARCT×112 yday $13.45 → 09:30 $13.33 -13.44; AUTL×506 yday $2.41 → 09:30 $2.40 -5.06; CRDL×648 yday $1.86 → 09:30 $1.88 +12.96; CRSP×20 yday $59.50 → 09:30 $58.75 -15.00; CYPH×947 yday $1.42 → 09:30 $1.83 +388.27 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,465.84 | ▲ +6.74 after sell → book $10,577.64; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 72 | $16.57 | $2.23 | $-49.79 | $2,656.65 | ▼ -49.79 after sell → book $10,575.41; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,739.77 | ▼ -0.38 after sell → book $10,573.38; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 112 | $13.33 | $2.36 | $+241.72 | $5,230.38 | ▲ +241.72 after sell → book $10,571.03; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 506 | $2.40 | $6.62 | $-48.57 | $6,438.16 | ▼ -48.57 after sell → book $10,564.41; vs 09:30 mark -6.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 648 | $1.88 | $8.48 | $-49.24 | $7,647.92 | ▼ -49.24 after sell → book $10,555.93; vs 09:30 mark -8.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 20 | $58.75 | $2.07 | $-23.52 | $8,820.85 | ▼ -23.52 after sell → book $10,553.86; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 947 | $1.83 | $12.39 | $+458.37 | $10,541.47 | ▲ +458.37 after sell → book $10,541.47; vs 09:30 mark -12.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,541.47 | ▲ close $10,541.47 vs 09:30 $10,579.68 (session +0.00) | 16:00 close · cash $10,541.47 · no lots left · equity $10,541.47. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,541.47 | ▲ 09:30 equity $10,541.47 vs yday $10,541.47 (+0.00) | 09:30 open · cash $10,541.47 · no holdings · equity $10,541.47 vs prior close $10,541.47 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 55 | $23.77 | $2.15 | — | $9,231.97 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; ⚪; ret5=+13.0; leftover $1317.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 157 | $8.35 | $2.46 | — | $7,918.56 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1317.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 808 | $1.63 | $10.42 | — | $6,591.09 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1317.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 251 | $5.24 | $3.24 | — | $5,272.62 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+35.3; leftover $1317.68 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 149 | $8.79 | $2.44 | — | $3,960.47 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+36.1; leftover $1317.68 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 844 | $1.56 | $10.89 | — | $2,632.94 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $1317.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 2125 | $0.62 | $19.55 | — | $1,295.89 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+33.3; leftover $1317.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 203 | $6.37 | $2.62 | — | $0.16 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,oppset; 🔵; ⚪; ret5=+10.9; leftover $1317.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.16 | ▲ close $10,685.11 vs 09:30 $10,541.47 (session +197.41) | 16:00 close · cash $0.16 · equity $10,685.11 vs 09:30 $10,541.47 (+143.64; session marks +197.41) · 8 name(s) marked open→close (per-name table). MOS×55 09:30 $23.77 → close $24.27 +27.50; CRMD×157 09:30 $8.35 → close $8.56 +32.97; BMEA×808 09:30 $1.63 → close $1.73 +80.80; ALVO×251 09:30 $5.24 → close $5.05 -47.69; SUJA×149 09:30 $8.79 → close $9.33 +80.46; CYPH×844 09:30 $1.56 → close $1.64 +67.52; DEFT×2125 09:30 $0.62 → close $0.60 -34.00; ZURA×203 09:30 $6.37 → close $6.32 -10.15 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.16 | ▼ 09:30 equity $10,649.23 vs yday $10,685.11 (-35.88) | 09:30 open · cash $0.16 (unchanged overnight, no fees) · equity $10,649.23 vs prior close $10,685.11 (-35.88) · 8 name(s) re-marked at the open (per-name table). MOS×55 yday $24.27 → 09:30 $24.84 +31.35; CRMD×157 yday $8.56 → 09:30 $8.60 +6.28; BMEA×808 yday $1.73 → 09:30 $1.75 +20.20; ALVO×251 yday $5.05 → 09:30 $4.98 -17.57; SUJA×149 yday $9.33 → 09:30 $9.39 +8.94; CYPH×844 yday $1.64 → 09:30 $1.60 -33.76; DEFT×2125 yday $0.60 → 09:30 $0.60 -12.75; ZURA×203 yday $6.32 → 09:30 $6.13 -38.57 | — |
| 2026-08-26 09:30 ET | **SELL** | `MOS` | 55 | $24.84 | $2.18 | $+54.52 | $1,364.19 | ▲ +54.52 after sell → book $10,647.06; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **SELL** | `CRMD` | 157 | $8.60 | $2.50 | $+34.29 | $2,711.89 | ▲ +34.29 after sell → book $10,644.56; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 808 | $1.75 | $10.57 | $+80.01 | $4,119.36 | ▲ +80.01 after sell → book $10,633.99; vs 09:30 mark -10.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ALVO` | 251 | $4.98 | $3.29 | $-71.79 | $5,366.05 | ▼ -71.79 after sell → book $10,630.70; vs 09:30 mark -3.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SUJA` | 149 | $9.39 | $2.47 | $+84.49 | $6,762.69 | ▲ +84.49 after sell → book $10,628.23; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 844 | $1.60 | $11.04 | $+11.83 | $8,102.05 | ▲ +11.83 after sell → book $10,617.19; vs 09:30 mark -11.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `DEFT` | 2125 | $0.60 | $19.45 | $-85.75 | $9,353.35 | ▼ -85.75 after sell → book $10,597.74; vs 09:30 mark -19.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZURA` | 203 | $6.13 | $2.66 | $-54.00 | $10,595.08 | ▼ -54.00 after sell → book $10,595.08; vs 09:30 mark -2.66 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,595.08 | ▲ close $10,595.08 vs 09:30 $10,649.23 (session +0.00) | 16:00 close · cash $10,595.08 · no lots left · equity $10,595.08. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,595.08 | ▲ 09:30 equity $10,595.08 vs yday $10,595.08 (+0.00) | 09:30 open · cash $10,595.08 · no holdings · equity $10,595.08 vs prior close $10,595.08 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,595.08 | ▲ close $10,595.08 vs 09:30 $10,595.08 (session +0.00) | 16:00 close · cash $10,595.08 · no lots left · equity $10,595.08. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,595.08 | ▲ 09:30 equity $10,595.08 vs yday $10,595.08 (+0.00) | 09:30 open · cash $10,595.08 · no holdings · equity $10,595.08 vs prior close $10,595.08 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $9,331.88 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1324.39 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $8,054.02 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover,mover_buy,oppset; 🔵; ⚪; ret5=+14.1; leftover $1324.39 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 10 | $122.81 | $2.02 | — | $6,823.90 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $1324.39 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $5,524.26 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1324.39 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 14 | $91.49 | $2.03 | — | $4,241.37 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $1324.39 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 21 | $62.82 | $2.05 | — | $2,920.09 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+5.4; leftover $1324.39 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 4 | $289.44 | $2.00 | — | $1,760.33 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+1.8; leftover $1324.39 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 11 | $119.76 | $2.02 | — | $440.95 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+2.5; leftover $1324.39 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $440.95 | ▼ close $10,202.59 vs 09:30 $10,595.08 (session -376.34) | 16:00 close · cash $440.95 · equity $10,202.59 vs 09:30 $10,595.08 (-392.49; session marks -376.34) · 8 name(s) marked open→close (per-name table). SIMO×5 09:30 $252.24 → close $245.81 -32.15; SMTC×9 09:30 $141.76 → close $131.17 -95.31; TTMI×10 09:30 $122.81 → close $118.65 -41.60; KEYS×4 09:30 $324.41 → close $319.97 -17.76; AVT×14 09:30 $91.49 → close $88.63 -40.04; CGNX×21 09:30 $62.82 → close $60.46 -49.56; COHR×4 09:30 $289.44 → close $279.20 -40.96; LSCC×11 09:30 $119.76 → close $114.40 -58.96 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $440.95 | ▲ 09:30 equity $10,258.44 vs yday $10,202.59 (+55.85) | 09:30 open · cash $440.95 (unchanged overnight, no fees) · equity $10,258.44 vs prior close $10,202.59 (+55.85) · 8 name(s) re-marked at the open (per-name table). SIMO×5 yday $245.81 → 09:30 $247.05 +6.20; SMTC×9 yday $131.17 → 09:30 $132.30 +10.17; TTMI×10 yday $118.65 → 09:30 $118.83 +1.80; KEYS×4 yday $319.97 → 09:30 $322.49 +10.08; AVT×14 yday $88.63 → 09:30 $89.39 +10.64; CGNX×21 yday $60.46 → 09:30 $60.46 +0.00; COHR×4 yday $279.20 → 09:30 $280.25 +4.20; LSCC×11 yday $114.40 → 09:30 $115.56 +12.76 | — |
| 2026-08-31 09:30 ET | **SELL** | `SIMO` | 5 | $247.05 | $2.02 | $-29.98 | $1,674.17 | ▼ -29.98 after sell → book $10,256.41; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $2,862.84 | ▼ -89.19 after sell → book $10,254.38; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TTMI` | 10 | $118.83 | $2.04 | $-43.86 | $4,049.10 | ▼ -43.86 after sell → book $10,252.34; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 4 | $322.49 | $2.02 | $-11.70 | $5,337.03 | ▼ -11.70 after sell → book $10,250.31; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVT` | 14 | $89.39 | $2.05 | $-33.48 | $6,586.44 | ▼ -33.48 after sell → book $10,248.26; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CGNX` | 21 | $60.46 | $2.07 | $-53.69 | $7,854.03 | ▼ -53.69 after sell → book $10,246.19; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `COHR` | 4 | $280.25 | $2.02 | $-40.78 | $8,973.01 | ▼ -40.78 after sell → book $10,244.17; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LSCC` | 11 | $115.56 | $2.04 | $-50.27 | $10,242.12 | ▼ -50.27 after sell → book $10,242.12; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,242.12 | ▲ close $10,242.12 vs 09:30 $10,258.44 (session +0.00) | 16:00 close · cash $10,242.12 · no lots left · equity $10,242.12. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,242.12 | ▲ 09:30 equity $10,242.12 vs yday $10,242.12 (+0.00) | 09:30 open · cash $10,242.12 · no holdings · equity $10,242.12 vs prior close $10,242.12 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,242.12 | ▲ close $10,242.12 vs 09:30 $10,242.12 (session +0.00) | 16:00 close · cash $10,242.12 · no lots left · equity $10,242.12. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,242.12 | ▲ 09:30 equity $10,242.12 vs yday $10,242.12 (+0.00) | 09:30 open · cash $10,242.12 · no holdings · equity $10,242.12 vs prior close $10,242.12 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,242.12 | ▲ close $10,242.12 vs 09:30 $10,242.12 (session +0.00) | 16:00 close · cash $10,242.12 · no lots left · equity $10,242.12. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,242.12 | ▲ 09:30 equity $10,242.12 vs yday $10,242.12 (+0.00) | 09:30 open · cash $10,242.12 · no holdings · equity $10,242.12 vs prior close $10,242.12 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 24 | $52.88 | $2.06 | — | $8,970.94 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1280.27 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 29 | $42.93 | $2.08 | — | $7,723.89 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1280.27 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 352 | $3.63 | $4.54 | — | $6,441.59 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1280.27 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 159 | $8.03 | $2.47 | — | $5,162.36 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1280.27 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $3,968.29 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1280.27 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 76 | $16.77 | $2.22 | — | $2,691.55 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy,oppset; 🔵; ⚪; ret5=+5.7; leftover $1280.27 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 86 | $14.85 | $2.25 | — | $1,412.20 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1280.27 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 587 | $2.18 | $7.57 | — | $124.97 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1280.27 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $124.97 | ▼ close $9,989.82 vs 09:30 $10,242.12 (session -227.10) | 16:00 close · cash $124.97 · equity $9,989.82 vs 09:30 $10,242.12 (-252.30; session marks -227.10) · 8 name(s) marked open→close (per-name table). ATRC×24 09:30 $52.88 → close $52.46 -10.08; HRMY×29 09:30 $42.93 → close $41.86 -31.03; CABA×352 09:30 $3.63 → close $3.48 -52.80; VSTM×159 09:30 $8.03 → close $7.98 -7.95; RVTY×9 09:30 $132.45 → close $130.63 -16.38; ARCT×76 09:30 $16.77 → close $15.56 -91.96; SLN×86 09:30 $14.85 → close $14.79 -5.16; CRDL×587 09:30 $2.18 → close $2.16 -11.74 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $124.97 | ▼ 09:30 equity $9,935.53 vs yday $9,989.82 (-54.29) | 09:30 open · cash $124.97 (unchanged overnight, no fees) · equity $9,935.53 vs prior close $9,989.82 (-54.29) · 8 name(s) re-marked at the open (per-name table). ATRC×24 yday $52.46 → 09:30 $52.03 -10.32; HRMY×29 yday $41.86 → 09:30 $41.50 -10.44; CABA×352 yday $3.48 → 09:30 $3.46 -7.04; VSTM×159 yday $7.98 → 09:30 $7.91 -11.13; RVTY×9 yday $130.63 → 09:30 $130.03 -5.40; ARCT×76 yday $15.56 → 09:30 $15.61 +3.80; SLN×86 yday $14.79 → 09:30 $14.63 -13.76; CRDL×587 yday $2.16 → 09:30 $2.16 +0.00 | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 29 | $41.50 | $2.10 | $-45.64 | $1,326.37 | ▼ -45.64 after sell → book $9,933.43; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 159 | $7.91 | $2.50 | $-24.05 | $2,581.56 | ▼ -24.05 after sell → book $9,930.93; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $130.03 | $2.04 | $-25.83 | $3,749.79 | ▼ -25.83 after sell → book $9,928.89; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 76 | $15.61 | $2.24 | $-92.62 | $4,933.91 | ▼ -92.62 after sell → book $9,926.65; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SLN` | 86 | $14.63 | $2.27 | $-23.44 | $6,189.82 | ▼ -23.44 after sell → book $9,924.38; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 587 | $2.16 | $7.68 | $-26.99 | $7,450.06 | ▼ -26.99 after sell → book $9,916.70; vs 09:30 mark -7.68 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 492 | $2.52 | $6.35 | — | $6,203.87 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1241.68 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 185 | $6.71 | $2.54 | — | $4,959.98 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1241.68 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 653 | $1.90 | $8.42 | — | $3,710.85 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1241.68 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 259 | $4.78 | $3.34 | — | $2,469.49 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1241.68 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 780 | $1.59 | $10.06 | — | $1,219.23 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1241.68 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 107 | $11.31 | $2.31 | — | $6.75 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1241.68 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.75 | ▼ close $9,833.05 vs 09:30 $9,935.53 (session -50.63) | 16:00 close · cash $6.75 · equity $9,833.05 vs 09:30 $9,935.53 (-102.48; session marks -50.63) · 8 name(s) marked open→close (per-name table). ATRC×24 09:30 $52.03 → close $51.52 -12.24; CABA×352 09:30 $3.46 → close $3.47 +3.52; ALEC×492 09:30 $2.52 → close $2.46 -29.52; BHC×185 09:30 $6.71 → close $6.56 -27.75; BMEA×653 09:30 $1.90 → close $2.03 +84.89; OABI×259 09:30 $4.78 → close $4.33 -116.55; OPK×780 09:30 $1.59 → close $1.64 +39.00; VIR×107 09:30 $11.31 → close $11.38 +8.02 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.75 | ▼ 09:30 equity $9,795.60 vs yday $9,833.05 (-37.45) | 09:30 open · cash $6.75 (unchanged overnight, no fees) · equity $9,795.60 vs prior close $9,833.05 (-37.45) · 8 name(s) re-marked at the open (per-name table). ATRC×24 yday $51.52 → 09:30 $54.31 +66.96; CABA×352 yday $3.47 → 09:30 $3.43 -14.08; ALEC×492 yday $2.46 → 09:30 $2.38 -39.36; BHC×185 yday $6.56 → 09:30 $6.57 +1.85; BMEA×653 yday $2.03 → 09:30 $2.00 -19.59; OABI×259 yday $4.33 → 09:30 $4.30 -7.77; OPK×780 yday $1.64 → 09:30 $1.63 -7.80; VIR×107 yday $11.38 → 09:30 $11.22 -17.65 | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 24 | $54.31 | $2.08 | $+30.18 | $1,308.11 | ▲ +30.18 after sell → book $9,793.52; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 352 | $3.43 | $4.61 | $-79.55 | $2,510.86 | ▼ -79.55 after sell → book $9,788.91; vs 09:30 mark -4.61 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 492 | $2.38 | $6.44 | $-81.67 | $3,675.38 | ▼ -81.67 after sell → book $9,782.47; vs 09:30 mark -6.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 185 | $6.57 | $2.59 | $-31.03 | $4,888.24 | ▼ -31.03 after sell → book $9,779.88; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 653 | $2.00 | $8.54 | $+48.33 | $6,185.70 | ▲ +48.33 after sell → book $9,771.34; vs 09:30 mark -8.54 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 259 | $4.30 | $3.39 | $-131.06 | $7,296.01 | ▼ -131.06 after sell → book $9,767.95; vs 09:30 mark -3.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 780 | $1.63 | $10.20 | $+10.94 | $8,557.21 | ▲ +10.94 after sell → book $9,757.75; vs 09:30 mark -10.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 107 | $11.22 | $2.34 | $-14.28 | $9,755.41 | ▼ -14.28 after sell → book $9,755.41; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,755.41 | ▲ close $9,755.41 vs 09:30 $9,795.60 (session +0.00) | 16:00 close · cash $9,755.41 · no lots left · equity $9,755.41. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,755.41 | ▲ 09:30 equity $9,755.41 vs yday $9,755.41 (-0.00) | 09:30 open · cash $9,755.41 · no holdings · equity $9,755.41 vs prior close $9,755.41 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,755.41 | ▲ close $9,755.41 vs 09:30 $9,755.41 (session +0.00) | 16:00 close · cash $9,755.41 · no lots left · equity $9,755.41. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,755.41 | ▲ 09:30 equity $9,755.41 vs yday $9,755.41 (-0.00) | 09:30 open · cash $9,755.41 · no holdings · equity $9,755.41 vs prior close $9,755.41 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,755.41 | ▲ close $9,755.41 vs 09:30 $9,755.41 (session +0.00) | 16:00 close · cash $9,755.41 · no lots left · equity $9,755.41. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,755.41 | ▲ 09:30 equity $9,755.41 vs yday $9,755.41 (-0.00) | 09:30 open · cash $9,755.41 · no holdings · equity $9,755.41 vs prior close $9,755.41 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 9 | $164.43 | $2.02 | — | $8,273.52 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1625.90 | join🟢 sector🟢 gen🟡 news🟢 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 30 | $52.55 | $2.08 | — | $6,694.94 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $1625.90 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `PAGS` | 160 | $10.11 | $2.47 | — | $5,074.87 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer; 🔵; ⚪; ret5=+2.9; leftover $1625.90 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ZSQR` | 500 | $3.25 | $6.45 | — | $3,443.42 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer; 🔵; ⚪; ret5=+3.6; leftover $1625.90 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟡 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `PAYP` | 88 | $18.30 | $2.25 | — | $1,830.77 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1625.90 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `SEDG` | 44 | $36.78 | $2.12 | — | $210.32 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list oppset; 🔵; ⚪; ret5=+8.2; leftover $1625.90 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $210.32 | ▼ close $9,572.66 vs 09:30 $9,755.41 (session -165.35) | 16:00 close · cash $210.32 · equity $9,572.66 vs 09:30 $9,755.41 (-182.75; session marks -165.35) · 6 name(s) marked open→close (per-name table). ORCL×9 09:30 $164.43 → close $150.28 -127.35; BAND×30 09:30 $52.55 → close $56.87 +129.60; PAGS×160 09:30 $10.11 → close $10.12 +1.60; ZSQR×500 09:30 $3.25 → close $3.07 -90.00; PAYP×88 09:30 $18.30 → close $18.45 +13.20; SEDG×44 09:30 $36.78 → close $34.68 -92.40 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $210.32 | ▼ 09:30 equity $9,408.90 vs yday $9,572.66 (-163.76) | 09:30 open · cash $210.32 (unchanged overnight, no fees) · equity $9,408.90 vs prior close $9,572.66 (-163.76) · 6 name(s) re-marked at the open (per-name table). ORCL×9 yday $150.28 → 09:30 $141.42 -79.74; BAND×30 yday $56.87 → 09:30 $56.90 +0.90; PAGS×160 yday $10.12 → 09:30 $10.00 -19.20; ZSQR×500 yday $3.07 → 09:30 $3.06 -5.00; PAYP×88 yday $18.45 → 09:30 $18.28 -14.96; SEDG×44 yday $34.68 → 09:30 $33.64 -45.76 | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 9 | $141.42 | $2.04 | $-211.14 | $1,481.07 | ▼ -211.14 after sell → book $9,406.87; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🔴 digest🔴 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-14 09:30 ET | **SELL** | `BAND` | 30 | $56.90 | $2.10 | $+126.32 | $3,185.96 | ▲ +126.32 after sell → book $9,404.76; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `PAGS` | 160 | $10.00 | $2.51 | $-22.58 | $4,783.45 | ▼ -22.58 after sell → book $9,402.25; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ZSQR` | 500 | $3.06 | $6.55 | $-108.00 | $6,306.91 | ▼ -108.00 after sell → book $9,395.71; vs 09:30 mark -6.54 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `PAYP` | 88 | $18.28 | $2.28 | $-6.30 | $7,913.27 | ▼ -6.30 after sell → book $9,393.43; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SEDG` | 44 | $33.64 | $2.14 | $-142.43 | $9,391.28 | ▼ -142.43 after sell → book $9,391.28; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,391.28 | ▲ close $9,391.28 vs 09:30 $9,408.90 (session +0.00) | 16:00 close · cash $9,391.28 · no lots left · equity $9,391.28. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,391.28 | ▲ 09:30 equity $9,391.28 vs yday $9,391.28 (+0.00) | 09:30 open · cash $9,391.28 · no holdings · equity $9,391.28 vs prior close $9,391.28 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,391.28 | ▲ close $9,391.28 vs 09:30 $9,391.28 (session +0.00) | 16:00 close · cash $9,391.28 · no lots left · equity $9,391.28. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,391.28 | ▲ 09:30 equity $9,391.28 vs yday $9,391.28 (+0.00) | 09:30 open · cash $9,391.28 · no holdings · equity $9,391.28 vs prior close $9,391.28 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 52 | $89.38 | $2.15 | — | $4,741.38 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $4695.64 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `QRVO` | 39 | $118.18 | $2.11 | — | $130.25 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $4695.64 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $130.25 | ▼ close $9,025.76 vs 09:30 $9,391.28 (session -361.27) | 16:00 close · cash $130.25 · equity $9,025.76 vs 09:30 $9,391.28 (-365.52; session marks -361.27) · 2 name(s) marked open→close (per-name table). SWKS×52 09:30 $89.38 → close $85.59 -197.08; QRVO×39 09:30 $118.18 → close $113.97 -164.19 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $130.25 | ▲ 09:30 equity $9,122.87 vs yday $9,025.76 (+97.11) | 09:30 open · cash $130.25 (unchanged overnight, no fees) · equity $9,122.87 vs prior close $9,025.76 (+97.11) · 2 name(s) re-marked at the open (per-name table). SWKS×52 yday $85.59 → 09:30 $86.76 +60.84; QRVO×39 yday $113.97 → 09:30 $114.90 +36.27 | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 52 | $86.76 | $2.19 | $-140.58 | $4,639.58 | ▼ -140.58 after sell → book $9,120.68; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QRVO` | 39 | $114.90 | $2.15 | $-132.18 | $9,118.53 | ▼ -132.18 after sell → book $9,118.53; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `BULL` | 382 | $7.95 | $4.93 | — | $6,076.70 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_mover; 🔵; ⚪; ret5=-18.4; leftover $3039.51 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `VOD` | 173 | $17.56 | $2.51 | — | $3,036.31 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list ohlc_hot; 🔵; ⚪; ret5=+7.9; leftover $3039.51 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `ASAN` | 317 | $9.55 | $4.09 | — | $4.87 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list oppset; 🔵; ⚪; ret5=+17.0; leftover $3039.51 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.87 | ▲ close $9,179.58 vs 09:30 $9,122.87 (session +72.58) | 16:00 close · cash $4.87 · equity $9,179.58 vs 09:30 $9,122.87 (+56.71; session marks +72.58) · 3 name(s) marked open→close (per-name table). BULL×382 09:30 $7.95 → close $7.71 -91.68; VOD×173 09:30 $17.56 → close $17.52 -6.92; ASAN×317 09:30 $9.55 → close $10.09 +171.18 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.87 | ▼ 09:30 equity $9,096.39 vs yday $9,179.58 (-83.19) | 09:30 open · cash $4.87 (unchanged overnight, no fees) · equity $9,096.39 vs prior close $9,179.58 (-83.19) · 3 name(s) re-marked at the open (per-name table). BULL×382 yday $7.71 → 09:30 $7.85 +53.48; VOD×173 yday $17.52 → 09:30 $16.73 -136.67; ASAN×317 yday $10.09 → 09:30 $10.09 +0.00 | — |
| 2026-09-18 09:30 ET | **SELL** | `BULL` | 382 | $7.85 | $5.02 | $-48.14 | $2,998.55 | ▼ -48.14 after sell → book $9,091.37; vs 09:30 mark -5.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `VOD` | 173 | $16.73 | $2.56 | $-148.66 | $5,890.28 | ▼ -148.66 after sell → book $9,088.81; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ASAN` | 317 | $10.09 | $4.17 | $+162.92 | $9,084.65 | ▲ +162.92 after sell → book $9,084.65; vs 09:30 mark -4.16 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-18 09:30 ET | **BUY** | `ILMN` | 4 | $249.13 | $2.00 | — | $8,086.12 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+21.8; leftover $1135.58 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 38 | $29.32 | $2.10 | — | $6,969.86 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+60.9; leftover $1135.58 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `ARQT` | 43 | $26.14 | $2.12 | — | $5,843.72 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,ohlc_hot,oppset; 🔵; ⚪; ret5=+13.2; leftover $1135.58 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `FTRE` | 56 | $20.10 | $2.16 | — | $4,715.96 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+19.2; leftover $1135.58 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 76 | $14.79 | $2.22 | — | $3,589.71 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $1135.58 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 374 | $3.04 | $4.82 | — | $2,449.79 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $1135.58 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 13 | $81.40 | $2.03 | — | $1,389.56 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+36.8; leftover $1135.58 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `RXT` | 288 | $3.94 | $3.72 | — | $251.13 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer; 🔵; ⚪; ret5=+25.2; leftover $1135.58 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $251.13 | ▲ close $9,075.27 vs 09:30 $9,096.39 (session +11.79) | 16:00 close · cash $251.13 · equity $9,075.27 vs 09:30 $9,096.39 (-21.12; session marks +11.79) · 8 name(s) marked open→close (per-name table). ILMN×4 09:30 $249.13 → close $239.62 -38.04; SDGR×38 09:30 $29.32 → close $29.02 -11.40; ARQT×43 09:30 $26.14 → close $25.38 -32.68; FTRE×56 09:30 $20.10 → close $19.93 -9.52; RARE×76 09:30 $14.79 → close $14.51 -21.28; CYPH×374 09:30 $3.04 → close $3.60 +211.31; TEM×13 09:30 $81.40 → close $77.84 -46.28; RXT×288 09:30 $3.94 → close $3.80 -40.32 | — |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ILMN` | 4 | 2026-09-18 @ $249.13 | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+21.8; leftover $1135.58 |
| `SDGR` | 38 | 2026-09-18 @ $29.32 | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+60.9; leftover $1135.58 |
| `ARQT` | 43 | 2026-09-18 @ $26.14 | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,ohlc_hot,oppset; 🔵; ⚪; ret5=+13.2; leftover $1135.58 |
| `FTRE` | 56 | 2026-09-18 @ $20.10 | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+19.2; leftover $1135.58 |
| `RARE` | 76 | 2026-09-18 @ $14.79 | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $1135.58 |
| `CYPH` | 374 | 2026-09-18 @ $3.04 | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $1135.58 |
| `TEM` | 13 | 2026-09-18 @ $81.40 | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+36.8; leftover $1135.58 |
| `RXT` | 288 | 2026-09-18 @ $3.94 | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer; 🔵; ⚪; ret5=+25.2; leftover $1135.58 |
