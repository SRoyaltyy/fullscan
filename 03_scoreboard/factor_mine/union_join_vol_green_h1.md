# Factor mine action — `union_join_vol_green_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **+11.98%** ($11,198) · signal-only (no cash/fees) was +7.36%. Starts YES **19/21**. Fills 123 · skips 24 · realized $+1113.57.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the join camera (do several factors agree?) is green.
- Must-have: the volume camera (is this name unusually active?) is green.
- Must-have: the last finished bar was green (closed up).
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
- **Gate** `join=good,vol=good,last_green=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $106.98.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `BTBT` | 833 | — | $1.50 | +0.00 | $1.57 | +58.31 | +58.31 | +0.00 | +58.31 |
| 2026-08-14 | `BETR` | 84 | — | $14.80 | +0.00 | $13.73 | -89.88 | -89.88 | +0.00 | -89.88 |
| 2026-08-14 | `ANGX` | 290 | — | $4.31 | +0.00 | $4.37 | +17.40 | +17.40 | +0.00 | +17.40 |
| 2026-08-14 | `HYLN` | 299 | — | $4.18 | +0.00 | $4.06 | -35.88 | -35.88 | +0.00 | -35.88 |
| 2026-08-14 | `ADUR` | 75 | — | $16.50 | +0.00 | $16.17 | -24.75 | -24.75 | +0.00 | -24.75 |
| 2026-08-14 | `AIRO` | 112 | — | $11.12 | +0.00 | $9.57 | -173.60 | -173.60 | +0.00 | -173.60 |
| 2026-08-14 | `NCMI` | 464 | — | $2.69 | +0.00 | $2.86 | +78.88 | +78.88 | +0.00 | +78.88 |
| 2026-08-14 | `QMLS` | 170 | — | $7.29 | +0.00 | $7.32 | +5.10 | +5.10 | +0.00 | +5.10 |
| 2026-08-17 | `BTBT` | 833 | $1.57 | $1.52 | -41.65 | — | +0.00 | -41.65 | +16.66 | — |
| 2026-08-17 | `BETR` | 84 | $13.73 | $13.67 | -5.04 | — | +0.00 | -5.04 | -94.92 | — |
| 2026-08-17 | `ANGX` | 290 | $4.37 | $4.60 | +66.70 | — | +0.00 | +66.70 | +84.10 | — |
| 2026-08-17 | `HYLN` | 299 | $4.06 | $4.10 | +11.96 | — | +0.00 | +11.96 | -23.92 | — |
| 2026-08-17 | `ADUR` | 75 | $16.17 | $15.73 | -33.00 | — | +0.00 | -33.00 | -57.75 | — |
| 2026-08-17 | `AIRO` | 112 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -173.60 | — |
| 2026-08-17 | `NCMI` | 464 | $2.86 | $2.80 | -27.84 | — | +0.00 | -27.84 | +51.04 | — |
| 2026-08-17 | `QMLS` | 170 | $7.32 | $7.24 | -13.60 | — | +0.00 | -13.60 | -8.50 | — |
| 2026-08-17 | `ABX` | 213 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `ALOY` | 132 | — | $14.66 | +0.00 | $13.86 | -106.26 | -106.26 | +0.00 | -106.26 |
| 2026-08-17 | `BORR` | 423 | — | $4.59 | +0.00 | $4.50 | -38.07 | -38.07 | +0.00 | -38.07 |
| 2026-08-17 | `XHG` | 464 | — | $4.19 | +0.00 | $3.91 | -129.92 | -129.92 | +0.00 | -129.92 |
| 2026-08-17 | `MP` | 33 | — | $58.01 | +0.00 | $58.51 | +16.50 | +16.50 | +0.00 | +16.50 |
| 2026-08-18 | `ABX` | 213 | $9.12 | $9.03 | -19.17 | — | +0.00 | -19.17 | -19.17 | — |
| 2026-08-18 | `ALOY` | 132 | $13.86 | $13.19 | -87.78 | — | +0.00 | -87.78 | -194.04 | — |
| 2026-08-18 | `BORR` | 423 | $4.50 | $4.56 | +25.38 | — | +0.00 | +25.38 | -12.69 | — |
| 2026-08-18 | `XHG` | 464 | $3.91 | $3.94 | +13.92 | — | +0.00 | +13.92 | -116.00 | — |
| 2026-08-18 | `MP` | 33 | $58.51 | $56.35 | -71.28 | — | +0.00 | -71.28 | -54.78 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 56 | — | $20.55 | +0.00 | $21.19 | +35.84 | +35.84 | +0.00 | +35.84 |
| 2026-08-20 | `CDE` | 56 | — | $20.65 | +0.00 | $21.11 | +25.76 | +25.76 | +0.00 | +25.76 |
| 2026-08-20 | `HDSN` | 201 | — | $5.77 | +0.00 | $5.57 | -40.20 | -40.20 | +0.00 | -40.20 |
| 2026-08-20 | `IAG` | 59 | — | $19.63 | +0.00 | $20.50 | +51.33 | +51.33 | +0.00 | +51.33 |
| 2026-08-20 | `KGC` | 39 | — | $29.63 | +0.00 | $31.43 | +70.20 | +70.20 | +0.00 | +70.20 |
| 2026-08-20 | `NFGC` | 663 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-20 | `ABUS` | 236 | — | $4.92 | +0.00 | $4.77 | -35.40 | -35.40 | +0.00 | -35.40 |
| 2026-08-21 | `AG` | 56 | $21.19 | $21.90 | +39.76 | — | +0.00 | +39.76 | +75.60 | — |
| 2026-08-21 | `CDE` | 56 | $21.11 | $21.75 | +35.84 | — | +0.00 | +35.84 | +61.60 | — |
| 2026-08-21 | `HDSN` | 201 | $5.57 | $5.67 | +20.10 | — | +0.00 | +20.10 | -20.10 | — |
| 2026-08-21 | `IAG` | 59 | $20.50 | $21.17 | +39.53 | — | +0.00 | +39.53 | +90.86 | — |
| 2026-08-21 | `KGC` | 39 | $31.43 | $32.17 | +28.86 | — | +0.00 | +28.86 | +99.06 | — |
| 2026-08-21 | `NFGC` | 663 | $1.75 | $1.79 | +26.52 | — | +0.00 | +26.52 | +26.52 | — |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | — | +0.00 | +35.60 | +81.28 | — |
| 2026-08-21 | `ABUS` | 236 | $4.77 | $5.20 | +101.48 | — | +0.00 | +101.48 | +66.08 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `AUPH` | 70 | — | $17.20 | +0.00 | $16.65 | -38.50 | -38.50 | +0.00 | -38.50 |
| 2026-08-21 | `AEM` | 5 | — | $216.30 | +0.00 | $216.06 | -1.20 | -1.20 | +0.00 | -1.20 |
| 2026-08-21 | `ARCT` | 109 | — | $11.13 | +0.00 | $13.45 | +252.88 | +252.88 | +0.00 | +252.88 |
| 2026-08-21 | `CYPH` | 920 | — | $1.32 | +0.00 | $1.42 | +92.00 | +92.00 | +0.00 | +92.00 |
| 2026-08-21 | `BTBT` | 732 | — | $1.66 | +0.00 | $1.53 | -95.16 | -95.16 | +0.00 | -95.16 |
| 2026-08-21 | `INDP` | 874 | — | $1.39 | +0.00 | $1.29 | -87.40 | -87.40 | +0.00 | -87.40 |
| 2026-08-21 | `MRVI` | 146 | — | $8.28 | +0.00 | $8.64 | +52.56 | +52.56 | +0.00 | +52.56 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.51 | -7.10 | — | +0.00 | -7.10 | +10.80 | — |
| 2026-08-24 | `AUPH` | 70 | $16.65 | $16.57 | -5.60 | — | +0.00 | -5.60 | -44.10 | — |
| 2026-08-24 | `AEM` | 5 | $216.06 | $217.03 | +4.85 | — | +0.00 | +4.85 | +3.65 | — |
| 2026-08-24 | `ARCT` | 109 | $13.45 | $13.33 | -13.08 | — | +0.00 | -13.08 | +239.80 | — |
| 2026-08-24 | `CYPH` | 920 | $1.42 | $1.83 | +377.20 | — | +0.00 | +377.20 | +469.20 | — |
| 2026-08-24 | `BTBT` | 732 | $1.53 | $1.55 | +14.64 | — | +0.00 | +14.64 | -80.52 | — |
| 2026-08-24 | `INDP` | 874 | $1.29 | $1.24 | -43.70 | — | +0.00 | -43.70 | -131.10 | — |
| 2026-08-24 | `MRVI` | 146 | $8.64 | $8.59 | -7.30 | — | +0.00 | -7.30 | +45.26 | — |
| 2026-08-25 | `BMEA` | 778 | — | $1.63 | +0.00 | $1.73 | +77.80 | +77.80 | +0.00 | +77.80 |
| 2026-08-25 | `GORO` | 357 | — | $3.55 | +0.00 | $3.87 | +114.24 | +114.24 | +0.00 | +114.24 |
| 2026-08-25 | `ZURA` | 199 | — | $6.37 | +0.00 | $6.32 | -9.95 | -9.95 | +0.00 | -9.95 |
| 2026-08-25 | `EZPW` | 36 | — | $35.05 | +0.00 | $35.23 | +6.48 | +6.48 | +0.00 | +6.48 |
| 2026-08-25 | `ETON` | 19 | — | $64.55 | +0.00 | $63.05 | -28.50 | -28.50 | +0.00 | -28.50 |
| 2026-08-25 | `WPM` | 8 | — | $156.51 | +0.00 | $163.72 | +57.68 | +57.68 | +0.00 | +57.68 |
| 2026-08-25 | `SUZ` | 141 | — | $8.98 | +0.00 | $9.03 | +7.05 | +7.05 | +0.00 | +7.05 |
| 2026-08-25 | `IAUX` | 667 | — | $1.90 | +0.00 | $1.92 | +13.34 | +13.34 | +0.00 | +13.34 |
| 2026-08-26 | `BMEA` | 778 | $1.73 | $1.75 | +19.45 | — | +0.00 | +19.45 | +97.25 | — |
| 2026-08-26 | `GORO` | 357 | $3.87 | $3.77 | -35.70 | — | +0.00 | -35.70 | +78.54 | — |
| 2026-08-26 | `ZURA` | 199 | $6.32 | $6.13 | -37.81 | — | +0.00 | -37.81 | -47.76 | — |
| 2026-08-26 | `EZPW` | 36 | $35.23 | $35.70 | +16.92 | — | +0.00 | +16.92 | +23.40 | — |
| 2026-08-26 | `ETON` | 19 | $63.05 | $63.60 | +10.45 | — | +0.00 | +10.45 | -18.05 | — |
| 2026-08-26 | `WPM` | 8 | $163.72 | $160.93 | -22.32 | — | +0.00 | -22.32 | +35.36 | — |
| 2026-08-26 | `SUZ` | 141 | $9.03 | $9.03 | +0.00 | — | +0.00 | +0.00 | +7.05 | — |
| 2026-08-26 | `IAUX` | 667 | $1.92 | $1.87 | -33.35 | — | +0.00 | -33.35 | -20.01 | — |
| 2026-08-26 | `USDE` | 1757 | — | $5.81 | +0.00 | $5.98 | +298.69 | +298.69 | +0.00 | +298.69 |
| 2026-08-27 | `USDE` | 1757 | $5.98 | $6.50 | +913.64 | — | +0.00 | +913.64 | +1212.33 | — |
| 2026-08-28 | `ANF` | 19 | — | $146.07 | +0.00 | $148.42 | +44.65 | +44.65 | +0.00 | +44.65 |
| 2026-08-28 | `NCNO` | 122 | — | $23.30 | +0.00 | $22.99 | -37.82 | -37.82 | +0.00 | -37.82 |
| 2026-08-28 | `TH` | 150 | — | $19.00 | +0.00 | $18.55 | -67.50 | -67.50 | +0.00 | -67.50 |
| 2026-08-28 | `GAP` | 115 | — | $24.69 | +0.00 | $23.48 | -139.15 | -139.15 | +0.00 | -139.15 |
| 2026-08-31 | `ANF` | 19 | $148.42 | $148.03 | -7.41 | — | +0.00 | -7.41 | +37.24 | — |
| 2026-08-31 | `NCNO` | 122 | $22.99 | $22.66 | -40.26 | — | +0.00 | -40.26 | -78.08 | — |
| 2026-08-31 | `TH` | 150 | $18.55 | $18.12 | -63.75 | — | +0.00 | -63.75 | -131.25 | — |
| 2026-08-31 | `GAP` | 115 | $23.48 | $22.98 | -57.50 | — | +0.00 | -57.50 | -196.65 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `RVTY` | 10 | — | $132.45 | +0.00 | $130.63 | -18.20 | -18.20 | +0.00 | -18.20 |
| 2026-09-03 | `ARCT` | 82 | — | $16.77 | +0.00 | $15.56 | -99.22 | -99.22 | +0.00 | -99.22 |
| 2026-09-03 | `CRDL` | 631 | — | $2.18 | +0.00 | $2.16 | -12.62 | -12.62 | +0.00 | -12.62 |
| 2026-09-03 | `MMED` | 57 | — | $23.88 | +0.00 | $23.84 | -2.28 | -2.28 | +0.00 | -2.28 |
| 2026-09-03 | `NVAX` | 132 | — | $10.42 | +0.00 | $10.34 | -10.56 | -10.56 | +0.00 | -10.56 |
| 2026-09-03 | `BMEA` | 713 | — | $1.93 | +0.00 | $1.91 | -14.26 | -14.26 | +0.00 | -14.26 |
| 2026-09-03 | `DUOL` | 8 | — | $161.54 | +0.00 | $158.82 | -21.76 | -21.76 | +0.00 | -21.76 |
| 2026-09-03 | `ALMS` | 132 | — | $10.38 | +0.00 | $11.36 | +130.02 | +130.02 | +0.00 | +130.02 |
| 2026-09-04 | `RVTY` | 10 | $130.63 | $130.03 | -6.00 | — | +0.00 | -6.00 | -24.20 | — |
| 2026-09-04 | `ARCT` | 82 | $15.56 | $15.61 | +4.10 | — | +0.00 | +4.10 | -95.12 | — |
| 2026-09-04 | `CRDL` | 631 | $2.16 | $2.16 | +0.00 | — | +0.00 | +0.00 | -12.62 | — |
| 2026-09-04 | `MMED` | 57 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -2.28 | — |
| 2026-09-04 | `NVAX` | 132 | $10.34 | $10.50 | +21.12 | — | +0.00 | +21.12 | +10.56 | — |
| 2026-09-04 | `BMEA` | 713 | $1.91 | $1.90 | -7.13 | — | +0.00 | -7.13 | -21.39 | — |
| 2026-09-04 | `DUOL` | 8 | $158.82 | $157.46 | -10.88 | — | +0.00 | -10.88 | -32.64 | — |
| 2026-09-04 | `ALMS` | 132 | $11.36 | $11.23 | -17.16 | — | +0.00 | -17.16 | +112.86 | — |
| 2026-09-04 | `DELL` | 2 | — | $513.78 | +0.00 | $524.14 | +20.72 | +20.72 | +0.00 | +20.72 |
| 2026-09-04 | `TARS` | 16 | — | $82.70 | +0.00 | $90.78 | +129.28 | +129.28 | +0.00 | +129.28 |
| 2026-09-04 | `BRR` | 542 | — | $2.51 | +0.00 | $2.66 | +81.30 | +81.30 | +0.00 | +81.30 |
| 2026-09-04 | `MDB` | 3 | — | $378.34 | +0.00 | $368.74 | -28.80 | -28.80 | +0.00 | -28.80 |
| 2026-09-04 | `ASST` | 54 | — | $25.18 | +0.00 | $27.14 | +105.84 | +105.84 | +0.00 | +105.84 |
| 2026-09-04 | `DFDV` | 235 | — | $5.79 | +0.00 | $5.87 | +18.80 | +18.80 | +0.00 | +18.80 |
| 2026-09-04 | `TDS` | 36 | — | $37.44 | +0.00 | $37.83 | +14.04 | +14.04 | +0.00 | +14.04 |
| 2026-09-04 | `AHCO` | 215 | — | $6.32 | +0.00 | $6.49 | +36.55 | +36.55 | +0.00 | +36.55 |
| 2026-09-08 | `DELL` | 2 | $524.14 | $521.15 | -5.98 | — | +0.00 | -5.98 | +14.74 | — |
| 2026-09-08 | `TARS` | 16 | $90.78 | $89.67 | -17.76 | — | +0.00 | -17.76 | +111.52 | — |
| 2026-09-08 | `BRR` | 542 | $2.66 | $2.66 | +0.00 | — | +0.00 | +0.00 | +81.30 | — |
| 2026-09-08 | `MDB` | 3 | $368.74 | $360.75 | -23.97 | — | +0.00 | -23.97 | -52.77 | — |
| 2026-09-08 | `ASST` | 54 | $27.14 | $26.44 | -37.80 | — | +0.00 | -37.80 | +68.04 | — |
| 2026-09-08 | `DFDV` | 235 | $5.87 | $5.81 | -14.10 | — | +0.00 | -14.10 | +4.70 | — |
| 2026-09-08 | `TDS` | 36 | $37.83 | $37.75 | -2.88 | — | +0.00 | -2.88 | +11.16 | — |
| 2026-09-08 | `AHCO` | 215 | $6.49 | $6.48 | -2.15 | — | +0.00 | -2.15 | +34.40 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `INDP` | 588 | — | $2.70 | +0.00 | $2.77 | +41.16 | +41.16 | +0.00 | +41.16 |
| 2026-09-11 | `SWKS` | 18 | — | $84.27 | +0.00 | $88.35 | +73.44 | +73.44 | +0.00 | +73.44 |
| 2026-09-11 | `IRD` | 257 | — | $6.16 | +0.00 | $6.04 | -30.84 | -30.84 | +0.00 | -30.84 |
| 2026-09-11 | `NAUT` | 1587 | — | $1.00 | +0.00 | $1.02 | +31.74 | +31.74 | +0.00 | +31.74 |
| 2026-09-11 | `VRT` | 6 | — | $252.50 | +0.00 | $257.06 | +27.36 | +27.36 | +0.00 | +27.36 |
| 2026-09-11 | `HAFN` | 170 | — | $9.32 | +0.00 | $9.38 | +10.20 | +10.20 | +0.00 | +10.20 |
| 2026-09-11 | `CIG` | 711 | — | $2.23 | +0.00 | $2.20 | -21.33 | -21.33 | +0.00 | -21.33 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -164.42 | BTBT, BETR, ANGX, HYLN, ADUR, AIRO, NCMI, QMLS | — | $3.57 | $9,801.97 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, AIRO×112, NCMI×464, QMLS×170 |
| 2026-08-17 | +2.25 | $3.57 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, AIRO×112, NCMI×464, QMLS×170 | $9,759.50 | -42.47 | -257.75 | ABX, ALOY, BORR, XHG, MP | BTBT, BETR, ANGX, HYLN, ADUR, AIRO, NCMI, QMLS | $29.01 | $9,449.00 | ABX×213, ALOY×132, BORR×423, XHG×464, MP×33 |
| 2026-08-18 | -6.20 | $29.01 | ABX×213, ALOY×132, BORR×423, XHG×464, MP×33 | $9,310.07 | -138.93 | +0.00 | — | ABX, ALOY, BORR, XHG, MP | $9,291.12 | $9,291.12 | — |
| 2026-08-19 | -7.20 | $9,291.12 | — | $9,291.12 | -0.00 | +0.00 | — | — | $9,291.12 | $9,291.12 | — |
| 2026-08-20 | +1.12 | $9,291.12 | — | $9,291.12 | -0.00 | +153.21 | AG, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS | — | $7.92 | $9,419.53 | AG×56, CDE×56, HDSN×201, IAG×59, KGC×39, NFGC×663, WPM×8, ABUS×236 |
| 2026-08-21 | +3.25 | $7.92 | AG×56, CDE×56, HDSN×201, IAG×59, KGC×39, NFGC×663, WPM×8, ABUS×236 | $9,747.22 | +327.69 | +193.08 | AU, AUPH, AEM, ARCT, CYPH, BTBT, INDP, MRVI | AG, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS | $132.32 | $9,871.63 | AU×10, AUPH×70, AEM×5, ARCT×109, CYPH×920, BTBT×732, INDP×874, MRVI×146 |
| 2026-08-24 | -5.17 | $132.32 | AU×10, AUPH×70, AEM×5, ARCT×109, CYPH×920, BTBT×732, INDP×874, MRVI×146 | $10,191.54 | +319.91 | +0.00 | — | AU, AUPH, AEM, ARCT, CYPH, BTBT, INDP, MRVI | $10,147.41 | $10,147.41 | — |
| 2026-08-25 | +1.80 | $10,147.41 | — | $10,147.41 | -0.00 | +238.14 | BMEA, GORO, ZURA, EZPW, ETON, WPM, SUZ, IAUX | — | $36.07 | $10,351.14 | BMEA×778, GORO×357, ZURA×199, EZPW×36, ETON×19, WPM×8, SUZ×141, IAUX×667 |
| 2026-08-26 | +2.02 | $36.07 | BMEA×778, GORO×357, ZURA×199, EZPW×36, ETON×19, WPM×8, SUZ×141, IAUX×667 | $10,268.78 | -82.36 | +298.69 | USDE | BMEA, GORO, ZURA, EZPW, ETON, WPM, SUZ, IAUX | $3.08 | $10,509.94 | USDE×1757 |
| 2026-08-27 | — | $3.08 | USDE×1757 | $11,423.58 | +913.64 | +0.00 | — | USDE | $11,400.53 | $11,400.53 | — |
| 2026-08-28 | +0.75 | $11,400.53 | — | $11,400.53 | -0.00 | -199.82 | ANF, NCNO, TH, GAP | — | $84.07 | $11,191.53 | ANF×19, NCNO×122, TH×150, GAP×115 |
| 2026-08-31 | -5.85 | $84.07 | ANF×19, NCNO×122, TH×150, GAP×115 | $11,022.61 | -168.92 | +0.00 | — | ANF, NCNO, TH, GAP | $11,013.27 | $11,013.27 | — |
| 2026-09-01 | -6.30 | $11,013.27 | — | $11,013.27 | +0.00 | +0.00 | — | — | $11,013.27 | $11,013.27 | — |
| 2026-09-02 | -3.83 | $11,013.27 | — | $11,013.27 | +0.00 | +0.00 | — | — | $11,013.27 | $11,013.27 | — |
| 2026-09-03 | -0.90 | $11,013.27 | — | $11,013.27 | +0.00 | -48.88 | RVTY, ARCT, CRDL, MMED, NVAX, BMEA, DUOL, ALMS | — | $133.00 | $10,933.85 | RVTY×10, ARCT×82, CRDL×631, MMED×57, NVAX×132, BMEA×713, DUOL×8, ALMS×132 |
| 2026-09-04 | +2.25 | $133.00 | RVTY×10, ARCT×82, CRDL×631, MMED×57, NVAX×132, BMEA×713, DUOL×8, ALMS×132 | $10,917.90 | -15.95 | +377.73 | DELL, TARS, BRR, MDB, ASST, DFDV, TDS, AHCO | RVTY, ARCT, CRDL, MMED, NVAX, BMEA, DUOL, ALMS | $590.67 | $11,241.61 | DELL×2, TARS×16, BRR×542, MDB×3, ASST×54, DFDV×235, TDS×36, AHCO×215 |
| 2026-09-08 | -11.47 | $590.67 | DELL×2, TARS×16, BRR×542, MDB×3, ASST×54, DFDV×235, TDS×36, AHCO×215 | $11,136.97 | -104.64 | +0.00 | — | DELL, TARS, BRR, MDB, ASST, DFDV, TDS, AHCO | $11,113.59 | $11,113.59 | — |
| 2026-09-09 | -13.95 | $11,113.59 | — | $11,113.59 | +0.00 | +0.00 | — | — | $11,113.59 | $11,113.59 | — |
| 2026-09-10 | -13.28 | $11,113.59 | — | $11,113.59 | +0.00 | +0.00 | — | — | $11,113.59 | $11,113.59 | — |
| 2026-09-11 | +0.50 | $11,113.59 | — | $11,113.59 | +0.00 | +131.73 | INDP, SWKS, IRD, NAUT, VRT, HAFN, CIG | — | $106.98 | $11,198.22 | INDP×588, SWKS×18, IRD×257, NAUT×1587, VRT×6, HAFN×170, CIG×711 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $8,739.75 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 84 | $14.80 | $2.24 | — | $7,494.31 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $6,240.67 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 299 | $4.18 | $3.86 | — | $4,986.99 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $3,747.28 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 112 | $11.12 | $2.33 | — | $2,499.51 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 464 | $2.69 | $5.99 | — | $1,245.37 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 170 | $7.29 | $2.50 | — | $3.57 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.57 | ▼ close $9,801.97 vs 09:30 $10,000.00 (session -164.42) | 16:00 close · cash $3.57 · equity $9,801.97 vs 09:30 $10,000.00 (-198.03; session marks -164.42) · 8 name(s) marked open→close (per-name table). BTBT×833 09:30 $1.50 → close $1.57 +58.31; BETR×84 09:30 $14.80 → close $13.73 -89.88; ANGX×290 09:30 $4.31 → close $4.37 +17.40; HYLN×299 09:30 $4.18 → close $4.06 -35.88; ADUR×75 09:30 $16.50 → close $16.17 -24.75; AIRO×112 09:30 $11.12 → close $9.57 -173.60; NCMI×464 09:30 $2.69 → close $2.86 +78.88; QMLS×170 09:30 $7.29 → close $7.32 +5.10 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.57 | ▼ 09:30 equity $9,759.50 vs yday $9,801.97 (-42.47) | 09:30 open · cash $3.57 (unchanged overnight, no fees) · equity $9,759.50 vs prior close $9,801.97 (-42.47) · 8 name(s) re-marked at the open (per-name table). BTBT×833 yday $1.57 → 09:30 $1.52 -41.65; BETR×84 yday $13.73 → 09:30 $13.67 -5.04; ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; HYLN×299 yday $4.06 → 09:30 $4.10 +11.96; ADUR×75 yday $16.17 → 09:30 $15.73 -33.00; AIRO×112 yday $9.57 → 09:30 $9.57 +0.00; NCMI×464 yday $2.86 → 09:30 $2.80 -27.84; QMLS×170 yday $7.32 → 09:30 $7.24 -13.60 | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 833 | $1.52 | $10.89 | $-4.98 | $1,258.83 | ▼ -4.98 after sell → book $9,748.60; vs 09:30 mark -10.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 84 | $13.67 | $2.27 | $-99.43 | $2,404.85 | ▼ -99.43 after sell → book $9,746.34; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $3,735.05 | ▲ +76.56 after sell → book $9,742.54; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 299 | $4.10 | $3.92 | $-31.69 | $4,957.03 | ▼ -31.69 after sell → book $9,738.62; vs 09:30 mark -3.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 75 | $15.73 | $2.24 | $-62.20 | $6,134.54 | ▼ -62.20 after sell → book $9,736.38; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 112 | $9.57 | $2.35 | $-178.28 | $7,204.03 | ▼ -178.28 after sell → book $9,734.03; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NCMI` | 464 | $2.80 | $6.07 | $+38.98 | $8,497.16 | ▲ +38.98 after sell → book $9,727.96; vs 09:30 mark -6.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMLS` | 170 | $7.24 | $2.54 | $-13.54 | $9,725.42 | ▼ -13.54 after sell → book $9,725.42; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 213 | $9.12 | $2.75 | — | $7,780.11 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1945.08 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 132 | $14.66 | $2.39 | — | $5,842.60 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $1945.08 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `BORR` | 423 | $4.59 | $5.46 | — | $3,895.58 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ⚪; ret5=+14.8; leftover $1945.08 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 464 | $4.19 | $5.99 | — | $1,945.43 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ⚪; ret5=+291.8; leftover $1945.08 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `MP` | 33 | $58.01 | $2.09 | — | $29.01 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1945.08 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $29.01 | ▼ close $9,449.00 vs 09:30 $9,759.50 (session -257.75) | 16:00 close · cash $29.01 · equity $9,449.00 vs 09:30 $9,759.50 (-310.50; session marks -257.75) · 5 name(s) marked open→close (per-name table). ABX×213 09:30 $9.12 → close $9.12 +0.00; ALOY×132 09:30 $14.66 → close $13.86 -106.26; BORR×423 09:30 $4.59 → close $4.50 -38.07; XHG×464 09:30 $4.19 → close $3.91 -129.92; MP×33 09:30 $58.01 → close $58.51 +16.50 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29.01 | ▼ 09:30 equity $9,310.07 vs yday $9,449.00 (-138.93) | 09:30 open · cash $29.01 (unchanged overnight, no fees) · equity $9,310.07 vs prior close $9,449.00 (-138.93) · 5 name(s) re-marked at the open (per-name table). ABX×213 yday $9.12 → 09:30 $9.03 -19.17; ALOY×132 yday $13.86 → 09:30 $13.19 -87.78; BORR×423 yday $4.50 → 09:30 $4.56 +25.38; XHG×464 yday $3.91 → 09:30 $3.94 +13.92; MP×33 yday $58.51 → 09:30 $56.35 -71.28 | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 213 | $9.03 | $2.80 | $-24.72 | $1,949.60 | ▼ -24.72 after sell → book $9,307.27; vs 09:30 mark -2.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 132 | $13.19 | $2.42 | $-198.85 | $3,688.26 | ▼ -198.85 after sell → book $9,304.85; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `BORR` | 423 | $4.56 | $5.54 | $-23.69 | $5,611.60 | ▼ -23.69 after sell → book $9,299.31; vs 09:30 mark -5.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 464 | $3.94 | $6.08 | $-128.06 | $7,433.68 | ▼ -128.06 after sell → book $9,293.23; vs 09:30 mark -6.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `MP` | 33 | $56.35 | $2.11 | $-58.98 | $9,291.12 | ▼ -58.98 after sell → book $9,291.12; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,291.12 | ▲ close $9,291.12 vs 09:30 $9,310.07 (session +0.00) | 16:00 close · cash $9,291.12 · no lots left · equity $9,291.12. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,291.12 | ▲ 09:30 equity $9,291.12 vs yday $9,291.12 (-0.00) | 09:30 open · cash $9,291.12 · no holdings · equity $9,291.12 vs prior close $9,291.12 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,291.12 | ▲ close $9,291.12 vs 09:30 $9,291.12 (session +0.00) | 16:00 close · cash $9,291.12 · no lots left · equity $9,291.12. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,291.12 | ▲ 09:30 equity $9,291.12 vs yday $9,291.12 (-0.00) | 09:30 open · cash $9,291.12 · no holdings · equity $9,291.12 vs prior close $9,291.12 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 56 | $20.55 | $2.16 | — | $8,138.16 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1161.39 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 56 | $20.65 | $2.16 | — | $6,979.60 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1161.39 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 201 | $5.77 | $2.60 | — | $5,817.24 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1161.39 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 59 | $19.63 | $2.17 | — | $4,656.90 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1161.39 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 39 | $29.63 | $2.11 | — | $3,499.22 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1161.39 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 663 | $1.75 | $8.55 | — | $2,330.42 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1161.39 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $1,172.08 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1161.39 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 236 | $4.92 | $3.04 | — | $7.92 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1161.39 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.92 | ▲ close $9,419.53 vs 09:30 $9,291.12 (session +153.21) | 16:00 close · cash $7.92 · equity $9,419.53 vs 09:30 $9,291.12 (+128.41; session marks +153.21) · 8 name(s) marked open→close (per-name table). AG×56 09:30 $20.55 → close $21.19 +35.84; CDE×56 09:30 $20.65 → close $21.11 +25.76; HDSN×201 09:30 $5.77 → close $5.57 -40.20; IAG×59 09:30 $19.63 → close $20.50 +51.33; KGC×39 09:30 $29.63 → close $31.43 +70.20; NFGC×663 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68; ABUS×236 09:30 $4.92 → close $4.77 -35.40 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.92 | ▲ 09:30 equity $9,747.22 vs yday $9,419.53 (+327.69) | 09:30 open · cash $7.92 (unchanged overnight, no fees) · equity $9,747.22 vs prior close $9,419.53 (+327.69) · 8 name(s) re-marked at the open (per-name table). AG×56 yday $21.19 → 09:30 $21.90 +39.76; CDE×56 yday $21.11 → 09:30 $21.75 +35.84; HDSN×201 yday $5.57 → 09:30 $5.67 +20.10; IAG×59 yday $20.50 → 09:30 $21.17 +39.53; KGC×39 yday $31.43 → 09:30 $32.17 +28.86; NFGC×663 yday $1.75 → 09:30 $1.79 +26.52; WPM×8 yday $150.25 → 09:30 $154.70 +35.60; ABUS×236 yday $4.77 → 09:30 $5.20 +101.48 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 56 | $21.90 | $2.18 | $+71.26 | $1,232.14 | ▲ +71.26 after sell → book $9,745.04; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 56 | $21.75 | $2.18 | $+57.26 | $2,447.96 | ▲ +57.26 after sell → book $9,742.86; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 201 | $5.67 | $2.64 | $-25.34 | $3,584.99 | ▼ -25.34 after sell → book $9,740.22; vs 09:30 mark -2.64 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 59 | $21.17 | $2.19 | $+86.51 | $4,831.84 | ▲ +86.51 after sell → book $9,738.04; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 39 | $32.17 | $2.13 | $+94.83 | $6,084.34 | ▲ +94.83 after sell → book $9,735.91; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 663 | $1.79 | $8.67 | $+9.29 | $7,262.44 | ▲ +9.29 after sell → book $9,727.24; vs 09:30 mark -8.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $8,498.00 | ▲ +77.23 after sell → book $9,725.20; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 236 | $5.20 | $3.09 | $+59.94 | $9,722.11 | ▲ +59.94 after sell → book $9,722.11; vs 09:30 mark -3.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $8,525.79 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1215.26 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 70 | $17.20 | $2.20 | — | $7,319.59 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1215.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $6,236.08 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1215.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 109 | $11.13 | $2.32 | — | $5,020.60 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1215.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 920 | $1.32 | $11.87 | — | $3,794.33 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1215.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 732 | $1.66 | $9.44 | — | $2,569.77 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1215.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `INDP` | 874 | $1.39 | $11.27 | — | $1,343.63 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+30.2; leftover $1215.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 146 | $8.28 | $2.43 | — | $132.32 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; leftover $1215.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $132.32 | ▲ close $9,871.63 vs 09:30 $9,747.22 (session +193.08) | 16:00 close · cash $132.32 · equity $9,871.63 vs 09:30 $9,747.22 (+124.41; session marks +193.08) · 8 name(s) marked open→close (per-name table). AU×10 09:30 $119.43 → close $121.22 +17.90; AUPH×70 09:30 $17.20 → close $16.65 -38.50; AEM×5 09:30 $216.30 → close $216.06 -1.20; ARCT×109 09:30 $11.13 → close $13.45 +252.88; CYPH×920 09:30 $1.32 → close $1.42 +92.00; BTBT×732 09:30 $1.66 → close $1.53 -95.16; INDP×874 09:30 $1.39 → close $1.29 -87.40; MRVI×146 09:30 $8.28 → close $8.64 +52.56 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $132.32 | ▲ 09:30 equity $10,191.54 vs yday $9,871.63 (+319.91) | 09:30 open · cash $132.32 (unchanged overnight, no fees) · equity $10,191.54 vs prior close $9,871.63 (+319.91) · 8 name(s) re-marked at the open (per-name table). AU×10 yday $121.22 → 09:30 $120.51 -7.10; AUPH×70 yday $16.65 → 09:30 $16.57 -5.60; AEM×5 yday $216.06 → 09:30 $217.03 +4.85; ARCT×109 yday $13.45 → 09:30 $13.33 -13.08; CYPH×920 yday $1.42 → 09:30 $1.83 +377.20; BTBT×732 yday $1.53 → 09:30 $1.55 +14.64; INDP×874 yday $1.29 → 09:30 $1.24 -43.70; MRVI×146 yday $8.64 → 09:30 $8.59 -7.30 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,335.38 | ▲ +6.74 after sell → book $10,189.50; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 70 | $16.57 | $2.22 | $-48.52 | $2,493.06 | ▼ -48.52 after sell → book $10,187.28; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,576.19 | ▼ -0.38 after sell → book $10,185.26; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 109 | $13.33 | $2.35 | $+235.14 | $5,026.81 | ▲ +235.14 after sell → book $10,182.91; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 920 | $1.83 | $12.03 | $+445.30 | $6,698.38 | ▲ +445.30 after sell → book $10,170.88; vs 09:30 mark -12.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 732 | $1.55 | $9.57 | $-99.54 | $7,823.40 | ▼ -99.54 after sell → book $10,161.30; vs 09:30 mark -9.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `INDP` | 874 | $1.24 | $11.43 | $-153.80 | $8,895.73 | ▼ -153.80 after sell → book $10,149.87; vs 09:30 mark -11.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 146 | $8.59 | $2.46 | $+40.37 | $10,147.41 | ▲ +40.37 after sell → book $10,147.41; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,147.41 | ▲ close $10,147.41 vs 09:30 $10,191.54 (session +0.00) | 16:00 close · cash $10,147.41 · no lots left · equity $10,147.41. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,147.41 | ▲ 09:30 equity $10,147.41 vs yday $10,147.41 (-0.00) | 09:30 open · cash $10,147.41 · no holdings · equity $10,147.41 vs prior close $10,147.41 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 778 | $1.63 | $10.04 | — | $8,869.23 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1268.43 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 357 | $3.55 | $4.61 | — | $7,597.28 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+27.9; leftover $1268.43 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 199 | $6.37 | $2.59 | — | $6,327.06 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+10.9; leftover $1268.43 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 36 | $35.05 | $2.10 | — | $5,063.16 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1268.43 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ETON` | 19 | $64.55 | $2.05 | — | $3,834.67 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+4.4; leftover $1268.43 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `WPM` | 8 | $156.51 | $2.01 | — | $2,580.57 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+17.4; leftover $1268.43 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUZ` | 141 | $8.98 | $2.41 | — | $1,311.98 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+15.4; leftover $1268.43 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `IAUX` | 667 | $1.90 | $8.60 | — | $36.07 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+16.4; leftover $1268.43 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $36.07 | ▲ close $10,351.14 vs 09:30 $10,147.41 (session +238.14) | 16:00 close · cash $36.07 · equity $10,351.14 vs 09:30 $10,147.41 (+203.73; session marks +238.14) · 8 name(s) marked open→close (per-name table). BMEA×778 09:30 $1.63 → close $1.73 +77.80; GORO×357 09:30 $3.55 → close $3.87 +114.24; ZURA×199 09:30 $6.37 → close $6.32 -9.95; EZPW×36 09:30 $35.05 → close $35.23 +6.48; ETON×19 09:30 $64.55 → close $63.05 -28.50; WPM×8 09:30 $156.51 → close $163.72 +57.68; SUZ×141 09:30 $8.98 → close $9.03 +7.05; IAUX×667 09:30 $1.90 → close $1.92 +13.34 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $36.07 | ▼ 09:30 equity $10,268.78 vs yday $10,351.14 (-82.36) | 09:30 open · cash $36.07 (unchanged overnight, no fees) · equity $10,268.78 vs prior close $10,351.14 (-82.36) · 8 name(s) re-marked at the open (per-name table). BMEA×778 yday $1.73 → 09:30 $1.75 +19.45; GORO×357 yday $3.87 → 09:30 $3.77 -35.70; ZURA×199 yday $6.32 → 09:30 $6.13 -37.81; EZPW×36 yday $35.23 → 09:30 $35.70 +16.92; ETON×19 yday $63.05 → 09:30 $63.60 +10.45; WPM×8 yday $163.72 → 09:30 $160.93 -22.32; SUZ×141 yday $9.03 → 09:30 $9.03 +0.00; IAUX×667 yday $1.92 → 09:30 $1.87 -33.35 | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 778 | $1.75 | $10.18 | $+77.04 | $1,391.29 | ▲ +77.04 after sell → book $10,258.61; vs 09:30 mark -10.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GORO` | 357 | $3.77 | $4.68 | $+69.26 | $2,732.50 | ▲ +69.26 after sell → book $10,253.93; vs 09:30 mark -4.68 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZURA` | 199 | $6.13 | $2.63 | $-52.98 | $3,949.74 | ▼ -52.98 after sell → book $10,251.30; vs 09:30 mark -2.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 36 | $35.70 | $2.12 | $+19.18 | $5,232.82 | ▲ +19.18 after sell → book $10,249.18; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ETON` | 19 | $63.60 | $2.07 | $-22.16 | $6,439.16 | ▼ -22.16 after sell → book $10,247.12; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `WPM` | 8 | $160.93 | $2.03 | $+31.31 | $7,724.56 | ▲ +31.31 after sell → book $10,245.08; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SUZ` | 141 | $9.03 | $2.45 | $+2.19 | $8,995.35 | ▲ +2.19 after sell → book $10,242.64; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `IAUX` | 667 | $1.87 | $8.72 | $-37.34 | $10,233.91 | ▼ -37.34 after sell → book $10,233.91; vs 09:30 mark -8.73 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 1757 | $5.81 | $22.67 | — | $3.08 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $10233.91 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.08 | ▲ close $10,509.94 vs 09:30 $10,268.78 (session +298.69) | 16:00 close · cash $3.08 · equity $10,509.94 vs 09:30 $10,268.78 (+241.16; session marks +298.69) · 1 name(s) marked open→close (per-name table). USDE×1757 09:30 $5.81 → close $5.98 +298.69 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.08 | ▲ 09:30 equity $11,423.58 vs yday $10,509.94 (+913.64) | 09:30 open · cash $3.08 (unchanged overnight, no fees) · equity $11,423.58 vs prior close $10,509.94 (+913.64) · 1 name(s) re-marked at the open (per-name table). USDE×1757 yday $5.98 → 09:30 $6.50 +913.64 | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 1757 | $6.50 | $23.05 | $+1166.62 | $11,400.53 | ▲ +1,166.62 after sell → book $11,400.53; vs 09:30 mark -23.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,400.53 | ▲ close $11,400.53 vs 09:30 $11,423.58 (session +0.00) | 16:00 close · cash $11,400.53 · no lots left · equity $11,400.53. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,400.53 | ▲ 09:30 equity $11,400.53 vs yday $11,400.53 (-0.00) | 09:30 open · cash $11,400.53 · no holdings · equity $11,400.53 vs prior close $11,400.53 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 19 | $146.07 | $2.05 | — | $8,623.15 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $2850.13 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 122 | $23.30 | $2.36 | — | $5,778.20 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+14.5; leftover $2850.13 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 150 | $19.00 | $2.44 | — | $2,925.76 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; ret5=+7.5; leftover $2850.13 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 115 | $24.69 | $2.33 | — | $84.07 | — | combo gate; gate join=good,vol=good,last_green=True; list earn_react; ret5=+5.8; leftover $2850.13 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $84.07 | ▼ close $11,191.53 vs 09:30 $11,400.53 (session -199.82) | 16:00 close · cash $84.07 · equity $11,191.53 vs 09:30 $11,400.53 (-209.00; session marks -199.82) · 4 name(s) marked open→close (per-name table). ANF×19 09:30 $146.07 → close $148.42 +44.65; NCNO×122 09:30 $23.30 → close $22.99 -37.82; TH×150 09:30 $19.00 → close $18.55 -67.50; GAP×115 09:30 $24.69 → close $23.48 -139.15 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $84.07 | ▼ 09:30 equity $11,022.61 vs yday $11,191.53 (-168.92) | 09:30 open · cash $84.07 (unchanged overnight, no fees) · equity $11,022.61 vs prior close $11,191.53 (-168.92) · 4 name(s) re-marked at the open (per-name table). ANF×19 yday $148.42 → 09:30 $148.03 -7.41; NCNO×122 yday $22.99 → 09:30 $22.66 -40.26; TH×150 yday $18.55 → 09:30 $18.12 -63.75; GAP×115 yday $23.48 → 09:30 $22.98 -57.50 | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 19 | $148.03 | $2.08 | $+33.11 | $2,894.56 | ▲ +33.11 after sell → book $11,020.53; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NCNO` | 122 | $22.66 | $2.40 | $-82.83 | $5,656.68 | ▼ -82.83 after sell → book $11,018.13; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `TH` | 150 | $18.12 | $2.49 | $-136.18 | $8,372.95 | ▼ -136.18 after sell → book $11,015.65; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `GAP` | 115 | $22.98 | $2.38 | $-201.36 | $11,013.27 | ▼ -201.36 after sell → book $11,013.27; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,013.27 | ▲ close $11,013.27 vs 09:30 $11,022.61 (session +0.00) | 16:00 close · cash $11,013.27 · no lots left · equity $11,013.27. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,013.27 | ▲ 09:30 equity $11,013.27 vs yday $11,013.27 (+0.00) | 09:30 open · cash $11,013.27 · no holdings · equity $11,013.27 vs prior close $11,013.27 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,013.27 | ▲ close $11,013.27 vs 09:30 $11,013.27 (session +0.00) | 16:00 close · cash $11,013.27 · no lots left · equity $11,013.27. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,013.27 | ▲ 09:30 equity $11,013.27 vs yday $11,013.27 (+0.00) | 09:30 open · cash $11,013.27 · no holdings · equity $11,013.27 vs prior close $11,013.27 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,013.27 | ▲ close $11,013.27 vs 09:30 $11,013.27 (session +0.00) | 16:00 close · cash $11,013.27 · no lots left · equity $11,013.27. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,013.27 | ▲ 09:30 equity $11,013.27 vs yday $11,013.27 (+0.00) | 09:30 open · cash $11,013.27 · no holdings · equity $11,013.27 vs prior close $11,013.27 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $9,686.75 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1376.66 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 82 | $16.77 | $2.24 | — | $8,309.37 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1376.66 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 631 | $2.18 | $8.14 | — | $6,925.65 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1376.66 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 57 | $23.88 | $2.16 | — | $5,562.33 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1376.66 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 132 | $10.42 | $2.39 | — | $4,184.51 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; leftover $1376.66 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 713 | $1.93 | $9.20 | — | $2,799.22 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1376.66 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DUOL` | 8 | $161.54 | $2.01 | — | $1,504.89 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+12.0; leftover $1376.66 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ALMS` | 132 | $10.38 | $2.39 | — | $133.00 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ret5=-56.2; leftover $1376.66 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $133.00 | ▼ close $10,933.85 vs 09:30 $11,013.27 (session -48.88) | 16:00 close · cash $133.00 · equity $10,933.85 vs 09:30 $11,013.27 (-79.42; session marks -48.88) · 8 name(s) marked open→close (per-name table). RVTY×10 09:30 $132.45 → close $130.63 -18.20; ARCT×82 09:30 $16.77 → close $15.56 -99.22; CRDL×631 09:30 $2.18 → close $2.16 -12.62; MMED×57 09:30 $23.88 → close $23.84 -2.28; NVAX×132 09:30 $10.42 → close $10.34 -10.56; BMEA×713 09:30 $1.93 → close $1.91 -14.26; DUOL×8 09:30 $161.54 → close $158.82 -21.76; ALMS×132 09:30 $10.38 → close $11.36 +130.02 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $133.00 | ▼ 09:30 equity $10,917.90 vs yday $10,933.85 (-15.95) | 09:30 open · cash $133.00 (unchanged overnight, no fees) · equity $10,917.90 vs prior close $10,933.85 (-15.95) · 8 name(s) re-marked at the open (per-name table). RVTY×10 yday $130.63 → 09:30 $130.03 -6.00; ARCT×82 yday $15.56 → 09:30 $15.61 +4.10; CRDL×631 yday $2.16 → 09:30 $2.16 +0.00; MMED×57 yday $23.84 → 09:30 $23.84 +0.00; NVAX×132 yday $10.34 → 09:30 $10.50 +21.12; BMEA×713 yday $1.91 → 09:30 $1.90 -7.13; DUOL×8 yday $158.82 → 09:30 $157.46 -10.88; ALMS×132 yday $11.36 → 09:30 $11.23 -17.16 | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $130.03 | $2.04 | $-28.26 | $1,431.26 | ▼ -28.26 after sell → book $10,915.86; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 82 | $15.61 | $2.26 | $-99.62 | $2,709.02 | ▼ -99.62 after sell → book $10,913.60; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 631 | $2.16 | $8.26 | $-29.02 | $4,063.72 | ▼ -29.02 after sell → book $10,905.34; vs 09:30 mark -8.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 57 | $23.84 | $2.18 | $-6.62 | $5,420.42 | ▼ -6.62 after sell → book $10,903.16; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 132 | $10.50 | $2.42 | $+5.75 | $6,804.00 | ▲ +5.75 after sell → book $10,900.74; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `BMEA` | 713 | $1.90 | $9.33 | $-39.91 | $8,149.38 | ▼ -39.91 after sell → book $10,891.42; vs 09:30 mark -9.32 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `DUOL` | 8 | $157.46 | $2.03 | $-36.69 | $9,407.02 | ▼ -36.69 after sell → book $10,889.38; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ALMS` | 132 | $11.23 | $2.42 | $+108.05 | $10,886.96 | ▲ +108.05 after sell → book $10,886.96; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $513.78 | $2.00 | — | $9,857.41 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; leftover $1360.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 16 | $82.70 | $2.04 | — | $8,532.17 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+15.7; leftover $1360.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 542 | $2.51 | $6.99 | — | $7,164.76 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+21.8; leftover $1360.87 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MDB` | 3 | $378.34 | $2.00 | — | $6,027.74 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-12.7; leftover $1360.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 54 | $25.18 | $2.15 | — | $4,665.87 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+16.0; leftover $1360.87 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 235 | $5.79 | $3.03 | — | $3,302.18 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $1360.87 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TDS` | 36 | $37.44 | $2.10 | — | $1,952.25 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+14.1; leftover $1360.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `AHCO` | 215 | $6.32 | $2.77 | — | $590.67 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+10.7; leftover $1360.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $590.67 | ▲ close $11,241.61 vs 09:30 $10,917.90 (session +377.73) | 16:00 close · cash $590.67 · equity $11,241.61 vs 09:30 $10,917.90 (+323.71; session marks +377.73) · 8 name(s) marked open→close (per-name table). DELL×2 09:30 $513.78 → close $524.14 +20.72; TARS×16 09:30 $82.70 → close $90.78 +129.28; BRR×542 09:30 $2.51 → close $2.66 +81.30; MDB×3 09:30 $378.34 → close $368.74 -28.80; ASST×54 09:30 $25.18 → close $27.14 +105.84; DFDV×235 09:30 $5.79 → close $5.87 +18.80; TDS×36 09:30 $37.44 → close $37.83 +14.04; AHCO×215 09:30 $6.32 → close $6.49 +36.55 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $590.67 | ▼ 09:30 equity $11,136.97 vs yday $11,241.61 (-104.64) | 09:30 open · cash $590.67 (unchanged overnight, no fees) · equity $11,136.97 vs prior close $11,241.61 (-104.64) · 8 name(s) re-marked at the open (per-name table). DELL×2 yday $524.14 → 09:30 $521.15 -5.98; TARS×16 yday $90.78 → 09:30 $89.67 -17.76; BRR×542 yday $2.66 → 09:30 $2.66 +0.00; MDB×3 yday $368.74 → 09:30 $360.75 -23.97; ASST×54 yday $27.14 → 09:30 $26.44 -37.80; DFDV×235 yday $5.87 → 09:30 $5.81 -14.10; TDS×36 yday $37.83 → 09:30 $37.75 -2.88; AHCO×215 yday $6.49 → 09:30 $6.48 -2.15 | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 2 | $521.15 | $2.02 | $+10.73 | $1,630.96 | ▲ +10.73 after sell → book $11,134.96; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `TARS` | 16 | $89.67 | $2.06 | $+107.42 | $3,063.62 | ▲ +107.42 after sell → book $11,132.90; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BRR` | 542 | $2.66 | $7.09 | $+67.21 | $4,498.24 | ▲ +67.21 after sell → book $11,125.80; vs 09:30 mark -7.10 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `MDB` | 3 | $360.75 | $2.02 | $-56.79 | $5,578.47 | ▼ -56.79 after sell → book $11,123.78; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 54 | $26.44 | $2.17 | $+63.71 | $7,004.06 | ▲ +63.71 after sell → book $11,121.61; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 235 | $5.81 | $3.08 | $-1.41 | $8,366.33 | ▼ -1.41 after sell → book $11,118.53; vs 09:30 mark -3.08 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `TDS` | 36 | $37.75 | $2.12 | $+6.94 | $9,723.21 | ▲ +6.94 after sell → book $11,116.41; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `AHCO` | 215 | $6.48 | $2.82 | $+28.81 | $11,113.59 | ▲ +28.81 after sell → book $11,113.59; vs 09:30 mark -2.82 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,113.59 | ▲ close $11,113.59 vs 09:30 $11,136.97 (session +0.00) | 16:00 close · cash $11,113.59 · no lots left · equity $11,113.59. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,113.59 | ▲ 09:30 equity $11,113.59 vs yday $11,113.59 (+0.00) | 09:30 open · cash $11,113.59 · no holdings · equity $11,113.59 vs prior close $11,113.59 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,113.59 | ▲ close $11,113.59 vs 09:30 $11,113.59 (session +0.00) | 16:00 close · cash $11,113.59 · no lots left · equity $11,113.59. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,113.59 | ▲ 09:30 equity $11,113.59 vs yday $11,113.59 (+0.00) | 09:30 open · cash $11,113.59 · no holdings · equity $11,113.59 vs prior close $11,113.59 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,113.59 | ▲ close $11,113.59 vs 09:30 $11,113.59 (session +0.00) | 16:00 close · cash $11,113.59 · no lots left · equity $11,113.59. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,113.59 | ▲ 09:30 equity $11,113.59 vs yday $11,113.59 (+0.00) | 09:30 open · cash $11,113.59 · no holdings · equity $11,113.59 vs prior close $11,113.59 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 588 | $2.70 | $7.59 | — | $9,518.41 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $1587.66 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 18 | $84.27 | $2.04 | — | $7,999.50 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ⚪; ret5=+12.5; leftover $1587.66 | join🟢 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 257 | $6.16 | $3.32 | — | $6,413.07 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+36.4; leftover $1587.66 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `NAUT` | 1587 | $1.00 | $20.47 | — | $4,805.59 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ret5=-4.5; leftover $1587.66 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `VRT` | 6 | $252.50 | $2.01 | — | $3,288.59 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; ret5=+12.4; leftover $1587.66 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `HAFN` | 170 | $9.32 | $2.50 | — | $1,701.69 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; ret5=+5.4; leftover $1587.66 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CIG` | 711 | $2.23 | $9.17 | — | $106.98 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; ret5=+10.9; leftover $1587.66 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $106.98 | ▲ close $11,198.22 vs 09:30 $11,113.59 (session +131.73) | 16:00 close · cash $106.98 · equity $11,198.22 vs 09:30 $11,113.59 (+84.63; session marks +131.73) · 7 name(s) marked open→close (per-name table). INDP×588 09:30 $2.70 → close $2.77 +41.16; SWKS×18 09:30 $84.27 → close $88.35 +73.44; IRD×257 09:30 $6.16 → close $6.04 -30.84; NAUT×1587 09:30 $1.00 → close $1.02 +31.74; VRT×6 09:30 $252.50 → close $257.06 +27.36; HAFN×170 09:30 $9.32 → close $9.38 +10.20; CIG×711 09:30 $2.23 → close $2.20 -21.33 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CRCL` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TRMD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBA` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CMPS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `EQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `UROY` | hard_red | hard-red S=-13.28 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `INDP` | 588 | 2026-09-11 @ $2.70 | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $1587.66 |
| `SWKS` | 18 | 2026-09-11 @ $84.27 | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ⚪; ret5=+12.5; leftover $1587.66 |
| `IRD` | 257 | 2026-09-11 @ $6.16 | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+36.4; leftover $1587.66 |
| `NAUT` | 1587 | 2026-09-11 @ $1.00 | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ret5=-4.5; leftover $1587.66 |
| `VRT` | 6 | 2026-09-11 @ $252.50 | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; ret5=+12.4; leftover $1587.66 |
| `HAFN` | 170 | 2026-09-11 @ $9.32 | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; ret5=+5.4; leftover $1587.66 |
| `CIG` | 711 | 2026-09-11 @ $2.23 | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; ret5=+10.9; leftover $1587.66 |
