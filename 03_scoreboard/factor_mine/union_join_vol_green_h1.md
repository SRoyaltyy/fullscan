# Factor mine action — `union_join_vol_green_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **+16.88%** ($11,688) · signal-only (no cash/fees) was +13.00%. Starts YES **26/26**. Fills 176 · skips 44 · realized $+1544.79.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $181.45.

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
| 2026-08-26 | `USDE` | 880 | — | $5.81 | +0.00 | $5.98 | +149.60 | +149.60 | +0.00 | +149.60 |
| 2026-08-26 | `ASST` | 246 | — | $20.72 | +0.00 | $21.50 | +191.88 | +191.88 | +0.00 | +191.88 |
| 2026-08-27 | `USDE` | 880 | $5.98 | $6.50 | +457.60 | — | +0.00 | +457.60 | +607.20 | — |
| 2026-08-27 | `ASST` | 246 | $21.50 | $22.45 | +233.70 | — | +0.00 | +233.70 | +425.58 | — |
| 2026-08-27 | `DKS` | 87 | — | $128.73 | +0.00 | $131.77 | +264.48 | +264.48 | +0.00 | +264.48 |
| 2026-08-28 | `DKS` | 87 | $131.77 | $132.80 | +89.61 | — | +0.00 | +89.61 | +354.09 | — |
| 2026-08-28 | `ANF` | 11 | — | $146.07 | +0.00 | $148.42 | +25.85 | +25.85 | +0.00 | +25.85 |
| 2026-08-28 | `NCNO` | 71 | — | $23.30 | +0.00 | $22.99 | -22.01 | -22.01 | +0.00 | -22.01 |
| 2026-08-28 | `TH` | 87 | — | $19.00 | +0.00 | $18.55 | -39.15 | -39.15 | +0.00 | -39.15 |
| 2026-08-28 | `GAP` | 67 | — | $24.69 | +0.00 | $23.48 | -81.07 | -81.07 | +0.00 | -81.07 |
| 2026-08-28 | `PLAB` | 55 | — | $30.01 | +0.00 | $27.73 | -125.40 | -125.40 | +0.00 | -125.40 |
| 2026-08-28 | `SLF` | 20 | — | $78.95 | +0.00 | $78.76 | -3.80 | -3.80 | +0.00 | -3.80 |
| 2026-08-28 | `WSM` | 7 | — | $235.67 | +0.00 | $235.09 | -4.06 | -4.06 | +0.00 | -4.06 |
| 2026-08-31 | `ANF` | 11 | $148.42 | $148.03 | -4.29 | — | +0.00 | -4.29 | +21.56 | — |
| 2026-08-31 | `NCNO` | 71 | $22.99 | $22.66 | -23.43 | — | +0.00 | -23.43 | -45.44 | — |
| 2026-08-31 | `TH` | 87 | $18.55 | $18.12 | -36.98 | — | +0.00 | -36.98 | -76.12 | — |
| 2026-08-31 | `GAP` | 67 | $23.48 | $22.98 | -33.50 | — | +0.00 | -33.50 | -114.57 | — |
| 2026-08-31 | `PLAB` | 55 | $27.73 | $28.04 | +17.05 | — | +0.00 | +17.05 | -108.35 | — |
| 2026-08-31 | `SLF` | 20 | $78.76 | $78.70 | -1.20 | — | +0.00 | -1.20 | -5.00 | — |
| 2026-08-31 | `WSM` | 7 | $235.09 | $232.06 | -21.21 | — | +0.00 | -21.21 | -25.27 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `RVTY` | 10 | — | $132.45 | +0.00 | $130.63 | -18.20 | -18.20 | +0.00 | -18.20 |
| 2026-09-03 | `ARCT` | 83 | — | $16.77 | +0.00 | $15.56 | -100.43 | -100.43 | +0.00 | -100.43 |
| 2026-09-03 | `CRDL` | 642 | — | $2.18 | +0.00 | $2.16 | -12.84 | -12.84 | +0.00 | -12.84 |
| 2026-09-03 | `MMED` | 58 | — | $23.88 | +0.00 | $23.84 | -2.32 | -2.32 | +0.00 | -2.32 |
| 2026-09-03 | `NVAX` | 134 | — | $10.42 | +0.00 | $10.34 | -10.72 | -10.72 | +0.00 | -10.72 |
| 2026-09-03 | `BMEA` | 725 | — | $1.93 | +0.00 | $1.91 | -14.50 | -14.50 | +0.00 | -14.50 |
| 2026-09-03 | `DUOL` | 8 | — | $161.54 | +0.00 | $158.82 | -21.76 | -21.76 | +0.00 | -21.76 |
| 2026-09-03 | `ALMS` | 134 | — | $10.38 | +0.00 | $11.36 | +131.99 | +131.99 | +0.00 | +131.99 |
| 2026-09-04 | `RVTY` | 10 | $130.63 | $130.03 | -6.00 | — | +0.00 | -6.00 | -24.20 | — |
| 2026-09-04 | `ARCT` | 83 | $15.56 | $15.61 | +4.15 | — | +0.00 | +4.15 | -96.28 | — |
| 2026-09-04 | `CRDL` | 642 | $2.16 | $2.16 | +0.00 | — | +0.00 | +0.00 | -12.84 | — |
| 2026-09-04 | `MMED` | 58 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -2.32 | — |
| 2026-09-04 | `NVAX` | 134 | $10.34 | $10.50 | +21.44 | — | +0.00 | +21.44 | +10.72 | — |
| 2026-09-04 | `BMEA` | 725 | $1.91 | $1.90 | -7.25 | — | +0.00 | -7.25 | -21.75 | — |
| 2026-09-04 | `DUOL` | 8 | $158.82 | $157.46 | -10.88 | — | +0.00 | -10.88 | -32.64 | — |
| 2026-09-04 | `ALMS` | 134 | $11.36 | $11.23 | -17.42 | — | +0.00 | -17.42 | +114.57 | — |
| 2026-09-04 | `DELL` | 2 | — | $513.78 | +0.00 | $524.14 | +20.72 | +20.72 | +0.00 | +20.72 |
| 2026-09-04 | `TARS` | 16 | — | $82.70 | +0.00 | $90.78 | +129.28 | +129.28 | +0.00 | +129.28 |
| 2026-09-04 | `BRR` | 551 | — | $2.51 | +0.00 | $2.66 | +82.65 | +82.65 | +0.00 | +82.65 |
| 2026-09-04 | `MDB` | 3 | — | $378.34 | +0.00 | $368.74 | -28.80 | -28.80 | +0.00 | -28.80 |
| 2026-09-04 | `ASST` | 54 | — | $25.18 | +0.00 | $27.14 | +105.84 | +105.84 | +0.00 | +105.84 |
| 2026-09-04 | `DFDV` | 239 | — | $5.79 | +0.00 | $5.87 | +19.12 | +19.12 | +0.00 | +19.12 |
| 2026-09-04 | `TDS` | 36 | — | $37.44 | +0.00 | $37.83 | +14.04 | +14.04 | +0.00 | +14.04 |
| 2026-09-04 | `AHCO` | 219 | — | $6.32 | +0.00 | $6.49 | +37.23 | +37.23 | +0.00 | +37.23 |
| 2026-09-08 | `DELL` | 2 | $524.14 | $521.15 | -5.98 | — | +0.00 | -5.98 | +14.74 | — |
| 2026-09-08 | `TARS` | 16 | $90.78 | $89.67 | -17.76 | — | +0.00 | -17.76 | +111.52 | — |
| 2026-09-08 | `BRR` | 551 | $2.66 | $2.66 | +0.00 | — | +0.00 | +0.00 | +82.65 | — |
| 2026-09-08 | `MDB` | 3 | $368.74 | $360.75 | -23.97 | — | +0.00 | -23.97 | -52.77 | — |
| 2026-09-08 | `ASST` | 54 | $27.14 | $26.44 | -37.80 | — | +0.00 | -37.80 | +68.04 | — |
| 2026-09-08 | `DFDV` | 239 | $5.87 | $5.81 | -14.34 | — | +0.00 | -14.34 | +4.78 | — |
| 2026-09-08 | `TDS` | 36 | $37.83 | $37.75 | -2.88 | — | +0.00 | -2.88 | +11.16 | — |
| 2026-09-08 | `AHCO` | 219 | $6.49 | $6.48 | -2.19 | — | +0.00 | -2.19 | +35.04 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `TYRA` | 59 | — | $23.63 | +0.00 | $22.03 | -94.40 | -94.40 | +0.00 | -94.40 |
| 2026-09-11 | `INDP` | 523 | — | $2.70 | +0.00 | $2.77 | +36.61 | +36.61 | +0.00 | +36.61 |
| 2026-09-11 | `WLTH` | 129 | — | $10.95 | +0.00 | $10.38 | -73.53 | -73.53 | +0.00 | -73.53 |
| 2026-09-11 | `BNC` | 287 | — | $4.91 | +0.00 | $4.80 | -31.57 | -31.57 | +0.00 | -31.57 |
| 2026-09-11 | `SWKS` | 16 | — | $84.27 | +0.00 | $88.35 | +65.28 | +65.28 | +0.00 | +65.28 |
| 2026-09-11 | `ASO` | 25 | — | $54.91 | +0.00 | $55.36 | +11.25 | +11.25 | +0.00 | +11.25 |
| 2026-09-11 | `IRD` | 229 | — | $6.16 | +0.00 | $6.04 | -27.48 | -27.48 | +0.00 | -27.48 |
| 2026-09-11 | `COO` | 25 | — | $54.66 | +0.00 | $53.91 | -18.75 | -18.75 | +0.00 | -18.75 |
| 2026-09-14 | `TYRA` | 59 | $22.03 | $23.20 | +69.03 | — | +0.00 | +69.03 | -25.37 | — |
| 2026-09-14 | `INDP` | 523 | $2.77 | $2.80 | +15.69 | — | +0.00 | +15.69 | +52.30 | — |
| 2026-09-14 | `WLTH` | 129 | $10.38 | $10.29 | -11.61 | — | +0.00 | -11.61 | -85.14 | — |
| 2026-09-14 | `BNC` | 287 | $4.80 | $5.03 | +66.01 | — | +0.00 | +66.01 | +34.44 | — |
| 2026-09-14 | `SWKS` | 16 | $88.35 | $86.06 | -36.64 | — | +0.00 | -36.64 | +28.64 | — |
| 2026-09-14 | `ASO` | 25 | $55.36 | $54.75 | -15.25 | — | +0.00 | -15.25 | -4.00 | — |
| 2026-09-14 | `IRD` | 229 | $6.04 | $6.02 | -4.58 | — | +0.00 | -4.58 | -32.06 | — |
| 2026-09-14 | `COO` | 25 | $53.91 | $54.78 | +21.75 | — | +0.00 | +21.75 | +3.00 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `RDNT` | 18 | — | $77.12 | +0.00 | $75.78 | -24.12 | -24.12 | +0.00 | -24.12 |
| 2026-09-16 | `RIG` | 239 | — | $5.87 | +0.00 | $5.54 | -78.87 | -78.87 | +0.00 | -78.87 |
| 2026-09-16 | `VAL` | 16 | — | $87.40 | +0.00 | $82.52 | -78.08 | -78.08 | +0.00 | -78.08 |
| 2026-09-16 | `ADPT` | 51 | — | $27.09 | +0.00 | $27.67 | +29.58 | +29.58 | +0.00 | +29.58 |
| 2026-09-16 | `SWKS` | 15 | — | $89.38 | +0.00 | $85.59 | -56.85 | -56.85 | +0.00 | -56.85 |
| 2026-09-16 | `SDGR` | 60 | — | $23.29 | +0.00 | $23.93 | +38.40 | +38.40 | +0.00 | +38.40 |
| 2026-09-16 | `FPS` | 42 | — | $33.14 | +0.00 | $34.84 | +71.40 | +71.40 | +0.00 | +71.40 |
| 2026-09-16 | `CAI` | 49 | — | $28.16 | +0.00 | $28.21 | +2.45 | +2.45 | +0.00 | +2.45 |
| 2026-09-17 | `RDNT` | 18 | $75.78 | $76.44 | +11.88 | — | +0.00 | +11.88 | -12.24 | — |
| 2026-09-17 | `RIG` | 239 | $5.54 | $5.58 | +9.56 | — | +0.00 | +9.56 | -69.31 | — |
| 2026-09-17 | `VAL` | 16 | $82.52 | $83.20 | +10.88 | — | +0.00 | +10.88 | -67.20 | — |
| 2026-09-17 | `ADPT` | 51 | $27.67 | $28.23 | +28.56 | — | +0.00 | +28.56 | +58.14 | — |
| 2026-09-17 | `SWKS` | 15 | $85.59 | $86.76 | +17.55 | — | +0.00 | +17.55 | -39.30 | — |
| 2026-09-17 | `SDGR` | 60 | $23.93 | $24.09 | +9.60 | — | +0.00 | +9.60 | +48.00 | — |
| 2026-09-17 | `FPS` | 42 | $34.84 | $36.76 | +80.64 | $38.06 | +54.60 | +135.24 | +152.04 | +206.64 |
| 2026-09-17 | `CAI` | 49 | $28.21 | $28.59 | +18.86 | — | +0.00 | +18.86 | +21.31 | — |
| 2026-09-17 | `ILMN` | 5 | — | $233.85 | +0.00 | $245.18 | +56.65 | +56.65 | +0.00 | +56.65 |
| 2026-09-17 | `RVTY` | 9 | — | $147.61 | +0.00 | $146.73 | -7.92 | -7.92 | +0.00 | -7.92 |
| 2026-09-17 | `PGEN` | 183 | — | $7.59 | +0.00 | $7.87 | +51.24 | +51.24 | +0.00 | +51.24 |
| 2026-09-17 | `ARQT` | 53 | — | $25.95 | +0.00 | $26.46 | +27.03 | +27.03 | +0.00 | +27.03 |
| 2026-09-17 | `SMTC` | 8 | — | $170.85 | +0.00 | $178.19 | +58.72 | +58.72 | +0.00 | +58.72 |
| 2026-09-17 | `CIFR` | 77 | — | $18.04 | +0.00 | $16.94 | -84.31 | -84.31 | +0.00 | -84.31 |
| 2026-09-17 | `BRKR` | 22 | — | $61.90 | +0.00 | $63.05 | +25.30 | +25.30 | +0.00 | +25.30 |
| 2026-09-18 | `FPS` | 42 | $38.06 | $39.50 | +60.48 | — | +0.00 | +60.48 | +267.12 | — |
| 2026-09-18 | `ILMN` | 5 | $245.18 | $249.13 | +19.75 | $239.62 | -47.55 | -27.80 | +76.40 | +28.85 |
| 2026-09-18 | `RVTY` | 9 | $146.73 | $146.50 | -2.07 | — | +0.00 | -2.07 | -9.99 | — |
| 2026-09-18 | `PGEN` | 183 | $7.87 | $7.98 | +20.13 | — | +0.00 | +20.13 | +71.37 | — |
| 2026-09-18 | `ARQT` | 53 | $26.46 | $26.14 | -16.96 | $25.38 | -40.28 | -57.24 | +10.07 | -30.21 |
| 2026-09-18 | `SMTC` | 8 | $178.19 | $182.33 | +33.12 | — | +0.00 | +33.12 | +91.84 | — |
| 2026-09-18 | `CIFR` | 77 | $16.94 | $17.80 | +66.22 | — | +0.00 | +66.22 | -18.09 | — |
| 2026-09-18 | `BRKR` | 22 | $63.05 | $63.37 | +7.04 | — | +0.00 | +7.04 | +32.34 | — |
| 2026-09-18 | `SDGR` | 51 | — | $29.32 | +0.00 | $29.02 | -15.30 | -15.30 | +0.00 | -15.30 |
| 2026-09-18 | `EYPT` | 379 | — | $3.95 | +0.00 | $3.85 | -37.90 | -37.90 | +0.00 | -37.90 |
| 2026-09-18 | `BHVN` | 106 | — | $14.07 | +0.00 | $13.62 | -47.70 | -47.70 | +0.00 | -47.70 |
| 2026-09-18 | `RARE` | 101 | — | $14.79 | +0.00 | $14.51 | -28.28 | -28.28 | +0.00 | -28.28 |
| 2026-09-18 | `CYPH` | 494 | — | $3.04 | +0.00 | $3.60 | +279.11 | +279.11 | +0.00 | +279.11 |
| 2026-09-18 | `VICR` | 6 | — | $219.62 | +0.00 | $222.72 | +18.60 | +18.60 | +0.00 | +18.60 |

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
| 2026-08-26 | +2.02 | $36.07 | BMEA×778, GORO×357, ZURA×199, EZPW×36, ETON×19, WPM×8, SUZ×141, IAUX×667 | $10,268.78 | -82.36 | +341.48 | USDE, ASST | BMEA, GORO, ZURA, EZPW, ETON, WPM, SUZ, IAUX | $9.47 | $10,560.87 | USDE×880, ASST×246 |
| 2026-08-27 | — | $9.47 | USDE×880, ASST×246 | $11,252.17 | +691.30 | +264.48 | DKS | USDE, ASST | $35.60 | $11,499.59 | DKS×87 |
| 2026-08-28 | +0.75 | $35.60 | DKS×87 | $11,589.20 | +89.61 | -249.64 | ANF, NCNO, TH, GAP, PLAB, SLF, WSM | DKS | $124.42 | $11,322.32 | ANF×11, NCNO×71, TH×87, GAP×67, PLAB×55, SLF×20, WSM×7 |
| 2026-08-31 | -5.85 | $124.42 | ANF×11, NCNO×71, TH×87, GAP×67, PLAB×55, SLF×20, WSM×7 | $11,218.77 | -103.55 | +0.00 | — | ANF, NCNO, TH, GAP, PLAB, SLF, WSM | $11,203.72 | $11,203.72 | — |
| 2026-09-01 | -6.30 | $11,203.72 | — | $11,203.72 | -0.00 | +0.00 | — | — | $11,203.72 | $11,203.72 | — |
| 2026-09-02 | -3.83 | $11,203.72 | — | $11,203.72 | -0.00 | +0.00 | — | — | $11,203.72 | $11,203.72 | — |
| 2026-09-03 | -0.90 | $11,203.72 | — | $11,203.72 | -0.00 | -48.78 | RVTY, ARCT, CRDL, MMED, NVAX, BMEA, DUOL, ALMS | — | $193.75 | $11,124.08 | RVTY×10, ARCT×83, CRDL×642, MMED×58, NVAX×134, BMEA×725, DUOL×8, ALMS×134 |
| 2026-09-04 | +2.25 | $193.75 | RVTY×10, ARCT×83, CRDL×642, MMED×58, NVAX×134, BMEA×725, DUOL×8, ALMS×134 | $11,108.12 | -15.96 | +380.08 | DELL, TARS, BRR, MDB, ASST, DFDV, TDS, AHCO | RVTY, ARCT, CRDL, MMED, NVAX, BMEA, DUOL, ALMS | $709.32 | $11,433.64 | DELL×2, TARS×16, BRR×551, MDB×3, ASST×54, DFDV×239, TDS×36, AHCO×219 |
| 2026-09-08 | -11.47 | $709.32 | DELL×2, TARS×16, BRR×551, MDB×3, ASST×54, DFDV×239, TDS×36, AHCO×219 | $11,328.72 | -104.92 | +0.00 | — | DELL, TARS, BRR, MDB, ASST, DFDV, TDS, AHCO | $11,305.12 | $11,305.12 | — |
| 2026-09-09 | -13.95 | $11,305.12 | — | $11,305.12 | -0.00 | +0.00 | — | — | $11,305.12 | $11,305.12 | — |
| 2026-09-10 | -13.28 | $11,305.12 | — | $11,305.12 | -0.00 | +0.00 | — | — | $11,305.12 | $11,305.12 | — |
| 2026-09-11 | +0.50 | $11,305.12 | — | $11,305.12 | -0.00 | -132.59 | TYRA, INDP, WLTH, BNC, SWKS, ASO, IRD, COO | — | $154.80 | $11,148.41 | TYRA×59, INDP×523, WLTH×129, BNC×287, SWKS×16, ASO×25, IRD×229, COO×25 |
| 2026-09-14 | -11.00 | $154.80 | TYRA×59, INDP×523, WLTH×129, BNC×287, SWKS×16, ASO×25, IRD×229, COO×25 | $11,252.81 | +104.40 | +0.00 | — | TYRA, INDP, WLTH, BNC, SWKS, ASO, IRD, COO | $11,228.38 | $11,228.38 | — |
| 2026-09-15 | -3.84 | $11,228.38 | — | $11,228.38 | -0.00 | +0.00 | — | — | $11,228.38 | $11,228.38 | — |
| 2026-09-16 | +5.30 | $11,228.38 | — | $11,228.38 | -0.00 | -96.09 | RDNT, RIG, VAL, ADPT, SWKS, SDGR, FPS, CAI | — | $129.71 | $11,114.52 | RDNT×18, RIG×239, VAL×16, ADPT×51, SWKS×15, SDGR×60, FPS×42, CAI×49 |
| 2026-09-17 | +7.38 | $129.71 | RDNT×18, RIG×239, VAL×16, ADPT×51, SWKS×15, SDGR×60, FPS×42, CAI×49 | $11,302.06 | +187.54 | +181.31 | ILMN, RVTY, PGEN, ARQT, SMTC, CIFR, BRKR | RDNT, RIG, VAL, ADPT, SWKS, SDGR, CAI | $347.95 | $11,452.53 | FPS×42, ILMN×5, RVTY×9, PGEN×183, ARQT×53, SMTC×8, CIFR×77, BRKR×22 |
| 2026-09-18 | +4.86 | $347.95 | FPS×42, ILMN×5, RVTY×9, PGEN×183, ARQT×53, SMTC×8, CIFR×77, BRKR×22 | $11,640.24 | +187.71 | +80.70 | SDGR, EYPT, BHVN, RARE, CYPH, VICR | FPS, RVTY, PGEN, SMTC, CIFR, BRKR | $181.45 | $11,687.81 | ILMN×5, ARQT×53, SDGR×51, EYPT×379, BHVN×106, RARE×101, CYPH×494, VICR×6 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $8,739.75 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 84 | $14.80 | $2.24 | — | $7,494.31 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $6,240.67 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 299 | $4.18 | $3.86 | — | $4,986.99 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $3,747.28 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 112 | $11.12 | $2.33 | — | $2,499.51 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+36.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 464 | $2.69 | $5.99 | — | $1,245.37 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 170 | $7.29 | $2.50 | — | $3.57 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.57 | ▼ close $9,801.97 vs 09:30 $10,000.00 (session -164.42) | 16:00 close · cash $3.57 · equity $9,801.97 vs 09:30 $10,000.00 (-198.03; session marks -164.42) · 8 name(s) marked open→close (per-name table). BTBT×833 09:30 $1.50 → close $1.57 +58.31; BETR×84 09:30 $14.80 → close $13.73 -89.88; ANGX×290 09:30 $4.31 → close $4.37 +17.40; HYLN×299 09:30 $4.18 → close $4.06 -35.88; ADUR×75 09:30 $16.50 → close $16.17 -24.75; AIRO×112 09:30 $11.12 → close $9.57 -173.60; NCMI×464 09:30 $2.69 → close $2.86 +78.88; QMLS×170 09:30 $7.29 → close $7.32 +5.10 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.57 | ▼ 09:30 equity $9,759.50 vs yday $9,801.97 (-42.47) | 09:30 open · cash $3.57 (unchanged overnight, no fees) · equity $9,759.50 vs prior close $9,801.97 (-42.47) · 8 name(s) re-marked at the open (per-name table). BTBT×833 yday $1.57 → 09:30 $1.52 -41.65; BETR×84 yday $13.73 → 09:30 $13.67 -5.04; ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; HYLN×299 yday $4.06 → 09:30 $4.10 +11.96; ADUR×75 yday $16.17 → 09:30 $15.73 -33.00; AIRO×112 yday $9.57 → 09:30 $9.57 +0.00; NCMI×464 yday $2.86 → 09:30 $2.80 -27.84; QMLS×170 yday $7.32 → 09:30 $7.24 -13.60 | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 833 | $1.52 | $10.89 | $-4.98 | $1,258.83 | ▼ -4.98 after sell → book $9,748.60; vs 09:30 mark -10.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 84 | $13.67 | $2.27 | $-99.43 | $2,404.85 | ▼ -99.43 after sell → book $9,746.34; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $3,735.05 | ▲ +76.56 after sell → book $9,742.54; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 299 | $4.10 | $3.92 | $-31.69 | $4,957.03 | ▼ -31.69 after sell → book $9,738.62; vs 09:30 mark -3.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 75 | $15.73 | $2.24 | $-62.20 | $6,134.54 | ▼ -62.20 after sell → book $9,736.38; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 112 | $9.57 | $2.35 | $-178.28 | $7,204.03 | ▼ -178.28 after sell → book $9,734.03; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
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
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $1,172.08 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy,oppset; 🔵; ⚪; ret5=+9.2; leftover $1161.39 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
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
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 109 | $11.13 | $2.32 | — | $5,020.60 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,mover_buy,oppset; 🔵; ⚪; ret5=+39.8; leftover $1215.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 920 | $1.32 | $11.87 | — | $3,794.33 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1215.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 732 | $1.66 | $9.44 | — | $2,569.77 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1215.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `INDP` | 874 | $1.39 | $11.27 | — | $1,343.63 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+30.2; leftover $1215.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 146 | $8.28 | $2.43 | — | $132.32 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+36.6; leftover $1215.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $132.32 | ▲ close $9,871.63 vs 09:30 $9,747.22 (session +193.08) | 16:00 close · cash $132.32 · equity $9,871.63 vs 09:30 $9,747.22 (+124.41; session marks +193.08) · 8 name(s) marked open→close (per-name table). AU×10 09:30 $119.43 → close $121.22 +17.90; AUPH×70 09:30 $17.20 → close $16.65 -38.50; AEM×5 09:30 $216.30 → close $216.06 -1.20; ARCT×109 09:30 $11.13 → close $13.45 +252.88; CYPH×920 09:30 $1.32 → close $1.42 +92.00; BTBT×732 09:30 $1.66 → close $1.53 -95.16; INDP×874 09:30 $1.39 → close $1.29 -87.40; MRVI×146 09:30 $8.28 → close $8.64 +52.56 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $132.32 | ▲ 09:30 equity $10,191.54 vs yday $9,871.63 (+319.91) | 09:30 open · cash $132.32 (unchanged overnight, no fees) · equity $10,191.54 vs prior close $9,871.63 (+319.91) · 8 name(s) re-marked at the open (per-name table). AU×10 yday $121.22 → 09:30 $120.51 -7.10; AUPH×70 yday $16.65 → 09:30 $16.57 -5.60; AEM×5 yday $216.06 → 09:30 $217.03 +4.85; ARCT×109 yday $13.45 → 09:30 $13.33 -13.08; CYPH×920 yday $1.42 → 09:30 $1.83 +377.20; BTBT×732 yday $1.53 → 09:30 $1.55 +14.64; INDP×874 yday $1.29 → 09:30 $1.24 -43.70; MRVI×146 yday $8.64 → 09:30 $8.59 -7.30 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,335.38 | ▲ +6.74 after sell → book $10,189.50; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 70 | $16.57 | $2.22 | $-48.52 | $2,493.06 | ▼ -48.52 after sell → book $10,187.28; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,576.19 | ▼ -0.38 after sell → book $10,185.26; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 109 | $13.33 | $2.35 | $+235.14 | $5,026.81 | ▲ +235.14 after sell → book $10,182.91; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 920 | $1.83 | $12.03 | $+445.30 | $6,698.38 | ▲ +445.30 after sell → book $10,170.88; vs 09:30 mark -12.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 732 | $1.55 | $9.57 | $-99.54 | $7,823.40 | ▼ -99.54 after sell → book $10,161.30; vs 09:30 mark -9.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `INDP` | 874 | $1.24 | $11.43 | $-153.80 | $8,895.73 | ▼ -153.80 after sell → book $10,149.87; vs 09:30 mark -11.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 146 | $8.59 | $2.46 | $+40.37 | $10,147.41 | ▲ +40.37 after sell → book $10,147.41; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,147.41 | ▲ close $10,147.41 vs 09:30 $10,191.54 (session +0.00) | 16:00 close · cash $10,147.41 · no lots left · equity $10,147.41. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,147.41 | ▲ 09:30 equity $10,147.41 vs yday $10,147.41 (-0.00) | 09:30 open · cash $10,147.41 · no holdings · equity $10,147.41 vs prior close $10,147.41 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 778 | $1.63 | $10.04 | — | $8,869.23 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1268.43 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 357 | $3.55 | $4.61 | — | $7,597.28 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+27.9; leftover $1268.43 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 199 | $6.37 | $2.59 | — | $6,327.06 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,oppset; 🔵; ⚪; ret5=+10.9; leftover $1268.43 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 36 | $35.05 | $2.10 | — | $5,063.16 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,oppset; 🔵; ⚪; ret5=+19.7; leftover $1268.43 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ETON` | 19 | $64.55 | $2.05 | — | $3,834.67 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+4.4; leftover $1268.43 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `WPM` | 8 | $156.51 | $2.01 | — | $2,580.57 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+17.4; leftover $1268.43 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUZ` | 141 | $8.98 | $2.41 | — | $1,311.98 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy,oppset; ⚪; ret5=+15.4; leftover $1268.43 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `IAUX` | 667 | $1.90 | $8.60 | — | $36.07 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+16.4; leftover $1268.43 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $36.07 | ▲ close $10,351.14 vs 09:30 $10,147.41 (session +238.14) | 16:00 close · cash $36.07 · equity $10,351.14 vs 09:30 $10,147.41 (+203.73; session marks +238.14) · 8 name(s) marked open→close (per-name table). BMEA×778 09:30 $1.63 → close $1.73 +77.80; GORO×357 09:30 $3.55 → close $3.87 +114.24; ZURA×199 09:30 $6.37 → close $6.32 -9.95; EZPW×36 09:30 $35.05 → close $35.23 +6.48; ETON×19 09:30 $64.55 → close $63.05 -28.50; WPM×8 09:30 $156.51 → close $163.72 +57.68; SUZ×141 09:30 $8.98 → close $9.03 +7.05; IAUX×667 09:30 $1.90 → close $1.92 +13.34 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $36.07 | ▼ 09:30 equity $10,268.78 vs yday $10,351.14 (-82.36) | 09:30 open · cash $36.07 (unchanged overnight, no fees) · equity $10,268.78 vs prior close $10,351.14 (-82.36) · 8 name(s) re-marked at the open (per-name table). BMEA×778 yday $1.73 → 09:30 $1.75 +19.45; GORO×357 yday $3.87 → 09:30 $3.77 -35.70; ZURA×199 yday $6.32 → 09:30 $6.13 -37.81; EZPW×36 yday $35.23 → 09:30 $35.70 +16.92; ETON×19 yday $63.05 → 09:30 $63.60 +10.45; WPM×8 yday $163.72 → 09:30 $160.93 -22.32; SUZ×141 yday $9.03 → 09:30 $9.03 +0.00; IAUX×667 yday $1.92 → 09:30 $1.87 -33.35 | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 778 | $1.75 | $10.18 | $+77.04 | $1,391.29 | ▲ +77.04 after sell → book $10,258.61; vs 09:30 mark -10.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GORO` | 357 | $3.77 | $4.68 | $+69.26 | $2,732.50 | ▲ +69.26 after sell → book $10,253.93; vs 09:30 mark -4.68 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZURA` | 199 | $6.13 | $2.63 | $-52.98 | $3,949.74 | ▼ -52.98 after sell → book $10,251.30; vs 09:30 mark -2.63 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 36 | $35.70 | $2.12 | $+19.18 | $5,232.82 | ▲ +19.18 after sell → book $10,249.18; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ETON` | 19 | $63.60 | $2.07 | $-22.16 | $6,439.16 | ▼ -22.16 after sell → book $10,247.12; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `WPM` | 8 | $160.93 | $2.03 | $+31.31 | $7,724.56 | ▲ +31.31 after sell → book $10,245.08; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SUZ` | 141 | $9.03 | $2.45 | $+2.19 | $8,995.35 | ▲ +2.19 after sell → book $10,242.64; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `IAUX` | 667 | $1.87 | $8.72 | $-37.34 | $10,233.91 | ▼ -37.34 after sell → book $10,233.91; vs 09:30 mark -8.73 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 880 | $5.81 | $11.35 | — | $5,109.76 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $5116.96 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `ASST` | 246 | $20.72 | $3.17 | — | $9.47 | — | combo gate; gate join=good,vol=good,last_green=True; list oppset; 🔵; ret5=+67.1; leftover $5116.96 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.47 | ▲ close $10,560.87 vs 09:30 $10,268.78 (session +341.48) | 16:00 close · cash $9.47 · equity $10,560.87 vs 09:30 $10,268.78 (+292.09; session marks +341.48) · 2 name(s) marked open→close (per-name table). USDE×880 09:30 $5.81 → close $5.98 +149.60; ASST×246 09:30 $20.72 → close $21.50 +191.88 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.47 | ▲ 09:30 equity $11,252.17 vs yday $10,560.87 (+691.30) | 09:30 open · cash $9.47 (unchanged overnight, no fees) · equity $11,252.17 vs prior close $10,560.87 (+691.30) · 2 name(s) re-marked at the open (per-name table). USDE×880 yday $5.98 → 09:30 $6.50 +457.60; ASST×246 yday $21.50 → 09:30 $22.45 +233.70 | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 880 | $6.50 | $11.54 | $+584.30 | $5,717.92 | ▲ +584.30 after sell → book $11,240.62; vs 09:30 mark -11.55 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟡 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `ASST` | 246 | $22.45 | $3.26 | $+419.15 | $11,237.36 | ▲ +419.15 after sell → book $11,237.36; vs 09:30 mark -3.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `DKS` | 87 | $128.73 | $2.25 | — | $35.60 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-32.2; leftover $11237.36 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.60 | ▲ close $11,499.59 vs 09:30 $11,252.17 (session +264.48) | 16:00 close · cash $35.60 · equity $11,499.59 vs 09:30 $11,252.17 (+247.42; session marks +264.48) · 1 name(s) marked open→close (per-name table). DKS×87 09:30 $128.73 → close $131.77 +264.48 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.60 | ▲ 09:30 equity $11,589.20 vs yday $11,499.59 (+89.61) | 09:30 open · cash $35.60 (unchanged overnight, no fees) · equity $11,589.20 vs prior close $11,499.59 (+89.61) · 1 name(s) re-marked at the open (per-name table). DKS×87 yday $131.77 → 09:30 $132.80 +89.61 | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 87 | $132.80 | $2.36 | $+349.48 | $11,586.85 | ▲ +349.48 after sell → book $11,586.85; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 11 | $146.07 | $2.02 | — | $9,978.05 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,oppset; 🔵; ret5=+38.8; leftover $1655.26 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 71 | $23.30 | $2.20 | — | $8,321.55 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+14.5; leftover $1655.26 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 87 | $19.00 | $2.25 | — | $6,666.30 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; ret5=+7.5; leftover $1655.26 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 67 | $24.69 | $2.19 | — | $5,009.88 | — | combo gate; gate join=good,vol=good,last_green=True; list earn_react; ret5=+5.8; leftover $1655.26 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `PLAB` | 55 | $30.01 | $2.15 | — | $3,357.17 | — | combo gate; gate join=good,vol=good,last_green=True; list oppset; 🔵; ret5=-0.9; leftover $1655.26 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SLF` | 20 | $78.95 | $2.05 | — | $1,776.12 | — | combo gate; gate join=good,vol=good,last_green=True; list oppset; ret5=+0.4; leftover $1655.26 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `WSM` | 7 | $235.67 | $2.01 | — | $124.42 | — | combo gate; gate join=good,vol=good,last_green=True; list oppset; ret5=+1.1; leftover $1655.26 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $124.42 | ▼ close $11,322.32 vs 09:30 $11,589.20 (session -249.64) | 16:00 close · cash $124.42 · equity $11,322.32 vs 09:30 $11,589.20 (-266.88; session marks -249.64) · 7 name(s) marked open→close (per-name table). ANF×11 09:30 $146.07 → close $148.42 +25.85; NCNO×71 09:30 $23.30 → close $22.99 -22.01; TH×87 09:30 $19.00 → close $18.55 -39.15; GAP×67 09:30 $24.69 → close $23.48 -81.07; PLAB×55 09:30 $30.01 → close $27.73 -125.40; SLF×20 09:30 $78.95 → close $78.76 -3.80; WSM×7 09:30 $235.67 → close $235.09 -4.06 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $124.42 | ▼ 09:30 equity $11,218.77 vs yday $11,322.32 (-103.55) | 09:30 open · cash $124.42 (unchanged overnight, no fees) · equity $11,218.77 vs prior close $11,322.32 (-103.55) · 7 name(s) re-marked at the open (per-name table). ANF×11 yday $148.42 → 09:30 $148.03 -4.29; NCNO×71 yday $22.99 → 09:30 $22.66 -23.43; TH×87 yday $18.55 → 09:30 $18.12 -36.98; GAP×67 yday $23.48 → 09:30 $22.98 -33.50; PLAB×55 yday $27.73 → 09:30 $28.04 +17.05; SLF×20 yday $78.76 → 09:30 $78.70 -1.20; WSM×7 yday $235.09 → 09:30 $232.06 -21.21 | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 11 | $148.03 | $2.05 | $+17.49 | $1,750.71 | ▲ +17.49 after sell → book $11,216.72; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NCNO` | 71 | $22.66 | $2.23 | $-49.87 | $3,357.34 | ▼ -49.87 after sell → book $11,214.49; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `TH` | 87 | $18.12 | $2.28 | $-80.65 | $4,931.93 | ▼ -80.65 after sell → book $11,212.21; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `GAP` | 67 | $22.98 | $2.21 | $-118.98 | $6,469.38 | ▼ -118.98 after sell → book $11,210.00; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟢 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `PLAB` | 55 | $28.04 | $2.18 | $-112.68 | $8,009.40 | ▼ -112.68 after sell → book $11,207.82; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLF` | 20 | $78.70 | $2.07 | $-9.12 | $9,581.33 | ▼ -9.12 after sell → book $11,205.75; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `WSM` | 7 | $232.06 | $2.03 | $-29.31 | $11,203.72 | ▼ -29.31 after sell → book $11,203.72; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,203.72 | ▲ close $11,203.72 vs 09:30 $11,218.77 (session +0.00) | 16:00 close · cash $11,203.72 · no lots left · equity $11,203.72. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,203.72 | ▲ 09:30 equity $11,203.72 vs yday $11,203.72 (-0.00) | 09:30 open · cash $11,203.72 · no holdings · equity $11,203.72 vs prior close $11,203.72 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,203.72 | ▲ close $11,203.72 vs 09:30 $11,203.72 (session +0.00) | 16:00 close · cash $11,203.72 · no lots left · equity $11,203.72. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,203.72 | ▲ 09:30 equity $11,203.72 vs yday $11,203.72 (-0.00) | 09:30 open · cash $11,203.72 · no holdings · equity $11,203.72 vs prior close $11,203.72 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,203.72 | ▲ close $11,203.72 vs 09:30 $11,203.72 (session +0.00) | 16:00 close · cash $11,203.72 · no lots left · equity $11,203.72. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,203.72 | ▲ 09:30 equity $11,203.72 vs yday $11,203.72 (-0.00) | 09:30 open · cash $11,203.72 · no holdings · equity $11,203.72 vs prior close $11,203.72 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $9,877.20 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1400.46 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 83 | $16.77 | $2.24 | — | $8,483.05 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy,oppset; 🔵; ⚪; ret5=+5.7; leftover $1400.46 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 642 | $2.18 | $8.28 | — | $7,075.21 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1400.46 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 58 | $23.88 | $2.16 | — | $5,688.00 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+21.9; leftover $1400.46 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 134 | $10.42 | $2.39 | — | $4,289.33 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; leftover $1400.46 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 725 | $1.93 | $9.35 | — | $2,880.73 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1400.46 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DUOL` | 8 | $161.54 | $2.01 | — | $1,586.39 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+12.0; leftover $1400.46 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ALMS` | 134 | $10.38 | $2.39 | — | $193.75 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover,oppset; 🔵; ret5=-56.2; leftover $1400.46 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $193.75 | ▼ close $11,124.08 vs 09:30 $11,203.72 (session -48.78) | 16:00 close · cash $193.75 · equity $11,124.08 vs 09:30 $11,203.72 (-79.64; session marks -48.78) · 8 name(s) marked open→close (per-name table). RVTY×10 09:30 $132.45 → close $130.63 -18.20; ARCT×83 09:30 $16.77 → close $15.56 -100.43; CRDL×642 09:30 $2.18 → close $2.16 -12.84; MMED×58 09:30 $23.88 → close $23.84 -2.32; NVAX×134 09:30 $10.42 → close $10.34 -10.72; BMEA×725 09:30 $1.93 → close $1.91 -14.50; DUOL×8 09:30 $161.54 → close $158.82 -21.76; ALMS×134 09:30 $10.38 → close $11.36 +131.99 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $193.75 | ▼ 09:30 equity $11,108.12 vs yday $11,124.08 (-15.96) | 09:30 open · cash $193.75 (unchanged overnight, no fees) · equity $11,108.12 vs prior close $11,124.08 (-15.96) · 8 name(s) re-marked at the open (per-name table). RVTY×10 yday $130.63 → 09:30 $130.03 -6.00; ARCT×83 yday $15.56 → 09:30 $15.61 +4.15; CRDL×642 yday $2.16 → 09:30 $2.16 +0.00; MMED×58 yday $23.84 → 09:30 $23.84 +0.00; NVAX×134 yday $10.34 → 09:30 $10.50 +21.44; BMEA×725 yday $1.91 → 09:30 $1.90 -7.25; DUOL×8 yday $158.82 → 09:30 $157.46 -10.88; ALMS×134 yday $11.36 → 09:30 $11.23 -17.42 | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $130.03 | $2.04 | $-28.26 | $1,492.01 | ▼ -28.26 after sell → book $11,106.08; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 83 | $15.61 | $2.26 | $-100.78 | $2,785.38 | ▼ -100.78 after sell → book $11,103.82; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 642 | $2.16 | $8.40 | $-29.52 | $4,163.70 | ▼ -29.52 after sell → book $11,095.42; vs 09:30 mark -8.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 58 | $23.84 | $2.19 | $-6.67 | $5,544.23 | ▼ -6.67 after sell → book $11,093.23; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 134 | $10.50 | $2.43 | $+5.90 | $6,948.81 | ▲ +5.90 after sell → book $11,090.81; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `BMEA` | 725 | $1.90 | $9.48 | $-40.59 | $8,316.82 | ▼ -40.59 after sell → book $11,081.32; vs 09:30 mark -9.49 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `DUOL` | 8 | $157.46 | $2.03 | $-36.69 | $9,574.47 | ▼ -36.69 after sell → book $11,079.29; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ALMS` | 134 | $11.23 | $2.43 | $+109.75 | $11,076.86 | ▲ +109.75 after sell → book $11,076.86; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $513.78 | $2.00 | — | $10,047.31 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy,oppset; 🔵; ⚪; ret5=+9.3; leftover $1384.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 16 | $82.70 | $2.04 | — | $8,722.07 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+15.7; leftover $1384.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 551 | $2.51 | $7.11 | — | $7,331.95 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+21.8; leftover $1384.61 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MDB` | 3 | $378.34 | $2.00 | — | $6,194.93 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-12.7; leftover $1384.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 54 | $25.18 | $2.15 | — | $4,833.06 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,oppset; 🔵; ret5=+16.0; leftover $1384.61 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 239 | $5.79 | $3.08 | — | $3,446.17 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $1384.61 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TDS` | 36 | $37.44 | $2.10 | — | $2,096.23 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+14.1; leftover $1384.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `AHCO` | 219 | $6.32 | $2.83 | — | $709.32 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+10.7; leftover $1384.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $709.32 | ▲ close $11,433.64 vs 09:30 $11,108.12 (session +380.08) | 16:00 close · cash $709.32 · equity $11,433.64 vs 09:30 $11,108.12 (+325.52; session marks +380.08) · 8 name(s) marked open→close (per-name table). DELL×2 09:30 $513.78 → close $524.14 +20.72; TARS×16 09:30 $82.70 → close $90.78 +129.28; BRR×551 09:30 $2.51 → close $2.66 +82.65; MDB×3 09:30 $378.34 → close $368.74 -28.80; ASST×54 09:30 $25.18 → close $27.14 +105.84; DFDV×239 09:30 $5.79 → close $5.87 +19.12; TDS×36 09:30 $37.44 → close $37.83 +14.04; AHCO×219 09:30 $6.32 → close $6.49 +37.23 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $709.32 | ▼ 09:30 equity $11,328.72 vs yday $11,433.64 (-104.92) | 09:30 open · cash $709.32 (unchanged overnight, no fees) · equity $11,328.72 vs prior close $11,433.64 (-104.92) · 8 name(s) re-marked at the open (per-name table). DELL×2 yday $524.14 → 09:30 $521.15 -5.98; TARS×16 yday $90.78 → 09:30 $89.67 -17.76; BRR×551 yday $2.66 → 09:30 $2.66 +0.00; MDB×3 yday $368.74 → 09:30 $360.75 -23.97; ASST×54 yday $27.14 → 09:30 $26.44 -37.80; DFDV×239 yday $5.87 → 09:30 $5.81 -14.34; TDS×36 yday $37.83 → 09:30 $37.75 -2.88; AHCO×219 yday $6.49 → 09:30 $6.48 -2.19 | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 2 | $521.15 | $2.02 | $+10.73 | $1,749.61 | ▲ +10.73 after sell → book $11,326.71; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `TARS` | 16 | $89.67 | $2.06 | $+107.42 | $3,182.27 | ▲ +107.42 after sell → book $11,324.65; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BRR` | 551 | $2.66 | $7.21 | $+68.33 | $4,640.72 | ▲ +68.33 after sell → book $11,317.44; vs 09:30 mark -7.21 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `MDB` | 3 | $360.75 | $2.02 | $-56.79 | $5,720.95 | ▼ -56.79 after sell → book $11,315.42; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 54 | $26.44 | $2.17 | $+63.71 | $7,146.53 | ▲ +63.71 after sell → book $11,313.24; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 239 | $5.81 | $3.13 | $-1.44 | $8,531.99 | ▼ -1.44 after sell → book $11,310.11; vs 09:30 mark -3.13 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `TDS` | 36 | $37.75 | $2.12 | $+6.94 | $9,888.87 | ▲ +6.94 after sell → book $11,307.99; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `AHCO` | 219 | $6.48 | $2.87 | $+29.34 | $11,305.12 | ▲ +29.34 after sell → book $11,305.12; vs 09:30 mark -2.87 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,305.12 | ▲ close $11,305.12 vs 09:30 $11,328.72 (session +0.00) | 16:00 close · cash $11,305.12 · no lots left · equity $11,305.12. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,305.12 | ▲ 09:30 equity $11,305.12 vs yday $11,305.12 (-0.00) | 09:30 open · cash $11,305.12 · no holdings · equity $11,305.12 vs prior close $11,305.12 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,305.12 | ▲ close $11,305.12 vs 09:30 $11,305.12 (session +0.00) | 16:00 close · cash $11,305.12 · no lots left · equity $11,305.12. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,305.12 | ▲ 09:30 equity $11,305.12 vs yday $11,305.12 (-0.00) | 09:30 open · cash $11,305.12 · no holdings · equity $11,305.12 vs prior close $11,305.12 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,305.12 | ▲ close $11,305.12 vs 09:30 $11,305.12 (session +0.00) | 16:00 close · cash $11,305.12 · no lots left · equity $11,305.12. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,305.12 | ▲ 09:30 equity $11,305.12 vs yday $11,305.12 (-0.00) | 09:30 open · cash $11,305.12 · no holdings · equity $11,305.12 vs prior close $11,305.12 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 59 | $23.63 | $2.17 | — | $9,908.78 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,oppset; ret5=-6.3; leftover $1413.14 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 523 | $2.70 | $6.75 | — | $8,489.94 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $1413.14 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 129 | $10.95 | $2.38 | — | $7,075.01 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,oppset; 🔵; ret5=+20.8; leftover $1413.14 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 287 | $4.91 | $3.70 | — | $5,662.14 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $1413.14 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 16 | $84.27 | $2.04 | — | $4,311.78 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ret5=+17.2; leftover $1413.14 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ASO` | 25 | $54.91 | $2.06 | — | $2,936.96 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+24.3; leftover $1413.14 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 229 | $6.16 | $2.95 | — | $1,523.37 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,oppset; 🔵; ret5=+36.4; leftover $1413.14 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `COO` | 25 | $54.66 | $2.06 | — | $154.80 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover,oppset; ret5=-22.3; leftover $1413.14 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $154.80 | ▼ close $11,148.41 vs 09:30 $11,305.12 (session -132.59) | 16:00 close · cash $154.80 · equity $11,148.41 vs 09:30 $11,305.12 (-156.71; session marks -132.59) · 8 name(s) marked open→close (per-name table). TYRA×59 09:30 $23.63 → close $22.03 -94.40; INDP×523 09:30 $2.70 → close $2.77 +36.61; WLTH×129 09:30 $10.95 → close $10.38 -73.53; BNC×287 09:30 $4.91 → close $4.80 -31.57; SWKS×16 09:30 $84.27 → close $88.35 +65.28; ASO×25 09:30 $54.91 → close $55.36 +11.25; IRD×229 09:30 $6.16 → close $6.04 -27.48; COO×25 09:30 $54.66 → close $53.91 -18.75 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $154.80 | ▲ 09:30 equity $11,252.81 vs yday $11,148.41 (+104.40) | 09:30 open · cash $154.80 (unchanged overnight, no fees) · equity $11,252.81 vs prior close $11,148.41 (+104.40) · 8 name(s) re-marked at the open (per-name table). TYRA×59 yday $22.03 → 09:30 $23.20 +69.03; INDP×523 yday $2.77 → 09:30 $2.80 +15.69; WLTH×129 yday $10.38 → 09:30 $10.29 -11.61; BNC×287 yday $4.80 → 09:30 $5.03 +66.01; SWKS×16 yday $88.35 → 09:30 $86.06 -36.64; ASO×25 yday $55.36 → 09:30 $54.75 -15.25; IRD×229 yday $6.04 → 09:30 $6.02 -4.58; COO×25 yday $53.91 → 09:30 $54.78 +21.75 | — |
| 2026-09-14 09:30 ET | **SELL** | `TYRA` | 59 | $23.20 | $2.19 | $-29.72 | $1,521.42 | ▼ -29.72 after sell → book $11,250.63; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `INDP` | 523 | $2.80 | $6.85 | $+38.71 | $2,978.97 | ▲ +38.71 after sell → book $11,243.78; vs 09:30 mark -6.85 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🟡 digest🔴 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `WLTH` | 129 | $10.29 | $2.41 | $-89.93 | $4,303.97 | ▼ -89.93 after sell → book $11,241.37; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `BNC` | 287 | $5.03 | $3.76 | $+26.98 | $5,743.82 | ▲ +26.98 after sell → book $11,237.61; vs 09:30 mark -3.76 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `SWKS` | 16 | $86.06 | $2.06 | $+24.54 | $7,118.72 | ▲ +24.54 after sell → book $11,235.55; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ASO` | 25 | $54.75 | $2.09 | $-8.15 | $8,485.39 | ▼ -8.15 after sell → book $11,233.47; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 229 | $6.02 | $3.00 | $-38.02 | $9,860.96 | ▼ -38.02 after sell → book $11,230.46; vs 09:30 mark -3.01 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🔴 judge🟡 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `COO` | 25 | $54.78 | $2.09 | $-1.15 | $11,228.38 | ▼ -1.15 after sell → book $11,228.38; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🔴 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,228.38 | ▲ close $11,228.38 vs 09:30 $11,252.81 (session +0.00) | 16:00 close · cash $11,228.38 · no lots left · equity $11,228.38. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,228.38 | ▲ 09:30 equity $11,228.38 vs yday $11,228.38 (-0.00) | 09:30 open · cash $11,228.38 · no holdings · equity $11,228.38 vs prior close $11,228.38 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,228.38 | ▲ close $11,228.38 vs 09:30 $11,228.38 (session +0.00) | 16:00 close · cash $11,228.38 · no lots left · equity $11,228.38. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,228.38 | ▲ 09:30 equity $11,228.38 vs yday $11,228.38 (-0.00) | 09:30 open · cash $11,228.38 · no holdings · equity $11,228.38 vs prior close $11,228.38 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 18 | $77.12 | $2.04 | — | $9,838.17 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; ret5=+7.2; leftover $1403.55 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 239 | $5.87 | $3.08 | — | $8,432.16 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1403.55 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 catal🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 16 | $87.40 | $2.04 | — | $7,031.72 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1403.55 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 51 | $27.09 | $2.14 | — | $5,647.99 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1403.55 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 15 | $89.38 | $2.04 | — | $4,305.25 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $1403.55 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 60 | $23.29 | $2.17 | — | $2,905.68 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,oppset; 🔵; ret5=+16.1; leftover $1403.55 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 42 | $33.14 | $2.12 | — | $1,511.69 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,oppset; 🔵; ret5=-2.9; leftover $1403.55 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `CAI` | 49 | $28.16 | $2.14 | — | $129.71 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ret5=+14.8; leftover $1403.55 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $129.71 | ▼ close $11,114.52 vs 09:30 $11,228.38 (session -96.09) | 16:00 close · cash $129.71 · equity $11,114.52 vs 09:30 $11,228.38 (-113.86; session marks -96.09) · 8 name(s) marked open→close (per-name table). RDNT×18 09:30 $77.12 → close $75.78 -24.12; RIG×239 09:30 $5.87 → close $5.54 -78.87; VAL×16 09:30 $87.40 → close $82.52 -78.08; ADPT×51 09:30 $27.09 → close $27.67 +29.58; SWKS×15 09:30 $89.38 → close $85.59 -56.85; SDGR×60 09:30 $23.29 → close $23.93 +38.40; FPS×42 09:30 $33.14 → close $34.84 +71.40; CAI×49 09:30 $28.16 → close $28.21 +2.45 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.71 | ▲ 09:30 equity $11,302.06 vs yday $11,114.52 (+187.54) | 09:30 open · cash $129.71 (unchanged overnight, no fees) · equity $11,302.06 vs prior close $11,114.52 (+187.54) · 8 name(s) re-marked at the open (per-name table). RDNT×18 yday $75.78 → 09:30 $76.44 +11.88; RIG×239 yday $5.54 → 09:30 $5.58 +9.56; VAL×16 yday $82.52 → 09:30 $83.20 +10.88; ADPT×51 yday $27.67 → 09:30 $28.23 +28.56; SWKS×15 yday $85.59 → 09:30 $86.76 +17.55; SDGR×60 yday $23.93 → 09:30 $24.09 +9.60; FPS×42 yday $34.84 → 09:30 $36.76 +80.64; CAI×49 yday $28.21 → 09:30 $28.59 +18.86 | — |
| 2026-09-17 09:30 ET | **SELL** | `RDNT` | 18 | $76.44 | $2.06 | $-16.35 | $1,503.57 | ▼ -16.35 after sell → book $11,299.99; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 239 | $5.58 | $3.13 | $-75.53 | $2,834.05 | ▼ -75.53 after sell → book $11,296.86; vs 09:30 mark -3.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 16 | $83.20 | $2.06 | $-71.30 | $4,163.19 | ▼ -71.30 after sell → book $11,294.80; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ADPT` | 51 | $28.23 | $2.16 | $+53.83 | $5,600.76 | ▲ +53.83 after sell → book $11,292.63; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 15 | $86.76 | $2.06 | $-43.39 | $6,900.10 | ▼ -43.39 after sell → book $11,290.58; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 60 | $24.09 | $2.19 | $+43.64 | $8,343.31 | ▲ +43.64 after sell → book $11,288.39; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `CAI` | 49 | $28.59 | $2.16 | $+17.02 | $9,742.31 | ▲ +17.02 after sell → book $11,286.23; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 5 | $233.85 | $2.00 | — | $8,571.05 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; ret5=+11.7; leftover $1391.76 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 9 | $147.61 | $2.02 | — | $7,240.55 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; ret5=+17.7; leftover $1391.76 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 183 | $7.59 | $2.54 | — | $5,849.04 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $1391.76 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 53 | $25.95 | $2.15 | — | $4,471.54 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,oppset; 🔵; ret5=+9.6; leftover $1391.76 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 8 | $170.85 | $2.01 | — | $3,102.72 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1391.76 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `CIFR` | 77 | $18.04 | $2.22 | — | $1,711.81 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.1; leftover $1391.76 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `BRKR` | 22 | $61.90 | $2.06 | — | $347.95 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; 🔵; ret5=+11.5; leftover $1391.76 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $347.95 | ▲ close $11,452.53 vs 09:30 $11,302.06 (session +181.31) | 16:00 close · cash $347.95 · equity $11,452.53 vs 09:30 $11,302.06 (+150.47; session marks +181.31) · 8 name(s) marked open→close (per-name table). FPS×42 09:30 $36.76 → close $38.06 +54.60; ILMN×5 09:30 $233.85 → close $245.18 +56.65; RVTY×9 09:30 $147.61 → close $146.73 -7.92; PGEN×183 09:30 $7.59 → close $7.87 +51.24; ARQT×53 09:30 $25.95 → close $26.46 +27.03; SMTC×8 09:30 $170.85 → close $178.19 +58.72; CIFR×77 09:30 $18.04 → close $16.94 -84.31; BRKR×22 09:30 $61.90 → close $63.05 +25.30 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $347.95 | ▲ 09:30 equity $11,640.24 vs yday $11,452.53 (+187.71) | 09:30 open · cash $347.95 (unchanged overnight, no fees) · equity $11,640.24 vs prior close $11,452.53 (+187.71) · 8 name(s) re-marked at the open (per-name table). FPS×42 yday $38.06 → 09:30 $39.50 +60.48; ILMN×5 yday $245.18 → 09:30 $249.13 +19.75; RVTY×9 yday $146.73 → 09:30 $146.50 -2.07; PGEN×183 yday $7.87 → 09:30 $7.98 +20.13; ARQT×53 yday $26.46 → 09:30 $26.14 -16.96; SMTC×8 yday $178.19 → 09:30 $182.33 +33.12; CIFR×77 yday $16.94 → 09:30 $17.80 +66.22; BRKR×22 yday $63.05 → 09:30 $63.37 +7.04 | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 42 | $39.50 | $2.14 | $+262.86 | $2,004.81 | ▲ +262.86 after sell → book $11,638.10; vs 09:30 mark -2.14 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 9 | $146.50 | $2.04 | $-14.04 | $3,321.28 | ▼ -14.04 after sell → book $11,636.07; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 183 | $7.98 | $2.58 | $+66.25 | $4,779.03 | ▲ +66.25 after sell → book $11,633.48; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 8 | $182.33 | $2.04 | $+87.79 | $6,235.64 | ▲ +87.79 after sell → book $11,631.45; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `CIFR` | 77 | $17.80 | $2.24 | $-22.56 | $7,603.99 | ▼ -22.56 after sell → book $11,629.20; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BRKR` | 22 | $63.37 | $2.08 | $+28.21 | $8,996.06 | ▲ +28.21 after sell → book $11,627.13; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 51 | $29.32 | $2.14 | — | $7,498.59 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+60.9; leftover $1499.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 379 | $3.95 | $4.89 | — | $5,996.65 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $1499.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 106 | $14.07 | $2.31 | — | $4,502.93 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1499.34 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 101 | $14.79 | $2.29 | — | $3,006.84 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $1499.34 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 494 | $3.04 | $6.37 | — | $1,501.18 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $1499.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 6 | $219.62 | $2.01 | — | $181.45 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $1499.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $181.45 | ▲ close $11,687.81 vs 09:30 $11,640.24 (session +80.70) | 16:00 close · cash $181.45 · equity $11,687.81 vs 09:30 $11,640.24 (+47.57; session marks +80.70) · 8 name(s) marked open→close (per-name table). ILMN×5 09:30 $249.13 → close $239.62 -47.55; ARQT×53 09:30 $26.14 → close $25.38 -40.28; SDGR×51 09:30 $29.32 → close $29.02 -15.30; EYPT×379 09:30 $3.95 → close $3.85 -37.90; BHVN×106 09:30 $14.07 → close $13.62 -47.70; RARE×101 09:30 $14.79 → close $14.51 -28.28; CYPH×494 09:30 $3.04 → close $3.60 +279.11; VICR×6 09:30 $219.62 → close $222.72 +18.60 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-19 | `TRI` | hard_red | hard-red S=-7.20 sit; no new buys |
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
| 2026-08-31 | `HQY` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CRCL` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TRMD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BSBR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SU` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBA` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CMPS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ARBE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPQ` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `INSP` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `GME` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DHT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `AVT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `KHC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `S` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `IOT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAFX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `FRO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `VLO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `HITI` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `BNC` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ILMN` | 5 | 2026-09-17 @ $233.85 | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; ret5=+11.7; leftover $1391.76 |
| `ARQT` | 53 | 2026-09-17 @ $25.95 | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,oppset; 🔵; ret5=+9.6; leftover $1391.76 |
| `SDGR` | 51 | 2026-09-18 @ $29.32 | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,yday_mover,oppset; 🔵; ⚪; ret5=+60.9; leftover $1499.34 |
| `EYPT` | 379 | 2026-09-18 @ $3.95 | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $1499.34 |
| `BHVN` | 106 | 2026-09-18 @ $14.07 | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1499.34 |
| `RARE` | 101 | 2026-09-18 @ $14.79 | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,oppset; 🔵; ⚪; ret5=+0.7; leftover $1499.34 |
| `CYPH` | 494 | 2026-09-18 @ $3.04 | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $1499.34 |
| `VICR` | 6 | 2026-09-18 @ $219.62 | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $1499.34 |
