# Factor mine action — `union_join_vol_green_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-10.40%** ($8,959) · signal-only (no cash/fees) was +8.55%. Starts YES **3/19**. Fills 81 · skips 115 · realized $-1040.21.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `join=good,vol=good,last_green=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,911.71.

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
| 2026-08-17 | `BTBT` | 833 | $1.57 | $1.52 | -41.65 | $1.60 | +66.64 | +24.99 | +16.66 | +83.30 |
| 2026-08-17 | `BETR` | 84 | $13.73 | $13.67 | -5.04 | $13.54 | -10.92 | -15.96 | -94.92 | -105.84 |
| 2026-08-17 | `ANGX` | 290 | $4.37 | $4.60 | +66.70 | $4.71 | +31.90 | +98.60 | +84.10 | +116.00 |
| 2026-08-17 | `HYLN` | 299 | $4.06 | $4.10 | +11.96 | $4.09 | -2.99 | +8.97 | -23.92 | -26.91 |
| 2026-08-17 | `ADUR` | 75 | $16.17 | $15.73 | -33.00 | $15.85 | +9.00 | -24.00 | -57.75 | -48.75 |
| 2026-08-17 | `AIRO` | 112 | $9.57 | $9.57 | +0.00 | $9.41 | -17.92 | -17.92 | -173.60 | -191.52 |
| 2026-08-17 | `NCMI` | 464 | $2.86 | $2.80 | -27.84 | $2.73 | -32.48 | -60.32 | +51.04 | +18.56 |
| 2026-08-17 | `QMLS` | 170 | $7.32 | $7.24 | -13.60 | $7.14 | -17.00 | -30.60 | -8.50 | -25.50 |
| 2026-08-18 | `BTBT` | 833 | $1.60 | $1.54 | -49.98 | $1.45 | -74.97 | -124.95 | +33.32 | -41.65 |
| 2026-08-18 | `BETR` | 84 | $13.54 | $13.21 | -27.72 | $13.05 | -13.44 | -41.16 | -133.56 | -147.00 |
| 2026-08-18 | `ANGX` | 290 | $4.71 | $4.79 | +23.20 | $4.85 | +17.40 | +40.60 | +139.20 | +156.60 |
| 2026-08-18 | `HYLN` | 299 | $4.09 | $3.95 | -41.86 | $3.86 | -26.91 | -68.77 | -68.77 | -95.68 |
| 2026-08-18 | `ADUR` | 75 | $15.85 | $15.41 | -33.00 | $15.63 | +16.50 | -16.50 | -81.75 | -65.25 |
| 2026-08-18 | `AIRO` | 112 | $9.41 | $9.01 | -44.80 | $8.98 | -3.36 | -48.16 | -236.32 | -239.68 |
| 2026-08-18 | `NCMI` | 464 | $2.73 | $2.71 | -9.28 | $2.52 | -88.16 | -97.44 | +9.28 | -78.88 |
| 2026-08-18 | `QMLS` | 170 | $7.14 | $6.85 | -49.30 | $6.74 | -18.70 | -68.00 | -74.80 | -93.50 |
| 2026-08-19 | `BTBT` | 833 | $1.45 | $1.42 | -24.99 | — | +0.00 | -24.99 | -66.64 | — |
| 2026-08-19 | `BETR` | 84 | $13.05 | $13.03 | -1.68 | — | +0.00 | -1.68 | -148.68 | — |
| 2026-08-19 | `ANGX` | 290 | $4.85 | $4.79 | -17.40 | — | +0.00 | -17.40 | +139.20 | — |
| 2026-08-19 | `HYLN` | 299 | $3.86 | $3.87 | +2.99 | — | +0.00 | +2.99 | -92.69 | — |
| 2026-08-19 | `ADUR` | 75 | $15.63 | $15.65 | +1.50 | — | +0.00 | +1.50 | -63.75 | — |
| 2026-08-19 | `AIRO` | 112 | $8.98 | $9.10 | +13.44 | — | +0.00 | +13.44 | -226.24 | — |
| 2026-08-19 | `NCMI` | 464 | $2.52 | $2.56 | +18.56 | — | +0.00 | +18.56 | -60.32 | — |
| 2026-08-19 | `QMLS` | 170 | $6.74 | $6.74 | +0.00 | — | +0.00 | +0.00 | -93.50 | — |
| 2026-08-20 | `AG` | 56 | — | $20.55 | +0.00 | $21.19 | +35.84 | +35.84 | +0.00 | +35.84 |
| 2026-08-20 | `CDE` | 56 | — | $20.65 | +0.00 | $21.11 | +25.76 | +25.76 | +0.00 | +25.76 |
| 2026-08-20 | `HDSN` | 201 | — | $5.77 | +0.00 | $5.57 | -40.20 | -40.20 | +0.00 | -40.20 |
| 2026-08-20 | `IAG` | 59 | — | $19.63 | +0.00 | $20.50 | +51.33 | +51.33 | +0.00 | +51.33 |
| 2026-08-20 | `KGC` | 39 | — | $29.63 | +0.00 | $31.43 | +70.20 | +70.20 | +0.00 | +70.20 |
| 2026-08-20 | `NFGC` | 665 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-20 | `ABUS` | 236 | — | $4.92 | +0.00 | $4.77 | -35.40 | -35.40 | +0.00 | -35.40 |
| 2026-08-21 | `AG` | 56 | $21.19 | $21.90 | +39.76 | $21.09 | -45.36 | -5.60 | +75.60 | +30.24 |
| 2026-08-21 | `CDE` | 56 | $21.11 | $21.75 | +35.84 | $20.97 | -43.68 | -7.84 | +61.60 | +17.92 |
| 2026-08-21 | `HDSN` | 201 | $5.57 | $5.67 | +20.10 | $5.63 | -8.04 | +12.06 | -20.10 | -28.14 |
| 2026-08-21 | `IAG` | 59 | $20.50 | $21.17 | +39.53 | $21.14 | -1.77 | +37.76 | +90.86 | +89.09 |
| 2026-08-21 | `KGC` | 39 | $31.43 | $32.17 | +28.86 | $32.76 | +23.01 | +51.87 | +99.06 | +122.07 |
| 2026-08-21 | `NFGC` | 665 | $1.75 | $1.79 | +26.60 | $1.84 | +33.25 | +59.85 | +26.60 | +59.85 |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | $157.78 | +24.64 | +60.24 | +81.28 | +105.92 |
| 2026-08-21 | `ABUS` | 236 | $4.77 | $5.20 | +101.48 | $5.21 | +2.36 | +103.84 | +66.08 | +68.44 |
| 2026-08-21 | `CYPH` | 3 | — | $1.32 | +0.00 | $1.42 | +0.30 | +0.30 | +0.00 | +0.30 |
| 2026-08-21 | `BTBT` | 2 | — | $1.66 | +0.00 | $1.53 | -0.26 | -0.26 | +0.00 | -0.26 |
| 2026-08-21 | `INDP` | 2 | — | $1.39 | +0.00 | $1.29 | -0.20 | -0.20 | +0.00 | -0.20 |
| 2026-08-24 | `AG` | 56 | $21.09 | $21.30 | +11.76 | $20.83 | -26.32 | -14.56 | +42.00 | +15.68 |
| 2026-08-24 | `CDE` | 56 | $20.97 | $21.26 | +16.24 | $20.88 | -21.28 | -5.04 | +34.16 | +12.88 |
| 2026-08-24 | `HDSN` | 201 | $5.63 | $5.69 | +12.06 | $5.52 | -34.17 | -22.11 | -16.08 | -50.25 |
| 2026-08-24 | `IAG` | 59 | $21.14 | $21.38 | +14.16 | $21.80 | +24.78 | +38.94 | +103.25 | +128.03 |
| 2026-08-24 | `KGC` | 39 | $32.76 | $33.03 | +10.53 | $32.98 | -1.95 | +8.58 | +132.60 | +130.65 |
| 2026-08-24 | `NFGC` | 665 | $1.84 | $1.86 | +13.30 | $1.90 | +26.60 | +39.90 | +73.15 | +99.75 |
| 2026-08-24 | `WPM` | 8 | $157.78 | $159.50 | +13.76 | $160.19 | +5.52 | +19.28 | +119.68 | +125.20 |
| 2026-08-24 | `ABUS` | 236 | $5.21 | $5.18 | -7.08 | $5.20 | +4.72 | -2.36 | +61.36 | +66.08 |
| 2026-08-24 | `CYPH` | 3 | $1.42 | $1.83 | +1.23 | $1.68 | -0.45 | +0.78 | +1.53 | +1.08 |
| 2026-08-24 | `BTBT` | 2 | $1.53 | $1.55 | +0.04 | $1.51 | -0.08 | -0.04 | -0.22 | -0.30 |
| 2026-08-24 | `INDP` | 2 | $1.29 | $1.24 | -0.10 | $1.16 | -0.16 | -0.26 | -0.30 | -0.46 |
| 2026-08-25 | `AG` | 56 | $20.83 | $20.32 | -28.56 | — | +0.00 | -28.56 | -12.88 | — |
| 2026-08-25 | `CDE` | 56 | $20.88 | $20.47 | -22.96 | — | +0.00 | -22.96 | -10.08 | — |
| 2026-08-25 | `HDSN` | 201 | $5.52 | $5.53 | +2.01 | — | +0.00 | +2.01 | -48.24 | — |
| 2026-08-25 | `IAG` | 59 | $21.80 | $21.21 | -34.81 | — | +0.00 | -34.81 | +93.22 | — |
| 2026-08-25 | `KGC` | 39 | $32.98 | $32.32 | -25.74 | — | +0.00 | -25.74 | +104.91 | — |
| 2026-08-25 | `NFGC` | 665 | $1.90 | $1.90 | +0.00 | — | +0.00 | +0.00 | +99.75 | — |
| 2026-08-25 | `WPM` | 8 | $160.19 | $156.51 | -29.44 | $163.72 | +57.68 | +28.24 | +95.76 | +153.44 |
| 2026-08-25 | `ABUS` | 236 | $5.20 | $5.25 | +11.80 | — | +0.00 | +11.80 | +77.88 | — |
| 2026-08-25 | `CYPH` | 3 | $1.68 | $1.56 | -0.36 | $1.64 | +0.24 | -0.12 | +0.72 | +0.96 |
| 2026-08-25 | `BTBT` | 2 | $1.51 | $1.51 | +0.00 | $1.58 | +0.14 | +0.14 | -0.30 | -0.16 |
| 2026-08-25 | `INDP` | 2 | $1.16 | $1.15 | -0.02 | $1.09 | -0.11 | -0.13 | -0.48 | -0.59 |
| 2026-08-25 | `BMEA` | 737 | — | $1.63 | +0.00 | $1.73 | +73.70 | +73.70 | +0.00 | +73.70 |
| 2026-08-25 | `GORO` | 338 | — | $3.55 | +0.00 | $3.87 | +108.16 | +108.16 | +0.00 | +108.16 |
| 2026-08-25 | `ZURA` | 188 | — | $6.37 | +0.00 | $6.32 | -9.40 | -9.40 | +0.00 | -9.40 |
| 2026-08-25 | `EZPW` | 34 | — | $35.05 | +0.00 | $35.23 | +6.12 | +6.12 | +0.00 | +6.12 |
| 2026-08-25 | `ETON` | 18 | — | $64.55 | +0.00 | $63.05 | -27.00 | -27.00 | +0.00 | -27.00 |
| 2026-08-25 | `SUZ` | 133 | — | $8.98 | +0.00 | $9.03 | +6.65 | +6.65 | +0.00 | +6.65 |
| 2026-08-25 | `IAUX` | 632 | — | $1.90 | +0.00 | $1.92 | +12.64 | +12.64 | +0.00 | +12.64 |
| 2026-08-26 | `WPM` | 8 | $163.72 | $160.93 | -22.32 | — | +0.00 | -22.32 | +131.12 | — |
| 2026-08-26 | `CYPH` | 3 | $1.64 | $1.60 | -0.12 | — | +0.00 | -0.12 | +0.84 | — |
| 2026-08-26 | `BTBT` | 2 | $1.58 | $1.53 | -0.10 | — | +0.00 | -0.10 | -0.26 | — |
| 2026-08-26 | `INDP` | 2 | $1.09 | $1.09 | -0.01 | — | +0.00 | -0.01 | -0.60 | — |
| 2026-08-26 | `BMEA` | 737 | $1.73 | $1.75 | +18.42 | $1.71 | -33.16 | -14.74 | +92.12 | +58.96 |
| 2026-08-26 | `GORO` | 338 | $3.87 | $3.77 | -33.80 | $3.56 | -70.98 | -104.78 | +74.36 | +3.38 |
| 2026-08-26 | `ZURA` | 188 | $6.32 | $6.13 | -35.72 | $5.99 | -26.32 | -62.04 | -45.12 | -71.44 |
| 2026-08-26 | `EZPW` | 34 | $35.23 | $35.70 | +15.98 | $33.90 | -61.20 | -45.22 | +22.10 | -39.10 |
| 2026-08-26 | `ETON` | 18 | $63.05 | $63.60 | +9.90 | $62.62 | -17.64 | -7.74 | -17.10 | -34.74 |
| 2026-08-26 | `SUZ` | 133 | $9.03 | $9.03 | +0.00 | $8.94 | -11.97 | -11.97 | +6.65 | -5.32 |
| 2026-08-26 | `IAUX` | 632 | $1.92 | $1.87 | -31.60 | $1.88 | +6.32 | -25.28 | -18.96 | -12.64 |
| 2026-08-26 | `USDE` | 227 | — | $5.81 | +0.00 | $5.98 | +38.59 | +38.59 | +0.00 | +38.59 |
| 2026-08-27 | `BMEA` | 737 | $1.71 | $1.74 | +22.11 | $1.68 | -44.22 | -22.11 | +81.07 | +36.85 |
| 2026-08-27 | `GORO` | 338 | $3.56 | $3.59 | +10.14 | $3.79 | +67.60 | +77.74 | +13.52 | +81.12 |
| 2026-08-27 | `ZURA` | 188 | $5.99 | $6.02 | +5.64 | $5.85 | -31.96 | -26.32 | -65.80 | -97.76 |
| 2026-08-27 | `EZPW` | 34 | $33.90 | $33.50 | -13.60 | $34.41 | +30.94 | +17.34 | -52.70 | -21.76 |
| 2026-08-27 | `ETON` | 18 | $62.62 | $62.50 | -2.16 | $62.67 | +3.06 | +0.90 | -36.90 | -33.84 |
| 2026-08-27 | `SUZ` | 133 | $8.94 | $8.88 | -7.98 | $8.96 | +10.64 | +2.66 | -13.30 | -2.66 |
| 2026-08-27 | `IAUX` | 632 | $1.88 | $1.87 | -6.32 | $1.92 | +31.60 | +25.28 | -18.96 | +12.64 |
| 2026-08-27 | `USDE` | 227 | $5.98 | $6.50 | +118.04 | $8.07 | +356.39 | +474.43 | +156.63 | +513.02 |
| 2026-08-28 | `BMEA` | 737 | $1.68 | $1.69 | +7.37 | — | +0.00 | +7.37 | +44.22 | — |
| 2026-08-28 | `GORO` | 338 | $3.79 | $3.80 | +3.38 | — | +0.00 | +3.38 | +84.50 | — |
| 2026-08-28 | `ZURA` | 188 | $5.85 | $5.88 | +5.64 | — | +0.00 | +5.64 | -92.12 | — |
| 2026-08-28 | `EZPW` | 34 | $34.41 | $34.50 | +3.06 | — | +0.00 | +3.06 | -18.70 | — |
| 2026-08-28 | `ETON` | 18 | $62.67 | $61.98 | -12.42 | — | +0.00 | -12.42 | -46.26 | — |
| 2026-08-28 | `SUZ` | 133 | $8.96 | $9.05 | +11.97 | — | +0.00 | +11.97 | +9.31 | — |
| 2026-08-28 | `IAUX` | 632 | $1.92 | $1.93 | +6.32 | — | +0.00 | +6.32 | +18.96 | — |
| 2026-08-28 | `USDE` | 227 | $8.07 | $7.24 | -188.41 | $6.76 | -108.96 | -297.37 | +324.61 | +215.65 |
| 2026-08-28 | `ANF` | 14 | — | $146.07 | +0.00 | $148.42 | +32.90 | +32.90 | +0.00 | +32.90 |
| 2026-08-28 | `NCNO` | 89 | — | $23.30 | +0.00 | $22.99 | -27.59 | -27.59 | +0.00 | -27.59 |
| 2026-08-28 | `TH` | 109 | — | $19.00 | +0.00 | $18.55 | -49.05 | -49.05 | +0.00 | -49.05 |
| 2026-08-28 | `GAP` | 84 | — | $24.69 | +0.00 | $23.48 | -101.64 | -101.64 | +0.00 | -101.64 |
| 2026-08-31 | `USDE` | 227 | $6.76 | $6.76 | +0.00 | — | +0.00 | +0.00 | +215.65 | — |
| 2026-08-31 | `ANF` | 14 | $148.42 | $148.03 | -5.46 | $143.08 | -69.30 | -74.76 | +27.44 | -41.86 |
| 2026-08-31 | `NCNO` | 89 | $22.99 | $22.66 | -29.37 | $22.60 | -5.34 | -34.71 | -56.96 | -62.30 |
| 2026-08-31 | `TH` | 109 | $18.55 | $18.12 | -46.33 | $18.52 | +43.05 | -3.28 | -95.38 | -52.32 |
| 2026-08-31 | `GAP` | 84 | $23.48 | $22.98 | -42.00 | $22.31 | -56.28 | -98.28 | -143.64 | -199.92 |
| 2026-09-01 | `ANF` | 14 | $143.08 | $142.00 | -15.12 | $140.68 | -18.48 | -33.60 | -56.98 | -75.46 |
| 2026-09-01 | `NCNO` | 89 | $22.60 | $22.15 | -40.05 | $22.30 | +13.35 | -26.70 | -102.35 | -89.00 |
| 2026-09-01 | `TH` | 109 | $18.52 | $18.45 | -7.63 | $18.07 | -41.42 | -49.05 | -59.95 | -101.37 |
| 2026-09-01 | `GAP` | 84 | $22.31 | $22.05 | -21.84 | $22.00 | -4.20 | -26.04 | -221.76 | -225.96 |
| 2026-09-02 | `ANF` | 14 | $140.68 | $139.65 | -14.42 | — | +0.00 | -14.42 | -89.88 | — |
| 2026-09-02 | `NCNO` | 89 | $22.30 | $22.20 | -8.90 | — | +0.00 | -8.90 | -97.90 | — |
| 2026-09-02 | `TH` | 109 | $18.07 | $17.98 | -9.81 | — | +0.00 | -9.81 | -111.18 | — |
| 2026-09-02 | `GAP` | 84 | $22.00 | $21.97 | -2.52 | — | +0.00 | -2.52 | -228.48 | — |
| 2026-09-03 | `RVTY` | 8 | — | $132.45 | +0.00 | $130.63 | -14.56 | -14.56 | +0.00 | -14.56 |
| 2026-09-03 | `ARCT` | 69 | — | $16.77 | +0.00 | $15.56 | -83.49 | -83.49 | +0.00 | -83.49 |
| 2026-09-03 | `CRDL` | 533 | — | $2.18 | +0.00 | $2.16 | -10.66 | -10.66 | +0.00 | -10.66 |
| 2026-09-03 | `MMED` | 48 | — | $23.88 | +0.00 | $23.84 | -1.92 | -1.92 | +0.00 | -1.92 |
| 2026-09-03 | `NVAX` | 111 | — | $10.42 | +0.00 | $10.34 | -8.88 | -8.88 | +0.00 | -8.88 |
| 2026-09-03 | `BMEA` | 602 | — | $1.93 | +0.00 | $1.91 | -12.04 | -12.04 | +0.00 | -12.04 |
| 2026-09-03 | `DUOL` | 7 | — | $161.54 | +0.00 | $158.82 | -19.04 | -19.04 | +0.00 | -19.04 |
| 2026-09-03 | `ALMS` | 112 | — | $10.38 | +0.00 | $11.36 | +110.32 | +110.32 | +0.00 | +110.32 |
| 2026-09-04 | `RVTY` | 8 | $130.63 | $130.03 | -4.80 | $130.22 | +1.52 | -3.28 | -19.36 | -17.84 |
| 2026-09-04 | `ARCT` | 69 | $15.56 | $15.61 | +3.45 | $15.82 | +14.49 | +17.94 | -80.04 | -65.55 |
| 2026-09-04 | `CRDL` | 533 | $2.16 | $2.16 | +0.00 | $2.20 | +21.32 | +21.32 | -10.66 | +10.66 |
| 2026-09-04 | `MMED` | 48 | $23.84 | $23.84 | +0.00 | $23.29 | -26.40 | -26.40 | -1.92 | -28.32 |
| 2026-09-04 | `NVAX` | 111 | $10.34 | $10.50 | +17.76 | $10.22 | -31.08 | -13.32 | +8.88 | -22.20 |
| 2026-09-04 | `BMEA` | 602 | $1.91 | $1.90 | -6.02 | $2.03 | +78.26 | +72.24 | -18.06 | +60.20 |
| 2026-09-04 | `DUOL` | 7 | $158.82 | $157.46 | -9.52 | $154.46 | -21.00 | -30.52 | -28.56 | -49.56 |
| 2026-09-04 | `ALMS` | 112 | $11.36 | $11.23 | -14.56 | $11.10 | -14.56 | -29.12 | +95.76 | +81.20 |
| 2026-09-04 | `BRR` | 7 | — | $2.51 | +0.00 | $2.66 | +1.05 | +1.05 | +0.00 | +1.05 |
| 2026-09-04 | `DFDV` | 3 | — | $5.79 | +0.00 | $5.87 | +0.24 | +0.24 | +0.00 | +0.24 |
| 2026-09-04 | `AHCO` | 2 | — | $6.32 | +0.00 | $6.49 | +0.34 | +0.34 | +0.00 | +0.34 |
| 2026-09-08 | `RVTY` | 8 | $130.22 | $128.50 | -13.76 | $127.08 | -11.36 | -25.12 | -31.60 | -42.96 |
| 2026-09-08 | `ARCT` | 69 | $15.82 | $15.47 | -24.15 | $15.63 | +11.04 | -13.11 | -89.70 | -78.66 |
| 2026-09-08 | `CRDL` | 533 | $2.20 | $2.20 | +0.00 | $2.22 | +10.66 | +10.66 | +10.66 | +21.32 |
| 2026-09-08 | `MMED` | 48 | $23.29 | $23.16 | -6.24 | $23.32 | +7.68 | +1.44 | -34.56 | -26.88 |
| 2026-09-08 | `NVAX` | 111 | $10.22 | $10.10 | -13.32 | $10.19 | +9.99 | -3.33 | -35.52 | -25.53 |
| 2026-09-08 | `BMEA` | 602 | $2.03 | $2.00 | -18.06 | $1.93 | -42.14 | -60.20 | +42.14 | +0.00 |
| 2026-09-08 | `DUOL` | 7 | $154.46 | $152.53 | -13.51 | $146.39 | -42.98 | -56.49 | -63.07 | -106.05 |
| 2026-09-08 | `ALMS` | 112 | $11.10 | $11.05 | -5.60 | $10.58 | -52.64 | -58.24 | +75.60 | +22.96 |
| 2026-09-08 | `BRR` | 7 | $2.66 | $2.66 | +0.00 | $2.73 | +0.49 | +0.49 | +1.05 | +1.54 |
| 2026-09-08 | `DFDV` | 3 | $5.87 | $5.81 | -0.18 | $5.99 | +0.54 | +0.36 | +0.06 | +0.60 |
| 2026-09-08 | `AHCO` | 2 | $6.49 | $6.48 | -0.02 | $6.19 | -0.58 | -0.60 | +0.32 | -0.26 |
| 2026-09-09 | `RVTY` | 8 | $127.08 | $125.77 | -10.48 | — | +0.00 | -10.48 | -53.44 | — |
| 2026-09-09 | `ARCT` | 69 | $15.63 | $15.46 | -11.73 | — | +0.00 | -11.73 | -90.39 | — |
| 2026-09-09 | `CRDL` | 533 | $2.22 | $2.22 | +0.00 | — | +0.00 | +0.00 | +21.32 | — |
| 2026-09-09 | `MMED` | 48 | $23.32 | $23.22 | -4.80 | — | +0.00 | -4.80 | -31.68 | — |
| 2026-09-09 | `NVAX` | 111 | $10.19 | $10.02 | -18.87 | — | +0.00 | -18.87 | -44.40 | — |
| 2026-09-09 | `BMEA` | 602 | $1.93 | $1.94 | +6.02 | — | +0.00 | +6.02 | +6.02 | — |
| 2026-09-09 | `DUOL` | 7 | $146.39 | $145.58 | -5.67 | — | +0.00 | -5.67 | -111.72 | — |
| 2026-09-09 | `ALMS` | 112 | $10.58 | $10.49 | -10.08 | — | +0.00 | -10.08 | +12.88 | — |
| 2026-09-09 | `BRR` | 7 | $2.73 | $2.75 | +0.14 | $2.86 | +0.77 | +0.91 | +1.68 | +2.45 |
| 2026-09-09 | `DFDV` | 3 | $5.99 | $6.02 | +0.09 | $5.44 | -1.74 | -1.65 | +0.69 | -1.05 |
| 2026-09-09 | `AHCO` | 2 | $6.19 | $5.92 | -0.54 | $5.70 | -0.44 | -0.98 | -0.80 | -1.24 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -164.42 | BTBT, BETR, ANGX, HYLN, ADUR, AIRO, NCMI, QMLS | — | $3.57 | $9,801.97 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, AIRO×112, NCMI×464, QMLS×170 |
| 2026-08-17 | +2.25 | $3.57 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, AIRO×112, NCMI×464, QMLS×170 | $9,759.50 | -42.47 | +26.23 | — | — | $3.57 | $9,785.73 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, AIRO×112, NCMI×464, QMLS×170 |
| 2026-08-18 | -6.20 | $3.57 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, AIRO×112, NCMI×464, QMLS×170 | $9,552.99 | -232.74 | -191.64 | — | — | $3.57 | $9,361.35 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, AIRO×112, NCMI×464, QMLS×170 |
| 2026-08-19 | -7.20 | $3.57 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, AIRO×112, NCMI×464, QMLS×170 | $9,353.77 | -7.58 | +0.00 | — | BTBT, BETR, ANGX, HYLN, ADUR, AIRO, NCMI, QMLS | $9,319.69 | $9,319.69 | — |
| 2026-08-20 | +1.12 | $9,319.69 | — | $9,319.69 | -0.00 | +153.21 | AG, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS | — | $32.96 | $9,448.07 | AG×56, CDE×56, HDSN×201, IAG×59, KGC×39, NFGC×665, WPM×8, ABUS×236 |
| 2026-08-21 | +3.25 | $32.96 | AG×56, CDE×56, HDSN×201, IAG×59, KGC×39, NFGC×665, WPM×8, ABUS×236 | $9,775.84 | +327.77 | -15.75 | CYPH, BTBT, INDP | — | $22.78 | $9,759.97 | AG×56, CDE×56, HDSN×201, IAG×59, KGC×39, NFGC×665, WPM×8, ABUS×236, CYPH×3, BTBT×2, INDP×2 |
| 2026-08-24 | -5.17 | $22.78 | AG×56, CDE×56, HDSN×201, IAG×59, KGC×39, NFGC×665, WPM×8, ABUS×236, CYPH×3, BTBT×2, INDP×2 | $9,845.87 | +85.90 | -22.79 | — | — | $22.78 | $9,823.08 | AG×56, CDE×56, HDSN×201, IAG×59, KGC×39, NFGC×665, WPM×8, ABUS×236, CYPH×3, BTBT×2, INDP×2 |
| 2026-08-25 | +1.80 | $22.78 | AG×56, CDE×56, HDSN×201, IAG×59, KGC×39, NFGC×665, WPM×8, ABUS×236, CYPH×3, BTBT×2, INDP×2 | $9,695.00 | -128.08 | +228.82 | BMEA, GORO, ZURA, EZPW, ETON, SUZ, IAUX | AG, CDE, HDSN, IAG, KGC, NFGC, ABUS | $31.21 | $9,869.62 | WPM×8, CYPH×3, BTBT×2, INDP×2, BMEA×737, GORO×338, ZURA×188, EZPW×34, ETON×18, SUZ×133, IAUX×632 |
| 2026-08-26 | +2.02 | $31.21 | WPM×8, CYPH×3, BTBT×2, INDP×2, BMEA×737, GORO×338, ZURA×188, EZPW×34, ETON×18, SUZ×133, IAUX×632 | $9,790.25 | -79.37 | -176.36 | USDE | WPM, CYPH, BTBT, INDP | $4.67 | $9,608.74 | BMEA×737, GORO×338, ZURA×188, EZPW×34, ETON×18, SUZ×133, IAUX×632, USDE×227 |
| 2026-08-27 | — | $4.67 | BMEA×737, GORO×338, ZURA×188, EZPW×34, ETON×18, SUZ×133, IAUX×632, USDE×227 | $9,734.61 | +125.87 | +424.05 | — | — | $4.67 | $10,158.66 | BMEA×737, GORO×338, ZURA×188, EZPW×34, ETON×18, SUZ×133, IAUX×632, USDE×227 |
| 2026-08-28 | +0.75 | $4.67 | BMEA×737, GORO×338, ZURA×188, EZPW×34, ETON×18, SUZ×133, IAUX×632, USDE×227 | $9,995.57 | -163.09 | -254.34 | ANF, NCNO, TH, GAP | BMEA, GORO, ZURA, EZPW, ETON, SUZ, IAUX | $48.08 | $9,700.86 | USDE×227, ANF×14, NCNO×89, TH×109, GAP×84 |
| 2026-08-31 | -5.85 | $48.08 | USDE×227, ANF×14, NCNO×89, TH×109, GAP×84 | $9,577.70 | -123.16 | -87.87 | — | USDE | $1,579.62 | $9,486.86 | ANF×14, NCNO×89, TH×109, GAP×84 |
| 2026-09-01 | -6.30 | $1,579.62 | ANF×14, NCNO×89, TH×109, GAP×84 | $9,402.22 | -84.64 | -50.75 | — | — | $1,579.62 | $9,351.47 | ANF×14, NCNO×89, TH×109, GAP×84 |
| 2026-09-02 | -3.83 | $1,579.62 | ANF×14, NCNO×89, TH×109, GAP×84 | $9,315.82 | -35.65 | +0.00 | — | ANF, NCNO, TH, GAP | $9,306.85 | $9,306.85 | — |
| 2026-09-03 | -0.90 | $9,306.85 | — | $9,306.85 | +0.00 | -40.27 | RVTY, ARCT, CRDL, MMED, NVAX, BMEA, DUOL, ALMS | — | $143.04 | $9,238.94 | RVTY×8, ARCT×69, CRDL×533, MMED×48, NVAX×111, BMEA×602, DUOL×7, ALMS×112 |
| 2026-09-04 | +2.25 | $143.04 | RVTY×8, ARCT×69, CRDL×533, MMED×48, NVAX×111, BMEA×602, DUOL×7, ALMS×112 | $9,225.25 | -13.69 | +24.18 | BRR, DFDV, AHCO | — | $94.95 | $9,248.92 | RVTY×8, ARCT×69, CRDL×533, MMED×48, NVAX×111, BMEA×602, DUOL×7, ALMS×112, BRR×7, DFDV×3, AHCO×2 |
| 2026-09-08 | -11.47 | $94.95 | RVTY×8, ARCT×69, CRDL×533, MMED×48, NVAX×111, BMEA×602, DUOL×7, ALMS×112, BRR×7, DFDV×3, AHCO×2 | $9,154.08 | -94.84 | -109.30 | — | — | $94.95 | $9,044.78 | RVTY×8, ARCT×69, CRDL×533, MMED×48, NVAX×111, BMEA×602, DUOL×7, ALMS×112, BRR×7, DFDV×3, AHCO×2 |
| 2026-09-09 | -13.95 | $94.95 | RVTY×8, ARCT×69, CRDL×533, MMED×48, NVAX×111, BMEA×602, DUOL×7, ALMS×112, BRR×7, DFDV×3, AHCO×2 | $8,988.86 | -55.92 | -1.41 | — | RVTY, ARCT, CRDL, MMED, NVAX, BMEA, DUOL, ALMS | $8,911.71 | $8,959.45 | BRR×7, DFDV×3, AHCO×2 |

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
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 112 | $11.12 | $2.33 | — | $2,499.51 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 464 | $2.69 | $5.99 | — | $1,245.37 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 170 | $7.29 | $2.50 | — | $3.57 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.57 | ▼ close $9,801.97 vs 09:30 $10,000.00 (session -164.42) | 16:00 close · cash $3.57 · equity $9,801.97 vs 09:30 $10,000.00 (-198.03; session marks -164.42) · 8 name(s) marked open→close (per-name table). BTBT×833 09:30 $1.50 → close $1.57 +58.31; BETR×84 09:30 $14.80 → close $13.73 -89.88; ANGX×290 09:30 $4.31 → close $4.37 +17.40; HYLN×299 09:30 $4.18 → close $4.06 -35.88; ADUR×75 09:30 $16.50 → close $16.17 -24.75; AIRO×112 09:30 $11.12 → close $9.57 -173.60; NCMI×464 09:30 $2.69 → close $2.86 +78.88; QMLS×170 09:30 $7.29 → close $7.32 +5.10 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.57 | ▼ 09:30 equity $9,759.50 vs yday $9,801.97 (-42.47) | 09:30 open · cash $3.57 (unchanged overnight, no fees) · equity $9,759.50 vs prior close $9,801.97 (-42.47) · 8 name(s) re-marked at the open (per-name table). BTBT×833 yday $1.57 → 09:30 $1.52 -41.65; BETR×84 yday $13.73 → 09:30 $13.67 -5.04; ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; HYLN×299 yday $4.06 → 09:30 $4.10 +11.96; ADUR×75 yday $16.17 → 09:30 $15.73 -33.00; AIRO×112 yday $9.57 → 09:30 $9.57 +0.00; NCMI×464 yday $2.86 → 09:30 $2.80 -27.84; QMLS×170 yday $7.32 → 09:30 $7.24 -13.60 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.57 | ▲ close $9,785.73 vs 09:30 $9,759.50 (session +26.23) | 16:00 close · cash $3.57 · equity $9,785.73 vs 09:30 $9,759.50 (+26.23; session marks +26.23) · 8 name(s) marked open→close (per-name table). BTBT×833 09:30 $1.52 → close $1.60 +66.64; BETR×84 09:30 $13.67 → close $13.54 -10.92; ANGX×290 09:30 $4.60 → close $4.71 +31.90; HYLN×299 09:30 $4.10 → close $4.09 -2.99; ADUR×75 09:30 $15.73 → close $15.85 +9.00; AIRO×112 09:30 $9.57 → close $9.41 -17.92; NCMI×464 09:30 $2.80 → close $2.73 -32.48; QMLS×170 09:30 $7.24 → close $7.14 -17.00 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.57 | ▼ 09:30 equity $9,552.99 vs yday $9,785.73 (-232.74) | 09:30 open · cash $3.57 (unchanged overnight, no fees) · equity $9,552.99 vs prior close $9,785.73 (-232.74) · 8 name(s) re-marked at the open (per-name table). BTBT×833 yday $1.60 → 09:30 $1.54 -49.98; BETR×84 yday $13.54 → 09:30 $13.21 -27.72; ANGX×290 yday $4.71 → 09:30 $4.79 +23.20; HYLN×299 yday $4.09 → 09:30 $3.95 -41.86; ADUR×75 yday $15.85 → 09:30 $15.41 -33.00; AIRO×112 yday $9.41 → 09:30 $9.01 -44.80; NCMI×464 yday $2.73 → 09:30 $2.71 -9.28; QMLS×170 yday $7.14 → 09:30 $6.85 -49.30 | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.57 | ▼ close $9,361.35 vs 09:30 $9,552.99 (session -191.64) | 16:00 close · cash $3.57 · equity $9,361.35 vs 09:30 $9,552.99 (-191.64; session marks -191.64) · 8 name(s) marked open→close (per-name table). BTBT×833 09:30 $1.54 → close $1.45 -74.97; BETR×84 09:30 $13.21 → close $13.05 -13.44; ANGX×290 09:30 $4.79 → close $4.85 +17.40; HYLN×299 09:30 $3.95 → close $3.86 -26.91; ADUR×75 09:30 $15.41 → close $15.63 +16.50; AIRO×112 09:30 $9.01 → close $8.98 -3.36; NCMI×464 09:30 $2.71 → close $2.52 -88.16; QMLS×170 09:30 $6.85 → close $6.74 -18.70 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.57 | ▼ 09:30 equity $9,353.77 vs yday $9,361.35 (-7.58) | 09:30 open · cash $3.57 (unchanged overnight, no fees) · equity $9,353.77 vs prior close $9,361.35 (-7.58) · 8 name(s) re-marked at the open (per-name table). BTBT×833 yday $1.45 → 09:30 $1.42 -24.99; BETR×84 yday $13.05 → 09:30 $13.03 -1.68; ANGX×290 yday $4.85 → 09:30 $4.79 -17.40; HYLN×299 yday $3.86 → 09:30 $3.87 +2.99; ADUR×75 yday $15.63 → 09:30 $15.65 +1.50; AIRO×112 yday $8.98 → 09:30 $9.10 +13.44; NCMI×464 yday $2.52 → 09:30 $2.56 +18.56; QMLS×170 yday $6.74 → 09:30 $6.74 +0.00 | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 833 | $1.42 | $10.89 | $-88.28 | $1,175.53 | ▼ -88.28 after sell → book $9,342.87; vs 09:30 mark -10.90 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BETR` | 84 | $13.03 | $2.27 | $-153.19 | $2,267.79 | ▼ -153.19 after sell → book $9,340.61; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 290 | $4.79 | $3.80 | $+131.66 | $3,653.09 | ▲ +131.66 after sell → book $9,336.81; vs 09:30 mark -3.80 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🟢 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `HYLN` | 299 | $3.87 | $3.92 | $-100.46 | $4,806.30 | ▼ -100.46 after sell → book $9,332.89; vs 09:30 mark -3.92 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ADUR` | 75 | $15.65 | $2.24 | $-68.20 | $5,977.81 | ▼ -68.20 after sell → book $9,330.65; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `AIRO` | 112 | $9.10 | $2.35 | $-230.92 | $6,994.66 | ▼ -230.92 after sell → book $9,328.30; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `NCMI` | 464 | $2.56 | $6.07 | $-72.38 | $8,176.43 | ▼ -72.38 after sell → book $9,322.23; vs 09:30 mark -6.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `QMLS` | 170 | $6.74 | $2.54 | $-98.54 | $9,319.69 | ▼ -98.54 after sell → book $9,319.69; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,319.69 | ▲ close $9,319.69 vs 09:30 $9,353.77 (session +0.00) | 16:00 close · cash $9,319.69 · no lots left · equity $9,319.69. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,319.69 | ▲ 09:30 equity $9,319.69 vs yday $9,319.69 (-0.00) | 09:30 open · cash $9,319.69 · no holdings · equity $9,319.69 vs prior close $9,319.69 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 56 | $20.55 | $2.16 | — | $8,166.73 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1164.96 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 56 | $20.65 | $2.16 | — | $7,008.17 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1164.96 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 201 | $5.77 | $2.60 | — | $5,845.80 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1164.96 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 59 | $19.63 | $2.17 | — | $4,685.47 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1164.96 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 39 | $29.63 | $2.11 | — | $3,527.79 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1164.96 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 665 | $1.75 | $8.58 | — | $2,355.46 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1164.96 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $1,197.13 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1164.96 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 236 | $4.92 | $3.04 | — | $32.96 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1164.96 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.96 | ▲ close $9,448.07 vs 09:30 $9,319.69 (session +153.21) | 16:00 close · cash $32.96 · equity $9,448.07 vs 09:30 $9,319.69 (+128.38; session marks +153.21) · 8 name(s) marked open→close (per-name table). AG×56 09:30 $20.55 → close $21.19 +35.84; CDE×56 09:30 $20.65 → close $21.11 +25.76; HDSN×201 09:30 $5.77 → close $5.57 -40.20; IAG×59 09:30 $19.63 → close $20.50 +51.33; KGC×39 09:30 $29.63 → close $31.43 +70.20; NFGC×665 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68; ABUS×236 09:30 $4.92 → close $4.77 -35.40 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.96 | ▲ 09:30 equity $9,775.84 vs yday $9,448.07 (+327.77) | 09:30 open · cash $32.96 (unchanged overnight, no fees) · equity $9,775.84 vs prior close $9,448.07 (+327.77) · 8 name(s) re-marked at the open (per-name table). AG×56 yday $21.19 → 09:30 $21.90 +39.76; CDE×56 yday $21.11 → 09:30 $21.75 +35.84; HDSN×201 yday $5.57 → 09:30 $5.67 +20.10; IAG×59 yday $20.50 → 09:30 $21.17 +39.53; KGC×39 yday $31.43 → 09:30 $32.17 +28.86; NFGC×665 yday $1.75 → 09:30 $1.79 +26.60; WPM×8 yday $150.25 → 09:30 $154.70 +35.60; ABUS×236 yday $4.77 → 09:30 $5.20 +101.48 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 3 | $1.32 | $0.05 | — | $28.95 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $4.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 2 | $1.66 | $0.04 | — | $25.60 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $4.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `INDP` | 2 | $1.39 | $0.03 | — | $22.78 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+30.2; leftover $4.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22.78 | ▼ close $9,759.97 vs 09:30 $9,775.84 (session -15.75) | 16:00 close · cash $22.78 · equity $9,759.97 vs 09:30 $9,775.84 (-15.87; session marks -15.75) · 11 name(s) marked open→close (per-name table). AG×56 09:30 $21.90 → close $21.09 -45.36; CDE×56 09:30 $21.75 → close $20.97 -43.68; HDSN×201 09:30 $5.67 → close $5.63 -8.04; IAG×59 09:30 $21.17 → close $21.14 -1.77; KGC×39 09:30 $32.17 → close $32.76 +23.01; NFGC×665 09:30 $1.79 → close $1.84 +33.25; WPM×8 09:30 $154.70 → close $157.78 +24.64; ABUS×236 09:30 $5.20 → close $5.21 +2.36; CYPH×3 09:30 $1.32 → close $1.42 +0.30; BTBT×2 09:30 $1.66 → close $1.53 -0.26; INDP×2 09:30 $1.39 → close $1.29 -0.20 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.78 | ▲ 09:30 equity $9,845.87 vs yday $9,759.97 (+85.90) | 09:30 open · cash $22.78 (unchanged overnight, no fees) · equity $9,845.87 vs prior close $9,759.97 (+85.90) · 11 name(s) re-marked at the open (per-name table). AG×56 yday $21.09 → 09:30 $21.30 +11.76; CDE×56 yday $20.97 → 09:30 $21.26 +16.24; HDSN×201 yday $5.63 → 09:30 $5.69 +12.06; IAG×59 yday $21.14 → 09:30 $21.38 +14.16; KGC×39 yday $32.76 → 09:30 $33.03 +10.53; NFGC×665 yday $1.84 → 09:30 $1.86 +13.30; WPM×8 yday $157.78 → 09:30 $159.50 +13.76; ABUS×236 yday $5.21 → 09:30 $5.18 -7.08; CYPH×3 yday $1.42 → 09:30 $1.83 +1.23; BTBT×2 yday $1.53 → 09:30 $1.55 +0.04; INDP×2 yday $1.29 → 09:30 $1.24 -0.10 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22.78 | ▼ close $9,823.08 vs 09:30 $9,845.87 (session -22.79) | 16:00 close · cash $22.78 · equity $9,823.08 vs 09:30 $9,845.87 (-22.79; session marks -22.79) · 11 name(s) marked open→close (per-name table). AG×56 09:30 $21.30 → close $20.83 -26.32; CDE×56 09:30 $21.26 → close $20.88 -21.28; HDSN×201 09:30 $5.69 → close $5.52 -34.17; IAG×59 09:30 $21.38 → close $21.80 +24.78; KGC×39 09:30 $33.03 → close $32.98 -1.95; NFGC×665 09:30 $1.86 → close $1.90 +26.60; WPM×8 09:30 $159.50 → close $160.19 +5.52; ABUS×236 09:30 $5.18 → close $5.20 +4.72; CYPH×3 09:30 $1.83 → close $1.68 -0.45; BTBT×2 09:30 $1.55 → close $1.51 -0.08; INDP×2 09:30 $1.24 → close $1.16 -0.16 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.78 | ▼ 09:30 equity $9,695.00 vs yday $9,823.08 (-128.08) | 09:30 open · cash $22.78 (unchanged overnight, no fees) · equity $9,695.00 vs prior close $9,823.08 (-128.08) · 11 name(s) re-marked at the open (per-name table). AG×56 yday $20.83 → 09:30 $20.32 -28.56; CDE×56 yday $20.88 → 09:30 $20.47 -22.96; HDSN×201 yday $5.52 → 09:30 $5.53 +2.01; IAG×59 yday $21.80 → 09:30 $21.21 -34.81; KGC×39 yday $32.98 → 09:30 $32.32 -25.74; NFGC×665 yday $1.90 → 09:30 $1.90 +0.00; WPM×8 yday $160.19 → 09:30 $156.51 -29.44; ABUS×236 yday $5.20 → 09:30 $5.25 +11.80; CYPH×3 yday $1.68 → 09:30 $1.56 -0.36; BTBT×2 yday $1.51 → 09:30 $1.51 +0.00; INDP×2 yday $1.16 → 09:30 $1.15 -0.02 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 56 | $20.32 | $2.18 | $-17.22 | $1,158.52 | ▼ -17.22 after sell → book $9,692.82; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 56 | $20.47 | $2.18 | $-14.42 | $2,302.67 | ▼ -14.42 after sell → book $9,690.65; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 201 | $5.53 | $2.64 | $-53.48 | $3,411.55 | ▼ -53.48 after sell → book $9,688.00; vs 09:30 mark -2.65 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 59 | $21.21 | $2.19 | $+88.87 | $4,660.76 | ▲ +88.87 after sell → book $9,685.82; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 39 | $32.32 | $2.13 | $+100.68 | $5,919.11 | ▲ +100.68 after sell → book $9,683.69; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 665 | $1.90 | $8.70 | $+82.47 | $7,173.91 | ▲ +82.47 after sell → book $9,674.99; vs 09:30 mark -8.70 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ABUS` | 236 | $5.25 | $3.09 | $+71.74 | $8,409.82 | ▲ +71.74 after sell → book $9,671.90; vs 09:30 mark -3.09 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 737 | $1.63 | $9.51 | — | $7,199.00 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1201.40 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 338 | $3.55 | $4.36 | — | $5,994.74 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+27.9; leftover $1201.40 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 188 | $6.37 | $2.55 | — | $4,794.63 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+10.9; leftover $1201.40 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 34 | $35.05 | $2.09 | — | $3,600.83 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1201.40 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ETON` | 18 | $64.55 | $2.04 | — | $2,436.89 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+4.4; leftover $1201.40 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUZ` | 133 | $8.98 | $2.39 | — | $1,240.16 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+15.4; leftover $1201.40 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `IAUX` | 632 | $1.90 | $8.15 | — | $31.21 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+16.4; leftover $1201.40 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $31.21 | ▲ close $9,869.62 vs 09:30 $9,695.00 (session +228.82) | 16:00 close · cash $31.21 · equity $9,869.62 vs 09:30 $9,695.00 (+174.62; session marks +228.82) · 11 name(s) marked open→close (per-name table). WPM×8 09:30 $156.51 → close $163.72 +57.68; CYPH×3 09:30 $1.56 → close $1.64 +0.24; BTBT×2 09:30 $1.51 → close $1.58 +0.14; INDP×2 09:30 $1.15 → close $1.09 -0.11; BMEA×737 09:30 $1.63 → close $1.73 +73.70; GORO×338 09:30 $3.55 → close $3.87 +108.16; ZURA×188 09:30 $6.37 → close $6.32 -9.40; EZPW×34 09:30 $35.05 → close $35.23 +6.12; ETON×18 09:30 $64.55 → close $63.05 -27.00; SUZ×133 09:30 $8.98 → close $9.03 +6.65; IAUX×632 09:30 $1.90 → close $1.92 +12.64 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $31.21 | ▼ 09:30 equity $9,790.25 vs yday $9,869.62 (-79.37) | 09:30 open · cash $31.21 (unchanged overnight, no fees) · equity $9,790.25 vs prior close $9,869.62 (-79.37) · 11 name(s) re-marked at the open (per-name table). WPM×8 yday $163.72 → 09:30 $160.93 -22.32; CYPH×3 yday $1.64 → 09:30 $1.60 -0.12; BTBT×2 yday $1.58 → 09:30 $1.53 -0.10; INDP×2 yday $1.09 → 09:30 $1.09 -0.01; BMEA×737 yday $1.73 → 09:30 $1.75 +18.42; GORO×338 yday $3.87 → 09:30 $3.77 -33.80; ZURA×188 yday $6.32 → 09:30 $6.13 -35.72; EZPW×34 yday $35.23 → 09:30 $35.70 +15.98; ETON×18 yday $63.05 → 09:30 $63.60 +9.90; SUZ×133 yday $9.03 → 09:30 $9.03 +0.00; IAUX×632 yday $1.92 → 09:30 $1.87 -31.60 | — |
| 2026-08-26 09:30 ET | **SELL** | `WPM` | 8 | $160.93 | $2.03 | $+127.07 | $1,316.61 | ▲ +127.07 after sell → book $9,788.22; vs 09:30 mark -2.03 | dropped from list after 4 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 3 | $1.60 | $0.08 | $+0.71 | $1,321.34 | ▲ +0.71 after sell → book $9,788.14; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BTBT` | 2 | $1.53 | $0.06 | $-0.36 | $1,324.34 | ▼ -0.36 after sell → book $9,788.09; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `INDP` | 2 | $1.09 | $0.05 | $-0.68 | $1,326.47 | ▼ -0.68 after sell → book $9,788.04; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 227 | $5.81 | $2.93 | — | $4.67 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $1326.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.67 | ▼ close $9,608.74 vs 09:30 $9,790.25 (session -176.36) | 16:00 close · cash $4.67 · equity $9,608.74 vs 09:30 $9,790.25 (-181.51; session marks -176.36) · 8 name(s) marked open→close (per-name table). BMEA×737 09:30 $1.75 → close $1.71 -33.16; GORO×338 09:30 $3.77 → close $3.56 -70.98; ZURA×188 09:30 $6.13 → close $5.99 -26.32; EZPW×34 09:30 $35.70 → close $33.90 -61.20; ETON×18 09:30 $63.60 → close $62.62 -17.64; SUZ×133 09:30 $9.03 → close $8.94 -11.97; IAUX×632 09:30 $1.87 → close $1.88 +6.32; USDE×227 09:30 $5.81 → close $5.98 +38.59 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.67 | ▲ 09:30 equity $9,734.61 vs yday $9,608.74 (+125.87) | 09:30 open · cash $4.67 (unchanged overnight, no fees) · equity $9,734.61 vs prior close $9,608.74 (+125.87) · 8 name(s) re-marked at the open (per-name table). BMEA×737 yday $1.71 → 09:30 $1.74 +22.11; GORO×338 yday $3.56 → 09:30 $3.59 +10.14; ZURA×188 yday $5.99 → 09:30 $6.02 +5.64; EZPW×34 yday $33.90 → 09:30 $33.50 -13.60; ETON×18 yday $62.62 → 09:30 $62.50 -2.16; SUZ×133 yday $8.94 → 09:30 $8.88 -7.98; IAUX×632 yday $1.88 → 09:30 $1.87 -6.32; USDE×227 yday $5.98 → 09:30 $6.50 +118.04 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.67 | ▲ close $10,158.66 vs 09:30 $9,734.61 (session +424.05) | 16:00 close · cash $4.67 · equity $10,158.66 vs 09:30 $9,734.61 (+424.05; session marks +424.05) · 8 name(s) marked open→close (per-name table). BMEA×737 09:30 $1.74 → close $1.68 -44.22; GORO×338 09:30 $3.59 → close $3.79 +67.60; ZURA×188 09:30 $6.02 → close $5.85 -31.96; EZPW×34 09:30 $33.50 → close $34.41 +30.94; ETON×18 09:30 $62.50 → close $62.67 +3.06; SUZ×133 09:30 $8.88 → close $8.96 +10.64; IAUX×632 09:30 $1.87 → close $1.92 +31.60; USDE×227 09:30 $6.50 → close $8.07 +356.39 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.67 | ▼ 09:30 equity $9,995.57 vs yday $10,158.66 (-163.09) | 09:30 open · cash $4.67 (unchanged overnight, no fees) · equity $9,995.57 vs prior close $10,158.66 (-163.09) · 8 name(s) re-marked at the open (per-name table). BMEA×737 yday $1.68 → 09:30 $1.69 +7.37; GORO×338 yday $3.79 → 09:30 $3.80 +3.38; ZURA×188 yday $5.85 → 09:30 $5.88 +5.64; EZPW×34 yday $34.41 → 09:30 $34.50 +3.06; ETON×18 yday $62.67 → 09:30 $61.98 -12.42; SUZ×133 yday $8.96 → 09:30 $9.05 +11.97; IAUX×632 yday $1.92 → 09:30 $1.93 +6.32; USDE×227 yday $8.07 → 09:30 $7.24 -188.41 | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 737 | $1.69 | $9.64 | $+25.07 | $1,240.56 | ▲ +25.07 after sell → book $9,985.93; vs 09:30 mark -9.64 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GORO` | 338 | $3.80 | $4.43 | $+75.71 | $2,520.54 | ▲ +75.71 after sell → book $9,981.51; vs 09:30 mark -4.42 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZURA` | 188 | $5.88 | $2.60 | $-97.27 | $3,623.38 | ▼ -97.27 after sell → book $9,978.91; vs 09:30 mark -2.60 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EZPW` | 34 | $34.50 | $2.11 | $-22.90 | $4,794.27 | ▼ -22.90 after sell → book $9,976.80; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ETON` | 18 | $61.98 | $2.06 | $-50.37 | $5,907.85 | ▼ -50.37 after sell → book $9,974.74; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SUZ` | 133 | $9.05 | $2.42 | $+4.50 | $7,109.08 | ▲ +4.50 after sell → book $9,972.32; vs 09:30 mark -2.42 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `IAUX` | 632 | $1.93 | $8.27 | $+2.54 | $8,320.57 | ▲ +2.54 after sell → book $9,964.05; vs 09:30 mark -8.27 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 14 | $146.07 | $2.03 | — | $6,273.56 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $2080.14 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 89 | $23.30 | $2.26 | — | $4,197.60 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+14.5; leftover $2080.14 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 109 | $19.00 | $2.32 | — | $2,124.28 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; ret5=+7.5; leftover $2080.14 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 84 | $24.69 | $2.24 | — | $48.08 | — | combo gate; gate join=good,vol=good,last_green=True; list earn_react; ret5=+5.8; leftover $2080.14 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.08 | ▼ close $9,700.86 vs 09:30 $9,995.57 (session -254.34) | 16:00 close · cash $48.08 · equity $9,700.86 vs 09:30 $9,995.57 (-294.71; session marks -254.34) · 5 name(s) marked open→close (per-name table). USDE×227 09:30 $7.24 → close $6.76 -108.96; ANF×14 09:30 $146.07 → close $148.42 +32.90; NCNO×89 09:30 $23.30 → close $22.99 -27.59; TH×109 09:30 $19.00 → close $18.55 -49.05; GAP×84 09:30 $24.69 → close $23.48 -101.64 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.08 | ▼ 09:30 equity $9,577.70 vs yday $9,700.86 (-123.16) | 09:30 open · cash $48.08 (unchanged overnight, no fees) · equity $9,577.70 vs prior close $9,700.86 (-123.16) · 5 name(s) re-marked at the open (per-name table). USDE×227 yday $6.76 → 09:30 $6.76 +0.00; ANF×14 yday $148.42 → 09:30 $148.03 -5.46; NCNO×89 yday $22.99 → 09:30 $22.66 -29.37; TH×109 yday $18.55 → 09:30 $18.12 -46.33; GAP×84 yday $23.48 → 09:30 $22.98 -42.00 | — |
| 2026-08-31 09:30 ET | **SELL** | `USDE` | 227 | $6.76 | $2.98 | $+209.74 | $1,579.62 | ▲ +209.74 after sell → book $9,574.73; vs 09:30 mark -2.97 | dropped from list after 3 sess (min 3) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,579.62 | ▼ close $9,486.86 vs 09:30 $9,577.70 (session -87.87) | 16:00 close · cash $1,579.62 · equity $9,486.86 vs 09:30 $9,577.70 (-90.84; session marks -87.87) · 4 name(s) marked open→close (per-name table). ANF×14 09:30 $148.03 → close $143.08 -69.30; NCNO×89 09:30 $22.66 → close $22.60 -5.34; TH×109 09:30 $18.12 → close $18.52 +43.05; GAP×84 09:30 $22.98 → close $22.31 -56.28 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,579.62 | ▼ 09:30 equity $9,402.22 vs yday $9,486.86 (-84.64) | 09:30 open · cash $1,579.62 (unchanged overnight, no fees) · equity $9,402.22 vs prior close $9,486.86 (-84.64) · 4 name(s) re-marked at the open (per-name table). ANF×14 yday $143.08 → 09:30 $142.00 -15.12; NCNO×89 yday $22.60 → 09:30 $22.15 -40.05; TH×109 yday $18.52 → 09:30 $18.45 -7.63; GAP×84 yday $22.31 → 09:30 $22.05 -21.84 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,579.62 | ▼ close $9,351.47 vs 09:30 $9,402.22 (session -50.75) | 16:00 close · cash $1,579.62 · equity $9,351.47 vs 09:30 $9,402.22 (-50.75; session marks -50.75) · 4 name(s) marked open→close (per-name table). ANF×14 09:30 $142.00 → close $140.68 -18.48; NCNO×89 09:30 $22.15 → close $22.30 +13.35; TH×109 09:30 $18.45 → close $18.07 -41.42; GAP×84 09:30 $22.05 → close $22.00 -4.20 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,579.62 | ▼ 09:30 equity $9,315.82 vs yday $9,351.47 (-35.65) | 09:30 open · cash $1,579.62 (unchanged overnight, no fees) · equity $9,315.82 vs prior close $9,351.47 (-35.65) · 4 name(s) re-marked at the open (per-name table). ANF×14 yday $140.68 → 09:30 $139.65 -14.42; NCNO×89 yday $22.30 → 09:30 $22.20 -8.90; TH×109 yday $18.07 → 09:30 $17.98 -9.81; GAP×84 yday $22.00 → 09:30 $21.97 -2.52 | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 14 | $139.65 | $2.06 | $-93.97 | $3,532.66 | ▼ -93.97 after sell → book $9,313.76; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `NCNO` | 89 | $22.20 | $2.29 | $-102.44 | $5,506.18 | ▼ -102.44 after sell → book $9,311.48; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TH` | 109 | $17.98 | $2.35 | $-115.85 | $7,463.65 | ▼ -115.85 after sell → book $9,309.13; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GAP` | 84 | $21.97 | $2.27 | $-232.99 | $9,306.85 | ▼ -232.99 after sell → book $9,306.85; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,306.85 | ▲ close $9,306.85 vs 09:30 $9,315.82 (session +0.00) | 16:00 close · cash $9,306.85 · no lots left · equity $9,306.85. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,306.85 | ▲ 09:30 equity $9,306.85 vs yday $9,306.85 (+0.00) | 09:30 open · cash $9,306.85 · no holdings · equity $9,306.85 vs prior close $9,306.85 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 8 | $132.45 | $2.01 | — | $8,245.24 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1163.36 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 69 | $16.77 | $2.20 | — | $7,085.91 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1163.36 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 533 | $2.18 | $6.88 | — | $5,917.10 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1163.36 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 48 | $23.88 | $2.13 | — | $4,768.72 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1163.36 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 111 | $10.42 | $2.32 | — | $3,609.78 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; leftover $1163.36 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 602 | $1.93 | $7.77 | — | $2,440.16 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1163.36 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DUOL` | 7 | $161.54 | $2.01 | — | $1,307.36 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+12.0; leftover $1163.36 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ALMS` | 112 | $10.38 | $2.33 | — | $143.04 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ret5=-56.2; leftover $1163.36 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $143.04 | ▼ close $9,238.94 vs 09:30 $9,306.85 (session -40.27) | 16:00 close · cash $143.04 · equity $9,238.94 vs 09:30 $9,306.85 (-67.91; session marks -40.27) · 8 name(s) marked open→close (per-name table). RVTY×8 09:30 $132.45 → close $130.63 -14.56; ARCT×69 09:30 $16.77 → close $15.56 -83.49; CRDL×533 09:30 $2.18 → close $2.16 -10.66; MMED×48 09:30 $23.88 → close $23.84 -1.92; NVAX×111 09:30 $10.42 → close $10.34 -8.88; BMEA×602 09:30 $1.93 → close $1.91 -12.04; DUOL×7 09:30 $161.54 → close $158.82 -19.04; ALMS×112 09:30 $10.38 → close $11.36 +110.32 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $143.04 | ▼ 09:30 equity $9,225.25 vs yday $9,238.94 (-13.69) | 09:30 open · cash $143.04 (unchanged overnight, no fees) · equity $9,225.25 vs prior close $9,238.94 (-13.69) · 8 name(s) re-marked at the open (per-name table). RVTY×8 yday $130.63 → 09:30 $130.03 -4.80; ARCT×69 yday $15.56 → 09:30 $15.61 +3.45; CRDL×533 yday $2.16 → 09:30 $2.16 +0.00; MMED×48 yday $23.84 → 09:30 $23.84 +0.00; NVAX×111 yday $10.34 → 09:30 $10.50 +17.76; BMEA×602 yday $1.91 → 09:30 $1.90 -6.02; DUOL×7 yday $158.82 → 09:30 $157.46 -9.52; ALMS×112 yday $11.36 → 09:30 $11.23 -14.56 | — |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 7 | $2.51 | $0.20 | — | $125.27 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+21.8; leftover $17.88 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 3 | $5.79 | $0.18 | — | $107.72 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $17.88 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `AHCO` | 2 | $6.32 | $0.13 | — | $94.95 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+10.7; leftover $17.88 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $94.95 | ▲ close $9,248.92 vs 09:30 $9,225.25 (session +24.18) | 16:00 close · cash $94.95 · equity $9,248.92 vs 09:30 $9,225.25 (+23.67; session marks +24.18) · 11 name(s) marked open→close (per-name table). RVTY×8 09:30 $130.03 → close $130.22 +1.52; ARCT×69 09:30 $15.61 → close $15.82 +14.49; CRDL×533 09:30 $2.16 → close $2.20 +21.32; MMED×48 09:30 $23.84 → close $23.29 -26.40; NVAX×111 09:30 $10.50 → close $10.22 -31.08; BMEA×602 09:30 $1.90 → close $2.03 +78.26; DUOL×7 09:30 $157.46 → close $154.46 -21.00; ALMS×112 09:30 $11.23 → close $11.10 -14.56; BRR×7 09:30 $2.51 → close $2.66 +1.05; DFDV×3 09:30 $5.79 → close $5.87 +0.24; AHCO×2 09:30 $6.32 → close $6.49 +0.34 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $94.95 | ▼ 09:30 equity $9,154.08 vs yday $9,248.92 (-94.84) | 09:30 open · cash $94.95 (unchanged overnight, no fees) · equity $9,154.08 vs prior close $9,248.92 (-94.84) · 11 name(s) re-marked at the open (per-name table). RVTY×8 yday $130.22 → 09:30 $128.50 -13.76; ARCT×69 yday $15.82 → 09:30 $15.47 -24.15; CRDL×533 yday $2.20 → 09:30 $2.20 +0.00; MMED×48 yday $23.29 → 09:30 $23.16 -6.24; NVAX×111 yday $10.22 → 09:30 $10.10 -13.32; BMEA×602 yday $2.03 → 09:30 $2.00 -18.06; DUOL×7 yday $154.46 → 09:30 $152.53 -13.51; ALMS×112 yday $11.10 → 09:30 $11.05 -5.60; BRR×7 yday $2.66 → 09:30 $2.66 +0.00; DFDV×3 yday $5.87 → 09:30 $5.81 -0.18; AHCO×2 yday $6.49 → 09:30 $6.48 -0.02 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $94.95 | ▼ close $9,044.78 vs 09:30 $9,154.08 (session -109.30) | 16:00 close · cash $94.95 · equity $9,044.78 vs 09:30 $9,154.08 (-109.30; session marks -109.30) · 11 name(s) marked open→close (per-name table). RVTY×8 09:30 $128.50 → close $127.08 -11.36; ARCT×69 09:30 $15.47 → close $15.63 +11.04; CRDL×533 09:30 $2.20 → close $2.22 +10.66; MMED×48 09:30 $23.16 → close $23.32 +7.68; NVAX×111 09:30 $10.10 → close $10.19 +9.99; BMEA×602 09:30 $2.00 → close $1.93 -42.14; DUOL×7 09:30 $152.53 → close $146.39 -42.98; ALMS×112 09:30 $11.05 → close $10.58 -52.64; BRR×7 09:30 $2.66 → close $2.73 +0.49; DFDV×3 09:30 $5.81 → close $5.99 +0.54; AHCO×2 09:30 $6.48 → close $6.19 -0.58 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $94.95 | ▼ 09:30 equity $8,988.86 vs yday $9,044.78 (-55.92) | 09:30 open · cash $94.95 (unchanged overnight, no fees) · equity $8,988.86 vs prior close $9,044.78 (-55.92) · 11 name(s) re-marked at the open (per-name table). RVTY×8 yday $127.08 → 09:30 $125.77 -10.48; ARCT×69 yday $15.63 → 09:30 $15.46 -11.73; CRDL×533 yday $2.22 → 09:30 $2.22 +0.00; MMED×48 yday $23.32 → 09:30 $23.22 -4.80; NVAX×111 yday $10.19 → 09:30 $10.02 -18.87; BMEA×602 yday $1.93 → 09:30 $1.94 +6.02; DUOL×7 yday $146.39 → 09:30 $145.58 -5.67; ALMS×112 yday $10.58 → 09:30 $10.49 -10.08; BRR×7 yday $2.73 → 09:30 $2.75 +0.14; DFDV×3 yday $5.99 → 09:30 $6.02 +0.09; AHCO×2 yday $6.19 → 09:30 $5.92 -0.54 | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 8 | $125.77 | $2.03 | $-57.49 | $1,099.07 | ▼ -57.49 after sell → book $8,986.82; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 69 | $15.46 | $2.22 | $-94.81 | $2,163.59 | ▼ -94.81 after sell → book $8,984.60; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRDL` | 533 | $2.22 | $6.97 | $+7.47 | $3,339.88 | ▲ +7.47 after sell → book $8,977.63; vs 09:30 mark -6.97 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MMED` | 48 | $23.22 | $2.15 | $-35.97 | $4,452.29 | ▼ -35.97 after sell → book $8,975.48; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `NVAX` | 111 | $10.02 | $2.35 | $-49.07 | $5,562.15 | ▼ -49.07 after sell → book $8,973.12; vs 09:30 mark -2.36 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `BMEA` | 602 | $1.94 | $7.88 | $-9.62 | $6,722.16 | ▼ -9.62 after sell → book $8,965.25; vs 09:30 mark -7.87 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DUOL` | 7 | $145.58 | $2.03 | $-115.76 | $7,739.19 | ▼ -115.76 after sell → book $8,963.22; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ALMS` | 112 | $10.49 | $2.35 | $+8.20 | $8,911.71 | ▲ +8.20 after sell → book $8,960.86; vs 09:30 mark -2.36 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,911.71 | ▼ close $8,959.45 vs 09:30 $8,988.86 (session -1.41) | 16:00 close · cash $8,911.71 · equity $8,959.45 vs 09:30 $8,988.86 (-29.41; session marks -1.41) · 3 name(s) marked open→close (per-name table). BRR×7 09:30 $2.75 → close $2.86 +0.77; DFDV×3 09:30 $6.02 → close $5.44 -1.74; AHCO×2 09:30 $5.92 → close $5.70 -0.44 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BETR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ADUR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AIRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `NCMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `QMLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ABX` | cash | leftover split 0.71 < 1 share @ 9.12 |
| 2026-08-17 | `ALOY` | cash | leftover split 0.71 < 1 share @ 14.66 |
| 2026-08-17 | `BORR` | cash | leftover split 0.71 < 1 share @ 4.59 |
| 2026-08-17 | `XHG` | cash | leftover split 0.71 < 1 share @ 4.19 |
| 2026-08-17 | `MP` | cash | leftover split 0.71 < 1 share @ 58.01 |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BETR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ADUR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AIRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NCMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `QMLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ABUS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 4.12 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 4.12 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 4.12 < 1 share @ 216.30 |
| 2026-08-21 | `ARCT` | cash | leftover split 4.12 < 1 share @ 11.13 |
| 2026-08-21 | `MRVI` | cash | leftover split 4.12 < 1 share @ 8.28 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABUS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `INDP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `INDP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `GORO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EZPW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ETON` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SUZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `IAUX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `GORO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EZPW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ETON` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SUZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `IAUX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `USDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `USDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `NCNO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GAP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NCNO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GAP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `NVAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DUOL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ALMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | cash | leftover split 17.88 < 1 share @ 513.78 |
| 2026-09-04 | `TARS` | cash | leftover split 17.88 < 1 share @ 82.70 |
| 2026-09-04 | `MDB` | cash | leftover split 17.88 < 1 share @ 378.34 |
| 2026-09-04 | `ASST` | cash | leftover split 17.88 < 1 share @ 25.18 |
| 2026-09-04 | `TDS` | cash | leftover split 17.88 < 1 share @ 37.44 |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MMED` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `NVAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DUOL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ALMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `BRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `DFDV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `AHCO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CRCL` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TRMD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `BRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `DFDV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `AHCO` | min_hold | dropped but min-hold 2/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `BRR` | 7 | 2026-09-04 @ $2.51 | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+21.8; leftover $17.88 |
| `DFDV` | 3 | 2026-09-04 @ $5.79 | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $17.88 |
| `AHCO` | 2 | 2026-09-04 @ $6.32 | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+10.7; leftover $17.88 |
