# Factor mine action — `union_blue_vol_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-7.69%** ($9,231) · signal-only (no cash/fees) was +154.18%. Starts YES **1/19**. Fills 90 · skips 134 · realized $-768.80.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the volume camera (is this name unusually active?) is green.
- Must-have: the name is painted 🔵 (a turn higher on a still-red row).
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
- **Gate** `vol=good,blue=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,217.75.

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
| 2026-08-14 | `ARX` | 63 | — | $19.57 | +0.00 | $19.58 | +0.63 | +0.63 | +0.00 | +0.63 |
| 2026-08-14 | `AIRO` | 112 | — | $11.12 | +0.00 | $9.57 | -173.60 | -173.60 | +0.00 | -173.60 |
| 2026-08-14 | `NCMI` | 464 | — | $2.69 | +0.00 | $2.86 | +78.88 | +78.88 | +0.00 | +78.88 |
| 2026-08-17 | `BTBT` | 833 | $1.57 | $1.52 | -41.65 | $1.60 | +66.64 | +24.99 | +16.66 | +83.30 |
| 2026-08-17 | `BETR` | 84 | $13.73 | $13.67 | -5.04 | $13.54 | -10.92 | -15.96 | -94.92 | -105.84 |
| 2026-08-17 | `ANGX` | 290 | $4.37 | $4.60 | +66.70 | $4.71 | +31.90 | +98.60 | +84.10 | +116.00 |
| 2026-08-17 | `HYLN` | 299 | $4.06 | $4.10 | +11.96 | $4.09 | -2.99 | +8.97 | -23.92 | -26.91 |
| 2026-08-17 | `ADUR` | 75 | $16.17 | $15.73 | -33.00 | $15.85 | +9.00 | -24.00 | -57.75 | -48.75 |
| 2026-08-17 | `ARX` | 63 | $19.58 | $19.57 | -0.63 | $19.54 | -1.89 | -2.52 | +0.00 | -1.89 |
| 2026-08-17 | `AIRO` | 112 | $9.57 | $9.57 | +0.00 | $9.41 | -17.92 | -17.92 | -173.60 | -191.52 |
| 2026-08-17 | `NCMI` | 464 | $2.86 | $2.80 | -27.84 | $2.73 | -32.48 | -60.32 | +51.04 | +18.56 |
| 2026-08-18 | `BTBT` | 833 | $1.60 | $1.54 | -49.98 | $1.45 | -74.97 | -124.95 | +33.32 | -41.65 |
| 2026-08-18 | `BETR` | 84 | $13.54 | $13.21 | -27.72 | $13.05 | -13.44 | -41.16 | -133.56 | -147.00 |
| 2026-08-18 | `ANGX` | 290 | $4.71 | $4.79 | +23.20 | $4.85 | +17.40 | +40.60 | +139.20 | +156.60 |
| 2026-08-18 | `HYLN` | 299 | $4.09 | $3.95 | -41.86 | $3.86 | -26.91 | -68.77 | -68.77 | -95.68 |
| 2026-08-18 | `ADUR` | 75 | $15.85 | $15.41 | -33.00 | $15.63 | +16.50 | -16.50 | -81.75 | -65.25 |
| 2026-08-18 | `ARX` | 63 | $19.54 | $19.57 | +1.89 | $19.56 | -0.63 | +1.26 | +0.00 | -0.63 |
| 2026-08-18 | `AIRO` | 112 | $9.41 | $9.01 | -44.80 | $8.98 | -3.36 | -48.16 | -236.32 | -239.68 |
| 2026-08-18 | `NCMI` | 464 | $2.73 | $2.71 | -9.28 | $2.52 | -88.16 | -97.44 | +9.28 | -78.88 |
| 2026-08-19 | `BTBT` | 833 | $1.45 | $1.42 | -24.99 | — | +0.00 | -24.99 | -66.64 | — |
| 2026-08-19 | `BETR` | 84 | $13.05 | $13.03 | -1.68 | — | +0.00 | -1.68 | -148.68 | — |
| 2026-08-19 | `ANGX` | 290 | $4.85 | $4.79 | -17.40 | $4.60 | -55.10 | -72.50 | +139.20 | +84.10 |
| 2026-08-19 | `HYLN` | 299 | $3.86 | $3.87 | +2.99 | — | +0.00 | +2.99 | -92.69 | — |
| 2026-08-19 | `ADUR` | 75 | $15.63 | $15.65 | +1.50 | — | +0.00 | +1.50 | -63.75 | — |
| 2026-08-19 | `ARX` | 63 | $19.56 | $19.58 | +1.26 | — | +0.00 | +1.26 | +0.63 | — |
| 2026-08-19 | `AIRO` | 112 | $8.98 | $9.10 | +13.44 | — | +0.00 | +13.44 | -226.24 | — |
| 2026-08-19 | `NCMI` | 464 | $2.52 | $2.56 | +18.56 | — | +0.00 | +18.56 | -60.32 | — |
| 2026-08-20 | `ANGX` | 290 | $4.60 | $4.57 | -8.70 | — | +0.00 | -8.70 | +75.40 | — |
| 2026-08-20 | `AG` | 56 | — | $20.55 | +0.00 | $21.19 | +35.84 | +35.84 | +0.00 | +35.84 |
| 2026-08-20 | `BHP` | 12 | — | $91.01 | +0.00 | $93.63 | +31.44 | +31.44 | +0.00 | +31.44 |
| 2026-08-20 | `CDE` | 56 | — | $20.65 | +0.00 | $21.11 | +25.76 | +25.76 | +0.00 | +25.76 |
| 2026-08-20 | `HDSN` | 202 | — | $5.77 | +0.00 | $5.57 | -40.40 | -40.40 | +0.00 | -40.40 |
| 2026-08-20 | `IAG` | 59 | — | $19.63 | +0.00 | $20.50 | +51.33 | +51.33 | +0.00 | +51.33 |
| 2026-08-20 | `KGC` | 39 | — | $29.63 | +0.00 | $31.43 | +70.20 | +70.20 | +0.00 | +70.20 |
| 2026-08-20 | `NFGC` | 667 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-21 | `AG` | 56 | $21.19 | $21.90 | +39.76 | $21.09 | -45.36 | -5.60 | +75.60 | +30.24 |
| 2026-08-21 | `BHP` | 12 | $93.63 | $95.72 | +25.08 | $97.03 | +15.72 | +40.80 | +56.52 | +72.24 |
| 2026-08-21 | `CDE` | 56 | $21.11 | $21.75 | +35.84 | $20.97 | -43.68 | -7.84 | +61.60 | +17.92 |
| 2026-08-21 | `HDSN` | 202 | $5.57 | $5.67 | +20.20 | $5.63 | -8.08 | +12.12 | -20.20 | -28.28 |
| 2026-08-21 | `IAG` | 59 | $20.50 | $21.17 | +39.53 | $21.14 | -1.77 | +37.76 | +90.86 | +89.09 |
| 2026-08-21 | `KGC` | 39 | $31.43 | $32.17 | +28.86 | $32.76 | +23.01 | +51.87 | +99.06 | +122.07 |
| 2026-08-21 | `NFGC` | 667 | $1.75 | $1.79 | +26.68 | $1.84 | +33.35 | +60.03 | +26.68 | +60.03 |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | $157.78 | +24.64 | +60.24 | +81.28 | +105.92 |
| 2026-08-21 | `ARCT` | 1 | — | $11.13 | +0.00 | $13.45 | +2.32 | +2.32 | +0.00 | +2.32 |
| 2026-08-21 | `AUTL` | 6 | — | $2.47 | +0.00 | $2.41 | -0.36 | -0.36 | +0.00 | -0.36 |
| 2026-08-21 | `CRDL` | 8 | — | $1.93 | +0.00 | $1.86 | -0.56 | -0.56 | +0.00 | -0.56 |
| 2026-08-21 | `CYPH` | 11 | — | $1.32 | +0.00 | $1.42 | +1.10 | +1.10 | +0.00 | +1.10 |
| 2026-08-24 | `AG` | 56 | $21.09 | $21.30 | +11.76 | $20.83 | -26.32 | -14.56 | +42.00 | +15.68 |
| 2026-08-24 | `BHP` | 12 | $97.03 | $97.31 | +3.36 | $97.13 | -2.16 | +1.20 | +75.60 | +73.44 |
| 2026-08-24 | `CDE` | 56 | $20.97 | $21.26 | +16.24 | $20.88 | -21.28 | -5.04 | +34.16 | +12.88 |
| 2026-08-24 | `HDSN` | 202 | $5.63 | $5.69 | +12.12 | $5.52 | -34.34 | -22.22 | -16.16 | -50.50 |
| 2026-08-24 | `IAG` | 59 | $21.14 | $21.38 | +14.16 | $21.80 | +24.78 | +38.94 | +103.25 | +128.03 |
| 2026-08-24 | `KGC` | 39 | $32.76 | $33.03 | +10.53 | $32.98 | -1.95 | +8.58 | +132.60 | +130.65 |
| 2026-08-24 | `NFGC` | 667 | $1.84 | $1.86 | +13.34 | $1.90 | +26.68 | +40.02 | +73.37 | +100.05 |
| 2026-08-24 | `WPM` | 8 | $157.78 | $159.50 | +13.76 | $160.19 | +5.52 | +19.28 | +119.68 | +125.20 |
| 2026-08-24 | `ARCT` | 1 | $13.45 | $13.33 | -0.12 | $14.34 | +1.01 | +0.89 | +2.20 | +3.21 |
| 2026-08-24 | `AUTL` | 6 | $2.41 | $2.40 | -0.06 | $2.34 | -0.36 | -0.42 | -0.42 | -0.78 |
| 2026-08-24 | `CRDL` | 8 | $1.86 | $1.88 | +0.16 | $1.86 | -0.16 | +0.00 | -0.40 | -0.56 |
| 2026-08-24 | `CYPH` | 11 | $1.42 | $1.83 | +4.51 | $1.68 | -1.65 | +2.86 | +5.61 | +3.96 |
| 2026-08-25 | `AG` | 56 | $20.83 | $20.32 | -28.56 | — | +0.00 | -28.56 | -12.88 | — |
| 2026-08-25 | `BHP` | 12 | $97.13 | $95.86 | -15.24 | — | +0.00 | -15.24 | +58.20 | — |
| 2026-08-25 | `CDE` | 56 | $20.88 | $20.47 | -22.96 | — | +0.00 | -22.96 | -10.08 | — |
| 2026-08-25 | `HDSN` | 202 | $5.52 | $5.53 | +2.02 | — | +0.00 | +2.02 | -48.48 | — |
| 2026-08-25 | `IAG` | 59 | $21.80 | $21.21 | -34.81 | — | +0.00 | -34.81 | +93.22 | — |
| 2026-08-25 | `KGC` | 39 | $32.98 | $32.32 | -25.74 | — | +0.00 | -25.74 | +104.91 | — |
| 2026-08-25 | `NFGC` | 667 | $1.90 | $1.90 | +0.00 | — | +0.00 | +0.00 | +100.05 | — |
| 2026-08-25 | `WPM` | 8 | $160.19 | $156.51 | -29.44 | — | +0.00 | -29.44 | +95.76 | — |
| 2026-08-25 | `ARCT` | 1 | $14.34 | $14.12 | -0.22 | $15.44 | +1.32 | +1.10 | +2.99 | +4.31 |
| 2026-08-25 | `AUTL` | 6 | $2.34 | $2.38 | +0.24 | $2.44 | +0.36 | +0.60 | -0.54 | -0.18 |
| 2026-08-25 | `CRDL` | 8 | $1.86 | $1.89 | +0.24 | $2.00 | +0.88 | +1.12 | -0.32 | +0.56 |
| 2026-08-25 | `CYPH` | 11 | $1.68 | $1.56 | -1.32 | $1.64 | +0.88 | -0.44 | +2.64 | +3.52 |
| 2026-08-25 | `CAPR` | 165 | — | $7.25 | +0.00 | $8.29 | +171.60 | +171.60 | +0.00 | +171.60 |
| 2026-08-25 | `KURA` | 88 | — | $13.59 | +0.00 | $13.59 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `CCOI` | 126 | — | $9.49 | +0.00 | $9.88 | +49.14 | +49.14 | +0.00 | +49.14 |
| 2026-08-25 | `LIFE` | 32 | — | $36.96 | +0.00 | $38.56 | +51.20 | +51.20 | +0.00 | +51.20 |
| 2026-08-25 | `ZIP` | 264 | — | $4.55 | +0.00 | $4.35 | -52.80 | -52.80 | +0.00 | -52.80 |
| 2026-08-25 | `BMEA` | 738 | — | $1.63 | +0.00 | $1.73 | +73.80 | +73.80 | +0.00 | +73.80 |
| 2026-08-25 | `NPWR` | 601 | — | $2.00 | +0.00 | $1.95 | -30.05 | -30.05 | +0.00 | -30.05 |
| 2026-08-25 | `PUSA` | 316 | — | $3.80 | +0.00 | $3.78 | -6.32 | -6.32 | +0.00 | -6.32 |
| 2026-08-26 | `ARCT` | 1 | $15.44 | $15.35 | -0.09 | — | +0.00 | -0.09 | +4.22 | — |
| 2026-08-26 | `AUTL` | 6 | $2.44 | $2.41 | -0.18 | — | +0.00 | -0.18 | -0.36 | — |
| 2026-08-26 | `CRDL` | 8 | $2.00 | $2.03 | +0.24 | — | +0.00 | +0.24 | +0.80 | — |
| 2026-08-26 | `CYPH` | 11 | $1.64 | $1.60 | -0.44 | — | +0.00 | -0.44 | +3.08 | — |
| 2026-08-26 | `CAPR` | 165 | $8.29 | $8.29 | +0.00 | $9.36 | +176.55 | +176.55 | +171.60 | +348.15 |
| 2026-08-26 | `KURA` | 88 | $13.59 | $13.63 | +3.52 | $13.06 | -50.16 | -46.64 | +3.52 | -46.64 |
| 2026-08-26 | `CCOI` | 126 | $9.88 | $9.89 | +1.26 | $10.05 | +20.16 | +21.42 | +50.40 | +70.56 |
| 2026-08-26 | `LIFE` | 32 | $38.56 | $38.24 | -10.24 | $39.11 | +27.84 | +17.60 | +40.96 | +68.80 |
| 2026-08-26 | `ZIP` | 264 | $4.35 | $4.31 | -10.56 | $4.31 | +0.00 | -10.56 | -63.36 | -63.36 |
| 2026-08-26 | `BMEA` | 738 | $1.73 | $1.75 | +18.45 | $1.71 | -33.21 | -14.76 | +92.25 | +59.04 |
| 2026-08-26 | `NPWR` | 601 | $1.95 | $1.93 | -12.02 | $1.81 | -72.12 | -84.14 | -42.07 | -114.19 |
| 2026-08-26 | `PUSA` | 316 | $3.78 | $3.83 | +17.38 | $3.85 | +4.74 | +22.12 | +11.06 | +15.80 |
| 2026-08-26 | `SLQT` | 44 | — | $0.58 | +0.00 | $0.55 | -1.45 | -1.45 | +0.00 | -1.45 |
| 2026-08-26 | `USDE` | 4 | — | $5.81 | +0.00 | $5.98 | +0.68 | +0.68 | +0.00 | +0.68 |
| 2026-08-27 | `CAPR` | 165 | $9.36 | $9.19 | -28.05 | $10.06 | +143.55 | +115.50 | +320.10 | +463.65 |
| 2026-08-27 | `KURA` | 88 | $13.06 | $12.98 | -7.04 | $13.18 | +17.60 | +10.56 | -53.68 | -36.08 |
| 2026-08-27 | `CCOI` | 126 | $10.05 | $9.83 | -27.72 | $9.67 | -20.16 | -47.88 | +42.84 | +22.68 |
| 2026-08-27 | `LIFE` | 32 | $39.11 | $39.40 | +9.28 | $39.44 | +1.28 | +10.56 | +78.08 | +79.36 |
| 2026-08-27 | `ZIP` | 264 | $4.31 | $4.30 | -2.64 | $4.29 | -2.64 | -5.28 | -66.00 | -68.64 |
| 2026-08-27 | `BMEA` | 738 | $1.71 | $1.74 | +22.14 | $1.68 | -44.28 | -22.14 | +81.18 | +36.90 |
| 2026-08-27 | `NPWR` | 601 | $1.81 | $1.83 | +12.02 | $1.89 | +36.06 | +48.08 | -102.17 | -66.11 |
| 2026-08-27 | `PUSA` | 316 | $3.85 | $3.86 | +3.16 | $3.79 | -22.12 | -18.96 | +18.96 | -3.16 |
| 2026-08-27 | `SLQT` | 44 | $0.55 | $0.53 | -0.88 | $0.54 | +0.44 | -0.44 | -2.33 | -1.89 |
| 2026-08-27 | `USDE` | 4 | $5.98 | $6.50 | +2.08 | $8.07 | +6.28 | +8.36 | +2.76 | +9.04 |
| 2026-08-28 | `CAPR` | 165 | $10.06 | $9.73 | -54.45 | — | +0.00 | -54.45 | +409.20 | — |
| 2026-08-28 | `KURA` | 88 | $13.18 | $13.05 | -11.44 | — | +0.00 | -11.44 | -47.52 | — |
| 2026-08-28 | `CCOI` | 126 | $9.67 | $9.70 | +3.78 | — | +0.00 | +3.78 | +26.46 | — |
| 2026-08-28 | `LIFE` | 32 | $39.44 | $39.60 | +5.12 | — | +0.00 | +5.12 | +84.48 | — |
| 2026-08-28 | `ZIP` | 264 | $4.29 | $4.21 | -21.12 | — | +0.00 | -21.12 | -89.76 | — |
| 2026-08-28 | `BMEA` | 738 | $1.68 | $1.69 | +7.38 | — | +0.00 | +7.38 | +44.28 | — |
| 2026-08-28 | `NPWR` | 601 | $1.89 | $1.89 | +0.00 | — | +0.00 | +0.00 | -66.11 | — |
| 2026-08-28 | `PUSA` | 316 | $3.79 | $3.77 | -6.32 | — | +0.00 | -6.32 | -9.48 | — |
| 2026-08-28 | `SLQT` | 44 | $0.54 | $0.53 | -0.40 | $0.52 | -0.48 | -0.88 | -2.29 | -2.77 |
| 2026-08-28 | `USDE` | 4 | $8.07 | $7.24 | -3.32 | $6.76 | -1.92 | -5.24 | +5.72 | +3.80 |
| 2026-08-28 | `SEDG` | 60 | — | $32.90 | +0.00 | $31.41 | -89.40 | -89.40 | +0.00 | -89.40 |
| 2026-08-28 | `URBN` | 24 | — | $79.42 | +0.00 | $81.09 | +40.08 | +40.08 | +0.00 | +40.08 |
| 2026-08-28 | `ANF` | 13 | — | $146.07 | +0.00 | $148.42 | +30.55 | +30.55 | +0.00 | +30.55 |
| 2026-08-28 | `SMTC` | 14 | — | $141.76 | +0.00 | $131.17 | -148.26 | -148.26 | +0.00 | -148.26 |
| 2026-08-28 | `NCNO` | 85 | — | $23.30 | +0.00 | $22.99 | -26.35 | -26.35 | +0.00 | -26.35 |
| 2026-08-31 | `SLQT` | 44 | $0.52 | $0.51 | -0.44 | — | +0.00 | -0.44 | -3.21 | — |
| 2026-08-31 | `USDE` | 4 | $6.76 | $6.76 | +0.00 | — | +0.00 | +0.00 | +3.80 | — |
| 2026-08-31 | `SEDG` | 60 | $31.41 | $31.15 | -15.60 | $32.20 | +63.00 | +47.40 | -105.00 | -42.00 |
| 2026-08-31 | `URBN` | 24 | $81.09 | $80.44 | -15.60 | $80.69 | +6.00 | -9.60 | +24.48 | +30.48 |
| 2026-08-31 | `ANF` | 13 | $148.42 | $148.03 | -5.07 | $143.08 | -64.35 | -69.42 | +25.48 | -38.87 |
| 2026-08-31 | `SMTC` | 14 | $131.17 | $132.30 | +15.82 | $132.96 | +9.24 | +25.06 | -132.44 | -123.20 |
| 2026-08-31 | `NCNO` | 85 | $22.99 | $22.66 | -28.05 | $22.60 | -5.10 | -33.15 | -54.40 | -59.50 |
| 2026-09-01 | `SEDG` | 60 | $32.20 | $31.87 | -19.80 | $32.49 | +37.20 | +17.40 | -61.80 | -24.60 |
| 2026-09-01 | `URBN` | 24 | $80.69 | $79.12 | -37.68 | $79.29 | +4.08 | -33.60 | -7.20 | -3.12 |
| 2026-09-01 | `ANF` | 13 | $143.08 | $142.00 | -14.04 | $140.68 | -17.16 | -31.20 | -52.91 | -70.07 |
| 2026-09-01 | `SMTC` | 14 | $132.96 | $127.63 | -74.62 | $132.27 | +64.96 | -9.66 | -197.82 | -132.86 |
| 2026-09-01 | `NCNO` | 85 | $22.60 | $22.15 | -38.25 | $22.30 | +12.75 | -25.50 | -97.75 | -85.00 |
| 2026-09-02 | `SEDG` | 60 | $32.49 | $32.42 | -4.20 | — | +0.00 | -4.20 | -28.80 | — |
| 2026-09-02 | `URBN` | 24 | $79.29 | $78.84 | -10.80 | — | +0.00 | -10.80 | -13.92 | — |
| 2026-09-02 | `ANF` | 13 | $140.68 | $139.65 | -13.39 | — | +0.00 | -13.39 | -83.46 | — |
| 2026-09-02 | `SMTC` | 14 | $132.27 | $133.00 | +10.22 | — | +0.00 | +10.22 | -122.64 | — |
| 2026-09-02 | `NCNO` | 85 | $22.30 | $22.20 | -8.50 | — | +0.00 | -8.50 | -93.50 | — |
| 2026-09-03 | `RVTY` | 9 | — | $132.45 | +0.00 | $130.63 | -16.38 | -16.38 | +0.00 | -16.38 |
| 2026-09-03 | `CRK` | 77 | — | $15.45 | +0.00 | $14.95 | -38.50 | -38.50 | +0.00 | -38.50 |
| 2026-09-03 | `MRNA` | 8 | — | $145.94 | +0.00 | $148.87 | +23.40 | +23.40 | +0.00 | +23.40 |
| 2026-09-03 | `ARCT` | 71 | — | $16.77 | +0.00 | $15.56 | -85.91 | -85.91 | +0.00 | -85.91 |
| 2026-09-03 | `CRDL` | 550 | — | $2.18 | +0.00 | $2.16 | -11.00 | -11.00 | +0.00 | -11.00 |
| 2026-09-03 | `MMED` | 50 | — | $23.88 | +0.00 | $23.84 | -2.00 | -2.00 | +0.00 | -2.00 |
| 2026-09-03 | `DEFT` | 1847 | — | $0.65 | +0.00 | $0.68 | +53.56 | +53.56 | +0.00 | +53.56 |
| 2026-09-03 | `CTMX` | 322 | — | $3.73 | +0.00 | $3.68 | -16.10 | -16.10 | +0.00 | -16.10 |
| 2026-09-04 | `RVTY` | 9 | $130.63 | $130.03 | -5.40 | $130.22 | +1.71 | -3.69 | -21.78 | -20.07 |
| 2026-09-04 | `CRK` | 77 | $14.95 | $15.00 | +3.85 | $15.26 | +20.02 | +23.87 | -34.65 | -14.63 |
| 2026-09-04 | `MRNA` | 8 | $148.87 | $153.62 | +38.00 | $145.55 | -64.56 | -26.56 | +61.40 | -3.16 |
| 2026-09-04 | `ARCT` | 71 | $15.56 | $15.61 | +3.55 | $15.82 | +14.91 | +18.46 | -82.36 | -67.45 |
| 2026-09-04 | `CRDL` | 550 | $2.16 | $2.16 | +0.00 | $2.20 | +22.00 | +22.00 | -11.00 | +11.00 |
| 2026-09-04 | `MMED` | 50 | $23.84 | $23.84 | +0.00 | $23.29 | -27.50 | -27.50 | -2.00 | -29.50 |
| 2026-09-04 | `DEFT` | 1847 | $0.68 | $0.69 | +20.32 | $0.68 | -12.93 | +7.39 | +73.88 | +60.95 |
| 2026-09-04 | `CTMX` | 322 | $3.68 | $3.64 | -12.88 | $3.71 | +22.54 | +9.66 | -28.98 | -6.44 |
| 2026-09-04 | `CABA` | 1 | — | $3.46 | +0.00 | $3.47 | +0.01 | +0.01 | +0.00 | +0.01 |
| 2026-09-04 | `ALEC` | 1 | — | $2.52 | +0.00 | $2.46 | -0.06 | -0.06 | +0.00 | -0.06 |
| 2026-09-04 | `BMEA` | 2 | — | $1.90 | +0.00 | $2.03 | +0.26 | +0.26 | +0.00 | +0.26 |
| 2026-09-04 | `EOSE` | 1 | — | $3.52 | +0.00 | $3.88 | +0.36 | +0.36 | +0.00 | +0.36 |
| 2026-09-08 | `RVTY` | 9 | $130.22 | $128.50 | -15.48 | $127.08 | -12.78 | -28.26 | -35.55 | -48.33 |
| 2026-09-08 | `CRK` | 77 | $15.26 | $15.50 | +18.48 | $15.16 | -26.18 | -7.70 | +3.85 | -22.33 |
| 2026-09-08 | `MRNA` | 8 | $145.55 | $145.98 | +3.44 | $140.33 | -45.20 | -41.76 | +0.28 | -44.92 |
| 2026-09-08 | `ARCT` | 71 | $15.82 | $15.47 | -24.85 | $15.63 | +11.36 | -13.49 | -92.30 | -80.94 |
| 2026-09-08 | `CRDL` | 550 | $2.20 | $2.20 | +0.00 | $2.22 | +11.00 | +11.00 | +11.00 | +22.00 |
| 2026-09-08 | `MMED` | 50 | $23.29 | $23.16 | -6.50 | $23.32 | +8.00 | +1.50 | -36.00 | -28.00 |
| 2026-09-08 | `DEFT` | 1847 | $0.68 | $0.64 | -79.42 | $0.62 | -35.09 | -114.51 | -18.47 | -53.56 |
| 2026-09-08 | `CTMX` | 322 | $3.71 | $3.61 | -31.56 | $3.65 | +12.24 | -19.32 | -38.00 | -25.76 |
| 2026-09-08 | `CABA` | 1 | $3.47 | $3.43 | -0.04 | $3.27 | -0.16 | -0.20 | -0.03 | -0.19 |
| 2026-09-08 | `ALEC` | 1 | $2.46 | $2.38 | -0.08 | $2.47 | +0.09 | +0.01 | -0.14 | -0.05 |
| 2026-09-08 | `BMEA` | 2 | $2.03 | $2.00 | -0.06 | $1.93 | -0.14 | -0.20 | +0.20 | +0.06 |
| 2026-09-08 | `EOSE` | 1 | $3.88 | $3.99 | +0.11 | $4.30 | +0.31 | +0.42 | +0.47 | +0.78 |
| 2026-09-09 | `RVTY` | 9 | $127.08 | $125.77 | -11.79 | — | +0.00 | -11.79 | -60.12 | — |
| 2026-09-09 | `CRK` | 77 | $15.16 | $15.16 | +0.00 | — | +0.00 | +0.00 | -22.33 | — |
| 2026-09-09 | `MRNA` | 8 | $140.33 | $140.29 | -0.28 | — | +0.00 | -0.28 | -45.20 | — |
| 2026-09-09 | `ARCT` | 71 | $15.63 | $15.46 | -12.07 | — | +0.00 | -12.07 | -93.01 | — |
| 2026-09-09 | `CRDL` | 550 | $2.22 | $2.22 | +0.00 | — | +0.00 | +0.00 | +22.00 | — |
| 2026-09-09 | `MMED` | 50 | $23.32 | $23.22 | -5.00 | — | +0.00 | -5.00 | -33.00 | — |
| 2026-09-09 | `DEFT` | 1847 | $0.62 | $0.63 | +9.24 | — | +0.00 | +9.24 | -44.33 | — |
| 2026-09-09 | `CTMX` | 322 | $3.65 | $3.66 | +3.22 | — | +0.00 | +3.22 | -22.54 | — |
| 2026-09-09 | `CABA` | 1 | $3.27 | $3.28 | +0.01 | $2.91 | -0.37 | -0.36 | -0.18 | -0.55 |
| 2026-09-09 | `ALEC` | 1 | $2.47 | $2.47 | +0.00 | $2.27 | -0.20 | -0.20 | -0.05 | -0.25 |
| 2026-09-09 | `BMEA` | 2 | $1.93 | $1.94 | +0.02 | $1.84 | -0.19 | -0.17 | +0.08 | -0.11 |
| 2026-09-09 | `EOSE` | 1 | $4.30 | $4.18 | -0.12 | $4.15 | -0.03 | -0.15 | +0.66 | +0.63 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -168.89 | BTBT, BETR, ANGX, HYLN, ADUR, ARX, AIRO, NCMI | — | $10.28 | $9,797.82 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 |
| 2026-08-17 | +2.25 | $10.28 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | $9,768.32 | -29.50 | +41.34 | — | — | $10.28 | $9,809.66 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 |
| 2026-08-18 | -6.20 | $10.28 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | $9,628.11 | -181.55 | -173.57 | — | — | $10.28 | $9,454.54 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 |
| 2026-08-19 | -7.20 | $10.28 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | $9,448.22 | -6.32 | -55.10 | — | BTBT, BETR, HYLN, ADUR, ARX, AIRO, NCMI | $8,029.18 | $9,363.18 | ANGX×290 |
| 2026-08-20 | +1.12 | $8,029.18 | ANGX×290 | $9,354.48 | -8.70 | +219.85 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | ANGX | $124.67 | $9,546.69 | AG×56, BHP×12, CDE×56, HDSN×202, IAG×59, KGC×39, NFGC×667, WPM×8 |
| 2026-08-21 | +3.25 | $124.67 | AG×56, BHP×12, CDE×56, HDSN×202, IAG×59, KGC×39, NFGC×667, WPM×8 | $9,798.24 | +251.55 | +0.33 | ARCT, AUTL, CRDL, CYPH | — | $68.12 | $9,797.93 | AG×56, BHP×12, CDE×56, HDSN×202, IAG×59, KGC×39, NFGC×667, WPM×8, ARCT×1, AUTL×6, CRDL×8, CYPH×11 |
| 2026-08-24 | -5.17 | $68.12 | AG×56, BHP×12, CDE×56, HDSN×202, IAG×59, KGC×39, NFGC×667, WPM×8, ARCT×1, AUTL×6, CRDL×8, CYPH×11 | $9,897.69 | +99.76 | -30.23 | — | — | $68.12 | $9,867.46 | AG×56, BHP×12, CDE×56, HDSN×202, IAG×59, KGC×39, NFGC×667, WPM×8, ARCT×1, AUTL×6, CRDL×8, CYPH×11 |
| 2026-08-25 | +1.80 | $68.12 | AG×56, BHP×12, CDE×56, HDSN×202, IAG×59, KGC×39, NFGC×667, WPM×8, ARCT×1, AUTL×6, CRDL×8, CYPH×11 | $9,711.67 | -155.79 | +260.01 | CAPR, KURA, CCOI, LIFE, ZIP, BMEA, NPWR, PUSA | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $15.35 | $9,913.61 | ARCT×1, AUTL×6, CRDL×8, CYPH×11, CAPR×165, KURA×88, CCOI×126, LIFE×32, ZIP×264, BMEA×738, NPWR×601, PUSA×316 |
| 2026-08-26 | +2.02 | $15.35 | ARCT×1, AUTL×6, CRDL×8, CYPH×11, CAPR×165, KURA×88, CCOI×126, LIFE×32, ZIP×264, BMEA×738, NPWR×601, PUSA×316 | $9,920.93 | +7.32 | +73.03 | SLQT, USDE | ARCT, AUTL, CRDL, CYPH | $28.68 | $9,992.53 | CAPR×165, KURA×88, CCOI×126, LIFE×32, ZIP×264, BMEA×738, NPWR×601, PUSA×316, SLQT×44, USDE×4 |
| 2026-08-27 | — | $28.68 | CAPR×165, KURA×88, CCOI×126, LIFE×32, ZIP×264, BMEA×738, NPWR×601, PUSA×316, SLQT×44, USDE×4 | $9,974.88 | -17.65 | +116.01 | — | — | $28.68 | $10,090.89 | CAPR×165, KURA×88, CCOI×126, LIFE×32, ZIP×264, BMEA×738, NPWR×601, PUSA×316, SLQT×44, USDE×4 |
| 2026-08-28 | +0.75 | $28.68 | CAPR×165, KURA×88, CCOI×126, LIFE×32, ZIP×264, BMEA×738, NPWR×601, PUSA×316, SLQT×44, USDE×4 | $10,010.12 | -80.77 | -195.78 | SEDG, URBN, ANF, SMTC, NCNO | CAPR, KURA, CCOI, LIFE, ZIP, BMEA, NPWR, PUSA | $168.71 | $9,769.38 | SLQT×44, USDE×4, SEDG×60, URBN×24, ANF×13, SMTC×14, NCNO×85 |
| 2026-08-31 | -5.85 | $168.71 | SLQT×44, USDE×4, SEDG×60, URBN×24, ANF×13, SMTC×14, NCNO×85 | $9,720.44 | -48.94 | +8.79 | — | SLQT, USDE | $217.51 | $9,728.55 | SEDG×60, URBN×24, ANF×13, SMTC×14, NCNO×85 |
| 2026-09-01 | -6.30 | $217.51 | SEDG×60, URBN×24, ANF×13, SMTC×14, NCNO×85 | $9,544.16 | -184.39 | +101.83 | — | — | $217.51 | $9,645.99 | SEDG×60, URBN×24, ANF×13, SMTC×14, NCNO×85 |
| 2026-09-02 | -3.83 | $217.51 | SEDG×60, URBN×24, ANF×13, SMTC×14, NCNO×85 | $9,619.32 | -26.67 | +0.00 | — | SEDG, URBN, ANF, SMTC, NCNO | $9,608.65 | $9,608.65 | — |
| 2026-09-03 | -0.90 | $9,608.65 | — | $9,608.65 | +0.00 | -92.93 | RVTY, CRK, MRNA, ARCT, CRDL, MMED, DEFT, CTMX | — | $34.72 | $9,476.33 | RVTY×9, CRK×77, MRNA×8, ARCT×71, CRDL×550, MMED×50, DEFT×1847, CTMX×322 |
| 2026-09-04 | +2.25 | $34.72 | RVTY×9, CRK×77, MRNA×8, ARCT×71, CRDL×550, MMED×50, DEFT×1847, CTMX×322 | $9,523.77 | +47.44 | -23.24 | CABA, ALEC, BMEA, EOSE | — | $21.27 | $9,500.38 | RVTY×9, CRK×77, MRNA×8, ARCT×71, CRDL×550, MMED×50, DEFT×1847, CTMX×322, CABA×1, ALEC×1, BMEA×2, EOSE×1 |
| 2026-09-08 | -11.47 | $21.27 | RVTY×9, CRK×77, MRNA×8, ARCT×71, CRDL×550, MMED×50, DEFT×1847, CTMX×322, CABA×1, ALEC×1, BMEA×2, EOSE×1 | $9,364.43 | -135.95 | -76.55 | — | — | $21.27 | $9,287.87 | RVTY×9, CRK×77, MRNA×8, ARCT×71, CRDL×550, MMED×50, DEFT×1847, CTMX×322, CABA×1, ALEC×1, BMEA×2, EOSE×1 |
| 2026-09-09 | -13.95 | $21.27 | RVTY×9, CRK×77, MRNA×8, ARCT×71, CRDL×550, MMED×50, DEFT×1847, CTMX×322, CABA×1, ALEC×1, BMEA×2, EOSE×1 | $9,271.10 | -16.77 | -0.79 | — | RVTY, CRK, MRNA, ARCT, CRDL, MMED, DEFT, CTMX | $9,217.75 | $9,230.77 | CABA×1, ALEC×1, BMEA×2, EOSE×1 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $8,739.75 | — | combo gate; gate vol=good,blue=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 84 | $14.80 | $2.24 | — | $7,494.31 | — | combo gate; gate vol=good,blue=True; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $6,240.67 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 299 | $4.18 | $3.86 | — | $4,986.99 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $3,747.28 | — | combo gate; gate vol=good,blue=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $2,512.19 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 112 | $11.12 | $2.33 | — | $1,264.42 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 464 | $2.69 | $5.99 | — | $10.28 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.28 | ▼ close $9,797.82 vs 09:30 $10,000.00 (session -168.89) | 16:00 close · cash $10.28 · equity $9,797.82 vs 09:30 $10,000.00 (-202.18; session marks -168.89) · 8 name(s) marked open→close (per-name table). BTBT×833 09:30 $1.50 → close $1.57 +58.31; BETR×84 09:30 $14.80 → close $13.73 -89.88; ANGX×290 09:30 $4.31 → close $4.37 +17.40; HYLN×299 09:30 $4.18 → close $4.06 -35.88; ADUR×75 09:30 $16.50 → close $16.17 -24.75; ARX×63 09:30 $19.57 → close $19.58 +0.63; AIRO×112 09:30 $11.12 → close $9.57 -173.60; NCMI×464 09:30 $2.69 → close $2.86 +78.88 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.28 | ▼ 09:30 equity $9,768.32 vs yday $9,797.82 (-29.50) | 09:30 open · cash $10.28 (unchanged overnight, no fees) · equity $9,768.32 vs prior close $9,797.82 (-29.50) · 8 name(s) re-marked at the open (per-name table). BTBT×833 yday $1.57 → 09:30 $1.52 -41.65; BETR×84 yday $13.73 → 09:30 $13.67 -5.04; ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; HYLN×299 yday $4.06 → 09:30 $4.10 +11.96; ADUR×75 yday $16.17 → 09:30 $15.73 -33.00; ARX×63 yday $19.58 → 09:30 $19.57 -0.63; AIRO×112 yday $9.57 → 09:30 $9.57 +0.00; NCMI×464 yday $2.86 → 09:30 $2.80 -27.84 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.28 | ▲ close $9,809.66 vs 09:30 $9,768.32 (session +41.34) | 16:00 close · cash $10.28 · equity $9,809.66 vs 09:30 $9,768.32 (+41.34; session marks +41.34) · 8 name(s) marked open→close (per-name table). BTBT×833 09:30 $1.52 → close $1.60 +66.64; BETR×84 09:30 $13.67 → close $13.54 -10.92; ANGX×290 09:30 $4.60 → close $4.71 +31.90; HYLN×299 09:30 $4.10 → close $4.09 -2.99; ADUR×75 09:30 $15.73 → close $15.85 +9.00; ARX×63 09:30 $19.57 → close $19.54 -1.89; AIRO×112 09:30 $9.57 → close $9.41 -17.92; NCMI×464 09:30 $2.80 → close $2.73 -32.48 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.28 | ▼ 09:30 equity $9,628.11 vs yday $9,809.66 (-181.55) | 09:30 open · cash $10.28 (unchanged overnight, no fees) · equity $9,628.11 vs prior close $9,809.66 (-181.55) · 8 name(s) re-marked at the open (per-name table). BTBT×833 yday $1.60 → 09:30 $1.54 -49.98; BETR×84 yday $13.54 → 09:30 $13.21 -27.72; ANGX×290 yday $4.71 → 09:30 $4.79 +23.20; HYLN×299 yday $4.09 → 09:30 $3.95 -41.86; ADUR×75 yday $15.85 → 09:30 $15.41 -33.00; ARX×63 yday $19.54 → 09:30 $19.57 +1.89; AIRO×112 yday $9.41 → 09:30 $9.01 -44.80; NCMI×464 yday $2.73 → 09:30 $2.71 -9.28 | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.28 | ▼ close $9,454.54 vs 09:30 $9,628.11 (session -173.57) | 16:00 close · cash $10.28 · equity $9,454.54 vs 09:30 $9,628.11 (-173.57; session marks -173.57) · 8 name(s) marked open→close (per-name table). BTBT×833 09:30 $1.54 → close $1.45 -74.97; BETR×84 09:30 $13.21 → close $13.05 -13.44; ANGX×290 09:30 $4.79 → close $4.85 +17.40; HYLN×299 09:30 $3.95 → close $3.86 -26.91; ADUR×75 09:30 $15.41 → close $15.63 +16.50; ARX×63 09:30 $19.57 → close $19.56 -0.63; AIRO×112 09:30 $9.01 → close $8.98 -3.36; NCMI×464 09:30 $2.71 → close $2.52 -88.16 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.28 | ▼ 09:30 equity $9,448.22 vs yday $9,454.54 (-6.32) | 09:30 open · cash $10.28 (unchanged overnight, no fees) · equity $9,448.22 vs prior close $9,454.54 (-6.32) · 8 name(s) re-marked at the open (per-name table). BTBT×833 yday $1.45 → 09:30 $1.42 -24.99; BETR×84 yday $13.05 → 09:30 $13.03 -1.68; ANGX×290 yday $4.85 → 09:30 $4.79 -17.40; HYLN×299 yday $3.86 → 09:30 $3.87 +2.99; ADUR×75 yday $15.63 → 09:30 $15.65 +1.50; ARX×63 yday $19.56 → 09:30 $19.58 +1.26; AIRO×112 yday $8.98 → 09:30 $9.10 +13.44; NCMI×464 yday $2.52 → 09:30 $2.56 +18.56 | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 833 | $1.42 | $10.89 | $-88.28 | $1,182.24 | ▼ -88.28 after sell → book $9,437.32; vs 09:30 mark -10.90 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BETR` | 84 | $13.03 | $2.27 | $-153.19 | $2,274.50 | ▼ -153.19 after sell → book $9,435.06; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `HYLN` | 299 | $3.87 | $3.92 | $-100.46 | $3,427.71 | ▼ -100.46 after sell → book $9,431.14; vs 09:30 mark -3.92 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ADUR` | 75 | $15.65 | $2.24 | $-68.20 | $4,599.22 | ▼ -68.20 after sell → book $9,428.90; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ARX` | 63 | $19.58 | $2.20 | $-3.75 | $5,830.57 | ▼ -3.75 after sell → book $9,426.71; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | join🟡 sector🟡 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `AIRO` | 112 | $9.10 | $2.35 | $-230.92 | $6,847.41 | ▼ -230.92 after sell → book $9,424.35; vs 09:30 mark -2.36 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `NCMI` | 464 | $2.56 | $6.07 | $-72.38 | $8,029.18 | ▼ -72.38 after sell → book $9,418.28; vs 09:30 mark -6.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,029.18 | ▼ close $9,363.18 vs 09:30 $9,448.22 (session -55.10) | 16:00 close · cash $8,029.18 · equity $9,363.18 vs 09:30 $9,448.22 (-85.04; session marks -55.10) · 1 name(s) marked open→close (per-name table). ANGX×290 09:30 $4.79 → close $4.60 -55.10 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,029.18 | ▼ 09:30 equity $9,354.48 vs yday $9,363.18 (-8.70) | 09:30 open · cash $8,029.18 (unchanged overnight, no fees) · equity $9,354.48 vs prior close $9,363.18 (-8.70) · 1 name(s) re-marked at the open (per-name table). ANGX×290 yday $4.60 → 09:30 $4.57 -8.70 | — |
| 2026-08-20 09:30 ET | **SELL** | `ANGX` | 290 | $4.57 | $3.80 | $+67.86 | $9,350.68 | ▲ +67.86 after sell → book $9,350.68; vs 09:30 mark -3.80 | dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 56 | $20.55 | $2.16 | — | $8,197.72 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1168.83 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 12 | $91.01 | $2.03 | — | $7,103.57 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1168.83 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 56 | $20.65 | $2.16 | — | $5,945.02 | — | combo gate; gate vol=good,blue=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1168.83 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 202 | $5.77 | $2.61 | — | $4,776.87 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1168.83 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 59 | $19.63 | $2.17 | — | $3,616.53 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1168.83 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 39 | $29.63 | $2.11 | — | $2,458.86 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1168.83 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 667 | $1.75 | $8.60 | — | $1,283.00 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1168.83 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $124.67 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1168.83 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $124.67 | ▲ close $9,546.69 vs 09:30 $9,354.48 (session +219.85) | 16:00 close · cash $124.67 · equity $9,546.69 vs 09:30 $9,354.48 (+192.21; session marks +219.85) · 8 name(s) marked open→close (per-name table). AG×56 09:30 $20.55 → close $21.19 +35.84; BHP×12 09:30 $91.01 → close $93.63 +31.44; CDE×56 09:30 $20.65 → close $21.11 +25.76; HDSN×202 09:30 $5.77 → close $5.57 -40.40; IAG×59 09:30 $19.63 → close $20.50 +51.33; KGC×39 09:30 $29.63 → close $31.43 +70.20; NFGC×667 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $124.67 | ▲ 09:30 equity $9,798.24 vs yday $9,546.69 (+251.55) | 09:30 open · cash $124.67 (unchanged overnight, no fees) · equity $9,798.24 vs prior close $9,546.69 (+251.55) · 8 name(s) re-marked at the open (per-name table). AG×56 yday $21.19 → 09:30 $21.90 +39.76; BHP×12 yday $93.63 → 09:30 $95.72 +25.08; CDE×56 yday $21.11 → 09:30 $21.75 +35.84; HDSN×202 yday $5.57 → 09:30 $5.67 +20.20; IAG×59 yday $20.50 → 09:30 $21.17 +39.53; KGC×39 yday $31.43 → 09:30 $32.17 +28.86; NFGC×667 yday $1.75 → 09:30 $1.79 +26.68; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $113.42 | — | combo gate; gate vol=good,blue=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $15.58 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 6 | $2.47 | $0.17 | — | $98.44 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $15.58 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 8 | $1.93 | $0.18 | — | $82.82 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $15.58 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 11 | $1.32 | $0.18 | — | $68.12 | — | combo gate; gate vol=good,blue=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $15.58 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $68.12 | ▲ close $9,797.93 vs 09:30 $9,798.24 (session +0.33) | 16:00 close · cash $68.12 · equity $9,797.93 vs 09:30 $9,798.24 (-0.31; session marks +0.33) · 12 name(s) marked open→close (per-name table). AG×56 09:30 $21.90 → close $21.09 -45.36; BHP×12 09:30 $95.72 → close $97.03 +15.72; CDE×56 09:30 $21.75 → close $20.97 -43.68; HDSN×202 09:30 $5.67 → close $5.63 -8.08; IAG×59 09:30 $21.17 → close $21.14 -1.77; KGC×39 09:30 $32.17 → close $32.76 +23.01; NFGC×667 09:30 $1.79 → close $1.84 +33.35; WPM×8 09:30 $154.70 → close $157.78 +24.64; ARCT×1 09:30 $11.13 → close $13.45 +2.32; AUTL×6 09:30 $2.47 → close $2.41 -0.36; CRDL×8 09:30 $1.93 → close $1.86 -0.56; CYPH×11 09:30 $1.32 → close $1.42 +1.10 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $68.12 | ▲ 09:30 equity $9,897.69 vs yday $9,797.93 (+99.76) | 09:30 open · cash $68.12 (unchanged overnight, no fees) · equity $9,897.69 vs prior close $9,797.93 (+99.76) · 12 name(s) re-marked at the open (per-name table). AG×56 yday $21.09 → 09:30 $21.30 +11.76; BHP×12 yday $97.03 → 09:30 $97.31 +3.36; CDE×56 yday $20.97 → 09:30 $21.26 +16.24; HDSN×202 yday $5.63 → 09:30 $5.69 +12.12; IAG×59 yday $21.14 → 09:30 $21.38 +14.16; KGC×39 yday $32.76 → 09:30 $33.03 +10.53; NFGC×667 yday $1.84 → 09:30 $1.86 +13.34; WPM×8 yday $157.78 → 09:30 $159.50 +13.76; ARCT×1 yday $13.45 → 09:30 $13.33 -0.12; AUTL×6 yday $2.41 → 09:30 $2.40 -0.06; CRDL×8 yday $1.86 → 09:30 $1.88 +0.16; CYPH×11 yday $1.42 → 09:30 $1.83 +4.51 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $68.12 | ▼ close $9,867.46 vs 09:30 $9,897.69 (session -30.23) | 16:00 close · cash $68.12 · equity $9,867.46 vs 09:30 $9,897.69 (-30.23; session marks -30.23) · 12 name(s) marked open→close (per-name table). AG×56 09:30 $21.30 → close $20.83 -26.32; BHP×12 09:30 $97.31 → close $97.13 -2.16; CDE×56 09:30 $21.26 → close $20.88 -21.28; HDSN×202 09:30 $5.69 → close $5.52 -34.34; IAG×59 09:30 $21.38 → close $21.80 +24.78; KGC×39 09:30 $33.03 → close $32.98 -1.95; NFGC×667 09:30 $1.86 → close $1.90 +26.68; WPM×8 09:30 $159.50 → close $160.19 +5.52; ARCT×1 09:30 $13.33 → close $14.34 +1.01; AUTL×6 09:30 $2.40 → close $2.34 -0.36; CRDL×8 09:30 $1.88 → close $1.86 -0.16; CYPH×11 09:30 $1.83 → close $1.68 -1.65 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $68.12 | ▼ 09:30 equity $9,711.67 vs yday $9,867.46 (-155.79) | 09:30 open · cash $68.12 (unchanged overnight, no fees) · equity $9,711.67 vs prior close $9,867.46 (-155.79) · 12 name(s) re-marked at the open (per-name table). AG×56 yday $20.83 → 09:30 $20.32 -28.56; BHP×12 yday $97.13 → 09:30 $95.86 -15.24; CDE×56 yday $20.88 → 09:30 $20.47 -22.96; HDSN×202 yday $5.52 → 09:30 $5.53 +2.02; IAG×59 yday $21.80 → 09:30 $21.21 -34.81; KGC×39 yday $32.98 → 09:30 $32.32 -25.74; NFGC×667 yday $1.90 → 09:30 $1.90 +0.00; WPM×8 yday $160.19 → 09:30 $156.51 -29.44; ARCT×1 yday $14.34 → 09:30 $14.12 -0.22; AUTL×6 yday $2.34 → 09:30 $2.38 +0.24; CRDL×8 yday $1.86 → 09:30 $1.89 +0.24; CYPH×11 yday $1.68 → 09:30 $1.56 -1.32 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 56 | $20.32 | $2.18 | $-17.22 | $1,203.86 | ▼ -17.22 after sell → book $9,709.49; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 12 | $95.86 | $2.05 | $+54.13 | $2,352.14 | ▲ +54.13 after sell → book $9,707.45; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 56 | $20.47 | $2.18 | $-14.42 | $3,496.28 | ▼ -14.42 after sell → book $9,705.27; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 202 | $5.53 | $2.65 | $-53.74 | $4,610.69 | ▼ -53.74 after sell → book $9,702.62; vs 09:30 mark -2.65 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 59 | $21.21 | $2.19 | $+88.87 | $5,859.89 | ▲ +88.87 after sell → book $9,700.43; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 39 | $32.32 | $2.13 | $+100.68 | $7,118.25 | ▲ +100.68 after sell → book $9,698.31; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 667 | $1.90 | $8.73 | $+82.72 | $8,376.82 | ▲ +82.72 after sell → book $9,689.58; vs 09:30 mark -8.73 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $156.51 | $2.03 | $+91.71 | $9,626.87 | ▲ +91.71 after sell → book $9,687.55; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 165 | $7.25 | $2.48 | — | $8,428.13 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1203.36 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 88 | $13.59 | $2.25 | — | $7,229.96 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1203.36 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 126 | $9.49 | $2.37 | — | $6,031.85 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $1203.36 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 32 | $36.96 | $2.09 | — | $4,847.04 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1203.36 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 264 | $4.55 | $3.41 | — | $3,642.44 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1203.36 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 738 | $1.63 | $9.52 | — | $2,429.98 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1203.36 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 601 | $2.00 | $7.75 | — | $1,220.22 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+15.0; leftover $1203.36 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 316 | $3.80 | $4.08 | — | $15.35 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+3.1; leftover $1203.36 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.35 | ▲ close $9,913.61 vs 09:30 $9,711.67 (session +260.01) | 16:00 close · cash $15.35 · equity $9,913.61 vs 09:30 $9,711.67 (+201.94; session marks +260.01) · 12 name(s) marked open→close (per-name table). ARCT×1 09:30 $14.12 → close $15.44 +1.32; AUTL×6 09:30 $2.38 → close $2.44 +0.36; CRDL×8 09:30 $1.89 → close $2.00 +0.88; CYPH×11 09:30 $1.56 → close $1.64 +0.88; CAPR×165 09:30 $7.25 → close $8.29 +171.60; KURA×88 09:30 $13.59 → close $13.59 +0.00; CCOI×126 09:30 $9.49 → close $9.88 +49.14; LIFE×32 09:30 $36.96 → close $38.56 +51.20; ZIP×264 09:30 $4.55 → close $4.35 -52.80; BMEA×738 09:30 $1.63 → close $1.73 +73.80; NPWR×601 09:30 $2.00 → close $1.95 -30.05; PUSA×316 09:30 $3.80 → close $3.78 -6.32 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.35 | ▲ 09:30 equity $9,920.93 vs yday $9,913.61 (+7.32) | 09:30 open · cash $15.35 (unchanged overnight, no fees) · equity $9,920.93 vs prior close $9,913.61 (+7.32) · 12 name(s) re-marked at the open (per-name table). ARCT×1 yday $15.44 → 09:30 $15.35 -0.09; AUTL×6 yday $2.44 → 09:30 $2.41 -0.18; CRDL×8 yday $2.00 → 09:30 $2.03 +0.24; CYPH×11 yday $1.64 → 09:30 $1.60 -0.44; CAPR×165 yday $8.29 → 09:30 $8.29 +0.00; KURA×88 yday $13.59 → 09:30 $13.63 +3.52; CCOI×126 yday $9.88 → 09:30 $9.89 +1.26; LIFE×32 yday $38.56 → 09:30 $38.24 -10.24; ZIP×264 yday $4.35 → 09:30 $4.31 -10.56; BMEA×738 yday $1.73 → 09:30 $1.75 +18.45; NPWR×601 yday $1.95 → 09:30 $1.93 -12.02; PUSA×316 yday $3.78 → 09:30 $3.83 +17.38 | — |
| 2026-08-26 09:30 ET | **SELL** | `ARCT` | 1 | $15.35 | $0.18 | $+3.93 | $30.52 | ▲ +3.93 after sell → book $9,920.75; vs 09:30 mark -0.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `AUTL` | 6 | $2.41 | $0.18 | $-0.71 | $44.80 | ▼ -0.71 after sell → book $9,920.57; vs 09:30 mark -0.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CRDL` | 8 | $2.03 | $0.21 | $+0.42 | $60.83 | ▲ +0.42 after sell → book $9,920.36; vs 09:30 mark -0.21 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟡 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 11 | $1.60 | $0.23 | $+2.67 | $78.20 | ▲ +2.67 after sell → book $9,920.13; vs 09:30 mark -0.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 44 | $0.58 | $0.39 | — | $52.16 | — | combo gate; gate vol=good,blue=True; list yday_mover; 🔵; ret5=-27.5; leftover $26.07 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 4 | $5.81 | $0.24 | — | $28.68 | — | combo gate; gate vol=good,blue=True; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $26.07 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.68 | ▲ close $9,992.53 vs 09:30 $9,920.93 (session +73.03) | 16:00 close · cash $28.68 · equity $9,992.53 vs 09:30 $9,920.93 (+71.60; session marks +73.03) · 10 name(s) marked open→close (per-name table). CAPR×165 09:30 $8.29 → close $9.36 +176.55; KURA×88 09:30 $13.63 → close $13.06 -50.16; CCOI×126 09:30 $9.89 → close $10.05 +20.16; LIFE×32 09:30 $38.24 → close $39.11 +27.84; ZIP×264 09:30 $4.31 → close $4.31 +0.00; BMEA×738 09:30 $1.75 → close $1.71 -33.21; NPWR×601 09:30 $1.93 → close $1.81 -72.12; PUSA×316 09:30 $3.83 → close $3.85 +4.74; SLQT×44 09:30 $0.58 → close $0.55 -1.45; USDE×4 09:30 $5.81 → close $5.98 +0.68 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.68 | ▼ 09:30 equity $9,974.88 vs yday $9,992.53 (-17.65) | 09:30 open · cash $28.68 (unchanged overnight, no fees) · equity $9,974.88 vs prior close $9,992.53 (-17.65) · 10 name(s) re-marked at the open (per-name table). CAPR×165 yday $9.36 → 09:30 $9.19 -28.05; KURA×88 yday $13.06 → 09:30 $12.98 -7.04; CCOI×126 yday $10.05 → 09:30 $9.83 -27.72; LIFE×32 yday $39.11 → 09:30 $39.40 +9.28; ZIP×264 yday $4.31 → 09:30 $4.30 -2.64; BMEA×738 yday $1.71 → 09:30 $1.74 +22.14; NPWR×601 yday $1.81 → 09:30 $1.83 +12.02; PUSA×316 yday $3.85 → 09:30 $3.86 +3.16; SLQT×44 yday $0.55 → 09:30 $0.53 -0.88; USDE×4 yday $5.98 → 09:30 $6.50 +2.08 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.68 | ▲ close $10,090.89 vs 09:30 $9,974.88 (session +116.01) | 16:00 close · cash $28.68 · equity $10,090.89 vs 09:30 $9,974.88 (+116.01; session marks +116.01) · 10 name(s) marked open→close (per-name table). CAPR×165 09:30 $9.19 → close $10.06 +143.55; KURA×88 09:30 $12.98 → close $13.18 +17.60; CCOI×126 09:30 $9.83 → close $9.67 -20.16; LIFE×32 09:30 $39.40 → close $39.44 +1.28; ZIP×264 09:30 $4.30 → close $4.29 -2.64; BMEA×738 09:30 $1.74 → close $1.68 -44.28; NPWR×601 09:30 $1.83 → close $1.89 +36.06; PUSA×316 09:30 $3.86 → close $3.79 -22.12; SLQT×44 09:30 $0.53 → close $0.54 +0.44; USDE×4 09:30 $6.50 → close $8.07 +6.28 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.68 | ▼ 09:30 equity $10,010.12 vs yday $10,090.89 (-80.77) | 09:30 open · cash $28.68 (unchanged overnight, no fees) · equity $10,010.12 vs prior close $10,090.89 (-80.77) · 10 name(s) re-marked at the open (per-name table). CAPR×165 yday $10.06 → 09:30 $9.73 -54.45; KURA×88 yday $13.18 → 09:30 $13.05 -11.44; CCOI×126 yday $9.67 → 09:30 $9.70 +3.78; LIFE×32 yday $39.44 → 09:30 $39.60 +5.12; ZIP×264 yday $4.29 → 09:30 $4.21 -21.12; BMEA×738 yday $1.68 → 09:30 $1.69 +7.38; NPWR×601 yday $1.89 → 09:30 $1.89 +0.00; PUSA×316 yday $3.79 → 09:30 $3.77 -6.32; SLQT×44 yday $0.54 → 09:30 $0.53 -0.40; USDE×4 yday $8.07 → 09:30 $7.24 -3.32 | — |
| 2026-08-28 09:30 ET | **SELL** | `CAPR` | 165 | $9.73 | $2.53 | $+404.19 | $1,631.60 | ▲ +404.19 after sell → book $10,007.60; vs 09:30 mark -2.52 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 88 | $13.05 | $2.28 | $-52.05 | $2,777.72 | ▼ -52.05 after sell → book $10,005.32; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CCOI` | 126 | $9.70 | $2.40 | $+21.69 | $3,997.53 | ▲ +21.69 after sell → book $10,002.92; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `LIFE` | 32 | $39.60 | $2.11 | $+80.29 | $5,262.62 | ▲ +80.29 after sell → book $10,000.81; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZIP` | 264 | $4.21 | $3.46 | $-96.62 | $6,370.60 | ▼ -96.62 after sell → book $9,997.35; vs 09:30 mark -3.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 738 | $1.69 | $9.65 | $+25.11 | $7,608.17 | ▲ +25.11 after sell → book $9,987.70; vs 09:30 mark -9.65 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `NPWR` | 601 | $1.89 | $7.86 | $-81.73 | $8,736.19 | ▼ -81.73 after sell → book $9,979.84; vs 09:30 mark -7.86 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `PUSA` | 316 | $3.77 | $4.14 | $-17.70 | $9,923.38 | ▼ -17.70 after sell → book $9,975.70; vs 09:30 mark -4.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 60 | $32.90 | $2.17 | — | $7,947.21 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1984.68 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 24 | $79.42 | $2.06 | — | $6,039.06 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1984.68 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 13 | $146.07 | $2.03 | — | $4,138.13 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1984.68 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 14 | $141.76 | $2.03 | — | $2,151.45 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1984.68 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 85 | $23.30 | $2.25 | — | $168.71 | — | combo gate; gate vol=good,blue=True; list ohlc_hot; 🔵; ret5=+14.5; leftover $1984.68 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $168.71 | ▼ close $9,769.38 vs 09:30 $10,010.12 (session -195.78) | 16:00 close · cash $168.71 · equity $9,769.38 vs 09:30 $10,010.12 (-240.74; session marks -195.78) · 7 name(s) marked open→close (per-name table). SLQT×44 09:30 $0.53 → close $0.52 -0.48; USDE×4 09:30 $7.24 → close $6.76 -1.92; SEDG×60 09:30 $32.90 → close $31.41 -89.40; URBN×24 09:30 $79.42 → close $81.09 +40.08; ANF×13 09:30 $146.07 → close $148.42 +30.55; SMTC×14 09:30 $141.76 → close $131.17 -148.26; NCNO×85 09:30 $23.30 → close $22.99 -26.35 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $168.71 | ▼ 09:30 equity $9,720.44 vs yday $9,769.38 (-48.94) | 09:30 open · cash $168.71 (unchanged overnight, no fees) · equity $9,720.44 vs prior close $9,769.38 (-48.94) · 7 name(s) re-marked at the open (per-name table). SLQT×44 yday $0.52 → 09:30 $0.51 -0.44; USDE×4 yday $6.76 → 09:30 $6.76 +0.00; SEDG×60 yday $31.41 → 09:30 $31.15 -15.60; URBN×24 yday $81.09 → 09:30 $80.44 -15.60; ANF×13 yday $148.42 → 09:30 $148.03 -5.07; SMTC×14 yday $131.17 → 09:30 $132.30 +15.82; NCNO×85 yday $22.99 → 09:30 $22.66 -28.05 | — |
| 2026-08-31 09:30 ET | **SELL** | `SLQT` | 44 | $0.51 | $0.38 | $-3.98 | $190.77 | ▼ -3.98 after sell → book $9,720.06; vs 09:30 mark -0.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `USDE` | 4 | $6.76 | $0.30 | $+3.25 | $217.51 | ▲ +3.25 after sell → book $9,719.76; vs 09:30 mark -0.30 | dropped from list after 3 sess (min 3) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $217.51 | ▲ close $9,728.55 vs 09:30 $9,720.44 (session +8.79) | 16:00 close · cash $217.51 · equity $9,728.55 vs 09:30 $9,720.44 (+8.11; session marks +8.79) · 5 name(s) marked open→close (per-name table). SEDG×60 09:30 $31.15 → close $32.20 +63.00; URBN×24 09:30 $80.44 → close $80.69 +6.00; ANF×13 09:30 $148.03 → close $143.08 -64.35; SMTC×14 09:30 $132.30 → close $132.96 +9.24; NCNO×85 09:30 $22.66 → close $22.60 -5.10 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $217.51 | ▼ 09:30 equity $9,544.16 vs yday $9,728.55 (-184.39) | 09:30 open · cash $217.51 (unchanged overnight, no fees) · equity $9,544.16 vs prior close $9,728.55 (-184.39) · 5 name(s) re-marked at the open (per-name table). SEDG×60 yday $32.20 → 09:30 $31.87 -19.80; URBN×24 yday $80.69 → 09:30 $79.12 -37.68; ANF×13 yday $143.08 → 09:30 $142.00 -14.04; SMTC×14 yday $132.96 → 09:30 $127.63 -74.62; NCNO×85 yday $22.60 → 09:30 $22.15 -38.25 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $217.51 | ▲ close $9,645.99 vs 09:30 $9,544.16 (session +101.83) | 16:00 close · cash $217.51 · equity $9,645.99 vs 09:30 $9,544.16 (+101.83; session marks +101.83) · 5 name(s) marked open→close (per-name table). SEDG×60 09:30 $31.87 → close $32.49 +37.20; URBN×24 09:30 $79.12 → close $79.29 +4.08; ANF×13 09:30 $142.00 → close $140.68 -17.16; SMTC×14 09:30 $127.63 → close $132.27 +64.96; NCNO×85 09:30 $22.15 → close $22.30 +12.75 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $217.51 | ▼ 09:30 equity $9,619.32 vs yday $9,645.99 (-26.67) | 09:30 open · cash $217.51 (unchanged overnight, no fees) · equity $9,619.32 vs prior close $9,645.99 (-26.67) · 5 name(s) re-marked at the open (per-name table). SEDG×60 yday $32.49 → 09:30 $32.42 -4.20; URBN×24 yday $79.29 → 09:30 $78.84 -10.80; ANF×13 yday $140.68 → 09:30 $139.65 -13.39; SMTC×14 yday $132.27 → 09:30 $133.00 +10.22; NCNO×85 yday $22.30 → 09:30 $22.20 -8.50 | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 60 | $32.42 | $2.20 | $-33.17 | $2,160.51 | ▼ -33.17 after sell → book $9,617.12; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `URBN` | 24 | $78.84 | $2.09 | $-18.07 | $4,050.59 | ▼ -18.07 after sell → book $9,615.04; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 13 | $139.65 | $2.05 | $-87.54 | $5,863.98 | ▼ -87.54 after sell → book $9,612.98; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 14 | $133.00 | $2.06 | $-126.73 | $7,723.93 | ▼ -126.73 after sell → book $9,610.93; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `NCNO` | 85 | $22.20 | $2.27 | $-98.02 | $9,608.65 | ▼ -98.02 after sell → book $9,608.65; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,608.65 | ▲ close $9,608.65 vs 09:30 $9,619.32 (session +0.00) | 16:00 close · cash $9,608.65 · no lots left · equity $9,608.65. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,608.65 | ▲ 09:30 equity $9,608.65 vs yday $9,608.65 (+0.00) | 09:30 open · cash $9,608.65 · no holdings · equity $9,608.65 vs prior close $9,608.65 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $8,414.58 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1201.08 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 77 | $15.45 | $2.22 | — | $7,222.71 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1201.08 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $145.94 | $2.01 | — | $6,053.14 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1201.08 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 71 | $16.77 | $2.20 | — | $4,860.27 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1201.08 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 550 | $2.18 | $7.09 | — | $3,654.17 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1201.08 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 50 | $23.88 | $2.14 | — | $2,458.03 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1201.08 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DEFT` | 1847 | $0.65 | $17.55 | — | $1,239.94 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+10.2; leftover $1201.08 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CTMX` | 322 | $3.73 | $4.15 | — | $34.72 | — | combo gate; gate vol=good,blue=True; list yday_gainer; 🔵; ret5=+12.0; leftover $1201.08 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.72 | ▼ close $9,476.33 vs 09:30 $9,608.65 (session -92.93) | 16:00 close · cash $34.72 · equity $9,476.33 vs 09:30 $9,608.65 (-132.32; session marks -92.93) · 8 name(s) marked open→close (per-name table). RVTY×9 09:30 $132.45 → close $130.63 -16.38; CRK×77 09:30 $15.45 → close $14.95 -38.50; MRNA×8 09:30 $145.94 → close $148.87 +23.40; ARCT×71 09:30 $16.77 → close $15.56 -85.91; CRDL×550 09:30 $2.18 → close $2.16 -11.00; MMED×50 09:30 $23.88 → close $23.84 -2.00; DEFT×1847 09:30 $0.65 → close $0.68 +53.56; CTMX×322 09:30 $3.73 → close $3.68 -16.10 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.72 | ▲ 09:30 equity $9,523.77 vs yday $9,476.33 (+47.44) | 09:30 open · cash $34.72 (unchanged overnight, no fees) · equity $9,523.77 vs prior close $9,476.33 (+47.44) · 8 name(s) re-marked at the open (per-name table). RVTY×9 yday $130.63 → 09:30 $130.03 -5.40; CRK×77 yday $14.95 → 09:30 $15.00 +3.85; MRNA×8 yday $148.87 → 09:30 $153.62 +38.00; ARCT×71 yday $15.56 → 09:30 $15.61 +3.55; CRDL×550 yday $2.16 → 09:30 $2.16 +0.00; MMED×50 yday $23.84 → 09:30 $23.84 +0.00; DEFT×1847 yday $0.68 → 09:30 $0.69 +20.32; CTMX×322 yday $3.68 → 09:30 $3.64 -12.88 | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 1 | $3.46 | $0.04 | — | $31.22 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $4.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 1 | $2.52 | $0.03 | — | $28.68 | — | combo gate; gate vol=good,blue=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $4.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 2 | $1.90 | $0.04 | — | $24.83 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $4.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 1 | $3.52 | $0.04 | — | $21.27 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.3; leftover $4.34 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.27 | ▼ close $9,500.38 vs 09:30 $9,523.77 (session -23.24) | 16:00 close · cash $21.27 · equity $9,500.38 vs 09:30 $9,523.77 (-23.39; session marks -23.24) · 12 name(s) marked open→close (per-name table). RVTY×9 09:30 $130.03 → close $130.22 +1.71; CRK×77 09:30 $15.00 → close $15.26 +20.02; MRNA×8 09:30 $153.62 → close $145.55 -64.56; ARCT×71 09:30 $15.61 → close $15.82 +14.91; CRDL×550 09:30 $2.16 → close $2.20 +22.00; MMED×50 09:30 $23.84 → close $23.29 -27.50; DEFT×1847 09:30 $0.69 → close $0.68 -12.93; CTMX×322 09:30 $3.64 → close $3.71 +22.54; CABA×1 09:30 $3.46 → close $3.47 +0.01; ALEC×1 09:30 $2.52 → close $2.46 -0.06; BMEA×2 09:30 $1.90 → close $2.03 +0.26; EOSE×1 09:30 $3.52 → close $3.88 +0.36 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.27 | ▼ 09:30 equity $9,364.43 vs yday $9,500.38 (-135.95) | 09:30 open · cash $21.27 (unchanged overnight, no fees) · equity $9,364.43 vs prior close $9,500.38 (-135.95) · 12 name(s) re-marked at the open (per-name table). RVTY×9 yday $130.22 → 09:30 $128.50 -15.48; CRK×77 yday $15.26 → 09:30 $15.50 +18.48; MRNA×8 yday $145.55 → 09:30 $145.98 +3.44; ARCT×71 yday $15.82 → 09:30 $15.47 -24.85; CRDL×550 yday $2.20 → 09:30 $2.20 +0.00; MMED×50 yday $23.29 → 09:30 $23.16 -6.50; DEFT×1847 yday $0.68 → 09:30 $0.64 -79.42; CTMX×322 yday $3.71 → 09:30 $3.61 -31.56; CABA×1 yday $3.47 → 09:30 $3.43 -0.04; ALEC×1 yday $2.46 → 09:30 $2.38 -0.08; BMEA×2 yday $2.03 → 09:30 $2.00 -0.06; EOSE×1 yday $3.88 → 09:30 $3.99 +0.11 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.27 | ▼ close $9,287.87 vs 09:30 $9,364.43 (session -76.55) | 16:00 close · cash $21.27 · equity $9,287.87 vs 09:30 $9,364.43 (-76.56; session marks -76.55) · 12 name(s) marked open→close (per-name table). RVTY×9 09:30 $128.50 → close $127.08 -12.78; CRK×77 09:30 $15.50 → close $15.16 -26.18; MRNA×8 09:30 $145.98 → close $140.33 -45.20; ARCT×71 09:30 $15.47 → close $15.63 +11.36; CRDL×550 09:30 $2.20 → close $2.22 +11.00; MMED×50 09:30 $23.16 → close $23.32 +8.00; DEFT×1847 09:30 $0.64 → close $0.62 -35.09; CTMX×322 09:30 $3.61 → close $3.65 +12.24; CABA×1 09:30 $3.43 → close $3.27 -0.16; ALEC×1 09:30 $2.38 → close $2.47 +0.09; BMEA×2 09:30 $2.00 → close $1.93 -0.14; EOSE×1 09:30 $3.99 → close $4.30 +0.31 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.27 | ▼ 09:30 equity $9,271.10 vs yday $9,287.87 (-16.77) | 09:30 open · cash $21.27 (unchanged overnight, no fees) · equity $9,271.10 vs prior close $9,287.87 (-16.77) · 12 name(s) re-marked at the open (per-name table). RVTY×9 yday $127.08 → 09:30 $125.77 -11.79; CRK×77 yday $15.16 → 09:30 $15.16 +0.00; MRNA×8 yday $140.33 → 09:30 $140.29 -0.28; ARCT×71 yday $15.63 → 09:30 $15.46 -12.07; CRDL×550 yday $2.22 → 09:30 $2.22 +0.00; MMED×50 yday $23.32 → 09:30 $23.22 -5.00; DEFT×1847 yday $0.62 → 09:30 $0.63 +9.24; CTMX×322 yday $3.65 → 09:30 $3.66 +3.22; CABA×1 yday $3.27 → 09:30 $3.28 +0.01; ALEC×1 yday $2.47 → 09:30 $2.47 +0.00; BMEA×2 yday $1.93 → 09:30 $1.94 +0.02; EOSE×1 yday $4.30 → 09:30 $4.18 -0.12 | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 9 | $125.77 | $2.04 | $-64.17 | $1,151.17 | ▼ -64.17 after sell → book $9,269.06; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRK` | 77 | $15.16 | $2.24 | $-26.79 | $2,316.24 | ▼ -26.79 after sell → book $9,266.81; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRNA` | 8 | $140.29 | $2.03 | $-49.25 | $3,436.57 | ▼ -49.25 after sell → book $9,264.78; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 71 | $15.46 | $2.22 | $-97.44 | $4,532.00 | ▼ -97.44 after sell → book $9,262.56; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRDL` | 550 | $2.22 | $7.20 | $+7.71 | $5,745.81 | ▲ +7.71 after sell → book $9,255.36; vs 09:30 mark -7.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MMED` | 50 | $23.22 | $2.16 | $-37.30 | $6,904.65 | ▼ -37.30 after sell → book $9,253.20; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DEFT` | 1847 | $0.63 | $17.42 | $-79.29 | $8,043.45 | ▼ -79.29 after sell → book $9,235.78; vs 09:30 mark -17.42 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CTMX` | 322 | $3.66 | $4.22 | $-30.91 | $9,217.75 | ▼ -30.91 after sell → book $9,231.56; vs 09:30 mark -4.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,217.75 | ▼ close $9,230.77 vs 09:30 $9,271.10 (session -0.79) | 16:00 close · cash $9,217.75 · equity $9,230.77 vs 09:30 $9,271.10 (-40.33; session marks -0.79) · 4 name(s) marked open→close (per-name table). CABA×1 09:30 $3.28 → close $2.91 -0.37; ALEC×1 09:30 $2.47 → close $2.27 -0.20; BMEA×2 09:30 $1.94 → close $1.84 -0.19; EOSE×1 09:30 $4.18 → close $4.15 -0.03 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BETR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ADUR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AIRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `NCMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `TMC` | cash | leftover split 1.28 < 1 share @ 4.05 |
| 2026-08-17 | `ABX` | cash | leftover split 1.28 < 1 share @ 9.12 |
| 2026-08-17 | `ALOY` | cash | leftover split 1.28 < 1 share @ 14.66 |
| 2026-08-17 | `NU` | cash | leftover split 1.28 < 1 share @ 15.40 |
| 2026-08-17 | `INV` | cash | leftover split 1.28 < 1 share @ 1.62 |
| 2026-08-17 | `KLC` | cash | leftover split 1.28 < 1 share @ 2.62 |
| 2026-08-17 | `ENHA` | cash | leftover split 1.28 < 1 share @ 2.01 |
| 2026-08-17 | `MP` | cash | leftover split 1.28 < 1 share @ 58.01 |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BETR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ADUR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AIRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NCMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `UGI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NMAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 15.58 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 15.58 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 15.58 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 15.58 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SBSW` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `KURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CCOI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `LIFE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZIP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `PUSA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `DKS` | cash | leftover split 26.07 < 1 share @ 121.87 |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `KURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CCOI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `LIFE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZIP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `PUSA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SLQT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `USDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `SLQT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `USDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `URBN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `NCNO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SRPT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `URBN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NCNO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `YDDL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CTMX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `BHC` | cash | leftover split 4.34 < 1 share @ 6.71 |
| 2026-09-04 | `OABI` | cash | leftover split 4.34 < 1 share @ 4.78 |
| 2026-09-04 | `VIR` | cash | leftover split 4.34 < 1 share @ 11.31 |
| 2026-09-04 | `DELL` | cash | leftover split 4.34 < 1 share @ 513.78 |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MMED` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CTMX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `EOSE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `EOSE` | min_hold | dropped but min-hold 2/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CABA` | 1 | 2026-09-04 @ $3.46 | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $4.34 |
| `ALEC` | 1 | 2026-09-04 @ $2.52 | combo gate; gate vol=good,blue=True; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $4.34 |
| `BMEA` | 2 | 2026-09-04 @ $1.90 | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $4.34 |
| `EOSE` | 1 | 2026-09-04 @ $3.52 | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.3; leftover $4.34 |
