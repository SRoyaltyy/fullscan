# Factor mine action — `flatten_white_yday_h5`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

Side **long** · universe `flatten` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · flatten looker: 0 red + yesterday up

Cash book **-2.75%** ($9,725) · signal-only (no cash/fees) was +0.36%. Starts YES **1/26**. Fills 52 · skips 102 · realized $-62.52.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the flatten wish-list (names the flatten board wanted that morning) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 5 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the flatten wish-list (names the flatten board wanted that morning).
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

- At 09:30, take names on the flatten wish-list (names the flatten board wanted that morning) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Sort the keepers by how many morning cameras are green vs red and keep the top 8.
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- This is a LONG sleeve: it buys shares and wants the price to go up.

### When it sells

- Sell first, then buy. Never sell a ticker we do not hold.
- Minimum hold is 5 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 5 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `flatten` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `zero_red=True,yday_up=True` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $267.92.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `BTSG` | 41 | — | $59.80 | +0.00 | $60.23 | +17.63 | +17.63 | +0.00 | +17.63 |
| 2026-08-13 | `IREN` | 54 | — | $45.98 | +0.00 | $44.76 | -65.88 | -65.88 | +0.00 | -65.88 |
| 2026-08-13 | `SLS` | 213 | — | $11.70 | +0.00 | $12.36 | +140.58 | +140.58 | +0.00 | +140.58 |
| 2026-08-13 | `TPG` | 49 | — | $50.62 | +0.00 | $54.62 | +195.84 | +195.84 | +0.00 | +195.84 |
| 2026-08-14 | `BTSG` | 41 | $60.23 | $59.65 | -23.78 | $61.71 | +84.46 | +60.68 | -6.15 | +78.31 |
| 2026-08-14 | `IREN` | 54 | $44.76 | $44.09 | -36.18 | $44.06 | -1.62 | -37.80 | -102.06 | -103.68 |
| 2026-08-14 | `SLS` | 213 | $12.36 | $12.40 | +8.52 | $12.78 | +80.94 | +89.46 | +149.10 | +230.04 |
| 2026-08-14 | `TPG` | 49 | $54.62 | $55.29 | +32.83 | $53.03 | -110.74 | -77.91 | +228.67 | +117.93 |
| 2026-08-14 | `BETR` | 1 | — | $14.80 | +0.00 | $13.73 | -1.07 | -1.07 | +0.00 | -1.07 |
| 2026-08-14 | `BTBT` | 11 | — | $1.50 | +0.00 | $1.57 | +0.77 | +0.77 | +0.00 | +0.77 |
| 2026-08-14 | `LDI` | 17 | — | $0.94 | +0.00 | $0.90 | -0.68 | -0.68 | +0.00 | -0.68 |
| 2026-08-17 | `BTSG` | 41 | $61.71 | $61.69 | -0.82 | $60.38 | -53.71 | -54.53 | +77.49 | +23.78 |
| 2026-08-17 | `IREN` | 54 | $44.06 | $45.23 | +63.18 | $44.90 | -17.82 | +45.36 | -40.50 | -58.32 |
| 2026-08-17 | `SLS` | 213 | $12.78 | $12.78 | +0.00 | $13.00 | +46.86 | +46.86 | +230.04 | +276.90 |
| 2026-08-17 | `TPG` | 49 | $53.03 | $52.67 | -17.64 | $51.77 | -44.10 | -61.74 | +100.29 | +56.19 |
| 2026-08-17 | `BETR` | 1 | $13.73 | $13.67 | -0.06 | $13.54 | -0.13 | -0.19 | -1.13 | -1.26 |
| 2026-08-17 | `BTBT` | 11 | $1.57 | $1.52 | -0.55 | $1.60 | +0.88 | +0.33 | +0.22 | +1.10 |
| 2026-08-17 | `LDI` | 17 | $0.90 | $0.91 | +0.17 | $0.88 | -0.54 | -0.37 | -0.51 | -1.05 |
| 2026-08-18 | `BTSG` | 41 | $60.38 | $60.00 | -15.58 | $59.50 | -20.50 | -36.08 | +8.20 | -12.30 |
| 2026-08-18 | `IREN` | 54 | $44.90 | $43.56 | -72.36 | $42.00 | -84.24 | -156.60 | -130.68 | -214.92 |
| 2026-08-18 | `SLS` | 213 | $13.00 | $12.66 | -72.42 | $13.10 | +93.72 | +21.30 | +204.48 | +298.20 |
| 2026-08-18 | `TPG` | 49 | $51.77 | $51.77 | +0.00 | $52.02 | +12.25 | +12.25 | +56.19 | +68.44 |
| 2026-08-18 | `BETR` | 1 | $13.54 | $13.21 | -0.33 | $13.05 | -0.16 | -0.49 | -1.59 | -1.75 |
| 2026-08-18 | `BTBT` | 11 | $1.60 | $1.54 | -0.66 | $1.45 | -0.99 | -1.65 | +0.44 | -0.55 |
| 2026-08-18 | `LDI` | 17 | $0.88 | $0.87 | -0.09 | $0.86 | -0.20 | -0.29 | -1.14 | -1.34 |
| 2026-08-19 | `BTSG` | 41 | $59.50 | $60.15 | +26.65 | $59.33 | -33.62 | -6.97 | +14.35 | -19.27 |
| 2026-08-19 | `IREN` | 54 | $42.00 | $41.41 | -31.59 | $42.84 | +76.95 | +45.36 | -246.51 | -169.56 |
| 2026-08-19 | `SLS` | 213 | $13.10 | $13.46 | +76.68 | $13.85 | +83.07 | +159.75 | +374.88 | +457.95 |
| 2026-08-19 | `TPG` | 49 | $52.02 | $52.26 | +11.76 | $53.18 | +45.08 | +56.84 | +80.20 | +125.28 |
| 2026-08-19 | `BETR` | 1 | $13.05 | $13.03 | -0.02 | $13.03 | +0.00 | -0.02 | -1.77 | -1.77 |
| 2026-08-19 | `BTBT` | 11 | $1.45 | $1.42 | -0.33 | $1.40 | -0.22 | -0.55 | -0.88 | -1.10 |
| 2026-08-19 | `LDI` | 17 | $0.86 | $0.88 | +0.37 | $0.88 | -0.07 | +0.30 | -0.97 | -1.04 |
| 2026-08-20 | `BTSG` | 41 | $59.33 | $58.64 | -28.29 | — | +0.00 | -28.29 | -47.56 | — |
| 2026-08-20 | `IREN` | 54 | $42.84 | $42.46 | -20.52 | — | +0.00 | -20.52 | -190.08 | — |
| 2026-08-20 | `SLS` | 213 | $13.85 | $13.84 | -2.13 | — | +0.00 | -2.13 | +455.82 | — |
| 2026-08-20 | `TPG` | 49 | $53.18 | $53.06 | -5.88 | — | +0.00 | -5.88 | +119.40 | — |
| 2026-08-20 | `BETR` | 1 | $13.03 | $12.95 | -0.08 | $11.60 | -1.35 | -1.43 | -1.85 | -3.20 |
| 2026-08-20 | `BTBT` | 11 | $1.40 | $1.46 | +0.61 | $1.59 | +1.54 | +2.15 | -0.49 | +1.04 |
| 2026-08-20 | `LDI` | 17 | $0.88 | $0.87 | -0.09 | $0.87 | -0.02 | -0.11 | -1.12 | -1.14 |
| 2026-08-20 | `AG` | 62 | — | $20.55 | +0.00 | $21.19 | +39.68 | +39.68 | +0.00 | +39.68 |
| 2026-08-20 | `BHP` | 14 | — | $91.01 | +0.00 | $93.63 | +36.68 | +36.68 | +0.00 | +36.68 |
| 2026-08-20 | `CDE` | 62 | — | $20.65 | +0.00 | $21.11 | +28.52 | +28.52 | +0.00 | +28.52 |
| 2026-08-20 | `HDSN` | 222 | — | $5.77 | +0.00 | $5.57 | -44.40 | -44.40 | +0.00 | -44.40 |
| 2026-08-20 | `IAG` | 65 | — | $19.63 | +0.00 | $20.50 | +56.55 | +56.55 | +0.00 | +56.55 |
| 2026-08-20 | `KGC` | 43 | — | $29.63 | +0.00 | $31.43 | +77.40 | +77.40 | +0.00 | +77.40 |
| 2026-08-20 | `NFGC` | 733 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-21 | `BETR` | 1 | $11.60 | $11.73 | +0.13 | — | +0.00 | +0.13 | -3.07 | — |
| 2026-08-21 | `BTBT` | 11 | $1.59 | $1.66 | +0.71 | — | +0.00 | +0.71 | +1.76 | — |
| 2026-08-21 | `LDI` | 17 | $0.87 | $0.87 | -0.05 | — | +0.00 | -0.05 | -1.19 | — |
| 2026-08-21 | `AG` | 62 | $21.19 | $21.90 | +44.02 | $21.09 | -50.22 | -6.20 | +83.70 | +33.48 |
| 2026-08-21 | `BHP` | 14 | $93.63 | $95.72 | +29.26 | $97.03 | +18.34 | +47.60 | +65.94 | +84.28 |
| 2026-08-21 | `CDE` | 62 | $21.11 | $21.75 | +39.68 | $20.97 | -48.36 | -8.68 | +68.20 | +19.84 |
| 2026-08-21 | `HDSN` | 222 | $5.57 | $5.67 | +22.20 | $5.63 | -8.88 | +13.32 | -22.20 | -31.08 |
| 2026-08-21 | `IAG` | 65 | $20.50 | $21.17 | +43.55 | $21.14 | -1.95 | +41.60 | +100.10 | +98.15 |
| 2026-08-21 | `KGC` | 43 | $31.43 | $32.17 | +31.82 | $32.76 | +25.37 | +57.19 | +109.22 | +134.59 |
| 2026-08-21 | `NFGC` | 733 | $1.75 | $1.79 | +29.32 | $1.84 | +36.65 | +65.97 | +29.32 | +65.97 |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | $157.78 | +24.64 | +60.24 | +81.28 | +105.92 |
| 2026-08-21 | `AUPH` | 1 | — | $17.20 | +0.00 | $16.65 | -0.55 | -0.55 | +0.00 | -0.55 |
| 2026-08-21 | `ARCT` | 2 | — | $11.13 | +0.00 | $13.45 | +4.64 | +4.64 | +0.00 | +4.64 |
| 2026-08-21 | `CYPH` | 24 | — | $1.32 | +0.00 | $1.42 | +2.40 | +2.40 | +0.00 | +2.40 |
| 2026-08-24 | `AG` | 62 | $21.09 | $21.30 | +13.02 | $20.83 | -29.14 | -16.12 | +46.50 | +17.36 |
| 2026-08-24 | `BHP` | 14 | $97.03 | $97.31 | +3.92 | $97.13 | -2.52 | +1.40 | +88.20 | +85.68 |
| 2026-08-24 | `CDE` | 62 | $20.97 | $21.26 | +17.98 | $20.88 | -23.56 | -5.58 | +37.82 | +14.26 |
| 2026-08-24 | `HDSN` | 222 | $5.63 | $5.69 | +13.32 | $5.52 | -37.74 | -24.42 | -17.76 | -55.50 |
| 2026-08-24 | `IAG` | 65 | $21.14 | $21.38 | +15.60 | $21.80 | +27.30 | +42.90 | +113.75 | +141.05 |
| 2026-08-24 | `KGC` | 43 | $32.76 | $33.03 | +11.61 | $32.98 | -2.15 | +9.46 | +146.20 | +144.05 |
| 2026-08-24 | `NFGC` | 733 | $1.84 | $1.86 | +14.66 | $1.90 | +29.32 | +43.98 | +80.63 | +109.95 |
| 2026-08-24 | `WPM` | 8 | $157.78 | $159.50 | +13.76 | $160.19 | +5.52 | +19.28 | +119.68 | +125.20 |
| 2026-08-24 | `AUPH` | 1 | $16.65 | $16.57 | -0.08 | $16.57 | +0.00 | -0.08 | -0.63 | -0.63 |
| 2026-08-24 | `ARCT` | 2 | $13.45 | $13.33 | -0.24 | $14.34 | +2.02 | +1.78 | +4.40 | +6.42 |
| 2026-08-24 | `CYPH` | 24 | $1.42 | $1.83 | +9.84 | $1.68 | -3.60 | +6.24 | +12.24 | +8.64 |
| 2026-08-25 | `AG` | 62 | $20.83 | $20.32 | -31.62 | $21.24 | +57.04 | +25.42 | -14.26 | +42.78 |
| 2026-08-25 | `BHP` | 14 | $97.13 | $95.86 | -17.78 | $98.69 | +39.62 | +21.84 | +67.90 | +107.52 |
| 2026-08-25 | `CDE` | 62 | $20.88 | $20.47 | -25.42 | $21.59 | +69.44 | +44.02 | -11.16 | +58.28 |
| 2026-08-25 | `HDSN` | 222 | $5.52 | $5.53 | +2.22 | $5.47 | -13.32 | -11.10 | -53.28 | -66.60 |
| 2026-08-25 | `IAG` | 65 | $21.80 | $21.21 | -38.35 | $22.17 | +62.40 | +24.05 | +102.70 | +165.10 |
| 2026-08-25 | `KGC` | 43 | $32.98 | $32.32 | -28.38 | $33.48 | +49.88 | +21.50 | +115.67 | +165.55 |
| 2026-08-25 | `NFGC` | 733 | $1.90 | $1.90 | +0.00 | $2.04 | +102.62 | +102.62 | +109.95 | +212.57 |
| 2026-08-25 | `WPM` | 8 | $160.19 | $156.51 | -29.44 | $163.72 | +57.68 | +28.24 | +95.76 | +153.44 |
| 2026-08-25 | `AUPH` | 1 | $16.57 | $16.63 | +0.06 | $16.75 | +0.12 | +0.18 | -0.57 | -0.45 |
| 2026-08-25 | `ARCT` | 2 | $14.34 | $14.12 | -0.44 | $15.44 | +2.64 | +2.20 | +5.98 | +8.62 |
| 2026-08-25 | `CYPH` | 24 | $1.68 | $1.56 | -2.88 | $1.64 | +1.92 | -0.96 | +5.76 | +7.68 |
| 2026-08-25 | `CRMD` | 14 | — | $8.35 | +0.00 | $8.56 | +2.94 | +2.94 | +0.00 | +2.94 |
| 2026-08-26 | `AG` | 62 | $21.24 | $20.63 | -37.82 | $20.99 | +22.32 | -15.50 | +4.96 | +27.28 |
| 2026-08-26 | `BHP` | 14 | $98.69 | $96.99 | -23.80 | $96.33 | -9.24 | -33.04 | +83.72 | +74.48 |
| 2026-08-26 | `CDE` | 62 | $21.59 | $21.00 | -36.58 | $21.44 | +27.28 | -9.30 | +21.70 | +48.98 |
| 2026-08-26 | `HDSN` | 222 | $5.47 | $5.51 | +8.88 | $5.29 | -48.84 | -39.96 | -57.72 | -106.56 |
| 2026-08-26 | `IAG` | 65 | $22.17 | $21.64 | -34.45 | $21.54 | -6.50 | -40.95 | +130.65 | +124.15 |
| 2026-08-26 | `KGC` | 43 | $33.48 | $32.90 | -24.94 | $32.32 | -24.94 | -49.88 | +140.61 | +115.67 |
| 2026-08-26 | `NFGC` | 733 | $2.04 | $2.00 | -29.32 | $1.90 | -73.30 | -102.62 | +183.25 | +109.95 |
| 2026-08-26 | `WPM` | 8 | $163.72 | $160.93 | -22.32 | $156.02 | -39.28 | -61.60 | +131.12 | +91.84 |
| 2026-08-26 | `AUPH` | 1 | $16.75 | $16.60 | -0.15 | $16.54 | -0.06 | -0.21 | -0.60 | -0.66 |
| 2026-08-26 | `ARCT` | 2 | $15.44 | $15.35 | -0.18 | $15.83 | +0.96 | +0.78 | +8.44 | +9.40 |
| 2026-08-26 | `CYPH` | 24 | $1.64 | $1.60 | -0.96 | $1.63 | +0.72 | -0.24 | +6.72 | +7.44 |
| 2026-08-26 | `CRMD` | 14 | $8.56 | $8.60 | +0.56 | $8.39 | -2.94 | -2.38 | +3.50 | +0.56 |
| 2026-08-27 | `AG` | 62 | $20.99 | $20.93 | -3.72 | — | +0.00 | -3.72 | +23.56 | — |
| 2026-08-27 | `BHP` | 14 | $96.33 | $95.52 | -11.34 | — | +0.00 | -11.34 | +63.14 | — |
| 2026-08-27 | `CDE` | 62 | $21.44 | $21.31 | -8.06 | — | +0.00 | -8.06 | +40.92 | — |
| 2026-08-27 | `HDSN` | 222 | $5.29 | $5.49 | +44.40 | — | +0.00 | +44.40 | -62.16 | — |
| 2026-08-27 | `IAG` | 65 | $21.54 | $21.47 | -4.55 | — | +0.00 | -4.55 | +119.60 | — |
| 2026-08-27 | `KGC` | 43 | $32.32 | $32.32 | +0.00 | — | +0.00 | +0.00 | +115.67 | — |
| 2026-08-27 | `NFGC` | 733 | $1.90 | $1.91 | +7.33 | — | +0.00 | +7.33 | +117.28 | — |
| 2026-08-27 | `WPM` | 8 | $156.02 | $155.89 | -1.04 | — | +0.00 | -1.04 | +90.80 | — |
| 2026-08-27 | `AUPH` | 1 | $16.54 | $16.47 | -0.07 | $16.48 | +0.01 | -0.06 | -0.73 | -0.72 |
| 2026-08-27 | `ARCT` | 2 | $15.83 | $15.74 | -0.18 | $16.17 | +0.86 | +0.68 | +9.22 | +10.08 |
| 2026-08-27 | `CYPH` | 24 | $1.63 | $1.75 | +2.88 | $1.89 | +3.36 | +6.24 | +10.32 | +13.68 |
| 2026-08-27 | `CRMD` | 14 | $8.39 | $8.49 | +1.40 | $8.31 | -2.52 | -1.12 | +1.96 | -0.56 |
| 2026-08-28 | `AUPH` | 1 | $16.48 | $16.44 | -0.04 | — | +0.00 | -0.04 | -0.76 | — |
| 2026-08-28 | `ARCT` | 2 | $16.17 | $15.43 | -1.48 | — | +0.00 | -1.48 | +8.60 | — |
| 2026-08-28 | `CYPH` | 24 | $1.89 | $1.82 | -1.68 | — | +0.00 | -1.68 | +12.00 | — |
| 2026-08-28 | `CRMD` | 14 | $8.31 | $8.28 | -0.42 | $8.30 | +0.28 | -0.14 | -0.98 | -0.70 |
| 2026-08-31 | `CRMD` | 14 | $8.30 | $8.26 | -0.56 | $8.26 | +0.00 | -0.56 | -1.26 | -1.26 |
| 2026-09-01 | `CRMD` | 14 | $8.26 | $8.25 | -0.14 | — | +0.00 | -0.14 | -1.40 | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `HRMY` | 50 | — | $42.93 | +0.00 | $41.86 | -53.50 | -53.50 | +0.00 | -53.50 |
| 2026-09-03 | `RVTY` | 16 | — | $132.45 | +0.00 | $130.63 | -29.12 | -29.12 | +0.00 | -29.12 |
| 2026-09-03 | `VSTM` | 268 | — | $8.03 | +0.00 | $7.98 | -13.40 | -13.40 | +0.00 | -13.40 |
| 2026-09-03 | `ATRC` | 40 | — | $52.88 | +0.00 | $52.46 | -16.80 | -16.80 | +0.00 | -16.80 |
| 2026-09-03 | `CABA` | 594 | — | $3.63 | +0.00 | $3.48 | -89.10 | -89.10 | +0.00 | -89.10 |
| 2026-09-04 | `HRMY` | 50 | $41.86 | $41.50 | -18.00 | $42.25 | +37.50 | +19.50 | -71.50 | -34.00 |
| 2026-09-04 | `RVTY` | 16 | $130.63 | $130.03 | -9.60 | $130.22 | +3.04 | -6.56 | -38.72 | -35.68 |
| 2026-09-04 | `VSTM` | 268 | $7.98 | $7.91 | -18.76 | $8.20 | +77.72 | +58.96 | -32.16 | +45.56 |
| 2026-09-04 | `ATRC` | 40 | $52.46 | $52.03 | -17.20 | $51.52 | -20.40 | -37.60 | -34.00 | -54.40 |
| 2026-09-04 | `CABA` | 594 | $3.48 | $3.46 | -11.88 | $3.47 | +5.94 | -5.94 | -100.98 | -95.04 |
| 2026-09-08 | `HRMY` | 50 | $42.25 | $42.20 | -2.50 | $42.07 | -6.50 | -9.00 | -36.50 | -43.00 |
| 2026-09-08 | `RVTY` | 16 | $130.22 | $128.50 | -27.52 | $127.08 | -22.72 | -50.24 | -63.20 | -85.92 |
| 2026-09-08 | `VSTM` | 268 | $8.20 | $8.20 | +0.00 | $8.08 | -32.16 | -32.16 | +45.56 | +13.40 |
| 2026-09-08 | `ATRC` | 40 | $51.52 | $54.31 | +111.60 | $53.73 | -23.20 | +88.40 | +57.20 | +34.00 |
| 2026-09-08 | `CABA` | 594 | $3.47 | $3.43 | -23.76 | $3.27 | -95.04 | -118.80 | -118.80 | -213.84 |
| 2026-09-09 | `HRMY` | 50 | $42.07 | $42.01 | -3.00 | $41.62 | -19.50 | -22.50 | -46.00 | -65.50 |
| 2026-09-09 | `RVTY` | 16 | $127.08 | $125.77 | -20.96 | $123.85 | -30.72 | -51.68 | -106.88 | -137.60 |
| 2026-09-09 | `VSTM` | 268 | $8.08 | $8.01 | -18.76 | $7.94 | -18.76 | -37.52 | -5.36 | -24.12 |
| 2026-09-09 | `ATRC` | 40 | $53.73 | $53.16 | -22.80 | $53.03 | -5.20 | -28.00 | +11.20 | +6.00 |
| 2026-09-09 | `CABA` | 594 | $3.27 | $3.28 | +5.94 | $2.91 | -219.78 | -213.84 | -207.90 | -427.68 |
| 2026-09-10 | `HRMY` | 50 | $41.62 | $41.26 | -18.00 | $41.10 | -8.00 | -26.00 | -83.50 | -91.50 |
| 2026-09-10 | `RVTY` | 16 | $123.85 | $122.77 | -17.28 | $120.94 | -29.28 | -46.56 | -154.88 | -184.16 |
| 2026-09-10 | `VSTM` | 268 | $7.94 | $7.91 | -8.04 | $7.62 | -77.72 | -85.76 | -32.16 | -109.88 |
| 2026-09-10 | `ATRC` | 40 | $53.03 | $52.31 | -28.80 | $52.96 | +26.00 | -2.80 | -22.80 | +3.20 |
| 2026-09-10 | `CABA` | 594 | $2.91 | $2.85 | -35.64 | $2.74 | -65.34 | -100.98 | -463.32 | -528.66 |
| 2026-09-11 | `HRMY` | 50 | $41.10 | $41.30 | +10.00 | — | +0.00 | +10.00 | -81.50 | — |
| 2026-09-11 | `RVTY` | 16 | $120.94 | $122.40 | +23.36 | — | +0.00 | +23.36 | -160.80 | — |
| 2026-09-11 | `VSTM` | 268 | $7.62 | $7.70 | +21.44 | — | +0.00 | +21.44 | -88.44 | — |
| 2026-09-11 | `ATRC` | 40 | $52.96 | $53.53 | +22.80 | — | +0.00 | +22.80 | +26.00 | — |
| 2026-09-11 | `CABA` | 594 | $2.74 | $2.77 | +17.82 | — | +0.00 | +17.82 | -510.84 | — |
| 2026-09-14 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-17 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-18 | `ILMN` | 9 | — | $249.13 | +0.00 | $239.62 | -85.59 | -85.59 | +0.00 | -85.59 |
| 2026-09-18 | `ARQT` | 95 | — | $26.14 | +0.00 | $25.38 | -72.20 | -72.20 | +0.00 | -72.20 |
| 2026-09-18 | `FTRE` | 123 | — | $20.10 | +0.00 | $19.93 | -20.91 | -20.91 | +0.00 | -20.91 |
| 2026-09-18 | `SDGR` | 84 | — | $29.32 | +0.00 | $29.02 | -25.20 | -25.20 | +0.00 | -25.20 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +288.17 | BTSG, IREN, SLS, TPG | — | $83.49 | $10,279.02 | BTSG×41, IREN×54, SLS×213, TPG×49 |
| 2026-08-14 | +5.50 | $83.49 | BTSG×41, IREN×54, SLS×213, TPG×49 | $10,260.41 | -18.61 | +52.06 | BETR, BTBT, LDI | — | $35.71 | $10,311.91 | BTSG×41, IREN×54, SLS×213, TPG×49, BETR×1, BTBT×11, LDI×17 |
| 2026-08-17 | +2.25 | $35.71 | BTSG×41, IREN×54, SLS×213, TPG×49, BETR×1, BTBT×11, LDI×17 | $10,356.19 | +44.28 | -68.56 | — | — | $35.71 | $10,287.63 | BTSG×41, IREN×54, SLS×213, TPG×49, BETR×1, BTBT×11, LDI×17 |
| 2026-08-18 | -6.20 | $35.71 | BTSG×41, IREN×54, SLS×213, TPG×49, BETR×1, BTBT×11, LDI×17 | $10,126.20 | -161.43 | -0.12 | — | — | $35.71 | $10,126.07 | BTSG×41, IREN×54, SLS×213, TPG×49, BETR×1, BTBT×11, LDI×17 |
| 2026-08-19 | -7.20 | $35.71 | BTSG×41, IREN×54, SLS×213, TPG×49, BETR×1, BTBT×11, LDI×17 | $10,209.60 | +83.53 | +171.19 | — | — | $35.71 | $10,380.79 | BTSG×41, IREN×54, SLS×213, TPG×49, BETR×1, BTBT×11, LDI×17 |
| 2026-08-20 | +1.12 | $35.71 | BTSG×41, IREN×54, SLS×213, TPG×49, BETR×1, BTBT×11, LDI×17 | $10,324.41 | -56.38 | +240.28 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | BTSG, IREN, SLS, TPG | $147.74 | $10,530.37 | BETR×1, BTBT×11, LDI×17, AG×62, BHP×14, CDE×62, HDSN×222, IAG×65, KGC×43, NFGC×733, WPM×8 |
| 2026-08-21 | +3.25 | $147.74 | BETR×1, BTBT×11, LDI×17, AG×62, BHP×14, CDE×62, HDSN×222, IAG×65, KGC×43, NFGC×733, WPM×8 | $10,806.62 | +276.25 | +2.08 | AUPH, ARCT, CYPH | BETR, BTBT, LDI | $119.94 | $10,807.31 | AG×62, BHP×14, CDE×62, HDSN×222, IAG×65, KGC×43, NFGC×733, WPM×8, AUPH×1, ARCT×2, CYPH×24 |
| 2026-08-24 | -5.17 | $119.94 | AG×62, BHP×14, CDE×62, HDSN×222, IAG×65, KGC×43, NFGC×733, WPM×8, AUPH×1, ARCT×2, CYPH×24 | $10,920.70 | +113.39 | -34.55 | — | — | $119.94 | $10,886.15 | AG×62, BHP×14, CDE×62, HDSN×222, IAG×65, KGC×43, NFGC×733, WPM×8, AUPH×1, ARCT×2, CYPH×24 |
| 2026-08-25 | +1.80 | $119.94 | AG×62, BHP×14, CDE×62, HDSN×222, IAG×65, KGC×43, NFGC×733, WPM×8, AUPH×1, ARCT×2, CYPH×24 | $10,714.12 | -172.03 | +432.98 | CRMD | — | $1.83 | $11,145.89 | AG×62, BHP×14, CDE×62, HDSN×222, IAG×65, KGC×43, NFGC×733, WPM×8, AUPH×1, ARCT×2, CYPH×24, CRMD×14 |
| 2026-08-26 | +2.02 | $1.83 | AG×62, BHP×14, CDE×62, HDSN×222, IAG×65, KGC×43, NFGC×733, WPM×8, AUPH×1, ARCT×2, CYPH×24, CRMD×14 | $10,944.81 | -201.08 | -153.82 | — | — | $1.83 | $10,790.99 | AG×62, BHP×14, CDE×62, HDSN×222, IAG×65, KGC×43, NFGC×733, WPM×8, AUPH×1, ARCT×2, CYPH×24, CRMD×14 |
| 2026-08-27 | — | $1.83 | AG×62, BHP×14, CDE×62, HDSN×222, IAG×65, KGC×43, NFGC×733, WPM×8, AUPH×1, ARCT×2, CYPH×24, CRMD×14 | $10,818.04 | +27.05 | +1.71 | — | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $10,583.90 | $10,794.42 | AUPH×1, ARCT×2, CYPH×24, CRMD×14 |
| 2026-08-28 | +0.75 | $10,583.90 | AUPH×1, ARCT×2, CYPH×24, CRMD×14 | $10,790.80 | -3.62 | +0.28 | — | AUPH, ARCT, CYPH | $10,673.83 | $10,790.03 | CRMD×14 |
| 2026-08-31 | -5.85 | $10,673.83 | CRMD×14 | $10,789.47 | -0.56 | +0.00 | — | — | $10,673.83 | $10,789.47 | CRMD×14 |
| 2026-09-01 | -6.30 | $10,673.83 | CRMD×14 | $10,789.33 | -0.14 | +0.00 | — | CRMD | $10,788.11 | $10,788.11 | — |
| 2026-09-02 | -3.83 | $10,788.11 | — | $10,788.11 | +0.00 | +0.00 | — | — | $10,788.11 | $10,788.11 | — |
| 2026-09-03 | -0.90 | $10,788.11 | — | $10,788.11 | +0.00 | -201.92 | HRMY, RVTY, VSTM, ATRC, CABA | — | $81.55 | $10,568.79 | HRMY×50, RVTY×16, VSTM×268, ATRC×40, CABA×594 |
| 2026-09-04 | +2.25 | $81.55 | HRMY×50, RVTY×16, VSTM×268, ATRC×40, CABA×594 | $10,493.35 | -75.44 | +103.80 | — | — | $81.55 | $10,597.15 | HRMY×50, RVTY×16, VSTM×268, ATRC×40, CABA×594 |
| 2026-09-08 | -11.47 | $81.55 | HRMY×50, RVTY×16, VSTM×268, ATRC×40, CABA×594 | $10,654.97 | +57.82 | -179.62 | — | — | $81.55 | $10,475.35 | HRMY×50, RVTY×16, VSTM×268, ATRC×40, CABA×594 |
| 2026-09-09 | -13.95 | $81.55 | HRMY×50, RVTY×16, VSTM×268, ATRC×40, CABA×594 | $10,415.77 | -59.58 | -293.96 | — | — | $81.55 | $10,121.81 | HRMY×50, RVTY×16, VSTM×268, ATRC×40, CABA×594 |
| 2026-09-10 | -13.28 | $81.55 | HRMY×50, RVTY×16, VSTM×268, ATRC×40, CABA×594 | $10,014.05 | -107.76 | -154.34 | — | — | $81.55 | $9,859.71 | HRMY×50, RVTY×16, VSTM×268, ATRC×40, CABA×594 |
| 2026-09-11 | +0.50 | $81.55 | HRMY×50, RVTY×16, VSTM×268, ATRC×40, CABA×594 | $9,955.13 | +95.42 | +0.00 | — | HRMY, RVTY, VSTM, ATRC, CABA | $9,937.47 | $9,937.47 | — |
| 2026-09-14 | -11.00 | $9,937.47 | — | $9,937.47 | -0.00 | +0.00 | — | — | $9,937.47 | $9,937.47 | — |
| 2026-09-15 | -3.84 | $9,937.47 | — | $9,937.47 | -0.00 | +0.00 | — | — | $9,937.47 | $9,937.47 | — |
| 2026-09-16 | +5.30 | $9,937.47 | — | $9,937.47 | -0.00 | +0.00 | — | — | $9,937.47 | $9,937.47 | — |
| 2026-09-17 | +7.38 | $9,937.47 | — | $9,937.47 | -0.00 | +0.00 | — | — | $9,937.47 | $9,937.47 | — |
| 2026-09-18 | +4.86 | $9,937.47 | — | $9,937.47 | -0.00 | -203.90 | ILMN, ARQT, FTRE, SDGR | — | $267.92 | $9,724.67 | ILMN×9, ARQT×95, FTRE×123, SDGR×84 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 41 | $59.80 | $2.11 | — | $7,546.09 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 54 | $45.98 | $2.15 | — | $5,061.02 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+12.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 213 | $11.70 | $2.75 | — | $2,566.17 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $2500.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 49 | $50.62 | $2.14 | — | $83.49 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+6.2; leftover $2500.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $83.49 | ▲ close $10,279.02 vs 09:30 $10,000.00 (session +288.17) | 16:00 close · cash $83.49 · equity $10,279.02 vs 09:30 $10,000.00 (+279.02; session marks +288.17) · 4 name(s) marked open→close (per-name table). BTSG×41 09:30 $59.80 → close $60.23 +17.63; IREN×54 09:30 $45.98 → close $44.76 -65.88; SLS×213 09:30 $11.70 → close $12.36 +140.58; TPG×49 09:30 $50.62 → close $54.62 +195.84 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $83.49 | ▼ 09:30 equity $10,260.41 vs yday $10,279.02 (-18.61) | 09:30 open · cash $83.49 (unchanged overnight, no fees) · equity $10,260.41 vs prior close $10,279.02 (-18.61) · 4 name(s) re-marked at the open (per-name table). BTSG×41 yday $60.23 → 09:30 $59.65 -23.78; IREN×54 yday $44.76 → 09:30 $44.09 -36.18; SLS×213 yday $12.36 → 09:30 $12.40 +8.52; TPG×49 yday $54.62 → 09:30 $55.29 +32.83 | — |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 1 | $14.80 | $0.15 | — | $68.54 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-9.9; leftover $16.70 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 11 | $1.50 | $0.20 | — | $51.84 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $16.70 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 17 | $0.94 | $0.21 | — | $35.71 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.5; leftover $16.70 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.71 | ▲ close $10,311.91 vs 09:30 $10,260.41 (session +52.06) | 16:00 close · cash $35.71 · equity $10,311.91 vs 09:30 $10,260.41 (+51.50; session marks +52.06) · 7 name(s) marked open→close (per-name table). BTSG×41 09:30 $59.65 → close $61.71 +84.46; IREN×54 09:30 $44.09 → close $44.06 -1.62; SLS×213 09:30 $12.40 → close $12.78 +80.94; TPG×49 09:30 $55.29 → close $53.03 -110.74; BETR×1 09:30 $14.80 → close $13.73 -1.07; BTBT×11 09:30 $1.50 → close $1.57 +0.77; LDI×17 09:30 $0.94 → close $0.90 -0.68 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.71 | ▲ 09:30 equity $10,356.19 vs yday $10,311.91 (+44.28) | 09:30 open · cash $35.71 (unchanged overnight, no fees) · equity $10,356.19 vs prior close $10,311.91 (+44.28) · 7 name(s) re-marked at the open (per-name table). BTSG×41 yday $61.71 → 09:30 $61.69 -0.82; IREN×54 yday $44.06 → 09:30 $45.23 +63.18; SLS×213 yday $12.78 → 09:30 $12.78 +0.00; TPG×49 yday $53.03 → 09:30 $52.67 -17.64; BETR×1 yday $13.73 → 09:30 $13.67 -0.06; BTBT×11 yday $1.57 → 09:30 $1.52 -0.55; LDI×17 yday $0.90 → 09:30 $0.91 +0.17 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.71 | ▼ close $10,287.63 vs 09:30 $10,356.19 (session -68.56) | 16:00 close · cash $35.71 · equity $10,287.63 vs 09:30 $10,356.19 (-68.56; session marks -68.56) · 7 name(s) marked open→close (per-name table). BTSG×41 09:30 $61.69 → close $60.38 -53.71; IREN×54 09:30 $45.23 → close $44.90 -17.82; SLS×213 09:30 $12.78 → close $13.00 +46.86; TPG×49 09:30 $52.67 → close $51.77 -44.10; BETR×1 09:30 $13.67 → close $13.54 -0.13; BTBT×11 09:30 $1.52 → close $1.60 +0.88; LDI×17 09:30 $0.91 → close $0.88 -0.54 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.71 | ▼ 09:30 equity $10,126.20 vs yday $10,287.63 (-161.43) | 09:30 open · cash $35.71 (unchanged overnight, no fees) · equity $10,126.20 vs prior close $10,287.63 (-161.43) · 7 name(s) re-marked at the open (per-name table). BTSG×41 yday $60.38 → 09:30 $60.00 -15.58; IREN×54 yday $44.90 → 09:30 $43.56 -72.36; SLS×213 yday $13.00 → 09:30 $12.66 -72.42; TPG×49 yday $51.77 → 09:30 $51.77 +0.00; BETR×1 yday $13.54 → 09:30 $13.21 -0.33; BTBT×11 yday $1.60 → 09:30 $1.54 -0.66; LDI×17 yday $0.88 → 09:30 $0.87 -0.09 | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.71 | ▼ close $10,126.07 vs 09:30 $10,126.20 (session -0.12) | 16:00 close · cash $35.71 · equity $10,126.07 vs 09:30 $10,126.20 (-0.13; session marks -0.12) · 7 name(s) marked open→close (per-name table). BTSG×41 09:30 $60.00 → close $59.50 -20.50; IREN×54 09:30 $43.56 → close $42.00 -84.24; SLS×213 09:30 $12.66 → close $13.10 +93.72; TPG×49 09:30 $51.77 → close $52.02 +12.25; BETR×1 09:30 $13.21 → close $13.05 -0.16; BTBT×11 09:30 $1.54 → close $1.45 -0.99; LDI×17 09:30 $0.87 → close $0.86 -0.20 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.71 | ▲ 09:30 equity $10,209.60 vs yday $10,126.07 (+83.53) | 09:30 open · cash $35.71 (unchanged overnight, no fees) · equity $10,209.60 vs prior close $10,126.07 (+83.53) · 7 name(s) re-marked at the open (per-name table). BTSG×41 yday $59.50 → 09:30 $60.15 +26.65; IREN×54 yday $42.00 → 09:30 $41.41 -31.59; SLS×213 yday $13.10 → 09:30 $13.46 +76.68; TPG×49 yday $52.02 → 09:30 $52.26 +11.76; BETR×1 yday $13.05 → 09:30 $13.03 -0.02; BTBT×11 yday $1.45 → 09:30 $1.42 -0.33; LDI×17 yday $0.86 → 09:30 $0.88 +0.37 | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.71 | ▲ close $10,380.79 vs 09:30 $10,209.60 (session +171.19) | 16:00 close · cash $35.71 · equity $10,380.79 vs 09:30 $10,209.60 (+171.19; session marks +171.19) · 7 name(s) marked open→close (per-name table). BTSG×41 09:30 $60.15 → close $59.33 -33.62; IREN×54 09:30 $41.41 → close $42.84 +76.95; SLS×213 09:30 $13.46 → close $13.85 +83.07; TPG×49 09:30 $52.26 → close $53.18 +45.08; BETR×1 09:30 $13.03 → close $13.03 +0.00; BTBT×11 09:30 $1.42 → close $1.40 -0.22; LDI×17 09:30 $0.88 → close $0.88 -0.07 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.71 | ▼ 09:30 equity $10,324.41 vs yday $10,380.79 (-56.38) | 09:30 open · cash $35.71 (unchanged overnight, no fees) · equity $10,324.41 vs prior close $10,380.79 (-56.38) · 7 name(s) re-marked at the open (per-name table). BTSG×41 yday $59.33 → 09:30 $58.64 -28.29; IREN×54 yday $42.84 → 09:30 $42.46 -20.52; SLS×213 yday $13.85 → 09:30 $13.84 -2.13; TPG×49 yday $53.18 → 09:30 $53.06 -5.88; BETR×1 yday $13.03 → 09:30 $12.95 -0.08; BTBT×11 yday $1.40 → 09:30 $1.46 +0.61; LDI×17 yday $0.88 → 09:30 $0.87 -0.09 | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 41 | $58.64 | $2.14 | $-51.82 | $2,437.80 | ▼ -51.82 after sell → book $10,322.27; vs 09:30 mark -2.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 54 | $42.46 | $2.18 | $-194.41 | $4,728.46 | ▼ -194.41 after sell → book $10,320.08; vs 09:30 mark -2.19 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 213 | $13.84 | $2.81 | $+450.27 | $7,673.58 | ▲ +450.27 after sell → book $10,317.28; vs 09:30 mark -2.80 | dropped from list after 5 sess (min 5) | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 49 | $53.06 | $2.17 | $+115.10 | $10,271.35 | ▲ +115.10 after sell → book $10,315.11; vs 09:30 mark -2.17 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 62 | $20.55 | $2.18 | — | $8,995.07 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $1283.92 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $7,718.90 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $1283.92 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 62 | $20.65 | $2.18 | — | $6,436.42 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $1283.92 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 222 | $5.77 | $2.86 | — | $5,152.62 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $1283.92 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 65 | $19.63 | $2.19 | — | $3,874.49 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $1283.92 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 43 | $29.63 | $2.12 | — | $2,598.28 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $1283.92 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 733 | $1.75 | $9.46 | — | $1,306.07 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $1283.92 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $147.74 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy,oppset; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $1283.92 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $147.74 | ▲ close $10,530.37 vs 09:30 $10,324.41 (session +240.28) | 16:00 close · cash $147.74 · equity $10,530.37 vs 09:30 $10,324.41 (+205.96; session marks +240.28) · 11 name(s) marked open→close (per-name table). BETR×1 09:30 $12.95 → close $11.60 -1.35; BTBT×11 09:30 $1.46 → close $1.59 +1.54; LDI×17 09:30 $0.87 → close $0.87 -0.02; AG×62 09:30 $20.55 → close $21.19 +39.68; BHP×14 09:30 $91.01 → close $93.63 +36.68; CDE×62 09:30 $20.65 → close $21.11 +28.52; HDSN×222 09:30 $5.77 → close $5.57 -44.40; IAG×65 09:30 $19.63 → close $20.50 +56.55; KGC×43 09:30 $29.63 → close $31.43 +77.40; NFGC×733 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $147.74 | ▲ 09:30 equity $10,806.62 vs yday $10,530.37 (+276.25) | 09:30 open · cash $147.74 (unchanged overnight, no fees) · equity $10,806.62 vs prior close $10,530.37 (+276.25) · 11 name(s) re-marked at the open (per-name table). BETR×1 yday $11.60 → 09:30 $11.73 +0.13; BTBT×11 yday $1.59 → 09:30 $1.66 +0.71; LDI×17 yday $0.87 → 09:30 $0.87 -0.05; AG×62 yday $21.19 → 09:30 $21.90 +44.02; BHP×14 yday $93.63 → 09:30 $95.72 +29.26; CDE×62 yday $21.11 → 09:30 $21.75 +39.68; HDSN×222 yday $5.57 → 09:30 $5.67 +22.20; IAG×65 yday $20.50 → 09:30 $21.17 +43.55; KGC×43 yday $31.43 → 09:30 $32.17 +31.82; NFGC×733 yday $1.75 → 09:30 $1.79 +29.32; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **SELL** | `BETR` | 1 | $11.73 | $0.14 | $-3.36 | $159.33 | ▼ -3.36 after sell → book $10,806.48; vs 09:30 mark -0.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 11 | $1.66 | $0.24 | $+1.33 | $177.35 | ▲ +1.33 after sell → book $10,806.24; vs 09:30 mark -0.24 | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 17 | $0.87 | $0.22 | $-1.62 | $191.87 | ▼ -1.62 after sell → book $10,806.02; vs 09:30 mark -0.22 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $174.50 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $31.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $152.01 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,yday_gainer,mover_buy,oppset; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $31.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 24 | $1.32 | $0.39 | — | $119.94 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $31.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $119.94 | ▲ close $10,807.31 vs 09:30 $10,806.62 (session +2.08) | 16:00 close · cash $119.94 · equity $10,807.31 vs 09:30 $10,806.62 (+0.69; session marks +2.08) · 11 name(s) marked open→close (per-name table). AG×62 09:30 $21.90 → close $21.09 -50.22; BHP×14 09:30 $95.72 → close $97.03 +18.34; CDE×62 09:30 $21.75 → close $20.97 -48.36; HDSN×222 09:30 $5.67 → close $5.63 -8.88; IAG×65 09:30 $21.17 → close $21.14 -1.95; KGC×43 09:30 $32.17 → close $32.76 +25.37; NFGC×733 09:30 $1.79 → close $1.84 +36.65; WPM×8 09:30 $154.70 → close $157.78 +24.64; AUPH×1 09:30 $17.20 → close $16.65 -0.55; ARCT×2 09:30 $11.13 → close $13.45 +4.64; CYPH×24 09:30 $1.32 → close $1.42 +2.40 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $119.94 | ▲ 09:30 equity $10,920.70 vs yday $10,807.31 (+113.39) | 09:30 open · cash $119.94 (unchanged overnight, no fees) · equity $10,920.70 vs prior close $10,807.31 (+113.39) · 11 name(s) re-marked at the open (per-name table). AG×62 yday $21.09 → 09:30 $21.30 +13.02; BHP×14 yday $97.03 → 09:30 $97.31 +3.92; CDE×62 yday $20.97 → 09:30 $21.26 +17.98; HDSN×222 yday $5.63 → 09:30 $5.69 +13.32; IAG×65 yday $21.14 → 09:30 $21.38 +15.60; KGC×43 yday $32.76 → 09:30 $33.03 +11.61; NFGC×733 yday $1.84 → 09:30 $1.86 +14.66; WPM×8 yday $157.78 → 09:30 $159.50 +13.76; AUPH×1 yday $16.65 → 09:30 $16.57 -0.08; ARCT×2 yday $13.45 → 09:30 $13.33 -0.24; CYPH×24 yday $1.42 → 09:30 $1.83 +9.84 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $119.94 | ▼ close $10,886.15 vs 09:30 $10,920.70 (session -34.55) | 16:00 close · cash $119.94 · equity $10,886.15 vs 09:30 $10,920.70 (-34.55; session marks -34.55) · 11 name(s) marked open→close (per-name table). AG×62 09:30 $21.30 → close $20.83 -29.14; BHP×14 09:30 $97.31 → close $97.13 -2.52; CDE×62 09:30 $21.26 → close $20.88 -23.56; HDSN×222 09:30 $5.69 → close $5.52 -37.74; IAG×65 09:30 $21.38 → close $21.80 +27.30; KGC×43 09:30 $33.03 → close $32.98 -2.15; NFGC×733 09:30 $1.86 → close $1.90 +29.32; WPM×8 09:30 $159.50 → close $160.19 +5.52; AUPH×1 09:30 $16.57 → close $16.57 +0.00; ARCT×2 09:30 $13.33 → close $14.34 +2.02; CYPH×24 09:30 $1.83 → close $1.68 -3.60 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $119.94 | ▼ 09:30 equity $10,714.12 vs yday $10,886.15 (-172.03) | 09:30 open · cash $119.94 (unchanged overnight, no fees) · equity $10,714.12 vs prior close $10,886.15 (-172.03) · 11 name(s) re-marked at the open (per-name table). AG×62 yday $20.83 → 09:30 $20.32 -31.62; BHP×14 yday $97.13 → 09:30 $95.86 -17.78; CDE×62 yday $20.88 → 09:30 $20.47 -25.42; HDSN×222 yday $5.52 → 09:30 $5.53 +2.22; IAG×65 yday $21.80 → 09:30 $21.21 -38.35; KGC×43 yday $32.98 → 09:30 $32.32 -28.38; NFGC×733 yday $1.90 → 09:30 $1.90 +0.00; WPM×8 yday $160.19 → 09:30 $156.51 -29.44; AUPH×1 yday $16.57 → 09:30 $16.63 +0.06; ARCT×2 yday $14.34 → 09:30 $14.12 -0.44; CYPH×24 yday $1.68 → 09:30 $1.56 -2.88 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 14 | $8.35 | $1.21 | — | $1.83 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.0; leftover $119.94 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.83 | ▲ close $11,145.89 vs 09:30 $10,714.12 (session +432.98) | 16:00 close · cash $1.83 · equity $11,145.89 vs 09:30 $10,714.12 (+431.77; session marks +432.98) · 12 name(s) marked open→close (per-name table). AG×62 09:30 $20.32 → close $21.24 +57.04; BHP×14 09:30 $95.86 → close $98.69 +39.62; CDE×62 09:30 $20.47 → close $21.59 +69.44; HDSN×222 09:30 $5.53 → close $5.47 -13.32; IAG×65 09:30 $21.21 → close $22.17 +62.40; KGC×43 09:30 $32.32 → close $33.48 +49.88; NFGC×733 09:30 $1.90 → close $2.04 +102.62; WPM×8 09:30 $156.51 → close $163.72 +57.68; AUPH×1 09:30 $16.63 → close $16.75 +0.12; ARCT×2 09:30 $14.12 → close $15.44 +2.64; CYPH×24 09:30 $1.56 → close $1.64 +1.92; CRMD×14 09:30 $8.35 → close $8.56 +2.94 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.83 | ▼ 09:30 equity $10,944.81 vs yday $11,145.89 (-201.08) | 09:30 open · cash $1.83 (unchanged overnight, no fees) · equity $10,944.81 vs prior close $11,145.89 (-201.08) · 12 name(s) re-marked at the open (per-name table). AG×62 yday $21.24 → 09:30 $20.63 -37.82; BHP×14 yday $98.69 → 09:30 $96.99 -23.80; CDE×62 yday $21.59 → 09:30 $21.00 -36.58; HDSN×222 yday $5.47 → 09:30 $5.51 +8.88; IAG×65 yday $22.17 → 09:30 $21.64 -34.45; KGC×43 yday $33.48 → 09:30 $32.90 -24.94; NFGC×733 yday $2.04 → 09:30 $2.00 -29.32; WPM×8 yday $163.72 → 09:30 $160.93 -22.32; AUPH×1 yday $16.75 → 09:30 $16.60 -0.15; ARCT×2 yday $15.44 → 09:30 $15.35 -0.18; CYPH×24 yday $1.64 → 09:30 $1.60 -0.96; CRMD×14 yday $8.56 → 09:30 $8.60 +0.56 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.83 | ▼ close $10,790.99 vs 09:30 $10,944.81 (session -153.82) | 16:00 close · cash $1.83 · equity $10,790.99 vs 09:30 $10,944.81 (-153.82; session marks -153.82) · 12 name(s) marked open→close (per-name table). AG×62 09:30 $20.63 → close $20.99 +22.32; BHP×14 09:30 $96.99 → close $96.33 -9.24; CDE×62 09:30 $21.00 → close $21.44 +27.28; HDSN×222 09:30 $5.51 → close $5.29 -48.84; IAG×65 09:30 $21.64 → close $21.54 -6.50; KGC×43 09:30 $32.90 → close $32.32 -24.94; NFGC×733 09:30 $2.00 → close $1.90 -73.30; WPM×8 09:30 $160.93 → close $156.02 -39.28; AUPH×1 09:30 $16.60 → close $16.54 -0.06; ARCT×2 09:30 $15.35 → close $15.83 +0.96; CYPH×24 09:30 $1.60 → close $1.63 +0.72; CRMD×14 09:30 $8.60 → close $8.39 -2.94 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.83 | ▲ 09:30 equity $10,818.04 vs yday $10,790.99 (+27.05) | 09:30 open · cash $1.83 (unchanged overnight, no fees) · equity $10,818.04 vs prior close $10,790.99 (+27.05) · 12 name(s) re-marked at the open (per-name table). AG×62 yday $20.99 → 09:30 $20.93 -3.72; BHP×14 yday $96.33 → 09:30 $95.52 -11.34; CDE×62 yday $21.44 → 09:30 $21.31 -8.06; HDSN×222 yday $5.29 → 09:30 $5.49 +44.40; IAG×65 yday $21.54 → 09:30 $21.47 -4.55; KGC×43 yday $32.32 → 09:30 $32.32 +0.00; NFGC×733 yday $1.90 → 09:30 $1.91 +7.33; WPM×8 yday $156.02 → 09:30 $155.89 -1.04; AUPH×1 yday $16.54 → 09:30 $16.47 -0.07; ARCT×2 yday $15.83 → 09:30 $15.74 -0.18; CYPH×24 yday $1.63 → 09:30 $1.75 +2.88; CRMD×14 yday $8.39 → 09:30 $8.49 +1.40 | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 62 | $20.93 | $2.20 | $+19.19 | $1,297.29 | ▲ +19.19 after sell → book $10,815.84; vs 09:30 mark -2.20 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 14 | $95.52 | $2.05 | $+59.06 | $2,632.52 | ▲ +59.06 after sell → book $10,813.79; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 62 | $21.31 | $2.20 | $+36.55 | $3,951.54 | ▲ +36.55 after sell → book $10,811.59; vs 09:30 mark -2.20 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 222 | $5.49 | $2.91 | $-67.93 | $5,167.41 | ▼ -67.93 after sell → book $10,808.68; vs 09:30 mark -2.91 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 65 | $21.47 | $2.21 | $+115.21 | $6,560.75 | ▲ +115.21 after sell → book $10,806.47; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 43 | $32.32 | $2.14 | $+111.41 | $7,948.37 | ▲ +111.41 after sell → book $10,804.33; vs 09:30 mark -2.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 733 | $1.91 | $9.59 | $+98.24 | $9,338.82 | ▲ +98.24 after sell → book $10,794.75; vs 09:30 mark -9.58 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 8 | $155.89 | $2.03 | $+86.75 | $10,583.90 | ▲ +86.75 after sell → book $10,792.71; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,583.90 | ▲ close $10,794.42 vs 09:30 $10,818.04 (session +1.71) | 16:00 close · cash $10,583.90 · equity $10,794.42 vs 09:30 $10,818.04 (-23.62; session marks +1.71) · 4 name(s) marked open→close (per-name table). AUPH×1 09:30 $16.47 → close $16.48 +0.01; ARCT×2 09:30 $15.74 → close $16.17 +0.86; CYPH×24 09:30 $1.75 → close $1.89 +3.36; CRMD×14 09:30 $8.49 → close $8.31 -2.52 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,583.90 | ▼ 09:30 equity $10,790.80 vs yday $10,794.42 (-3.62) | 09:30 open · cash $10,583.90 (unchanged overnight, no fees) · equity $10,790.80 vs prior close $10,794.42 (-3.62) · 4 name(s) re-marked at the open (per-name table). AUPH×1 yday $16.48 → 09:30 $16.44 -0.04; ARCT×2 yday $16.17 → 09:30 $15.43 -1.48; CYPH×24 yday $1.89 → 09:30 $1.82 -1.68; CRMD×14 yday $8.31 → 09:30 $8.28 -0.42 | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 1 | $16.44 | $0.19 | $-1.12 | $10,600.15 | ▼ -1.12 after sell → book $10,790.61; vs 09:30 mark -0.19 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 2 | $15.43 | $0.33 | $+8.04 | $10,630.68 | ▲ +8.04 after sell → book $10,790.28; vs 09:30 mark -0.33 | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 24 | $1.82 | $0.53 | $+11.08 | $10,673.83 | ▲ +11.08 after sell → book $10,789.75; vs 09:30 mark -0.53 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,673.83 | ▲ close $10,790.03 vs 09:30 $10,790.80 (session +0.28) | 16:00 close · cash $10,673.83 · equity $10,790.03 vs 09:30 $10,790.80 (-0.77; session marks +0.28) · 1 name(s) marked open→close (per-name table). CRMD×14 09:30 $8.28 → close $8.30 +0.28 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,673.83 | ▼ 09:30 equity $10,789.47 vs yday $10,790.03 (-0.56) | 09:30 open · cash $10,673.83 (unchanged overnight, no fees) · equity $10,789.47 vs prior close $10,790.03 (-0.56) · 1 name(s) re-marked at the open (per-name table). CRMD×14 yday $8.30 → 09:30 $8.26 -0.56 | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,673.83 | ▲ close $10,789.47 vs 09:30 $10,789.47 (session +0.00) | 16:00 close · cash $10,673.83 · equity $10,789.47 vs 09:30 $10,789.47 (+0.00; session marks +0.00) · 1 name(s) marked open→close (per-name table). CRMD×14 09:30 $8.26 → close $8.26 +0.00 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,673.83 | ▼ 09:30 equity $10,789.33 vs yday $10,789.47 (-0.14) | 09:30 open · cash $10,673.83 (unchanged overnight, no fees) · equity $10,789.33 vs prior close $10,789.47 (-0.14) · 1 name(s) re-marked at the open (per-name table). CRMD×14 yday $8.26 → 09:30 $8.25 -0.14 | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 14 | $8.25 | $1.22 | $-3.83 | $10,788.11 | ▼ -3.83 after sell → book $10,788.11; vs 09:30 mark -1.22 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,788.11 | ▲ close $10,788.11 vs 09:30 $10,789.33 (session +0.00) | 16:00 close · cash $10,788.11 · no lots left · equity $10,788.11. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,788.11 | ▲ 09:30 equity $10,788.11 vs yday $10,788.11 (+0.00) | 09:30 open · cash $10,788.11 · no holdings · equity $10,788.11 vs prior close $10,788.11 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,788.11 | ▲ close $10,788.11 vs 09:30 $10,788.11 (session +0.00) | 16:00 close · cash $10,788.11 · no lots left · equity $10,788.11. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,788.11 | ▲ 09:30 equity $10,788.11 vs yday $10,788.11 (+0.00) | 09:30 open · cash $10,788.11 · no holdings · equity $10,788.11 vs prior close $10,788.11 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 50 | $42.93 | $2.14 | — | $8,639.47 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.1; leftover $2157.62 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 16 | $132.45 | $2.04 | — | $6,518.24 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.6; leftover $2157.62 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 268 | $8.03 | $3.46 | — | $4,362.74 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,ohlc_hot,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.5; leftover $2157.62 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 40 | $52.88 | $2.11 | — | $2,245.43 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $2157.62 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 594 | $3.63 | $7.66 | — | $81.55 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.8; leftover $2157.62 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $81.55 | ▼ close $10,568.79 vs 09:30 $10,788.11 (session -201.92) | 16:00 close · cash $81.55 · equity $10,568.79 vs 09:30 $10,788.11 (-219.32; session marks -201.92) · 5 name(s) marked open→close (per-name table). HRMY×50 09:30 $42.93 → close $41.86 -53.50; RVTY×16 09:30 $132.45 → close $130.63 -29.12; VSTM×268 09:30 $8.03 → close $7.98 -13.40; ATRC×40 09:30 $52.88 → close $52.46 -16.80; CABA×594 09:30 $3.63 → close $3.48 -89.10 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $81.55 | ▼ 09:30 equity $10,493.35 vs yday $10,568.79 (-75.44) | 09:30 open · cash $81.55 (unchanged overnight, no fees) · equity $10,493.35 vs prior close $10,568.79 (-75.44) · 5 name(s) re-marked at the open (per-name table). HRMY×50 yday $41.86 → 09:30 $41.50 -18.00; RVTY×16 yday $130.63 → 09:30 $130.03 -9.60; VSTM×268 yday $7.98 → 09:30 $7.91 -18.76; ATRC×40 yday $52.46 → 09:30 $52.03 -17.20; CABA×594 yday $3.48 → 09:30 $3.46 -11.88 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $81.55 | ▲ close $10,597.15 vs 09:30 $10,493.35 (session +103.80) | 16:00 close · cash $81.55 · equity $10,597.15 vs 09:30 $10,493.35 (+103.80; session marks +103.80) · 5 name(s) marked open→close (per-name table). HRMY×50 09:30 $41.50 → close $42.25 +37.50; RVTY×16 09:30 $130.03 → close $130.22 +3.04; VSTM×268 09:30 $7.91 → close $8.20 +77.72; ATRC×40 09:30 $52.03 → close $51.52 -20.40; CABA×594 09:30 $3.46 → close $3.47 +5.94 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $81.55 | ▲ 09:30 equity $10,654.97 vs yday $10,597.15 (+57.82) | 09:30 open · cash $81.55 (unchanged overnight, no fees) · equity $10,654.97 vs prior close $10,597.15 (+57.82) · 5 name(s) re-marked at the open (per-name table). HRMY×50 yday $42.25 → 09:30 $42.20 -2.50; RVTY×16 yday $130.22 → 09:30 $128.50 -27.52; VSTM×268 yday $8.20 → 09:30 $8.20 +0.00; ATRC×40 yday $51.52 → 09:30 $54.31 +111.60; CABA×594 yday $3.47 → 09:30 $3.43 -23.76 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $81.55 | ▼ close $10,475.35 vs 09:30 $10,654.97 (session -179.62) | 16:00 close · cash $81.55 · equity $10,475.35 vs 09:30 $10,654.97 (-179.62; session marks -179.62) · 5 name(s) marked open→close (per-name table). HRMY×50 09:30 $42.20 → close $42.07 -6.50; RVTY×16 09:30 $128.50 → close $127.08 -22.72; VSTM×268 09:30 $8.20 → close $8.08 -32.16; ATRC×40 09:30 $54.31 → close $53.73 -23.20; CABA×594 09:30 $3.43 → close $3.27 -95.04 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $81.55 | ▼ 09:30 equity $10,415.77 vs yday $10,475.35 (-59.58) | 09:30 open · cash $81.55 (unchanged overnight, no fees) · equity $10,415.77 vs prior close $10,475.35 (-59.58) · 5 name(s) re-marked at the open (per-name table). HRMY×50 yday $42.07 → 09:30 $42.01 -3.00; RVTY×16 yday $127.08 → 09:30 $125.77 -20.96; VSTM×268 yday $8.08 → 09:30 $8.01 -18.76; ATRC×40 yday $53.73 → 09:30 $53.16 -22.80; CABA×594 yday $3.27 → 09:30 $3.28 +5.94 | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $81.55 | ▼ close $10,121.81 vs 09:30 $10,415.77 (session -293.96) | 16:00 close · cash $81.55 · equity $10,121.81 vs 09:30 $10,415.77 (-293.96; session marks -293.96) · 5 name(s) marked open→close (per-name table). HRMY×50 09:30 $42.01 → close $41.62 -19.50; RVTY×16 09:30 $125.77 → close $123.85 -30.72; VSTM×268 09:30 $8.01 → close $7.94 -18.76; ATRC×40 09:30 $53.16 → close $53.03 -5.20; CABA×594 09:30 $3.28 → close $2.91 -219.78 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $81.55 | ▼ 09:30 equity $10,014.05 vs yday $10,121.81 (-107.76) | 09:30 open · cash $81.55 (unchanged overnight, no fees) · equity $10,014.05 vs prior close $10,121.81 (-107.76) · 5 name(s) re-marked at the open (per-name table). HRMY×50 yday $41.62 → 09:30 $41.26 -18.00; RVTY×16 yday $123.85 → 09:30 $122.77 -17.28; VSTM×268 yday $7.94 → 09:30 $7.91 -8.04; ATRC×40 yday $53.03 → 09:30 $52.31 -28.80; CABA×594 yday $2.91 → 09:30 $2.85 -35.64 | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $81.55 | ▼ close $9,859.71 vs 09:30 $10,014.05 (session -154.34) | 16:00 close · cash $81.55 · equity $9,859.71 vs 09:30 $10,014.05 (-154.34; session marks -154.34) · 5 name(s) marked open→close (per-name table). HRMY×50 09:30 $41.26 → close $41.10 -8.00; RVTY×16 09:30 $122.77 → close $120.94 -29.28; VSTM×268 09:30 $7.91 → close $7.62 -77.72; ATRC×40 09:30 $52.31 → close $52.96 +26.00; CABA×594 09:30 $2.85 → close $2.74 -65.34 | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $81.55 | ▲ 09:30 equity $9,955.13 vs yday $9,859.71 (+95.42) | 09:30 open · cash $81.55 (unchanged overnight, no fees) · equity $9,955.13 vs prior close $9,859.71 (+95.42) · 5 name(s) re-marked at the open (per-name table). HRMY×50 yday $41.10 → 09:30 $41.30 +10.00; RVTY×16 yday $120.94 → 09:30 $122.40 +23.36; VSTM×268 yday $7.62 → 09:30 $7.70 +21.44; ATRC×40 yday $52.96 → 09:30 $53.53 +22.80; CABA×594 yday $2.74 → 09:30 $2.77 +17.82 | — |
| 2026-09-11 09:30 ET | **SELL** | `HRMY` | 50 | $41.30 | $2.17 | $-85.81 | $2,144.38 | ▼ -85.81 after sell → book $9,952.96; vs 09:30 mark -2.17 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `RVTY` | 16 | $122.40 | $2.06 | $-164.90 | $4,100.72 | ▼ -164.90 after sell → book $9,950.90; vs 09:30 mark -2.06 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `VSTM` | 268 | $7.70 | $3.52 | $-95.42 | $6,160.80 | ▼ -95.42 after sell → book $9,947.38; vs 09:30 mark -3.52 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `ATRC` | 40 | $53.53 | $2.14 | $+21.75 | $8,299.86 | ▲ +21.75 after sell → book $9,945.24; vs 09:30 mark -2.14 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CABA` | 594 | $2.77 | $7.77 | $-526.28 | $9,937.47 | ▼ -526.28 after sell → book $9,937.47; vs 09:30 mark -7.77 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,937.47 | ▲ close $9,937.47 vs 09:30 $9,955.13 (session +0.00) | 16:00 close · cash $9,937.47 · no lots left · equity $9,937.47. | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,937.47 | ▲ 09:30 equity $9,937.47 vs yday $9,937.47 (-0.00) | 09:30 open · cash $9,937.47 · no holdings · equity $9,937.47 vs prior close $9,937.47 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,937.47 | ▲ close $9,937.47 vs 09:30 $9,937.47 (session +0.00) | 16:00 close · cash $9,937.47 · no lots left · equity $9,937.47. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,937.47 | ▲ 09:30 equity $9,937.47 vs yday $9,937.47 (-0.00) | 09:30 open · cash $9,937.47 · no holdings · equity $9,937.47 vs prior close $9,937.47 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,937.47 | ▲ close $9,937.47 vs 09:30 $9,937.47 (session +0.00) | 16:00 close · cash $9,937.47 · no lots left · equity $9,937.47. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,937.47 | ▲ 09:30 equity $9,937.47 vs yday $9,937.47 (-0.00) | 09:30 open · cash $9,937.47 · no holdings · equity $9,937.47 vs prior close $9,937.47 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,937.47 | ▲ close $9,937.47 vs 09:30 $9,937.47 (session +0.00) | 16:00 close · cash $9,937.47 · no lots left · equity $9,937.47. | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,937.47 | ▲ 09:30 equity $9,937.47 vs yday $9,937.47 (-0.00) | 09:30 open · cash $9,937.47 · no holdings · equity $9,937.47 vs prior close $9,937.47 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,937.47 | ▲ close $9,937.47 vs 09:30 $9,937.47 (session +0.00) | 16:00 close · cash $9,937.47 · no lots left · equity $9,937.47. | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,937.47 | ▲ 09:30 equity $9,937.47 vs yday $9,937.47 (-0.00) | 09:30 open · cash $9,937.47 · no holdings · equity $9,937.47 vs prior close $9,937.47 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-18 09:30 ET | **BUY** | `ILMN` | 9 | $249.13 | $2.02 | — | $7,693.28 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+21.8; leftover $2484.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-18 09:30 ET | **BUY** | `ARQT` | 95 | $26.14 | $2.27 | — | $5,207.70 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,ohlc_hot,oppset; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+13.2; leftover $2484.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `FTRE` | 123 | $20.10 | $2.36 | — | $2,733.04 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+19.2; leftover $2484.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 84 | $29.32 | $2.24 | — | $267.92 | — | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,yday_gainer,yday_mover,oppset; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+60.9; leftover $2484.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $267.92 | ▼ close $9,724.67 vs 09:30 $9,937.47 (session -203.90) | 16:00 close · cash $267.92 · equity $9,724.67 vs 09:30 $9,937.47 (-212.80; session marks -203.90) · 4 name(s) marked open→close (per-name table). ILMN×9 09:30 $249.13 → close $239.62 -85.59; ARQT×95 09:30 $26.14 → close $25.38 -72.20; FTRE×123 09:30 $20.10 → close $19.93 -20.91; SDGR×84 09:30 $29.32 → close $29.02 -25.20 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `DAVE` | cash | leftover split 16.70 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 16.70 < 1 share @ 57.61 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `BETR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `BTSG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `IREN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `SLS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TPG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `BETR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `BTSG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `IREN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `SLS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TPG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `BETR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `LDI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `BETR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `BTBT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `LDI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 31.98 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 31.98 < 1 share @ 216.30 |
| 2026-08-21 | `FUTU` | cash | leftover split 31.98 < 1 share @ 115.18 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-25 | `AG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `BHP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `CDE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `HDSN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `IAG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `KGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `NFGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `WPM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-26 | `AG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `BHP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `CDE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `HDSN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `IAG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `KGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `NFGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `WPM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `AUPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `ARCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRMD` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-27 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CYPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `CRMD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `CRMD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `ATRC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CABA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CRM` | cash | leftover split 81.55 < 1 share @ 263.36 |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `VSTM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `HRMY` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `RVTY` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `VSTM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `ATRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `CABA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `HRMY` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `RVTY` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `VSTM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `ATRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `CABA` | min_hold | dropped but min-hold 4/5 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ILMN` | 9 | 2026-09-18 @ $249.13 | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+21.8; leftover $2484.37 |
| `ARQT` | 95 | 2026-09-18 @ $26.14 | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,ohlc_hot,oppset; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+13.2; leftover $2484.37 |
| `FTRE` | 123 | 2026-09-18 @ $20.10 | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+19.2; leftover $2484.37 |
| `SDGR` | 84 | 2026-09-18 @ $29.32 | flatten looker: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,yday_gainer,yday_mover,oppset; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+60.9; leftover $2484.37 |
