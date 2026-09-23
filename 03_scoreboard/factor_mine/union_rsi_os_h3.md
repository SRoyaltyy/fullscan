# Factor mine action — `union_rsi_os_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ rsi_os, no 🚨

Cash book **-23.89%** ($7,611) · signal-only (no cash/fees) was -44.40%. Starts YES **6/29**. Fills 64 · skips 85 · realized $-2081.88.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: prior RSI is oversold (≤30) — Finviz prior export, else computed on prior bars.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

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
- **Gate** `rsi_os=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $1.43.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `NCMI` | 929 | — | $2.69 | +0.00 | $2.86 | +157.93 | +157.93 | +0.00 | +157.93 |
| 2026-08-14 | `QMLS` | 342 | — | $7.29 | +0.00 | $7.32 | +10.26 | +10.26 | +0.00 | +10.26 |
| 2026-08-14 | `CLBT` | 230 | — | $10.83 | +0.00 | $11.14 | +71.30 | +71.30 | +0.00 | +71.30 |
| 2026-08-14 | `YSS` | 247 | — | $10.06 | +0.00 | $10.93 | +214.89 | +214.89 | +0.00 | +214.89 |
| 2026-08-17 | `NCMI` | 929 | $2.86 | $2.80 | -55.74 | $2.73 | -65.03 | -120.77 | +102.19 | +37.16 |
| 2026-08-17 | `QMLS` | 342 | $7.32 | $7.24 | -27.36 | $7.14 | -34.20 | -61.56 | -17.10 | -51.30 |
| 2026-08-17 | `CLBT` | 230 | $11.14 | $11.19 | +11.50 | $10.44 | -172.50 | -161.00 | +82.80 | -89.70 |
| 2026-08-17 | `YSS` | 247 | $10.93 | $10.36 | -140.79 | $10.66 | +74.10 | -66.69 | +74.10 | +148.20 |
| 2026-08-17 | `INV` | 1 | — | $1.62 | +0.00 | $1.39 | -0.24 | -0.24 | +0.00 | -0.24 |
| 2026-08-18 | `NCMI` | 929 | $2.73 | $2.71 | -18.58 | $2.52 | -176.51 | -195.09 | +18.58 | -157.93 |
| 2026-08-18 | `QMLS` | 342 | $7.14 | $6.85 | -99.18 | $6.74 | -37.62 | -136.80 | -150.48 | -188.10 |
| 2026-08-18 | `CLBT` | 230 | $10.44 | $10.44 | +0.00 | $11.00 | +128.80 | +128.80 | -89.70 | +39.10 |
| 2026-08-18 | `YSS` | 247 | $10.66 | $10.24 | -103.74 | $10.42 | +44.46 | -59.28 | +44.46 | +88.92 |
| 2026-08-18 | `INV` | 1 | $1.39 | $1.32 | -0.06 | $1.32 | +0.00 | -0.06 | -0.30 | -0.30 |
| 2026-08-19 | `NCMI` | 929 | $2.52 | $2.56 | +37.16 | — | +0.00 | +37.16 | -120.77 | — |
| 2026-08-19 | `QMLS` | 342 | $6.74 | $6.74 | +0.00 | — | +0.00 | +0.00 | -188.10 | — |
| 2026-08-19 | `CLBT` | 230 | $11.00 | $10.85 | -34.50 | — | +0.00 | -34.50 | +4.60 | — |
| 2026-08-19 | `YSS` | 247 | $10.42 | $10.32 | -24.70 | — | +0.00 | -24.70 | +64.22 | — |
| 2026-08-19 | `INV` | 1 | $1.32 | $1.39 | +0.06 | $1.54 | +0.15 | +0.21 | -0.23 | -0.08 |
| 2026-08-20 | `INV` | 1 | $1.54 | $1.55 | +0.01 | — | +0.00 | +0.01 | -0.07 | — |
| 2026-08-20 | `LZB` | 288 | — | $33.61 | +0.00 | $33.65 | +11.52 | +11.52 | +0.00 | +11.52 |
| 2026-08-21 | `LZB` | 288 | $33.65 | $33.63 | -5.76 | $33.71 | +23.04 | +17.28 | +5.76 | +28.80 |
| 2026-08-24 | `LZB` | 288 | $33.71 | $33.55 | -46.08 | $32.12 | -411.84 | -457.92 | -17.28 | -429.12 |
| 2026-08-25 | `LZB` | 288 | $32.12 | $32.33 | +60.48 | — | +0.00 | +60.48 | -368.64 | — |
| 2026-08-25 | `QFIN` | 421 | — | $11.09 | +0.00 | $11.53 | +185.24 | +185.24 | +0.00 | +185.24 |
| 2026-08-25 | `QMLS` | 784 | — | $5.93 | +0.00 | $6.27 | +266.56 | +266.56 | +0.00 | +266.56 |
| 2026-08-26 | `QFIN` | 421 | $11.53 | $9.76 | -745.17 | $9.35 | -172.61 | -917.78 | -559.93 | -732.54 |
| 2026-08-26 | `QMLS` | 784 | $6.27 | $6.47 | +156.80 | $6.10 | -290.08 | -133.28 | +423.36 | +133.28 |
| 2026-08-27 | `QFIN` | 421 | $9.35 | $9.42 | +29.47 | $9.17 | -105.25 | -75.78 | -703.07 | -808.32 |
| 2026-08-27 | `QMLS` | 784 | $6.10 | $6.33 | +180.32 | $6.47 | +109.76 | +290.08 | +313.60 | +423.36 |
| 2026-08-28 | `QFIN` | 421 | $9.17 | $9.15 | -8.42 | $8.80 | -147.35 | -155.77 | -816.74 | -964.09 |
| 2026-08-28 | `QMLS` | 784 | $6.47 | $6.27 | -156.80 | — | +0.00 | -156.80 | +266.56 | — |
| 2026-08-28 | `DY` | 8 | — | $306.34 | +0.00 | $294.34 | -96.00 | -96.00 | +0.00 | -96.00 |
| 2026-08-28 | `LX` | 2094 | — | $1.16 | +0.00 | $1.18 | +41.88 | +41.88 | +0.00 | +41.88 |
| 2026-08-31 | `QFIN` | 421 | $8.80 | $8.70 | -42.10 | — | +0.00 | -42.10 | -1006.19 | — |
| 2026-08-31 | `DY` | 8 | $294.34 | $298.01 | +29.36 | $291.21 | -54.40 | -25.04 | -66.64 | -121.04 |
| 2026-08-31 | `LX` | 2094 | $1.18 | $1.01 | -355.98 | $1.07 | +125.64 | -230.34 | -314.10 | -188.46 |
| 2026-09-01 | `DY` | 8 | $291.21 | $289.16 | -16.40 | $287.30 | -14.88 | -31.28 | -137.44 | -152.32 |
| 2026-09-01 | `LX` | 2094 | $1.07 | $1.01 | -125.64 | $0.88 | -268.03 | -393.67 | -314.10 | -582.13 |
| 2026-09-02 | `DY` | 8 | $287.30 | $287.99 | +5.52 | — | +0.00 | +5.52 | -146.80 | — |
| 2026-09-02 | `LX` | 2094 | $0.88 | $0.91 | +50.26 | — | +0.00 | +50.26 | -531.88 | — |
| 2026-09-03 | `SION` | 133 | — | $7.31 | +0.00 | $6.75 | -74.48 | -74.48 | +0.00 | -74.48 |
| 2026-09-03 | `ALMS` | 94 | — | $10.38 | +0.00 | $11.36 | +92.59 | +92.59 | +0.00 | +92.59 |
| 2026-09-03 | `LX` | 1183 | — | $0.83 | +0.00 | $0.85 | +29.58 | +29.58 | +0.00 | +29.58 |
| 2026-09-03 | `EVTL` | 1529 | — | $0.64 | +0.00 | $0.60 | -61.16 | -61.16 | +0.00 | -61.16 |
| 2026-09-03 | `FJET` | 344 | — | $2.84 | +0.00 | $2.78 | -20.64 | -20.64 | +0.00 | -20.64 |
| 2026-09-03 | `OSW` | 44 | — | $22.00 | +0.00 | $22.27 | +11.88 | +11.88 | +0.00 | +11.88 |
| 2026-09-03 | `PL` | 49 | — | $19.86 | +0.00 | $18.35 | -73.99 | -73.99 | +0.00 | -73.99 |
| 2026-09-03 | `SWBI` | 75 | — | $12.78 | +0.00 | $12.27 | -38.25 | -38.25 | +0.00 | -38.25 |
| 2026-09-04 | `SION` | 133 | $6.75 | $6.68 | -9.31 | $7.18 | +66.50 | +57.19 | -83.79 | -17.29 |
| 2026-09-04 | `ALMS` | 94 | $11.36 | $11.23 | -12.22 | $11.10 | -12.22 | -24.44 | +80.37 | +68.15 |
| 2026-09-04 | `LX` | 1183 | $0.85 | $0.86 | +7.10 | $0.88 | +28.39 | +35.49 | +36.67 | +65.07 |
| 2026-09-04 | `EVTL` | 1529 | $0.60 | $0.60 | +0.00 | $0.60 | -3.06 | -3.06 | -61.16 | -64.22 |
| 2026-09-04 | `FJET` | 344 | $2.78 | $2.80 | +6.88 | $2.80 | +0.00 | +6.88 | -13.76 | -13.76 |
| 2026-09-04 | `OSW` | 44 | $22.27 | $22.27 | +0.00 | $22.40 | +5.72 | +5.72 | +11.88 | +17.60 |
| 2026-09-04 | `PL` | 49 | $18.35 | $19.64 | +63.21 | $18.12 | -74.48 | -11.27 | -10.78 | -85.26 |
| 2026-09-04 | `SWBI` | 75 | $12.27 | $14.12 | +138.75 | $12.89 | -92.25 | +46.50 | +100.50 | +8.25 |
| 2026-09-04 | `AIIO` | 4 | — | $1.75 | +0.00 | $1.84 | +0.36 | +0.36 | +0.00 | +0.36 |
| 2026-09-08 | `SION` | 133 | $7.18 | $7.13 | -6.65 | $7.30 | +22.61 | +15.96 | -23.94 | -1.33 |
| 2026-09-08 | `ALMS` | 94 | $11.10 | $11.05 | -4.70 | $10.58 | -44.18 | -48.88 | +63.45 | +19.27 |
| 2026-09-08 | `LX` | 1183 | $0.88 | $0.90 | +21.29 | $0.85 | -55.60 | -34.31 | +86.36 | +30.76 |
| 2026-09-08 | `EVTL` | 1529 | $0.60 | $0.60 | +0.00 | $0.59 | -12.23 | -12.23 | -64.22 | -76.45 |
| 2026-09-08 | `FJET` | 344 | $2.80 | $2.80 | +0.00 | $2.63 | -58.48 | -58.48 | -13.76 | -72.24 |
| 2026-09-08 | `OSW` | 44 | $22.40 | $22.29 | -4.84 | $22.12 | -7.48 | -12.32 | +12.76 | +5.28 |
| 2026-09-08 | `PL` | 49 | $18.12 | $17.85 | -13.23 | $17.81 | -1.96 | -15.19 | -98.49 | -100.45 |
| 2026-09-08 | `SWBI` | 75 | $12.89 | $12.51 | -28.50 | $13.23 | +54.00 | +25.50 | -20.25 | +33.75 |
| 2026-09-08 | `AIIO` | 4 | $1.84 | $1.81 | -0.12 | $1.95 | +0.56 | +0.44 | +0.24 | +0.80 |
| 2026-09-09 | `SION` | 133 | $7.30 | $7.27 | -3.99 | — | +0.00 | -3.99 | -5.32 | — |
| 2026-09-09 | `ALMS` | 94 | $10.58 | $10.49 | -8.46 | — | +0.00 | -8.46 | +10.81 | — |
| 2026-09-09 | `LX` | 1183 | $0.85 | $0.83 | -21.29 | — | +0.00 | -21.29 | +9.46 | — |
| 2026-09-09 | `EVTL` | 1529 | $0.59 | $0.59 | +6.12 | — | +0.00 | +6.12 | -70.33 | — |
| 2026-09-09 | `FJET` | 344 | $2.63 | $2.63 | +0.00 | — | +0.00 | +0.00 | -72.24 | — |
| 2026-09-09 | `OSW` | 44 | $22.12 | $21.86 | -11.44 | — | +0.00 | -11.44 | -6.16 | — |
| 2026-09-09 | `PL` | 49 | $17.81 | $18.00 | +9.31 | — | +0.00 | +9.31 | -91.14 | — |
| 2026-09-09 | `SWBI` | 75 | $13.23 | $13.12 | -8.25 | — | +0.00 | -8.25 | +25.50 | — |
| 2026-09-09 | `AIIO` | 4 | $1.95 | $1.93 | -0.08 | $1.86 | -0.28 | -0.36 | +0.72 | +0.44 |
| 2026-09-10 | `AIIO` | 4 | $1.86 | $1.81 | -0.20 | — | +0.00 | -0.20 | +0.24 | — |
| 2026-09-11 | `NAVN` | 122 | — | $20.61 | +0.00 | $21.02 | +50.02 | +50.02 | +0.00 | +50.02 |
| 2026-09-11 | `RWT` | 714 | — | $3.52 | +0.00 | $3.55 | +21.42 | +21.42 | +0.00 | +21.42 |
| 2026-09-11 | `COO` | 45 | — | $54.66 | +0.00 | $53.91 | -33.75 | -33.75 | +0.00 | -33.75 |
| 2026-09-14 | `NAVN` | 122 | $21.02 | $21.10 | +9.76 | $21.37 | +32.94 | +42.70 | +59.78 | +92.72 |
| 2026-09-14 | `RWT` | 714 | $3.55 | $3.53 | -14.28 | $3.83 | +214.20 | +199.92 | +7.14 | +221.34 |
| 2026-09-14 | `COO` | 45 | $53.91 | $54.78 | +39.15 | $54.22 | -25.20 | +13.95 | +5.40 | -19.80 |
| 2026-09-15 | `NAVN` | 122 | $21.37 | $21.32 | -6.10 | $22.42 | +134.20 | +128.10 | +86.62 | +220.82 |
| 2026-09-15 | `RWT` | 714 | $3.83 | $3.80 | -21.42 | $3.91 | +78.54 | +57.12 | +199.92 | +278.46 |
| 2026-09-15 | `COO` | 45 | $54.22 | $54.40 | +8.10 | $53.27 | -50.85 | -42.75 | -11.70 | -62.55 |
| 2026-09-16 | `NAVN` | 122 | $22.42 | $22.50 | +9.76 | — | +0.00 | +9.76 | +230.58 | — |
| 2026-09-16 | `RWT` | 714 | $3.91 | $3.98 | +49.98 | — | +0.00 | +49.98 | +328.44 | — |
| 2026-09-16 | `COO` | 45 | $53.27 | $54.37 | +49.50 | — | +0.00 | +49.50 | -13.05 | — |
| 2026-09-16 | `ALHC` | 97 | — | $10.30 | +0.00 | $8.71 | -154.23 | -154.23 | +0.00 | -154.23 |
| 2026-09-16 | `PLAY` | 146 | — | $6.86 | +0.00 | $6.86 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-16 | `DVLT` | 6300 | — | $0.16 | +0.00 | $0.18 | +126.00 | +126.00 | +0.00 | +126.00 |
| 2026-09-16 | `NMRA` | 1194 | — | $0.84 | +0.00 | $0.77 | -84.77 | -84.77 | +0.00 | -84.77 |
| 2026-09-16 | `ZSQR` | 430 | — | $2.34 | +0.00 | $2.29 | -21.50 | -21.50 | +0.00 | -21.50 |
| 2026-09-16 | `CTMX` | 370 | — | $2.72 | +0.00 | $2.77 | +16.65 | +16.65 | +0.00 | +16.65 |
| 2026-09-16 | `CRBP` | 146 | — | $6.86 | +0.00 | $7.26 | +58.40 | +58.40 | +0.00 | +58.40 |
| 2026-09-16 | `EYPT` | 265 | — | $3.66 | +0.00 | $3.45 | -55.65 | -55.65 | +0.00 | -55.65 |
| 2026-09-17 | `ALHC` | 97 | $8.71 | $8.58 | -12.61 | $8.70 | +11.64 | -0.97 | -166.84 | -155.20 |
| 2026-09-17 | `PLAY` | 146 | $6.86 | $6.96 | +14.60 | $6.54 | -61.32 | -46.72 | +14.60 | -46.72 |
| 2026-09-17 | `DVLT` | 6300 | $0.18 | $0.17 | -63.00 | $0.16 | -63.00 | -126.00 | +63.00 | +0.00 |
| 2026-09-17 | `NMRA` | 1194 | $0.77 | $0.78 | +8.36 | $0.75 | -35.82 | -27.46 | -76.42 | -112.24 |
| 2026-09-17 | `ZSQR` | 430 | $2.29 | $2.35 | +25.80 | $2.54 | +81.70 | +107.50 | +4.30 | +86.00 |
| 2026-09-17 | `CTMX` | 370 | $2.77 | $2.83 | +24.05 | $2.82 | -3.70 | +20.35 | +40.70 | +37.00 |
| 2026-09-17 | `CRBP` | 146 | $7.26 | $7.26 | +0.00 | $7.26 | +0.00 | +0.00 | +58.40 | +58.40 |
| 2026-09-17 | `EYPT` | 265 | $3.45 | $3.57 | +31.80 | $3.99 | +111.30 | +143.10 | -23.85 | +87.45 |
| 2026-09-18 | `ALHC` | 97 | $8.70 | $8.68 | -1.94 | $8.35 | -32.01 | -33.95 | -157.14 | -189.15 |
| 2026-09-18 | `PLAY` | 146 | $6.54 | $6.64 | +14.60 | $6.73 | +13.14 | +27.74 | -32.12 | -18.98 |
| 2026-09-18 | `DVLT` | 6300 | $0.16 | $0.17 | +63.00 | $0.15 | -126.00 | -63.00 | +63.00 | -63.00 |
| 2026-09-18 | `NMRA` | 1194 | $0.75 | $0.74 | -10.75 | $0.73 | -13.13 | -23.88 | -122.98 | -136.12 |
| 2026-09-18 | `ZSQR` | 430 | $2.54 | $2.50 | -17.20 | $2.68 | +77.40 | +60.20 | +68.80 | +146.20 |
| 2026-09-18 | `CTMX` | 370 | $2.82 | $2.83 | +3.70 | $2.76 | -25.90 | -22.20 | +40.70 | +14.80 |
| 2026-09-18 | `CRBP` | 146 | $7.26 | $7.22 | -5.84 | $7.48 | +37.96 | +32.12 | +52.56 | +90.52 |
| 2026-09-18 | `EYPT` | 265 | $3.99 | $3.95 | -10.60 | $3.85 | -26.50 | -37.10 | +76.85 | +50.35 |
| 2026-09-21 | `ALHC` | 97 | $8.35 | $8.33 | -1.94 | — | +0.00 | -1.94 | -191.09 | — |
| 2026-09-21 | `PLAY` | 146 | $6.73 | $6.68 | -7.30 | — | +0.00 | -7.30 | -26.28 | — |
| 2026-09-21 | `DVLT` | 6300 | $0.15 | $0.16 | +63.00 | — | +0.00 | +63.00 | +0.00 | — |
| 2026-09-21 | `NMRA` | 1194 | $0.73 | $0.74 | +8.36 | — | +0.00 | +8.36 | -127.76 | — |
| 2026-09-21 | `ZSQR` | 430 | $2.68 | $2.68 | +0.00 | — | +0.00 | +0.00 | +146.20 | — |
| 2026-09-21 | `CTMX` | 370 | $2.76 | $2.80 | +14.80 | — | +0.00 | +14.80 | +29.60 | — |
| 2026-09-21 | `CRBP` | 146 | $7.48 | $7.51 | +4.38 | — | +0.00 | +4.38 | +94.90 | — |
| 2026-09-21 | `EYPT` | 265 | $3.85 | $3.87 | +5.30 | — | +0.00 | +5.30 | +55.65 | — |
| 2026-09-21 | `XENE` | 65 | — | $40.00 | +0.00 | $38.99 | -65.65 | -65.65 | +0.00 | -65.65 |
| 2026-09-21 | `SION` | 439 | — | $6.00 | +0.00 | $6.00 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-21 | `THO` | 38 | — | $68.39 | +0.00 | $69.94 | +58.90 | +58.90 | +0.00 | +58.90 |
| 2026-09-22 | `XENE` | 65 | $38.99 | $39.10 | +7.15 | $38.61 | -31.85 | -24.70 | -58.50 | -90.35 |
| 2026-09-22 | `SION` | 439 | $6.00 | $5.99 | -4.39 | $6.02 | +13.17 | +8.78 | -4.39 | +8.78 |
| 2026-09-22 | `THO` | 38 | $69.94 | $70.64 | +26.60 | $70.00 | -24.32 | +2.28 | +85.50 | +61.18 |
| 2026-09-22 | `FJET` | 36 | — | $2.03 | +0.00 | $2.02 | -0.36 | -0.36 | +0.00 | -0.36 |
| 2026-09-23 | `XENE` | 65 | $38.61 | $39.51 | +58.50 | $36.56 | -191.75 | -133.25 | -31.85 | -223.60 |
| 2026-09-23 | `SION` | 439 | $6.02 | $6.03 | +4.39 | $5.51 | -228.28 | -223.89 | +13.17 | -215.11 |
| 2026-09-23 | `THO` | 38 | $70.00 | $71.41 | +53.58 | $72.30 | +33.82 | +87.40 | +114.76 | +148.58 |
| 2026-09-23 | `FJET` | 36 | $2.02 | $1.98 | -1.44 | $1.87 | -4.11 | -5.55 | -1.80 | -5.91 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +454.38 | NCMI, QMLS, CLBT, YSS | — | $9.54 | $10,431.83 | NCMI×929, QMLS×342, CLBT×230, YSS×247 |
| 2026-08-17 | +2.25 | $9.54 | NCMI×929, QMLS×342, CLBT×230, YSS×247 | $10,219.44 | -212.39 | -197.87 | INV | — | $7.90 | $10,021.56 | NCMI×929, QMLS×342, CLBT×230, YSS×247, INV×1 |
| 2026-08-18 | -6.20 | $7.90 | NCMI×929, QMLS×342, CLBT×230, YSS×247, INV×1 | $9,800.00 | -221.56 | -40.87 | — | — | $7.90 | $9,759.13 | NCMI×929, QMLS×342, CLBT×230, YSS×247, INV×1 |
| 2026-08-19 | -7.20 | $7.90 | NCMI×929, QMLS×342, CLBT×230, YSS×247, INV×1 | $9,737.15 | -21.98 | +0.15 | — | NCMI, QMLS, CLBT, YSS | $9,712.84 | $9,714.38 | INV×1 |
| 2026-08-20 | +1.12 | $9,712.84 | INV×1 | $9,714.39 | +0.01 | +11.52 | LZB | INV | $30.96 | $9,722.16 | LZB×288 |
| 2026-08-21 | +3.25 | $30.96 | LZB×288 | $9,716.40 | -5.76 | +23.04 | — | — | $30.96 | $9,739.44 | LZB×288 |
| 2026-08-24 | -5.17 | $30.96 | LZB×288 | $9,693.36 | -46.08 | -411.84 | — | — | $30.96 | $9,281.52 | LZB×288 |
| 2026-08-25 | +1.80 | $30.96 | LZB×288 | $9,342.00 | +60.48 | +451.80 | QFIN, QMLS | LZB | $4.61 | $9,774.42 | QFIN×421, QMLS×784 |
| 2026-08-26 | +2.02 | $4.61 | QFIN×421, QMLS×784 | $9,186.05 | -588.37 | -462.69 | — | — | $4.61 | $8,723.36 | QFIN×421, QMLS×784 |
| 2026-08-27 | — | $4.61 | QFIN×421, QMLS×784 | $8,933.15 | +209.79 | +4.51 | — | — | $4.61 | $8,937.66 | QFIN×421, QMLS×784 |
| 2026-08-28 | +0.75 | $4.61 | QFIN×421, QMLS×784 | $8,772.44 | -165.22 | -201.47 | DY, LX | QMLS | $1.22 | $8,531.66 | QFIN×421, DY×8, LX×2094 |
| 2026-08-31 | -5.85 | $1.22 | QFIN×421, DY×8, LX×2094 | $8,162.94 | -368.72 | +71.24 | — | QFIN | $3,658.39 | $8,228.65 | DY×8, LX×2094 |
| 2026-09-01 | -6.30 | $3,658.39 | DY×8, LX×2094 | $8,086.61 | -142.04 | -282.91 | — | — | $3,658.39 | $7,803.70 | DY×8, LX×2094 |
| 2026-09-02 | -3.83 | $3,658.39 | DY×8, LX×2094 | $7,859.47 | +55.77 | +0.00 | — | DY, LX | $7,831.81 | $7,831.81 | — |
| 2026-09-03 | -0.90 | $7,831.81 | — | $7,831.81 | +0.00 | -134.47 | SION, ALMS, LX, EVTL, FJET, OSW, PL, SWBI | — | $7.56 | $7,654.06 | SION×133, ALMS×94, LX×1183, EVTL×1529, FJET×344, OSW×44, PL×49, SWBI×75 |
| 2026-09-04 | +2.25 | $7.56 | SION×133, ALMS×94, LX×1183, EVTL×1529, FJET×344, OSW×44, PL×49, SWBI×75 | $7,848.47 | +194.41 | -81.04 | AIIO | — | $0.47 | $7,767.35 | SION×133, ALMS×94, LX×1183, EVTL×1529, FJET×344, OSW×44, PL×49, SWBI×75, AIIO×4 |
| 2026-09-08 | -11.47 | $0.47 | SION×133, ALMS×94, LX×1183, EVTL×1529, FJET×344, OSW×44, PL×49, SWBI×75, AIIO×4 | $7,730.61 | -36.74 | -102.76 | — | — | $0.47 | $7,627.84 | SION×133, ALMS×94, LX×1183, EVTL×1529, FJET×344, OSW×44, PL×49, SWBI×75, AIIO×4 |
| 2026-09-09 | -13.95 | $0.47 | SION×133, ALMS×94, LX×1183, EVTL×1529, FJET×344, OSW×44, PL×49, SWBI×75, AIIO×4 | $7,589.75 | -38.09 | -0.28 | — | SION, ALMS, LX, EVTL, FJET, OSW, PL, SWBI | $7,538.71 | $7,546.15 | AIIO×4 |
| 2026-09-10 | -13.28 | $7,538.71 | AIIO×4 | $7,545.95 | -0.20 | +0.00 | — | AIIO | $7,545.84 | $7,545.84 | — |
| 2026-09-11 | +0.50 | $7,545.84 | — | $7,545.84 | +0.00 | +37.69 | NAVN, RWT, COO | — | $44.75 | $7,569.84 | NAVN×122, RWT×714, COO×45 |
| 2026-09-14 | -11.00 | $44.75 | NAVN×122, RWT×714, COO×45 | $7,604.47 | +34.63 | +221.94 | — | — | $44.75 | $7,826.41 | NAVN×122, RWT×714, COO×45 |
| 2026-09-15 | -3.84 | $44.75 | NAVN×122, RWT×714, COO×45 | $7,806.99 | -19.42 | +161.89 | — | — | $44.75 | $7,968.88 | NAVN×122, RWT×714, COO×45 |
| 2026-09-16 | +5.30 | $44.75 | NAVN×122, RWT×714, COO×45 | $8,078.12 | +109.24 | -115.10 | ALHC, PLAY, DVLT, NMRA, ZSQR, CTMX, CRBP, EYPT | NAVN, RWT, COO | $0.25 | $7,885.60 | ALHC×97, PLAY×146, DVLT×6300, NMRA×1194, ZSQR×430, CTMX×370, CRBP×146, EYPT×265 |
| 2026-09-17 | +7.38 | $0.25 | ALHC×97, PLAY×146, DVLT×6300, NMRA×1194, ZSQR×430, CTMX×370, CRBP×146, EYPT×265 | $7,914.60 | +29.00 | +40.80 | — | — | $0.25 | $7,955.40 | ALHC×97, PLAY×146, DVLT×6300, NMRA×1194, ZSQR×430, CTMX×370, CRBP×146, EYPT×265 |
| 2026-09-18 | +4.86 | $0.25 | ALHC×97, PLAY×146, DVLT×6300, NMRA×1194, ZSQR×430, CTMX×370, CRBP×146, EYPT×265 | $7,990.37 | +34.97 | -95.04 | — | — | $0.25 | $7,895.33 | ALHC×97, PLAY×146, DVLT×6300, NMRA×1194, ZSQR×430, CTMX×370, CRBP×146, EYPT×265 |
| 2026-09-21 | +12.87 | $0.25 | ALHC×97, PLAY×146, DVLT×6300, NMRA×1194, ZSQR×430, CTMX×370, CRBP×146, EYPT×265 | $7,981.92 | +86.59 | -6.75 | XENE, SION, THO | ALHC, PLAY, DVLT, NMRA, ZSQR, CTMX, CRBP, EYPT | $75.35 | $7,901.42 | XENE×65, SION×439, THO×38 |
| 2026-09-22 | -0.50 | $75.35 | XENE×65, SION×439, THO×38 | $7,930.78 | +29.36 | -43.36 | FJET | — | $1.43 | $7,886.58 | XENE×65, SION×439, THO×38, FJET×36 |
| 2026-09-23 | +2.29 | $1.43 | XENE×65, SION×439, THO×38, FJET×36 | $8,001.61 | +115.03 | -390.32 | — | — | $1.43 | $7,611.29 | XENE×65, SION×439, THO×38, FJET×36 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 929 | $2.69 | $11.98 | — | $7,489.01 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $2500.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 342 | $7.29 | $4.41 | — | $4,991.41 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $2500.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 230 | $10.83 | $2.97 | — | $2,497.55 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ⚪; ret5=-30.1; leftover $2500.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `YSS` | 247 | $10.06 | $3.19 | — | $9.54 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list earn_react; 🔵; ⚪; ret5=+5.7; leftover $2500.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.54 | ▲ close $10,431.83 vs 09:30 $10,000.00 (session +454.38) | 16:00 close · cash $9.54 · equity $10,431.83 vs 09:30 $10,000.00 (+431.83; session marks +454.38) · 4 name(s) marked open→close (per-name table). NCMI×929 09:30 $2.69 → close $2.86 +157.93; QMLS×342 09:30 $7.29 → close $7.32 +10.26; CLBT×230 09:30 $10.83 → close $11.14 +71.30; YSS×247 09:30 $10.06 → close $10.93 +214.89 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.54 | ▼ 09:30 equity $10,219.44 vs yday $10,431.83 (-212.39) | 09:30 open · cash $9.54 (unchanged overnight, no fees) · equity $10,219.44 vs prior close $10,431.83 (-212.39) · 4 name(s) re-marked at the open (per-name table). NCMI×929 yday $2.86 → 09:30 $2.80 -55.74; QMLS×342 yday $7.32 → 09:30 $7.24 -27.36; CLBT×230 yday $11.14 → 09:30 $11.19 +11.50; YSS×247 yday $10.93 → 09:30 $10.36 -140.79 | — |
| 2026-08-17 09:30 ET | **BUY** | `INV` | 1 | $1.62 | $0.02 | — | $7.90 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ⚪; ret5=-53.0; leftover $2.39 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.90 | ▼ close $10,021.56 vs 09:30 $10,219.44 (session -197.87) | 16:00 close · cash $7.90 · equity $10,021.56 vs 09:30 $10,219.44 (-197.88; session marks -197.87) · 5 name(s) marked open→close (per-name table). NCMI×929 09:30 $2.80 → close $2.73 -65.03; QMLS×342 09:30 $7.24 → close $7.14 -34.20; CLBT×230 09:30 $11.19 → close $10.44 -172.50; YSS×247 09:30 $10.36 → close $10.66 +74.10; INV×1 09:30 $1.62 → close $1.39 -0.24 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.90 | ▼ 09:30 equity $9,800.00 vs yday $10,021.56 (-221.56) | 09:30 open · cash $7.90 (unchanged overnight, no fees) · equity $9,800.00 vs prior close $10,021.56 (-221.56) · 5 name(s) re-marked at the open (per-name table). NCMI×929 yday $2.73 → 09:30 $2.71 -18.58; QMLS×342 yday $7.14 → 09:30 $6.85 -99.18; CLBT×230 yday $10.44 → 09:30 $10.44 +0.00; YSS×247 yday $10.66 → 09:30 $10.24 -103.74; INV×1 yday $1.39 → 09:30 $1.32 -0.06 | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.90 | ▼ close $9,759.13 vs 09:30 $9,800.00 (session -40.87) | 16:00 close · cash $7.90 · equity $9,759.13 vs 09:30 $9,800.00 (-40.87; session marks -40.87) · 5 name(s) marked open→close (per-name table). NCMI×929 09:30 $2.71 → close $2.52 -176.51; QMLS×342 09:30 $6.85 → close $6.74 -37.62; CLBT×230 09:30 $10.44 → close $11.00 +128.80; YSS×247 09:30 $10.24 → close $10.42 +44.46; INV×1 09:30 $1.32 → close $1.32 +0.00 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.90 | ▼ 09:30 equity $9,737.15 vs yday $9,759.13 (-21.98) | 09:30 open · cash $7.90 (unchanged overnight, no fees) · equity $9,737.15 vs prior close $9,759.13 (-21.98) · 5 name(s) re-marked at the open (per-name table). NCMI×929 yday $2.52 → 09:30 $2.56 +37.16; QMLS×342 yday $6.74 → 09:30 $6.74 +0.00; CLBT×230 yday $11.00 → 09:30 $10.85 -34.50; YSS×247 yday $10.42 → 09:30 $10.32 -24.70; INV×1 yday $1.32 → 09:30 $1.39 +0.06 | — |
| 2026-08-19 09:30 ET | **SELL** | `NCMI` | 929 | $2.56 | $12.16 | $-144.91 | $2,373.98 | ▼ -144.91 after sell → book $9,724.99; vs 09:30 mark -12.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `QMLS` | 342 | $6.74 | $4.49 | $-197.00 | $4,674.58 | ▼ -197.00 after sell → book $9,720.51; vs 09:30 mark -4.48 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `CLBT` | 230 | $10.85 | $3.03 | $-1.39 | $7,167.05 | ▼ -1.39 after sell → book $9,717.48; vs 09:30 mark -3.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `YSS` | 247 | $10.32 | $3.25 | $+57.79 | $9,712.84 | ▲ +57.79 after sell → book $9,714.23; vs 09:30 mark -3.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,712.84 | ▲ close $9,714.38 vs 09:30 $9,737.15 (session +0.15) | 16:00 close · cash $9,712.84 · equity $9,714.38 vs 09:30 $9,737.15 (-22.77; session marks +0.15) · 1 name(s) marked open→close (per-name table). INV×1 09:30 $1.39 → close $1.54 +0.15 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,712.84 | ▲ 09:30 equity $9,714.39 vs yday $9,714.38 (+0.01) | 09:30 open · cash $9,712.84 (unchanged overnight, no fees) · equity $9,714.39 vs prior close $9,714.38 (+0.01) · 1 name(s) re-marked at the open (per-name table). INV×1 yday $1.54 → 09:30 $1.55 +0.01 | — |
| 2026-08-20 09:30 ET | **SELL** | `INV` | 1 | $1.55 | $0.04 | $-0.13 | $9,714.36 | ▼ -0.13 after sell → book $9,714.36; vs 09:30 mark -0.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `LZB` | 288 | $33.61 | $3.72 | — | $30.96 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-17.4; leftover $9714.36 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.96 | ▲ close $9,722.16 vs 09:30 $9,714.39 (session +11.52) | 16:00 close · cash $30.96 · equity $9,722.16 vs 09:30 $9,714.39 (+7.77; session marks +11.52) · 1 name(s) marked open→close (per-name table). LZB×288 09:30 $33.61 → close $33.65 +11.52 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.96 | ▼ 09:30 equity $9,716.40 vs yday $9,722.16 (-5.76) | 09:30 open · cash $30.96 (unchanged overnight, no fees) · equity $9,716.40 vs prior close $9,722.16 (-5.76) · 1 name(s) re-marked at the open (per-name table). LZB×288 yday $33.65 → 09:30 $33.63 -5.76 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.96 | ▲ close $9,739.44 vs 09:30 $9,716.40 (session +23.04) | 16:00 close · cash $30.96 · equity $9,739.44 vs 09:30 $9,716.40 (+23.04; session marks +23.04) · 1 name(s) marked open→close (per-name table). LZB×288 09:30 $33.63 → close $33.71 +23.04 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.96 | ▼ 09:30 equity $9,693.36 vs yday $9,739.44 (-46.08) | 09:30 open · cash $30.96 (unchanged overnight, no fees) · equity $9,693.36 vs prior close $9,739.44 (-46.08) · 1 name(s) re-marked at the open (per-name table). LZB×288 yday $33.71 → 09:30 $33.55 -46.08 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.96 | ▼ close $9,281.52 vs 09:30 $9,693.36 (session -411.84) | 16:00 close · cash $30.96 · equity $9,281.52 vs 09:30 $9,693.36 (-411.84; session marks -411.84) · 1 name(s) marked open→close (per-name table). LZB×288 09:30 $33.55 → close $32.12 -411.84 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.96 | ▲ 09:30 equity $9,342.00 vs yday $9,281.52 (+60.48) | 09:30 open · cash $30.96 (unchanged overnight, no fees) · equity $9,342.00 vs prior close $9,281.52 (+60.48) · 1 name(s) re-marked at the open (per-name table). LZB×288 yday $32.12 → 09:30 $32.33 +60.48 | — |
| 2026-08-25 09:30 ET | **SELL** | `LZB` | 288 | $32.33 | $3.84 | $-376.19 | $9,338.16 | ▼ -376.19 after sell → book $9,338.16; vs 09:30 mark -3.84 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `QFIN` | 421 | $11.09 | $5.43 | — | $4,663.84 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list overnight; 🔵; ret5=-8.0; leftover $4669.08 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `QMLS` | 784 | $5.93 | $10.11 | — | $4.61 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list overnight; 🔵; ret5=-17.5; leftover $4669.08 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.61 | ▲ close $9,774.42 vs 09:30 $9,342.00 (session +451.80) | 16:00 close · cash $4.61 · equity $9,774.42 vs 09:30 $9,342.00 (+432.42; session marks +451.80) · 2 name(s) marked open→close (per-name table). QFIN×421 09:30 $11.09 → close $11.53 +185.24; QMLS×784 09:30 $5.93 → close $6.27 +266.56 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.61 | ▼ 09:30 equity $9,186.05 vs yday $9,774.42 (-588.37) | 09:30 open · cash $4.61 (unchanged overnight, no fees) · equity $9,186.05 vs prior close $9,774.42 (-588.37) · 2 name(s) re-marked at the open (per-name table). QFIN×421 yday $11.53 → 09:30 $9.76 -745.17; QMLS×784 yday $6.27 → 09:30 $6.47 +156.80 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.61 | ▼ close $8,723.36 vs 09:30 $9,186.05 (session -462.69) | 16:00 close · cash $4.61 · equity $8,723.36 vs 09:30 $9,186.05 (-462.69; session marks -462.69) · 2 name(s) marked open→close (per-name table). QFIN×421 09:30 $9.76 → close $9.35 -172.61; QMLS×784 09:30 $6.47 → close $6.10 -290.08 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.61 | ▲ 09:30 equity $8,933.15 vs yday $8,723.36 (+209.79) | 09:30 open · cash $4.61 (unchanged overnight, no fees) · equity $8,933.15 vs prior close $8,723.36 (+209.79) · 2 name(s) re-marked at the open (per-name table). QFIN×421 yday $9.35 → 09:30 $9.42 +29.47; QMLS×784 yday $6.10 → 09:30 $6.33 +180.32 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.61 | ▲ close $8,937.66 vs 09:30 $8,933.15 (session +4.51) | 16:00 close · cash $4.61 · equity $8,937.66 vs 09:30 $8,933.15 (+4.51; session marks +4.51) · 2 name(s) marked open→close (per-name table). QFIN×421 09:30 $9.42 → close $9.17 -105.25; QMLS×784 09:30 $6.33 → close $6.47 +109.76 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.61 | ▼ 09:30 equity $8,772.44 vs yday $8,937.66 (-165.22) | 09:30 open · cash $4.61 (unchanged overnight, no fees) · equity $8,772.44 vs prior close $8,937.66 (-165.22) · 2 name(s) re-marked at the open (per-name table). QFIN×421 yday $9.17 → 09:30 $9.15 -8.42; QMLS×784 yday $6.47 → 09:30 $6.27 -156.80 | — |
| 2026-08-28 09:30 ET | **SELL** | `QMLS` | 784 | $6.27 | $10.28 | $+246.16 | $4,910.01 | ▲ +246.16 after sell → book $8,762.16; vs 09:30 mark -10.28 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `DY` | 8 | $306.34 | $2.01 | — | $2,457.27 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-23.0; leftover $2455.00 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LX` | 2094 | $1.16 | $27.01 | — | $1.22 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list overnight; ret5=-13.8; leftover $2455.00 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.22 | ▼ close $8,531.66 vs 09:30 $8,772.44 (session -201.47) | 16:00 close · cash $1.22 · equity $8,531.66 vs 09:30 $8,772.44 (-240.78; session marks -201.47) · 3 name(s) marked open→close (per-name table). QFIN×421 09:30 $9.15 → close $8.80 -147.35; DY×8 09:30 $306.34 → close $294.34 -96.00; LX×2094 09:30 $1.16 → close $1.18 +41.88 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.22 | ▼ 09:30 equity $8,162.94 vs yday $8,531.66 (-368.72) | 09:30 open · cash $1.22 (unchanged overnight, no fees) · equity $8,162.94 vs prior close $8,531.66 (-368.72) · 3 name(s) re-marked at the open (per-name table). QFIN×421 yday $8.80 → 09:30 $8.70 -42.10; DY×8 yday $294.34 → 09:30 $298.01 +29.36; LX×2094 yday $1.18 → 09:30 $1.01 -355.98 | — |
| 2026-08-31 09:30 ET | **SELL** | `QFIN` | 421 | $8.70 | $5.53 | $-1017.15 | $3,658.39 | ▼ -1,017.15 after sell → book $8,157.41; vs 09:30 mark -5.53 | dropped from list after 4 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,658.39 | ▲ close $8,228.65 vs 09:30 $8,162.94 (session +71.24) | 16:00 close · cash $3,658.39 · equity $8,228.65 vs 09:30 $8,162.94 (+65.71; session marks +71.24) · 2 name(s) marked open→close (per-name table). DY×8 09:30 $298.01 → close $291.21 -54.40; LX×2094 09:30 $1.01 → close $1.07 +125.64 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,658.39 | ▼ 09:30 equity $8,086.61 vs yday $8,228.65 (-142.04) | 09:30 open · cash $3,658.39 (unchanged overnight, no fees) · equity $8,086.61 vs prior close $8,228.65 (-142.04) · 2 name(s) re-marked at the open (per-name table). DY×8 yday $291.21 → 09:30 $289.16 -16.40; LX×2094 yday $1.07 → 09:30 $1.01 -125.64 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,658.39 | ▼ close $7,803.70 vs 09:30 $8,086.61 (session -282.91) | 16:00 close · cash $3,658.39 · equity $7,803.70 vs 09:30 $8,086.61 (-282.91; session marks -282.91) · 2 name(s) marked open→close (per-name table). DY×8 09:30 $289.16 → close $287.30 -14.88; LX×2094 09:30 $1.01 → close $0.88 -268.03 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,658.39 | ▲ 09:30 equity $7,859.47 vs yday $7,803.70 (+55.77) | 09:30 open · cash $3,658.39 (unchanged overnight, no fees) · equity $7,859.47 vs prior close $7,803.70 (+55.77) · 2 name(s) re-marked at the open (per-name table). DY×8 yday $287.30 → 09:30 $287.99 +5.52; LX×2094 yday $0.88 → 09:30 $0.91 +50.26 | — |
| 2026-09-02 09:30 ET | **SELL** | `DY` | 8 | $287.99 | $2.04 | $-150.86 | $5,960.27 | ▼ -150.86 after sell → book $7,857.43; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `LX` | 2094 | $0.91 | $25.62 | $-584.51 | $7,831.81 | ▼ -584.51 after sell → book $7,831.81; vs 09:30 mark -25.62 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,831.81 | ▲ close $7,831.81 vs 09:30 $7,859.47 (session +0.00) | 16:00 close · cash $7,831.81 · no lots left · equity $7,831.81. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,831.81 | ▲ 09:30 equity $7,831.81 vs yday $7,831.81 (+0.00) | 09:30 open · cash $7,831.81 · no holdings · equity $7,831.81 vs prior close $7,831.81 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `SION` | 133 | $7.31 | $2.39 | — | $6,857.20 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_gainer; 🔵; ret5=+18.5; leftover $978.98 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ALMS` | 94 | $10.38 | $2.27 | — | $5,879.67 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-56.2; leftover $978.98 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `LX` | 1183 | $0.83 | $13.33 | — | $4,888.00 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-30.4; leftover $978.98 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EVTL` | 1529 | $0.64 | $14.37 | — | $3,895.07 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-22.0; leftover $978.98 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FJET` | 344 | $2.84 | $4.44 | — | $2,913.67 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-26.9; leftover $978.98 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OSW` | 44 | $22.00 | $2.12 | — | $1,943.55 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-17.3; leftover $978.98 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PL` | 49 | $19.86 | $2.14 | — | $968.27 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list overnight; ret5=-5.5; leftover $978.98 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SWBI` | 75 | $12.78 | $2.21 | — | $7.56 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list overnight; 🔵; ret5=-4.4; leftover $978.98 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.56 | ▼ close $7,654.06 vs 09:30 $7,831.81 (session -134.47) | 16:00 close · cash $7.56 · equity $7,654.06 vs 09:30 $7,831.81 (-177.75; session marks -134.47) · 8 name(s) marked open→close (per-name table). SION×133 09:30 $7.31 → close $6.75 -74.48; ALMS×94 09:30 $10.38 → close $11.36 +92.59; LX×1183 09:30 $0.83 → close $0.85 +29.58; EVTL×1529 09:30 $0.64 → close $0.60 -61.16; FJET×344 09:30 $2.84 → close $2.78 -20.64; OSW×44 09:30 $22.00 → close $22.27 +11.88; PL×49 09:30 $19.86 → close $18.35 -73.99; SWBI×75 09:30 $12.78 → close $12.27 -38.25 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.56 | ▲ 09:30 equity $7,848.47 vs yday $7,654.06 (+194.41) | 09:30 open · cash $7.56 (unchanged overnight, no fees) · equity $7,848.47 vs prior close $7,654.06 (+194.41) · 8 name(s) re-marked at the open (per-name table). SION×133 yday $6.75 → 09:30 $6.68 -9.31; ALMS×94 yday $11.36 → 09:30 $11.23 -12.22; LX×1183 yday $0.85 → 09:30 $0.86 +7.10; EVTL×1529 yday $0.60 → 09:30 $0.60 +0.00; FJET×344 yday $2.78 → 09:30 $2.80 +6.88; OSW×44 yday $22.27 → 09:30 $22.27 +0.00; PL×49 yday $18.35 → 09:30 $19.64 +63.21; SWBI×75 yday $12.27 → 09:30 $14.12 +138.75 | — |
| 2026-09-04 09:30 ET | **BUY** | `AIIO` | 4 | $1.75 | $0.08 | — | $0.47 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-24.6; leftover $7.56 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.47 | ▼ close $7,767.35 vs 09:30 $7,848.47 (session -81.04) | 16:00 close · cash $0.47 · equity $7,767.35 vs 09:30 $7,848.47 (-81.12; session marks -81.04) · 9 name(s) marked open→close (per-name table). SION×133 09:30 $6.68 → close $7.18 +66.50; ALMS×94 09:30 $11.23 → close $11.10 -12.22; LX×1183 09:30 $0.86 → close $0.88 +28.39; EVTL×1529 09:30 $0.60 → close $0.60 -3.06; FJET×344 09:30 $2.80 → close $2.80 +0.00; OSW×44 09:30 $22.27 → close $22.40 +5.72; PL×49 09:30 $19.64 → close $18.12 -74.48; SWBI×75 09:30 $14.12 → close $12.89 -92.25; AIIO×4 09:30 $1.75 → close $1.84 +0.36 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.47 | ▼ 09:30 equity $7,730.61 vs yday $7,767.35 (-36.74) | 09:30 open · cash $0.47 (unchanged overnight, no fees) · equity $7,730.61 vs prior close $7,767.35 (-36.74) · 9 name(s) re-marked at the open (per-name table). SION×133 yday $7.18 → 09:30 $7.13 -6.65; ALMS×94 yday $11.10 → 09:30 $11.05 -4.70; LX×1183 yday $0.88 → 09:30 $0.90 +21.29; EVTL×1529 yday $0.60 → 09:30 $0.60 +0.00; FJET×344 yday $2.80 → 09:30 $2.80 +0.00; OSW×44 yday $22.40 → 09:30 $22.29 -4.84; PL×49 yday $18.12 → 09:30 $17.85 -13.23; SWBI×75 yday $12.89 → 09:30 $12.51 -28.50; AIIO×4 yday $1.84 → 09:30 $1.81 -0.12 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.47 | ▼ close $7,627.84 vs 09:30 $7,730.61 (session -102.76) | 16:00 close · cash $0.47 · equity $7,627.84 vs 09:30 $7,730.61 (-102.77; session marks -102.76) · 9 name(s) marked open→close (per-name table). SION×133 09:30 $7.13 → close $7.30 +22.61; ALMS×94 09:30 $11.05 → close $10.58 -44.18; LX×1183 09:30 $0.90 → close $0.85 -55.60; EVTL×1529 09:30 $0.60 → close $0.59 -12.23; FJET×344 09:30 $2.80 → close $2.63 -58.48; OSW×44 09:30 $22.29 → close $22.12 -7.48; PL×49 09:30 $17.85 → close $17.81 -1.96; SWBI×75 09:30 $12.51 → close $13.23 +54.00; AIIO×4 09:30 $1.81 → close $1.95 +0.56 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.47 | ▼ 09:30 equity $7,589.75 vs yday $7,627.84 (-38.09) | 09:30 open · cash $0.47 (unchanged overnight, no fees) · equity $7,589.75 vs prior close $7,627.84 (-38.09) · 9 name(s) re-marked at the open (per-name table). SION×133 yday $7.30 → 09:30 $7.27 -3.99; ALMS×94 yday $10.58 → 09:30 $10.49 -8.46; LX×1183 yday $0.85 → 09:30 $0.83 -21.29; EVTL×1529 yday $0.59 → 09:30 $0.59 +6.12; FJET×344 yday $2.63 → 09:30 $2.63 +0.00; OSW×44 yday $22.12 → 09:30 $21.86 -11.44; PL×49 yday $17.81 → 09:30 $18.00 +9.31; SWBI×75 yday $13.23 → 09:30 $13.12 -8.25; AIIO×4 yday $1.95 → 09:30 $1.93 -0.08 | — |
| 2026-09-09 09:30 ET | **SELL** | `SION` | 133 | $7.27 | $2.42 | $-10.13 | $964.96 | ▼ -10.13 after sell → book $7,587.33; vs 09:30 mark -2.42 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ALMS` | 94 | $10.49 | $2.30 | $+6.24 | $1,948.72 | ▲ +6.24 after sell → book $7,585.04; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `LX` | 1183 | $0.83 | $13.63 | $-17.50 | $2,922.90 | ▼ -17.50 after sell → book $7,571.40; vs 09:30 mark -13.64 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `EVTL` | 1529 | $0.59 | $13.93 | $-98.64 | $3,817.19 | ▼ -98.64 after sell → book $7,557.47; vs 09:30 mark -13.93 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FJET` | 344 | $2.63 | $4.50 | $-81.18 | $4,717.40 | ▼ -81.18 after sell → book $7,552.96; vs 09:30 mark -4.51 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `OSW` | 44 | $21.86 | $2.14 | $-10.42 | $5,677.10 | ▼ -10.42 after sell → book $7,550.82; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `PL` | 49 | $18.00 | $2.16 | $-95.43 | $6,556.95 | ▼ -95.43 after sell → book $7,548.67; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `SWBI` | 75 | $13.12 | $2.24 | $+21.05 | $7,538.71 | ▲ +21.05 after sell → book $7,546.43; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,538.71 | ▼ close $7,546.15 vs 09:30 $7,589.75 (session -0.28) | 16:00 close · cash $7,538.71 · equity $7,546.15 vs 09:30 $7,589.75 (-43.60; session marks -0.28) · 1 name(s) marked open→close (per-name table). AIIO×4 09:30 $1.93 → close $1.86 -0.28 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,538.71 | ▼ 09:30 equity $7,545.95 vs yday $7,546.15 (-0.20) | 09:30 open · cash $7,538.71 (unchanged overnight, no fees) · equity $7,545.95 vs prior close $7,546.15 (-0.20) · 1 name(s) re-marked at the open (per-name table). AIIO×4 yday $1.86 → 09:30 $1.81 -0.20 | — |
| 2026-09-10 09:30 ET | **SELL** | `AIIO` | 4 | $1.81 | $0.10 | $+0.05 | $7,545.84 | ▲ +0.05 after sell → book $7,545.84; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,545.84 | ▲ close $7,545.84 vs 09:30 $7,545.95 (session +0.00) | 16:00 close · cash $7,545.84 · no lots left · equity $7,545.84. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,545.84 | ▲ 09:30 equity $7,545.84 vs yday $7,545.84 (+0.00) | 09:30 open · cash $7,545.84 · no holdings · equity $7,545.84 vs prior close $7,545.84 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `NAVN` | 122 | $20.61 | $2.36 | — | $5,029.07 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-24.7; leftover $2515.28 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `RWT` | 714 | $3.52 | $9.21 | — | $2,506.58 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-19.2; leftover $2515.28 | join🔴 sector🔴 gen🟡 news🔴 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `COO` | 45 | $54.66 | $2.12 | — | $44.75 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-22.3; leftover $2515.28 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $44.75 | ▲ close $7,569.84 vs 09:30 $7,545.84 (session +37.69) | 16:00 close · cash $44.75 · equity $7,569.84 vs 09:30 $7,545.84 (+24.00; session marks +37.69) · 3 name(s) marked open→close (per-name table). NAVN×122 09:30 $20.61 → close $21.02 +50.02; RWT×714 09:30 $3.52 → close $3.55 +21.42; COO×45 09:30 $54.66 → close $53.91 -33.75 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $44.75 | ▲ 09:30 equity $7,604.47 vs yday $7,569.84 (+34.63) | 09:30 open · cash $44.75 (unchanged overnight, no fees) · equity $7,604.47 vs prior close $7,569.84 (+34.63) · 3 name(s) re-marked at the open (per-name table). NAVN×122 yday $21.02 → 09:30 $21.10 +9.76; RWT×714 yday $3.55 → 09:30 $3.53 -14.28; COO×45 yday $53.91 → 09:30 $54.78 +39.15 | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $44.75 | ▲ close $7,826.41 vs 09:30 $7,604.47 (session +221.94) | 16:00 close · cash $44.75 · equity $7,826.41 vs 09:30 $7,604.47 (+221.94; session marks +221.94) · 3 name(s) marked open→close (per-name table). NAVN×122 09:30 $21.10 → close $21.37 +32.94; RWT×714 09:30 $3.53 → close $3.83 +214.20; COO×45 09:30 $54.78 → close $54.22 -25.20 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $44.75 | ▼ 09:30 equity $7,806.99 vs yday $7,826.41 (-19.42) | 09:30 open · cash $44.75 (unchanged overnight, no fees) · equity $7,806.99 vs prior close $7,826.41 (-19.42) · 3 name(s) re-marked at the open (per-name table). NAVN×122 yday $21.37 → 09:30 $21.32 -6.10; RWT×714 yday $3.83 → 09:30 $3.80 -21.42; COO×45 yday $54.22 → 09:30 $54.40 +8.10 | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $44.75 | ▲ close $7,968.88 vs 09:30 $7,806.99 (session +161.89) | 16:00 close · cash $44.75 · equity $7,968.88 vs 09:30 $7,806.99 (+161.89; session marks +161.89) · 3 name(s) marked open→close (per-name table). NAVN×122 09:30 $21.32 → close $22.42 +134.20; RWT×714 09:30 $3.80 → close $3.91 +78.54; COO×45 09:30 $54.40 → close $53.27 -50.85 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $44.75 | ▲ 09:30 equity $8,078.12 vs yday $7,968.88 (+109.24) | 09:30 open · cash $44.75 (unchanged overnight, no fees) · equity $8,078.12 vs prior close $7,968.88 (+109.24) · 3 name(s) re-marked at the open (per-name table). NAVN×122 yday $22.42 → 09:30 $22.50 +9.76; RWT×714 yday $3.91 → 09:30 $3.98 +49.98; COO×45 yday $53.27 → 09:30 $54.37 +49.50 | — |
| 2026-09-16 09:30 ET | **SELL** | `NAVN` | 122 | $22.50 | $2.40 | $+225.83 | $2,787.35 | ▲ +225.83 after sell → book $8,075.72; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `RWT` | 714 | $3.98 | $9.35 | $+309.88 | $5,619.72 | ▲ +309.88 after sell → book $8,066.37; vs 09:30 mark -9.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `COO` | 45 | $54.37 | $2.15 | $-17.33 | $8,064.22 | ▼ -17.33 after sell → book $8,064.22; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `ALHC` | 97 | $10.30 | $2.28 | — | $7,062.84 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-23.0; leftover $1008.03 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `PLAY` | 146 | $6.86 | $2.43 | — | $6,058.85 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-22.4; leftover $1008.03 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `DVLT` | 6300 | $0.16 | $28.98 | — | $5,021.87 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-23.8; leftover $1008.03 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `NMRA` | 1194 | $0.84 | $13.66 | — | $4,000.47 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-33.5; leftover $1008.03 | join🔴 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `ZSQR` | 430 | $2.34 | $5.55 | — | $2,988.73 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-25.2; leftover $1008.03 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `CTMX` | 370 | $2.72 | $4.77 | — | $1,977.55 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-26.3; leftover $1008.03 | join🔴 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `CRBP` | 146 | $6.86 | $2.43 | — | $973.57 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-34.7; leftover $1008.03 | join🔴 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `EYPT` | 265 | $3.66 | $3.42 | — | $0.25 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-19.7; leftover $1008.03 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.25 | ▼ close $7,885.60 vs 09:30 $8,078.12 (session -115.10) | 16:00 close · cash $0.25 · equity $7,885.60 vs 09:30 $8,078.12 (-192.52; session marks -115.10) · 8 name(s) marked open→close (per-name table). ALHC×97 09:30 $10.30 → close $8.71 -154.23; PLAY×146 09:30 $6.86 → close $6.86 +0.00; DVLT×6300 09:30 $0.16 → close $0.18 +126.00; NMRA×1194 09:30 $0.84 → close $0.77 -84.77; ZSQR×430 09:30 $2.34 → close $2.29 -21.50; CTMX×370 09:30 $2.72 → close $2.77 +16.65; CRBP×146 09:30 $6.86 → close $7.26 +58.40; EYPT×265 09:30 $3.66 → close $3.45 -55.65 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.25 | ▲ 09:30 equity $7,914.60 vs yday $7,885.60 (+29.00) | 09:30 open · cash $0.25 (unchanged overnight, no fees) · equity $7,914.60 vs prior close $7,885.60 (+29.00) · 8 name(s) re-marked at the open (per-name table). ALHC×97 yday $8.71 → 09:30 $8.58 -12.61; PLAY×146 yday $6.86 → 09:30 $6.96 +14.60; DVLT×6300 yday $0.18 → 09:30 $0.17 -63.00; NMRA×1194 yday $0.77 → 09:30 $0.78 +8.36; ZSQR×430 yday $2.29 → 09:30 $2.35 +25.80; CTMX×370 yday $2.77 → 09:30 $2.83 +24.05; CRBP×146 yday $7.26 → 09:30 $7.26 +0.00; EYPT×265 yday $3.45 → 09:30 $3.57 +31.80 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.25 | ▲ close $7,955.40 vs 09:30 $7,914.60 (session +40.80) | 16:00 close · cash $0.25 · equity $7,955.40 vs 09:30 $7,914.60 (+40.80; session marks +40.80) · 8 name(s) marked open→close (per-name table). ALHC×97 09:30 $8.58 → close $8.70 +11.64; PLAY×146 09:30 $6.96 → close $6.54 -61.32; DVLT×6300 09:30 $0.17 → close $0.16 -63.00; NMRA×1194 09:30 $0.78 → close $0.75 -35.82; ZSQR×430 09:30 $2.35 → close $2.54 +81.70; CTMX×370 09:30 $2.83 → close $2.82 -3.70; CRBP×146 09:30 $7.26 → close $7.26 +0.00; EYPT×265 09:30 $3.57 → close $3.99 +111.30 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.25 | ▲ 09:30 equity $7,990.37 vs yday $7,955.40 (+34.97) | 09:30 open · cash $0.25 (unchanged overnight, no fees) · equity $7,990.37 vs prior close $7,955.40 (+34.97) · 8 name(s) re-marked at the open (per-name table). ALHC×97 yday $8.70 → 09:30 $8.68 -1.94; PLAY×146 yday $6.54 → 09:30 $6.64 +14.60; DVLT×6300 yday $0.16 → 09:30 $0.17 +63.00; NMRA×1194 yday $0.75 → 09:30 $0.74 -10.75; ZSQR×430 yday $2.54 → 09:30 $2.50 -17.20; CTMX×370 yday $2.82 → 09:30 $2.83 +3.70; CRBP×146 yday $7.26 → 09:30 $7.22 -5.84; EYPT×265 yday $3.99 → 09:30 $3.95 -10.60 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.25 | ▼ close $7,895.33 vs 09:30 $7,990.37 (session -95.04) | 16:00 close · cash $0.25 · equity $7,895.33 vs 09:30 $7,990.37 (-95.04; session marks -95.04) · 8 name(s) marked open→close (per-name table). ALHC×97 09:30 $8.68 → close $8.35 -32.01; PLAY×146 09:30 $6.64 → close $6.73 +13.14; DVLT×6300 09:30 $0.17 → close $0.15 -126.00; NMRA×1194 09:30 $0.74 → close $0.73 -13.13; ZSQR×430 09:30 $2.50 → close $2.68 +77.40; CTMX×370 09:30 $2.83 → close $2.76 -25.90; CRBP×146 09:30 $7.22 → close $7.48 +37.96; EYPT×265 09:30 $3.95 → close $3.85 -26.50 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.25 | ▲ 09:30 equity $7,981.92 vs yday $7,895.33 (+86.59) | 09:30 open · cash $0.25 (unchanged overnight, no fees) · equity $7,981.92 vs prior close $7,895.33 (+86.59) · 8 name(s) re-marked at the open (per-name table). ALHC×97 yday $8.35 → 09:30 $8.33 -1.94; PLAY×146 yday $6.73 → 09:30 $6.68 -7.30; DVLT×6300 yday $0.15 → 09:30 $0.16 +63.00; NMRA×1194 yday $0.73 → 09:30 $0.74 +8.36; ZSQR×430 yday $2.68 → 09:30 $2.68 +0.00; CTMX×370 yday $2.76 → 09:30 $2.80 +14.80; CRBP×146 yday $7.48 → 09:30 $7.51 +4.38; EYPT×265 yday $3.85 → 09:30 $3.87 +5.30 | — |
| 2026-09-21 09:30 ET | **SELL** | `ALHC` | 97 | $8.33 | $2.31 | $-195.68 | $805.95 | ▼ -195.68 after sell → book $7,979.62; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `PLAY` | 146 | $6.68 | $2.46 | $-31.17 | $1,778.77 | ▼ -31.17 after sell → book $7,977.16; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `DVLT` | 6300 | $0.16 | $30.04 | $-59.02 | $2,756.73 | ▼ -59.02 after sell → book $7,947.12; vs 09:30 mark -30.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `NMRA` | 1194 | $0.74 | $12.59 | $-154.01 | $3,624.12 | ▼ -154.01 after sell → book $7,934.53; vs 09:30 mark -12.59 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ZSQR` | 430 | $2.68 | $5.63 | $+135.02 | $4,770.89 | ▲ +135.02 after sell → book $7,928.90; vs 09:30 mark -5.63 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `CTMX` | 370 | $2.80 | $4.84 | $+19.98 | $5,802.05 | ▲ +19.98 after sell → book $7,924.06; vs 09:30 mark -4.84 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `CRBP` | 146 | $7.51 | $2.46 | $+90.01 | $6,896.04 | ▲ +90.01 after sell → book $7,921.59; vs 09:30 mark -2.47 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `EYPT` | 265 | $3.87 | $3.47 | $+48.76 | $7,918.12 | ▲ +48.76 after sell → book $7,918.12; vs 09:30 mark -3.47 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `XENE` | 65 | $40.00 | $2.19 | — | $5,315.94 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-32.2; leftover $2639.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `SION` | 439 | $6.00 | $5.66 | — | $2,676.27 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-24.1; leftover $2639.37 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `THO` | 38 | $68.39 | $2.10 | — | $75.35 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list overnight; ret5=-7.0; leftover $2639.37 | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $75.35 | ▼ close $7,901.42 vs 09:30 $7,981.92 (session -6.75) | 16:00 close · cash $75.35 · equity $7,901.42 vs 09:30 $7,981.92 (-80.50; session marks -6.75) · 3 name(s) marked open→close (per-name table). XENE×65 09:30 $40.00 → close $38.99 -65.65; SION×439 09:30 $6.00 → close $6.00 +0.00; THO×38 09:30 $68.39 → close $69.94 +58.90 | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $75.35 | ▲ 09:30 equity $7,930.78 vs yday $7,901.42 (+29.36) | 09:30 open · cash $75.35 (unchanged overnight, no fees) · equity $7,930.78 vs prior close $7,901.42 (+29.36) · 3 name(s) re-marked at the open (per-name table). XENE×65 yday $38.99 → 09:30 $39.10 +7.15; SION×439 yday $6.00 → 09:30 $5.99 -4.39; THO×38 yday $69.94 → 09:30 $70.64 +26.60 | — |
| 2026-09-22 09:30 ET | **BUY** | `FJET` | 36 | $2.03 | $0.84 | — | $1.43 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list probable,yday_gainer,yday_mover; ret5=+0.5; leftover $75.35 | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.43 | ▼ close $7,886.58 vs 09:30 $7,930.78 (session -43.36) | 16:00 close · cash $1.43 · equity $7,886.58 vs 09:30 $7,930.78 (-44.20; session marks -43.36) · 4 name(s) marked open→close (per-name table). XENE×65 09:30 $39.10 → close $38.61 -31.85; SION×439 09:30 $5.99 → close $6.02 +13.17; THO×38 09:30 $70.64 → close $70.00 -24.32; FJET×36 09:30 $2.03 → close $2.02 -0.36 | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.43 | ▲ 09:30 equity $8,001.61 vs yday $7,886.58 (+115.03) | 09:30 open · cash $1.43 (unchanged overnight, no fees) · equity $8,001.61 vs prior close $7,886.58 (+115.03) · 4 name(s) re-marked at the open (per-name table). XENE×65 yday $38.61 → 09:30 $39.51 +58.50; SION×439 yday $6.02 → 09:30 $6.03 +4.39; THO×38 yday $70.00 → 09:30 $71.41 +53.58; FJET×36 yday $2.02 → 09:30 $1.98 -1.44 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.43 | ▼ close $7,611.29 vs 09:30 $8,001.61 (session -390.32) | 16:00 close · cash $1.43 · equity $7,611.29 vs 09:30 $8,001.61 (-390.32; session marks -390.32) · 4 name(s) marked open→close (per-name table). XENE×65 09:30 $39.51 → close $36.56 -191.75; SION×439 09:30 $6.03 → close $5.51 -228.28; THO×38 09:30 $71.41 → close $72.30 +33.82; FJET×36 09:30 $1.98 → close $1.87 -4.11 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `NCMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `QMLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `CLBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `YSS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `CDNL` | cash | leftover split 2.39 < 1 share @ 39.85 |
| 2026-08-17 | `KLC` | cash | leftover split 2.39 < 1 share @ 2.62 |
| 2026-08-17 | `CSAN` | cash | leftover split 2.39 < 1 share @ 2.50 |
| 2026-08-18 | `NCMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `QMLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `CLBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `YSS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `INV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `EYPT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CIG` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `INV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `LZB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AAP` | cash | leftover split 15.48 < 1 share @ 42.41 |
| 2026-08-21 | `WMT` | cash | leftover split 15.48 < 1 share @ 103.69 |
| 2026-08-24 | `LZB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `DKS` | cash | leftover split 4.61 < 1 share @ 121.87 |
| 2026-08-27 | `DKS` | cash | leftover split 4.61 < 1 share @ 128.73 |
| 2026-08-31 | `DY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-01 | `DY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `LX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-04 | `ALMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `LX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `EVTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `FJET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `OSW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `PL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SION` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ALMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `LX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `EVTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FJET` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `OSW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `PL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SWBI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `AIIO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SUNB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `AIIO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `DYN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NAMS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `TTAN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `FJET` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OCGN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `XPOF` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `NAVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `RWT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `COO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `NAVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `RWT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `COO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `TCOM` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `PLAY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `DVLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `NMRA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ZSQR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `CTMX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `CRBP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `EYPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `MRLN` | cash | leftover split 0.25 < 1 share @ 2.27 |
| 2026-09-18 | `ALHC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `PLAY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `DVLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `NMRA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ZSQR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `CTMX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `CRBP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `EYPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RARE` | cash | leftover split 0.12 < 1 share @ 14.79 |
| 2026-09-18 | `FLNC` | cash | leftover split 0.12 < 1 share @ 7.54 |
| 2026-09-22 | `XENE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SION` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `THO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `XENE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SION` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `THO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `FJET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `NMRA` | cash | leftover split 0.36 < 1 share @ 0.76 |
| 2026-09-23 | `CMPX` | cash | leftover split 0.36 < 1 share @ 1.22 |
| 2026-09-23 | `XNDU` | cash | leftover split 0.36 < 1 share @ 5.99 |
| 2026-09-23 | `EVER` | cash | leftover split 0.36 < 1 share @ 19.46 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `XENE` | 65 | 2026-09-21 @ $40.00 | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-32.2; leftover $2639.37 |
| `SION` | 439 | 2026-09-21 @ $6.00 | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-24.1; leftover $2639.37 |
| `THO` | 38 | 2026-09-21 @ $68.39 | union ∩ rsi_os, no 🚨; gate rsi_os=True; list overnight; ret5=-7.0; leftover $2639.37 |
| `FJET` | 36 | 2026-09-22 @ $2.03 | union ∩ rsi_os, no 🚨; gate rsi_os=True; list probable,yday_gainer,yday_mover; ret5=+0.5; leftover $75.35 |
