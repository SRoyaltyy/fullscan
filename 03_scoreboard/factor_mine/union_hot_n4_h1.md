# Factor mine action — `union_hot_n4_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `hot_score` · size `leftover` · sell `list` · S-boost `none` · top 4 by hot

Cash book **+17.40%** ($11,740) · signal-only (no cash/fees) was +30.26%. Starts YES **10/19**. Fills 80 · skips 30 · realized $+1740.08.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written and skip anything on the must-not list. They take up to 4 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how hot the prior tape looked.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is thrown out if any must-not is true.
- Sort the keepers by how hot the prior tape looked and keep the top 4.
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
- **Gate** `none (list as ranked)` · **rank** `hot_score` · **top_n** 4.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $11,740.07.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `IREN` | 54 | — | $45.98 | +0.00 | $44.76 | -65.88 | -65.88 | +0.00 | -65.88 |
| 2026-08-13 | `TNDM` | 107 | — | $23.33 | +0.00 | $23.13 | -21.40 | -21.40 | +0.00 | -21.40 |
| 2026-08-13 | `TPG` | 49 | — | $50.62 | +0.00 | $54.62 | +195.84 | +195.84 | +0.00 | +195.84 |
| 2026-08-13 | `INO` | 3085 | — | $0.81 | +0.00 | $0.90 | +277.65 | +277.65 | +0.00 | +277.65 |
| 2026-08-14 | `IREN` | 54 | $44.76 | $44.09 | -36.18 | — | +0.00 | -36.18 | -102.06 | — |
| 2026-08-14 | `TNDM` | 107 | $23.13 | $22.92 | -22.47 | — | +0.00 | -22.47 | -43.87 | — |
| 2026-08-14 | `TPG` | 49 | $54.62 | $55.29 | +32.83 | — | +0.00 | +32.83 | +228.67 | — |
| 2026-08-14 | `INO` | 3085 | $0.90 | $0.93 | +92.55 | — | +0.00 | +92.55 | +370.20 | — |
| 2026-08-14 | `QMCO` | 105 | — | $24.68 | +0.00 | $26.11 | +150.15 | +150.15 | +0.00 | +150.15 |
| 2026-08-14 | `ARX` | 132 | — | $19.57 | +0.00 | $19.58 | +1.32 | +1.32 | +0.00 | +1.32 |
| 2026-08-14 | `ZENA` | 1178 | — | $2.20 | +0.00 | $2.14 | -70.68 | -70.68 | +0.00 | -70.68 |
| 2026-08-14 | `AIRO` | 231 | — | $11.12 | +0.00 | $9.57 | -358.05 | -358.05 | +0.00 | -358.05 |
| 2026-08-17 | `QMCO` | 105 | $26.11 | $24.83 | -134.40 | — | +0.00 | -134.40 | +15.75 | — |
| 2026-08-17 | `ARX` | 132 | $19.58 | $19.57 | -1.32 | — | +0.00 | -1.32 | +0.00 | — |
| 2026-08-17 | `ZENA` | 1178 | $2.14 | $2.08 | -64.79 | — | +0.00 | -64.79 | -135.47 | — |
| 2026-08-17 | `AIRO` | 231 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -358.05 | — |
| 2026-08-17 | `XHG` | 587 | — | $4.19 | +0.00 | $3.91 | -164.36 | -164.36 | +0.00 | -164.36 |
| 2026-08-17 | `CAPR` | 358 | — | $6.87 | +0.00 | $7.45 | +207.64 | +207.64 | +0.00 | +207.64 |
| 2026-08-17 | `STDN` | 180 | — | $13.64 | +0.00 | $13.31 | -59.40 | -59.40 | +0.00 | -59.40 |
| 2026-08-17 | `HTFL` | 59 | — | $41.23 | +0.00 | $41.94 | +41.89 | +41.89 | +0.00 | +41.89 |
| 2026-08-18 | `XHG` | 587 | $3.91 | $3.94 | +17.61 | — | +0.00 | +17.61 | -146.75 | — |
| 2026-08-18 | `CAPR` | 358 | $7.45 | $7.50 | +17.90 | $7.08 | -150.36 | -132.46 | +225.54 | +75.18 |
| 2026-08-18 | `STDN` | 180 | $13.31 | $13.31 | +0.00 | — | +0.00 | +0.00 | -59.40 | — |
| 2026-08-18 | `HTFL` | 59 | $41.94 | $41.50 | -25.96 | — | +0.00 | -25.96 | +15.93 | — |
| 2026-08-19 | `CAPR` | 358 | $7.08 | $7.19 | +39.38 | — | +0.00 | +39.38 | +114.56 | — |
| 2026-08-20 | `MRNA` | 16 | — | $150.14 | +0.00 | $133.32 | -269.12 | -269.12 | +0.00 | -269.12 |
| 2026-08-20 | `CYPH` | 2115 | — | $1.15 | +0.00 | $1.19 | +84.60 | +84.60 | +0.00 | +84.60 |
| 2026-08-20 | `ABCL` | 205 | — | $11.81 | +0.00 | $11.57 | -50.22 | -50.22 | +0.00 | -50.22 |
| 2026-08-20 | `AZI` | 1767 | — | $1.37 | +0.00 | $1.44 | +123.69 | +123.69 | +0.00 | +123.69 |
| 2026-08-21 | `MRNA` | 16 | $133.32 | $133.11 | -3.36 | $145.13 | +192.32 | +188.96 | -272.48 | -80.16 |
| 2026-08-21 | `CYPH` | 2115 | $1.19 | $1.32 | +274.95 | $1.42 | +211.50 | +486.45 | +359.55 | +571.05 |
| 2026-08-21 | `ABCL` | 205 | $11.57 | $11.57 | +0.00 | — | +0.00 | +0.00 | -50.22 | — |
| 2026-08-21 | `AZI` | 1767 | $1.44 | $1.46 | +35.34 | — | +0.00 | +35.34 | +159.03 | — |
| 2026-08-21 | `XHG` | 548 | — | $4.49 | +0.00 | $4.41 | -43.84 | -43.84 | +0.00 | -43.84 |
| 2026-08-21 | `CAPR` | 360 | — | $6.81 | +0.00 | $6.29 | -187.20 | -187.20 | +0.00 | -187.20 |
| 2026-08-24 | `MRNA` | 16 | $145.13 | $142.70 | -38.88 | — | +0.00 | -38.88 | -119.04 | — |
| 2026-08-24 | `CYPH` | 2115 | $1.42 | $1.83 | +867.15 | — | +0.00 | +867.15 | +1438.20 | — |
| 2026-08-24 | `XHG` | 548 | $4.41 | $4.32 | -49.32 | — | +0.00 | -49.32 | -93.16 | — |
| 2026-08-24 | `CAPR` | 360 | $6.29 | $8.03 | +626.40 | — | +0.00 | +626.40 | +439.20 | — |
| 2026-08-25 | `REAX` | 117 | — | $24.11 | +0.00 | $28.43 | +505.44 | +505.44 | +0.00 | +505.44 |
| 2026-08-25 | `CYPH` | 1822 | — | $1.56 | +0.00 | $1.64 | +145.76 | +145.76 | +0.00 | +145.76 |
| 2026-08-25 | `XHG` | 698 | — | $4.07 | +0.00 | $4.02 | -34.90 | -34.90 | +0.00 | -34.90 |
| 2026-08-25 | `ASST` | 148 | — | $19.04 | +0.00 | $21.39 | +347.80 | +347.80 | +0.00 | +347.80 |
| 2026-08-26 | `REAX` | 117 | $28.43 | $26.61 | -212.94 | — | +0.00 | -212.94 | +292.50 | — |
| 2026-08-26 | `CYPH` | 1822 | $1.64 | $1.60 | -72.88 | — | +0.00 | -72.88 | +72.88 | — |
| 2026-08-26 | `XHG` | 698 | $4.02 | $3.81 | -146.58 | $4.06 | +174.50 | +27.92 | -181.48 | -6.98 |
| 2026-08-26 | `ASST` | 148 | $21.39 | $20.72 | -99.16 | — | +0.00 | -99.16 | +248.64 | — |
| 2026-08-26 | `BYND` | 214 | — | $14.11 | +0.00 | $14.25 | +29.96 | +29.96 | +0.00 | +29.96 |
| 2026-08-26 | `USDE` | 520 | — | $5.81 | +0.00 | $5.98 | +88.40 | +88.40 | +0.00 | +88.40 |
| 2026-08-26 | `SUJA` | 322 | — | $9.39 | +0.00 | $9.44 | +16.10 | +16.10 | +0.00 | +16.10 |
| 2026-08-27 | `XHG` | 698 | $4.06 | $4.06 | +0.00 | — | +0.00 | +0.00 | -6.98 | — |
| 2026-08-27 | `BYND` | 214 | $14.25 | $14.20 | -10.70 | — | +0.00 | -10.70 | +19.26 | — |
| 2026-08-27 | `USDE` | 520 | $5.98 | $6.50 | +270.40 | — | +0.00 | +270.40 | +358.80 | — |
| 2026-08-27 | `SUJA` | 322 | $9.44 | $9.41 | -9.66 | — | +0.00 | -9.66 | +6.44 | — |
| 2026-08-27 | `SLI` | 1179 | — | $2.60 | +0.00 | $2.64 | +47.16 | +47.16 | +0.00 | +47.16 |
| 2026-08-27 | `RRC` | 73 | — | $41.44 | +0.00 | $41.64 | +14.60 | +14.60 | +0.00 | +14.60 |
| 2026-08-27 | `PGY` | 133 | — | $22.93 | +0.00 | $23.26 | +43.89 | +43.89 | +0.00 | +43.89 |
| 2026-08-27 | `CRK` | 212 | — | $14.42 | +0.00 | $14.62 | +42.40 | +42.40 | +0.00 | +42.40 |
| 2026-08-28 | `SLI` | 1179 | $2.64 | $2.68 | +47.16 | — | +0.00 | +47.16 | +94.32 | — |
| 2026-08-28 | `RRC` | 73 | $41.64 | $41.74 | +7.30 | — | +0.00 | +7.30 | +21.90 | — |
| 2026-08-28 | `PGY` | 133 | $23.26 | $23.21 | -6.65 | — | +0.00 | -6.65 | +37.24 | — |
| 2026-08-28 | `CRK` | 212 | $14.62 | $14.63 | +2.12 | — | +0.00 | +2.12 | +44.52 | — |
| 2026-08-28 | `BYND` | 221 | — | $14.00 | +0.00 | $13.86 | -30.94 | -30.94 | +0.00 | -30.94 |
| 2026-08-28 | `CAPR` | 318 | — | $9.73 | +0.00 | $9.59 | -44.52 | -44.52 | +0.00 | -44.52 |
| 2026-08-28 | `MRNA` | 22 | — | $137.19 | +0.00 | $137.99 | +17.60 | +17.60 | +0.00 | +17.60 |
| 2026-08-28 | `ANF` | 21 | — | $146.07 | +0.00 | $148.42 | +49.35 | +49.35 | +0.00 | +49.35 |
| 2026-08-31 | `BYND` | 221 | $13.86 | $13.81 | -11.05 | $13.30 | -112.71 | -123.76 | -41.99 | -154.70 |
| 2026-08-31 | `CAPR` | 318 | $9.59 | $9.50 | -28.62 | — | +0.00 | -28.62 | -73.14 | — |
| 2026-08-31 | `MRNA` | 22 | $137.99 | $134.10 | -85.58 | — | +0.00 | -85.58 | -67.98 | — |
| 2026-08-31 | `ANF` | 21 | $148.42 | $148.03 | -8.19 | — | +0.00 | -8.19 | +41.16 | — |
| 2026-09-01 | `BYND` | 221 | $13.30 | $13.04 | -57.46 | — | +0.00 | -57.46 | -212.16 | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `GPRO` | 1696 | — | $1.78 | +0.00 | $1.39 | -661.44 | -661.44 | +0.00 | -661.44 |
| 2026-09-03 | `REAX` | 164 | — | $18.40 | +0.00 | $18.40 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `CNH` | 220 | — | $13.71 | +0.00 | $13.84 | +28.60 | +28.60 | +0.00 | +28.60 |
| 2026-09-03 | `MMED` | 125 | — | $23.88 | +0.00 | $23.84 | -5.00 | -5.00 | +0.00 | -5.00 |
| 2026-09-04 | `GPRO` | 1696 | $1.39 | $1.48 | +152.64 | $1.70 | +373.12 | +525.76 | -508.80 | -135.68 |
| 2026-09-04 | `REAX` | 164 | $18.40 | $18.15 | -41.00 | — | +0.00 | -41.00 | -41.00 | — |
| 2026-09-04 | `CNH` | 220 | $13.84 | $13.89 | +11.00 | — | +0.00 | +11.00 | +39.60 | — |
| 2026-09-04 | `MMED` | 125 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | -5.00 | — |
| 2026-09-04 | `ASST` | 119 | — | $25.18 | +0.00 | $27.14 | +233.24 | +233.24 | +0.00 | +233.24 |
| 2026-09-04 | `USDE` | 381 | — | $7.87 | +0.00 | $7.93 | +22.86 | +22.86 | +0.00 | +22.86 |
| 2026-09-04 | `DFDV` | 519 | — | $5.79 | +0.00 | $5.87 | +41.52 | +41.52 | +0.00 | +41.52 |
| 2026-09-08 | `GPRO` | 1696 | $1.70 | $1.56 | -228.96 | — | +0.00 | -228.96 | -364.64 | — |
| 2026-09-08 | `ASST` | 119 | $27.14 | $26.44 | -83.30 | — | +0.00 | -83.30 | +149.94 | — |
| 2026-09-08 | `USDE` | 381 | $7.93 | $7.76 | -64.77 | — | +0.00 | -64.77 | -41.91 | — |
| 2026-09-08 | `DFDV` | 519 | $5.87 | $5.81 | -31.14 | — | +0.00 | -31.14 | +10.38 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +386.21 | IREN, TNDM, TPG, INO | — | $0.54 | $10,345.37 | IREN×54, TNDM×107, TPG×49, INO×3085 |
| 2026-08-14 | +5.50 | $0.54 | IREN×54, TNDM×107, TPG×49, INO×3085 | $10,412.10 | +66.73 | -277.26 | QMCO, ARX, ZENA, AIRO | IREN, TNDM, TPG, INO | $9.09 | $10,066.79 | QMCO×105, ARX×132, ZENA×1178, AIRO×231 |
| 2026-08-17 | +2.25 | $9.09 | QMCO×105, ARX×132, ZENA×1178, AIRO×231 | $9,866.28 | -200.51 | +25.77 | XHG, CAPR, STDN, HTFL | QMCO, ARX, ZENA, AIRO | $19.42 | $9,851.95 | XHG×587, CAPR×358, STDN×180, HTFL×59 |
| 2026-08-18 | -6.20 | $19.42 | XHG×587, CAPR×358, STDN×180, HTFL×59 | $9,861.50 | +9.55 | -150.36 | — | XHG, STDN, HTFL | $7,164.03 | $9,698.67 | CAPR×358 |
| 2026-08-19 | -7.20 | $7,164.03 | CAPR×358 | $9,738.05 | +39.38 | +0.00 | — | CAPR | $9,733.36 | $9,733.36 | — |
| 2026-08-20 | +1.12 | $9,733.36 | — | $9,733.36 | -0.00 | -111.05 | MRNA, CYPH, ABCL, AZI | — | $1.24 | $9,567.54 | MRNA×16, CYPH×2115, ABCL×205, AZI×1767 |
| 2026-08-21 | +3.25 | $1.24 | MRNA×16, CYPH×2115, ABCL×205, AZI×1767 | $9,874.47 | +306.93 | +172.78 | XHG, CAPR | ABCL, AZI | $3.27 | $10,009.73 | MRNA×16, CYPH×2115, XHG×548, CAPR×360 |
| 2026-08-24 | -5.17 | $3.27 | MRNA×16, CYPH×2115, XHG×548, CAPR×360 | $11,415.08 | +1,405.35 | +0.00 | — | MRNA, CYPH, XHG, CAPR | $11,373.44 | $11,373.44 | — |
| 2026-08-25 | +1.80 | $11,373.44 | — | $11,373.44 | +0.00 | +964.10 | REAX, CYPH, XHG, ASST | — | $14.19 | $12,300.26 | REAX×117, CYPH×1822, XHG×698, ASST×148 |
| 2026-08-26 | +2.02 | $14.19 | REAX×117, CYPH×1822, XHG×698, ASST×148 | $11,768.70 | -531.56 | +308.96 | BYND, USDE, SUJA | REAX, CYPH, ASST | $2.68 | $12,035.34 | XHG×698, BYND×214, USDE×520, SUJA×322 |
| 2026-08-27 | — | $2.68 | XHG×698, BYND×214, USDE×520, SUJA×322 | $12,285.38 | +250.04 | +148.05 | SLI, RRC, PGY, CRK | XHG, BYND, USDE, SUJA | $42.57 | $12,387.87 | SLI×1179, RRC×73, PGY×133, CRK×212 |
| 2026-08-28 | +0.75 | $42.57 | SLI×1179, RRC×73, PGY×133, CRK×212 | $12,437.80 | +49.93 | -8.51 | BYND, CAPR, MRNA, ANF | SLI, RRC, PGY, CRK | $130.04 | $12,395.32 | BYND×221, CAPR×318, MRNA×22, ANF×21 |
| 2026-08-31 | -5.85 | $130.04 | BYND×221, CAPR×318, MRNA×22, ANF×21 | $12,261.88 | -133.44 | -112.71 | — | CAPR, MRNA, ANF | $9,201.52 | $12,140.82 | BYND×221 |
| 2026-09-01 | -6.30 | $9,201.52 | BYND×221 | $12,083.36 | -57.46 | +0.00 | — | BYND | $12,080.45 | $12,080.45 | — |
| 2026-09-02 | -3.83 | $12,080.45 | — | $12,080.45 | -0.00 | +0.00 | — | — | $12,080.45 | $12,080.45 | — |
| 2026-09-03 | -0.90 | $12,080.45 | — | $12,080.45 | -0.00 | -637.84 | GPRO, REAX, CNH, MMED | — | $13.20 | $11,413.04 | GPRO×1696, REAX×164, CNH×220, MMED×125 |
| 2026-09-04 | +2.25 | $13.20 | GPRO×1696, REAX×164, CNH×220, MMED×125 | $11,535.68 | +122.64 | +670.74 | ASST, USDE, DFDV | REAX, CNH, MMED | $3.90 | $12,184.62 | GPRO×1696, ASST×119, USDE×381, DFDV×519 |
| 2026-09-08 | -11.47 | $3.90 | GPRO×1696, ASST×119, USDE×381, DFDV×519 | $11,776.45 | -408.17 | +0.00 | — | GPRO, ASST, USDE, DFDV | $11,740.07 | $11,740.07 | — |
| 2026-09-09 | -13.95 | $11,740.07 | — | $11,740.07 | +0.00 | +0.00 | — | — | $11,740.07 | $11,740.07 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 54 | $45.98 | $2.15 | — | $7,514.93 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+12.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 107 | $23.33 | $2.31 | — | $5,016.31 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+19.7; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 49 | $50.62 | $2.14 | — | $2,533.63 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+6.2; leftover $2500.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 3085 | $0.81 | $34.24 | — | $0.54 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+13.2; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.54 | ▲ close $10,345.37 vs 09:30 $10,000.00 (session +386.21) | 16:00 close · cash $0.54 · equity $10,345.37 vs 09:30 $10,000.00 (+345.37; session marks +386.21) · 4 name(s) marked open→close (per-name table). IREN×54 09:30 $45.98 → close $44.76 -65.88; TNDM×107 09:30 $23.33 → close $23.13 -21.40; TPG×49 09:30 $50.62 → close $54.62 +195.84; INO×3085 09:30 $0.81 → close $0.90 +277.65 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.54 | ▲ 09:30 equity $10,412.10 vs yday $10,345.37 (+66.73) | 09:30 open · cash $0.54 (unchanged overnight, no fees) · equity $10,412.10 vs prior close $10,345.37 (+66.73) · 4 name(s) re-marked at the open (per-name table). IREN×54 yday $44.76 → 09:30 $44.09 -36.18; TNDM×107 yday $23.13 → 09:30 $22.92 -22.47; TPG×49 yday $54.62 → 09:30 $55.29 +32.83; INO×3085 yday $0.90 → 09:30 $0.93 +92.55 | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 54 | $44.09 | $2.18 | $-106.39 | $2,379.22 | ▼ -106.39 after sell → book $10,409.92; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 107 | $22.92 | $2.35 | $-48.53 | $4,829.31 | ▼ -48.53 after sell → book $10,407.57; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 49 | $55.29 | $2.17 | $+224.37 | $7,536.35 | ▲ +224.37 after sell → book $10,405.40; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 3085 | $0.93 | $38.48 | $+297.48 | $10,366.92 | ▲ +297.48 after sell → book $10,366.92; vs 09:30 mark -38.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 105 | $24.68 | $2.31 | — | $7,773.22 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+111.3; leftover $2591.73 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 132 | $19.57 | $2.39 | — | $5,187.59 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $2591.73 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 1178 | $2.20 | $15.20 | — | $2,580.79 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $2591.73 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 231 | $11.12 | $2.98 | — | $9.09 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $2591.73 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.09 | ▼ close $10,066.79 vs 09:30 $10,412.10 (session -277.26) | 16:00 close · cash $9.09 · equity $10,066.79 vs 09:30 $10,412.10 (-345.31; session marks -277.26) · 4 name(s) marked open→close (per-name table). QMCO×105 09:30 $24.68 → close $26.11 +150.15; ARX×132 09:30 $19.57 → close $19.58 +1.32; ZENA×1178 09:30 $2.20 → close $2.14 -70.68; AIRO×231 09:30 $11.12 → close $9.57 -358.05 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.09 | ▼ 09:30 equity $9,866.28 vs yday $10,066.79 (-200.51) | 09:30 open · cash $9.09 (unchanged overnight, no fees) · equity $9,866.28 vs prior close $10,066.79 (-200.51) · 4 name(s) re-marked at the open (per-name table). QMCO×105 yday $26.11 → 09:30 $24.83 -134.40; ARX×132 yday $19.58 → 09:30 $19.57 -1.32; ZENA×1178 yday $2.14 → 09:30 $2.08 -64.79; AIRO×231 yday $9.57 → 09:30 $9.57 +0.00 | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 105 | $24.83 | $2.34 | $+11.10 | $2,613.90 | ▲ +11.10 after sell → book $9,863.94; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 132 | $19.57 | $2.43 | $-4.81 | $5,194.71 | ▼ -4.81 after sell → book $9,861.51; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 1178 | $2.08 | $15.41 | $-166.08 | $7,635.43 | ▼ -166.08 after sell → book $9,846.10; vs 09:30 mark -15.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 231 | $9.57 | $3.04 | $-364.07 | $9,843.06 | ▼ -364.07 after sell → book $9,843.06; vs 09:30 mark -3.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 587 | $4.19 | $7.57 | — | $7,375.96 | — | top 4 by hot; rank hot_score; list yday_mover; ⚪; ret5=+291.8; leftover $2460.77 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 358 | $6.87 | $4.62 | — | $4,911.88 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; leftover $2460.77 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 180 | $13.64 | $2.53 | — | $2,454.15 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; leftover $2460.77 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 59 | $41.23 | $2.17 | — | $19.42 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; leftover $2460.77 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.42 | ▲ close $9,851.95 vs 09:30 $9,866.28 (session +25.77) | 16:00 close · cash $19.42 · equity $9,851.95 vs 09:30 $9,866.28 (-14.33; session marks +25.77) · 4 name(s) marked open→close (per-name table). XHG×587 09:30 $4.19 → close $3.91 -164.36; CAPR×358 09:30 $6.87 → close $7.45 +207.64; STDN×180 09:30 $13.64 → close $13.31 -59.40; HTFL×59 09:30 $41.23 → close $41.94 +41.89 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.42 | ▲ 09:30 equity $9,861.50 vs yday $9,851.95 (+9.55) | 09:30 open · cash $19.42 (unchanged overnight, no fees) · equity $9,861.50 vs prior close $9,851.95 (+9.55) · 4 name(s) re-marked at the open (per-name table). XHG×587 yday $3.91 → 09:30 $3.94 +17.61; CAPR×358 yday $7.45 → 09:30 $7.50 +17.90; STDN×180 yday $13.31 → 09:30 $13.31 +0.00; HTFL×59 yday $41.94 → 09:30 $41.50 -25.96 | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 587 | $3.94 | $7.69 | $-162.01 | $2,324.51 | ▼ -162.01 after sell → book $9,853.81; vs 09:30 mark -7.69 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 180 | $13.31 | $2.58 | $-64.51 | $4,717.73 | ▼ -64.51 after sell → book $9,851.23; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 judge🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 59 | $41.50 | $2.20 | $+11.57 | $7,164.03 | ▲ +11.57 after sell → book $9,849.03; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,164.03 | ▼ close $9,698.67 vs 09:30 $9,861.50 (session -150.36) | 16:00 close · cash $7,164.03 · equity $9,698.67 vs 09:30 $9,861.50 (-162.83; session marks -150.36) · 1 name(s) marked open→close (per-name table). CAPR×358 09:30 $7.50 → close $7.08 -150.36 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,164.03 | ▲ 09:30 equity $9,738.05 vs yday $9,698.67 (+39.38) | 09:30 open · cash $7,164.03 (unchanged overnight, no fees) · equity $9,738.05 vs prior close $9,698.67 (+39.38) · 1 name(s) re-marked at the open (per-name table). CAPR×358 yday $7.08 → 09:30 $7.19 +39.38 | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 358 | $7.19 | $4.70 | $+105.24 | $9,733.36 | ▲ +105.24 after sell → book $9,733.36; vs 09:30 mark -4.69 | dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,733.36 | ▲ close $9,733.36 vs 09:30 $9,738.05 (session +0.00) | 16:00 close · cash $9,733.36 · no lots left · equity $9,733.36. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,733.36 | ▲ 09:30 equity $9,733.36 vs yday $9,733.36 (-0.00) | 09:30 open · cash $9,733.36 · no holdings · equity $9,733.36 vs prior close $9,733.36 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 16 | $150.14 | $2.04 | — | $7,329.08 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $2433.34 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 2115 | $1.15 | $27.28 | — | $4,869.54 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $2433.34 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 205 | $11.81 | $2.64 | — | $2,444.82 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $2433.34 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 1767 | $1.37 | $22.79 | — | $1.24 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $2433.34 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.24 | ▼ close $9,567.54 vs 09:30 $9,733.36 (session -111.05) | 16:00 close · cash $1.24 · equity $9,567.54 vs 09:30 $9,733.36 (-165.82; session marks -111.05) · 4 name(s) marked open→close (per-name table). MRNA×16 09:30 $150.14 → close $133.32 -269.12; CYPH×2115 09:30 $1.15 → close $1.19 +84.60; ABCL×205 09:30 $11.81 → close $11.57 -50.22; AZI×1767 09:30 $1.37 → close $1.44 +123.69 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.24 | ▲ 09:30 equity $9,874.47 vs yday $9,567.54 (+306.93) | 09:30 open · cash $1.24 (unchanged overnight, no fees) · equity $9,874.47 vs prior close $9,567.54 (+306.93) · 4 name(s) re-marked at the open (per-name table). MRNA×16 yday $133.32 → 09:30 $133.11 -3.36; CYPH×2115 yday $1.19 → 09:30 $1.32 +274.95; ABCL×205 yday $11.57 → 09:30 $11.57 +0.00; AZI×1767 yday $1.44 → 09:30 $1.46 +35.34 | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 205 | $11.57 | $2.70 | $-55.57 | $2,370.39 | ▼ -55.57 after sell → book $9,871.77; vs 09:30 mark -2.70 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 1767 | $1.46 | $23.11 | $+113.13 | $4,927.10 | ▲ +113.13 after sell → book $9,848.66; vs 09:30 mark -23.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 548 | $4.49 | $7.07 | — | $2,459.51 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; leftover $2463.55 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 360 | $6.81 | $4.64 | — | $3.27 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+62.5; leftover $2463.55 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.27 | ▲ close $10,009.73 vs 09:30 $9,874.47 (session +172.78) | 16:00 close · cash $3.27 · equity $10,009.73 vs 09:30 $9,874.47 (+135.26; session marks +172.78) · 4 name(s) marked open→close (per-name table). MRNA×16 09:30 $133.11 → close $145.13 +192.32; CYPH×2115 09:30 $1.32 → close $1.42 +211.50; XHG×548 09:30 $4.49 → close $4.41 -43.84; CAPR×360 09:30 $6.81 → close $6.29 -187.20 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.27 | ▲ 09:30 equity $11,415.08 vs yday $10,009.73 (+1,405.35) | 09:30 open · cash $3.27 (unchanged overnight, no fees) · equity $11,415.08 vs prior close $10,009.73 (+1405.35) · 4 name(s) re-marked at the open (per-name table). MRNA×16 yday $145.13 → 09:30 $142.70 -38.88; CYPH×2115 yday $1.42 → 09:30 $1.83 +867.15; XHG×548 yday $4.41 → 09:30 $4.32 -49.32; CAPR×360 yday $6.29 → 09:30 $8.03 +626.40 | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 16 | $142.70 | $2.07 | $-123.14 | $2,284.40 | ▼ -123.14 after sell → book $11,413.01; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 2115 | $1.83 | $27.67 | $+1383.25 | $6,127.19 | ▲ +1,383.25 after sell → book $11,385.35; vs 09:30 mark -27.66 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 548 | $4.32 | $7.18 | $-107.41 | $8,487.37 | ▼ -107.41 after sell → book $11,378.17; vs 09:30 mark -7.18 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 360 | $8.03 | $4.73 | $+429.83 | $11,373.44 | ▲ +429.83 after sell → book $11,373.44; vs 09:30 mark -4.73 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,373.44 | ▲ close $11,373.44 vs 09:30 $11,415.08 (session +0.00) | 16:00 close · cash $11,373.44 · no lots left · equity $11,373.44. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,373.44 | ▲ 09:30 equity $11,373.44 vs yday $11,373.44 (+0.00) | 09:30 open · cash $11,373.44 · no holdings · equity $11,373.44 vs prior close $11,373.44 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 117 | $24.11 | $2.34 | — | $8,550.23 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+891.7; leftover $2843.36 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 1822 | $1.56 | $23.50 | — | $5,684.41 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $2843.36 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 698 | $4.07 | $9.00 | — | $2,834.54 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; leftover $2843.36 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 148 | $19.04 | $2.43 | — | $14.19 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; leftover $2843.36 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.19 | ▲ close $12,300.26 vs 09:30 $11,373.44 (session +964.10) | 16:00 close · cash $14.19 · equity $12,300.26 vs 09:30 $11,373.44 (+926.82; session marks +964.10) · 4 name(s) marked open→close (per-name table). REAX×117 09:30 $24.11 → close $28.43 +505.44; CYPH×1822 09:30 $1.56 → close $1.64 +145.76; XHG×698 09:30 $4.07 → close $4.02 -34.90; ASST×148 09:30 $19.04 → close $21.39 +347.80 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.19 | ▼ 09:30 equity $11,768.70 vs yday $12,300.26 (-531.56) | 09:30 open · cash $14.19 (unchanged overnight, no fees) · equity $11,768.70 vs prior close $12,300.26 (-531.56) · 4 name(s) re-marked at the open (per-name table). REAX×117 yday $28.43 → 09:30 $26.61 -212.94; CYPH×1822 yday $1.64 → 09:30 $1.60 -72.88; XHG×698 yday $4.02 → 09:30 $3.81 -146.58; ASST×148 yday $21.39 → 09:30 $20.72 -99.16 | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 117 | $26.61 | $2.39 | $+287.77 | $3,125.17 | ▲ +287.77 after sell → book $11,766.31; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 1822 | $1.60 | $23.83 | $+25.55 | $6,016.55 | ▲ +25.55 after sell → book $11,742.49; vs 09:30 mark -23.82 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 148 | $20.72 | $2.48 | $+243.72 | $9,080.62 | ▲ +243.72 after sell → book $11,740.00; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 214 | $14.11 | $2.76 | — | $6,058.32 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+11.4; leftover $3026.87 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 520 | $5.81 | $6.71 | — | $3,030.41 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $3026.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SUJA` | 322 | $9.39 | $4.15 | — | $2.68 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+39.0; leftover $3026.87 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.68 | ▲ close $12,035.34 vs 09:30 $11,768.70 (session +308.96) | 16:00 close · cash $2.68 · equity $12,035.34 vs 09:30 $11,768.70 (+266.64; session marks +308.96) · 4 name(s) marked open→close (per-name table). XHG×698 09:30 $3.81 → close $4.06 +174.50; BYND×214 09:30 $14.11 → close $14.25 +29.96; USDE×520 09:30 $5.81 → close $5.98 +88.40; SUJA×322 09:30 $9.39 → close $9.44 +16.10 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.68 | ▲ 09:30 equity $12,285.38 vs yday $12,035.34 (+250.04) | 09:30 open · cash $2.68 (unchanged overnight, no fees) · equity $12,285.38 vs prior close $12,035.34 (+250.04) · 4 name(s) re-marked at the open (per-name table). XHG×698 yday $4.06 → 09:30 $4.06 +0.00; BYND×214 yday $14.25 → 09:30 $14.20 -10.70; USDE×520 yday $5.98 → 09:30 $6.50 +270.40; SUJA×322 yday $9.44 → 09:30 $9.41 -9.66 | — |
| 2026-08-27 09:30 ET | **SELL** | `XHG` | 698 | $4.06 | $9.14 | $-25.13 | $2,827.42 | ▼ -25.13 after sell → book $12,276.24; vs 09:30 mark -9.14 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 214 | $14.20 | $2.82 | $+13.68 | $5,863.40 | ▲ +13.68 after sell → book $12,273.42; vs 09:30 mark -2.82 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 520 | $6.50 | $6.82 | $+345.27 | $9,236.58 | ▲ +345.27 after sell → book $12,266.60; vs 09:30 mark -6.82 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SUJA` | 322 | $9.41 | $4.23 | $-1.95 | $12,262.36 | ▼ -1.95 after sell → book $12,262.36; vs 09:30 mark -4.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 1179 | $2.60 | $15.21 | — | $9,181.75 | — | top 4 by hot; rank hot_score; list flatten; ret5=+13.0; leftover $3065.59 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 73 | $41.44 | $2.21 | — | $6,154.43 | — | top 4 by hot; rank hot_score; list flatten; ret5=+3.1; leftover $3065.59 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PGY` | 133 | $22.93 | $2.39 | — | $3,102.35 | — | top 4 by hot; rank hot_score; list mover_buy; 🔵; ret5=+9.5; leftover $3065.59 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 212 | $14.42 | $2.73 | — | $42.57 | — | top 4 by hot; rank hot_score; list flatten; ret5=+7.1; leftover $3065.59 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $42.57 | ▲ close $12,387.87 vs 09:30 $12,285.38 (session +148.05) | 16:00 close · cash $42.57 · equity $12,387.87 vs 09:30 $12,285.38 (+102.49; session marks +148.05) · 4 name(s) marked open→close (per-name table). SLI×1179 09:30 $2.60 → close $2.64 +47.16; RRC×73 09:30 $41.44 → close $41.64 +14.60; PGY×133 09:30 $22.93 → close $23.26 +43.89; CRK×212 09:30 $14.42 → close $14.62 +42.40 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $42.57 | ▲ 09:30 equity $12,437.80 vs yday $12,387.87 (+49.93) | 09:30 open · cash $42.57 (unchanged overnight, no fees) · equity $12,437.80 vs prior close $12,387.87 (+49.93) · 4 name(s) re-marked at the open (per-name table). SLI×1179 yday $2.64 → 09:30 $2.68 +47.16; RRC×73 yday $41.64 → 09:30 $41.74 +7.30; PGY×133 yday $23.26 → 09:30 $23.21 -6.65; CRK×212 yday $14.62 → 09:30 $14.63 +2.12 | — |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 1179 | $2.68 | $15.43 | $+63.68 | $3,186.86 | ▲ +63.68 after sell → book $12,422.37; vs 09:30 mark -15.43 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `RRC` | 73 | $41.74 | $2.25 | $+17.45 | $6,231.64 | ▲ +17.45 after sell → book $12,420.13; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `PGY` | 133 | $23.21 | $2.44 | $+32.42 | $9,316.13 | ▲ +32.42 after sell → book $12,417.69; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRK` | 212 | $14.63 | $2.79 | $+38.99 | $12,414.90 | ▲ +38.99 after sell → book $12,414.90; vs 09:30 mark -2.79 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 221 | $14.00 | $2.85 | — | $9,318.04 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-3.3; leftover $3103.72 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 318 | $9.73 | $4.10 | — | $6,219.80 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+47.1; leftover $3103.72 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MRNA` | 22 | $137.19 | $2.06 | — | $3,199.57 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+7.1; leftover $3103.72 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 21 | $146.07 | $2.05 | — | $130.04 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $3103.72 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $130.04 | ▼ close $12,395.32 vs 09:30 $12,437.80 (session -8.51) | 16:00 close · cash $130.04 · equity $12,395.32 vs 09:30 $12,437.80 (-42.48; session marks -8.51) · 4 name(s) marked open→close (per-name table). BYND×221 09:30 $14.00 → close $13.86 -30.94; CAPR×318 09:30 $9.73 → close $9.59 -44.52; MRNA×22 09:30 $137.19 → close $137.99 +17.60; ANF×21 09:30 $146.07 → close $148.42 +49.35 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $130.04 | ▼ 09:30 equity $12,261.88 vs yday $12,395.32 (-133.44) | 09:30 open · cash $130.04 (unchanged overnight, no fees) · equity $12,261.88 vs prior close $12,395.32 (-133.44) · 4 name(s) re-marked at the open (per-name table). BYND×221 yday $13.86 → 09:30 $13.81 -11.05; CAPR×318 yday $9.59 → 09:30 $9.50 -28.62; MRNA×22 yday $137.99 → 09:30 $134.10 -85.58; ANF×21 yday $148.42 → 09:30 $148.03 -8.19 | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 318 | $9.50 | $4.18 | $-81.42 | $3,146.86 | ▼ -81.42 after sell → book $12,257.70; vs 09:30 mark -4.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 22 | $134.10 | $2.09 | $-72.13 | $6,094.97 | ▼ -72.13 after sell → book $12,255.61; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 21 | $148.03 | $2.09 | $+37.02 | $9,201.52 | ▲ +37.02 after sell → book $12,253.53; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,201.52 | ▼ close $12,140.82 vs 09:30 $12,261.88 (session -112.71) | 16:00 close · cash $9,201.52 · equity $12,140.82 vs 09:30 $12,261.88 (-121.06; session marks -112.71) · 1 name(s) marked open→close (per-name table). BYND×221 09:30 $13.81 → close $13.30 -112.71 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,201.52 | ▼ 09:30 equity $12,083.36 vs yday $12,140.82 (-57.46) | 09:30 open · cash $9,201.52 (unchanged overnight, no fees) · equity $12,083.36 vs prior close $12,140.82 (-57.46) · 1 name(s) re-marked at the open (per-name table). BYND×221 yday $13.30 → 09:30 $13.04 -57.46 | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 221 | $13.04 | $2.91 | $-217.92 | $12,080.45 | ▼ -217.92 after sell → book $12,080.45; vs 09:30 mark -2.91 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,080.45 | ▲ close $12,080.45 vs 09:30 $12,083.36 (session +0.00) | 16:00 close · cash $12,080.45 · no lots left · equity $12,080.45. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,080.45 | ▲ 09:30 equity $12,080.45 vs yday $12,080.45 (-0.00) | 09:30 open · cash $12,080.45 · no holdings · equity $12,080.45 vs prior close $12,080.45 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,080.45 | ▲ close $12,080.45 vs 09:30 $12,080.45 (session +0.00) | 16:00 close · cash $12,080.45 · no lots left · equity $12,080.45. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,080.45 | ▲ 09:30 equity $12,080.45 vs yday $12,080.45 (-0.00) | 09:30 open · cash $12,080.45 · no holdings · equity $12,080.45 vs prior close $12,080.45 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1696 | $1.78 | $21.88 | — | $9,039.69 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; leftover $3020.11 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 164 | $18.40 | $2.48 | — | $6,019.61 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; leftover $3020.11 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 220 | $13.71 | $2.84 | — | $3,000.57 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; leftover $3020.11 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 125 | $23.88 | $2.37 | — | $13.20 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $3020.11 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.20 | ▼ close $11,413.04 vs 09:30 $12,080.45 (session -637.84) | 16:00 close · cash $13.20 · equity $11,413.04 vs 09:30 $12,080.45 (-667.41; session marks -637.84) · 4 name(s) marked open→close (per-name table). GPRO×1696 09:30 $1.78 → close $1.39 -661.44; REAX×164 09:30 $18.40 → close $18.40 +0.00; CNH×220 09:30 $13.71 → close $13.84 +28.60; MMED×125 09:30 $23.88 → close $23.84 -5.00 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.20 | ▲ 09:30 equity $11,535.68 vs yday $11,413.04 (+122.64) | 09:30 open · cash $13.20 (unchanged overnight, no fees) · equity $11,535.68 vs prior close $11,413.04 (+122.64) · 4 name(s) re-marked at the open (per-name table). GPRO×1696 yday $1.39 → 09:30 $1.48 +152.64; REAX×164 yday $18.40 → 09:30 $18.15 -41.00; CNH×220 yday $13.84 → 09:30 $13.89 +11.00; MMED×125 yday $23.84 → 09:30 $23.84 +0.00 | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 164 | $18.15 | $2.53 | $-46.01 | $2,987.27 | ▼ -46.01 after sell → book $11,533.15; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 220 | $13.89 | $2.90 | $+33.86 | $6,040.17 | ▲ +33.86 after sell → book $11,530.25; vs 09:30 mark -2.90 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 125 | $23.84 | $2.41 | $-9.77 | $9,017.76 | ▼ -9.77 after sell → book $11,527.84; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 119 | $25.18 | $2.35 | — | $6,018.99 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+16.0; leftover $3005.92 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 381 | $7.87 | $4.91 | — | $3,015.61 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; leftover $3005.92 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 519 | $5.79 | $6.70 | — | $3.90 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $3005.92 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.90 | ▲ close $12,184.62 vs 09:30 $11,535.68 (session +670.74) | 16:00 close · cash $3.90 · equity $12,184.62 vs 09:30 $11,535.68 (+648.94; session marks +670.74) · 4 name(s) marked open→close (per-name table). GPRO×1696 09:30 $1.48 → close $1.70 +373.12; ASST×119 09:30 $25.18 → close $27.14 +233.24; USDE×381 09:30 $7.87 → close $7.93 +22.86; DFDV×519 09:30 $5.79 → close $5.87 +41.52 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.90 | ▼ 09:30 equity $11,776.45 vs yday $12,184.62 (-408.17) | 09:30 open · cash $3.90 (unchanged overnight, no fees) · equity $11,776.45 vs prior close $12,184.62 (-408.17) · 4 name(s) re-marked at the open (per-name table). GPRO×1696 yday $1.70 → 09:30 $1.56 -228.96; ASST×119 yday $27.14 → 09:30 $26.44 -83.30; USDE×381 yday $7.93 → 09:30 $7.76 -64.77; DFDV×519 yday $5.87 → 09:30 $5.81 -31.14 | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 1696 | $1.56 | $22.18 | $-408.70 | $2,635.96 | ▼ -408.70 after sell → book $11,754.27; vs 09:30 mark -22.18 | dropped from list after 2 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 119 | $26.44 | $2.39 | $+145.20 | $5,779.93 | ▲ +145.20 after sell → book $11,751.88; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 381 | $7.76 | $5.00 | $-51.83 | $8,731.49 | ▼ -51.83 after sell → book $11,746.88; vs 09:30 mark -5.00 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 519 | $5.81 | $6.81 | $-3.12 | $11,740.07 | ▼ -3.12 after sell → book $11,740.07; vs 09:30 mark -6.81 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,740.07 | ▲ close $11,740.07 vs 09:30 $11,776.45 (session +0.00) | 16:00 close · cash $11,740.07 · no lots left · equity $11,740.07. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,740.07 | ▲ 09:30 equity $11,740.07 vs yday $11,740.07 (+0.00) | 09:30 open · cash $11,740.07 · no holdings · equity $11,740.07 vs prior close $11,740.07 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,740.07 | ▲ close $11,740.07 vs 09:30 $11,740.07 (session +0.00) | 16:00 close · cash $11,740.07 · no lots left · equity $11,740.07. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `REAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MRNA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new buys |
