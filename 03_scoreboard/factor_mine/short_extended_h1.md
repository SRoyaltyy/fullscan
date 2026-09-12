# Factor mine action — `short_extended_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · ret_5>15

Cash book **-8.13%** ($9,187) · signal-only (no cash/fees) was -9.24%. Starts YES **10/21**. Fills 146 · skips 63 · realized $-823.47.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only short names that pass every must-have on the checklist. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will fall.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: prior 5-session return is at least 15%.

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- Keep the first 8 names in list order.
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- This is a SHORT sleeve: it borrows the name and profits if the price falls. Equity treats the short as a liability (must keep enough to cover).

### When it sells

- Sell first, then buy. Never sell a ticker we do not hold.
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `ret_5_min=15.0` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $13,688.34.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `TNDM` | 214 | — | $23.33 | +0.00 | $23.13 | +42.80 | +42.80 | -0.00 | +42.80 |
| 2026-08-14 | `TNDM` | 214 | $23.13 | $22.92 | +44.94 | — | +0.00 | +44.94 | +87.74 | — |
| 2026-08-14 | `ARX` | 32 | — | $19.57 | +0.00 | $19.58 | -0.32 | -0.32 | -0.00 | -0.32 |
| 2026-08-14 | `OMER` | 36 | — | $17.35 | +0.00 | $17.19 | +5.76 | +5.76 | -0.00 | +5.76 |
| 2026-08-14 | `AIRO` | 56 | — | $11.12 | +0.00 | $9.57 | +86.80 | +86.80 | -0.00 | +86.80 |
| 2026-08-14 | `MXCT` | 453 | — | $1.39 | +0.00 | $1.32 | +31.71 | +31.71 | -0.00 | +31.71 |
| 2026-08-14 | `QMLS` | 86 | — | $7.29 | +0.00 | $7.32 | -2.58 | -2.58 | -0.00 | -2.58 |
| 2026-08-14 | `AVAH` | 52 | — | $11.91 | +0.00 | $12.32 | -21.32 | -21.32 | -0.00 | -21.32 |
| 2026-08-14 | `TBBB` | 12 | — | $48.82 | +0.00 | $47.79 | +12.36 | +12.36 | -0.00 | +12.36 |
| 2026-08-14 | `AMPY` | 127 | — | $4.94 | +0.00 | $4.78 | +20.32 | +20.32 | -0.00 | +20.32 |
| 2026-08-17 | `ARX` | 32 | $19.58 | $19.57 | +0.32 | — | +0.00 | +0.32 | -0.00 | — |
| 2026-08-17 | `OMER` | 36 | $17.19 | $17.17 | +0.72 | — | +0.00 | +0.72 | +6.48 | — |
| 2026-08-17 | `AIRO` | 56 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | +86.80 | — |
| 2026-08-17 | `MXCT` | 453 | $1.32 | $1.32 | +0.00 | — | +0.00 | +0.00 | +31.71 | — |
| 2026-08-17 | `QMLS` | 86 | $7.32 | $7.24 | +6.88 | — | +0.00 | +6.88 | +4.30 | — |
| 2026-08-17 | `AVAH` | 52 | $12.32 | $12.21 | +5.72 | — | +0.00 | +5.72 | -15.60 | — |
| 2026-08-17 | `TBBB` | 12 | $47.79 | $47.39 | +4.80 | — | +0.00 | +4.80 | +17.16 | — |
| 2026-08-17 | `AMPY` | 127 | $4.78 | $4.86 | -10.16 | — | +0.00 | -10.16 | +10.16 | — |
| 2026-08-17 | `CAPR` | 92 | — | $6.87 | +0.00 | $7.45 | -53.36 | -53.36 | -0.00 | -53.36 |
| 2026-08-17 | `HTFL` | 15 | — | $41.23 | +0.00 | $41.94 | -10.65 | -10.65 | -0.00 | -10.65 |
| 2026-08-17 | `UMAC` | 19 | — | $32.55 | +0.00 | $30.15 | +45.60 | +45.60 | -0.00 | +45.60 |
| 2026-08-17 | `NPWR` | 331 | — | $1.92 | +0.00 | $1.73 | +62.89 | +62.89 | -0.00 | +62.89 |
| 2026-08-17 | `LPTH` | 42 | — | $14.94 | +0.00 | $14.80 | +5.88 | +5.88 | -0.00 | +5.88 |
| 2026-08-17 | `NMAX` | 58 | — | $10.97 | +0.00 | $10.36 | +35.38 | +35.38 | -0.00 | +35.38 |
| 2026-08-17 | `ALOY` | 43 | — | $14.66 | +0.00 | $13.86 | +34.61 | +34.61 | -0.00 | +34.61 |
| 2026-08-17 | `INO` | 594 | — | $1.07 | +0.00 | $1.15 | -47.52 | -47.52 | -0.00 | -47.52 |
| 2026-08-18 | `CAPR` | 92 | $7.45 | $7.50 | -4.60 | — | +0.00 | -4.60 | -57.96 | — |
| 2026-08-18 | `HTFL` | 15 | $41.94 | $41.50 | +6.60 | — | +0.00 | +6.60 | -4.05 | — |
| 2026-08-18 | `UMAC` | 19 | $30.15 | $28.59 | +29.64 | — | +0.00 | +29.64 | +75.24 | — |
| 2026-08-18 | `NPWR` | 331 | $1.73 | $1.70 | +9.93 | — | +0.00 | +9.93 | +72.82 | — |
| 2026-08-18 | `LPTH` | 42 | $14.80 | $14.01 | +33.18 | — | +0.00 | +33.18 | +39.06 | — |
| 2026-08-18 | `NMAX` | 58 | $10.36 | $10.31 | +2.90 | — | +0.00 | +2.90 | +38.28 | — |
| 2026-08-18 | `ALOY` | 43 | $13.86 | $13.19 | +28.60 | — | +0.00 | +28.60 | +63.21 | — |
| 2026-08-18 | `INO` | 594 | $1.15 | $1.14 | +5.94 | — | +0.00 | +5.94 | -41.58 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `MRNA` | 4 | — | $150.14 | +0.00 | $133.32 | +67.28 | +67.28 | -0.00 | +67.28 |
| 2026-08-20 | `AZI` | 470 | — | $1.37 | +0.00 | $1.44 | -32.90 | -32.90 | -0.00 | -32.90 |
| 2026-08-20 | `CYPH` | 560 | — | $1.15 | +0.00 | $1.19 | -22.40 | -22.40 | -0.00 | -22.40 |
| 2026-08-20 | `BNTX` | 5 | — | $109.06 | +0.00 | $110.89 | -9.15 | -9.15 | -0.00 | -9.15 |
| 2026-08-20 | `BTGO` | 97 | — | $6.61 | +0.00 | $6.60 | +0.49 | +0.49 | -0.00 | +0.49 |
| 2026-08-20 | `ASST` | 40 | — | $16.00 | +0.00 | $16.13 | -5.20 | -5.20 | -0.00 | -5.20 |
| 2026-08-20 | `PPC` | 21 | — | $30.65 | +0.00 | $31.24 | -12.39 | -12.39 | -0.00 | -12.39 |
| 2026-08-20 | `ABCL` | 54 | — | $11.81 | +0.00 | $11.57 | +13.23 | +13.23 | -0.00 | +13.23 |
| 2026-08-21 | `MRNA` | 4 | $133.32 | $133.11 | +0.84 | — | +0.00 | +0.84 | +68.12 | — |
| 2026-08-21 | `AZI` | 470 | $1.44 | $1.46 | -9.40 | — | +0.00 | -9.40 | -42.30 | — |
| 2026-08-21 | `CYPH` | 560 | $1.19 | $1.32 | -72.80 | $1.42 | -56.00 | -128.80 | -95.20 | -151.20 |
| 2026-08-21 | `BNTX` | 5 | $110.89 | $110.92 | -0.15 | — | +0.00 | -0.15 | -9.30 | — |
| 2026-08-21 | `BTGO` | 97 | $6.60 | $6.95 | -33.95 | — | +0.00 | -33.95 | -33.46 | — |
| 2026-08-21 | `ASST` | 40 | $16.13 | $17.66 | -61.20 | — | +0.00 | -61.20 | -66.40 | — |
| 2026-08-21 | `PPC` | 21 | $31.24 | $31.13 | +2.31 | — | +0.00 | +2.31 | -10.08 | — |
| 2026-08-21 | `ABCL` | 54 | $11.57 | $11.57 | +0.00 | — | +0.00 | +0.00 | +13.23 | — |
| 2026-08-21 | `AU` | 6 | — | $119.43 | +0.00 | $121.22 | -10.74 | -10.74 | -0.00 | -10.74 |
| 2026-08-21 | `AEM` | 3 | — | $216.30 | +0.00 | $216.06 | +0.72 | +0.72 | -0.00 | +0.72 |
| 2026-08-21 | `ARCT` | 64 | — | $11.13 | +0.00 | $13.45 | -148.48 | -148.48 | -0.00 | -148.48 |
| 2026-08-21 | `INDP` | 518 | — | $1.39 | +0.00 | $1.29 | +51.80 | +51.80 | -0.00 | +51.80 |
| 2026-08-21 | `CAN` | 2452 | — | $0.29 | +0.00 | $0.35 | -149.57 | -149.57 | -0.00 | -149.57 |
| 2026-08-21 | `MRVI` | 87 | — | $8.28 | +0.00 | $8.64 | -31.32 | -31.32 | -0.00 | -31.32 |
| 2026-08-21 | `DFDV` | 178 | — | $4.04 | +0.00 | $3.94 | +17.80 | +17.80 | -0.00 | +17.80 |
| 2026-08-24 | `CYPH` | 560 | $1.42 | $1.83 | -229.60 | — | +0.00 | -229.60 | -380.80 | — |
| 2026-08-24 | `AU` | 6 | $121.22 | $120.51 | +4.26 | — | +0.00 | +4.26 | -6.48 | — |
| 2026-08-24 | `AEM` | 3 | $216.06 | $217.03 | -2.91 | — | +0.00 | -2.91 | -2.19 | — |
| 2026-08-24 | `ARCT` | 64 | $13.45 | $13.33 | +7.68 | $14.34 | -64.64 | -56.96 | -140.80 | -205.44 |
| 2026-08-24 | `INDP` | 518 | $1.29 | $1.24 | +25.90 | — | +0.00 | +25.90 | +77.70 | — |
| 2026-08-24 | `CAN` | 2452 | $0.35 | $0.38 | -68.66 | $0.36 | +56.40 | -12.26 | -218.23 | -161.83 |
| 2026-08-24 | `MRVI` | 87 | $8.64 | $8.59 | +4.35 | — | +0.00 | +4.35 | -26.97 | — |
| 2026-08-24 | `DFDV` | 178 | $3.94 | $4.16 | -39.16 | — | +0.00 | -39.16 | -21.36 | — |
| 2026-08-25 | `ARCT` | 64 | $14.34 | $14.12 | +14.08 | — | +0.00 | +14.08 | -191.36 | — |
| 2026-08-25 | `CAN` | 2452 | $0.36 | $0.36 | +0.00 | — | +0.00 | +0.00 | -161.83 | — |
| 2026-08-25 | `BMEA` | 360 | — | $1.63 | +0.00 | $1.73 | -36.00 | -36.00 | -0.00 | -36.00 |
| 2026-08-25 | `NPWR` | 293 | — | $2.00 | +0.00 | $1.95 | +14.65 | +14.65 | -0.00 | +14.65 |
| 2026-08-25 | `ALVO` | 112 | — | $5.24 | +0.00 | $5.05 | +21.28 | +21.28 | -0.00 | +21.28 |
| 2026-08-25 | `SUJA` | 66 | — | $8.79 | +0.00 | $9.33 | -35.64 | -35.64 | -0.00 | -35.64 |
| 2026-08-25 | `CYPH` | 376 | — | $1.56 | +0.00 | $1.64 | -30.08 | -30.08 | -0.00 | -30.08 |
| 2026-08-25 | `FWDI` | 102 | — | $5.71 | +0.00 | $6.05 | -34.68 | -34.68 | -0.00 | -34.68 |
| 2026-08-25 | `DEFT` | 947 | — | $0.62 | +0.00 | $0.60 | +15.15 | +15.15 | -0.00 | +15.15 |
| 2026-08-25 | `GORO` | 165 | — | $3.55 | +0.00 | $3.87 | -52.80 | -52.80 | -0.00 | -52.80 |
| 2026-08-26 | `BMEA` | 360 | $1.73 | $1.75 | -9.00 | — | +0.00 | -9.00 | -45.00 | — |
| 2026-08-26 | `NPWR` | 293 | $1.95 | $1.93 | +5.86 | — | +0.00 | +5.86 | +20.51 | — |
| 2026-08-26 | `ALVO` | 112 | $5.05 | $4.98 | +7.84 | — | +0.00 | +7.84 | +29.12 | — |
| 2026-08-26 | `SUJA` | 66 | $9.33 | $9.39 | -3.96 | $9.44 | -3.30 | -7.26 | -39.60 | -42.90 |
| 2026-08-26 | `CYPH` | 376 | $1.64 | $1.60 | +15.04 | — | +0.00 | +15.04 | -15.04 | — |
| 2026-08-26 | `FWDI` | 102 | $6.05 | $5.97 | +8.16 | — | +0.00 | +8.16 | -26.52 | — |
| 2026-08-26 | `DEFT` | 947 | $0.60 | $0.60 | +5.68 | — | +0.00 | +5.68 | +20.83 | — |
| 2026-08-26 | `GORO` | 165 | $3.87 | $3.77 | +16.50 | — | +0.00 | +16.50 | -36.30 | — |
| 2026-08-26 | `INDP` | 606 | — | $1.09 | +0.00 | $1.14 | -30.30 | -30.30 | -0.00 | -30.30 |
| 2026-08-26 | `CAPR` | 79 | — | $8.29 | +0.00 | $9.36 | -84.53 | -84.53 | -0.00 | -84.53 |
| 2026-08-26 | `BRR` | 300 | — | $2.20 | +0.00 | $2.17 | +9.00 | +9.00 | -0.00 | +9.00 |
| 2026-08-26 | `USDE` | 113 | — | $5.81 | +0.00 | $5.98 | -19.21 | -19.21 | -0.00 | -19.21 |
| 2026-08-26 | `FIGR` | 16 | — | $40.50 | +0.00 | $37.08 | +54.72 | +54.72 | -0.00 | +54.72 |
| 2026-08-26 | `MNRO` | 47 | — | $14.00 | +0.00 | $12.61 | +65.33 | +65.33 | -0.00 | +65.33 |
| 2026-08-26 | `FUTU` | 5 | — | $124.67 | +0.00 | $127.34 | -13.35 | -13.35 | -0.00 | -13.35 |
| 2026-08-27 | `SUJA` | 66 | $9.44 | $9.41 | +1.98 | — | +0.00 | +1.98 | -40.92 | — |
| 2026-08-27 | `INDP` | 606 | $1.14 | $1.13 | +6.06 | — | +0.00 | +6.06 | -24.24 | — |
| 2026-08-27 | `CAPR` | 79 | $9.36 | $9.19 | +13.43 | — | +0.00 | +13.43 | -71.10 | — |
| 2026-08-27 | `BRR` | 300 | $2.17 | $2.19 | -6.00 | — | +0.00 | -6.00 | +3.00 | — |
| 2026-08-27 | `USDE` | 113 | $5.98 | $6.50 | -58.76 | — | +0.00 | -58.76 | -77.97 | — |
| 2026-08-27 | `FIGR` | 16 | $37.08 | $37.42 | -5.44 | — | +0.00 | -5.44 | +49.28 | — |
| 2026-08-27 | `MNRO` | 47 | $12.61 | $12.56 | +2.35 | — | +0.00 | +2.35 | +67.68 | — |
| 2026-08-27 | `FUTU` | 5 | $127.34 | $128.00 | -3.30 | — | +0.00 | -3.30 | -16.65 | — |
| 2026-08-28 | `SLI` | 212 | — | $2.68 | +0.00 | $2.55 | +27.56 | +27.56 | -0.00 | +27.56 |
| 2026-08-28 | `ANF` | 3 | — | $146.07 | +0.00 | $148.42 | -7.05 | -7.05 | -0.00 | -7.05 |
| 2026-08-28 | `BHVN` | 35 | — | $15.88 | +0.00 | $15.41 | +16.45 | +16.45 | -0.00 | +16.45 |
| 2026-08-28 | `CAPR` | 58 | — | $9.73 | +0.00 | $9.59 | +8.12 | +8.12 | -0.00 | +8.12 |
| 2026-08-28 | `LVWR` | 410 | — | $1.39 | +0.00 | $1.35 | +16.40 | +16.40 | -0.00 | +16.40 |
| 2026-08-28 | `VYX` | 62 | — | $9.13 | +0.00 | $8.78 | +21.70 | +21.70 | -0.00 | +21.70 |
| 2026-08-28 | `OPTU` | 570 | — | $1.00 | +0.00 | $1.02 | -11.40 | -11.40 | -0.00 | -11.40 |
| 2026-08-28 | `SBET` | 65 | — | $8.65 | +0.00 | $8.20 | +29.25 | +29.25 | -0.00 | +29.25 |
| 2026-08-31 | `SLI` | 212 | $2.55 | $2.58 | -6.36 | — | +0.00 | -6.36 | +21.20 | — |
| 2026-08-31 | `ANF` | 3 | $148.42 | $148.03 | +1.17 | — | +0.00 | +1.17 | -5.88 | — |
| 2026-08-31 | `BHVN` | 35 | $15.41 | $15.46 | -1.75 | — | +0.00 | -1.75 | +14.70 | — |
| 2026-08-31 | `CAPR` | 58 | $9.59 | $9.50 | +5.22 | — | +0.00 | +5.22 | +13.34 | — |
| 2026-08-31 | `LVWR` | 410 | $1.35 | $1.30 | +20.50 | — | +0.00 | +20.50 | +36.90 | — |
| 2026-08-31 | `VYX` | 62 | $8.78 | $8.66 | +7.44 | — | +0.00 | +7.44 | +29.14 | — |
| 2026-08-31 | `OPTU` | 570 | $1.02 | $1.06 | -22.80 | — | +0.00 | -22.80 | -34.20 | — |
| 2026-08-31 | `SBET` | 65 | $8.20 | $8.24 | -2.60 | — | +0.00 | -2.60 | +26.65 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `GPRO` | 322 | — | $1.78 | +0.00 | $1.39 | +125.58 | +125.58 | -0.00 | +125.58 |
| 2026-09-03 | `FRVO` | 31 | — | $18.28 | +0.00 | $17.16 | +34.72 | +34.72 | -0.00 | +34.72 |
| 2026-09-03 | `MMED` | 24 | — | $23.88 | +0.00 | $23.84 | +0.96 | +0.96 | -0.00 | +0.96 |
| 2026-09-03 | `CNXC` | 17 | — | $32.88 | +0.00 | $32.85 | +0.51 | +0.51 | -0.00 | +0.51 |
| 2026-09-03 | `SION` | 78 | — | $7.31 | +0.00 | $6.75 | +43.68 | +43.68 | -0.00 | +43.68 |
| 2026-09-03 | `CNH` | 41 | — | $13.71 | +0.00 | $13.84 | -5.33 | -5.33 | -0.00 | -5.33 |
| 2026-09-03 | `TARS` | 6 | — | $82.76 | +0.00 | $83.20 | -2.67 | -2.67 | -0.00 | -2.67 |
| 2026-09-03 | `DFDV` | 102 | — | $5.59 | +0.00 | $6.08 | -49.47 | -49.47 | -0.00 | -49.47 |
| 2026-09-04 | `GPRO` | 322 | $1.39 | $1.48 | -28.98 | $1.70 | -70.84 | -99.82 | +96.60 | +25.76 |
| 2026-09-04 | `FRVO` | 31 | $17.16 | $17.27 | -3.41 | — | +0.00 | -3.41 | +31.31 | — |
| 2026-09-04 | `MMED` | 24 | $23.84 | $23.84 | +0.00 | — | +0.00 | +0.00 | +0.96 | — |
| 2026-09-04 | `CNXC` | 17 | $32.85 | $32.48 | +6.29 | — | +0.00 | +6.29 | +6.80 | — |
| 2026-09-04 | `SION` | 78 | $6.75 | $6.68 | +5.46 | — | +0.00 | +5.46 | +49.14 | — |
| 2026-09-04 | `CNH` | 41 | $13.84 | $13.89 | -2.05 | — | +0.00 | -2.05 | -7.38 | — |
| 2026-09-04 | `TARS` | 6 | $83.20 | $82.70 | +3.03 | $90.78 | -48.48 | -45.45 | +0.36 | -48.12 |
| 2026-09-04 | `DFDV` | 102 | $6.08 | $5.79 | +29.58 | — | +0.00 | +29.58 | -19.89 | — |
| 2026-09-04 | `BAK` | 399 | — | $1.94 | +0.00 | $1.89 | +19.95 | +19.95 | -0.00 | +19.95 |
| 2026-09-04 | `SLBT` | 246 | — | $3.15 | +0.00 | $2.88 | +66.42 | +66.42 | -0.00 | +66.42 |
| 2026-09-04 | `IRD` | 171 | — | $4.53 | +0.00 | $4.67 | -23.94 | -23.94 | -0.00 | -23.94 |
| 2026-09-04 | `FMC` | 59 | — | $12.95 | +0.00 | $12.97 | -1.18 | -1.18 | -0.00 | -1.18 |
| 2026-09-04 | `BRR` | 308 | — | $2.51 | +0.00 | $2.66 | -46.20 | -46.20 | -0.00 | -46.20 |
| 2026-09-04 | `LENZ` | 134 | — | $5.75 | +0.00 | $5.96 | -28.14 | -28.14 | -0.00 | -28.14 |
| 2026-09-08 | `GPRO` | 322 | $1.70 | $1.56 | +43.47 | — | +0.00 | +43.47 | +69.23 | — |
| 2026-09-08 | `TARS` | 6 | $90.78 | $89.67 | +6.66 | — | +0.00 | +6.66 | -41.46 | — |
| 2026-09-08 | `BAK` | 399 | $1.89 | $1.94 | -19.95 | — | +0.00 | -19.95 | -0.00 | — |
| 2026-09-08 | `SLBT` | 246 | $2.88 | $2.88 | +0.00 | — | +0.00 | +0.00 | +66.42 | — |
| 2026-09-08 | `IRD` | 171 | $4.67 | $4.53 | +23.94 | — | +0.00 | +23.94 | -0.00 | — |
| 2026-09-08 | `FMC` | 59 | $12.97 | $13.11 | -8.26 | — | +0.00 | -8.26 | -9.44 | — |
| 2026-09-08 | `BRR` | 308 | $2.66 | $2.66 | +0.00 | — | +0.00 | +0.00 | -46.20 | — |
| 2026-09-08 | `LENZ` | 134 | $5.96 | $5.95 | +1.34 | — | +0.00 | +1.34 | -26.80 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `COHU` | 10 | — | $56.09 | +0.00 | $57.08 | -9.90 | -9.90 | -0.00 | -9.90 |
| 2026-09-11 | `INDP` | 212 | — | $2.70 | +0.00 | $2.77 | -14.84 | -14.84 | -0.00 | -14.84 |
| 2026-09-11 | `BNC` | 116 | — | $4.91 | +0.00 | $4.80 | +12.76 | +12.76 | -0.00 | +12.76 |
| 2026-09-11 | `ANGX` | 106 | — | $5.38 | +0.00 | $5.45 | -7.42 | -7.42 | -0.00 | -7.42 |
| 2026-09-11 | `IRD` | 93 | — | $6.16 | +0.00 | $6.04 | +11.16 | +11.16 | -0.00 | +11.16 |
| 2026-09-11 | `CYPH` | 239 | — | $2.39 | +0.00 | $2.27 | +29.88 | +29.88 | -0.00 | +29.88 |
| 2026-09-11 | `PAYP` | 31 | — | $18.30 | +0.00 | $18.45 | -4.65 | -4.65 | -0.00 | -4.65 |
| 2026-09-11 | `CRWV` | 6 | — | $91.08 | +0.00 | $88.99 | +12.54 | +12.54 | -0.00 | +12.54 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +42.80 | TNDM | — | $14,989.65 | $10,039.83 | TNDM×214 |
| 2026-08-14 | +5.50 | $14,989.65 | TNDM×214 | $10,084.77 | +44.94 | +132.73 | ARX, OMER, AIRO, MXCT, QMLS, AVAH, TBBB, AMPY | TNDM | $15,023.36 | $10,193.38 | ARX×32, OMER×36, AIRO×56, MXCT×453, QMLS×86, AVAH×52, TBBB×12, AMPY×127 |
| 2026-08-17 | +2.25 | $15,023.36 | ARX×32, OMER×36, AIRO×56, MXCT×453, QMLS×86, AVAH×52, TBBB×12, AMPY×127 | $10,201.66 | +8.28 | +72.83 | CAPR, HTFL, UMAC, NPWR, LPTH, NMAX, ALOY, INO | ARX, OMER, AIRO, MXCT, QMLS, AVAH, TBBB, AMPY | $15,189.73 | $10,228.41 | CAPR×92, HTFL×15, UMAC×19, NPWR×331, LPTH×42, NMAX×58, ALOY×43, INO×594 |
| 2026-08-18 | -6.20 | $15,189.73 | CAPR×92, HTFL×15, UMAC×19, NPWR×331, LPTH×42, NMAX×58, ALOY×43, INO×594 | $10,340.59 | +112.18 | +0.00 | — | CAPR, HTFL, UMAC, NPWR, LPTH, NMAX, ALOY, INO | $10,315.91 | $10,315.91 | — |
| 2026-08-19 | -7.20 | $10,315.91 | — | $10,315.91 | +0.00 | +0.00 | — | — | $10,315.91 | $10,315.91 | — |
| 2026-08-20 | +1.12 | $10,315.91 | — | $10,315.91 | +0.00 | -1.04 | MRNA, AZI, CYPH, BNTX, BTGO, ASST, PPC, ABCL | — | $15,285.67 | $10,288.52 | MRNA×4, AZI×470, CYPH×560, BNTX×5, BTGO×97, ASST×40, PPC×21, ABCL×54 |
| 2026-08-21 | +3.25 | $15,285.67 | MRNA×4, AZI×470, CYPH×560, BNTX×5, BTGO×97, ASST×40, PPC×21, ABCL×54 | $10,114.17 | -174.35 | -325.79 | AU, AEM, ARCT, INDP, CAN, MRVI, DFDV | MRNA, AZI, BNTX, BTGO, ASST, PPC, ABCL | $15,759.91 | $9,736.73 | CYPH×560, AU×6, AEM×3, ARCT×64, INDP×518, CAN×2452, MRVI×87, DFDV×178 |
| 2026-08-24 | -5.17 | $15,759.91 | CYPH×560, AU×6, AEM×3, ARCT×64, INDP×518, CAN×2452, MRVI×87, DFDV×178 | $9,438.59 | -298.14 | -8.24 | — | CYPH, AU, AEM, INDP, MRVI, DFDV | $11,208.14 | $9,407.66 | ARCT×64, CAN×2452 |
| 2026-08-25 | +1.80 | $11,208.14 | ARCT×64, CAN×2452 | $9,421.74 | +14.08 | -138.12 | BMEA, NPWR, ALVO, SUJA, CYPH, FWDI, DEFT, GORO | ARCT, CAN | $14,053.17 | $9,233.37 | BMEA×360, NPWR×293, ALVO×112, SUJA×66, CYPH×376, FWDI×102, DEFT×947, GORO×165 |
| 2026-08-26 | +2.02 | $14,053.17 | BMEA×360, NPWR×293, ALVO×112, SUJA×66, CYPH×376, FWDI×102, DEFT×947, GORO×165 | $9,279.49 | +46.12 | -21.64 | INDP, CAPR, BRR, USDE, FIGR, MNRO, FUTU | BMEA, NPWR, ALVO, CYPH, FWDI, DEFT, GORO | $14,408.85 | $9,206.14 | SUJA×66, INDP×606, CAPR×79, BRR×300, USDE×113, FIGR×16, MNRO×47, FUTU×5 |
| 2026-08-27 | — | $14,408.85 | SUJA×66, INDP×606, CAPR×79, BRR×300, USDE×113, FIGR×16, MNRO×47, FUTU×5 | $9,156.46 | -49.68 | +0.00 | — | SUJA, INDP, CAPR, BRR, USDE, FIGR, MNRO, FUTU | $9,131.85 | $9,131.85 | — |
| 2026-08-28 | +0.75 | $9,131.85 | — | $9,131.85 | +0.00 | +101.03 | SLI, ANF, BHVN, CAPR, LVWR, VYX, OPTU, SBET | — | $13,500.13 | $9,206.44 | SLI×212, ANF×3, BHVN×35, CAPR×58, LVWR×410, VYX×62, OPTU×570, SBET×65 |
| 2026-08-31 | -5.85 | $13,500.13 | SLI×212, ANF×3, BHVN×35, CAPR×58, LVWR×410, VYX×62, OPTU×570, SBET×65 | $9,207.26 | +0.82 | +0.00 | — | SLI, ANF, BHVN, CAPR, LVWR, VYX, OPTU, SBET | $9,181.26 | $9,181.26 | — |
| 2026-09-01 | -6.30 | $9,181.26 | — | $9,181.26 | +0.00 | +0.00 | — | — | $9,181.26 | $9,181.26 | — |
| 2026-09-02 | -3.83 | $9,181.26 | — | $9,181.26 | +0.00 | +0.00 | — | — | $9,181.26 | $9,181.26 | — |
| 2026-09-03 | -0.90 | $9,181.26 | — | $9,181.26 | +0.00 | +147.98 | GPRO, FRVO, MMED, CNXC, SION, CNH, TARS, DFDV | — | $13,633.41 | $9,309.93 | GPRO×322, FRVO×31, MMED×24, CNXC×17, SION×78, CNH×41, TARS×6, DFDV×102 |
| 2026-09-04 | +2.25 | $13,633.41 | GPRO×322, FRVO×31, MMED×24, CNXC×17, SION×78, CNH×41, TARS×6, DFDV×102 | $9,319.85 | +9.92 | -132.41 | BAK, SLBT, IRD, FMC, BRR, LENZ | FRVO, MMED, CNXC, SION, CNH, DFDV | $14,891.25 | $9,154.86 | GPRO×322, TARS×6, BAK×399, SLBT×246, IRD×171, FMC×59, BRR×308, LENZ×134 |
| 2026-09-08 | -11.47 | $14,891.25 | GPRO×322, TARS×6, BAK×399, SLBT×246, IRD×171, FMC×59, BRR×308, LENZ×134 | $9,202.06 | +47.20 | +0.00 | — | GPRO, TARS, BAK, SLBT, IRD, FMC, BRR, LENZ | $9,176.54 | $9,176.54 | — |
| 2026-09-09 | -13.95 | $9,176.54 | — | $9,176.54 | -0.00 | +0.00 | — | — | $9,176.54 | $9,176.54 | — |
| 2026-09-10 | -13.28 | $9,176.54 | — | $9,176.54 | -0.00 | +0.00 | — | — | $9,176.54 | $9,176.54 | — |
| 2026-09-11 | +0.50 | $9,176.54 | — | $9,176.54 | -0.00 | +29.53 | COHU, INDP, BNC, ANGX, IRD, CYPH, PAYP, CRWV | — | $13,688.34 | $9,186.86 | COHU×10, INDP×212, BNC×116, ANGX×106, IRD×93, CYPH×239, PAYP×31, CRWV×6 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **SHORT** | `TNDM` | 214 | $23.33 | $2.97 | — | $14,989.65 | — | ret_5>15; gate ret_5_min=15.0; list flatten; ⚪; ret5=+19.7; leftover $5000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,989.65 | ▲ close $10,039.83 vs 09:30 $10,000.00 (session +42.80) | 16:00 close · cash $14,989.65 · equity $10,039.83 vs 09:30 $10,000.00 (+39.83; session marks +42.80) · 1 name(s) marked open→close (per-name table). TNDM×214 09:30 $23.33 → close $23.13 +42.80 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,989.65 | ▲ 09:30 equity $10,084.77 vs yday $10,039.83 (+44.94) | 09:30 open · cash $14,989.65 (unchanged overnight, no fees) · equity $10,084.77 vs prior close $10,039.83 (+44.94) · 1 name(s) re-marked at the open (per-name table). TNDM×214 yday $23.13 → 09:30 $22.92 +44.94 | — |
| 2026-08-14 09:30 ET | **COVER** | `TNDM` | 214 | $22.92 | $2.76 | $+82.01 | $10,082.01 | ▲ +82.01 after sell → book $10,082.01; vs 09:30 mark -2.76 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SHORT** | `ARX` | 32 | $19.57 | $2.12 | — | $10,706.12 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $630.13 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `OMER` | 36 | $17.35 | $2.14 | — | $11,328.59 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+31.9; leftover $630.13 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `AIRO` | 56 | $11.12 | $2.20 | — | $11,949.11 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $630.13 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `MXCT` | 453 | $1.39 | $5.95 | — | $12,572.84 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+25.2; leftover $630.13 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `QMLS` | 86 | $7.29 | $2.29 | — | $13,197.49 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $630.13 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `AVAH` | 52 | $11.91 | $2.18 | — | $13,814.62 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+21.3; leftover $630.13 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `TBBB` | 12 | $48.82 | $2.06 | — | $14,398.40 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.0; leftover $630.13 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `AMPY` | 127 | $4.94 | $2.42 | — | $15,023.36 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.4; leftover $630.13 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,023.36 | ▲ close $10,193.38 vs 09:30 $10,084.77 (session +132.73) | 16:00 close · cash $15,023.36 · equity $10,193.38 vs 09:30 $10,084.77 (+108.61; session marks +132.73) · 8 name(s) marked open→close (per-name table). ARX×32 09:30 $19.57 → close $19.58 -0.32; OMER×36 09:30 $17.35 → close $17.19 +5.76; AIRO×56 09:30 $11.12 → close $9.57 +86.80; MXCT×453 09:30 $1.39 → close $1.32 +31.71; QMLS×86 09:30 $7.29 → close $7.32 -2.58; AVAH×52 09:30 $11.91 → close $12.32 -21.32; TBBB×12 09:30 $48.82 → close $47.79 +12.36; AMPY×127 09:30 $4.94 → close $4.78 +20.32 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,023.36 | ▲ 09:30 equity $10,201.66 vs yday $10,193.38 (+8.28) | 09:30 open · cash $15,023.36 (unchanged overnight, no fees) · equity $10,201.66 vs prior close $10,193.38 (+8.28) · 8 name(s) re-marked at the open (per-name table). ARX×32 yday $19.58 → 09:30 $19.57 +0.32; OMER×36 yday $17.19 → 09:30 $17.17 +0.72; AIRO×56 yday $9.57 → 09:30 $9.57 -0.00; MXCT×453 yday $1.32 → 09:30 $1.32 -0.00; QMLS×86 yday $7.32 → 09:30 $7.24 +6.88; AVAH×52 yday $12.32 → 09:30 $12.21 +5.72; TBBB×12 yday $47.79 → 09:30 $47.39 +4.80; AMPY×127 yday $4.78 → 09:30 $4.86 -10.16 | — |
| 2026-08-17 09:30 ET | **COVER** | `ARX` | 32 | $19.57 | $2.09 | $-4.21 | $14,395.04 | ▼ -4.21 after sell → book $10,199.58; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `OMER` | 36 | $17.17 | $2.10 | $+2.25 | $13,774.82 | ▲ +2.25 after sell → book $10,197.48; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `AIRO` | 56 | $9.57 | $2.16 | $+82.45 | $13,236.74 | ▲ +82.45 after sell → book $10,195.32; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `MXCT` | 453 | $1.32 | $5.84 | $+19.92 | $12,632.94 | ▲ +19.92 after sell → book $10,189.48; vs 09:30 mark -5.84 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `QMLS` | 86 | $7.24 | $2.25 | $-0.24 | $12,008.05 | ▼ -0.24 after sell → book $10,187.23; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `AVAH` | 52 | $12.21 | $2.15 | $-19.93 | $11,370.98 | ▼ -19.93 after sell → book $10,185.08; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `TBBB` | 12 | $47.39 | $2.03 | $+13.07 | $10,800.28 | ▲ +13.07 after sell → book $10,183.06; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `AMPY` | 127 | $4.86 | $2.37 | $+5.37 | $10,180.69 | ▲ +5.37 after sell → book $10,180.69; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SHORT** | `CAPR` | 92 | $6.87 | $2.31 | — | $10,810.42 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+62.6; leftover $636.29 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `HTFL` | 15 | $41.23 | $2.07 | — | $11,426.80 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+46.0; leftover $636.29 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `UMAC` | 19 | $32.55 | $2.08 | — | $12,043.16 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $636.29 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `NPWR` | 331 | $1.92 | $4.35 | — | $12,674.33 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $636.29 | join🟡 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `LPTH` | 42 | $14.94 | $2.15 | — | $13,299.66 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $636.29 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **SHORT** | `NMAX` | 58 | $10.97 | $2.20 | — | $13,933.72 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ⚪; ret5=+21.2; leftover $636.29 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `ALOY` | 43 | $14.66 | $2.16 | — | $14,561.94 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $636.29 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `INO` | 594 | $1.07 | $7.79 | — | $15,189.73 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+62.7; leftover $636.29 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,189.73 | ▲ close $10,228.41 vs 09:30 $10,201.66 (session +72.83) | 16:00 close · cash $15,189.73 · equity $10,228.41 vs 09:30 $10,201.66 (+26.75; session marks +72.83) · 8 name(s) marked open→close (per-name table). CAPR×92 09:30 $6.87 → close $7.45 -53.36; HTFL×15 09:30 $41.23 → close $41.94 -10.65; UMAC×19 09:30 $32.55 → close $30.15 +45.60; NPWR×331 09:30 $1.92 → close $1.73 +62.89; LPTH×42 09:30 $14.94 → close $14.80 +5.88; NMAX×58 09:30 $10.97 → close $10.36 +35.38; ALOY×43 09:30 $14.66 → close $13.86 +34.61; INO×594 09:30 $1.07 → close $1.15 -47.52 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,189.73 | ▲ 09:30 equity $10,340.59 vs yday $10,228.41 (+112.18) | 09:30 open · cash $15,189.73 (unchanged overnight, no fees) · equity $10,340.59 vs prior close $10,228.41 (+112.18) · 8 name(s) re-marked at the open (per-name table). CAPR×92 yday $7.45 → 09:30 $7.50 -4.60; HTFL×15 yday $41.94 → 09:30 $41.50 +6.60; UMAC×19 yday $30.15 → 09:30 $28.59 +29.64; NPWR×331 yday $1.73 → 09:30 $1.70 +9.93; LPTH×42 yday $14.80 → 09:30 $14.01 +33.18; NMAX×58 yday $10.36 → 09:30 $10.31 +2.90; ALOY×43 yday $13.86 → 09:30 $13.19 +28.60; INO×594 yday $1.15 → 09:30 $1.14 +5.94 | — |
| 2026-08-18 09:30 ET | **COVER** | `CAPR` | 92 | $7.50 | $2.27 | $-62.53 | $14,497.46 | ▼ -62.53 after sell → book $10,338.32; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **COVER** | `HTFL` | 15 | $41.50 | $2.04 | $-8.16 | $13,872.93 | ▼ -8.16 after sell → book $10,336.29; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `UMAC` | 19 | $28.59 | $2.05 | $+71.11 | $13,327.67 | ▲ +71.11 after sell → book $10,334.24; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `NPWR` | 331 | $1.70 | $4.27 | $+64.20 | $12,760.70 | ▲ +64.20 after sell → book $10,329.97; vs 09:30 mark -4.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `LPTH` | 42 | $14.01 | $2.12 | $+34.79 | $12,170.17 | ▲ +34.79 after sell → book $10,327.86; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `NMAX` | 58 | $10.31 | $2.16 | $+33.91 | $11,570.02 | ▲ +33.91 after sell → book $10,325.69; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **COVER** | `ALOY` | 43 | $13.19 | $2.12 | $+58.93 | $11,000.73 | ▲ +58.93 after sell → book $10,323.57; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **COVER** | `INO` | 594 | $1.14 | $7.66 | $-57.03 | $10,315.91 | ▼ -57.03 after sell → book $10,315.91; vs 09:30 mark -7.66 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,315.91 | ▲ close $10,315.91 vs 09:30 $10,340.59 (session +0.00) | 16:00 close · cash $10,315.91 · no lots left · equity $10,315.91. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,315.91 | ▲ 09:30 equity $10,315.91 vs yday $10,315.91 (+0.00) | 09:30 open · cash $10,315.91 · no holdings · equity $10,315.91 vs prior close $10,315.91 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,315.91 | ▲ close $10,315.91 vs 09:30 $10,315.91 (session +0.00) | 16:00 close · cash $10,315.91 · no lots left · equity $10,315.91. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,315.91 | ▲ 09:30 equity $10,315.91 vs yday $10,315.91 (+0.00) | 09:30 open · cash $10,315.91 · no holdings · equity $10,315.91 vs prior close $10,315.91 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **SHORT** | `MRNA` | 4 | $150.14 | $2.04 | — | $10,914.43 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $644.74 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AZI` | 470 | $1.37 | $6.17 | — | $11,552.16 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $644.74 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `CYPH` | 560 | $1.15 | $7.34 | — | $12,188.82 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $644.74 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `BNTX` | 5 | $109.06 | $2.04 | — | $12,732.08 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+22.0; leftover $644.74 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `BTGO` | 97 | $6.61 | $2.32 | — | $13,370.44 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $644.74 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `ASST` | 40 | $16.00 | $2.15 | — | $14,008.29 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $644.74 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `PPC` | 21 | $30.65 | $2.09 | — | $14,649.85 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+16.5; leftover $644.74 | join🟡 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 54 | $11.81 | $2.19 | — | $15,285.67 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $644.74 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,285.67 | ▼ close $10,288.52 vs 09:30 $10,315.91 (session -1.04) | 16:00 close · cash $15,285.67 · equity $10,288.52 vs 09:30 $10,315.91 (-27.39; session marks -1.04) · 8 name(s) marked open→close (per-name table). MRNA×4 09:30 $150.14 → close $133.32 +67.28; AZI×470 09:30 $1.37 → close $1.44 -32.90; CYPH×560 09:30 $1.15 → close $1.19 -22.40; BNTX×5 09:30 $109.06 → close $110.89 -9.15; BTGO×97 09:30 $6.61 → close $6.60 +0.49; ASST×40 09:30 $16.00 → close $16.13 -5.20; PPC×21 09:30 $30.65 → close $31.24 -12.39; ABCL×54 09:30 $11.81 → close $11.57 +13.23 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,285.67 | ▼ 09:30 equity $10,114.17 vs yday $10,288.52 (-174.35) | 09:30 open · cash $15,285.67 (unchanged overnight, no fees) · equity $10,114.17 vs prior close $10,288.52 (-174.35) · 8 name(s) re-marked at the open (per-name table). MRNA×4 yday $133.32 → 09:30 $133.11 +0.84; AZI×470 yday $1.44 → 09:30 $1.46 -9.40; CYPH×560 yday $1.19 → 09:30 $1.32 -72.80; BNTX×5 yday $110.89 → 09:30 $110.92 -0.15; BTGO×97 yday $6.60 → 09:30 $6.95 -33.95; ASST×40 yday $16.13 → 09:30 $17.66 -61.20; PPC×21 yday $31.24 → 09:30 $31.13 +2.31; ABCL×54 yday $11.57 → 09:30 $11.57 -0.00 | — |
| 2026-08-21 09:30 ET | **COVER** | `MRNA` | 4 | $133.11 | $2.00 | $+64.08 | $14,751.23 | ▲ +64.08 after sell → book $10,112.17; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **COVER** | `AZI` | 470 | $1.46 | $6.06 | $-54.53 | $14,058.97 | ▼ -54.53 after sell → book $10,106.11; vs 09:30 mark -6.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `BNTX` | 5 | $110.92 | $2.00 | $-13.34 | $13,502.36 | ▼ -13.34 after sell → book $10,104.10; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `BTGO` | 97 | $6.95 | $2.28 | $-38.07 | $12,825.93 | ▼ -38.07 after sell → book $10,101.82; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `ASST` | 40 | $17.66 | $2.11 | $-70.66 | $12,117.42 | ▼ -70.66 after sell → book $10,099.71; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `PPC` | 21 | $31.13 | $2.05 | $-14.22 | $11,461.64 | ▼ -14.22 after sell → book $10,097.66; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `ABCL` | 54 | $11.57 | $2.15 | $+8.89 | $10,834.71 | ▲ +8.89 after sell → book $10,095.51; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SHORT** | `AU` | 6 | $119.43 | $2.05 | — | $11,549.24 | — | ret_5>15; gate ret_5_min=15.0; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $721.11 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `AEM` | 3 | $216.30 | $2.04 | — | $12,196.10 | — | ret_5>15; gate ret_5_min=15.0; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $721.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `ARCT` | 64 | $11.13 | $2.22 | — | $12,906.20 | — | ret_5>15; gate ret_5_min=15.0; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $721.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `INDP` | 518 | $1.39 | $6.80 | — | $13,619.42 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+30.2; leftover $721.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `CAN` | 2452 | $0.29 | $15.00 | — | $14,325.31 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $721.11 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `MRVI` | 87 | $8.28 | $2.30 | — | $15,043.37 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; leftover $721.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `DFDV` | 178 | $4.04 | $2.58 | — | $15,759.91 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+23.6; leftover $721.11 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,759.91 | ▼ close $9,736.73 vs 09:30 $10,114.17 (session -325.79) | 16:00 close · cash $15,759.91 · equity $9,736.73 vs 09:30 $10,114.17 (-377.44; session marks -325.79) · 8 name(s) marked open→close (per-name table). CYPH×560 09:30 $1.32 → close $1.42 -56.00; AU×6 09:30 $119.43 → close $121.22 -10.74; AEM×3 09:30 $216.30 → close $216.06 +0.72; ARCT×64 09:30 $11.13 → close $13.45 -148.48; INDP×518 09:30 $1.39 → close $1.29 +51.80; CAN×2452 09:30 $0.29 → close $0.35 -149.57; MRVI×87 09:30 $8.28 → close $8.64 -31.32; DFDV×178 09:30 $4.04 → close $3.94 +17.80 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,759.91 | ▼ 09:30 equity $9,438.59 vs yday $9,736.73 (-298.14) | 09:30 open · cash $15,759.91 (unchanged overnight, no fees) · equity $9,438.59 vs prior close $9,736.73 (-298.14) · 8 name(s) re-marked at the open (per-name table). CYPH×560 yday $1.42 → 09:30 $1.83 -229.60; AU×6 yday $121.22 → 09:30 $120.51 +4.26; AEM×3 yday $216.06 → 09:30 $217.03 -2.91; ARCT×64 yday $13.45 → 09:30 $13.33 +7.68; INDP×518 yday $1.29 → 09:30 $1.24 +25.90; CAN×2452 yday $0.35 → 09:30 $0.38 -68.66; MRVI×87 yday $8.64 → 09:30 $8.59 +4.35; DFDV×178 yday $3.94 → 09:30 $4.16 -39.16 | — |
| 2026-08-24 09:30 ET | **COVER** | `CYPH` | 560 | $1.83 | $7.22 | $-395.37 | $14,727.89 | ▼ -395.37 after sell → book $9,431.37; vs 09:30 mark -7.22 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `AU` | 6 | $120.51 | $2.01 | $-10.54 | $14,002.82 | ▼ -10.54 after sell → book $9,429.36; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `AEM` | 3 | $217.03 | $2.00 | $-6.23 | $13,349.73 | ▼ -6.23 after sell → book $9,427.36; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **COVER** | `INDP` | 518 | $1.24 | $6.68 | $+64.22 | $12,700.73 | ▲ +64.22 after sell → book $9,420.68; vs 09:30 mark -6.68 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `MRVI` | 87 | $8.59 | $2.25 | $-31.52 | $11,951.15 | ▼ -31.52 after sell → book $9,418.43; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `DFDV` | 178 | $4.16 | $2.52 | $-26.47 | $11,208.14 | ▼ -26.47 after sell → book $9,415.91; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,208.14 | ▼ close $9,407.66 vs 09:30 $9,438.59 (session -8.24) | 16:00 close · cash $11,208.14 · equity $9,407.66 vs 09:30 $9,438.59 (-30.93; session marks -8.24) · 2 name(s) marked open→close (per-name table). ARCT×64 09:30 $13.33 → close $14.34 -64.64; CAN×2452 09:30 $0.38 → close $0.36 +56.40 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,208.14 | ▲ 09:30 equity $9,421.74 vs yday $9,407.66 (+14.08) | 09:30 open · cash $11,208.14 (unchanged overnight, no fees) · equity $9,421.74 vs prior close $9,407.66 (+14.08) · 2 name(s) re-marked at the open (per-name table). ARCT×64 yday $14.34 → 09:30 $14.12 +14.08; CAN×2452 yday $0.36 → 09:30 $0.36 -0.00 | — |
| 2026-08-25 09:30 ET | **COVER** | `ARCT` | 64 | $14.12 | $2.18 | $-195.76 | $10,302.28 | ▼ -195.76 after sell → book $9,419.56; vs 09:30 mark -2.18 | dropped from list after 2 sess (min 1) | — |
| 2026-08-25 09:30 ET | **COVER** | `CAN` | 2452 | $0.36 | $16.18 | $-193.02 | $9,403.38 | ▼ -193.02 after sell → book $9,403.38; vs 09:30 mark -16.18 | dropped from list after 2 sess (min 1) | — |
| 2026-08-25 09:30 ET | **SHORT** | `BMEA` | 360 | $1.63 | $4.73 | — | $9,985.45 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $587.71 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `NPWR` | 293 | $2.00 | $3.85 | — | $10,567.59 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+15.0; leftover $587.71 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `ALVO` | 112 | $5.24 | $2.37 | — | $11,152.10 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $587.71 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `SUJA` | 66 | $8.79 | $2.22 | — | $11,730.02 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $587.71 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `CYPH` | 376 | $1.56 | $4.94 | — | $12,311.64 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $587.71 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `FWDI` | 102 | $5.71 | $2.34 | — | $12,891.72 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+33.9; leftover $587.71 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `DEFT` | 947 | $0.62 | $8.90 | — | $13,469.96 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+33.3; leftover $587.71 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `GORO` | 165 | $3.55 | $2.54 | — | $14,053.17 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+27.9; leftover $587.71 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,053.17 | ▼ close $9,233.37 vs 09:30 $9,421.74 (session -138.12) | 16:00 close · cash $14,053.17 · equity $9,233.37 vs 09:30 $9,421.74 (-188.37; session marks -138.12) · 8 name(s) marked open→close (per-name table). BMEA×360 09:30 $1.63 → close $1.73 -36.00; NPWR×293 09:30 $2.00 → close $1.95 +14.65; ALVO×112 09:30 $5.24 → close $5.05 +21.28; SUJA×66 09:30 $8.79 → close $9.33 -35.64; CYPH×376 09:30 $1.56 → close $1.64 -30.08; FWDI×102 09:30 $5.71 → close $6.05 -34.68; DEFT×947 09:30 $0.62 → close $0.60 +15.15; GORO×165 09:30 $3.55 → close $3.87 -52.80 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,053.17 | ▲ 09:30 equity $9,279.49 vs yday $9,233.37 (+46.12) | 09:30 open · cash $14,053.17 (unchanged overnight, no fees) · equity $9,279.49 vs prior close $9,233.37 (+46.12) · 8 name(s) re-marked at the open (per-name table). BMEA×360 yday $1.73 → 09:30 $1.75 -9.00; NPWR×293 yday $1.95 → 09:30 $1.93 +5.86; ALVO×112 yday $5.05 → 09:30 $4.98 +7.84; SUJA×66 yday $9.33 → 09:30 $9.39 -3.96; CYPH×376 yday $1.64 → 09:30 $1.60 +15.04; FWDI×102 yday $6.05 → 09:30 $5.97 +8.16; DEFT×947 yday $0.60 → 09:30 $0.60 +5.68; GORO×165 yday $3.87 → 09:30 $3.77 +16.50 | — |
| 2026-08-26 09:30 ET | **COVER** | `BMEA` | 360 | $1.75 | $4.64 | $-54.37 | $13,416.73 | ▼ -54.37 after sell → book $9,274.84; vs 09:30 mark -4.65 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `NPWR` | 293 | $1.93 | $3.78 | $+12.88 | $12,847.46 | ▲ +12.88 after sell → book $9,271.06; vs 09:30 mark -3.78 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `ALVO` | 112 | $4.98 | $2.33 | $+24.42 | $12,287.37 | ▲ +24.42 after sell → book $9,268.74; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `CYPH` | 376 | $1.60 | $4.85 | $-24.83 | $11,680.92 | ▼ -24.83 after sell → book $9,263.89; vs 09:30 mark -4.85 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `FWDI` | 102 | $5.97 | $2.30 | $-31.15 | $11,069.69 | ▼ -31.15 after sell → book $9,261.59; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `DEFT` | 947 | $0.60 | $8.50 | $+3.43 | $10,494.88 | ▲ +3.43 after sell → book $9,253.09; vs 09:30 mark -8.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `GORO` | 165 | $3.77 | $2.48 | $-41.32 | $9,870.34 | ▼ -41.32 after sell → book $9,250.60; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SHORT** | `INDP` | 606 | $1.09 | $7.95 | — | $10,522.94 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+17.0; leftover $660.76 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `CAPR` | 79 | $8.29 | $2.27 | — | $11,175.58 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; leftover $660.76 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `BRR` | 300 | $2.20 | $3.95 | — | $11,831.63 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+17.8; leftover $660.76 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `USDE` | 113 | $5.81 | $2.38 | — | $12,485.79 | — | ret_5>15; gate ret_5_min=15.0; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $660.76 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `FIGR` | 16 | $40.50 | $2.08 | — | $13,131.71 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; 🔵; ret5=+15.8; leftover $660.76 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **SHORT** | `MNRO` | 47 | $14.00 | $2.17 | — | $13,787.54 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+17.8; leftover $660.76 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `FUTU` | 5 | $124.67 | $2.04 | — | $14,408.85 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; 🔵; ret5=+15.7; leftover $660.76 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,408.85 | ▼ close $9,206.14 vs 09:30 $9,279.49 (session -21.64) | 16:00 close · cash $14,408.85 · equity $9,206.14 vs 09:30 $9,279.49 (-73.35; session marks -21.64) · 8 name(s) marked open→close (per-name table). SUJA×66 09:30 $9.39 → close $9.44 -3.30; INDP×606 09:30 $1.09 → close $1.14 -30.30; CAPR×79 09:30 $8.29 → close $9.36 -84.53; BRR×300 09:30 $2.20 → close $2.17 +9.00; USDE×113 09:30 $5.81 → close $5.98 -19.21; FIGR×16 09:30 $40.50 → close $37.08 +54.72; MNRO×47 09:30 $14.00 → close $12.61 +65.33; FUTU×5 09:30 $124.67 → close $127.34 -13.35 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,408.85 | ▼ 09:30 equity $9,156.46 vs yday $9,206.14 (-49.68) | 09:30 open · cash $14,408.85 (unchanged overnight, no fees) · equity $9,156.46 vs prior close $9,206.14 (-49.68) · 8 name(s) re-marked at the open (per-name table). SUJA×66 yday $9.44 → 09:30 $9.41 +1.98; INDP×606 yday $1.14 → 09:30 $1.13 +6.06; CAPR×79 yday $9.36 → 09:30 $9.19 +13.43; BRR×300 yday $2.17 → 09:30 $2.19 -6.00; USDE×113 yday $5.98 → 09:30 $6.50 -58.76; FIGR×16 yday $37.08 → 09:30 $37.42 -5.44; MNRO×47 yday $12.61 → 09:30 $12.56 +2.35; FUTU×5 yday $127.34 → 09:30 $128.00 -3.30 | — |
| 2026-08-27 09:30 ET | **COVER** | `SUJA` | 66 | $9.41 | $2.19 | $-45.33 | $13,785.60 | ▼ -45.33 after sell → book $9,154.27; vs 09:30 mark -2.19 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `INDP` | 606 | $1.13 | $7.82 | $-40.00 | $13,093.00 | ▼ -40.00 after sell → book $9,146.45; vs 09:30 mark -7.82 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `CAPR` | 79 | $9.19 | $2.23 | $-75.60 | $12,364.77 | ▼ -75.60 after sell → book $9,144.23; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `BRR` | 300 | $2.19 | $3.87 | $-4.82 | $11,703.90 | ▼ -4.82 after sell → book $9,140.36; vs 09:30 mark -3.87 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `USDE` | 113 | $6.50 | $2.33 | $-82.67 | $10,967.07 | ▼ -82.67 after sell → book $9,138.03; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `FIGR` | 16 | $37.42 | $2.04 | $+45.17 | $10,366.31 | ▲ +45.17 after sell → book $9,135.99; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `MNRO` | 47 | $12.56 | $2.13 | $+63.38 | $9,773.86 | ▲ +63.38 after sell → book $9,133.86; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `FUTU` | 5 | $128.00 | $2.00 | $-20.70 | $9,131.85 | ▼ -20.70 after sell → book $9,131.85; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,131.85 | ▲ close $9,131.85 vs 09:30 $9,156.46 (session +0.00) | 16:00 close · cash $9,131.85 · no lots left · equity $9,131.85. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,131.85 | ▲ 09:30 equity $9,131.85 vs yday $9,131.85 (+0.00) | 09:30 open · cash $9,131.85 · no holdings · equity $9,131.85 vs prior close $9,131.85 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **SHORT** | `SLI` | 212 | $2.68 | $2.80 | — | $9,697.22 | — | ret_5>15; gate ret_5_min=15.0; list flatten,ohlc_hot; ret5=+16.3; leftover $570.74 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SHORT** | `ANF` | 3 | $146.07 | $2.03 | — | $10,133.40 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $570.74 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `BHVN` | 35 | $15.88 | $2.13 | — | $10,687.07 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+19.4; leftover $570.74 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `CAPR` | 58 | $9.73 | $2.20 | — | $11,249.21 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+47.1; leftover $570.74 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `LVWR` | 410 | $1.39 | $5.38 | — | $11,813.72 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+20.4; leftover $570.74 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `VYX` | 62 | $9.13 | $2.21 | — | $12,377.57 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+20.0; leftover $570.74 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `OPTU` | 570 | $1.00 | $7.47 | — | $12,940.10 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; ret5=+16.8; leftover $570.74 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `SBET` | 65 | $8.65 | $2.22 | — | $13,500.13 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+17.0; leftover $570.74 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,500.13 | ▲ close $9,206.44 vs 09:30 $9,131.85 (session +101.03) | 16:00 close · cash $13,500.13 · equity $9,206.44 vs 09:30 $9,131.85 (+74.59; session marks +101.03) · 8 name(s) marked open→close (per-name table). SLI×212 09:30 $2.68 → close $2.55 +27.56; ANF×3 09:30 $146.07 → close $148.42 -7.05; BHVN×35 09:30 $15.88 → close $15.41 +16.45; CAPR×58 09:30 $9.73 → close $9.59 +8.12; LVWR×410 09:30 $1.39 → close $1.35 +16.40; VYX×62 09:30 $9.13 → close $8.78 +21.70; OPTU×570 09:30 $1.00 → close $1.02 -11.40; SBET×65 09:30 $8.65 → close $8.20 +29.25 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,500.13 | ▲ 09:30 equity $9,207.26 vs yday $9,206.44 (+0.82) | 09:30 open · cash $13,500.13 (unchanged overnight, no fees) · equity $9,207.26 vs prior close $9,206.44 (+0.82) · 8 name(s) re-marked at the open (per-name table). SLI×212 yday $2.55 → 09:30 $2.58 -6.36; ANF×3 yday $148.42 → 09:30 $148.03 +1.17; BHVN×35 yday $15.41 → 09:30 $15.46 -1.75; CAPR×58 yday $9.59 → 09:30 $9.50 +5.22; LVWR×410 yday $1.35 → 09:30 $1.30 +20.50; VYX×62 yday $8.78 → 09:30 $8.66 +7.44; OPTU×570 yday $1.02 → 09:30 $1.06 -22.80; SBET×65 yday $8.20 → 09:30 $8.24 -2.60 | — |
| 2026-08-31 09:30 ET | **COVER** | `SLI` | 212 | $2.58 | $2.73 | $+15.67 | $12,950.43 | ▲ +15.67 after sell → book $9,204.52; vs 09:30 mark -2.74 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `ANF` | 3 | $148.03 | $2.00 | $-9.91 | $12,504.34 | ▼ -9.91 after sell → book $9,202.52; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `BHVN` | 35 | $15.46 | $2.10 | $+10.47 | $11,961.15 | ▲ +10.47 after sell → book $9,200.43; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `CAPR` | 58 | $9.50 | $2.16 | $+8.98 | $11,407.99 | ▲ +8.98 after sell → book $9,198.27; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `LVWR` | 410 | $1.30 | $5.29 | $+26.23 | $10,869.70 | ▲ +26.23 after sell → book $9,192.98; vs 09:30 mark -5.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `VYX` | 62 | $8.66 | $2.18 | $+24.75 | $10,330.60 | ▲ +24.75 after sell → book $9,190.80; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `OPTU` | 570 | $1.06 | $7.35 | $-49.03 | $9,719.05 | ▼ -49.03 after sell → book $9,183.45; vs 09:30 mark -7.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `SBET` | 65 | $8.24 | $2.19 | $+22.24 | $9,181.26 | ▲ +22.24 after sell → book $9,181.26; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,181.26 | ▲ close $9,181.26 vs 09:30 $9,207.26 (session +0.00) | 16:00 close · cash $9,181.26 · no lots left · equity $9,181.26. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,181.26 | ▲ 09:30 equity $9,181.26 vs yday $9,181.26 (+0.00) | 09:30 open · cash $9,181.26 · no holdings · equity $9,181.26 vs prior close $9,181.26 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,181.26 | ▲ close $9,181.26 vs 09:30 $9,181.26 (session +0.00) | 16:00 close · cash $9,181.26 · no lots left · equity $9,181.26. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,181.26 | ▲ 09:30 equity $9,181.26 vs yday $9,181.26 (+0.00) | 09:30 open · cash $9,181.26 · no holdings · equity $9,181.26 vs prior close $9,181.26 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,181.26 | ▲ close $9,181.26 vs 09:30 $9,181.26 (session +0.00) | 16:00 close · cash $9,181.26 · no lots left · equity $9,181.26. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,181.26 | ▲ 09:30 equity $9,181.26 vs yday $9,181.26 (+0.00) | 09:30 open · cash $9,181.26 · no holdings · equity $9,181.26 vs prior close $9,181.26 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **SHORT** | `GPRO` | 322 | $1.78 | $4.23 | — | $9,750.19 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+183.1; leftover $573.83 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `FRVO` | 31 | $18.28 | $2.12 | — | $10,314.75 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+16.5; leftover $573.83 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `MMED` | 24 | $23.88 | $2.10 | — | $10,885.77 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $573.83 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `CNXC` | 17 | $32.88 | $2.08 | — | $11,442.66 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+16.2; leftover $573.83 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `SION` | 78 | $7.31 | $2.26 | — | $12,010.57 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+18.5; leftover $573.83 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `CNH` | 41 | $13.71 | $2.15 | — | $12,570.54 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; 🔵; ret5=+17.5; leftover $573.83 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `TARS` | 6 | $82.76 | $2.04 | — | $13,065.05 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+17.1; leftover $573.83 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `DFDV` | 102 | $5.59 | $2.34 | — | $13,633.41 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+16.0; leftover $573.83 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,633.41 | ▲ close $9,309.93 vs 09:30 $9,181.26 (session +147.98) | 16:00 close · cash $13,633.41 · equity $9,309.93 vs 09:30 $9,181.26 (+128.67; session marks +147.98) · 8 name(s) marked open→close (per-name table). GPRO×322 09:30 $1.78 → close $1.39 +125.58; FRVO×31 09:30 $18.28 → close $17.16 +34.72; MMED×24 09:30 $23.88 → close $23.84 +0.96; CNXC×17 09:30 $32.88 → close $32.85 +0.51; SION×78 09:30 $7.31 → close $6.75 +43.68; CNH×41 09:30 $13.71 → close $13.84 -5.33; TARS×6 09:30 $82.76 → close $83.20 -2.67; DFDV×102 09:30 $5.59 → close $6.08 -49.47 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,633.41 | ▲ 09:30 equity $9,319.85 vs yday $9,309.93 (+9.92) | 09:30 open · cash $13,633.41 (unchanged overnight, no fees) · equity $9,319.85 vs prior close $9,309.93 (+9.92) · 8 name(s) re-marked at the open (per-name table). GPRO×322 yday $1.39 → 09:30 $1.48 -28.98; FRVO×31 yday $17.16 → 09:30 $17.27 -3.41; MMED×24 yday $23.84 → 09:30 $23.84 -0.00; CNXC×17 yday $32.85 → 09:30 $32.48 +6.29; SION×78 yday $6.75 → 09:30 $6.68 +5.46; CNH×41 yday $13.84 → 09:30 $13.89 -2.05; TARS×6 yday $83.20 → 09:30 $82.70 +3.03; DFDV×102 yday $6.08 → 09:30 $5.79 +29.58 | — |
| 2026-09-04 09:30 ET | **COVER** | `FRVO` | 31 | $17.27 | $2.08 | $+27.11 | $13,095.95 | ▲ +27.11 after sell → book $9,317.76; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `MMED` | 24 | $23.84 | $2.06 | $-3.20 | $12,521.73 | ▼ -3.20 after sell → book $9,315.70; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `CNXC` | 17 | $32.48 | $2.04 | $+2.68 | $11,967.53 | ▲ +2.68 after sell → book $9,313.66; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `SION` | 78 | $6.68 | $2.22 | $+44.65 | $11,444.27 | ▲ +44.65 after sell → book $9,311.44; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **COVER** | `CNH` | 41 | $13.89 | $2.11 | $-11.64 | $10,872.66 | ▼ -11.64 after sell → book $9,309.32; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `DFDV` | 102 | $5.79 | $2.30 | $-24.52 | $10,279.79 | ▼ -24.52 after sell → book $9,307.03; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `BAK` | 399 | $1.94 | $5.24 | — | $11,048.60 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+18.3; leftover $775.59 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `SLBT` | 246 | $3.15 | $3.25 | — | $11,820.26 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+19.3; leftover $775.59 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `IRD` | 171 | $4.53 | $2.56 | — | $12,592.32 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.5; leftover $775.59 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `FMC` | 59 | $12.95 | $2.21 | — | $13,354.17 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+21.8; leftover $775.59 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `BRR` | 308 | $2.51 | $4.06 | — | $14,123.19 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ⚪; ret5=+21.8; leftover $775.59 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `LENZ` | 134 | $5.75 | $2.45 | — | $14,891.25 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ⚪; ret5=+21.2; leftover $775.59 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,891.25 | ▼ close $9,154.86 vs 09:30 $9,319.85 (session -132.41) | 16:00 close · cash $14,891.25 · equity $9,154.86 vs 09:30 $9,319.85 (-164.99; session marks -132.41) · 8 name(s) marked open→close (per-name table). GPRO×322 09:30 $1.48 → close $1.70 -70.84; TARS×6 09:30 $82.70 → close $90.78 -48.48; BAK×399 09:30 $1.94 → close $1.89 +19.95; SLBT×246 09:30 $3.15 → close $2.88 +66.42; IRD×171 09:30 $4.53 → close $4.67 -23.94; FMC×59 09:30 $12.95 → close $12.97 -1.18; BRR×308 09:30 $2.51 → close $2.66 -46.20; LENZ×134 09:30 $5.75 → close $5.96 -28.14 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,891.25 | ▲ 09:30 equity $9,202.06 vs yday $9,154.86 (+47.20) | 09:30 open · cash $14,891.25 (unchanged overnight, no fees) · equity $9,202.06 vs prior close $9,154.86 (+47.20) · 8 name(s) re-marked at the open (per-name table). GPRO×322 yday $1.70 → 09:30 $1.56 +43.47; TARS×6 yday $90.78 → 09:30 $89.67 +6.66; BAK×399 yday $1.89 → 09:30 $1.94 -19.95; SLBT×246 yday $2.88 → 09:30 $2.88 -0.00; IRD×171 yday $4.67 → 09:30 $4.53 +23.94; FMC×59 yday $12.97 → 09:30 $13.11 -8.26; BRR×308 yday $2.66 → 09:30 $2.66 -0.00; LENZ×134 yday $5.96 → 09:30 $5.95 +1.34 | — |
| 2026-09-08 09:30 ET | **COVER** | `GPRO` | 322 | $1.56 | $4.15 | $+60.84 | $14,383.16 | ▲ +60.84 after sell → book $9,197.90; vs 09:30 mark -4.16 | dropped from list after 2 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **COVER** | `TARS` | 6 | $89.67 | $2.01 | $-45.51 | $13,843.13 | ▼ -45.51 after sell → book $9,195.89; vs 09:30 mark -2.01 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `BAK` | 399 | $1.94 | $5.15 | $-10.39 | $13,063.93 | ▼ -10.39 after sell → book $9,190.75; vs 09:30 mark -5.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `SLBT` | 246 | $2.88 | $3.17 | $+60.00 | $12,352.27 | ▲ +60.00 after sell → book $9,187.57; vs 09:30 mark -3.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `IRD` | 171 | $4.53 | $2.50 | $-5.07 | $11,575.14 | ▼ -5.07 after sell → book $9,185.07; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `FMC` | 59 | $13.11 | $2.17 | $-13.81 | $10,799.48 | ▼ -13.81 after sell → book $9,182.90; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `BRR` | 308 | $2.66 | $3.97 | $-54.23 | $9,976.23 | ▼ -54.23 after sell → book $9,178.93; vs 09:30 mark -3.97 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **COVER** | `LENZ` | 134 | $5.95 | $2.39 | $-31.64 | $9,176.54 | ▼ -31.64 after sell → book $9,176.54; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,176.54 | ▲ close $9,176.54 vs 09:30 $9,202.06 (session +0.00) | 16:00 close · cash $9,176.54 · no lots left · equity $9,176.54. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,176.54 | ▲ 09:30 equity $9,176.54 vs yday $9,176.54 (-0.00) | 09:30 open · cash $9,176.54 · no holdings · equity $9,176.54 vs prior close $9,176.54 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,176.54 | ▲ close $9,176.54 vs 09:30 $9,176.54 (session +0.00) | 16:00 close · cash $9,176.54 · no lots left · equity $9,176.54. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,176.54 | ▲ 09:30 equity $9,176.54 vs yday $9,176.54 (-0.00) | 09:30 open · cash $9,176.54 · no holdings · equity $9,176.54 vs prior close $9,176.54 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,176.54 | ▲ close $9,176.54 vs 09:30 $9,176.54 (session +0.00) | 16:00 close · cash $9,176.54 · no lots left · equity $9,176.54. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,176.54 | ▲ 09:30 equity $9,176.54 vs yday $9,176.54 (-0.00) | 09:30 open · cash $9,176.54 · no holdings · equity $9,176.54 vs prior close $9,176.54 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **SHORT** | `COHU` | 10 | $56.09 | $2.06 | — | $9,735.38 | — | ret_5>15; gate ret_5_min=15.0; list flatten; 🔵; ⚪; ret5=+15.3; leftover $573.53 | join🟢 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **SHORT** | `INDP` | 212 | $2.70 | $2.80 | — | $10,304.99 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $573.53 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **SHORT** | `BNC` | 116 | $4.91 | $2.38 | — | $10,872.16 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+69.9; leftover $573.53 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **SHORT** | `ANGX` | 106 | $5.38 | $2.35 | — | $11,440.09 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+19.8; leftover $573.53 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **SHORT** | `IRD` | 93 | $6.16 | $2.31 | — | $12,010.66 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+36.4; leftover $573.53 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **SHORT** | `CYPH` | 239 | $2.39 | $3.15 | — | $12,578.72 | — | ret_5>15; gate ret_5_min=15.0; list yday_mover; 🔵; ret5=+31.0; leftover $573.53 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **SHORT** | `PAYP` | 31 | $18.30 | $2.12 | — | $13,143.91 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ⚪; ret5=+16.1; leftover $573.53 | join🟢 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **SHORT** | `CRWV` | 6 | $91.08 | $2.04 | — | $13,688.34 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ⚪; ret5=+17.6; leftover $573.53 | join🟡 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,688.34 | ▲ close $9,186.86 vs 09:30 $9,176.54 (session +29.53) | 16:00 close · cash $13,688.34 · equity $9,186.86 vs 09:30 $9,176.54 (+10.32; session marks +29.53) · 8 name(s) marked open→close (per-name table). COHU×10 09:30 $56.09 → close $57.08 -9.90; INDP×212 09:30 $2.70 → close $2.77 -14.84; BNC×116 09:30 $4.91 → close $4.80 +12.76; ANGX×106 09:30 $5.38 → close $5.45 -7.42; IRD×93 09:30 $6.16 → close $6.04 +11.16; CYPH×239 09:30 $2.39 → close $2.27 +29.88; PAYP×31 09:30 $18.30 → close $18.45 -4.65; CRWV×6 09:30 $91.08 → close $88.99 +12.54 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HIVE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `SNDK` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `SMTC` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ALM` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KURA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NMAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `USDE` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ASST` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SLS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `COIN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DFDV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CNXC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NABL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PURR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SKYX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RXST` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RZLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `SID` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `USDE` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `DFDV` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CRCL` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ORBS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `BE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `PAYP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `HYLN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LIFE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SID` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `COHU` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BNC` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `WYHG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `GPRO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `EQ` | hard_red | hard-red S=-13.28 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `COHU` | 10 | 2026-09-11 @ $56.09 | ret_5>15; gate ret_5_min=15.0; list flatten; 🔵; ⚪; ret5=+15.3; leftover $573.53 |
| `INDP` | 212 | 2026-09-11 @ $2.70 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $573.53 |
| `BNC` | 116 | 2026-09-11 @ $4.91 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+69.9; leftover $573.53 |
| `ANGX` | 106 | 2026-09-11 @ $5.38 | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+19.8; leftover $573.53 |
| `IRD` | 93 | 2026-09-11 @ $6.16 | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+36.4; leftover $573.53 |
| `CYPH` | 239 | 2026-09-11 @ $2.39 | ret_5>15; gate ret_5_min=15.0; list yday_mover; 🔵; ret5=+31.0; leftover $573.53 |
| `PAYP` | 31 | 2026-09-11 @ $18.30 | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ⚪; ret5=+16.1; leftover $573.53 |
| `CRWV` | 6 | 2026-09-11 @ $91.08 | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ⚪; ret5=+17.6; leftover $573.53 |
