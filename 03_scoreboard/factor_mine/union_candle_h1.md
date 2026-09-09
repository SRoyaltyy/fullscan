# Factor mine action — `union_candle_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ candle, no 🚨

Cash book **+0.38%** ($10,038) · signal-only (no cash/fees) was +2.73%. Starts YES **5/18**. Fills 154 · skips 56 · realized $+38.03.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the prior-candle capture flag is on.
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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `candle_capture=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,038.03.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `IREN` | 72 | — | $45.98 | +0.00 | $44.76 | -87.84 | -87.84 | +0.00 | -87.84 |
| 2026-08-13 | `TPG` | 65 | — | $50.62 | +0.00 | $54.62 | +259.79 | +259.79 | +0.00 | +259.79 |
| 2026-08-13 | `TNDM` | 142 | — | $23.33 | +0.00 | $23.13 | -28.40 | -28.40 | +0.00 | -28.40 |
| 2026-08-14 | `IREN` | 72 | $44.76 | $44.09 | -48.24 | — | +0.00 | -48.24 | -136.08 | — |
| 2026-08-14 | `TPG` | 65 | $54.62 | $55.29 | +43.55 | — | +0.00 | +43.55 | +303.34 | — |
| 2026-08-14 | `TNDM` | 142 | $23.13 | $22.92 | -29.82 | — | +0.00 | -29.82 | -58.22 | — |
| 2026-08-14 | `SLG` | 21 | — | $57.61 | +0.00 | $56.09 | -31.92 | -31.92 | +0.00 | -31.92 |
| 2026-08-14 | `ANGX` | 292 | — | $4.31 | +0.00 | $4.37 | +17.52 | +17.52 | +0.00 | +17.52 |
| 2026-08-14 | `WDC` | 2 | — | $503.50 | +0.00 | $508.80 | +10.60 | +10.60 | +0.00 | +10.60 |
| 2026-08-14 | `ADUR` | 76 | — | $16.50 | +0.00 | $16.17 | -25.08 | -25.08 | +0.00 | -25.08 |
| 2026-08-14 | `ARX` | 64 | — | $19.57 | +0.00 | $19.58 | +0.64 | +0.64 | +0.00 | +0.64 |
| 2026-08-14 | `AIRO` | 113 | — | $11.12 | +0.00 | $9.57 | -175.15 | -175.15 | +0.00 | -175.15 |
| 2026-08-14 | `QMLS` | 173 | — | $7.29 | +0.00 | $7.32 | +5.19 | +5.19 | +0.00 | +5.19 |
| 2026-08-14 | `TBBB` | 25 | — | $48.82 | +0.00 | $47.79 | -25.75 | -25.75 | +0.00 | -25.75 |
| 2026-08-17 | `SLG` | 21 | $56.09 | $55.37 | -15.12 | — | +0.00 | -15.12 | -47.04 | — |
| 2026-08-17 | `ANGX` | 292 | $4.37 | $4.60 | +67.16 | — | +0.00 | +67.16 | +84.68 | — |
| 2026-08-17 | `WDC` | 2 | $508.80 | $525.53 | +33.46 | — | +0.00 | +33.46 | +44.06 | — |
| 2026-08-17 | `ADUR` | 76 | $16.17 | $15.73 | -33.44 | — | +0.00 | -33.44 | -58.52 | — |
| 2026-08-17 | `ARX` | 64 | $19.58 | $19.57 | -0.64 | — | +0.00 | -0.64 | +0.00 | — |
| 2026-08-17 | `AIRO` | 113 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -175.15 | — |
| 2026-08-17 | `QMLS` | 173 | $7.32 | $7.24 | -13.84 | — | +0.00 | -13.84 | -8.65 | — |
| 2026-08-17 | `TBBB` | 25 | $47.79 | $47.39 | -10.00 | — | +0.00 | -10.00 | -35.75 | — |
| 2026-08-17 | `DVN` | 26 | — | $46.18 | +0.00 | $47.57 | +36.14 | +36.14 | +0.00 | +36.14 |
| 2026-08-17 | `FANG` | 6 | — | $202.70 | +0.00 | $206.29 | +21.54 | +21.54 | +0.00 | +21.54 |
| 2026-08-17 | `CDNL` | 30 | — | $39.85 | +0.00 | $39.23 | -18.60 | -18.60 | +0.00 | -18.60 |
| 2026-08-17 | `ABX` | 135 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `VERA` | 39 | — | $31.30 | +0.00 | $31.63 | +12.87 | +12.87 | +0.00 | +12.87 |
| 2026-08-17 | `HTFL` | 29 | — | $41.23 | +0.00 | $41.94 | +20.59 | +20.59 | +0.00 | +20.59 |
| 2026-08-17 | `UMAC` | 37 | — | $32.55 | +0.00 | $30.15 | -88.80 | -88.80 | +0.00 | -88.80 |
| 2026-08-17 | `NPWR` | 641 | — | $1.92 | +0.00 | $1.73 | -121.79 | -121.79 | +0.00 | -121.79 |
| 2026-08-18 | `DVN` | 26 | $47.57 | $48.00 | +11.18 | — | +0.00 | +11.18 | +47.32 | — |
| 2026-08-18 | `FANG` | 6 | $206.29 | $208.93 | +15.84 | — | +0.00 | +15.84 | +37.38 | — |
| 2026-08-18 | `CDNL` | 30 | $39.23 | $41.57 | +70.20 | — | +0.00 | +70.20 | +51.60 | — |
| 2026-08-18 | `ABX` | 135 | $9.12 | $9.03 | -12.15 | — | +0.00 | -12.15 | -12.15 | — |
| 2026-08-18 | `VERA` | 39 | $31.63 | $31.31 | -12.48 | — | +0.00 | -12.48 | +0.39 | — |
| 2026-08-18 | `HTFL` | 29 | $41.94 | $41.50 | -12.76 | — | +0.00 | -12.76 | +7.83 | — |
| 2026-08-18 | `UMAC` | 37 | $30.15 | $28.59 | -57.72 | — | +0.00 | -57.72 | -146.52 | — |
| 2026-08-18 | `NPWR` | 641 | $1.73 | $1.70 | -19.23 | — | +0.00 | -19.23 | -141.02 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 58 | — | $20.55 | +0.00 | $21.19 | +37.12 | +37.12 | +0.00 | +37.12 |
| 2026-08-20 | `CDE` | 58 | — | $20.65 | +0.00 | $21.11 | +26.68 | +26.68 | +0.00 | +26.68 |
| 2026-08-20 | `IAG` | 61 | — | $19.63 | +0.00 | $20.50 | +53.07 | +53.07 | +0.00 | +53.07 |
| 2026-08-20 | `KGC` | 40 | — | $29.63 | +0.00 | $31.43 | +72.00 | +72.00 | +0.00 | +72.00 |
| 2026-08-20 | `NFGC` | 689 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-20 | `ABUS` | 245 | — | $4.92 | +0.00 | $4.77 | -36.75 | -36.75 | +0.00 | -36.75 |
| 2026-08-20 | `AEM` | 5 | — | $204.45 | +0.00 | $212.04 | +37.95 | +37.95 | +0.00 | +37.95 |
| 2026-08-21 | `AG` | 58 | $21.19 | $21.90 | +41.18 | — | +0.00 | +41.18 | +78.30 | — |
| 2026-08-21 | `CDE` | 58 | $21.11 | $21.75 | +37.12 | — | +0.00 | +37.12 | +63.80 | — |
| 2026-08-21 | `IAG` | 61 | $20.50 | $21.17 | +40.87 | — | +0.00 | +40.87 | +93.94 | — |
| 2026-08-21 | `KGC` | 40 | $31.43 | $32.17 | +29.60 | — | +0.00 | +29.60 | +101.60 | — |
| 2026-08-21 | `NFGC` | 689 | $1.75 | $1.79 | +27.56 | — | +0.00 | +27.56 | +27.56 | — |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | — | +0.00 | +35.60 | +81.28 | — |
| 2026-08-21 | `ABUS` | 245 | $4.77 | $5.20 | +105.35 | — | +0.00 | +105.35 | +68.60 | — |
| 2026-08-21 | `AEM` | 5 | $212.04 | $216.30 | +21.30 | $216.06 | -1.20 | +20.10 | +59.25 | +58.05 |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `AUPH` | 75 | — | $17.20 | +0.00 | $16.65 | -41.25 | -41.25 | +0.00 | -41.25 |
| 2026-08-21 | `ARCT` | 116 | — | $11.13 | +0.00 | $13.45 | +269.12 | +269.12 | +0.00 | +269.12 |
| 2026-08-21 | `CRSP` | 21 | — | $59.72 | +0.00 | $59.50 | -4.62 | -4.62 | +0.00 | -4.62 |
| 2026-08-21 | `CYPH` | 985 | — | $1.32 | +0.00 | $1.42 | +98.50 | +98.50 | +0.00 | +98.50 |
| 2026-08-21 | `GMAB` | 38 | — | $33.36 | +0.00 | $33.45 | +3.42 | +3.42 | +0.00 | +3.42 |
| 2026-08-21 | `BTBT` | 783 | — | $1.66 | +0.00 | $1.53 | -101.79 | -101.79 | +0.00 | -101.79 |
| 2026-08-24 | `AEM` | 5 | $216.06 | $217.03 | +4.85 | — | +0.00 | +4.85 | +62.90 | — |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.51 | -7.10 | — | +0.00 | -7.10 | +10.80 | — |
| 2026-08-24 | `AUPH` | 75 | $16.65 | $16.57 | -6.00 | — | +0.00 | -6.00 | -47.25 | — |
| 2026-08-24 | `ARCT` | 116 | $13.45 | $13.33 | -13.92 | — | +0.00 | -13.92 | +255.20 | — |
| 2026-08-24 | `CRSP` | 21 | $59.50 | $58.75 | -15.75 | — | +0.00 | -15.75 | -20.37 | — |
| 2026-08-24 | `CYPH` | 985 | $1.42 | $1.83 | +403.85 | — | +0.00 | +403.85 | +502.35 | — |
| 2026-08-24 | `GMAB` | 38 | $33.45 | $32.82 | -23.94 | — | +0.00 | -23.94 | -20.52 | — |
| 2026-08-24 | `BTBT` | 783 | $1.53 | $1.55 | +15.66 | — | +0.00 | +15.66 | -86.13 | — |
| 2026-08-25 | `HCA` | 3 | — | $426.97 | +0.00 | $428.76 | +5.37 | +5.37 | +0.00 | +5.37 |
| 2026-08-25 | `KURA` | 98 | — | $13.59 | +0.00 | $13.59 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `CCOI` | 141 | — | $9.49 | +0.00 | $9.88 | +54.99 | +54.99 | +0.00 | +54.99 |
| 2026-08-25 | `LIFE` | 36 | — | $36.96 | +0.00 | $38.56 | +57.60 | +57.60 | +0.00 | +57.60 |
| 2026-08-25 | `NPWR` | 669 | — | $2.00 | +0.00 | $1.95 | -33.45 | -33.45 | +0.00 | -33.45 |
| 2026-08-25 | `ALVO` | 255 | — | $5.24 | +0.00 | $5.05 | -48.45 | -48.45 | +0.00 | -48.45 |
| 2026-08-25 | `SUJA` | 152 | — | $8.79 | +0.00 | $9.33 | +82.08 | +82.08 | +0.00 | +82.08 |
| 2026-08-25 | `FWDI` | 234 | — | $5.71 | +0.00 | $6.05 | +79.56 | +79.56 | +0.00 | +79.56 |
| 2026-08-26 | `HCA` | 3 | $428.76 | $427.50 | -3.78 | — | +0.00 | -3.78 | +1.59 | — |
| 2026-08-26 | `KURA` | 98 | $13.59 | $13.63 | +3.92 | — | +0.00 | +3.92 | +3.92 | — |
| 2026-08-26 | `CCOI` | 141 | $9.88 | $9.89 | +1.41 | — | +0.00 | +1.41 | +56.40 | — |
| 2026-08-26 | `LIFE` | 36 | $38.56 | $38.24 | -11.52 | — | +0.00 | -11.52 | +46.08 | — |
| 2026-08-26 | `NPWR` | 669 | $1.95 | $1.93 | -13.38 | — | +0.00 | -13.38 | -46.83 | — |
| 2026-08-26 | `ALVO` | 255 | $5.05 | $4.98 | -17.85 | — | +0.00 | -17.85 | -66.30 | — |
| 2026-08-26 | `SUJA` | 152 | $9.33 | $9.39 | +9.12 | $9.44 | +7.60 | +16.72 | +91.20 | +98.80 |
| 2026-08-26 | `FWDI` | 234 | $6.05 | $5.97 | -18.72 | — | +0.00 | -18.72 | +60.84 | — |
| 2026-08-26 | `CRMD` | 155 | — | $8.60 | +0.00 | $8.39 | -32.55 | -32.55 | +0.00 | -32.55 |
| 2026-08-26 | `AVBP` | 42 | — | $31.21 | +0.00 | $31.14 | -2.94 | -2.94 | +0.00 | -2.94 |
| 2026-08-26 | `BZ` | 79 | — | $16.77 | +0.00 | $18.84 | +163.53 | +163.53 | +0.00 | +163.53 |
| 2026-08-26 | `ACRS` | 205 | — | $6.53 | +0.00 | $6.19 | -69.70 | -69.70 | +0.00 | -69.70 |
| 2026-08-26 | `TMCI` | 280 | — | $4.78 | +0.00 | $4.72 | -16.80 | -16.80 | +0.00 | -16.80 |
| 2026-08-26 | `BRR` | 609 | — | $2.20 | +0.00 | $2.17 | -18.27 | -18.27 | +0.00 | -18.27 |
| 2026-08-26 | `GRRR` | 95 | — | $14.03 | +0.00 | $15.45 | +134.90 | +134.90 | +0.00 | +134.90 |
| 2026-08-27 | `SUJA` | 152 | $9.44 | $9.41 | -4.56 | — | +0.00 | -4.56 | +94.24 | — |
| 2026-08-27 | `CRMD` | 155 | $8.39 | $8.49 | +15.50 | — | +0.00 | +15.50 | -17.05 | — |
| 2026-08-27 | `AVBP` | 42 | $31.14 | $30.79 | -14.70 | — | +0.00 | -14.70 | -17.64 | — |
| 2026-08-27 | `BZ` | 79 | $18.84 | $18.50 | -26.86 | — | +0.00 | -26.86 | +136.67 | — |
| 2026-08-27 | `ACRS` | 205 | $6.19 | $6.15 | -8.20 | — | +0.00 | -8.20 | -77.90 | — |
| 2026-08-27 | `TMCI` | 280 | $4.72 | $4.72 | +0.00 | — | +0.00 | +0.00 | -16.80 | — |
| 2026-08-27 | `BRR` | 609 | $2.17 | $2.19 | +12.18 | — | +0.00 | +12.18 | -6.09 | — |
| 2026-08-27 | `GRRR` | 95 | $15.45 | $15.94 | +46.55 | — | +0.00 | +46.55 | +181.45 | — |
| 2026-08-27 | `RRC` | 33 | — | $41.44 | +0.00 | $41.64 | +6.60 | +6.60 | +0.00 | +6.60 |
| 2026-08-27 | `CRK` | 94 | — | $14.42 | +0.00 | $14.62 | +18.80 | +18.80 | +0.00 | +18.80 |
| 2026-08-27 | `SLI` | 526 | — | $2.60 | +0.00 | $2.64 | +21.04 | +21.04 | +0.00 | +21.04 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-27 | `ANET` | 6 | — | $205.90 | +0.00 | $201.09 | -28.86 | -28.86 | +0.00 | -28.86 |
| 2026-08-27 | `DLO` | 89 | — | $15.33 | +0.00 | $15.14 | -16.91 | -16.91 | +0.00 | -16.91 |
| 2026-08-27 | `GEN` | 45 | — | $29.83 | +0.00 | $30.50 | +30.15 | +30.15 | +0.00 | +30.15 |
| 2026-08-27 | `MRVL` | 5 | — | $253.44 | +0.00 | $241.45 | -59.95 | -59.95 | +0.00 | -59.95 |
| 2026-08-28 | `RRC` | 33 | $41.64 | $41.74 | +3.30 | $41.46 | -9.24 | -5.94 | +9.90 | +0.66 |
| 2026-08-28 | `CRK` | 94 | $14.62 | $14.63 | +0.94 | $14.29 | -31.96 | -31.02 | +19.74 | -12.22 |
| 2026-08-28 | `SLI` | 526 | $2.64 | $2.68 | +21.04 | $2.55 | -68.38 | -47.34 | +42.08 | -26.30 |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `ANET` | 6 | $201.09 | $200.00 | -6.54 | — | +0.00 | -6.54 | -35.40 | — |
| 2026-08-28 | `DLO` | 89 | $15.14 | $15.19 | +4.45 | — | +0.00 | +4.45 | -12.46 | — |
| 2026-08-28 | `GEN` | 45 | $30.50 | $30.50 | +0.00 | — | +0.00 | +0.00 | +30.15 | — |
| 2026-08-28 | `MRVL` | 5 | $241.45 | $225.26 | -80.95 | — | +0.00 | -80.95 | -140.90 | — |
| 2026-08-28 | `SEDG` | 40 | — | $32.90 | +0.00 | $31.41 | -59.60 | -59.60 | +0.00 | -59.60 |
| 2026-08-28 | `BZ` | 72 | — | $18.15 | +0.00 | $17.80 | -25.20 | -25.20 | +0.00 | -25.20 |
| 2026-08-28 | `CAPR` | 136 | — | $9.73 | +0.00 | $9.59 | -19.04 | -19.04 | +0.00 | -19.04 |
| 2026-08-28 | `LVWR` | 952 | — | $1.39 | +0.00 | $1.35 | -38.08 | -38.08 | +0.00 | -38.08 |
| 2026-08-28 | `VYX` | 144 | — | $9.13 | +0.00 | $8.78 | -50.40 | -50.40 | +0.00 | -50.40 |
| 2026-08-31 | `RRC` | 33 | $41.46 | $42.00 | +17.82 | — | +0.00 | +17.82 | +18.48 | — |
| 2026-08-31 | `CRK` | 94 | $14.29 | $14.54 | +23.50 | — | +0.00 | +23.50 | +11.28 | — |
| 2026-08-31 | `SLI` | 526 | $2.55 | $2.58 | +15.78 | — | +0.00 | +15.78 | -10.52 | — |
| 2026-08-31 | `SEDG` | 40 | $31.41 | $31.15 | -10.40 | — | +0.00 | -10.40 | -70.00 | — |
| 2026-08-31 | `BZ` | 72 | $17.80 | $17.70 | -7.20 | — | +0.00 | -7.20 | -32.40 | — |
| 2026-08-31 | `CAPR` | 136 | $9.59 | $9.50 | -12.24 | — | +0.00 | -12.24 | -31.28 | — |
| 2026-08-31 | `LVWR` | 952 | $1.35 | $1.30 | -47.60 | — | +0.00 | -47.60 | -85.68 | — |
| 2026-08-31 | `VYX` | 144 | $8.78 | $8.66 | -17.28 | — | +0.00 | -17.28 | -67.68 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 24 | — | $52.88 | +0.00 | $52.46 | -10.08 | -10.08 | +0.00 | -10.08 |
| 2026-09-03 | `HRMY` | 30 | — | $42.93 | +0.00 | $41.86 | -32.10 | -32.10 | +0.00 | -32.10 |
| 2026-09-03 | `CABA` | 357 | — | $3.63 | +0.00 | $3.48 | -53.55 | -53.55 | +0.00 | -53.55 |
| 2026-09-03 | `VSTM` | 161 | — | $8.03 | +0.00 | $7.98 | -8.05 | -8.05 | +0.00 | -8.05 |
| 2026-09-03 | `RVTY` | 9 | — | $132.45 | +0.00 | $130.63 | -16.38 | -16.38 | +0.00 | -16.38 |
| 2026-09-03 | `MRNA` | 8 | — | $145.94 | +0.00 | $148.87 | +23.40 | +23.40 | +0.00 | +23.40 |
| 2026-09-03 | `ARCT` | 77 | — | $16.77 | +0.00 | $15.56 | -93.17 | -93.17 | +0.00 | -93.17 |
| 2026-09-03 | `SLN` | 87 | — | $14.85 | +0.00 | $14.79 | -5.22 | -5.22 | +0.00 | -5.22 |
| 2026-09-04 | `ATRC` | 24 | $52.46 | $52.03 | -10.32 | — | +0.00 | -10.32 | -20.40 | — |
| 2026-09-04 | `HRMY` | 30 | $41.86 | $41.50 | -10.80 | $42.25 | +22.50 | +11.70 | -42.90 | -20.40 |
| 2026-09-04 | `CABA` | 357 | $3.48 | $3.46 | -7.14 | — | +0.00 | -7.14 | -60.69 | — |
| 2026-09-04 | `VSTM` | 161 | $7.98 | $7.91 | -11.27 | — | +0.00 | -11.27 | -19.32 | — |
| 2026-09-04 | `RVTY` | 9 | $130.63 | $130.03 | -5.40 | — | +0.00 | -5.40 | -21.78 | — |
| 2026-09-04 | `MRNA` | 8 | $148.87 | $153.62 | +38.00 | — | +0.00 | +38.00 | +61.40 | — |
| 2026-09-04 | `ARCT` | 77 | $15.56 | $15.61 | +3.85 | — | +0.00 | +3.85 | -89.32 | — |
| 2026-09-04 | `SLN` | 87 | $14.79 | $14.63 | -13.92 | — | +0.00 | -13.92 | -19.14 | — |
| 2026-09-04 | `BMEA` | 668 | — | $1.90 | +0.00 | $2.03 | +86.84 | +86.84 | +0.00 | +86.84 |
| 2026-09-04 | `CRM` | 4 | — | $263.36 | +0.00 | $259.23 | -16.52 | -16.52 | +0.00 | -16.52 |
| 2026-09-04 | `CCOI` | 126 | — | $10.02 | +0.00 | $10.05 | +3.78 | +3.78 | +0.00 | +3.78 |
| 2026-09-04 | `UAMY` | 241 | — | $5.25 | +0.00 | $5.20 | -12.05 | -12.05 | +0.00 | -12.05 |
| 2026-09-04 | `SLBT` | 403 | — | $3.15 | +0.00 | $2.88 | -108.81 | -108.81 | +0.00 | -108.81 |
| 2026-09-04 | `IRD` | 280 | — | $4.53 | +0.00 | $4.67 | +39.20 | +39.20 | +0.00 | +39.20 |
| 2026-09-04 | `FMC` | 98 | — | $12.95 | +0.00 | $12.97 | +1.96 | +1.96 | +0.00 | +1.96 |
| 2026-09-08 | `HRMY` | 30 | $42.25 | $42.20 | -1.50 | — | +0.00 | -1.50 | -21.90 | — |
| 2026-09-08 | `BMEA` | 668 | $2.03 | $2.00 | -20.04 | — | +0.00 | -20.04 | +66.80 | — |
| 2026-09-08 | `CRM` | 4 | $259.23 | $253.72 | -22.04 | — | +0.00 | -22.04 | -38.56 | — |
| 2026-09-08 | `CCOI` | 126 | $10.05 | $9.98 | -8.82 | — | +0.00 | -8.82 | -5.04 | — |
| 2026-09-08 | `UAMY` | 241 | $5.20 | $5.28 | +19.28 | — | +0.00 | +19.28 | +7.23 | — |
| 2026-09-08 | `SLBT` | 403 | $2.88 | $2.88 | +0.00 | — | +0.00 | +0.00 | -108.81 | — |
| 2026-09-08 | `IRD` | 280 | $4.67 | $4.53 | -39.20 | — | +0.00 | -39.20 | +0.00 | — |
| 2026-09-08 | `FMC` | 98 | $12.97 | $13.11 | +13.72 | — | +0.00 | +13.72 | +15.68 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +143.55 | IREN, TPG, TNDM | — | $79.27 | $10,136.75 | IREN×72, TPG×65, TNDM×142 |
| 2026-08-14 | +5.50 | $79.27 | IREN×72, TPG×65, TNDM×142 | $10,102.24 | -34.51 | -223.95 | SLG, ANGX, WDC, ADUR, ARX, AIRO, QMLS, TBBB | IREN, TPG, TNDM | $356.14 | $9,852.23 | SLG×21, ANGX×292, WDC×2, ADUR×76, ARX×64, AIRO×113, QMLS×173, TBBB×25 |
| 2026-08-17 | +2.25 | $356.14 | SLG×21, ANGX×292, WDC×2, ADUR×76, ARX×64, AIRO×113, QMLS×173, TBBB×25 | $9,879.81 | +27.58 | -138.05 | DVN, FANG, CDNL, ABX, VERA, HTFL, UMAC, NPWR | SLG, ANGX, WDC, ADUR, ARX, AIRO, QMLS, TBBB | $142.34 | $9,699.31 | DVN×26, FANG×6, CDNL×30, ABX×135, VERA×39, HTFL×29, UMAC×37, NPWR×641 |
| 2026-08-18 | -6.20 | $142.34 | DVN×26, FANG×6, CDNL×30, ABX×135, VERA×39, HTFL×29, UMAC×37, NPWR×641 | $9,682.19 | -17.12 | +0.00 | — | DVN, FANG, CDNL, ABX, VERA, HTFL, UMAC, NPWR | $9,658.82 | $9,658.82 | — |
| 2026-08-19 | -7.20 | $9,658.82 | — | $9,658.82 | -0.00 | +0.00 | — | — | $9,658.82 | $9,658.82 | — |
| 2026-08-20 | +1.12 | $9,658.82 | — | $9,658.82 | -0.00 | +235.75 | AG, CDE, IAG, KGC, NFGC, WPM, ABUS, AEM | — | $272.19 | $9,869.89 | AG×58, CDE×58, IAG×61, KGC×40, NFGC×689, WPM×8, ABUS×245, AEM×5 |
| 2026-08-21 | +3.25 | $272.19 | AG×58, CDE×58, IAG×61, KGC×40, NFGC×689, WPM×8, ABUS×245, AEM×5 | $10,208.47 | +338.58 | +240.08 | AU, AUPH, ARCT, CRSP, CYPH, GMAB, BTBT | AG, CDE, IAG, KGC, NFGC, WPM, ABUS | $173.32 | $10,392.06 | AEM×5, AU×10, AUPH×75, ARCT×116, CRSP×21, CYPH×985, GMAB×38, BTBT×783 |
| 2026-08-24 | -5.17 | $173.32 | AEM×5, AU×10, AUPH×75, ARCT×116, CRSP×21, CYPH×985, GMAB×38, BTBT×783 | $10,749.71 | +357.65 | +0.00 | — | AEM, AU, AUPH, ARCT, CRSP, CYPH, GMAB, BTBT | $10,713.72 | $10,713.72 | — |
| 2026-08-25 | +1.80 | $10,713.72 | — | $10,713.72 | -0.00 | +197.70 | HCA, KURA, CCOI, LIFE, NPWR, ALVO, SUJA, FWDI | — | $59.74 | $10,885.24 | HCA×3, KURA×98, CCOI×141, LIFE×36, NPWR×669, ALVO×255, SUJA×152, FWDI×234 |
| 2026-08-26 | +2.02 | $59.74 | HCA×3, KURA×98, CCOI×141, LIFE×36, NPWR×669, ALVO×255, SUJA×152, FWDI×234 | $10,834.44 | -50.80 | +165.77 | CRMD, AVBP, BZ, ACRS, TMCI, BRR, GRRR | HCA, KURA, CCOI, LIFE, NPWR, ALVO, FWDI | $41.56 | $10,952.96 | SUJA×152, CRMD×155, AVBP×42, BZ×79, ACRS×205, TMCI×280, BRR×609, GRRR×95 |
| 2026-08-27 | — | $41.56 | SUJA×152, CRMD×155, AVBP×42, BZ×79, ACRS×205, TMCI×280, BRR×609, GRRR×95 | $10,972.87 | +19.91 | -60.75 | RRC, CRK, SLI, MU, ANET, DLO, GEN, MRVL | SUJA, CRMD, AVBP, BZ, ACRS, TMCI, BRR, GRRR | $658.42 | $10,864.60 | RRC×33, CRK×94, SLI×526, MU×1, ANET×6, DLO×89, GEN×45, MRVL×5 |
| 2026-08-28 | +0.75 | $658.42 | RRC×33, CRK×94, SLI×526, MU×1, ANET×6, DLO×89, GEN×45, MRVL×5 | $10,790.74 | -73.86 | -301.90 | SEDG, BZ, CAPR, LVWR, VYX | MU, ANET, DLO, GEN, MRVL | $12.43 | $10,456.93 | RRC×33, CRK×94, SLI×526, SEDG×40, BZ×72, CAPR×136, LVWR×952, VYX×144 |
| 2026-08-31 | -5.85 | $12.43 | RRC×33, CRK×94, SLI×526, SEDG×40, BZ×72, CAPR×136, LVWR×952, VYX×144 | $10,419.31 | -37.62 | +0.00 | — | RRC, CRK, SLI, SEDG, BZ, CAPR, LVWR, VYX | $10,386.32 | $10,386.32 | — |
| 2026-09-01 | -6.30 | $10,386.32 | — | $10,386.32 | +0.00 | +0.00 | — | — | $10,386.32 | $10,386.32 | — |
| 2026-09-02 | -3.83 | $10,386.32 | — | $10,386.32 | +0.00 | +0.00 | — | — | $10,386.32 | $10,386.32 | — |
| 2026-09-03 | -0.90 | $10,386.32 | — | $10,386.32 | +0.00 | -195.15 | ATRC, HRMY, CABA, VSTM, RVTY, MRNA, ARCT, SLN | — | $277.99 | $10,171.45 | ATRC×24, HRMY×30, CABA×357, VSTM×161, RVTY×9, MRNA×8, ARCT×77, SLN×87 |
| 2026-09-04 | +2.25 | $277.99 | ATRC×24, HRMY×30, CABA×357, VSTM×161, RVTY×9, MRNA×8, ARCT×77, SLN×87 | $10,154.45 | -17.00 | +16.90 | BMEA, CRM, CCOI, UAMY, SLBT, IRD, FMC | ATRC, CABA, VSTM, RVTY, MRNA, ARCT, SLN | $207.04 | $10,126.30 | HRMY×30, BMEA×668, CRM×4, CCOI×126, UAMY×241, SLBT×403, IRD×280, FMC×98 |
| 2026-09-08 | -11.47 | $207.04 | HRMY×30, BMEA×668, CRM×4, CCOI×126, UAMY×241, SLBT×403, IRD×280, FMC×98 | $10,067.70 | -58.60 | +0.00 | — | HRMY, BMEA, CRM, CCOI, UAMY, SLBT, IRD, FMC | $10,038.03 | $10,038.03 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 72 | $45.98 | $2.21 | — | $6,687.23 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ⚪; ret5=+12.3; leftover $3333.33 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 65 | $50.62 | $2.19 | — | $3,394.54 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ⚪; ret5=+6.2; leftover $3333.33 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 142 | $23.33 | $2.42 | — | $79.27 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ⚪; ret5=+19.7; leftover $3333.33 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $79.27 | ▲ close $10,136.75 vs 09:30 $10,000.00 (session +143.55) | 16:00 close · cash $79.27 · equity $10,136.75 vs 09:30 $10,000.00 (+136.75; session marks +143.55) · 3 name(s) marked open→close (per-name table). IREN×72 09:30 $45.98 → close $44.76 -87.84; TPG×65 09:30 $50.62 → close $54.62 +259.79; TNDM×142 09:30 $23.33 → close $23.13 -28.40 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.27 | ▼ 09:30 equity $10,102.24 vs yday $10,136.75 (-34.51) | 09:30 open · cash $79.27 (unchanged overnight, no fees) · equity $10,102.24 vs prior close $10,136.75 (-34.51) · 3 name(s) re-marked at the open (per-name table). IREN×72 yday $44.76 → 09:30 $44.09 -48.24; TPG×65 yday $54.62 → 09:30 $55.29 +43.55; TNDM×142 yday $23.13 → 09:30 $22.92 -29.82 | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 72 | $44.09 | $2.24 | $-140.53 | $3,251.50 | ▼ -140.53 after sell → book $10,099.99; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 65 | $55.29 | $2.22 | $+298.93 | $6,843.13 | ▲ +298.93 after sell → book $10,097.77; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 142 | $22.92 | $2.47 | $-63.10 | $10,095.30 | ▼ -63.10 after sell → book $10,095.30; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 21 | $57.61 | $2.05 | — | $8,883.44 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ret5=+5.7; leftover $1261.91 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 292 | $4.31 | $3.77 | — | $7,621.15 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1261.91 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 2 | $503.50 | $2.00 | — | $6,612.16 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable; 🔵; ⚪; ret5=+7.9; leftover $1261.91 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 76 | $16.50 | $2.22 | — | $5,355.94 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1261.91 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 64 | $19.57 | $2.18 | — | $4,101.28 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1261.91 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 113 | $11.12 | $2.33 | — | $2,842.39 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1261.91 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 173 | $7.29 | $2.51 | — | $1,578.71 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $1261.91 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `TBBB` | 25 | $48.82 | $2.06 | — | $356.14 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.0; leftover $1261.91 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $356.14 | ▼ close $9,852.23 vs 09:30 $10,102.24 (session -223.95) | 16:00 close · cash $356.14 · equity $9,852.23 vs 09:30 $10,102.24 (-250.01; session marks -223.95) · 8 name(s) marked open→close (per-name table). SLG×21 09:30 $57.61 → close $56.09 -31.92; ANGX×292 09:30 $4.31 → close $4.37 +17.52; WDC×2 09:30 $503.50 → close $508.80 +10.60; ADUR×76 09:30 $16.50 → close $16.17 -25.08; ARX×64 09:30 $19.57 → close $19.58 +0.64; AIRO×113 09:30 $11.12 → close $9.57 -175.15; QMLS×173 09:30 $7.29 → close $7.32 +5.19; TBBB×25 09:30 $48.82 → close $47.79 -25.75 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $356.14 | ▲ 09:30 equity $9,879.81 vs yday $9,852.23 (+27.58) | 09:30 open · cash $356.14 (unchanged overnight, no fees) · equity $9,879.81 vs prior close $9,852.23 (+27.58) · 8 name(s) re-marked at the open (per-name table). SLG×21 yday $56.09 → 09:30 $55.37 -15.12; ANGX×292 yday $4.37 → 09:30 $4.60 +67.16; WDC×2 yday $508.80 → 09:30 $525.53 +33.46; ADUR×76 yday $16.17 → 09:30 $15.73 -33.44; ARX×64 yday $19.58 → 09:30 $19.57 -0.64; AIRO×113 yday $9.57 → 09:30 $9.57 +0.00; QMLS×173 yday $7.32 → 09:30 $7.24 -13.84; TBBB×25 yday $47.79 → 09:30 $47.39 -10.00 | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 21 | $55.37 | $2.07 | $-51.17 | $1,516.84 | ▼ -51.17 after sell → book $9,877.74; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 292 | $4.60 | $3.83 | $+77.09 | $2,856.21 | ▲ +77.09 after sell → book $9,873.91; vs 09:30 mark -3.83 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 2 | $525.53 | $2.02 | $+40.05 | $3,905.26 | ▲ +40.05 after sell → book $9,871.90; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 76 | $15.73 | $2.24 | $-62.98 | $5,098.50 | ▼ -62.98 after sell → book $9,869.66; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 64 | $19.57 | $2.20 | $-4.38 | $6,348.77 | ▼ -4.38 after sell → book $9,867.45; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 113 | $9.57 | $2.36 | $-179.84 | $7,427.83 | ▼ -179.84 after sell → book $9,865.10; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMLS` | 173 | $7.24 | $2.55 | $-13.71 | $8,677.80 | ▼ -13.71 after sell → book $9,862.55; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TBBB` | 25 | $47.39 | $2.08 | $-39.90 | $9,860.46 | ▼ -39.90 after sell → book $9,860.46; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 26 | $46.18 | $2.07 | — | $8,657.72 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ret5=+6.7; leftover $1232.56 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $7,439.51 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ret5=+8.3; leftover $1232.56 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 30 | $39.85 | $2.08 | — | $6,241.93 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1232.56 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 135 | $9.12 | $2.40 | — | $5,008.33 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1232.56 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 39 | $31.30 | $2.11 | — | $3,785.53 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; ret5=-3.8; leftover $1232.56 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 29 | $41.23 | $2.08 | — | $2,587.78 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=+46.0; leftover $1232.56 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 37 | $32.55 | $2.10 | — | $1,381.33 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1232.56 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 641 | $1.92 | $8.27 | — | $142.34 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1232.56 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $142.34 | ▼ close $9,699.31 vs 09:30 $9,879.81 (session -138.05) | 16:00 close · cash $142.34 · equity $9,699.31 vs 09:30 $9,879.81 (-180.50; session marks -138.05) · 8 name(s) marked open→close (per-name table). DVN×26 09:30 $46.18 → close $47.57 +36.14; FANG×6 09:30 $202.70 → close $206.29 +21.54; CDNL×30 09:30 $39.85 → close $39.23 -18.60; ABX×135 09:30 $9.12 → close $9.12 +0.00; VERA×39 09:30 $31.30 → close $31.63 +12.87; HTFL×29 09:30 $41.23 → close $41.94 +20.59; UMAC×37 09:30 $32.55 → close $30.15 -88.80; NPWR×641 09:30 $1.92 → close $1.73 -121.79 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $142.34 | ▼ 09:30 equity $9,682.19 vs yday $9,699.31 (-17.12) | 09:30 open · cash $142.34 (unchanged overnight, no fees) · equity $9,682.19 vs prior close $9,699.31 (-17.12) · 8 name(s) re-marked at the open (per-name table). DVN×26 yday $47.57 → 09:30 $48.00 +11.18; FANG×6 yday $206.29 → 09:30 $208.93 +15.84; CDNL×30 yday $39.23 → 09:30 $41.57 +70.20; ABX×135 yday $9.12 → 09:30 $9.03 -12.15; VERA×39 yday $31.63 → 09:30 $31.31 -12.48; HTFL×29 yday $41.94 → 09:30 $41.50 -12.76; UMAC×37 yday $30.15 → 09:30 $28.59 -57.72; NPWR×641 yday $1.73 → 09:30 $1.70 -19.23 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 26 | $48.00 | $2.09 | $+43.16 | $1,388.25 | ▲ +43.16 after sell → book $9,680.10; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $2,639.80 | ▲ +33.34 after sell → book $9,678.07; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 30 | $41.57 | $2.10 | $+47.42 | $3,884.80 | ▲ +47.42 after sell → book $9,675.97; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 135 | $9.03 | $2.43 | $-16.97 | $5,101.43 | ▼ -16.97 after sell → book $9,673.55; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERA` | 39 | $31.31 | $2.13 | $-3.84 | $6,320.39 | ▼ -3.84 after sell → book $9,671.42; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 29 | $41.50 | $2.10 | $+3.66 | $7,521.79 | ▲ +3.66 after sell → book $9,669.32; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 37 | $28.59 | $2.12 | $-150.74 | $8,577.50 | ▼ -150.74 after sell → book $9,667.20; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 641 | $1.70 | $8.39 | $-157.67 | $9,658.82 | ▼ -157.67 after sell → book $9,658.82; vs 09:30 mark -8.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,658.82 | ▲ close $9,658.82 vs 09:30 $9,682.19 (session +0.00) | 16:00 close · cash $9,658.82 · no lots left · equity $9,658.82. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,658.82 | ▲ 09:30 equity $9,658.82 vs yday $9,658.82 (-0.00) | 09:30 open · cash $9,658.82 · no holdings · equity $9,658.82 vs prior close $9,658.82 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,658.82 | ▲ close $9,658.82 vs 09:30 $9,658.82 (session +0.00) | 16:00 close · cash $9,658.82 · no lots left · equity $9,658.82. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,658.82 | ▲ 09:30 equity $9,658.82 vs yday $9,658.82 (-0.00) | 09:30 open · cash $9,658.82 · no holdings · equity $9,658.82 vs prior close $9,658.82 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 58 | $20.55 | $2.16 | — | $8,464.75 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1207.35 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 58 | $20.65 | $2.16 | — | $7,264.89 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1207.35 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 61 | $19.63 | $2.17 | — | $6,065.28 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1207.35 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 40 | $29.63 | $2.11 | — | $4,877.97 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1207.35 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 689 | $1.75 | $8.89 | — | $3,663.34 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1207.35 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $2,505.00 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1207.35 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 245 | $4.92 | $3.16 | — | $1,296.44 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1207.35 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AEM` | 5 | $204.45 | $2.00 | — | $272.19 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $1207.35 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $272.19 | ▲ close $9,869.89 vs 09:30 $9,658.82 (session +235.75) | 16:00 close · cash $272.19 · equity $9,869.89 vs 09:30 $9,658.82 (+211.07; session marks +235.75) · 8 name(s) marked open→close (per-name table). AG×58 09:30 $20.55 → close $21.19 +37.12; CDE×58 09:30 $20.65 → close $21.11 +26.68; IAG×61 09:30 $19.63 → close $20.50 +53.07; KGC×40 09:30 $29.63 → close $31.43 +72.00; NFGC×689 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68; ABUS×245 09:30 $4.92 → close $4.77 -36.75; AEM×5 09:30 $204.45 → close $212.04 +37.95 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $272.19 | ▲ 09:30 equity $10,208.47 vs yday $9,869.89 (+338.58) | 09:30 open · cash $272.19 (unchanged overnight, no fees) · equity $10,208.47 vs prior close $9,869.89 (+338.58) · 8 name(s) re-marked at the open (per-name table). AG×58 yday $21.19 → 09:30 $21.90 +41.18; CDE×58 yday $21.11 → 09:30 $21.75 +37.12; IAG×61 yday $20.50 → 09:30 $21.17 +40.87; KGC×40 yday $31.43 → 09:30 $32.17 +29.60; NFGC×689 yday $1.75 → 09:30 $1.79 +27.56; WPM×8 yday $150.25 → 09:30 $154.70 +35.60; ABUS×245 yday $4.77 → 09:30 $5.20 +105.35; AEM×5 yday $212.04 → 09:30 $216.30 +21.30 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 58 | $21.90 | $2.18 | $+73.95 | $1,540.20 | ▲ +73.95 after sell → book $10,206.28; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 58 | $21.75 | $2.18 | $+59.45 | $2,799.52 | ▲ +59.45 after sell → book $10,204.10; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 61 | $21.17 | $2.19 | $+89.57 | $4,088.70 | ▲ +89.57 after sell → book $10,201.91; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 40 | $32.17 | $2.13 | $+97.36 | $5,373.36 | ▲ +97.36 after sell → book $10,199.77; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 689 | $1.79 | $9.01 | $+9.66 | $6,597.66 | ▲ +9.66 after sell → book $10,190.76; vs 09:30 mark -9.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $7,833.23 | ▲ +77.23 after sell → book $10,188.73; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 245 | $5.20 | $3.21 | $+62.23 | $9,104.02 | ▲ +62.23 after sell → book $10,185.52; vs 09:30 mark -3.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $7,907.70 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1300.57 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 75 | $17.20 | $2.21 | — | $6,615.48 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1300.57 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 116 | $11.13 | $2.34 | — | $5,322.06 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1300.57 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $4,065.89 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1300.57 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 985 | $1.32 | $12.71 | — | $2,752.98 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1300.57 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GMAB` | 38 | $33.36 | $2.10 | — | $1,483.20 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.6; leftover $1300.57 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 783 | $1.66 | $10.10 | — | $173.32 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1300.57 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $173.32 | ▲ close $10,392.06 vs 09:30 $10,208.47 (session +240.08) | 16:00 close · cash $173.32 · equity $10,392.06 vs 09:30 $10,208.47 (+183.59; session marks +240.08) · 8 name(s) marked open→close (per-name table). AEM×5 09:30 $216.30 → close $216.06 -1.20; AU×10 09:30 $119.43 → close $121.22 +17.90; AUPH×75 09:30 $17.20 → close $16.65 -41.25; ARCT×116 09:30 $11.13 → close $13.45 +269.12; CRSP×21 09:30 $59.72 → close $59.50 -4.62; CYPH×985 09:30 $1.32 → close $1.42 +98.50; GMAB×38 09:30 $33.36 → close $33.45 +3.42; BTBT×783 09:30 $1.66 → close $1.53 -101.79 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $173.32 | ▲ 09:30 equity $10,749.71 vs yday $10,392.06 (+357.65) | 09:30 open · cash $173.32 (unchanged overnight, no fees) · equity $10,749.71 vs prior close $10,392.06 (+357.65) · 8 name(s) re-marked at the open (per-name table). AEM×5 yday $216.06 → 09:30 $217.03 +4.85; AU×10 yday $121.22 → 09:30 $120.51 -7.10; AUPH×75 yday $16.65 → 09:30 $16.57 -6.00; ARCT×116 yday $13.45 → 09:30 $13.33 -13.92; CRSP×21 yday $59.50 → 09:30 $58.75 -15.75; CYPH×985 yday $1.42 → 09:30 $1.83 +403.85; GMAB×38 yday $33.45 → 09:30 $32.82 -23.94; BTBT×783 yday $1.53 → 09:30 $1.55 +15.66 | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $+58.87 | $1,256.44 | ▲ +58.87 after sell → book $10,747.68; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $2,459.50 | ▲ +6.74 after sell → book $10,745.64; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 75 | $16.57 | $2.24 | $-51.70 | $3,700.02 | ▼ -51.70 after sell → book $10,743.41; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 116 | $13.33 | $2.37 | $+250.49 | $5,243.93 | ▲ +250.49 after sell → book $10,741.04; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 21 | $58.75 | $2.07 | $-24.50 | $6,475.60 | ▼ -24.50 after sell → book $10,738.96; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 985 | $1.83 | $12.88 | $+476.76 | $8,265.27 | ▲ +476.76 after sell → book $10,726.08; vs 09:30 mark -12.88 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GMAB` | 38 | $32.82 | $2.12 | $-24.75 | $9,510.31 | ▼ -24.75 after sell → book $10,723.96; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 783 | $1.55 | $10.24 | $-106.47 | $10,713.72 | ▼ -106.47 after sell → book $10,713.72; vs 09:30 mark -10.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,713.72 | ▲ close $10,713.72 vs 09:30 $10,749.71 (session +0.00) | 16:00 close · cash $10,713.72 · no lots left · equity $10,713.72. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,713.72 | ▲ 09:30 equity $10,713.72 vs yday $10,713.72 (-0.00) | 09:30 open · cash $10,713.72 · no holdings · equity $10,713.72 vs prior close $10,713.72 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $426.97 | $2.00 | — | $9,430.81 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+6.0; leftover $1339.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 98 | $13.59 | $2.28 | — | $8,096.70 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1339.21 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 141 | $9.49 | $2.41 | — | $6,756.20 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $1339.21 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 36 | $36.96 | $2.10 | — | $5,423.54 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1339.21 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 669 | $2.00 | $8.63 | — | $4,076.91 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ret5=+15.0; leftover $1339.21 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 255 | $5.24 | $3.29 | — | $2,737.42 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1339.21 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 152 | $8.79 | $2.45 | — | $1,398.90 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $1339.21 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FWDI` | 234 | $5.71 | $3.02 | — | $59.74 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ret5=+33.9; leftover $1339.21 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $59.74 | ▲ close $10,885.24 vs 09:30 $10,713.72 (session +197.70) | 16:00 close · cash $59.74 · equity $10,885.24 vs 09:30 $10,713.72 (+171.52; session marks +197.70) · 8 name(s) marked open→close (per-name table). HCA×3 09:30 $426.97 → close $428.76 +5.37; KURA×98 09:30 $13.59 → close $13.59 +0.00; CCOI×141 09:30 $9.49 → close $9.88 +54.99; LIFE×36 09:30 $36.96 → close $38.56 +57.60; NPWR×669 09:30 $2.00 → close $1.95 -33.45; ALVO×255 09:30 $5.24 → close $5.05 -48.45; SUJA×152 09:30 $8.79 → close $9.33 +82.08; FWDI×234 09:30 $5.71 → close $6.05 +79.56 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.74 | ▼ 09:30 equity $10,834.44 vs yday $10,885.24 (-50.80) | 09:30 open · cash $59.74 (unchanged overnight, no fees) · equity $10,834.44 vs prior close $10,885.24 (-50.80) · 8 name(s) re-marked at the open (per-name table). HCA×3 yday $428.76 → 09:30 $427.50 -3.78; KURA×98 yday $13.59 → 09:30 $13.63 +3.92; CCOI×141 yday $9.88 → 09:30 $9.89 +1.41; LIFE×36 yday $38.56 → 09:30 $38.24 -11.52; NPWR×669 yday $1.95 → 09:30 $1.93 -13.38; ALVO×255 yday $5.05 → 09:30 $4.98 -17.85; SUJA×152 yday $9.33 → 09:30 $9.39 +9.12; FWDI×234 yday $6.05 → 09:30 $5.97 -18.72 | — |
| 2026-08-26 09:30 ET | **SELL** | `HCA` | 3 | $427.50 | $2.02 | $-2.43 | $1,340.22 | ▼ -2.43 after sell → book $10,832.42; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `KURA` | 98 | $13.63 | $2.31 | $-0.67 | $2,673.65 | ▼ -0.67 after sell → book $10,830.11; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `CCOI` | 141 | $9.89 | $2.45 | $+51.54 | $4,065.69 | ▲ +51.54 after sell → book $10,827.66; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `LIFE` | 36 | $38.24 | $2.12 | $+41.86 | $5,440.21 | ▲ +41.86 after sell → book $10,825.54; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `NPWR` | 669 | $1.93 | $8.75 | $-64.21 | $6,722.63 | ▼ -64.21 after sell → book $10,816.79; vs 09:30 mark -8.75 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ALVO` | 255 | $4.98 | $3.34 | $-72.93 | $7,989.19 | ▼ -72.93 after sell → book $10,813.45; vs 09:30 mark -3.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FWDI` | 234 | $5.97 | $3.07 | $+54.75 | $9,383.10 | ▲ +54.75 after sell → book $10,810.38; vs 09:30 mark -3.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `CRMD` | 155 | $8.60 | $2.46 | — | $8,047.64 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ret5=+4.8; leftover $1340.44 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `AVBP` | 42 | $31.21 | $2.12 | — | $6,734.71 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.5; leftover $1340.44 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BZ` | 79 | $16.77 | $2.23 | — | $5,407.65 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ret5=+3.1; leftover $1340.44 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟢 |
| 2026-08-26 09:30 ET | **BUY** | `ACRS` | 205 | $6.53 | $2.64 | — | $4,066.36 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer; 🔵; ret5=+3.6; leftover $1340.44 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TMCI` | 280 | $4.78 | $3.61 | — | $2,724.34 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer; 🔵; ret5=+8.1; leftover $1340.44 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `BRR` | 609 | $2.20 | $7.86 | — | $1,376.69 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer; 🔵; ret5=+17.8; leftover $1340.44 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `GRRR` | 95 | $14.03 | $2.27 | — | $41.56 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_mover; ret5=-7.6; leftover $1340.44 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $41.56 | ▲ close $10,952.96 vs 09:30 $10,834.44 (session +165.77) | 16:00 close · cash $41.56 · equity $10,952.96 vs 09:30 $10,834.44 (+118.52; session marks +165.77) · 8 name(s) marked open→close (per-name table). SUJA×152 09:30 $9.39 → close $9.44 +7.60; CRMD×155 09:30 $8.60 → close $8.39 -32.55; AVBP×42 09:30 $31.21 → close $31.14 -2.94; BZ×79 09:30 $16.77 → close $18.84 +163.53; ACRS×205 09:30 $6.53 → close $6.19 -69.70; TMCI×280 09:30 $4.78 → close $4.72 -16.80; BRR×609 09:30 $2.20 → close $2.17 -18.27; GRRR×95 09:30 $14.03 → close $15.45 +134.90 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $41.56 | ▲ 09:30 equity $10,972.87 vs yday $10,952.96 (+19.91) | 09:30 open · cash $41.56 (unchanged overnight, no fees) · equity $10,972.87 vs prior close $10,952.96 (+19.91) · 8 name(s) re-marked at the open (per-name table). SUJA×152 yday $9.44 → 09:30 $9.41 -4.56; CRMD×155 yday $8.39 → 09:30 $8.49 +15.50; AVBP×42 yday $31.14 → 09:30 $30.79 -14.70; BZ×79 yday $18.84 → 09:30 $18.50 -26.86; ACRS×205 yday $6.19 → 09:30 $6.15 -8.20; TMCI×280 yday $4.72 → 09:30 $4.72 +0.00; BRR×609 yday $2.17 → 09:30 $2.19 +12.18; GRRR×95 yday $15.45 → 09:30 $15.94 +46.55 | — |
| 2026-08-27 09:30 ET | **SELL** | `SUJA` | 152 | $9.41 | $2.48 | $+89.31 | $1,469.40 | ▲ +89.31 after sell → book $10,970.39; vs 09:30 mark -2.48 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 155 | $8.49 | $2.49 | $-22.00 | $2,782.86 | ▼ -22.00 after sell → book $10,967.90; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AVBP` | 42 | $30.79 | $2.14 | $-21.89 | $4,073.90 | ▼ -21.89 after sell → book $10,965.76; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BZ` | 79 | $18.50 | $2.25 | $+132.19 | $5,533.15 | ▲ +132.19 after sell → book $10,963.51; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ACRS` | 205 | $6.15 | $2.69 | $-83.23 | $6,791.21 | ▼ -83.23 after sell → book $10,960.82; vs 09:30 mark -2.69 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TMCI` | 280 | $4.72 | $3.67 | $-24.08 | $8,109.14 | ▼ -24.08 after sell → book $10,957.15; vs 09:30 mark -3.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BRR` | 609 | $2.19 | $7.97 | $-21.91 | $9,434.89 | ▼ -21.91 after sell → book $10,949.19; vs 09:30 mark -7.96 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `GRRR` | 95 | $15.94 | $2.30 | $+176.87 | $10,946.88 | ▲ +176.87 after sell → book $10,946.88; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 33 | $41.44 | $2.09 | — | $9,577.27 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+3.1; leftover $1368.36 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 94 | $14.42 | $2.27 | — | $8,219.52 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+7.1; leftover $1368.36 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 526 | $2.60 | $6.79 | — | $6,845.14 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+13.0; leftover $1368.36 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $5,876.13 | — | union ∩ candle, no 🚨; gate candle_capture=True; list mover_buy; 🔵; ret5=+0.1; leftover $1368.36 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ANET` | 6 | $205.90 | $2.01 | — | $4,638.72 | — | union ∩ candle, no 🚨; gate candle_capture=True; list mover_buy; 🔵; ret5=+8.5; leftover $1368.36 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 89 | $15.33 | $2.26 | — | $3,272.10 | — | union ∩ candle, no 🚨; gate candle_capture=True; list mover_buy; 🔵; ret5=+7.4; leftover $1368.36 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 45 | $29.83 | $2.12 | — | $1,927.62 | — | union ∩ candle, no 🚨; gate candle_capture=True; list mover_buy; 🔵; ret5=+7.6; leftover $1368.36 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 5 | $253.44 | $2.00 | — | $658.42 | — | union ∩ candle, no 🚨; gate candle_capture=True; list mover_buy; 🔵; ret5=+3.3; leftover $1368.36 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $658.42 | ▼ close $10,864.60 vs 09:30 $10,972.87 (session -60.75) | 16:00 close · cash $658.42 · equity $10,864.60 vs 09:30 $10,972.87 (-108.27; session marks -60.75) · 8 name(s) marked open→close (per-name table). RRC×33 09:30 $41.44 → close $41.64 +6.60; CRK×94 09:30 $14.42 → close $14.62 +18.80; SLI×526 09:30 $2.60 → close $2.64 +21.04; MU×1 09:30 $967.01 → close $935.39 -31.62; ANET×6 09:30 $205.90 → close $201.09 -28.86; DLO×89 09:30 $15.33 → close $15.14 -16.91; GEN×45 09:30 $29.83 → close $30.50 +30.15; MRVL×5 09:30 $253.44 → close $241.45 -59.95 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $658.42 | ▼ 09:30 equity $10,790.74 vs yday $10,864.60 (-73.86) | 09:30 open · cash $658.42 (unchanged overnight, no fees) · equity $10,790.74 vs prior close $10,864.60 (-73.86) · 8 name(s) re-marked at the open (per-name table). RRC×33 yday $41.64 → 09:30 $41.74 +3.30; CRK×94 yday $14.62 → 09:30 $14.63 +0.94; SLI×526 yday $2.64 → 09:30 $2.68 +21.04; MU×1 yday $935.39 → 09:30 $919.29 -16.10; ANET×6 yday $201.09 → 09:30 $200.00 -6.54; DLO×89 yday $15.14 → 09:30 $15.19 +4.45; GEN×45 yday $30.50 → 09:30 $30.50 +0.00; MRVL×5 yday $241.45 → 09:30 $225.26 -80.95 | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $1,575.69 | ▼ -51.73 after sell → book $10,788.72; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ANET` | 6 | $200.00 | $2.03 | $-39.44 | $2,773.67 | ▼ -39.44 after sell → book $10,786.70; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DLO` | 89 | $15.19 | $2.28 | $-17.00 | $4,123.29 | ▼ -17.00 after sell → book $10,784.41; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 45 | $30.50 | $2.15 | $+25.88 | $5,493.65 | ▲ +25.88 after sell → book $10,782.27; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MRVL` | 5 | $225.26 | $2.02 | $-144.93 | $6,617.92 | ▼ -144.93 after sell → book $10,780.24; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 40 | $32.90 | $2.11 | — | $5,299.81 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1323.58 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 72 | $18.15 | $2.21 | — | $3,990.81 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=+14.1; leftover $1323.58 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 136 | $9.73 | $2.40 | — | $2,665.13 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=+47.1; leftover $1323.58 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 952 | $1.39 | $12.28 | — | $1,329.57 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=+20.4; leftover $1323.58 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VYX` | 144 | $9.13 | $2.42 | — | $12.43 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer; 🔵; ret5=+20.0; leftover $1323.58 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.43 | ▼ close $10,456.93 vs 09:30 $10,790.74 (session -301.90) | 16:00 close · cash $12.43 · equity $10,456.93 vs 09:30 $10,790.74 (-333.81; session marks -301.90) · 8 name(s) marked open→close (per-name table). RRC×33 09:30 $41.74 → close $41.46 -9.24; CRK×94 09:30 $14.63 → close $14.29 -31.96; SLI×526 09:30 $2.68 → close $2.55 -68.38; SEDG×40 09:30 $32.90 → close $31.41 -59.60; BZ×72 09:30 $18.15 → close $17.80 -25.20; CAPR×136 09:30 $9.73 → close $9.59 -19.04; LVWR×952 09:30 $1.39 → close $1.35 -38.08; VYX×144 09:30 $9.13 → close $8.78 -50.40 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.43 | ▼ 09:30 equity $10,419.31 vs yday $10,456.93 (-37.62) | 09:30 open · cash $12.43 (unchanged overnight, no fees) · equity $10,419.31 vs prior close $10,456.93 (-37.62) · 8 name(s) re-marked at the open (per-name table). RRC×33 yday $41.46 → 09:30 $42.00 +17.82; CRK×94 yday $14.29 → 09:30 $14.54 +23.50; SLI×526 yday $2.55 → 09:30 $2.58 +15.78; SEDG×40 yday $31.41 → 09:30 $31.15 -10.40; BZ×72 yday $17.80 → 09:30 $17.70 -7.20; CAPR×136 yday $9.59 → 09:30 $9.50 -12.24; LVWR×952 yday $1.35 → 09:30 $1.30 -47.60; VYX×144 yday $8.78 → 09:30 $8.66 -17.28 | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 33 | $42.00 | $2.11 | $+14.28 | $1,396.32 | ▲ +14.28 after sell → book $10,417.20; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 94 | $14.54 | $2.30 | $+6.71 | $2,760.78 | ▲ +6.71 after sell → book $10,414.90; vs 09:30 mark -2.30 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 526 | $2.58 | $6.88 | $-24.19 | $4,110.97 | ▼ -24.19 after sell → book $10,408.01; vs 09:30 mark -6.89 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 40 | $31.15 | $2.13 | $-74.24 | $5,354.84 | ▼ -74.24 after sell → book $10,405.88; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 72 | $17.70 | $2.23 | $-36.83 | $6,627.02 | ▼ -36.83 after sell → book $10,403.66; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 136 | $9.50 | $2.43 | $-36.11 | $7,916.59 | ▼ -36.11 after sell → book $10,401.23; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LVWR` | 952 | $1.30 | $12.45 | $-110.41 | $9,141.74 | ▼ -110.41 after sell → book $10,388.78; vs 09:30 mark -12.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `VYX` | 144 | $8.66 | $2.46 | $-72.56 | $10,386.32 | ▼ -72.56 after sell → book $10,386.32; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,386.32 | ▲ close $10,386.32 vs 09:30 $10,419.31 (session +0.00) | 16:00 close · cash $10,386.32 · no lots left · equity $10,386.32. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,386.32 | ▲ 09:30 equity $10,386.32 vs yday $10,386.32 (+0.00) | 09:30 open · cash $10,386.32 · no holdings · equity $10,386.32 vs prior close $10,386.32 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,386.32 | ▲ close $10,386.32 vs 09:30 $10,386.32 (session +0.00) | 16:00 close · cash $10,386.32 · no lots left · equity $10,386.32. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,386.32 | ▲ 09:30 equity $10,386.32 vs yday $10,386.32 (+0.00) | 09:30 open · cash $10,386.32 · no holdings · equity $10,386.32 vs prior close $10,386.32 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,386.32 | ▲ close $10,386.32 vs 09:30 $10,386.32 (session +0.00) | 16:00 close · cash $10,386.32 · no lots left · equity $10,386.32. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,386.32 | ▲ 09:30 equity $10,386.32 vs yday $10,386.32 (+0.00) | 09:30 open · cash $10,386.32 · no holdings · equity $10,386.32 vs prior close $10,386.32 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 24 | $52.88 | $2.06 | — | $9,115.14 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1298.29 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 30 | $42.93 | $2.08 | — | $7,825.16 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1298.29 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 357 | $3.63 | $4.61 | — | $6,524.64 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1298.29 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 161 | $8.03 | $2.47 | — | $5,229.34 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1298.29 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $4,035.27 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1298.29 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $145.94 | $2.01 | — | $2,865.70 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1298.29 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 77 | $16.77 | $2.22 | — | $1,572.19 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1298.29 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 87 | $14.85 | $2.25 | — | $277.99 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1298.29 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $277.99 | ▼ close $10,171.45 vs 09:30 $10,386.32 (session -195.15) | 16:00 close · cash $277.99 · equity $10,171.45 vs 09:30 $10,386.32 (-214.87; session marks -195.15) · 8 name(s) marked open→close (per-name table). ATRC×24 09:30 $52.88 → close $52.46 -10.08; HRMY×30 09:30 $42.93 → close $41.86 -32.10; CABA×357 09:30 $3.63 → close $3.48 -53.55; VSTM×161 09:30 $8.03 → close $7.98 -8.05; RVTY×9 09:30 $132.45 → close $130.63 -16.38; MRNA×8 09:30 $145.94 → close $148.87 +23.40; ARCT×77 09:30 $16.77 → close $15.56 -93.17; SLN×87 09:30 $14.85 → close $14.79 -5.22 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $277.99 | ▼ 09:30 equity $10,154.45 vs yday $10,171.45 (-17.00) | 09:30 open · cash $277.99 (unchanged overnight, no fees) · equity $10,154.45 vs prior close $10,171.45 (-17.00) · 8 name(s) re-marked at the open (per-name table). ATRC×24 yday $52.46 → 09:30 $52.03 -10.32; HRMY×30 yday $41.86 → 09:30 $41.50 -10.80; CABA×357 yday $3.48 → 09:30 $3.46 -7.14; VSTM×161 yday $7.98 → 09:30 $7.91 -11.27; RVTY×9 yday $130.63 → 09:30 $130.03 -5.40; MRNA×8 yday $148.87 → 09:30 $153.62 +38.00; ARCT×77 yday $15.56 → 09:30 $15.61 +3.85; SLN×87 yday $14.79 → 09:30 $14.63 -13.92 | — |
| 2026-09-04 09:30 ET | **SELL** | `ATRC` | 24 | $52.03 | $2.08 | $-24.54 | $1,524.63 | ▼ -24.54 after sell → book $10,152.37; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `CABA` | 357 | $3.46 | $4.67 | $-69.97 | $2,755.17 | ▼ -69.97 after sell → book $10,147.69; vs 09:30 mark -4.68 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 161 | $7.91 | $2.51 | $-24.30 | $4,026.17 | ▼ -24.30 after sell → book $10,145.18; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $130.03 | $2.04 | $-25.83 | $5,194.40 | ▼ -25.83 after sell → book $10,143.14; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 8 | $153.62 | $2.03 | $+57.35 | $6,421.33 | ▲ +57.35 after sell → book $10,141.11; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 77 | $15.61 | $2.24 | $-93.78 | $7,621.06 | ▼ -93.78 after sell → book $10,138.87; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SLN` | 87 | $14.63 | $2.28 | $-23.67 | $8,891.59 | ▼ -23.67 after sell → book $10,136.59; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 668 | $1.90 | $8.62 | — | $7,613.77 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1270.23 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 4 | $263.36 | $2.00 | — | $6,558.33 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1270.23 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `CCOI` | 126 | $10.02 | $2.37 | — | $5,293.44 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.2; leftover $1270.23 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `UAMY` | 241 | $5.25 | $3.11 | — | $4,025.08 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; ret5=-0.4; leftover $1270.23 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 403 | $3.15 | $5.20 | — | $2,750.44 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ret5=+19.3; leftover $1270.23 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 280 | $4.53 | $3.61 | — | $1,478.42 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.5; leftover $1270.23 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `FMC` | 98 | $12.95 | $2.28 | — | $207.04 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=+21.8; leftover $1270.23 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $207.04 | ▲ close $10,126.30 vs 09:30 $10,154.45 (session +16.90) | 16:00 close · cash $207.04 · equity $10,126.30 vs 09:30 $10,154.45 (-28.15; session marks +16.90) · 8 name(s) marked open→close (per-name table). HRMY×30 09:30 $41.50 → close $42.25 +22.50; BMEA×668 09:30 $1.90 → close $2.03 +86.84; CRM×4 09:30 $263.36 → close $259.23 -16.52; CCOI×126 09:30 $10.02 → close $10.05 +3.78; UAMY×241 09:30 $5.25 → close $5.20 -12.05; SLBT×403 09:30 $3.15 → close $2.88 -108.81; IRD×280 09:30 $4.53 → close $4.67 +39.20; FMC×98 09:30 $12.95 → close $12.97 +1.96 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $207.04 | ▼ 09:30 equity $10,067.70 vs yday $10,126.30 (-58.60) | 09:30 open · cash $207.04 (unchanged overnight, no fees) · equity $10,067.70 vs prior close $10,126.30 (-58.60) · 8 name(s) re-marked at the open (per-name table). HRMY×30 yday $42.25 → 09:30 $42.20 -1.50; BMEA×668 yday $2.03 → 09:30 $2.00 -20.04; CRM×4 yday $259.23 → 09:30 $253.72 -22.04; CCOI×126 yday $10.05 → 09:30 $9.98 -8.82; UAMY×241 yday $5.20 → 09:30 $5.28 +19.28; SLBT×403 yday $2.88 → 09:30 $2.88 +0.00; IRD×280 yday $4.67 → 09:30 $4.53 -39.20; FMC×98 yday $12.97 → 09:30 $13.11 +13.72 | — |
| 2026-09-08 09:30 ET | **SELL** | `HRMY` | 30 | $42.20 | $2.10 | $-26.08 | $1,470.94 | ▼ -26.08 after sell → book $10,065.60; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 668 | $2.00 | $8.74 | $+49.44 | $2,798.20 | ▲ +49.44 after sell → book $10,056.86; vs 09:30 mark -8.74 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 4 | $253.72 | $2.02 | $-42.58 | $3,811.06 | ▼ -42.58 after sell → book $10,054.84; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CCOI` | 126 | $9.98 | $2.40 | $-9.81 | $5,066.14 | ▼ -9.81 after sell → book $10,052.44; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `UAMY` | 241 | $5.28 | $3.16 | $+0.96 | $6,335.46 | ▲ +0.96 after sell → book $10,049.28; vs 09:30 mark -3.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `SLBT` | 403 | $2.88 | $5.28 | $-119.28 | $7,490.82 | ▼ -119.28 after sell → book $10,044.00; vs 09:30 mark -5.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `IRD` | 280 | $4.53 | $3.67 | $-7.28 | $8,755.56 | ▼ -7.28 after sell → book $10,040.34; vs 09:30 mark -3.66 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `FMC` | 98 | $13.11 | $2.31 | $+11.09 | $10,038.03 | ▲ +11.09 after sell → book $10,038.03; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,038.03 | ▲ close $10,038.03 vs 09:30 $10,067.70 (session +0.00) | 16:00 close · cash $10,038.03 · no lots left · equity $10,038.03. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FROG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DFDV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RPD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TENB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRGP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SUJA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `IRD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BMNR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MSTR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BTBT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `USDE` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CRCL` | hard_red | hard-red S=-11.47 sit; no new buys |
