# Factor mine action — `union_flow_in_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ flow_in, no 🚨

Cash book **-2.58%** ($9,742) · signal-only (no cash/fees) was +15.53%. Starts YES **3/30**. Fills 77 · skips 105 · realized $-281.48.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: money came in (prior rel vol ≥ 1.5) but price barely moved (|1-day| ≤ 1.2%).
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
- **Gate** `flow_in=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,431.09.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `TPG` | 197 | — | $50.62 | +0.00 | $54.62 | +787.37 | +787.37 | +0.00 | +787.37 |
| 2026-08-14 | `TPG` | 197 | $54.62 | $55.29 | +131.99 | $53.03 | -445.22 | -313.23 | +919.36 | +474.14 |
| 2026-08-14 | `KULR` | 2 | — | $2.50 | +0.00 | $2.64 | +0.28 | +0.28 | +0.00 | +0.28 |
| 2026-08-14 | `NPWR` | 3 | — | $1.56 | +0.00 | $1.95 | +1.17 | +1.17 | +0.00 | +1.17 |
| 2026-08-14 | `RLX` | 3 | — | $1.85 | +0.00 | $1.94 | +0.27 | +0.27 | +0.00 | +0.27 |
| 2026-08-17 | `TPG` | 197 | $53.03 | $52.67 | -70.92 | $51.77 | -177.30 | -248.22 | +403.22 | +225.92 |
| 2026-08-17 | `KULR` | 2 | $2.64 | $2.63 | -0.02 | $2.62 | -0.02 | -0.04 | +0.26 | +0.24 |
| 2026-08-17 | `NPWR` | 3 | $1.95 | $1.92 | -0.09 | $1.73 | -0.57 | -0.66 | +1.08 | +0.51 |
| 2026-08-17 | `RLX` | 3 | $1.94 | $1.92 | -0.06 | $1.87 | -0.15 | -0.21 | +0.21 | +0.06 |
| 2026-08-18 | `TPG` | 197 | $51.77 | $51.77 | +0.00 | — | +0.00 | +0.00 | +225.92 | — |
| 2026-08-18 | `KULR` | 2 | $2.62 | $2.53 | -0.18 | $2.57 | +0.08 | -0.10 | +0.06 | +0.14 |
| 2026-08-18 | `NPWR` | 3 | $1.73 | $1.70 | -0.09 | $1.65 | -0.15 | -0.24 | +0.42 | +0.27 |
| 2026-08-18 | `RLX` | 3 | $1.87 | $1.86 | -0.03 | $1.83 | -0.09 | -0.12 | +0.03 | -0.06 |
| 2026-08-19 | `KULR` | 2 | $2.57 | $2.55 | -0.04 | — | +0.00 | -0.04 | +0.10 | — |
| 2026-08-19 | `NPWR` | 3 | $1.65 | $1.70 | +0.15 | — | +0.00 | +0.15 | +0.42 | — |
| 2026-08-19 | `RLX` | 3 | $1.83 | $1.84 | +0.03 | — | +0.00 | +0.03 | -0.03 | — |
| 2026-08-20 | `FUTU` | 28 | — | $117.65 | +0.00 | $112.73 | -137.76 | -137.76 | +0.00 | -137.76 |
| 2026-08-20 | `WMT` | 32 | — | $106.38 | +0.00 | $103.84 | -81.28 | -81.28 | +0.00 | -81.28 |
| 2026-08-20 | `BJ` | 38 | — | $88.91 | +0.00 | $91.30 | +90.82 | +90.82 | +0.00 | +90.82 |
| 2026-08-21 | `FUTU` | 28 | $112.73 | $115.18 | +68.60 | $123.64 | +236.88 | +305.48 | -69.16 | +167.72 |
| 2026-08-21 | `WMT` | 32 | $103.84 | $103.69 | -4.80 | $103.70 | +0.32 | -4.48 | -86.08 | -85.76 |
| 2026-08-21 | `BJ` | 38 | $91.30 | $93.98 | +101.84 | $96.42 | +92.72 | +194.56 | +192.66 | +285.38 |
| 2026-08-21 | `HITI` | 55 | — | $2.43 | +0.00 | $2.45 | +1.10 | +1.10 | +0.00 | +1.10 |
| 2026-08-24 | `FUTU` | 28 | $123.64 | $121.00 | -73.92 | $115.81 | -145.32 | -219.24 | +93.80 | -51.52 |
| 2026-08-24 | `WMT` | 32 | $103.70 | $104.14 | +14.08 | $106.49 | +75.20 | +89.28 | -71.68 | +3.52 |
| 2026-08-24 | `BJ` | 38 | $96.42 | $97.02 | +22.80 | $98.49 | +55.86 | +78.66 | +308.18 | +364.04 |
| 2026-08-24 | `HITI` | 55 | $2.45 | $2.45 | +0.00 | $2.46 | +0.55 | +0.55 | +1.10 | +1.65 |
| 2026-08-25 | `FUTU` | 28 | $115.81 | $118.00 | +61.32 | — | +0.00 | +61.32 | +9.80 | — |
| 2026-08-25 | `WMT` | 32 | $106.49 | $105.58 | -29.12 | — | +0.00 | -29.12 | -25.60 | — |
| 2026-08-25 | `BJ` | 38 | $98.49 | $97.63 | -32.68 | — | +0.00 | -32.68 | +331.36 | — |
| 2026-08-25 | `HITI` | 55 | $2.46 | $2.48 | +1.10 | $2.57 | +4.95 | +6.05 | +2.75 | +7.70 |
| 2026-08-25 | `ZYME` | 119 | — | $28.86 | +0.00 | $27.47 | -165.41 | -165.41 | +0.00 | -165.41 |
| 2026-08-25 | `RHI` | 79 | — | $43.76 | +0.00 | $44.90 | +90.06 | +90.06 | +0.00 | +90.06 |
| 2026-08-25 | `ABUS` | 659 | — | $5.25 | +0.00 | $5.20 | -32.95 | -32.95 | +0.00 | -32.95 |
| 2026-08-26 | `HITI` | 55 | $2.57 | $2.57 | +0.00 | — | +0.00 | +0.00 | +7.70 | — |
| 2026-08-26 | `ZYME` | 119 | $27.47 | $27.56 | +10.71 | $29.30 | +207.66 | +218.37 | -154.70 | +52.96 |
| 2026-08-26 | `RHI` | 79 | $44.90 | $44.33 | -45.03 | $44.54 | +16.59 | -28.44 | +45.03 | +61.62 |
| 2026-08-26 | `ABUS` | 659 | $5.20 | $5.19 | -6.59 | $5.19 | +0.00 | -6.59 | -39.54 | -39.54 |
| 2026-08-26 | `NCNO` | 2 | — | $19.33 | +0.00 | $21.51 | +4.36 | +4.36 | +0.00 | +4.36 |
| 2026-08-26 | `PLAB` | 1 | — | $37.26 | +0.00 | $30.25 | -7.01 | -7.01 | +0.00 | -7.01 |
| 2026-08-27 | `ZYME` | 119 | $29.30 | $29.33 | +2.97 | $29.01 | -38.08 | -35.11 | +55.93 | +17.85 |
| 2026-08-27 | `RHI` | 79 | $44.54 | $44.41 | -10.27 | $44.97 | +44.24 | +33.97 | +51.35 | +95.59 |
| 2026-08-27 | `ABUS` | 659 | $5.19 | $5.16 | -19.77 | $5.17 | +6.59 | -13.18 | -59.31 | -52.72 |
| 2026-08-27 | `NCNO` | 2 | $21.51 | $22.03 | +1.04 | $23.32 | +2.58 | +3.62 | +5.40 | +7.98 |
| 2026-08-27 | `PLAB` | 1 | $30.25 | $30.12 | -0.13 | $30.26 | +0.14 | +0.01 | -7.14 | -7.00 |
| 2026-08-27 | `TRLV` | 3 | — | $11.38 | +0.00 | $11.03 | -1.05 | -1.05 | +0.00 | -1.05 |
| 2026-08-27 | `BOX` | 1 | — | $33.79 | +0.00 | $34.74 | +0.95 | +0.95 | +0.00 | +0.95 |
| 2026-08-28 | `ZYME` | 119 | $29.01 | $28.91 | -11.90 | — | +0.00 | -11.90 | +5.95 | — |
| 2026-08-28 | `RHI` | 79 | $44.97 | $44.51 | -36.34 | — | +0.00 | -36.34 | +59.25 | — |
| 2026-08-28 | `ABUS` | 659 | $5.17 | $5.15 | -13.18 | — | +0.00 | -13.18 | -65.90 | — |
| 2026-08-28 | `NCNO` | 2 | $23.32 | $23.30 | -0.04 | $22.99 | -0.62 | -0.66 | +7.94 | +7.32 |
| 2026-08-28 | `PLAB` | 1 | $30.26 | $30.01 | -0.25 | $27.73 | -2.28 | -2.53 | -7.25 | -9.53 |
| 2026-08-28 | `TRLV` | 3 | $11.03 | $11.00 | -0.09 | $11.82 | +2.46 | +2.37 | -1.14 | +1.32 |
| 2026-08-28 | `BOX` | 1 | $34.74 | $34.75 | +0.01 | $34.98 | +0.23 | +0.24 | +0.96 | +1.19 |
| 2026-08-28 | `JKS` | 258 | — | $13.37 | +0.00 | $13.54 | +43.86 | +43.86 | +0.00 | +43.86 |
| 2026-08-28 | `DY` | 11 | — | $306.34 | +0.00 | $294.34 | -132.00 | -132.00 | +0.00 | -132.00 |
| 2026-08-28 | `ULTA` | 6 | — | $542.00 | +0.00 | $517.50 | -147.00 | -147.00 | +0.00 | -147.00 |
| 2026-08-31 | `NCNO` | 2 | $22.99 | $22.66 | -0.66 | — | +0.00 | -0.66 | +6.66 | — |
| 2026-08-31 | `PLAB` | 1 | $27.73 | $28.04 | +0.31 | — | +0.00 | +0.31 | -9.22 | — |
| 2026-08-31 | `TRLV` | 3 | $11.82 | $11.80 | -0.06 | $11.51 | -0.87 | -0.93 | +1.26 | +0.39 |
| 2026-08-31 | `BOX` | 1 | $34.98 | $34.72 | -0.26 | $35.78 | +1.06 | +0.80 | +0.93 | +1.99 |
| 2026-08-31 | `JKS` | 258 | $13.54 | $13.54 | +0.00 | $12.58 | -247.68 | -247.68 | +43.86 | -203.82 |
| 2026-08-31 | `DY` | 11 | $294.34 | $298.01 | +40.37 | $291.21 | -74.80 | -34.43 | -91.63 | -166.43 |
| 2026-08-31 | `ULTA` | 6 | $517.50 | $521.10 | +21.60 | $537.10 | +96.00 | +117.60 | -125.40 | -29.40 |
| 2026-09-01 | `TRLV` | 3 | $11.51 | $11.54 | +0.09 | — | +0.00 | +0.09 | +0.48 | — |
| 2026-09-01 | `BOX` | 1 | $35.78 | $35.69 | -0.09 | — | +0.00 | -0.09 | +1.90 | — |
| 2026-09-01 | `JKS` | 258 | $12.58 | $12.50 | -20.64 | $12.45 | -12.90 | -33.54 | -224.46 | -237.36 |
| 2026-09-01 | `DY` | 11 | $291.21 | $289.16 | -22.55 | $287.30 | -20.46 | -43.01 | -188.98 | -209.44 |
| 2026-09-01 | `ULTA` | 6 | $537.10 | $527.84 | -55.56 | $545.66 | +106.92 | +51.36 | -84.96 | +21.96 |
| 2026-09-02 | `JKS` | 258 | $12.45 | $12.45 | +0.00 | — | +0.00 | +0.00 | -237.36 | — |
| 2026-09-02 | `DY` | 11 | $287.30 | $287.99 | +7.59 | — | +0.00 | +7.59 | -201.85 | — |
| 2026-09-02 | `ULTA` | 6 | $545.66 | $545.68 | +0.12 | — | +0.00 | +0.12 | +22.08 | — |
| 2026-09-03 | `VIR` | 145 | — | $11.54 | +0.00 | $11.45 | -13.05 | -13.05 | +0.00 | -13.05 |
| 2026-09-03 | `AVGO` | 4 | — | $351.74 | +0.00 | $357.16 | +21.68 | +21.68 | +0.00 | +21.68 |
| 2026-09-03 | `FIVE` | 6 | — | $257.00 | +0.00 | $239.96 | -102.24 | -102.24 | +0.00 | -102.24 |
| 2026-09-03 | `MOMO` | 305 | — | $5.50 | +0.00 | $5.10 | -122.00 | -122.00 | +0.00 | -122.00 |
| 2026-09-03 | `PVH` | 22 | — | $74.96 | +0.00 | $72.46 | -55.00 | -55.00 | +0.00 | -55.00 |
| 2026-09-03 | `VSXY` | 21 | — | $76.86 | +0.00 | $73.64 | -67.62 | -67.62 | +0.00 | -67.62 |
| 2026-09-04 | `VIR` | 145 | $11.45 | $11.31 | -20.30 | $11.38 | +10.87 | -9.43 | -33.35 | -22.47 |
| 2026-09-04 | `AVGO` | 4 | $357.16 | $359.70 | +10.16 | $357.90 | -7.20 | +2.96 | +31.84 | +24.64 |
| 2026-09-04 | `FIVE` | 6 | $239.96 | $238.88 | -6.48 | $252.20 | +79.92 | +73.44 | -108.72 | -28.80 |
| 2026-09-04 | `MOMO` | 305 | $5.10 | $5.13 | +9.15 | $5.37 | +73.20 | +82.35 | -112.85 | -39.65 |
| 2026-09-04 | `PVH` | 22 | $72.46 | $72.79 | +7.26 | $74.33 | +33.88 | +41.14 | -47.74 | -13.86 |
| 2026-09-04 | `VSXY` | 21 | $73.64 | $73.63 | -0.21 | $75.56 | +40.53 | +40.32 | -67.83 | -27.30 |
| 2026-09-04 | `ATRC` | 1 | — | $52.03 | +0.00 | $51.52 | -0.51 | -0.51 | +0.00 | -0.51 |
| 2026-09-04 | `WNC` | 4 | — | $14.17 | +0.00 | $14.31 | +0.56 | +0.56 | +0.00 | +0.56 |
| 2026-09-04 | `ADCT` | 53 | — | $1.30 | +0.00 | $1.36 | +3.18 | +3.18 | +0.00 | +3.18 |
| 2026-09-04 | `HAFN` | 7 | — | $8.94 | +0.00 | $9.22 | +1.96 | +1.96 | +0.00 | +1.96 |
| 2026-09-04 | `DOCU` | 1 | — | $68.52 | +0.00 | $68.41 | -0.11 | -0.11 | +0.00 | -0.11 |
| 2026-09-04 | `XP` | 3 | — | $19.67 | +0.00 | $19.86 | +0.57 | +0.57 | +0.00 | +0.57 |
| 2026-09-08 | `VIR` | 145 | $11.38 | $11.22 | -23.92 | $11.18 | -5.80 | -29.72 | -46.40 | -52.20 |
| 2026-09-08 | `AVGO` | 4 | $357.90 | $363.68 | +23.12 | $368.56 | +19.52 | +42.64 | +47.76 | +67.28 |
| 2026-09-08 | `FIVE` | 6 | $252.20 | $251.22 | -5.88 | $254.07 | +17.10 | +11.22 | -34.68 | -17.58 |
| 2026-09-08 | `MOMO` | 305 | $5.37 | $5.28 | -27.45 | $5.26 | -6.10 | -33.55 | -67.10 | -73.20 |
| 2026-09-08 | `PVH` | 22 | $74.33 | $74.50 | +3.74 | $71.26 | -71.28 | -67.54 | -10.12 | -81.40 |
| 2026-09-08 | `VSXY` | 21 | $75.56 | $73.51 | -43.05 | $78.47 | +104.16 | +61.11 | -70.35 | +33.81 |
| 2026-09-08 | `ATRC` | 1 | $51.52 | $54.31 | +2.79 | $53.73 | -0.58 | +2.21 | +2.28 | +1.70 |
| 2026-09-08 | `WNC` | 4 | $14.31 | $14.22 | -0.36 | $13.61 | -2.44 | -2.80 | +0.20 | -2.24 |
| 2026-09-08 | `ADCT` | 53 | $1.36 | $1.33 | -1.59 | $1.30 | -1.59 | -3.18 | +1.59 | +0.00 |
| 2026-09-08 | `HAFN` | 7 | $9.22 | $8.81 | -2.87 | $8.96 | +1.05 | -1.82 | -0.91 | +0.14 |
| 2026-09-08 | `DOCU` | 1 | $68.41 | $67.05 | -1.36 | $65.08 | -1.97 | -3.33 | -1.47 | -3.44 |
| 2026-09-08 | `XP` | 3 | $19.86 | $20.46 | +1.80 | $19.99 | -1.41 | +0.39 | +2.37 | +0.96 |
| 2026-09-09 | `VIR` | 145 | $11.18 | $11.04 | -20.30 | — | +0.00 | -20.30 | -72.50 | — |
| 2026-09-09 | `AVGO` | 4 | $368.56 | $366.23 | -9.32 | — | +0.00 | -9.32 | +57.96 | — |
| 2026-09-09 | `FIVE` | 6 | $254.07 | $252.92 | -6.90 | — | +0.00 | -6.90 | -24.48 | — |
| 2026-09-09 | `MOMO` | 305 | $5.26 | $5.26 | +0.00 | — | +0.00 | +0.00 | -73.20 | — |
| 2026-09-09 | `PVH` | 22 | $71.26 | $70.83 | -9.46 | — | +0.00 | -9.46 | -90.86 | — |
| 2026-09-09 | `VSXY` | 21 | $78.47 | $77.16 | -27.51 | — | +0.00 | -27.51 | +6.30 | — |
| 2026-09-09 | `ATRC` | 1 | $53.73 | $53.16 | -0.57 | $53.03 | -0.13 | -0.70 | +1.13 | +1.00 |
| 2026-09-09 | `WNC` | 4 | $13.61 | $13.46 | -0.60 | $13.00 | -1.84 | -2.44 | -2.84 | -4.68 |
| 2026-09-09 | `ADCT` | 53 | $1.30 | $1.28 | -1.06 | $1.22 | -3.18 | -4.24 | -1.06 | -4.24 |
| 2026-09-09 | `HAFN` | 7 | $8.96 | $9.00 | +0.28 | $9.16 | +1.12 | +1.40 | +0.42 | +1.54 |
| 2026-09-09 | `DOCU` | 1 | $65.08 | $64.64 | -0.44 | $64.45 | -0.19 | -0.63 | -3.88 | -4.07 |
| 2026-09-09 | `XP` | 3 | $19.99 | $19.81 | -0.54 | $19.04 | -2.31 | -2.85 | +0.42 | -1.89 |
| 2026-09-10 | `ATRC` | 1 | $53.03 | $52.31 | -0.72 | — | +0.00 | -0.72 | +0.28 | — |
| 2026-09-10 | `WNC` | 4 | $13.00 | $12.79 | -0.84 | — | +0.00 | -0.84 | -5.52 | — |
| 2026-09-10 | `ADCT` | 53 | $1.22 | $1.21 | -0.53 | — | +0.00 | -0.53 | -4.77 | — |
| 2026-09-10 | `HAFN` | 7 | $9.16 | $9.08 | -0.56 | — | +0.00 | -0.56 | +0.98 | — |
| 2026-09-10 | `DOCU` | 1 | $64.45 | $64.60 | +0.15 | — | +0.00 | +0.15 | -3.92 | — |
| 2026-09-10 | `XP` | 3 | $19.04 | $18.86 | -0.54 | — | +0.00 | -0.54 | -2.43 | — |
| 2026-09-11 | `SSL` | 683 | — | $14.35 | +0.00 | $14.59 | +163.92 | +163.92 | +0.00 | +163.92 |
| 2026-09-14 | `SSL` | 683 | $14.59 | $14.69 | +68.30 | $14.50 | -129.77 | -61.47 | +232.22 | +102.45 |
| 2026-09-15 | `SSL` | 683 | $14.50 | $14.49 | -6.83 | $15.11 | +423.46 | +416.63 | +95.62 | +519.08 |
| 2026-09-16 | `SSL` | 683 | $15.11 | $14.62 | -334.67 | — | +0.00 | -334.67 | +184.41 | — |
| 2026-09-16 | `ATRC` | 59 | — | $55.66 | +0.00 | $57.14 | +87.32 | +87.32 | +0.00 | +87.32 |
| 2026-09-16 | `TCOM` | 81 | — | $40.93 | +0.00 | $40.43 | -40.50 | -40.50 | +0.00 | -40.50 |
| 2026-09-16 | `LEN` | 41 | — | $80.63 | +0.00 | $78.36 | -93.07 | -93.07 | +0.00 | -93.07 |
| 2026-09-17 | `ATRC` | 59 | $57.14 | $57.96 | +48.38 | $59.12 | +68.44 | +116.82 | +135.70 | +204.14 |
| 2026-09-17 | `TCOM` | 81 | $40.43 | $40.79 | +29.16 | $40.36 | -34.83 | -5.67 | -11.34 | -46.17 |
| 2026-09-17 | `LEN` | 41 | $78.36 | $81.00 | +108.24 | $79.70 | -53.30 | +54.94 | +15.17 | -38.13 |
| 2026-09-18 | `ATRC` | 59 | $59.12 | $58.51 | -35.99 | $58.10 | -24.19 | -60.18 | +168.15 | +143.96 |
| 2026-09-18 | `TCOM` | 81 | $40.36 | $40.61 | +20.25 | $40.68 | +5.67 | +25.92 | -25.92 | -20.25 |
| 2026-09-18 | `LEN` | 41 | $79.70 | $78.25 | -59.45 | $76.43 | -74.62 | -134.07 | -97.58 | -172.20 |
| 2026-09-21 | `ATRC` | 59 | $58.10 | $58.23 | +7.67 | — | +0.00 | +7.67 | +151.63 | — |
| 2026-09-21 | `TCOM` | 81 | $40.68 | $41.00 | +25.92 | — | +0.00 | +25.92 | +5.67 | — |
| 2026-09-21 | `LEN` | 41 | $76.43 | $76.98 | +22.55 | — | +0.00 | +22.55 | -149.65 | — |
| 2026-09-21 | `A` | 15 | — | $157.87 | +0.00 | $161.94 | +61.05 | +61.05 | +0.00 | +61.05 |
| 2026-09-21 | `HUM` | 6 | — | $386.20 | +0.00 | $378.58 | -45.72 | -45.72 | +0.00 | -45.72 |
| 2026-09-21 | `UMC` | 100 | — | $24.93 | +0.00 | $25.43 | +50.00 | +50.00 | +0.00 | +50.00 |
| 2026-09-21 | `NEO` | 125 | — | $19.92 | +0.00 | $19.41 | -63.75 | -63.75 | +0.00 | -63.75 |
| 2026-09-22 | `A` | 15 | $161.94 | $161.94 | +0.00 | $161.94 | +0.00 | +0.00 | +61.05 | +61.05 |
| 2026-09-22 | `HUM` | 6 | $378.58 | $378.58 | +0.00 | $378.58 | +0.00 | +0.00 | -45.72 | -45.72 |
| 2026-09-22 | `UMC` | 100 | $25.43 | $25.26 | -17.00 | $25.77 | +51.00 | +34.00 | +33.00 | +84.00 |
| 2026-09-22 | `NEO` | 125 | $19.41 | $19.41 | +0.00 | $19.41 | +0.00 | +0.00 | -63.75 | -63.75 |
| 2026-09-23 | `A` | 15 | $161.94 | $166.54 | +69.00 | $165.32 | -18.30 | +50.70 | +130.05 | +111.75 |
| 2026-09-23 | `HUM` | 6 | $378.58 | $370.00 | -51.48 | $374.78 | +28.68 | -22.80 | -97.20 | -68.52 |
| 2026-09-23 | `UMC` | 100 | $25.77 | $25.28 | -49.00 | $24.68 | -60.00 | -109.00 | +35.00 | -25.00 |
| 2026-09-23 | `NEO` | 125 | $19.41 | $18.69 | -90.00 | $18.36 | -41.25 | -131.25 | -153.75 | -195.00 |
| 2026-09-23 | `CBRL` | 6 | — | $47.57 | +0.00 | $47.52 | -0.30 | -0.30 | +0.00 | -0.30 |
| 2026-09-24 | `A` | 15 | $165.32 | $163.95 | -20.55 | — | +0.00 | -20.55 | +91.20 | — |
| 2026-09-24 | `HUM` | 6 | $374.78 | $374.54 | -1.44 | — | +0.00 | -1.44 | -69.96 | — |
| 2026-09-24 | `UMC` | 100 | $24.68 | $24.11 | -57.00 | — | +0.00 | -57.00 | -82.00 | — |
| 2026-09-24 | `NEO` | 125 | $18.36 | $18.47 | +13.75 | — | +0.00 | +13.75 | -181.25 | — |
| 2026-09-24 | `CBRL` | 6 | $47.52 | $46.88 | -3.84 | $51.84 | +29.76 | +25.92 | -4.14 | +25.62 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +787.37 | TPG | — | $24.65 | $10,784.79 | TPG×197 |
| 2026-08-14 | +5.50 | $24.65 | TPG×197 | $10,916.78 | +131.99 | -443.50 | KULR, NPWR, RLX | — | $9.24 | $10,473.10 | TPG×197, KULR×2, NPWR×3, RLX×3 |
| 2026-08-17 | +2.25 | $9.24 | TPG×197, KULR×2, NPWR×3, RLX×3 | $10,402.01 | -71.09 | -178.04 | — | — | $9.24 | $10,223.97 | TPG×197, KULR×2, NPWR×3, RLX×3 |
| 2026-08-18 | -6.20 | $9.24 | TPG×197, KULR×2, NPWR×3, RLX×3 | $10,223.67 | -0.30 | -0.16 | — | TPG | $10,205.24 | $10,220.82 | KULR×2, NPWR×3, RLX×3 |
| 2026-08-19 | -7.20 | $10,205.24 | KULR×2, NPWR×3, RLX×3 | $10,220.96 | +0.14 | +0.00 | — | KULR, NPWR, RLX | $10,220.72 | $10,220.72 | — |
| 2026-08-20 | +1.12 | $10,220.72 | — | $10,220.72 | -0.00 | -128.22 | FUTU, WMT, BJ | — | $137.51 | $10,086.23 | FUTU×28, WMT×32, BJ×38 |
| 2026-08-21 | +3.25 | $137.51 | FUTU×28, WMT×32, BJ×38 | $10,251.87 | +165.64 | +331.02 | HITI | — | $2.36 | $10,581.39 | FUTU×28, WMT×32, BJ×38, HITI×55 |
| 2026-08-24 | -5.17 | $2.36 | FUTU×28, WMT×32, BJ×38, HITI×55 | $10,544.35 | -37.04 | -13.71 | — | — | $2.36 | $10,530.64 | FUTU×28, WMT×32, BJ×38, HITI×55 |
| 2026-08-25 | +1.80 | $2.36 | FUTU×28, WMT×32, BJ×38, HITI×55 | $10,531.26 | +0.62 | -103.35 | ZYME, RHI, ABUS | FUTU, WMT, BJ | $24.28 | $10,408.46 | HITI×55, ZYME×119, RHI×79, ABUS×659 |
| 2026-08-26 | +2.02 | $24.28 | HITI×55, ZYME×119, RHI×79, ABUS×659 | $10,367.55 | -40.91 | +221.60 | NCNO, PLAB | HITI | $87.34 | $10,586.78 | ZYME×119, RHI×79, ABUS×659, NCNO×2, PLAB×1 |
| 2026-08-27 | — | $87.34 | ZYME×119, RHI×79, ABUS×659, NCNO×2, PLAB×1 | $10,560.62 | -26.16 | +15.37 | TRLV, BOX | — | $18.72 | $10,575.30 | ZYME×119, RHI×79, ABUS×659, NCNO×2, PLAB×1, TRLV×3, BOX×1 |
| 2026-08-28 | +0.75 | $18.72 | ZYME×119, RHI×79, ABUS×659, NCNO×2, PLAB×1, TRLV×3, BOX×1 | $10,513.51 | -61.79 | -235.35 | JKS, DY, ULTA | ZYME, RHI, ABUS | $277.29 | $10,257.50 | NCNO×2, PLAB×1, TRLV×3, BOX×1, JKS×258, DY×11, ULTA×6 |
| 2026-08-31 | -5.85 | $277.29 | NCNO×2, PLAB×1, TRLV×3, BOX×1, JKS×258, DY×11, ULTA×6 | $10,318.80 | +61.30 | -226.29 | — | NCNO, PLAB | $349.87 | $10,091.73 | TRLV×3, BOX×1, JKS×258, DY×11, ULTA×6 |
| 2026-09-01 | -6.30 | $349.87 | TRLV×3, BOX×1, JKS×258, DY×11, ULTA×6 | $9,992.98 | -98.75 | +73.56 | — | TRLV, BOX | $419.42 | $10,065.78 | JKS×258, DY×11, ULTA×6 |
| 2026-09-02 | -3.83 | $419.42 | JKS×258, DY×11, ULTA×6 | $10,073.49 | +7.71 | +0.00 | — | JKS, DY, ULTA | $10,065.99 | $10,065.99 | — |
| 2026-09-03 | -0.90 | $10,065.99 | — | $10,065.99 | +0.00 | -338.23 | VIR, AVGO, FIVE, MOMO, PVH, VSXY | — | $488.58 | $9,713.29 | VIR×145, AVGO×4, FIVE×6, MOMO×305, PVH×22, VSXY×21 |
| 2026-09-04 | +2.25 | $488.58 | VIR×145, AVGO×4, FIVE×6, MOMO×305, PVH×22, VSXY×21 | $9,712.87 | -0.42 | +236.85 | ATRC, WNC, ADCT, HAFN, DOCU, XP | — | $116.97 | $9,945.84 | VIR×145, AVGO×4, FIVE×6, MOMO×305, PVH×22, VSXY×21, ATRC×1, WNC×4, ADCT×53, HAFN×7, DOCU×1, XP×3 |
| 2026-09-08 | -11.47 | $116.97 | VIR×145, AVGO×4, FIVE×6, MOMO×305, PVH×22, VSXY×21, ATRC×1, WNC×4, ADCT×53, HAFN×7, DOCU×1, XP×3 | $9,870.80 | -75.04 | +50.66 | — | — | $116.97 | $9,921.46 | VIR×145, AVGO×4, FIVE×6, MOMO×305, PVH×22, VSXY×21, ATRC×1, WNC×4, ADCT×53, HAFN×7, DOCU×1, XP×3 |
| 2026-09-09 | -13.95 | $116.97 | VIR×145, AVGO×4, FIVE×6, MOMO×305, PVH×22, VSXY×21, ATRC×1, WNC×4, ADCT×53, HAFN×7, DOCU×1, XP×3 | $9,845.04 | -76.42 | -6.53 | — | VIR, AVGO, FIVE, MOMO, PVH, VSXY | $9,468.46 | $9,823.84 | ATRC×1, WNC×4, ADCT×53, HAFN×7, DOCU×1, XP×3 |
| 2026-09-10 | -13.28 | $9,468.46 | ATRC×1, WNC×4, ADCT×53, HAFN×7, DOCU×1, XP×3 | $9,820.80 | -3.04 | +0.00 | — | ATRC, WNC, ADCT, HAFN, DOCU, XP | $9,816.95 | $9,816.95 | — |
| 2026-09-11 | +0.50 | $9,816.95 | — | $9,816.95 | +0.00 | +163.92 | SSL | — | $7.09 | $9,972.06 | SSL×683 |
| 2026-09-14 | -11.00 | $7.09 | SSL×683 | $10,040.36 | +68.30 | -129.77 | — | — | $7.09 | $9,910.59 | SSL×683 |
| 2026-09-15 | -3.84 | $7.09 | SSL×683 | $9,903.76 | -6.83 | +423.46 | — | — | $7.09 | $10,327.22 | SSL×683 |
| 2026-09-16 | +5.30 | $7.09 | SSL×683 | $9,992.55 | -334.67 | -46.25 | ATRC, TCOM, LEN | SSL | $71.93 | $9,930.78 | ATRC×59, TCOM×81, LEN×41 |
| 2026-09-17 | +7.38 | $71.93 | ATRC×59, TCOM×81, LEN×41 | $10,116.56 | +185.78 | -19.69 | — | — | $71.93 | $10,096.87 | ATRC×59, TCOM×81, LEN×41 |
| 2026-09-18 | +4.86 | $71.93 | ATRC×59, TCOM×81, LEN×41 | $10,021.68 | -75.19 | -93.14 | — | — | $71.93 | $9,928.54 | ATRC×59, TCOM×81, LEN×41 |
| 2026-09-21 | +12.87 | $71.93 | ATRC×59, TCOM×81, LEN×41 | $9,984.68 | +56.14 | +1.58 | A, HUM, UMC, NEO | ATRC, TCOM, LEN | $301.11 | $9,970.94 | A×15, HUM×6, UMC×100, NEO×125 |
| 2026-09-22 | -0.50 | $301.11 | A×15, HUM×6, UMC×100, NEO×125 | $9,953.94 | -17.00 | +51.00 | — | — | $301.11 | $10,004.94 | A×15, HUM×6, UMC×100, NEO×125 |
| 2026-09-23 | +2.29 | $301.11 | A×15, HUM×6, UMC×100, NEO×125 | $9,883.46 | -121.48 | -91.17 | CBRL | — | $13.68 | $9,790.28 | A×15, HUM×6, UMC×100, NEO×125, CBRL×6 |
| 2026-09-24 | -7.66 | $13.68 | A×15, HUM×6, UMC×100, NEO×125, CBRL×6 | $9,721.20 | -69.08 | +29.76 | — | A, HUM, UMC, NEO | $9,431.09 | $9,742.13 | CBRL×6 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 197 | $50.62 | $2.58 | — | $24.65 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list flatten; ⚪; ret5=+6.2; leftover $10000.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.65 | ▲ close $10,784.79 vs 09:30 $10,000.00 (session +787.37) | 16:00 close · cash $24.65 · equity $10,784.79 vs 09:30 $10,000.00 (+784.79; session marks +787.37) · 1 name(s) marked open→close (per-name table). TPG×197 09:30 $50.62 → close $54.62 +787.37 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.65 | ▲ 09:30 equity $10,916.78 vs yday $10,784.79 (+131.99) | 09:30 open · cash $24.65 (unchanged overnight, no fees) · equity $10,916.78 vs prior close $10,784.79 (+131.99) · 1 name(s) re-marked at the open (per-name table). TPG×197 yday $54.62 → 09:30 $55.29 +131.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `KULR` | 2 | $2.50 | $0.06 | — | $19.59 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ⚪; ret5=+7.6; leftover $6.16 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NPWR` | 3 | $1.56 | $0.06 | — | $14.86 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+8.0; leftover $6.16 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `RLX` | 3 | $1.85 | $0.06 | — | $9.24 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ⚪; ret5=+0.5; leftover $6.16 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.24 | ▼ close $10,473.10 vs 09:30 $10,916.78 (session -443.50) | 16:00 close · cash $9.24 · equity $10,473.10 vs 09:30 $10,916.78 (-443.68; session marks -443.50) · 4 name(s) marked open→close (per-name table). TPG×197 09:30 $55.29 → close $53.03 -445.22; KULR×2 09:30 $2.50 → close $2.64 +0.28; NPWR×3 09:30 $1.56 → close $1.95 +1.17; RLX×3 09:30 $1.85 → close $1.94 +0.27 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.24 | ▼ 09:30 equity $10,402.01 vs yday $10,473.10 (-71.09) | 09:30 open · cash $9.24 (unchanged overnight, no fees) · equity $10,402.01 vs prior close $10,473.10 (-71.09) · 4 name(s) re-marked at the open (per-name table). TPG×197 yday $53.03 → 09:30 $52.67 -70.92; KULR×2 yday $2.64 → 09:30 $2.63 -0.02; NPWR×3 yday $1.95 → 09:30 $1.92 -0.09; RLX×3 yday $1.94 → 09:30 $1.92 -0.06 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.24 | ▼ close $10,223.97 vs 09:30 $10,402.01 (session -178.04) | 16:00 close · cash $9.24 · equity $10,223.97 vs 09:30 $10,402.01 (-178.04; session marks -178.04) · 4 name(s) marked open→close (per-name table). TPG×197 09:30 $52.67 → close $51.77 -177.30; KULR×2 09:30 $2.63 → close $2.62 -0.02; NPWR×3 09:30 $1.92 → close $1.73 -0.57; RLX×3 09:30 $1.92 → close $1.87 -0.15 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.24 | ▼ 09:30 equity $10,223.67 vs yday $10,223.97 (-0.30) | 09:30 open · cash $9.24 (unchanged overnight, no fees) · equity $10,223.67 vs prior close $10,223.97 (-0.30) · 4 name(s) re-marked at the open (per-name table). TPG×197 yday $51.77 → 09:30 $51.77 +0.00; KULR×2 yday $2.62 → 09:30 $2.53 -0.18; NPWR×3 yday $1.73 → 09:30 $1.70 -0.09; RLX×3 yday $1.87 → 09:30 $1.86 -0.03 | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 197 | $51.77 | $2.70 | $+220.64 | $10,205.24 | ▲ +220.64 after sell → book $10,220.98; vs 09:30 mark -2.69 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,205.24 | ▼ close $10,220.82 vs 09:30 $10,223.67 (session -0.16) | 16:00 close · cash $10,205.24 · equity $10,220.82 vs 09:30 $10,223.67 (-2.85; session marks -0.16) · 3 name(s) marked open→close (per-name table). KULR×2 09:30 $2.53 → close $2.57 +0.08; NPWR×3 09:30 $1.70 → close $1.65 -0.15; RLX×3 09:30 $1.86 → close $1.83 -0.09 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,205.24 | ▲ 09:30 equity $10,220.96 vs yday $10,220.82 (+0.14) | 09:30 open · cash $10,205.24 (unchanged overnight, no fees) · equity $10,220.96 vs prior close $10,220.82 (+0.14) · 3 name(s) re-marked at the open (per-name table). KULR×2 yday $2.57 → 09:30 $2.55 -0.04; NPWR×3 yday $1.65 → 09:30 $1.70 +0.15; RLX×3 yday $1.83 → 09:30 $1.84 +0.03 | — |
| 2026-08-19 09:30 ET | **SELL** | `KULR` | 2 | $2.55 | $0.08 | $-0.03 | $10,210.26 | ▼ -0.03 after sell → book $10,220.88; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `NPWR` | 3 | $1.70 | $0.08 | $+0.28 | $10,215.28 | ▲ +0.28 after sell → book $10,220.80; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `RLX` | 3 | $1.84 | $0.08 | $-0.18 | $10,220.72 | ▼ -0.18 after sell → book $10,220.72; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,220.72 | ▲ close $10,220.72 vs 09:30 $10,220.96 (session +0.00) | 16:00 close · cash $10,220.72 · no lots left · equity $10,220.72. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,220.72 | ▲ 09:30 equity $10,220.72 vs yday $10,220.72 (-0.00) | 09:30 open · cash $10,220.72 · no holdings · equity $10,220.72 vs prior close $10,220.72 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `FUTU` | 28 | $117.65 | $2.07 | — | $6,924.44 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+4.1; leftover $3406.91 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WMT` | 32 | $106.38 | $2.09 | — | $3,518.20 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-1.7; leftover $3406.91 | join🟢 sector🟡 gen🟢 news🔴 digest🟡 judge🔴 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BJ` | 38 | $88.91 | $2.10 | — | $137.51 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list overnight; 🔵; ret5=-1.0; leftover $3406.91 | join🟡 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟡 vol🔴 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $137.51 | ▼ close $10,086.23 vs 09:30 $10,220.72 (session -128.22) | 16:00 close · cash $137.51 · equity $10,086.23 vs 09:30 $10,220.72 (-134.49; session marks -128.22) · 3 name(s) marked open→close (per-name table). FUTU×28 09:30 $117.65 → close $112.73 -137.76; WMT×32 09:30 $106.38 → close $103.84 -81.28; BJ×38 09:30 $88.91 → close $91.30 +90.82 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $137.51 | ▲ 09:30 equity $10,251.87 vs yday $10,086.23 (+165.64) | 09:30 open · cash $137.51 (unchanged overnight, no fees) · equity $10,251.87 vs prior close $10,086.23 (+165.64) · 3 name(s) re-marked at the open (per-name table). FUTU×28 yday $112.73 → 09:30 $115.18 +68.60; WMT×32 yday $103.84 → 09:30 $103.69 -4.80; BJ×38 yday $91.30 → 09:30 $93.98 +101.84 | — |
| 2026-08-21 09:30 ET | **BUY** | `HITI` | 55 | $2.43 | $1.50 | — | $2.36 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list mover_buy; 🔵; ⚪; ret5=+5.6; leftover $137.51 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.36 | ▲ close $10,581.39 vs 09:30 $10,251.87 (session +331.02) | 16:00 close · cash $2.36 · equity $10,581.39 vs 09:30 $10,251.87 (+329.52; session marks +331.02) · 4 name(s) marked open→close (per-name table). FUTU×28 09:30 $115.18 → close $123.64 +236.88; WMT×32 09:30 $103.69 → close $103.70 +0.32; BJ×38 09:30 $93.98 → close $96.42 +92.72; HITI×55 09:30 $2.43 → close $2.45 +1.10 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.36 | ▼ 09:30 equity $10,544.35 vs yday $10,581.39 (-37.04) | 09:30 open · cash $2.36 (unchanged overnight, no fees) · equity $10,544.35 vs prior close $10,581.39 (-37.04) · 4 name(s) re-marked at the open (per-name table). FUTU×28 yday $123.64 → 09:30 $121.00 -73.92; WMT×32 yday $103.70 → 09:30 $104.14 +14.08; BJ×38 yday $96.42 → 09:30 $97.02 +22.80; HITI×55 yday $2.45 → 09:30 $2.45 +0.00 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.36 | ▼ close $10,530.64 vs 09:30 $10,544.35 (session -13.71) | 16:00 close · cash $2.36 · equity $10,530.64 vs 09:30 $10,544.35 (-13.71; session marks -13.71) · 4 name(s) marked open→close (per-name table). FUTU×28 09:30 $121.00 → close $115.81 -145.32; WMT×32 09:30 $104.14 → close $106.49 +75.20; BJ×38 09:30 $97.02 → close $98.49 +55.86; HITI×55 09:30 $2.45 → close $2.46 +0.55 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.36 | ▲ 09:30 equity $10,531.26 vs yday $10,530.64 (+0.62) | 09:30 open · cash $2.36 (unchanged overnight, no fees) · equity $10,531.26 vs prior close $10,530.64 (+0.62) · 4 name(s) re-marked at the open (per-name table). FUTU×28 yday $115.81 → 09:30 $118.00 +61.32; WMT×32 yday $106.49 → 09:30 $105.58 -29.12; BJ×38 yday $98.49 → 09:30 $97.63 -32.68; HITI×55 yday $2.46 → 09:30 $2.48 +1.10 | — |
| 2026-08-25 09:30 ET | **SELL** | `FUTU` | 28 | $118.00 | $2.11 | $+5.62 | $3,304.25 | ▲ +5.62 after sell → book $10,529.15; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WMT` | 32 | $105.58 | $2.12 | $-29.81 | $6,680.69 | ▼ -29.81 after sell → book $10,527.03; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BJ` | 38 | $97.63 | $2.14 | $+327.11 | $10,388.48 | ▲ +327.11 after sell → book $10,524.88; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 119 | $28.86 | $2.35 | — | $6,951.80 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; 🔵; ret5=+13.7; leftover $3462.83 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 79 | $43.76 | $2.23 | — | $3,492.53 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list mover_buy; 🔵; ⚪; ret5=+6.2; leftover $3462.83 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ABUS` | 659 | $5.25 | $8.50 | — | $24.28 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list mover_buy; 🔵; ⚪; ret5=+10.4; leftover $3462.83 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.28 | ▼ close $10,408.46 vs 09:30 $10,531.26 (session -103.35) | 16:00 close · cash $24.28 · equity $10,408.46 vs 09:30 $10,531.26 (-122.80; session marks -103.35) · 4 name(s) marked open→close (per-name table). HITI×55 09:30 $2.48 → close $2.57 +4.95; ZYME×119 09:30 $28.86 → close $27.47 -165.41; RHI×79 09:30 $43.76 → close $44.90 +90.06; ABUS×659 09:30 $5.25 → close $5.20 -32.95 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.28 | ▼ 09:30 equity $10,367.55 vs yday $10,408.46 (-40.91) | 09:30 open · cash $24.28 (unchanged overnight, no fees) · equity $10,367.55 vs prior close $10,408.46 (-40.91) · 4 name(s) re-marked at the open (per-name table). HITI×55 yday $2.57 → 09:30 $2.57 +0.00; ZYME×119 yday $27.47 → 09:30 $27.56 +10.71; RHI×79 yday $44.90 → 09:30 $44.33 -45.03; ABUS×659 yday $5.20 → 09:30 $5.19 -6.59 | — |
| 2026-08-26 09:30 ET | **SELL** | `HITI` | 55 | $2.57 | $1.60 | $+4.60 | $164.03 | ▲ +4.60 after sell → book $10,365.95; vs 09:30 mark -1.60 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `NCNO` | 2 | $19.33 | $0.39 | — | $124.98 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; ret5=+3.0; leftover $41.01 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `PLAB` | 1 | $37.26 | $0.38 | — | $87.34 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-8.0; leftover $41.01 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $87.34 | ▲ close $10,586.78 vs 09:30 $10,367.55 (session +221.60) | 16:00 close · cash $87.34 · equity $10,586.78 vs 09:30 $10,367.55 (+219.23; session marks +221.60) · 5 name(s) marked open→close (per-name table). ZYME×119 09:30 $27.56 → close $29.30 +207.66; RHI×79 09:30 $44.33 → close $44.54 +16.59; ABUS×659 09:30 $5.19 → close $5.19 +0.00; NCNO×2 09:30 $19.33 → close $21.51 +4.36; PLAB×1 09:30 $37.26 → close $30.25 -7.01 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $87.34 | ▼ 09:30 equity $10,560.62 vs yday $10,586.78 (-26.16) | 09:30 open · cash $87.34 (unchanged overnight, no fees) · equity $10,560.62 vs prior close $10,586.78 (-26.16) · 5 name(s) re-marked at the open (per-name table). ZYME×119 yday $29.30 → 09:30 $29.33 +2.97; RHI×79 yday $44.54 → 09:30 $44.41 -10.27; ABUS×659 yday $5.19 → 09:30 $5.16 -19.77; NCNO×2 yday $21.51 → 09:30 $22.03 +1.04; PLAB×1 yday $30.25 → 09:30 $30.12 -0.13 | — |
| 2026-08-27 09:30 ET | **BUY** | `TRLV` | 3 | $11.38 | $0.35 | — | $52.85 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; ret5=+13.3; leftover $43.67 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `BOX` | 1 | $33.79 | $0.34 | — | $18.72 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+0.8; leftover $43.67 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18.72 | ▲ close $10,575.30 vs 09:30 $10,560.62 (session +15.37) | 16:00 close · cash $18.72 · equity $10,575.30 vs 09:30 $10,560.62 (+14.68; session marks +15.37) · 7 name(s) marked open→close (per-name table). ZYME×119 09:30 $29.33 → close $29.01 -38.08; RHI×79 09:30 $44.41 → close $44.97 +44.24; ABUS×659 09:30 $5.16 → close $5.17 +6.59; NCNO×2 09:30 $22.03 → close $23.32 +2.58; PLAB×1 09:30 $30.12 → close $30.26 +0.14; TRLV×3 09:30 $11.38 → close $11.03 -1.05; BOX×1 09:30 $33.79 → close $34.74 +0.95 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18.72 | ▼ 09:30 equity $10,513.51 vs yday $10,575.30 (-61.79) | 09:30 open · cash $18.72 (unchanged overnight, no fees) · equity $10,513.51 vs prior close $10,575.30 (-61.79) · 7 name(s) re-marked at the open (per-name table). ZYME×119 yday $29.01 → 09:30 $28.91 -11.90; RHI×79 yday $44.97 → 09:30 $44.51 -36.34; ABUS×659 yday $5.17 → 09:30 $5.15 -13.18; NCNO×2 yday $23.32 → 09:30 $23.30 -0.04; PLAB×1 yday $30.26 → 09:30 $30.01 -0.25; TRLV×3 yday $11.03 → 09:30 $11.00 -0.09; BOX×1 yday $34.74 → 09:30 $34.75 +0.01 | — |
| 2026-08-28 09:30 ET | **SELL** | `ZYME` | 119 | $28.91 | $2.39 | $+1.21 | $3,456.62 | ▲ +1.21 after sell → book $10,511.12; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `RHI` | 79 | $44.51 | $2.27 | $+54.75 | $6,970.64 | ▲ +54.75 after sell → book $10,508.85; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ABUS` | 659 | $5.15 | $8.64 | $-83.04 | $10,355.85 | ▼ -83.04 after sell → book $10,500.21; vs 09:30 mark -8.64 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `JKS` | 258 | $13.37 | $3.33 | — | $6,903.06 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list yday_mover; ret5=-14.9; leftover $3451.95 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DY` | 11 | $306.34 | $2.02 | — | $3,531.30 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list yday_mover; ret5=-23.0; leftover $3451.95 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ULTA` | 6 | $542.00 | $2.01 | — | $277.29 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; ret5=+4.8; leftover $3451.95 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $277.29 | ▼ close $10,257.50 vs 09:30 $10,513.51 (session -235.35) | 16:00 close · cash $277.29 · equity $10,257.50 vs 09:30 $10,513.51 (-256.01; session marks -235.35) · 7 name(s) marked open→close (per-name table). NCNO×2 09:30 $23.30 → close $22.99 -0.62; PLAB×1 09:30 $30.01 → close $27.73 -2.28; TRLV×3 09:30 $11.00 → close $11.82 +2.46; BOX×1 09:30 $34.75 → close $34.98 +0.23; JKS×258 09:30 $13.37 → close $13.54 +43.86; DY×11 09:30 $306.34 → close $294.34 -132.00; ULTA×6 09:30 $542.00 → close $517.50 -147.00 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $277.29 | ▲ 09:30 equity $10,318.80 vs yday $10,257.50 (+61.30) | 09:30 open · cash $277.29 (unchanged overnight, no fees) · equity $10,318.80 vs prior close $10,257.50 (+61.30) · 7 name(s) re-marked at the open (per-name table). NCNO×2 yday $22.99 → 09:30 $22.66 -0.66; PLAB×1 yday $27.73 → 09:30 $28.04 +0.31; TRLV×3 yday $11.82 → 09:30 $11.80 -0.06; BOX×1 yday $34.98 → 09:30 $34.72 -0.26; JKS×258 yday $13.54 → 09:30 $13.54 +0.00; DY×11 yday $294.34 → 09:30 $298.01 +40.37; ULTA×6 yday $517.50 → 09:30 $521.10 +21.60 | — |
| 2026-08-31 09:30 ET | **SELL** | `NCNO` | 2 | $22.66 | $0.48 | $+5.79 | $322.13 | ▲ +5.79 after sell → book $10,318.32; vs 09:30 mark -0.48 | dropped from list after 3 sess (min 3) | join🟡 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `PLAB` | 1 | $28.04 | $0.30 | $-9.90 | $349.87 | ▼ -9.90 after sell → book $10,318.02; vs 09:30 mark -0.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $349.87 | ▼ close $10,091.73 vs 09:30 $10,318.80 (session -226.29) | 16:00 close · cash $349.87 · equity $10,091.73 vs 09:30 $10,318.80 (-227.07; session marks -226.29) · 5 name(s) marked open→close (per-name table). TRLV×3 09:30 $11.80 → close $11.51 -0.87; BOX×1 09:30 $34.72 → close $35.78 +1.06; JKS×258 09:30 $13.54 → close $12.58 -247.68; DY×11 09:30 $298.01 → close $291.21 -74.80; ULTA×6 09:30 $521.10 → close $537.10 +96.00 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $349.87 | ▼ 09:30 equity $9,992.98 vs yday $10,091.73 (-98.75) | 09:30 open · cash $349.87 (unchanged overnight, no fees) · equity $9,992.98 vs prior close $10,091.73 (-98.75) · 5 name(s) re-marked at the open (per-name table). TRLV×3 yday $11.51 → 09:30 $11.54 +0.09; BOX×1 yday $35.78 → 09:30 $35.69 -0.09; JKS×258 yday $12.58 → 09:30 $12.50 -20.64; DY×11 yday $291.21 → 09:30 $289.16 -22.55; ULTA×6 yday $537.10 → 09:30 $527.84 -55.56 | — |
| 2026-09-01 09:30 ET | **SELL** | `TRLV` | 3 | $11.54 | $0.38 | $-0.25 | $384.11 | ▼ -0.25 after sell → book $9,992.60; vs 09:30 mark -0.38 | dropped from list after 3 sess (min 3) | join🟡 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `BOX` | 1 | $35.69 | $0.38 | $+1.18 | $419.42 | ▲ +1.18 after sell → book $9,992.22; vs 09:30 mark -0.38 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $419.42 | ▲ close $10,065.78 vs 09:30 $9,992.98 (session +73.56) | 16:00 close · cash $419.42 · equity $10,065.78 vs 09:30 $9,992.98 (+72.80; session marks +73.56) · 3 name(s) marked open→close (per-name table). JKS×258 09:30 $12.50 → close $12.45 -12.90; DY×11 09:30 $289.16 → close $287.30 -20.46; ULTA×6 09:30 $527.84 → close $545.66 +106.92 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $419.42 | ▲ 09:30 equity $10,073.49 vs yday $10,065.78 (+7.71) | 09:30 open · cash $419.42 (unchanged overnight, no fees) · equity $10,073.49 vs prior close $10,065.78 (+7.71) · 3 name(s) re-marked at the open (per-name table). JKS×258 yday $12.45 → 09:30 $12.45 +0.00; DY×11 yday $287.30 → 09:30 $287.99 +7.59; ULTA×6 yday $545.66 → 09:30 $545.68 +0.12 | — |
| 2026-09-02 09:30 ET | **SELL** | `JKS` | 258 | $12.45 | $3.40 | $-244.08 | $3,628.13 | ▼ -244.08 after sell → book $10,070.10; vs 09:30 mark -3.39 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `DY` | 11 | $287.99 | $2.06 | $-205.93 | $6,793.96 | ▼ -205.93 after sell → book $10,068.04; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ULTA` | 6 | $545.68 | $2.04 | $+18.03 | $10,065.99 | ▲ +18.03 after sell → book $10,065.99; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,065.99 | ▲ close $10,065.99 vs 09:30 $10,073.49 (session +0.00) | 16:00 close · cash $10,065.99 · no lots left · equity $10,065.99. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,065.99 | ▲ 09:30 equity $10,065.99 vs yday $10,065.99 (+0.00) | 09:30 open · cash $10,065.99 · no holdings · equity $10,065.99 vs prior close $10,065.99 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `VIR` | 145 | $11.54 | $2.42 | — | $8,390.27 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list yday_gainer; 🔵; ⚪; ret5=+3.8; leftover $1677.67 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 4 | $351.74 | $2.00 | — | $6,981.31 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+3.3; leftover $1677.67 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 6 | $257.00 | $2.01 | — | $5,437.30 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-5.5; leftover $1677.67 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MOMO` | 305 | $5.50 | $3.93 | — | $3,755.86 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-4.8; leftover $1677.67 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PVH` | 22 | $74.96 | $2.06 | — | $2,104.69 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-7.9; leftover $1677.67 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSXY` | 21 | $76.86 | $2.05 | — | $488.58 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-6.6; leftover $1677.67 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $488.58 | ▼ close $9,713.29 vs 09:30 $10,065.99 (session -338.23) | 16:00 close · cash $488.58 · equity $9,713.29 vs 09:30 $10,065.99 (-352.70; session marks -338.23) · 6 name(s) marked open→close (per-name table). VIR×145 09:30 $11.54 → close $11.45 -13.05; AVGO×4 09:30 $351.74 → close $357.16 +21.68; FIVE×6 09:30 $257.00 → close $239.96 -102.24; MOMO×305 09:30 $5.50 → close $5.10 -122.00; PVH×22 09:30 $74.96 → close $72.46 -55.00; VSXY×21 09:30 $76.86 → close $73.64 -67.62 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $488.58 | ▼ 09:30 equity $9,712.87 vs yday $9,713.29 (-0.42) | 09:30 open · cash $488.58 (unchanged overnight, no fees) · equity $9,712.87 vs prior close $9,713.29 (-0.42) · 6 name(s) re-marked at the open (per-name table). VIR×145 yday $11.45 → 09:30 $11.31 -20.30; AVGO×4 yday $357.16 → 09:30 $359.70 +10.16; FIVE×6 yday $239.96 → 09:30 $238.88 -6.48; MOMO×305 yday $5.10 → 09:30 $5.13 +9.15; PVH×22 yday $72.46 → 09:30 $72.79 +7.26; VSXY×21 yday $73.64 → 09:30 $73.63 -0.21 | — |
| 2026-09-04 09:30 ET | **BUY** | `ATRC` | 1 | $52.03 | $0.52 | — | $436.02 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.5; leftover $69.80 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `WNC` | 4 | $14.17 | $0.58 | — | $378.76 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list yday_gainer; 🔵; ret5=+7.9; leftover $69.80 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ADCT` | 53 | $1.30 | $0.85 | — | $309.02 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; 🔵; ⚪; ret5=+17.9; leftover $69.80 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `HAFN` | 7 | $8.94 | $0.65 | — | $245.79 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; ret5=+7.7; leftover $69.80 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 1 | $68.52 | $0.69 | — | $176.58 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+3.4; leftover $69.80 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `XP` | 3 | $19.67 | $0.60 | — | $116.97 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list mover_buy; 🔵; ⚪; ret5=+13.1; leftover $69.80 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $116.97 | ▲ close $9,945.84 vs 09:30 $9,712.87 (session +236.85) | 16:00 close · cash $116.97 · equity $9,945.84 vs 09:30 $9,712.87 (+232.97; session marks +236.85) · 12 name(s) marked open→close (per-name table). VIR×145 09:30 $11.31 → close $11.38 +10.87; AVGO×4 09:30 $359.70 → close $357.90 -7.20; FIVE×6 09:30 $238.88 → close $252.20 +79.92; MOMO×305 09:30 $5.13 → close $5.37 +73.20; PVH×22 09:30 $72.79 → close $74.33 +33.88; VSXY×21 09:30 $73.63 → close $75.56 +40.53; ATRC×1 09:30 $52.03 → close $51.52 -0.51; WNC×4 09:30 $14.17 → close $14.31 +0.56; ADCT×53 09:30 $1.30 → close $1.36 +3.18; HAFN×7 09:30 $8.94 → close $9.22 +1.96; DOCU×1 09:30 $68.52 → close $68.41 -0.11; XP×3 09:30 $19.67 → close $19.86 +0.57 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $116.97 | ▼ 09:30 equity $9,870.80 vs yday $9,945.84 (-75.04) | 09:30 open · cash $116.97 (unchanged overnight, no fees) · equity $9,870.80 vs prior close $9,945.84 (-75.04) · 12 name(s) re-marked at the open (per-name table). VIR×145 yday $11.38 → 09:30 $11.22 -23.92; AVGO×4 yday $357.90 → 09:30 $363.68 +23.12; FIVE×6 yday $252.20 → 09:30 $251.22 -5.88; MOMO×305 yday $5.37 → 09:30 $5.28 -27.45; PVH×22 yday $74.33 → 09:30 $74.50 +3.74; VSXY×21 yday $75.56 → 09:30 $73.51 -43.05; ATRC×1 yday $51.52 → 09:30 $54.31 +2.79; WNC×4 yday $14.31 → 09:30 $14.22 -0.36; ADCT×53 yday $1.36 → 09:30 $1.33 -1.59; HAFN×7 yday $9.22 → 09:30 $8.81 -2.87; DOCU×1 yday $68.41 → 09:30 $67.05 -1.36; XP×3 yday $19.86 → 09:30 $20.46 +1.80 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $116.97 | ▲ close $9,921.46 vs 09:30 $9,870.80 (session +50.66) | 16:00 close · cash $116.97 · equity $9,921.46 vs 09:30 $9,870.80 (+50.66; session marks +50.66) · 12 name(s) marked open→close (per-name table). VIR×145 09:30 $11.22 → close $11.18 -5.80; AVGO×4 09:30 $363.68 → close $368.56 +19.52; FIVE×6 09:30 $251.22 → close $254.07 +17.10; MOMO×305 09:30 $5.28 → close $5.26 -6.10; PVH×22 09:30 $74.50 → close $71.26 -71.28; VSXY×21 09:30 $73.51 → close $78.47 +104.16; ATRC×1 09:30 $54.31 → close $53.73 -0.58; WNC×4 09:30 $14.22 → close $13.61 -2.44; ADCT×53 09:30 $1.33 → close $1.30 -1.59; HAFN×7 09:30 $8.81 → close $8.96 +1.05; DOCU×1 09:30 $67.05 → close $65.08 -1.97; XP×3 09:30 $20.46 → close $19.99 -1.41 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $116.97 | ▼ 09:30 equity $9,845.04 vs yday $9,921.46 (-76.42) | 09:30 open · cash $116.97 (unchanged overnight, no fees) · equity $9,845.04 vs prior close $9,921.46 (-76.42) · 12 name(s) re-marked at the open (per-name table). VIR×145 yday $11.18 → 09:30 $11.04 -20.30; AVGO×4 yday $368.56 → 09:30 $366.23 -9.32; FIVE×6 yday $254.07 → 09:30 $252.92 -6.90; MOMO×305 yday $5.26 → 09:30 $5.26 +0.00; PVH×22 yday $71.26 → 09:30 $70.83 -9.46; VSXY×21 yday $78.47 → 09:30 $77.16 -27.51; ATRC×1 yday $53.73 → 09:30 $53.16 -0.57; WNC×4 yday $13.61 → 09:30 $13.46 -0.60; ADCT×53 yday $1.30 → 09:30 $1.28 -1.06; HAFN×7 yday $8.96 → 09:30 $9.00 +0.28; DOCU×1 yday $65.08 → 09:30 $64.64 -0.44; XP×3 yday $19.99 → 09:30 $19.81 -0.54 | — |
| 2026-09-09 09:30 ET | **SELL** | `VIR` | 145 | $11.04 | $2.46 | $-77.39 | $1,715.31 | ▼ -77.39 after sell → book $9,842.58; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 4 | $366.23 | $2.02 | $+53.93 | $3,178.21 | ▲ +53.93 after sell → book $9,840.56; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FIVE` | 6 | $252.92 | $2.03 | $-28.52 | $4,693.70 | ▼ -28.52 after sell → book $9,838.53; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MOMO` | 305 | $5.26 | $4.00 | $-81.13 | $6,294.00 | ▼ -81.13 after sell → book $9,834.53; vs 09:30 mark -4.00 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `PVH` | 22 | $70.83 | $2.08 | $-94.99 | $7,850.18 | ▼ -94.99 after sell → book $9,832.45; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `VSXY` | 21 | $77.16 | $2.08 | $+2.17 | $9,468.46 | ▲ +2.17 after sell → book $9,830.37; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,468.46 | ▼ close $9,823.84 vs 09:30 $9,845.04 (session -6.53) | 16:00 close · cash $9,468.46 · equity $9,823.84 vs 09:30 $9,845.04 (-21.20; session marks -6.53) · 6 name(s) marked open→close (per-name table). ATRC×1 09:30 $53.16 → close $53.03 -0.13; WNC×4 09:30 $13.46 → close $13.00 -1.84; ADCT×53 09:30 $1.28 → close $1.22 -3.18; HAFN×7 09:30 $9.00 → close $9.16 +1.12; DOCU×1 09:30 $64.64 → close $64.45 -0.19; XP×3 09:30 $19.81 → close $19.04 -2.31 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,468.46 | ▼ 09:30 equity $9,820.80 vs yday $9,823.84 (-3.04) | 09:30 open · cash $9,468.46 (unchanged overnight, no fees) · equity $9,820.80 vs prior close $9,823.84 (-3.04) · 6 name(s) re-marked at the open (per-name table). ATRC×1 yday $53.03 → 09:30 $52.31 -0.72; WNC×4 yday $13.00 → 09:30 $12.79 -0.84; ADCT×53 yday $1.22 → 09:30 $1.21 -0.53; HAFN×7 yday $9.16 → 09:30 $9.08 -0.56; DOCU×1 yday $64.45 → 09:30 $64.60 +0.15; XP×3 yday $19.04 → 09:30 $18.86 -0.54 | — |
| 2026-09-10 09:30 ET | **SELL** | `ATRC` | 1 | $52.31 | $0.55 | $-0.79 | $9,520.23 | ▼ -0.79 after sell → book $9,820.26; vs 09:30 mark -0.54 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `WNC` | 4 | $12.79 | $0.54 | $-6.64 | $9,570.84 | ▼ -6.64 after sell → book $9,819.71; vs 09:30 mark -0.55 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `ADCT` | 53 | $1.21 | $0.82 | $-6.44 | $9,634.15 | ▼ -6.44 after sell → book $9,818.89; vs 09:30 mark -0.82 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `HAFN` | 7 | $9.08 | $0.68 | $-0.34 | $9,697.04 | ▼ -0.34 after sell → book $9,818.22; vs 09:30 mark -0.67 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DOCU` | 1 | $64.60 | $0.67 | $-5.28 | $9,760.97 | ▼ -5.28 after sell → book $9,817.55; vs 09:30 mark -0.67 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `XP` | 3 | $18.86 | $0.59 | $-3.62 | $9,816.95 | ▼ -3.62 after sell → book $9,816.95; vs 09:30 mark -0.60 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,816.95 | ▲ close $9,816.95 vs 09:30 $9,820.80 (session +0.00) | 16:00 close · cash $9,816.95 · no lots left · equity $9,816.95. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,816.95 | ▲ 09:30 equity $9,816.95 vs yday $9,816.95 (+0.00) | 09:30 open · cash $9,816.95 · no holdings · equity $9,816.95 vs prior close $9,816.95 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `SSL` | 683 | $14.35 | $8.81 | — | $7.09 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; 🔵; ret5=+15.5; leftover $9816.95 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.09 | ▲ close $9,972.06 vs 09:30 $9,816.95 (session +163.92) | 16:00 close · cash $7.09 · equity $9,972.06 vs 09:30 $9,816.95 (+155.11; session marks +163.92) · 1 name(s) marked open→close (per-name table). SSL×683 09:30 $14.35 → close $14.59 +163.92 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.09 | ▲ 09:30 equity $10,040.36 vs yday $9,972.06 (+68.30) | 09:30 open · cash $7.09 (unchanged overnight, no fees) · equity $10,040.36 vs prior close $9,972.06 (+68.30) · 1 name(s) re-marked at the open (per-name table). SSL×683 yday $14.59 → 09:30 $14.69 +68.30 | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.09 | ▼ close $9,910.59 vs 09:30 $10,040.36 (session -129.77) | 16:00 close · cash $7.09 · equity $9,910.59 vs 09:30 $10,040.36 (-129.77; session marks -129.77) · 1 name(s) marked open→close (per-name table). SSL×683 09:30 $14.69 → close $14.50 -129.77 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.09 | ▼ 09:30 equity $9,903.76 vs yday $9,910.59 (-6.83) | 09:30 open · cash $7.09 (unchanged overnight, no fees) · equity $9,903.76 vs prior close $9,910.59 (-6.83) · 1 name(s) re-marked at the open (per-name table). SSL×683 yday $14.50 → 09:30 $14.49 -6.83 | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.09 | ▲ close $10,327.22 vs 09:30 $9,903.76 (session +423.46) | 16:00 close · cash $7.09 · equity $10,327.22 vs 09:30 $9,903.76 (+423.46; session marks +423.46) · 1 name(s) marked open→close (per-name table). SSL×683 09:30 $14.49 → close $15.11 +423.46 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.09 | ▼ 09:30 equity $9,992.55 vs yday $10,327.22 (-334.67) | 09:30 open · cash $7.09 (unchanged overnight, no fees) · equity $9,992.55 vs prior close $10,327.22 (-334.67) · 1 name(s) re-marked at the open (per-name table). SSL×683 yday $15.11 → 09:30 $14.62 -334.67 | — |
| 2026-09-16 09:30 ET | **SELL** | `SSL` | 683 | $14.62 | $9.00 | $+166.60 | $9,983.55 | ▲ +166.60 after sell → book $9,983.55; vs 09:30 mark -9.00 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `ATRC` | 59 | $55.66 | $2.17 | — | $6,697.44 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; 🔵; ret5=+4.6; leftover $3327.85 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 81 | $40.93 | $2.23 | — | $3,379.88 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; ret5=-3.1; leftover $3327.85 | join🔴 sector🔴 gen🟢 news🟡 digest🔴 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `LEN` | 41 | $80.63 | $2.11 | — | $71.93 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list overnight; ret5=-0.4; leftover $3327.85 | join🔴 sector🔴 gen🟢 news🟡 digest🔴 ab🔴 peer🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $71.93 | ▼ close $9,930.78 vs 09:30 $9,992.55 (session -46.25) | 16:00 close · cash $71.93 · equity $9,930.78 vs 09:30 $9,992.55 (-61.77; session marks -46.25) · 3 name(s) marked open→close (per-name table). ATRC×59 09:30 $55.66 → close $57.14 +87.32; TCOM×81 09:30 $40.93 → close $40.43 -40.50; LEN×41 09:30 $80.63 → close $78.36 -93.07 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $71.93 | ▲ 09:30 equity $10,116.56 vs yday $9,930.78 (+185.78) | 09:30 open · cash $71.93 (unchanged overnight, no fees) · equity $10,116.56 vs prior close $9,930.78 (+185.78) · 3 name(s) re-marked at the open (per-name table). ATRC×59 yday $57.14 → 09:30 $57.96 +48.38; TCOM×81 yday $40.43 → 09:30 $40.79 +29.16; LEN×41 yday $78.36 → 09:30 $81.00 +108.24 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $71.93 | ▼ close $10,096.87 vs 09:30 $10,116.56 (session -19.69) | 16:00 close · cash $71.93 · equity $10,096.87 vs 09:30 $10,116.56 (-19.69; session marks -19.69) · 3 name(s) marked open→close (per-name table). ATRC×59 09:30 $57.96 → close $59.12 +68.44; TCOM×81 09:30 $40.79 → close $40.36 -34.83; LEN×41 09:30 $81.00 → close $79.70 -53.30 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $71.93 | ▼ 09:30 equity $10,021.68 vs yday $10,096.87 (-75.19) | 09:30 open · cash $71.93 (unchanged overnight, no fees) · equity $10,021.68 vs prior close $10,096.87 (-75.19) · 3 name(s) re-marked at the open (per-name table). ATRC×59 yday $59.12 → 09:30 $58.51 -35.99; TCOM×81 yday $40.36 → 09:30 $40.61 +20.25; LEN×41 yday $79.70 → 09:30 $78.25 -59.45 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $71.93 | ▼ close $9,928.54 vs 09:30 $10,021.68 (session -93.14) | 16:00 close · cash $71.93 · equity $9,928.54 vs 09:30 $10,021.68 (-93.14; session marks -93.14) · 3 name(s) marked open→close (per-name table). ATRC×59 09:30 $58.51 → close $58.10 -24.19; TCOM×81 09:30 $40.61 → close $40.68 +5.67; LEN×41 09:30 $78.25 → close $76.43 -74.62 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $71.93 | ▲ 09:30 equity $9,984.68 vs yday $9,928.54 (+56.14) | 09:30 open · cash $71.93 (unchanged overnight, no fees) · equity $9,984.68 vs prior close $9,928.54 (+56.14) · 3 name(s) re-marked at the open (per-name table). ATRC×59 yday $58.10 → 09:30 $58.23 +7.67; TCOM×81 yday $40.68 → 09:30 $41.00 +25.92; LEN×41 yday $76.43 → 09:30 $76.98 +22.55 | — |
| 2026-09-21 09:30 ET | **SELL** | `ATRC` | 59 | $58.23 | $2.20 | $+147.26 | $3,505.30 | ▲ +147.26 after sell → book $9,982.48; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TCOM` | 81 | $41.00 | $2.27 | $+1.16 | $6,824.03 | ▲ +1.16 after sell → book $9,980.21; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `LEN` | 41 | $76.98 | $2.15 | $-153.91 | $9,978.06 | ▼ -153.91 after sell → book $9,978.06; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 15 | $157.87 | $2.04 | — | $7,607.97 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list flatten; ret5=+6.5; leftover $2494.51 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 6 | $386.20 | $2.01 | — | $5,288.77 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list flatten; ret5=-5.8; leftover $2494.51 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `UMC` | 100 | $24.93 | $2.29 | — | $2,793.48 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; ret5=+8.7; leftover $2494.51 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `NEO` | 125 | $19.92 | $2.37 | — | $301.11 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; ret5=+15.0; leftover $2494.51 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $301.11 | ▲ close $9,970.94 vs 09:30 $9,984.68 (session +1.58) | 16:00 close · cash $301.11 · equity $9,970.94 vs 09:30 $9,984.68 (-13.74; session marks +1.58) · 4 name(s) marked open→close (per-name table). A×15 09:30 $157.87 → close $161.94 +61.05; HUM×6 09:30 $386.20 → close $378.58 -45.72; UMC×100 09:30 $24.93 → close $25.43 +50.00; NEO×125 09:30 $19.92 → close $19.41 -63.75 | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $301.11 | ▼ 09:30 equity $9,953.94 vs yday $9,970.94 (-17.00) | 09:30 open · cash $301.11 (unchanged overnight, no fees) · equity $9,953.94 vs prior close $9,970.94 (-17.00) · 4 name(s) re-marked at the open (per-name table). A×15 yday $161.94 → 09:30 $161.94 +0.00; HUM×6 yday $378.58 → 09:30 $378.58 +0.00; UMC×100 yday $25.43 → 09:30 $25.26 -17.00; NEO×125 yday $19.41 → 09:30 $19.41 +0.00 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $301.11 | ▲ close $10,004.94 vs 09:30 $9,953.94 (session +51.00) | 16:00 close · cash $301.11 · equity $10,004.94 vs 09:30 $9,953.94 (+51.00; session marks +51.00) · 4 name(s) marked open→close (per-name table). A×15 09:30 $161.94 → close $161.94 +0.00; HUM×6 09:30 $378.58 → close $378.58 +0.00; UMC×100 09:30 $25.26 → close $25.77 +51.00; NEO×125 09:30 $19.41 → close $19.41 +0.00 | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $301.11 | ▼ 09:30 equity $9,883.46 vs yday $10,004.94 (-121.48) | 09:30 open · cash $301.11 (unchanged overnight, no fees) · equity $9,883.46 vs prior close $10,004.94 (-121.48) · 4 name(s) re-marked at the open (per-name table). A×15 yday $161.94 → 09:30 $166.54 +69.00; HUM×6 yday $378.58 → 09:30 $370.00 -51.48; UMC×100 yday $25.77 → 09:30 $25.28 -49.00; NEO×125 yday $19.41 → 09:30 $18.69 -90.00 | — |
| 2026-09-23 09:30 ET | **BUY** | `CBRL` | 6 | $47.57 | $2.01 | — | $13.68 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-11.2; leftover $301.11 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.68 | ▼ close $9,790.28 vs 09:30 $9,883.46 (session -91.17) | 16:00 close · cash $13.68 · equity $9,790.28 vs 09:30 $9,883.46 (-93.18; session marks -91.17) · 5 name(s) marked open→close (per-name table). A×15 09:30 $166.54 → close $165.32 -18.30; HUM×6 09:30 $370.00 → close $374.78 +28.68; UMC×100 09:30 $25.28 → close $24.68 -60.00; NEO×125 09:30 $18.69 → close $18.36 -41.25; CBRL×6 09:30 $47.57 → close $47.52 -0.30 | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.68 | ▼ 09:30 equity $9,721.20 vs yday $9,790.28 (-69.08) | 09:30 open · cash $13.68 (unchanged overnight, no fees) · equity $9,721.20 vs prior close $9,790.28 (-69.08) · 5 name(s) re-marked at the open (per-name table). A×15 yday $165.32 → 09:30 $163.95 -20.55; HUM×6 yday $374.78 → 09:30 $374.54 -1.44; UMC×100 yday $24.68 → 09:30 $24.11 -57.00; NEO×125 yday $18.36 → 09:30 $18.47 +13.75; CBRL×6 yday $47.52 → 09:30 $46.88 -3.84 | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 15 | $163.95 | $2.06 | $+87.10 | $2,470.87 | ▲ +87.10 after sell → book $9,719.14; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `HUM` | 6 | $374.54 | $2.04 | $-74.00 | $4,716.07 | ▼ -74.00 after sell → book $9,717.10; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `UMC` | 100 | $24.11 | $2.33 | $-86.62 | $7,124.75 | ▼ -86.62 after sell → book $9,714.78; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `NEO` | 125 | $18.47 | $2.40 | $-186.02 | $9,431.09 | ▼ -186.02 after sell → book $9,712.37; vs 09:30 mark -2.41 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,431.09 | ▲ close $9,742.13 vs 09:30 $9,721.20 (session +29.76) | 16:00 close · cash $9,431.09 · equity $9,742.13 vs 09:30 $9,721.20 (+20.93; session marks +29.76) · 1 name(s) marked open→close (per-name table). CBRL×6 09:30 $46.88 → close $51.84 +29.76 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SPHR` | cash | leftover split 6.16 < 1 share @ 176.68 |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `KULR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `RLX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `XP` | cash | leftover split 9.24 < 1 share @ 15.93 |
| 2026-08-18 | `KULR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `RLX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KEYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AURA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CIG` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CRDL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WB` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `FUTU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WMT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `FUTU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WMT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BJ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HITI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-25 | `HITI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `ZYME` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `RHI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ABUS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SJM` | cash | leftover split 41.01 < 1 share @ 134.80 |
| 2026-08-26 | `URBN` | cash | leftover split 41.01 < 1 share @ 78.90 |
| 2026-08-27 | `ZYME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RHI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ABUS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `NCNO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `PLAB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `NCNO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `PLAB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `TRLV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `BOX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TRLV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `BOX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `JKS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `DY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ULTA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TENB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `JKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ULTA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `AME` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `VIR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `AVGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `FIVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MOMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `PVH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSXY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDO` | cash | leftover split 69.80 < 1 share @ 162.10 |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `AVGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FIVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MOMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `PVH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `VSXY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `WNC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ADCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `HAFN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `DOCU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `XP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `KFY` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `WNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `ADCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `HAFN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `DOCU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `XP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LFMD` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NAMS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `JMKE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `DBI` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `SSL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `SSL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `LFMD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TCOM` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `ATRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `TCOM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `LEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TCOM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `LEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `HUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `UMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `NEO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `CBRL` | no_price | no 09:30 open |
| 2026-09-23 | `A` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `HUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `UMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `NEO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `CBRL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CHKP` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `FUL` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CBRL` | 6 | 2026-09-23 @ $47.57 | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-11.2; leftover $301.11 |
