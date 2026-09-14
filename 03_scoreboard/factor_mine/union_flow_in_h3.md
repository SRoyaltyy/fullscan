# Factor mine action — `union_flow_in_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ flow_in, no 🚨

Cash book **-4.03%** ($9,597) · signal-only (no cash/fees) was -4.57%. Starts YES **4/22**. Fills 47 · skips 73 · realized $-493.45.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $12.68.

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
| 2026-08-20 | `FUTU` | 43 | — | $117.65 | +0.00 | $112.73 | -211.56 | -211.56 | +0.00 | -211.56 |
| 2026-08-20 | `WMT` | 48 | — | $106.38 | +0.00 | $103.84 | -121.92 | -121.92 | +0.00 | -121.92 |
| 2026-08-21 | `FUTU` | 43 | $112.73 | $115.18 | +105.35 | $123.64 | +363.78 | +469.13 | -106.21 | +257.57 |
| 2026-08-21 | `WMT` | 48 | $103.84 | $103.69 | -7.20 | $103.70 | +0.48 | -6.72 | -129.12 | -128.64 |
| 2026-08-21 | `HITI` | 10 | — | $2.43 | +0.00 | $2.45 | +0.20 | +0.20 | +0.00 | +0.20 |
| 2026-08-24 | `FUTU` | 43 | $123.64 | $121.00 | -113.52 | $115.81 | -223.17 | -336.69 | +144.05 | -79.12 |
| 2026-08-24 | `WMT` | 48 | $103.70 | $104.14 | +21.12 | $106.49 | +112.80 | +133.92 | -107.52 | +5.28 |
| 2026-08-24 | `HITI` | 10 | $2.45 | $2.45 | +0.00 | $2.46 | +0.10 | +0.10 | +0.20 | +0.30 |
| 2026-08-25 | `FUTU` | 43 | $115.81 | $118.00 | +94.17 | — | +0.00 | +94.17 | +15.05 | — |
| 2026-08-25 | `WMT` | 48 | $106.49 | $105.58 | -43.68 | — | +0.00 | -43.68 | -38.40 | — |
| 2026-08-25 | `HITI` | 10 | $2.46 | $2.48 | +0.20 | $2.57 | +0.90 | +1.10 | +0.50 | +1.40 |
| 2026-08-25 | `ZYME` | 117 | — | $28.86 | +0.00 | $27.47 | -162.63 | -162.63 | +0.00 | -162.63 |
| 2026-08-25 | `RHI` | 77 | — | $43.76 | +0.00 | $44.90 | +87.78 | +87.78 | +0.00 | +87.78 |
| 2026-08-25 | `ABUS` | 645 | — | $5.25 | +0.00 | $5.20 | -32.25 | -32.25 | +0.00 | -32.25 |
| 2026-08-26 | `HITI` | 10 | $2.57 | $2.57 | +0.00 | — | +0.00 | +0.00 | +1.40 | — |
| 2026-08-26 | `ZYME` | 117 | $27.47 | $27.56 | +10.53 | $29.30 | +204.17 | +214.70 | -152.10 | +52.07 |
| 2026-08-26 | `RHI` | 77 | $44.90 | $44.33 | -43.89 | $44.54 | +16.17 | -27.72 | +43.89 | +60.06 |
| 2026-08-26 | `ABUS` | 645 | $5.20 | $5.19 | -6.45 | $5.19 | +0.00 | -6.45 | -38.70 | -38.70 |
| 2026-08-27 | `ZYME` | 117 | $29.30 | $29.33 | +2.92 | $29.01 | -37.44 | -34.52 | +54.99 | +17.55 |
| 2026-08-27 | `RHI` | 77 | $44.54 | $44.41 | -10.01 | $44.97 | +43.12 | +33.11 | +50.05 | +93.17 |
| 2026-08-27 | `ABUS` | 645 | $5.19 | $5.16 | -19.35 | $5.17 | +6.45 | -12.90 | -58.05 | -51.60 |
| 2026-08-28 | `ZYME` | 117 | $29.01 | $28.91 | -11.70 | — | +0.00 | -11.70 | +5.85 | — |
| 2026-08-28 | `RHI` | 77 | $44.97 | $44.51 | -35.42 | — | +0.00 | -35.42 | +57.75 | — |
| 2026-08-28 | `ABUS` | 645 | $5.17 | $5.15 | -12.90 | — | +0.00 | -12.90 | -64.50 | — |
| 2026-08-28 | `JKS` | 253 | — | $13.37 | +0.00 | $13.54 | +43.01 | +43.01 | +0.00 | +43.01 |
| 2026-08-28 | `DY` | 11 | — | $306.34 | +0.00 | $294.34 | -132.00 | -132.00 | +0.00 | -132.00 |
| 2026-08-28 | `ULTA` | 6 | — | $542.00 | +0.00 | $517.50 | -147.00 | -147.00 | +0.00 | -147.00 |
| 2026-08-31 | `JKS` | 253 | $13.54 | $13.54 | +0.00 | $12.58 | -242.88 | -242.88 | +43.01 | -199.87 |
| 2026-08-31 | `DY` | 11 | $294.34 | $298.01 | +40.37 | $291.21 | -74.80 | -34.43 | -91.63 | -166.43 |
| 2026-08-31 | `ULTA` | 6 | $517.50 | $521.10 | +21.60 | $537.10 | +96.00 | +117.60 | -125.40 | -29.40 |
| 2026-09-01 | `JKS` | 253 | $12.58 | $12.50 | -20.24 | $12.45 | -12.65 | -32.89 | -220.11 | -232.76 |
| 2026-09-01 | `DY` | 11 | $291.21 | $289.16 | -22.55 | $287.30 | -20.46 | -43.01 | -188.98 | -209.44 |
| 2026-09-01 | `ULTA` | 6 | $537.10 | $527.84 | -55.56 | $545.66 | +106.92 | +51.36 | -84.96 | +21.96 |
| 2026-09-02 | `JKS` | 253 | $12.45 | $12.45 | +0.00 | — | +0.00 | +0.00 | -232.76 | — |
| 2026-09-02 | `DY` | 11 | $287.30 | $287.99 | +7.59 | — | +0.00 | +7.59 | -201.85 | — |
| 2026-09-02 | `ULTA` | 6 | $545.66 | $545.68 | +0.12 | — | +0.00 | +0.12 | +22.08 | — |
| 2026-09-03 | `VIR` | 140 | — | $11.54 | +0.00 | $11.45 | -12.60 | -12.60 | +0.00 | -12.60 |
| 2026-09-03 | `AVGO` | 4 | — | $351.74 | +0.00 | $357.16 | +21.68 | +21.68 | +0.00 | +21.68 |
| 2026-09-03 | `FIVE` | 6 | — | $257.00 | +0.00 | $239.96 | -102.24 | -102.24 | +0.00 | -102.24 |
| 2026-09-03 | `MOMO` | 295 | — | $5.50 | +0.00 | $5.10 | -118.00 | -118.00 | +0.00 | -118.00 |
| 2026-09-03 | `PVH` | 21 | — | $74.96 | +0.00 | $72.46 | -52.50 | -52.50 | +0.00 | -52.50 |
| 2026-09-03 | `VSXY` | 21 | — | $76.86 | +0.00 | $73.64 | -67.62 | -67.62 | +0.00 | -67.62 |
| 2026-09-04 | `VIR` | 140 | $11.45 | $11.31 | -19.60 | $11.38 | +10.50 | -9.10 | -32.20 | -21.70 |
| 2026-09-04 | `AVGO` | 4 | $357.16 | $359.70 | +10.16 | $357.90 | -7.20 | +2.96 | +31.84 | +24.64 |
| 2026-09-04 | `FIVE` | 6 | $239.96 | $238.88 | -6.48 | $252.20 | +79.92 | +73.44 | -108.72 | -28.80 |
| 2026-09-04 | `MOMO` | 295 | $5.10 | $5.13 | +8.85 | $5.37 | +70.80 | +79.65 | -109.15 | -38.35 |
| 2026-09-04 | `PVH` | 21 | $72.46 | $72.79 | +6.93 | $74.33 | +32.34 | +39.27 | -45.57 | -13.23 |
| 2026-09-04 | `VSXY` | 21 | $73.64 | $73.63 | -0.21 | $75.56 | +40.53 | +40.32 | -67.83 | -27.30 |
| 2026-09-04 | `WNC` | 3 | — | $14.17 | +0.00 | $14.31 | +0.42 | +0.42 | +0.00 | +0.42 |
| 2026-09-04 | `ADCT` | 38 | — | $1.30 | +0.00 | $1.36 | +2.28 | +2.28 | +0.00 | +2.28 |
| 2026-09-04 | `HAFN` | 5 | — | $8.94 | +0.00 | $9.22 | +1.40 | +1.40 | +0.00 | +1.40 |
| 2026-09-04 | `XP` | 2 | — | $19.67 | +0.00 | $19.86 | +0.38 | +0.38 | +0.00 | +0.38 |
| 2026-09-08 | `VIR` | 140 | $11.38 | $11.22 | -23.10 | $11.18 | -5.60 | -28.70 | -44.80 | -50.40 |
| 2026-09-08 | `AVGO` | 4 | $357.90 | $363.68 | +23.12 | $368.56 | +19.52 | +42.64 | +47.76 | +67.28 |
| 2026-09-08 | `FIVE` | 6 | $252.20 | $251.22 | -5.88 | $254.07 | +17.10 | +11.22 | -34.68 | -17.58 |
| 2026-09-08 | `MOMO` | 295 | $5.37 | $5.28 | -26.55 | $5.26 | -5.90 | -32.45 | -64.90 | -70.80 |
| 2026-09-08 | `PVH` | 21 | $74.33 | $74.50 | +3.57 | $71.26 | -68.04 | -64.47 | -9.66 | -77.70 |
| 2026-09-08 | `VSXY` | 21 | $75.56 | $73.51 | -43.05 | $78.47 | +104.16 | +61.11 | -70.35 | +33.81 |
| 2026-09-08 | `WNC` | 3 | $14.31 | $14.22 | -0.27 | $13.61 | -1.83 | -2.10 | +0.15 | -1.68 |
| 2026-09-08 | `ADCT` | 38 | $1.36 | $1.33 | -1.14 | $1.30 | -1.14 | -2.28 | +1.14 | +0.00 |
| 2026-09-08 | `HAFN` | 5 | $9.22 | $8.81 | -2.05 | $8.96 | +0.75 | -1.30 | -0.65 | +0.10 |
| 2026-09-08 | `XP` | 2 | $19.86 | $20.46 | +1.20 | $19.99 | -0.94 | +0.26 | +1.58 | +0.64 |
| 2026-09-09 | `VIR` | 140 | $11.18 | $11.04 | -19.60 | — | +0.00 | -19.60 | -70.00 | — |
| 2026-09-09 | `AVGO` | 4 | $368.56 | $366.23 | -9.32 | — | +0.00 | -9.32 | +57.96 | — |
| 2026-09-09 | `FIVE` | 6 | $254.07 | $252.92 | -6.90 | — | +0.00 | -6.90 | -24.48 | — |
| 2026-09-09 | `MOMO` | 295 | $5.26 | $5.26 | +0.00 | — | +0.00 | +0.00 | -70.80 | — |
| 2026-09-09 | `PVH` | 21 | $71.26 | $70.83 | -9.03 | — | +0.00 | -9.03 | -86.73 | — |
| 2026-09-09 | `VSXY` | 21 | $78.47 | $77.16 | -27.51 | — | +0.00 | -27.51 | +6.30 | — |
| 2026-09-09 | `WNC` | 3 | $13.61 | $13.46 | -0.45 | $13.00 | -1.38 | -1.83 | -2.13 | -3.51 |
| 2026-09-09 | `ADCT` | 38 | $1.30 | $1.28 | -0.76 | $1.22 | -2.28 | -3.04 | -0.76 | -3.04 |
| 2026-09-09 | `HAFN` | 5 | $8.96 | $9.00 | +0.20 | $9.16 | +0.80 | +1.00 | +0.30 | +1.10 |
| 2026-09-09 | `XP` | 2 | $19.99 | $19.81 | -0.36 | $19.04 | -1.54 | -1.90 | +0.28 | -1.26 |
| 2026-09-10 | `WNC` | 3 | $13.00 | $12.79 | -0.63 | — | +0.00 | -0.63 | -4.14 | — |
| 2026-09-10 | `ADCT` | 38 | $1.22 | $1.21 | -0.38 | — | +0.00 | -0.38 | -3.42 | — |
| 2026-09-10 | `HAFN` | 5 | $9.16 | $9.08 | -0.40 | — | +0.00 | -0.40 | +0.70 | — |
| 2026-09-10 | `XP` | 2 | $19.04 | $18.86 | -0.36 | — | +0.00 | -0.36 | -1.62 | — |
| 2026-09-11 | `SSL` | 661 | — | $14.35 | +0.00 | $14.59 | +158.64 | +158.64 | +0.00 | +158.64 |
| 2026-09-14 | `SSL` | 661 | $14.59 | $14.69 | +66.10 | $14.50 | -125.59 | -59.49 | +224.74 | +99.15 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +787.37 | TPG | — | $24.65 | $10,784.79 | TPG×197 |
| 2026-08-14 | +5.50 | $24.65 | TPG×197 | $10,916.78 | +131.99 | -443.50 | KULR, NPWR, RLX | — | $9.24 | $10,473.10 | TPG×197, KULR×2, NPWR×3, RLX×3 |
| 2026-08-17 | +2.25 | $9.24 | TPG×197, KULR×2, NPWR×3, RLX×3 | $10,402.01 | -71.09 | -178.04 | — | — | $9.24 | $10,223.97 | TPG×197, KULR×2, NPWR×3, RLX×3 |
| 2026-08-18 | -6.20 | $9.24 | TPG×197, KULR×2, NPWR×3, RLX×3 | $10,223.67 | -0.30 | -0.16 | — | TPG | $10,205.24 | $10,220.82 | KULR×2, NPWR×3, RLX×3 |
| 2026-08-19 | -7.20 | $10,205.24 | KULR×2, NPWR×3, RLX×3 | $10,220.96 | +0.14 | +0.00 | — | KULR, NPWR, RLX | $10,220.72 | $10,220.72 | — |
| 2026-08-20 | +1.12 | $10,220.72 | — | $10,220.72 | -0.00 | -333.48 | FUTU, WMT | — | $51.27 | $9,882.98 | FUTU×43, WMT×48 |
| 2026-08-21 | +3.25 | $51.27 | FUTU×43, WMT×48 | $9,981.13 | +98.15 | +364.46 | HITI | — | $26.70 | $10,345.32 | FUTU×43, WMT×48, HITI×10 |
| 2026-08-24 | -5.17 | $26.70 | FUTU×43, WMT×48, HITI×10 | $10,252.92 | -92.40 | -110.27 | — | — | $26.70 | $10,142.65 | FUTU×43, WMT×48, HITI×10 |
| 2026-08-25 | +1.80 | $26.70 | FUTU×43, WMT×48, HITI×10 | $10,193.34 | +50.69 | -106.20 | ZYME, RHI, ABUS | FUTU, WMT | $18.91 | $10,069.90 | HITI×10, ZYME×117, RHI×77, ABUS×645 |
| 2026-08-26 | +2.02 | $18.91 | HITI×10, ZYME×117, RHI×77, ABUS×645 | $10,030.09 | -39.81 | +220.34 | — | HITI | $44.31 | $10,250.12 | ZYME×117, RHI×77, ABUS×645 |
| 2026-08-27 | — | $44.31 | ZYME×117, RHI×77, ABUS×645 | $10,223.69 | -26.43 | +12.13 | — | — | $44.31 | $10,235.82 | ZYME×117, RHI×77, ABUS×645 |
| 2026-08-28 | +0.75 | $44.31 | ZYME×117, RHI×77, ABUS×645 | $10,175.80 | -60.02 | -235.99 | JKS, DY, ULTA | ZYME, RHI, ABUS | $151.05 | $9,919.41 | JKS×253, DY×11, ULTA×6 |
| 2026-08-31 | -5.85 | $151.05 | JKS×253, DY×11, ULTA×6 | $9,981.38 | +61.97 | -221.68 | — | — | $151.05 | $9,759.70 | JKS×253, DY×11, ULTA×6 |
| 2026-09-01 | -6.30 | $151.05 | JKS×253, DY×11, ULTA×6 | $9,661.35 | -98.35 | +73.81 | — | — | $151.05 | $9,735.16 | JKS×253, DY×11, ULTA×6 |
| 2026-09-02 | -3.83 | $151.05 | JKS×253, DY×11, ULTA×6 | $9,742.87 | +7.71 | +0.00 | — | JKS, DY, ULTA | $9,735.44 | $9,735.44 | — |
| 2026-09-03 | -0.90 | $9,735.44 | — | $9,735.44 | -0.00 | -331.28 | VIR, AVGO, FIVE, MOMO, PVH, VSXY | — | $345.82 | $9,389.82 | VIR×140, AVGO×4, FIVE×6, MOMO×295, PVH×21, VSXY×21 |
| 2026-09-04 | +2.25 | $345.82 | VIR×140, AVGO×4, FIVE×6, MOMO×295, PVH×21, VSXY×21 | $9,389.47 | -0.35 | +231.37 | WNC, ADCT, HAFN, XP | — | $167.97 | $9,618.94 | VIR×140, AVGO×4, FIVE×6, MOMO×295, PVH×21, VSXY×21, WNC×3, ADCT×38, HAFN×5, XP×2 |
| 2026-09-08 | -11.47 | $167.97 | VIR×140, AVGO×4, FIVE×6, MOMO×295, PVH×21, VSXY×21, WNC×3, ADCT×38, HAFN×5, XP×2 | $9,544.79 | -74.15 | +58.08 | — | — | $167.97 | $9,602.87 | VIR×140, AVGO×4, FIVE×6, MOMO×295, PVH×21, VSXY×21, WNC×3, ADCT×38, HAFN×5, XP×2 |
| 2026-09-09 | -13.95 | $167.97 | VIR×140, AVGO×4, FIVE×6, MOMO×295, PVH×21, VSXY×21, WNC×3, ADCT×38, HAFN×5, XP×2 | $9,529.14 | -73.73 | -4.40 | — | VIR, AVGO, FIVE, MOMO, PVH, VSXY | $9,340.98 | $9,510.22 | WNC×3, ADCT×38, HAFN×5, XP×2 |
| 2026-09-10 | -13.28 | $9,340.98 | WNC×3, ADCT×38, HAFN×5, XP×2 | $9,508.45 | -1.77 | +0.00 | — | WNC, ADCT, HAFN, XP | $9,506.55 | $9,506.55 | — |
| 2026-09-11 | +0.50 | $9,506.55 | — | $9,506.55 | +0.00 | +158.64 | SSL | — | $12.68 | $9,656.67 | SSL×661 |
| 2026-09-14 | -11.00 | $12.68 | SSL×661 | $9,722.77 | +66.10 | -125.59 | — | — | $12.68 | $9,597.18 | SSL×661 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 197 | $50.62 | $2.58 | — | $24.65 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list flatten; ⚪; ret5=+6.2; leftover $10000.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.65 | ▲ close $10,784.79 vs 09:30 $10,000.00 (session +787.37) | 16:00 close · cash $24.65 · equity $10,784.79 vs 09:30 $10,000.00 (+784.79; session marks +787.37) · 1 name(s) marked open→close (per-name table). TPG×197 09:30 $50.62 → close $54.62 +787.37 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.65 | ▲ 09:30 equity $10,916.78 vs yday $10,784.79 (+131.99) | 09:30 open · cash $24.65 (unchanged overnight, no fees) · equity $10,916.78 vs prior close $10,784.79 (+131.99) · 1 name(s) re-marked at the open (per-name table). TPG×197 yday $54.62 → 09:30 $55.29 +131.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `KULR` | 2 | $2.50 | $0.06 | — | $19.59 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ⚪; ret5=+7.6; leftover $6.16 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟡 buy🟡 |
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
| 2026-08-20 09:30 ET | **BUY** | `FUTU` | 43 | $117.65 | $2.12 | — | $5,159.65 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+4.1; leftover $5110.36 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WMT` | 48 | $106.38 | $2.13 | — | $51.27 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-1.7; leftover $5110.36 | join🟢 sector🟡 gen🟢 news🔴 digest🟡 judge🔴 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $51.27 | ▼ close $9,882.98 vs 09:30 $10,220.72 (session -333.48) | 16:00 close · cash $51.27 · equity $9,882.98 vs 09:30 $10,220.72 (-337.74; session marks -333.48) · 2 name(s) marked open→close (per-name table). FUTU×43 09:30 $117.65 → close $112.73 -211.56; WMT×48 09:30 $106.38 → close $103.84 -121.92 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.27 | ▲ 09:30 equity $9,981.13 vs yday $9,882.98 (+98.15) | 09:30 open · cash $51.27 (unchanged overnight, no fees) · equity $9,981.13 vs prior close $9,882.98 (+98.15) · 2 name(s) re-marked at the open (per-name table). FUTU×43 yday $112.73 → 09:30 $115.18 +105.35; WMT×48 yday $103.84 → 09:30 $103.69 -7.20 | — |
| 2026-08-21 09:30 ET | **BUY** | `HITI` | 10 | $2.43 | $0.27 | — | $26.70 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list mover_buy; 🔵; ⚪; ret5=+5.6; leftover $25.64 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26.70 | ▲ close $10,345.32 vs 09:30 $9,981.13 (session +364.46) | 16:00 close · cash $26.70 · equity $10,345.32 vs 09:30 $9,981.13 (+364.19; session marks +364.46) · 3 name(s) marked open→close (per-name table). FUTU×43 09:30 $115.18 → close $123.64 +363.78; WMT×48 09:30 $103.69 → close $103.70 +0.48; HITI×10 09:30 $2.43 → close $2.45 +0.20 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26.70 | ▼ 09:30 equity $10,252.92 vs yday $10,345.32 (-92.40) | 09:30 open · cash $26.70 (unchanged overnight, no fees) · equity $10,252.92 vs prior close $10,345.32 (-92.40) · 3 name(s) re-marked at the open (per-name table). FUTU×43 yday $123.64 → 09:30 $121.00 -113.52; WMT×48 yday $103.70 → 09:30 $104.14 +21.12; HITI×10 yday $2.45 → 09:30 $2.45 +0.00 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26.70 | ▼ close $10,142.65 vs 09:30 $10,252.92 (session -110.27) | 16:00 close · cash $26.70 · equity $10,142.65 vs 09:30 $10,252.92 (-110.27; session marks -110.27) · 3 name(s) marked open→close (per-name table). FUTU×43 09:30 $121.00 → close $115.81 -223.17; WMT×48 09:30 $104.14 → close $106.49 +112.80; HITI×10 09:30 $2.45 → close $2.46 +0.10 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26.70 | ▲ 09:30 equity $10,193.34 vs yday $10,142.65 (+50.69) | 09:30 open · cash $26.70 (unchanged overnight, no fees) · equity $10,193.34 vs prior close $10,142.65 (+50.69) · 3 name(s) re-marked at the open (per-name table). FUTU×43 yday $115.81 → 09:30 $118.00 +94.17; WMT×48 yday $106.49 → 09:30 $105.58 -43.68; HITI×10 yday $2.46 → 09:30 $2.48 +0.20 | — |
| 2026-08-25 09:30 ET | **SELL** | `FUTU` | 43 | $118.00 | $2.17 | $+10.76 | $5,098.53 | ▲ +10.76 after sell → book $10,191.17; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WMT` | 48 | $105.58 | $2.18 | $-42.72 | $10,164.19 | ▼ -42.72 after sell → book $10,188.99; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 117 | $28.86 | $2.34 | — | $6,785.22 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; 🔵; ret5=+13.7; leftover $3388.06 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 77 | $43.76 | $2.22 | — | $3,413.48 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list mover_buy; 🔵; ⚪; ret5=+6.2; leftover $3388.06 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ABUS` | 645 | $5.25 | $8.32 | — | $18.91 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list mover_buy; 🔵; ⚪; ret5=+10.4; leftover $3388.06 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18.91 | ▼ close $10,069.90 vs 09:30 $10,193.34 (session -106.20) | 16:00 close · cash $18.91 · equity $10,069.90 vs 09:30 $10,193.34 (-123.44; session marks -106.20) · 4 name(s) marked open→close (per-name table). HITI×10 09:30 $2.48 → close $2.57 +0.90; ZYME×117 09:30 $28.86 → close $27.47 -162.63; RHI×77 09:30 $43.76 → close $44.90 +87.78; ABUS×645 09:30 $5.25 → close $5.20 -32.25 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18.91 | ▼ 09:30 equity $10,030.09 vs yday $10,069.90 (-39.81) | 09:30 open · cash $18.91 (unchanged overnight, no fees) · equity $10,030.09 vs prior close $10,069.90 (-39.81) · 4 name(s) re-marked at the open (per-name table). HITI×10 yday $2.57 → 09:30 $2.57 +0.00; ZYME×117 yday $27.47 → 09:30 $27.56 +10.53; RHI×77 yday $44.90 → 09:30 $44.33 -43.89; ABUS×645 yday $5.20 → 09:30 $5.19 -6.45 | — |
| 2026-08-26 09:30 ET | **SELL** | `HITI` | 10 | $2.57 | $0.31 | $+0.82 | $44.31 | ▲ +0.82 after sell → book $10,029.79; vs 09:30 mark -0.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $44.31 | ▲ close $10,250.12 vs 09:30 $10,030.09 (session +220.34) | 16:00 close · cash $44.31 · equity $10,250.12 vs 09:30 $10,030.09 (+220.03; session marks +220.34) · 3 name(s) marked open→close (per-name table). ZYME×117 09:30 $27.56 → close $29.30 +204.17; RHI×77 09:30 $44.33 → close $44.54 +16.17; ABUS×645 09:30 $5.19 → close $5.19 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $44.31 | ▼ 09:30 equity $10,223.69 vs yday $10,250.12 (-26.43) | 09:30 open · cash $44.31 (unchanged overnight, no fees) · equity $10,223.69 vs prior close $10,250.12 (-26.43) · 3 name(s) re-marked at the open (per-name table). ZYME×117 yday $29.30 → 09:30 $29.33 +2.92; RHI×77 yday $44.54 → 09:30 $44.41 -10.01; ABUS×645 yday $5.19 → 09:30 $5.16 -19.35 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $44.31 | ▲ close $10,235.82 vs 09:30 $10,223.69 (session +12.13) | 16:00 close · cash $44.31 · equity $10,235.82 vs 09:30 $10,223.69 (+12.13; session marks +12.13) · 3 name(s) marked open→close (per-name table). ZYME×117 09:30 $29.33 → close $29.01 -37.44; RHI×77 09:30 $44.41 → close $44.97 +43.12; ABUS×645 09:30 $5.16 → close $5.17 +6.45 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $44.31 | ▼ 09:30 equity $10,175.80 vs yday $10,235.82 (-60.02) | 09:30 open · cash $44.31 (unchanged overnight, no fees) · equity $10,175.80 vs prior close $10,235.82 (-60.02) · 3 name(s) re-marked at the open (per-name table). ZYME×117 yday $29.01 → 09:30 $28.91 -11.70; RHI×77 yday $44.97 → 09:30 $44.51 -35.42; ABUS×645 yday $5.17 → 09:30 $5.15 -12.90 | — |
| 2026-08-28 09:30 ET | **SELL** | `ZYME` | 117 | $28.91 | $2.39 | $+1.12 | $3,424.39 | ▲ +1.12 after sell → book $10,173.41; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `RHI` | 77 | $44.51 | $2.26 | $+53.27 | $6,849.40 | ▲ +53.27 after sell → book $10,171.15; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ABUS` | 645 | $5.15 | $8.45 | $-81.27 | $10,162.69 | ▼ -81.27 after sell → book $10,162.69; vs 09:30 mark -8.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `JKS` | 253 | $13.37 | $3.26 | — | $6,776.82 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list yday_mover; ret5=-14.9; leftover $3387.56 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DY` | 11 | $306.34 | $2.02 | — | $3,405.06 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list yday_mover; ret5=-23.0; leftover $3387.56 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ULTA` | 6 | $542.00 | $2.01 | — | $151.05 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; ret5=+4.8; leftover $3387.56 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $151.05 | ▼ close $9,919.41 vs 09:30 $10,175.80 (session -235.99) | 16:00 close · cash $151.05 · equity $9,919.41 vs 09:30 $10,175.80 (-256.39; session marks -235.99) · 3 name(s) marked open→close (per-name table). JKS×253 09:30 $13.37 → close $13.54 +43.01; DY×11 09:30 $306.34 → close $294.34 -132.00; ULTA×6 09:30 $542.00 → close $517.50 -147.00 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $151.05 | ▲ 09:30 equity $9,981.38 vs yday $9,919.41 (+61.97) | 09:30 open · cash $151.05 (unchanged overnight, no fees) · equity $9,981.38 vs prior close $9,919.41 (+61.97) · 3 name(s) re-marked at the open (per-name table). JKS×253 yday $13.54 → 09:30 $13.54 +0.00; DY×11 yday $294.34 → 09:30 $298.01 +40.37; ULTA×6 yday $517.50 → 09:30 $521.10 +21.60 | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $151.05 | ▼ close $9,759.70 vs 09:30 $9,981.38 (session -221.68) | 16:00 close · cash $151.05 · equity $9,759.70 vs 09:30 $9,981.38 (-221.68; session marks -221.68) · 3 name(s) marked open→close (per-name table). JKS×253 09:30 $13.54 → close $12.58 -242.88; DY×11 09:30 $298.01 → close $291.21 -74.80; ULTA×6 09:30 $521.10 → close $537.10 +96.00 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $151.05 | ▼ 09:30 equity $9,661.35 vs yday $9,759.70 (-98.35) | 09:30 open · cash $151.05 (unchanged overnight, no fees) · equity $9,661.35 vs prior close $9,759.70 (-98.35) · 3 name(s) re-marked at the open (per-name table). JKS×253 yday $12.58 → 09:30 $12.50 -20.24; DY×11 yday $291.21 → 09:30 $289.16 -22.55; ULTA×6 yday $537.10 → 09:30 $527.84 -55.56 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $151.05 | ▲ close $9,735.16 vs 09:30 $9,661.35 (session +73.81) | 16:00 close · cash $151.05 · equity $9,735.16 vs 09:30 $9,661.35 (+73.81; session marks +73.81) · 3 name(s) marked open→close (per-name table). JKS×253 09:30 $12.50 → close $12.45 -12.65; DY×11 09:30 $289.16 → close $287.30 -20.46; ULTA×6 09:30 $527.84 → close $545.66 +106.92 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $151.05 | ▲ 09:30 equity $9,742.87 vs yday $9,735.16 (+7.71) | 09:30 open · cash $151.05 (unchanged overnight, no fees) · equity $9,742.87 vs prior close $9,735.16 (+7.71) · 3 name(s) re-marked at the open (per-name table). JKS×253 yday $12.45 → 09:30 $12.45 +0.00; DY×11 yday $287.30 → 09:30 $287.99 +7.59; ULTA×6 yday $545.66 → 09:30 $545.68 +0.12 | — |
| 2026-09-02 09:30 ET | **SELL** | `JKS` | 253 | $12.45 | $3.33 | $-239.35 | $3,297.57 | ▼ -239.35 after sell → book $9,739.54; vs 09:30 mark -3.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `DY` | 11 | $287.99 | $2.06 | $-205.93 | $6,463.40 | ▼ -205.93 after sell → book $9,737.48; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ULTA` | 6 | $545.68 | $2.04 | $+18.03 | $9,735.44 | ▲ +18.03 after sell → book $9,735.44; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,735.44 | ▲ close $9,735.44 vs 09:30 $9,742.87 (session +0.00) | 16:00 close · cash $9,735.44 · no lots left · equity $9,735.44. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,735.44 | ▲ 09:30 equity $9,735.44 vs yday $9,735.44 (-0.00) | 09:30 open · cash $9,735.44 · no holdings · equity $9,735.44 vs prior close $9,735.44 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `VIR` | 140 | $11.54 | $2.41 | — | $8,117.43 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list yday_gainer; 🔵; ⚪; ret5=+3.8; leftover $1622.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 4 | $351.74 | $2.00 | — | $6,708.46 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+3.3; leftover $1622.57 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 6 | $257.00 | $2.01 | — | $5,164.46 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-5.5; leftover $1622.57 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MOMO` | 295 | $5.50 | $3.81 | — | $3,538.15 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-4.8; leftover $1622.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PVH` | 21 | $74.96 | $2.05 | — | $1,961.94 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-7.9; leftover $1622.57 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSXY` | 21 | $76.86 | $2.05 | — | $345.82 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-6.6; leftover $1622.57 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $345.82 | ▼ close $9,389.82 vs 09:30 $9,735.44 (session -331.28) | 16:00 close · cash $345.82 · equity $9,389.82 vs 09:30 $9,735.44 (-345.62; session marks -331.28) · 6 name(s) marked open→close (per-name table). VIR×140 09:30 $11.54 → close $11.45 -12.60; AVGO×4 09:30 $351.74 → close $357.16 +21.68; FIVE×6 09:30 $257.00 → close $239.96 -102.24; MOMO×295 09:30 $5.50 → close $5.10 -118.00; PVH×21 09:30 $74.96 → close $72.46 -52.50; VSXY×21 09:30 $76.86 → close $73.64 -67.62 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $345.82 | ▼ 09:30 equity $9,389.47 vs yday $9,389.82 (-0.35) | 09:30 open · cash $345.82 (unchanged overnight, no fees) · equity $9,389.47 vs prior close $9,389.82 (-0.35) · 6 name(s) re-marked at the open (per-name table). VIR×140 yday $11.45 → 09:30 $11.31 -19.60; AVGO×4 yday $357.16 → 09:30 $359.70 +10.16; FIVE×6 yday $239.96 → 09:30 $238.88 -6.48; MOMO×295 yday $5.10 → 09:30 $5.13 +8.85; PVH×21 yday $72.46 → 09:30 $72.79 +6.93; VSXY×21 yday $73.64 → 09:30 $73.63 -0.21 | — |
| 2026-09-04 09:30 ET | **BUY** | `WNC` | 3 | $14.17 | $0.43 | — | $302.88 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list yday_gainer; 🔵; ret5=+7.9; leftover $49.40 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ADCT` | 38 | $1.30 | $0.61 | — | $252.87 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; 🔵; ⚪; ret5=+17.9; leftover $49.40 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `HAFN` | 5 | $8.94 | $0.46 | — | $207.71 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; ret5=+7.7; leftover $49.40 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `XP` | 2 | $19.67 | $0.40 | — | $167.97 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list mover_buy; 🔵; ⚪; ret5=+13.1; leftover $49.40 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $167.97 | ▲ close $9,618.94 vs 09:30 $9,389.47 (session +231.37) | 16:00 close · cash $167.97 · equity $9,618.94 vs 09:30 $9,389.47 (+229.47; session marks +231.37) · 10 name(s) marked open→close (per-name table). VIR×140 09:30 $11.31 → close $11.38 +10.50; AVGO×4 09:30 $359.70 → close $357.90 -7.20; FIVE×6 09:30 $238.88 → close $252.20 +79.92; MOMO×295 09:30 $5.13 → close $5.37 +70.80; PVH×21 09:30 $72.79 → close $74.33 +32.34; VSXY×21 09:30 $73.63 → close $75.56 +40.53; WNC×3 09:30 $14.17 → close $14.31 +0.42; ADCT×38 09:30 $1.30 → close $1.36 +2.28; HAFN×5 09:30 $8.94 → close $9.22 +1.40; XP×2 09:30 $19.67 → close $19.86 +0.38 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $167.97 | ▼ 09:30 equity $9,544.79 vs yday $9,618.94 (-74.15) | 09:30 open · cash $167.97 (unchanged overnight, no fees) · equity $9,544.79 vs prior close $9,618.94 (-74.15) · 10 name(s) re-marked at the open (per-name table). VIR×140 yday $11.38 → 09:30 $11.22 -23.10; AVGO×4 yday $357.90 → 09:30 $363.68 +23.12; FIVE×6 yday $252.20 → 09:30 $251.22 -5.88; MOMO×295 yday $5.37 → 09:30 $5.28 -26.55; PVH×21 yday $74.33 → 09:30 $74.50 +3.57; VSXY×21 yday $75.56 → 09:30 $73.51 -43.05; WNC×3 yday $14.31 → 09:30 $14.22 -0.27; ADCT×38 yday $1.36 → 09:30 $1.33 -1.14; HAFN×5 yday $9.22 → 09:30 $8.81 -2.05; XP×2 yday $19.86 → 09:30 $20.46 +1.20 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $167.97 | ▲ close $9,602.87 vs 09:30 $9,544.79 (session +58.08) | 16:00 close · cash $167.97 · equity $9,602.87 vs 09:30 $9,544.79 (+58.08; session marks +58.08) · 10 name(s) marked open→close (per-name table). VIR×140 09:30 $11.22 → close $11.18 -5.60; AVGO×4 09:30 $363.68 → close $368.56 +19.52; FIVE×6 09:30 $251.22 → close $254.07 +17.10; MOMO×295 09:30 $5.28 → close $5.26 -5.90; PVH×21 09:30 $74.50 → close $71.26 -68.04; VSXY×21 09:30 $73.51 → close $78.47 +104.16; WNC×3 09:30 $14.22 → close $13.61 -1.83; ADCT×38 09:30 $1.33 → close $1.30 -1.14; HAFN×5 09:30 $8.81 → close $8.96 +0.75; XP×2 09:30 $20.46 → close $19.99 -0.94 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $167.97 | ▼ 09:30 equity $9,529.14 vs yday $9,602.87 (-73.73) | 09:30 open · cash $167.97 (unchanged overnight, no fees) · equity $9,529.14 vs prior close $9,602.87 (-73.73) · 10 name(s) re-marked at the open (per-name table). VIR×140 yday $11.18 → 09:30 $11.04 -19.60; AVGO×4 yday $368.56 → 09:30 $366.23 -9.32; FIVE×6 yday $254.07 → 09:30 $252.92 -6.90; MOMO×295 yday $5.26 → 09:30 $5.26 +0.00; PVH×21 yday $71.26 → 09:30 $70.83 -9.03; VSXY×21 yday $78.47 → 09:30 $77.16 -27.51; WNC×3 yday $13.61 → 09:30 $13.46 -0.45; ADCT×38 yday $1.30 → 09:30 $1.28 -0.76; HAFN×5 yday $8.96 → 09:30 $9.00 +0.20; XP×2 yday $19.99 → 09:30 $19.81 -0.36 | — |
| 2026-09-09 09:30 ET | **SELL** | `VIR` | 140 | $11.04 | $2.45 | $-74.86 | $1,711.12 | ▼ -74.86 after sell → book $9,526.69; vs 09:30 mark -2.45 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 4 | $366.23 | $2.02 | $+53.93 | $3,174.02 | ▲ +53.93 after sell → book $9,524.67; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FIVE` | 6 | $252.92 | $2.03 | $-28.52 | $4,689.51 | ▼ -28.52 after sell → book $9,522.64; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MOMO` | 295 | $5.26 | $3.87 | $-78.47 | $6,237.34 | ▼ -78.47 after sell → book $9,518.77; vs 09:30 mark -3.87 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `PVH` | 21 | $70.83 | $2.07 | $-90.86 | $7,722.70 | ▼ -90.86 after sell → book $9,516.70; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `VSXY` | 21 | $77.16 | $2.08 | $+2.17 | $9,340.98 | ▲ +2.17 after sell → book $9,514.62; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,340.98 | ▼ close $9,510.22 vs 09:30 $9,529.14 (session -4.40) | 16:00 close · cash $9,340.98 · equity $9,510.22 vs 09:30 $9,529.14 (-18.92; session marks -4.40) · 4 name(s) marked open→close (per-name table). WNC×3 09:30 $13.46 → close $13.00 -1.38; ADCT×38 09:30 $1.28 → close $1.22 -2.28; HAFN×5 09:30 $9.00 → close $9.16 +0.80; XP×2 09:30 $19.81 → close $19.04 -1.54 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,340.98 | ▼ 09:30 equity $9,508.45 vs yday $9,510.22 (-1.77) | 09:30 open · cash $9,340.98 (unchanged overnight, no fees) · equity $9,508.45 vs prior close $9,510.22 (-1.77) · 4 name(s) re-marked at the open (per-name table). WNC×3 yday $13.00 → 09:30 $12.79 -0.63; ADCT×38 yday $1.22 → 09:30 $1.21 -0.38; HAFN×5 yday $9.16 → 09:30 $9.08 -0.40; XP×2 yday $19.04 → 09:30 $18.86 -0.36 | — |
| 2026-09-10 09:30 ET | **SELL** | `WNC` | 3 | $12.79 | $0.41 | $-4.99 | $9,378.94 | ▼ -4.99 after sell → book $9,508.04; vs 09:30 mark -0.41 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `ADCT` | 38 | $1.21 | $0.59 | $-4.62 | $9,424.33 | ▼ -4.62 after sell → book $9,507.45; vs 09:30 mark -0.59 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `HAFN` | 5 | $9.08 | $0.49 | $-0.25 | $9,469.24 | ▼ -0.25 after sell → book $9,506.96; vs 09:30 mark -0.49 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `XP` | 2 | $18.86 | $0.40 | $-2.42 | $9,506.55 | ▼ -2.42 after sell → book $9,506.55; vs 09:30 mark -0.41 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,506.55 | ▲ close $9,506.55 vs 09:30 $9,508.45 (session +0.00) | 16:00 close · cash $9,506.55 · no lots left · equity $9,506.55. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,506.55 | ▲ 09:30 equity $9,506.55 vs yday $9,506.55 (+0.00) | 09:30 open · cash $9,506.55 · no holdings · equity $9,506.55 vs prior close $9,506.55 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `SSL` | 661 | $14.35 | $8.53 | — | $12.68 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; 🔵; ret5=+15.5; leftover $9506.55 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.68 | ▲ close $9,656.67 vs 09:30 $9,506.55 (session +158.64) | 16:00 close · cash $12.68 · equity $9,656.67 vs 09:30 $9,506.55 (+150.12; session marks +158.64) · 1 name(s) marked open→close (per-name table). SSL×661 09:30 $14.35 → close $14.59 +158.64 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.68 | ▲ 09:30 equity $9,722.77 vs yday $9,656.67 (+66.10) | 09:30 open · cash $12.68 (unchanged overnight, no fees) · equity $9,722.77 vs prior close $9,656.67 (+66.10) · 1 name(s) re-marked at the open (per-name table). SSL×661 yday $14.59 → 09:30 $14.69 +66.10 | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.68 | ▼ close $9,597.18 vs 09:30 $9,722.77 (session -125.59) | 16:00 close · cash $12.68 · equity $9,597.18 vs 09:30 $9,722.77 (-125.59; session marks -125.59) · 1 name(s) marked open→close (per-name table). SSL×661 09:30 $14.69 → close $14.50 -125.59 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SPHR` | cash | leftover split 6.16 < 1 share @ 176.68 |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `KULR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `RLX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `KULR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `RLX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AURA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CIG` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CRDL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WB` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `FUTU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WMT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BJ` | cash | leftover split 25.64 < 1 share @ 93.98 |
| 2026-08-24 | `FUTU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WMT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HITI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-25 | `HITI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `ZYME` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `RHI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ABUS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `NCNO` | cash | leftover split 14.77 < 1 share @ 19.33 |
| 2026-08-26 | `PLAB` | cash | leftover split 14.77 < 1 share @ 37.26 |
| 2026-08-26 | `SJM` | cash | leftover split 14.77 < 1 share @ 134.80 |
| 2026-08-27 | `ZYME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RHI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ABUS` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-04 | `ATRC` | cash | leftover split 49.40 < 1 share @ 52.03 |
| 2026-09-04 | `CRDO` | cash | leftover split 49.40 < 1 share @ 162.10 |
| 2026-09-04 | `DOCU` | cash | leftover split 49.40 < 1 share @ 68.52 |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `AVGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FIVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MOMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `PVH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `VSXY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `WNC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ADCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `HAFN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `XP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `WNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `ADCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `HAFN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `XP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LFMD` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NAMS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `JMKE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `ARLO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DBI` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `SSL` | min_hold | dropped but min-hold 1/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `SSL` | 661 | 2026-09-11 @ $14.35 | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; 🔵; ret5=+15.5; leftover $9506.55 |
