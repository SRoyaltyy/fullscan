# Factor mine action — `union_flow_in_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ flow_in, no 🚨

Cash book **+7.33%** ($10,734) · signal-only (no cash/fees) was +9.35%. Starts YES **9/27**. Fills 85 · skips 24 · realized $+775.14.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `flow_in=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $5.37.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `TPG` | 197 | — | $50.62 | +0.00 | $54.62 | +787.37 | +787.37 | +0.00 | +787.37 |
| 2026-08-14 | `TPG` | 197 | $54.62 | $55.29 | +131.99 | — | +0.00 | +131.99 | +919.36 | — |
| 2026-08-14 | `SPHR` | 15 | — | $176.68 | +0.00 | $168.00 | -130.20 | -130.20 | +0.00 | -130.20 |
| 2026-08-14 | `KULR` | 1091 | — | $2.50 | +0.00 | $2.64 | +152.74 | +152.74 | +0.00 | +152.74 |
| 2026-08-14 | `NPWR` | 1749 | — | $1.56 | +0.00 | $1.95 | +682.11 | +682.11 | +0.00 | +682.11 |
| 2026-08-14 | `RLX` | 1474 | — | $1.85 | +0.00 | $1.94 | +132.66 | +132.66 | +0.00 | +132.66 |
| 2026-08-17 | `SPHR` | 15 | $168.00 | $168.10 | +1.50 | — | +0.00 | +1.50 | -128.70 | — |
| 2026-08-17 | `KULR` | 1091 | $2.64 | $2.63 | -10.91 | — | +0.00 | -10.91 | +141.83 | — |
| 2026-08-17 | `NPWR` | 1749 | $1.95 | $1.92 | -52.47 | — | +0.00 | -52.47 | +629.64 | — |
| 2026-08-17 | `RLX` | 1474 | $1.94 | $1.92 | -29.48 | — | +0.00 | -29.48 | +103.18 | — |
| 2026-08-17 | `XP` | 724 | — | $15.93 | +0.00 | $15.70 | -166.52 | -166.52 | +0.00 | -166.52 |
| 2026-08-18 | `XP` | 724 | $15.70 | $15.70 | +0.00 | — | +0.00 | +0.00 | -166.52 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `FUTU` | 32 | — | $117.65 | +0.00 | $112.73 | -157.44 | -157.44 | +0.00 | -157.44 |
| 2026-08-20 | `WMT` | 35 | — | $106.38 | +0.00 | $103.84 | -88.90 | -88.90 | +0.00 | -88.90 |
| 2026-08-20 | `BJ` | 42 | — | $88.91 | +0.00 | $91.30 | +100.38 | +100.38 | +0.00 | +100.38 |
| 2026-08-21 | `FUTU` | 32 | $112.73 | $115.18 | +78.40 | — | +0.00 | +78.40 | -79.04 | — |
| 2026-08-21 | `WMT` | 35 | $103.84 | $103.69 | -5.25 | — | +0.00 | -5.25 | -94.15 | — |
| 2026-08-21 | `BJ` | 42 | $91.30 | $93.98 | +112.56 | $96.42 | +102.48 | +215.04 | +212.94 | +315.42 |
| 2026-08-21 | `HITI` | 3045 | — | $2.43 | +0.00 | $2.45 | +60.90 | +60.90 | +0.00 | +60.90 |
| 2026-08-24 | `BJ` | 42 | $96.42 | $97.02 | +25.20 | — | +0.00 | +25.20 | +340.62 | — |
| 2026-08-24 | `HITI` | 3045 | $2.45 | $2.45 | +0.00 | — | +0.00 | +0.00 | +60.90 | — |
| 2026-08-25 | `ZYME` | 132 | — | $28.86 | +0.00 | $27.47 | -183.48 | -183.48 | +0.00 | -183.48 |
| 2026-08-25 | `RHI` | 87 | — | $43.76 | +0.00 | $44.90 | +99.18 | +99.18 | +0.00 | +99.18 |
| 2026-08-25 | `ABUS` | 729 | — | $5.25 | +0.00 | $5.20 | -36.45 | -36.45 | +0.00 | -36.45 |
| 2026-08-26 | `ZYME` | 132 | $27.47 | $27.56 | +11.88 | — | +0.00 | +11.88 | -171.60 | — |
| 2026-08-26 | `RHI` | 87 | $44.90 | $44.33 | -49.59 | — | +0.00 | -49.59 | +49.59 | — |
| 2026-08-26 | `ABUS` | 729 | $5.20 | $5.19 | -7.29 | — | +0.00 | -7.29 | -43.74 | — |
| 2026-08-26 | `NCNO` | 146 | — | $19.33 | +0.00 | $21.51 | +318.28 | +318.28 | +0.00 | +318.28 |
| 2026-08-26 | `PLAB` | 75 | — | $37.26 | +0.00 | $30.25 | -525.75 | -525.75 | +0.00 | -525.75 |
| 2026-08-26 | `SJM` | 20 | — | $134.80 | +0.00 | $130.90 | -78.00 | -78.00 | +0.00 | -78.00 |
| 2026-08-26 | `URBN` | 35 | — | $78.90 | +0.00 | $82.95 | +141.75 | +141.75 | +0.00 | +141.75 |
| 2026-08-27 | `NCNO` | 146 | $21.51 | $22.03 | +75.92 | — | +0.00 | +75.92 | +394.20 | — |
| 2026-08-27 | `PLAB` | 75 | $30.25 | $30.12 | -9.75 | — | +0.00 | -9.75 | -535.50 | — |
| 2026-08-27 | `SJM` | 20 | $130.90 | $130.29 | -12.20 | — | +0.00 | -12.20 | -90.20 | — |
| 2026-08-27 | `URBN` | 35 | $82.95 | $82.70 | -8.75 | — | +0.00 | -8.75 | +133.00 | — |
| 2026-08-27 | `TRLV` | 491 | — | $11.38 | +0.00 | $11.03 | -171.85 | -171.85 | +0.00 | -171.85 |
| 2026-08-27 | `BOX` | 165 | — | $33.79 | +0.00 | $34.74 | +156.75 | +156.75 | +0.00 | +156.75 |
| 2026-08-28 | `TRLV` | 491 | $11.03 | $11.00 | -14.73 | — | +0.00 | -14.73 | -186.58 | — |
| 2026-08-28 | `BOX` | 165 | $34.74 | $34.75 | +1.65 | — | +0.00 | +1.65 | +158.40 | — |
| 2026-08-28 | `JKS` | 277 | — | $13.37 | +0.00 | $13.54 | +47.09 | +47.09 | +0.00 | +47.09 |
| 2026-08-28 | `DY` | 12 | — | $306.34 | +0.00 | $294.34 | -144.00 | -144.00 | +0.00 | -144.00 |
| 2026-08-28 | `ULTA` | 6 | — | $542.00 | +0.00 | $517.50 | -147.00 | -147.00 | +0.00 | -147.00 |
| 2026-08-31 | `JKS` | 277 | $13.54 | $13.54 | +0.00 | — | +0.00 | +0.00 | +47.09 | — |
| 2026-08-31 | `DY` | 12 | $294.34 | $298.01 | +44.04 | — | +0.00 | +44.04 | -99.96 | — |
| 2026-08-31 | `ULTA` | 6 | $517.50 | $521.10 | +21.60 | — | +0.00 | +21.60 | -125.40 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `VIR` | 158 | — | $11.54 | +0.00 | $11.45 | -14.22 | -14.22 | +0.00 | -14.22 |
| 2026-09-03 | `AVGO` | 5 | — | $351.74 | +0.00 | $357.16 | +27.10 | +27.10 | +0.00 | +27.10 |
| 2026-09-03 | `FIVE` | 7 | — | $257.00 | +0.00 | $239.96 | -119.28 | -119.28 | +0.00 | -119.28 |
| 2026-09-03 | `MOMO` | 331 | — | $5.50 | +0.00 | $5.10 | -132.40 | -132.40 | +0.00 | -132.40 |
| 2026-09-03 | `PVH` | 24 | — | $74.96 | +0.00 | $72.46 | -60.00 | -60.00 | +0.00 | -60.00 |
| 2026-09-03 | `VSXY` | 23 | — | $76.86 | +0.00 | $73.64 | -74.06 | -74.06 | +0.00 | -74.06 |
| 2026-09-04 | `VIR` | 158 | $11.45 | $11.31 | -22.12 | — | +0.00 | -22.12 | -36.34 | — |
| 2026-09-04 | `AVGO` | 5 | $357.16 | $359.70 | +12.70 | — | +0.00 | +12.70 | +39.80 | — |
| 2026-09-04 | `FIVE` | 7 | $239.96 | $238.88 | -7.56 | — | +0.00 | -7.56 | -126.84 | — |
| 2026-09-04 | `MOMO` | 331 | $5.10 | $5.13 | +9.93 | — | +0.00 | +9.93 | -122.47 | — |
| 2026-09-04 | `PVH` | 24 | $72.46 | $72.79 | +7.92 | — | +0.00 | +7.92 | -52.08 | — |
| 2026-09-04 | `VSXY` | 23 | $73.64 | $73.63 | -0.23 | — | +0.00 | -0.23 | -74.29 | — |
| 2026-09-04 | `ATRC` | 28 | — | $52.03 | +0.00 | $51.52 | -14.28 | -14.28 | +0.00 | -14.28 |
| 2026-09-04 | `WNC` | 106 | — | $14.17 | +0.00 | $14.31 | +14.84 | +14.84 | +0.00 | +14.84 |
| 2026-09-04 | `CRDO` | 9 | — | $162.10 | +0.00 | $170.57 | +76.23 | +76.23 | +0.00 | +76.23 |
| 2026-09-04 | `ADCT` | 1158 | — | $1.30 | +0.00 | $1.36 | +69.48 | +69.48 | +0.00 | +69.48 |
| 2026-09-04 | `HAFN` | 168 | — | $8.94 | +0.00 | $9.22 | +47.04 | +47.04 | +0.00 | +47.04 |
| 2026-09-04 | `DOCU` | 21 | — | $68.52 | +0.00 | $68.41 | -2.31 | -2.31 | +0.00 | -2.31 |
| 2026-09-04 | `XP` | 76 | — | $19.67 | +0.00 | $19.86 | +14.44 | +14.44 | +0.00 | +14.44 |
| 2026-09-08 | `ATRC` | 28 | $51.52 | $54.31 | +78.12 | — | +0.00 | +78.12 | +63.84 | — |
| 2026-09-08 | `WNC` | 106 | $14.31 | $14.22 | -9.54 | — | +0.00 | -9.54 | +5.30 | — |
| 2026-09-08 | `CRDO` | 9 | $170.57 | $170.54 | -0.23 | — | +0.00 | -0.23 | +76.00 | — |
| 2026-09-08 | `ADCT` | 1158 | $1.36 | $1.33 | -34.74 | — | +0.00 | -34.74 | +34.74 | — |
| 2026-09-08 | `HAFN` | 168 | $9.22 | $8.81 | -68.88 | — | +0.00 | -68.88 | -21.84 | — |
| 2026-09-08 | `DOCU` | 21 | $68.41 | $67.05 | -28.56 | — | +0.00 | -28.56 | -30.87 | — |
| 2026-09-08 | `XP` | 76 | $19.86 | $20.46 | +45.60 | — | +0.00 | +45.60 | +60.04 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `SSL` | 371 | — | $14.35 | +0.00 | $14.59 | +89.04 | +89.04 | +0.00 | +89.04 |
| 2026-09-11 | `ARLO` | 403 | — | $13.22 | +0.00 | $13.19 | -12.09 | -12.09 | +0.00 | -12.09 |
| 2026-09-14 | `SSL` | 371 | $14.59 | $14.69 | +37.10 | — | +0.00 | +37.10 | +126.14 | — |
| 2026-09-14 | `ARLO` | 403 | $13.19 | $13.07 | -48.36 | — | +0.00 | -48.36 | -60.45 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `ATRC` | 48 | — | $55.66 | +0.00 | $57.14 | +71.04 | +71.04 | +0.00 | +71.04 |
| 2026-09-16 | `ARLO` | 196 | — | $13.62 | +0.00 | $13.40 | -43.12 | -43.12 | +0.00 | -43.12 |
| 2026-09-16 | `TCOM` | 65 | — | $40.93 | +0.00 | $40.43 | -32.50 | -32.50 | +0.00 | -32.50 |
| 2026-09-16 | `LEN` | 33 | — | $80.63 | +0.00 | $78.36 | -74.91 | -74.91 | +0.00 | -74.91 |
| 2026-09-17 | `ATRC` | 48 | $57.14 | $57.96 | +39.36 | — | +0.00 | +39.36 | +110.40 | — |
| 2026-09-17 | `ARLO` | 196 | $13.40 | $13.62 | +43.12 | $13.35 | -52.92 | -9.80 | +0.00 | -52.92 |
| 2026-09-17 | `TCOM` | 65 | $40.43 | $40.79 | +23.40 | — | +0.00 | +23.40 | -9.10 | — |
| 2026-09-17 | `LEN` | 33 | $78.36 | $81.00 | +87.12 | — | +0.00 | +87.12 | +12.21 | — |
| 2026-09-18 | `ARLO` | 196 | $13.35 | $13.42 | +13.72 | — | +0.00 | +13.72 | -39.20 | — |
| 2026-09-21 | `UMC` | 144 | — | $24.93 | +0.00 | $25.43 | +72.00 | +72.00 | +0.00 | +72.00 |
| 2026-09-21 | `NEO` | 180 | — | $19.92 | +0.00 | $19.41 | -91.80 | -91.80 | +0.00 | -91.80 |
| 2026-09-21 | `ARLO` | 268 | — | $13.38 | +0.00 | $13.33 | -13.40 | -13.40 | +0.00 | -13.40 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +787.37 | TPG | — | $24.65 | $10,784.79 | TPG×197 |
| 2026-08-14 | +5.50 | $24.65 | TPG×197 | $10,916.78 | +131.99 | +837.31 | SPHR, KULR, NPWR, RLX | TPG | $23.35 | $11,693.70 | SPHR×15, KULR×1091, NPWR×1749, RLX×1474 |
| 2026-08-17 | +2.25 | $23.35 | SPHR×15, KULR×1091, NPWR×1749, RLX×1474 | $11,602.34 | -91.36 | -166.52 | XP | SPHR, KULR, NPWR, RLX | $1.18 | $11,367.98 | XP×724 |
| 2026-08-18 | -6.20 | $1.18 | XP×724 | $11,367.98 | -0.00 | +0.00 | — | XP | $11,358.43 | $11,358.43 | — |
| 2026-08-19 | -7.20 | $11,358.43 | — | $11,358.43 | -0.00 | +0.00 | — | — | $11,358.43 | $11,358.43 | — |
| 2026-08-20 | +1.12 | $11,358.43 | — | $11,358.43 | -0.00 | -145.96 | FUTU, WMT, BJ | — | $129.81 | $11,206.17 | FUTU×32, WMT×35, BJ×42 |
| 2026-08-21 | +3.25 | $129.81 | FUTU×32, WMT×35, BJ×42 | $11,391.88 | +185.71 | +163.38 | HITI | FUTU, WMT | $1.83 | $11,511.72 | BJ×42, HITI×3045 |
| 2026-08-24 | -5.17 | $1.83 | BJ×42, HITI×3045 | $11,536.92 | +25.20 | +0.00 | — | BJ, HITI | $11,494.92 | $11,494.92 | — |
| 2026-08-25 | +1.80 | $11,494.92 | — | $11,494.92 | -0.00 | -120.75 | ZYME, RHI, ABUS | — | $36.99 | $11,360.13 | ZYME×132, RHI×87, ABUS×729 |
| 2026-08-26 | +2.02 | $36.99 | ZYME×132, RHI×87, ABUS×729 | $11,315.13 | -45.00 | -143.72 | NCNO, PLAB, SJM, URBN | ZYME, RHI, ABUS | $217.87 | $11,148.33 | NCNO×146, PLAB×75, SJM×20, URBN×35 |
| 2026-08-27 | — | $217.87 | NCNO×146, PLAB×75, SJM×20, URBN×35 | $11,193.55 | +45.22 | -15.10 | TRLV, BOX | NCNO, PLAB, SJM, URBN | $12.87 | $11,160.70 | TRLV×491, BOX×165 |
| 2026-08-28 | +0.75 | $12.87 | TRLV×491, BOX×165 | $11,147.62 | -13.08 | -243.91 | JKS, DY, ULTA | TRLV, BOX | $499.42 | $10,887.08 | JKS×277, DY×12, ULTA×6 |
| 2026-08-31 | -5.85 | $499.42 | JKS×277, DY×12, ULTA×6 | $10,952.72 | +65.64 | +0.00 | — | JKS, DY, ULTA | $10,944.97 | $10,944.97 | — |
| 2026-09-01 | -6.30 | $10,944.97 | — | $10,944.97 | -0.00 | +0.00 | — | — | $10,944.97 | $10,944.97 | — |
| 2026-09-02 | -3.83 | $10,944.97 | — | $10,944.97 | -0.00 | +0.00 | — | — | $10,944.97 | $10,944.97 | — |
| 2026-09-03 | -0.90 | $10,944.97 | — | $10,944.97 | -0.00 | -372.86 | VIR, AVGO, FIVE, MOMO, PVH, VSXY | — | $161.76 | $10,557.24 | VIR×158, AVGO×5, FIVE×7, MOMO×331, PVH×24, VSXY×23 |
| 2026-09-04 | +2.25 | $161.76 | VIR×158, AVGO×5, FIVE×7, MOMO×331, PVH×24, VSXY×23 | $10,557.88 | +0.64 | +205.44 | ATRC, WNC, CRDO, ADCT, HAFN, DOCU, XP | VIR, AVGO, FIVE, MOMO, PVH, VSXY | $155.78 | $10,720.14 | ATRC×28, WNC×106, CRDO×9, ADCT×1158, HAFN×168, DOCU×21, XP×76 |
| 2026-09-08 | -11.47 | $155.78 | ATRC×28, WNC×106, CRDO×9, ADCT×1158, HAFN×168, DOCU×21, XP×76 | $10,701.91 | -18.23 | +0.00 | — | ATRC, WNC, CRDO, ADCT, HAFN, DOCU, XP | $10,673.45 | $10,673.45 | — |
| 2026-09-09 | -13.95 | $10,673.45 | — | $10,673.45 | -0.00 | +0.00 | — | — | $10,673.45 | $10,673.45 | — |
| 2026-09-10 | -13.28 | $10,673.45 | — | $10,673.45 | -0.00 | +0.00 | — | — | $10,673.45 | $10,673.45 | — |
| 2026-09-11 | +0.50 | $10,673.45 | — | $10,673.45 | -0.00 | +76.95 | SSL, ARLO | — | $11.95 | $10,740.41 | SSL×371, ARLO×403 |
| 2026-09-14 | -11.00 | $11.95 | SSL×371, ARLO×403 | $10,729.15 | -11.26 | +0.00 | — | SSL, ARLO | $10,718.95 | $10,718.95 | — |
| 2026-09-15 | -3.84 | $10,718.95 | — | $10,718.95 | +0.00 | +0.00 | — | — | $10,718.95 | $10,718.95 | — |
| 2026-09-16 | +5.30 | $10,718.95 | — | $10,718.95 | +0.00 | -79.49 | ATRC, ARLO, TCOM, LEN | — | $47.53 | $10,630.48 | ATRC×48, ARLO×196, TCOM×65, LEN×33 |
| 2026-09-17 | +7.38 | $47.53 | ATRC×48, ARLO×196, TCOM×65, LEN×33 | $10,823.48 | +193.00 | -52.92 | — | ATRC, TCOM, LEN | $8,147.45 | $10,764.05 | ARLO×196 |
| 2026-09-18 | +4.86 | $8,147.45 | ARLO×196 | $10,777.77 | +13.72 | +0.00 | — | ARLO | $10,775.14 | $10,775.14 | — |
| 2026-09-21 | +12.87 | $10,775.14 | — | $10,775.14 | +0.00 | -33.20 | UMC, NEO, ARLO | — | $5.37 | $10,733.53 | UMC×144, NEO×180, ARLO×268 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 197 | $50.62 | $2.58 | — | $24.65 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list flatten; ⚪; ret5=+6.2; leftover $10000.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.65 | ▲ close $10,784.79 vs 09:30 $10,000.00 (session +787.37) | 16:00 close · cash $24.65 · equity $10,784.79 vs 09:30 $10,000.00 (+784.79; session marks +787.37) · 1 name(s) marked open→close (per-name table). TPG×197 09:30 $50.62 → close $54.62 +787.37 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.65 | ▲ 09:30 equity $10,916.78 vs yday $10,784.79 (+131.99) | 09:30 open · cash $24.65 (unchanged overnight, no fees) · equity $10,916.78 vs prior close $10,784.79 (+131.99) · 1 name(s) re-marked at the open (per-name table). TPG×197 yday $54.62 → 09:30 $55.29 +131.99 | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 197 | $55.29 | $2.70 | $+914.08 | $10,914.08 | ▲ +914.08 after sell → book $10,914.08; vs 09:30 mark -2.70 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SPHR` | 15 | $176.68 | $2.04 | — | $8,261.84 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; 🔵; ⚪; ret5=+14.4; leftover $2728.52 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `KULR` | 1091 | $2.50 | $14.07 | — | $5,520.27 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ⚪; ret5=+7.6; leftover $2728.52 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NPWR` | 1749 | $1.56 | $22.56 | — | $2,769.27 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+8.0; leftover $2728.52 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `RLX` | 1474 | $1.85 | $19.01 | — | $23.35 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ⚪; ret5=+0.5; leftover $2728.52 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.35 | ▲ close $11,693.70 vs 09:30 $10,916.78 (session +837.31) | 16:00 close · cash $23.35 · equity $11,693.70 vs 09:30 $10,916.78 (+776.92; session marks +837.31) · 4 name(s) marked open→close (per-name table). SPHR×15 09:30 $176.68 → close $168.00 -130.20; KULR×1091 09:30 $2.50 → close $2.64 +152.74; NPWR×1749 09:30 $1.56 → close $1.95 +682.11; RLX×1474 09:30 $1.85 → close $1.94 +132.66 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.35 | ▼ 09:30 equity $11,602.34 vs yday $11,693.70 (-91.36) | 09:30 open · cash $23.35 (unchanged overnight, no fees) · equity $11,602.34 vs prior close $11,693.70 (-91.36) · 4 name(s) re-marked at the open (per-name table). SPHR×15 yday $168.00 → 09:30 $168.10 +1.50; KULR×1091 yday $2.64 → 09:30 $2.63 -10.91; NPWR×1749 yday $1.95 → 09:30 $1.92 -52.47; RLX×1474 yday $1.94 → 09:30 $1.92 -29.48 | — |
| 2026-08-17 09:30 ET | **SELL** | `SPHR` | 15 | $168.10 | $2.07 | $-132.80 | $2,542.79 | ▼ -132.80 after sell → book $11,600.28; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `KULR` | 1091 | $2.63 | $14.28 | $+113.48 | $5,397.84 | ▲ +113.48 after sell → book $11,586.00; vs 09:30 mark -14.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NPWR` | 1749 | $1.92 | $22.88 | $+584.20 | $8,733.04 | ▲ +584.20 after sell → book $11,563.12; vs 09:30 mark -22.88 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `RLX` | 1474 | $1.92 | $19.28 | $+64.88 | $11,543.84 | ▲ +64.88 after sell → book $11,543.84; vs 09:30 mark -19.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XP` | 724 | $15.93 | $9.34 | — | $1.18 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list overnight; ⚪; ret5=-2.6; leftover $11543.84 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.18 | ▼ close $11,367.98 vs 09:30 $11,602.34 (session -166.52) | 16:00 close · cash $1.18 · equity $11,367.98 vs 09:30 $11,602.34 (-234.36; session marks -166.52) · 1 name(s) marked open→close (per-name table). XP×724 09:30 $15.93 → close $15.70 -166.52 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.18 | ▲ 09:30 equity $11,367.98 vs yday $11,367.98 (-0.00) | 09:30 open · cash $1.18 (unchanged overnight, no fees) · equity $11,367.98 vs prior close $11,367.98 (-0.00) · 1 name(s) re-marked at the open (per-name table). XP×724 yday $15.70 → 09:30 $15.70 +0.00 | — |
| 2026-08-18 09:30 ET | **SELL** | `XP` | 724 | $15.70 | $9.55 | $-185.41 | $11,358.43 | ▼ -185.41 after sell → book $11,358.43; vs 09:30 mark -9.55 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🔴 buy🟡 |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,358.43 | ▲ close $11,358.43 vs 09:30 $11,367.98 (session +0.00) | 16:00 close · cash $11,358.43 · no lots left · equity $11,358.43. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,358.43 | ▲ 09:30 equity $11,358.43 vs yday $11,358.43 (-0.00) | 09:30 open · cash $11,358.43 · no holdings · equity $11,358.43 vs prior close $11,358.43 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,358.43 | ▲ close $11,358.43 vs 09:30 $11,358.43 (session +0.00) | 16:00 close · cash $11,358.43 · no lots left · equity $11,358.43. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,358.43 | ▲ 09:30 equity $11,358.43 vs yday $11,358.43 (-0.00) | 09:30 open · cash $11,358.43 · no holdings · equity $11,358.43 vs prior close $11,358.43 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `FUTU` | 32 | $117.65 | $2.09 | — | $7,591.54 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+4.1; leftover $3786.14 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WMT` | 35 | $106.38 | $2.10 | — | $3,866.15 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-1.7; leftover $3786.14 | join🟢 sector🟡 gen🟢 news🔴 digest🟡 judge🔴 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BJ` | 42 | $88.91 | $2.12 | — | $129.81 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list overnight; 🔵; ret5=-1.0; leftover $3786.14 | join🟡 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟡 vol🔴 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $129.81 | ▼ close $11,206.17 vs 09:30 $11,358.43 (session -145.96) | 16:00 close · cash $129.81 · equity $11,206.17 vs 09:30 $11,358.43 (-152.26; session marks -145.96) · 3 name(s) marked open→close (per-name table). FUTU×32 09:30 $117.65 → close $112.73 -157.44; WMT×35 09:30 $106.38 → close $103.84 -88.90; BJ×42 09:30 $88.91 → close $91.30 +100.38 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.81 | ▲ 09:30 equity $11,391.88 vs yday $11,206.17 (+185.71) | 09:30 open · cash $129.81 (unchanged overnight, no fees) · equity $11,391.88 vs prior close $11,206.17 (+185.71) · 3 name(s) re-marked at the open (per-name table). FUTU×32 yday $112.73 → 09:30 $115.18 +78.40; WMT×35 yday $103.84 → 09:30 $103.69 -5.25; BJ×42 yday $91.30 → 09:30 $93.98 +112.56 | — |
| 2026-08-21 09:30 ET | **SELL** | `FUTU` | 32 | $115.18 | $2.13 | $-83.25 | $3,813.45 | ▼ -83.25 after sell → book $11,389.76; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `WMT` | 35 | $103.69 | $2.13 | $-98.38 | $7,440.46 | ▼ -98.38 after sell → book $11,387.62; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HITI` | 3045 | $2.43 | $39.28 | — | $1.83 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list mover_buy; 🔵; ⚪; ret5=+5.6; leftover $7440.46 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.83 | ▲ close $11,511.72 vs 09:30 $11,391.88 (session +163.38) | 16:00 close · cash $1.83 · equity $11,511.72 vs 09:30 $11,391.88 (+119.84; session marks +163.38) · 2 name(s) marked open→close (per-name table). BJ×42 09:30 $93.98 → close $96.42 +102.48; HITI×3045 09:30 $2.43 → close $2.45 +60.90 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.83 | ▲ 09:30 equity $11,536.92 vs yday $11,511.72 (+25.20) | 09:30 open · cash $1.83 (unchanged overnight, no fees) · equity $11,536.92 vs prior close $11,511.72 (+25.20) · 2 name(s) re-marked at the open (per-name table). BJ×42 yday $96.42 → 09:30 $97.02 +25.20; HITI×3045 yday $2.45 → 09:30 $2.45 +0.00 | — |
| 2026-08-24 09:30 ET | **SELL** | `BJ` | 42 | $97.02 | $2.16 | $+336.35 | $4,074.51 | ▲ +336.35 after sell → book $11,534.76; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `HITI` | 3045 | $2.45 | $39.85 | $-18.23 | $11,494.92 | ▼ -18.23 after sell → book $11,494.92; vs 09:30 mark -39.84 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,494.92 | ▲ close $11,494.92 vs 09:30 $11,536.92 (session +0.00) | 16:00 close · cash $11,494.92 · no lots left · equity $11,494.92. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,494.92 | ▲ 09:30 equity $11,494.92 vs yday $11,494.92 (-0.00) | 09:30 open · cash $11,494.92 · no holdings · equity $11,494.92 vs prior close $11,494.92 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 132 | $28.86 | $2.39 | — | $7,683.01 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; 🔵; ret5=+13.7; leftover $3831.64 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 87 | $43.76 | $2.25 | — | $3,873.64 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list mover_buy; 🔵; ⚪; ret5=+6.2; leftover $3831.64 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ABUS` | 729 | $5.25 | $9.40 | — | $36.99 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list mover_buy; 🔵; ⚪; ret5=+10.4; leftover $3831.64 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $36.99 | ▼ close $11,360.13 vs 09:30 $11,494.92 (session -120.75) | 16:00 close · cash $36.99 · equity $11,360.13 vs 09:30 $11,494.92 (-134.79; session marks -120.75) · 3 name(s) marked open→close (per-name table). ZYME×132 09:30 $28.86 → close $27.47 -183.48; RHI×87 09:30 $43.76 → close $44.90 +99.18; ABUS×729 09:30 $5.25 → close $5.20 -36.45 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $36.99 | ▼ 09:30 equity $11,315.13 vs yday $11,360.13 (-45.00) | 09:30 open · cash $36.99 (unchanged overnight, no fees) · equity $11,315.13 vs prior close $11,360.13 (-45.00) · 3 name(s) re-marked at the open (per-name table). ZYME×132 yday $27.47 → 09:30 $27.56 +11.88; RHI×87 yday $44.90 → 09:30 $44.33 -49.59; ABUS×729 yday $5.20 → 09:30 $5.19 -7.29 | — |
| 2026-08-26 09:30 ET | **SELL** | `ZYME` | 132 | $27.56 | $2.44 | $-176.42 | $3,672.47 | ▼ -176.42 after sell → book $11,312.69; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RHI` | 87 | $44.33 | $2.30 | $+45.04 | $7,526.88 | ▲ +45.04 after sell → book $11,310.39; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ABUS` | 729 | $5.19 | $9.56 | $-62.70 | $11,300.84 | ▼ -62.70 after sell → book $11,300.84; vs 09:30 mark -9.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `NCNO` | 146 | $19.33 | $2.43 | — | $8,476.23 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; ret5=+3.0; leftover $2825.21 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `PLAB` | 75 | $37.26 | $2.21 | — | $5,679.51 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-8.0; leftover $2825.21 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SJM` | 20 | $134.80 | $2.05 | — | $2,981.46 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+5.9; leftover $2825.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `URBN` | 35 | $78.90 | $2.10 | — | $217.87 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list overnight; 🔵; ret5=+1.6; leftover $2825.21 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $217.87 | ▼ close $11,148.33 vs 09:30 $11,315.13 (session -143.72) | 16:00 close · cash $217.87 · equity $11,148.33 vs 09:30 $11,315.13 (-166.80; session marks -143.72) · 4 name(s) marked open→close (per-name table). NCNO×146 09:30 $19.33 → close $21.51 +318.28; PLAB×75 09:30 $37.26 → close $30.25 -525.75; SJM×20 09:30 $134.80 → close $130.90 -78.00; URBN×35 09:30 $78.90 → close $82.95 +141.75 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $217.87 | ▲ 09:30 equity $11,193.55 vs yday $11,148.33 (+45.22) | 09:30 open · cash $217.87 (unchanged overnight, no fees) · equity $11,193.55 vs prior close $11,148.33 (+45.22) · 4 name(s) re-marked at the open (per-name table). NCNO×146 yday $21.51 → 09:30 $22.03 +75.92; PLAB×75 yday $30.25 → 09:30 $30.12 -9.75; SJM×20 yday $130.90 → 09:30 $130.29 -12.20; URBN×35 yday $82.95 → 09:30 $82.70 -8.75 | — |
| 2026-08-27 09:30 ET | **SELL** | `NCNO` | 146 | $22.03 | $2.48 | $+389.29 | $3,431.77 | ▲ +389.29 after sell → book $11,191.07; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `PLAB` | 75 | $30.12 | $2.25 | $-539.96 | $5,688.52 | ▼ -539.96 after sell → book $11,188.82; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SJM` | 20 | $130.29 | $2.08 | $-94.33 | $8,292.24 | ▼ -94.33 after sell → book $11,186.74; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `URBN` | 35 | $82.70 | $2.13 | $+128.78 | $11,184.62 | ▲ +128.78 after sell → book $11,184.62; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `TRLV` | 491 | $11.38 | $6.33 | — | $5,590.70 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; ret5=+13.3; leftover $5592.31 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `BOX` | 165 | $33.79 | $2.48 | — | $12.87 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+0.8; leftover $5592.31 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.87 | ▼ close $11,160.70 vs 09:30 $11,193.55 (session -15.10) | 16:00 close · cash $12.87 · equity $11,160.70 vs 09:30 $11,193.55 (-32.85; session marks -15.10) · 2 name(s) marked open→close (per-name table). TRLV×491 09:30 $11.38 → close $11.03 -171.85; BOX×165 09:30 $33.79 → close $34.74 +156.75 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.87 | ▼ 09:30 equity $11,147.62 vs yday $11,160.70 (-13.08) | 09:30 open · cash $12.87 (unchanged overnight, no fees) · equity $11,147.62 vs prior close $11,160.70 (-13.08) · 2 name(s) re-marked at the open (per-name table). TRLV×491 yday $11.03 → 09:30 $11.00 -14.73; BOX×165 yday $34.74 → 09:30 $34.75 +1.65 | — |
| 2026-08-28 09:30 ET | **SELL** | `TRLV` | 491 | $11.00 | $6.46 | $-199.37 | $5,407.41 | ▼ -199.37 after sell → book $11,141.16; vs 09:30 mark -6.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BOX` | 165 | $34.75 | $2.56 | $+153.36 | $11,138.60 | ▲ +153.36 after sell → book $11,138.60; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `JKS` | 277 | $13.37 | $3.57 | — | $7,431.54 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list yday_mover; ret5=-14.9; leftover $3712.87 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DY` | 12 | $306.34 | $2.03 | — | $3,753.43 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list yday_mover; ret5=-23.0; leftover $3712.87 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ULTA` | 6 | $542.00 | $2.01 | — | $499.42 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; ret5=+4.8; leftover $3712.87 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $499.42 | ▼ close $10,887.08 vs 09:30 $11,147.62 (session -243.91) | 16:00 close · cash $499.42 · equity $10,887.08 vs 09:30 $11,147.62 (-260.54; session marks -243.91) · 3 name(s) marked open→close (per-name table). JKS×277 09:30 $13.37 → close $13.54 +47.09; DY×12 09:30 $306.34 → close $294.34 -144.00; ULTA×6 09:30 $542.00 → close $517.50 -147.00 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $499.42 | ▲ 09:30 equity $10,952.72 vs yday $10,887.08 (+65.64) | 09:30 open · cash $499.42 (unchanged overnight, no fees) · equity $10,952.72 vs prior close $10,887.08 (+65.64) · 3 name(s) re-marked at the open (per-name table). JKS×277 yday $13.54 → 09:30 $13.54 +0.00; DY×12 yday $294.34 → 09:30 $298.01 +44.04; ULTA×6 yday $517.50 → 09:30 $521.10 +21.60 | — |
| 2026-08-31 09:30 ET | **SELL** | `JKS` | 277 | $13.54 | $3.65 | $+39.87 | $4,246.35 | ▲ +39.87 after sell → book $10,949.07; vs 09:30 mark -3.65 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DY` | 12 | $298.01 | $2.06 | $-104.05 | $7,820.41 | ▼ -104.05 after sell → book $10,947.01; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ULTA` | 6 | $521.10 | $2.04 | $-129.45 | $10,944.97 | ▼ -129.45 after sell → book $10,944.97; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,944.97 | ▲ close $10,944.97 vs 09:30 $10,952.72 (session +0.00) | 16:00 close · cash $10,944.97 · no lots left · equity $10,944.97. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,944.97 | ▲ 09:30 equity $10,944.97 vs yday $10,944.97 (-0.00) | 09:30 open · cash $10,944.97 · no holdings · equity $10,944.97 vs prior close $10,944.97 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,944.97 | ▲ close $10,944.97 vs 09:30 $10,944.97 (session +0.00) | 16:00 close · cash $10,944.97 · no lots left · equity $10,944.97. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,944.97 | ▲ 09:30 equity $10,944.97 vs yday $10,944.97 (-0.00) | 09:30 open · cash $10,944.97 · no holdings · equity $10,944.97 vs prior close $10,944.97 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,944.97 | ▲ close $10,944.97 vs 09:30 $10,944.97 (session +0.00) | 16:00 close · cash $10,944.97 · no lots left · equity $10,944.97. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,944.97 | ▲ 09:30 equity $10,944.97 vs yday $10,944.97 (-0.00) | 09:30 open · cash $10,944.97 · no holdings · equity $10,944.97 vs prior close $10,944.97 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `VIR` | 158 | $11.54 | $2.46 | — | $9,119.18 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list yday_gainer; 🔵; ⚪; ret5=+3.8; leftover $1824.16 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 5 | $351.74 | $2.00 | — | $7,358.48 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+3.3; leftover $1824.16 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 7 | $257.00 | $2.01 | — | $5,557.47 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-5.5; leftover $1824.16 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MOMO` | 331 | $5.50 | $4.27 | — | $3,732.70 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-4.8; leftover $1824.16 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PVH` | 24 | $74.96 | $2.06 | — | $1,931.59 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-7.9; leftover $1824.16 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSXY` | 23 | $76.86 | $2.06 | — | $161.76 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-6.6; leftover $1824.16 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $161.76 | ▼ close $10,557.24 vs 09:30 $10,944.97 (session -372.86) | 16:00 close · cash $161.76 · equity $10,557.24 vs 09:30 $10,944.97 (-387.73; session marks -372.86) · 6 name(s) marked open→close (per-name table). VIR×158 09:30 $11.54 → close $11.45 -14.22; AVGO×5 09:30 $351.74 → close $357.16 +27.10; FIVE×7 09:30 $257.00 → close $239.96 -119.28; MOMO×331 09:30 $5.50 → close $5.10 -132.40; PVH×24 09:30 $74.96 → close $72.46 -60.00; VSXY×23 09:30 $76.86 → close $73.64 -74.06 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $161.76 | ▲ 09:30 equity $10,557.88 vs yday $10,557.24 (+0.64) | 09:30 open · cash $161.76 (unchanged overnight, no fees) · equity $10,557.88 vs prior close $10,557.24 (+0.64) · 6 name(s) re-marked at the open (per-name table). VIR×158 yday $11.45 → 09:30 $11.31 -22.12; AVGO×5 yday $357.16 → 09:30 $359.70 +12.70; FIVE×7 yday $239.96 → 09:30 $238.88 -7.56; MOMO×331 yday $5.10 → 09:30 $5.13 +9.93; PVH×24 yday $72.46 → 09:30 $72.79 +7.92; VSXY×23 yday $73.64 → 09:30 $73.63 -0.23 | — |
| 2026-09-04 09:30 ET | **SELL** | `VIR` | 158 | $11.31 | $2.50 | $-41.31 | $1,946.23 | ▼ -41.31 after sell → book $10,555.37; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 5 | $359.70 | $2.03 | $+35.77 | $3,742.70 | ▲ +35.77 after sell → book $10,553.34; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FIVE` | 7 | $238.88 | $2.03 | $-130.89 | $5,412.83 | ▼ -130.89 after sell → book $10,551.31; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MOMO` | 331 | $5.13 | $4.34 | $-131.08 | $7,106.52 | ▼ -131.08 after sell → book $10,546.97; vs 09:30 mark -4.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PVH` | 24 | $72.79 | $2.09 | $-56.23 | $8,851.39 | ▼ -56.23 after sell → book $10,544.88; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSXY` | 23 | $73.63 | $2.08 | $-78.43 | $10,542.80 | ▼ -78.43 after sell → book $10,542.80; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ATRC` | 28 | $52.03 | $2.07 | — | $9,083.89 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.5; leftover $1506.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `WNC` | 106 | $14.17 | $2.31 | — | $7,579.56 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list yday_gainer; 🔵; ret5=+7.9; leftover $1506.11 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CRDO` | 9 | $162.10 | $2.02 | — | $6,118.64 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list yday_mover; 🔵; ret5=-31.7; leftover $1506.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ADCT` | 1158 | $1.30 | $14.94 | — | $4,598.30 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; 🔵; ⚪; ret5=+17.9; leftover $1506.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `HAFN` | 168 | $8.94 | $2.49 | — | $3,093.89 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; ret5=+7.7; leftover $1506.11 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 21 | $68.52 | $2.05 | — | $1,652.92 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+3.4; leftover $1506.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `XP` | 76 | $19.67 | $2.22 | — | $155.78 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list mover_buy; 🔵; ⚪; ret5=+13.1; leftover $1506.11 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $155.78 | ▲ close $10,720.14 vs 09:30 $10,557.88 (session +205.44) | 16:00 close · cash $155.78 · equity $10,720.14 vs 09:30 $10,557.88 (+162.26; session marks +205.44) · 7 name(s) marked open→close (per-name table). ATRC×28 09:30 $52.03 → close $51.52 -14.28; WNC×106 09:30 $14.17 → close $14.31 +14.84; CRDO×9 09:30 $162.10 → close $170.57 +76.23; ADCT×1158 09:30 $1.30 → close $1.36 +69.48; HAFN×168 09:30 $8.94 → close $9.22 +47.04; DOCU×21 09:30 $68.52 → close $68.41 -2.31; XP×76 09:30 $19.67 → close $19.86 +14.44 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $155.78 | ▼ 09:30 equity $10,701.91 vs yday $10,720.14 (-18.23) | 09:30 open · cash $155.78 (unchanged overnight, no fees) · equity $10,701.91 vs prior close $10,720.14 (-18.23) · 7 name(s) re-marked at the open (per-name table). ATRC×28 yday $51.52 → 09:30 $54.31 +78.12; WNC×106 yday $14.31 → 09:30 $14.22 -9.54; CRDO×9 yday $170.57 → 09:30 $170.54 -0.23; ADCT×1158 yday $1.36 → 09:30 $1.33 -34.74; HAFN×168 yday $9.22 → 09:30 $8.81 -68.88; DOCU×21 yday $68.41 → 09:30 $67.05 -28.56; XP×76 yday $19.86 → 09:30 $20.46 +45.60 | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 28 | $54.31 | $2.10 | $+59.67 | $1,674.36 | ▲ +59.67 after sell → book $10,699.82; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `WNC` | 106 | $14.22 | $2.34 | $+0.65 | $3,179.34 | ▲ +0.65 after sell → book $10,697.48; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRDO` | 9 | $170.54 | $2.04 | $+71.95 | $4,712.21 | ▲ +71.95 after sell → book $10,695.44; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ADCT` | 1158 | $1.33 | $15.14 | $+4.66 | $6,237.21 | ▲ +4.66 after sell → book $10,680.30; vs 09:30 mark -15.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `HAFN` | 168 | $8.81 | $2.53 | $-26.87 | $7,714.75 | ▼ -26.87 after sell → book $10,677.76; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `DOCU` | 21 | $67.05 | $2.07 | $-35.00 | $9,120.73 | ▼ -35.00 after sell → book $10,675.69; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `XP` | 76 | $20.46 | $2.24 | $+55.58 | $10,673.45 | ▲ +55.58 after sell → book $10,673.45; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,673.45 | ▲ close $10,673.45 vs 09:30 $10,701.91 (session +0.00) | 16:00 close · cash $10,673.45 · no lots left · equity $10,673.45. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,673.45 | ▲ 09:30 equity $10,673.45 vs yday $10,673.45 (-0.00) | 09:30 open · cash $10,673.45 · no holdings · equity $10,673.45 vs prior close $10,673.45 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,673.45 | ▲ close $10,673.45 vs 09:30 $10,673.45 (session +0.00) | 16:00 close · cash $10,673.45 · no lots left · equity $10,673.45. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,673.45 | ▲ 09:30 equity $10,673.45 vs yday $10,673.45 (-0.00) | 09:30 open · cash $10,673.45 · no holdings · equity $10,673.45 vs prior close $10,673.45 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,673.45 | ▲ close $10,673.45 vs 09:30 $10,673.45 (session +0.00) | 16:00 close · cash $10,673.45 · no lots left · equity $10,673.45. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,673.45 | ▲ 09:30 equity $10,673.45 vs yday $10,673.45 (-0.00) | 09:30 open · cash $10,673.45 · no holdings · equity $10,673.45 vs prior close $10,673.45 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `SSL` | 371 | $14.35 | $4.79 | — | $5,344.81 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; 🔵; ret5=+15.5; leftover $5336.72 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ARLO` | 403 | $13.22 | $5.20 | — | $11.95 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; ret5=+7.3; leftover $5336.72 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.95 | ▲ close $10,740.41 vs 09:30 $10,673.45 (session +76.95) | 16:00 close · cash $11.95 · equity $10,740.41 vs 09:30 $10,673.45 (+66.96; session marks +76.95) · 2 name(s) marked open→close (per-name table). SSL×371 09:30 $14.35 → close $14.59 +89.04; ARLO×403 09:30 $13.22 → close $13.19 -12.09 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.95 | ▼ 09:30 equity $10,729.15 vs yday $10,740.41 (-11.26) | 09:30 open · cash $11.95 (unchanged overnight, no fees) · equity $10,729.15 vs prior close $10,740.41 (-11.26) · 2 name(s) re-marked at the open (per-name table). SSL×371 yday $14.59 → 09:30 $14.69 +37.10; ARLO×403 yday $13.19 → 09:30 $13.07 -48.36 | — |
| 2026-09-14 09:30 ET | **SELL** | `SSL` | 371 | $14.69 | $4.89 | $+116.46 | $5,457.05 | ▲ +116.46 after sell → book $10,724.26; vs 09:30 mark -4.89 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ARLO` | 403 | $13.07 | $5.31 | $-70.96 | $10,718.95 | ▼ -70.96 after sell → book $10,718.95; vs 09:30 mark -5.31 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,718.95 | ▲ close $10,718.95 vs 09:30 $10,729.15 (session +0.00) | 16:00 close · cash $10,718.95 · no lots left · equity $10,718.95. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,718.95 | ▲ 09:30 equity $10,718.95 vs yday $10,718.95 (+0.00) | 09:30 open · cash $10,718.95 · no holdings · equity $10,718.95 vs prior close $10,718.95 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,718.95 | ▲ close $10,718.95 vs 09:30 $10,718.95 (session +0.00) | 16:00 close · cash $10,718.95 · no lots left · equity $10,718.95. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,718.95 | ▲ 09:30 equity $10,718.95 vs yday $10,718.95 (+0.00) | 09:30 open · cash $10,718.95 · no holdings · equity $10,718.95 vs prior close $10,718.95 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `ATRC` | 48 | $55.66 | $2.13 | — | $8,045.14 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; 🔵; ret5=+4.6; leftover $2679.74 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `ARLO` | 196 | $13.62 | $2.58 | — | $5,373.04 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; ret5=+7.3; leftover $2679.74 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 65 | $40.93 | $2.19 | — | $2,710.41 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; ret5=-3.1; leftover $2679.74 | join🔴 sector🔴 gen🟢 news🟡 digest🔴 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `LEN` | 33 | $80.63 | $2.09 | — | $47.53 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list overnight; ret5=-0.4; leftover $2679.74 | join🔴 sector🔴 gen🟢 news🟡 digest🔴 ab🔴 peer🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $47.53 | ▼ close $10,630.48 vs 09:30 $10,718.95 (session -79.49) | 16:00 close · cash $47.53 · equity $10,630.48 vs 09:30 $10,718.95 (-88.47; session marks -79.49) · 4 name(s) marked open→close (per-name table). ATRC×48 09:30 $55.66 → close $57.14 +71.04; ARLO×196 09:30 $13.62 → close $13.40 -43.12; TCOM×65 09:30 $40.93 → close $40.43 -32.50; LEN×33 09:30 $80.63 → close $78.36 -74.91 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.53 | ▲ 09:30 equity $10,823.48 vs yday $10,630.48 (+193.00) | 09:30 open · cash $47.53 (unchanged overnight, no fees) · equity $10,823.48 vs prior close $10,630.48 (+193.00) · 4 name(s) re-marked at the open (per-name table). ATRC×48 yday $57.14 → 09:30 $57.96 +39.36; ARLO×196 yday $13.40 → 09:30 $13.62 +43.12; TCOM×65 yday $40.43 → 09:30 $40.79 +23.40; LEN×33 yday $78.36 → 09:30 $81.00 +87.12 | — |
| 2026-09-17 09:30 ET | **SELL** | `ATRC` | 48 | $57.96 | $2.17 | $+106.10 | $2,827.44 | ▲ +106.10 after sell → book $10,821.31; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `TCOM` | 65 | $40.79 | $2.22 | $-13.50 | $5,476.57 | ▼ -13.50 after sell → book $10,819.09; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `LEN` | 33 | $81.00 | $2.12 | $+8.00 | $8,147.45 | ▲ +8.00 after sell → book $10,816.97; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🔴 digest🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,147.45 | ▼ close $10,764.05 vs 09:30 $10,823.48 (session -52.92) | 16:00 close · cash $8,147.45 · equity $10,764.05 vs 09:30 $10,823.48 (-59.43; session marks -52.92) · 1 name(s) marked open→close (per-name table). ARLO×196 09:30 $13.62 → close $13.35 -52.92 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,147.45 | ▲ 09:30 equity $10,777.77 vs yday $10,764.05 (+13.72) | 09:30 open · cash $8,147.45 (unchanged overnight, no fees) · equity $10,777.77 vs prior close $10,764.05 (+13.72) · 1 name(s) re-marked at the open (per-name table). ARLO×196 yday $13.35 → 09:30 $13.42 +13.72 | — |
| 2026-09-18 09:30 ET | **SELL** | `ARLO` | 196 | $13.42 | $2.63 | $-44.41 | $10,775.14 | ▼ -44.41 after sell → book $10,775.14; vs 09:30 mark -2.63 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,775.14 | ▲ close $10,775.14 vs 09:30 $10,777.77 (session +0.00) | 16:00 close · cash $10,775.14 · no lots left · equity $10,775.14. | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,775.14 | ▲ 09:30 equity $10,775.14 vs yday $10,775.14 (+0.00) | 09:30 open · cash $10,775.14 · no holdings · equity $10,775.14 vs prior close $10,775.14 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-21 09:30 ET | **BUY** | `UMC` | 144 | $24.93 | $2.42 | — | $7,182.80 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; ret5=+8.7; leftover $3591.71 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `NEO` | 180 | $19.92 | $2.53 | — | $3,594.67 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; 🔵; ⚪; ret5=+15.0; leftover $3591.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `ARLO` | 268 | $13.38 | $3.46 | — | $5.37 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; ret5=+7.3; leftover $3591.71 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.37 | ▼ close $10,733.53 vs 09:30 $10,775.14 (session -33.20) | 16:00 close · cash $5.37 · equity $10,733.53 vs 09:30 $10,775.14 (-41.61; session marks -33.20) · 3 name(s) marked open→close (per-name table). UMC×144 09:30 $24.93 → close $25.43 +72.00; NEO×180 09:30 $19.92 → close $19.41 -91.80; ARLO×268 09:30 $13.38 → close $13.33 -13.40 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KEYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AURA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CIG` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CRDL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WB` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-31 | `TENB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `AME` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `KFY` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LFMD` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NAMS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `JMKE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `ARLO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DBI` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ANAB` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `LFMD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ARLO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TCOM` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `UMC` | 144 | 2026-09-21 @ $24.93 | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; ret5=+8.7; leftover $3591.71 |
| `NEO` | 180 | 2026-09-21 @ $19.92 | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; 🔵; ⚪; ret5=+15.0; leftover $3591.71 |
| `ARLO` | 268 | 2026-09-21 @ $13.38 | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; ret5=+7.3; leftover $3591.71 |
