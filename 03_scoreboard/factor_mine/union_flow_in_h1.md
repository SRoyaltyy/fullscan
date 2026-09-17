# Factor mine action — `union_flow_in_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ flow_in, no 🚨

Cash book **+5.18%** ($10,518) · signal-only (no cash/fees) was +12.48%. Starts YES **6/25**. Fills 66 · skips 21 · realized $+518.08.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,518.05.

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
| 2026-08-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `FUTU` | 49 | — | $117.65 | +0.00 | $112.73 | -241.08 | -241.08 | +0.00 | -241.08 |
| 2026-08-20 | `WMT` | 54 | — | $106.38 | +0.00 | $103.84 | -137.16 | -137.16 | +0.00 | -137.16 |
| 2026-08-21 | `FUTU` | 49 | $112.73 | $115.18 | +120.05 | — | +0.00 | +120.05 | -121.03 | — |
| 2026-08-21 | `WMT` | 54 | $103.84 | $103.69 | -8.10 | — | +0.00 | -8.10 | -145.26 | — |
| 2026-08-21 | `BJ` | 59 | — | $93.98 | +0.00 | $96.42 | +143.96 | +143.96 | +0.00 | +143.96 |
| 2026-08-21 | `HITI` | 2318 | — | $2.43 | +0.00 | $2.45 | +46.36 | +46.36 | +0.00 | +46.36 |
| 2026-08-24 | `BJ` | 59 | $96.42 | $97.02 | +35.40 | — | +0.00 | +35.40 | +179.36 | — |
| 2026-08-24 | `HITI` | 2318 | $2.45 | $2.45 | +0.00 | — | +0.00 | +0.00 | +46.36 | — |
| 2026-08-25 | `ZYME` | 132 | — | $28.86 | +0.00 | $27.47 | -183.48 | -183.48 | +0.00 | -183.48 |
| 2026-08-25 | `RHI` | 87 | — | $43.76 | +0.00 | $44.90 | +99.18 | +99.18 | +0.00 | +99.18 |
| 2026-08-25 | `ABUS` | 723 | — | $5.25 | +0.00 | $5.20 | -36.15 | -36.15 | +0.00 | -36.15 |
| 2026-08-26 | `ZYME` | 132 | $27.47 | $27.56 | +11.88 | — | +0.00 | +11.88 | -171.60 | — |
| 2026-08-26 | `RHI` | 87 | $44.90 | $44.33 | -49.59 | — | +0.00 | -49.59 | +49.59 | — |
| 2026-08-26 | `ABUS` | 723 | $5.20 | $5.19 | -7.23 | — | +0.00 | -7.23 | -43.38 | — |
| 2026-08-26 | `NCNO` | 193 | — | $19.33 | +0.00 | $21.51 | +420.74 | +420.74 | +0.00 | +420.74 |
| 2026-08-26 | `PLAB` | 100 | — | $37.26 | +0.00 | $30.25 | -701.00 | -701.00 | +0.00 | -701.00 |
| 2026-08-26 | `SJM` | 27 | — | $134.80 | +0.00 | $130.90 | -105.30 | -105.30 | +0.00 | -105.30 |
| 2026-08-27 | `NCNO` | 193 | $21.51 | $22.03 | +100.36 | — | +0.00 | +100.36 | +521.10 | — |
| 2026-08-27 | `PLAB` | 100 | $30.25 | $30.12 | -13.00 | — | +0.00 | -13.00 | -714.00 | — |
| 2026-08-27 | `SJM` | 27 | $130.90 | $130.29 | -16.47 | — | +0.00 | -16.47 | -121.77 | — |
| 2026-08-28 | `JKS` | 271 | — | $13.37 | +0.00 | $13.54 | +46.07 | +46.07 | +0.00 | +46.07 |
| 2026-08-28 | `DY` | 11 | — | $306.34 | +0.00 | $294.34 | -132.00 | -132.00 | +0.00 | -132.00 |
| 2026-08-28 | `ULTA` | 6 | — | $542.00 | +0.00 | $517.50 | -147.00 | -147.00 | +0.00 | -147.00 |
| 2026-08-31 | `JKS` | 271 | $13.54 | $13.54 | +0.00 | — | +0.00 | +0.00 | +46.07 | — |
| 2026-08-31 | `DY` | 11 | $294.34 | $298.01 | +40.37 | — | +0.00 | +40.37 | -91.63 | — |
| 2026-08-31 | `ULTA` | 6 | $517.50 | $521.10 | +21.60 | — | +0.00 | +21.60 | -125.40 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `VIR` | 154 | — | $11.54 | +0.00 | $11.45 | -13.86 | -13.86 | +0.00 | -13.86 |
| 2026-09-03 | `AVGO` | 5 | — | $351.74 | +0.00 | $357.16 | +27.10 | +27.10 | +0.00 | +27.10 |
| 2026-09-03 | `FIVE` | 6 | — | $257.00 | +0.00 | $239.96 | -102.24 | -102.24 | +0.00 | -102.24 |
| 2026-09-03 | `MOMO` | 324 | — | $5.50 | +0.00 | $5.10 | -129.60 | -129.60 | +0.00 | -129.60 |
| 2026-09-03 | `PVH` | 23 | — | $74.96 | +0.00 | $72.46 | -57.50 | -57.50 | +0.00 | -57.50 |
| 2026-09-03 | `VSXY` | 23 | — | $76.86 | +0.00 | $73.64 | -74.06 | -74.06 | +0.00 | -74.06 |
| 2026-09-04 | `VIR` | 154 | $11.45 | $11.31 | -21.56 | — | +0.00 | -21.56 | -35.42 | — |
| 2026-09-04 | `AVGO` | 5 | $357.16 | $359.70 | +12.70 | — | +0.00 | +12.70 | +39.80 | — |
| 2026-09-04 | `FIVE` | 6 | $239.96 | $238.88 | -6.48 | — | +0.00 | -6.48 | -108.72 | — |
| 2026-09-04 | `MOMO` | 324 | $5.10 | $5.13 | +9.72 | — | +0.00 | +9.72 | -119.88 | — |
| 2026-09-04 | `PVH` | 23 | $72.46 | $72.79 | +7.59 | — | +0.00 | +7.59 | -49.91 | — |
| 2026-09-04 | `VSXY` | 23 | $73.64 | $73.63 | -0.23 | — | +0.00 | -0.23 | -74.29 | — |
| 2026-09-04 | `ATRC` | 28 | — | $52.03 | +0.00 | $51.52 | -14.28 | -14.28 | +0.00 | -14.28 |
| 2026-09-04 | `WNC` | 104 | — | $14.17 | +0.00 | $14.31 | +14.56 | +14.56 | +0.00 | +14.56 |
| 2026-09-04 | `CRDO` | 9 | — | $162.10 | +0.00 | $170.57 | +76.23 | +76.23 | +0.00 | +76.23 |
| 2026-09-04 | `ADCT` | 1136 | — | $1.30 | +0.00 | $1.36 | +68.16 | +68.16 | +0.00 | +68.16 |
| 2026-09-04 | `HAFN` | 165 | — | $8.94 | +0.00 | $9.22 | +46.20 | +46.20 | +0.00 | +46.20 |
| 2026-09-04 | `DOCU` | 21 | — | $68.52 | +0.00 | $68.41 | -2.31 | -2.31 | +0.00 | -2.31 |
| 2026-09-04 | `XP` | 75 | — | $19.67 | +0.00 | $19.86 | +14.25 | +14.25 | +0.00 | +14.25 |
| 2026-09-08 | `ATRC` | 28 | $51.52 | $54.31 | +78.12 | — | +0.00 | +78.12 | +63.84 | — |
| 2026-09-08 | `WNC` | 104 | $14.31 | $14.22 | -9.36 | — | +0.00 | -9.36 | +5.20 | — |
| 2026-09-08 | `CRDO` | 9 | $170.57 | $170.54 | -0.23 | — | +0.00 | -0.23 | +76.00 | — |
| 2026-09-08 | `ADCT` | 1136 | $1.36 | $1.33 | -34.08 | — | +0.00 | -34.08 | +34.08 | — |
| 2026-09-08 | `HAFN` | 165 | $9.22 | $8.81 | -67.65 | — | +0.00 | -67.65 | -21.45 | — |
| 2026-09-08 | `DOCU` | 21 | $68.41 | $67.05 | -28.56 | — | +0.00 | -28.56 | -30.87 | — |
| 2026-09-08 | `XP` | 75 | $19.86 | $20.46 | +45.00 | — | +0.00 | +45.00 | +59.25 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `SSL` | 364 | — | $14.35 | +0.00 | $14.59 | +87.36 | +87.36 | +0.00 | +87.36 |
| 2026-09-11 | `ARLO` | 396 | — | $13.22 | +0.00 | $13.19 | -11.88 | -11.88 | +0.00 | -11.88 |
| 2026-09-14 | `SSL` | 364 | $14.59 | $14.69 | +36.40 | — | +0.00 | +36.40 | +123.76 | — |
| 2026-09-14 | `ARLO` | 396 | $13.19 | $13.07 | -47.52 | — | +0.00 | -47.52 | -59.40 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-17 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +787.37 | TPG | — | $24.65 | $10,784.79 | TPG×197 |
| 2026-08-14 | +5.50 | $24.65 | TPG×197 | $10,916.78 | +131.99 | +837.31 | SPHR, KULR, NPWR, RLX | TPG | $23.35 | $11,693.70 | SPHR×15, KULR×1091, NPWR×1749, RLX×1474 |
| 2026-08-17 | +2.25 | $23.35 | SPHR×15, KULR×1091, NPWR×1749, RLX×1474 | $11,602.34 | -91.36 | +0.00 | — | SPHR, KULR, NPWR, RLX | $11,543.84 | $11,543.84 | — |
| 2026-08-18 | -6.20 | $11,543.84 | — | $11,543.84 | -0.00 | +0.00 | — | — | $11,543.84 | $11,543.84 | — |
| 2026-08-19 | -7.20 | $11,543.84 | — | $11,543.84 | -0.00 | +0.00 | — | — | $11,543.84 | $11,543.84 | — |
| 2026-08-20 | +1.12 | $11,543.84 | — | $11,543.84 | -0.00 | -378.24 | FUTU, WMT | — | $30.18 | $11,161.31 | FUTU×49, WMT×54 |
| 2026-08-21 | +3.25 | $30.18 | FUTU×49, WMT×54 | $11,273.26 | +111.95 | +190.32 | BJ, HITI | FUTU, WMT | $59.23 | $11,427.11 | BJ×59, HITI×2318 |
| 2026-08-24 | -5.17 | $59.23 | BJ×59, HITI×2318 | $11,462.51 | +35.40 | +0.00 | — | BJ, HITI | $11,429.96 | $11,429.96 | — |
| 2026-08-25 | +1.80 | $11,429.96 | — | $11,429.96 | -0.00 | -120.45 | ZYME, RHI, ABUS | — | $3.60 | $11,295.54 | ZYME×132, RHI×87, ABUS×723 |
| 2026-08-26 | +2.02 | $3.60 | ZYME×132, RHI×87, ABUS×723 | $11,250.60 | -44.94 | -385.56 | NCNO, PLAB, SJM | ZYME, RHI, ABUS | $133.17 | $10,843.90 | NCNO×193, PLAB×100, SJM×27 |
| 2026-08-27 | — | $133.17 | NCNO×193, PLAB×100, SJM×27 | $10,914.79 | +70.89 | +0.00 | — | NCNO, PLAB, SJM | $10,907.72 | $10,907.72 | — |
| 2026-08-28 | +0.75 | $10,907.72 | — | $10,907.72 | -0.00 | -232.93 | JKS, DY, ULTA | — | $655.18 | $10,667.26 | JKS×271, DY×11, ULTA×6 |
| 2026-08-31 | -5.85 | $655.18 | JKS×271, DY×11, ULTA×6 | $10,729.23 | +61.97 | +0.00 | — | JKS, DY, ULTA | $10,721.56 | $10,721.56 | — |
| 2026-09-01 | -6.30 | $10,721.56 | — | $10,721.56 | -0.00 | +0.00 | — | — | $10,721.56 | $10,721.56 | — |
| 2026-09-02 | -3.83 | $10,721.56 | — | $10,721.56 | -0.00 | +0.00 | — | — | $10,721.56 | $10,721.56 | — |
| 2026-09-03 | -0.90 | $10,721.56 | — | $10,721.56 | -0.00 | -350.16 | VIR, AVGO, FIVE, MOMO, PVH, VSXY | — | $355.07 | $10,356.63 | VIR×154, AVGO×5, FIVE×6, MOMO×324, PVH×23, VSXY×23 |
| 2026-09-04 | +2.25 | $355.07 | VIR×154, AVGO×5, FIVE×6, MOMO×324, PVH×23, VSXY×23 | $10,358.37 | +1.74 | +202.81 | ATRC, WNC, CRDO, ADCT, HAFN, DOCU, XP | VIR, AVGO, FIVE, MOMO, PVH, VSXY | $60.12 | $10,518.42 | ATRC×28, WNC×104, CRDO×9, ADCT×1136, HAFN×165, DOCU×21, XP×75 |
| 2026-09-08 | -11.47 | $60.12 | ATRC×28, WNC×104, CRDO×9, ADCT×1136, HAFN×165, DOCU×21, XP×75 | $10,501.67 | -16.75 | +0.00 | — | ATRC, WNC, CRDO, ADCT, HAFN, DOCU, XP | $10,473.51 | $10,473.51 | — |
| 2026-09-09 | -13.95 | $10,473.51 | — | $10,473.51 | -0.00 | +0.00 | — | — | $10,473.51 | $10,473.51 | — |
| 2026-09-10 | -13.28 | $10,473.51 | — | $10,473.51 | -0.00 | +0.00 | — | — | $10,473.51 | $10,473.51 | — |
| 2026-09-11 | +0.50 | $10,473.51 | — | $10,473.51 | -0.00 | +75.48 | SSL, ARLO | — | $5.18 | $10,539.18 | SSL×364, ARLO×396 |
| 2026-09-14 | -11.00 | $5.18 | SSL×364, ARLO×396 | $10,528.06 | -11.12 | +0.00 | — | SSL, ARLO | $10,518.05 | $10,518.05 | — |
| 2026-09-15 | -3.84 | $10,518.05 | — | $10,518.05 | -0.00 | +0.00 | — | — | $10,518.05 | $10,518.05 | — |
| 2026-09-16 | +5.30 | $10,518.05 | — | $10,518.05 | -0.00 | +0.00 | — | — | $10,518.05 | $10,518.05 | — |
| 2026-09-17 | +7.38 | $10,518.05 | — | $10,518.05 | -0.00 | +0.00 | — | — | $10,518.05 | $10,518.05 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 197 | $50.62 | $2.58 | — | $24.65 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list flatten; ret5=+6.2; leftover $10000.00 | join🟢 sector🟢 gen🟢 judge🔴 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.65 | ▲ close $10,784.79 vs 09:30 $10,000.00 (session +787.37) | 16:00 close · cash $24.65 · equity $10,784.79 vs 09:30 $10,000.00 (+784.79; session marks +787.37) · 1 name(s) marked open→close (per-name table). TPG×197 09:30 $50.62 → close $54.62 +787.37 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.65 | ▲ 09:30 equity $10,916.78 vs yday $10,784.79 (+131.99) | 09:30 open · cash $24.65 (unchanged overnight, no fees) · equity $10,916.78 vs prior close $10,784.79 (+131.99) · 1 name(s) re-marked at the open (per-name table). TPG×197 yday $54.62 → 09:30 $55.29 +131.99 | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 197 | $55.29 | $2.70 | $+914.08 | $10,914.08 | ▲ +914.08 after sell → book $10,914.08; vs 09:30 mark -2.70 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
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
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,543.84 | ▲ close $11,543.84 vs 09:30 $11,602.34 (session +0.00) | 16:00 close · cash $11,543.84 · no lots left · equity $11,543.84. | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,543.84 | ▲ 09:30 equity $11,543.84 vs yday $11,543.84 (-0.00) | 09:30 open · cash $11,543.84 · no holdings · equity $11,543.84 vs prior close $11,543.84 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,543.84 | ▲ close $11,543.84 vs 09:30 $11,543.84 (session +0.00) | 16:00 close · cash $11,543.84 · no lots left · equity $11,543.84. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,543.84 | ▲ 09:30 equity $11,543.84 vs yday $11,543.84 (-0.00) | 09:30 open · cash $11,543.84 · no holdings · equity $11,543.84 vs prior close $11,543.84 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,543.84 | ▲ close $11,543.84 vs 09:30 $11,543.84 (session +0.00) | 16:00 close · cash $11,543.84 · no lots left · equity $11,543.84. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,543.84 | ▲ 09:30 equity $11,543.84 vs yday $11,543.84 (-0.00) | 09:30 open · cash $11,543.84 · no holdings · equity $11,543.84 vs prior close $11,543.84 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `FUTU` | 49 | $117.65 | $2.14 | — | $5,776.85 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+4.1; leftover $5771.92 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WMT` | 54 | $106.38 | $2.15 | — | $30.18 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-1.7; leftover $5771.92 | join🟢 sector🟡 gen🟢 news🔴 digest🟡 judge🔴 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.18 | ▼ close $11,161.31 vs 09:30 $11,543.84 (session -378.24) | 16:00 close · cash $30.18 · equity $11,161.31 vs 09:30 $11,543.84 (-382.53; session marks -378.24) · 2 name(s) marked open→close (per-name table). FUTU×49 09:30 $117.65 → close $112.73 -241.08; WMT×54 09:30 $106.38 → close $103.84 -137.16 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.18 | ▲ 09:30 equity $11,273.26 vs yday $11,161.31 (+111.95) | 09:30 open · cash $30.18 (unchanged overnight, no fees) · equity $11,273.26 vs prior close $11,161.31 (+111.95) · 2 name(s) re-marked at the open (per-name table). FUTU×49 yday $112.73 → 09:30 $115.18 +120.05; WMT×54 yday $103.84 → 09:30 $103.69 -8.10 | — |
| 2026-08-21 09:30 ET | **SELL** | `FUTU` | 49 | $115.18 | $2.19 | $-125.36 | $5,671.81 | ▼ -125.36 after sell → book $11,271.07; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `WMT` | 54 | $103.69 | $2.21 | $-149.62 | $11,268.86 | ▼ -149.62 after sell → book $11,268.86; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 59 | $93.98 | $2.17 | — | $5,721.87 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; ret5=-2.4; leftover $5634.43 | join🟡 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HITI` | 2318 | $2.43 | $29.90 | — | $59.23 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list mover_buy; 🔵; ⚪; ret5=+5.6; leftover $5634.43 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $59.23 | ▲ close $11,427.11 vs 09:30 $11,273.26 (session +190.32) | 16:00 close · cash $59.23 · equity $11,427.11 vs 09:30 $11,273.26 (+153.85; session marks +190.32) · 2 name(s) marked open→close (per-name table). BJ×59 09:30 $93.98 → close $96.42 +143.96; HITI×2318 09:30 $2.43 → close $2.45 +46.36 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.23 | ▲ 09:30 equity $11,462.51 vs yday $11,427.11 (+35.40) | 09:30 open · cash $59.23 (unchanged overnight, no fees) · equity $11,462.51 vs prior close $11,427.11 (+35.40) · 2 name(s) re-marked at the open (per-name table). BJ×59 yday $96.42 → 09:30 $97.02 +35.40; HITI×2318 yday $2.45 → 09:30 $2.45 +0.00 | — |
| 2026-08-24 09:30 ET | **SELL** | `BJ` | 59 | $97.02 | $2.22 | $+174.97 | $5,781.19 | ▲ +174.97 after sell → book $11,460.29; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `HITI` | 2318 | $2.45 | $30.33 | $-13.87 | $11,429.96 | ▼ -13.87 after sell → book $11,429.96; vs 09:30 mark -30.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,429.96 | ▲ close $11,429.96 vs 09:30 $11,462.51 (session +0.00) | 16:00 close · cash $11,429.96 · no lots left · equity $11,429.96. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,429.96 | ▲ 09:30 equity $11,429.96 vs yday $11,429.96 (-0.00) | 09:30 open · cash $11,429.96 · no holdings · equity $11,429.96 vs prior close $11,429.96 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 132 | $28.86 | $2.39 | — | $7,618.05 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; 🔵; ret5=+13.7; leftover $3809.99 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 87 | $43.76 | $2.25 | — | $3,808.68 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list mover_buy; 🔵; ⚪; ret5=+6.2; leftover $3809.99 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ABUS` | 723 | $5.25 | $9.33 | — | $3.60 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list mover_buy; 🔵; ⚪; ret5=+10.4; leftover $3809.99 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.60 | ▼ close $11,295.54 vs 09:30 $11,429.96 (session -120.45) | 16:00 close · cash $3.60 · equity $11,295.54 vs 09:30 $11,429.96 (-134.42; session marks -120.45) · 3 name(s) marked open→close (per-name table). ZYME×132 09:30 $28.86 → close $27.47 -183.48; RHI×87 09:30 $43.76 → close $44.90 +99.18; ABUS×723 09:30 $5.25 → close $5.20 -36.15 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.60 | ▼ 09:30 equity $11,250.60 vs yday $11,295.54 (-44.94) | 09:30 open · cash $3.60 (unchanged overnight, no fees) · equity $11,250.60 vs prior close $11,295.54 (-44.94) · 3 name(s) re-marked at the open (per-name table). ZYME×132 yday $27.47 → 09:30 $27.56 +11.88; RHI×87 yday $44.90 → 09:30 $44.33 -49.59; ABUS×723 yday $5.20 → 09:30 $5.19 -7.23 | — |
| 2026-08-26 09:30 ET | **SELL** | `ZYME` | 132 | $27.56 | $2.44 | $-176.42 | $3,639.08 | ▼ -176.42 after sell → book $11,248.16; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RHI` | 87 | $44.33 | $2.30 | $+45.04 | $7,493.50 | ▲ +45.04 after sell → book $11,245.87; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ABUS` | 723 | $5.19 | $9.48 | $-62.18 | $11,236.39 | ▼ -62.18 after sell → book $11,236.39; vs 09:30 mark -9.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `NCNO` | 193 | $19.33 | $2.57 | — | $7,503.13 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; ret5=+3.0; leftover $3745.46 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `PLAB` | 100 | $37.26 | $2.29 | — | $3,774.84 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-8.0; leftover $3745.46 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SJM` | 27 | $134.80 | $2.07 | — | $133.17 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+5.9; leftover $3745.46 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $133.17 | ▼ close $10,843.90 vs 09:30 $11,250.60 (session -385.56) | 16:00 close · cash $133.17 · equity $10,843.90 vs 09:30 $11,250.60 (-406.70; session marks -385.56) · 3 name(s) marked open→close (per-name table). NCNO×193 09:30 $19.33 → close $21.51 +420.74; PLAB×100 09:30 $37.26 → close $30.25 -701.00; SJM×27 09:30 $134.80 → close $130.90 -105.30 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $133.17 | ▲ 09:30 equity $10,914.79 vs yday $10,843.90 (+70.89) | 09:30 open · cash $133.17 (unchanged overnight, no fees) · equity $10,914.79 vs prior close $10,843.90 (+70.89) · 3 name(s) re-marked at the open (per-name table). NCNO×193 yday $21.51 → 09:30 $22.03 +100.36; PLAB×100 yday $30.25 → 09:30 $30.12 -13.00; SJM×27 yday $130.90 → 09:30 $130.29 -16.47 | — |
| 2026-08-27 09:30 ET | **SELL** | `NCNO` | 193 | $22.03 | $2.64 | $+515.90 | $4,382.33 | ▲ +515.90 after sell → book $10,912.16; vs 09:30 mark -2.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PLAB` | 100 | $30.12 | $2.33 | $-718.62 | $7,392.00 | ▼ -718.62 after sell → book $10,909.83; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SJM` | 27 | $130.29 | $2.11 | $-125.95 | $10,907.72 | ▼ -125.95 after sell → book $10,907.72; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,907.72 | ▲ close $10,907.72 vs 09:30 $10,914.79 (session +0.00) | 16:00 close · cash $10,907.72 · no lots left · equity $10,907.72. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,907.72 | ▲ 09:30 equity $10,907.72 vs yday $10,907.72 (-0.00) | 09:30 open · cash $10,907.72 · no holdings · equity $10,907.72 vs prior close $10,907.72 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `JKS` | 271 | $13.37 | $3.50 | — | $7,280.95 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list yday_mover; ret5=-14.9; leftover $3635.91 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DY` | 11 | $306.34 | $2.02 | — | $3,909.19 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list yday_mover; ret5=-23.0; leftover $3635.91 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ULTA` | 6 | $542.00 | $2.01 | — | $655.18 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; ret5=+4.8; leftover $3635.91 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $655.18 | ▼ close $10,667.26 vs 09:30 $10,907.72 (session -232.93) | 16:00 close · cash $655.18 · equity $10,667.26 vs 09:30 $10,907.72 (-240.46; session marks -232.93) · 3 name(s) marked open→close (per-name table). JKS×271 09:30 $13.37 → close $13.54 +46.07; DY×11 09:30 $306.34 → close $294.34 -132.00; ULTA×6 09:30 $542.00 → close $517.50 -147.00 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $655.18 | ▲ 09:30 equity $10,729.23 vs yday $10,667.26 (+61.97) | 09:30 open · cash $655.18 (unchanged overnight, no fees) · equity $10,729.23 vs prior close $10,667.26 (+61.97) · 3 name(s) re-marked at the open (per-name table). JKS×271 yday $13.54 → 09:30 $13.54 +0.00; DY×11 yday $294.34 → 09:30 $298.01 +40.37; ULTA×6 yday $517.50 → 09:30 $521.10 +21.60 | — |
| 2026-08-31 09:30 ET | **SELL** | `JKS` | 271 | $13.54 | $3.57 | $+39.00 | $4,320.95 | ▲ +39.00 after sell → book $10,725.66; vs 09:30 mark -3.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DY` | 11 | $298.01 | $2.06 | $-95.71 | $7,597.00 | ▼ -95.71 after sell → book $10,723.60; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ULTA` | 6 | $521.10 | $2.04 | $-129.45 | $10,721.56 | ▼ -129.45 after sell → book $10,721.56; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,721.56 | ▲ close $10,721.56 vs 09:30 $10,729.23 (session +0.00) | 16:00 close · cash $10,721.56 · no lots left · equity $10,721.56. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,721.56 | ▲ 09:30 equity $10,721.56 vs yday $10,721.56 (-0.00) | 09:30 open · cash $10,721.56 · no holdings · equity $10,721.56 vs prior close $10,721.56 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,721.56 | ▲ close $10,721.56 vs 09:30 $10,721.56 (session +0.00) | 16:00 close · cash $10,721.56 · no lots left · equity $10,721.56. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,721.56 | ▲ 09:30 equity $10,721.56 vs yday $10,721.56 (-0.00) | 09:30 open · cash $10,721.56 · no holdings · equity $10,721.56 vs prior close $10,721.56 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,721.56 | ▲ close $10,721.56 vs 09:30 $10,721.56 (session +0.00) | 16:00 close · cash $10,721.56 · no lots left · equity $10,721.56. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,721.56 | ▲ 09:30 equity $10,721.56 vs yday $10,721.56 (-0.00) | 09:30 open · cash $10,721.56 · no holdings · equity $10,721.56 vs prior close $10,721.56 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `VIR` | 154 | $11.54 | $2.45 | — | $8,941.95 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list yday_gainer; 🔵; ⚪; ret5=+3.8; leftover $1786.93 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 5 | $351.74 | $2.00 | — | $7,181.24 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+3.3; leftover $1786.93 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 6 | $257.00 | $2.01 | — | $5,637.23 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-5.5; leftover $1786.93 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MOMO` | 324 | $5.50 | $4.18 | — | $3,851.05 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-4.8; leftover $1786.93 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PVH` | 23 | $74.96 | $2.06 | — | $2,124.91 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-7.9; leftover $1786.93 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSXY` | 23 | $76.86 | $2.06 | — | $355.07 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-6.6; leftover $1786.93 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $355.07 | ▼ close $10,356.63 vs 09:30 $10,721.56 (session -350.16) | 16:00 close · cash $355.07 · equity $10,356.63 vs 09:30 $10,721.56 (-364.93; session marks -350.16) · 6 name(s) marked open→close (per-name table). VIR×154 09:30 $11.54 → close $11.45 -13.86; AVGO×5 09:30 $351.74 → close $357.16 +27.10; FIVE×6 09:30 $257.00 → close $239.96 -102.24; MOMO×324 09:30 $5.50 → close $5.10 -129.60; PVH×23 09:30 $74.96 → close $72.46 -57.50; VSXY×23 09:30 $76.86 → close $73.64 -74.06 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $355.07 | ▲ 09:30 equity $10,358.37 vs yday $10,356.63 (+1.74) | 09:30 open · cash $355.07 (unchanged overnight, no fees) · equity $10,358.37 vs prior close $10,356.63 (+1.74) · 6 name(s) re-marked at the open (per-name table). VIR×154 yday $11.45 → 09:30 $11.31 -21.56; AVGO×5 yday $357.16 → 09:30 $359.70 +12.70; FIVE×6 yday $239.96 → 09:30 $238.88 -6.48; MOMO×324 yday $5.10 → 09:30 $5.13 +9.72; PVH×23 yday $72.46 → 09:30 $72.79 +7.59; VSXY×23 yday $73.64 → 09:30 $73.63 -0.23 | — |
| 2026-09-04 09:30 ET | **SELL** | `VIR` | 154 | $11.31 | $2.49 | $-40.36 | $2,094.32 | ▼ -40.36 after sell → book $10,355.88; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 5 | $359.70 | $2.03 | $+35.77 | $3,890.79 | ▲ +35.77 after sell → book $10,353.85; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FIVE` | 6 | $238.88 | $2.03 | $-112.76 | $5,322.04 | ▼ -112.76 after sell → book $10,351.82; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MOMO` | 324 | $5.13 | $4.25 | $-128.31 | $6,979.92 | ▼ -128.31 after sell → book $10,347.58; vs 09:30 mark -4.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PVH` | 23 | $72.79 | $2.08 | $-54.05 | $8,652.01 | ▼ -54.05 after sell → book $10,345.50; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSXY` | 23 | $73.63 | $2.08 | $-78.43 | $10,343.41 | ▼ -78.43 after sell → book $10,343.41; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ATRC` | 28 | $52.03 | $2.07 | — | $8,884.50 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.5; leftover $1477.63 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `WNC` | 104 | $14.17 | $2.30 | — | $7,408.52 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list yday_gainer; 🔵; ret5=+7.9; leftover $1477.63 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CRDO` | 9 | $162.10 | $2.02 | — | $5,947.60 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list yday_mover; 🔵; ret5=-31.7; leftover $1477.63 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ADCT` | 1136 | $1.30 | $14.65 | — | $4,456.15 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; 🔵; ⚪; ret5=+17.9; leftover $1477.63 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `HAFN` | 165 | $8.94 | $2.48 | — | $2,978.56 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; ret5=+7.7; leftover $1477.63 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 21 | $68.52 | $2.05 | — | $1,537.59 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+3.4; leftover $1477.63 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `XP` | 75 | $19.67 | $2.21 | — | $60.12 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list mover_buy; 🔵; ⚪; ret5=+13.1; leftover $1477.63 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $60.12 | ▲ close $10,518.42 vs 09:30 $10,358.37 (session +202.81) | 16:00 close · cash $60.12 · equity $10,518.42 vs 09:30 $10,358.37 (+160.05; session marks +202.81) · 7 name(s) marked open→close (per-name table). ATRC×28 09:30 $52.03 → close $51.52 -14.28; WNC×104 09:30 $14.17 → close $14.31 +14.56; CRDO×9 09:30 $162.10 → close $170.57 +76.23; ADCT×1136 09:30 $1.30 → close $1.36 +68.16; HAFN×165 09:30 $8.94 → close $9.22 +46.20; DOCU×21 09:30 $68.52 → close $68.41 -2.31; XP×75 09:30 $19.67 → close $19.86 +14.25 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $60.12 | ▼ 09:30 equity $10,501.67 vs yday $10,518.42 (-16.75) | 09:30 open · cash $60.12 (unchanged overnight, no fees) · equity $10,501.67 vs prior close $10,518.42 (-16.75) · 7 name(s) re-marked at the open (per-name table). ATRC×28 yday $51.52 → 09:30 $54.31 +78.12; WNC×104 yday $14.31 → 09:30 $14.22 -9.36; CRDO×9 yday $170.57 → 09:30 $170.54 -0.23; ADCT×1136 yday $1.36 → 09:30 $1.33 -34.08; HAFN×165 yday $9.22 → 09:30 $8.81 -67.65; DOCU×21 yday $68.41 → 09:30 $67.05 -28.56; XP×75 yday $19.86 → 09:30 $20.46 +45.00 | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 28 | $54.31 | $2.10 | $+59.67 | $1,578.71 | ▲ +59.67 after sell → book $10,499.57; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `WNC` | 104 | $14.22 | $2.33 | $+0.57 | $3,055.26 | ▲ +0.57 after sell → book $10,497.24; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRDO` | 9 | $170.54 | $2.04 | $+71.95 | $4,588.12 | ▲ +71.95 after sell → book $10,495.20; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ADCT` | 1136 | $1.33 | $14.86 | $+4.57 | $6,084.15 | ▲ +4.57 after sell → book $10,480.35; vs 09:30 mark -14.85 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `HAFN` | 165 | $8.81 | $2.52 | $-26.46 | $7,535.27 | ▼ -26.46 after sell → book $10,477.82; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `DOCU` | 21 | $67.05 | $2.07 | $-35.00 | $8,941.25 | ▼ -35.00 after sell → book $10,475.75; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `XP` | 75 | $20.46 | $2.24 | $+54.80 | $10,473.51 | ▲ +54.80 after sell → book $10,473.51; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,473.51 | ▲ close $10,473.51 vs 09:30 $10,501.67 (session +0.00) | 16:00 close · cash $10,473.51 · no lots left · equity $10,473.51. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,473.51 | ▲ 09:30 equity $10,473.51 vs yday $10,473.51 (-0.00) | 09:30 open · cash $10,473.51 · no holdings · equity $10,473.51 vs prior close $10,473.51 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,473.51 | ▲ close $10,473.51 vs 09:30 $10,473.51 (session +0.00) | 16:00 close · cash $10,473.51 · no lots left · equity $10,473.51. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,473.51 | ▲ 09:30 equity $10,473.51 vs yday $10,473.51 (-0.00) | 09:30 open · cash $10,473.51 · no holdings · equity $10,473.51 vs prior close $10,473.51 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,473.51 | ▲ close $10,473.51 vs 09:30 $10,473.51 (session +0.00) | 16:00 close · cash $10,473.51 · no lots left · equity $10,473.51. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,473.51 | ▲ 09:30 equity $10,473.51 vs yday $10,473.51 (-0.00) | 09:30 open · cash $10,473.51 · no holdings · equity $10,473.51 vs prior close $10,473.51 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `SSL` | 364 | $14.35 | $4.70 | — | $5,245.41 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; 🔵; ret5=+15.5; leftover $5236.75 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `ARLO` | 396 | $13.22 | $5.11 | — | $5.18 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; ret5=+7.3; leftover $5236.75 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.18 | ▲ close $10,539.18 vs 09:30 $10,473.51 (session +75.48) | 16:00 close · cash $5.18 · equity $10,539.18 vs 09:30 $10,473.51 (+65.67; session marks +75.48) · 2 name(s) marked open→close (per-name table). SSL×364 09:30 $14.35 → close $14.59 +87.36; ARLO×396 09:30 $13.22 → close $13.19 -11.88 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.18 | ▼ 09:30 equity $10,528.06 vs yday $10,539.18 (-11.12) | 09:30 open · cash $5.18 (unchanged overnight, no fees) · equity $10,528.06 vs prior close $10,539.18 (-11.12) · 2 name(s) re-marked at the open (per-name table). SSL×364 yday $14.59 → 09:30 $14.69 +36.40; ARLO×396 yday $13.19 → 09:30 $13.07 -47.52 | — |
| 2026-09-14 09:30 ET | **SELL** | `SSL` | 364 | $14.69 | $4.80 | $+114.27 | $5,347.54 | ▲ +114.27 after sell → book $10,523.26; vs 09:30 mark -4.80 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ARLO` | 396 | $13.07 | $5.22 | $-69.72 | $10,518.05 | ▼ -69.72 after sell → book $10,518.05; vs 09:30 mark -5.22 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,518.05 | ▲ close $10,518.05 vs 09:30 $10,528.06 (session +0.00) | 16:00 close · cash $10,518.05 · no lots left · equity $10,518.05. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,518.05 | ▲ 09:30 equity $10,518.05 vs yday $10,518.05 (-0.00) | 09:30 open · cash $10,518.05 · no holdings · equity $10,518.05 vs prior close $10,518.05 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,518.05 | ▲ close $10,518.05 vs 09:30 $10,518.05 (session +0.00) | 16:00 close · cash $10,518.05 · no lots left · equity $10,518.05. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,518.05 | ▲ 09:30 equity $10,518.05 vs yday $10,518.05 (-0.00) | 09:30 open · cash $10,518.05 · no holdings · equity $10,518.05 vs prior close $10,518.05 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,518.05 | ▲ close $10,518.05 vs 09:30 $10,518.05 (session +0.00) | 16:00 close · cash $10,518.05 · no lots left · equity $10,518.05. | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,518.05 | ▲ 09:30 equity $10,518.05 vs yday $10,518.05 (-0.00) | 09:30 open · cash $10,518.05 · no holdings · equity $10,518.05 vs prior close $10,518.05 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,518.05 | ▲ close $10,518.05 vs 09:30 $10,518.05 (session +0.00) | 16:00 close · cash $10,518.05 · no lots left · equity $10,518.05. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AURA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CIG` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CRDL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WB` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-31 | `TENB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `AME` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new buys |
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
