# Factor mine action — `union_clk_nr7_mom_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `hot_score` · size `leftover` · sell `list` · S-boost `none` · Clock-B #10 NR7 + moderate momentum

Cash book **+12.48%** ($11,248) · signal-only (no cash/fees) was +28.14%. Starts YES **26/26**. Fills 49 · skips 16 · realized $+1241.50.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how hot the prior tape looked.
- Must-have: Clock-B #10: prior NR7 compression plus moderate momentum.
- Must-not: Clock-B #5 long veto: extreme prior extension plus diminishing progress or a failed breakout.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Sort the keepers by how hot the prior tape looked and keep the top 8.
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
- **Gate** `clk_nr7_mom=True` · **rank** `hot_score` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $11.80.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `NPWR` | 3205 | — | $1.56 | +0.00 | $1.95 | +1249.95 | +1249.95 | +0.00 | +1249.95 |
| 2026-08-14 | `TMC` | 1171 | — | $4.22 | +0.00 | $4.01 | -245.91 | -245.91 | +0.00 | -245.91 |
| 2026-08-17 | `NPWR` | 3205 | $1.95 | $1.92 | -96.15 | — | +0.00 | -96.15 | +1153.80 | — |
| 2026-08-17 | `TMC` | 1171 | $4.01 | $4.05 | +46.84 | — | +0.00 | +46.84 | -199.07 | — |
| 2026-08-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `CTRE` | 272 | — | $39.79 | +0.00 | $39.76 | -8.16 | -8.16 | +0.00 | -8.16 |
| 2026-08-21 | `CTRE` | 272 | $39.76 | $40.00 | +65.28 | — | +0.00 | +65.28 | +57.12 | — |
| 2026-08-24 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-25 | `GRRR` | 260 | — | $13.92 | +0.00 | $14.04 | +31.20 | +31.20 | +0.00 | +31.20 |
| 2026-08-25 | `AMX` | 152 | — | $23.80 | +0.00 | $23.75 | -7.60 | -7.60 | +0.00 | -7.60 |
| 2026-08-25 | `SAN` | 247 | — | $14.65 | +0.00 | $14.63 | -4.94 | -4.94 | +0.00 | -4.94 |
| 2026-08-26 | `GRRR` | 260 | $14.04 | $14.03 | -2.60 | — | +0.00 | -2.60 | +28.60 | — |
| 2026-08-26 | `AMX` | 152 | $23.75 | $23.75 | +0.00 | — | +0.00 | +0.00 | -7.60 | — |
| 2026-08-26 | `SAN` | 247 | $14.63 | $14.82 | +46.93 | — | +0.00 | +46.93 | +41.99 | — |
| 2026-08-26 | `INO` | 2847 | — | $1.28 | +0.00 | $1.30 | +56.94 | +56.94 | +0.00 | +56.94 |
| 2026-08-26 | `HCA` | 8 | — | $427.50 | +0.00 | $427.16 | -2.72 | -2.72 | +0.00 | -2.72 |
| 2026-08-26 | `LI` | 300 | — | $12.14 | +0.00 | $12.14 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-27 | `INO` | 2847 | $1.30 | $1.29 | -28.47 | — | +0.00 | -28.47 | +28.47 | — |
| 2026-08-27 | `HCA` | 8 | $427.16 | $424.61 | -20.40 | — | +0.00 | -20.40 | -23.12 | — |
| 2026-08-27 | `LI` | 300 | $12.14 | $12.35 | +63.00 | — | +0.00 | +63.00 | +63.00 | — |
| 2026-08-27 | `DASH` | 7 | — | $235.94 | +0.00 | $231.89 | -28.35 | -28.35 | +0.00 | -28.35 |
| 2026-08-27 | `MRVL` | 7 | — | $253.44 | +0.00 | $241.45 | -83.93 | -83.93 | +0.00 | -83.93 |
| 2026-08-27 | `TD` | 15 | — | $120.17 | +0.00 | $121.09 | +13.80 | +13.80 | +0.00 | +13.80 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-27 | `GGB` | 398 | — | $4.57 | +0.00 | $4.70 | +51.74 | +51.74 | +0.00 | +51.74 |
| 2026-08-27 | `LRCX` | 5 | — | $318.88 | +0.00 | $318.58 | -1.50 | -1.50 | +0.00 | -1.50 |
| 2026-08-28 | `DASH` | 7 | $231.89 | $233.37 | +10.36 | — | +0.00 | +10.36 | -17.99 | — |
| 2026-08-28 | `MRVL` | 7 | $241.45 | $225.26 | -113.33 | — | +0.00 | -113.33 | -197.26 | — |
| 2026-08-28 | `TD` | 15 | $121.09 | $122.07 | +14.70 | — | +0.00 | +14.70 | +28.50 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `GGB` | 398 | $4.70 | $4.67 | -11.94 | — | +0.00 | -11.94 | +39.80 | — |
| 2026-08-28 | `LRCX` | 5 | $318.58 | $318.03 | -2.75 | — | +0.00 | -2.75 | -4.25 | — |
| 2026-08-28 | `MRNA` | 25 | — | $137.19 | +0.00 | $137.99 | +20.00 | +20.00 | +0.00 | +20.00 |
| 2026-08-28 | `MNRO` | 287 | — | $12.38 | +0.00 | $12.96 | +166.46 | +166.46 | +0.00 | +166.46 |
| 2026-08-28 | `MOS` | 148 | — | $23.95 | +0.00 | $23.60 | -51.80 | -51.80 | +0.00 | -51.80 |
| 2026-08-31 | `MRNA` | 25 | $137.99 | $134.10 | -97.25 | — | +0.00 | -97.25 | -77.25 | — |
| 2026-08-31 | `MNRO` | 287 | $12.96 | $12.77 | -54.53 | — | +0.00 | -54.53 | +111.93 | — |
| 2026-08-31 | `MOS` | 148 | $23.60 | $23.68 | +11.84 | — | +0.00 | +11.84 | -39.96 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-04 | `HAFN` | 1191 | — | $8.94 | +0.00 | $9.22 | +333.48 | +333.48 | +0.00 | +333.48 |
| 2026-09-08 | `HAFN` | 1191 | $9.22 | $8.81 | -488.31 | — | +0.00 | -488.31 | -154.83 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `VLO` | 9 | — | $388.00 | +0.00 | $390.42 | +21.78 | +21.78 | +0.00 | +21.78 |
| 2026-09-11 | `DRVN` | 278 | — | $12.55 | +0.00 | $12.15 | -111.20 | -111.20 | +0.00 | -111.20 |
| 2026-09-11 | `CIM` | 310 | — | $11.24 | +0.00 | $11.13 | -34.10 | -34.10 | +0.00 | -34.10 |
| 2026-09-14 | `VLO` | 9 | $390.42 | $395.26 | +43.56 | $382.95 | -110.79 | -67.23 | +65.34 | -45.45 |
| 2026-09-14 | `DRVN` | 278 | $12.15 | $12.33 | +50.04 | — | +0.00 | +50.04 | -61.16 | — |
| 2026-09-14 | `CIM` | 310 | $11.13 | $11.12 | -3.10 | — | +0.00 | -3.10 | -37.20 | — |
| 2026-09-15 | `VLO` | 9 | $382.95 | $383.51 | +5.04 | $397.04 | +121.77 | +126.81 | -40.41 | +81.36 |
| 2026-09-16 | `VLO` | 9 | $397.04 | $391.68 | -48.24 | $403.28 | +104.40 | +56.16 | +33.12 | +137.52 |
| 2026-09-17 | `VLO` | 9 | $403.28 | $398.45 | -43.47 | — | +0.00 | -43.47 | +94.05 | — |
| 2026-09-17 | `BNC` | 1039 | — | $5.03 | +0.00 | $5.42 | +405.21 | +405.21 | +0.00 | +405.21 |
| 2026-09-17 | `KNX` | 78 | — | $66.85 | +0.00 | $66.67 | -14.04 | -14.04 | +0.00 | -14.04 |
| 2026-09-18 | `BNC` | 1039 | $5.42 | $5.83 | +425.99 | — | +0.00 | +425.99 | +831.20 | — |
| 2026-09-18 | `KNX` | 78 | $66.67 | $66.65 | -1.56 | — | +0.00 | -1.56 | -15.60 | — |
| 2026-09-18 | `DRVN` | 915 | — | $12.26 | +0.00 | $12.28 | +18.30 | +18.30 | +0.00 | +18.30 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +1,004.04 | NPWR, TMC | — | $2.13 | $10,947.59 | NPWR×3205, TMC×1171 |
| 2026-08-17 | +2.25 | $2.13 | NPWR×3205, TMC×1171 | $10,898.28 | -49.31 | +0.00 | — | NPWR, TMC | $10,841.02 | $10,841.02 | — |
| 2026-08-18 | -6.20 | $10,841.02 | — | $10,841.02 | -0.00 | +0.00 | — | — | $10,841.02 | $10,841.02 | — |
| 2026-08-19 | -7.20 | $10,841.02 | — | $10,841.02 | -0.00 | +0.00 | — | — | $10,841.02 | $10,841.02 | — |
| 2026-08-20 | +1.12 | $10,841.02 | — | $10,841.02 | -0.00 | -8.16 | CTRE | — | $14.63 | $10,829.35 | CTRE×272 |
| 2026-08-21 | +3.25 | $14.63 | CTRE×272 | $10,894.63 | +65.28 | +0.00 | — | CTRE | $10,890.99 | $10,890.99 | — |
| 2026-08-24 | -5.17 | $10,890.99 | — | $10,890.99 | -0.00 | +0.00 | — | — | $10,890.99 | $10,890.99 | — |
| 2026-08-25 | +1.80 | $10,890.99 | — | $10,890.99 | -0.00 | +18.66 | GRRR, AMX, SAN | — | $26.65 | $10,900.66 | GRRR×260, AMX×152, SAN×247 |
| 2026-08-26 | +2.02 | $26.65 | GRRR×260, AMX×152, SAN×247 | $10,944.99 | +44.33 | +54.22 | INO, HCA, LI | GRRR, AMX, SAN | $187.04 | $10,947.42 | INO×2847, HCA×8, LI×300 |
| 2026-08-27 | — | $187.04 | INO×2847, HCA×8, LI×300 | $10,961.55 | +14.13 | -79.86 | DASH, MRVL, TD, MU, GGB, LRCX | INO, HCA, LI | $1,294.65 | $10,823.27 | DASH×7, MRVL×7, TD×15, MU×1, GGB×398, LRCX×5 |
| 2026-08-28 | +0.75 | $1,294.65 | DASH×7, MRVL×7, TD×15, MU×1, GGB×398, LRCX×5 | $10,704.21 | -119.06 | +134.66 | MRNA, MNRO, MOS | DASH, MRVL, TD, MU, GGB, LRCX | $153.21 | $10,815.28 | MRNA×25, MNRO×287, MOS×148 |
| 2026-08-31 | -5.85 | $153.21 | MRNA×25, MNRO×287, MOS×148 | $10,675.34 | -139.94 | +0.00 | — | MRNA, MNRO, MOS | $10,666.98 | $10,666.98 | — |
| 2026-09-01 | -6.30 | $10,666.98 | — | $10,666.98 | -0.00 | +0.00 | — | — | $10,666.98 | $10,666.98 | — |
| 2026-09-02 | -3.83 | $10,666.98 | — | $10,666.98 | -0.00 | +0.00 | — | — | $10,666.98 | $10,666.98 | — |
| 2026-09-03 | -0.90 | $10,666.98 | — | $10,666.98 | -0.00 | +0.00 | — | — | $10,666.98 | $10,666.98 | — |
| 2026-09-04 | +2.25 | $10,666.98 | — | $10,666.98 | -0.00 | +333.48 | HAFN | — | $4.07 | $10,985.09 | HAFN×1191 |
| 2026-09-08 | -11.47 | $4.07 | HAFN×1191 | $10,496.78 | -488.31 | +0.00 | — | HAFN | $10,481.14 | $10,481.14 | — |
| 2026-09-09 | -13.95 | $10,481.14 | — | $10,481.14 | -0.00 | +0.00 | — | — | $10,481.14 | $10,481.14 | — |
| 2026-09-10 | -13.28 | $10,481.14 | — | $10,481.14 | -0.00 | +0.00 | — | — | $10,481.14 | $10,481.14 | — |
| 2026-09-11 | +0.50 | $10,481.14 | — | $10,481.14 | -0.00 | -123.52 | VLO, DRVN, CIM | — | $6.23 | $10,348.01 | VLO×9, DRVN×278, CIM×310 |
| 2026-09-14 | -11.00 | $6.23 | VLO×9, DRVN×278, CIM×310 | $10,438.51 | +90.50 | -110.79 | — | DRVN, CIM | $6,873.44 | $10,319.99 | VLO×9 |
| 2026-09-15 | -3.84 | $6,873.44 | VLO×9 | $10,325.03 | +5.04 | +121.77 | — | — | $6,873.44 | $10,446.80 | VLO×9 |
| 2026-09-16 | +5.30 | $6,873.44 | VLO×9 | $10,398.56 | -48.24 | +104.40 | — | — | $6,873.44 | $10,502.96 | VLO×9 |
| 2026-09-17 | +7.38 | $6,873.44 | VLO×9 | $10,459.49 | -43.47 | +391.17 | BNC, KNX | VLO | $1.33 | $10,832.97 | BNC×1039, KNX×78 |
| 2026-09-18 | +4.86 | $1.33 | BNC×1039, KNX×78 | $11,257.40 | +424.43 | +18.30 | DRVN | BNC, KNX | $11.80 | $11,248.00 | DRVN×915 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `NPWR` | 3205 | $1.56 | $41.34 | — | $4,958.86 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list earn_react; 🔵; ret5=+8.0; leftover $5000.00 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `TMC` | 1171 | $4.22 | $15.11 | — | $2.13 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list earn_react; 🔵; ret5=+7.0; leftover $5000.00 | join🟢 sector🔴 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.13 | ▲ close $10,947.59 vs 09:30 $10,000.00 (session +1,004.04) | 16:00 close · cash $2.13 · equity $10,947.59 vs 09:30 $10,000.00 (+947.59; session marks +1004.04) · 2 name(s) marked open→close (per-name table). NPWR×3205 09:30 $1.56 → close $1.95 +1249.95; TMC×1171 09:30 $4.22 → close $4.01 -245.91 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.13 | ▼ 09:30 equity $10,898.28 vs yday $10,947.59 (-49.31) | 09:30 open · cash $2.13 (unchanged overnight, no fees) · equity $10,898.28 vs prior close $10,947.59 (-49.31) · 2 name(s) re-marked at the open (per-name table). NPWR×3205 yday $1.95 → 09:30 $1.92 -96.15; TMC×1171 yday $4.01 → 09:30 $4.05 +46.84 | — |
| 2026-08-17 09:30 ET | **SELL** | `NPWR` | 3205 | $1.92 | $41.93 | $+1070.53 | $6,113.80 | ▲ +1,070.53 after sell → book $10,856.35; vs 09:30 mark -41.93 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `TMC` | 1171 | $4.05 | $15.34 | $-229.51 | $10,841.02 | ▼ -229.51 after sell → book $10,841.02; vs 09:30 mark -15.33 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,841.02 | ▲ close $10,841.02 vs 09:30 $10,898.28 (session +0.00) | 16:00 close · cash $10,841.02 · no lots left · equity $10,841.02. | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,841.02 | ▲ 09:30 equity $10,841.02 vs yday $10,841.02 (-0.00) | 09:30 open · cash $10,841.02 · no holdings · equity $10,841.02 vs prior close $10,841.02 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,841.02 | ▲ close $10,841.02 vs 09:30 $10,841.02 (session +0.00) | 16:00 close · cash $10,841.02 · no lots left · equity $10,841.02. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,841.02 | ▲ 09:30 equity $10,841.02 vs yday $10,841.02 (-0.00) | 09:30 open · cash $10,841.02 · no holdings · equity $10,841.02 vs prior close $10,841.02 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,841.02 | ▲ close $10,841.02 vs 09:30 $10,841.02 (session +0.00) | 16:00 close · cash $10,841.02 · no lots left · equity $10,841.02. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,841.02 | ▲ 09:30 equity $10,841.02 vs yday $10,841.02 (-0.00) | 09:30 open · cash $10,841.02 · no holdings · equity $10,841.02 vs prior close $10,841.02 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `CTRE` | 272 | $39.79 | $3.51 | — | $14.63 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list oppset; 🔵; ret5=+1.7; leftover $10841.02 | join🟢 sector🔴 gen🟢 news🟡 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.63 | ▼ close $10,829.35 vs 09:30 $10,841.02 (session -8.16) | 16:00 close · cash $14.63 · equity $10,829.35 vs 09:30 $10,841.02 (-11.67; session marks -8.16) · 1 name(s) marked open→close (per-name table). CTRE×272 09:30 $39.79 → close $39.76 -8.16 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.63 | ▲ 09:30 equity $10,894.63 vs yday $10,829.35 (+65.28) | 09:30 open · cash $14.63 (unchanged overnight, no fees) · equity $10,894.63 vs prior close $10,829.35 (+65.28) · 1 name(s) re-marked at the open (per-name table). CTRE×272 yday $39.76 → 09:30 $40.00 +65.28 | — |
| 2026-08-21 09:30 ET | **SELL** | `CTRE` | 272 | $40.00 | $3.64 | $+49.97 | $10,890.99 | ▲ +49.97 after sell → book $10,890.99; vs 09:30 mark -3.64 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,890.99 | ▲ close $10,890.99 vs 09:30 $10,894.63 (session +0.00) | 16:00 close · cash $10,890.99 · no lots left · equity $10,890.99. | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,890.99 | ▲ 09:30 equity $10,890.99 vs yday $10,890.99 (-0.00) | 09:30 open · cash $10,890.99 · no holdings · equity $10,890.99 vs prior close $10,890.99 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,890.99 | ▲ close $10,890.99 vs 09:30 $10,890.99 (session +0.00) | 16:00 close · cash $10,890.99 · no lots left · equity $10,890.99. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,890.99 | ▲ 09:30 equity $10,890.99 vs yday $10,890.99 (-0.00) | 09:30 open · cash $10,890.99 · no holdings · equity $10,890.99 vs prior close $10,890.99 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 260 | $13.92 | $3.35 | — | $7,268.43 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list earn_react; 🔵; ret5=+5.9; leftover $3630.33 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AMX` | 152 | $23.80 | $2.45 | — | $3,648.39 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list oppset; 🔵; ret5=+0.5; leftover $3630.33 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SAN` | 247 | $14.65 | $3.19 | — | $26.65 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list oppset; 🔵; ret5=+0.9; leftover $3630.33 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26.65 | ▲ close $10,900.66 vs 09:30 $10,890.99 (session +18.66) | 16:00 close · cash $26.65 · equity $10,900.66 vs 09:30 $10,890.99 (+9.67; session marks +18.66) · 3 name(s) marked open→close (per-name table). GRRR×260 09:30 $13.92 → close $14.04 +31.20; AMX×152 09:30 $23.80 → close $23.75 -7.60; SAN×247 09:30 $14.65 → close $14.63 -4.94 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26.65 | ▲ 09:30 equity $10,944.99 vs yday $10,900.66 (+44.33) | 09:30 open · cash $26.65 (unchanged overnight, no fees) · equity $10,944.99 vs prior close $10,900.66 (+44.33) · 3 name(s) re-marked at the open (per-name table). GRRR×260 yday $14.04 → 09:30 $14.03 -2.60; AMX×152 yday $23.75 → 09:30 $23.75 +0.00; SAN×247 yday $14.63 → 09:30 $14.82 +46.93 | — |
| 2026-08-26 09:30 ET | **SELL** | `GRRR` | 260 | $14.03 | $3.43 | $+21.82 | $3,671.02 | ▲ +21.82 after sell → book $10,941.56; vs 09:30 mark -3.43 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `AMX` | 152 | $23.75 | $2.50 | $-12.55 | $7,278.52 | ▼ -12.55 after sell → book $10,939.06; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `SAN` | 247 | $14.82 | $3.26 | $+35.55 | $10,935.81 | ▲ +35.55 after sell → book $10,935.81; vs 09:30 mark -3.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `INO` | 2847 | $1.28 | $36.73 | — | $7,254.92 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list ohlc_hot; 🔵; ret5=+7.5; leftover $3645.27 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `HCA` | 8 | $427.50 | $2.01 | — | $3,832.91 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list flatten; ret5=+4.1; leftover $3645.27 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `LI` | 300 | $12.14 | $3.87 | — | $187.04 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list earn_react; ret5=+1.2; leftover $3645.27 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $187.04 | ▲ close $10,947.42 vs 09:30 $10,944.99 (session +54.22) | 16:00 close · cash $187.04 · equity $10,947.42 vs 09:30 $10,944.99 (+2.43; session marks +54.22) · 3 name(s) marked open→close (per-name table). INO×2847 09:30 $1.28 → close $1.30 +56.94; HCA×8 09:30 $427.50 → close $427.16 -2.72; LI×300 09:30 $12.14 → close $12.14 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $187.04 | ▲ 09:30 equity $10,961.55 vs yday $10,947.42 (+14.13) | 09:30 open · cash $187.04 (unchanged overnight, no fees) · equity $10,961.55 vs prior close $10,947.42 (+14.13) · 3 name(s) re-marked at the open (per-name table). INO×2847 yday $1.30 → 09:30 $1.29 -28.47; HCA×8 yday $427.16 → 09:30 $424.61 -20.40; LI×300 yday $12.14 → 09:30 $12.35 +63.00 | — |
| 2026-08-27 09:30 ET | **SELL** | `INO` | 2847 | $1.29 | $37.23 | $-45.48 | $3,822.44 | ▼ -45.48 after sell → book $10,924.32; vs 09:30 mark -37.23 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 8 | $424.61 | $2.05 | $-27.19 | $7,217.27 | ▼ -27.19 after sell → book $10,922.27; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `LI` | 300 | $12.35 | $3.95 | $+55.18 | $10,918.32 | ▲ +55.18 after sell → book $10,918.32; vs 09:30 mark -3.95 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `DASH` | 7 | $235.94 | $2.01 | — | $9,264.73 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list ohlc_hot; ret5=+7.6; leftover $1819.72 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 7 | $253.44 | $2.01 | — | $7,488.64 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list mover_buy; 🔵; ret5=+3.3; leftover $1819.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `TD` | 15 | $120.17 | $2.04 | — | $5,684.05 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list earn_react; ret5=+0.9; leftover $1819.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $4,715.05 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list mover_buy; 🔵; ret5=+0.1; leftover $1819.72 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 398 | $4.57 | $5.13 | — | $2,891.05 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list mover_buy; 🔵; ret5=+1.1; leftover $1819.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 5 | $318.88 | $2.00 | — | $1,294.65 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list mover_buy; 🔵; ret5=+1.9; leftover $1819.72 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,294.65 | ▼ close $10,823.27 vs 09:30 $10,961.55 (session -79.86) | 16:00 close · cash $1,294.65 · equity $10,823.27 vs 09:30 $10,961.55 (-138.28; session marks -79.86) · 6 name(s) marked open→close (per-name table). DASH×7 09:30 $235.94 → close $231.89 -28.35; MRVL×7 09:30 $253.44 → close $241.45 -83.93; TD×15 09:30 $120.17 → close $121.09 +13.80; MU×1 09:30 $967.01 → close $935.39 -31.62; GGB×398 09:30 $4.57 → close $4.70 +51.74; LRCX×5 09:30 $318.88 → close $318.58 -1.50 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,294.65 | ▼ 09:30 equity $10,704.21 vs yday $10,823.27 (-119.06) | 09:30 open · cash $1,294.65 (unchanged overnight, no fees) · equity $10,704.21 vs prior close $10,823.27 (-119.06) · 6 name(s) re-marked at the open (per-name table). DASH×7 yday $231.89 → 09:30 $233.37 +10.36; MRVL×7 yday $241.45 → 09:30 $225.26 -113.33; TD×15 yday $121.09 → 09:30 $122.07 +14.70; MU×1 yday $935.39 → 09:30 $919.29 -16.10; GGB×398 yday $4.70 → 09:30 $4.67 -11.94; LRCX×5 yday $318.58 → 09:30 $318.03 -2.75 | — |
| 2026-08-28 09:30 ET | **SELL** | `DASH` | 7 | $233.37 | $2.03 | $-22.04 | $2,926.20 | ▼ -22.04 after sell → book $10,702.17; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MRVL` | 7 | $225.26 | $2.03 | $-201.30 | $4,500.99 | ▼ -201.30 after sell → book $10,700.14; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `TD` | 15 | $122.07 | $2.06 | $+24.41 | $6,329.98 | ▲ +24.41 after sell → book $10,698.08; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $7,247.26 | ▼ -51.73 after sell → book $10,696.07; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 398 | $4.67 | $5.22 | $+29.45 | $9,100.70 | ▲ +29.45 after sell → book $10,690.85; vs 09:30 mark -5.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 5 | $318.03 | $2.03 | $-8.28 | $10,688.82 | ▼ -8.28 after sell → book $10,688.82; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `MRNA` | 25 | $137.19 | $2.06 | — | $7,257.01 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list ohlc_hot; ret5=+7.1; leftover $3562.94 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MNRO` | 287 | $12.38 | $3.70 | — | $3,700.25 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list yday_mover; ret5=+2.4; leftover $3562.94 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MOS` | 148 | $23.95 | $2.43 | — | $153.21 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list flatten; ret5=+1.8; leftover $3562.94 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $153.21 | ▲ close $10,815.28 vs 09:30 $10,704.21 (session +134.66) | 16:00 close · cash $153.21 · equity $10,815.28 vs 09:30 $10,704.21 (+111.07; session marks +134.66) · 3 name(s) marked open→close (per-name table). MRNA×25 09:30 $137.19 → close $137.99 +20.00; MNRO×287 09:30 $12.38 → close $12.96 +166.46; MOS×148 09:30 $23.95 → close $23.60 -51.80 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $153.21 | ▼ 09:30 equity $10,675.34 vs yday $10,815.28 (-139.94) | 09:30 open · cash $153.21 (unchanged overnight, no fees) · equity $10,675.34 vs prior close $10,815.28 (-139.94) · 3 name(s) re-marked at the open (per-name table). MRNA×25 yday $137.99 → 09:30 $134.10 -97.25; MNRO×287 yday $12.96 → 09:30 $12.77 -54.53; MOS×148 yday $23.60 → 09:30 $23.68 +11.84 | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 25 | $134.10 | $2.10 | $-81.42 | $3,503.61 | ▼ -81.42 after sell → book $10,673.24; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `MNRO` | 287 | $12.77 | $3.78 | $+104.45 | $7,164.82 | ▲ +104.45 after sell → book $10,669.46; vs 09:30 mark -3.78 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 148 | $23.68 | $2.49 | $-44.88 | $10,666.98 | ▼ -44.88 after sell → book $10,666.98; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,666.98 | ▲ close $10,666.98 vs 09:30 $10,675.34 (session +0.00) | 16:00 close · cash $10,666.98 · no lots left · equity $10,666.98. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,666.98 | ▲ 09:30 equity $10,666.98 vs yday $10,666.98 (-0.00) | 09:30 open · cash $10,666.98 · no holdings · equity $10,666.98 vs prior close $10,666.98 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,666.98 | ▲ close $10,666.98 vs 09:30 $10,666.98 (session +0.00) | 16:00 close · cash $10,666.98 · no lots left · equity $10,666.98. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,666.98 | ▲ 09:30 equity $10,666.98 vs yday $10,666.98 (-0.00) | 09:30 open · cash $10,666.98 · no holdings · equity $10,666.98 vs prior close $10,666.98 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,666.98 | ▲ close $10,666.98 vs 09:30 $10,666.98 (session +0.00) | 16:00 close · cash $10,666.98 · no lots left · equity $10,666.98. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,666.98 | ▲ 09:30 equity $10,666.98 vs yday $10,666.98 (-0.00) | 09:30 open · cash $10,666.98 · no holdings · equity $10,666.98 vs prior close $10,666.98 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,666.98 | ▲ close $10,666.98 vs 09:30 $10,666.98 (session +0.00) | 16:00 close · cash $10,666.98 · no lots left · equity $10,666.98. | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,666.98 | ▲ 09:30 equity $10,666.98 vs yday $10,666.98 (-0.00) | 09:30 open · cash $10,666.98 · no holdings · equity $10,666.98 vs prior close $10,666.98 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-04 09:30 ET | **BUY** | `HAFN` | 1191 | $8.94 | $15.36 | — | $4.07 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list ohlc_hot; ret5=+7.7; leftover $10666.98 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.07 | ▲ close $10,985.09 vs 09:30 $10,666.98 (session +333.48) | 16:00 close · cash $4.07 · equity $10,985.09 vs 09:30 $10,666.98 (+318.11; session marks +333.48) · 1 name(s) marked open→close (per-name table). HAFN×1191 09:30 $8.94 → close $9.22 +333.48 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.07 | ▼ 09:30 equity $10,496.78 vs yday $10,985.09 (-488.31) | 09:30 open · cash $4.07 (unchanged overnight, no fees) · equity $10,496.78 vs prior close $10,985.09 (-488.31) · 1 name(s) re-marked at the open (per-name table). HAFN×1191 yday $9.22 → 09:30 $8.81 -488.31 | — |
| 2026-09-08 09:30 ET | **SELL** | `HAFN` | 1191 | $8.81 | $15.65 | $-185.84 | $10,481.14 | ▼ -185.84 after sell → book $10,481.14; vs 09:30 mark -15.64 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,481.14 | ▲ close $10,481.14 vs 09:30 $10,496.78 (session +0.00) | 16:00 close · cash $10,481.14 · no lots left · equity $10,481.14. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,481.14 | ▲ 09:30 equity $10,481.14 vs yday $10,481.14 (-0.00) | 09:30 open · cash $10,481.14 · no holdings · equity $10,481.14 vs prior close $10,481.14 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,481.14 | ▲ close $10,481.14 vs 09:30 $10,481.14 (session +0.00) | 16:00 close · cash $10,481.14 · no lots left · equity $10,481.14. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,481.14 | ▲ 09:30 equity $10,481.14 vs yday $10,481.14 (-0.00) | 09:30 open · cash $10,481.14 · no holdings · equity $10,481.14 vs prior close $10,481.14 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,481.14 | ▲ close $10,481.14 vs 09:30 $10,481.14 (session +0.00) | 16:00 close · cash $10,481.14 · no lots left · equity $10,481.14. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,481.14 | ▲ 09:30 equity $10,481.14 vs yday $10,481.14 (-0.00) | 09:30 open · cash $10,481.14 · no holdings · equity $10,481.14 vs prior close $10,481.14 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `VLO` | 9 | $388.00 | $2.02 | — | $6,987.12 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list ohlc_hot; ret5=+6.7; leftover $3493.71 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `DRVN` | 278 | $12.55 | $3.59 | — | $3,494.63 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list oppset; ret5=+6.4; leftover $3493.71 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CIM` | 310 | $11.24 | $4.00 | — | $6.23 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list oppset; 🔵; ret5=+1.1; leftover $3493.71 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.23 | ▼ close $10,348.01 vs 09:30 $10,481.14 (session -123.52) | 16:00 close · cash $6.23 · equity $10,348.01 vs 09:30 $10,481.14 (-133.13; session marks -123.52) · 3 name(s) marked open→close (per-name table). VLO×9 09:30 $388.00 → close $390.42 +21.78; DRVN×278 09:30 $12.55 → close $12.15 -111.20; CIM×310 09:30 $11.24 → close $11.13 -34.10 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.23 | ▲ 09:30 equity $10,438.51 vs yday $10,348.01 (+90.50) | 09:30 open · cash $6.23 (unchanged overnight, no fees) · equity $10,438.51 vs prior close $10,348.01 (+90.50) · 3 name(s) re-marked at the open (per-name table). VLO×9 yday $390.42 → 09:30 $395.26 +43.56; DRVN×278 yday $12.15 → 09:30 $12.33 +50.04; CIM×310 yday $11.13 → 09:30 $11.12 -3.10 | — |
| 2026-09-14 09:30 ET | **SELL** | `DRVN` | 278 | $12.33 | $3.66 | $-68.41 | $3,430.31 | ▼ -68.41 after sell → book $10,434.85; vs 09:30 mark -3.66 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CIM` | 310 | $11.12 | $4.08 | $-45.28 | $6,873.44 | ▼ -45.28 after sell → book $10,430.78; vs 09:30 mark -4.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,873.44 | ▼ close $10,319.99 vs 09:30 $10,438.51 (session -110.79) | 16:00 close · cash $6,873.44 · equity $10,319.99 vs 09:30 $10,438.51 (-118.52; session marks -110.79) · 1 name(s) marked open→close (per-name table). VLO×9 09:30 $395.26 → close $382.95 -110.79 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,873.44 | ▲ 09:30 equity $10,325.03 vs yday $10,319.99 (+5.04) | 09:30 open · cash $6,873.44 (unchanged overnight, no fees) · equity $10,325.03 vs prior close $10,319.99 (+5.04) · 1 name(s) re-marked at the open (per-name table). VLO×9 yday $382.95 → 09:30 $383.51 +5.04 | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,873.44 | ▲ close $10,446.80 vs 09:30 $10,325.03 (session +121.77) | 16:00 close · cash $6,873.44 · equity $10,446.80 vs 09:30 $10,325.03 (+121.77; session marks +121.77) · 1 name(s) marked open→close (per-name table). VLO×9 09:30 $383.51 → close $397.04 +121.77 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,873.44 | ▼ 09:30 equity $10,398.56 vs yday $10,446.80 (-48.24) | 09:30 open · cash $6,873.44 (unchanged overnight, no fees) · equity $10,398.56 vs prior close $10,446.80 (-48.24) · 1 name(s) re-marked at the open (per-name table). VLO×9 yday $397.04 → 09:30 $391.68 -48.24 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,873.44 | ▲ close $10,502.96 vs 09:30 $10,398.56 (session +104.40) | 16:00 close · cash $6,873.44 · equity $10,502.96 vs 09:30 $10,398.56 (+104.40; session marks +104.40) · 1 name(s) marked open→close (per-name table). VLO×9 09:30 $391.68 → close $403.28 +104.40 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,873.44 | ▼ 09:30 equity $10,459.49 vs yday $10,502.96 (-43.47) | 09:30 open · cash $6,873.44 (unchanged overnight, no fees) · equity $10,459.49 vs prior close $10,502.96 (-43.47) · 1 name(s) re-marked at the open (per-name table). VLO×9 yday $403.28 → 09:30 $398.45 -43.47 | — |
| 2026-09-17 09:30 ET | **SELL** | `VLO` | 9 | $398.45 | $2.06 | $+89.98 | $10,457.43 | ▲ +89.98 after sell → book $10,457.43; vs 09:30 mark -2.06 | dropped from list after 4 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `BNC` | 1039 | $5.03 | $13.40 | — | $5,217.86 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list ohlc_hot; ret5=+7.4; leftover $5228.72 | join🟡 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `KNX` | 78 | $66.85 | $2.22 | — | $1.33 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list oppset; 🔵; ret5=+3.3; leftover $5228.72 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.33 | ▲ close $10,832.97 vs 09:30 $10,459.49 (session +391.17) | 16:00 close · cash $1.33 · equity $10,832.97 vs 09:30 $10,459.49 (+373.48; session marks +391.17) · 2 name(s) marked open→close (per-name table). BNC×1039 09:30 $5.03 → close $5.42 +405.21; KNX×78 09:30 $66.85 → close $66.67 -14.04 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.33 | ▲ 09:30 equity $11,257.40 vs yday $10,832.97 (+424.43) | 09:30 open · cash $1.33 (unchanged overnight, no fees) · equity $11,257.40 vs prior close $10,832.97 (+424.43) · 2 name(s) re-marked at the open (per-name table). BNC×1039 yday $5.42 → 09:30 $5.83 +425.99; KNX×78 yday $66.67 → 09:30 $66.65 -1.56 | — |
| 2026-09-18 09:30 ET | **SELL** | `BNC` | 1039 | $5.83 | $13.62 | $+804.17 | $6,045.08 | ▲ +804.17 after sell → book $11,243.78; vs 09:30 mark -13.62 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **SELL** | `KNX` | 78 | $66.65 | $2.28 | $-20.10 | $11,241.50 | ▼ -20.10 after sell → book $11,241.50; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `DRVN` | 915 | $12.26 | $11.80 | — | $11.80 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list oppset; 🔵; ⚪; ret5=+6.4; leftover $11241.50 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.80 | ▲ close $11,248.00 vs 09:30 $11,257.40 (session +18.30) | 16:00 close · cash $11.80 · equity $11,248.00 vs 09:30 $11,257.40 (-9.40; session marks +18.30) · 1 name(s) marked open→close (per-name table). DRVN×915 09:30 $12.26 → close $12.28 +18.30 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ALAB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ANET` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CRK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-08 | `LOGI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `GEMI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BSBR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `VLO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OPFI` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `TECK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TECK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `GRNT` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `DRVN` | 915 | 2026-09-18 @ $12.26 | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list oppset; 🔵; ⚪; ret5=+6.4; leftover $11241.50 |
