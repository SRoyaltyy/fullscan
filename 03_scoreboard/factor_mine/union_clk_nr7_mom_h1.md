# Factor mine action — `union_clk_nr7_mom_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `hot_score` · size `leftover` · sell `list` · S-boost `none` · Clock-B #10 NR7 + moderate momentum

Cash book **+11.56%** ($11,156) · signal-only (no cash/fees) was +24.10%. Starts YES **25/27**. Fills 43 · skips 11 · realized $+1641.09.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $24.48.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `DUOT` | 353 | — | $9.43 | +0.00 | $9.11 | -112.96 | -112.96 | +0.00 | -112.96 |
| 2026-08-14 | `NPWR` | 2136 | — | $1.56 | +0.00 | $1.95 | +833.04 | +833.04 | +0.00 | +833.04 |
| 2026-08-14 | `TMC` | 781 | — | $4.22 | +0.00 | $4.01 | -164.01 | -164.01 | +0.00 | -164.01 |
| 2026-08-17 | `DUOT` | 353 | $9.11 | $10.35 | +437.72 | — | +0.00 | +437.72 | +324.76 | — |
| 2026-08-17 | `NPWR` | 2136 | $1.95 | $1.92 | -64.08 | — | +0.00 | -64.08 | +768.96 | — |
| 2026-08-17 | `TMC` | 781 | $4.01 | $4.05 | +31.24 | — | +0.00 | +31.24 | -132.77 | — |
| 2026-08-17 | `IQ` | 7979 | — | $1.35 | +0.00 | $1.33 | -159.58 | -159.58 | +0.00 | -159.58 |
| 2026-08-18 | `IQ` | 7979 | $1.33 | $1.27 | -478.74 | — | +0.00 | -478.74 | -638.32 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-21 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-24 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-25 | `SJM` | 40 | — | $123.87 | +0.00 | $125.45 | +63.20 | +63.20 | +0.00 | +63.20 |
| 2026-08-25 | `GRRR` | 360 | — | $13.92 | +0.00 | $14.04 | +43.20 | +43.20 | +0.00 | +43.20 |
| 2026-08-26 | `SJM` | 40 | $125.45 | $134.80 | +374.00 | — | +0.00 | +374.00 | +437.20 | — |
| 2026-08-26 | `GRRR` | 360 | $14.04 | $14.03 | -3.60 | — | +0.00 | -3.60 | +39.60 | — |
| 2026-08-26 | `INO` | 2732 | — | $1.28 | +0.00 | $1.30 | +54.64 | +54.64 | +0.00 | +54.64 |
| 2026-08-26 | `HCA` | 8 | — | $427.50 | +0.00 | $427.16 | -2.72 | -2.72 | +0.00 | -2.72 |
| 2026-08-26 | `LI` | 288 | — | $12.14 | +0.00 | $12.14 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-27 | `INO` | 2732 | $1.30 | $1.29 | -27.32 | — | +0.00 | -27.32 | +27.32 | — |
| 2026-08-27 | `HCA` | 8 | $427.16 | $424.61 | -20.40 | — | +0.00 | -20.40 | -23.12 | — |
| 2026-08-27 | `LI` | 288 | $12.14 | $12.35 | +60.48 | — | +0.00 | +60.48 | +60.48 | — |
| 2026-08-27 | `DASH` | 6 | — | $235.94 | +0.00 | $231.89 | -24.30 | -24.30 | +0.00 | -24.30 |
| 2026-08-27 | `MRVL` | 5 | — | $253.44 | +0.00 | $241.45 | -59.95 | -59.95 | +0.00 | -59.95 |
| 2026-08-27 | `BBAR` | 100 | — | $14.96 | +0.00 | $14.60 | -36.00 | -36.00 | +0.00 | -36.00 |
| 2026-08-27 | `TD` | 12 | — | $120.17 | +0.00 | $121.09 | +11.04 | +11.04 | +0.00 | +11.04 |
| 2026-08-27 | `MU` | 1 | — | $967.01 | +0.00 | $935.39 | -31.62 | -31.62 | +0.00 | -31.62 |
| 2026-08-27 | `GGB` | 327 | — | $4.57 | +0.00 | $4.70 | +42.51 | +42.51 | +0.00 | +42.51 |
| 2026-08-27 | `LRCX` | 4 | — | $318.88 | +0.00 | $318.58 | -1.20 | -1.20 | +0.00 | -1.20 |
| 2026-08-28 | `DASH` | 6 | $231.89 | $233.37 | +8.88 | — | +0.00 | +8.88 | -15.42 | — |
| 2026-08-28 | `MRVL` | 5 | $241.45 | $225.26 | -80.95 | — | +0.00 | -80.95 | -140.90 | — |
| 2026-08-28 | `BBAR` | 100 | $14.60 | $15.01 | +41.00 | — | +0.00 | +41.00 | +5.00 | — |
| 2026-08-28 | `TD` | 12 | $121.09 | $122.07 | +11.76 | — | +0.00 | +11.76 | +22.80 | — |
| 2026-08-28 | `MU` | 1 | $935.39 | $919.29 | -16.10 | — | +0.00 | -16.10 | -47.72 | — |
| 2026-08-28 | `GGB` | 327 | $4.70 | $4.67 | -9.81 | — | +0.00 | -9.81 | +32.70 | — |
| 2026-08-28 | `LRCX` | 4 | $318.58 | $318.03 | -2.20 | — | +0.00 | -2.20 | -3.40 | — |
| 2026-08-28 | `MRNA` | 25 | — | $137.19 | +0.00 | $137.99 | +20.00 | +20.00 | +0.00 | +20.00 |
| 2026-08-28 | `MNRO` | 277 | — | $12.38 | +0.00 | $12.96 | +160.66 | +160.66 | +0.00 | +160.66 |
| 2026-08-28 | `MOS` | 143 | — | $23.95 | +0.00 | $23.60 | -50.05 | -50.05 | +0.00 | -50.05 |
| 2026-08-31 | `MRNA` | 25 | $137.99 | $134.10 | -97.25 | — | +0.00 | -97.25 | -77.25 | — |
| 2026-08-31 | `MNRO` | 277 | $12.96 | $12.77 | -52.63 | — | +0.00 | -52.63 | +108.03 | — |
| 2026-08-31 | `MOS` | 143 | $23.60 | $23.68 | +11.44 | — | +0.00 | +11.44 | -38.61 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-04 | `HAFN` | 1147 | — | $8.94 | +0.00 | $9.22 | +321.16 | +321.16 | +0.00 | +321.16 |
| 2026-09-08 | `HAFN` | 1147 | $9.22 | $8.81 | -470.27 | — | +0.00 | -470.27 | -149.11 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-14 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-17 | `BNC` | 2001 | — | $5.03 | +0.00 | $5.42 | +780.39 | +780.39 | +0.00 | +780.39 |
| 2026-09-18 | `BNC` | 2001 | $5.42 | $5.83 | +820.41 | — | +0.00 | +820.41 | +1600.80 | — |
| 2026-09-21 | `ASST` | 367 | — | $31.64 | +0.00 | $30.33 | -480.77 | -480.77 | +0.00 | -480.77 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +556.07 | DUOT, NPWR, TMC | — | $1.05 | $10,513.89 | DUOT×353, NPWR×2136, TMC×781 |
| 2026-08-17 | +2.25 | $1.05 | DUOT×353, NPWR×2136, TMC×781 | $10,918.77 | +404.88 | -159.58 | IQ | DUOT, NPWR, TMC | $1.37 | $10,613.44 | IQ×7979 |
| 2026-08-18 | -6.20 | $1.37 | IQ×7979 | $10,134.70 | -478.74 | +0.00 | — | IQ | $10,030.37 | $10,030.37 | — |
| 2026-08-19 | -7.20 | $10,030.37 | — | $10,030.37 | -0.00 | +0.00 | — | — | $10,030.37 | $10,030.37 | — |
| 2026-08-20 | +1.12 | $10,030.37 | — | $10,030.37 | -0.00 | +0.00 | — | — | $10,030.37 | $10,030.37 | — |
| 2026-08-21 | +3.25 | $10,030.37 | — | $10,030.37 | -0.00 | +0.00 | — | — | $10,030.37 | $10,030.37 | — |
| 2026-08-24 | -5.17 | $10,030.37 | — | $10,030.37 | -0.00 | +0.00 | — | — | $10,030.37 | $10,030.37 | — |
| 2026-08-25 | +1.80 | $10,030.37 | — | $10,030.37 | -0.00 | +106.40 | SJM, GRRR | — | $57.62 | $10,130.02 | SJM×40, GRRR×360 |
| 2026-08-26 | +2.02 | $57.62 | SJM×40, GRRR×360 | $10,500.42 | +370.40 | +51.92 | INO, HCA, LI | SJM, GRRR | $39.26 | $10,504.46 | INO×2732, HCA×8, LI×288 |
| 2026-08-27 | — | $39.26 | INO×2732, HCA×8, LI×288 | $10,517.22 | +12.76 | -99.52 | DASH, MRVL, BBAR, TD, MU, GGB, LRCX | INO, HCA, LI | $1,101.31 | $10,359.59 | DASH×6, MRVL×5, BBAR×100, TD×12, MU×1, GGB×327, LRCX×4 |
| 2026-08-28 | +0.75 | $1,101.31 | DASH×6, MRVL×5, BBAR×100, TD×12, MU×1, GGB×327, LRCX×4 | $10,312.17 | -47.42 | +130.61 | MRNA, MNRO, MOS | DASH, MRVL, BBAR, TD, MU, GGB, LRCX | $3.51 | $10,417.98 | MRNA×25, MNRO×277, MOS×143 |
| 2026-08-31 | -5.85 | $3.51 | MRNA×25, MNRO×277, MOS×143 | $10,279.54 | -138.44 | +0.00 | — | MRNA, MNRO, MOS | $10,271.32 | $10,271.32 | — |
| 2026-09-01 | -6.30 | $10,271.32 | — | $10,271.32 | +0.00 | +0.00 | — | — | $10,271.32 | $10,271.32 | — |
| 2026-09-02 | -3.83 | $10,271.32 | — | $10,271.32 | +0.00 | +0.00 | — | — | $10,271.32 | $10,271.32 | — |
| 2026-09-03 | -0.90 | $10,271.32 | — | $10,271.32 | +0.00 | +0.00 | — | — | $10,271.32 | $10,271.32 | — |
| 2026-09-04 | +2.25 | $10,271.32 | — | $10,271.32 | +0.00 | +321.16 | HAFN | — | $2.34 | $10,577.68 | HAFN×1147 |
| 2026-09-08 | -11.47 | $2.34 | HAFN×1147 | $10,107.41 | -470.27 | +0.00 | — | HAFN | $10,092.35 | $10,092.35 | — |
| 2026-09-09 | -13.95 | $10,092.35 | — | $10,092.35 | -0.00 | +0.00 | — | — | $10,092.35 | $10,092.35 | — |
| 2026-09-10 | -13.28 | $10,092.35 | — | $10,092.35 | -0.00 | +0.00 | — | — | $10,092.35 | $10,092.35 | — |
| 2026-09-11 | +0.50 | $10,092.35 | — | $10,092.35 | -0.00 | +0.00 | — | — | $10,092.35 | $10,092.35 | — |
| 2026-09-14 | -11.00 | $10,092.35 | — | $10,092.35 | -0.00 | +0.00 | — | — | $10,092.35 | $10,092.35 | — |
| 2026-09-15 | -3.84 | $10,092.35 | — | $10,092.35 | -0.00 | +0.00 | — | — | $10,092.35 | $10,092.35 | — |
| 2026-09-16 | +5.30 | $10,092.35 | — | $10,092.35 | -0.00 | +0.00 | — | — | $10,092.35 | $10,092.35 | — |
| 2026-09-17 | +7.38 | $10,092.35 | — | $10,092.35 | -0.00 | +780.39 | BNC | — | $1.50 | $10,846.92 | BNC×2001 |
| 2026-09-18 | +4.86 | $1.50 | BNC×2001 | $11,667.33 | +820.41 | +0.00 | — | BNC | $11,641.10 | $11,641.10 | — |
| 2026-09-21 | +12.87 | $11,641.10 | — | $11,641.10 | -0.00 | -480.77 | ASST | — | $24.48 | $11,155.59 | ASST×367 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `DUOT` | 353 | $9.43 | $4.55 | — | $6,666.66 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list overnight; 🔵; ⚪; ret5=+7.7; leftover $3333.33 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NPWR` | 2136 | $1.56 | $27.55 | — | $3,306.94 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list earn_react; 🔵; ret5=+8.0; leftover $3333.33 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `TMC` | 781 | $4.22 | $10.07 | — | $1.05 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list earn_react; 🔵; ret5=+7.0; leftover $3333.33 | join🟢 sector🔴 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.05 | ▲ close $10,513.89 vs 09:30 $10,000.00 (session +556.07) | 16:00 close · cash $1.05 · equity $10,513.89 vs 09:30 $10,000.00 (+513.89; session marks +556.07) · 3 name(s) marked open→close (per-name table). DUOT×353 09:30 $9.43 → close $9.11 -112.96; NPWR×2136 09:30 $1.56 → close $1.95 +833.04; TMC×781 09:30 $4.22 → close $4.01 -164.01 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.05 | ▲ 09:30 equity $10,918.77 vs yday $10,513.89 (+404.88) | 09:30 open · cash $1.05 (unchanged overnight, no fees) · equity $10,918.77 vs prior close $10,513.89 (+404.88) · 3 name(s) re-marked at the open (per-name table). DUOT×353 yday $9.11 → 09:30 $10.35 +437.72; NPWR×2136 yday $1.95 → 09:30 $1.92 -64.08; TMC×781 yday $4.01 → 09:30 $4.05 +31.24 | — |
| 2026-08-17 09:30 ET | **SELL** | `DUOT` | 353 | $10.35 | $4.64 | $+315.56 | $3,649.96 | ▲ +315.56 after sell → book $10,914.13; vs 09:30 mark -4.64 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `NPWR` | 2136 | $1.92 | $27.94 | $+713.46 | $7,723.13 | ▲ +713.46 after sell → book $10,886.18; vs 09:30 mark -27.95 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `TMC` | 781 | $4.05 | $10.23 | $-153.07 | $10,875.95 | ▼ -153.07 after sell → book $10,875.95; vs 09:30 mark -10.23 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `IQ` | 7979 | $1.35 | $102.93 | — | $1.37 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list overnight; ⚪; ret5=+1.5; leftover $10875.95 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.37 | ▼ close $10,613.44 vs 09:30 $10,918.77 (session -159.58) | 16:00 close · cash $1.37 · equity $10,613.44 vs 09:30 $10,918.77 (-305.33; session marks -159.58) · 1 name(s) marked open→close (per-name table). IQ×7979 09:30 $1.35 → close $1.33 -159.58 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.37 | ▼ 09:30 equity $10,134.70 vs yday $10,613.44 (-478.74) | 09:30 open · cash $1.37 (unchanged overnight, no fees) · equity $10,134.70 vs prior close $10,613.44 (-478.74) · 1 name(s) re-marked at the open (per-name table). IQ×7979 yday $1.33 → 09:30 $1.27 -478.74 | — |
| 2026-08-18 09:30 ET | **SELL** | `IQ` | 7979 | $1.27 | $104.33 | $-845.58 | $10,030.37 | ▼ -845.58 after sell → book $10,030.37; vs 09:30 mark -104.33 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟡 buy🟡 |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,030.37 | ▲ close $10,030.37 vs 09:30 $10,134.70 (session +0.00) | 16:00 close · cash $10,030.37 · no lots left · equity $10,030.37. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,030.37 | ▲ 09:30 equity $10,030.37 vs yday $10,030.37 (-0.00) | 09:30 open · cash $10,030.37 · no holdings · equity $10,030.37 vs prior close $10,030.37 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,030.37 | ▲ close $10,030.37 vs 09:30 $10,030.37 (session +0.00) | 16:00 close · cash $10,030.37 · no lots left · equity $10,030.37. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,030.37 | ▲ 09:30 equity $10,030.37 vs yday $10,030.37 (-0.00) | 09:30 open · cash $10,030.37 · no holdings · equity $10,030.37 vs prior close $10,030.37 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,030.37 | ▲ close $10,030.37 vs 09:30 $10,030.37 (session +0.00) | 16:00 close · cash $10,030.37 · no lots left · equity $10,030.37. | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,030.37 | ▲ 09:30 equity $10,030.37 vs yday $10,030.37 (-0.00) | 09:30 open · cash $10,030.37 · no holdings · equity $10,030.37 vs prior close $10,030.37 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,030.37 | ▲ close $10,030.37 vs 09:30 $10,030.37 (session +0.00) | 16:00 close · cash $10,030.37 · no lots left · equity $10,030.37. | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,030.37 | ▲ 09:30 equity $10,030.37 vs yday $10,030.37 (-0.00) | 09:30 open · cash $10,030.37 · no holdings · equity $10,030.37 vs prior close $10,030.37 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,030.37 | ▲ close $10,030.37 vs 09:30 $10,030.37 (session +0.00) | 16:00 close · cash $10,030.37 · no lots left · equity $10,030.37. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,030.37 | ▲ 09:30 equity $10,030.37 vs yday $10,030.37 (-0.00) | 09:30 open · cash $10,030.37 · no holdings · equity $10,030.37 vs prior close $10,030.37 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `SJM` | 40 | $123.87 | $2.11 | — | $5,073.46 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list overnight; ret5=+6.8; leftover $5015.18 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 360 | $13.92 | $4.64 | — | $57.62 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list earn_react; 🔵; ret5=+5.9; leftover $5015.18 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.62 | ▲ close $10,130.02 vs 09:30 $10,030.37 (session +106.40) | 16:00 close · cash $57.62 · equity $10,130.02 vs 09:30 $10,030.37 (+99.65; session marks +106.40) · 2 name(s) marked open→close (per-name table). SJM×40 09:30 $123.87 → close $125.45 +63.20; GRRR×360 09:30 $13.92 → close $14.04 +43.20 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.62 | ▲ 09:30 equity $10,500.42 vs yday $10,130.02 (+370.40) | 09:30 open · cash $57.62 (unchanged overnight, no fees) · equity $10,500.42 vs prior close $10,130.02 (+370.40) · 2 name(s) re-marked at the open (per-name table). SJM×40 yday $125.45 → 09:30 $134.80 +374.00; GRRR×360 yday $14.04 → 09:30 $14.03 -3.60 | — |
| 2026-08-26 09:30 ET | **SELL** | `SJM` | 40 | $134.80 | $2.16 | $+432.93 | $5,447.45 | ▲ +432.93 after sell → book $10,498.25; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `GRRR` | 360 | $14.03 | $4.74 | $+30.21 | $10,493.51 | ▲ +30.21 after sell → book $10,493.51; vs 09:30 mark -4.74 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `INO` | 2732 | $1.28 | $35.24 | — | $6,961.31 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list ohlc_hot; 🔵; ret5=+7.5; leftover $3497.84 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `HCA` | 8 | $427.50 | $2.01 | — | $3,539.29 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list flatten; ret5=+4.1; leftover $3497.84 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `LI` | 288 | $12.14 | $3.72 | — | $39.26 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list earn_react; ret5=+1.2; leftover $3497.84 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $39.26 | ▲ close $10,504.46 vs 09:30 $10,500.42 (session +51.92) | 16:00 close · cash $39.26 · equity $10,504.46 vs 09:30 $10,500.42 (+4.04; session marks +51.92) · 3 name(s) marked open→close (per-name table). INO×2732 09:30 $1.28 → close $1.30 +54.64; HCA×8 09:30 $427.50 → close $427.16 -2.72; LI×288 09:30 $12.14 → close $12.14 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.26 | ▲ 09:30 equity $10,517.22 vs yday $10,504.46 (+12.76) | 09:30 open · cash $39.26 (unchanged overnight, no fees) · equity $10,517.22 vs prior close $10,504.46 (+12.76) · 3 name(s) re-marked at the open (per-name table). INO×2732 yday $1.30 → 09:30 $1.29 -27.32; HCA×8 yday $427.16 → 09:30 $424.61 -20.40; LI×288 yday $12.14 → 09:30 $12.35 +60.48 | — |
| 2026-08-27 09:30 ET | **SELL** | `INO` | 2732 | $1.29 | $35.72 | $-43.65 | $3,527.81 | ▼ -43.65 after sell → book $10,481.49; vs 09:30 mark -35.73 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 8 | $424.61 | $2.05 | $-27.19 | $6,922.64 | ▼ -27.19 after sell → book $10,479.44; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `LI` | 288 | $12.35 | $3.79 | $+52.97 | $10,475.65 | ▲ +52.97 after sell → book $10,475.65; vs 09:30 mark -3.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `DASH` | 6 | $235.94 | $2.01 | — | $9,058.00 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list ohlc_hot; ret5=+7.6; leftover $1496.52 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 5 | $253.44 | $2.00 | — | $7,788.80 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list overnight,overnight_mega,mover_buy; 🔵; ret5=+3.3; leftover $1496.52 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `BBAR` | 100 | $14.96 | $2.29 | — | $6,290.51 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list overnight; ret5=+3.0; leftover $1496.52 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `TD` | 12 | $120.17 | $2.03 | — | $4,846.44 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list earn_react; ret5=+0.9; leftover $1496.52 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $3,877.44 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list mover_buy; 🔵; ret5=+0.1; leftover $1496.52 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 327 | $4.57 | $4.22 | — | $2,378.83 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list mover_buy; 🔵; ret5=+1.1; leftover $1496.52 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 4 | $318.88 | $2.00 | — | $1,101.31 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list mover_buy; 🔵; ret5=+1.9; leftover $1496.52 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,101.31 | ▼ close $10,359.59 vs 09:30 $10,517.22 (session -99.52) | 16:00 close · cash $1,101.31 · equity $10,359.59 vs 09:30 $10,517.22 (-157.63; session marks -99.52) · 7 name(s) marked open→close (per-name table). DASH×6 09:30 $235.94 → close $231.89 -24.30; MRVL×5 09:30 $253.44 → close $241.45 -59.95; BBAR×100 09:30 $14.96 → close $14.60 -36.00; TD×12 09:30 $120.17 → close $121.09 +11.04; MU×1 09:30 $967.01 → close $935.39 -31.62; GGB×327 09:30 $4.57 → close $4.70 +42.51; LRCX×4 09:30 $318.88 → close $318.58 -1.20 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,101.31 | ▼ 09:30 equity $10,312.17 vs yday $10,359.59 (-47.42) | 09:30 open · cash $1,101.31 (unchanged overnight, no fees) · equity $10,312.17 vs prior close $10,359.59 (-47.42) · 7 name(s) re-marked at the open (per-name table). DASH×6 yday $231.89 → 09:30 $233.37 +8.88; MRVL×5 yday $241.45 → 09:30 $225.26 -80.95; BBAR×100 yday $14.60 → 09:30 $15.01 +41.00; TD×12 yday $121.09 → 09:30 $122.07 +11.76; MU×1 yday $935.39 → 09:30 $919.29 -16.10; GGB×327 yday $4.70 → 09:30 $4.67 -9.81; LRCX×4 yday $318.58 → 09:30 $318.03 -2.20 | — |
| 2026-08-28 09:30 ET | **SELL** | `DASH` | 6 | $233.37 | $2.03 | $-19.46 | $2,499.50 | ▼ -19.46 after sell → book $10,310.14; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MRVL` | 5 | $225.26 | $2.02 | $-144.93 | $3,623.77 | ▼ -144.93 after sell → book $10,308.11; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `BBAR` | 100 | $15.01 | $2.32 | $+0.39 | $5,122.45 | ▲ +0.39 after sell → book $10,305.79; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `TD` | 12 | $122.07 | $2.05 | $+18.73 | $6,585.25 | ▲ +18.73 after sell → book $10,303.75; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $7,502.52 | ▼ -51.73 after sell → book $10,301.73; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 327 | $4.67 | $4.28 | $+24.20 | $9,025.33 | ▲ +24.20 after sell → book $10,297.45; vs 09:30 mark -4.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 4 | $318.03 | $2.02 | $-7.42 | $10,295.43 | ▼ -7.42 after sell → book $10,295.43; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `MRNA` | 25 | $137.19 | $2.06 | — | $6,863.61 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list ohlc_hot; ret5=+7.1; leftover $3431.81 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MNRO` | 277 | $12.38 | $3.57 | — | $3,430.78 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list yday_mover; ret5=+2.4; leftover $3431.81 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MOS` | 143 | $23.95 | $2.42 | — | $3.51 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list flatten; ret5=+1.8; leftover $3431.81 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.51 | ▲ close $10,417.98 vs 09:30 $10,312.17 (session +130.61) | 16:00 close · cash $3.51 · equity $10,417.98 vs 09:30 $10,312.17 (+105.81; session marks +130.61) · 3 name(s) marked open→close (per-name table). MRNA×25 09:30 $137.19 → close $137.99 +20.00; MNRO×277 09:30 $12.38 → close $12.96 +160.66; MOS×143 09:30 $23.95 → close $23.60 -50.05 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.51 | ▼ 09:30 equity $10,279.54 vs yday $10,417.98 (-138.44) | 09:30 open · cash $3.51 (unchanged overnight, no fees) · equity $10,279.54 vs prior close $10,417.98 (-138.44) · 3 name(s) re-marked at the open (per-name table). MRNA×25 yday $137.99 → 09:30 $134.10 -97.25; MNRO×277 yday $12.96 → 09:30 $12.77 -52.63; MOS×143 yday $23.60 → 09:30 $23.68 +11.44 | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 25 | $134.10 | $2.10 | $-81.42 | $3,353.91 | ▼ -81.42 after sell → book $10,277.44; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `MNRO` | 277 | $12.77 | $3.65 | $+100.81 | $6,887.55 | ▲ +100.81 after sell → book $10,273.79; vs 09:30 mark -3.65 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 143 | $23.68 | $2.47 | $-43.50 | $10,271.32 | ▼ -43.50 after sell → book $10,271.32; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,271.32 | ▲ close $10,271.32 vs 09:30 $10,279.54 (session +0.00) | 16:00 close · cash $10,271.32 · no lots left · equity $10,271.32. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,271.32 | ▲ 09:30 equity $10,271.32 vs yday $10,271.32 (+0.00) | 09:30 open · cash $10,271.32 · no holdings · equity $10,271.32 vs prior close $10,271.32 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,271.32 | ▲ close $10,271.32 vs 09:30 $10,271.32 (session +0.00) | 16:00 close · cash $10,271.32 · no lots left · equity $10,271.32. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,271.32 | ▲ 09:30 equity $10,271.32 vs yday $10,271.32 (+0.00) | 09:30 open · cash $10,271.32 · no holdings · equity $10,271.32 vs prior close $10,271.32 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,271.32 | ▲ close $10,271.32 vs 09:30 $10,271.32 (session +0.00) | 16:00 close · cash $10,271.32 · no lots left · equity $10,271.32. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,271.32 | ▲ 09:30 equity $10,271.32 vs yday $10,271.32 (+0.00) | 09:30 open · cash $10,271.32 · no holdings · equity $10,271.32 vs prior close $10,271.32 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,271.32 | ▲ close $10,271.32 vs 09:30 $10,271.32 (session +0.00) | 16:00 close · cash $10,271.32 · no lots left · equity $10,271.32. | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,271.32 | ▲ 09:30 equity $10,271.32 vs yday $10,271.32 (+0.00) | 09:30 open · cash $10,271.32 · no holdings · equity $10,271.32 vs prior close $10,271.32 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-04 09:30 ET | **BUY** | `HAFN` | 1147 | $8.94 | $14.80 | — | $2.34 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list ohlc_hot; ret5=+7.7; leftover $10271.32 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.34 | ▲ close $10,577.68 vs 09:30 $10,271.32 (session +321.16) | 16:00 close · cash $2.34 · equity $10,577.68 vs 09:30 $10,271.32 (+306.36; session marks +321.16) · 1 name(s) marked open→close (per-name table). HAFN×1147 09:30 $8.94 → close $9.22 +321.16 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.34 | ▼ 09:30 equity $10,107.41 vs yday $10,577.68 (-470.27) | 09:30 open · cash $2.34 (unchanged overnight, no fees) · equity $10,107.41 vs prior close $10,577.68 (-470.27) · 1 name(s) re-marked at the open (per-name table). HAFN×1147 yday $9.22 → 09:30 $8.81 -470.27 | — |
| 2026-09-08 09:30 ET | **SELL** | `HAFN` | 1147 | $8.81 | $15.07 | $-178.97 | $10,092.35 | ▼ -178.97 after sell → book $10,092.35; vs 09:30 mark -15.06 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,092.35 | ▲ close $10,092.35 vs 09:30 $10,107.41 (session +0.00) | 16:00 close · cash $10,092.35 · no lots left · equity $10,092.35. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,092.35 | ▲ 09:30 equity $10,092.35 vs yday $10,092.35 (-0.00) | 09:30 open · cash $10,092.35 · no holdings · equity $10,092.35 vs prior close $10,092.35 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,092.35 | ▲ close $10,092.35 vs 09:30 $10,092.35 (session +0.00) | 16:00 close · cash $10,092.35 · no lots left · equity $10,092.35. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,092.35 | ▲ 09:30 equity $10,092.35 vs yday $10,092.35 (-0.00) | 09:30 open · cash $10,092.35 · no holdings · equity $10,092.35 vs prior close $10,092.35 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,092.35 | ▲ close $10,092.35 vs 09:30 $10,092.35 (session +0.00) | 16:00 close · cash $10,092.35 · no lots left · equity $10,092.35. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,092.35 | ▲ 09:30 equity $10,092.35 vs yday $10,092.35 (-0.00) | 09:30 open · cash $10,092.35 · no holdings · equity $10,092.35 vs prior close $10,092.35 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,092.35 | ▲ close $10,092.35 vs 09:30 $10,092.35 (session +0.00) | 16:00 close · cash $10,092.35 · no lots left · equity $10,092.35. | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,092.35 | ▲ 09:30 equity $10,092.35 vs yday $10,092.35 (-0.00) | 09:30 open · cash $10,092.35 · no holdings · equity $10,092.35 vs prior close $10,092.35 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,092.35 | ▲ close $10,092.35 vs 09:30 $10,092.35 (session +0.00) | 16:00 close · cash $10,092.35 · no lots left · equity $10,092.35. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,092.35 | ▲ 09:30 equity $10,092.35 vs yday $10,092.35 (-0.00) | 09:30 open · cash $10,092.35 · no holdings · equity $10,092.35 vs prior close $10,092.35 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,092.35 | ▲ close $10,092.35 vs 09:30 $10,092.35 (session +0.00) | 16:00 close · cash $10,092.35 · no lots left · equity $10,092.35. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,092.35 | ▲ 09:30 equity $10,092.35 vs yday $10,092.35 (-0.00) | 09:30 open · cash $10,092.35 · no holdings · equity $10,092.35 vs prior close $10,092.35 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,092.35 | ▲ close $10,092.35 vs 09:30 $10,092.35 (session +0.00) | 16:00 close · cash $10,092.35 · no lots left · equity $10,092.35. | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,092.35 | ▲ 09:30 equity $10,092.35 vs yday $10,092.35 (-0.00) | 09:30 open · cash $10,092.35 · no holdings · equity $10,092.35 vs prior close $10,092.35 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-17 09:30 ET | **BUY** | `BNC` | 2001 | $5.03 | $25.81 | — | $1.50 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list ohlc_hot; ret5=+7.4; leftover $10092.35 | join🟡 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.50 | ▲ close $10,846.92 vs 09:30 $10,092.35 (session +780.39) | 16:00 close · cash $1.50 · equity $10,846.92 vs 09:30 $10,092.35 (+754.57; session marks +780.39) · 1 name(s) marked open→close (per-name table). BNC×2001 09:30 $5.03 → close $5.42 +780.39 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.50 | ▲ 09:30 equity $11,667.33 vs yday $10,846.92 (+820.41) | 09:30 open · cash $1.50 (unchanged overnight, no fees) · equity $11,667.33 vs prior close $10,846.92 (+820.41) · 1 name(s) re-marked at the open (per-name table). BNC×2001 yday $5.42 → 09:30 $5.83 +820.41 | — |
| 2026-09-18 09:30 ET | **SELL** | `BNC` | 2001 | $5.83 | $26.24 | $+1548.75 | $11,641.10 | ▲ +1,548.75 after sell → book $11,641.10; vs 09:30 mark -26.23 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,641.10 | ▲ close $11,641.10 vs 09:30 $11,667.33 (session +0.00) | 16:00 close · cash $11,641.10 · no lots left · equity $11,641.10. | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,641.10 | ▲ 09:30 equity $11,641.10 vs yday $11,641.10 (-0.00) | 09:30 open · cash $11,641.10 · no holdings · equity $11,641.10 vs prior close $11,641.10 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-21 09:30 ET | **BUY** | `ASST` | 367 | $31.64 | $4.73 | — | $24.48 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list ohlc_hot; 🔵; ret5=+8.9; leftover $11641.10 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.48 | ▼ close $11,155.59 vs 09:30 $11,641.10 (session -480.77) | 16:00 close · cash $24.48 · equity $11,155.59 vs 09:30 $11,641.10 (-485.51; session marks -480.77) · 1 name(s) marked open→close (per-name table). ASST×367 09:30 $31.64 → close $30.33 -480.77 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-19 | `ATAT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ALAB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ANET` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CRK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-08 | `LOGI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `GEMI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ASST` | 367 | 2026-09-21 @ $31.64 | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list ohlc_hot; 🔵; ret5=+8.9; leftover $11641.10 |
