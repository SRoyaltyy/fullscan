# Factor mine action — `union_clk_nr7_mom_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `hot_score` · size `leftover` · sell `list` · S-boost `none` · Clock-B #10 NR7 + moderate momentum

Cash book **+4.25%** ($10,425) · signal-only (no cash/fees) was +27.44%. Starts YES **25/30**. Fills 55 · skips 16 · realized $+1589.64.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $11,589.65.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `DUOT` | 353 | $9.43 | $4.55 | — | $6,666.66 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list overnight; 🔵; ⚪; ret5=+7.7; leftover $3333.33 | — |
| 2026-08-14 09:30 ET | **BUY** | `NPWR` | 2136 | $1.56 | $27.55 | — | $3,306.94 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list earn_react; 🔵; ret5=+8.0; leftover $3333.33 | — |
| 2026-08-14 09:30 ET | **BUY** | `TMC` | 781 | $4.22 | $10.07 | — | $1.05 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list earn_react; 🔵; ret5=+7.0; leftover $3333.33 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.05 | ▲ close $10,513.89 vs 09:30 $10,000.00 (session +556.07) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.05 | ▲ 09:30 equity $10,918.77 vs yday $10,513.89 (+404.88) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `DUOT` | 353 | $10.35 | $4.64 | $+315.56 | $3,649.96 | ▲ +315.56 after sell → book $10,914.13; vs 09:30 mark -4.64 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NPWR` | 2136 | $1.92 | $27.94 | $+713.46 | $7,723.13 | ▲ +713.46 after sell → book $10,886.18; vs 09:30 mark -27.95 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TMC` | 781 | $4.05 | $10.23 | $-153.07 | $10,875.95 | ▼ -153.07 after sell → book $10,875.95; vs 09:30 mark -10.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `IQ` | 7979 | $1.35 | $102.93 | — | $1.37 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list overnight; ⚪; ret5=+1.5; leftover $10875.95 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.37 | ▼ close $10,613.44 vs 09:30 $10,918.77 (session -159.58) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.37 | ▼ 09:30 equity $10,134.70 vs yday $10,613.44 (-478.74) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `IQ` | 7979 | $1.27 | $104.33 | $-845.58 | $10,030.37 | ▼ -845.58 after sell → book $10,030.37; vs 09:30 mark -104.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,030.37 | ▲ close $10,030.37 vs 09:30 $10,134.70 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,030.37 | ▲ 09:30 equity $10,030.37 vs yday $10,030.37 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,030.37 | ▲ close $10,030.37 vs 09:30 $10,030.37 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,030.37 | ▲ 09:30 equity $10,030.37 vs yday $10,030.37 (-0.00) | — | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,030.37 | ▲ close $10,030.37 vs 09:30 $10,030.37 (session +0.00) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,030.37 | ▲ 09:30 equity $10,030.37 vs yday $10,030.37 (-0.00) | — | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,030.37 | ▲ close $10,030.37 vs 09:30 $10,030.37 (session +0.00) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,030.37 | ▲ 09:30 equity $10,030.37 vs yday $10,030.37 (-0.00) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,030.37 | ▲ close $10,030.37 vs 09:30 $10,030.37 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,030.37 | ▲ 09:30 equity $10,030.37 vs yday $10,030.37 (-0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `SJM` | 40 | $123.87 | $2.11 | — | $5,073.46 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list overnight; ret5=+6.8; leftover $5015.18 | — |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 360 | $13.92 | $4.64 | — | $57.62 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list earn_react; 🔵; ret5=+5.9; leftover $5015.18 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.62 | ▲ close $10,130.02 vs 09:30 $10,030.37 (session +106.40) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.62 | ▲ 09:30 equity $10,500.42 vs yday $10,130.02 (+370.40) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `SJM` | 40 | $134.80 | $2.16 | $+432.93 | $5,447.45 | ▲ +432.93 after sell → book $10,498.25; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GRRR` | 360 | $14.03 | $4.74 | $+30.21 | $10,493.51 | ▲ +30.21 after sell → book $10,493.51; vs 09:30 mark -4.74 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `INO` | 2732 | $1.28 | $35.24 | — | $6,961.31 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list ohlc_hot; 🔵; ret5=+7.5; leftover $3497.84 | — |
| 2026-08-26 09:30 ET | **BUY** | `HCA` | 8 | $427.50 | $2.01 | — | $3,539.29 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list flatten; ret5=+4.1; leftover $3497.84 | — |
| 2026-08-26 09:30 ET | **BUY** | `LI` | 288 | $12.14 | $3.72 | — | $39.26 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list earn_react; ret5=+1.2; leftover $3497.84 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $39.26 | ▲ close $10,504.46 vs 09:30 $10,500.42 (session +51.92) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.26 | ▲ 09:30 equity $10,517.22 vs yday $10,504.46 (+12.76) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `INO` | 2732 | $1.29 | $35.72 | $-43.65 | $3,527.81 | ▼ -43.65 after sell → book $10,481.49; vs 09:30 mark -35.73 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 8 | $424.61 | $2.05 | $-27.19 | $6,922.64 | ▼ -27.19 after sell → book $10,479.44; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `LI` | 288 | $12.35 | $3.79 | $+52.97 | $10,475.65 | ▲ +52.97 after sell → book $10,475.65; vs 09:30 mark -3.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `DASH` | 6 | $235.94 | $2.01 | — | $9,058.00 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list ohlc_hot; ret5=+7.6; leftover $1496.52 | — |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 5 | $253.44 | $2.00 | — | $7,788.80 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list overnight,overnight_mega,mover_buy; 🔵; ret5=+3.3; leftover $1496.52 | — |
| 2026-08-27 09:30 ET | **BUY** | `BBAR` | 100 | $14.96 | $2.29 | — | $6,290.51 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list overnight; ret5=+3.0; leftover $1496.52 | — |
| 2026-08-27 09:30 ET | **BUY** | `TD` | 12 | $120.17 | $2.03 | — | $4,846.44 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list earn_react; ret5=+0.9; leftover $1496.52 | — |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $3,877.44 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list mover_buy; 🔵; ret5=+0.1; leftover $1496.52 | — |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 327 | $4.57 | $4.22 | — | $2,378.83 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list mover_buy; 🔵; ret5=+1.1; leftover $1496.52 | — |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 4 | $318.88 | $2.00 | — | $1,101.31 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list mover_buy; 🔵; ret5=+1.9; leftover $1496.52 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,101.31 | ▼ close $10,359.59 vs 09:30 $10,517.22 (session -99.52) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,101.31 | ▼ 09:30 equity $10,312.17 vs yday $10,359.59 (-47.42) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `DASH` | 6 | $233.37 | $2.03 | $-19.46 | $2,499.50 | ▼ -19.46 after sell → book $10,310.14; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MRVL` | 5 | $225.26 | $2.02 | $-144.93 | $3,623.77 | ▼ -144.93 after sell → book $10,308.11; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BBAR` | 100 | $15.01 | $2.32 | $+0.39 | $5,122.45 | ▲ +0.39 after sell → book $10,305.79; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TD` | 12 | $122.07 | $2.05 | $+18.73 | $6,585.25 | ▲ +18.73 after sell → book $10,303.75; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $7,502.52 | ▼ -51.73 after sell → book $10,301.73; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 327 | $4.67 | $4.28 | $+24.20 | $9,025.33 | ▲ +24.20 after sell → book $10,297.45; vs 09:30 mark -4.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 4 | $318.03 | $2.02 | $-7.42 | $10,295.43 | ▼ -7.42 after sell → book $10,295.43; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `MRNA` | 25 | $137.19 | $2.06 | — | $6,863.61 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list ohlc_hot; ret5=+7.1; leftover $3431.81 | — |
| 2026-08-28 09:30 ET | **BUY** | `MNRO` | 277 | $12.38 | $3.57 | — | $3,430.78 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list yday_mover; ret5=+2.4; leftover $3431.81 | — |
| 2026-08-28 09:30 ET | **BUY** | `MOS` | 143 | $23.95 | $2.42 | — | $3.51 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list flatten; ret5=+1.8; leftover $3431.81 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.51 | ▲ close $10,417.98 vs 09:30 $10,312.17 (session +130.61) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.51 | ▼ 09:30 equity $10,279.54 vs yday $10,417.98 (-138.44) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 25 | $134.10 | $2.10 | $-81.42 | $3,353.91 | ▼ -81.42 after sell → book $10,277.44; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MNRO` | 277 | $12.77 | $3.65 | $+100.81 | $6,887.55 | ▲ +100.81 after sell → book $10,273.79; vs 09:30 mark -3.65 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 143 | $23.68 | $2.47 | $-43.50 | $10,271.32 | ▼ -43.50 after sell → book $10,271.32; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,271.32 | ▲ close $10,271.32 vs 09:30 $10,279.54 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,271.32 | ▲ 09:30 equity $10,271.32 vs yday $10,271.32 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,271.32 | ▲ close $10,271.32 vs 09:30 $10,271.32 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,271.32 | ▲ 09:30 equity $10,271.32 vs yday $10,271.32 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,271.32 | ▲ close $10,271.32 vs 09:30 $10,271.32 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,271.32 | ▲ 09:30 equity $10,271.32 vs yday $10,271.32 (+0.00) | — | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,271.32 | ▲ close $10,271.32 vs 09:30 $10,271.32 (session +0.00) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,271.32 | ▲ 09:30 equity $10,271.32 vs yday $10,271.32 (+0.00) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `HAFN` | 1147 | $8.94 | $14.80 | — | $2.34 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list ohlc_hot; ret5=+7.7; leftover $10271.32 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.34 | ▲ close $10,577.68 vs 09:30 $10,271.32 (session +321.16) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.34 | ▼ 09:30 equity $10,107.41 vs yday $10,577.68 (-470.27) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `HAFN` | 1147 | $8.81 | $15.07 | $-178.97 | $10,092.35 | ▼ -178.97 after sell → book $10,092.35; vs 09:30 mark -15.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,092.35 | ▲ close $10,092.35 vs 09:30 $10,107.41 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,092.35 | ▲ 09:30 equity $10,092.35 vs yday $10,092.35 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,092.35 | ▲ close $10,092.35 vs 09:30 $10,092.35 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,092.35 | ▲ 09:30 equity $10,092.35 vs yday $10,092.35 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,092.35 | ▲ close $10,092.35 vs 09:30 $10,092.35 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,092.35 | ▲ 09:30 equity $10,092.35 vs yday $10,092.35 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `VLO` | 26 | $388.00 | $2.07 | — | $2.28 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list ohlc_hot; ret5=+6.7; leftover $10092.35 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.28 | ▲ close $10,153.20 vs 09:30 $10,092.35 (session +62.92) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.28 | ▲ 09:30 equity $10,279.04 vs yday $10,153.20 (+125.84) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.28 | ▼ close $9,958.98 vs 09:30 $10,279.04 (session -320.06) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.28 | ▲ 09:30 equity $9,973.54 vs yday $9,958.98 (+14.56) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.28 | ▲ close $10,325.32 vs 09:30 $9,973.54 (session +351.78) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.28 | ▼ 09:30 equity $10,185.96 vs yday $10,325.32 (-139.36) | — | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.28 | ▲ close $10,487.56 vs 09:30 $10,185.96 (session +301.60) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.28 | ▼ 09:30 equity $10,361.98 vs yday $10,487.56 (-125.58) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `VLO` | 26 | $398.45 | $2.16 | $+267.47 | $10,359.82 | ▲ +267.47 after sell → book $10,359.82; vs 09:30 mark -2.16 | dropped from list after 4 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `BNC` | 2054 | $5.03 | $26.50 | — | $1.70 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list ohlc_hot; ret5=+7.4; leftover $10359.82 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.70 | ▲ close $11,134.38 vs 09:30 $10,361.98 (session +801.06) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.70 | ▲ 09:30 equity $11,976.52 vs yday $11,134.38 (+842.14) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `BNC` | 2054 | $5.83 | $26.93 | $+1589.77 | $11,949.59 | ▲ +1,589.77 after sell → book $11,949.59; vs 09:30 mark -26.93 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,949.59 | ▲ close $11,949.59 vs 09:30 $11,976.52 (session +0.00) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,949.59 | ▲ 09:30 equity $11,949.59 vs yday $11,949.59 (-0.00) | — | — |
| 2026-09-21 09:30 ET | **BUY** | `ASST` | 188 | $31.64 | $2.55 | — | $5,998.71 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list ohlc_hot; 🔵; ret5=+8.9; leftover $5974.79 | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 37 | $157.87 | $2.10 | — | $155.42 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list flatten; ret5=+6.5; leftover $5974.79 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $155.42 | ▼ close $11,849.24 vs 09:30 $11,949.59 (session -95.69) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $155.42 | ▼ 09:30 equity $11,655.60 vs yday $11,849.24 (-193.64) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `ASST` | 188 | $29.30 | $2.63 | $-445.10 | $5,661.19 | ▼ -445.10 after sell → book $11,652.97; vs 09:30 mark -2.63 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,661.19 | ▲ close $11,652.97 vs 09:30 $11,655.60 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,661.19 | ▲ 09:30 equity $11,823.17 vs yday $11,652.97 (+170.20) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `A` | 37 | $166.54 | $2.16 | $+316.53 | $11,821.01 | ▲ +316.53 after sell → book $11,821.01; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `BLSH` | 98 | $40.00 | $2.28 | — | $7,898.73 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list ohlc_hot; 🔵; ret5=+6.7; leftover $3940.34 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 190 | $20.65 | $2.56 | — | $3,972.67 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $3940.34 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLLN` | 33 | $116.00 | $2.09 | — | $142.58 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list probable,yday_gainer; 🔵; ⚪; ret5=+3.0; leftover $3940.34 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $142.58 | ▼ close $11,789.82 vs 09:30 $11,823.17 (session -24.26) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $142.58 | ▼ 09:30 equity $11,596.73 vs yday $11,789.82 (-193.09) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `BLSH` | 98 | $39.27 | $2.33 | $-76.16 | $3,988.71 | ▼ -76.16 after sell → book $11,594.40; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 190 | $20.52 | $2.62 | $-29.88 | $7,884.89 | ▼ -29.88 after sell → book $11,591.78; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BLLN` | 33 | $112.33 | $2.13 | $-125.33 | $11,589.65 | ▼ -125.33 after sell → book $11,589.65; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,589.65 | ▲ close $11,589.65 vs 09:30 $11,596.73 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,706.96 | ▲ 09:30 equity $10,706.96 vs yday $10,706.96 (+0.00) | 09:30 open · cash $10,706.96 · no holdings · equity $10,706.96 vs prior close $10,706.96 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `OMER` | 519 | $20.61 | $6.70 | — | $3.67 | — | Clock-B #10 NR7 + moderate momentum; gate clk_nr7_mom=True; rank hot_score; list flatten; 🔵; ret5=+9.1; leftover $10706.96 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.67 | ▼ close $10,425.19 vs 09:30 $10,706.96 (session -275.07) | 16:00 close · cash $3.67 · equity $10,425.19 vs 09:30 $10,706.96 (-281.77; session marks -275.07) · 1 name(s) marked open→close (per-name table). OMER×519 09:30 $20.61 → close $20.08 -275.07 | — |

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
| 2026-09-10 | `VLO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `A` | no_price | no 09:30 open — carry |
| 2026-09-22 | `MRAM` | no_price | no 09:30 open |
| 2026-09-24 | `DH` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `S` | hard_red | hard-red S=-7.66 sit; no new buys |
