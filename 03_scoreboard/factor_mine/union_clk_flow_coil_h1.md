# Factor mine action — `union_clk_flow_coil_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · Clock-B #8 flow-in + green + not extended

Cash book **-4.55%** ($9,545) · signal-only (no cash/fees) was -14.71%. Starts YES **10/30**. Fills 22 · skips 8 · realized $-732.31.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how many morning cameras are green vs red.
- Must-have: Clock-B #8: prior flow-in, last bar green, not already extended.
- Must-not: Clock-B #5 long veto: extreme prior extension plus diminishing progress or a failed breakout.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Sort the keepers by how many morning cameras are green vs red and keep the top 8.
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
- **Gate** `clk_flow_coil=True` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,267.69.

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
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `FUTU` | 84 | $117.65 | $2.24 | — | $115.16 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list earn_react; 🔵; ret5=+4.1; leftover $10000.00 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $115.16 | ▼ close $9,584.48 vs 09:30 $10,000.00 (session -413.28) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $115.16 | ▲ 09:30 equity $9,790.28 vs yday $9,584.48 (+205.80) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `FUTU` | 84 | $115.18 | $2.33 | $-212.06 | $9,787.94 | ▼ -212.06 after sell → book $9,787.94; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,787.94 | ▲ close $9,787.94 vs 09:30 $9,790.28 (session +0.00) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,787.94 | ▲ 09:30 equity $9,787.94 vs yday $9,787.94 (+0.00) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,787.94 | ▲ close $9,787.94 vs 09:30 $9,787.94 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,787.94 | ▲ 09:30 equity $9,787.94 vs yday $9,787.94 (+0.00) | — | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,787.94 | ▲ close $9,787.94 vs 09:30 $9,787.94 (session +0.00) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,787.94 | ▲ 09:30 equity $9,787.94 vs yday $9,787.94 (+0.00) | — | — |
| 2026-08-26 09:30 ET | **BUY** | `SJM` | 24 | $134.80 | $2.06 | — | $6,550.68 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list earn_react; 🔵; ret5=+5.9; leftover $3262.65 | — |
| 2026-08-26 09:30 ET | **BUY** | `URBN` | 41 | $78.90 | $2.11 | — | $3,313.67 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list overnight; 🔵; ret5=+1.6; leftover $3262.65 | — |
| 2026-08-26 09:30 ET | **BUY** | `NCNO` | 168 | $19.33 | $2.49 | — | $63.74 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list earn_react; ret5=+3.0; leftover $3262.65 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.74 | ▲ close $10,219.97 vs 09:30 $9,787.94 (session +438.69) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.74 | ▲ 09:30 equity $10,282.44 vs yday $10,219.97 (+62.47) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `SJM` | 24 | $130.29 | $2.10 | $-112.40 | $3,188.60 | ▼ -112.40 after sell → book $10,280.34; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `URBN` | 41 | $82.70 | $2.15 | $+151.54 | $6,577.15 | ▲ +151.54 after sell → book $10,278.19; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NCNO` | 168 | $22.03 | $2.55 | $+448.55 | $10,275.64 | ▲ +448.55 after sell → book $10,275.64; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,275.64 | ▲ close $10,275.64 vs 09:30 $10,282.44 (session +0.00) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,275.64 | ▲ 09:30 equity $10,275.64 vs yday $10,275.64 (-0.00) | — | — |
| 2026-08-28 09:30 ET | **BUY** | `ULTA` | 18 | $542.00 | $2.04 | — | $517.59 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list earn_react; ret5=+4.8; leftover $10275.64 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $517.59 | ▼ close $9,832.59 vs 09:30 $10,275.64 (session -441.00) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $517.59 | ▲ 09:30 equity $9,897.39 vs yday $9,832.59 (+64.80) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `ULTA` | 18 | $521.10 | $2.13 | $-380.37 | $9,895.26 | ▼ -380.37 after sell → book $9,895.26; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,895.26 | ▲ close $9,895.26 vs 09:30 $9,897.39 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,895.26 | ▲ 09:30 equity $9,895.26 vs yday $9,895.26 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,895.26 | ▲ close $9,895.26 vs 09:30 $9,895.26 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,895.26 | ▲ 09:30 equity $9,895.26 vs yday $9,895.26 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,895.26 | ▲ close $9,895.26 vs 09:30 $9,895.26 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,895.26 | ▲ 09:30 equity $9,895.26 vs yday $9,895.26 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `MOMO` | 899 | $5.50 | $11.60 | — | $4,939.17 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list earn_react; 🔵; ret5=-4.8; leftover $4947.63 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSXY` | 64 | $76.86 | $2.18 | — | $17.95 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list earn_react; 🔵; ret5=-6.6; leftover $4947.63 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.95 | ▼ close $9,315.81 vs 09:30 $9,895.26 (session -565.68) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.95 | ▲ 09:30 equity $9,342.13 vs yday $9,315.81 (+26.32) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `MOMO` | 899 | $5.13 | $11.78 | $-356.01 | $4,618.03 | ▼ -356.01 after sell → book $9,330.35; vs 09:30 mark -11.78 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSXY` | 64 | $73.63 | $2.23 | $-211.13 | $9,328.12 | ▼ -211.13 after sell → book $9,328.12; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `HAFN` | 1041 | $8.94 | $13.43 | — | $8.15 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list ohlc_hot; ret5=+7.7; leftover $9328.12 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.15 | ▲ close $9,606.17 vs 09:30 $9,342.13 (session +291.48) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.15 | ▼ 09:30 equity $9,179.36 vs yday $9,606.17 (-426.81) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `HAFN` | 1041 | $8.81 | $13.68 | $-162.43 | $9,165.69 | ▼ -162.43 after sell → book $9,165.69; vs 09:30 mark -13.67 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,165.69 | ▲ close $9,165.69 vs 09:30 $9,179.36 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,165.69 | ▲ 09:30 equity $9,165.69 vs yday $9,165.69 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,165.69 | ▲ close $9,165.69 vs 09:30 $9,165.69 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,165.69 | ▲ 09:30 equity $9,165.69 vs yday $9,165.69 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,165.69 | ▲ close $9,165.69 vs 09:30 $9,165.69 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,165.69 | ▲ 09:30 equity $9,165.69 vs yday $9,165.69 (-0.00) | — | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,165.69 | ▲ close $9,165.69 vs 09:30 $9,165.69 (session +0.00) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,165.69 | ▲ 09:30 equity $9,165.69 vs yday $9,165.69 (-0.00) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,165.69 | ▲ close $9,165.69 vs 09:30 $9,165.69 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,165.69 | ▲ 09:30 equity $9,165.69 vs yday $9,165.69 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,165.69 | ▲ close $9,165.69 vs 09:30 $9,165.69 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,165.69 | ▲ 09:30 equity $9,165.69 vs yday $9,165.69 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `LEN` | 113 | $80.63 | $2.33 | — | $52.17 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list overnight; ret5=-0.4; leftover $9165.69 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $52.17 | ▼ close $8,906.85 vs 09:30 $9,165.69 (session -256.51) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $52.17 | ▲ 09:30 equity $9,205.17 vs yday $8,906.85 (+298.32) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `LEN` | 113 | $81.00 | $2.42 | $+37.06 | $9,202.75 | ▲ +37.06 after sell → book $9,202.75; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,202.75 | ▲ close $9,202.75 vs 09:30 $9,205.17 (session +0.00) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,202.75 | ▲ 09:30 equity $9,202.75 vs yday $9,202.75 (-0.00) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,202.75 | ▲ close $9,202.75 vs 09:30 $9,202.75 (session +0.00) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,202.75 | ▲ 09:30 equity $9,202.75 vs yday $9,202.75 (-0.00) | — | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 29 | $157.87 | $2.08 | — | $4,622.44 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list flatten; ret5=+6.5; leftover $4601.37 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 11 | $386.20 | $2.02 | — | $372.22 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list flatten; ret5=-5.8; leftover $4601.37 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $372.22 | ▲ close $9,232.86 vs 09:30 $9,202.75 (session +34.21) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $372.22 | ▲ 09:30 equity $9,232.86 vs yday $9,232.86 (-0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $372.22 | ▲ close $9,232.86 vs 09:30 $9,232.86 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $372.22 | ▲ 09:30 equity $9,271.88 vs yday $9,232.86 (+39.02) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `A` | 29 | $166.54 | $2.13 | $+247.23 | $5,199.75 | ▲ +247.23 after sell → book $9,269.75; vs 09:30 mark -2.13 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `HUM` | 11 | $370.00 | $2.07 | $-182.29 | $9,267.69 | ▼ -182.29 after sell → book $9,267.69; vs 09:30 mark -2.06 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,267.69 | ▲ close $9,267.69 vs 09:30 $9,271.88 (session +0.00) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,267.69 | ▲ 09:30 equity $9,267.69 vs yday $9,267.69 (-0.00) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,267.69 | ▲ close $9,267.69 vs 09:30 $9,267.69 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,545.19 | ▲ 09:30 equity $9,545.19 vs yday $9,545.19 (+0.00) | 09:30 open · cash $9,545.19 · no holdings · equity $9,545.19 vs prior close $9,545.19 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,545.19 | ▲ close $9,545.19 vs 09:30 $9,545.19 (session +0.00) | 16:00 close · cash $9,545.19 · no lots left · equity $9,545.19. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `KEYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-09-08 | `KFY` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `LFMD` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `DBI` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-22 | `A` | no_price | no 09:30 open — carry |
| 2026-09-22 | `HUM` | no_price | no 09:30 open — carry |
| 2026-09-24 | `CHKP` | hard_red | hard-red S=-7.66 sit; no new buys |
