# Factor mine action — `union_flow_in_white_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-0.73%** ($9,927) · signal-only (no cash/fees) was +10.92%. Starts YES **15/30**. Fills 24 · skips 0 · realized $+806.69.

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
- Must-have: no morning camera is red (the 'white' / all-clear row).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).
- Must-not: the news camera (does the morning packet like the headline?) is red.

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
- **Gate** `flow_in=True,zero_red=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,806.69.

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
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 197 | $50.62 | $2.58 | — | $24.65 | — | combo gate; gate flow_in=True,zero_red=True; list flatten; ⚪; ret5=+6.2; leftover $10000.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.65 | ▲ close $10,784.79 vs 09:30 $10,000.00 (session +787.37) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.65 | ▲ 09:30 equity $10,916.78 vs yday $10,784.79 (+131.99) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 197 | $55.29 | $2.70 | $+914.08 | $10,914.08 | ▲ +914.08 after sell → book $10,914.08; vs 09:30 mark -2.70 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `SPHR` | 20 | $176.68 | $2.05 | — | $7,378.43 | — | combo gate; gate flow_in=True,zero_red=True; list ohlc_hot; 🔵; ⚪; ret5=+14.4; leftover $3638.03 | — |
| 2026-08-14 09:30 ET | **BUY** | `KULR` | 1455 | $2.50 | $18.77 | — | $3,722.16 | — | combo gate; gate flow_in=True,zero_red=True; list earn_react; 🔵; ⚪; ret5=+7.6; leftover $3638.03 | — |
| 2026-08-14 09:30 ET | **BUY** | `RLX` | 1966 | $1.85 | $25.36 | — | $59.70 | — | combo gate; gate flow_in=True,zero_red=True; list earn_react; 🔵; ⚪; ret5=+0.5; leftover $3638.03 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $59.70 | ▲ close $11,074.94 vs 09:30 $10,916.78 (session +207.04) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.70 | ▼ 09:30 equity $11,023.07 vs yday $11,074.94 (-51.87) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `SPHR` | 20 | $168.10 | $2.09 | $-175.74 | $3,419.61 | ▼ -175.74 after sell → book $11,020.98; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `KULR` | 1455 | $2.63 | $19.04 | $+151.34 | $7,227.22 | ▲ +151.34 after sell → book $11,001.94; vs 09:30 mark -19.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `RLX` | 1966 | $1.92 | $25.72 | $+86.54 | $10,976.22 | ▲ +86.54 after sell → book $10,976.22; vs 09:30 mark -25.72 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XP` | 688 | $15.93 | $8.88 | — | $7.51 | — | combo gate; gate flow_in=True,zero_red=True; list overnight; ⚪; ret5=-2.6; leftover $10976.22 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.51 | ▼ close $10,809.11 vs 09:30 $11,023.07 (session -158.24) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.51 | ▲ 09:30 equity $10,809.11 vs yday $10,809.11 (-0.00) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `XP` | 688 | $15.70 | $9.08 | $-176.19 | $10,800.03 | ▼ -176.19 after sell → book $10,800.03; vs 09:30 mark -9.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,800.03 | ▲ close $10,800.03 vs 09:30 $10,809.11 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,800.03 | ▲ 09:30 equity $10,800.03 vs yday $10,800.03 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,800.03 | ▲ close $10,800.03 vs 09:30 $10,800.03 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,800.03 | ▲ 09:30 equity $10,800.03 vs yday $10,800.03 (-0.00) | — | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,800.03 | ▲ close $10,800.03 vs 09:30 $10,800.03 (session +0.00) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,800.03 | ▲ 09:30 equity $10,800.03 vs yday $10,800.03 (-0.00) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `HITI` | 4420 | $2.43 | $57.02 | — | $2.41 | — | combo gate; gate flow_in=True,zero_red=True; list mover_buy; 🔵; ⚪; ret5=+5.6; leftover $10800.03 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.41 | ▲ close $10,831.41 vs 09:30 $10,800.03 (session +88.40) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.41 | ▲ 09:30 equity $10,831.41 vs yday $10,831.41 (+0.00) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `HITI` | 4420 | $2.45 | $57.84 | $-26.46 | $10,773.57 | ▼ -26.46 after sell → book $10,773.57; vs 09:30 mark -57.84 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,773.57 | ▲ close $10,773.57 vs 09:30 $10,831.41 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,773.57 | ▲ 09:30 equity $10,773.57 vs yday $10,773.57 (+0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 123 | $43.76 | $2.36 | — | $5,388.73 | — | combo gate; gate flow_in=True,zero_red=True; list mover_buy; 🔵; ⚪; ret5=+6.2; leftover $5386.79 | — |
| 2026-08-25 09:30 ET | **BUY** | `ABUS` | 1023 | $5.25 | $13.20 | — | $4.79 | — | combo gate; gate flow_in=True,zero_red=True; list mover_buy; 🔵; ⚪; ret5=+10.4; leftover $5386.79 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.79 | ▲ close $10,847.09 vs 09:30 $10,773.57 (session +89.07) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.79 | ▼ 09:30 equity $10,766.75 vs yday $10,847.09 (-80.34) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `RHI` | 123 | $44.33 | $2.42 | $+65.33 | $5,454.95 | ▲ +65.33 after sell → book $10,764.32; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ABUS` | 1023 | $5.19 | $13.41 | $-87.99 | $10,750.92 | ▼ -87.99 after sell → book $10,750.92; vs 09:30 mark -13.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,750.92 | ▲ close $10,750.92 vs 09:30 $10,766.75 (session +0.00) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,750.92 | ▲ 09:30 equity $10,750.92 vs yday $10,750.92 (-0.00) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,750.92 | ▲ close $10,750.92 vs 09:30 $10,750.92 (session +0.00) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,750.92 | ▲ 09:30 equity $10,750.92 vs yday $10,750.92 (-0.00) | — | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,750.92 | ▲ close $10,750.92 vs 09:30 $10,750.92 (session +0.00) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,750.92 | ▲ 09:30 equity $10,750.92 vs yday $10,750.92 (-0.00) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,750.92 | ▲ close $10,750.92 vs 09:30 $10,750.92 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,750.92 | ▲ 09:30 equity $10,750.92 vs yday $10,750.92 (-0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,750.92 | ▲ close $10,750.92 vs 09:30 $10,750.92 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,750.92 | ▲ 09:30 equity $10,750.92 vs yday $10,750.92 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,750.92 | ▲ close $10,750.92 vs 09:30 $10,750.92 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,750.92 | ▲ 09:30 equity $10,750.92 vs yday $10,750.92 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `VIR` | 930 | $11.54 | $12.00 | — | $6.72 | — | combo gate; gate flow_in=True,zero_red=True; list yday_gainer; 🔵; ⚪; ret5=+3.8; leftover $10750.92 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.72 | ▼ close $10,655.22 vs 09:30 $10,750.92 (session -83.70) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.72 | ▼ 09:30 equity $10,525.02 vs yday $10,655.22 (-130.20) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `VIR` | 930 | $11.31 | $12.24 | $-238.13 | $10,512.78 | ▼ -238.13 after sell → book $10,512.78; vs 09:30 mark -12.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ATRC` | 67 | $52.03 | $2.19 | — | $7,024.58 | — | combo gate; gate flow_in=True,zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.5; leftover $3504.26 | — |
| 2026-09-04 09:30 ET | **BUY** | `ADCT` | 2695 | $1.30 | $34.77 | — | $3,486.32 | — | combo gate; gate flow_in=True,zero_red=True; list ohlc_hot; 🔵; ⚪; ret5=+17.9; leftover $3504.26 | — |
| 2026-09-04 09:30 ET | **BUY** | `XP` | 177 | $19.67 | $2.52 | — | $2.21 | — | combo gate; gate flow_in=True,zero_red=True; list mover_buy; 🔵; ⚪; ret5=+13.1; leftover $3504.26 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.21 | ▲ close $10,634.47 vs 09:30 $10,525.02 (session +161.16) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.21 | ▲ 09:30 equity $10,846.75 vs yday $10,634.47 (+212.28) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 67 | $54.31 | $2.23 | $+148.34 | $3,638.74 | ▲ +148.34 after sell → book $10,844.51; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ADCT` | 2695 | $1.33 | $35.24 | $+10.84 | $7,187.85 | ▲ +10.84 after sell → book $10,809.27; vs 09:30 mark -35.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `XP` | 177 | $20.46 | $2.58 | $+134.73 | $10,806.69 | ▲ +134.73 after sell → book $10,806.69; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,806.69 | ▲ close $10,806.69 vs 09:30 $10,846.75 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,806.69 | ▲ 09:30 equity $10,806.69 vs yday $10,806.69 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,806.69 | ▲ close $10,806.69 vs 09:30 $10,806.69 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,806.69 | ▲ 09:30 equity $10,806.69 vs yday $10,806.69 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,806.69 | ▲ close $10,806.69 vs 09:30 $10,806.69 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,806.69 | ▲ 09:30 equity $10,806.69 vs yday $10,806.69 (+0.00) | — | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,806.69 | ▲ close $10,806.69 vs 09:30 $10,806.69 (session +0.00) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,806.69 | ▲ 09:30 equity $10,806.69 vs yday $10,806.69 (+0.00) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,806.69 | ▲ close $10,806.69 vs 09:30 $10,806.69 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,806.69 | ▲ 09:30 equity $10,806.69 vs yday $10,806.69 (+0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,806.69 | ▲ close $10,806.69 vs 09:30 $10,806.69 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,806.69 | ▲ 09:30 equity $10,806.69 vs yday $10,806.69 (+0.00) | — | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,806.69 | ▲ close $10,806.69 vs 09:30 $10,806.69 (session +0.00) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,806.69 | ▲ 09:30 equity $10,806.69 vs yday $10,806.69 (+0.00) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,806.69 | ▲ close $10,806.69 vs 09:30 $10,806.69 (session +0.00) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,806.69 | ▲ 09:30 equity $10,806.69 vs yday $10,806.69 (+0.00) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,806.69 | ▲ close $10,806.69 vs 09:30 $10,806.69 (session +0.00) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,806.69 | ▲ 09:30 equity $10,806.69 vs yday $10,806.69 (+0.00) | — | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,806.69 | ▲ close $10,806.69 vs 09:30 $10,806.69 (session +0.00) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,806.69 | ▲ 09:30 equity $10,806.69 vs yday $10,806.69 (+0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,806.69 | ▲ close $10,806.69 vs 09:30 $10,806.69 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,806.69 | ▲ 09:30 equity $10,806.69 vs yday $10,806.69 (+0.00) | — | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,806.69 | ▲ close $10,806.69 vs 09:30 $10,806.69 (session +0.00) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,806.69 | ▲ 09:30 equity $10,806.69 vs yday $10,806.69 (+0.00) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,806.69 | ▲ close $10,806.69 vs 09:30 $10,806.69 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,926.95 | ▲ 09:30 equity $9,926.95 vs yday $9,926.95 (+0.00) | 09:30 open · cash $9,926.95 · no holdings · equity $9,926.95 vs prior close $9,926.95 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,926.95 | ▲ close $9,926.95 vs 09:30 $9,926.95 (session +0.00) | 16:00 close · cash $9,926.95 · no lots left · equity $9,926.95. | — |
