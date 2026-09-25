# Factor mine action — `union_flow_in_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ flow_in, no 🚨

Cash book **+0.84%** ($10,084) · signal-only (no cash/fees) was +17.31%. Starts YES **6/30**. Fills 88 · skips 27 · realized $+766.09.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,766.07.

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
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 197 | $50.62 | $2.58 | — | $24.65 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list flatten; ⚪; ret5=+6.2; leftover $10000.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.65 | ▲ close $10,784.79 vs 09:30 $10,000.00 (session +787.37) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.65 | ▲ 09:30 equity $10,916.78 vs yday $10,784.79 (+131.99) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 197 | $55.29 | $2.70 | $+914.08 | $10,914.08 | ▲ +914.08 after sell → book $10,914.08; vs 09:30 mark -2.70 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `SPHR` | 15 | $176.68 | $2.04 | — | $8,261.84 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; 🔵; ⚪; ret5=+14.4; leftover $2728.52 | — |
| 2026-08-14 09:30 ET | **BUY** | `KULR` | 1091 | $2.50 | $14.07 | — | $5,520.27 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ⚪; ret5=+7.6; leftover $2728.52 | — |
| 2026-08-14 09:30 ET | **BUY** | `NPWR` | 1749 | $1.56 | $22.56 | — | $2,769.27 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+8.0; leftover $2728.52 | — |
| 2026-08-14 09:30 ET | **BUY** | `RLX` | 1474 | $1.85 | $19.01 | — | $23.35 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ⚪; ret5=+0.5; leftover $2728.52 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.35 | ▲ close $11,693.70 vs 09:30 $10,916.78 (session +837.31) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.35 | ▼ 09:30 equity $11,602.34 vs yday $11,693.70 (-91.36) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `SPHR` | 15 | $168.10 | $2.07 | $-132.80 | $2,542.79 | ▼ -132.80 after sell → book $11,600.28; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `KULR` | 1091 | $2.63 | $14.28 | $+113.48 | $5,397.84 | ▲ +113.48 after sell → book $11,586.00; vs 09:30 mark -14.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NPWR` | 1749 | $1.92 | $22.88 | $+584.20 | $8,733.04 | ▲ +584.20 after sell → book $11,563.12; vs 09:30 mark -22.88 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `RLX` | 1474 | $1.92 | $19.28 | $+64.88 | $11,543.84 | ▲ +64.88 after sell → book $11,543.84; vs 09:30 mark -19.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XP` | 724 | $15.93 | $9.34 | — | $1.18 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list overnight; ⚪; ret5=-2.6; leftover $11543.84 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.18 | ▼ close $11,367.98 vs 09:30 $11,602.34 (session -166.52) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.18 | ▲ 09:30 equity $11,367.98 vs yday $11,367.98 (-0.00) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `XP` | 724 | $15.70 | $9.55 | $-185.41 | $11,358.43 | ▼ -185.41 after sell → book $11,358.43; vs 09:30 mark -9.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,358.43 | ▲ close $11,358.43 vs 09:30 $11,367.98 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,358.43 | ▲ 09:30 equity $11,358.43 vs yday $11,358.43 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,358.43 | ▲ close $11,358.43 vs 09:30 $11,358.43 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,358.43 | ▲ 09:30 equity $11,358.43 vs yday $11,358.43 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `FUTU` | 32 | $117.65 | $2.09 | — | $7,591.54 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+4.1; leftover $3786.14 | — |
| 2026-08-20 09:30 ET | **BUY** | `WMT` | 35 | $106.38 | $2.10 | — | $3,866.15 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-1.7; leftover $3786.14 | — |
| 2026-08-20 09:30 ET | **BUY** | `BJ` | 42 | $88.91 | $2.12 | — | $129.81 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list overnight; 🔵; ret5=-1.0; leftover $3786.14 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $129.81 | ▼ close $11,206.17 vs 09:30 $11,358.43 (session -145.96) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.81 | ▲ 09:30 equity $11,391.88 vs yday $11,206.17 (+185.71) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `FUTU` | 32 | $115.18 | $2.13 | $-83.25 | $3,813.45 | ▼ -83.25 after sell → book $11,389.76; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WMT` | 35 | $103.69 | $2.13 | $-98.38 | $7,440.46 | ▼ -98.38 after sell → book $11,387.62; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `HITI` | 3045 | $2.43 | $39.28 | — | $1.83 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list mover_buy; 🔵; ⚪; ret5=+5.6; leftover $7440.46 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.83 | ▲ close $11,511.72 vs 09:30 $11,391.88 (session +163.38) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.83 | ▲ 09:30 equity $11,536.92 vs yday $11,511.72 (+25.20) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `BJ` | 42 | $97.02 | $2.16 | $+336.35 | $4,074.51 | ▲ +336.35 after sell → book $11,534.76; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HITI` | 3045 | $2.45 | $39.85 | $-18.23 | $11,494.92 | ▼ -18.23 after sell → book $11,494.92; vs 09:30 mark -39.84 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,494.92 | ▲ close $11,494.92 vs 09:30 $11,536.92 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,494.92 | ▲ 09:30 equity $11,494.92 vs yday $11,494.92 (-0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 132 | $28.86 | $2.39 | — | $7,683.01 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; 🔵; ret5=+13.7; leftover $3831.64 | — |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 87 | $43.76 | $2.25 | — | $3,873.64 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list mover_buy; 🔵; ⚪; ret5=+6.2; leftover $3831.64 | — |
| 2026-08-25 09:30 ET | **BUY** | `ABUS` | 729 | $5.25 | $9.40 | — | $36.99 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list mover_buy; 🔵; ⚪; ret5=+10.4; leftover $3831.64 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $36.99 | ▼ close $11,360.13 vs 09:30 $11,494.92 (session -120.75) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $36.99 | ▼ 09:30 equity $11,315.13 vs yday $11,360.13 (-45.00) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `ZYME` | 132 | $27.56 | $2.44 | $-176.42 | $3,672.47 | ▼ -176.42 after sell → book $11,312.69; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RHI` | 87 | $44.33 | $2.30 | $+45.04 | $7,526.88 | ▲ +45.04 after sell → book $11,310.39; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ABUS` | 729 | $5.19 | $9.56 | $-62.70 | $11,300.84 | ▼ -62.70 after sell → book $11,300.84; vs 09:30 mark -9.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `NCNO` | 146 | $19.33 | $2.43 | — | $8,476.23 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; ret5=+3.0; leftover $2825.21 | — |
| 2026-08-26 09:30 ET | **BUY** | `PLAB` | 75 | $37.26 | $2.21 | — | $5,679.51 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-8.0; leftover $2825.21 | — |
| 2026-08-26 09:30 ET | **BUY** | `SJM` | 20 | $134.80 | $2.05 | — | $2,981.46 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+5.9; leftover $2825.21 | — |
| 2026-08-26 09:30 ET | **BUY** | `URBN` | 35 | $78.90 | $2.10 | — | $217.87 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list overnight; 🔵; ret5=+1.6; leftover $2825.21 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $217.87 | ▼ close $11,148.33 vs 09:30 $11,315.13 (session -143.72) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $217.87 | ▲ 09:30 equity $11,193.55 vs yday $11,148.33 (+45.22) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `NCNO` | 146 | $22.03 | $2.48 | $+389.29 | $3,431.77 | ▲ +389.29 after sell → book $11,191.07; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PLAB` | 75 | $30.12 | $2.25 | $-539.96 | $5,688.52 | ▼ -539.96 after sell → book $11,188.82; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SJM` | 20 | $130.29 | $2.08 | $-94.33 | $8,292.24 | ▼ -94.33 after sell → book $11,186.74; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `URBN` | 35 | $82.70 | $2.13 | $+128.78 | $11,184.62 | ▲ +128.78 after sell → book $11,184.62; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `TRLV` | 491 | $11.38 | $6.33 | — | $5,590.70 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; ret5=+13.3; leftover $5592.31 | — |
| 2026-08-27 09:30 ET | **BUY** | `BOX` | 165 | $33.79 | $2.48 | — | $12.87 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+0.8; leftover $5592.31 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.87 | ▼ close $11,160.70 vs 09:30 $11,193.55 (session -15.10) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.87 | ▼ 09:30 equity $11,147.62 vs yday $11,160.70 (-13.08) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `TRLV` | 491 | $11.00 | $6.46 | $-199.37 | $5,407.41 | ▼ -199.37 after sell → book $11,141.16; vs 09:30 mark -6.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BOX` | 165 | $34.75 | $2.56 | $+153.36 | $11,138.60 | ▲ +153.36 after sell → book $11,138.60; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `JKS` | 277 | $13.37 | $3.57 | — | $7,431.54 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list yday_mover; ret5=-14.9; leftover $3712.87 | — |
| 2026-08-28 09:30 ET | **BUY** | `DY` | 12 | $306.34 | $2.03 | — | $3,753.43 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list yday_mover; ret5=-23.0; leftover $3712.87 | — |
| 2026-08-28 09:30 ET | **BUY** | `ULTA` | 6 | $542.00 | $2.01 | — | $499.42 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; ret5=+4.8; leftover $3712.87 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $499.42 | ▼ close $10,887.08 vs 09:30 $11,147.62 (session -243.91) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $499.42 | ▲ 09:30 equity $10,952.72 vs yday $10,887.08 (+65.64) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `JKS` | 277 | $13.54 | $3.65 | $+39.87 | $4,246.35 | ▲ +39.87 after sell → book $10,949.07; vs 09:30 mark -3.65 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DY` | 12 | $298.01 | $2.06 | $-104.05 | $7,820.41 | ▼ -104.05 after sell → book $10,947.01; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ULTA` | 6 | $521.10 | $2.04 | $-129.45 | $10,944.97 | ▼ -129.45 after sell → book $10,944.97; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,944.97 | ▲ close $10,944.97 vs 09:30 $10,952.72 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,944.97 | ▲ 09:30 equity $10,944.97 vs yday $10,944.97 (-0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,944.97 | ▲ close $10,944.97 vs 09:30 $10,944.97 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,944.97 | ▲ 09:30 equity $10,944.97 vs yday $10,944.97 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,944.97 | ▲ close $10,944.97 vs 09:30 $10,944.97 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,944.97 | ▲ 09:30 equity $10,944.97 vs yday $10,944.97 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `VIR` | 158 | $11.54 | $2.46 | — | $9,119.18 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list yday_gainer; 🔵; ⚪; ret5=+3.8; leftover $1824.16 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 5 | $351.74 | $2.00 | — | $7,358.48 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+3.3; leftover $1824.16 | — |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 7 | $257.00 | $2.01 | — | $5,557.47 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-5.5; leftover $1824.16 | — |
| 2026-09-03 09:30 ET | **BUY** | `MOMO` | 331 | $5.50 | $4.27 | — | $3,732.70 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-4.8; leftover $1824.16 | — |
| 2026-09-03 09:30 ET | **BUY** | `PVH` | 24 | $74.96 | $2.06 | — | $1,931.59 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-7.9; leftover $1824.16 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSXY` | 23 | $76.86 | $2.06 | — | $161.76 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-6.6; leftover $1824.16 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $161.76 | ▼ close $10,557.24 vs 09:30 $10,944.97 (session -372.86) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $161.76 | ▲ 09:30 equity $10,557.88 vs yday $10,557.24 (+0.64) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `VIR` | 158 | $11.31 | $2.50 | $-41.31 | $1,946.23 | ▼ -41.31 after sell → book $10,555.37; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 5 | $359.70 | $2.03 | $+35.77 | $3,742.70 | ▲ +35.77 after sell → book $10,553.34; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FIVE` | 7 | $238.88 | $2.03 | $-130.89 | $5,412.83 | ▼ -130.89 after sell → book $10,551.31; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MOMO` | 331 | $5.13 | $4.34 | $-131.08 | $7,106.52 | ▼ -131.08 after sell → book $10,546.97; vs 09:30 mark -4.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PVH` | 24 | $72.79 | $2.09 | $-56.23 | $8,851.39 | ▼ -56.23 after sell → book $10,544.88; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSXY` | 23 | $73.63 | $2.08 | $-78.43 | $10,542.80 | ▼ -78.43 after sell → book $10,542.80; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ATRC` | 28 | $52.03 | $2.07 | — | $9,083.89 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.5; leftover $1506.11 | — |
| 2026-09-04 09:30 ET | **BUY** | `WNC` | 106 | $14.17 | $2.31 | — | $7,579.56 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list yday_gainer; 🔵; ret5=+7.9; leftover $1506.11 | — |
| 2026-09-04 09:30 ET | **BUY** | `CRDO` | 9 | $162.10 | $2.02 | — | $6,118.64 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list yday_mover; 🔵; ret5=-31.7; leftover $1506.11 | — |
| 2026-09-04 09:30 ET | **BUY** | `ADCT` | 1158 | $1.30 | $14.94 | — | $4,598.30 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; 🔵; ⚪; ret5=+17.9; leftover $1506.11 | — |
| 2026-09-04 09:30 ET | **BUY** | `HAFN` | 168 | $8.94 | $2.49 | — | $3,093.89 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; ret5=+7.7; leftover $1506.11 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 21 | $68.52 | $2.05 | — | $1,652.92 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+3.4; leftover $1506.11 | — |
| 2026-09-04 09:30 ET | **BUY** | `XP` | 76 | $19.67 | $2.22 | — | $155.78 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list mover_buy; 🔵; ⚪; ret5=+13.1; leftover $1506.11 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $155.78 | ▲ close $10,720.14 vs 09:30 $10,557.88 (session +205.44) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $155.78 | ▼ 09:30 equity $10,701.91 vs yday $10,720.14 (-18.23) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 28 | $54.31 | $2.10 | $+59.67 | $1,674.36 | ▲ +59.67 after sell → book $10,699.82; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `WNC` | 106 | $14.22 | $2.34 | $+0.65 | $3,179.34 | ▲ +0.65 after sell → book $10,697.48; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRDO` | 9 | $170.54 | $2.04 | $+71.95 | $4,712.21 | ▲ +71.95 after sell → book $10,695.44; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ADCT` | 1158 | $1.33 | $15.14 | $+4.66 | $6,237.21 | ▲ +4.66 after sell → book $10,680.30; vs 09:30 mark -15.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `HAFN` | 168 | $8.81 | $2.53 | $-26.87 | $7,714.75 | ▼ -26.87 after sell → book $10,677.76; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DOCU` | 21 | $67.05 | $2.07 | $-35.00 | $9,120.73 | ▼ -35.00 after sell → book $10,675.69; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `XP` | 76 | $20.46 | $2.24 | $+55.58 | $10,673.45 | ▲ +55.58 after sell → book $10,673.45; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,673.45 | ▲ close $10,673.45 vs 09:30 $10,701.91 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,673.45 | ▲ 09:30 equity $10,673.45 vs yday $10,673.45 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,673.45 | ▲ close $10,673.45 vs 09:30 $10,673.45 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,673.45 | ▲ 09:30 equity $10,673.45 vs yday $10,673.45 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,673.45 | ▲ close $10,673.45 vs 09:30 $10,673.45 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,673.45 | ▲ 09:30 equity $10,673.45 vs yday $10,673.45 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `SSL` | 743 | $14.35 | $9.58 | — | $1.81 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; 🔵; ret5=+15.5; leftover $10673.45 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.81 | ▲ close $10,842.18 vs 09:30 $10,673.45 (session +178.32) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.81 | ▲ 09:30 equity $10,916.48 vs yday $10,842.18 (+74.30) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `SSL` | 743 | $14.69 | $9.80 | $+233.24 | $10,906.69 | ▲ +233.24 after sell → book $10,906.69; vs 09:30 mark -9.79 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,906.69 | ▲ close $10,906.69 vs 09:30 $10,916.48 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,906.69 | ▲ 09:30 equity $10,906.69 vs yday $10,906.69 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,906.69 | ▲ close $10,906.69 vs 09:30 $10,906.69 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,906.69 | ▲ 09:30 equity $10,906.69 vs yday $10,906.69 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `ATRC` | 65 | $55.66 | $2.19 | — | $7,286.60 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; 🔵; ret5=+4.6; leftover $3635.56 | — |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 88 | $40.93 | $2.25 | — | $3,682.51 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; ret5=-3.1; leftover $3635.56 | — |
| 2026-09-16 09:30 ET | **BUY** | `LEN` | 45 | $80.63 | $2.12 | — | $52.03 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list overnight; ret5=-0.4; leftover $3635.56 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $52.03 | ▼ close $10,850.17 vs 09:30 $10,906.69 (session -49.95) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $52.03 | ▲ 09:30 equity $11,053.95 vs yday $10,850.17 (+203.78) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `ATRC` | 65 | $57.96 | $2.23 | $+145.09 | $3,817.21 | ▲ +145.09 after sell → book $11,051.73; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TCOM` | 88 | $40.79 | $2.30 | $-16.87 | $7,404.43 | ▼ -16.87 after sell → book $11,049.43; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `LEN` | 45 | $81.00 | $2.16 | $+12.36 | $11,047.26 | ▲ +12.36 after sell → book $11,047.26; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,047.26 | ▲ close $11,047.26 vs 09:30 $11,053.95 (session +0.00) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,047.26 | ▲ 09:30 equity $11,047.26 vs yday $11,047.26 (+0.00) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,047.26 | ▲ close $11,047.26 vs 09:30 $11,047.26 (session +0.00) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,047.26 | ▲ 09:30 equity $11,047.26 vs yday $11,047.26 (+0.00) | — | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 17 | $157.87 | $2.04 | — | $8,361.43 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list flatten; ret5=+6.5; leftover $2761.82 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 7 | $386.20 | $2.01 | — | $5,656.02 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list flatten; ret5=-5.8; leftover $2761.82 | — |
| 2026-09-21 09:30 ET | **BUY** | `UMC` | 110 | $24.93 | $2.32 | — | $2,911.40 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; ret5=+8.7; leftover $2761.82 | — |
| 2026-09-21 09:30 ET | **BUY** | `NEO` | 138 | $19.92 | $2.40 | — | $160.04 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list ohlc_hot; ret5=+15.0; leftover $2761.82 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $160.04 | ▲ close $11,038.96 vs 09:30 $11,047.26 (session +0.47) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $160.04 | ▼ 09:30 equity $11,020.26 vs yday $11,038.96 (-18.70) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `UMC` | 110 | $25.26 | $2.36 | $+31.62 | $2,936.28 | ▲ +31.62 after sell → book $11,017.90; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,936.28 | ▲ close $11,017.90 vs 09:30 $11,020.26 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,936.28 | ▼ 09:30 equity $10,936.68 vs yday $11,017.90 (-81.22) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `A` | 17 | $166.54 | $2.07 | $+143.28 | $5,765.38 | ▲ +143.28 after sell → book $10,934.60; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `HUM` | 7 | $370.00 | $2.04 | $-117.45 | $8,353.34 | ▼ -117.45 after sell → book $10,932.56; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `NEO` | 138 | $18.69 | $2.45 | $-174.59 | $10,930.11 | ▼ -174.59 after sell → book $10,930.11; vs 09:30 mark -2.45 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `CBRL` | 229 | $47.57 | $2.95 | — | $33.63 | — | union ∩ flow_in, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-11.2; leftover $10930.11 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $33.63 | ▼ close $10,915.71 vs 09:30 $10,936.68 (session -11.45) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $33.63 | ▼ 09:30 equity $10,769.15 vs yday $10,915.71 (-146.56) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `CBRL` | 229 | $46.88 | $3.08 | $-164.04 | $10,766.07 | ▼ -164.04 after sell → book $10,766.07; vs 09:30 mark -3.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,766.07 | ▲ close $10,766.07 vs 09:30 $10,769.15 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,084.16 | ▲ 09:30 equity $10,084.16 vs yday $10,084.16 (+0.00) | 09:30 open · cash $10,084.16 · no holdings · equity $10,084.16 vs prior close $10,084.16 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,084.16 | ▲ close $10,084.16 vs 09:30 $10,084.16 (session +0.00) | 16:00 close · cash $10,084.16 · no lots left · equity $10,084.16. | — |

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
| 2026-09-10 | `DBI` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-15 | `LFMD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TCOM` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `A` | no_price | no 09:30 open — carry |
| 2026-09-22 | `HUM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `NEO` | no_price | no 09:30 open — carry |
| 2026-09-22 | `CBRL` | no_price | no 09:30 open |
| 2026-09-24 | `CHKP` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `FUL` | hard_red | hard-red S=-7.66 sit; no new buys |
