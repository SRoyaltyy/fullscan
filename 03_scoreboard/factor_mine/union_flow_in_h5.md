# Factor mine action — `union_flow_in_h5`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ flow_in hold 5, no 🚨

Cash book **-6.34%** ($9,366) · signal-only (no cash/fees) was -2.74%. Starts YES **16/30**. Fills 51 · skips 144 · realized $+404.42.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 5 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 5 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 5 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `flow_in=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $12.64.

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
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 197 | $50.62 | $2.58 | — | $24.65 | — | union ∩ flow_in hold 5, no 🚨; gate flow_in=True; list flatten; ⚪; ret5=+6.2; leftover $10000.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.65 | ▲ close $10,784.79 vs 09:30 $10,000.00 (session +787.37) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.65 | ▲ 09:30 equity $10,916.78 vs yday $10,784.79 (+131.99) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `KULR` | 2 | $2.50 | $0.06 | — | $19.59 | — | union ∩ flow_in hold 5, no 🚨; gate flow_in=True; list earn_react; 🔵; ⚪; ret5=+7.6; leftover $6.16 | — |
| 2026-08-14 09:30 ET | **BUY** | `NPWR` | 3 | $1.56 | $0.06 | — | $14.86 | — | union ∩ flow_in hold 5, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+8.0; leftover $6.16 | — |
| 2026-08-14 09:30 ET | **BUY** | `RLX` | 3 | $1.85 | $0.06 | — | $9.24 | — | union ∩ flow_in hold 5, no 🚨; gate flow_in=True; list earn_react; 🔵; ⚪; ret5=+0.5; leftover $6.16 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.24 | ▼ close $10,473.10 vs 09:30 $10,916.78 (session -443.50) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.24 | ▼ 09:30 equity $10,402.01 vs yday $10,473.10 (-71.09) | — | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.24 | ▼ close $10,223.97 vs 09:30 $10,402.01 (session -178.04) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.24 | ▼ 09:30 equity $10,223.67 vs yday $10,223.97 (-0.30) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.24 | ▲ close $10,272.76 vs 09:30 $10,223.67 (session +49.09) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.24 | ▲ 09:30 equity $10,320.18 vs yday $10,272.76 (+47.42) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.24 | ▲ close $10,501.50 vs 09:30 $10,320.18 (session +181.32) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.24 | ▼ 09:30 equity $10,477.85 vs yday $10,501.50 (-23.65) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 197 | $53.06 | $2.70 | $+474.77 | $10,459.36 | ▲ +474.77 after sell → book $10,475.16; vs 09:30 mark -2.69 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `FUTU` | 29 | $117.65 | $2.08 | — | $7,045.44 | — | union ∩ flow_in hold 5, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+4.1; leftover $3486.45 | — |
| 2026-08-20 09:30 ET | **BUY** | `WMT` | 32 | $106.38 | $2.09 | — | $3,639.19 | — | union ∩ flow_in hold 5, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-1.7; leftover $3486.45 | — |
| 2026-08-20 09:30 ET | **BUY** | `BJ` | 39 | $88.91 | $2.11 | — | $169.59 | — | union ∩ flow_in hold 5, no 🚨; gate flow_in=True; list overnight; 🔵; ret5=-1.0; leftover $3486.45 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $169.59 | ▼ close $10,338.00 vs 09:30 $10,477.85 (session -130.89) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $169.59 | ▲ 09:30 equity $10,508.93 vs yday $10,338.00 (+170.93) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `KULR` | 2 | $2.66 | $0.08 | $+0.18 | $174.84 | ▲ +0.18 after sell → book $10,508.86; vs 09:30 mark -0.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `NPWR` | 3 | $1.68 | $0.08 | $+0.22 | $179.80 | ▲ +0.22 after sell → book $10,508.78; vs 09:30 mark -0.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `RLX` | 3 | $1.82 | $0.08 | $-0.24 | $185.17 | ▼ -0.24 after sell → book $10,508.69; vs 09:30 mark -0.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `HITI` | 75 | $2.43 | $2.05 | — | $0.88 | — | union ∩ flow_in hold 5, no 🚨; gate flow_in=True; list mover_buy; 🔵; ⚪; ret5=+5.6; leftover $185.17 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.88 | ▲ close $10,848.97 vs 09:30 $10,508.93 (session +342.32) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.88 | ▼ 09:30 equity $10,809.89 vs yday $10,848.97 (-39.08) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.88 | ▼ close $10,792.66 vs 09:30 $10,809.89 (session -17.23) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.88 | ▲ 09:30 equity $10,795.01 vs yday $10,792.66 (+2.35) | — | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.88 | ▲ close $10,889.49 vs 09:30 $10,795.01 (session +94.48) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.88 | ▼ 09:30 equity $10,874.78 vs yday $10,889.49 (-14.71) | — | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.88 | ▼ close $10,861.49 vs 09:30 $10,874.78 (session -13.29) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.88 | ▼ 09:30 equity $10,808.52 vs yday $10,861.49 (-52.97) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `FUTU` | 29 | $128.00 | $2.12 | $+295.96 | $3,710.76 | ▲ +295.96 after sell → book $10,806.40; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WMT` | 32 | $103.61 | $2.12 | $-92.85 | $7,024.16 | ▼ -92.85 after sell → book $10,804.28; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BJ` | 39 | $92.08 | $2.15 | $+119.38 | $10,613.13 | ▲ +119.38 after sell → book $10,802.13; vs 09:30 mark -2.15 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `TRLV` | 466 | $11.38 | $6.01 | — | $5,304.04 | — | union ∩ flow_in hold 5, no 🚨; gate flow_in=True; list ohlc_hot; ret5=+13.3; leftover $5306.57 | — |
| 2026-08-27 09:30 ET | **BUY** | `BOX` | 156 | $33.79 | $2.46 | — | $30.34 | — | union ∩ flow_in hold 5, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+0.8; leftover $5306.57 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.34 | ▼ close $10,782.51 vs 09:30 $10,808.52 (session -11.15) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.34 | ▼ 09:30 equity $10,770.09 vs yday $10,782.51 (-12.42) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `HITI` | 75 | $2.57 | $2.17 | $+6.28 | $220.92 | ▲ +6.28 after sell → book $10,767.92; vs 09:30 mark -2.17 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `JKS` | 5 | $13.37 | $0.68 | — | $153.38 | — | union ∩ flow_in hold 5, no 🚨; gate flow_in=True; list yday_mover; ret5=-14.9; leftover $73.64 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $153.38 | ▲ close $11,186.08 vs 09:30 $10,770.09 (session +418.85) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $153.38 | ▼ 09:30 equity $11,136.20 vs yday $11,186.08 (-49.88) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $153.38 | ▲ close $11,161.62 vs 09:30 $11,136.20 (session +25.42) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $153.38 | ▼ 09:30 equity $11,161.16 vs yday $11,161.62 (-0.46) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $153.38 | ▲ close $11,171.43 vs 09:30 $11,161.16 (session +10.27) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $153.38 | ▲ 09:30 equity $11,174.47 vs yday $11,171.43 (+3.04) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $153.38 | ▼ close $11,059.26 vs 09:30 $11,174.47 (session -115.21) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $153.38 | ▲ 09:30 equity $11,261.66 vs yday $11,059.26 (+202.40) | — | — |
| 2026-09-03 09:30 ET | **SELL** | `TRLV` | 466 | $11.89 | $6.13 | $+225.52 | $5,687.99 | ▲ +225.52 after sell → book $11,255.53; vs 09:30 mark -6.13 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `BOX` | 156 | $35.29 | $2.53 | $+229.01 | $11,190.70 | ▲ +229.01 after sell → book $11,253.00; vs 09:30 mark -2.53 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `VIR` | 161 | $11.54 | $2.47 | — | $9,330.29 | — | union ∩ flow_in hold 5, no 🚨; gate flow_in=True; list yday_gainer; 🔵; ⚪; ret5=+3.8; leftover $1865.12 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 5 | $351.74 | $2.00 | — | $7,569.58 | — | union ∩ flow_in hold 5, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=+3.3; leftover $1865.12 | — |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 7 | $257.00 | $2.01 | — | $5,768.57 | — | union ∩ flow_in hold 5, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-5.5; leftover $1865.12 | — |
| 2026-09-03 09:30 ET | **BUY** | `MOMO` | 339 | $5.50 | $4.37 | — | $3,899.70 | — | union ∩ flow_in hold 5, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-4.8; leftover $1865.12 | — |
| 2026-09-03 09:30 ET | **BUY** | `PVH` | 24 | $74.96 | $2.06 | — | $2,098.60 | — | union ∩ flow_in hold 5, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-7.9; leftover $1865.12 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSXY` | 24 | $76.86 | $2.06 | — | $251.90 | — | union ∩ flow_in hold 5, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-6.6; leftover $1865.12 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $251.90 | ▼ close $10,857.12 vs 09:30 $11,261.66 (session -380.90) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $251.90 | ▲ 09:30 equity $10,857.32 vs yday $10,857.12 (+0.20) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `JKS` | 5 | $12.14 | $0.64 | $-7.48 | $311.95 | ▼ -7.48 after sell → book $10,856.67; vs 09:30 mark -0.65 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `WNC` | 3 | $14.17 | $0.43 | — | $269.01 | — | union ∩ flow_in hold 5, no 🚨; gate flow_in=True; list yday_gainer; 🔵; ret5=+7.9; leftover $44.56 | — |
| 2026-09-04 09:30 ET | **BUY** | `ADCT` | 34 | $1.30 | $0.54 | — | $224.27 | — | union ∩ flow_in hold 5, no 🚨; gate flow_in=True; list ohlc_hot; 🔵; ⚪; ret5=+17.9; leftover $44.56 | — |
| 2026-09-04 09:30 ET | **BUY** | `HAFN` | 4 | $8.94 | $0.37 | — | $188.14 | — | union ∩ flow_in hold 5, no 🚨; gate flow_in=True; list ohlc_hot; ret5=+7.7; leftover $44.56 | — |
| 2026-09-04 09:30 ET | **BUY** | `XP` | 2 | $19.67 | $0.40 | — | $148.40 | — | union ∩ flow_in hold 5, no 🚨; gate flow_in=True; list mover_buy; 🔵; ⚪; ret5=+13.1; leftover $44.56 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $148.40 | ▲ close $11,119.84 vs 09:30 $10,857.32 (session +264.91) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $148.40 | ▼ 09:30 equity $11,037.96 vs yday $11,119.84 (-81.88) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $148.40 | ▲ close $11,107.18 vs 09:30 $11,037.96 (session +69.22) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $148.40 | ▼ 09:30 equity $11,021.85 vs yday $11,107.18 (-85.33) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $148.40 | ▼ close $10,813.29 vs 09:30 $11,021.85 (session -208.56) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $148.40 | ▼ 09:30 equity $10,689.99 vs yday $10,813.29 (-123.30) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $148.40 | ▼ close $10,632.19 vs 09:30 $10,689.99 (session -57.80) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $148.40 | ▲ 09:30 equity $10,756.50 vs yday $10,632.19 (+124.31) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `VIR` | 161 | $10.79 | $2.51 | $-125.74 | $1,883.07 | ▼ -125.74 after sell → book $10,753.98; vs 09:30 mark -2.52 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `AVGO` | 5 | $364.85 | $2.03 | $+61.52 | $3,705.29 | ▲ +61.52 after sell → book $10,751.95; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `FIVE` | 7 | $243.49 | $2.03 | $-98.62 | $5,407.69 | ▼ -98.62 after sell → book $10,749.92; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `MOMO` | 339 | $5.06 | $4.44 | $-157.98 | $7,118.59 | ▼ -157.98 after sell → book $10,745.48; vs 09:30 mark -4.44 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `PVH` | 24 | $70.39 | $2.09 | $-113.83 | $8,805.86 | ▼ -113.83 after sell → book $10,743.39; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `VSXY` | 24 | $74.30 | $2.09 | $-65.59 | $10,586.97 | ▼ -65.59 after sell → book $10,741.30; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **BUY** | `SSL` | 737 | $14.35 | $9.51 | — | $1.52 | — | union ∩ flow_in hold 5, no 🚨; gate flow_in=True; list ohlc_hot; 🔵; ret5=+15.5; leftover $10586.97 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.52 | ▲ close $10,905.62 vs 09:30 $10,756.50 (session +173.82) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.52 | ▲ 09:30 equity $10,976.76 vs yday $10,905.62 (+71.14) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `WNC` | 3 | $12.15 | $0.39 | $-6.89 | $37.57 | ▼ -6.89 after sell → book $10,976.36; vs 09:30 mark -0.40 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADCT` | 34 | $1.07 | $0.49 | $-8.85 | $73.47 | ▼ -8.85 after sell → book $10,975.88; vs 09:30 mark -0.48 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `HAFN` | 4 | $9.35 | $0.41 | $+0.86 | $110.46 | ▲ +0.86 after sell → book $10,975.47; vs 09:30 mark -0.41 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `XP` | 2 | $19.24 | $0.41 | $-1.67 | $148.53 | ▼ -1.67 after sell → book $10,975.06; vs 09:30 mark -0.41 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $148.53 | ▼ close $10,835.03 vs 09:30 $10,976.76 (session -140.03) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $148.53 | ▼ 09:30 equity $10,827.66 vs yday $10,835.03 (-7.37) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $148.53 | ▲ close $11,284.60 vs 09:30 $10,827.66 (session +456.94) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $148.53 | ▼ 09:30 equity $10,923.47 vs yday $11,284.60 (-361.13) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 1 | $40.93 | $0.41 | — | $107.19 | — | union ∩ flow_in hold 5, no 🚨; gate flow_in=True; list earn_react; ret5=-3.1; leftover $49.51 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $107.19 | ▼ close $10,679.35 vs 09:30 $10,923.47 (session -243.71) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $107.19 | ▼ 09:30 equity $10,296.47 vs yday $10,679.35 (-382.88) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $107.19 | ▲ close $10,568.73 vs 09:30 $10,296.47 (session +272.26) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $107.19 | ▼ 09:30 equity $10,414.21 vs yday $10,568.73 (-154.52) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `SSL` | 737 | $13.93 | $9.71 | $-328.76 | $10,363.89 | ▼ -328.76 after sell → book $10,404.50; vs 09:30 mark -9.71 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,363.89 | ▲ close $10,404.57 vs 09:30 $10,414.21 (session +0.07) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,363.89 | ▲ 09:30 equity $10,404.89 vs yday $10,404.57 (+0.32) | — | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 16 | $157.87 | $2.04 | — | $7,835.93 | — | union ∩ flow_in hold 5, no 🚨; gate flow_in=True; list flatten; ret5=+6.5; leftover $2590.97 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 6 | $386.20 | $2.01 | — | $5,516.72 | — | union ∩ flow_in hold 5, no 🚨; gate flow_in=True; list flatten; ret5=-5.8; leftover $2590.97 | — |
| 2026-09-21 09:30 ET | **BUY** | `UMC` | 103 | $24.93 | $2.30 | — | $2,946.63 | — | union ∩ flow_in hold 5, no 🚨; gate flow_in=True; list ohlc_hot; ret5=+8.7; leftover $2590.97 | — |
| 2026-09-21 09:30 ET | **BUY** | `NEO` | 130 | $19.92 | $2.38 | — | $354.65 | — | union ∩ flow_in hold 5, no 🚨; gate flow_in=True; list ohlc_hot; ret5=+15.0; leftover $2590.97 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $354.65 | ▲ close $10,400.41 vs 09:30 $10,404.89 (session +4.25) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $354.65 | ▼ 09:30 equity $10,382.90 vs yday $10,400.41 (-17.51) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $354.65 | ▲ close $10,435.43 vs 09:30 $10,382.90 (session +52.53) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $354.65 | ▼ 09:30 equity $10,313.83 vs yday $10,435.43 (-121.60) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `TCOM` | 1 | $41.00 | $0.43 | $-0.78 | $395.22 | ▼ -0.78 after sell → book $10,313.40; vs 09:30 mark -0.43 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **BUY** | `CBRL` | 8 | $47.57 | $2.01 | — | $12.64 | — | union ∩ flow_in hold 5, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-11.2; leftover $395.22 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.64 | ▼ close $10,215.44 vs 09:30 $10,313.83 (session -95.94) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.64 | ▼ 09:30 equity $10,142.55 vs yday $10,215.44 (-72.89) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.64 | ▲ close $10,499.82 vs 09:30 $10,142.55 (session +357.27) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.06 | ▲ 09:30 equity $9,366.30 vs yday $9,365.75 (+0.55) | 09:30 open · cash $1.06 (unchanged overnight, no fees) · equity $9,366.30 vs prior close $9,365.75 (+0.55) · 3 name(s) re-marked at the open (per-name table). BB×1065 yday $8.73 → 09:30 $8.73 +0.00; CBRL×1 yday $51.84 → 09:30 $52.39 +0.55; PGEN×2 yday $7.70 → 09:30 $7.70 +0.00 | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.06 | ▼ close $9,365.72 vs 09:30 $9,366.30 (session -0.58) | 16:00 close · cash $1.06 · equity $9,365.72 vs 09:30 $9,366.30 (-0.58; session marks -0.58) · 3 name(s) marked open→close (per-name table). BB×1065 09:30 $8.73 → close $8.73 -0.00; CBRL×1 09:30 $52.39 → close $51.81 -0.58; PGEN×2 09:30 $7.70 → close $7.70 -0.00 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `SPHR` | cash | leftover split 6.16 < 1 share @ 176.68 |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `KULR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `NPWR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `RLX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `XP` | cash | leftover split 9.24 < 1 share @ 15.93 |
| 2026-08-18 | `TPG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `KULR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `NPWR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `RLX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KEYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AURA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CIG` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CRDL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `TPG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `KULR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `NPWR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `RLX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WB` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | `KULR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `NPWR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `RLX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `FUTU` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `WMT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `FUTU` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `WMT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `BJ` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `HITI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-25 | `FUTU` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `WMT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `BJ` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `HITI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ZYME` | cash | leftover split 0.29 < 1 share @ 28.86 |
| 2026-08-25 | `RHI` | cash | leftover split 0.29 < 1 share @ 43.76 |
| 2026-08-25 | `ABUS` | cash | leftover split 0.29 < 1 share @ 5.25 |
| 2026-08-26 | `FUTU` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `WMT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `BJ` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `HITI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `NCNO` | cash | leftover split 0.22 < 1 share @ 19.33 |
| 2026-08-26 | `PLAB` | cash | leftover split 0.22 < 1 share @ 37.26 |
| 2026-08-26 | `SJM` | cash | leftover split 0.22 < 1 share @ 134.80 |
| 2026-08-26 | `URBN` | cash | leftover split 0.22 < 1 share @ 78.90 |
| 2026-08-27 | `HITI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-28 | `TRLV` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `BOX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `DY` | cash | leftover split 73.64 < 1 share @ 306.34 |
| 2026-08-28 | `ULTA` | cash | leftover split 73.64 < 1 share @ 542.00 |
| 2026-08-31 | `TRLV` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `BOX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `JKS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `TENB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `TRLV` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `BOX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `JKS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `AME` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `TRLV` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `BOX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `JKS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | `JKS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-04 | `VIR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `AVGO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `FIVE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `MOMO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `PVH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `VSXY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `ATRC` | cash | leftover split 44.56 < 1 share @ 52.03 |
| 2026-09-04 | `CRDO` | cash | leftover split 44.56 < 1 share @ 162.10 |
| 2026-09-04 | `DOCU` | cash | leftover split 44.56 < 1 share @ 68.52 |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `AVGO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `FIVE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `MOMO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `PVH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `VSXY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `WNC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `ADCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `HAFN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `XP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `KFY` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `VIR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `AVGO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `FIVE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `MOMO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `PVH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `VSXY` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `WNC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `ADCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `HAFN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `XP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LFMD` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NAMS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `JMKE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `VIR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `AVGO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `FIVE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `MOMO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `PVH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `VSXY` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `WNC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `ADCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `HAFN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `XP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `DBI` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-11 | `WNC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `ADCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `HAFN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `XP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-14 | `SSL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-15 | `SSL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `LFMD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TCOM` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-16 | `SSL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `ATRC` | cash | leftover split 49.51 < 1 share @ 55.66 |
| 2026-09-16 | `LEN` | cash | leftover split 49.51 < 1 share @ 80.63 |
| 2026-09-17 | `SSL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `TCOM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `TCOM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `TCOM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `TCOM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `A` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `HUM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `UMC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `NEO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `CBRL` | no_price | no 09:30 open |
| 2026-09-23 | `A` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `HUM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `UMC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `NEO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-24 | `A` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `HUM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `UMC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `NEO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `CBRL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `CHKP` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `FUL` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `A` | 16 | 2026-09-21 @ $157.87 | union ∩ flow_in hold 5, no 🚨; gate flow_in=True; list flatten; ret5=+6.5; leftover $2590.97 |
| `HUM` | 6 | 2026-09-21 @ $386.20 | union ∩ flow_in hold 5, no 🚨; gate flow_in=True; list flatten; ret5=-5.8; leftover $2590.97 |
| `UMC` | 103 | 2026-09-21 @ $24.93 | union ∩ flow_in hold 5, no 🚨; gate flow_in=True; list ohlc_hot; ret5=+8.7; leftover $2590.97 |
| `NEO` | 130 | 2026-09-21 @ $19.92 | union ∩ flow_in hold 5, no 🚨; gate flow_in=True; list ohlc_hot; ret5=+15.0; leftover $2590.97 |
| `CBRL` | 8 | 2026-09-23 @ $47.57 | union ∩ flow_in hold 5, no 🚨; gate flow_in=True; list earn_react; 🔵; ret5=-11.2; leftover $395.22 |
