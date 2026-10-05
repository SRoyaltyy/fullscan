# Factor mine action — `ohlc_hot_coil_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `ohlc_hot` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · hot list ∩ not exploded

Cash book **-9.52%** ($9,048) · signal-only (no cash/fees) was -11.86%. Starts YES **9/30**. Fills 207 · skips 74 · realized $-1849.08.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at names that looked hot on the prior price/volume tape and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: names that looked hot on the prior price/volume tape.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: prior 5-session return is at least 0%.
- Must-have: prior 5-session return is at most 10% (not already exploded).
- Must-have: prior relative volume is at most 2.2 (not a blow-off).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on names that looked hot on the prior price/volume tape that pass the must-haves.
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

- **Universe** `ohlc_hot` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $5,362.22.

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
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 605 | $16.50 | $7.80 | — | $9.70 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $10000.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.70 | ▼ close $9,792.55 vs 09:30 $10,000.00 (session -199.65) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.70 | ▼ 09:30 equity $9,526.35 vs yday $9,792.55 (-266.20) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 605 | $15.73 | $7.98 | $-481.64 | $9,518.36 | ▼ -481.64 after sell → book $9,518.36; vs 09:30 mark -7.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 173 | $18.24 | $2.51 | — | $6,360.34 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $3172.79 | — |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 195 | $16.20 | $2.58 | — | $3,198.76 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $3172.79 | — |
| 2026-08-17 09:30 ET | **BUY** | `NEWP` | 457 | $6.94 | $5.90 | — | $21.29 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.1; leftover $3172.79 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.29 | ▼ close $9,216.87 vs 09:30 $9,526.35 (session -290.52) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.29 | ▼ 09:30 equity $8,876.06 vs yday $9,216.87 (-340.81) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 173 | $16.20 | $2.56 | $-357.99 | $2,821.32 | ▼ -357.99 after sell → book $8,873.49; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 195 | $15.78 | $2.63 | $-87.11 | $5,895.79 | ▼ -87.11 after sell → book $8,870.86; vs 09:30 mark -2.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NEWP` | 457 | $6.51 | $6.00 | $-208.40 | $8,864.87 | ▼ -208.40 after sell → book $8,864.87; vs 09:30 mark -5.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,864.87 | ▲ close $8,864.87 vs 09:30 $8,876.06 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,864.87 | ▲ 09:30 equity $8,864.87 vs yday $8,864.87 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,864.87 | ▲ close $8,864.87 vs 09:30 $8,864.87 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,864.87 | ▲ 09:30 equity $8,864.87 vs yday $8,864.87 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `NIQ` | 121 | $18.31 | $2.35 | — | $6,647.00 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.9; leftover $2216.22 | — |
| 2026-08-20 09:30 ET | **BUY** | `AUGO` | 26 | $83.58 | $2.07 | — | $4,471.86 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+9.6; leftover $2216.22 | — |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 83 | $26.57 | $2.24 | — | $2,264.31 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+4.8; leftover $2216.22 | — |
| 2026-08-20 09:30 ET | **BUY** | `PAYS` | 161 | $13.76 | $2.47 | — | $46.47 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+7.8; leftover $2216.22 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $46.47 | ▲ close $8,881.24 vs 09:30 $8,864.87 (session +25.51) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $46.47 | ▲ 09:30 equity $8,995.63 vs yday $8,881.24 (+114.39) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `NIQ` | 121 | $18.30 | $2.39 | $-5.95 | $2,258.38 | ▼ -5.95 after sell → book $8,993.24; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AUGO` | 26 | $89.10 | $2.10 | $+139.36 | $4,572.89 | ▲ +139.36 after sell → book $8,991.15; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 83 | $26.25 | $2.27 | $-31.07 | $6,749.37 | ▼ -31.07 after sell → book $8,988.88; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `PAYS` | 161 | $13.91 | $2.52 | $+19.16 | $8,986.36 | ▲ +19.16 after sell → book $8,986.36; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 2600 | $0.86 | $30.26 | — | $6,709.70 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $2246.59 | — |
| 2026-08-21 09:30 ET | **BUY** | `EMBC` | 413 | $5.43 | $5.33 | — | $4,461.78 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+7.0; leftover $2246.59 | — |
| 2026-08-21 09:30 ET | **BUY** | `TXG` | 34 | $64.39 | $2.09 | — | $2,270.43 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $2246.59 | — |
| 2026-08-21 09:30 ET | **BUY** | `DXYZ` | 64 | $34.89 | $2.18 | — | $35.28 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.6; leftover $2246.59 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.28 | ▼ close $8,900.87 vs 09:30 $8,995.63 (session -45.62) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.28 | ▼ 09:30 equity $8,760.32 vs yday $8,900.87 (-140.55) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 2600 | $0.89 | $31.39 | $+5.95 | $2,317.89 | ▲ +5.95 after sell → book $8,728.93; vs 09:30 mark -31.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `EMBC` | 413 | $5.20 | $5.41 | $-107.80 | $4,458.02 | ▼ -107.80 after sell → book $8,723.52; vs 09:30 mark -5.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TXG` | 34 | $63.15 | $2.12 | $-46.37 | $6,603.00 | ▼ -46.37 after sell → book $8,721.40; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DXYZ` | 64 | $33.10 | $2.21 | $-118.95 | $8,719.19 | ▼ -118.95 after sell → book $8,719.19; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,719.19 | ▲ close $8,719.19 vs 09:30 $8,760.32 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,719.19 | ▲ 09:30 equity $8,719.19 vs yday $8,719.19 (-0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 428 | $4.07 | $5.52 | — | $6,971.71 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+4.9; leftover $1743.84 | — |
| 2026-08-25 09:30 ET | **BUY** | `AVAH` | 127 | $13.62 | $2.37 | — | $5,238.96 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; leftover $1743.84 | — |
| 2026-08-25 09:30 ET | **BUY** | `ETON` | 27 | $64.55 | $2.07 | — | $3,494.04 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+4.4; leftover $1743.84 | — |
| 2026-08-25 09:30 ET | **BUY** | `INO` | 1395 | $1.25 | $18.00 | — | $1,732.29 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+8.7; leftover $1743.84 | — |
| 2026-08-25 09:30 ET | **BUY** | `ANRO` | 47 | $36.52 | $2.13 | — | $13.72 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+7.9; leftover $1743.84 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.72 | ▼ close $8,668.68 vs 09:30 $8,719.19 (session -20.42) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.72 | ▼ 09:30 equity $8,563.35 vs yday $8,668.68 (-105.33) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `XHG` | 428 | $3.81 | $5.61 | $-122.41 | $1,638.80 | ▼ -122.41 after sell → book $8,557.75; vs 09:30 mark -5.60 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AVAH` | 127 | $13.65 | $2.41 | $-1.60 | $3,369.94 | ▼ -1.60 after sell → book $8,555.34; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ETON` | 27 | $63.60 | $2.09 | $-29.82 | $5,085.05 | ▼ -29.82 after sell → book $8,553.25; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ANRO` | 47 | $35.80 | $2.15 | $-38.13 | $6,765.49 | ▼ -38.13 after sell → book $8,551.09; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `CRDL` | 3311 | $2.03 | $42.71 | — | $1.45 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+5.5; leftover $6765.49 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.45 | ▲ close $8,900.49 vs 09:30 $8,563.35 (session +392.11) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.45 | ▼ 09:30 equity $8,720.99 vs yday $8,900.49 (-179.50) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 3311 | $2.09 | $43.32 | $+112.63 | $6,878.12 | ▲ +112.63 after sell → book $8,677.67; vs 09:30 mark -43.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `HTFL` | 20 | $48.92 | $2.05 | — | $5,897.67 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+7.5; leftover $982.59 | — |
| 2026-08-27 09:30 ET | **BUY** | `NABL` | 253 | $3.87 | $3.26 | — | $4,915.30 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+9.8; leftover $982.59 | — |
| 2026-08-27 09:30 ET | **BUY** | `SRRK` | 16 | $60.00 | $2.04 | — | $3,953.26 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+6.2; leftover $982.59 | — |
| 2026-08-27 09:30 ET | **BUY** | `PAGP` | 35 | $28.00 | $2.10 | — | $2,971.17 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+7.8; leftover $982.59 | — |
| 2026-08-27 09:30 ET | **BUY** | `DASH` | 4 | $235.94 | $2.00 | — | $2,025.40 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+7.6; leftover $982.59 | — |
| 2026-08-27 09:30 ET | **BUY** | `PRGO` | 67 | $14.63 | $2.19 | — | $1,043.00 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+5.7; leftover $982.59 | — |
| 2026-08-27 09:30 ET | **BUY** | `AEO` | 56 | $17.27 | $2.16 | — | $73.73 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+5.5; leftover $982.59 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $73.73 | ▼ close $8,546.98 vs 09:30 $8,720.99 (session -114.90) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $73.73 | ▲ 09:30 equity $8,646.30 vs yday $8,546.98 (+99.32) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `INO` | 1395 | $1.27 | $18.24 | $-8.34 | $1,827.13 | ▼ -8.34 after sell → book $8,628.05; vs 09:30 mark -18.25 | dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `HTFL` | 20 | $48.50 | $2.07 | $-12.52 | $2,795.06 | ▼ -12.52 after sell → book $8,625.98; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NABL` | 253 | $4.25 | $3.32 | $+89.56 | $3,867.00 | ▲ +89.56 after sell → book $8,622.67; vs 09:30 mark -3.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SRRK` | 16 | $58.75 | $2.06 | $-24.10 | $4,804.94 | ▼ -24.10 after sell → book $8,620.61; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `PAGP` | 35 | $28.08 | $2.12 | $-1.41 | $5,785.63 | ▼ -1.41 after sell → book $8,618.50; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DASH` | 4 | $233.37 | $2.02 | $-14.30 | $6,717.08 | ▼ -14.30 after sell → book $8,616.47; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `PRGO` | 67 | $14.09 | $2.21 | $-40.58 | $7,658.90 | ▼ -40.58 after sell → book $8,614.26; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AEO` | 56 | $17.06 | $2.18 | $-16.10 | $8,612.08 | ▼ -16.10 after sell → book $8,612.08; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `MRNA` | 20 | $137.19 | $2.05 | — | $5,866.23 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+7.1; leftover $2870.69 | — |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 151 | $19.00 | $2.44 | — | $2,994.79 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+7.5; leftover $2870.69 | — |
| 2026-08-28 09:30 ET | **BUY** | `FSM` | 223 | $12.84 | $2.88 | — | $128.59 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+7.6; leftover $2870.69 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $128.59 | ▼ close $8,423.42 vs 09:30 $8,646.30 (session -181.29) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $128.59 | ▼ 09:30 equity $8,281.45 vs yday $8,423.42 (-141.97) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 20 | $134.10 | $2.08 | $-65.93 | $2,808.51 | ▼ -65.93 after sell → book $8,279.37; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TH` | 151 | $18.12 | $2.49 | $-137.06 | $5,542.90 | ▼ -137.06 after sell → book $8,276.88; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FSM` | 223 | $12.26 | $2.94 | $-135.15 | $8,273.94 | ▼ -135.15 after sell → book $8,273.94; vs 09:30 mark -2.94 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,273.94 | ▲ close $8,273.94 vs 09:30 $8,281.45 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,273.94 | ▲ 09:30 equity $8,273.94 vs yday $8,273.94 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,273.94 | ▲ close $8,273.94 vs 09:30 $8,273.94 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,273.94 | ▲ 09:30 equity $8,273.94 vs yday $8,273.94 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,273.94 | ▲ close $8,273.94 vs 09:30 $8,273.94 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,273.94 | ▲ 09:30 equity $8,273.94 vs yday $8,273.94 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 455 | $3.63 | $5.87 | — | $6,616.42 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1654.79 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 98 | $16.77 | $2.28 | — | $4,970.68 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1654.79 | — |
| 2026-09-03 09:30 ET | **BUY** | `EBS` | 252 | $6.56 | $3.25 | — | $3,314.31 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ⚪; ret5=+8.2; leftover $1654.79 | — |
| 2026-09-03 09:30 ET | **BUY** | `GALT` | 367 | $4.50 | $4.73 | — | $1,658.07 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.0; leftover $1654.79 | — |
| 2026-09-03 09:30 ET | **BUY** | `CTVA` | 18 | $90.24 | $2.04 | — | $31.71 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.6; leftover $1654.79 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $31.71 | ▼ close $7,901.56 vs 09:30 $8,273.94 (session -354.20) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $31.71 | ▼ 09:30 equity $7,879.94 vs yday $7,901.56 (-21.62) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `CABA` | 455 | $3.46 | $5.96 | $-89.18 | $1,600.05 | ▼ -89.18 after sell → book $7,873.98; vs 09:30 mark -5.96 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 98 | $15.61 | $2.31 | $-118.28 | $3,127.52 | ▼ -118.28 after sell → book $7,871.67; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `EBS` | 252 | $6.26 | $3.31 | $-82.16 | $4,701.73 | ▼ -82.16 after sell → book $7,868.36; vs 09:30 mark -3.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `GALT` | 367 | $4.33 | $4.81 | $-71.93 | $6,286.04 | ▼ -71.93 after sell → book $7,863.56; vs 09:30 mark -4.80 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CTVA` | 18 | $87.64 | $2.07 | $-50.91 | $7,861.49 | ▼ -50.91 after sell → book $7,861.49; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 124 | $7.87 | $2.36 | — | $6,883.25 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.7; leftover $982.69 | — |
| 2026-09-04 09:30 ET | **BUY** | `GORO` | 248 | $3.95 | $3.20 | — | $5,900.45 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+6.9; leftover $982.69 | — |
| 2026-09-04 09:30 ET | **BUY** | `CRCL` | 10 | $97.98 | $2.02 | — | $4,918.63 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+9.5; leftover $982.69 | — |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 7 | $137.35 | $2.01 | — | $3,955.17 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+5.4; leftover $982.69 | — |
| 2026-09-04 09:30 ET | **BUY** | `BLSH` | 28 | $34.69 | $2.07 | — | $2,981.77 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+7.9; leftover $982.69 | — |
| 2026-09-04 09:30 ET | **BUY** | `ZETA` | 30 | $32.65 | $2.08 | — | $2,000.19 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+8.1; leftover $982.69 | — |
| 2026-09-04 09:30 ET | **BUY** | `HAFN` | 109 | $8.94 | $2.32 | — | $1,023.42 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+7.7; leftover $982.69 | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 4 | $236.82 | $2.00 | — | $74.13 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+8.1; leftover $982.69 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $74.13 | ▲ close $8,071.71 vs 09:30 $7,879.94 (session +228.29) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $74.13 | ▼ 09:30 equity $7,999.38 vs yday $8,071.71 (-72.33) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 124 | $7.76 | $2.39 | $-18.39 | $1,033.98 | ▼ -18.39 after sell → book $7,996.99; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `GORO` | 248 | $4.13 | $3.25 | $+38.19 | $2,054.97 | ▲ +38.19 after sell → book $7,993.74; vs 09:30 mark -3.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRCL` | 10 | $100.65 | $2.04 | $+22.64 | $3,059.43 | ▲ +22.64 after sell → book $7,991.70; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MSTR` | 7 | $137.62 | $2.03 | $-2.15 | $4,020.74 | ▼ -2.15 after sell → book $7,989.67; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BLSH` | 28 | $35.90 | $2.09 | $+29.71 | $5,023.85 | ▲ +29.71 after sell → book $7,987.58; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ZETA` | 30 | $31.08 | $2.10 | $-51.28 | $5,954.15 | ▼ -51.28 after sell → book $7,985.48; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 4 | $267.76 | $2.02 | $+119.74 | $7,023.16 | ▲ +119.74 after sell → book $7,983.45; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,023.16 | ▲ close $7,999.80 vs 09:30 $7,999.38 (session +16.35) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,023.16 | ▲ 09:30 equity $8,004.16 vs yday $7,999.80 (+4.36) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `HAFN` | 109 | $9.00 | $2.35 | $+1.88 | $8,001.82 | ▲ +1.88 after sell → book $8,001.82; vs 09:30 mark -2.34 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,001.82 | ▲ close $8,001.82 vs 09:30 $8,004.16 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,001.82 | ▲ 09:30 equity $8,001.82 vs yday $8,001.82 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,001.82 | ▲ close $8,001.82 vs 09:30 $8,001.82 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,001.82 | ▲ 09:30 equity $8,001.82 vs yday $8,001.82 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 490 | $2.04 | $6.32 | — | $6,995.90 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1000.23 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 210 | $4.75 | $2.71 | — | $5,995.69 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1000.23 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 471 | $2.12 | $6.08 | — | $4,991.09 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1000.23 | — |
| 2026-09-11 09:30 ET | **BUY** | `TJGC` | 93 | $10.65 | $2.27 | — | $3,998.37 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+6.3; leftover $1000.23 | — |
| 2026-09-11 09:30 ET | **BUY** | `HAFN` | 107 | $9.32 | $2.31 | — | $2,998.82 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+5.4; leftover $1000.23 | — |
| 2026-09-11 09:30 ET | **BUY** | `INSP` | 14 | $69.88 | $2.03 | — | $2,018.47 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.0; leftover $1000.23 | — |
| 2026-09-11 09:30 ET | **BUY** | `FRO` | 20 | $48.05 | $2.05 | — | $1,055.42 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+6.6; leftover $1000.23 | — |
| 2026-09-11 09:30 ET | **BUY** | `GME` | 47 | $21.04 | $2.13 | — | $64.41 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+7.5; leftover $1000.23 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $64.41 | ▲ close $8,085.77 vs 09:30 $8,001.82 (session +109.85) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $64.41 | ▼ 09:30 equity $8,075.25 vs yday $8,085.77 (-10.52) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 490 | $2.01 | $6.41 | $-27.43 | $1,042.90 | ▼ -27.43 after sell → book $8,068.84; vs 09:30 mark -6.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CLOV` | 210 | $4.82 | $2.75 | $+9.24 | $2,052.34 | ▲ +9.24 after sell → book $8,066.09; vs 09:30 mark -2.75 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 471 | $2.05 | $6.16 | $-45.21 | $3,011.73 | ▼ -45.21 after sell → book $8,059.92; vs 09:30 mark -6.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `TJGC` | 93 | $11.24 | $2.29 | $+50.77 | $4,055.22 | ▲ +50.77 after sell → book $8,057.63; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `HAFN` | 107 | $9.35 | $2.34 | $-1.44 | $5,053.33 | ▼ -1.44 after sell → book $8,055.29; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `INSP` | 14 | $72.14 | $2.05 | $+27.56 | $6,061.24 | ▲ +27.56 after sell → book $8,053.24; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,061.24 | ▲ close $8,087.78 vs 09:30 $8,075.25 (session +34.54) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,061.24 | ▲ 09:30 equity $8,096.41 vs yday $8,087.78 (+8.63) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `GME` | 47 | $21.51 | $2.15 | $+17.81 | $7,070.06 | ▲ +17.81 after sell → book $8,094.26; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,070.06 | ▲ close $8,101.86 vs 09:30 $8,096.41 (session +7.60) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,070.06 | ▲ 09:30 equity $8,120.46 vs yday $8,101.86 (+18.60) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `FRO` | 20 | $52.52 | $2.07 | $+85.28 | $8,118.39 | ▲ +85.28 after sell → book $8,118.39; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 1) | — |
| 2026-09-16 09:30 ET | **BUY** | `META` | 1 | $679.91 | $1.99 | — | $7,436.49 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+9.3; leftover $1014.80 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 25 | $39.99 | $2.06 | — | $6,434.67 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+9.3; leftover $1014.80 | — |
| 2026-09-16 09:30 ET | **BUY** | `KR` | 16 | $61.93 | $2.04 | — | $5,441.75 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.8; leftover $1014.80 | — |
| 2026-09-16 09:30 ET | **BUY** | `ATRC` | 18 | $55.66 | $2.04 | — | $4,437.83 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+4.6; leftover $1014.80 | — |
| 2026-09-16 09:30 ET | **BUY** | `APA` | 21 | $46.44 | $2.05 | — | $3,460.54 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.9; leftover $1014.80 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 5 | $189.17 | $2.00 | — | $2,512.68 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+7.9; leftover $1014.80 | — |
| 2026-09-16 09:30 ET | **BUY** | `TK` | 71 | $14.26 | $2.20 | — | $1,498.02 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+6.8; leftover $1014.80 | — |
| 2026-09-16 09:30 ET | **BUY** | `KGS` | 17 | $58.00 | $2.04 | — | $509.98 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+9.1; leftover $1014.80 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $509.98 | ▼ close $8,023.50 vs 09:30 $8,120.46 (session -78.45) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $509.98 | ▲ 09:30 equity $8,064.83 vs yday $8,023.50 (+41.33) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `META` | 1 | $682.44 | $2.01 | $-1.48 | $1,190.40 | ▼ -1.48 after sell → book $8,062.81; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 25 | $37.57 | $2.08 | $-64.65 | $2,127.57 | ▼ -64.65 after sell → book $8,060.73; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `KR` | 16 | $61.02 | $2.06 | $-18.66 | $3,101.83 | ▼ -18.66 after sell → book $8,058.67; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ATRC` | 18 | $57.96 | $2.06 | $+37.29 | $4,143.05 | ▲ +37.29 after sell → book $8,056.61; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `APA` | 21 | $44.63 | $2.07 | $-42.14 | $5,078.20 | ▼ -42.14 after sell → book $8,054.53; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 5 | $190.35 | $2.02 | $+1.87 | $6,027.93 | ▲ +1.87 after sell → book $8,052.51; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 132 | $7.59 | $2.39 | — | $5,023.66 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $1004.65 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 418 | $2.40 | $5.39 | — | $4,015.07 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $1004.65 | — |
| 2026-09-17 09:30 ET | **BUY** | `QTRX` | 341 | $2.94 | $4.40 | — | $3,008.13 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list yday_gainer,ohlc_hot; 🔵; ret5=+9.8; leftover $1004.65 | — |
| 2026-09-17 09:30 ET | **BUY** | `BNC` | 199 | $5.03 | $2.59 | — | $2,004.57 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+7.4; leftover $1004.65 | — |
| 2026-09-17 09:30 ET | **BUY** | `SFL` | 74 | $13.55 | $2.21 | — | $999.66 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+7.8; leftover $1004.65 | — |
| 2026-09-17 09:30 ET | **BUY** | `PUMP` | 96 | $10.31 | $2.28 | — | $7.62 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+5.9; leftover $1004.65 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.62 | ▲ close $8,212.71 vs 09:30 $8,064.83 (session +179.46) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.62 | ▲ 09:30 equity $8,294.19 vs yday $8,212.71 (+81.48) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `TK` | 71 | $14.60 | $2.22 | $+19.71 | $1,042.00 | ▲ +19.71 after sell → book $8,291.97; vs 09:30 mark -2.22 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `KGS` | 17 | $58.38 | $2.06 | $+2.36 | $2,032.40 | ▲ +2.36 after sell → book $8,289.91; vs 09:30 mark -2.06 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 132 | $7.98 | $2.42 | $+46.68 | $3,083.34 | ▲ +46.68 after sell → book $8,287.49; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SABR` | 418 | $2.29 | $5.47 | $-56.84 | $4,035.09 | ▼ -56.84 after sell → book $8,282.02; vs 09:30 mark -5.47 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `QTRX` | 341 | $3.12 | $4.47 | $+52.52 | $5,094.54 | ▲ +52.52 after sell → book $8,277.55; vs 09:30 mark -4.47 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SFL` | 74 | $13.74 | $2.23 | $+9.61 | $6,109.07 | ▲ +9.61 after sell → book $8,275.32; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PUMP` | 96 | $10.48 | $2.30 | $+11.74 | $7,112.85 | ▲ +11.74 after sell → book $8,273.02; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `AMD` | 4 | $547.37 | $2.00 | — | $4,921.36 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+8.2; leftover $2370.95 | — |
| 2026-09-18 09:30 ET | **BUY** | `SYM` | 53 | $44.70 | $2.15 | — | $2,550.11 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.5; leftover $2370.95 | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 113 | $20.91 | $2.33 | — | $184.96 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $2370.95 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $184.96 | ▼ close $8,228.90 vs 09:30 $8,294.19 (session -37.64) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $184.96 | ▲ 09:30 equity $8,491.77 vs yday $8,228.90 (+262.87) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `BNC` | 199 | $6.42 | $2.63 | $+270.40 | $1,458.91 | ▲ +270.40 after sell → book $8,489.14; vs 09:30 mark -2.63 | dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SYM` | 53 | $42.42 | $2.18 | $-125.17 | $3,704.99 | ▼ -125.17 after sell → book $8,486.96; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 113 | $21.65 | $2.37 | $+78.92 | $6,149.08 | ▲ +78.92 after sell → book $8,484.60; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 65 | $13.47 | $2.19 | — | $5,271.02 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $878.44 | — |
| 2026-09-21 09:30 ET | **BUY** | `MXL` | 10 | $83.53 | $2.02 | — | $4,433.70 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+8.8; leftover $878.44 | — |
| 2026-09-21 09:30 ET | **BUY** | `FORM` | 7 | $123.00 | $2.01 | — | $3,570.68 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+3.0; leftover $878.44 | — |
| 2026-09-21 09:30 ET | **BUY** | `COHR` | 2 | $326.48 | $2.00 | — | $2,915.73 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+3.9; leftover $878.44 | — |
| 2026-09-21 09:30 ET | **BUY** | `ASST` | 27 | $31.64 | $2.07 | — | $2,059.38 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.9; leftover $878.44 | — |
| 2026-09-21 09:30 ET | **BUY** | `SHMD` | 234 | $3.75 | $3.02 | — | $1,178.86 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+9.2; leftover $878.44 | — |
| 2026-09-21 09:30 ET | **BUY** | `TRMD` | 23 | $37.47 | $2.06 | — | $314.99 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+9.0; leftover $878.44 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $314.99 | ▼ close $8,467.87 vs 09:30 $8,491.77 (session -1.36) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $314.99 | ▼ 09:30 equity $8,381.80 vs yday $8,467.87 (-86.07) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `AMD` | 4 | $606.57 | $2.03 | $+232.77 | $2,739.24 | ▲ +232.77 after sell → book $8,379.77; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `COHR` | 2 | $310.29 | $2.02 | $-36.39 | $3,357.80 | ▼ -36.39 after sell → book $8,377.75; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `ASST` | 27 | $29.30 | $2.09 | $-67.34 | $4,146.81 | ▼ -67.34 after sell → book $8,375.66; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,146.81 | ▲ close $8,375.66 vs 09:30 $8,381.80 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,146.81 | ▼ 09:30 equity $8,370.67 vs yday $8,375.66 (-4.99) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `BTDR` | 65 | $12.84 | $2.21 | $-45.67 | $4,979.21 | ▼ -45.67 after sell → book $8,368.47; vs 09:30 mark -2.20 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `MXL` | 10 | $86.57 | $2.04 | $+26.34 | $5,842.87 | ▲ +26.34 after sell → book $8,366.43; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `FORM` | 7 | $125.39 | $2.03 | $+12.69 | $6,718.56 | ▲ +12.69 after sell → book $8,364.39; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SHMD` | 234 | $3.61 | $3.07 | $-38.85 | $7,560.24 | ▼ -38.85 after sell → book $8,361.33; vs 09:30 mark -3.06 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `TRMD` | 23 | $34.83 | $2.08 | $-64.86 | $8,359.25 | ▼ -64.86 after sell → book $8,359.25; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `INDP` | 354 | $3.93 | $4.57 | — | $6,963.46 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1393.21 | — |
| 2026-09-23 09:30 ET | **BUY** | `NTSK` | 75 | $18.57 | $2.21 | — | $5,568.12 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.2; leftover $1393.21 | — |
| 2026-09-23 09:30 ET | **BUY** | `ZS` | 6 | $213.00 | $2.01 | — | $4,288.11 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.0; leftover $1393.21 | — |
| 2026-09-23 09:30 ET | **BUY** | `HIMS` | 45 | $30.40 | $2.12 | — | $2,917.99 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+9.3; leftover $1393.21 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLSH` | 34 | $40.00 | $2.09 | — | $1,555.90 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+6.7; leftover $1393.21 | — |
| 2026-09-23 09:30 ET | **BUY** | `OPRT` | 169 | $8.23 | $2.50 | — | $162.53 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+8.7; leftover $1393.21 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $162.53 | ▼ close $8,250.97 vs 09:30 $8,370.67 (session -92.77) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $162.53 | ▼ 09:30 equity $8,192.07 vs yday $8,250.97 (-58.90) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `INDP` | 354 | $3.77 | $4.64 | $-65.84 | $1,492.47 | ▼ -65.84 after sell → book $8,187.43; vs 09:30 mark -4.64 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ZS` | 6 | $213.47 | $2.03 | $-1.19 | $2,771.30 | ▼ -1.19 after sell → book $8,185.41; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `HIMS` | 45 | $28.00 | $2.15 | $-112.27 | $4,029.15 | ▼ -112.27 after sell → book $8,183.26; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BLSH` | 34 | $39.27 | $2.11 | $-29.02 | $5,362.22 | ▼ -29.02 after sell → book $8,181.15; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,362.22 | ▼ close $8,157.67 vs 09:30 $8,192.07 (session -23.48) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.48 | ▲ 09:30 equity $9,815.44 vs yday $9,815.44 (-0.00) | 09:30 open · cash $10.48 (unchanged overnight, no fees) · equity $9,815.44 vs prior close $9,815.44 (-0.00) · 1 name(s) re-marked at the open (per-name table). NTSK×528 yday $18.57 → 09:30 $18.57 +0.00 | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.48 | ▲ close $9,815.44 vs 09:30 $9,815.44 (session +0.00) | 16:00 close · cash $10.48 · equity $9,815.44 vs 09:30 $9,815.44 (-0.00; session marks +0.00) · 1 name(s) marked open→close (per-name table). NTSK×528 09:30 $18.57 → close $18.57 -0.00 | — |
| 2026-09-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.48 | ▼ 09:30 equity $9,068.21 vs yday $9,815.44 (-747.23) | 09:30 open · cash $10.48 (unchanged overnight, no fees) · equity $9,068.21 vs prior close $9,815.44 (-747.23) · 1 name(s) re-marked at the open (per-name table). NTSK×528 yday $18.57 → 09:30 $17.15 -747.23 | — |
| 2026-09-28 09:30 ET | **SELL** | `NTSK` | 528 | $17.15 | $6.97 | $-581.49 | $9,061.24 | ▼ -581.49 after sell → book $9,061.24; vs 09:30 mark -6.97 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,061.24 | ▲ close $9,061.24 vs 09:30 $9,068.21 (session +0.00) | 16:00 close · cash $9,061.24 · no lots left · equity $9,061.24. | — |
| 2026-09-29 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,061.24 | ▲ 09:30 equity $9,061.24 vs yday $9,061.24 (+0.00) | 09:30 open · cash $9,061.24 · no holdings · equity $9,061.24 vs prior close $9,061.24 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-29 09:30 ET | **BUY** | `ABCL` | 80 | $14.12 | $2.23 | — | $7,929.41 | — | hot list ∩ not exploded; gate ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; ret5=+10.0; leftover $1132.65 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `ADMA` | 112 | $10.04 | $2.33 | — | $6,802.60 | — | hot list ∩ not exploded; gate ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; ⚪; ret5=+6.5; leftover $1132.65 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `ALVO` | 184 | $6.13 | $2.54 | — | $5,672.14 | — | hot list ∩ not exploded; gate ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2; list yday_gainer,ohlc_hot; ret5=+4.6; leftover $1132.65 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `SUIG` | 699 | $1.62 | $9.02 | — | $4,530.74 | — | hot list ∩ not exploded; gate ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2; list ohlc_hot; ret5=+9.6; leftover $1132.65 | join🟡 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `RGEN` | 5 | $196.00 | $2.00 | — | $3,548.74 | — | hot list ∩ not exploded; gate ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+7.4; leftover $1132.65 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `VSXY` | 12 | $89.78 | $2.03 | — | $2,469.35 | — | hot list ∩ not exploded; gate ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.2; leftover $1132.65 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `A` | 6 | $174.49 | $2.01 | — | $1,420.41 | — | hot list ∩ not exploded; gate ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2; list ohlc_hot; ret5=+8.2; leftover $1132.65 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `GAP` | 49 | $23.07 | $2.14 | — | $287.84 | — | hot list ∩ not exploded; gate ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.0; leftover $1132.65 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-29 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $287.84 | ▲ close $9,057.87 vs 09:30 $9,061.24 (session +20.92) | 16:00 close · cash $287.84 · equity $9,057.87 vs 09:30 $9,061.24 (-3.37; session marks +20.92) · 8 name(s) marked open→close (per-name table). ABCL×80 09:30 $14.12 → close $14.82 +56.00; ADMA×112 09:30 $10.04 → close $9.96 -8.96; ALVO×184 09:30 $6.13 → close $5.92 -38.64; SUIG×699 09:30 $1.62 → close $1.62 +0.00; RGEN×5 09:30 $196.00 → close $194.57 -7.15; VSXY×12 09:30 $89.78 → close $89.23 -6.60; A×6 09:30 $174.49 → close $175.03 +3.24; GAP×49 09:30 $23.07 → close $23.54 +23.03 | — |
| 2026-09-30 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $287.84 | ▲ 09:30 equity $9,057.87 vs yday $9,057.87 (-0.00) | 09:30 open · cash $287.84 (unchanged overnight, no fees) · equity $9,057.87 vs prior close $9,057.87 (-0.00) · 8 name(s) re-marked at the open (per-name table). A×6 yday $175.03 → 09:30 $175.03 +0.00; ABCL×80 yday $14.82 → 09:30 $14.82 +0.00; ADMA×112 yday $9.96 → 09:30 $9.96 +0.00; ALVO×184 yday $5.92 → 09:30 $5.92 +0.00; GAP×49 yday $23.54 → 09:30 $23.54 +0.00; RGEN×5 yday $194.57 → 09:30 $194.57 +0.00; SUIG×699 yday $1.62 → 09:30 $1.62 +0.00; VSXY×12 yday $89.23 → 09:30 $89.23 +0.00 | — |
| 2026-09-30 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $287.84 | ▲ close $9,057.87 vs 09:30 $9,057.87 (session +0.00) | 16:00 close · cash $287.84 · equity $9,057.87 vs 09:30 $9,057.87 (-0.00; session marks +0.00) · 8 name(s) marked open→close (per-name table). A×6 09:30 $175.03 → close $175.03 +0.00; ABCL×80 09:30 $14.82 → close $14.82 +0.00; ADMA×112 09:30 $9.96 → close $9.96 +0.00; ALVO×184 09:30 $5.92 → close $5.92 +0.00; GAP×49 09:30 $23.54 → close $23.54 +0.00; RGEN×5 09:30 $194.57 → close $194.57 +0.00; SUIG×699 09:30 $1.62 → close $1.62 +0.00; VSXY×12 09:30 $89.23 → close $89.23 +0.00 | — |
| 2026-10-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $287.84 | ▼ 09:30 equity $8,888.08 vs yday $9,057.87 (-169.79) | 09:30 open · cash $287.84 (unchanged overnight, no fees) · equity $8,888.08 vs prior close $9,057.87 (-169.79) · 8 name(s) re-marked at the open (per-name table). A×6 yday $175.03 → 09:30 $172.00 -18.18; ABCL×80 yday $14.82 → 09:30 $14.59 -18.80; ADMA×112 yday $9.96 → 09:30 $10.07 +12.32; ALVO×184 yday $5.92 → 09:30 $5.81 -20.24; GAP×49 yday $23.54 → 09:30 $22.76 -38.22; RGEN×5 yday $194.57 → 09:30 $194.00 -2.85; SUIG×699 yday $1.62 → 09:30 $1.52 -69.90; VSXY×12 yday $89.23 → 09:30 $88.07 -13.92 | — |
| 2026-10-01 09:30 ET | **SELL** | `A` | 6 | $172.00 | $2.03 | $-18.98 | $1,317.81 | ▼ -18.98 after sell → book $8,886.05; vs 09:30 mark -2.03 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `ABCL` | 80 | $14.59 | $2.25 | $+32.72 | $2,482.36 | ▲ +32.72 after sell → book $8,883.80; vs 09:30 mark -2.25 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `ADMA` | 112 | $10.07 | $2.35 | $-1.32 | $3,607.84 | ▼ -1.32 after sell → book $8,881.44; vs 09:30 mark -2.36 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `ALVO` | 184 | $5.81 | $2.58 | $-64.00 | $4,674.30 | ▼ -64.00 after sell → book $8,878.86; vs 09:30 mark -2.58 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `GAP` | 49 | $22.76 | $2.16 | $-19.48 | $5,787.38 | ▼ -19.48 after sell → book $8,876.70; vs 09:30 mark -2.16 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `RGEN` | 5 | $194.00 | $2.02 | $-14.03 | $6,755.36 | ▼ -14.03 after sell → book $8,874.68; vs 09:30 mark -2.02 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `SUIG` | 699 | $1.52 | $9.14 | $-88.06 | $7,808.70 | ▼ -88.06 after sell → book $8,865.54; vs 09:30 mark -9.14 | exit unpriced hold on first bar after 2 sess | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **SELL** | `VSXY` | 12 | $88.07 | $2.05 | $-24.59 | $8,863.49 | ▼ -24.59 after sell → book $8,863.49; vs 09:30 mark -2.05 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **BUY** | `AVPT` | 77 | $14.27 | $2.22 | — | $7,762.48 | — | hot list ∩ not exploded; gate ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2; list flatten,ohlc_hot; 🔵; ret5=+7.3; leftover $1107.94 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-10-01 09:30 ET | **BUY** | `SECZ` | 76 | $14.55 | $2.22 | — | $6,654.46 | — | hot list ∩ not exploded; gate ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+1.6; leftover $1107.94 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `FOSL` | 169 | $6.55 | $2.50 | — | $5,545.01 | — | hot list ∩ not exploded; gate ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2; list ohlc_hot; ret5=+6.5; leftover $1107.94 | join🔴 sector🔴 gen🔴 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `ONTO` | 3 | $315.94 | $2.00 | — | $4,595.20 | — | hot list ∩ not exploded; gate ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+9.0; leftover $1107.94 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `PDFS` | 21 | $51.41 | $2.05 | — | $3,513.53 | — | hot list ∩ not exploded; gate ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+5.4; leftover $1107.94 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `ICHR` | 17 | $61.92 | $2.04 | — | $2,458.85 | — | hot list ∩ not exploded; gate ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+5.9; leftover $1107.94 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `LEGN` | 56 | $19.61 | $2.16 | — | $1,358.53 | — | hot list ∩ not exploded; gate ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+7.8; leftover $1107.94 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `PAYS` | 82 | $13.50 | $2.24 | — | $249.30 | — | hot list ∩ not exploded; gate ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+6.4; leftover $1107.94 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-10-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $249.30 | ▲ close $8,914.33 vs 09:30 $8,888.08 (session +68.26) | 16:00 close · cash $249.30 · equity $8,914.33 vs 09:30 $8,888.08 (+26.25; session marks +68.26) · 8 name(s) marked open→close (per-name table). AVPT×77 09:30 $14.27 → close $14.08 -14.63; SECZ×76 09:30 $14.55 → close $13.74 -61.56; FOSL×169 09:30 $6.55 → close $6.80 +42.25; ONTO×3 09:30 $315.94 → close $314.88 -3.18; PDFS×21 09:30 $51.41 → close $53.39 +41.58; ICHR×17 09:30 $61.92 → close $62.48 +9.52; LEGN×56 09:30 $19.61 → close $19.32 -16.24; PAYS×82 09:30 $13.50 → close $14.36 +70.52 | — |
| 2026-10-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $249.30 | ▲ 09:30 equity $9,084.73 vs yday $8,914.33 (+170.40) | 09:30 open · cash $249.30 (unchanged overnight, no fees) · equity $9,084.73 vs prior close $8,914.33 (+170.40) · 8 name(s) re-marked at the open (per-name table). AVPT×77 yday $14.08 → 09:30 $14.22 +10.78; FOSL×169 yday $6.80 → 09:30 $6.95 +25.35; ICHR×17 yday $62.48 → 09:30 $65.01 +43.01; LEGN×56 yday $19.32 → 09:30 $19.32 +0.00; ONTO×3 yday $314.88 → 09:30 $328.89 +42.03; PAYS×82 yday $14.36 → 09:30 $14.19 -13.94; PDFS×21 yday $53.39 → 09:30 $55.24 +38.85; SECZ×76 yday $13.74 → 09:30 $14.06 +24.32 | — |
| 2026-10-02 09:30 ET | **SELL** | `AVPT` | 77 | $14.22 | $2.24 | $-8.31 | $1,342.00 | ▼ -8.31 after sell → book $9,082.49; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `FOSL` | 169 | $6.95 | $2.54 | $+62.57 | $2,514.01 | ▲ +62.57 after sell → book $9,079.95; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **SELL** | `ICHR` | 17 | $65.01 | $2.06 | $+48.43 | $3,617.12 | ▲ +48.43 after sell → book $9,077.89; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `LEGN` | 56 | $19.32 | $2.18 | $-20.58 | $4,696.86 | ▼ -20.58 after sell → book $9,075.71; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `ONTO` | 3 | $328.89 | $2.02 | $+34.83 | $5,681.51 | ▲ +34.83 after sell → book $9,073.69; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-10-02 09:30 ET | **SELL** | `PAYS` | 82 | $14.19 | $2.26 | $+52.08 | $6,842.83 | ▲ +52.08 after sell → book $9,071.43; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `PDFS` | 21 | $55.24 | $2.07 | $+76.30 | $8,000.80 | ▲ +76.30 after sell → book $9,069.36; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-02 09:30 ET | **SELL** | `SECZ` | 76 | $14.06 | $2.24 | $-41.70 | $9,067.12 | ▼ -41.70 after sell → book $9,067.12; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **BUY** | `CDNA` | 17 | $66.33 | $2.04 | — | $7,937.47 | — | hot list ∩ not exploded; gate ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2; list flatten,ohlc_hot; 🔵; ⚪; ret5=+7.9; leftover $1133.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `USDE` | 74 | $15.24 | $2.21 | — | $6,807.50 | — | hot list ∩ not exploded; gate ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+9.1; leftover $1133.39 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 heat🔴 vol🔴 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `ARHS` | 111 | $10.12 | $2.32 | — | $5,681.85 | — | hot list ∩ not exploded; gate ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+4.2; leftover $1133.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `ACMR` | 13 | $86.09 | $2.03 | — | $4,560.65 | — | hot list ∩ not exploded; gate ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2; list ohlc_hot; ret5=+9.7; leftover $1133.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `CDNS` | 3 | $356.50 | $2.00 | — | $3,489.16 | — | hot list ∩ not exploded; gate ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2; list ohlc_hot; ret5=+8.9; leftover $1133.39 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `SYNA` | 9 | $121.66 | $2.02 | — | $2,392.20 | — | hot list ∩ not exploded; gate ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2; list ohlc_hot; ret5=+9.6; leftover $1133.39 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `VPG` | 14 | $77.34 | $2.03 | — | $1,307.41 | — | hot list ∩ not exploded; gate ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2; list ohlc_hot; ret5=+9.5; leftover $1133.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `GRAL` | 8 | $135.52 | $2.01 | — | $221.23 | — | hot list ∩ not exploded; gate ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+5.9; leftover $1133.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $221.23 | ▼ close $9,027.47 vs 09:30 $9,084.73 (session -22.98) | 16:00 close · cash $221.23 · equity $9,027.47 vs 09:30 $9,084.73 (-57.26; session marks -22.98) · 8 name(s) marked open→close (per-name table). CDNA×17 09:30 $66.33 → close $67.15 +13.94; USDE×74 09:30 $15.24 → close $14.15 -80.66; ARHS×111 09:30 $10.12 → close $10.24 +13.32; ACMR×13 09:30 $86.09 → close $83.38 -35.23; CDNS×3 09:30 $356.50 → close $351.35 -15.45; SYNA×9 09:30 $121.66 → close $121.10 -5.04; VPG×14 09:30 $77.34 → close $79.07 +24.22; GRAL×8 09:30 $135.52 → close $143.26 +61.92 | — |
| 2026-10-05 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $221.23 | ▲ 09:30 equity $9,031.27 vs yday $9,027.47 (+3.80) | 09:30 open · cash $221.23 (unchanged overnight, no fees) · equity $9,031.27 vs prior close $9,027.47 (+3.80) · 8 name(s) re-marked at the open (per-name table). ACMR×13 yday $83.38 → 09:30 $82.81 -7.41; ARHS×111 yday $10.24 → 09:30 $10.23 -1.11; CDNA×17 yday $67.15 → 09:30 $66.90 -4.25; CDNS×3 yday $351.35 → 09:30 $354.72 +10.11; GRAL×8 yday $143.26 → 09:30 $142.75 -4.08; SYNA×9 yday $121.10 → 09:30 $120.98 -1.08; USDE×74 yday $14.15 → 09:30 $14.36 +15.54; VPG×14 yday $79.07 → 09:30 $78.79 -3.92 | — |
| 2026-10-05 09:30 ET | **SELL** | `ACMR` | 13 | $82.81 | $2.05 | $-46.72 | $1,295.71 | ▼ -46.72 after sell → book $9,029.22; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `ARHS` | 111 | $10.23 | $2.35 | $+7.54 | $2,428.89 | ▲ +7.54 after sell → book $9,026.87; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **SELL** | `CDNA` | 17 | $66.90 | $2.06 | $+5.59 | $3,564.13 | ▲ +5.59 after sell → book $9,024.81; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `CDNS` | 3 | $354.72 | $2.02 | $-9.36 | $4,626.27 | ▼ -9.36 after sell → book $9,022.79; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `GRAL` | 8 | $142.75 | $2.03 | $+53.79 | $5,766.24 | ▲ +53.79 after sell → book $9,020.76; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **SELL** | `SYNA` | 9 | $120.98 | $2.04 | $-10.17 | $6,853.02 | ▼ -10.17 after sell → book $9,018.72; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **SELL** | `USDE` | 74 | $14.36 | $2.23 | $-69.57 | $7,913.42 | ▼ -69.57 after sell → book $9,016.48; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `VPG` | 14 | $78.79 | $2.05 | $+16.22 | $9,014.43 | ▲ +16.22 after sell → book $9,014.43; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `WNC` | 321 | $14.02 | $4.14 | — | $4,509.87 | — | hot list ∩ not exploded; gate ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+7.8; leftover $4507.22 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `ON` | 53 | $85.00 | $2.15 | — | $2.72 | — | hot list ∩ not exploded; gate ret_5_max=10.0,ret_5_min=0.0,rvol_max=2.2; list ohlc_hot; ret5=+10.0; leftover $4507.22 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.72 | ▲ close $9,047.80 vs 09:30 $9,031.27 (session +39.66) | 16:00 close · cash $2.72 · equity $9,047.80 vs 09:30 $9,031.27 (+16.53; session marks +39.66) · 2 name(s) marked open→close (per-name table). WNC×321 09:30 $14.02 → close $13.99 -9.63; ON×53 09:30 $85.00 → close $85.93 +49.29 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `PAYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBNX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BETA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `U` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `VSTM` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMTX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `PSX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-31 | `CAN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RCKT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `GWRE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TII` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `YEXT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FNKO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ARCT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AGCO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ZETA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TSLA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SIGA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `CYPH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ARCT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VIR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AGRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CSAN` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRNM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `LAND` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `LAND` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TRMD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `XRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `STX` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CDZI` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TJGC` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLMT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `QRVO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CRDL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `KGS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `PUMP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `PGNY` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DHT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `AVT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `TK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `KGS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `PUMP` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `META` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `IOVA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `BTDR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `MXL` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FORM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SHMD` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TRMD` | no_price | no 09:30 open — carry |
| 2026-09-22 | `NTSK` | no_price | no 09:30 open |
| 2026-09-22 | `ZS` | no_price | no 09:30 open |
| 2026-09-24 | `CRWD` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `OKTA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RNG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RPD` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `AVT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `DDOG` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `NTSK` | 75 | 2026-09-23 @ $18.57 | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.2; leftover $1393.21 |
| `OPRT` | 169 | 2026-09-23 @ $8.23 | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+8.7; leftover $1393.21 |
