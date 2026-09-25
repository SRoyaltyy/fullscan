# Factor mine action — `overnight_mega_green_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `overnight_mega` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · mega calendar ∩ last bar green; still a print-night long, not a reaction

Cash book **+0.00%** ($10,000) · signal-only (no cash/fees) was -9.06%. Starts YES **0/30**. Fills 20 · skips 7 · realized $-886.90.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the same calendar list, kept only when prior-export mcap is at least $50B and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the same calendar list, kept only when prior-export mcap is at least $50B.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the last finished bar was green (closed up).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the same calendar list, kept only when prior-export mcap is at least $50B that pass the must-haves.
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

- **Universe** `overnight_mega` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `last_green=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,113.10.

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
| 2026-08-20 09:30 ET | **BUY** | `ROST` | 43 | $229.55 | $2.12 | — | $127.23 | — | mega calendar ∩ last bar green; still a print-night long, not a reaction; gate last_green=True; list overnight,overnight_mega; 🔵; ret5=-5.5; leftover $10000.00 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $127.23 | ▼ close $9,973.80 vs 09:30 $10,000.00 (session -24.08) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $127.23 | ▲ 09:30 equity $10,612.78 vs yday $9,973.80 (+638.98) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `ROST` | 43 | $243.85 | $2.21 | $+610.57 | $10,610.57 | ▲ +610.57 after sell → book $10,610.57; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `PDD` | 117 | $90.03 | $2.34 | — | $74.72 | — | mega calendar ∩ last bar green; still a print-night long, not a reaction; gate last_green=True; list overnight,overnight_mega; 🔵; ret5=+6.4; leftover $10610.57 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $74.72 | ▼ close $10,415.18 vs 09:30 $10,612.78 (session -193.05) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $74.72 | ▲ 09:30 equity $10,715.87 vs yday $10,415.18 (+300.69) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `PDD` | 117 | $90.95 | $2.45 | $+102.85 | $10,713.42 | ▲ +102.85 after sell → book $10,713.42; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,713.42 | ▲ close $10,713.42 vs 09:30 $10,715.87 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,713.42 | ▲ 09:30 equity $10,713.42 vs yday $10,713.42 (+0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `INTU` | 29 | $364.35 | $2.08 | — | $145.19 | — | mega calendar ∩ last bar green; still a print-night long, not a reaction; gate last_green=True; list overnight,overnight_mega; 🔵; ret5=+10.2; leftover $10713.42 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $145.19 | ▼ close $10,511.53 vs 09:30 $10,713.42 (session -199.81) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $145.19 | ▼ 09:30 equity $9,525.82 vs yday $10,511.53 (-985.71) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `INTU` | 29 | $323.47 | $2.16 | $-1189.76 | $9,523.66 | ▼ -1,189.76 after sell → book $9,523.66; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 16 | $118.50 | $2.04 | — | $7,625.62 | — | mega calendar ∩ last bar green; still a print-night long, not a reaction; gate last_green=True; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $1904.73 | — |
| 2026-08-26 09:30 ET | **BUY** | `NVDA` | 8 | $212.64 | $2.01 | — | $5,922.49 | — | mega calendar ∩ last bar green; still a print-night long, not a reaction; gate last_green=True; list overnight,overnight_mega; 🔵; ret5=-3.0; leftover $1904.73 | — |
| 2026-08-26 09:30 ET | **BUY** | `RY` | 9 | $206.95 | $2.02 | — | $4,057.92 | — | mega calendar ∩ last bar green; still a print-night long, not a reaction; gate last_green=True; list overnight,overnight_mega; 🔵; ret5=-2.8; leftover $1904.73 | — |
| 2026-08-26 09:30 ET | **BUY** | `SNPS` | 4 | $405.10 | $2.00 | — | $2,435.52 | — | mega calendar ∩ last bar green; still a print-night long, not a reaction; gate last_green=True; list overnight,overnight_mega; 🔵; ret5=+1.1; leftover $1904.73 | — |
| 2026-08-26 09:30 ET | **BUY** | `TD` | 15 | $119.11 | $2.04 | — | $646.84 | — | mega calendar ∩ last bar green; still a print-night long, not a reaction; gate last_green=True; list overnight,overnight_mega; 🔵; ret5=-2.4; leftover $1904.73 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $646.84 | ▼ close $9,511.66 vs 09:30 $9,525.82 (session -1.90) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $646.84 | ▲ 09:30 equity $9,672.61 vs yday $9,511.66 (+160.95) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `CM` | 16 | $118.77 | $2.06 | $+0.22 | $2,545.09 | ▲ +0.22 after sell → book $9,670.54; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NVDA` | 8 | $222.86 | $2.04 | $+77.71 | $4,325.93 | ▲ +77.71 after sell → book $9,668.50; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RY` | 9 | $206.82 | $2.04 | $-5.23 | $6,185.27 | ▼ -5.23 after sell → book $9,666.46; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SNPS` | 4 | $419.66 | $2.03 | $+54.21 | $7,861.89 | ▲ +54.21 after sell → book $9,664.44; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TD` | 15 | $120.17 | $2.06 | $+11.81 | $9,662.38 | ▲ +11.81 after sell → book $9,662.38; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ADSK` | 18 | $261.47 | $2.04 | — | $4,953.87 | — | mega calendar ∩ last bar green; still a print-night long, not a reaction; gate last_green=True; list overnight,overnight_mega; 🔵; ret5=+1.4; leftover $4831.19 | — |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 19 | $253.44 | $2.05 | — | $136.47 | — | mega calendar ∩ last bar green; still a print-night long, not a reaction; gate last_green=True; list overnight,overnight_mega,mover_buy; 🔵; ret5=+3.3; leftover $4831.19 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $136.47 | ▼ close $9,594.46 vs 09:30 $9,672.61 (session -63.83) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $136.47 | ▼ 09:30 equity $9,117.29 vs yday $9,594.46 (-477.17) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `ADSK` | 18 | $261.16 | $2.09 | $-9.72 | $4,835.26 | ▼ -9.72 after sell → book $9,115.20; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MRVL` | 19 | $225.26 | $2.09 | $-539.56 | $9,113.10 | ▼ -539.56 after sell → book $9,113.10; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,113.10 | ▲ close $9,113.10 vs 09:30 $9,117.29 (session +0.00) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,113.10 | ▲ 09:30 equity $9,113.10 vs yday $9,113.10 (+0.00) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,113.10 | ▲ close $9,113.10 vs 09:30 $9,113.10 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,113.10 | ▲ 09:30 equity $9,113.10 vs yday $9,113.10 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,113.10 | ▲ close $9,113.10 vs 09:30 $9,113.10 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,113.10 | ▲ 09:30 equity $9,113.10 vs yday $9,113.10 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,113.10 | ▲ close $9,113.10 vs 09:30 $9,113.10 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,113.10 | ▲ 09:30 equity $9,113.10 vs yday $9,113.10 (+0.00) | — | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,113.10 | ▲ close $9,113.10 vs 09:30 $9,113.10 (session +0.00) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,113.10 | ▲ 09:30 equity $9,113.10 vs yday $9,113.10 (+0.00) | — | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,113.10 | ▲ close $9,113.10 vs 09:30 $9,113.10 (session +0.00) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,113.10 | ▲ 09:30 equity $9,113.10 vs yday $9,113.10 (+0.00) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,113.10 | ▲ close $9,113.10 vs 09:30 $9,113.10 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,113.10 | ▲ 09:30 equity $9,113.10 vs yday $9,113.10 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,113.10 | ▲ close $9,113.10 vs 09:30 $9,113.10 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,113.10 | ▲ 09:30 equity $9,113.10 vs yday $9,113.10 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,113.10 | ▲ close $9,113.10 vs 09:30 $9,113.10 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,113.10 | ▲ 09:30 equity $9,113.10 vs yday $9,113.10 (+0.00) | — | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,113.10 | ▲ close $9,113.10 vs 09:30 $9,113.10 (session +0.00) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,113.10 | ▲ 09:30 equity $9,113.10 vs yday $9,113.10 (+0.00) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,113.10 | ▲ close $9,113.10 vs 09:30 $9,113.10 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,113.10 | ▲ 09:30 equity $9,113.10 vs yday $9,113.10 (+0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,113.10 | ▲ close $9,113.10 vs 09:30 $9,113.10 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,113.10 | ▲ 09:30 equity $9,113.10 vs yday $9,113.10 (+0.00) | — | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,113.10 | ▲ close $9,113.10 vs 09:30 $9,113.10 (session +0.00) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,113.10 | ▲ 09:30 equity $9,113.10 vs yday $9,113.10 (+0.00) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,113.10 | ▲ close $9,113.10 vs 09:30 $9,113.10 (session +0.00) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,113.10 | ▲ 09:30 equity $9,113.10 vs yday $9,113.10 (+0.00) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,113.10 | ▲ close $9,113.10 vs 09:30 $9,113.10 (session +0.00) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,113.10 | ▲ 09:30 equity $9,113.10 vs yday $9,113.10 (+0.00) | — | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,113.10 | ▲ close $9,113.10 vs 09:30 $9,113.10 (session +0.00) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,113.10 | ▲ 09:30 equity $9,113.10 vs yday $9,113.10 (+0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,113.10 | ▲ close $9,113.10 vs 09:30 $9,113.10 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,113.10 | ▲ 09:30 equity $9,113.10 vs yday $9,113.10 (+0.00) | — | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,113.10 | ▲ close $9,113.10 vs 09:30 $9,113.10 (session +0.00) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,113.10 | ▲ 09:30 equity $9,113.10 vs yday $9,113.10 (+0.00) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,113.10 | ▲ close $9,113.10 vs 09:30 $9,113.10 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `KEYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `LOW` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `BABA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NTES` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `BNS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `MDT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `PANW` | hard_red | hard-red S=-6.30 sit; no new buys |
