# Factor mine action — `union_news_g_conv_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `cond` · size `conviction` · sell `list` · S-boost `none` · merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5

Cash book **-16.38%** ($8,362) · signal-only (no cash/fees) was +13.54%. Starts YES **26/30**. Fills 113 · skips 51 · realized $+1398.96.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 4 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how many morning cameras are green vs red.
- Must-have: the news camera (does the morning packet like the headline?) is green.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Sort the keepers by how many morning cameras are green vs red and keep the top 4.
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
- **Gate** `news=good` · **rank** `cond` · **top_n** 4.
- **Size** `conviction` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $11,399.01.

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
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 531 | $13.18 | $6.85 | — | $2,994.57 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $7000.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 232 | $4.31 | $2.99 | — | $1,991.66 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1000.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 51 | $19.57 | $2.14 | — | $991.44 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1000.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $991.44 | ▲ close $10,395.38 vs 09:30 $10,000.00 (session +407.37) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $991.44 | ▲ 09:30 equity $10,405.75 vs yday $10,395.38 (+10.37) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 531 | $13.84 | $7.00 | $+336.61 | $8,333.49 | ▲ +336.61 after sell → book $10,398.76; vs 09:30 mark -6.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 232 | $4.60 | $3.04 | $+61.25 | $9,397.65 | ▲ +61.25 after sell → book $10,395.72; vs 09:30 mark -3.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 51 | $19.57 | $2.16 | $-4.31 | $10,393.55 | ▼ -4.31 after sell → book $10,393.55; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 90 | $46.18 | $2.26 | — | $6,235.09 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten; 🔵; ret5=+6.7; leftover $4157.42 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 21 | $142.77 | $2.05 | — | $3,234.87 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten; 🔵; ret5=+5.8; leftover $3118.07 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 10 | $202.70 | $2.02 | — | $1,205.85 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten; 🔵; ret5=+8.3; leftover $2078.71 | — |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 21 | $49.00 | $2.05 | — | $174.80 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer; ⚪; ret5=+12.2; leftover $1039.36 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $174.80 | ▲ close $10,598.88 vs 09:30 $10,405.75 (session +213.71) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $174.80 | ▲ 09:30 equity $10,639.83 vs yday $10,598.88 (+40.95) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 90 | $48.00 | $2.31 | $+159.23 | $4,492.49 | ▲ +159.23 after sell → book $10,637.52; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 21 | $148.04 | $2.09 | $+106.53 | $7,599.24 | ▲ +106.53 after sell → book $10,635.43; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 10 | $208.93 | $2.05 | $+58.23 | $9,686.49 | ▲ +58.23 after sell → book $10,633.38; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OUST` | 21 | $45.09 | $2.07 | $-86.24 | $10,631.31 | ▼ -86.24 after sell → book $10,631.31; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,631.31 | ▲ close $10,631.31 vs 09:30 $10,639.83 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,631.31 | ▲ 09:30 equity $10,631.31 vs yday $10,631.31 (+0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,631.31 | ▲ close $10,631.31 vs 09:30 $10,631.31 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,631.31 | ▲ 09:30 equity $10,631.31 vs yday $10,631.31 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 81 | $91.01 | $2.23 | — | $3,257.27 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $7441.92 | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 23 | $44.76 | $2.06 | — | $2,225.73 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $1063.13 | — |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 430 | $2.47 | $5.55 | — | $1,158.08 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1063.13 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 18 | $58.73 | $2.04 | — | $98.90 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1063.13 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $98.90 | ▲ close $10,807.86 vs 09:30 $10,631.31 (session +188.43) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $98.90 | ▲ 09:30 equity $11,013.24 vs yday $10,807.86 (+205.38) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 81 | $95.72 | $2.31 | $+376.97 | $7,849.91 | ▲ +376.97 after sell → book $11,010.93; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 23 | $44.52 | $2.08 | $-9.66 | $8,871.79 | ▼ -9.66 after sell → book $11,008.85; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 51 | $119.43 | $2.14 | — | $2,778.72 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $6210.25 | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 23 | $115.18 | $2.06 | — | $127.52 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $2661.54 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $127.52 | ▲ close $11,260.76 vs 09:30 $11,013.24 (session +256.11) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $127.52 | ▼ 09:30 equity $11,146.03 vs yday $11,260.76 (-114.73) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 430 | $2.40 | $5.63 | $-41.28 | $1,153.89 | ▼ -41.28 after sell → book $11,140.40; vs 09:30 mark -5.63 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 51 | $120.51 | $2.20 | $+50.73 | $7,297.70 | ▲ +50.73 after sell → book $11,138.20; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 23 | $121.00 | $2.09 | $+129.71 | $10,078.61 | ▲ +129.71 after sell → book $11,136.11; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,078.61 | ▼ close $11,105.96 vs 09:30 $11,146.03 (session -30.15) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,078.61 | ▲ 09:30 equity $11,121.35 vs yday $11,105.96 (+15.39) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 18 | $57.93 | $2.06 | $-18.51 | $11,119.28 | ▼ -18.51 after sell → book $11,119.28; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 65 | $118.52 | $2.19 | — | $3,413.30 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $7783.50 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 14 | $77.13 | $2.03 | — | $2,331.44 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $1111.93 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 31 | $35.05 | $2.08 | — | $1,242.81 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1111.93 | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 118 | $9.42 | $2.34 | — | $128.91 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1111.93 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $128.91 | ▲ close $11,567.27 vs 09:30 $11,121.35 (session +456.63) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $128.91 | ▼ 09:30 equity $11,321.63 vs yday $11,567.27 (-245.64) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 65 | $119.80 | $2.26 | $+78.76 | $7,913.65 | ▲ +78.76 after sell → book $11,319.37; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 14 | $79.34 | $2.05 | $+26.86 | $9,022.36 | ▲ +26.86 after sell → book $11,317.32; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 31 | $35.70 | $2.10 | $+15.96 | $10,126.95 | ▲ +15.96 after sell → book $11,315.21; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 118 | $10.07 | $2.37 | $+71.98 | $11,312.84 | ▲ +71.98 after sell → book $11,312.84; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 29 | $267.02 | $2.08 | — | $3,567.18 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $7918.99 | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 9 | $118.50 | $2.02 | — | $2,498.67 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $1131.28 | — |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 100 | $11.22 | $2.29 | — | $1,374.38 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+16.8; leftover $1131.28 | — |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 136 | $8.29 | $2.40 | — | $244.54 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; leftover $1131.28 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $244.54 | ▲ close $11,478.03 vs 09:30 $11,321.63 (session +173.97) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $244.54 | ▼ 09:30 equity $11,450.98 vs yday $11,478.03 (-27.05) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 29 | $267.23 | $2.15 | $+1.86 | $7,992.06 | ▲ +1.86 after sell → book $11,448.83; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TRLV` | 100 | $11.38 | $2.32 | $+11.39 | $9,127.74 | ▲ +11.39 after sell → book $11,446.51; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 136 | $9.19 | $2.43 | $+117.57 | $10,375.15 | ▲ +117.57 after sell → book $11,444.08; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 88 | $81.65 | $2.25 | — | $3,187.70 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $7262.61 | — |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $2,218.70 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $1556.27 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,218.70 | ▼ close $11,270.77 vs 09:30 $11,450.98 (session -169.07) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,218.70 | ▼ 09:30 equity $11,154.69 vs yday $11,270.77 (-116.08) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 9 | $115.66 | $2.04 | $-29.61 | $3,257.60 | ▼ -29.61 after sell → book $11,152.65; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 88 | $79.27 | $2.32 | $-214.02 | $10,231.03 | ▼ -214.02 after sell → book $11,150.32; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $11,148.31 | ▼ -51.73 after sell → book $11,148.31; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 24 | $324.41 | $2.06 | — | $3,360.41 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $7803.82 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 7 | $141.76 | $2.01 | — | $2,366.08 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1114.83 | — |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 2 | $400.42 | $2.00 | — | $1,563.24 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1114.83 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,563.24 | ▼ close $10,917.59 vs 09:30 $11,154.69 (session -224.65) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,563.24 | ▲ 09:30 equity $10,985.98 vs yday $10,917.59 (+68.39) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 24 | $322.49 | $2.13 | $-50.28 | $9,300.87 | ▼ -50.28 after sell → book $10,983.85; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 7 | $132.30 | $2.03 | $-70.26 | $10,224.94 | ▼ -70.26 after sell → book $10,981.82; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 2 | $378.44 | $2.02 | $-47.97 | $10,979.80 | ▼ -47.97 after sell → book $10,979.80; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,979.80 | ▲ close $10,979.80 vs 09:30 $10,985.98 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,979.80 | ▲ 09:30 equity $10,979.80 vs yday $10,979.80 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,979.80 | ▲ close $10,979.80 vs 09:30 $10,979.80 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,979.80 | ▲ 09:30 equity $10,979.80 vs yday $10,979.80 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,979.80 | ▲ close $10,979.80 vs 09:30 $10,979.80 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,979.80 | ▲ 09:30 equity $10,979.80 vs yday $10,979.80 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 21 | $351.74 | $2.05 | — | $3,591.21 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $7685.86 | — |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $2,616.59 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1097.98 | — |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 33 | $32.31 | $2.09 | — | $1,548.27 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1097.98 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 69 | $15.87 | $2.20 | — | $451.05 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1097.98 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $451.05 | ▲ close $11,261.07 vs 09:30 $10,979.80 (session +289.60) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $451.05 | ▲ 09:30 equity $11,268.09 vs yday $11,261.07 (+7.02) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 21 | $359.70 | $2.12 | $+162.98 | $8,002.62 | ▲ +162.98 after sell → book $11,265.96; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 2 | $513.78 | $2.02 | $+50.93 | $9,028.17 | ▲ +50.93 after sell → book $11,263.95; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 33 | $33.46 | $2.11 | $+33.75 | $10,130.24 | ▲ +33.75 after sell → book $11,261.84; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 26 | $263.36 | $2.07 | — | $3,280.81 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $7091.17 | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 20 | $75.65 | $2.05 | — | $1,765.76 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $1519.54 | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 6 | $236.82 | $2.01 | — | $342.83 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; ret5=+8.1; leftover $1519.54 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $342.83 | ▲ close $11,290.82 vs 09:30 $11,268.09 (session +35.11) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $342.83 | ▼ 09:30 equity $11,277.97 vs yday $11,290.82 (-12.85) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 69 | $16.74 | $2.22 | $+55.61 | $1,495.67 | ▲ +55.61 after sell → book $11,275.75; vs 09:30 mark -2.22 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 26 | $253.72 | $2.13 | $-254.84 | $8,090.26 | ▼ -254.84 after sell → book $11,273.62; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 6 | $267.76 | $2.03 | $+181.60 | $9,694.79 | ▲ +181.60 after sell → book $11,271.59; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,694.79 | ▼ close $11,228.99 vs 09:30 $11,277.97 (session -42.60) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,694.79 | ▼ 09:30 equity $11,226.79 vs yday $11,228.99 (-2.20) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 20 | $76.60 | $2.07 | $+14.88 | $11,224.72 | ▲ +14.88 after sell → book $11,224.72; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,224.72 | ▲ close $11,224.72 vs 09:30 $11,226.79 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,224.72 | ▲ 09:30 equity $11,224.72 vs yday $11,224.72 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,224.72 | ▲ close $11,224.72 vs 09:30 $11,224.72 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,224.72 | ▲ 09:30 equity $11,224.72 vs yday $11,224.72 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 47 | $164.43 | $2.13 | — | $3,494.38 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $7857.30 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 4 | $242.17 | $2.00 | — | $2,523.70 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list earn_react; ret5=-11.1; leftover $1122.47 | — |
| 2026-09-11 09:30 ET | **BUY** | `AVTR` | 74 | $15.01 | $2.21 | — | $1,410.74 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $1122.47 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 529 | $2.12 | $6.82 | — | $282.44 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1122.47 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $282.44 | ▼ close $10,550.78 vs 09:30 $11,224.72 (session -660.77) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $282.44 | ▼ 09:30 equity $10,160.05 vs yday $10,550.78 (-390.73) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 47 | $141.42 | $2.19 | $-1085.80 | $6,926.99 | ▼ -1,085.80 after sell → book $10,157.86; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 4 | $261.51 | $2.02 | $+73.34 | $7,971.00 | ▲ +73.34 after sell → book $10,155.83; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 529 | $2.05 | $6.92 | $-50.78 | $9,048.53 | ▼ -50.78 after sell → book $10,148.91; vs 09:30 mark -6.92 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,048.53 | ▲ close $10,170.37 vs 09:30 $10,160.05 (session +21.46) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,048.53 | ▲ 09:30 equity $10,174.81 vs yday $10,170.37 (+4.44) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,048.53 | ▲ close $10,191.09 vs 09:30 $10,174.81 (session +16.28) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,048.53 | ▲ 09:30 equity $10,197.75 vs yday $10,191.09 (+6.66) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 241 | $26.27 | $3.11 | — | $2,714.35 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $6333.97 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 7 | $189.17 | $2.01 | — | $1,388.15 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $1357.28 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 33 | $39.99 | $2.09 | — | $66.39 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $1357.28 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $66.39 | ▼ close $10,182.88 vs 09:30 $10,197.75 (session -7.66) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $66.39 | ▲ 09:30 equity $10,197.50 vs yday $10,182.88 (+14.62) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 241 | $26.51 | $3.20 | $+51.53 | $6,452.10 | ▲ +51.53 after sell → book $10,194.30; vs 09:30 mark -3.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 7 | $190.35 | $2.03 | $+4.22 | $7,782.52 | ▲ +4.22 after sell → book $10,192.27; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 33 | $37.57 | $2.11 | $-84.06 | $9,020.22 | ▼ -84.06 after sell → book $10,190.16; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 36 | $170.85 | $2.10 | — | $2,867.52 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $6314.16 | — |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 61 | $22.12 | $2.17 | — | $1,516.03 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $1353.03 | — |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 5 | $238.60 | $2.00 | — | $321.03 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_mover; ret5=-11.6; leftover $1353.03 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $321.03 | ▲ close $10,482.48 vs 09:30 $10,197.50 (session +298.59) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $321.03 | ▲ 09:30 equity $10,640.19 vs yday $10,482.48 (+157.71) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 74 | $15.87 | $2.23 | $+59.19 | $1,493.17 | ▲ +59.19 after sell → book $10,637.95; vs 09:30 mark -2.24 | dropped from list after 5 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 36 | $182.33 | $2.16 | $+409.02 | $8,054.89 | ▲ +409.02 after sell → book $10,635.79; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 5 | $236.80 | $2.02 | $-13.03 | $9,236.87 | ▼ -13.03 after sell → book $10,633.77; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 309 | $20.91 | $3.99 | — | $2,771.69 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $6465.81 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 93 | $14.79 | $2.27 | — | $1,393.95 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $1385.53 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 98 | $14.07 | $2.28 | — | $12.81 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1385.53 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.81 | ▲ close $10,625.75 vs 09:30 $10,640.19 (session +0.52) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.81 | ▲ 09:30 equity $10,810.38 vs yday $10,625.75 (+184.63) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 61 | $22.78 | $2.19 | $+35.89 | $1,400.19 | ▲ +35.89 after sell → book $10,808.18; vs 09:30 mark -2.20 | dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 309 | $21.65 | $4.09 | $+220.58 | $8,085.95 | ▲ +220.58 after sell → book $10,804.09; vs 09:30 mark -4.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 93 | $14.58 | $2.30 | $-24.09 | $9,439.60 | ▼ -24.09 after sell → book $10,801.80; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 98 | $13.90 | $2.31 | $-21.26 | $10,799.49 | ▼ -21.26 after sell → book $10,799.49; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 32 | $230.25 | $2.09 | — | $3,429.40 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; ret5=+12.5; leftover $7559.64 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 5 | $190.30 | $2.00 | — | $2,475.90 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; ret5=+10.6; leftover $1079.95 | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 41 | $25.95 | $2.11 | — | $1,409.83 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1079.95 | — |
| 2026-09-21 09:30 ET | **BUY** | `MARA` | 77 | $13.94 | $2.22 | — | $334.23 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+10.5; leftover $1079.95 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $334.23 | ▼ close $10,477.31 vs 09:30 $10,810.38 (session -313.75) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $334.23 | ▼ 09:30 equity $10,465.76 vs yday $10,477.31 (-11.55) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `MARA` | 77 | $13.13 | $2.24 | $-66.83 | $1,343.00 | ▼ -66.83 after sell → book $10,463.52; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `MRNA` | 1 | $168.50 | $1.69 | — | $1,172.81 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; ret5=+17.9; leftover $268.60 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,172.81 | ▲ close $10,475.89 vs 09:30 $10,465.76 (session +14.06) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,172.81 | ▲ 09:30 equity $11,846.49 vs yday $10,475.89 (+1,370.60) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `VICR` | 32 | $266.50 | $2.16 | $+1155.75 | $9,698.65 | ▲ +1,155.75 after sell → book $11,844.33; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 5 | $174.50 | $2.02 | $-83.03 | $10,569.12 | ▼ -83.03 after sell → book $11,842.31; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `GLXY` | 41 | $26.58 | $2.13 | $+21.58 | $11,656.77 | ▲ +21.58 after sell → book $11,840.17; vs 09:30 mark -2.14 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `MRNA` | 1 | $183.41 | $1.86 | $+11.36 | $11,838.32 | ▲ +11.36 after sell → book $11,838.32; vs 09:30 mark -1.85 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 42 | $196.78 | $2.12 | — | $3,571.44 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $8286.82 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 148 | $7.95 | $2.43 | — | $2,392.41 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $1183.83 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 75 | $15.72 | $2.21 | — | $1,211.19 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $1183.83 | — |
| 2026-09-23 09:30 ET | **BUY** | `VERI` | 910 | $1.30 | $11.74 | — | $16.45 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ret5=+15.3; leftover $1183.83 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.45 | ▼ close $11,446.21 vs 09:30 $11,846.49 (session -373.60) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.45 | ▼ 09:30 equity $11,417.81 vs yday $11,446.21 (-28.40) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 42 | $192.26 | $2.19 | $-194.15 | $8,089.18 | ▼ -194.15 after sell → book $11,415.62; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 148 | $7.38 | $2.47 | $-89.26 | $9,178.95 | ▼ -89.26 after sell → book $11,413.15; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 75 | $14.38 | $2.24 | $-104.95 | $10,255.21 | ▼ -104.95 after sell → book $11,410.91; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VERI` | 910 | $1.27 | $11.90 | $-50.94 | $11,399.01 | ▼ -50.94 after sell → book $11,399.01; vs 09:30 mark -11.90 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,399.01 | ▲ close $11,399.01 vs 09:30 $11,417.81 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,529.29 | ▲ 09:30 equity $8,529.29 vs yday $8,529.29 (+0.00) | 09:30 open · cash $8,529.29 · no holdings · equity $8,529.29 vs prior close $8,529.29 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 1546 | $3.86 | $19.94 | — | $2,541.79 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $5970.50 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 3 | $272.16 | $2.00 | — | $1,723.31 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $852.93 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 52 | $16.21 | $2.15 | — | $878.24 | — | merged news🟢 rank cameras; 70% leftover if #1 net ≥ 5; gate news=good; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $852.93 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $878.24 | ▼ close $8,362.04 vs 09:30 $8,529.29 (session -143.16) | 16:00 close · cash $878.24 · equity $8,362.04 vs 09:30 $8,529.29 (-167.25; session marks -143.16) · 3 name(s) marked open→close (per-name table). ZSQR×1546 09:30 $3.86 → close $3.78 -123.68; ILMN×3 09:30 $272.16 → close $270.00 -6.48; SECZ×52 09:30 $16.21 → close $15.96 -13.00 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1000.00 < 1 share @ 1646.93 |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ANGX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `ASML` | cash | leftover split 1556.27 < 1 share @ 1746.53 |
| 2026-08-28 | `MPWR` | cash | leftover split 1114.83 < 1 share @ 1306.03 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DVN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EOLS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `RLMD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MSTR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DFDV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `VICR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SMTC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `GLXY` | no_price | no 09:30 open — carry |
| 2026-09-22 | `WBD` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `YSS` | no_price | no 09:30 open |
| 2026-09-24 | `OKTA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ADCT` | hard_red | hard-red S=-7.66 sit; no new buys |
