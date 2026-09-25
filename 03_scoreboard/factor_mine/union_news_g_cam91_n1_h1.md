# Factor mine action — `union_news_g_cam91_n1_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 1 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · all leftover on the rare news🟢 +9 −≤1 name (KILL example)

Cash book **-22.11%** ($7,789) · signal-only (no cash/fees) was -2.85%. Starts YES **27/30**. Fills 6 · skips 1 · realized $+1232.62.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 1 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how many morning cameras are green vs red.
- Must-have: the news camera (does the morning packet like the headline?) is green.
- Must-have: at least 9 green cameras (the +G half of +G −R).
- Must-have: at most 1 red cameras (the −R half of +G −R; 🚨 is not counted here).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Sort the keepers by how many morning cameras are green vs red and keep the top 1.
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
- **Gate** `news=good,n_pos_min=9,cam_bad_max=1` · **rank** `cond` · **top_n** 1.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $11,232.62.

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
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 83 | $119.43 | $2.24 | — | $85.07 | — | all leftover on the rare news🟢 +9 −≤1 name (KILL example); gate news=good,n_pos_min=9,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $10000.00 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $85.07 | ▲ close $10,146.33 vs 09:30 $10,000.00 (session +148.57) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.07 | ▼ 09:30 equity $10,087.40 vs yday $10,146.33 (-58.93) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 83 | $120.51 | $2.33 | $+85.07 | $10,085.07 | ▲ +85.07 after sell → book $10,085.07; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,085.07 | ▲ close $10,085.07 vs 09:30 $10,087.40 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,085.07 | ▲ 09:30 equity $10,085.07 vs yday $10,085.07 (-0.00) | — | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,085.07 | ▲ close $10,085.07 vs 09:30 $10,085.07 (session +0.00) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,085.07 | ▲ 09:30 equity $10,085.07 vs yday $10,085.07 (-0.00) | — | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,085.07 | ▲ close $10,085.07 vs 09:30 $10,085.07 (session +0.00) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,085.07 | ▲ 09:30 equity $10,085.07 vs yday $10,085.07 (-0.00) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,085.07 | ▲ close $10,085.07 vs 09:30 $10,085.07 (session +0.00) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,085.07 | ▲ 09:30 equity $10,085.07 vs yday $10,085.07 (-0.00) | — | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,085.07 | ▲ close $10,085.07 vs 09:30 $10,085.07 (session +0.00) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,085.07 | ▲ 09:30 equity $10,085.07 vs yday $10,085.07 (-0.00) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,085.07 | ▲ close $10,085.07 vs 09:30 $10,085.07 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,085.07 | ▲ 09:30 equity $10,085.07 vs yday $10,085.07 (-0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,085.07 | ▲ close $10,085.07 vs 09:30 $10,085.07 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,085.07 | ▲ 09:30 equity $10,085.07 vs yday $10,085.07 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,085.07 | ▲ close $10,085.07 vs 09:30 $10,085.07 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,085.07 | ▲ 09:30 equity $10,085.07 vs yday $10,085.07 (-0.00) | — | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,085.07 | ▲ close $10,085.07 vs 09:30 $10,085.07 (session +0.00) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,085.07 | ▲ 09:30 equity $10,085.07 vs yday $10,085.07 (-0.00) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 38 | $263.36 | $2.10 | — | $75.28 | — | all leftover on the rare news🟢 +9 −≤1 name (KILL example); gate news=good,n_pos_min=9,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $10085.07 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $75.28 | ▼ close $9,926.02 vs 09:30 $10,085.07 (session -156.94) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $75.28 | ▼ 09:30 equity $9,716.64 vs yday $9,926.02 (-209.38) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 38 | $253.72 | $2.19 | $-370.62 | $9,714.45 | ▼ -370.62 after sell → book $9,714.45; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,714.45 | ▲ close $9,714.45 vs 09:30 $9,716.64 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,714.45 | ▲ 09:30 equity $9,714.45 vs yday $9,714.45 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,714.45 | ▲ close $9,714.45 vs 09:30 $9,714.45 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,714.45 | ▲ 09:30 equity $9,714.45 vs yday $9,714.45 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,714.45 | ▲ close $9,714.45 vs 09:30 $9,714.45 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,714.45 | ▲ 09:30 equity $9,714.45 vs yday $9,714.45 (+0.00) | — | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,714.45 | ▲ close $9,714.45 vs 09:30 $9,714.45 (session +0.00) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,714.45 | ▲ 09:30 equity $9,714.45 vs yday $9,714.45 (+0.00) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,714.45 | ▲ close $9,714.45 vs 09:30 $9,714.45 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,714.45 | ▲ 09:30 equity $9,714.45 vs yday $9,714.45 (+0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,714.45 | ▲ close $9,714.45 vs 09:30 $9,714.45 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,714.45 | ▲ 09:30 equity $9,714.45 vs yday $9,714.45 (+0.00) | — | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,714.45 | ▲ close $9,714.45 vs 09:30 $9,714.45 (session +0.00) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,714.45 | ▲ 09:30 equity $9,714.45 vs yday $9,714.45 (+0.00) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,714.45 | ▲ close $9,714.45 vs 09:30 $9,714.45 (session +0.00) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,714.45 | ▲ 09:30 equity $9,714.45 vs yday $9,714.45 (+0.00) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,714.45 | ▲ close $9,714.45 vs 09:30 $9,714.45 (session +0.00) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,714.45 | ▲ 09:30 equity $9,714.45 vs yday $9,714.45 (+0.00) | — | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 42 | $230.25 | $2.12 | — | $41.84 | — | all leftover on the rare news🟢 +9 −≤1 name (KILL example); gate news=good,n_pos_min=9,cam_bad_max=1; rank cond; list ohlc_hot; ret5=+12.5; leftover $9714.45 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $41.84 | ▼ close $9,445.64 vs 09:30 $9,714.45 (session -266.70) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $41.84 | ▲ 09:30 equity $9,445.64 vs yday $9,445.64 (-0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $41.84 | ▲ close $9,445.64 vs 09:30 $9,445.64 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $41.84 | ▲ 09:30 equity $11,234.84 vs yday $9,445.64 (+1,789.20) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `VICR` | 42 | $266.50 | $2.22 | $+1518.17 | $11,232.62 | ▲ +1,518.17 after sell → book $11,232.62; vs 09:30 mark -2.22 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,232.62 | ▲ close $11,232.62 vs 09:30 $11,234.84 (session +0.00) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,232.62 | ▲ 09:30 equity $11,232.62 vs yday $11,232.62 (+0.00) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,232.62 | ▲ close $11,232.62 vs 09:30 $11,232.62 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,788.92 | ▲ 09:30 equity $7,788.92 vs yday $7,788.92 (+0.00) | 09:30 open · cash $7,788.92 · no holdings · equity $7,788.92 vs prior close $7,788.92 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,788.92 | ▲ close $7,788.92 vs 09:30 $7,788.92 (session +0.00) | 16:00 close · cash $7,788.92 · no lots left · equity $7,788.92. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-09-22 | `VICR` | no_price | no 09:30 open — carry |
