# Factor mine action — `short_news_r_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · news🔴

Cash book **-3.28%** ($9,672) · signal-only (no cash/fees) was +4.61%. Starts YES **16/30**. Fills 112 · skips 28 · realized $+7.41.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only short names that pass every must-have on the checklist. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will fall.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the news camera (does the morning packet like the headline?) is red.

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- Keep the first 8 names in list order.
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- This is a SHORT sleeve: it borrows the name and profits if the price falls. Equity treats the short as a liability (must keep enough to cover).

### When it sells

- Sell first, then buy. Never sell a ticker we do not hold.
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news=bad` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $14,993.75.

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
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 1412 | $1.18 | $18.51 | — | $11,647.65 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-0.9; leftover $1666.67 | — |
| 2026-08-14 09:30 ET | **SHORT** | `LUNR` | 86 | $19.17 | $2.32 | — | $13,293.95 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1666.67 | — |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 131 | $12.70 | $2.46 | — | $14,954.53 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.6; leftover $1666.67 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,954.53 | ▲ close $10,010.33 vs 09:30 $10,000.00 (session +33.62) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,954.53 | ▼ 09:30 equity $9,916.79 vs yday $10,010.33 (-93.54) | — | — |
| 2026-08-17 09:30 ET | **COVER** | `EU` | 1412 | $1.21 | $18.21 | $-79.08 | $13,227.80 | ▼ -79.08 after sell → book $9,898.58; vs 09:30 mark -18.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `LUNR` | 86 | $20.25 | $2.25 | $-97.45 | $11,484.05 | ▼ -97.45 after sell → book $9,896.33; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `OWL` | 131 | $12.12 | $2.38 | $+70.48 | $9,893.95 | ▲ +70.48 after sell → book $9,893.95; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 860 | $1.15 | $11.27 | — | $10,871.67 | — | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; leftover $989.39 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 277 | $3.56 | $3.66 | — | $11,854.14 | — | news🔴; gate news=bad; list yday_mover; ret5=-15.6; leftover $989.39 | — |
| 2026-08-17 09:30 ET | **SHORT** | `APMD` | 31 | $31.70 | $2.13 | — | $12,834.71 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+17.6; leftover $989.39 | — |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 328 | $3.01 | $4.32 | — | $13,817.66 | — | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; leftover $989.39 | — |
| 2026-08-17 09:30 ET | **SHORT** | `RNW` | 145 | $6.80 | $2.49 | — | $14,801.18 | — | news🔴; gate news=bad; list overnight; ⚪; ret5=+10.4; leftover $989.39 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,801.18 | ▼ close $9,836.88 vs 09:30 $9,916.79 (session -33.20) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,801.18 | ▲ 09:30 equity $9,879.85 vs yday $9,836.88 (+42.97) | — | — |
| 2026-08-18 09:30 ET | **COVER** | `VERI` | 860 | $1.05 | $11.09 | $+63.63 | $13,887.08 | ▲ +63.63 after sell → book $9,868.75; vs 09:30 mark -11.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `ZNTL` | 277 | $3.75 | $3.57 | $-59.86 | $12,844.76 | ▼ -59.86 after sell → book $9,865.18; vs 09:30 mark -3.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `APMD` | 31 | $32.85 | $2.08 | $-39.86 | $11,824.33 | ▼ -39.86 after sell → book $9,863.10; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `HIVE` | 328 | $2.96 | $4.23 | $+7.85 | $10,849.22 | ▲ +7.85 after sell → book $9,858.87; vs 09:30 mark -4.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,849.22 | ▲ close $9,860.32 vs 09:30 $9,879.85 (session +1.45) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,849.22 | ▼ 09:30 equity $9,857.42 vs yday $9,860.32 (-2.90) | — | — |
| 2026-08-19 09:30 ET | **COVER** | `RNW` | 145 | $6.84 | $2.42 | $-10.71 | $9,854.99 | ▼ -10.71 after sell → book $9,854.99; vs 09:30 mark -2.43 | dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,854.99 | ▲ close $9,854.99 vs 09:30 $9,857.42 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,854.99 | ▲ 09:30 equity $9,854.99 vs yday $9,854.99 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 3 | $204.45 | $2.04 | — | $10,466.31 | — | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $615.94 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 28 | $21.40 | $2.11 | — | $11,063.40 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-25.2; leftover $615.94 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 139 | $4.43 | $2.46 | — | $11,676.71 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-23.1; leftover $615.94 | — |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 52 | $11.81 | $2.18 | — | $12,288.91 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $615.94 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 3 | $173.90 | $2.03 | — | $12,808.57 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; leftover $615.94 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 13 | $46.85 | $2.07 | — | $13,415.56 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=+5.0; leftover $615.94 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 5 | $106.38 | $2.04 | — | $13,945.42 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; leftover $615.94 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 133 | $4.61 | $2.44 | — | $14,556.11 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; leftover $615.94 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,556.11 | ▲ close $9,905.45 vs 09:30 $9,854.99 (session +67.83) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,556.11 | ▼ 09:30 equity $9,855.67 vs yday $9,905.45 (-49.78) | — | — |
| 2026-08-21 09:30 ET | **COVER** | `AEM` | 3 | $216.30 | $2.00 | $-39.58 | $13,905.21 | ▼ -39.58 after sell → book $9,853.67; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `WYFI` | 28 | $21.54 | $2.07 | $-8.10 | $13,300.02 | ▼ -8.10 after sell → book $9,851.60; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `TOYO` | 139 | $4.68 | $2.41 | $-39.61 | $12,647.09 | ▼ -39.61 after sell → book $9,849.19; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `ABCL` | 52 | $11.57 | $2.15 | $+8.41 | $12,043.30 | ▲ +8.41 after sell → book $9,847.04; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `TEAM` | 3 | $174.22 | $2.00 | $-4.99 | $11,518.64 | ▼ -4.99 after sell → book $9,845.04; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `AAP` | 13 | $42.41 | $2.03 | $+53.63 | $10,965.28 | ▲ +53.63 after sell → book $9,843.01; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `WMT` | 5 | $103.69 | $2.00 | $+9.41 | $10,444.83 | ▲ +9.41 after sell → book $9,841.01; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `AQST` | 133 | $4.54 | $2.39 | $+4.48 | $9,838.62 | ▲ +4.48 after sell → book $9,838.62; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 263 | $3.11 | $3.47 | — | $10,653.08 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; leftover $819.89 | — |
| 2026-08-21 09:30 ET | **SHORT** | `MRNA` | 6 | $133.11 | $2.05 | — | $11,449.69 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=+109.5; leftover $819.89 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 9 | $89.10 | $2.06 | — | $12,249.53 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; leftover $819.89 | — |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 21 | $38.40 | $2.10 | — | $13,053.84 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; leftover $819.89 | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 39 | $20.90 | $2.15 | — | $13,866.79 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $819.89 | — |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 30 | $27.00 | $2.12 | — | $14,674.67 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; leftover $819.89 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,674.67 | ▼ close $9,805.27 vs 09:30 $9,855.67 (session -19.41) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,674.67 | ▼ 09:30 equity $9,798.16 vs yday $9,805.27 (-7.11) | — | — |
| 2026-08-24 09:30 ET | **COVER** | `QTRX` | 263 | $2.99 | $3.39 | $+24.70 | $13,884.90 | ▲ +24.70 after sell → book $9,794.76; vs 09:30 mark -3.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `MRNA` | 6 | $142.70 | $2.01 | $-61.60 | $13,026.70 | ▼ -61.60 after sell → book $9,792.76; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `AUGO` | 9 | $88.60 | $2.02 | $+0.42 | $12,227.28 | ▲ +0.42 after sell → book $9,790.74; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `SSRM` | 21 | $38.32 | $2.05 | $-2.47 | $11,420.51 | ▼ -2.47 after sell → book $9,788.69; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `ARIS` | 39 | $20.98 | $2.11 | $-7.38 | $10,600.18 | ▼ -7.38 after sell → book $9,786.58; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,600.18 | ▲ close $9,794.98 vs 09:30 $9,798.16 (session +8.40) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,600.18 | ▲ 09:30 equity $9,818.38 vs yday $9,794.98 (+23.40) | — | — |
| 2026-08-25 09:30 ET | **COVER** | `NOG` | 30 | $26.06 | $2.08 | $+24.00 | $9,816.30 | ▲ +24.00 after sell → book $9,816.30; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 72 | $13.62 | $2.25 | — | $10,795.04 | — | news🔴; gate news=bad; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; leftover $981.63 | — |
| 2026-08-25 09:30 ET | **SHORT** | `SSRM` | 26 | $37.75 | $2.11 | — | $11,774.43 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+17.8; leftover $981.63 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 18 | $54.51 | $2.09 | — | $12,753.52 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+15.1; leftover $981.63 | — |
| 2026-08-25 09:30 ET | **SHORT** | `BMO` | 5 | $175.01 | $2.05 | — | $13,626.52 | — | news🔴; gate news=bad; list earn_react; ret5=-7.0; leftover $981.63 | — |
| 2026-08-25 09:30 ET | **SHORT** | `INTU` | 2 | $364.35 | $2.04 | — | $14,353.18 | — | news🔴; gate news=bad; list overnight,overnight_mega; 🔵; ret5=+10.2; leftover $981.63 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,353.18 | ▲ close $9,820.82 vs 09:30 $9,818.38 (session +15.07) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,353.18 | ▲ 09:30 equity $9,908.82 vs yday $9,820.82 (+88.00) | — | — |
| 2026-08-26 09:30 ET | **COVER** | `AVAH` | 72 | $13.65 | $2.21 | $-6.26 | $13,368.18 | ▼ -6.26 after sell → book $9,906.62; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 26 | $38.41 | $2.07 | $-21.34 | $12,367.45 | ▼ -21.34 after sell → book $9,904.55; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARE` | 18 | $52.77 | $2.04 | $+27.19 | $11,415.55 | ▲ +27.19 after sell → book $9,902.51; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `BMO` | 5 | $173.22 | $2.00 | $+4.90 | $10,547.44 | ▲ +4.90 after sell → book $9,900.50; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **COVER** | `INTU` | 2 | $323.47 | $2.00 | $+77.73 | $9,898.50 | ▲ +77.73 after sell → book $9,898.50; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 4 | $213.94 | $2.05 | — | $10,752.22 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; leftover $989.85 | — |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 81 | $12.22 | $2.28 | — | $11,739.76 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.4; leftover $989.85 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 194 | $5.08 | $2.64 | — | $12,722.63 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+17.6; leftover $989.85 | — |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 7 | $132.64 | $2.06 | — | $13,649.06 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+16.5; leftover $989.85 | — |
| 2026-08-26 09:30 ET | **SHORT** | `CRM` | 4 | $199.94 | $2.04 | — | $14,446.77 | — | news🔴; gate news=bad; list overnight,overnight_mega; ret5=+2.1; leftover $989.85 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,446.77 | ▼ close $9,793.15 vs 09:30 $9,908.82 (session -94.28) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,446.77 | ▼ 09:30 equity $9,663.12 vs yday $9,793.15 (-130.03) | — | — |
| 2026-08-27 09:30 ET | **COVER** | `BE` | 4 | $227.10 | $2.00 | $-56.69 | $13,536.37 | ▼ -56.69 after sell → book $9,661.12; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `ABCL` | 81 | $12.25 | $2.23 | $-6.95 | $12,541.89 | ▼ -6.95 after sell → book $9,658.89; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `NEM` | 7 | $131.02 | $2.01 | $+7.27 | $11,622.74 | ▲ +7.27 after sell → book $9,656.88; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `CRM` | 4 | $230.05 | $2.00 | $-124.49 | $10,700.54 | ▼ -124.49 after sell → book $9,654.88; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SHORT** | `INTU` | 4 | $353.54 | $2.06 | — | $12,112.63 | — | news🔴; gate news=bad; list earn_react; ret5=-4.6; leftover $1609.15 | — |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 21 | $74.54 | $2.12 | — | $13,675.86 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=-0.1; leftover $1609.15 | — |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 29 | $55.25 | $2.14 | — | $15,275.96 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=+2.1; leftover $1609.15 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,275.96 | ▲ close $9,696.62 vs 09:30 $9,663.12 (session +48.07) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,275.96 | ▼ 09:30 equity $9,687.02 vs yday $9,696.62 (-9.60) | — | — |
| 2026-08-28 09:30 ET | **COVER** | `AQST` | 194 | $5.11 | $2.57 | $-11.03 | $14,282.05 | ▼ -11.03 after sell → book $9,684.45; vs 09:30 mark -2.57 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `INTU` | 4 | $347.82 | $2.00 | $+18.82 | $12,888.77 | ▲ +18.82 after sell → book $9,682.45; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `MT` | 21 | $75.39 | $2.05 | $-22.02 | $11,303.52 | ▼ -22.02 after sell → book $9,680.39; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **COVER** | `TX` | 29 | $55.97 | $2.08 | $-25.10 | $9,678.32 | ▼ -25.10 after sell → book $9,678.32; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 9 | $252.24 | $2.11 | — | $11,946.37 | — | news🔴; gate news=bad; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $2419.58 | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 80 | $30.18 | $2.33 | — | $14,358.44 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+12.1; leftover $2419.58 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,358.44 | ▲ close $9,840.55 vs 09:30 $9,687.02 (session +166.67) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,358.44 | ▲ 09:30 equity $9,926.99 vs yday $9,840.55 (+86.44) | — | — |
| 2026-08-31 09:30 ET | **COVER** | `SIMO` | 9 | $247.05 | $2.02 | $+42.59 | $12,132.97 | ▲ +42.59 after sell → book $9,924.97; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `FIG` | 80 | $27.60 | $2.23 | $+201.84 | $9,922.74 | ▲ +201.84 after sell → book $9,922.74; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,922.74 | ▲ close $9,922.74 vs 09:30 $9,926.99 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,922.74 | ▲ 09:30 equity $9,922.74 vs yday $9,922.74 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,922.74 | ▲ close $9,922.74 vs 09:30 $9,922.74 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,922.74 | ▲ 09:30 equity $9,922.74 vs yday $9,922.74 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,922.74 | ▲ close $9,922.74 vs 09:30 $9,922.74 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,922.74 | ▲ 09:30 equity $9,922.74 vs yday $9,922.74 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 167 | $14.85 | $2.61 | — | $12,400.09 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $2480.69 | — |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 1450 | $1.71 | $19.03 | — | $14,860.55 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+11.9; leftover $2480.69 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,860.55 | ▲ close $10,056.12 vs 09:30 $9,922.74 (session +155.02) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,860.55 | ▲ 09:30 equity $10,111.84 vs yday $10,056.12 (+55.72) | — | — |
| 2026-09-04 09:30 ET | **COVER** | `SLN` | 167 | $14.63 | $2.49 | $+31.64 | $12,414.85 | ▲ +31.64 after sell → book $10,109.35; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 541 | $4.67 | $7.16 | — | $14,934.17 | — | news🔴; gate news=bad; list yday_gainer; ret5=+11.9; leftover $2527.34 | — |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 33 | $76.55 | $2.19 | — | $17,458.13 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; leftover $2527.34 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,458.13 | ▼ close $10,011.34 vs 09:30 $10,111.84 (session -88.67) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,458.13 | ▼ 09:30 equity $9,995.76 vs yday $10,011.34 (-15.58) | — | — |
| 2026-09-08 09:30 ET | **COVER** | `OPK` | 1450 | $1.63 | $18.70 | $+78.26 | $15,075.92 | ▲ +78.26 after sell → book $9,977.05; vs 09:30 mark -18.71 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **COVER** | `PIPR` | 33 | $76.64 | $2.09 | $-7.25 | $12,544.71 | ▼ -7.25 after sell → book $9,974.96; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,544.71 | ▲ close $10,099.39 vs 09:30 $9,995.76 (session +124.43) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,544.71 | ▲ 09:30 equity $10,099.39 vs yday $10,099.39 (+0.00) | — | — |
| 2026-09-09 09:30 ET | **COVER** | `GSM` | 541 | $4.52 | $6.98 | $+67.01 | $10,092.41 | ▲ +67.01 after sell → book $10,092.41; vs 09:30 mark -6.98 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,092.41 | ▲ close $10,092.41 vs 09:30 $10,099.39 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,092.41 | ▲ 09:30 equity $10,092.41 vs yday $10,092.41 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,092.41 | ▲ close $10,092.41 vs 09:30 $10,092.41 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,092.41 | ▲ 09:30 equity $10,092.41 vs yday $10,092.41 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 8 | $112.83 | $2.06 | — | $10,993.04 | — | news🔴; gate news=bad; list yday_gainer,ohlc_hot; ret5=+11.7; leftover $1009.24 | — |
| 2026-09-11 09:30 ET | **SHORT** | `RWT` | 286 | $3.52 | $3.77 | — | $11,995.98 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-19.2; leftover $1009.24 | — |
| 2026-09-11 09:30 ET | **SHORT** | `CRDL` | 497 | $2.03 | $6.53 | — | $12,998.36 | — | news🔴; gate news=bad; list yday_mover; ret5=-8.8; leftover $1009.24 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BKV` | 40 | $24.97 | $2.16 | — | $13,995.00 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.8; leftover $1009.24 | — |
| 2026-09-11 09:30 ET | **SHORT** | `MYGN` | 299 | $3.37 | $3.94 | — | $14,998.69 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+4.0; leftover $1009.24 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,998.69 | ▼ close $10,066.89 vs 09:30 $10,092.41 (session -7.06) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,998.69 | ▲ 09:30 equity $10,096.20 vs yday $10,066.89 (+29.31) | — | — |
| 2026-09-14 09:30 ET | **COVER** | `QRVO` | 8 | $114.11 | $2.01 | $-14.27 | $14,083.79 | ▼ -14.27 after sell → book $10,094.18; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **COVER** | `RWT` | 286 | $3.53 | $3.69 | $-10.32 | $13,070.52 | ▼ -10.32 after sell → book $10,090.49; vs 09:30 mark -3.69 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **COVER** | `CRDL` | 497 | $1.98 | $6.41 | $+11.91 | $12,080.05 | ▲ +11.91 after sell → book $10,084.08; vs 09:30 mark -6.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,080.05 | ▼ close $9,994.04 vs 09:30 $10,096.20 (session -90.04) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,080.05 | ▼ 09:30 equity $9,973.85 vs yday $9,994.04 (-20.19) | — | — |
| 2026-09-15 09:30 ET | **COVER** | `MYGN` | 299 | $3.80 | $3.86 | $-136.37 | $10,940.00 | ▼ -136.37 after sell → book $9,970.00; vs 09:30 mark -3.85 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,940.00 | ▼ close $9,966.00 vs 09:30 $9,973.85 (session -4.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,940.00 | ▼ 09:30 equity $9,963.20 vs yday $9,966.00 (-2.80) | — | — |
| 2026-09-16 09:30 ET | **COVER** | `BKV` | 40 | $24.42 | $2.11 | $+17.73 | $9,961.09 | ▲ +17.73 after sell → book $9,961.09; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 1) | — |
| 2026-09-16 09:30 ET | **SHORT** | `BBNX` | 133 | $18.61 | $2.50 | — | $12,433.72 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $2490.27 | — |
| 2026-09-16 09:30 ET | **SHORT** | `GFR` | 364 | $6.83 | $4.84 | — | $14,914.99 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+11.2; leftover $2490.27 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,914.99 | ▼ close $9,602.69 vs 09:30 $9,963.20 (session -351.05) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,914.99 | ▼ 09:30 equity $9,569.09 vs yday $9,602.69 (-33.60) | — | — |
| 2026-09-17 09:30 ET | **COVER** | `GFR` | 364 | $6.48 | $4.70 | $+117.86 | $12,551.58 | ▲ +117.86 after sell → book $9,564.40; vs 09:30 mark -4.69 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SHORT** | `BULL` | 300 | $7.95 | $4.00 | — | $14,932.57 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-18.4; leftover $2391.10 | — |
| 2026-09-17 09:30 ET | **SHORT** | `LEN` | 29 | $81.00 | $2.17 | — | $17,279.40 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-3.0; leftover $2391.10 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,279.40 | ▲ close $9,804.91 vs 09:30 $9,569.09 (session +246.69) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,279.40 | ▲ 09:30 equity $9,822.25 vs yday $9,804.91 (+17.34) | — | — |
| 2026-09-18 09:30 ET | **COVER** | `BBNX` | 133 | $21.30 | $2.39 | $-362.66 | $14,444.11 | ▼ -362.66 after sell → book $9,819.86; vs 09:30 mark -2.39 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `BULL` | 300 | $7.85 | $3.87 | $+22.13 | $12,085.24 | ▲ +22.13 after sell → book $9,815.99; vs 09:30 mark -3.87 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **COVER** | `LEN` | 29 | $78.25 | $2.08 | $+75.50 | $9,813.92 | ▲ +75.50 after sell → book $9,813.92; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SHORT** | `FIVN` | 142 | $34.44 | $2.61 | — | $14,701.78 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; leftover $4906.96 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,701.78 | ▲ close $10,091.04 vs 09:30 $9,822.25 (session +279.74) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,701.78 | ▼ 09:30 equity $10,015.78 vs yday $10,091.04 (-75.26) | — | — |
| 2026-09-21 09:30 ET | **COVER** | `FIVN` | 142 | $33.00 | $2.42 | $+199.45 | $10,013.37 | ▲ +199.45 after sell → book $10,013.37; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 303 | $8.26 | $4.05 | — | $12,512.10 | — | news🔴; gate news=bad; list yday_mover; ret5=+7.7; leftover $2503.34 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AMD` | 4 | $583.88 | $2.09 | — | $14,845.53 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+8.5; leftover $2503.34 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,845.53 | ▲ close $10,286.69 vs 09:30 $10,015.78 (session +279.46) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,845.53 | ▲ 09:30 equity $10,322.49 vs yday $10,286.69 (+35.80) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `AMD` | 4 | $606.57 | $2.00 | $-94.86 | $12,417.24 | ▼ -94.86 after sell → book $10,320.48; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SHORT** | `USFD` | 18 | $93.97 | $2.11 | — | $14,106.59 | — | news🔴; gate news=bad; list flatten; ret5=-0.6; leftover $1720.08 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,106.59 | ▲ close $10,319.09 vs 09:30 $10,322.49 (session +0.72) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,106.59 | ▼ 09:30 equity $10,106.27 vs yday $10,319.09 (-212.82) | — | — |
| 2026-09-23 09:30 ET | **COVER** | `USFD` | 18 | $93.97 | $2.04 | $-4.16 | $12,413.09 | ▼ -4.16 after sell → book $10,104.23; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SHORT** | `HALO` | 21 | $116.85 | $2.15 | — | $14,864.79 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; leftover $2526.06 | — |
| 2026-09-23 09:30 ET | **SHORT** | `FIVN` | 64 | $38.91 | $2.28 | — | $17,352.43 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+16.6; leftover $2526.06 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,352.43 | ▼ close $10,085.98 vs 09:30 $10,106.27 (session -13.82) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,352.43 | ▲ 09:30 equity $10,107.54 vs yday $10,085.98 (+21.56) | — | — |
| 2026-09-24 09:30 ET | **COVER** | `HALO` | 21 | $112.22 | $2.05 | $+93.03 | $14,993.75 | ▲ +93.03 after sell → book $10,105.48; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,993.75 | ▼ close $9,932.63 vs 09:30 $10,107.54 (session -172.85) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,424.33 | ▼ 09:30 equity $9,718.38 vs yday $9,745.29 (-26.91) | 09:30 open · cash $12,424.33 (unchanged overnight, no fees) · equity $9,718.38 vs prior close $9,745.29 (-26.91) · 1 name(s) re-marked at the open (per-name table). AEHL×299 yday $8.96 → 09:30 $9.05 -26.91 | — |
| 2026-09-25 09:30 ET | **SHORT** | `HALO` | 21 | $115.36 | $2.15 | — | $14,844.74 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+5.1; leftover $2429.59 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `RSKD` | 309 | $7.85 | $4.12 | — | $17,266.27 | — | news🔴; gate news=bad; list yday_gainer; 🔵; ⚪; ret5=+25.4; leftover $2429.59 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,266.27 | ▼ close $9,671.71 vs 09:30 $9,718.38 (session -40.40) | 16:00 close · cash $17,266.27 · equity $9,671.71 vs 09:30 $9,718.38 (-46.67; session marks -40.40) · 3 name(s) marked open→close (per-name table). AEHL×299 09:30 $9.05 → close $9.36 -92.69; HALO×21 09:30 $115.36 → close $113.90 +30.66; RSKD×309 09:30 $7.85 → close $7.78 +21.63 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CADL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `LUNR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AAP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `EU` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BMO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `GEMI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `NVS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BKV` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-15 | `TYRA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `FIVN` | no_price | no 09:30 open |
| 2026-09-22 | `PAYX` | no_price | no 09:30 open |
| 2026-09-24 | `PANW` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `VOYG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `BMEA` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `AEHL` | 303 | 2026-09-21 @ $8.26 | news🔴; gate news=bad; list yday_mover; ret5=+7.7; leftover $2503.34 |
| `FIVN` | 64 | 2026-09-23 @ $38.91 | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+16.6; leftover $2526.06 |
