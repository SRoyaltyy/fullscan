# Factor mine action — `combo_sj_3070_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared short_news_r_h3/union_join_vol_green_h1 w=0.3,0.7 net=priority

Cash book **-10.80%** ($8,920) · signal-only (no cash/fees) was —. Starts YES **10/30**. Fills 323 · skips 173 · realized $+1600.28.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: short_news_r_h3 30%, union_join_vol_green_h1 70%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: short_news_r_h3 30%, union_join_vol_green_h1 70%.
- Member: short_news_r_h3 (30% · short · hold 3).
- Member: union_join_vol_green_h1 (70% · long · hold 1).
- Each lot remembers the owner kid, so that kid’s min-hold and list-drop rule apply. A hold-3 fresh-E lot is not sold because the heat kid only holds 1 day.

### When it buys

- At 09:30, each member runs its own pick_day on its own list and gates. Nobody mashes the names into one ranked list first.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- One ticker, one side. Claim order: fresh-E, then heat, then the other longs, then shorts. A name already held cannot be opened on the other side.
- Shared pile: leftover cash is offered in claim order (fresh-E, then heat, then other longs, then shorts). Each kid splits their room equally across *their* new names (leftover, whole shares, fees out of cash). Unused room spills to the next kid. A short fill adds cash; that cash can later fund a long, still capped by the cover rule (equity ≥ 2× notional).
- Skip a name if the slice cannot buy 1 share after fees.
- Skip a name if there is no official 09:30 open.
- Long lots buy shares (want the price up). Short lots borrow (want the price down) and are marked as a liability.

### When it sells

- Sell first, then buy. Never sell a ticker we do not hold.
- Minimum hold is the owner kid’s hold — the buy morning counts as 1.
- No extra panic button unless that owner recipe has one (🚨 / last-red / news🔴).
- List-drop: after the owner’s min-hold, sell at the 09:30 open if the name is no longer on *that owner’s* list today. The heat kid falling off does not sell a fresh-E lot.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `combo` — each member keeps its own 09:30 list (not a mashed shopping list).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **owner mix**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,897.53.

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
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 583 | $1.50 | $7.52 | — | $9,117.98 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=+9.2; combo leftover $875.00; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 59 | $14.80 | $2.17 | — | $8,242.61 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=-9.9; combo leftover $875.00; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 203 | $4.31 | $2.62 | — | $7,365.06 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; combo leftover $875.00; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 209 | $4.18 | $2.70 | — | $6,488.75 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; combo leftover $875.00; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 53 | $16.50 | $2.15 | — | $5,612.10 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; combo leftover $875.00; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 78 | $11.12 | $2.22 | — | $4,742.51 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; combo leftover $875.00; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 325 | $2.69 | $4.19 | — | $3,864.07 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; combo leftover $875.00; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 120 | $7.29 | $2.35 | — | $2,986.92 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; combo leftover $875.00; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 843 | $1.18 | $11.05 | — | $3,970.61 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $995.64; owner short_news_r_h3 | — |
| 2026-08-14 09:30 ET | **SHORT** | `LUNR` | 51 | $19.17 | $2.19 | — | $4,946.09 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+17.6; combo leftover $995.64; owner short_news_r_h3 | — |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 78 | $12.70 | $2.27 | — | $5,934.03 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.6; combo leftover $995.64; owner short_news_r_h3 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,934.03 | ▼ close $9,863.73 vs 09:30 $10,000.00 (session -94.84) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,934.03 | ▼ 09:30 equity $9,778.23 vs yday $9,863.73 (-85.50) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 583 | $1.52 | $7.63 | $-3.49 | $6,812.56 | ▼ -3.49 after sell → book $9,770.60; vs 09:30 mark -7.63 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 59 | $13.67 | $2.19 | $-71.02 | $7,616.90 | ▼ -71.02 after sell → book $9,768.41; vs 09:30 mark -2.19 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 203 | $4.60 | $2.66 | $+53.59 | $8,548.04 | ▲ +53.59 after sell → book $9,765.75; vs 09:30 mark -2.66 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 209 | $4.10 | $2.74 | $-22.16 | $9,402.20 | ▼ -22.16 after sell → book $9,763.01; vs 09:30 mark -2.74 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 53 | $15.73 | $2.17 | $-45.13 | $10,233.72 | ▼ -45.13 after sell → book $9,760.84; vs 09:30 mark -2.17 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 78 | $9.57 | $2.25 | $-125.37 | $10,977.93 | ▼ -125.37 after sell → book $9,758.59; vs 09:30 mark -2.25 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NCMI` | 325 | $2.80 | $4.26 | $+27.30 | $11,883.68 | ▲ +27.30 after sell → book $9,754.34; vs 09:30 mark -4.25 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMLS` | 120 | $7.24 | $2.38 | $-10.73 | $12,750.10 | ▼ -10.73 after sell → book $9,751.96; vs 09:30 mark -2.38 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 195 | $9.12 | $2.58 | — | $10,969.12 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; combo leftover $1785.01; owner union_join_vol_green_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 121 | $14.66 | $2.35 | — | $9,192.91 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; combo leftover $1785.01; owner union_join_vol_green_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `BORR` | 388 | $4.59 | $5.01 | — | $7,406.98 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ⚪; ret5=+14.8; combo leftover $1785.01; owner union_join_vol_green_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 426 | $4.19 | $5.50 | — | $5,616.55 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ⚪; ret5=+291.8; combo leftover $1785.01; owner union_join_vol_green_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `MP` | 30 | $58.01 | $2.08 | — | $3,874.17 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+14.9; combo leftover $1785.01; owner union_join_vol_green_h1 | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 673 | $1.15 | $8.82 | — | $4,639.29 | — | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; combo leftover $774.83; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 217 | $3.56 | $2.87 | — | $5,408.95 | — | news🔴; gate news=bad; list yday_mover; ret5=-15.6; combo leftover $774.83; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `APMD` | 24 | $31.70 | $2.10 | — | $6,167.64 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+17.6; combo leftover $774.83; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 257 | $3.01 | $3.39 | — | $6,937.82 | — | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; combo leftover $774.83; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `RNW` | 113 | $6.80 | $2.38 | — | $7,703.85 | — | news🔴; gate news=bad; list overnight; ⚪; ret5=+10.4; combo leftover $774.83; owner short_news_r_h3 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,703.85 | ▼ close $9,549.17 vs 09:30 $9,778.23 (session -165.72) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,703.85 | ▼ 09:30 equity $9,520.08 vs yday $9,549.17 (-29.09) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 195 | $9.03 | $2.62 | $-22.75 | $9,462.07 | ▼ -22.75 after sell → book $9,517.45; vs 09:30 mark -2.63 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 121 | $13.19 | $2.39 | $-182.61 | $11,055.68 | ▼ -182.61 after sell → book $9,515.07; vs 09:30 mark -2.38 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `BORR` | 388 | $4.56 | $5.08 | $-21.73 | $12,819.87 | ▼ -21.73 after sell → book $9,509.98; vs 09:30 mark -5.09 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 426 | $3.94 | $5.58 | $-117.57 | $14,492.74 | ▼ -117.57 after sell → book $9,504.41; vs 09:30 mark -5.57 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `MP` | 30 | $56.35 | $2.10 | $-53.98 | $16,181.13 | ▼ -53.98 after sell → book $9,502.30; vs 09:30 mark -2.11 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,181.13 | ▲ close $9,673.54 vs 09:30 $9,520.08 (session +171.24) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,181.13 | ▼ 09:30 equity $9,647.22 vs yday $9,673.54 (-26.32) | — | — |
| 2026-08-19 09:30 ET | **COVER** | `EU` | 843 | $1.07 | $10.87 | $+70.80 | $15,268.25 | ▲ +70.80 after sell → book $9,636.35; vs 09:30 mark -10.87 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 78 | $11.75 | $2.22 | $+69.21 | $14,349.52 | ▲ +69.21 after sell → book $9,634.12; vs 09:30 mark -2.23 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,349.52 | ▲ close $9,664.08 vs 09:30 $9,647.22 (session +29.96) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,349.52 | ▼ 09:30 equity $9,612.98 vs yday $9,664.08 (-51.10) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `LUNR` | 51 | $18.13 | $2.14 | $+48.71 | $13,422.75 | ▲ +48.71 after sell → book $9,610.84; vs 09:30 mark -2.14 | short_news_r_h3: dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 673 | $0.96 | $8.50 | $+108.53 | $12,766.15 | ▲ +108.53 after sell → book $9,602.34; vs 09:30 mark -8.50 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ZNTL` | 217 | $4.01 | $2.80 | $-104.40 | $11,892.10 | ▼ -104.40 after sell → book $9,599.54; vs 09:30 mark -2.80 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `APMD` | 24 | $31.87 | $2.06 | $-8.24 | $11,125.15 | ▼ -8.24 after sell → book $9,597.47; vs 09:30 mark -2.07 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 257 | $2.95 | $3.32 | $+8.72 | $10,363.69 | ▲ +8.72 after sell → book $9,594.16; vs 09:30 mark -3.31 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `RNW` | 113 | $6.81 | $2.33 | $-5.84 | $9,591.83 | ▼ -5.84 after sell → book $9,591.83; vs 09:30 mark -2.33 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 40 | $20.55 | $2.11 | — | $8,767.72 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; combo leftover $839.29; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 40 | $20.65 | $2.11 | — | $7,939.61 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; combo leftover $839.29; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 145 | $5.77 | $2.42 | — | $7,100.54 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; combo leftover $839.29; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 42 | $19.63 | $2.12 | — | $6,273.96 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; combo leftover $839.29; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 28 | $29.63 | $2.07 | — | $5,442.25 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; combo leftover $839.29; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 479 | $1.75 | $6.18 | — | $4,597.82 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; combo leftover $839.29; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 5 | $144.54 | $2.00 | — | $3,873.11 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; combo leftover $839.29; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 170 | $4.92 | $2.50 | — | $3,034.21 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $839.29; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 1 | $204.45 | $2.02 | — | $3,236.64 | — | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; combo leftover $379.28; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 17 | $21.40 | $2.07 | — | $3,598.37 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-25.2; combo leftover $379.28; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 85 | $4.43 | $2.28 | — | $3,972.64 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-23.1; combo leftover $379.28; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 32 | $11.81 | $2.12 | — | $4,348.61 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $379.28; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 2 | $173.90 | $2.03 | — | $4,694.38 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; combo leftover $379.28; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 8 | $46.85 | $2.04 | — | $5,067.14 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=+5.0; combo leftover $379.28; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 3 | $106.38 | $2.03 | — | $5,384.25 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; combo leftover $379.28; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 82 | $4.61 | $2.27 | — | $5,760.00 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $379.28; owner short_news_r_h3 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,760.00 | ▲ close $9,706.26 vs 09:30 $9,612.98 (session +152.81) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,760.00 | ▲ 09:30 equity $9,911.37 vs yday $9,706.26 (+205.11) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 40 | $21.90 | $2.13 | $+49.76 | $6,633.87 | ▲ +49.76 after sell → book $9,909.24; vs 09:30 mark -2.13 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 40 | $21.75 | $2.13 | $+39.76 | $7,501.74 | ▲ +39.76 after sell → book $9,907.11; vs 09:30 mark -2.13 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 145 | $5.67 | $2.46 | $-19.38 | $8,321.43 | ▼ -19.38 after sell → book $9,904.65; vs 09:30 mark -2.46 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 42 | $21.17 | $2.14 | $+60.43 | $9,208.43 | ▲ +60.43 after sell → book $9,902.51; vs 09:30 mark -2.14 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 28 | $32.17 | $2.09 | $+66.95 | $10,107.10 | ▲ +66.95 after sell → book $9,900.42; vs 09:30 mark -2.09 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 479 | $1.79 | $6.27 | $+6.71 | $10,958.24 | ▲ +6.71 after sell → book $9,894.15; vs 09:30 mark -6.27 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 5 | $154.70 | $2.02 | $+46.77 | $11,729.72 | ▲ +46.77 after sell → book $9,892.13; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 170 | $5.20 | $2.54 | $+42.56 | $12,611.18 | ▲ +42.56 after sell → book $9,889.59; vs 09:30 mark -2.54 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $11,414.86 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; combo leftover $1261.12; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 73 | $17.20 | $2.21 | — | $10,157.05 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; combo leftover $1261.12; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 113 | $11.13 | $2.33 | — | $8,897.03 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; combo leftover $1261.12; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 955 | $1.32 | $12.32 | — | $7,624.11 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; combo leftover $1261.12; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 759 | $1.66 | $9.79 | — | $6,354.38 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; combo leftover $1261.12; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `INDP` | 907 | $1.39 | $11.70 | — | $5,081.95 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+30.2; combo leftover $1261.12; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 152 | $8.28 | $2.45 | — | $3,820.94 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; combo leftover $1261.12; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 204 | $3.11 | $2.69 | — | $4,452.69 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; combo leftover $636.82; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `MRNA` | 4 | $133.11 | $2.04 | — | $4,983.09 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=+109.5; combo leftover $636.82; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 7 | $89.10 | $2.05 | — | $5,604.74 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $636.82; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 16 | $38.40 | $2.07 | — | $6,217.07 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $636.82; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 30 | $20.90 | $2.12 | — | $6,841.95 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $636.82; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 23 | $27.00 | $2.10 | — | $7,460.86 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; combo leftover $636.82; owner short_news_r_h3 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,460.86 | ▲ close $10,031.08 vs 09:30 $9,911.37 (session +197.37) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,460.86 | ▲ 09:30 equity $10,393.18 vs yday $10,031.08 (+362.10) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $8,663.92 | ▲ +6.74 after sell → book $10,391.14; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 73 | $16.57 | $2.23 | $-50.43 | $9,871.30 | ▼ -50.43 after sell → book $10,388.91; vs 09:30 mark -2.23 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 113 | $13.33 | $2.36 | $+243.91 | $11,375.23 | ▲ +243.91 after sell → book $10,386.55; vs 09:30 mark -2.36 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 955 | $1.83 | $12.49 | $+462.24 | $13,110.38 | ▲ +462.24 after sell → book $10,374.05; vs 09:30 mark -12.50 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 759 | $1.55 | $9.93 | $-103.21 | $14,276.91 | ▼ -103.21 after sell → book $10,364.13; vs 09:30 mark -9.92 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `INDP` | 907 | $1.24 | $11.86 | $-159.61 | $15,389.73 | ▼ -159.61 after sell → book $10,352.27; vs 09:30 mark -11.86 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 152 | $8.59 | $2.48 | $+42.19 | $16,692.92 | ▲ +42.19 after sell → book $10,349.78; vs 09:30 mark -2.49 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,692.92 | ▲ close $10,407.45 vs 09:30 $10,393.18 (session +57.67) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,692.92 | ▲ 09:30 equity $10,437.18 vs yday $10,407.45 (+29.73) | — | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 1 | $212.00 | $1.99 | $-11.56 | $16,478.93 | ▼ -11.56 after sell → book $10,435.19; vs 09:30 mark -1.99 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 17 | $20.90 | $2.04 | $+4.39 | $16,121.59 | ▲ +4.39 after sell → book $10,433.15; vs 09:30 mark -2.04 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TOYO` | 85 | $4.42 | $2.25 | $-3.67 | $15,743.65 | ▼ -3.67 after sell → book $10,430.91; vs 09:30 mark -2.24 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ABCL` | 32 | $11.00 | $2.09 | $+21.88 | $15,389.56 | ▲ +21.88 after sell → book $10,428.82; vs 09:30 mark -2.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 2 | $170.64 | $2.00 | $+2.50 | $15,046.28 | ▲ +2.50 after sell → book $10,426.82; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AAP` | 8 | $43.63 | $2.01 | $+21.70 | $14,695.23 | ▲ +21.70 after sell → book $10,424.81; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 3 | $105.58 | $2.00 | $-1.63 | $14,376.49 | ▼ -1.63 after sell → book $10,422.81; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 82 | $4.77 | $2.24 | $-17.63 | $13,983.11 | ▼ -17.63 after sell → book $10,420.57; vs 09:30 mark -2.24 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 750 | $1.63 | $9.68 | — | $12,750.94 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; combo leftover $1223.52; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 344 | $3.55 | $4.44 | — | $11,525.30 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+27.9; combo leftover $1223.52; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 192 | $6.37 | $2.57 | — | $10,299.70 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+10.9; combo leftover $1223.52; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 34 | $35.05 | $2.09 | — | $9,105.90 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+19.7; combo leftover $1223.52; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ETON` | 18 | $64.55 | $2.04 | — | $7,941.96 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+4.4; combo leftover $1223.52; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `WPM` | 7 | $156.51 | $2.01 | — | $6,844.38 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+17.4; combo leftover $1223.52; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUZ` | 136 | $8.98 | $2.40 | — | $5,620.70 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+15.4; combo leftover $1223.52; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `IAUX` | 643 | $1.90 | $8.29 | — | $4,390.71 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+16.4; combo leftover $1223.52; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 80 | $13.62 | $2.28 | — | $5,478.42 | — | news🔴; gate news=bad; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; combo leftover $1097.68; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 20 | $54.51 | $2.10 | — | $6,566.52 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+15.1; combo leftover $1097.68; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `BMO` | 6 | $175.01 | $2.06 | — | $7,614.53 | — | news🔴; gate news=bad; list earn_react; ret5=-7.0; combo leftover $1097.68; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `INTU` | 3 | $364.35 | $2.05 | — | $8,705.53 | — | news🔴; gate news=bad; list overnight,overnight_mega; 🔵; ret5=+10.2; combo leftover $1097.68; owner short_news_r_h3 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,705.53 | ▲ close $10,522.62 vs 09:30 $10,437.18 (session +144.05) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,705.53 | ▲ 09:30 equity $10,614.73 vs yday $10,522.62 (+92.11) | — | — |
| 2026-08-26 09:30 ET | **COVER** | `QTRX` | 204 | $2.83 | $2.63 | $+51.80 | $8,125.58 | ▲ +51.80 after sell → book $10,612.10; vs 09:30 mark -2.63 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `MRNA` | 4 | $154.20 | $2.00 | $-88.40 | $7,506.77 | ▼ -88.40 after sell → book $10,610.09; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 7 | $88.24 | $2.01 | $+1.96 | $6,887.08 | ▲ +1.96 after sell → book $10,608.08; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 16 | $38.41 | $2.04 | $-4.27 | $6,270.48 | ▼ -4.27 after sell → book $10,606.04; vs 09:30 mark -2.04 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 30 | $20.50 | $2.08 | $+7.80 | $5,653.40 | ▲ +7.80 after sell → book $10,603.96; vs 09:30 mark -2.08 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 23 | $26.00 | $2.06 | $+18.84 | $5,053.35 | ▲ +18.84 after sell → book $10,601.91; vs 09:30 mark -2.05 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 750 | $1.75 | $9.81 | $+74.27 | $6,359.79 | ▲ +74.27 after sell → book $10,592.10; vs 09:30 mark -9.81 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GORO` | 344 | $3.77 | $4.51 | $+66.74 | $7,652.16 | ▲ +66.74 after sell → book $10,587.59; vs 09:30 mark -4.51 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZURA` | 192 | $6.13 | $2.61 | $-51.25 | $8,826.51 | ▼ -51.25 after sell → book $10,584.98; vs 09:30 mark -2.61 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 34 | $35.70 | $2.11 | $+17.90 | $10,038.20 | ▲ +17.90 after sell → book $10,582.87; vs 09:30 mark -2.11 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ETON` | 18 | $63.60 | $2.06 | $-21.21 | $11,180.94 | ▼ -21.21 after sell → book $10,580.81; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `WPM` | 7 | $160.93 | $2.03 | $+26.90 | $12,305.42 | ▲ +26.90 after sell → book $10,578.78; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SUZ` | 136 | $9.03 | $2.43 | $+1.97 | $13,531.07 | ▲ +1.97 after sell → book $10,576.35; vs 09:30 mark -2.43 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `IAUX` | 643 | $1.87 | $8.41 | $-36.00 | $14,725.06 | ▼ -36.00 after sell → book $10,567.93; vs 09:30 mark -8.42 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 1774 | $5.81 | $22.88 | — | $4,395.24 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ⚪; ret5=+117.2; combo leftover $10307.54; owner union_join_vol_green_h1 | — |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 4 | $213.94 | $2.05 | — | $5,248.95 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; combo leftover $879.05; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 71 | $12.22 | $2.25 | — | $6,114.33 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.4; combo leftover $879.05; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 173 | $5.08 | $2.57 | — | $6,990.59 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+17.6; combo leftover $879.05; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 6 | $132.64 | $2.05 | — | $7,784.38 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+16.5; combo leftover $879.05; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `CRM` | 4 | $199.94 | $2.04 | — | $8,582.10 | — | news🔴; gate news=bad; list overnight,overnight_mega; ret5=+2.1; combo leftover $879.05; owner short_news_r_h3 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,582.10 | ▲ close $10,680.15 vs 09:30 $10,614.73 (session +146.06) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,582.10 | ▲ 09:30 equity $11,459.84 vs yday $10,680.15 (+779.69) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 1774 | $6.50 | $23.27 | $+1177.90 | $20,089.83 | ▲ +1,177.90 after sell → book $11,436.57; vs 09:30 mark -23.27 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `DKS` | 109 | $128.73 | $2.32 | — | $6,055.94 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-32.2; combo leftover $14062.88; owner union_join_vol_green_h1 | — |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 38 | $74.54 | $2.21 | — | $8,886.25 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=-0.1; combo leftover $2858.56; owner short_news_r_h3 | — |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 51 | $55.25 | $2.25 | — | $11,701.74 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=+2.1; combo leftover $2858.56; owner short_news_r_h3 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,701.74 | ▲ close $11,707.08 vs 09:30 $11,459.84 (session +277.30) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,701.74 | ▲ 09:30 equity $11,799.35 vs yday $11,707.08 (+92.27) | — | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 80 | $13.90 | $2.23 | $-26.51 | $10,587.51 | ▼ -26.51 after sell → book $11,797.12; vs 09:30 mark -2.23 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 20 | $52.49 | $2.05 | $+36.25 | $9,535.66 | ▲ +36.25 after sell → book $11,795.07; vs 09:30 mark -2.05 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `BMO` | 6 | $172.76 | $2.01 | $+9.44 | $8,497.10 | ▲ +9.44 after sell → book $11,793.06; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `INTU` | 3 | $347.82 | $2.00 | $+45.54 | $7,451.64 | ▲ +45.54 after sell → book $11,791.06; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 109 | $132.80 | $2.45 | $+438.86 | $21,924.39 | ▲ +438.86 after sell → book $11,788.61; vs 09:30 mark -2.45 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 26 | $146.07 | $2.07 | — | $18,124.50 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+38.8; combo leftover $3836.77; owner union_join_vol_green_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 164 | $23.30 | $2.48 | — | $14,300.82 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+14.5; combo leftover $3836.77; owner union_join_vol_green_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 201 | $19.00 | $2.60 | — | $10,479.22 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; ret5=+7.5; combo leftover $3836.77; owner union_join_vol_green_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 155 | $24.69 | $2.46 | — | $6,649.81 | — | combo gate; gate join=good,vol=good,last_green=True; list earn_react; ret5=+5.8; combo leftover $3836.77; owner union_join_vol_green_h1 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 11 | $252.24 | $2.13 | — | $9,422.32 | — | news🔴; gate news=bad; list probable,yday_gainer; ⚪; ret5=+2.2; combo leftover $2944.75; owner short_news_r_h3 | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 97 | $30.18 | $2.40 | — | $12,347.38 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+12.1; combo leftover $2944.75; owner short_news_r_h3 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,347.38 | ▲ close $11,912.23 vs 09:30 $11,799.35 (session +137.76) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,347.38 | ▼ 09:30 equity $11,781.02 vs yday $11,912.23 (-131.21) | — | — |
| 2026-08-31 09:30 ET | **COVER** | `BE` | 4 | $208.88 | $2.00 | $+16.19 | $11,509.86 | ▲ +16.19 after sell → book $11,779.02; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 71 | $11.10 | $2.20 | $+75.07 | $10,719.56 | ▲ +75.07 after sell → book $11,776.82; vs 09:30 mark -2.20 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 173 | $4.97 | $2.51 | $+13.08 | $9,856.37 | ▲ +13.08 after sell → book $11,774.31; vs 09:30 mark -2.51 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `NEM` | 6 | $127.45 | $2.01 | $+27.08 | $9,089.66 | ▲ +27.08 after sell → book $11,772.30; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `CRM` | 4 | $254.39 | $2.00 | $-221.85 | $8,070.10 | ▼ -221.85 after sell → book $11,770.30; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 26 | $148.03 | $2.11 | $+46.78 | $11,916.77 | ▲ +46.78 after sell → book $11,768.19; vs 09:30 mark -2.11 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NCNO` | 164 | $22.66 | $2.54 | $-109.98 | $15,630.47 | ▼ -109.98 after sell → book $11,765.65; vs 09:30 mark -2.54 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TH` | 201 | $18.12 | $2.66 | $-181.13 | $19,270.94 | ▼ -181.13 after sell → book $11,762.99; vs 09:30 mark -2.66 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GAP` | 155 | $22.98 | $2.51 | $-270.01 | $22,830.33 | ▼ -270.01 after sell → book $11,760.48; vs 09:30 mark -2.51 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22,830.33 | ▲ close $11,817.94 vs 09:30 $11,781.02 (session +57.46) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22,830.33 | ▲ 09:30 equity $11,989.40 vs yday $11,817.94 (+171.46) | — | — |
| 2026-09-01 09:30 ET | **COVER** | `MT` | 38 | $73.22 | $2.10 | $+45.84 | $20,045.87 | ▲ +45.84 after sell → book $11,987.30; vs 09:30 mark -2.10 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `TX` | 51 | $54.76 | $2.14 | $+20.59 | $17,250.96 | ▲ +20.59 after sell → book $11,985.15; vs 09:30 mark -2.15 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,250.96 | ▲ close $12,001.71 vs 09:30 $11,989.40 (session +16.56) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,250.96 | ▲ 09:30 equity $12,060.49 vs yday $12,001.71 (+58.78) | — | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 11 | $235.71 | $2.02 | $+177.68 | $14,656.13 | ▲ +177.68 after sell → book $12,058.47; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 97 | $26.78 | $2.28 | $+325.12 | $12,056.19 | ▲ +325.12 after sell → book $12,056.19; vs 09:30 mark -2.28 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,056.19 | ▲ close $12,056.19 vs 09:30 $12,060.49 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,056.19 | ▲ 09:30 equity $12,056.19 vs yday $12,056.19 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 7 | $132.45 | $2.01 | — | $11,127.03 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; combo leftover $1054.92; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 62 | $16.77 | $2.18 | — | $10,085.11 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; combo leftover $1054.92; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 483 | $2.18 | $6.23 | — | $9,025.94 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; combo leftover $1054.92; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 44 | $23.88 | $2.12 | — | $7,973.10 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $1054.92; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 101 | $10.42 | $2.29 | — | $6,918.39 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; combo leftover $1054.92; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 546 | $1.93 | $7.04 | — | $5,857.56 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; combo leftover $1054.92; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `DUOL` | 6 | $161.54 | $2.01 | — | $4,886.31 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+12.0; combo leftover $1054.92; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `ALMS` | 101 | $10.38 | $2.29 | — | $3,836.15 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ret5=-56.2; combo leftover $1054.92; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 129 | $14.85 | $2.47 | — | $5,749.33 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; combo leftover $1918.07; owner short_news_r_h3 | — |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 1121 | $1.71 | $14.71 | — | $7,651.53 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+11.9; combo leftover $1918.07; owner short_news_r_h3 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,651.53 | ▲ close $12,097.66 vs 09:30 $12,056.19 (session +84.82) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,651.53 | ▲ 09:30 equity $12,129.03 vs yday $12,097.66 (+31.37) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 7 | $130.03 | $2.03 | $-20.98 | $8,559.70 | ▼ -20.98 after sell → book $12,126.99; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 62 | $15.61 | $2.20 | $-76.29 | $9,525.33 | ▼ -76.29 after sell → book $12,124.80; vs 09:30 mark -2.19 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 483 | $2.16 | $6.32 | $-22.21 | $10,562.29 | ▼ -22.21 after sell → book $12,118.48; vs 09:30 mark -6.32 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 44 | $23.84 | $2.14 | $-6.02 | $11,609.11 | ▼ -6.02 after sell → book $12,116.34; vs 09:30 mark -2.14 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 101 | $10.50 | $2.32 | $+3.47 | $12,667.29 | ▲ +3.47 after sell → book $12,114.02; vs 09:30 mark -2.32 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `BMEA` | 546 | $1.90 | $7.14 | $-30.57 | $13,697.54 | ▼ -30.57 after sell → book $12,106.87; vs 09:30 mark -7.15 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DUOL` | 6 | $157.46 | $2.03 | $-28.52 | $14,640.27 | ▼ -28.52 after sell → book $12,104.84; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ALMS` | 101 | $11.23 | $2.32 | $+81.74 | $15,772.18 | ▲ +81.74 after sell → book $12,102.52; vs 09:30 mark -2.32 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $513.78 | $2.00 | — | $14,742.63 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; combo leftover $1380.07; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 16 | $82.70 | $2.04 | — | $13,417.39 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+15.7; combo leftover $1380.07; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 549 | $2.51 | $7.08 | — | $12,032.32 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+21.8; combo leftover $1380.07; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `MDB` | 3 | $378.34 | $2.00 | — | $10,895.30 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-12.7; combo leftover $1380.07; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 54 | $25.18 | $2.15 | — | $9,533.43 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+16.0; combo leftover $1380.07; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 238 | $5.79 | $3.07 | — | $8,152.34 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+15.2; combo leftover $1380.07; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `TDS` | 36 | $37.44 | $2.10 | — | $6,802.40 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+14.1; combo leftover $1380.07; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `AHCO` | 218 | $6.32 | $2.81 | — | $5,421.83 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+10.7; combo leftover $1380.07; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 580 | $4.67 | $7.67 | — | $8,122.75 | — | news🔴; gate news=bad; list yday_gainer; ret5=+11.9; combo leftover $2710.91; owner short_news_r_h3 | — |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 35 | $76.55 | $2.20 | — | $10,799.80 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; combo leftover $2710.91; owner short_news_r_h3 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,799.80 | ▲ close $12,380.89 vs 09:30 $12,129.03 (session +311.49) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,799.80 | ▼ 09:30 equity $12,300.00 vs yday $12,380.89 (-80.89) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 2 | $521.15 | $2.02 | $+10.73 | $11,840.09 | ▲ +10.73 after sell → book $12,297.99; vs 09:30 mark -2.01 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TARS` | 16 | $89.67 | $2.06 | $+107.42 | $13,272.75 | ▲ +107.42 after sell → book $12,295.93; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BRR` | 549 | $2.66 | $7.18 | $+68.08 | $14,725.90 | ▲ +68.08 after sell → book $12,288.74; vs 09:30 mark -7.19 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MDB` | 3 | $360.75 | $2.02 | $-56.79 | $15,806.13 | ▼ -56.79 after sell → book $12,286.72; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 54 | $26.44 | $2.17 | $+63.71 | $17,231.72 | ▲ +63.71 after sell → book $12,284.55; vs 09:30 mark -2.17 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 238 | $5.81 | $3.12 | $-1.43 | $18,611.38 | ▼ -1.43 after sell → book $12,281.43; vs 09:30 mark -3.12 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TDS` | 36 | $37.75 | $2.12 | $+6.94 | $19,968.26 | ▲ +6.94 after sell → book $12,279.31; vs 09:30 mark -2.12 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `AHCO` | 218 | $6.48 | $2.86 | $+29.21 | $21,378.04 | ▲ +29.21 after sell → book $12,276.45; vs 09:30 mark -2.86 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,378.04 | ▲ close $12,501.14 vs 09:30 $12,300.00 (session +224.69) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,378.04 | ▲ 09:30 equity $12,527.46 vs yday $12,501.14 (+26.32) | — | — |
| 2026-09-09 09:30 ET | **COVER** | `SLN` | 129 | $13.60 | $2.38 | $+156.41 | $19,621.26 | ▲ +156.41 after sell → book $12,525.08; vs 09:30 mark -2.38 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 1121 | $1.58 | $14.46 | $+116.55 | $17,835.62 | ▲ +116.55 after sell → book $12,510.62; vs 09:30 mark -14.46 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,835.62 | ▲ close $12,537.82 vs 09:30 $12,527.46 (session +27.20) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,835.62 | ▲ 09:30 equity $12,619.17 vs yday $12,537.82 (+81.35) | — | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 580 | $4.36 | $7.48 | $+164.64 | $15,299.34 | ▲ +164.64 after sell → book $12,611.69; vs 09:30 mark -7.48 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PIPR` | 35 | $76.79 | $2.10 | $-12.69 | $12,609.60 | ▼ -12.69 after sell → book $12,609.60; vs 09:30 mark -2.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,609.60 | ▲ close $12,609.60 vs 09:30 $12,619.17 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,609.60 | ▲ 09:30 equity $12,609.60 vs yday $12,609.60 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 46 | $23.63 | $2.13 | — | $11,520.49 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; ret5=-6.3; combo leftover $1103.34; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 408 | $2.70 | $5.26 | — | $10,413.62 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+118.8; combo leftover $1103.34; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 100 | $10.95 | $2.29 | — | $9,316.33 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+20.8; combo leftover $1103.34; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 224 | $4.91 | $2.89 | — | $8,213.60 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+76.3; combo leftover $1103.34; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 13 | $84.27 | $2.03 | — | $7,116.07 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ret5=+17.2; combo leftover $1103.34; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `ASO` | 20 | $54.91 | $2.05 | — | $6,015.82 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+24.3; combo leftover $1103.34; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 179 | $6.16 | $2.53 | — | $4,910.65 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+36.4; combo leftover $1103.34; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `COO` | 20 | $54.66 | $2.05 | — | $3,815.40 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-22.3; combo leftover $1103.34; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 6 | $112.83 | $2.05 | — | $4,490.36 | — | news🔴; gate news=bad; list yday_gainer,ohlc_hot; ret5=+11.7; combo leftover $763.08; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `RWT` | 216 | $3.52 | $2.85 | — | $5,247.83 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-19.2; combo leftover $763.08; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `CRDL` | 375 | $2.03 | $4.93 | — | $6,004.15 | — | news🔴; gate news=bad; list yday_mover; ret5=-8.8; combo leftover $763.08; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BKV` | 30 | $24.97 | $2.12 | — | $6,751.13 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.8; combo leftover $763.08; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `MYGN` | 226 | $3.37 | $2.98 | — | $7,509.76 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+4.0; combo leftover $763.08; owner short_news_r_h3 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,509.76 | ▼ close $12,466.97 vs 09:30 $12,609.60 (session -106.47) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,509.76 | ▲ 09:30 equity $12,569.42 vs yday $12,466.97 (+102.45) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `TYRA` | 46 | $23.20 | $2.15 | $-24.06 | $8,574.82 | ▼ -24.06 after sell → book $12,567.28; vs 09:30 mark -2.14 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `INDP` | 408 | $2.80 | $5.34 | $+30.20 | $9,711.88 | ▲ +30.20 after sell → book $12,561.94; vs 09:30 mark -5.34 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `WLTH` | 100 | $10.29 | $2.32 | $-70.61 | $10,738.56 | ▼ -70.61 after sell → book $12,559.62; vs 09:30 mark -2.32 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BNC` | 224 | $5.03 | $2.94 | $+21.05 | $11,862.34 | ▲ +21.05 after sell → book $12,556.68; vs 09:30 mark -2.94 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SWKS` | 13 | $86.06 | $2.05 | $+19.19 | $12,979.07 | ▲ +19.19 after sell → book $12,554.63; vs 09:30 mark -2.05 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ASO` | 20 | $54.75 | $2.07 | $-7.32 | $14,072.00 | ▼ -7.32 after sell → book $12,552.56; vs 09:30 mark -2.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 179 | $6.02 | $2.57 | $-30.15 | $15,147.02 | ▼ -30.15 after sell → book $12,550.00; vs 09:30 mark -2.56 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `COO` | 20 | $54.78 | $2.07 | $-1.72 | $16,240.55 | ▼ -1.72 after sell → book $12,547.93; vs 09:30 mark -2.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,240.55 | ▼ close $12,448.00 vs 09:30 $12,569.42 (session -99.93) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,240.55 | ▲ 09:30 equity $12,448.05 vs yday $12,448.00 (+0.05) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,240.55 | ▼ close $12,386.50 vs 09:30 $12,448.05 (session -61.55) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,240.55 | ▲ 09:30 equity $12,397.94 vs yday $12,386.50 (+11.44) | — | — |
| 2026-09-16 09:30 ET | **COVER** | `QRVO` | 6 | $118.18 | $2.01 | $-36.12 | $15,529.46 | ▼ -36.12 after sell → book $12,395.93; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `RWT` | 216 | $3.98 | $2.79 | $-105.00 | $14,666.99 | ▼ -105.00 after sell → book $12,393.14; vs 09:30 mark -2.79 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `CRDL` | 375 | $1.85 | $4.84 | $+57.73 | $13,968.40 | ▲ +57.73 after sell → book $12,388.30; vs 09:30 mark -4.84 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `BKV` | 30 | $24.42 | $2.08 | $+12.30 | $13,233.72 | ▲ +12.30 after sell → book $12,386.22; vs 09:30 mark -2.08 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `MYGN` | 226 | $3.75 | $2.92 | $-91.78 | $12,383.31 | ▼ -91.78 after sell → book $12,383.31; vs 09:30 mark -2.91 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 14 | $77.12 | $2.03 | — | $11,301.60 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; ret5=+7.2; combo leftover $1083.54; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 184 | $5.87 | $2.54 | — | $10,218.97 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+3.1; combo leftover $1083.54; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 12 | $87.40 | $2.03 | — | $9,168.15 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+3.2; combo leftover $1083.54; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 39 | $27.09 | $2.11 | — | $8,109.53 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; combo leftover $1083.54; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 12 | $89.38 | $2.03 | — | $7,034.95 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; combo leftover $1083.54; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 46 | $23.29 | $2.13 | — | $5,961.48 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+16.1; combo leftover $1083.54; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 32 | $33.14 | $2.09 | — | $4,898.91 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=-2.9; combo leftover $1083.54; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `CAI` | 38 | $28.16 | $2.10 | — | $3,826.73 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ret5=+14.8; combo leftover $1083.54; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **SHORT** | `BBNX` | 102 | $18.61 | $2.38 | — | $5,722.57 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; ret5=-0.5; combo leftover $1913.36; owner short_news_r_h3 | — |
| 2026-09-16 09:30 ET | **SHORT** | `GFR` | 280 | $6.83 | $3.73 | — | $7,631.24 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+11.2; combo leftover $1913.36; owner short_news_r_h3 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,631.24 | ▼ close $12,016.05 vs 09:30 $12,397.94 (session -344.10) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,631.24 | ▲ 09:30 equity $12,134.36 vs yday $12,016.05 (+118.31) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `RDNT` | 14 | $76.44 | $2.05 | $-13.60 | $8,699.35 | ▼ -13.60 after sell → book $12,132.31; vs 09:30 mark -2.05 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 184 | $5.58 | $2.58 | $-58.48 | $9,723.49 | ▼ -58.48 after sell → book $12,129.73; vs 09:30 mark -2.58 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 12 | $83.20 | $2.05 | $-54.47 | $10,719.84 | ▼ -54.47 after sell → book $12,127.68; vs 09:30 mark -2.05 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ADPT` | 39 | $28.23 | $2.13 | $+40.23 | $11,818.68 | ▲ +40.23 after sell → book $12,125.55; vs 09:30 mark -2.13 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 12 | $86.76 | $2.05 | $-35.51 | $12,857.76 | ▼ -35.51 after sell → book $12,123.51; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 46 | $24.09 | $2.15 | $+32.52 | $13,963.75 | ▲ +32.52 after sell → book $12,121.36; vs 09:30 mark -2.15 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `CAI` | 38 | $28.59 | $2.12 | $+12.30 | $15,048.24 | ▲ +12.30 after sell → book $12,119.24; vs 09:30 mark -2.12 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 6 | $233.85 | $2.01 | — | $13,643.13 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; ret5=+11.7; combo leftover $1504.82; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 10 | $147.61 | $2.02 | — | $12,165.01 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; ret5=+17.7; combo leftover $1504.82; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 198 | $7.59 | $2.58 | — | $10,659.60 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; 🔵; ret5=+9.4; combo leftover $1504.82; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 57 | $25.95 | $2.16 | — | $9,178.29 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; combo leftover $1504.82; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 8 | $170.85 | $2.01 | — | $7,809.48 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; combo leftover $1504.82; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `CIFR` | 83 | $18.04 | $2.24 | — | $6,310.34 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.1; combo leftover $1504.82; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRKR` | 24 | $61.90 | $2.06 | — | $4,822.67 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; 🔵; ret5=+11.5; combo leftover $1504.82; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **SHORT** | `BULL` | 303 | $7.95 | $4.04 | — | $7,227.48 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-18.4; combo leftover $2411.34; owner short_news_r_h3 | — |
| 2026-09-17 09:30 ET | **SHORT** | `LEN` | 29 | $81.00 | $2.17 | — | $9,574.31 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-3.0; combo leftover $2411.34; owner short_news_r_h3 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,574.31 | ▲ close $12,443.74 vs 09:30 $12,134.36 (session +345.81) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,574.31 | ▲ 09:30 equity $12,645.43 vs yday $12,443.74 (+201.69) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 32 | $39.50 | $2.11 | $+199.33 | $10,836.20 | ▲ +199.33 after sell → book $12,643.32; vs 09:30 mark -2.11 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ILMN` | 6 | $249.13 | $2.03 | $+87.64 | $12,328.95 | ▲ +87.64 after sell → book $12,641.29; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 10 | $146.50 | $2.04 | $-15.16 | $13,791.91 | ▼ -15.16 after sell → book $12,639.25; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 198 | $7.98 | $2.63 | $+72.01 | $15,369.32 | ▲ +72.01 after sell → book $12,636.62; vs 09:30 mark -2.63 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ARQT` | 57 | $26.14 | $2.18 | $+6.49 | $16,857.12 | ▲ +6.49 after sell → book $12,634.44; vs 09:30 mark -2.18 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 8 | $182.33 | $2.04 | $+87.79 | $18,313.72 | ▲ +87.79 after sell → book $12,632.40; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `CIFR` | 83 | $17.80 | $2.26 | $-24.01 | $19,788.86 | ▼ -24.01 after sell → book $12,630.14; vs 09:30 mark -2.26 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BRKR` | 24 | $63.37 | $2.08 | $+31.13 | $21,307.65 | ▲ +31.13 after sell → book $12,628.05; vs 09:30 mark -2.09 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 8 | $219.62 | $2.01 | — | $19,548.68 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; combo leftover $1864.42; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 21 | $85.00 | $2.05 | — | $17,761.63 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=+18.3; combo leftover $1864.42; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 472 | $3.95 | $6.09 | — | $15,891.14 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; combo leftover $1864.42; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 132 | $14.07 | $2.39 | — | $14,031.51 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; combo leftover $1864.42; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 126 | $14.79 | $2.37 | — | $12,165.60 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; combo leftover $1864.42; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 63 | $29.32 | $2.18 | — | $10,316.27 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; combo leftover $1864.42; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 614 | $3.04 | $7.92 | — | $8,444.85 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; combo leftover $1864.42; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `VITL` | 163 | $11.38 | $2.48 | — | $6,587.44 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+19.5; combo leftover $1864.42; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **SHORT** | `FIVN` | 182 | $34.44 | $2.79 | — | $12,852.73 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $6300.28; owner short_news_r_h3 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,852.73 | ▲ close $13,131.43 vs 09:30 $12,645.43 (session +533.65) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,852.73 | ▲ 09:30 equity $13,282.57 vs yday $13,131.43 (+151.14) | — | — |
| 2026-09-21 09:30 ET | **COVER** | `BBNX` | 102 | $22.11 | $2.30 | $-361.68 | $10,595.21 | ▼ -361.68 after sell → book $13,280.27; vs 09:30 mark -2.30 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `GFR` | 280 | $6.55 | $3.61 | $+71.06 | $8,757.60 | ▲ +71.06 after sell → book $13,276.66; vs 09:30 mark -3.61 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `VICR` | 8 | $230.25 | $2.04 | $+80.99 | $10,597.56 | ▲ +80.99 after sell → book $13,274.62; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ECO` | 21 | $82.83 | $2.08 | $-49.70 | $12,334.91 | ▼ -49.70 after sell → book $13,272.54; vs 09:30 mark -2.08 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `EYPT` | 472 | $3.87 | $6.18 | $-50.03 | $14,155.37 | ▼ -50.03 after sell → book $13,266.36; vs 09:30 mark -6.18 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 132 | $13.90 | $2.42 | $-27.25 | $15,987.75 | ▼ -27.25 after sell → book $13,263.94; vs 09:30 mark -2.42 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 126 | $14.58 | $2.40 | $-31.23 | $17,822.43 | ▼ -31.23 after sell → book $13,261.54; vs 09:30 mark -2.40 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 63 | $29.43 | $2.20 | $+2.55 | $19,674.31 | ▲ +2.55 after sell → book $13,259.33; vs 09:30 mark -2.21 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 614 | $4.00 | $8.04 | $+576.55 | $22,122.27 | ▲ +576.55 after sell → book $13,251.29; vs 09:30 mark -8.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VITL` | 163 | $12.05 | $2.52 | $+104.21 | $24,083.90 | ▲ +104.21 after sell → book $13,248.77; vs 09:30 mark -2.52 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 13 | $157.87 | $2.03 | — | $22,029.56 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; ret5=+6.5; combo leftover $2107.34; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 23 | $88.83 | $2.06 | — | $19,984.41 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; ret5=+7.6; combo leftover $2107.34; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 226 | $9.31 | $2.92 | — | $17,877.43 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=+5.3; combo leftover $2107.34; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 156 | $13.47 | $2.46 | — | $15,772.88 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; combo leftover $2107.34; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 210 | $9.99 | $2.71 | — | $13,672.27 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+5.3; combo leftover $2107.34; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 124 | $16.91 | $2.36 | — | $11,573.07 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+50.5; combo leftover $2107.34; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `USDE` | 161 | $13.05 | $2.47 | — | $9,469.54 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+40.4; combo leftover $2107.34; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `GEMI` | 366 | $5.75 | $4.72 | — | $7,358.49 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+30.3; combo leftover $2107.34; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 400 | $8.26 | $5.34 | — | $10,657.15 | — | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $3306.76; owner short_news_r_h3 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AMD` | 5 | $583.88 | $2.12 | — | $13,574.43 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+8.5; combo leftover $3306.76; owner short_news_r_h3 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,574.43 | ▼ close $12,997.21 vs 09:30 $13,282.57 (session -222.37) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,574.43 | ▲ 09:30 equity $13,106.26 vs yday $12,997.21 (+109.05) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `BULL` | 303 | $8.28 | $3.91 | $-106.43 | $11,063.20 | ▼ -106.43 after sell → book $13,102.36; vs 09:30 mark -3.90 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SELL** | `SBET` | 210 | $9.91 | $2.76 | $-22.27 | $13,141.53 | ▼ -22.27 after sell → book $13,099.59; vs 09:30 mark -2.77 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `USDE` | 161 | $12.99 | $2.52 | $-14.65 | $15,230.41 | ▼ -14.65 after sell → book $13,097.08; vs 09:30 mark -2.51 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `GEMI` | 366 | $6.05 | $4.80 | $+100.28 | $17,441.74 | ▲ +100.28 after sell → book $13,092.28; vs 09:30 mark -4.80 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `ARM` | 6 | $319.41 | $2.01 | — | $15,523.27 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+35.1; combo leftover $2034.87; owner union_join_vol_green_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `FSLY` | 72 | $28.02 | $2.21 | — | $13,503.62 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; ret5=+10.3; combo leftover $2034.87; owner union_join_vol_green_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `META` | 2 | $731.40 | $2.00 | — | $12,038.83 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; ret5=+11.4; combo leftover $2034.87; owner union_join_vol_green_h1 | — |
| 2026-09-22 09:30 ET | **SHORT** | `USFD` | 34 | $93.97 | $2.22 | — | $15,231.59 | — | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $3271.52; owner short_news_r_h3 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,231.59 | ▼ close $12,952.67 vs 09:30 $13,106.26 (session -131.18) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,231.59 | ▼ 09:30 equity $12,244.72 vs yday $12,952.67 (-707.95) | — | — |
| 2026-09-23 09:30 ET | **COVER** | `LEN` | 29 | $82.00 | $2.08 | $-33.25 | $12,851.52 | ▼ -33.25 after sell → book $12,242.65; vs 09:30 mark -2.07 | short_news_r_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BKKT` | 226 | $9.50 | $2.97 | $+37.05 | $14,995.55 | ▲ +37.05 after sell → book $12,239.68; vs 09:30 mark -2.97 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BTDR` | 156 | $12.84 | $2.50 | $-104.02 | $16,996.09 | ▼ -104.02 after sell → book $12,237.18; vs 09:30 mark -2.50 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 124 | $16.92 | $2.40 | $-3.52 | $19,091.77 | ▼ -3.52 after sell → book $12,234.78; vs 09:30 mark -2.40 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `ARM` | 6 | $331.78 | $2.03 | $+70.18 | $21,080.41 | ▲ +70.18 after sell → book $12,232.74; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `FSLY` | 72 | $25.90 | $2.23 | $-157.08 | $22,942.98 | ▼ -157.08 after sell → book $12,230.51; vs 09:30 mark -2.23 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `META` | 2 | $747.60 | $2.02 | $+28.39 | $24,436.16 | ▲ +28.39 after sell → book $12,228.49; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 138 | $20.65 | $2.40 | — | $21,584.06 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; combo leftover $2850.89; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 181 | $15.72 | $2.53 | — | $18,736.21 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; combo leftover $2850.89; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `TNGX` | 112 | $25.40 | $2.33 | — | $15,889.08 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; combo leftover $2850.89; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `NMRA` | 3712 | $0.77 | $39.64 | — | $12,998.62 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; combo leftover $2850.89; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 68 | $41.76 | $2.19 | — | $10,156.74 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+36.4; combo leftover $2850.89; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `BFLY` | 287 | $9.90 | $3.70 | — | $7,311.74 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; combo leftover $2850.89; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **SHORT** | `HALO` | 52 | $116.85 | $2.37 | — | $13,385.57 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $6087.84; owner short_news_r_h3 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,385.57 | ▼ close $11,711.16 vs 09:30 $12,244.72 (session -462.15) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,385.57 | ▼ 09:30 equity $11,340.41 vs yday $11,711.16 (-370.75) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 13 | $163.95 | $2.06 | $+74.95 | $15,514.87 | ▲ +74.95 after sell → book $11,338.35; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 23 | $87.67 | $2.09 | $-30.71 | $17,529.31 | ▼ -30.71 after sell → book $11,336.27; vs 09:30 mark -2.08 | union_join_vol_green_h1: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `AMD` | 5 | $600.27 | $2.00 | $-86.07 | $14,525.95 | ▼ -86.07 after sell → book $11,334.26; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 138 | $20.52 | $2.45 | $-22.79 | $17,355.26 | ▼ -22.79 after sell → book $11,331.81; vs 09:30 mark -2.45 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 181 | $14.38 | $2.58 | $-247.66 | $19,955.46 | ▼ -247.66 after sell → book $11,329.23; vs 09:30 mark -2.58 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `TNGX` | 112 | $23.99 | $2.37 | $-162.61 | $22,639.97 | ▼ -162.61 after sell → book $11,326.86; vs 09:30 mark -2.37 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `NMRA` | 3712 | $0.75 | $39.47 | $-160.77 | $25,369.66 | ▼ -160.77 after sell → book $11,287.40; vs 09:30 mark -39.46 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 68 | $36.02 | $2.22 | $-394.40 | $27,817.13 | ▼ -394.40 after sell → book $11,285.17; vs 09:30 mark -2.23 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BFLY` | 287 | $9.12 | $3.77 | $-231.33 | $30,430.80 | ▼ -231.33 after sell → book $11,281.40; vs 09:30 mark -3.77 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30,430.80 | ▼ close $10,993.36 vs 09:30 $11,340.41 (session -288.04) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,826.68 | ▼ 09:30 equity $8,743.79 vs yday $8,774.67 (-30.88) | 09:30 open · cash $20,826.68 (unchanged overnight, no fees) · equity $8,743.79 vs prior close $8,774.67 (-30.88) | — |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 69 | $26.27 | $2.20 | — | $19,011.85 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; combo leftover $1822.33; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 472 | $3.86 | $6.09 | — | $17,183.84 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; combo leftover $1822.33; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TWST` | 9 | $184.00 | $2.02 | — | $15,525.83 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.3; combo leftover $1822.33; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 112 | $16.21 | $2.33 | — | $13,707.98 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; combo leftover $1822.33; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GRAL` | 14 | $123.50 | $2.03 | — | $11,976.95 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+56.6; combo leftover $1822.33; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `QMCO` | 61 | $29.80 | $2.17 | — | $10,156.98 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+18.2; combo leftover $1822.33; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CYPH` | 455 | $4.00 | $5.87 | — | $8,328.83 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+32.9; combo leftover $1822.33; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CDNA` | 29 | $61.33 | $2.08 | — | $6,548.18 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+13.1; combo leftover $1822.33; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `RSKD` | 555 | $7.85 | $7.41 | — | $10,897.53 | — | news🔴; gate news=bad; list yday_gainer; 🔵; ⚪; ret5=+25.4; combo leftover $4359.50; owner short_news_r_h3 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,897.53 | ▲ close $8,919.62 vs 09:30 $8,743.79 (session +208.02) | 16:00 close · cash $10,897.53 · equity $8,919.62 vs 09:30 $8,743.79 (+175.83; session marks +208.02) · 14 name(s) marked open→close (per-name table). AEHL×312 09:30 $9.05 → close $9.36 -96.72; BAND×40 09:30 $61.83 → close $61.83 -0.00; HALO×20 09:30 $115.36 → close $113.90 +29.20; PAYX×21 09:30 $101.59 → close $101.59 +0.00; USFD×25 09:30 $93.82 → close $93.82 +0.00; WRBY×69 09:30 $26.27 → close $26.71 +30.36; ZSQR×472 09:30 $3.86 → close $3.78 -37.76; TWST×9 09:30 $184.00 → close $182.83 -10.53; SECZ×112 09:30 $16.21 → close $15.96 -28.00; GRAL×14 09:30 $123.50 → close $126.89 +47.46; QMCO×61 09:30 $29.80 → close $31.68 +114.68; CYPH×455 09:30 $4.00 → close $4.12 +52.33; CDNA×29 09:30 $61.33 → close $63.68 +68.15; RSKD×555 09:30 $7.85 → close $7.78 +38.85 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `EU` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `LUNR` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `OWL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `EU` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `LUNR` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `OWL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `VERI` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `ZNTL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `APMD` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `HIVE` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new short short_news_r_h3 |
| 2026-08-18 | `CADL` | hard_red | hard-red S=-6.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `VERI` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `ZNTL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `APMD` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `HIVE` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `RNW` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `AAP` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-21 | `AEM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `WYFI` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `TOYO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `TEAM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `AAP` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `WMT` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `AQST` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `AEM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `WYFI` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `TOYO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `TEAM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AAP` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `WMT` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AQST` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `QTRX` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `MRNA` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `AUGO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `SSRM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `ARIS` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `EU` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `BMO` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-25 | `QTRX` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `MRNA` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `AUGO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `ARIS` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `NOG` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-26 | `AVAH` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `ARE` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `BMO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `INTU` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `AVAH` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `ARE` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `BMO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `BE` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `NEM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `CRM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `BE` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `AQST` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `NEM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `CRM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `MT` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `TX` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `MT` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `TX` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `SIMO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `FIG` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new long union_join_vol_green_h1 |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new long union_join_vol_green_h1 |
| 2026-09-01 | `SIMO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `FIG` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new short short_news_r_h3 |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new short short_news_r_h3 |
| 2026-09-04 | `SLN` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `SLN` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `OPK` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `PIPR` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `GEMI` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `CRCL` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `TRMD` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-09 | `GSM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `PIPR` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `NVS` | hard_red | hard-red S=-13.95 sit; no new short short_news_r_h3 |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new short short_news_r_h3 |
| 2026-09-10 | `BKV` | hard_red | hard-red S=-13.28 sit; no new short short_news_r_h3 |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `AMBA` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `CMPS` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `ARBE` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `QRVO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `RWT` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `CRDL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `HPQ` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `INSP` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `GME` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `DHT` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `AVT` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `QRVO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `RWT` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `CRDL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `MYGN` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `TYRA` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `S` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `IOT` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `SAFX` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `ECO` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-17 | `GFR` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `BBNX` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `GFR` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `BULL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `LEN` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-21 | `BULL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `LEN` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `FIVN` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-22 | `LEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `A` | no_price | no 09:30 open — carry |
| 2026-09-22 | `DXCM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BKKT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BTDR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TJGC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AMD` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-22 | `GRPN` | no_price | no 09:30 open |
| 2026-09-22 | `XXI` | no_price | no 09:30 open |
| 2026-09-22 | `THO` | no_price | no 09:30 open |
| 2026-09-22 | `PAYX` | no_price | no 09:30 open |
| 2026-09-23 | `AMD` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-23 | `USFD` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `USFD` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-24 | `HALO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `PANW` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `VOYG` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `BMEA` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `TLSA` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `GLND` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `FSLY` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `KVYO` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `VICR` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `FIVN` | 182 | 2026-09-18 @ $34.44 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $6300.28; owner short_news_r_h3 |
| `AEHL` | 400 | 2026-09-21 @ $8.26 | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $3306.76; owner short_news_r_h3 |
| `USFD` | 34 | 2026-09-22 @ $93.97 | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $3271.52; owner short_news_r_h3 |
| `HALO` | 52 | 2026-09-23 @ $116.85 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $6087.84; owner short_news_r_h3 |
