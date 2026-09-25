# Factor mine action — `combo_sj_7030_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared short_news_r_h3/union_join_vol_green_h1 w=0.7,0.3 net=priority

Cash book **-10.67%** ($8,933) · signal-only (no cash/fees) was —. Starts YES **7/30**. Fills 323 · skips 173 · realized $+799.96.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: short_news_r_h3 70%, union_join_vol_green_h1 30%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: short_news_r_h3 70%, union_join_vol_green_h1 30%.
- Member: short_news_r_h3 (70% · short · hold 3).
- Member: union_join_vol_green_h1 (30% · long · hold 1).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $18,988.88.

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
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 250 | $1.50 | $3.23 | — | $9,621.77 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=+9.2; combo leftover $375.00; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 25 | $14.80 | $2.06 | — | $9,249.71 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=-9.9; combo leftover $375.00; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 87 | $4.31 | $2.25 | — | $8,872.49 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; combo leftover $375.00; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 89 | $4.18 | $2.26 | — | $8,498.21 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; combo leftover $375.00; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 22 | $16.50 | $2.06 | — | $8,133.16 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; combo leftover $375.00; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 33 | $11.12 | $2.09 | — | $7,764.11 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; combo leftover $375.00; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 139 | $2.69 | $2.41 | — | $7,387.79 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; combo leftover $375.00; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 51 | $7.29 | $2.14 | — | $7,013.86 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; combo leftover $375.00; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 1409 | $1.18 | $18.47 | — | $8,658.01 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $1663.58; owner short_news_r_h3 | — |
| 2026-08-14 09:30 ET | **SHORT** | `LUNR` | 86 | $19.17 | $2.32 | — | $10,304.31 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+17.6; combo leftover $1663.58; owner short_news_r_h3 | — |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 131 | $12.70 | $2.46 | — | $11,964.89 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.6; combo leftover $1663.58; owner short_news_r_h3 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,964.89 | ▼ close $9,944.01 vs 09:30 $10,000.00 (session -14.25) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,964.89 | ▼ 09:30 equity $9,837.94 vs yday $9,944.01 (-106.07) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 250 | $1.52 | $3.28 | $-1.50 | $12,341.61 | ▼ -1.50 after sell → book $9,834.66; vs 09:30 mark -3.28 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 25 | $13.67 | $2.08 | $-32.40 | $12,681.28 | ▼ -32.40 after sell → book $9,832.58; vs 09:30 mark -2.08 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 87 | $4.60 | $2.28 | $+20.70 | $13,079.20 | ▲ +20.70 after sell → book $9,830.30; vs 09:30 mark -2.28 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 89 | $4.10 | $2.28 | $-11.66 | $13,441.82 | ▼ -11.66 after sell → book $9,828.02; vs 09:30 mark -2.28 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 22 | $15.73 | $2.08 | $-21.07 | $13,785.80 | ▼ -21.07 after sell → book $9,825.94; vs 09:30 mark -2.08 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 33 | $9.57 | $2.11 | $-55.35 | $14,099.51 | ▼ -55.35 after sell → book $9,823.84; vs 09:30 mark -2.10 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NCMI` | 139 | $2.80 | $2.44 | $+10.44 | $14,486.27 | ▲ +10.44 after sell → book $9,821.40; vs 09:30 mark -2.44 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMLS` | 51 | $7.24 | $2.16 | $-6.86 | $14,853.34 | ▼ -6.86 after sell → book $9,819.23; vs 09:30 mark -2.17 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 97 | $9.12 | $2.28 | — | $13,966.42 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; combo leftover $891.20; owner union_join_vol_green_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 60 | $14.66 | $2.17 | — | $13,084.65 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; combo leftover $891.20; owner union_join_vol_green_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `BORR` | 194 | $4.59 | $2.57 | — | $12,191.62 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ⚪; ret5=+14.8; combo leftover $891.20; owner union_join_vol_green_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 212 | $4.19 | $2.73 | — | $11,300.60 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ⚪; ret5=+291.8; combo leftover $891.20; owner union_join_vol_green_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `MP` | 15 | $58.01 | $2.04 | — | $10,428.42 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+14.9; combo leftover $891.20; owner union_join_vol_green_h1 | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 852 | $1.15 | $11.17 | — | $11,397.05 | — | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; combo leftover $980.74; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 275 | $3.56 | $3.63 | — | $12,372.42 | — | news🔴; gate news=bad; list yday_mover; ret5=-15.6; combo leftover $980.74; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `APMD` | 30 | $31.70 | $2.13 | — | $13,321.29 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+17.6; combo leftover $980.74; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 325 | $3.01 | $4.28 | — | $14,295.26 | — | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; combo leftover $980.74; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `RNW` | 144 | $6.80 | $2.48 | — | $15,271.98 | — | news🔴; gate news=bad; list overnight; ⚪; ret5=+10.4; combo leftover $980.74; owner short_news_r_h3 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,271.98 | ▲ close $9,795.55 vs 09:30 $9,837.94 (session +11.80) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,271.98 | ▲ 09:30 equity $9,883.02 vs yday $9,795.55 (+87.47) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 97 | $9.03 | $2.31 | $-13.32 | $16,145.58 | ▼ -13.32 after sell → book $9,880.71; vs 09:30 mark -2.31 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 60 | $13.19 | $2.19 | $-92.56 | $16,934.79 | ▼ -92.56 after sell → book $9,878.52; vs 09:30 mark -2.19 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `BORR` | 194 | $4.56 | $2.61 | $-11.01 | $17,816.82 | ▼ -11.01 after sell → book $9,875.91; vs 09:30 mark -2.61 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 212 | $3.94 | $2.78 | $-58.51 | $18,649.32 | ▼ -58.51 after sell → book $9,873.13; vs 09:30 mark -2.78 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `MP` | 15 | $56.35 | $2.06 | $-28.99 | $19,492.51 | ▼ -28.99 after sell → book $9,871.07; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,492.51 | ▲ close $10,106.31 vs 09:30 $9,883.02 (session +235.24) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,492.51 | ▼ 09:30 equity $10,074.99 vs yday $10,106.31 (-31.32) | — | — |
| 2026-08-19 09:30 ET | **COVER** | `EU` | 1409 | $1.07 | $18.18 | $+118.35 | $17,966.71 | ▲ +118.35 after sell → book $10,056.82; vs 09:30 mark -18.17 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 131 | $11.75 | $2.38 | $+118.95 | $16,425.07 | ▲ +118.95 after sell → book $10,054.43; vs 09:30 mark -2.39 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,425.07 | ▲ close $10,102.22 vs 09:30 $10,074.99 (session +47.79) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,425.07 | ▼ 09:30 equity $10,045.80 vs yday $10,102.22 (-56.42) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `LUNR` | 86 | $18.13 | $2.25 | $+84.87 | $14,863.64 | ▲ +84.87 after sell → book $10,043.55; vs 09:30 mark -2.25 | short_news_r_h3: dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 852 | $0.96 | $10.76 | $+137.39 | $14,032.41 | ▲ +137.39 after sell → book $10,032.79; vs 09:30 mark -10.76 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ZNTL` | 275 | $4.01 | $3.55 | $-132.30 | $12,924.74 | ▼ -132.30 after sell → book $10,029.25; vs 09:30 mark -3.54 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `APMD` | 30 | $31.87 | $2.08 | $-9.31 | $11,966.56 | ▼ -9.31 after sell → book $10,027.17; vs 09:30 mark -2.08 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 325 | $2.95 | $4.19 | $+11.02 | $11,003.61 | ▲ +11.02 after sell → book $10,022.97; vs 09:30 mark -4.20 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `RNW` | 144 | $6.81 | $2.42 | $-6.34 | $10,020.55 | ▼ -6.34 after sell → book $10,020.55; vs 09:30 mark -2.42 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 18 | $20.55 | $2.04 | — | $9,648.61 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; combo leftover $375.77; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 18 | $20.65 | $2.04 | — | $9,274.86 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; combo leftover $375.77; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 65 | $5.77 | $2.19 | — | $8,897.63 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; combo leftover $375.77; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 19 | $19.63 | $2.05 | — | $8,522.61 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; combo leftover $375.77; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 12 | $29.63 | $2.03 | — | $8,165.02 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; combo leftover $375.77; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 214 | $1.75 | $2.76 | — | $7,787.76 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; combo leftover $375.77; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 2 | $144.54 | $2.00 | — | $7,496.69 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; combo leftover $375.77; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 76 | $4.92 | $2.22 | — | $7,120.55 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $375.77; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 3 | $204.45 | $2.04 | — | $7,731.86 | — | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; combo leftover $625.20; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 29 | $21.40 | $2.11 | — | $8,350.35 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-25.2; combo leftover $625.20; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 141 | $4.43 | $2.46 | — | $8,972.52 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-23.1; combo leftover $625.20; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 52 | $11.81 | $2.18 | — | $9,584.71 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $625.20; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 3 | $173.90 | $2.03 | — | $10,104.38 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; combo leftover $625.20; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 13 | $46.85 | $2.07 | — | $10,711.37 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=+5.0; combo leftover $625.20; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 5 | $106.38 | $2.04 | — | $11,241.23 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; combo leftover $625.20; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 135 | $4.61 | $2.44 | — | $11,861.13 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $625.20; owner short_news_r_h3 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,861.13 | ▲ close $10,098.92 vs 09:30 $10,045.80 (session +113.08) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,861.13 | ▲ 09:30 equity $10,150.89 vs yday $10,098.92 (+51.97) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 18 | $21.90 | $2.06 | $+20.19 | $12,253.27 | ▲ +20.19 after sell → book $10,148.83; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 18 | $21.75 | $2.06 | $+15.69 | $12,642.70 | ▲ +15.69 after sell → book $10,146.76; vs 09:30 mark -2.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 65 | $5.67 | $2.21 | $-10.89 | $13,009.05 | ▼ -10.89 after sell → book $10,144.56; vs 09:30 mark -2.20 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 19 | $21.17 | $2.07 | $+25.15 | $13,409.21 | ▲ +25.15 after sell → book $10,142.49; vs 09:30 mark -2.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 12 | $32.17 | $2.05 | $+26.41 | $13,793.20 | ▲ +26.41 after sell → book $10,140.44; vs 09:30 mark -2.05 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 214 | $1.79 | $2.81 | $+2.99 | $14,173.46 | ▲ +2.99 after sell → book $10,137.64; vs 09:30 mark -2.80 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 2 | $154.70 | $2.02 | $+16.31 | $14,480.84 | ▲ +16.31 after sell → book $10,135.62; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 76 | $5.20 | $2.24 | $+16.82 | $14,873.80 | ▲ +16.82 after sell → book $10,133.38; vs 09:30 mark -2.24 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 5 | $119.43 | $2.00 | — | $14,274.65 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; combo leftover $637.45; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 37 | $17.20 | $2.10 | — | $13,636.15 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; combo leftover $637.45; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 57 | $11.13 | $2.16 | — | $12,999.57 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; combo leftover $637.45; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 482 | $1.32 | $6.22 | — | $12,357.12 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; combo leftover $637.45; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 384 | $1.66 | $4.95 | — | $11,714.72 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; combo leftover $637.45; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `INDP` | 458 | $1.39 | $5.91 | — | $11,072.20 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+30.2; combo leftover $637.45; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 76 | $8.28 | $2.22 | — | $10,440.70 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; combo leftover $637.45; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 270 | $3.11 | $3.56 | — | $11,276.84 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; combo leftover $842.32; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `MRNA` | 6 | $133.11 | $2.05 | — | $12,073.45 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=+109.5; combo leftover $842.32; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 9 | $89.10 | $2.06 | — | $12,873.29 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $842.32; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 21 | $38.40 | $2.10 | — | $13,677.59 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $842.32; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 40 | $20.90 | $2.15 | — | $14,511.44 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $842.32; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 31 | $27.00 | $2.13 | — | $15,346.31 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; combo leftover $842.32; owner short_news_r_h3 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,346.31 | ▲ close $10,182.11 vs 09:30 $10,150.89 (session +88.34) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,346.31 | ▲ 09:30 equity $10,407.94 vs yday $10,182.11 (+225.83) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 5 | $120.51 | $2.02 | $+1.37 | $15,946.84 | ▲ +1.37 after sell → book $10,405.92; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 37 | $16.57 | $2.12 | $-27.53 | $16,557.81 | ▼ -27.53 after sell → book $10,403.80; vs 09:30 mark -2.12 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 57 | $13.33 | $2.18 | $+121.06 | $17,315.44 | ▲ +121.06 after sell → book $10,401.62; vs 09:30 mark -2.18 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 482 | $1.83 | $6.31 | $+233.29 | $18,191.19 | ▲ +233.29 after sell → book $10,395.31; vs 09:30 mark -6.31 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 384 | $1.55 | $5.03 | $-52.22 | $18,781.36 | ▼ -52.22 after sell → book $10,390.28; vs 09:30 mark -5.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `INDP` | 458 | $1.24 | $5.99 | $-80.60 | $19,343.29 | ▼ -80.60 after sell → book $10,384.29; vs 09:30 mark -5.99 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 76 | $8.59 | $2.24 | $+19.10 | $19,993.89 | ▲ +19.10 after sell → book $10,382.05; vs 09:30 mark -2.24 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,993.89 | ▲ close $10,455.75 vs 09:30 $10,407.94 (session +73.70) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,993.89 | ▲ 09:30 equity $10,497.98 vs yday $10,455.75 (+42.23) | — | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 3 | $212.00 | $2.00 | $-26.68 | $19,355.89 | ▼ -26.68 after sell → book $10,495.98; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 29 | $20.90 | $2.08 | $+10.31 | $18,747.71 | ▲ +10.31 after sell → book $10,493.90; vs 09:30 mark -2.08 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TOYO` | 141 | $4.42 | $2.41 | $-3.47 | $18,122.08 | ▼ -3.47 after sell → book $10,491.49; vs 09:30 mark -2.41 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ABCL` | 52 | $11.00 | $2.15 | $+38.05 | $17,547.93 | ▲ +38.05 after sell → book $10,489.34; vs 09:30 mark -2.15 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 3 | $170.64 | $2.00 | $+5.75 | $17,034.01 | ▲ +5.75 after sell → book $10,487.34; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AAP` | 13 | $43.63 | $2.03 | $+37.77 | $16,464.79 | ▲ +37.77 after sell → book $10,485.31; vs 09:30 mark -2.03 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 5 | $105.58 | $2.00 | $-0.04 | $15,934.89 | ▼ -0.04 after sell → book $10,483.31; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 135 | $4.77 | $2.40 | $-26.44 | $15,288.54 | ▼ -26.44 after sell → book $10,480.91; vs 09:30 mark -2.40 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 351 | $1.63 | $4.53 | — | $14,711.89 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; combo leftover $573.32; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 161 | $3.55 | $2.47 | — | $14,137.86 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+27.9; combo leftover $573.32; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 90 | $6.37 | $2.26 | — | $13,562.30 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+10.9; combo leftover $573.32; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 16 | $35.05 | $2.04 | — | $12,999.46 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+19.7; combo leftover $573.32; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ETON` | 8 | $64.55 | $2.01 | — | $12,481.05 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+4.4; combo leftover $573.32; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `WPM` | 3 | $156.51 | $2.00 | — | $12,009.52 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+17.4; combo leftover $573.32; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUZ` | 63 | $8.98 | $2.18 | — | $11,441.60 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+15.4; combo leftover $573.32; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `IAUX` | 301 | $1.90 | $3.88 | — | $10,865.82 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+16.4; combo leftover $573.32; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 95 | $13.62 | $2.34 | — | $12,157.86 | — | news🔴; gate news=bad; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; combo leftover $1307.44; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 23 | $54.51 | $2.11 | — | $13,409.47 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+15.1; combo leftover $1307.44; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `BMO` | 7 | $175.01 | $2.06 | — | $14,632.48 | — | news🔴; gate news=bad; list earn_react; ret5=-7.0; combo leftover $1307.44; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `INTU` | 3 | $364.35 | $2.05 | — | $15,723.48 | — | news🔴; gate news=bad; list overnight,overnight_mega; 🔵; ret5=+10.2; combo leftover $1307.44; owner short_news_r_h3 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,723.48 | ▼ close $10,424.14 vs 09:30 $10,497.98 (session -26.83) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,723.48 | ▲ 09:30 equity $10,583.23 vs yday $10,424.14 (+159.09) | — | — |
| 2026-08-26 09:30 ET | **COVER** | `QTRX` | 270 | $2.83 | $3.48 | $+68.56 | $14,955.90 | ▲ +68.56 after sell → book $10,579.74; vs 09:30 mark -3.49 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `MRNA` | 6 | $154.20 | $2.01 | $-130.60 | $14,028.69 | ▼ -130.60 after sell → book $10,577.74; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 9 | $88.24 | $2.02 | $+3.66 | $13,232.51 | ▲ +3.66 after sell → book $10,575.72; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 21 | $38.41 | $2.05 | $-4.36 | $12,423.85 | ▼ -4.36 after sell → book $10,573.67; vs 09:30 mark -2.05 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 40 | $20.50 | $2.11 | $+11.74 | $11,601.74 | ▲ +11.74 after sell → book $10,571.56; vs 09:30 mark -2.11 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 31 | $26.00 | $2.08 | $+26.79 | $10,793.66 | ▲ +26.79 after sell → book $10,569.47; vs 09:30 mark -2.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 351 | $1.75 | $4.60 | $+34.75 | $11,405.07 | ▲ +34.75 after sell → book $10,564.88; vs 09:30 mark -4.59 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GORO` | 161 | $3.77 | $2.51 | $+30.44 | $12,009.53 | ▲ +30.44 after sell → book $10,562.37; vs 09:30 mark -2.51 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZURA` | 90 | $6.13 | $2.28 | $-26.14 | $12,558.94 | ▼ -26.14 after sell → book $10,560.08; vs 09:30 mark -2.29 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 16 | $35.70 | $2.06 | $+6.30 | $13,128.08 | ▲ +6.30 after sell → book $10,558.02; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ETON` | 8 | $63.60 | $2.03 | $-11.65 | $13,634.85 | ▼ -11.65 after sell → book $10,555.99; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `WPM` | 3 | $160.93 | $2.02 | $+9.24 | $14,115.62 | ▲ +9.24 after sell → book $10,553.97; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SUZ` | 63 | $9.03 | $2.20 | $-1.23 | $14,682.31 | ▼ -1.23 after sell → book $10,551.77; vs 09:30 mark -2.20 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `IAUX` | 301 | $1.87 | $3.94 | $-16.86 | $15,241.24 | ▼ -16.86 after sell → book $10,547.83; vs 09:30 mark -3.94 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 786 | $5.81 | $10.14 | — | $10,664.44 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ⚪; ret5=+117.2; combo leftover $4572.37; owner union_join_vol_green_h1 | — |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 4 | $213.94 | $2.05 | — | $11,518.15 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; combo leftover $1053.77; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 86 | $12.22 | $2.30 | — | $12,566.77 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.4; combo leftover $1053.77; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 207 | $5.08 | $2.74 | — | $13,615.59 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+17.6; combo leftover $1053.77; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 7 | $132.64 | $2.06 | — | $14,542.01 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+16.5; combo leftover $1053.77; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `CRM` | 5 | $199.94 | $2.05 | — | $15,539.66 | — | news🔴; gate news=bad; list overnight,overnight_mega; ret5=+2.1; combo leftover $1053.77; owner short_news_r_h3 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,539.66 | ▼ close $10,489.28 vs 09:30 $10,583.23 (session -37.21) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,539.66 | ▲ 09:30 equity $10,732.82 vs yday $10,489.28 (+243.54) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 786 | $6.50 | $10.31 | $+521.89 | $20,638.35 | ▲ +521.89 after sell → book $10,722.51; vs 09:30 mark -10.31 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `DKS` | 48 | $128.73 | $2.13 | — | $14,457.18 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-32.2; combo leftover $6191.50; owner union_join_vol_green_h1 | — |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 35 | $74.54 | $2.20 | — | $17,063.88 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=-0.1; combo leftover $2680.09; owner short_news_r_h3 | — |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 48 | $55.25 | $2.24 | — | $19,713.64 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=+2.1; combo leftover $2680.09; owner short_news_r_h3 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,713.64 | ▲ close $10,790.34 vs 09:30 $10,732.82 (session +74.40) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,713.64 | ▲ 09:30 equity $10,824.66 vs yday $10,790.34 (+34.32) | — | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 95 | $13.90 | $2.27 | $-30.74 | $18,390.86 | ▼ -30.74 after sell → book $10,822.38; vs 09:30 mark -2.28 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 23 | $52.49 | $2.06 | $+42.29 | $17,181.54 | ▲ +42.29 after sell → book $10,820.33; vs 09:30 mark -2.05 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `BMO` | 7 | $172.76 | $2.01 | $+11.67 | $15,970.20 | ▲ +11.67 after sell → book $10,818.31; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `INTU` | 3 | $347.82 | $2.00 | $+45.54 | $14,924.75 | ▲ +45.54 after sell → book $10,816.32; vs 09:30 mark -1.99 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 48 | $132.80 | $2.19 | $+191.03 | $21,296.95 | ▲ +191.03 after sell → book $10,814.12; vs 09:30 mark -2.20 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 10 | $146.07 | $2.02 | — | $19,834.23 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+38.8; combo leftover $1597.27; owner union_join_vol_green_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 68 | $23.30 | $2.19 | — | $18,247.64 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+14.5; combo leftover $1597.27; owner union_join_vol_green_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 84 | $19.00 | $2.24 | — | $16,649.39 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; ret5=+7.5; combo leftover $1597.27; owner union_join_vol_green_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 64 | $24.69 | $2.18 | — | $15,067.05 | — | combo gate; gate join=good,vol=good,last_green=True; list earn_react; ret5=+5.8; combo leftover $1597.27; owner union_join_vol_green_h1 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 10 | $252.24 | $2.12 | — | $17,587.33 | — | news🔴; gate news=bad; list probable,yday_gainer; ⚪; ret5=+2.2; combo leftover $2701.37; owner short_news_r_h3 | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 89 | $30.18 | $2.37 | — | $20,270.99 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+12.1; combo leftover $2701.37; owner short_news_r_h3 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,270.99 | ▲ close $11,086.20 vs 09:30 $10,824.66 (session +285.20) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,270.99 | ▲ 09:30 equity $11,090.66 vs yday $11,086.20 (+4.46) | — | — |
| 2026-08-31 09:30 ET | **COVER** | `BE` | 4 | $208.88 | $2.00 | $+16.19 | $19,433.46 | ▲ +16.19 after sell → book $11,088.66; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 86 | $11.10 | $2.25 | $+91.77 | $18,476.62 | ▲ +91.77 after sell → book $11,086.41; vs 09:30 mark -2.25 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 207 | $4.97 | $2.67 | $+16.32 | $17,444.12 | ▲ +16.32 after sell → book $11,083.74; vs 09:30 mark -2.67 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `NEM` | 7 | $127.45 | $2.01 | $+32.26 | $16,549.96 | ▲ +32.26 after sell → book $11,081.73; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `CRM` | 5 | $254.39 | $2.00 | $-276.31 | $15,276.01 | ▼ -276.31 after sell → book $11,079.73; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 10 | $148.03 | $2.04 | $+15.54 | $16,754.26 | ▲ +15.54 after sell → book $11,077.68; vs 09:30 mark -2.05 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NCNO` | 68 | $22.66 | $2.22 | $-47.93 | $18,292.93 | ▼ -47.93 after sell → book $11,075.47; vs 09:30 mark -2.21 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TH` | 84 | $18.12 | $2.27 | $-78.01 | $19,813.16 | ▼ -78.01 after sell → book $11,073.20; vs 09:30 mark -2.27 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GAP` | 64 | $22.98 | $2.20 | $-113.83 | $21,281.67 | ▼ -113.83 after sell → book $11,070.99; vs 09:30 mark -2.21 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,281.67 | ▲ close $11,124.30 vs 09:30 $11,090.66 (session +53.31) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,281.67 | ▲ 09:30 equity $11,281.25 vs yday $11,124.30 (+156.95) | — | — |
| 2026-09-01 09:30 ET | **COVER** | `MT` | 35 | $73.22 | $2.10 | $+41.91 | $18,716.88 | ▲ +41.91 after sell → book $11,279.16; vs 09:30 mark -2.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `TX` | 48 | $54.76 | $2.13 | $+19.15 | $16,086.26 | ▲ +19.15 after sell → book $11,277.02; vs 09:30 mark -2.14 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,086.26 | ▲ close $11,291.96 vs 09:30 $11,281.25 (session +14.94) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,086.26 | ▲ 09:30 equity $11,345.74 vs yday $11,291.96 (+53.78) | — | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 10 | $235.71 | $2.02 | $+161.16 | $13,727.14 | ▲ +161.16 after sell → book $11,343.72; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 89 | $26.78 | $2.26 | $+297.98 | $11,341.47 | ▲ +297.98 after sell → book $11,341.47; vs 09:30 mark -2.25 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,341.47 | ▲ close $11,341.47 vs 09:30 $11,345.74 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,341.47 | ▲ 09:30 equity $11,341.47 vs yday $11,341.47 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 3 | $132.45 | $2.00 | — | $10,942.12 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; combo leftover $425.31; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 25 | $16.77 | $2.06 | — | $10,520.80 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; combo leftover $425.31; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 195 | $2.18 | $2.58 | — | $10,093.13 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; combo leftover $425.31; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 17 | $23.88 | $2.04 | — | $9,685.13 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $425.31; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 40 | $10.42 | $2.11 | — | $9,266.22 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; combo leftover $425.31; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 220 | $1.93 | $2.84 | — | $8,838.78 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; combo leftover $425.31; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `DUOL` | 2 | $161.54 | $2.00 | — | $8,513.70 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+12.0; combo leftover $425.31; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `ALMS` | 40 | $10.38 | $2.11 | — | $8,096.59 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ret5=-56.2; combo leftover $425.31; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 190 | $14.85 | $2.69 | — | $10,915.40 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; combo leftover $2830.93; owner short_news_r_h3 | — |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 1655 | $1.71 | $21.72 | — | $13,723.73 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+11.9; combo leftover $2830.93; owner short_news_r_h3 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,723.73 | ▲ close $11,462.29 vs 09:30 $11,341.47 (session +162.97) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,723.73 | ▲ 09:30 equity $11,521.52 vs yday $11,462.29 (+59.23) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 3 | $130.03 | $2.02 | $-11.28 | $14,111.80 | ▼ -11.28 after sell → book $11,519.50; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 25 | $15.61 | $2.08 | $-33.15 | $14,499.96 | ▼ -33.15 after sell → book $11,517.41; vs 09:30 mark -2.09 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 195 | $2.16 | $2.62 | $-9.09 | $14,918.55 | ▼ -9.09 after sell → book $11,514.80; vs 09:30 mark -2.61 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 17 | $23.84 | $2.06 | $-4.78 | $15,321.77 | ▼ -4.78 after sell → book $11,512.74; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 40 | $10.50 | $2.13 | $-1.04 | $15,739.64 | ▼ -1.04 after sell → book $11,510.61; vs 09:30 mark -2.13 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `BMEA` | 220 | $1.90 | $2.88 | $-12.32 | $16,154.75 | ▼ -12.32 after sell → book $11,507.72; vs 09:30 mark -2.89 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DUOL` | 2 | $157.46 | $2.02 | $-12.17 | $16,467.65 | ▼ -12.17 after sell → book $11,505.70; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ALMS` | 40 | $11.23 | $2.13 | $+29.96 | $16,914.72 | ▲ +29.96 after sell → book $11,503.57; vs 09:30 mark -2.13 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 1 | $513.78 | $1.99 | — | $16,398.95 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; combo leftover $634.30; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 7 | $82.70 | $2.01 | — | $15,818.04 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+15.7; combo leftover $634.30; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 252 | $2.51 | $3.25 | — | $15,182.27 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+21.8; combo leftover $634.30; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `MDB` | 1 | $378.34 | $1.99 | — | $14,801.94 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-12.7; combo leftover $634.30; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 25 | $25.18 | $2.06 | — | $14,170.37 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+16.0; combo leftover $634.30; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 109 | $5.79 | $2.32 | — | $13,536.94 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+15.2; combo leftover $634.30; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `TDS` | 16 | $37.44 | $2.04 | — | $12,935.87 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+14.1; combo leftover $634.30; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `AHCO` | 100 | $6.32 | $2.29 | — | $12,301.58 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+10.7; combo leftover $634.30; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 614 | $4.67 | $8.12 | — | $15,160.83 | — | news🔴; gate news=bad; list yday_gainer; ret5=+11.9; combo leftover $2871.40; owner short_news_r_h3 | — |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 37 | $76.55 | $2.21 | — | $17,990.97 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; combo leftover $2871.40; owner short_news_r_h3 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,990.97 | ▲ close $11,558.08 vs 09:30 $11,521.52 (session +82.80) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,990.97 | ▲ 09:30 equity $11,561.74 vs yday $11,558.08 (+3.66) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 1 | $521.15 | $2.01 | $+3.36 | $18,510.11 | ▲ +3.36 after sell → book $11,559.73; vs 09:30 mark -2.01 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TARS` | 7 | $89.67 | $2.03 | $+44.75 | $19,135.77 | ▲ +44.75 after sell → book $11,557.70; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BRR` | 252 | $2.66 | $3.30 | $+31.25 | $19,802.79 | ▲ +31.25 after sell → book $11,554.40; vs 09:30 mark -3.30 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MDB` | 1 | $360.75 | $2.01 | $-21.60 | $20,161.52 | ▼ -21.60 after sell → book $11,552.38; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 25 | $26.44 | $2.08 | $+27.35 | $20,820.44 | ▲ +27.35 after sell → book $11,550.30; vs 09:30 mark -2.08 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 109 | $5.81 | $2.35 | $-2.48 | $21,451.38 | ▼ -2.48 after sell → book $11,547.95; vs 09:30 mark -2.35 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TDS` | 16 | $37.75 | $2.06 | $+0.86 | $22,053.32 | ▲ +0.86 after sell → book $11,545.89; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `AHCO` | 100 | $6.48 | $2.32 | $+11.39 | $22,699.01 | ▲ +11.39 after sell → book $11,543.58; vs 09:30 mark -2.31 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22,699.01 | ▲ close $11,829.60 vs 09:30 $11,561.74 (session +286.02) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22,699.01 | ▲ 09:30 equity $11,866.95 vs yday $11,829.60 (+37.35) | — | — |
| 2026-09-09 09:30 ET | **COVER** | `SLN` | 190 | $13.60 | $2.56 | $+232.25 | $20,112.45 | ▲ +232.25 after sell → book $11,864.39; vs 09:30 mark -2.56 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 1655 | $1.58 | $21.35 | $+172.08 | $17,476.20 | ▲ +172.08 after sell → book $11,843.04; vs 09:30 mark -21.35 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,476.20 | ▲ close $11,871.82 vs 09:30 $11,866.95 (session +28.78) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,476.20 | ▲ 09:30 equity $11,957.93 vs yday $11,871.82 (+86.11) | — | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 614 | $4.36 | $7.92 | $+174.30 | $14,791.24 | ▲ +174.30 after sell → book $11,950.01; vs 09:30 mark -7.92 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PIPR` | 37 | $76.79 | $2.10 | $-13.19 | $11,947.91 | ▼ -13.19 after sell → book $11,947.91; vs 09:30 mark -2.10 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,947.91 | ▲ close $11,947.91 vs 09:30 $11,957.93 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,947.91 | ▲ 09:30 equity $11,947.91 vs yday $11,947.91 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 18 | $23.63 | $2.04 | — | $11,520.52 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; ret5=-6.3; combo leftover $448.05; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 165 | $2.70 | $2.48 | — | $11,072.54 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+118.8; combo leftover $448.05; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 40 | $10.95 | $2.11 | — | $10,632.43 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+20.8; combo leftover $448.05; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 91 | $4.91 | $2.26 | — | $10,183.35 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+76.3; combo leftover $448.05; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 5 | $84.27 | $2.00 | — | $9,760.00 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ret5=+17.2; combo leftover $448.05; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `ASO` | 8 | $54.91 | $2.01 | — | $9,318.71 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+24.3; combo leftover $448.05; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 72 | $6.16 | $2.21 | — | $8,872.98 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+36.4; combo leftover $448.05; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `COO` | 8 | $54.66 | $2.01 | — | $8,433.69 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-22.3; combo leftover $448.05; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 10 | $112.83 | $2.07 | — | $9,559.96 | — | news🔴; gate news=bad; list yday_gainer,ohlc_hot; ret5=+11.7; combo leftover $1193.08; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `RWT` | 338 | $3.52 | $4.46 | — | $10,745.27 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-19.2; combo leftover $1193.08; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `CRDL` | 587 | $2.03 | $7.71 | — | $11,929.16 | — | news🔴; gate news=bad; list yday_mover; ret5=-8.8; combo leftover $1193.08; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BKV` | 47 | $24.97 | $2.18 | — | $13,100.57 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.8; combo leftover $1193.08; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `MYGN` | 354 | $3.37 | $4.67 | — | $14,288.88 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+4.0; combo leftover $1193.08; owner short_news_r_h3 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,288.88 | ▼ close $11,858.31 vs 09:30 $11,947.91 (session -51.37) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,288.88 | ▲ 09:30 equity $11,926.85 vs yday $11,858.31 (+68.54) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `TYRA` | 18 | $23.20 | $2.06 | $-11.85 | $14,704.42 | ▼ -11.85 after sell → book $11,924.79; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `INDP` | 165 | $2.80 | $2.52 | $+11.49 | $15,163.90 | ▲ +11.49 after sell → book $11,922.27; vs 09:30 mark -2.52 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `WLTH` | 40 | $10.29 | $2.13 | $-30.64 | $15,573.37 | ▼ -30.64 after sell → book $11,920.14; vs 09:30 mark -2.13 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BNC` | 91 | $5.03 | $2.29 | $+6.37 | $16,028.81 | ▲ +6.37 after sell → book $11,917.85; vs 09:30 mark -2.29 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SWKS` | 5 | $86.06 | $2.02 | $+4.92 | $16,457.08 | ▲ +4.92 after sell → book $11,915.82; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ASO` | 8 | $54.75 | $2.03 | $-5.33 | $16,893.05 | ▼ -5.33 after sell → book $11,913.79; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 72 | $6.02 | $2.23 | $-14.51 | $17,324.26 | ▼ -14.51 after sell → book $11,911.56; vs 09:30 mark -2.23 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `COO` | 8 | $54.78 | $2.03 | $-3.09 | $17,760.47 | ▼ -3.09 after sell → book $11,909.53; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,760.47 | ▼ close $11,756.80 vs 09:30 $11,926.85 (session -152.73) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,760.47 | ▼ 09:30 equity $11,756.60 vs yday $11,756.80 (-0.20) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,760.47 | ▼ close $11,654.37 vs 09:30 $11,756.60 (session -102.23) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,760.47 | ▲ 09:30 equity $11,672.24 vs yday $11,654.37 (+17.87) | — | — |
| 2026-09-16 09:30 ET | **COVER** | `QRVO` | 10 | $118.18 | $2.02 | $-57.54 | $16,576.65 | ▼ -57.54 after sell → book $11,670.22; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `RWT` | 338 | $3.98 | $4.36 | $-164.30 | $15,227.05 | ▼ -164.30 after sell → book $11,665.86; vs 09:30 mark -4.36 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `CRDL` | 587 | $1.85 | $7.57 | $+90.38 | $14,133.52 | ▲ +90.38 after sell → book $11,658.28; vs 09:30 mark -7.58 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `BKV` | 47 | $24.42 | $2.13 | $+21.54 | $12,983.65 | ▲ +21.54 after sell → book $11,656.15; vs 09:30 mark -2.13 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `MYGN` | 354 | $3.75 | $4.57 | $-143.75 | $11,651.59 | ▼ -143.75 after sell → book $11,651.59; vs 09:30 mark -4.56 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 5 | $77.12 | $2.00 | — | $11,263.98 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; ret5=+7.2; combo leftover $436.93; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 74 | $5.87 | $2.21 | — | $10,827.39 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+3.1; combo leftover $436.93; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 4 | $87.40 | $2.00 | — | $10,475.79 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+3.2; combo leftover $436.93; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 16 | $27.09 | $2.04 | — | $10,040.31 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; combo leftover $436.93; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 4 | $89.38 | $2.00 | — | $9,680.79 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; combo leftover $436.93; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 18 | $23.29 | $2.04 | — | $9,259.52 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+16.1; combo leftover $436.93; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 13 | $33.14 | $2.03 | — | $8,826.67 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=-2.9; combo leftover $436.93; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `CAI` | 15 | $28.16 | $2.04 | — | $8,402.24 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ret5=+14.8; combo leftover $436.93; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **SHORT** | `BBNX` | 156 | $18.61 | $2.59 | — | $11,302.81 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; ret5=-0.5; combo leftover $2908.80; owner short_news_r_h3 | — |
| 2026-09-16 09:30 ET | **SHORT** | `GFR` | 425 | $6.83 | $5.66 | — | $14,199.91 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+11.2; combo leftover $2908.80; owner short_news_r_h3 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,199.91 | ▼ close $11,192.41 vs 09:30 $11,672.24 (session -434.57) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,199.91 | ▲ 09:30 equity $11,209.21 vs yday $11,192.41 (+16.80) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `RDNT` | 5 | $76.44 | $2.02 | $-7.43 | $14,580.08 | ▼ -7.43 after sell → book $11,207.19; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 74 | $5.58 | $2.23 | $-25.91 | $14,990.77 | ▼ -25.91 after sell → book $11,204.95; vs 09:30 mark -2.24 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 4 | $83.20 | $2.02 | $-20.82 | $15,321.55 | ▼ -20.82 after sell → book $11,202.93; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ADPT` | 16 | $28.23 | $2.06 | $+14.14 | $15,771.17 | ▲ +14.14 after sell → book $11,200.87; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 4 | $86.76 | $2.02 | $-14.50 | $16,116.19 | ▼ -14.50 after sell → book $11,198.85; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 18 | $24.09 | $2.06 | $+10.29 | $16,547.74 | ▲ +10.29 after sell → book $11,196.79; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `CAI` | 15 | $28.59 | $2.06 | $+2.43 | $16,974.61 | ▲ +2.43 after sell → book $11,194.73; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 3 | $233.85 | $2.00 | — | $16,271.06 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; ret5=+11.7; combo leftover $727.48; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 4 | $147.61 | $2.00 | — | $15,678.62 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; ret5=+17.7; combo leftover $727.48; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 95 | $7.59 | $2.27 | — | $14,955.30 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; 🔵; ret5=+9.4; combo leftover $727.48; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 28 | $25.95 | $2.07 | — | $14,226.62 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; combo leftover $727.48; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 4 | $170.85 | $2.00 | — | $13,541.22 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; combo leftover $727.48; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `CIFR` | 40 | $18.04 | $2.11 | — | $12,817.71 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.1; combo leftover $727.48; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRKR` | 11 | $61.90 | $2.02 | — | $12,134.79 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; 🔵; ret5=+11.5; combo leftover $727.48; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **SHORT** | `BULL` | 351 | $7.95 | $4.68 | — | $14,920.55 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-18.4; combo leftover $2795.06; owner short_news_r_h3 | — |
| 2026-09-17 09:30 ET | **SHORT** | `LEN` | 34 | $81.00 | $2.20 | — | $17,672.35 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-3.0; combo leftover $2795.06; owner short_news_r_h3 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,672.35 | ▲ close $11,472.44 vs 09:30 $11,209.21 (session +299.08) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,672.35 | ▲ 09:30 equity $11,587.00 vs yday $11,472.44 (+114.56) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 13 | $39.50 | $2.05 | $+78.60 | $18,183.80 | ▲ +78.60 after sell → book $11,584.95; vs 09:30 mark -2.05 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ILMN` | 3 | $249.13 | $2.02 | $+41.82 | $18,929.17 | ▲ +41.82 after sell → book $11,582.93; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 4 | $146.50 | $2.02 | $-8.46 | $19,513.15 | ▼ -8.46 after sell → book $11,580.91; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 95 | $7.98 | $2.30 | $+32.47 | $20,268.95 | ▲ +32.47 after sell → book $11,578.61; vs 09:30 mark -2.30 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ARQT` | 28 | $26.14 | $2.09 | $+1.15 | $20,998.78 | ▲ +1.15 after sell → book $11,576.52; vs 09:30 mark -2.09 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 4 | $182.33 | $2.02 | $+41.90 | $21,726.08 | ▲ +41.90 after sell → book $11,574.50; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `CIFR` | 40 | $17.80 | $2.13 | $-13.64 | $22,435.95 | ▼ -13.64 after sell → book $11,572.37; vs 09:30 mark -2.13 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BRKR` | 11 | $63.37 | $2.04 | $+12.10 | $23,130.97 | ▲ +12.10 after sell → book $11,570.32; vs 09:30 mark -2.05 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 3 | $219.62 | $2.00 | — | $22,470.11 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; combo leftover $867.41; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 10 | $85.00 | $2.02 | — | $21,618.09 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=+18.3; combo leftover $867.41; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 219 | $3.95 | $2.83 | — | $20,750.22 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; combo leftover $867.41; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 61 | $14.07 | $2.17 | — | $19,889.78 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; combo leftover $867.41; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 58 | $14.79 | $2.16 | — | $19,029.79 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; combo leftover $867.41; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 29 | $29.32 | $2.08 | — | $18,177.43 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; combo leftover $867.41; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 285 | $3.04 | $3.68 | — | $17,308.78 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; combo leftover $867.41; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `VITL` | 76 | $11.38 | $2.22 | — | $16,441.68 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+19.5; combo leftover $867.41; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **SHORT** | `FIVN` | 167 | $34.44 | $2.72 | — | $22,190.44 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $5775.58; owner short_news_r_h3 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22,190.44 | ▲ close $11,840.27 vs 09:30 $11,587.00 (session +291.82) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22,190.44 | ▼ 09:30 equity $11,790.53 vs yday $11,840.27 (-49.74) | — | — |
| 2026-09-21 09:30 ET | **COVER** | `BBNX` | 156 | $22.11 | $2.46 | $-551.04 | $18,738.82 | ▼ -551.04 after sell → book $11,788.07; vs 09:30 mark -2.46 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `GFR` | 425 | $6.55 | $5.48 | $+107.86 | $15,949.59 | ▲ +107.86 after sell → book $11,782.59; vs 09:30 mark -5.48 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `VICR` | 3 | $230.25 | $2.02 | $+27.87 | $16,638.32 | ▲ +27.87 after sell → book $11,780.57; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ECO` | 10 | $82.83 | $2.04 | $-25.76 | $17,464.58 | ▼ -25.76 after sell → book $11,778.53; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `EYPT` | 219 | $3.87 | $2.87 | $-23.22 | $18,309.24 | ▼ -23.22 after sell → book $11,775.66; vs 09:30 mark -2.87 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 61 | $13.90 | $2.19 | $-14.74 | $19,154.95 | ▼ -14.74 after sell → book $11,773.47; vs 09:30 mark -2.19 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 58 | $14.58 | $2.18 | $-16.53 | $19,998.40 | ▼ -16.53 after sell → book $11,771.28; vs 09:30 mark -2.19 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 29 | $29.43 | $2.10 | $-0.98 | $20,849.78 | ▼ -0.98 after sell → book $11,769.19; vs 09:30 mark -2.09 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 285 | $4.00 | $3.73 | $+267.61 | $21,986.04 | ▲ +267.61 after sell → book $11,765.45; vs 09:30 mark -3.74 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VITL` | 76 | $12.05 | $2.24 | $+46.46 | $22,899.60 | ▲ +46.46 after sell → book $11,763.21; vs 09:30 mark -2.24 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 5 | $157.87 | $2.00 | — | $22,108.25 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; ret5=+6.5; combo leftover $858.74; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 9 | $88.83 | $2.02 | — | $21,306.76 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; ret5=+7.6; combo leftover $858.74; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 92 | $9.31 | $2.27 | — | $20,447.97 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=+5.3; combo leftover $858.74; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 63 | $13.47 | $2.18 | — | $19,596.87 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; combo leftover $858.74; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 85 | $9.99 | $2.25 | — | $18,745.48 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+5.3; combo leftover $858.74; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 50 | $16.91 | $2.14 | — | $17,897.84 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+50.5; combo leftover $858.74; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `USDE` | 65 | $13.05 | $2.19 | — | $17,047.40 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+40.4; combo leftover $858.74; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `GEMI` | 149 | $5.75 | $2.44 | — | $16,187.47 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+30.3; combo leftover $858.74; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 355 | $8.26 | $4.74 | — | $19,115.03 | — | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $2936.43; owner short_news_r_h3 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AMD` | 5 | $583.88 | $2.12 | — | $22,032.31 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+8.5; combo leftover $2936.43; owner short_news_r_h3 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22,032.31 | ▼ close $11,486.71 vs 09:30 $11,790.53 (session -252.16) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22,032.31 | ▲ 09:30 equity $11,551.77 vs yday $11,486.71 (+65.06) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `BULL` | 351 | $8.28 | $4.53 | $-123.29 | $19,123.26 | ▼ -123.29 after sell → book $11,547.24; vs 09:30 mark -4.53 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SELL** | `SBET` | 85 | $9.91 | $2.27 | $-11.31 | $19,963.34 | ▼ -11.31 after sell → book $11,544.97; vs 09:30 mark -2.27 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `USDE` | 65 | $12.99 | $2.21 | $-8.29 | $20,805.48 | ▼ -8.29 after sell → book $11,542.77; vs 09:30 mark -2.20 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `GEMI` | 149 | $6.05 | $2.47 | $+39.79 | $21,705.20 | ▲ +39.79 after sell → book $11,540.29; vs 09:30 mark -2.48 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `ARM` | 3 | $319.41 | $2.00 | — | $20,744.97 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+35.1; combo leftover $1085.26; owner union_join_vol_green_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `FSLY` | 38 | $28.02 | $2.10 | — | $19,678.11 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; ret5=+10.3; combo leftover $1085.26; owner union_join_vol_green_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `META` | 1 | $731.40 | $1.99 | — | $18,944.72 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; ret5=+11.4; combo leftover $1085.26; owner union_join_vol_green_h1 | — |
| 2026-09-22 09:30 ET | **SHORT** | `USFD` | 30 | $93.97 | $2.19 | — | $21,761.63 | — | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $2883.55; owner short_news_r_h3 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,761.63 | ▼ close $11,420.06 vs 09:30 $11,551.77 (session -111.95) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,761.63 | ▼ 09:30 equity $10,739.05 vs yday $11,420.06 (-681.01) | — | — |
| 2026-09-23 09:30 ET | **COVER** | `LEN` | 34 | $82.00 | $2.09 | $-38.29 | $18,971.54 | ▼ -38.29 after sell → book $10,736.96; vs 09:30 mark -2.09 | short_news_r_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BKKT` | 92 | $9.50 | $2.29 | $+12.92 | $19,843.24 | ▲ +12.92 after sell → book $10,734.67; vs 09:30 mark -2.29 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BTDR` | 63 | $12.84 | $2.20 | $-44.38 | $20,649.97 | ▼ -44.38 after sell → book $10,732.47; vs 09:30 mark -2.20 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 50 | $16.92 | $2.16 | $-3.80 | $21,493.81 | ▼ -3.80 after sell → book $10,730.31; vs 09:30 mark -2.16 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `ARM` | 3 | $331.78 | $2.02 | $+33.09 | $22,487.13 | ▲ +33.09 after sell → book $10,728.29; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `FSLY` | 38 | $25.90 | $2.12 | $-84.79 | $23,469.20 | ▼ -84.79 after sell → book $10,726.17; vs 09:30 mark -2.12 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `META` | 1 | $747.60 | $2.01 | $+12.19 | $24,214.79 | ▲ +12.19 after sell → book $10,724.15; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 58 | $20.65 | $2.16 | — | $23,014.93 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; combo leftover $1210.74; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 77 | $15.72 | $2.22 | — | $21,802.26 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; combo leftover $1210.74; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `TNGX` | 47 | $25.40 | $2.13 | — | $20,606.33 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; combo leftover $1210.74; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `NMRA` | 1576 | $0.77 | $16.83 | — | $19,379.13 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; combo leftover $1210.74; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 28 | $41.76 | $2.07 | — | $18,207.78 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+36.4; combo leftover $1210.74; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `BFLY` | 122 | $9.90 | $2.36 | — | $16,997.62 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; combo leftover $1210.74; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **SHORT** | `HALO` | 45 | $116.85 | $2.32 | — | $22,253.55 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $5348.19; owner short_news_r_h3 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22,253.55 | ▼ close $10,618.61 vs 09:30 $10,739.05 (session -75.44) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22,253.55 | ▼ 09:30 equity $10,528.09 vs yday $10,618.61 (-90.52) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 5 | $163.95 | $2.02 | $+26.37 | $23,071.28 | ▲ +26.37 after sell → book $10,526.07; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 9 | $87.67 | $2.04 | $-14.45 | $23,858.32 | ▼ -14.45 after sell → book $10,524.03; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `AMD` | 5 | $600.27 | $2.00 | $-86.07 | $20,854.96 | ▼ -86.07 after sell → book $10,522.03; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 58 | $20.52 | $2.18 | $-11.89 | $22,042.94 | ▼ -11.89 after sell → book $10,519.84; vs 09:30 mark -2.19 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 77 | $14.38 | $2.24 | $-107.64 | $23,147.95 | ▼ -107.64 after sell → book $10,517.60; vs 09:30 mark -2.24 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `TNGX` | 47 | $23.99 | $2.15 | $-70.55 | $24,273.33 | ▼ -70.55 after sell → book $10,515.45; vs 09:30 mark -2.15 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `NMRA` | 1576 | $0.75 | $16.76 | $-68.26 | $25,432.27 | ▼ -68.26 after sell → book $10,498.69; vs 09:30 mark -16.76 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 28 | $36.02 | $2.09 | $-164.75 | $26,438.88 | ▼ -164.75 after sell → book $10,496.60; vs 09:30 mark -2.09 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BFLY` | 122 | $9.12 | $2.39 | $-99.90 | $27,549.13 | ▼ -99.90 after sell → book $10,494.21; vs 09:30 mark -2.39 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $27,549.13 | ▼ close $10,246.61 vs 09:30 $10,528.09 (session -247.60) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,609.09 | ▼ 09:30 equity $8,886.43 vs yday $8,916.36 (-29.93) | 09:30 open · cash $20,609.09 (unchanged overnight, no fees) · equity $8,886.43 vs prior close $8,916.36 (-29.93) | — |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 29 | $26.27 | $2.08 | — | $19,845.18 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; combo leftover $772.84; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 200 | $3.86 | $2.59 | — | $19,070.59 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; combo leftover $772.84; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TWST` | 4 | $184.00 | $2.00 | — | $18,332.59 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.3; combo leftover $772.84; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 47 | $16.21 | $2.13 | — | $17,568.59 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; combo leftover $772.84; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GRAL` | 6 | $123.50 | $2.01 | — | $16,825.58 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+56.6; combo leftover $772.84; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `QMCO` | 25 | $29.80 | $2.06 | — | $16,078.52 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+18.2; combo leftover $772.84; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CYPH` | 192 | $4.00 | $2.57 | — | $15,306.99 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+32.9; combo leftover $772.84; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CDNA` | 12 | $61.33 | $2.03 | — | $14,569.00 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+13.1; combo leftover $772.84; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `RSKD` | 564 | $7.85 | $7.53 | — | $18,988.88 | — | news🔴; gate news=bad; list yday_gainer; 🔵; ⚪; ret5=+25.4; combo leftover $4434.48; owner short_news_r_h3 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,988.88 | ▲ close $8,932.68 vs 09:30 $8,886.43 (session +71.24) | 16:00 close · cash $18,988.88 · equity $8,932.68 vs 09:30 $8,886.43 (+46.25; session marks +71.24) · 14 name(s) marked open→close (per-name table). AEHL×303 09:30 $9.05 → close $9.36 -93.93; BAND×39 09:30 $61.83 → close $61.83 -0.00; HALO×19 09:30 $115.36 → close $113.90 +27.74; PAYX×20 09:30 $101.59 → close $101.59 +0.00; USFD×25 09:30 $93.82 → close $93.82 +0.00; WRBY×29 09:30 $26.27 → close $26.71 +12.76; ZSQR×200 09:30 $3.86 → close $3.78 -16.00; TWST×4 09:30 $184.00 → close $182.83 -4.68; SECZ×47 09:30 $16.21 → close $15.96 -11.75; GRAL×6 09:30 $123.50 → close $126.89 +20.34; QMCO×25 09:30 $29.80 → close $31.68 +47.00; CYPH×192 09:30 $4.00 → close $4.12 +22.08; CDNA×12 09:30 $61.33 → close $63.68 +28.20; RSKD×564 09:30 $7.85 → close $7.78 +39.48 | — |

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
| `FIVN` | 167 | 2026-09-18 @ $34.44 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $5775.58; owner short_news_r_h3 |
| `AEHL` | 355 | 2026-09-21 @ $8.26 | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $2936.43; owner short_news_r_h3 |
| `USFD` | 30 | 2026-09-22 @ $93.97 | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $2883.55; owner short_news_r_h3 |
| `HALO` | 45 | 2026-09-23 @ $116.85 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $5348.19; owner short_news_r_h3 |
