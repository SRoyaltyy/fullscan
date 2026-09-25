# Factor mine action — `combo_snj_333_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared short_news_r_h3/union_news_g_h1/union_join_vol_green_h1 w=0.33,0.33,0.33 net=priority

Cash book **-21.09%** ($7,891) · signal-only (no cash/fees) was —. Starts YES **0/30**. Fills 502 · skips 251 · realized $+228.56.

## How this sleeve decides (like you are 10)

Imagine 3 kids at the same 09:30 school bell sharing one $10,000 book: short_news_r_h3 33%, union_news_g_h1 33%, union_join_vol_green_h1 33%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: short_news_r_h3 33%, union_news_g_h1 33%, union_join_vol_green_h1 33%.
- Member: short_news_r_h3 (33% · short · hold 3).
- Member: union_news_g_h1 (33% · long · hold 1).
- Member: union_join_vol_green_h1 (33% · long · hold 1).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,852.24.

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
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 96 | $4.31 | $2.28 | — | $9,583.96 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; combo leftover $416.67; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 277 | $1.50 | $3.57 | — | $9,164.89 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=+9.2; combo leftover $416.67; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 28 | $14.80 | $2.07 | — | $8,748.41 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=-9.9; combo leftover $416.67; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 99 | $4.18 | $2.29 | — | $8,332.31 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; combo leftover $416.67; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 25 | $16.50 | $2.06 | — | $7,917.74 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; combo leftover $416.67; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 37 | $11.12 | $2.10 | — | $7,504.20 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; combo leftover $416.67; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 154 | $2.69 | $2.45 | — | $7,087.49 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; combo leftover $416.67; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 57 | $7.29 | $2.16 | — | $6,669.80 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; combo leftover $416.67; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 1 | $359.83 | $1.99 | — | $6,307.98 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.9; combo leftover $476.41; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 3 | $146.90 | $2.00 | — | $5,865.28 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+3.6; combo leftover $476.41; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 3 | $120.00 | $2.00 | — | $5,503.28 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+0.6; combo leftover $476.41; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 24 | $19.57 | $2.06 | — | $5,031.54 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; combo leftover $476.41; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 35 | $13.55 | $2.10 | — | $4,555.19 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+17.5; combo leftover $476.41; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 36 | $13.18 | $2.10 | — | $4,078.61 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+12.0; combo leftover $476.41; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 1152 | $1.18 | $15.10 | — | $5,422.87 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $1359.54; owner short_news_r_h3 | — |
| 2026-08-14 09:30 ET | **SHORT** | `LUNR` | 70 | $19.17 | $2.26 | — | $6,762.51 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+17.6; combo leftover $1359.54; owner short_news_r_h3 | — |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 107 | $12.70 | $2.38 | — | $8,118.50 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.6; combo leftover $1359.54; owner short_news_r_h3 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,118.50 | ▲ close $9,958.54 vs 09:30 $10,000.00 (session +9.51) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,118.50 | ▼ 09:30 equity $9,879.47 vs yday $9,958.54 (-79.07) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 96 | $4.60 | $2.30 | $+23.26 | $8,557.80 | ▲ +23.26 after sell → book $9,877.17; vs 09:30 mark -2.30 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 277 | $1.52 | $3.63 | $-1.66 | $8,975.21 | ▼ -1.66 after sell → book $9,873.54; vs 09:30 mark -3.63 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 28 | $13.67 | $2.09 | $-35.81 | $9,355.87 | ▼ -35.81 after sell → book $9,871.44; vs 09:30 mark -2.10 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 99 | $4.10 | $2.31 | $-12.52 | $9,759.46 | ▼ -12.52 after sell → book $9,869.13; vs 09:30 mark -2.31 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 25 | $15.73 | $2.08 | $-23.40 | $10,150.63 | ▼ -23.40 after sell → book $9,867.05; vs 09:30 mark -2.08 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 37 | $9.57 | $2.12 | $-61.57 | $10,502.59 | ▼ -61.57 after sell → book $9,864.92; vs 09:30 mark -2.13 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NCMI` | 154 | $2.80 | $2.49 | $+12.00 | $10,931.31 | ▲ +12.00 after sell → book $9,862.44; vs 09:30 mark -2.48 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMLS` | 57 | $7.24 | $2.18 | $-7.19 | $11,341.81 | ▼ -7.19 after sell → book $9,860.26; vs 09:30 mark -2.18 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 1 | $367.88 | $2.01 | $+4.04 | $11,707.67 | ▲ +4.04 after sell → book $9,858.24; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 3 | $149.37 | $2.02 | $+3.39 | $12,153.76 | ▲ +3.39 after sell → book $9,856.22; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 3 | $127.40 | $2.02 | $+18.18 | $12,533.95 | ▲ +18.18 after sell → book $9,854.21; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 24 | $19.57 | $2.08 | $-4.14 | $13,001.54 | ▼ -4.14 after sell → book $9,852.12; vs 09:30 mark -2.09 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 35 | $13.16 | $2.12 | $-17.86 | $13,460.03 | ▼ -17.86 after sell → book $9,850.01; vs 09:30 mark -2.11 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 36 | $13.84 | $2.12 | $+19.54 | $13,956.15 | ▲ +19.54 after sell → book $9,847.89; vs 09:30 mark -2.12 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 102 | $9.12 | $2.30 | — | $13,023.61 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; combo leftover $930.41; owner union_join_vol_green_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 63 | $14.66 | $2.18 | — | $12,097.86 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; combo leftover $930.41; owner union_join_vol_green_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `BORR` | 202 | $4.59 | $2.61 | — | $11,168.07 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ⚪; ret5=+14.8; combo leftover $930.41; owner union_join_vol_green_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 222 | $4.19 | $2.86 | — | $10,235.03 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ⚪; ret5=+291.8; combo leftover $930.41; owner union_join_vol_green_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `MP` | 16 | $58.01 | $2.04 | — | $9,304.83 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+14.9; combo leftover $930.41; owner union_join_vol_green_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 20 | $46.18 | $2.05 | — | $8,379.18 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+6.7; combo leftover $930.48; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 6 | $142.77 | $2.01 | — | $7,520.55 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.8; combo leftover $930.48; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 4 | $202.70 | $2.00 | — | $6,707.75 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+8.3; combo leftover $930.48; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 10 | $92.99 | $2.02 | — | $5,775.83 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.8; combo leftover $930.48; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 18 | $49.00 | $2.04 | — | $4,891.78 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ⚪; ret5=+12.2; combo leftover $930.48; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 850 | $1.15 | $11.14 | — | $5,858.14 | — | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; combo leftover $978.36; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 274 | $3.56 | $3.62 | — | $6,829.96 | — | news🔴; gate news=bad; list yday_mover; ret5=-15.6; combo leftover $978.36; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `APMD` | 30 | $31.70 | $2.13 | — | $7,778.84 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+17.6; combo leftover $978.36; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 325 | $3.01 | $4.28 | — | $8,752.80 | — | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; combo leftover $978.36; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `RNW` | 143 | $6.80 | $2.48 | — | $9,722.73 | — | news🔴; gate news=bad; list overnight; ⚪; ret5=+10.4; combo leftover $978.36; owner short_news_r_h3 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,722.73 | ▲ close $9,820.30 vs 09:30 $9,879.47 (session +18.17) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,722.73 | ▲ 09:30 equity $9,859.11 vs yday $9,820.30 (+38.81) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 102 | $9.03 | $2.32 | $-13.80 | $10,641.46 | ▼ -13.80 after sell → book $9,856.78; vs 09:30 mark -2.33 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 63 | $13.19 | $2.20 | $-96.99 | $11,470.23 | ▼ -96.99 after sell → book $9,854.58; vs 09:30 mark -2.20 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `BORR` | 202 | $4.56 | $2.65 | $-11.32 | $12,388.70 | ▼ -11.32 after sell → book $9,851.93; vs 09:30 mark -2.65 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 222 | $3.94 | $2.91 | $-61.27 | $13,260.47 | ▼ -61.27 after sell → book $9,849.02; vs 09:30 mark -2.91 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `MP` | 16 | $56.35 | $2.06 | $-30.66 | $14,160.01 | ▼ -30.66 after sell → book $9,846.96; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 20 | $48.00 | $2.07 | $+32.28 | $15,117.94 | ▲ +32.28 after sell → book $9,844.89; vs 09:30 mark -2.07 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 6 | $148.04 | $2.03 | $+27.58 | $16,004.16 | ▲ +27.58 after sell → book $9,842.87; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 4 | $208.93 | $2.02 | $+20.90 | $16,837.85 | ▲ +20.90 after sell → book $9,840.84; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 10 | $92.38 | $2.04 | $-10.16 | $17,759.61 | ▼ -10.16 after sell → book $9,838.80; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OUST` | 18 | $45.09 | $2.06 | $-74.49 | $18,569.17 | ▼ -74.49 after sell → book $9,836.74; vs 09:30 mark -2.06 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,569.17 | ▲ close $10,057.57 vs 09:30 $9,859.11 (session +220.83) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,569.17 | ▼ 09:30 equity $10,024.92 vs yday $10,057.57 (-32.65) | — | — |
| 2026-08-19 09:30 ET | **COVER** | `EU` | 1152 | $1.07 | $14.86 | $+96.76 | $17,321.67 | ▲ +96.76 after sell → book $10,010.06; vs 09:30 mark -14.86 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 107 | $11.75 | $2.31 | $+96.43 | $16,062.11 | ▲ +96.43 after sell → book $10,007.75; vs 09:30 mark -2.31 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,062.11 | ▲ close $10,048.13 vs 09:30 $10,024.92 (session +40.38) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,062.11 | ▼ 09:30 equity $9,985.67 vs yday $10,048.13 (-62.46) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `LUNR` | 70 | $18.13 | $2.20 | $+68.34 | $14,790.81 | ▲ +68.34 after sell → book $9,983.47; vs 09:30 mark -2.20 | short_news_r_h3: dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 850 | $0.96 | $10.74 | $+137.07 | $13,961.52 | ▲ +137.07 after sell → book $9,972.73; vs 09:30 mark -10.74 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ZNTL` | 274 | $4.01 | $3.53 | $-131.82 | $12,857.88 | ▼ -131.82 after sell → book $9,969.20; vs 09:30 mark -3.53 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `APMD` | 30 | $31.87 | $2.08 | $-9.31 | $11,899.70 | ▼ -9.31 after sell → book $9,967.12; vs 09:30 mark -2.08 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 325 | $2.95 | $4.19 | $+11.02 | $10,936.76 | ▲ +11.02 after sell → book $9,962.93; vs 09:30 mark -4.19 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `RNW` | 143 | $6.81 | $2.42 | $-6.33 | $9,960.51 | ▼ -6.33 after sell → book $9,960.51; vs 09:30 mark -2.42 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 20 | $20.55 | $2.05 | — | $9,547.46 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; combo leftover $415.02; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 20 | $20.65 | $2.05 | — | $9,132.41 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; combo leftover $415.02; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 71 | $5.77 | $2.20 | — | $8,720.53 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; combo leftover $415.02; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 21 | $19.63 | $2.05 | — | $8,306.25 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; combo leftover $415.02; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 14 | $29.63 | $2.03 | — | $7,889.40 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; combo leftover $415.02; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 237 | $1.75 | $3.06 | — | $7,471.59 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; combo leftover $415.02; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 2 | $144.54 | $2.00 | — | $7,180.52 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; combo leftover $415.02; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 84 | $4.92 | $2.24 | — | $6,764.99 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $415.02; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 4 | $91.01 | $2.00 | — | $6,398.95 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; combo leftover $422.81; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 2 | $150.14 | $2.00 | — | $6,096.68 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+173.9; combo leftover $422.81; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 598 | $0.71 | $6.02 | — | $5,667.87 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+10.5; combo leftover $422.81; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 64 | $6.61 | $2.18 | — | $5,242.97 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+25.3; combo leftover $422.81; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 26 | $16.00 | $2.07 | — | $4,824.90 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+18.9; combo leftover $422.81; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 15 | $26.57 | $2.04 | — | $4,424.31 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+4.8; combo leftover $422.81; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 7 | $58.73 | $2.01 | — | $4,011.19 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+10.8; combo leftover $422.81; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 9 | $44.76 | $2.02 | — | $3,606.33 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=+8.7; combo leftover $422.81; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 2 | $204.45 | $2.03 | — | $4,013.21 | — | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; combo leftover $450.79; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 21 | $21.40 | $2.09 | — | $4,460.52 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-25.2; combo leftover $450.79; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 101 | $4.43 | $2.33 | — | $4,905.62 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-23.1; combo leftover $450.79; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 38 | $11.81 | $2.14 | — | $5,352.45 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $450.79; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 2 | $173.90 | $2.03 | — | $5,698.23 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; combo leftover $450.79; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 9 | $46.85 | $2.05 | — | $6,117.83 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=+5.0; combo leftover $450.79; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 4 | $106.38 | $2.03 | — | $6,541.32 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; combo leftover $450.79; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 97 | $4.61 | $2.32 | — | $6,986.17 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $450.79; owner short_news_r_h3 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,986.17 | ▲ close $9,954.10 vs 09:30 $9,985.67 (session +48.62) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,986.17 | ▲ 09:30 equity $10,113.15 vs yday $9,954.10 (+159.05) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 20 | $21.90 | $2.07 | $+22.88 | $7,422.10 | ▲ +22.88 after sell → book $10,111.08; vs 09:30 mark -2.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 20 | $21.75 | $2.07 | $+17.88 | $7,855.03 | ▲ +17.88 after sell → book $10,109.01; vs 09:30 mark -2.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 71 | $5.67 | $2.22 | $-11.53 | $8,255.37 | ▼ -11.53 after sell → book $10,106.78; vs 09:30 mark -2.23 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 21 | $21.17 | $2.07 | $+28.21 | $8,697.87 | ▲ +28.21 after sell → book $10,104.71; vs 09:30 mark -2.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 14 | $32.17 | $2.05 | $+31.48 | $9,146.20 | ▲ +31.48 after sell → book $10,102.66; vs 09:30 mark -2.05 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 237 | $1.79 | $3.11 | $+3.32 | $9,567.32 | ▲ +3.32 after sell → book $10,099.55; vs 09:30 mark -3.11 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 2 | $154.70 | $2.02 | $+16.31 | $9,874.70 | ▲ +16.31 after sell → book $10,097.54; vs 09:30 mark -2.01 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 84 | $5.20 | $2.27 | $+19.01 | $10,309.24 | ▲ +19.01 after sell → book $10,095.27; vs 09:30 mark -2.27 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 4 | $95.72 | $2.02 | $+14.82 | $10,690.10 | ▲ +14.82 after sell → book $10,093.25; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 2 | $133.11 | $2.02 | $-38.07 | $10,954.30 | ▼ -38.07 after sell → book $10,091.23; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HUMA` | 598 | $0.67 | $5.93 | $-31.69 | $11,351.42 | ▼ -31.69 after sell → book $10,085.30; vs 09:30 mark -5.93 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 64 | $6.95 | $2.20 | $+17.70 | $11,794.02 | ▲ +17.70 after sell → book $10,083.10; vs 09:30 mark -2.20 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 26 | $17.66 | $2.09 | $+39.00 | $12,251.09 | ▲ +39.00 after sell → book $10,081.01; vs 09:30 mark -2.09 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 15 | $26.25 | $2.06 | $-8.89 | $12,642.78 | ▼ -8.89 after sell → book $10,078.95; vs 09:30 mark -2.06 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 9 | $44.52 | $2.04 | $-6.21 | $13,041.43 | ▼ -6.21 after sell → book $10,076.92; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 5 | $119.43 | $2.00 | — | $12,442.27 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; combo leftover $621.02; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 36 | $17.20 | $2.10 | — | $11,820.97 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; combo leftover $621.02; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 55 | $11.13 | $2.15 | — | $11,206.67 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; combo leftover $621.02; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 470 | $1.32 | $6.06 | — | $10,580.21 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; combo leftover $621.02; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 374 | $1.66 | $4.82 | — | $9,954.54 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; combo leftover $621.02; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `INDP` | 446 | $1.39 | $5.75 | — | $9,328.85 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+30.2; combo leftover $621.02; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 75 | $8.28 | $2.21 | — | $8,705.63 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; combo leftover $621.02; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 293 | $2.47 | $3.78 | — | $7,978.14 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; combo leftover $725.47; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 6 | $115.18 | $2.01 | — | $7,285.05 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; combo leftover $725.47; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 1 | $623.26 | $1.99 | — | $6,659.80 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ret5=+1.4; combo leftover $725.47; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 62 | $11.70 | $2.18 | — | $5,932.23 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+21.1; combo leftover $725.47; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 65 | $11.10 | $2.19 | — | $5,208.87 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+19.1; combo leftover $725.47; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 223 | $3.24 | $2.88 | — | $4,483.47 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+21.3; combo leftover $725.47; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 240 | $3.11 | $3.17 | — | $5,226.70 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; combo leftover $747.24; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `MRNA` | 5 | $133.11 | $2.04 | — | $5,890.21 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=+109.5; combo leftover $747.24; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 8 | $89.10 | $2.05 | — | $6,600.96 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $747.24; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 19 | $38.40 | $2.09 | — | $7,328.47 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $747.24; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 35 | $20.90 | $2.14 | — | $8,057.83 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $747.24; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 27 | $27.00 | $2.11 | — | $8,784.72 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; combo leftover $747.24; owner short_news_r_h3 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,784.72 | ▲ close $10,112.61 vs 09:30 $10,113.15 (session +89.42) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,784.72 | ▲ 09:30 equity $10,289.20 vs yday $10,112.61 (+176.59) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 5 | $120.51 | $2.02 | $+1.37 | $9,385.25 | ▲ +1.37 after sell → book $10,287.18; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 36 | $16.57 | $2.12 | $-26.90 | $9,979.65 | ▼ -26.90 after sell → book $10,285.06; vs 09:30 mark -2.12 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 55 | $13.33 | $2.17 | $+116.67 | $10,710.62 | ▲ +116.67 after sell → book $10,282.88; vs 09:30 mark -2.18 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 470 | $1.83 | $6.15 | $+227.49 | $11,564.57 | ▲ +227.49 after sell → book $10,276.73; vs 09:30 mark -6.15 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 374 | $1.55 | $4.90 | $-50.86 | $12,139.38 | ▼ -50.86 after sell → book $10,271.84; vs 09:30 mark -4.89 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `INDP` | 446 | $1.24 | $5.84 | $-78.49 | $12,686.58 | ▼ -78.49 after sell → book $10,266.00; vs 09:30 mark -5.84 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 75 | $8.59 | $2.24 | $+18.80 | $13,328.59 | ▲ +18.80 after sell → book $10,263.76; vs 09:30 mark -2.24 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 293 | $2.40 | $3.84 | $-28.13 | $14,027.95 | ▼ -28.13 after sell → book $10,259.92; vs 09:30 mark -3.84 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 6 | $121.00 | $2.03 | $+30.88 | $14,751.93 | ▲ +30.88 after sell → book $10,257.90; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 1 | $653.04 | $2.01 | $+25.77 | $15,402.95 | ▲ +25.77 after sell → book $10,255.88; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 62 | $11.17 | $2.20 | $-37.23 | $16,093.30 | ▼ -37.23 after sell → book $10,253.69; vs 09:30 mark -2.19 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTDR` | 65 | $11.48 | $2.21 | $+20.63 | $16,837.29 | ▲ +20.63 after sell → book $10,251.48; vs 09:30 mark -2.21 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 223 | $2.99 | $2.92 | $-61.55 | $17,501.14 | ▼ -61.55 after sell → book $10,248.56; vs 09:30 mark -2.92 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,501.14 | ▲ close $10,304.02 vs 09:30 $10,289.20 (session +55.47) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,501.14 | ▲ 09:30 equity $10,348.01 vs yday $10,304.02 (+43.99) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 7 | $57.93 | $2.03 | $-9.64 | $17,904.62 | ▼ -9.64 after sell → book $10,345.98; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 2 | $212.00 | $2.00 | $-19.12 | $17,478.62 | ▼ -19.12 after sell → book $10,343.98; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 21 | $20.90 | $2.05 | $+6.36 | $17,037.67 | ▲ +6.36 after sell → book $10,341.93; vs 09:30 mark -2.05 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TOYO` | 101 | $4.42 | $2.29 | $-3.62 | $16,588.95 | ▼ -3.62 after sell → book $10,339.63; vs 09:30 mark -2.30 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ABCL` | 38 | $11.00 | $2.10 | $+26.73 | $16,168.85 | ▲ +26.73 after sell → book $10,337.53; vs 09:30 mark -2.10 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 2 | $170.64 | $2.00 | $+2.50 | $15,825.57 | ▲ +2.50 after sell → book $10,335.53; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AAP` | 9 | $43.63 | $2.02 | $+24.91 | $15,430.89 | ▲ +24.91 after sell → book $10,333.52; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 4 | $105.58 | $2.00 | $-0.84 | $15,006.57 | ▼ -0.84 after sell → book $10,331.52; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 97 | $4.77 | $2.28 | $-20.12 | $14,541.59 | ▼ -20.12 after sell → book $10,329.23; vs 09:30 mark -2.29 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 17 | $35.05 | $2.04 | — | $13,943.70 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+19.7; combo leftover $605.90; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 371 | $1.63 | $4.79 | — | $13,334.19 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; combo leftover $605.90; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 170 | $3.55 | $2.50 | — | $12,728.19 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+27.9; combo leftover $605.90; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 95 | $6.37 | $2.27 | — | $12,120.76 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+10.9; combo leftover $605.90; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ETON` | 9 | $64.55 | $2.02 | — | $11,537.80 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+4.4; combo leftover $605.90; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `WPM` | 3 | $156.51 | $2.00 | — | $11,066.27 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+17.4; combo leftover $605.90; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUZ` | 67 | $8.98 | $2.19 | — | $10,462.42 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+15.4; combo leftover $605.90; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `IAUX` | 318 | $1.90 | $4.10 | — | $9,854.11 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+16.4; combo leftover $605.90; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 87 | $9.42 | $2.25 | — | $9,032.32 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; combo leftover $821.18; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 34 | $24.11 | $2.09 | — | $8,210.49 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=+891.7; combo leftover $821.18; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 28 | $28.86 | $2.07 | — | $7,400.34 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+13.7; combo leftover $821.18; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `EOLS` | 94 | $8.72 | $2.27 | — | $6,578.38 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+13.0; combo leftover $821.18; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 6 | $118.52 | $2.01 | — | $5,865.26 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ⚪; ret5=+21.7; combo leftover $821.18; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 10 | $77.13 | $2.02 | — | $5,091.94 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; ⚪; ret5=+13.8; combo leftover $821.18; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 93 | $13.62 | $2.33 | — | $6,356.73 | — | news🔴; gate news=bad; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; combo leftover $1272.98; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 23 | $54.51 | $2.11 | — | $7,608.35 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+15.1; combo leftover $1272.98; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `BMO` | 7 | $175.01 | $2.06 | — | $8,831.35 | — | news🔴; gate news=bad; list earn_react; ret5=-7.0; combo leftover $1272.98; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `INTU` | 3 | $364.35 | $2.05 | — | $9,922.36 | — | news🔴; gate news=bad; list overnight,overnight_mega; 🔵; ret5=+10.2; combo leftover $1272.98; owner short_news_r_h3 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,922.36 | ▲ close $10,550.15 vs 09:30 $10,348.01 (session +264.10) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,922.36 | ▲ 09:30 equity $10,584.50 vs yday $10,550.15 (+34.35) | — | — |
| 2026-08-26 09:30 ET | **COVER** | `QTRX` | 240 | $2.83 | $3.10 | $+60.94 | $9,240.06 | ▲ +60.94 after sell → book $10,581.40; vs 09:30 mark -3.10 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `MRNA` | 5 | $154.20 | $2.00 | $-109.50 | $8,467.05 | ▼ -109.50 after sell → book $10,579.40; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 8 | $88.24 | $2.01 | $+2.81 | $7,759.12 | ▲ +2.81 after sell → book $10,577.39; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 19 | $38.41 | $2.05 | $-4.32 | $7,027.28 | ▼ -4.32 after sell → book $10,575.34; vs 09:30 mark -2.05 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 35 | $20.50 | $2.10 | $+9.77 | $6,307.69 | ▲ +9.77 after sell → book $10,573.24; vs 09:30 mark -2.10 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 27 | $26.00 | $2.07 | $+22.82 | $5,603.62 | ▲ +22.82 after sell → book $10,571.17; vs 09:30 mark -2.07 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 17 | $35.70 | $2.06 | $+6.95 | $6,208.46 | ▲ +6.95 after sell → book $10,569.11; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 371 | $1.75 | $4.86 | $+36.73 | $6,854.70 | ▲ +36.73 after sell → book $10,564.25; vs 09:30 mark -4.86 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GORO` | 170 | $3.77 | $2.54 | $+32.36 | $7,493.07 | ▲ +32.36 after sell → book $10,561.72; vs 09:30 mark -2.53 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZURA` | 95 | $6.13 | $2.30 | $-27.38 | $8,073.11 | ▼ -27.38 after sell → book $10,559.41; vs 09:30 mark -2.31 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ETON` | 9 | $63.60 | $2.04 | $-12.60 | $8,643.48 | ▼ -12.60 after sell → book $10,557.38; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `WPM` | 3 | $160.93 | $2.02 | $+9.24 | $9,124.25 | ▲ +9.24 after sell → book $10,555.36; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SUZ` | 67 | $9.03 | $2.21 | $-1.05 | $9,727.05 | ▼ -1.05 after sell → book $10,553.15; vs 09:30 mark -2.21 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `IAUX` | 318 | $1.87 | $4.17 | $-17.81 | $10,317.54 | ▼ -17.81 after sell → book $10,548.98; vs 09:30 mark -4.17 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 87 | $10.07 | $2.28 | $+52.02 | $11,191.36 | ▲ +52.02 after sell → book $10,546.71; vs 09:30 mark -2.27 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 34 | $26.61 | $2.11 | $+80.80 | $12,093.98 | ▲ +80.80 after sell → book $10,544.59; vs 09:30 mark -2.12 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZYME` | 28 | $27.56 | $2.09 | $-40.57 | $12,863.57 | ▼ -40.57 after sell → book $10,542.50; vs 09:30 mark -2.09 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EOLS` | 94 | $8.86 | $2.30 | $+8.59 | $13,694.11 | ▲ +8.59 after sell → book $10,540.20; vs 09:30 mark -2.30 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 6 | $119.80 | $2.03 | $+3.64 | $14,410.88 | ▲ +3.64 after sell → book $10,538.17; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 10 | $79.34 | $2.04 | $+18.04 | $15,202.24 | ▲ +18.04 after sell → book $10,536.13; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 872 | $5.81 | $11.25 | — | $10,124.68 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ⚪; ret5=+117.2; combo leftover $5067.41; owner union_join_vol_green_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 75 | $11.12 | $2.21 | — | $9,288.46 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; combo leftover $843.72; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 101 | $8.29 | $2.29 | — | $8,448.88 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; combo leftover $843.72; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `FWRD` | 48 | $17.41 | $2.13 | — | $7,611.06 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-9.2; combo leftover $843.72; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 75 | $11.22 | $2.21 | — | $6,767.35 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+16.8; combo leftover $843.72; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 3 | $267.02 | $2.00 | — | $5,964.29 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+16.7; combo leftover $843.72; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 7 | $118.50 | $2.01 | — | $5,132.78 | — | union ∩ news_g, no 🚨; gate news=good; list overnight,overnight_mega; 🔵; ret5=-2.7; combo leftover $843.72; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 4 | $213.94 | $2.05 | — | $5,986.49 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; combo leftover $1026.56; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 84 | $12.22 | $2.29 | — | $7,010.68 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.4; combo leftover $1026.56; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 202 | $5.08 | $2.68 | — | $8,034.16 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+17.6; combo leftover $1026.56; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 7 | $132.64 | $2.06 | — | $8,960.59 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+16.5; combo leftover $1026.56; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `CRM` | 5 | $199.94 | $2.05 | — | $9,958.23 | — | news🔴; gate news=bad; list overnight,overnight_mega; ret5=+2.1; combo leftover $1026.56; owner short_news_r_h3 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,958.23 | ▲ close $10,610.16 vs 09:30 $10,584.50 (session +109.27) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,958.23 | ▲ 09:30 equity $10,912.65 vs yday $10,610.16 (+302.49) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 872 | $6.50 | $11.44 | $+578.99 | $15,614.79 | ▲ +578.99 after sell → book $10,901.21; vs 09:30 mark -11.44 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 3 | $267.23 | $2.02 | $-3.39 | $16,414.46 | ▼ -3.39 after sell → book $10,899.19; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `DKS` | 42 | $128.73 | $2.12 | — | $11,005.69 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-32.2; combo leftover $5471.49; owner union_join_vol_green_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 44 | $41.44 | $2.12 | — | $9,180.21 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; ret5=+3.1; combo leftover $1834.28; owner union_news_g_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `AXTI` | 26 | $70.30 | $2.07 | — | $7,350.34 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=-11.2; combo leftover $1834.28; owner union_news_g_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `SRRK` | 30 | $60.00 | $2.08 | — | $5,548.26 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+6.2; combo leftover $1834.28; owner union_news_g_h1 | — |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 36 | $74.54 | $2.20 | — | $8,229.50 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=-0.1; combo leftover $2722.70; owner short_news_r_h3 | — |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 49 | $55.25 | $2.24 | — | $10,934.50 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=+2.1; combo leftover $2722.70; owner short_news_r_h3 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,934.50 | ▼ close $10,872.28 vs 09:30 $10,912.65 (session -14.08) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,934.50 | ▼ 09:30 equity $10,804.12 vs yday $10,872.28 (-68.16) | — | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 93 | $13.90 | $2.27 | $-30.17 | $9,639.53 | ▼ -30.17 after sell → book $10,801.85; vs 09:30 mark -2.27 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 23 | $52.49 | $2.06 | $+42.29 | $8,430.20 | ▲ +42.29 after sell → book $10,799.79; vs 09:30 mark -2.06 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `BMO` | 7 | $172.76 | $2.01 | $+11.67 | $7,218.87 | ▲ +11.67 after sell → book $10,797.78; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `INTU` | 3 | $347.82 | $2.00 | $+45.54 | $6,173.41 | ▲ +45.54 after sell → book $10,795.78; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FLNC` | 75 | $11.27 | $2.24 | $+6.80 | $7,016.43 | ▲ +6.80 after sell → book $10,793.55; vs 09:30 mark -2.23 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `FWRD` | 48 | $17.70 | $2.15 | $+9.63 | $7,863.87 | ▲ +9.63 after sell → book $10,791.39; vs 09:30 mark -2.16 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TRLV` | 75 | $11.00 | $2.24 | $-20.95 | $8,686.64 | ▼ -20.95 after sell → book $10,789.16; vs 09:30 mark -2.23 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 7 | $115.66 | $2.03 | $-23.92 | $9,494.23 | ▼ -23.92 after sell → book $10,787.13; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 42 | $132.80 | $2.17 | $+166.65 | $15,069.65 | ▲ +166.65 after sell → book $10,784.95; vs 09:30 mark -2.18 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AXTI` | 26 | $65.29 | $2.09 | $-134.42 | $16,765.10 | ▼ -134.42 after sell → book $10,782.86; vs 09:30 mark -2.09 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SRRK` | 30 | $58.75 | $2.10 | $-41.68 | $18,525.50 | ▼ -41.68 after sell → book $10,780.76; vs 09:30 mark -2.10 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 10 | $146.07 | $2.02 | — | $17,062.78 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+38.8; combo leftover $1543.79; owner union_join_vol_green_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 66 | $23.30 | $2.19 | — | $15,522.79 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+14.5; combo leftover $1543.79; owner union_join_vol_green_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 81 | $19.00 | $2.23 | — | $13,981.56 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; ret5=+7.5; combo leftover $1543.79; owner union_join_vol_green_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 62 | $24.69 | $2.18 | — | $12,448.60 | — | combo gate; gate join=good,vol=good,last_green=True; list earn_react; ret5=+5.8; combo leftover $1543.79; owner union_join_vol_green_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 31 | $32.90 | $2.08 | — | $11,426.62 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; combo leftover $1037.38; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 120 | $8.61 | $2.35 | — | $10,391.07 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.7; combo leftover $1037.38; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 7 | $141.76 | $2.01 | — | $9,396.74 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; combo leftover $1037.38; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 53 | $19.25 | $2.15 | — | $8,374.34 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+14.1; combo leftover $1037.38; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 55 | $18.75 | $2.15 | — | $7,340.93 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=-5.0; combo leftover $1037.38; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 35 | $28.91 | $2.10 | — | $6,326.99 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+9.2; combo leftover $1037.38; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 10 | $252.24 | $2.12 | — | $8,847.27 | — | news🔴; gate news=bad; list probable,yday_gainer; ⚪; ret5=+2.2; combo leftover $2689.82; owner short_news_r_h3 | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 89 | $30.18 | $2.37 | — | $11,530.92 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+12.1; combo leftover $2689.82; owner short_news_r_h3 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,530.92 | ▲ close $10,825.38 vs 09:30 $10,804.12 (session +70.57) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,530.92 | ▲ 09:30 equity $10,831.19 vs yday $10,825.38 (+5.81) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 101 | $9.50 | $2.32 | $+117.60 | $12,488.10 | ▲ +117.60 after sell → book $10,828.87; vs 09:30 mark -2.32 | union_news_g_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `BE` | 4 | $208.88 | $2.00 | $+16.19 | $11,650.58 | ▲ +16.19 after sell → book $10,826.87; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 84 | $11.10 | $2.24 | $+89.54 | $10,715.94 | ▲ +89.54 after sell → book $10,824.62; vs 09:30 mark -2.25 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 202 | $4.97 | $2.61 | $+15.93 | $9,708.38 | ▲ +15.93 after sell → book $10,822.02; vs 09:30 mark -2.60 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `NEM` | 7 | $127.45 | $2.01 | $+32.26 | $8,814.22 | ▲ +32.26 after sell → book $10,820.01; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `CRM` | 5 | $254.39 | $2.00 | $-276.31 | $7,540.27 | ▼ -276.31 after sell → book $10,818.00; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 44 | $42.00 | $2.15 | $+20.37 | $9,386.12 | ▲ +20.37 after sell → book $10,815.86; vs 09:30 mark -2.14 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 10 | $148.03 | $2.04 | $+15.54 | $10,864.38 | ▲ +15.54 after sell → book $10,813.81; vs 09:30 mark -2.05 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NCNO` | 66 | $22.66 | $2.21 | $-46.64 | $12,357.73 | ▼ -46.64 after sell → book $10,811.60; vs 09:30 mark -2.21 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TH` | 81 | $18.12 | $2.26 | $-75.37 | $13,823.59 | ▼ -75.37 after sell → book $10,809.34; vs 09:30 mark -2.26 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GAP` | 62 | $22.98 | $2.20 | $-110.39 | $15,246.16 | ▼ -110.39 after sell → book $10,807.15; vs 09:30 mark -2.19 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 31 | $31.15 | $2.10 | $-58.44 | $16,209.70 | ▼ -58.44 after sell → book $10,805.04; vs 09:30 mark -2.11 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OPTX` | 120 | $8.52 | $2.38 | $-15.53 | $17,229.72 | ▼ -15.53 after sell → book $10,802.66; vs 09:30 mark -2.38 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 7 | $132.30 | $2.03 | $-70.26 | $18,153.79 | ▼ -70.26 after sell → book $10,800.63; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 53 | $17.87 | $2.17 | $-77.46 | $19,098.73 | ▼ -77.46 after sell → book $10,798.46; vs 09:30 mark -2.17 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 55 | $19.25 | $2.17 | $+23.17 | $20,155.31 | ▲ +23.17 after sell → book $10,796.29; vs 09:30 mark -2.17 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 35 | $28.06 | $2.12 | $-33.96 | $21,135.29 | ▼ -33.96 after sell → book $10,794.17; vs 09:30 mark -2.12 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,135.29 | ▲ close $10,848.50 vs 09:30 $10,831.19 (session +54.33) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,135.29 | ▲ 09:30 equity $11,006.89 vs yday $10,848.50 (+158.39) | — | — |
| 2026-09-01 09:30 ET | **COVER** | `MT` | 36 | $73.22 | $2.10 | $+43.22 | $18,497.28 | ▲ +43.22 after sell → book $11,004.80; vs 09:30 mark -2.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `TX` | 49 | $54.76 | $2.14 | $+19.63 | $15,811.90 | ▲ +19.63 after sell → book $11,002.66; vs 09:30 mark -2.14 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,811.90 | ▲ close $11,017.60 vs 09:30 $11,006.89 (session +14.94) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,811.90 | ▲ 09:30 equity $11,071.38 vs yday $11,017.60 (+53.78) | — | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 10 | $235.71 | $2.02 | $+161.16 | $13,452.78 | ▲ +161.16 after sell → book $11,069.36; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 89 | $26.78 | $2.26 | $+297.98 | $11,067.10 | ▲ +297.98 after sell → book $11,067.10; vs 09:30 mark -2.26 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,067.10 | ▲ close $11,067.10 vs 09:30 $11,071.38 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,067.10 | ▲ 09:30 equity $11,067.10 vs yday $11,067.10 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 19 | $23.88 | $2.05 | — | $10,611.33 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $461.13; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 3 | $132.45 | $2.00 | — | $10,211.99 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; combo leftover $461.13; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 27 | $16.77 | $2.07 | — | $9,757.12 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; combo leftover $461.13; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 211 | $2.18 | $2.72 | — | $9,294.42 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; combo leftover $461.13; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 44 | $10.42 | $2.12 | — | $8,833.82 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; combo leftover $461.13; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 238 | $1.93 | $3.07 | — | $8,371.41 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; combo leftover $461.13; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `DUOL` | 2 | $161.54 | $2.00 | — | $8,046.33 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+12.0; combo leftover $461.13; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `ALMS` | 44 | $10.38 | $2.12 | — | $7,587.71 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ret5=-56.2; combo leftover $461.13; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 16 | $32.88 | $2.04 | — | $7,059.59 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+16.2; combo leftover $541.98; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `OPTX` | 71 | $7.59 | $2.20 | — | $6,518.50 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-11.5; combo leftover $541.98; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 34 | $15.87 | $2.09 | — | $5,976.83 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; combo leftover $541.98; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 1 | $351.74 | $1.99 | — | $5,623.10 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=+3.3; combo leftover $541.98; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 1 | $354.49 | $1.99 | — | $5,266.61 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-12.3; combo leftover $541.98; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 11 | $47.60 | $2.02 | — | $4,740.99 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-6.2; combo leftover $541.98; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 159 | $14.85 | $2.58 | — | $7,099.56 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; combo leftover $2370.50; owner short_news_r_h3 | — |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 1386 | $1.71 | $18.19 | — | $9,451.43 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+11.9; combo leftover $2370.50; owner short_news_r_h3 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,451.43 | ▲ close $11,240.73 vs 09:30 $11,067.10 (session +224.89) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,451.43 | ▲ 09:30 equity $11,269.13 vs yday $11,240.73 (+28.40) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 19 | $23.84 | $2.07 | $-4.87 | $9,902.32 | ▼ -4.87 after sell → book $11,267.06; vs 09:30 mark -2.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 3 | $130.03 | $2.02 | $-11.28 | $10,290.39 | ▼ -11.28 after sell → book $11,265.04; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 27 | $15.61 | $2.09 | $-35.48 | $10,709.77 | ▼ -35.48 after sell → book $11,262.95; vs 09:30 mark -2.09 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 211 | $2.16 | $2.77 | $-9.71 | $11,162.77 | ▼ -9.71 after sell → book $11,260.19; vs 09:30 mark -2.76 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 44 | $10.50 | $2.14 | $-0.74 | $11,622.62 | ▼ -0.74 after sell → book $11,258.04; vs 09:30 mark -2.15 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `BMEA` | 238 | $1.90 | $3.12 | $-13.33 | $12,071.70 | ▼ -13.33 after sell → book $11,254.92; vs 09:30 mark -3.12 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DUOL` | 2 | $157.46 | $2.02 | $-12.17 | $12,384.61 | ▼ -12.17 after sell → book $11,252.91; vs 09:30 mark -2.01 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ALMS` | 44 | $11.23 | $2.14 | $+33.36 | $12,876.59 | ▲ +33.36 after sell → book $11,250.77; vs 09:30 mark -2.14 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNXC` | 16 | $32.48 | $2.06 | $-10.50 | $13,394.21 | ▼ -10.50 after sell → book $11,248.71; vs 09:30 mark -2.06 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `OPTX` | 71 | $7.79 | $2.22 | $+9.77 | $13,945.07 | ▲ +9.77 after sell → book $11,246.48; vs 09:30 mark -2.23 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 1 | $359.70 | $2.01 | $+3.95 | $14,302.76 | ▲ +3.95 after sell → book $11,244.47; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 1 | $321.67 | $2.01 | $-36.83 | $14,622.42 | ▼ -36.83 after sell → book $11,242.46; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 11 | $53.85 | $2.04 | $+64.68 | $15,212.72 | ▲ +64.68 after sell → book $11,240.41; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 1 | $513.78 | $1.99 | — | $14,696.95 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; combo leftover $633.86; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 7 | $82.70 | $2.01 | — | $14,116.04 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+15.7; combo leftover $633.86; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 252 | $2.51 | $3.25 | — | $13,480.27 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+21.8; combo leftover $633.86; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `MDB` | 1 | $378.34 | $1.99 | — | $13,099.94 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-12.7; combo leftover $633.86; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 25 | $25.18 | $2.06 | — | $12,468.37 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+16.0; combo leftover $633.86; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 109 | $5.79 | $2.32 | — | $11,834.94 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+15.2; combo leftover $633.86; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `TDS` | 16 | $37.44 | $2.04 | — | $11,233.87 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+14.1; combo leftover $633.86; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `AHCO` | 100 | $6.32 | $2.29 | — | $10,599.58 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+10.7; combo leftover $633.86; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 4 | $263.36 | $2.00 | — | $9,544.13 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; combo leftover $1059.96; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 546 | $1.94 | $7.04 | — | $8,477.85 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; ret5=+18.3; combo leftover $1059.96; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 7 | $137.35 | $2.01 | — | $7,514.39 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+5.4; combo leftover $1059.96; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 4 | $236.82 | $2.00 | — | $6,565.11 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+8.1; combo leftover $1059.96; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 14 | $75.65 | $2.03 | — | $5,503.98 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+6.0; combo leftover $1059.96; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 589 | $4.67 | $7.79 | — | $8,246.81 | — | news🔴; gate news=bad; list yday_gainer; ret5=+11.9; combo leftover $2751.99; owner short_news_r_h3 | — |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 35 | $76.55 | $2.20 | — | $10,923.86 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; combo leftover $2751.99; owner short_news_r_h3 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,923.86 | ▲ close $11,385.51 vs 09:30 $11,269.13 (session +188.14) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,923.86 | ▲ 09:30 equity $11,427.99 vs yday $11,385.51 (+42.48) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 34 | $16.74 | $2.11 | $+25.38 | $11,490.91 | ▲ +25.38 after sell → book $11,425.88; vs 09:30 mark -2.11 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 1 | $521.15 | $2.01 | $+3.36 | $12,010.05 | ▲ +3.36 after sell → book $11,423.87; vs 09:30 mark -2.01 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TARS` | 7 | $89.67 | $2.03 | $+44.75 | $12,635.71 | ▲ +44.75 after sell → book $11,421.84; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BRR` | 252 | $2.66 | $3.30 | $+31.25 | $13,302.72 | ▲ +31.25 after sell → book $11,418.53; vs 09:30 mark -3.31 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MDB` | 1 | $360.75 | $2.01 | $-21.60 | $13,661.46 | ▼ -21.60 after sell → book $11,416.52; vs 09:30 mark -2.01 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 25 | $26.44 | $2.08 | $+27.35 | $14,320.38 | ▲ +27.35 after sell → book $11,414.44; vs 09:30 mark -2.08 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 109 | $5.81 | $2.35 | $-2.48 | $14,951.32 | ▼ -2.48 after sell → book $11,412.09; vs 09:30 mark -2.35 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TDS` | 16 | $37.75 | $2.06 | $+0.86 | $15,553.26 | ▲ +0.86 after sell → book $11,410.03; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `AHCO` | 100 | $6.48 | $2.32 | $+11.39 | $16,198.95 | ▲ +11.39 after sell → book $11,407.72; vs 09:30 mark -2.31 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 4 | $253.72 | $2.02 | $-42.58 | $17,211.81 | ▼ -42.58 after sell → book $11,405.70; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 546 | $1.94 | $7.14 | $-14.19 | $18,263.90 | ▼ -14.19 after sell → book $11,398.55; vs 09:30 mark -7.15 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 4 | $267.76 | $2.02 | $+119.74 | $19,332.92 | ▲ +119.74 after sell → book $11,396.53; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,332.92 | ▲ close $11,612.87 vs 09:30 $11,427.99 (session +216.34) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,332.92 | ▲ 09:30 equity $11,680.10 vs yday $11,612.87 (+67.23) | — | — |
| 2026-09-09 09:30 ET | **COVER** | `SLN` | 159 | $13.60 | $2.47 | $+193.71 | $17,168.05 | ▲ +193.71 after sell → book $11,677.63; vs 09:30 mark -2.47 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 1386 | $1.58 | $17.88 | $+144.11 | $14,960.29 | ▲ +144.11 after sell → book $11,659.75; vs 09:30 mark -17.88 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MSTR` | 7 | $141.82 | $2.03 | $+27.25 | $15,951.00 | ▲ +27.25 after sell → book $11,657.72; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 14 | $76.60 | $2.05 | $+9.22 | $17,021.35 | ▲ +9.22 after sell → book $11,655.67; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,021.35 | ▲ close $11,683.14 vs 09:30 $11,680.10 (session +27.47) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,021.35 | ▲ 09:30 equity $11,765.66 vs yday $11,683.14 (+82.52) | — | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 589 | $4.36 | $7.60 | $+167.20 | $14,445.71 | ▲ +167.20 after sell → book $11,758.06; vs 09:30 mark -7.60 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PIPR` | 35 | $76.79 | $2.10 | $-12.69 | $11,755.97 | ▼ -12.69 after sell → book $11,755.97; vs 09:30 mark -2.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,755.97 | ▲ close $11,755.97 vs 09:30 $11,765.66 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,755.97 | ▲ 09:30 equity $11,755.97 vs yday $11,755.97 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 20 | $23.63 | $2.05 | — | $11,281.32 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; ret5=-6.3; combo leftover $489.83; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 181 | $2.70 | $2.53 | — | $10,790.08 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+118.8; combo leftover $489.83; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 44 | $10.95 | $2.12 | — | $10,306.16 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+20.8; combo leftover $489.83; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 99 | $4.91 | $2.29 | — | $9,817.78 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+76.3; combo leftover $489.83; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 5 | $84.27 | $2.00 | — | $9,394.43 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ret5=+17.2; combo leftover $489.83; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `ASO` | 8 | $54.91 | $2.01 | — | $8,953.14 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+24.3; combo leftover $489.83; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 79 | $6.16 | $2.23 | — | $8,464.27 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+36.4; combo leftover $489.83; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `COO` | 8 | $54.66 | $2.01 | — | $8,024.97 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-22.3; combo leftover $489.83; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 4 | $164.43 | $2.00 | — | $7,365.25 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,earn_react; ⚪; ret5=+4.9; combo leftover $668.75; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 327 | $2.04 | $4.22 | — | $6,693.95 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; combo leftover $668.75; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 315 | $2.12 | $4.06 | — | $6,022.09 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; combo leftover $668.75; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `AVTR` | 44 | $15.01 | $2.12 | — | $5,359.53 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+4.9; combo leftover $668.75; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 2 | $242.17 | $2.00 | — | $4,873.19 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; ret5=-11.1; combo leftover $668.75; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 4 | $135.71 | $2.00 | — | $4,328.35 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; ret5=-9.2; combo leftover $668.75; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 7 | $112.83 | $2.05 | — | $5,116.14 | — | news🔴; gate news=bad; list yday_gainer,ohlc_hot; ret5=+11.7; combo leftover $865.67; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `RWT` | 245 | $3.52 | $3.23 | — | $5,975.31 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-19.2; combo leftover $865.67; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `CRDL` | 426 | $2.03 | $5.60 | — | $6,834.49 | — | news🔴; gate news=bad; list yday_mover; ret5=-8.8; combo leftover $865.67; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BKV` | 34 | $24.97 | $2.14 | — | $7,681.33 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.8; combo leftover $865.67; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `MYGN` | 256 | $3.37 | $3.38 | — | $8,540.67 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+4.0; combo leftover $865.67; owner short_news_r_h3 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,540.67 | ▼ close $11,578.09 vs 09:30 $11,755.97 (session -127.82) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,540.67 | ▲ 09:30 equity $11,605.90 vs yday $11,578.09 (+27.81) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `TYRA` | 20 | $23.20 | $2.07 | $-12.72 | $9,002.60 | ▼ -12.72 after sell → book $11,603.83; vs 09:30 mark -2.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `INDP` | 181 | $2.80 | $2.57 | $+12.99 | $9,506.83 | ▲ +12.99 after sell → book $11,601.26; vs 09:30 mark -2.57 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `WLTH` | 44 | $10.29 | $2.14 | $-33.30 | $9,957.45 | ▼ -33.30 after sell → book $11,599.12; vs 09:30 mark -2.14 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BNC` | 99 | $5.03 | $2.31 | $+7.28 | $10,453.11 | ▲ +7.28 after sell → book $11,596.81; vs 09:30 mark -2.31 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SWKS` | 5 | $86.06 | $2.02 | $+4.92 | $10,881.38 | ▲ +4.92 after sell → book $11,594.78; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ASO` | 8 | $54.75 | $2.03 | $-5.33 | $11,317.35 | ▼ -5.33 after sell → book $11,592.75; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 79 | $6.02 | $2.25 | $-15.54 | $11,790.68 | ▼ -15.54 after sell → book $11,590.50; vs 09:30 mark -2.25 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `COO` | 8 | $54.78 | $2.03 | $-3.09 | $12,226.88 | ▼ -3.09 after sell → book $11,588.46; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 4 | $141.42 | $2.02 | $-96.06 | $12,790.54 | ▼ -96.06 after sell → book $11,586.44; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 327 | $2.01 | $4.28 | $-18.31 | $13,443.53 | ▼ -18.31 after sell → book $11,582.16; vs 09:30 mark -4.28 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 315 | $2.05 | $4.13 | $-30.24 | $14,085.15 | ▼ -30.24 after sell → book $11,578.03; vs 09:30 mark -4.13 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 2 | $261.51 | $2.02 | $+34.67 | $14,606.16 | ▲ +34.67 after sell → book $11,576.02; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RH` | 4 | $131.40 | $2.02 | $-21.26 | $15,129.73 | ▼ -21.26 after sell → book $11,573.99; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,129.73 | ▼ close $11,474.70 vs 09:30 $11,605.90 (session -99.29) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,129.73 | ▲ 09:30 equity $11,477.35 vs yday $11,474.70 (+2.65) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,129.73 | ▼ close $11,415.44 vs 09:30 $11,477.35 (session -61.91) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,129.73 | ▲ 09:30 equity $11,432.31 vs yday $11,415.44 (+16.87) | — | — |
| 2026-09-16 09:30 ET | **COVER** | `QRVO` | 7 | $118.18 | $2.01 | $-41.48 | $14,300.46 | ▼ -41.48 after sell → book $11,430.30; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `RWT` | 245 | $3.98 | $3.16 | $-119.10 | $13,322.20 | ▼ -119.10 after sell → book $11,427.14; vs 09:30 mark -3.16 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `CRDL` | 426 | $1.85 | $5.50 | $+65.58 | $12,528.61 | ▲ +65.58 after sell → book $11,421.65; vs 09:30 mark -5.49 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `BKV` | 34 | $24.42 | $2.09 | $+14.47 | $11,696.24 | ▲ +14.47 after sell → book $11,419.56; vs 09:30 mark -2.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `MYGN` | 256 | $3.75 | $3.30 | $-103.96 | $10,732.93 | ▼ -103.96 after sell → book $11,416.25; vs 09:30 mark -3.31 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 5 | $77.12 | $2.00 | — | $10,345.33 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; ret5=+7.2; combo leftover $447.21; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 76 | $5.87 | $2.22 | — | $9,896.99 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+3.1; combo leftover $447.21; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 5 | $87.40 | $2.00 | — | $9,457.99 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+3.2; combo leftover $447.21; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 16 | $27.09 | $2.04 | — | $9,022.51 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; combo leftover $447.21; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 5 | $89.38 | $2.00 | — | $8,573.60 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; combo leftover $447.21; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 19 | $23.29 | $2.05 | — | $8,129.05 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+16.1; combo leftover $447.21; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 13 | $33.14 | $2.03 | — | $7,696.20 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=-2.9; combo leftover $447.21; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `CAI` | 15 | $28.16 | $2.04 | — | $7,271.76 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ret5=+14.8; combo leftover $447.21; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 34 | $26.27 | $2.09 | — | $6,376.49 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+10.0; combo leftover $908.97; owner union_news_g_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SION` | 130 | $6.95 | $2.38 | — | $5,470.61 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-5.8; combo leftover $908.97; owner union_news_g_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 22 | $39.99 | $2.06 | — | $4,588.77 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+9.3; combo leftover $908.97; owner union_news_g_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 4 | $189.17 | $2.00 | — | $3,830.09 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+7.9; combo leftover $908.97; owner union_news_g_h1 | — |
| 2026-09-16 09:30 ET | **SHORT** | `BBNX` | 102 | $18.61 | $2.38 | — | $5,725.93 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; ret5=-0.5; combo leftover $1915.05; owner short_news_r_h3 | — |
| 2026-09-16 09:30 ET | **SHORT** | `GFR` | 280 | $6.83 | $3.73 | — | $7,634.61 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+11.2; combo leftover $1915.05; owner short_news_r_h3 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,634.61 | ▼ close $11,053.98 vs 09:30 $11,432.31 (session -331.26) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,634.61 | ▲ 09:30 equity $11,131.58 vs yday $11,053.98 (+77.60) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `RDNT` | 5 | $76.44 | $2.02 | $-7.43 | $8,014.78 | ▼ -7.43 after sell → book $11,129.56; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 76 | $5.58 | $2.24 | $-26.50 | $8,436.62 | ▼ -26.50 after sell → book $11,127.31; vs 09:30 mark -2.25 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 5 | $83.20 | $2.02 | $-25.03 | $8,850.59 | ▼ -25.03 after sell → book $11,125.29; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ADPT` | 16 | $28.23 | $2.06 | $+14.14 | $9,300.22 | ▲ +14.14 after sell → book $11,123.23; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 5 | $86.76 | $2.02 | $-17.13 | $9,731.99 | ▼ -17.13 after sell → book $11,121.21; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 19 | $24.09 | $2.07 | $+11.09 | $10,187.63 | ▲ +11.09 after sell → book $11,119.14; vs 09:30 mark -2.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `CAI` | 15 | $28.59 | $2.06 | $+2.43 | $10,614.50 | ▲ +2.43 after sell → book $11,117.08; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 34 | $26.51 | $2.11 | $+3.96 | $11,513.73 | ▲ +3.96 after sell → book $11,114.97; vs 09:30 mark -2.11 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SION` | 130 | $7.27 | $2.41 | $+36.81 | $12,456.42 | ▲ +36.81 after sell → book $11,112.56; vs 09:30 mark -2.41 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 22 | $37.57 | $2.08 | $-57.37 | $13,280.89 | ▼ -57.37 after sell → book $11,110.49; vs 09:30 mark -2.07 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 4 | $190.35 | $2.02 | $+0.70 | $14,040.26 | ▲ +0.70 after sell → book $11,108.46; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 3 | $170.85 | $2.00 | — | $13,525.71 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; combo leftover $668.58; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 2 | $233.85 | $2.00 | — | $13,056.02 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; ret5=+11.7; combo leftover $668.58; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 4 | $147.61 | $2.00 | — | $12,463.58 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; ret5=+17.7; combo leftover $668.58; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 88 | $7.59 | $2.25 | — | $11,793.40 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; 🔵; ret5=+9.4; combo leftover $668.58; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 25 | $25.95 | $2.06 | — | $11,142.59 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; combo leftover $668.58; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `CIFR` | 37 | $18.04 | $2.10 | — | $10,473.19 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.1; combo leftover $668.58; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRKR` | 10 | $61.90 | $2.02 | — | $9,852.17 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; 🔵; ret5=+11.5; combo leftover $668.58; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `LITE` | 1 | $934.88 | $1.99 | — | $8,915.30 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=-7.0; combo leftover $985.22; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `TNDM` | 55 | $17.72 | $2.15 | — | $7,938.54 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=-8.3; combo leftover $985.22; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `BAK` | 556 | $1.77 | $7.17 | — | $6,947.25 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-10.2; combo leftover $985.22; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 4 | $238.60 | $2.00 | — | $5,990.85 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-11.6; combo leftover $985.22; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 44 | $22.12 | $2.12 | — | $5,015.45 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+10.5; combo leftover $985.22; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **SHORT** | `BULL` | 315 | $7.95 | $4.20 | — | $7,515.49 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-18.4; combo leftover $2507.72; owner short_news_r_h3 | — |
| 2026-09-17 09:30 ET | **SHORT** | `LEN` | 30 | $81.00 | $2.18 | — | $9,943.32 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-3.0; combo leftover $2507.72; owner short_news_r_h3 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,943.32 | ▲ close $11,274.40 vs 09:30 $11,131.58 (session +202.20) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,943.32 | ▲ 09:30 equity $11,379.07 vs yday $11,274.40 (+104.67) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 44 | $15.87 | $2.14 | $+33.58 | $10,639.45 | ▲ +33.58 after sell → book $11,376.92; vs 09:30 mark -2.15 | union_news_g_h1: dropped from list after 5 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 13 | $39.50 | $2.05 | $+78.60 | $11,150.91 | ▲ +78.60 after sell → book $11,374.88; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 3 | $182.33 | $2.02 | $+30.42 | $11,695.88 | ▲ +30.42 after sell → book $11,372.86; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ILMN` | 2 | $249.13 | $2.02 | $+26.55 | $12,192.12 | ▲ +26.55 after sell → book $11,370.84; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 4 | $146.50 | $2.02 | $-8.46 | $12,776.10 | ▼ -8.46 after sell → book $11,368.82; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 88 | $7.98 | $2.28 | $+29.79 | $13,476.06 | ▲ +29.79 after sell → book $11,366.54; vs 09:30 mark -2.28 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ARQT` | 25 | $26.14 | $2.08 | $+0.60 | $14,127.47 | ▲ +0.60 after sell → book $11,364.45; vs 09:30 mark -2.09 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `CIFR` | 37 | $17.80 | $2.12 | $-12.92 | $14,783.95 | ▼ -12.92 after sell → book $11,362.33; vs 09:30 mark -2.12 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BRKR` | 10 | $63.37 | $2.04 | $+10.64 | $15,415.61 | ▲ +10.64 after sell → book $11,360.29; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `LITE` | 1 | $915.66 | $2.01 | $-23.23 | $16,329.26 | ▼ -23.23 after sell → book $11,358.28; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TNDM` | 55 | $17.13 | $2.17 | $-36.78 | $17,269.24 | ▼ -36.78 after sell → book $11,356.11; vs 09:30 mark -2.17 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BAK` | 556 | $1.77 | $7.27 | $-14.45 | $18,246.08 | ▼ -14.45 after sell → book $11,348.83; vs 09:30 mark -7.28 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 4 | $236.80 | $2.02 | $-11.22 | $19,191.26 | ▼ -11.22 after sell → book $11,346.81; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 56 | $14.07 | $2.16 | — | $18,401.18 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; combo leftover $799.64; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 54 | $14.79 | $2.15 | — | $17,600.37 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; combo leftover $799.64; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 3 | $219.62 | $2.00 | — | $16,939.51 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; combo leftover $799.64; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 9 | $85.00 | $2.02 | — | $16,172.49 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=+18.3; combo leftover $799.64; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 202 | $3.95 | $2.61 | — | $15,371.99 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; combo leftover $799.64; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 27 | $29.32 | $2.07 | — | $14,578.28 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; combo leftover $799.64; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 263 | $3.04 | $3.39 | — | $13,776.68 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; combo leftover $799.64; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `VITL` | 70 | $11.38 | $2.20 | — | $12,977.88 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+19.5; combo leftover $799.64; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 430 | $7.54 | $5.55 | — | $9,732.28 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; 🔵; ret5=-20.9; combo leftover $3244.47; owner union_news_g_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 155 | $20.91 | $2.46 | — | $6,488.78 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+8.6; combo leftover $3244.47; owner union_news_g_h1 | — |
| 2026-09-18 09:30 ET | **SHORT** | `FIVN` | 164 | $34.44 | $2.71 | — | $12,134.23 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $5660.11; owner short_news_r_h3 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,134.23 | ▲ close $11,574.44 vs 09:30 $11,379.07 (session +256.93) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,134.23 | ▲ 09:30 equity $11,620.72 vs yday $11,574.44 (+46.28) | — | — |
| 2026-09-21 09:30 ET | **COVER** | `BBNX` | 102 | $22.11 | $2.30 | $-361.68 | $9,876.71 | ▼ -361.68 after sell → book $11,618.42; vs 09:30 mark -2.30 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `GFR` | 280 | $6.55 | $3.61 | $+71.06 | $8,039.10 | ▲ +71.06 after sell → book $11,614.81; vs 09:30 mark -3.61 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 44 | $22.78 | $2.14 | $+24.78 | $9,039.28 | ▲ +24.78 after sell → book $11,612.67; vs 09:30 mark -2.14 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 56 | $13.90 | $2.18 | $-13.86 | $9,815.50 | ▼ -13.86 after sell → book $11,610.49; vs 09:30 mark -2.18 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 54 | $14.58 | $2.17 | $-15.66 | $10,600.65 | ▼ -15.66 after sell → book $11,608.32; vs 09:30 mark -2.17 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VICR` | 3 | $230.25 | $2.02 | $+27.87 | $11,289.38 | ▲ +27.87 after sell → book $11,606.30; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ECO` | 9 | $82.83 | $2.04 | $-23.58 | $12,032.81 | ▼ -23.58 after sell → book $11,604.26; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `EYPT` | 202 | $3.87 | $2.65 | $-21.42 | $12,811.90 | ▼ -21.42 after sell → book $11,601.61; vs 09:30 mark -2.65 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 27 | $29.43 | $2.09 | $-1.19 | $13,604.42 | ▼ -1.19 after sell → book $11,599.52; vs 09:30 mark -2.09 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 263 | $4.00 | $3.45 | $+246.96 | $14,652.97 | ▲ +246.96 after sell → book $11,596.07; vs 09:30 mark -3.45 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VITL` | 70 | $12.05 | $2.22 | $+42.48 | $15,494.25 | ▲ +42.48 after sell → book $11,593.85; vs 09:30 mark -2.22 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FLNC` | 430 | $7.36 | $5.64 | $-86.44 | $18,653.41 | ▼ -86.44 after sell → book $11,588.21; vs 09:30 mark -5.64 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 155 | $21.65 | $2.51 | $+109.74 | $22,006.65 | ▲ +109.74 after sell → book $11,585.70; vs 09:30 mark -2.51 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 5 | $157.87 | $2.00 | — | $21,215.30 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; ret5=+6.5; combo leftover $916.94; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 10 | $88.83 | $2.02 | — | $20,324.98 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; ret5=+7.6; combo leftover $916.94; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 98 | $9.31 | $2.28 | — | $19,410.31 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=+5.3; combo leftover $916.94; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 68 | $13.47 | $2.19 | — | $18,491.82 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; combo leftover $916.94; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 91 | $9.99 | $2.26 | — | $17,580.47 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+5.3; combo leftover $916.94; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 54 | $16.91 | $2.15 | — | $16,665.17 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+50.5; combo leftover $916.94; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `USDE` | 70 | $13.05 | $2.20 | — | $15,749.47 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+40.4; combo leftover $916.94; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `GEMI` | 159 | $5.75 | $2.47 | — | $14,831.96 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+30.3; combo leftover $916.94; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 47 | $25.95 | $2.13 | — | $13,610.18 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ret5=+1.2; combo leftover $1236.00; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `AMTX` | 574 | $2.15 | $7.40 | — | $12,368.68 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+7.5; combo leftover $1236.00; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `MARA` | 88 | $13.94 | $2.25 | — | $11,139.70 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,ohlc_hot; 🔵; ret5=+10.5; combo leftover $1236.00; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SION` | 205 | $6.00 | $2.64 | — | $9,907.06 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-24.1; combo leftover $1236.00; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 6 | $190.30 | $2.01 | — | $8,763.25 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+10.6; combo leftover $1236.00; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 5 | $230.25 | $2.00 | — | $7,609.99 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+12.5; combo leftover $1236.00; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 349 | $8.26 | $4.66 | — | $10,488.07 | — | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $2887.42; owner short_news_r_h3 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AMD` | 4 | $583.88 | $2.09 | — | $12,821.50 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+8.5; combo leftover $2887.42; owner short_news_r_h3 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,821.50 | ▼ close $11,123.41 vs 09:30 $11,620.72 (session -419.50) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,821.50 | ▲ 09:30 equity $11,167.35 vs yday $11,123.41 (+43.94) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `BULL` | 315 | $8.28 | $4.06 | $-110.64 | $10,210.81 | ▼ -110.64 after sell → book $11,163.28; vs 09:30 mark -4.07 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SELL** | `SBET` | 91 | $9.91 | $2.29 | $-11.83 | $11,110.33 | ▼ -11.83 after sell → book $11,161.00; vs 09:30 mark -2.28 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `USDE` | 70 | $12.99 | $2.22 | $-8.62 | $12,017.41 | ▼ -8.62 after sell → book $11,158.77; vs 09:30 mark -2.23 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `GEMI` | 159 | $6.05 | $2.50 | $+42.73 | $12,977.65 | ▲ +42.73 after sell → book $11,156.27; vs 09:30 mark -2.50 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `MARA` | 88 | $13.13 | $2.28 | $-75.81 | $14,130.81 | ▼ -75.81 after sell → book $11,153.99; vs 09:30 mark -2.28 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `SION` | 205 | $5.99 | $2.69 | $-7.38 | $15,356.07 | ▼ -7.38 after sell → book $11,151.30; vs 09:30 mark -2.69 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `ARM` | 2 | $319.41 | $2.00 | — | $14,715.26 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+35.1; combo leftover $853.12; owner union_join_vol_green_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `FSLY` | 30 | $28.02 | $2.08 | — | $13,872.58 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; ret5=+10.3; combo leftover $853.12; owner union_join_vol_green_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `META` | 1 | $731.40 | $1.99 | — | $13,139.18 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; ret5=+11.4; combo leftover $853.12; owner union_join_vol_green_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 1084 | $1.01 | $13.98 | — | $12,030.36 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; ret5=+14.3; combo leftover $1094.93; owner union_news_g_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `MRNA` | 6 | $168.50 | $2.01 | — | $11,017.35 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+17.9; combo leftover $1094.93; owner union_news_g_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `DGXX` | 254 | $4.30 | $3.28 | — | $9,921.88 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+16.9; combo leftover $1094.93; owner union_news_g_h1 | — |
| 2026-09-22 09:30 ET | **SHORT** | `USFD` | 29 | $93.97 | $2.18 | — | $12,644.82 | — | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $2781.49; owner short_news_r_h3 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,644.82 | ▼ close $11,044.90 vs 09:30 $11,167.35 (session -78.88) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,644.82 | ▼ 09:30 equity $10,618.40 vs yday $11,044.90 (-426.50) | — | — |
| 2026-09-23 09:30 ET | **COVER** | `LEN` | 30 | $82.00 | $2.08 | $-34.26 | $10,182.74 | ▼ -34.26 after sell → book $10,616.32; vs 09:30 mark -2.08 | short_news_r_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BKKT` | 98 | $9.50 | $2.31 | $+14.03 | $11,111.43 | ▲ +14.03 after sell → book $10,614.01; vs 09:30 mark -2.31 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BTDR` | 68 | $12.84 | $2.22 | $-47.59 | $11,982.34 | ▼ -47.59 after sell → book $10,611.80; vs 09:30 mark -2.21 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 54 | $16.92 | $2.17 | $-3.78 | $12,893.85 | ▼ -3.78 after sell → book $10,609.63; vs 09:30 mark -2.17 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `GLXY` | 47 | $26.58 | $2.15 | $+25.33 | $14,140.95 | ▲ +25.33 after sell → book $10,607.47; vs 09:30 mark -2.16 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `AMTX` | 574 | $2.09 | $7.51 | $-49.35 | $15,333.10 | ▼ -49.35 after sell → book $10,599.96; vs 09:30 mark -7.51 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 6 | $174.50 | $2.03 | $-98.84 | $16,378.08 | ▼ -98.84 after sell → book $10,597.94; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `VICR` | 5 | $266.50 | $2.03 | $+177.22 | $17,708.55 | ▲ +177.22 after sell → book $10,595.91; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `ARM` | 2 | $331.78 | $2.02 | $+20.73 | $18,370.09 | ▲ +20.73 after sell → book $10,593.89; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `FSLY` | 30 | $25.90 | $2.10 | $-67.78 | $19,144.99 | ▼ -67.78 after sell → book $10,591.79; vs 09:30 mark -2.10 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `META` | 1 | $747.60 | $2.01 | $+12.19 | $19,890.58 | ▲ +12.19 after sell → book $10,589.78; vs 09:30 mark -2.01 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `IVVD` | 1084 | $0.95 | $13.74 | $-92.76 | $20,906.64 | ▼ -92.76 after sell → book $10,576.04; vs 09:30 mark -13.74 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `MRNA` | 6 | $183.41 | $2.03 | $+85.39 | $22,005.04 | ▲ +85.39 after sell → book $10,574.01; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 77 | $15.72 | $2.22 | — | $20,792.38 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; combo leftover $1222.50; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 59 | $20.65 | $2.17 | — | $19,571.87 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; combo leftover $1222.50; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `TNGX` | 48 | $25.40 | $2.13 | — | $18,350.53 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; combo leftover $1222.50; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `NMRA` | 1591 | $0.77 | $16.99 | — | $17,111.65 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; combo leftover $1222.50; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 29 | $41.76 | $2.08 | — | $15,898.54 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+36.4; combo leftover $1222.50; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `BFLY` | 123 | $9.90 | $2.36 | — | $14,678.48 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; combo leftover $1222.50; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 184 | $7.95 | $2.54 | — | $13,213.13 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ⚪; ret5=+12.4; combo leftover $1467.85; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `CMPX` | 1203 | $1.22 | $15.52 | — | $11,729.96 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; 🔵; ret5=-33.0; combo leftover $1467.85; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `VERI` | 1129 | $1.30 | $14.56 | — | $10,247.69 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.3; combo leftover $1467.85; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLSH` | 36 | $40.00 | $2.10 | — | $8,805.59 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+6.7; combo leftover $1467.85; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 7 | $196.78 | $2.01 | — | $7,426.12 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $1467.85; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **SHORT** | `HALO` | 44 | $116.85 | $2.31 | — | $12,565.21 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $5254.67; owner short_news_r_h3 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,565.21 | ▼ close $10,200.91 vs 09:30 $10,618.40 (session -306.11) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,565.21 | ▼ 09:30 equity $10,002.04 vs yday $10,200.91 (-198.87) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 5 | $163.95 | $2.02 | $+26.37 | $13,382.93 | ▲ +26.37 after sell → book $10,000.01; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 10 | $87.67 | $2.04 | $-15.61 | $14,257.64 | ▼ -15.61 after sell → book $9,997.97; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **COVER** | `AMD` | 4 | $600.27 | $2.00 | $-69.66 | $11,854.56 | ▼ -69.66 after sell → book $9,995.97; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `DGXX` | 254 | $4.12 | $3.33 | $-52.33 | $12,897.71 | ▼ -52.33 after sell → book $9,992.64; vs 09:30 mark -3.33 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 77 | $14.38 | $2.24 | $-107.64 | $14,002.73 | ▼ -107.64 after sell → book $9,990.40; vs 09:30 mark -2.24 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 59 | $20.52 | $2.19 | $-12.02 | $15,211.22 | ▼ -12.02 after sell → book $9,988.21; vs 09:30 mark -2.19 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `TNGX` | 48 | $23.99 | $2.15 | $-71.97 | $16,360.59 | ▼ -71.97 after sell → book $9,986.06; vs 09:30 mark -2.15 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `NMRA` | 1591 | $0.75 | $16.92 | $-68.91 | $17,530.56 | ▼ -68.91 after sell → book $9,969.14; vs 09:30 mark -16.92 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 29 | $36.02 | $2.10 | $-170.49 | $18,573.19 | ▼ -170.49 after sell → book $9,967.05; vs 09:30 mark -2.09 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BFLY` | 123 | $9.12 | $2.39 | $-100.69 | $19,692.56 | ▼ -100.69 after sell → book $9,964.66; vs 09:30 mark -2.39 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 184 | $7.38 | $2.58 | $-110.01 | $21,047.89 | ▼ -110.01 after sell → book $9,962.07; vs 09:30 mark -2.59 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CMPX` | 1203 | $1.17 | $15.73 | $-91.40 | $22,439.67 | ▼ -91.40 after sell → book $9,946.34; vs 09:30 mark -15.73 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VERI` | 1129 | $1.27 | $14.76 | $-63.20 | $23,858.74 | ▼ -63.20 after sell → book $9,931.58; vs 09:30 mark -14.76 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BLSH` | 36 | $39.27 | $2.12 | $-30.50 | $25,270.34 | ▼ -30.50 after sell → book $9,929.46; vs 09:30 mark -2.12 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 7 | $192.26 | $2.03 | $-35.68 | $26,614.13 | ▼ -35.68 after sell → book $9,927.43; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26,614.13 | ▼ close $9,684.39 vs 09:30 $10,002.04 (session -243.04) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,848.24 | ▼ 09:30 equity $7,798.63 vs yday $7,826.98 (-28.35) | 09:30 open · cash $18,848.24 (unchanged overnight, no fees) · equity $7,798.63 vs prior close $7,826.98 (-28.35) | — |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 203 | $3.86 | $2.62 | — | $18,062.04 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; combo leftover $785.34; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 48 | $16.21 | $2.13 | — | $17,281.83 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; combo leftover $785.34; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 29 | $26.27 | $2.08 | — | $16,517.92 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; combo leftover $785.34; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TWST` | 4 | $184.00 | $2.00 | — | $15,779.92 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.3; combo leftover $785.34; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GRAL` | 6 | $123.50 | $2.01 | — | $15,036.91 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+56.6; combo leftover $785.34; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `QMCO` | 26 | $29.80 | $2.07 | — | $14,260.04 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+18.2; combo leftover $785.34; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CYPH` | 196 | $4.00 | $2.58 | — | $13,472.48 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+32.9; combo leftover $785.34; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CDNA` | 12 | $61.33 | $2.03 | — | $12,734.50 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+13.1; combo leftover $785.34; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 7 | $272.16 | $2.01 | — | $10,827.37 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+11.7; combo leftover $2122.42; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `RKLB` | 28 | $74.15 | $2.07 | — | $8,749.09 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+8.5; combo leftover $2122.42; owner union_news_g_h1 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 2 | $887.00 | $2.00 | — | $6,973.10 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=+0.3; combo leftover $2122.42; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `RSKD` | 495 | $7.85 | $6.61 | — | $10,852.24 | — | news🔴; gate news=bad; list yday_gainer; 🔵; ⚪; ret5=+25.4; combo leftover $3887.52; owner short_news_r_h3 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,852.24 | ▲ close $7,891.00 vs 09:30 $7,798.63 (session +122.57) | 16:00 close · cash $10,852.24 · equity $7,891.00 vs 09:30 $7,798.63 (+92.37; session marks +122.57) · 17 name(s) marked open→close (per-name table). AEHL×287 09:30 $9.05 → close $9.36 -88.97; BAND×37 09:30 $61.83 → close $61.83 -0.00; HALO×18 09:30 $115.36 → close $113.90 +26.28; PAYX×19 09:30 $101.59 → close $101.59 +0.00; USFD×23 09:30 $93.82 → close $93.82 +0.00; ZSQR×203 09:30 $3.86 → close $3.78 -16.24; SECZ×48 09:30 $16.21 → close $15.96 -12.00; WRBY×29 09:30 $26.27 → close $26.71 +12.76; TWST×4 09:30 $184.00 → close $182.83 -4.68; GRAL×6 09:30 $123.50 → close $126.89 +20.34; QMCO×26 09:30 $29.80 → close $31.68 +48.88; CYPH×196 09:30 $4.00 → close $4.12 +22.54; CDNA×12 09:30 $61.33 → close $63.68 +28.20; ILMN×7 09:30 $272.16 → close $270.00 -15.12; RKLB×28 09:30 $74.15 → close $73.95 -5.60; COST×2 09:30 $887.00 → close $922.76 +71.53; RSKD×495 09:30 $7.85 → close $7.78 +34.65 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 476.41 < 1 share @ 1646.93 |
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
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `GO` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-19 | `VERI` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `ZNTL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `APMD` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `HIVE` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `RNW` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `AAP` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new long union_news_g_h1 |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new long union_news_g_h1 |
| 2026-08-19 | `ANGX` | hard_red | hard-red S=-7.20 sit; no new long union_news_g_h1 |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new long union_news_g_h1 |
| 2026-08-19 | `AS` | hard_red | hard-red S=-7.20 sit; no new long union_news_g_h1 |
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
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new long union_news_g_h1 |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new long union_news_g_h1 |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new long union_news_g_h1 |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new long union_news_g_h1 |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new long union_news_g_h1 |
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
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new long union_news_g_h1 |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new long union_join_vol_green_h1 |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new long union_join_vol_green_h1 |
| 2026-09-01 | `SIMO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `FIG` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `SLBT` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new short short_news_r_h3 |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new short short_news_r_h3 |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `TEL` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `RLMD` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `POWW` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `FRNM` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `DE` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `CIEN` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-03 | `DE` | cash | leftover split 541.98 < 1 share @ 703.25 |
| 2026-09-04 | `SLN` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `SLN` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `OPK` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `PIPR` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `GEMI` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new long union_news_g_h1 |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new long union_news_g_h1 |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new long union_news_g_h1 |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new long union_news_g_h1 |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `CRCL` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `TRMD` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-09 | `GSM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `PIPR` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `NVS` | hard_red | hard-red S=-13.95 sit; no new short short_news_r_h3 |
| 2026-09-09 | `DFDV` | hard_red | hard-red S=-13.95 sit; no new long union_news_g_h1 |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new long union_news_g_h1 |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new long union_news_g_h1 |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new short short_news_r_h3 |
| 2026-09-10 | `BKV` | hard_red | hard-red S=-13.28 sit; no new short short_news_r_h3 |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-10 | `OCGN` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
| 2026-09-10 | `GME` | hard_red | hard-red S=-13.28 sit; no new long union_news_g_h1 |
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
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new long union_news_g_h1 |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new long union_news_g_h1 |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new long union_news_g_h1 |
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
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new long union_news_g_h1 |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new long union_news_g_h1 |
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
| 2026-09-22 | `GLXY` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AMTX` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SMTC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `VICR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AMD` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-22 | `GRPN` | no_price | no 09:30 open |
| 2026-09-22 | `XXI` | no_price | no 09:30 open |
| 2026-09-22 | `THO` | no_price | no 09:30 open |
| 2026-09-22 | `WBD` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `YSS` | no_price | no 09:30 open |
| 2026-09-22 | `PAYX` | no_price | no 09:30 open |
| 2026-09-23 | `AMD` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-23 | `USFD` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `USFD` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-24 | `HALO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `PANW` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `VOYG` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `BMEA` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `NNBR` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `ADCT` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `ZSQR` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
| 2026-09-24 | `AMPL` | hard_red | hard-red S=-7.66 sit; no new long union_news_g_h1 |
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
| `FIVN` | 164 | 2026-09-18 @ $34.44 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $5660.11; owner short_news_r_h3 |
| `AEHL` | 349 | 2026-09-21 @ $8.26 | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $2887.42; owner short_news_r_h3 |
| `USFD` | 29 | 2026-09-22 @ $93.97 | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $2781.49; owner short_news_r_h3 |
| `HALO` | 44 | 2026-09-23 @ $116.85 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $5254.67; owner short_news_r_h3 |
