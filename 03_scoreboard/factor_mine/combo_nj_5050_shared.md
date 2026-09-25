# Factor mine action — `combo_nj_5050_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared union_news_g_h1/union_join_vol_green_h1 w=0.5,0.5 net=priority

Cash book **-11.51%** ($8,849) · signal-only (no cash/fees) was —. Starts YES **9/30**. Fills 401 · skips 133 · realized $+83.38.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: union_news_g_h1 50%, union_join_vol_green_h1 50%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: union_news_g_h1 50%, union_join_vol_green_h1 50%.
- Member: union_news_g_h1 (50% · long · hold 1).
- Member: union_join_vol_green_h1 (50% · long · hold 1).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $908.78.

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
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 145 | $4.31 | $2.42 | — | $9,372.62 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; combo leftover $625.00; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 416 | $1.50 | $5.37 | — | $8,743.26 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=+9.2; combo leftover $625.00; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 42 | $14.80 | $2.12 | — | $8,119.54 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=-9.9; combo leftover $625.00; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 149 | $4.18 | $2.44 | — | $7,494.29 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; combo leftover $625.00; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 37 | $16.50 | $2.10 | — | $6,881.68 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; combo leftover $625.00; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 56 | $11.12 | $2.16 | — | $6,256.81 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; combo leftover $625.00; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 232 | $2.69 | $2.99 | — | $5,629.73 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; combo leftover $625.00; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 85 | $7.29 | $2.25 | — | $5,007.84 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; combo leftover $625.00; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 1 | $359.83 | $1.99 | — | $4,646.02 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.9; combo leftover $715.41; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 4 | $146.90 | $2.00 | — | $4,056.41 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+3.6; combo leftover $715.41; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 5 | $120.00 | $2.00 | — | $3,454.41 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+0.6; combo leftover $715.41; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 36 | $19.57 | $2.10 | — | $2,747.79 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; combo leftover $715.41; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 52 | $13.55 | $2.15 | — | $2,041.04 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+17.5; combo leftover $715.41; owner union_news_g_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 54 | $13.18 | $2.15 | — | $1,327.17 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+12.0; combo leftover $715.41; owner union_news_g_h1 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,327.17 | ▼ close $9,939.69 vs 09:30 $10,000.00 (session -26.07) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,327.17 | ▼ 09:30 equity $9,933.02 vs yday $9,939.69 (-6.67) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 145 | $4.60 | $2.46 | $+37.17 | $1,991.71 | ▲ +37.17 after sell → book $9,930.56; vs 09:30 mark -2.46 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 416 | $1.52 | $5.45 | $-2.49 | $2,618.59 | ▼ -2.49 after sell → book $9,925.12; vs 09:30 mark -5.44 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 42 | $13.67 | $2.14 | $-51.71 | $3,190.59 | ▼ -51.71 after sell → book $9,922.98; vs 09:30 mark -2.14 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 149 | $4.10 | $2.47 | $-16.83 | $3,799.02 | ▼ -16.83 after sell → book $9,920.51; vs 09:30 mark -2.47 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 37 | $15.73 | $2.12 | $-32.71 | $4,378.91 | ▼ -32.71 after sell → book $9,918.39; vs 09:30 mark -2.12 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 56 | $9.57 | $2.18 | $-91.14 | $4,912.65 | ▼ -91.14 after sell → book $9,916.21; vs 09:30 mark -2.18 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NCMI` | 232 | $2.80 | $3.04 | $+19.49 | $5,559.21 | ▲ +19.49 after sell → book $9,913.17; vs 09:30 mark -3.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMLS` | 85 | $7.24 | $2.27 | $-8.76 | $6,172.34 | ▼ -8.76 after sell → book $9,910.90; vs 09:30 mark -2.27 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 1 | $367.88 | $2.01 | $+4.04 | $6,538.21 | ▲ +4.04 after sell → book $9,908.89; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 4 | $149.37 | $2.02 | $+5.86 | $7,133.67 | ▲ +5.86 after sell → book $9,906.87; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 5 | $127.40 | $2.02 | $+32.97 | $7,768.64 | ▲ +32.97 after sell → book $9,904.84; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 36 | $19.57 | $2.12 | $-4.22 | $8,471.04 | ▼ -4.22 after sell → book $9,902.72; vs 09:30 mark -2.12 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 52 | $13.16 | $2.17 | $-24.59 | $9,153.20 | ▼ -24.59 after sell → book $9,900.56; vs 09:30 mark -2.16 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 54 | $13.84 | $2.17 | $+31.32 | $9,898.39 | ▲ +31.32 after sell → book $9,898.39; vs 09:30 mark -2.17 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 108 | $9.12 | $2.31 | — | $8,911.11 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; combo leftover $989.84; owner union_join_vol_green_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 67 | $14.66 | $2.19 | — | $7,926.70 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; combo leftover $989.84; owner union_join_vol_green_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `BORR` | 215 | $4.59 | $2.77 | — | $6,937.08 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ⚪; ret5=+14.8; combo leftover $989.84; owner union_join_vol_green_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 236 | $4.19 | $3.04 | — | $5,945.19 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ⚪; ret5=+291.8; combo leftover $989.84; owner union_join_vol_green_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `MP` | 17 | $58.01 | $2.04 | — | $4,956.98 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+14.9; combo leftover $989.84; owner union_join_vol_green_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 21 | $46.18 | $2.05 | — | $3,985.15 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+6.7; combo leftover $991.40; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 6 | $142.77 | $2.01 | — | $3,126.52 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.8; combo leftover $991.40; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 4 | $202.70 | $2.00 | — | $2,313.72 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+8.3; combo leftover $991.40; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 10 | $92.99 | $2.02 | — | $1,381.80 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.8; combo leftover $991.40; owner union_news_g_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 20 | $49.00 | $2.05 | — | $399.75 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ⚪; ret5=+12.2; combo leftover $991.40; owner union_news_g_h1 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $399.75 | ▼ close $9,785.95 vs 09:30 $9,933.02 (session -89.93) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $399.75 | ▼ 09:30 equity $9,684.47 vs yday $9,785.95 (-101.48) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 108 | $9.03 | $2.34 | $-14.38 | $1,372.65 | ▼ -14.38 after sell → book $9,682.13; vs 09:30 mark -2.34 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 67 | $13.19 | $2.21 | $-102.89 | $2,254.16 | ▼ -102.89 after sell → book $9,679.91; vs 09:30 mark -2.22 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `BORR` | 215 | $4.56 | $2.82 | $-12.04 | $3,231.74 | ▼ -12.04 after sell → book $9,677.10; vs 09:30 mark -2.81 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 236 | $3.94 | $3.09 | $-65.14 | $4,158.49 | ▼ -65.14 after sell → book $9,674.00; vs 09:30 mark -3.10 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `MP` | 17 | $56.35 | $2.06 | $-32.32 | $5,114.38 | ▼ -32.32 after sell → book $9,671.94; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 21 | $48.00 | $2.07 | $+34.09 | $6,120.31 | ▲ +34.09 after sell → book $9,669.87; vs 09:30 mark -2.07 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 6 | $148.04 | $2.03 | $+27.58 | $7,006.52 | ▲ +27.58 after sell → book $9,667.84; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 4 | $208.93 | $2.02 | $+20.90 | $7,840.22 | ▲ +20.90 after sell → book $9,665.82; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 10 | $92.38 | $2.04 | $-10.16 | $8,761.98 | ▼ -10.16 after sell → book $9,663.78; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OUST` | 20 | $45.09 | $2.07 | $-82.32 | $9,661.71 | ▼ -82.32 after sell → book $9,661.71; vs 09:30 mark -2.07 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,661.71 | ▲ close $9,661.71 vs 09:30 $9,684.47 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,661.71 | ▲ 09:30 equity $9,661.71 vs yday $9,661.71 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,661.71 | ▲ close $9,661.71 vs 09:30 $9,661.71 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,661.71 | ▲ 09:30 equity $9,661.71 vs yday $9,661.71 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 29 | $20.55 | $2.08 | — | $9,063.68 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; combo leftover $603.86; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 29 | $20.65 | $2.08 | — | $8,462.75 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; combo leftover $603.86; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 104 | $5.77 | $2.30 | — | $7,860.37 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; combo leftover $603.86; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 30 | $19.63 | $2.08 | — | $7,269.39 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; combo leftover $603.86; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 20 | $29.63 | $2.05 | — | $6,674.74 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; combo leftover $603.86; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 345 | $1.75 | $4.45 | — | $6,066.54 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; combo leftover $603.86; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 4 | $144.54 | $2.00 | — | $5,486.38 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; combo leftover $603.86; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 122 | $4.92 | $2.36 | — | $4,883.78 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $603.86; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 6 | $91.01 | $2.01 | — | $4,335.71 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; combo leftover $610.47; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 4 | $150.14 | $2.00 | — | $3,733.15 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+173.9; combo leftover $610.47; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 863 | $0.71 | $8.69 | — | $3,114.32 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+10.5; combo leftover $610.47; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 92 | $6.61 | $2.27 | — | $2,504.40 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+25.3; combo leftover $610.47; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 38 | $16.00 | $2.10 | — | $1,894.29 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+18.9; combo leftover $610.47; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 22 | $26.57 | $2.06 | — | $1,307.70 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+4.8; combo leftover $610.47; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 10 | $58.73 | $2.02 | — | $718.38 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+10.8; combo leftover $610.47; owner union_news_g_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 13 | $44.76 | $2.03 | — | $134.47 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=+8.7; combo leftover $610.47; owner union_news_g_h1 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $134.47 | ▼ close $9,604.35 vs 09:30 $9,661.71 (session -14.79) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $134.47 | ▲ 09:30 equity $9,891.61 vs yday $9,604.35 (+287.26) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 29 | $21.90 | $2.10 | $+34.98 | $767.47 | ▲ +34.98 after sell → book $9,889.51; vs 09:30 mark -2.10 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 29 | $21.75 | $2.10 | $+27.73 | $1,396.12 | ▲ +27.73 after sell → book $9,887.41; vs 09:30 mark -2.10 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 104 | $5.67 | $2.33 | $-15.03 | $1,983.47 | ▼ -15.03 after sell → book $9,885.09; vs 09:30 mark -2.32 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 30 | $21.17 | $2.10 | $+42.02 | $2,616.47 | ▲ +42.02 after sell → book $9,882.99; vs 09:30 mark -2.10 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 20 | $32.17 | $2.07 | $+46.68 | $3,257.80 | ▲ +46.68 after sell → book $9,880.92; vs 09:30 mark -2.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 345 | $1.79 | $4.52 | $+4.83 | $3,870.84 | ▲ +4.83 after sell → book $9,876.40; vs 09:30 mark -4.52 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 4 | $154.70 | $2.02 | $+36.62 | $4,487.61 | ▲ +36.62 after sell → book $9,874.38; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 122 | $5.20 | $2.39 | $+29.42 | $5,119.63 | ▲ +29.42 after sell → book $9,871.99; vs 09:30 mark -2.39 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 6 | $95.72 | $2.03 | $+24.22 | $5,691.92 | ▲ +24.22 after sell → book $9,869.96; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 4 | $133.11 | $2.02 | $-72.14 | $6,222.34 | ▼ -72.14 after sell → book $9,867.94; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HUMA` | 863 | $0.67 | $8.56 | $-45.73 | $6,795.44 | ▼ -45.73 after sell → book $9,859.38; vs 09:30 mark -8.56 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 92 | $6.95 | $2.29 | $+27.18 | $7,432.55 | ▲ +27.18 after sell → book $9,857.09; vs 09:30 mark -2.29 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 38 | $17.66 | $2.12 | $+58.85 | $8,101.50 | ▲ +58.85 after sell → book $9,854.96; vs 09:30 mark -2.13 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 22 | $26.25 | $2.08 | $-11.17 | $8,676.93 | ▼ -11.17 after sell → book $9,852.89; vs 09:30 mark -2.07 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 13 | $44.52 | $2.05 | $-7.20 | $9,253.64 | ▼ -7.20 after sell → book $9,850.84; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 4 | $119.43 | $2.00 | — | $8,773.92 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; combo leftover $578.35; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 33 | $17.20 | $2.09 | — | $8,204.23 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; combo leftover $578.35; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 2 | $216.30 | $2.00 | — | $7,769.63 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; combo leftover $578.35; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 51 | $11.13 | $2.14 | — | $7,199.86 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; combo leftover $578.35; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 438 | $1.32 | $5.65 | — | $6,616.05 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; combo leftover $578.35; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 348 | $1.66 | $4.49 | — | $6,033.88 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; combo leftover $578.35; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `INDP` | 416 | $1.39 | $5.37 | — | $5,450.27 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+30.2; combo leftover $578.35; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 69 | $8.28 | $2.20 | — | $4,876.76 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; combo leftover $578.35; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 329 | $2.47 | $4.24 | — | $4,059.88 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; combo leftover $812.79; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 7 | $115.18 | $2.01 | — | $3,251.61 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; combo leftover $812.79; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 1 | $623.26 | $1.99 | — | $2,626.36 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ret5=+1.4; combo leftover $812.79; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 69 | $11.70 | $2.20 | — | $1,816.86 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+21.1; combo leftover $812.79; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 73 | $11.10 | $2.21 | — | $1,004.72 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+19.1; combo leftover $812.79; owner union_news_g_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 250 | $3.24 | $3.23 | — | $191.49 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+21.3; combo leftover $812.79; owner union_news_g_h1 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $191.49 | ▲ close $9,896.38 vs 09:30 $9,891.61 (session +87.35) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $191.49 | ▲ 09:30 equity $10,017.13 vs yday $9,896.38 (+120.75) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 4 | $120.51 | $2.02 | $+0.30 | $671.51 | ▲ +0.30 after sell → book $10,015.11; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 33 | $16.57 | $2.11 | $-24.99 | $1,216.21 | ▼ -24.99 after sell → book $10,013.00; vs 09:30 mark -2.11 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 2 | $217.03 | $2.02 | $-2.55 | $1,648.26 | ▼ -2.55 after sell → book $10,010.99; vs 09:30 mark -2.01 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 51 | $13.33 | $2.16 | $+107.89 | $2,325.92 | ▲ +107.89 after sell → book $10,008.82; vs 09:30 mark -2.17 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 438 | $1.83 | $5.73 | $+212.00 | $3,121.73 | ▲ +212.00 after sell → book $10,003.09; vs 09:30 mark -5.73 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 348 | $1.55 | $4.56 | $-47.33 | $3,656.57 | ▼ -47.33 after sell → book $9,998.53; vs 09:30 mark -4.56 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `INDP` | 416 | $1.24 | $5.45 | $-73.21 | $4,166.97 | ▼ -73.21 after sell → book $9,993.09; vs 09:30 mark -5.44 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 69 | $8.59 | $2.22 | $+16.97 | $4,757.46 | ▲ +16.97 after sell → book $9,990.87; vs 09:30 mark -2.22 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 329 | $2.40 | $4.31 | $-31.58 | $5,542.75 | ▼ -31.58 after sell → book $9,986.56; vs 09:30 mark -4.31 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 7 | $121.00 | $2.03 | $+36.70 | $6,387.72 | ▲ +36.70 after sell → book $9,984.53; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 1 | $653.04 | $2.01 | $+25.77 | $7,038.75 | ▲ +25.77 after sell → book $9,982.52; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 69 | $11.17 | $2.22 | $-40.99 | $7,807.26 | ▼ -40.99 after sell → book $9,980.30; vs 09:30 mark -2.22 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTDR` | 73 | $11.48 | $2.23 | $+23.66 | $8,643.07 | ▲ +23.66 after sell → book $9,978.07; vs 09:30 mark -2.23 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 250 | $2.99 | $3.28 | $-69.00 | $9,387.29 | ▼ -69.00 after sell → book $9,974.79; vs 09:30 mark -3.28 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,387.29 | ▼ close $9,958.04 vs 09:30 $10,017.13 (session -16.75) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,387.29 | ▲ 09:30 equity $9,966.59 vs yday $9,958.04 (+8.55) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 10 | $57.93 | $2.04 | $-12.06 | $9,964.55 | ▼ -12.06 after sell → book $9,964.55; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 17 | $35.05 | $2.04 | — | $9,366.66 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+19.7; combo leftover $622.78; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 382 | $1.63 | $4.93 | — | $8,739.07 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; combo leftover $622.78; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 175 | $3.55 | $2.52 | — | $8,115.31 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+27.9; combo leftover $622.78; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 97 | $6.37 | $2.28 | — | $7,495.14 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+10.9; combo leftover $622.78; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ETON` | 9 | $64.55 | $2.02 | — | $6,912.17 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+4.4; combo leftover $622.78; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `WPM` | 3 | $156.51 | $2.00 | — | $6,440.64 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+17.4; combo leftover $622.78; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUZ` | 69 | $8.98 | $2.20 | — | $5,818.82 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+15.4; combo leftover $622.78; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `IAUX` | 327 | $1.90 | $4.22 | — | $5,193.30 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+16.4; combo leftover $622.78; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 91 | $9.42 | $2.26 | — | $4,333.82 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; combo leftover $865.55; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 35 | $24.11 | $2.10 | — | $3,487.88 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=+891.7; combo leftover $865.55; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 29 | $28.86 | $2.08 | — | $2,648.86 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+13.7; combo leftover $865.55; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `EOLS` | 99 | $8.72 | $2.29 | — | $1,783.29 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+13.0; combo leftover $865.55; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 7 | $118.52 | $2.01 | — | $951.64 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ⚪; ret5=+21.7; combo leftover $865.55; owner union_news_g_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 11 | $77.13 | $2.02 | — | $101.19 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; ⚪; ret5=+13.8; combo leftover $865.55; owner union_news_g_h1 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $101.19 | ▲ close $10,314.64 vs 09:30 $9,966.59 (session +385.04) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $101.19 | ▼ 09:30 equity $10,158.05 vs yday $10,314.64 (-156.59) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 17 | $35.70 | $2.06 | $+6.95 | $706.03 | ▲ +6.95 after sell → book $10,155.99; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 382 | $1.75 | $5.00 | $+37.82 | $1,371.44 | ▲ +37.82 after sell → book $10,150.99; vs 09:30 mark -5.00 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GORO` | 175 | $3.77 | $2.55 | $+33.43 | $2,028.63 | ▲ +33.43 after sell → book $10,148.43; vs 09:30 mark -2.56 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZURA` | 97 | $6.13 | $2.31 | $-27.87 | $2,620.93 | ▼ -27.87 after sell → book $10,146.12; vs 09:30 mark -2.31 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ETON` | 9 | $63.60 | $2.04 | $-12.60 | $3,191.30 | ▼ -12.60 after sell → book $10,144.09; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `WPM` | 3 | $160.93 | $2.02 | $+9.24 | $3,672.07 | ▲ +9.24 after sell → book $10,142.07; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SUZ` | 69 | $9.03 | $2.22 | $-0.97 | $4,292.92 | ▼ -0.97 after sell → book $10,139.85; vs 09:30 mark -2.22 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `IAUX` | 327 | $1.87 | $4.28 | $-18.31 | $4,900.13 | ▼ -18.31 after sell → book $10,135.57; vs 09:30 mark -4.28 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 91 | $10.07 | $2.29 | $+54.60 | $5,814.21 | ▲ +54.60 after sell → book $10,133.28; vs 09:30 mark -2.29 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 35 | $26.61 | $2.12 | $+83.29 | $6,743.44 | ▲ +83.29 after sell → book $10,131.16; vs 09:30 mark -2.12 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZYME` | 29 | $27.56 | $2.10 | $-41.87 | $7,540.59 | ▼ -41.87 after sell → book $10,129.07; vs 09:30 mark -2.09 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EOLS` | 99 | $8.86 | $2.31 | $+9.26 | $8,415.41 | ▲ +9.26 after sell → book $10,126.75; vs 09:30 mark -2.32 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 7 | $119.80 | $2.03 | $+4.92 | $9,251.98 | ▲ +4.92 after sell → book $10,124.72; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 11 | $79.34 | $2.04 | $+20.24 | $10,122.68 | ▲ +20.24 after sell → book $10,122.68; vs 09:30 mark -2.04 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 871 | $5.81 | $11.24 | — | $5,050.93 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ⚪; ret5=+117.2; combo leftover $5061.34; owner union_join_vol_green_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 75 | $11.12 | $2.21 | — | $4,214.72 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; combo leftover $841.82; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 101 | $8.29 | $2.29 | — | $3,375.14 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; combo leftover $841.82; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `FWRD` | 48 | $17.41 | $2.13 | — | $2,537.32 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-9.2; combo leftover $841.82; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 75 | $11.22 | $2.21 | — | $1,693.61 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+16.8; combo leftover $841.82; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 3 | $267.02 | $2.00 | — | $890.55 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+16.7; combo leftover $841.82; owner union_news_g_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 7 | $118.50 | $2.01 | — | $59.04 | — | union ∩ news_g, no 🚨; gate news=good; list overnight,overnight_mega; 🔵; ret5=-2.7; combo leftover $841.82; owner union_news_g_h1 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $59.04 | ▲ close $10,376.98 vs 09:30 $10,158.05 (session +278.40) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.04 | ▲ 09:30 equity $10,844.11 vs yday $10,376.98 (+467.13) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 871 | $6.50 | $11.43 | $+578.33 | $5,709.11 | ▲ +578.33 after sell → book $10,832.68; vs 09:30 mark -11.43 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 3 | $267.23 | $2.02 | $-3.39 | $6,508.78 | ▼ -3.39 after sell → book $10,830.66; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `DKS` | 25 | $128.73 | $2.06 | — | $3,288.47 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-32.2; combo leftover $3254.39; owner union_join_vol_green_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 26 | $41.44 | $2.07 | — | $2,208.96 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; ret5=+3.1; combo leftover $1096.16; owner union_news_g_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `AXTI` | 15 | $70.30 | $2.04 | — | $1,152.42 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=-11.2; combo leftover $1096.16; owner union_news_g_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `SRRK` | 18 | $60.00 | $2.04 | — | $70.38 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+6.2; combo leftover $1096.16; owner union_news_g_h1 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $70.38 | ▲ close $10,871.79 vs 09:30 $10,844.11 (session +49.34) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $70.38 | ▼ 09:30 equity $10,824.67 vs yday $10,871.79 (-47.12) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `FLNC` | 75 | $11.27 | $2.24 | $+6.80 | $913.39 | ▲ +6.80 after sell → book $10,822.43; vs 09:30 mark -2.24 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `FWRD` | 48 | $17.70 | $2.15 | $+9.63 | $1,760.84 | ▲ +9.63 after sell → book $10,820.28; vs 09:30 mark -2.15 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TRLV` | 75 | $11.00 | $2.24 | $-20.95 | $2,583.60 | ▼ -20.95 after sell → book $10,818.04; vs 09:30 mark -2.24 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 7 | $115.66 | $2.03 | $-23.92 | $3,391.19 | ▼ -23.92 after sell → book $10,816.01; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 25 | $132.80 | $2.10 | $+97.58 | $6,709.09 | ▲ +97.58 after sell → book $10,813.91; vs 09:30 mark -2.10 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AXTI` | 15 | $65.29 | $2.06 | $-79.24 | $7,686.38 | ▼ -79.24 after sell → book $10,811.85; vs 09:30 mark -2.06 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SRRK` | 18 | $58.75 | $2.06 | $-26.61 | $8,741.82 | ▼ -26.61 after sell → book $10,809.79; vs 09:30 mark -2.06 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 7 | $146.07 | $2.01 | — | $7,717.32 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+38.8; combo leftover $1092.73; owner union_join_vol_green_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 46 | $23.30 | $2.13 | — | $6,643.39 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+14.5; combo leftover $1092.73; owner union_join_vol_green_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 57 | $19.00 | $2.16 | — | $5,558.23 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; ret5=+7.5; combo leftover $1092.73; owner union_join_vol_green_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 44 | $24.69 | $2.12 | — | $4,469.75 | — | combo gate; gate join=good,vol=good,last_green=True; list earn_react; ret5=+5.8; combo leftover $1092.73; owner union_join_vol_green_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 22 | $32.90 | $2.06 | — | $3,743.89 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; combo leftover $744.96; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 86 | $8.61 | $2.25 | — | $3,001.18 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.7; combo leftover $744.96; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 5 | $141.76 | $2.00 | — | $2,290.38 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; combo leftover $744.96; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 38 | $19.25 | $2.10 | — | $1,556.78 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+14.1; combo leftover $744.96; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 39 | $18.75 | $2.11 | — | $823.42 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=-5.0; combo leftover $744.96; owner union_news_g_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 25 | $28.91 | $2.06 | — | $98.60 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+9.2; combo leftover $744.96; owner union_news_g_h1 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $98.60 | ▼ close $10,553.16 vs 09:30 $10,824.67 (session -235.62) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $98.60 | ▼ 09:30 equity $10,483.75 vs yday $10,553.16 (-69.41) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 101 | $9.50 | $2.32 | $+117.60 | $1,055.78 | ▲ +117.60 after sell → book $10,481.43; vs 09:30 mark -2.32 | union_news_g_h1: dropped from list after 3 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 26 | $42.00 | $2.09 | $+10.40 | $2,145.70 | ▲ +10.40 after sell → book $10,479.34; vs 09:30 mark -2.09 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 7 | $148.03 | $2.03 | $+9.68 | $3,179.87 | ▲ +9.68 after sell → book $10,477.31; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NCNO` | 46 | $22.66 | $2.15 | $-33.72 | $4,220.09 | ▼ -33.72 after sell → book $10,475.16; vs 09:30 mark -2.15 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TH` | 57 | $18.12 | $2.18 | $-54.22 | $5,251.03 | ▼ -54.22 after sell → book $10,472.98; vs 09:30 mark -2.18 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GAP` | 44 | $22.98 | $2.14 | $-79.50 | $6,260.01 | ▼ -79.50 after sell → book $10,470.84; vs 09:30 mark -2.14 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 22 | $31.15 | $2.08 | $-42.63 | $6,943.23 | ▼ -42.63 after sell → book $10,468.76; vs 09:30 mark -2.08 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OPTX` | 86 | $8.52 | $2.27 | $-12.26 | $7,673.68 | ▼ -12.26 after sell → book $10,466.49; vs 09:30 mark -2.27 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 5 | $132.30 | $2.02 | $-51.33 | $8,333.16 | ▼ -51.33 after sell → book $10,464.47; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 38 | $17.87 | $2.12 | $-56.67 | $9,010.09 | ▼ -56.67 after sell → book $10,462.34; vs 09:30 mark -2.12 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 39 | $19.25 | $2.13 | $+15.27 | $9,758.71 | ▲ +15.27 after sell → book $10,460.21; vs 09:30 mark -2.13 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 25 | $28.06 | $2.08 | $-25.40 | $10,458.13 | ▼ -25.40 after sell → book $10,458.13; vs 09:30 mark -2.08 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,458.13 | ▲ close $10,458.13 vs 09:30 $10,483.75 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,458.13 | ▲ 09:30 equity $10,458.13 vs yday $10,458.13 (-0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,458.13 | ▲ close $10,458.13 vs 09:30 $10,458.13 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,458.13 | ▲ 09:30 equity $10,458.13 vs yday $10,458.13 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,458.13 | ▲ close $10,458.13 vs 09:30 $10,458.13 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,458.13 | ▲ 09:30 equity $10,458.13 vs yday $10,458.13 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 27 | $23.88 | $2.07 | — | $9,811.30 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $653.63; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 4 | $132.45 | $2.00 | — | $9,279.50 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; combo leftover $653.63; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 38 | $16.77 | $2.10 | — | $8,640.13 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; combo leftover $653.63; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 299 | $2.18 | $3.86 | — | $7,984.45 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; combo leftover $653.63; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 62 | $10.42 | $2.18 | — | $7,336.24 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; combo leftover $653.63; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 338 | $1.93 | $4.36 | — | $6,679.54 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; combo leftover $653.63; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `DUOL` | 4 | $161.54 | $2.00 | — | $6,031.38 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+12.0; combo leftover $653.63; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `ALMS` | 63 | $10.38 | $2.18 | — | $5,375.57 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ret5=-56.2; combo leftover $653.63; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 23 | $32.88 | $2.06 | — | $4,617.27 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+16.2; combo leftover $767.94; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `OPTX` | 101 | $7.59 | $2.29 | — | $3,848.39 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-11.5; combo leftover $767.94; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 1 | $703.25 | $1.99 | — | $3,143.15 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+10.1; combo leftover $767.94; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 48 | $15.87 | $2.13 | — | $2,379.25 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; combo leftover $767.94; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 2 | $351.74 | $2.00 | — | $1,673.78 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=+3.3; combo leftover $767.94; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 2 | $354.49 | $2.00 | — | $962.80 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-12.3; combo leftover $767.94; owner union_news_g_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 16 | $47.60 | $2.04 | — | $199.16 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-6.2; combo leftover $767.94; owner union_news_g_h1 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $199.16 | ▲ close $10,505.30 vs 09:30 $10,458.13 (session +82.43) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $199.16 | ▼ 09:30 equity $10,469.91 vs yday $10,505.30 (-35.39) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 27 | $23.84 | $2.09 | $-5.24 | $840.75 | ▼ -5.24 after sell → book $10,467.82; vs 09:30 mark -2.09 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 4 | $130.03 | $2.02 | $-13.70 | $1,358.85 | ▼ -13.70 after sell → book $10,465.80; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 38 | $15.61 | $2.12 | $-48.31 | $1,949.91 | ▼ -48.31 after sell → book $10,463.68; vs 09:30 mark -2.12 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 299 | $2.16 | $3.92 | $-13.75 | $2,591.83 | ▼ -13.75 after sell → book $10,459.76; vs 09:30 mark -3.92 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 62 | $10.50 | $2.20 | $+0.59 | $3,240.63 | ▲ +0.59 after sell → book $10,457.56; vs 09:30 mark -2.20 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `BMEA` | 338 | $1.90 | $4.43 | $-18.93 | $3,878.41 | ▼ -18.93 after sell → book $10,453.14; vs 09:30 mark -4.42 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DUOL` | 4 | $157.46 | $2.02 | $-20.34 | $4,506.23 | ▼ -20.34 after sell → book $10,451.12; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ALMS` | 63 | $11.23 | $2.20 | $+49.49 | $5,211.52 | ▲ +49.49 after sell → book $10,448.92; vs 09:30 mark -2.20 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNXC` | 23 | $32.48 | $2.08 | $-13.34 | $5,956.48 | ▼ -13.34 after sell → book $10,446.84; vs 09:30 mark -2.08 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `OPTX` | 101 | $7.79 | $2.32 | $+15.59 | $6,740.95 | ▲ +15.59 after sell → book $10,444.52; vs 09:30 mark -2.32 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 1 | $692.03 | $2.01 | $-15.23 | $7,430.96 | ▼ -15.23 after sell → book $10,442.50; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 2 | $359.70 | $2.02 | $+11.91 | $8,148.35 | ▲ +11.91 after sell → book $10,440.49; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 2 | $321.67 | $2.02 | $-69.65 | $8,789.67 | ▼ -69.65 after sell → book $10,438.47; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 16 | $53.85 | $2.06 | $+95.90 | $9,649.21 | ▲ +95.90 after sell → book $10,436.41; vs 09:30 mark -2.06 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 1 | $513.78 | $1.99 | — | $9,133.44 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; combo leftover $603.08; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 7 | $82.70 | $2.01 | — | $8,552.53 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+15.7; combo leftover $603.08; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 240 | $2.51 | $3.10 | — | $7,947.03 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+21.8; combo leftover $603.08; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `MDB` | 1 | $378.34 | $1.99 | — | $7,566.70 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-12.7; combo leftover $603.08; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 23 | $25.18 | $2.06 | — | $6,985.50 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+16.0; combo leftover $603.08; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 104 | $5.79 | $2.30 | — | $6,381.04 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+15.2; combo leftover $603.08; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `TDS` | 16 | $37.44 | $2.04 | — | $5,779.96 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+14.1; combo leftover $603.08; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `AHCO` | 95 | $6.32 | $2.27 | — | $5,177.29 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+10.7; combo leftover $603.08; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 3 | $263.36 | $2.00 | — | $4,385.21 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; combo leftover $1035.46; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 533 | $1.94 | $6.88 | — | $3,344.31 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; ret5=+18.3; combo leftover $1035.46; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 7 | $137.35 | $2.01 | — | $2,380.85 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+5.4; combo leftover $1035.46; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 4 | $236.82 | $2.00 | — | $1,431.57 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+8.1; combo leftover $1035.46; owner union_news_g_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 13 | $75.65 | $2.03 | — | $446.09 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+6.0; combo leftover $1035.46; owner union_news_g_h1 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $446.09 | ▲ close $10,665.89 vs 09:30 $10,469.91 (session +262.16) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $446.09 | ▲ 09:30 equity $10,684.04 vs yday $10,665.89 (+18.15) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 48 | $16.74 | $2.15 | $+37.47 | $1,247.46 | ▲ +37.47 after sell → book $10,681.89; vs 09:30 mark -2.15 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 1 | $521.15 | $2.01 | $+3.36 | $1,766.59 | ▲ +3.36 after sell → book $10,679.87; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TARS` | 7 | $89.67 | $2.03 | $+44.75 | $2,392.25 | ▲ +44.75 after sell → book $10,677.84; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BRR` | 240 | $2.66 | $3.15 | $+29.76 | $3,027.51 | ▲ +29.76 after sell → book $10,674.70; vs 09:30 mark -3.14 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MDB` | 1 | $360.75 | $2.01 | $-21.60 | $3,386.24 | ▼ -21.60 after sell → book $10,672.68; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 23 | $26.44 | $2.08 | $+24.84 | $3,992.28 | ▲ +24.84 after sell → book $10,670.60; vs 09:30 mark -2.08 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 104 | $5.81 | $2.33 | $-2.55 | $4,594.20 | ▼ -2.55 after sell → book $10,668.28; vs 09:30 mark -2.32 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TDS` | 16 | $37.75 | $2.06 | $+0.86 | $5,196.14 | ▲ +0.86 after sell → book $10,666.22; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `AHCO` | 95 | $6.48 | $2.30 | $+10.62 | $5,809.44 | ▲ +10.62 after sell → book $10,663.92; vs 09:30 mark -2.30 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 3 | $253.72 | $2.02 | $-32.94 | $6,568.58 | ▼ -32.94 after sell → book $10,661.90; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 533 | $1.94 | $6.97 | $-13.85 | $7,595.62 | ▼ -13.85 after sell → book $10,654.92; vs 09:30 mark -6.98 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 4 | $267.76 | $2.02 | $+119.74 | $8,664.64 | ▲ +119.74 after sell → book $10,652.90; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,664.64 | ▼ close $10,617.51 vs 09:30 $10,684.04 (session -35.39) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,664.64 | ▲ 09:30 equity $10,653.18 vs yday $10,617.51 (+35.67) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `MSTR` | 7 | $141.82 | $2.03 | $+27.25 | $9,655.35 | ▲ +27.25 after sell → book $10,651.15; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 13 | $76.60 | $2.05 | $+8.27 | $10,649.10 | ▲ +8.27 after sell → book $10,649.10; vs 09:30 mark -2.05 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,649.10 | ▲ close $10,649.10 vs 09:30 $10,653.18 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,649.10 | ▲ 09:30 equity $10,649.10 vs yday $10,649.10 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,649.10 | ▲ close $10,649.10 vs 09:30 $10,649.10 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,649.10 | ▲ 09:30 equity $10,649.10 vs yday $10,649.10 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 28 | $23.63 | $2.07 | — | $9,985.39 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; ret5=-6.3; combo leftover $665.57; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 246 | $2.70 | $3.17 | — | $9,318.01 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+118.8; combo leftover $665.57; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 60 | $10.95 | $2.17 | — | $8,658.84 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+20.8; combo leftover $665.57; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 135 | $4.91 | $2.40 | — | $7,993.60 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+76.3; combo leftover $665.57; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 7 | $84.27 | $2.01 | — | $7,401.70 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ret5=+17.2; combo leftover $665.57; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `ASO` | 12 | $54.91 | $2.03 | — | $6,740.75 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+24.3; combo leftover $665.57; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 108 | $6.16 | $2.31 | — | $6,073.16 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+36.4; combo leftover $665.57; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `COO` | 12 | $54.66 | $2.03 | — | $5,415.21 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-22.3; combo leftover $665.57; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 5 | $164.43 | $2.00 | — | $4,591.06 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,earn_react; ⚪; ret5=+4.9; combo leftover $902.54; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 442 | $2.04 | $5.70 | — | $3,683.68 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; combo leftover $902.54; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 425 | $2.12 | $5.48 | — | $2,777.19 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; combo leftover $902.54; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `AVTR` | 60 | $15.01 | $2.17 | — | $1,874.42 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+4.9; combo leftover $902.54; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 3 | $242.17 | $2.00 | — | $1,145.91 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; ret5=-11.1; combo leftover $902.54; owner union_news_g_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 6 | $135.71 | $2.01 | — | $329.65 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; ret5=-9.2; combo leftover $902.54; owner union_news_g_h1 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $329.65 | ▼ close $10,454.25 vs 09:30 $10,649.10 (session -157.30) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $329.65 | ▲ 09:30 equity $10,463.34 vs yday $10,454.25 (+9.09) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `TYRA` | 28 | $23.20 | $2.09 | $-16.21 | $977.15 | ▼ -16.21 after sell → book $10,461.24; vs 09:30 mark -2.10 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `INDP` | 246 | $2.80 | $3.22 | $+18.20 | $1,662.73 | ▲ +18.20 after sell → book $10,458.02; vs 09:30 mark -3.22 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `WLTH` | 60 | $10.29 | $2.19 | $-43.96 | $2,277.94 | ▼ -43.96 after sell → book $10,455.83; vs 09:30 mark -2.19 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BNC` | 135 | $5.03 | $2.43 | $+11.38 | $2,954.56 | ▲ +11.38 after sell → book $10,453.40; vs 09:30 mark -2.43 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SWKS` | 7 | $86.06 | $2.03 | $+8.49 | $3,554.95 | ▲ +8.49 after sell → book $10,451.37; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ASO` | 12 | $54.75 | $2.05 | $-5.99 | $4,209.90 | ▼ -5.99 after sell → book $10,449.32; vs 09:30 mark -2.05 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 108 | $6.02 | $2.34 | $-19.78 | $4,857.72 | ▼ -19.78 after sell → book $10,446.98; vs 09:30 mark -2.34 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `COO` | 12 | $54.78 | $2.05 | $-2.63 | $5,513.04 | ▼ -2.63 after sell → book $10,444.94; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 5 | $141.42 | $2.02 | $-119.08 | $6,218.11 | ▼ -119.08 after sell → book $10,442.91; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 442 | $2.01 | $5.79 | $-24.75 | $7,100.74 | ▼ -24.75 after sell → book $10,437.12; vs 09:30 mark -5.79 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 425 | $2.05 | $5.56 | $-40.80 | $7,966.43 | ▼ -40.80 after sell → book $10,431.56; vs 09:30 mark -5.56 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 3 | $261.51 | $2.02 | $+54.00 | $8,748.94 | ▲ +54.00 after sell → book $10,429.54; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RH` | 6 | $131.40 | $2.03 | $-29.90 | $9,535.31 | ▼ -29.90 after sell → book $10,427.51; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,535.31 | ▲ close $10,444.91 vs 09:30 $10,463.34 (session +17.40) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,535.31 | ▲ 09:30 equity $10,448.51 vs yday $10,444.91 (+3.60) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,535.31 | ▲ close $10,461.71 vs 09:30 $10,448.51 (session +13.20) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,535.31 | ▲ 09:30 equity $10,467.11 vs yday $10,461.71 (+5.40) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 7 | $77.12 | $2.01 | — | $8,993.46 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; ret5=+7.2; combo leftover $595.96; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 101 | $5.87 | $2.29 | — | $8,398.30 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+3.1; combo leftover $595.96; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 6 | $87.40 | $2.01 | — | $7,871.89 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+3.2; combo leftover $595.96; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 21 | $27.09 | $2.05 | — | $7,300.95 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; combo leftover $595.96; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 6 | $89.38 | $2.01 | — | $6,762.66 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; combo leftover $595.96; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 25 | $23.29 | $2.06 | — | $6,178.35 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+16.1; combo leftover $595.96; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 17 | $33.14 | $2.04 | — | $5,612.93 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=-2.9; combo leftover $595.96; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `CAI` | 21 | $28.16 | $2.05 | — | $5,019.51 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ret5=+14.8; combo leftover $595.96; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 47 | $26.27 | $2.13 | — | $3,782.69 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+10.0; combo leftover $1254.88; owner union_news_g_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SION` | 180 | $6.95 | $2.53 | — | $2,529.16 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-5.8; combo leftover $1254.88; owner union_news_g_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 31 | $39.99 | $2.08 | — | $1,287.39 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+9.3; combo leftover $1254.88; owner union_news_g_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 6 | $189.17 | $2.01 | — | $150.36 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+7.9; combo leftover $1254.88; owner union_news_g_h1 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $150.36 | ▼ close $10,358.56 vs 09:30 $10,467.11 (session -83.27) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $150.36 | ▲ 09:30 equity $10,499.22 vs yday $10,358.56 (+140.66) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `RDNT` | 7 | $76.44 | $2.03 | $-8.80 | $683.41 | ▼ -8.80 after sell → book $10,497.18; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 101 | $5.58 | $2.32 | $-33.90 | $1,244.67 | ▼ -33.90 after sell → book $10,494.87; vs 09:30 mark -2.31 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 6 | $83.20 | $2.03 | $-29.24 | $1,741.84 | ▼ -29.24 after sell → book $10,492.84; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ADPT` | 21 | $28.23 | $2.07 | $+19.81 | $2,332.60 | ▲ +19.81 after sell → book $10,490.76; vs 09:30 mark -2.08 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 6 | $86.76 | $2.03 | $-19.76 | $2,851.13 | ▼ -19.76 after sell → book $10,488.74; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 25 | $24.09 | $2.08 | $+15.85 | $3,451.30 | ▲ +15.85 after sell → book $10,486.65; vs 09:30 mark -2.09 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `CAI` | 21 | $28.59 | $2.07 | $+5.01 | $4,049.72 | ▲ +5.01 after sell → book $10,484.58; vs 09:30 mark -2.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 47 | $26.51 | $2.15 | $+7.00 | $5,293.54 | ▲ +7.00 after sell → book $10,482.43; vs 09:30 mark -2.15 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SION` | 180 | $7.27 | $2.57 | $+52.50 | $6,599.57 | ▲ +52.50 after sell → book $10,479.86; vs 09:30 mark -2.57 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 31 | $37.57 | $2.10 | $-79.21 | $7,762.13 | ▼ -79.21 after sell → book $10,477.75; vs 09:30 mark -2.11 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 6 | $190.35 | $2.03 | $+3.04 | $8,902.21 | ▲ +3.04 after sell → book $10,475.73; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 3 | $170.85 | $2.00 | — | $8,387.66 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; combo leftover $635.87; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 2 | $233.85 | $2.00 | — | $7,917.96 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; ret5=+11.7; combo leftover $635.87; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 4 | $147.61 | $2.00 | — | $7,325.52 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; ret5=+17.7; combo leftover $635.87; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 83 | $7.59 | $2.24 | — | $6,693.31 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; 🔵; ret5=+9.4; combo leftover $635.87; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 24 | $25.95 | $2.06 | — | $6,068.45 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; combo leftover $635.87; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `CIFR` | 35 | $18.04 | $2.10 | — | $5,435.13 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.1; combo leftover $635.87; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRKR` | 10 | $61.90 | $2.02 | — | $4,814.11 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; 🔵; ret5=+11.5; combo leftover $635.87; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `LITE` | 1 | $934.88 | $1.99 | — | $3,877.23 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=-7.0; combo leftover $962.82; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `TNDM` | 54 | $17.72 | $2.15 | — | $2,918.20 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=-8.3; combo leftover $962.82; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `BAK` | 543 | $1.77 | $7.00 | — | $1,950.09 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-10.2; combo leftover $962.82; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 4 | $238.60 | $2.00 | — | $993.69 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-11.6; combo leftover $962.82; owner union_news_g_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 43 | $22.12 | $2.12 | — | $40.41 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+10.5; combo leftover $962.82; owner union_news_g_h1 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $40.41 | ▲ close $10,484.84 vs 09:30 $10,499.22 (session +38.80) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.41 | ▲ 09:30 equity $10,575.45 vs yday $10,484.84 (+90.61) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 60 | $15.87 | $2.19 | $+47.24 | $990.42 | ▲ +47.24 after sell → book $10,573.26; vs 09:30 mark -2.19 | union_news_g_h1: dropped from list after 5 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 17 | $39.50 | $2.06 | $+104.02 | $1,659.86 | ▲ +104.02 after sell → book $10,571.20; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 3 | $182.33 | $2.02 | $+30.42 | $2,204.83 | ▲ +30.42 after sell → book $10,569.18; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ILMN` | 2 | $249.13 | $2.02 | $+26.55 | $2,701.07 | ▲ +26.55 after sell → book $10,567.16; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 4 | $146.50 | $2.02 | $-8.46 | $3,285.05 | ▼ -8.46 after sell → book $10,565.14; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 83 | $7.98 | $2.26 | $+27.87 | $3,945.13 | ▲ +27.87 after sell → book $10,562.88; vs 09:30 mark -2.26 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ARQT` | 24 | $26.14 | $2.08 | $+0.42 | $4,570.40 | ▲ +0.42 after sell → book $10,560.79; vs 09:30 mark -2.09 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `CIFR` | 35 | $17.80 | $2.12 | $-12.44 | $5,191.29 | ▼ -12.44 after sell → book $10,558.68; vs 09:30 mark -2.11 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BRKR` | 10 | $63.37 | $2.04 | $+10.64 | $5,822.95 | ▲ +10.64 after sell → book $10,556.64; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `LITE` | 1 | $915.66 | $2.01 | $-23.23 | $6,736.60 | ▼ -23.23 after sell → book $10,554.63; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TNDM` | 54 | $17.13 | $2.17 | $-36.18 | $7,659.44 | ▼ -36.18 after sell → book $10,552.45; vs 09:30 mark -2.18 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BAK` | 543 | $1.77 | $7.10 | $-14.11 | $8,613.45 | ▼ -14.11 after sell → book $10,545.35; vs 09:30 mark -7.10 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 4 | $236.80 | $2.02 | $-11.22 | $9,558.63 | ▼ -11.22 after sell → book $10,543.33; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 42 | $14.07 | $2.12 | — | $8,965.57 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; combo leftover $597.41; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 40 | $14.79 | $2.11 | — | $8,371.86 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; combo leftover $597.41; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 2 | $219.62 | $2.00 | — | $7,930.63 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; combo leftover $597.41; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 7 | $85.00 | $2.01 | — | $7,333.61 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=+18.3; combo leftover $597.41; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 151 | $3.95 | $2.44 | — | $6,734.72 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; combo leftover $597.41; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 20 | $29.32 | $2.05 | — | $6,146.27 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; combo leftover $597.41; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 196 | $3.04 | $2.58 | — | $5,548.83 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; combo leftover $597.41; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `VITL` | 52 | $11.38 | $2.15 | — | $4,954.93 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+19.5; combo leftover $597.41; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 328 | $7.54 | $4.23 | — | $2,479.22 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; 🔵; ret5=-20.9; combo leftover $2477.46; owner union_news_g_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 118 | $20.91 | $2.34 | — | $9.49 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+8.6; combo leftover $2477.46; owner union_news_g_h1 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.49 | ▲ close $10,567.75 vs 09:30 $10,575.45 (session +48.45) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.49 | ▲ 09:30 equity $10,748.69 vs yday $10,567.75 (+180.94) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 43 | $22.78 | $2.14 | $+24.12 | $986.89 | ▲ +24.12 after sell → book $10,746.55; vs 09:30 mark -2.14 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 42 | $13.90 | $2.14 | $-11.39 | $1,568.56 | ▼ -11.39 after sell → book $10,744.42; vs 09:30 mark -2.13 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 40 | $14.58 | $2.13 | $-12.64 | $2,149.63 | ▼ -12.64 after sell → book $10,742.29; vs 09:30 mark -2.13 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VICR` | 2 | $230.25 | $2.02 | $+17.25 | $2,608.11 | ▲ +17.25 after sell → book $10,740.27; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ECO` | 7 | $82.83 | $2.03 | $-19.23 | $3,185.89 | ▼ -19.23 after sell → book $10,738.24; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `EYPT` | 151 | $3.87 | $2.48 | $-17.00 | $3,767.78 | ▼ -17.00 after sell → book $10,735.76; vs 09:30 mark -2.48 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 20 | $29.43 | $2.07 | $-1.92 | $4,354.31 | ▼ -1.92 after sell → book $10,733.69; vs 09:30 mark -2.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 196 | $4.00 | $2.62 | $+183.94 | $5,135.69 | ▲ +183.94 after sell → book $10,731.07; vs 09:30 mark -2.62 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VITL` | 52 | $12.05 | $2.17 | $+30.53 | $5,760.13 | ▲ +30.53 after sell → book $10,728.91; vs 09:30 mark -2.16 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FLNC` | 328 | $7.36 | $4.30 | $-65.94 | $8,169.90 | ▼ -65.94 after sell → book $10,724.60; vs 09:30 mark -4.31 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 118 | $21.65 | $2.38 | $+82.59 | $10,722.22 | ▲ +82.59 after sell → book $10,722.22; vs 09:30 mark -2.38 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 4 | $157.87 | $2.00 | — | $10,088.73 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; ret5=+6.5; combo leftover $670.14; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 7 | $88.83 | $2.01 | — | $9,464.91 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; ret5=+7.6; combo leftover $670.14; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 71 | $9.31 | $2.20 | — | $8,801.70 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=+5.3; combo leftover $670.14; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 49 | $13.47 | $2.14 | — | $8,139.29 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; combo leftover $670.14; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 67 | $9.99 | $2.19 | — | $7,467.77 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+5.3; combo leftover $670.14; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 39 | $16.91 | $2.11 | — | $6,806.17 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+50.5; combo leftover $670.14; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `USDE` | 51 | $13.05 | $2.14 | — | $6,138.48 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+40.4; combo leftover $670.14; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `GEMI` | 116 | $5.75 | $2.34 | — | $5,468.56 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+30.3; combo leftover $670.14; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 35 | $25.95 | $2.10 | — | $4,558.21 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ret5=+1.2; combo leftover $911.43; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `AMTX` | 423 | $2.15 | $5.46 | — | $3,643.31 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+7.5; combo leftover $911.43; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `MARA` | 65 | $13.94 | $2.19 | — | $2,735.02 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,ohlc_hot; 🔵; ret5=+10.5; combo leftover $911.43; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SION` | 151 | $6.00 | $2.44 | — | $1,826.58 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-24.1; combo leftover $911.43; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 4 | $190.30 | $2.00 | — | $1,063.38 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+10.6; combo leftover $911.43; owner union_news_g_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 3 | $230.25 | $2.00 | — | $370.63 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+12.5; combo leftover $911.43; owner union_news_g_h1 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $370.63 | ▼ close $10,574.00 vs 09:30 $10,748.69 (session -114.90) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $370.63 | ▲ 09:30 equity $10,585.48 vs yday $10,574.00 (+11.48) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `SBET` | 67 | $9.91 | $2.21 | $-9.76 | $1,032.39 | ▼ -9.76 after sell → book $10,583.27; vs 09:30 mark -2.21 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `USDE` | 51 | $12.99 | $2.16 | $-7.37 | $1,692.71 | ▼ -7.37 after sell → book $10,581.10; vs 09:30 mark -2.17 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `GEMI` | 116 | $6.05 | $2.37 | $+30.09 | $2,392.73 | ▲ +30.09 after sell → book $10,578.74; vs 09:30 mark -2.36 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `MARA` | 65 | $13.13 | $2.21 | $-57.04 | $3,243.97 | ▼ -57.04 after sell → book $10,576.53; vs 09:30 mark -2.21 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `SION` | 151 | $5.99 | $2.48 | $-6.43 | $4,145.98 | ▼ -6.43 after sell → book $10,574.05; vs 09:30 mark -2.48 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `ARM` | 1 | $319.41 | $1.99 | — | $3,824.58 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+35.1; combo leftover $345.50; owner union_join_vol_green_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `FSLY` | 12 | $28.02 | $2.03 | — | $3,486.31 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; ret5=+10.3; combo leftover $345.50; owner union_join_vol_green_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 575 | $1.01 | $7.42 | — | $2,898.15 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; ret5=+14.3; combo leftover $581.05; owner union_news_g_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `MRNA` | 3 | $168.50 | $2.00 | — | $2,390.65 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+17.9; combo leftover $581.05; owner union_news_g_h1 | — |
| 2026-09-22 09:30 ET | **BUY** | `DGXX` | 135 | $4.30 | $2.40 | — | $1,807.75 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+16.9; combo leftover $581.05; owner union_news_g_h1 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,807.75 | ▼ close $10,553.66 vs 09:30 $10,585.48 (session -4.56) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,807.75 | ▲ 09:30 equity $10,695.37 vs yday $10,553.66 (+141.71) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `BKKT` | 71 | $9.50 | $2.22 | $+9.06 | $2,480.03 | ▲ +9.06 after sell → book $10,693.14; vs 09:30 mark -2.23 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BTDR` | 49 | $12.84 | $2.16 | $-35.41 | $3,107.03 | ▼ -35.41 after sell → book $10,690.99; vs 09:30 mark -2.15 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 39 | $16.92 | $2.13 | $-3.84 | $3,764.78 | ▼ -3.84 after sell → book $10,688.86; vs 09:30 mark -2.13 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `GLXY` | 35 | $26.58 | $2.12 | $+17.84 | $4,692.97 | ▲ +17.84 after sell → book $10,686.74; vs 09:30 mark -2.12 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `AMTX` | 423 | $2.09 | $5.54 | $-36.37 | $5,571.50 | ▼ -36.37 after sell → book $10,681.21; vs 09:30 mark -5.53 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 4 | $174.50 | $2.02 | $-67.22 | $6,267.48 | ▼ -67.22 after sell → book $10,679.18; vs 09:30 mark -2.03 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `VICR` | 3 | $266.50 | $2.02 | $+104.73 | $7,064.96 | ▲ +104.73 after sell → book $10,677.17; vs 09:30 mark -2.01 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `ARM` | 1 | $331.78 | $2.01 | $+8.36 | $7,394.73 | ▲ +8.36 after sell → book $10,675.15; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `FSLY` | 12 | $25.90 | $2.05 | $-29.51 | $7,703.48 | ▼ -29.51 after sell → book $10,673.11; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `IVVD` | 575 | $0.95 | $7.29 | $-49.21 | $8,242.44 | ▼ -49.21 after sell → book $10,665.81; vs 09:30 mark -7.30 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `MRNA` | 3 | $183.41 | $2.02 | $+40.70 | $8,790.63 | ▲ +40.70 after sell → book $10,663.79; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 46 | $15.72 | $2.13 | — | $8,065.39 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; combo leftover $732.55; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 35 | $20.65 | $2.10 | — | $7,340.54 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; combo leftover $732.55; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `TNGX` | 28 | $25.40 | $2.07 | — | $6,627.27 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; combo leftover $732.55; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `NMRA` | 953 | $0.77 | $10.18 | — | $5,885.19 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; combo leftover $732.55; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 17 | $41.76 | $2.04 | — | $5,173.22 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+36.4; combo leftover $732.55; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `BFLY` | 73 | $9.90 | $2.21 | — | $4,448.32 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; combo leftover $732.55; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 111 | $7.95 | $2.32 | — | $3,563.54 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ⚪; ret5=+12.4; combo leftover $889.66; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `CMPX` | 729 | $1.22 | $9.40 | — | $2,664.76 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; 🔵; ret5=-33.0; combo leftover $889.66; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `VERI` | 684 | $1.30 | $8.82 | — | $1,766.73 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.3; combo leftover $889.66; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLSH` | 22 | $40.00 | $2.06 | — | $884.68 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+6.7; combo leftover $889.66; owner union_news_g_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 4 | $196.78 | $2.00 | — | $95.56 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $889.66; owner union_news_g_h1 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $95.56 | ▼ close $10,308.63 vs 09:30 $10,695.37 (session -309.83) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.56 | ▼ 09:30 equity $10,135.57 vs yday $10,308.63 (-173.06) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 4 | $163.95 | $2.02 | $+20.30 | $749.33 | ▲ +20.30 after sell → book $10,133.55; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 7 | $87.67 | $2.03 | $-12.13 | $1,361.03 | ▼ -12.13 after sell → book $10,131.52; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `DGXX` | 135 | $4.12 | $2.43 | $-29.12 | $1,914.80 | ▼ -29.12 after sell → book $10,129.09; vs 09:30 mark -2.43 | union_news_g_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 46 | $14.38 | $2.15 | $-65.92 | $2,574.13 | ▼ -65.92 after sell → book $10,126.95; vs 09:30 mark -2.14 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 35 | $20.52 | $2.12 | $-8.76 | $3,290.22 | ▼ -8.76 after sell → book $10,124.83; vs 09:30 mark -2.12 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `TNGX` | 28 | $23.99 | $2.09 | $-43.65 | $3,959.84 | ▼ -43.65 after sell → book $10,122.74; vs 09:30 mark -2.09 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `NMRA` | 953 | $0.75 | $10.14 | $-41.28 | $4,660.65 | ▼ -41.28 after sell → book $10,112.60; vs 09:30 mark -10.14 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 17 | $36.02 | $2.06 | $-101.60 | $5,271.01 | ▼ -101.60 after sell → book $10,110.54; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BFLY` | 73 | $9.12 | $2.23 | $-61.38 | $5,934.54 | ▼ -61.38 after sell → book $10,108.31; vs 09:30 mark -2.23 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 111 | $7.38 | $2.35 | $-67.94 | $6,751.37 | ▼ -67.94 after sell → book $10,105.96; vs 09:30 mark -2.35 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CMPX` | 729 | $1.17 | $9.54 | $-55.39 | $7,594.76 | ▼ -55.39 after sell → book $10,096.42; vs 09:30 mark -9.54 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VERI` | 684 | $1.27 | $8.95 | $-38.29 | $8,454.50 | ▼ -38.29 after sell → book $10,087.48; vs 09:30 mark -8.94 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BLSH` | 22 | $39.27 | $2.08 | $-20.19 | $9,316.36 | ▼ -20.19 after sell → book $10,085.40; vs 09:30 mark -2.08 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 4 | $192.26 | $2.02 | $-22.10 | $10,083.38 | ▼ -22.10 after sell → book $10,083.38; vs 09:30 mark -2.02 | union_news_g_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,083.38 | ▲ close $10,083.38 vs 09:30 $10,135.57 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,782.97 | ▲ 09:30 equity $8,782.97 vs yday $8,782.97 (+0.00) | 09:30 open · cash $8,782.97 (unchanged overnight, no fees) · equity $8,782.97 vs prior close $8,782.97 (+0.00) | — |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 142 | $3.86 | $2.42 | — | $8,232.43 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; combo leftover $548.94; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 33 | $16.21 | $2.09 | — | $7,695.41 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; combo leftover $548.94; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 20 | $26.27 | $2.05 | — | $7,167.96 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; combo leftover $548.94; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TWST` | 2 | $184.00 | $2.00 | — | $6,797.97 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.3; combo leftover $548.94; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GRAL` | 4 | $123.50 | $2.00 | — | $6,301.97 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+56.6; combo leftover $548.94; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `QMCO` | 18 | $29.80 | $2.04 | — | $5,763.52 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+18.2; combo leftover $548.94; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CYPH` | 137 | $4.00 | $2.40 | — | $5,212.44 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+32.9; combo leftover $548.94; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CDNA` | 8 | $61.33 | $2.01 | — | $4,719.78 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+13.1; combo leftover $548.94; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 5 | $272.16 | $2.00 | — | $3,356.98 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+11.7; combo leftover $1573.26; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `RKLB` | 21 | $74.15 | $2.05 | — | $1,797.77 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+8.5; combo leftover $1573.26; owner union_news_g_h1 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 1 | $887.00 | $1.99 | — | $908.78 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=+0.3; combo leftover $1573.26; owner union_news_g_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $908.78 | ▲ close $8,849.48 vs 09:30 $8,782.97 (session +89.57) | 16:00 close · cash $908.78 · equity $8,849.48 vs 09:30 $8,782.97 (+66.51; session marks +89.57) · 11 name(s) marked open→close (per-name table). ZSQR×142 09:30 $3.86 → close $3.78 -11.36; SECZ×33 09:30 $16.21 → close $15.96 -8.25; WRBY×20 09:30 $26.27 → close $26.71 +8.80; TWST×2 09:30 $184.00 → close $182.83 -2.34; GRAL×4 09:30 $123.50 → close $126.89 +13.56; QMCO×18 09:30 $29.80 → close $31.68 +33.84; CYPH×137 09:30 $4.00 → close $4.12 +15.76; CDNA×8 09:30 $61.33 → close $63.68 +18.80; ILMN×5 09:30 $272.16 → close $270.00 -10.80; RKLB×21 09:30 $74.15 → close $73.95 -4.20; COST×1 09:30 $887.00 → close $922.76 +35.76 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 715.41 < 1 share @ 1646.93 |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `GO` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new long union_news_g_h1 |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new long union_news_g_h1 |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new long union_news_g_h1 |
| 2026-08-19 | `ANGX` | hard_red | hard-red S=-7.20 sit; no new long union_news_g_h1 |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new long union_news_g_h1 |
| 2026-08-19 | `AS` | hard_red | hard-red S=-7.20 sit; no new long union_news_g_h1 |
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
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `SLBT` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new long union_news_g_h1 |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `TEL` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `RLMD` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `POWW` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `FRNM` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `DE` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-02 | `CIEN` | hard_red | hard-red S=-3.83 sit; no new long union_news_g_h1 |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new long union_news_g_h1 |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new long union_news_g_h1 |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new long union_news_g_h1 |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new long union_news_g_h1 |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `CRCL` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `TRMD` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-09 | `DFDV` | hard_red | hard-red S=-13.95 sit; no new long union_news_g_h1 |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new long union_news_g_h1 |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new long union_news_g_h1 |
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
| 2026-09-22 | `A` | no_price | no 09:30 open — carry |
| 2026-09-22 | `DXCM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BKKT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BTDR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TJGC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `GLXY` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AMTX` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SMTC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `VICR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `GRPN` | no_price | no 09:30 open |
| 2026-09-22 | `XXI` | no_price | no 09:30 open |
| 2026-09-22 | `META` | cash | leftover split 345.50 < 1 share @ 731.40 |
| 2026-09-22 | `THO` | no_price | no 09:30 open |
| 2026-09-22 | `WBD` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `YSS` | no_price | no 09:30 open |
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
