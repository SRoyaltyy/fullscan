# Factor mine action — `combo_p2s_5050_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared union_news_pack_net2_h1/short_news_r_h3 w=0.5,0.5 net=priority

Cash book **-13.74%** ($8,626) · signal-only (no cash/fees) was —. Starts YES **2/30**. Fills 170 · skips 145 · realized $+1088.80.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: union_news_pack_net2_h1 50%, short_news_r_h3 50%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: union_news_pack_net2_h1 50%, short_news_r_h3 50%.
- Member: union_news_pack_net2_h1 (50% · long · hold 1).
- Member: short_news_r_h3 (50% · short · hold 3).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $14,107.32.

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
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 13 | $120.00 | $2.03 | — | $8,437.97 | — | packet🟢 and camera net ≥ 2; gate news_box=good,cam_net_min=2; rank cond; list flatten; 🔵; ret5=+0.6; combo leftover $1666.67; owner union_news_pack_net2_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 4 | $359.83 | $2.00 | — | $6,996.65 | — | packet🟢 and camera net ≥ 2; gate news_box=good,cam_net_min=2; rank cond; list flatten; 🔵; ret5=+5.9; combo leftover $1666.67; owner union_news_pack_net2_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 11 | $146.90 | $2.02 | — | $5,378.73 | — | packet🟢 and camera net ≥ 2; gate news_box=good,cam_net_min=2; rank cond; list flatten; 🔵; ret5=+3.6; combo leftover $1666.67; owner union_news_pack_net2_h1 | — |
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 1411 | $1.18 | $18.50 | — | $7,025.21 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $1665.66; owner short_news_r_h3 | — |
| 2026-08-14 09:30 ET | **SHORT** | `LUNR` | 86 | $19.17 | $2.32 | — | $8,671.51 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+17.6; combo leftover $1665.66; owner short_news_r_h3 | — |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 131 | $12.70 | $2.46 | — | $10,332.09 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.6; combo leftover $1665.66; owner short_news_r_h3 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,332.09 | ▲ close $10,110.61 vs 09:30 $10,000.00 (session +139.94) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,332.09 | ▼ 09:30 equity $10,066.35 vs yday $10,110.61 (-44.26) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 13 | $127.40 | $2.05 | $+92.12 | $11,986.24 | ▲ +92.12 after sell → book $10,064.30; vs 09:30 mark -2.05 | union_news_pack_net2_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 4 | $367.88 | $2.02 | $+28.17 | $13,455.74 | ▲ +28.17 after sell → book $10,062.28; vs 09:30 mark -2.02 | union_news_pack_net2_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 11 | $149.37 | $2.05 | $+23.10 | $15,096.76 | ▲ +23.10 after sell → book $10,060.23; vs 09:30 mark -2.05 | union_news_pack_net2_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 54 | $46.18 | $2.15 | — | $12,600.89 | — | packet🟢 and camera net ≥ 2; gate news_box=good,cam_net_min=2; rank cond; list flatten; 🔵; ret5=+6.7; combo leftover $2516.13; owner union_news_pack_net2_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 17 | $142.77 | $2.04 | — | $10,171.76 | — | packet🟢 and camera net ≥ 2; gate news_box=good,cam_net_min=2; rank cond; list flatten; 🔵; ret5=+5.8; combo leftover $2516.13; owner union_news_pack_net2_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 12 | $202.70 | $2.03 | — | $7,737.33 | — | packet🟢 and camera net ≥ 2; gate news_box=good,cam_net_min=2; rank cond; list flatten; 🔵; ret5=+8.3; combo leftover $2516.13; owner union_news_pack_net2_h1 | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 874 | $1.15 | $11.46 | — | $8,730.97 | — | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; combo leftover $1005.40; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 282 | $3.56 | $3.72 | — | $9,731.17 | — | news🔴; gate news=bad; list yday_mover; ret5=-15.6; combo leftover $1005.40; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `APMD` | 31 | $31.70 | $2.13 | — | $10,711.74 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+17.6; combo leftover $1005.40; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 334 | $3.01 | $4.40 | — | $11,712.68 | — | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; combo leftover $1005.40; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `RNW` | 147 | $6.80 | $2.49 | — | $12,709.79 | — | news🔴; gate news=bad; list overnight; ⚪; ret5=+10.4; combo leftover $1005.40; owner short_news_r_h3 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,709.79 | ▲ close $10,333.96 vs 09:30 $10,066.35 (session +304.15) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,709.79 | ▲ 09:30 equity $10,572.60 vs yday $10,333.96 (+238.64) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 54 | $48.00 | $2.18 | $+93.95 | $15,299.60 | ▲ +93.95 after sell → book $10,570.41; vs 09:30 mark -2.19 | union_news_pack_net2_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 17 | $148.04 | $2.07 | $+85.48 | $17,814.21 | ▲ +85.48 after sell → book $10,568.34; vs 09:30 mark -2.07 | union_news_pack_net2_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 12 | $208.93 | $2.06 | $+70.68 | $20,319.32 | ▲ +70.68 after sell → book $10,566.29; vs 09:30 mark -2.05 | union_news_pack_net2_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,319.32 | ▲ close $10,806.04 vs 09:30 $10,572.60 (session +239.75) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,319.32 | ▼ 09:30 equity $10,773.67 vs yday $10,806.04 (-32.37) | — | — |
| 2026-08-19 09:30 ET | **COVER** | `EU` | 1411 | $1.07 | $18.20 | $+118.51 | $18,791.35 | ▲ +118.51 after sell → book $10,755.47; vs 09:30 mark -18.20 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 131 | $11.75 | $2.38 | $+118.95 | $17,249.71 | ▲ +118.95 after sell → book $10,753.08; vs 09:30 mark -2.39 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,249.71 | ▲ close $10,801.06 vs 09:30 $10,773.67 (session +47.98) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,249.71 | ▼ 09:30 equity $10,742.30 vs yday $10,801.06 (-58.76) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `LUNR` | 86 | $18.13 | $2.25 | $+84.87 | $15,688.28 | ▲ +84.87 after sell → book $10,740.05; vs 09:30 mark -2.25 | short_news_r_h3: dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 874 | $0.96 | $11.04 | $+140.94 | $14,835.58 | ▲ +140.94 after sell → book $10,729.01; vs 09:30 mark -11.04 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ZNTL` | 282 | $4.01 | $3.64 | $-135.67 | $13,699.72 | ▼ -135.67 after sell → book $10,725.38; vs 09:30 mark -3.63 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `APMD` | 31 | $31.87 | $2.08 | $-9.48 | $12,709.66 | ▼ -9.48 after sell → book $10,723.29; vs 09:30 mark -2.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 334 | $2.95 | $4.31 | $+11.33 | $11,720.05 | ▲ +11.33 after sell → book $10,718.98; vs 09:30 mark -4.31 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `RNW` | 147 | $6.81 | $2.43 | $-6.39 | $10,716.55 | ▼ -6.39 after sell → book $10,716.55; vs 09:30 mark -2.43 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 119 | $44.76 | $2.35 | — | $5,387.77 | — | packet🟢 and camera net ≥ 2; gate news_box=good,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+8.7; combo leftover $5358.28; owner union_news_pack_net2_h1 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 3 | $204.45 | $2.04 | — | $5,999.08 | — | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; combo leftover $669.64; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 31 | $21.40 | $2.12 | — | $6,660.36 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-25.2; combo leftover $669.64; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 151 | $4.43 | $2.50 | — | $7,326.79 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-23.1; combo leftover $669.64; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 56 | $11.81 | $2.20 | — | $7,986.24 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $669.64; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 3 | $173.90 | $2.03 | — | $8,505.90 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; combo leftover $669.64; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 14 | $46.85 | $2.07 | — | $9,159.73 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=+5.0; combo leftover $669.64; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 6 | $106.38 | $2.05 | — | $9,795.97 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; combo leftover $669.64; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 145 | $4.61 | $2.48 | — | $10,461.94 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $669.64; owner short_news_r_h3 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,461.94 | ▲ close $10,729.58 vs 09:30 $10,742.30 (session +32.86) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,461.94 | ▼ 09:30 equity $10,691.74 vs yday $10,729.58 (-37.84) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 119 | $44.52 | $2.41 | $-33.32 | $15,757.41 | ▼ -33.32 after sell → book $10,689.33; vs 09:30 mark -2.41 | union_news_pack_net2_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 32 | $119.43 | $2.09 | — | $11,933.57 | — | packet🟢 and camera net ≥ 2; gate news_box=good,cam_net_min=2; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; combo leftover $3939.35; owner union_news_pack_net2_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 6 | $623.26 | $2.01 | — | $8,192.00 | — | packet🟢 and camera net ≥ 2; gate news_box=good,cam_net_min=2; rank cond; list probable,yday_gainer; 🔵; ret5=+1.4; combo leftover $3939.35; owner union_news_pack_net2_h1 | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 286 | $3.11 | $3.77 | — | $9,077.69 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; combo leftover $890.44; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `MRNA` | 6 | $133.11 | $2.05 | — | $9,874.30 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=+109.5; combo leftover $890.44; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 9 | $89.10 | $2.06 | — | $10,674.14 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $890.44; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 23 | $38.40 | $2.10 | — | $11,555.23 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $890.44; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 42 | $20.90 | $2.16 | — | $12,430.87 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $890.44; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 32 | $27.00 | $2.13 | — | $13,292.74 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; combo leftover $890.44; owner short_news_r_h3 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,292.74 | ▲ close $10,863.94 vs 09:30 $10,691.74 (session +192.98) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,292.74 | ▲ 09:30 equity $10,938.31 vs yday $10,863.94 (+74.37) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 32 | $120.51 | $2.13 | $+30.35 | $17,146.94 | ▲ +30.35 after sell → book $10,936.19; vs 09:30 mark -2.12 | union_news_pack_net2_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 6 | $653.04 | $2.05 | $+174.62 | $21,063.13 | ▲ +174.62 after sell → book $10,934.14; vs 09:30 mark -2.05 | union_news_pack_net2_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,063.13 | ▲ close $11,008.59 vs 09:30 $10,938.31 (session +74.45) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,063.13 | ▲ 09:30 equity $11,053.05 vs yday $11,008.59 (+44.46) | — | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 3 | $212.00 | $2.00 | $-26.68 | $20,425.13 | ▼ -26.68 after sell → book $11,051.05; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 31 | $20.90 | $2.08 | $+11.30 | $19,775.15 | ▲ +11.30 after sell → book $11,048.97; vs 09:30 mark -2.08 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TOYO` | 151 | $4.42 | $2.44 | $-3.43 | $19,105.28 | ▼ -3.43 after sell → book $11,046.52; vs 09:30 mark -2.45 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ABCL` | 56 | $11.00 | $2.16 | $+41.29 | $18,487.13 | ▲ +41.29 after sell → book $11,044.37; vs 09:30 mark -2.15 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 3 | $170.64 | $2.00 | $+5.75 | $17,973.21 | ▲ +5.75 after sell → book $11,042.37; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AAP` | 14 | $43.63 | $2.03 | $+40.98 | $17,360.35 | ▲ +40.98 after sell → book $11,040.33; vs 09:30 mark -2.04 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 6 | $105.58 | $2.01 | $+0.75 | $16,724.87 | ▲ +0.75 after sell → book $11,038.33; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 145 | $4.77 | $2.42 | $-28.10 | $16,030.79 | ▼ -28.10 after sell → book $11,035.90; vs 09:30 mark -2.43 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 33 | $118.52 | $2.09 | — | $12,117.54 | — | packet🟢 and camera net ≥ 2; gate news_box=good,cam_net_min=2; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; combo leftover $4007.70; owner union_news_pack_net2_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 51 | $77.13 | $2.14 | — | $8,181.77 | — | packet🟢 and camera net ≥ 2; gate news_box=good,cam_net_min=2; rank cond; list mover_buy; ⚪; ret5=+13.8; combo leftover $4007.70; owner union_news_pack_net2_h1 | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 101 | $13.62 | $2.36 | — | $9,555.54 | — | news🔴; gate news=bad; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; combo leftover $1378.96; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 25 | $54.51 | $2.12 | — | $10,916.16 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+15.1; combo leftover $1378.96; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `BMO` | 7 | $175.01 | $2.06 | — | $12,139.17 | — | news🔴; gate news=bad; list earn_react; ret5=-7.0; combo leftover $1378.96; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `INTU` | 3 | $364.35 | $2.05 | — | $13,230.17 | — | news🔴; gate news=bad; list overnight,overnight_mega; 🔵; ret5=+10.2; combo leftover $1378.96; owner short_news_r_h3 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,230.17 | ▲ close $11,193.78 vs 09:30 $11,053.05 (session +170.71) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,230.17 | ▲ 09:30 equity $11,243.89 vs yday $11,193.78 (+50.11) | — | — |
| 2026-08-26 09:30 ET | **COVER** | `QTRX` | 286 | $2.83 | $3.69 | $+72.62 | $12,417.10 | ▲ +72.62 after sell → book $11,240.20; vs 09:30 mark -3.69 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `MRNA` | 6 | $154.20 | $2.01 | $-130.60 | $11,489.89 | ▼ -130.60 after sell → book $11,238.19; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 9 | $88.24 | $2.02 | $+3.66 | $10,693.71 | ▲ +3.66 after sell → book $11,236.17; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 23 | $38.41 | $2.06 | $-4.39 | $9,808.23 | ▼ -4.39 after sell → book $11,234.12; vs 09:30 mark -2.05 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 42 | $20.50 | $2.12 | $+12.52 | $8,945.11 | ▲ +12.52 after sell → book $11,232.00; vs 09:30 mark -2.12 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 32 | $26.00 | $2.09 | $+27.78 | $8,111.02 | ▲ +27.78 after sell → book $11,229.91; vs 09:30 mark -2.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 33 | $119.80 | $2.13 | $+38.02 | $12,062.29 | ▲ +38.02 after sell → book $11,227.78; vs 09:30 mark -2.13 | union_news_pack_net2_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 51 | $79.34 | $2.19 | $+108.38 | $16,106.45 | ▲ +108.38 after sell → book $11,225.60; vs 09:30 mark -2.18 | union_news_pack_net2_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 15 | $267.02 | $2.04 | — | $12,099.11 | — | packet🟢 and camera net ≥ 2; gate news_box=good,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+16.7; combo leftover $4026.61; owner union_news_pack_net2_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 33 | $118.50 | $2.09 | — | $8,186.52 | — | packet🟢 and camera net ≥ 2; gate news_box=good,cam_net_min=2; rank cond; list overnight,overnight_mega; 🔵; ret5=-2.7; combo leftover $4026.61; owner union_news_pack_net2_h1 | — |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 5 | $213.94 | $2.05 | — | $9,254.17 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; combo leftover $1122.15; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 91 | $12.22 | $2.32 | — | $10,363.87 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.4; combo leftover $1122.15; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 220 | $5.08 | $2.92 | — | $11,478.56 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+17.6; combo leftover $1122.15; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 8 | $132.64 | $2.06 | — | $12,537.61 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+16.5; combo leftover $1122.15; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `CRM` | 5 | $199.94 | $2.05 | — | $13,535.26 | — | news🔴; gate news=bad; list overnight,overnight_mega; ret5=+2.1; combo leftover $1122.15; owner short_news_r_h3 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,535.26 | ▼ close $11,027.01 vs 09:30 $11,243.89 (session -183.06) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,535.26 | ▼ 09:30 equity $10,871.22 vs yday $11,027.01 (-155.79) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 15 | $267.23 | $2.08 | $-0.96 | $17,541.63 | ▼ -0.96 after sell → book $10,869.14; vs 09:30 mark -2.08 | union_news_pack_net2_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 15 | $81.65 | $2.04 | — | $16,314.85 | — | packet🟢 and camera net ≥ 2; gate news_box=good,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+2.0; combo leftover $1252.97; owner union_news_pack_net2_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $15,345.84 | — | packet🟢 and camera net ≥ 2; gate news_box=good,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+0.1; combo leftover $1252.97; owner union_news_pack_net2_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 3 | $318.88 | $2.00 | — | $14,387.21 | — | packet🟢 and camera net ≥ 2; gate news_box=good,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+1.9; combo leftover $1252.97; owner union_news_pack_net2_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 5 | $222.86 | $2.00 | — | $13,270.90 | — | packet🟢 and camera net ≥ 2; gate news_box=good,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=-3.6; combo leftover $1252.97; owner union_news_pack_net2_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `ADSK` | 4 | $261.47 | $2.00 | — | $12,223.02 | — | packet🟢 and camera net ≥ 2; gate news_box=good,cam_net_min=2; rank cond; list overnight,overnight_mega; 🔵; ret5=+1.4; combo leftover $1252.97; owner union_news_pack_net2_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `AXTI` | 17 | $70.30 | $2.04 | — | $11,025.88 | — | packet🟢 and camera net ≥ 2; gate news_box=good,cam_net_min=2; rank cond; list yday_gainer,yday_mover; 🔵; ret5=-11.2; combo leftover $1252.97; owner union_news_pack_net2_h1 | — |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 36 | $74.54 | $2.20 | — | $13,707.11 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=-0.1; combo leftover $2714.27; owner short_news_r_h3 | — |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 49 | $55.25 | $2.24 | — | $16,412.12 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=+2.1; combo leftover $2714.27; owner short_news_r_h3 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,412.12 | ▼ close $10,614.78 vs 09:30 $10,871.22 (session -237.84) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,412.12 | ▼ 09:30 equity $10,523.57 vs yday $10,614.78 (-91.21) | — | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 101 | $13.90 | $2.29 | $-32.43 | $15,005.93 | ▼ -32.43 after sell → book $10,521.28; vs 09:30 mark -2.29 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 25 | $52.49 | $2.06 | $+46.31 | $13,691.61 | ▲ +46.31 after sell → book $10,519.21; vs 09:30 mark -2.07 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `BMO` | 7 | $172.76 | $2.01 | $+11.67 | $12,480.28 | ▲ +11.67 after sell → book $10,517.20; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `INTU` | 3 | $347.82 | $2.00 | $+45.54 | $11,434.82 | ▲ +45.54 after sell → book $10,515.20; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 33 | $115.66 | $2.13 | $-97.94 | $15,249.47 | ▼ -97.94 after sell → book $10,513.07; vs 09:30 mark -2.13 | union_news_pack_net2_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 15 | $79.27 | $2.06 | $-39.79 | $16,436.47 | ▼ -39.79 after sell → book $10,511.02; vs 09:30 mark -2.05 | union_news_pack_net2_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $17,353.75 | ▼ -51.73 after sell → book $10,509.01; vs 09:30 mark -2.01 | union_news_pack_net2_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 3 | $318.03 | $2.02 | $-6.57 | $18,305.82 | ▼ -6.57 after sell → book $10,506.99; vs 09:30 mark -2.02 | union_news_pack_net2_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 5 | $227.36 | $2.02 | $+18.47 | $19,440.59 | ▲ +18.47 after sell → book $10,504.96; vs 09:30 mark -2.03 | union_news_pack_net2_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AXTI` | 17 | $65.29 | $2.06 | $-89.27 | $20,548.46 | ▼ -89.27 after sell → book $10,502.90; vs 09:30 mark -2.06 | union_news_pack_net2_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 7 | $324.41 | $2.01 | — | $18,275.58 | — | packet🟢 and camera net ≥ 2; gate news_box=good,cam_net_min=2; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; combo leftover $2568.56; owner union_news_pack_net2_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 6 | $400.42 | $2.01 | — | $15,871.05 | — | packet🟢 and camera net ≥ 2; gate news_box=good,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+1.9; combo leftover $2568.56; owner union_news_pack_net2_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $14,563.03 | — | packet🟢 and camera net ≥ 2; gate news_box=good,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+0.0; combo leftover $2568.56; owner union_news_pack_net2_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 10 | $240.22 | $2.02 | — | $12,158.81 | — | packet🟢 and camera net ≥ 2; gate news_box=good,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+4.5; combo leftover $2568.56; owner union_news_pack_net2_h1 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 10 | $252.24 | $2.12 | — | $14,679.09 | — | news🔴; gate news=bad; list probable,yday_gainer; ⚪; ret5=+2.2; combo leftover $2623.72; owner short_news_r_h3 | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 86 | $30.18 | $2.35 | — | $17,272.22 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+12.1; combo leftover $2623.72; owner short_news_r_h3 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,272.22 | ▲ close $10,654.59 vs 09:30 $10,523.57 (session +164.20) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,272.22 | ▲ 09:30 equity $10,733.81 vs yday $10,654.59 (+79.22) | — | — |
| 2026-08-31 09:30 ET | **COVER** | `BE` | 5 | $208.88 | $2.00 | $+21.24 | $16,225.81 | ▲ +21.24 after sell → book $10,731.80; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 91 | $11.10 | $2.26 | $+97.34 | $15,213.45 | ▲ +97.34 after sell → book $10,729.54; vs 09:30 mark -2.26 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 220 | $4.97 | $2.84 | $+17.35 | $14,116.11 | ▲ +17.35 after sell → book $10,726.70; vs 09:30 mark -2.84 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `NEM` | 8 | $127.45 | $2.01 | $+37.44 | $13,094.50 | ▲ +37.44 after sell → book $10,724.69; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `CRM` | 5 | $254.39 | $2.00 | $-276.31 | $11,820.54 | ▼ -276.31 after sell → book $10,722.68; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 4 | $257.71 | $2.02 | $-19.06 | $12,849.36 | ▼ -19.06 after sell → book $10,720.66; vs 09:30 mark -2.02 | union_news_pack_net2_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 7 | $322.49 | $2.04 | $-17.49 | $15,104.75 | ▼ -17.49 after sell → book $10,718.62; vs 09:30 mark -2.04 | union_news_pack_net2_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 6 | $378.44 | $2.04 | $-135.92 | $17,373.35 | ▼ -135.92 after sell → book $10,716.58; vs 09:30 mark -2.04 | union_news_pack_net2_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MPWR` | 1 | $1261.90 | $2.01 | $-48.14 | $18,633.24 | ▼ -48.14 after sell → book $10,714.57; vs 09:30 mark -2.01 | union_news_pack_net2_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DDOG` | 10 | $233.97 | $2.05 | $-66.62 | $20,970.84 | ▼ -66.62 after sell → book $10,712.52; vs 09:30 mark -2.05 | union_news_pack_net2_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,970.84 | ▲ close $10,766.52 vs 09:30 $10,733.81 (session +54.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,970.84 | ▲ 09:30 equity $10,923.62 vs yday $10,766.52 (+157.10) | — | — |
| 2026-09-01 09:30 ET | **COVER** | `MT` | 36 | $73.22 | $2.10 | $+43.22 | $18,332.82 | ▲ +43.22 after sell → book $10,921.52; vs 09:30 mark -2.10 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `TX` | 49 | $54.76 | $2.14 | $+19.63 | $15,647.45 | ▲ +19.63 after sell → book $10,919.39; vs 09:30 mark -2.13 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,647.45 | ▲ close $10,934.75 vs 09:30 $10,923.62 (session +15.36) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,647.45 | ▲ 09:30 equity $10,987.27 vs yday $10,934.75 (+52.52) | — | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 10 | $235.71 | $2.02 | $+161.16 | $13,288.33 | ▲ +161.16 after sell → book $10,985.25; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 86 | $26.78 | $2.25 | $+287.80 | $10,983.00 | ▲ +287.80 after sell → book $10,983.00; vs 09:30 mark -2.25 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,983.00 | ▲ close $10,983.00 vs 09:30 $10,987.27 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,983.00 | ▲ 09:30 equity $10,983.00 vs yday $10,983.00 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 5 | $351.74 | $2.00 | — | $9,222.29 | — | packet🟢 and camera net ≥ 2; gate news_box=good,cam_net_min=2; rank cond; list earn_react; 🔵; ret5=+3.3; combo leftover $1830.50; owner union_news_pack_net2_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 3 | $486.31 | $2.00 | — | $7,761.37 | — | packet🟢 and camera net ≥ 2; gate news_box=good,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+6.1; combo leftover $1830.50; owner union_news_pack_net2_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 38 | $47.60 | $2.10 | — | $5,950.46 | — | packet🟢 and camera net ≥ 2; gate news_box=good,cam_net_min=2; rank cond; list earn_react; 🔵; ret5=-6.2; combo leftover $1830.50; owner union_news_pack_net2_h1 | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 184 | $14.85 | $2.67 | — | $8,680.19 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; combo leftover $2744.22; owner short_news_r_h3 | — |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 1604 | $1.71 | $21.05 | — | $11,401.98 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+11.9; combo leftover $2744.22; owner short_news_r_h3 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,401.98 | ▲ close $11,501.87 vs 09:30 $10,983.00 (session +548.70) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,401.98 | ▲ 09:30 equity $11,545.84 vs yday $11,501.87 (+43.97) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 5 | $359.70 | $2.03 | $+35.77 | $13,198.45 | ▲ +35.77 after sell → book $11,543.81; vs 09:30 mark -2.03 | union_news_pack_net2_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 3 | $513.78 | $2.02 | $+78.39 | $14,737.77 | ▲ +78.39 after sell → book $11,541.79; vs 09:30 mark -2.02 | union_news_pack_net2_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 38 | $53.85 | $2.13 | $+233.27 | $16,781.94 | ▲ +233.27 after sell → book $11,539.66; vs 09:30 mark -2.13 | union_news_pack_net2_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 15 | $263.36 | $2.04 | — | $12,829.50 | — | packet🟢 and camera net ≥ 2; gate news_box=good,cam_net_min=2; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; combo leftover $4195.48; owner union_news_pack_net2_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 17 | $236.82 | $2.04 | — | $8,801.52 | — | packet🟢 and camera net ≥ 2; gate news_box=good,cam_net_min=2; rank cond; list ohlc_hot; ret5=+8.1; combo leftover $4195.48; owner union_news_pack_net2_h1 | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 617 | $4.67 | $8.16 | — | $11,674.75 | — | news🔴; gate news=bad; list yday_gainer; ret5=+11.9; combo leftover $2883.89; owner short_news_r_h3 | — |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 37 | $76.55 | $2.21 | — | $14,504.88 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; combo leftover $2883.89; owner short_news_r_h3 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,504.88 | ▲ close $11,645.13 vs 09:30 $11,545.84 (session +119.93) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,504.88 | ▲ 09:30 equity $11,861.49 vs yday $11,645.13 (+216.36) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 15 | $253.72 | $2.08 | $-148.71 | $18,308.61 | ▼ -148.71 after sell → book $11,859.42; vs 09:30 mark -2.07 | union_news_pack_net2_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 17 | $267.76 | $2.09 | $+521.85 | $22,858.44 | ▲ +521.85 after sell → book $11,857.33; vs 09:30 mark -2.09 | union_news_pack_net2_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22,858.44 | ▲ close $12,138.70 vs 09:30 $11,861.49 (session +281.37) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22,858.44 | ▲ 09:30 equity $12,175.00 vs yday $12,138.70 (+36.30) | — | — |
| 2026-09-09 09:30 ET | **COVER** | `SLN` | 184 | $13.60 | $2.54 | $+224.79 | $20,353.50 | ▲ +224.79 after sell → book $12,172.46; vs 09:30 mark -2.54 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 1604 | $1.58 | $20.69 | $+166.77 | $17,798.49 | ▲ +166.77 after sell → book $12,151.77; vs 09:30 mark -20.69 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,798.49 | ▲ close $12,180.64 vs 09:30 $12,175.00 (session +28.87) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,798.49 | ▲ 09:30 equity $12,267.14 vs yday $12,180.64 (+86.50) | — | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 617 | $4.36 | $7.96 | $+175.15 | $15,100.41 | ▲ +175.15 after sell → book $12,259.18; vs 09:30 mark -7.96 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PIPR` | 37 | $76.79 | $2.10 | $-13.19 | $12,257.08 | ▼ -13.19 after sell → book $12,257.08; vs 09:30 mark -2.10 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,257.08 | ▲ close $12,257.08 vs 09:30 $12,267.14 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,257.08 | ▲ 09:30 equity $12,257.08 vs yday $12,257.08 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 25 | $242.17 | $2.06 | — | $6,200.76 | — | packet🟢 and camera net ≥ 2; gate news_box=good,cam_net_min=2; rank cond; list earn_react; ret5=-11.1; combo leftover $6128.54; owner union_news_pack_net2_h1 | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 10 | $112.83 | $2.07 | — | $7,327.04 | — | news🔴; gate news=bad; list yday_gainer,ohlc_hot; ret5=+11.7; combo leftover $1225.50; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `RWT` | 348 | $3.52 | $4.59 | — | $8,547.41 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-19.2; combo leftover $1225.50; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `CRDL` | 603 | $2.03 | $7.92 | — | $9,763.58 | — | news🔴; gate news=bad; list yday_mover; ret5=-8.8; combo leftover $1225.50; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BKV` | 49 | $24.97 | $2.19 | — | $10,984.92 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.8; combo leftover $1225.50; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `MYGN` | 363 | $3.37 | $4.79 | — | $12,203.44 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+4.0; combo leftover $1225.50; owner short_news_r_h3 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,203.44 | ▲ close $12,475.58 vs 09:30 $12,257.08 (session +242.12) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,203.44 | ▲ 09:30 equity $12,743.88 vs yday $12,475.58 (+268.30) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 25 | $261.51 | $2.13 | $+479.31 | $18,739.06 | ▲ +479.31 after sell → book $12,741.75; vs 09:30 mark -2.13 | union_news_pack_net2_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,739.06 | ▼ close $12,583.50 vs 09:30 $12,743.88 (session -158.25) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,739.06 | ▼ 09:30 equity $12,583.13 vs yday $12,583.50 (-0.37) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,739.06 | ▼ close $12,480.64 vs 09:30 $12,583.13 (session -102.49) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,739.06 | ▲ 09:30 equity $12,498.84 vs yday $12,480.64 (+18.20) | — | — |
| 2026-09-16 09:30 ET | **COVER** | `QRVO` | 10 | $118.18 | $2.02 | $-57.54 | $17,555.24 | ▼ -57.54 after sell → book $12,496.82; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `RWT` | 348 | $3.98 | $4.49 | $-169.16 | $16,165.72 | ▼ -169.16 after sell → book $12,492.34; vs 09:30 mark -4.48 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `CRDL` | 603 | $1.85 | $7.78 | $+92.84 | $15,042.39 | ▲ +92.84 after sell → book $12,484.56; vs 09:30 mark -7.78 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `BKV` | 49 | $24.42 | $2.14 | $+22.62 | $13,843.67 | ▲ +22.62 after sell → book $12,482.42; vs 09:30 mark -2.14 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `MYGN` | 363 | $3.75 | $4.68 | $-147.41 | $12,477.74 | ▼ -147.41 after sell → book $12,477.74; vs 09:30 mark -4.68 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 16 | $189.17 | $2.04 | — | $9,448.98 | — | packet🟢 and camera net ≥ 2; gate news_box=good,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+7.9; combo leftover $3119.43; owner union_news_pack_net2_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 78 | $39.99 | $2.22 | — | $6,327.54 | — | packet🟢 and camera net ≥ 2; gate news_box=good,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+9.3; combo leftover $3119.43; owner union_news_pack_net2_h1 | — |
| 2026-09-16 09:30 ET | **SHORT** | `BBNX` | 167 | $18.61 | $2.63 | — | $9,432.78 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; ret5=-0.5; combo leftover $3118.37; owner short_news_r_h3 | — |
| 2026-09-16 09:30 ET | **SHORT** | `GFR` | 456 | $6.83 | $6.07 | — | $12,541.19 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+11.2; combo leftover $3118.37; owner short_news_r_h3 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,541.19 | ▼ close $11,811.61 vs 09:30 $12,498.84 (session -653.17) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,541.19 | ▼ 09:30 equity $11,811.55 vs yday $11,811.61 (-0.06) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 16 | $190.35 | $2.07 | $+14.77 | $15,584.72 | ▲ +14.77 after sell → book $11,809.48; vs 09:30 mark -2.07 | union_news_pack_net2_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 78 | $37.57 | $2.26 | $-193.24 | $18,512.92 | ▼ -193.24 after sell → book $11,807.22; vs 09:30 mark -2.26 | union_news_pack_net2_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `LITE` | 9 | $934.88 | $2.02 | — | $10,096.98 | — | packet🟢 and camera net ≥ 2; gate news_box=good,cam_net_min=2; rank cond; list yday_gainer; ret5=-7.0; combo leftover $9256.46; owner union_news_pack_net2_h1 | — |
| 2026-09-17 09:30 ET | **SHORT** | `BULL` | 371 | $7.95 | $4.95 | — | $13,041.48 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-18.4; combo leftover $2951.30; owner short_news_r_h3 | — |
| 2026-09-17 09:30 ET | **SHORT** | `LEN` | 36 | $81.00 | $2.21 | — | $15,955.27 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-3.0; combo leftover $2951.30; owner short_news_r_h3 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,955.27 | ▼ close $11,652.38 vs 09:30 $11,811.55 (session -145.66) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,955.27 | ▲ 09:30 equity $11,881.92 vs yday $11,652.38 (+229.54) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `LITE` | 9 | $915.66 | $2.09 | $-177.09 | $24,194.11 | ▼ -177.09 after sell → book $11,879.82; vs 09:30 mark -2.10 | union_news_pack_net2_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SHORT** | `FIVN` | 172 | $34.44 | $2.74 | — | $30,115.05 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $5939.91; owner short_news_r_h3 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30,115.05 | ▲ close $12,025.39 vs 09:30 $11,881.92 (session +148.31) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30,115.05 | ▼ 09:30 equity $11,809.13 vs yday $12,025.39 (-216.26) | — | — |
| 2026-09-21 09:30 ET | **COVER** | `BBNX` | 167 | $22.11 | $2.49 | $-589.62 | $26,420.19 | ▼ -589.62 after sell → book $11,806.64; vs 09:30 mark -2.49 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `GFR` | 456 | $6.55 | $5.88 | $+115.73 | $23,427.50 | ▲ +115.73 after sell → book $11,800.75; vs 09:30 mark -5.89 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 357 | $8.26 | $4.77 | — | $26,371.56 | — | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $2950.19; owner short_news_r_h3 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AMD` | 5 | $583.88 | $2.12 | — | $29,288.84 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+8.5; combo leftover $2950.19; owner short_news_r_h3 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $29,288.84 | ▼ close $11,503.45 vs 09:30 $11,809.13 (session -290.42) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29,288.84 | ▲ 09:30 equity $11,538.92 vs yday $11,503.45 (+35.47) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `BULL` | 371 | $8.28 | $4.79 | $-130.31 | $26,214.03 | ▼ -130.31 after sell → book $11,534.14; vs 09:30 mark -4.78 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SHORT** | `USFD` | 30 | $93.97 | $2.19 | — | $29,030.94 | — | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $2883.53; owner short_news_r_h3 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $29,030.94 | ▼ close $11,447.15 vs 09:30 $11,538.92 (session -84.80) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29,030.94 | ▼ 09:30 equity $10,740.34 vs yday $11,447.15 (-706.81) | — | — |
| 2026-09-23 09:30 ET | **COVER** | `LEN` | 36 | $82.00 | $2.10 | $-40.31 | $26,076.84 | ▼ -40.31 after sell → book $10,738.24; vs 09:30 mark -2.10 | short_news_r_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 66 | $196.78 | $2.19 | — | $13,087.17 | — | packet🟢 and camera net ≥ 2; gate news_box=good,cam_net_min=2; rank cond; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $13038.42; owner union_news_pack_net2_h1 | — |
| 2026-09-23 09:30 ET | **SHORT** | `HALO` | 45 | $116.85 | $2.32 | — | $18,343.10 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $5368.03; owner short_news_r_h3 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,343.10 | ▼ close $10,643.30 vs 09:30 $10,740.34 (session -90.43) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,343.10 | ▲ 09:30 equity $10,772.02 vs yday $10,643.30 (+128.72) | — | — |
| 2026-09-24 09:30 ET | **COVER** | `AMD` | 5 | $600.27 | $2.00 | $-86.07 | $15,339.74 | ▼ -86.07 after sell → book $10,770.01; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 66 | $192.26 | $2.30 | $-302.81 | $28,026.60 | ▼ -302.81 after sell → book $10,767.71; vs 09:30 mark -2.30 | union_news_pack_net2_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28,026.60 | ▼ close $10,522.86 vs 09:30 $10,772.02 (session -244.85) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,841.59 | ▼ 09:30 equity $8,300.33 vs yday $8,327.19 (-26.86) | 09:30 open · cash $18,841.59 (unchanged overnight, no fees) · equity $8,300.33 vs prior close $8,327.19 (-26.86) | — |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 10 | $887.00 | $2.02 | — | $9,969.57 | — | packet🟢 and camera net ≥ 2; gate news_box=good,cam_net_min=2; rank cond; list earn_react; 🔵; ret5=+0.3; combo leftover $9420.80; owner union_news_pack_net2_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `RSKD` | 528 | $7.85 | $7.05 | — | $14,107.32 | — | news🔴; gate news=bad; list yday_gainer; 🔵; ⚪; ret5=+25.4; combo leftover $4149.16; owner short_news_r_h3 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,107.32 | ▲ close $8,626.37 vs 09:30 $8,300.33 (session +335.11) | 16:00 close · cash $14,107.32 · equity $8,626.37 vs 09:30 $8,300.33 (+326.04; session marks +335.11) · 7 name(s) marked open→close (per-name table). AEHL×272 09:30 $9.05 → close $9.36 -84.32; BAND×36 09:30 $61.83 → close $61.83 -0.00; HALO×17 09:30 $115.36 → close $113.90 +24.82; PAYX×18 09:30 $101.59 → close $101.59 +0.00; USFD×22 09:30 $93.82 → close $93.82 +0.00; COST×10 09:30 $887.00 → close $922.76 +357.65; RSKD×528 09:30 $7.85 → close $7.78 +36.96 | — |

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
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new long union_news_pack_net2_h1 |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new long union_news_pack_net2_h1 |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new long union_news_pack_net2_h1 |
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
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new long union_news_pack_net2_h1 |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new long union_news_pack_net2_h1 |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `EU` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `BMO` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
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
| 2026-08-27 | `ASML` | cash | leftover split 1252.97 < 1 share @ 1746.53 |
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
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new long union_news_pack_net2_h1 |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new long union_news_pack_net2_h1 |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new long union_news_pack_net2_h1 |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new long union_news_pack_net2_h1 |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new long union_news_pack_net2_h1 |
| 2026-09-01 | `SIMO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `FIG` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new long union_news_pack_net2_h1 |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new long union_news_pack_net2_h1 |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new long union_news_pack_net2_h1 |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new short short_news_r_h3 |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new short short_news_r_h3 |
| 2026-09-04 | `SLN` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `SLN` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `OPK` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `PIPR` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new long union_news_pack_net2_h1 |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new long union_news_pack_net2_h1 |
| 2026-09-08 | `MSTR` | hard_red | hard-red S=-11.47 sit; no new long union_news_pack_net2_h1 |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `GEMI` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-09 | `GSM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `PIPR` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new long union_news_pack_net2_h1 |
| 2026-09-09 | `NVS` | hard_red | hard-red S=-13.95 sit; no new short short_news_r_h3 |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new long union_news_pack_net2_h1 |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new long union_news_pack_net2_h1 |
| 2026-09-10 | `ORCL` | hard_red | hard-red S=-13.28 sit; no new long union_news_pack_net2_h1 |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new short short_news_r_h3 |
| 2026-09-10 | `BKV` | hard_red | hard-red S=-13.28 sit; no new short short_news_r_h3 |
| 2026-09-14 | `QRVO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `RWT` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `CRDL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new long union_news_pack_net2_h1 |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new long union_news_pack_net2_h1 |
| 2026-09-15 | `QRVO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `RWT` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `CRDL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `MYGN` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `TYRA` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-17 | `GFR` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `BBNX` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `GFR` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `BULL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `LEN` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-21 | `BULL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `LEN` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `FIVN` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-22 | `LEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AMD` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-22 | `PAYX` | no_price | no 09:30 open |
| 2026-09-23 | `AMD` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-23 | `USFD` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `USFD` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-24 | `HALO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new long union_news_pack_net2_h1 |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new long union_news_pack_net2_h1 |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new long union_news_pack_net2_h1 |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new long union_news_pack_net2_h1 |
| 2026-09-24 | `PANW` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `VOYG` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `BMEA` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `FIVN` | 172 | 2026-09-18 @ $34.44 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $5939.91; owner short_news_r_h3 |
| `AEHL` | 357 | 2026-09-21 @ $8.26 | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $2950.19; owner short_news_r_h3 |
| `USFD` | 30 | 2026-09-22 @ $93.97 | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $2883.53; owner short_news_r_h3 |
| `HALO` | 45 | 2026-09-23 @ $116.85 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $5368.03; owner short_news_r_h3 |
