# Factor mine action — `combo_se1_5050_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared short_news_r_h3/union_e_fresh_h1 w=0.5,0.5 net=priority

Cash book **-10.25%** ($8,975) · signal-only (no cash/fees) was —. Starts YES **1/30**. Fills 266 · skips 162 · realized $+1198.48.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: short_news_r_h3 50%, union_e_fresh_h1 50%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: short_news_r_h3 50%, union_e_fresh_h1 50%.
- Member: short_news_r_h3 (50% · short · hold 3).
- Member: union_e_fresh_h1 (50% · long · hold 1).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $14,423.92.

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
| 2026-08-13 09:30 ET | **BUY** | `INO` | 6172 | $0.81 | $68.51 | — | $4,932.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+13.2; combo leftover $5000.00; owner union_e_fresh_h1 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 223 | $22.01 | $2.88 | — | $21.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+0.3; combo leftover $5000.00; owner union_e_fresh_h1 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.06 | ▲ close $10,769.53 vs 09:30 $10,000.00 (session +840.92) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.06 | ▲ 09:30 equity $10,963.61 vs yday $10,769.53 (+194.08) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 6172 | $0.93 | $76.99 | $+595.14 | $5,684.04 | ▲ +595.14 after sell → book $10,886.63; vs 09:30 mark -76.98 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 223 | $23.33 | $2.96 | $+288.53 | $10,883.67 | ▲ +288.53 after sell → book $10,883.67; vs 09:30 mark -2.96 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `EU` | 576 | $1.18 | $7.43 | — | $10,196.56 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $680.23; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 35 | $19.17 | $2.10 | — | $9,523.52 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot; 🔵; ⚪; ret5=+17.6; combo leftover $680.23; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 453 | $1.50 | $5.84 | — | $8,838.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; 🔵; ⚪; ret5=+9.2; combo leftover $680.23; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 34 | $19.57 | $2.09 | — | $8,170.70 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; combo leftover $680.23; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 61 | $11.12 | $2.17 | — | $7,490.21 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; combo leftover $680.23; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 50 | $13.55 | $2.14 | — | $6,810.57 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ⚪; ret5=+17.5; combo leftover $680.23; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 62 | $10.83 | $2.18 | — | $6,136.93 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-30.1; combo leftover $680.23; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `NMAX` | 68 | $9.89 | $2.19 | — | $5,461.88 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ⚪; ret5=+10.9; combo leftover $680.23; owner union_e_fresh_h1 | — |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 427 | $12.70 | $5.77 | — | $10,876.87 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.6; combo leftover $5428.76; owner short_news_r_h3 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,876.87 | ▲ close $11,066.78 vs 09:30 $10,963.61 (session +215.02) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,876.87 | ▲ 09:30 equity $11,142.79 vs yday $11,066.78 (+76.01) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `EU` | 576 | $1.21 | $7.54 | $+2.31 | $11,566.30 | ▲ +2.31 after sell → book $11,135.26; vs 09:30 mark -7.53 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LUNR` | 35 | $20.25 | $2.12 | $+33.59 | $12,272.93 | ▲ +33.59 after sell → book $11,133.14; vs 09:30 mark -2.12 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 453 | $1.52 | $5.93 | $-2.71 | $12,955.56 | ▼ -2.71 after sell → book $11,127.21; vs 09:30 mark -5.93 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 34 | $19.57 | $2.11 | $-4.20 | $13,618.83 | ▼ -4.20 after sell → book $11,125.10; vs 09:30 mark -2.11 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 61 | $9.57 | $2.19 | $-98.92 | $14,200.41 | ▼ -98.92 after sell → book $11,122.91; vs 09:30 mark -2.19 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 50 | $13.16 | $2.16 | $-23.80 | $14,856.25 | ▼ -23.80 after sell → book $11,120.75; vs 09:30 mark -2.16 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `CLBT` | 62 | $11.19 | $2.20 | $+17.95 | $15,547.83 | ▲ +17.95 after sell → book $11,118.55; vs 09:30 mark -2.20 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NMAX` | 68 | $10.97 | $2.22 | $+68.69 | $16,291.58 | ▲ +68.69 after sell → book $11,116.34; vs 09:30 mark -2.21 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 966 | $1.15 | $12.66 | — | $17,389.81 | — | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; combo leftover $1111.63; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 312 | $3.56 | $4.12 | — | $18,496.42 | — | news🔴; gate news=bad; list yday_mover; ret5=-15.6; combo leftover $1111.63; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `APMD` | 35 | $31.70 | $2.15 | — | $19,603.77 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+17.6; combo leftover $1111.63; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 369 | $3.01 | $4.86 | — | $20,709.60 | — | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; combo leftover $1111.63; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `RNW` | 163 | $6.80 | $2.55 | — | $21,815.45 | — | news🔴; gate news=bad; list overnight; ⚪; ret5=+10.4; combo leftover $1111.63; owner short_news_r_h3 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,815.45 | ▲ close $11,248.82 vs 09:30 $11,142.79 (session +158.82) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,815.45 | ▲ 09:30 equity $11,348.29 vs yday $11,248.82 (+99.47) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,815.45 | ▲ close $11,506.36 vs 09:30 $11,348.29 (session +158.07) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,815.45 | ▼ 09:30 equity $11,393.79 vs yday $11,506.36 (-112.57) | — | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 427 | $11.75 | $5.51 | $+392.24 | $16,792.69 | ▲ +392.24 after sell → book $11,388.28; vs 09:30 mark -5.51 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,792.69 | ▲ close $11,397.67 vs 09:30 $11,393.79 (session +9.38) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,792.69 | ▼ 09:30 equity $11,295.73 vs yday $11,397.67 (-101.94) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 966 | $0.96 | $12.20 | $+155.78 | $15,850.24 | ▲ +155.78 after sell → book $11,283.53; vs 09:30 mark -12.20 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ZNTL` | 312 | $4.01 | $4.02 | $-150.10 | $14,593.53 | ▼ -150.10 after sell → book $11,279.50; vs 09:30 mark -4.03 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `APMD` | 35 | $31.87 | $2.10 | $-10.19 | $13,475.99 | ▼ -10.19 after sell → book $11,277.41; vs 09:30 mark -2.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 369 | $2.95 | $4.76 | $+12.52 | $12,382.68 | ▲ +12.52 after sell → book $11,272.65; vs 09:30 mark -4.76 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `RNW` | 163 | $6.81 | $2.48 | $-6.66 | $11,270.17 | ▼ -6.66 after sell → book $11,270.17; vs 09:30 mark -2.48 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 159 | $4.43 | $2.47 | — | $10,563.33 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-23.1; combo leftover $704.39; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 15 | $46.85 | $2.04 | — | $9,858.55 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.0; combo leftover $704.39; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 7 | $97.43 | $2.01 | — | $9,174.52 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; combo leftover $704.39; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 2347 | $0.30 | $14.08 | — | $8,456.34 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-3.2; combo leftover $704.39; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 78 | $9.01 | $2.22 | — | $7,751.34 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-1.3; combo leftover $704.39; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 181 | $3.89 | $2.53 | — | $7,044.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.5; combo leftover $704.39; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 20 | $34.05 | $2.05 | — | $6,361.67 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+9.3; combo leftover $704.39; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 31 | $22.44 | $2.08 | — | $5,663.94 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.1; combo leftover $704.39; owner union_e_fresh_h1 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 4 | $204.45 | $2.04 | — | $6,479.70 | — | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; combo leftover $936.72; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 43 | $21.40 | $2.16 | — | $7,397.73 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-25.2; combo leftover $936.72; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 79 | $11.81 | $2.28 | — | $8,328.84 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $936.72; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 5 | $173.90 | $2.05 | — | $9,196.29 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; combo leftover $936.72; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 8 | $106.38 | $2.06 | — | $10,045.28 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; combo leftover $936.72; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 203 | $4.61 | $2.69 | — | $10,978.42 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $936.72; owner short_news_r_h3 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,978.42 | ▲ close $11,312.79 vs 09:30 $11,295.73 (session +85.38) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,978.42 | ▼ 09:30 equity $11,297.26 vs yday $11,312.79 (-15.53) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `TOYO` | 159 | $4.68 | $2.50 | $+34.78 | $11,720.04 | ▲ +34.78 after sell → book $11,294.76; vs 09:30 mark -2.50 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `EL` | 7 | $96.75 | $2.03 | $-8.80 | $12,395.25 | ▼ -8.80 after sell → book $11,292.72; vs 09:30 mark -2.04 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DVLT` | 2347 | $0.31 | $14.72 | $-5.33 | $13,108.11 | ▼ -5.33 after sell → book $11,278.01; vs 09:30 mark -14.71 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AEG` | 78 | $9.04 | $2.25 | $-2.13 | $13,810.98 | ▼ -2.13 after sell → book $11,275.76; vs 09:30 mark -2.25 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ALVO` | 181 | $4.32 | $2.57 | $+72.72 | $14,590.33 | ▲ +72.72 after sell → book $11,273.19; vs 09:30 mark -2.57 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATAT` | 20 | $34.31 | $2.07 | $+1.08 | $15,274.46 | ▲ +1.08 after sell → book $11,271.12; vs 09:30 mark -2.07 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATHM` | 31 | $22.20 | $2.10 | $-11.63 | $15,960.56 | ▼ -11.63 after sell → book $11,269.02; vs 09:30 mark -2.10 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 11 | $115.18 | $2.02 | — | $14,691.55 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; combo leftover $1330.05; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $13,443.04 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list probable,yday_gainer; 🔵; ret5=+1.4; combo leftover $1330.05; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 74 | $17.93 | $2.21 | — | $12,113.63 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=+0.2; combo leftover $1330.05; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 14 | $93.98 | $2.03 | — | $10,795.88 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.4; combo leftover $1330.05; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 30 | $43.08 | $2.08 | — | $9,501.40 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $1330.05; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 578 | $2.30 | $7.46 | — | $8,164.55 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.0; combo leftover $1330.05; owner union_e_fresh_h1 | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 301 | $3.11 | $3.97 | — | $9,096.69 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; combo leftover $937.60; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `MRNA` | 7 | $133.11 | $2.06 | — | $10,026.40 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=+109.5; combo leftover $937.60; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 10 | $89.10 | $2.06 | — | $10,915.34 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $937.60; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 24 | $38.40 | $2.11 | — | $11,834.83 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $937.60; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 44 | $20.90 | $2.17 | — | $12,752.26 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $937.60; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 34 | $27.00 | $2.14 | — | $13,668.12 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; combo leftover $937.60; owner short_news_r_h3 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,668.12 | ▲ close $11,459.73 vs 09:30 $11,297.26 (session +223.02) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,668.12 | ▲ 09:30 equity $11,551.84 vs yday $11,459.73 (+92.11) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AAP` | 15 | $43.05 | $2.06 | $-61.09 | $14,311.82 | ▼ -61.09 after sell → book $11,549.79; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 11 | $121.00 | $2.04 | $+59.95 | $15,640.78 | ▲ +59.95 after sell → book $11,547.75; vs 09:30 mark -2.04 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.04 | $2.02 | $+55.55 | $16,944.84 | ▲ +55.55 after sell → book $11,545.73; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BEKE` | 74 | $18.05 | $2.23 | $+4.43 | $18,278.67 | ▲ +4.43 after sell → book $11,543.49; vs 09:30 mark -2.24 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BJ` | 14 | $97.02 | $2.05 | $+38.48 | $19,634.90 | ▲ +38.48 after sell → book $11,541.44; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BKE` | 30 | $44.22 | $2.10 | $+30.02 | $20,959.40 | ▲ +30.02 after sell → book $11,539.34; vs 09:30 mark -2.10 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `PSEC` | 578 | $2.34 | $7.56 | $+8.10 | $22,304.36 | ▲ +8.10 after sell → book $11,531.78; vs 09:30 mark -7.56 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22,304.36 | ▲ close $11,574.53 vs 09:30 $11,551.84 (session +42.75) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22,304.36 | ▲ 09:30 equity $11,625.57 vs yday $11,574.53 (+51.04) | — | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 4 | $212.00 | $2.00 | $-34.25 | $21,454.36 | ▼ -34.25 after sell → book $11,623.57; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 43 | $20.90 | $2.12 | $+17.22 | $20,553.54 | ▲ +17.22 after sell → book $11,621.45; vs 09:30 mark -2.12 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ABCL` | 79 | $11.00 | $2.23 | $+59.88 | $19,682.31 | ▲ +59.88 after sell → book $11,619.22; vs 09:30 mark -2.23 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 5 | $170.64 | $2.00 | $+12.25 | $18,827.11 | ▲ +12.25 after sell → book $11,617.22; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 8 | $105.58 | $2.01 | $+2.33 | $17,980.45 | ▲ +2.33 after sell → book $11,615.20; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 203 | $4.77 | $2.62 | $-37.79 | $17,009.52 | ▼ -37.79 after sell → book $11,612.58; vs 09:30 mark -2.62 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 6 | $175.01 | $2.01 | — | $15,957.45 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.0; combo leftover $1063.10; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 11 | $88.94 | $2.02 | — | $14,977.09 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.9; combo leftover $1063.10; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 69 | $15.28 | $2.20 | — | $13,920.57 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-0.7; combo leftover $1063.10; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 7 | $142.36 | $2.01 | — | $12,922.04 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.6; combo leftover $1063.10; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 208 | $5.10 | $2.68 | — | $11,858.56 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.9; combo leftover $1063.10; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 22 | $47.89 | $2.06 | — | $10,802.92 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ⚪; ret5=+14.0; combo leftover $1063.10; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 76 | $13.92 | $2.22 | — | $9,742.79 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.9; combo leftover $1063.10; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 233 | $4.54 | $3.01 | — | $8,680.80 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-14.6; combo leftover $1063.10; owner union_e_fresh_h1 | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 141 | $13.62 | $2.50 | — | $10,599.42 | — | news🔴; gate news=bad; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; combo leftover $1932.40; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 35 | $54.51 | $2.17 | — | $12,505.09 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+15.1; combo leftover $1932.40; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `INTU` | 5 | $364.35 | $2.08 | — | $14,324.76 | — | news🔴; gate news=bad; list overnight,overnight_mega; 🔵; ret5=+10.2; combo leftover $1932.40; owner short_news_r_h3 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,324.76 | ▼ close $11,144.95 vs 09:30 $11,625.57 (session -442.66) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,324.76 | ▲ 09:30 equity $11,390.29 vs yday $11,144.95 (+245.34) | — | — |
| 2026-08-26 09:30 ET | **COVER** | `QTRX` | 301 | $2.83 | $3.88 | $+76.43 | $13,469.05 | ▲ +76.43 after sell → book $11,386.41; vs 09:30 mark -3.88 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `MRNA` | 7 | $154.20 | $2.01 | $-151.70 | $12,387.64 | ▼ -151.70 after sell → book $11,384.40; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 10 | $88.24 | $2.02 | $+4.52 | $11,503.22 | ▲ +4.52 after sell → book $11,382.38; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 24 | $38.41 | $2.06 | $-4.41 | $10,579.32 | ▼ -4.41 after sell → book $11,380.32; vs 09:30 mark -2.06 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 44 | $20.50 | $2.12 | $+13.31 | $9,675.20 | ▲ +13.31 after sell → book $11,378.20; vs 09:30 mark -2.12 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 34 | $26.00 | $2.09 | $+29.77 | $8,789.10 | ▲ +29.77 after sell → book $11,376.10; vs 09:30 mark -2.10 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMO` | 6 | $173.22 | $2.03 | $-14.78 | $9,826.40 | ▼ -14.78 after sell → book $11,374.08; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BNS` | 11 | $92.65 | $2.04 | $+36.74 | $10,843.50 | ▲ +36.74 after sell → book $11,372.03; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EH` | 208 | $4.77 | $2.73 | $-74.05 | $11,832.94 | ▼ -74.05 after sell → book $11,369.31; vs 09:30 mark -2.72 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GFI` | 22 | $48.24 | $2.08 | $+3.57 | $12,892.14 | ▲ +3.57 after sell → book $11,367.23; vs 09:30 mark -2.08 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GRRR` | 76 | $14.03 | $2.24 | $+3.90 | $13,956.18 | ▲ +3.90 after sell → book $11,364.99; vs 09:30 mark -2.24 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SHMD` | 233 | $3.38 | $3.05 | $-277.51 | $14,740.66 | ▼ -277.51 after sell → book $11,361.93; vs 09:30 mark -3.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 2107 | $0.58 | $18.60 | — | $13,493.68 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-27.5; combo leftover $1228.39; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 235 | $5.21 | $3.03 | — | $12,266.30 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ret5=+14.3; combo leftover $1228.39; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `ANF` | 9 | $131.37 | $2.02 | — | $11,081.95 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.3; combo leftover $1228.39; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 67 | $18.26 | $2.19 | — | $9,856.34 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.4; combo leftover $1228.39; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `BOX` | 35 | $34.30 | $2.10 | — | $8,653.74 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.7; combo leftover $1228.39; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `DY` | 3 | $326.91 | $2.00 | — | $7,671.02 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-15.2; combo leftover $1228.39; owner union_e_fresh_h1 | — |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 5 | $213.94 | $2.05 | — | $8,738.66 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; combo leftover $1133.20; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 92 | $12.22 | $2.32 | — | $9,860.58 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.4; combo leftover $1133.20; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 223 | $5.08 | $2.95 | — | $10,990.46 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+17.6; combo leftover $1133.20; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 8 | $132.64 | $2.06 | — | $12,049.52 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+16.5; combo leftover $1133.20; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `CRM` | 5 | $199.94 | $2.05 | — | $13,047.17 | — | news🔴; gate news=bad; list overnight,overnight_mega; ret5=+2.1; combo leftover $1133.20; owner short_news_r_h3 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,047.17 | ▲ close $11,390.36 vs 09:30 $11,390.29 (session +69.81) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,047.17 | ▼ 09:30 equity $11,126.77 vs yday $11,390.36 (-263.59) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `BZ` | 69 | $18.50 | $2.22 | $+217.76 | $14,321.45 | ▲ +217.76 after sell → book $11,124.55; vs 09:30 mark -2.22 | union_e_fresh_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DKS` | 7 | $128.73 | $2.03 | $-99.45 | $15,220.53 | ▼ -99.45 after sell → book $11,122.52; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SLQT` | 2107 | $0.53 | $17.85 | $-148.12 | $16,319.39 | ▼ -148.12 after sell → book $11,104.67; vs 09:30 mark -17.85 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TIGR` | 235 | $5.49 | $3.08 | $+59.69 | $17,606.46 | ▲ +59.69 after sell → book $11,101.59; vs 09:30 mark -3.08 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ANF` | 9 | $144.70 | $2.04 | $+115.92 | $18,906.72 | ▲ +115.92 after sell → book $11,099.55; vs 09:30 mark -2.04 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BBWI` | 67 | $18.69 | $2.21 | $+24.41 | $20,156.74 | ▲ +24.41 after sell → book $11,097.34; vs 09:30 mark -2.21 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BOX` | 35 | $33.79 | $2.12 | $-22.06 | $21,337.28 | ▼ -22.06 after sell → book $11,095.23; vs 09:30 mark -2.11 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DY` | 3 | $314.90 | $2.02 | $-40.05 | $22,279.96 | ▼ -40.05 after sell → book $11,093.21; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `BBY` | 17 | $80.60 | $2.04 | — | $20,907.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.0; combo leftover $1392.50; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `BILI` | 86 | $16.18 | $2.25 | — | $19,513.99 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-6.7; combo leftover $1392.50; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 11 | $118.77 | $2.02 | — | $18,205.49 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.3; combo leftover $1392.50; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `CMBT` | 78 | $17.78 | $2.22 | — | $16,816.43 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.2; combo leftover $1392.50; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `CSIQ` | 103 | $13.41 | $2.30 | — | $15,432.90 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.1; combo leftover $1392.50; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `HQY` | 14 | $97.16 | $2.03 | — | $14,070.63 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.5; combo leftover $1392.50; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `RY` | 6 | $206.82 | $2.01 | — | $12,827.70 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.2; combo leftover $1392.50; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `TD` | 11 | $120.17 | $2.02 | — | $11,503.81 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; combo leftover $1392.50; owner union_e_fresh_h1 | — |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 37 | $74.54 | $2.21 | — | $14,259.58 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=-0.1; combo leftover $2769.08; owner short_news_r_h3 | — |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 50 | $55.25 | $2.25 | — | $17,019.83 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=+2.1; combo leftover $2769.08; owner short_news_r_h3 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,019.83 | ▲ close $11,107.42 vs 09:30 $11,126.77 (session +35.57) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,019.83 | ▲ 09:30 equity $11,130.68 vs yday $11,107.42 (+23.26) | — | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 141 | $13.90 | $2.41 | $-43.69 | $15,057.52 | ▼ -43.69 after sell → book $11,128.26; vs 09:30 mark -2.42 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 35 | $52.49 | $2.10 | $+66.43 | $13,218.27 | ▲ +66.43 after sell → book $11,126.17; vs 09:30 mark -2.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `INTU` | 5 | $347.82 | $2.00 | $+78.57 | $11,477.17 | ▲ +78.57 after sell → book $11,124.16; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BBY` | 17 | $83.85 | $2.06 | $+51.15 | $12,900.56 | ▲ +51.15 after sell → book $11,122.10; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BILI` | 86 | $16.94 | $2.27 | $+60.84 | $14,355.12 | ▲ +60.84 after sell → book $11,119.83; vs 09:30 mark -2.27 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 11 | $115.66 | $2.04 | $-38.28 | $15,625.34 | ▼ -38.28 after sell → book $11,117.78; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CMBT` | 78 | $18.58 | $2.25 | $+57.93 | $17,072.33 | ▲ +57.93 after sell → book $11,115.54; vs 09:30 mark -2.24 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CSIQ` | 103 | $13.65 | $2.33 | $+20.09 | $18,475.95 | ▲ +20.09 after sell → book $11,113.21; vs 09:30 mark -2.33 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `HQY` | 14 | $93.62 | $2.05 | $-53.64 | $19,784.58 | ▼ -53.64 after sell → book $11,111.16; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `RY` | 6 | $205.50 | $2.03 | $-11.96 | $21,015.55 | ▼ -11.96 after sell → book $11,109.13; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TD` | 11 | $122.07 | $2.04 | $+16.83 | $22,356.28 | ▲ +16.83 after sell → book $11,107.08; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 5 | $261.16 | $2.00 | — | $21,048.47 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+7.8; combo leftover $1397.27; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 93 | $15.01 | $2.27 | — | $19,650.28 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+3.7; combo leftover $1397.27; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 13 | $103.89 | $2.03 | — | $18,297.68 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.5; combo leftover $1397.27; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 360 | $3.88 | $4.64 | — | $16,896.23 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-8.6; combo leftover $1397.27; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 31 | $44.40 | $2.08 | — | $15,517.75 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.4; combo leftover $1397.27; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 56 | $24.69 | $2.16 | — | $14,132.95 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.8; combo leftover $1397.27; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 167 | $8.35 | $2.49 | — | $12,736.01 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.1; combo leftover $1397.27; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 37 | $37.65 | $2.10 | — | $11,341.04 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $1397.27; owner union_e_fresh_h1 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 10 | $252.24 | $2.12 | — | $13,861.33 | — | news🔴; gate news=bad; list probable,yday_gainer; ⚪; ret5=+2.2; combo leftover $2771.83; owner short_news_r_h3 | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 91 | $30.18 | $2.38 | — | $16,605.33 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+12.1; combo leftover $2771.83; owner short_news_r_h3 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,605.33 | ▲ close $11,091.58 vs 09:30 $11,130.68 (session +8.77) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,605.33 | ▲ 09:30 equity $11,205.50 vs yday $11,091.58 (+113.92) | — | — |
| 2026-08-31 09:30 ET | **COVER** | `BE` | 5 | $208.88 | $2.00 | $+21.24 | $15,558.93 | ▲ +21.24 after sell → book $11,203.49; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 92 | $11.10 | $2.27 | $+98.45 | $14,535.46 | ▲ +98.45 after sell → book $11,201.22; vs 09:30 mark -2.27 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 223 | $4.97 | $2.88 | $+17.58 | $13,423.16 | ▲ +17.58 after sell → book $11,198.35; vs 09:30 mark -2.87 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `NEM` | 8 | $127.45 | $2.01 | $+37.44 | $12,401.54 | ▲ +37.44 after sell → book $11,196.33; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `CRM` | 5 | $254.39 | $2.00 | $-276.31 | $11,127.59 | ▼ -276.31 after sell → book $11,194.33; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 5 | $257.71 | $2.03 | $-21.28 | $12,414.11 | ▼ -21.28 after sell → book $11,192.30; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBAR` | 93 | $14.88 | $2.30 | $-16.65 | $13,795.66 | ▼ -16.65 after sell → book $11,190.01; vs 09:30 mark -2.29 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ESTC` | 13 | $98.00 | $2.05 | $-80.65 | $15,067.61 | ▼ -80.65 after sell → book $11,187.96; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FINV` | 360 | $3.39 | $4.71 | $-185.76 | $16,283.29 | ▼ -185.76 after sell → book $11,183.24; vs 09:30 mark -4.72 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FRO` | 31 | $44.85 | $2.10 | $+9.76 | $17,671.54 | ▲ +9.76 after sell → book $11,181.14; vs 09:30 mark -2.10 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GAP` | 56 | $22.98 | $2.18 | $-100.10 | $18,956.24 | ▼ -100.10 after sell → book $11,178.96; vs 09:30 mark -2.18 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `HAFN` | 167 | $8.53 | $2.53 | $+25.04 | $20,378.22 | ▲ +25.04 after sell → book $11,176.43; vs 09:30 mark -2.53 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `IREN` | 37 | $35.81 | $2.12 | $-72.12 | $21,701.07 | ▼ -72.12 after sell → book $11,174.31; vs 09:30 mark -2.12 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21,701.07 | ▲ close $11,229.88 vs 09:30 $11,205.50 (session +55.57) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,701.07 | ▲ 09:30 equity $11,390.57 vs yday $11,229.88 (+160.69) | — | — |
| 2026-09-01 09:30 ET | **COVER** | `MT` | 37 | $73.22 | $2.10 | $+44.53 | $18,989.83 | ▲ +44.53 after sell → book $11,388.47; vs 09:30 mark -2.10 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `TX` | 50 | $54.76 | $2.14 | $+20.11 | $16,249.69 | ▲ +20.11 after sell → book $11,386.33; vs 09:30 mark -2.14 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,249.69 | ▲ close $11,400.99 vs 09:30 $11,390.57 (session +14.66) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,249.69 | ▲ 09:30 equity $11,455.61 vs yday $11,400.99 (+54.62) | — | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 10 | $235.71 | $2.02 | $+161.16 | $13,890.57 | ▲ +161.16 after sell → book $11,453.59; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 91 | $26.78 | $2.26 | $+304.76 | $11,451.33 | ▲ +304.76 after sell → book $11,451.33; vs 09:30 mark -2.26 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,451.33 | ▲ close $11,451.33 vs 09:30 $11,455.61 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,451.33 | ▲ 09:30 equity $11,451.33 vs yday $11,451.33 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 66 | $10.74 | $2.19 | — | $10,739.97 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+8.5; combo leftover $715.71; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 2 | $351.74 | $2.00 | — | $10,034.49 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.3; combo leftover $715.71; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 103 | $6.90 | $2.30 | — | $9,321.49 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.8; combo leftover $715.71; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 2 | $354.49 | $2.00 | — | $8,610.52 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.3; combo leftover $715.71; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 32 | $22.32 | $2.09 | — | $7,894.19 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+2.4; combo leftover $715.71; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 2 | $257.00 | $2.00 | — | $7,378.20 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-5.5; combo leftover $715.71; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 15 | $47.60 | $2.04 | — | $6,662.16 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.2; combo leftover $715.71; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 47 | $15.09 | $2.13 | — | $5,950.80 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+6.1; combo leftover $715.71; owner union_e_fresh_h1 | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 192 | $14.85 | $2.70 | — | $8,799.30 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; combo leftover $2858.65; owner short_news_r_h3 | — |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 1671 | $1.71 | $21.93 | — | $11,634.78 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+11.9; combo leftover $2858.65; owner short_news_r_h3 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,634.78 | ▲ close $11,833.39 vs 09:30 $11,451.33 (session +423.42) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,634.78 | ▲ 09:30 equity $11,921.26 vs yday $11,833.39 (+87.87) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `AI` | 66 | $10.91 | $2.21 | $+6.49 | $12,352.63 | ▲ +6.49 after sell → book $11,919.05; vs 09:30 mark -2.21 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 2 | $359.70 | $2.02 | $+11.91 | $13,070.01 | ▲ +11.91 after sell → book $11,917.03; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CHPT` | 103 | $9.28 | $2.33 | $+240.51 | $14,023.53 | ▲ +240.51 after sell → book $11,914.71; vs 09:30 mark -2.32 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 2 | $321.67 | $2.02 | $-69.65 | $14,664.85 | ▼ -69.65 after sell → book $11,912.69; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CPB` | 32 | $22.10 | $2.11 | $-11.23 | $15,369.94 | ▼ -11.23 after sell → book $11,910.58; vs 09:30 mark -2.11 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FIVE` | 2 | $238.88 | $2.02 | $-40.25 | $15,845.69 | ▼ -40.25 after sell → book $11,908.57; vs 09:30 mark -2.01 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 15 | $53.85 | $2.06 | $+89.66 | $16,651.38 | ▲ +89.66 after sell → book $11,906.51; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MEI` | 47 | $15.34 | $2.15 | $+7.47 | $17,370.21 | ▲ +7.47 after sell → book $11,904.36; vs 09:30 mark -2.15 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 17 | $63.18 | $2.04 | — | $16,294.11 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-10.9; combo leftover $1085.64; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 124 | $8.74 | $2.36 | — | $15,207.99 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.8; combo leftover $1085.64; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 15 | $68.52 | $2.04 | — | $14,178.15 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.4; combo leftover $1085.64; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 300 | $3.62 | $3.87 | — | $13,089.78 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.1; combo leftover $1085.64; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 6 | $167.55 | $2.01 | — | $12,082.48 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; combo leftover $1085.64; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 24 | $44.90 | $2.06 | — | $11,002.81 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.5; combo leftover $1085.64; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 11 | $98.15 | $2.02 | — | $9,921.14 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.9; combo leftover $1085.64; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 69 | $15.70 | $2.20 | — | $8,835.64 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.4; combo leftover $1085.64; owner union_e_fresh_h1 | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 636 | $4.67 | $8.42 | — | $11,797.35 | — | news🔴; gate news=bad; list yday_gainer; ret5=+11.9; combo leftover $2971.44; owner short_news_r_h3 | — |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 38 | $76.55 | $2.22 | — | $14,704.03 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; combo leftover $2971.44; owner short_news_r_h3 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,704.03 | ▼ close $11,708.46 vs 09:30 $11,921.26 (session -166.67) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,704.03 | ▼ 09:30 equity $11,706.02 vs yday $11,708.46 (-2.44) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `AMBA` | 17 | $63.83 | $2.06 | $+6.95 | $15,787.08 | ▲ +6.95 after sell → book $11,703.96; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASAN` | 124 | $8.73 | $2.39 | $-5.99 | $16,867.21 | ▼ -5.99 after sell → book $11,701.57; vs 09:30 mark -2.39 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DOCU` | 15 | $67.05 | $2.06 | $-26.14 | $17,870.90 | ▼ -26.14 after sell → book $11,699.51; vs 09:30 mark -2.06 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DOMO` | 300 | $3.84 | $3.93 | $+59.70 | $19,018.97 | ▲ +59.70 after sell → book $11,695.58; vs 09:30 mark -3.93 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `GWRE` | 6 | $160.52 | $2.03 | $-46.22 | $19,980.06 | ▼ -46.22 after sell → book $11,693.55; vs 09:30 mark -2.03 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `IOT` | 24 | $39.56 | $2.08 | $-132.30 | $20,927.42 | ▼ -132.30 after sell → book $11,691.47; vs 09:30 mark -2.08 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LULU` | 11 | $100.58 | $2.04 | $+22.66 | $22,031.76 | ▲ +22.66 after sell → book $11,689.43; vs 09:30 mark -2.04 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MAMA` | 69 | $15.20 | $2.22 | $-38.92 | $23,078.34 | ▼ -38.92 after sell → book $11,687.21; vs 09:30 mark -2.22 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23,078.34 | ▲ close $11,979.33 vs 09:30 $11,706.02 (session +292.12) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23,078.34 | ▲ 09:30 equity $12,017.12 vs yday $11,979.33 (+37.79) | — | — |
| 2026-09-09 09:30 ET | **COVER** | `SLN` | 192 | $13.60 | $2.57 | $+234.74 | $20,464.58 | ▲ +234.74 after sell → book $12,014.56; vs 09:30 mark -2.56 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 1671 | $1.58 | $21.56 | $+173.74 | $17,802.84 | ▲ +173.74 after sell → book $11,993.00; vs 09:30 mark -21.56 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17,802.84 | ▲ close $12,022.72 vs 09:30 $12,017.12 (session +29.72) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17,802.84 | ▲ 09:30 equity $12,111.86 vs yday $12,022.72 (+89.14) | — | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 636 | $4.36 | $8.20 | $+180.54 | $15,021.68 | ▲ +180.54 after sell → book $12,103.66; vs 09:30 mark -8.20 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PIPR` | 38 | $76.79 | $2.10 | $-13.44 | $12,101.55 | ▼ -13.44 after sell → book $12,101.55; vs 09:30 mark -2.11 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,101.55 | ▲ close $12,101.55 vs 09:30 $12,111.86 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,101.55 | ▲ 09:30 equity $12,101.55 vs yday $12,101.55 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 4 | $164.43 | $2.00 | — | $11,441.83 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,earn_react; ⚪; ret5=+4.9; combo leftover $756.35; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 127 | $5.91 | $2.37 | — | $10,688.89 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover,ohlc_hot; ret5=+14.1; combo leftover $756.35; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 3 | $242.17 | $2.00 | — | $9,960.38 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.1; combo leftover $756.35; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `CPRT` | 23 | $32.01 | $2.06 | — | $9,222.09 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.4; combo leftover $756.35; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `DSGX` | 10 | $71.71 | $2.02 | — | $8,502.97 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-9.1; combo leftover $756.35; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `KR` | 13 | $56.02 | $2.03 | — | $7,772.68 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.2; combo leftover $756.35; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `LPTH` | 80 | $9.37 | $2.23 | — | $7,020.85 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+1.5; combo leftover $756.35; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `REF` | 57 | $13.10 | $2.16 | — | $6,271.99 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.9; combo leftover $756.35; owner union_e_fresh_h1 | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 10 | $112.83 | $2.07 | — | $7,398.27 | — | news🔴; gate news=bad; list yday_gainer,ohlc_hot; ret5=+11.7; combo leftover $1208.47; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `RWT` | 343 | $3.52 | $4.52 | — | $8,601.10 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-19.2; combo leftover $1208.47; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `CRDL` | 595 | $2.03 | $7.82 | — | $9,801.14 | — | news🔴; gate news=bad; list yday_mover; ret5=-8.8; combo leftover $1208.47; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BKV` | 48 | $24.97 | $2.19 | — | $10,997.51 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.8; combo leftover $1208.47; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `MYGN` | 358 | $3.37 | $4.72 | — | $12,199.25 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+4.0; combo leftover $1208.47; owner short_news_r_h3 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,199.25 | ▲ close $12,090.57 vs 09:30 $12,101.55 (session +27.21) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,199.25 | ▲ 09:30 equity $12,138.71 vs yday $12,090.57 (+48.14) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 4 | $141.42 | $2.02 | $-96.06 | $12,762.91 | ▼ -96.06 after sell → book $12,136.69; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `DBI` | 127 | $5.86 | $2.40 | $-11.12 | $13,504.73 | ▼ -11.12 after sell → book $12,134.29; vs 09:30 mark -2.40 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 3 | $261.51 | $2.02 | $+54.00 | $14,287.24 | ▲ +54.00 after sell → book $12,132.27; vs 09:30 mark -2.02 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CPRT` | 23 | $30.63 | $2.08 | $-35.88 | $14,989.65 | ▼ -35.88 after sell → book $12,130.19; vs 09:30 mark -2.08 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `DSGX` | 10 | $77.68 | $2.04 | $+55.64 | $15,764.41 | ▲ +55.64 after sell → book $12,128.15; vs 09:30 mark -2.04 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `KR` | 13 | $59.31 | $2.05 | $+38.69 | $16,533.39 | ▲ +38.69 after sell → book $12,126.10; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `LPTH` | 80 | $8.85 | $2.25 | $-46.08 | $17,239.14 | ▼ -46.08 after sell → book $12,123.85; vs 09:30 mark -2.25 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `REF` | 57 | $14.16 | $2.18 | $+56.08 | $18,044.07 | ▲ +56.08 after sell → book $12,121.66; vs 09:30 mark -2.19 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,044.07 | ▼ close $11,966.35 vs 09:30 $12,138.71 (session -155.31) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,044.07 | ▼ 09:30 equity $11,966.07 vs yday $11,966.35 (-0.28) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,044.07 | ▼ close $11,863.75 vs 09:30 $11,966.07 (session -102.32) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,044.07 | ▲ 09:30 equity $11,881.72 vs yday $11,863.75 (+17.97) | — | — |
| 2026-09-16 09:30 ET | **COVER** | `QRVO` | 10 | $118.18 | $2.02 | $-57.54 | $16,860.25 | ▼ -57.54 after sell → book $11,879.70; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `RWT` | 343 | $3.98 | $4.42 | $-166.73 | $15,490.69 | ▼ -166.73 after sell → book $11,875.28; vs 09:30 mark -4.42 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `CRDL` | 595 | $1.85 | $7.68 | $+91.61 | $14,382.26 | ▲ +91.61 after sell → book $11,867.60; vs 09:30 mark -7.68 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `BKV` | 48 | $24.42 | $2.13 | $+22.08 | $13,207.97 | ▲ +22.08 after sell → book $11,865.47; vs 09:30 mark -2.13 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `MYGN` | 358 | $3.75 | $4.62 | $-145.38 | $11,860.85 | ▼ -145.38 after sell → book $11,860.85; vs 09:30 mark -4.62 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 89 | $33.14 | $2.26 | — | $8,909.13 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ret5=-2.9; combo leftover $2965.21; owner union_e_fresh_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 72 | $40.93 | $2.21 | — | $5,959.97 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $2965.21; owner union_e_fresh_h1 | — |
| 2026-09-16 09:30 ET | **SHORT** | `BBNX` | 159 | $18.61 | $2.60 | — | $8,916.36 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; ret5=-0.5; combo leftover $2964.10; owner short_news_r_h3 | — |
| 2026-09-16 09:30 ET | **SHORT** | `GFR` | 433 | $6.83 | $5.76 | — | $11,867.99 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+11.2; combo leftover $2964.10; owner short_news_r_h3 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,867.99 | ▼ close $11,542.92 vs 09:30 $11,881.72 (session -305.11) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,867.99 | ▲ 09:30 equity $11,699.53 vs yday $11,542.92 (+156.61) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `FPS` | 89 | $36.76 | $2.30 | $+317.63 | $15,137.33 | ▲ +317.63 after sell → book $11,697.23; vs 09:30 mark -2.30 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `TCOM` | 72 | $40.79 | $2.24 | $-14.53 | $18,071.97 | ▼ -14.53 after sell → book $11,694.99; vs 09:30 mark -2.24 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `LEN` | 55 | $81.00 | $2.15 | — | $13,614.81 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.0; combo leftover $4517.99; owner union_e_fresh_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `ALMU` | 403 | $11.21 | $5.20 | — | $9,091.99 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.0; combo leftover $4517.99; owner union_e_fresh_h1 | — |
| 2026-09-17 09:30 ET | **SHORT** | `BULL` | 735 | $7.95 | $9.81 | — | $14,925.43 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-18.4; combo leftover $5843.82; owner short_news_r_h3 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,925.43 | ▲ close $12,003.56 vs 09:30 $11,699.53 (session +325.73) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,925.43 | ▼ 09:30 equity $11,888.53 vs yday $12,003.56 (-115.03) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `LEN` | 55 | $78.25 | $2.20 | $-155.60 | $19,226.98 | ▼ -155.60 after sell → book $11,886.33; vs 09:30 mark -2.20 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ALMU` | 403 | $11.64 | $5.30 | $+162.79 | $23,912.59 | ▲ +162.79 after sell → book $11,881.02; vs 09:30 mark -5.31 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SHORT** | `FIVN` | 172 | $34.44 | $2.74 | — | $29,833.53 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $5940.51; owner short_news_r_h3 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $29,833.53 | ▼ close $11,820.65 vs 09:30 $11,888.53 (session -57.63) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29,833.53 | ▼ 09:30 equity $11,506.94 vs yday $11,820.65 (-313.71) | — | — |
| 2026-09-21 09:30 ET | **COVER** | `BBNX` | 159 | $22.11 | $2.47 | $-561.57 | $26,315.57 | ▼ -561.57 after sell → book $11,504.47; vs 09:30 mark -2.47 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `GFR` | 433 | $6.55 | $5.59 | $+109.89 | $23,473.84 | ▲ +109.89 after sell → book $11,498.89; vs 09:30 mark -5.58 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 348 | $8.26 | $4.65 | — | $26,343.67 | — | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $2874.72; owner short_news_r_h3 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AMD` | 4 | $583.88 | $2.09 | — | $28,677.09 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+8.5; combo leftover $2874.72; owner short_news_r_h3 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28,677.09 | ▼ close $11,377.38 vs 09:30 $11,506.94 (session -114.76) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28,677.09 | ▲ 09:30 equity $11,394.81 vs yday $11,377.38 (+17.43) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `BULL` | 735 | $8.28 | $9.48 | $-258.17 | $22,585.49 | ▼ -258.17 after sell → book $11,385.33; vs 09:30 mark -9.48 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SHORT** | `USFD` | 30 | $93.97 | $2.19 | — | $25,402.40 | — | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $2846.33; owner short_news_r_h3 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25,402.40 | ▼ close $11,315.54 vs 09:30 $11,394.81 (session -67.60) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $25,402.40 | ▼ 09:30 equity $10,753.88 vs yday $11,315.54 (-561.66) | — | — |
| 2026-09-23 09:30 ET | **BUY** | `CBRL` | 53 | $47.57 | $2.15 | — | $22,879.04 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-11.2; combo leftover $2540.24; owner union_e_fresh_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 12 | $196.78 | $2.03 | — | $20,515.65 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $2540.24; owner union_e_fresh_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `GIS` | 71 | $35.74 | $2.20 | — | $17,975.91 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $2540.24; owner union_e_fresh_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `KBH` | 53 | $47.15 | $2.15 | — | $15,474.81 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.9; combo leftover $2540.24; owner union_e_fresh_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `PAYX` | 23 | $109.67 | $2.06 | — | $12,950.34 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $2540.24; owner union_e_fresh_h1 | — |
| 2026-09-23 09:30 ET | **SHORT** | `HALO` | 45 | $116.85 | $2.32 | — | $18,206.27 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $5371.64; owner short_news_r_h3 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18,206.27 | ▲ close $10,792.77 vs 09:30 $10,753.88 (session +51.80) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18,206.27 | ▲ 09:30 equity $10,889.84 vs yday $10,792.77 (+97.07) | — | — |
| 2026-09-24 09:30 ET | **COVER** | `AMD` | 4 | $600.27 | $2.00 | $-69.66 | $15,803.19 | ▼ -69.66 after sell → book $10,887.84; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `CBRL` | 53 | $46.88 | $2.18 | $-40.90 | $18,285.65 | ▼ -40.90 after sell → book $10,885.66; vs 09:30 mark -2.18 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 12 | $192.26 | $2.05 | $-58.32 | $20,590.71 | ▼ -58.32 after sell → book $10,883.61; vs 09:30 mark -2.05 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `GIS` | 71 | $35.96 | $2.24 | $+11.18 | $23,141.64 | ▲ +11.18 after sell → book $10,881.37; vs 09:30 mark -2.24 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `KBH` | 53 | $47.14 | $2.18 | $-4.86 | $25,637.88 | ▼ -4.86 after sell → book $10,879.20; vs 09:30 mark -2.17 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PAYX` | 23 | $105.49 | $2.09 | $-100.24 | $28,062.11 | ▼ -100.24 after sell → book $10,877.11; vs 09:30 mark -2.09 | union_e_fresh_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28,062.11 | ▼ close $10,639.01 vs 09:30 $10,889.84 (session -238.10) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,888.46 | ▼ 09:30 equity $8,616.29 vs yday $8,644.91 (-28.62) | 09:30 open · cash $19,888.46 (unchanged overnight, no fees) · equity $8,616.29 vs prior close $8,644.91 (-28.62) | — |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 11 | $887.00 | $2.02 | — | $10,129.44 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+0.3; combo leftover $9944.23; owner union_e_fresh_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `RSKD` | 548 | $7.85 | $7.31 | — | $14,423.92 | — | news🔴; gate news=bad; list yday_gainer; 🔵; ⚪; ret5=+25.4; combo leftover $4307.13; owner short_news_r_h3 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,423.92 | ▲ close $8,975.11 vs 09:30 $8,616.29 (session +368.15) | 16:00 close · cash $14,423.92 · equity $8,975.11 vs 09:30 $8,616.29 (+358.82; session marks +368.15) · 7 name(s) marked open→close (per-name table). AEHL×290 09:30 $9.05 → close $9.36 -89.90; BAND×37 09:30 $61.83 → close $61.83 -0.00; HALO×18 09:30 $115.36 → close $113.90 +26.28; PAYX×20 09:30 $101.59 → close $101.59 +0.00; USFD×24 09:30 $93.82 → close $93.82 +0.00; COST×11 09:30 $887.00 → close $922.76 +393.41; RSKD×548 09:30 $7.85 → close $7.78 +38.36 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `OWL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `OWL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `VERI` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `ZNTL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `APMD` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `HIVE` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new short short_news_r_h3 |
| 2026-08-18 | `CADL` | hard_red | hard-red S=-6.20 sit; no new short short_news_r_h3 |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `KLAR` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `VERI` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `ZNTL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `APMD` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `HIVE` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `RNW` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `LUNR` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `AAP` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `KC` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-19 | `KEYS` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h1 |
| 2026-08-21 | `AEM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `WYFI` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `TEAM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `WMT` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `AQST` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `AEM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `WYFI` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `TEAM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
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
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new long union_e_fresh_h1 |
| 2026-08-25 | `QTRX` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `MRNA` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `AUGO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `ARIS` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `NOG` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-26 | `AVAH` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `ARE` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `INTU` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `AVAH` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `ARE` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
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
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new long union_e_fresh_h1 |
| 2026-09-01 | `SIMO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `FIG` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new short short_news_r_h3 |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new short short_news_r_h3 |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h1 |
| 2026-09-04 | `SLN` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `SLN` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `OPK` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `PIPR` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `GEMI` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h1 |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h1 |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `GSM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `PIPR` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `NVS` | hard_red | hard-red S=-13.95 sit; no new short short_news_r_h3 |
| 2026-09-09 | `ABM` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `AVO` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `JMKE` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `SAIL` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-09 | `TTAN` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new short short_news_r_h3 |
| 2026-09-10 | `BKV` | hard_red | hard-red S=-13.28 sit; no new short short_news_r_h3 |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `JMKE` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `AEO` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `AVAV` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h1 |
| 2026-09-14 | `QRVO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `RWT` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `CRDL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-15 | `QRVO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `RWT` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `CRDL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `MYGN` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `TYRA` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `FPS` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h1 |
| 2026-09-15 | `HITI` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h1 |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h1 |
| 2026-09-15 | `UROY` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h1 |
| 2026-09-17 | `GFR` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `BBNX` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `GFR` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `BULL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-21 | `BULL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `FIVN` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-22 | `AMD` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-22 | `ABVX` | no_price | no 09:30 open |
| 2026-09-22 | `ANAB` | no_price | no 09:30 open |
| 2026-09-22 | `MLKN` | no_price | no 09:30 open |
| 2026-09-22 | `THO` | no_price | no 09:30 open |
| 2026-09-22 | `PAYX` | no_price | no 09:30 open |
| 2026-09-23 | `AMD` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-23 | `USFD` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `USFD` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-24 | `HALO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `PANW` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `VOYG` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `BMEA` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `BB` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h1 |
| 2026-09-24 | `DRI` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h1 |
| 2026-09-24 | `FUL` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h1 |
| 2026-09-24 | `NEOV` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h1 |
| 2026-09-24 | `SNX` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h1 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `FIVN` | 172 | 2026-09-18 @ $34.44 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $5940.51; owner short_news_r_h3 |
| `AEHL` | 348 | 2026-09-21 @ $8.26 | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $2874.72; owner short_news_r_h3 |
| `USFD` | 30 | 2026-09-22 @ $93.97 | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $2846.33; owner short_news_r_h3 |
| `HALO` | 45 | 2026-09-23 @ $116.85 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $5371.64; owner short_news_r_h3 |
