# Factor mine action — `combo_es_9010_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared union_e_fresh_h3/short_news_r_h3 w=0.9,0.1 net=priority

Cash book **-11.98%** ($8,802) · signal-only (no cash/fees) was —. Starts YES **25/30**. Fills 233 · skips 319 · realized $+4040.10.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: union_e_fresh_h3 90%, short_news_r_h3 10%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: union_e_fresh_h3 90%, short_news_r_h3 10%.
- Member: union_e_fresh_h3 (90% · long · hold 3).
- Member: short_news_r_h3 (10% · short · hold 3).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $-2,059.94.

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
| 2026-08-13 09:30 ET | **BUY** | `INO` | 6172 | $0.81 | $68.51 | — | $4,932.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+13.2; combo leftover $5000.00; owner union_e_fresh_h3 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 223 | $22.01 | $2.88 | — | $21.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+0.3; combo leftover $5000.00; owner union_e_fresh_h3 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.06 | ▲ close $10,769.53 vs 09:30 $10,000.00 (session +840.92) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.06 | ▲ 09:30 equity $10,963.61 vs yday $10,769.53 (+194.08) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 1 | $1.50 | $0.02 | — | $19.55 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; 🔵; ⚪; ret5=+9.2; combo leftover $2.37; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `EU` | 2 | $1.18 | $0.03 | — | $17.16 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $2.37; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 1 | $12.70 | $0.15 | — | $29.70 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.6; combo leftover $17.16; owner short_news_r_h3 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $29.70 | ▲ close $11,884.64 vs 09:30 $10,963.61 (session +921.22) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29.70 | ▼ 09:30 equity $11,734.49 vs yday $11,884.64 (-150.15) | — | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 5 | $1.15 | $0.09 | — | $35.36 | — | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; combo leftover $5.94; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 1 | $3.56 | $0.06 | — | $38.86 | — | news🔴; gate news=bad; list yday_mover; ret5=-15.6; combo leftover $5.94; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 1 | $3.01 | $0.05 | — | $41.82 | — | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; combo leftover $5.94; owner short_news_r_h3 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $41.82 | ▲ close $12,250.85 vs 09:30 $11,734.49 (session +516.55) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $41.82 | ▼ 09:30 equity $12,147.06 vs yday $12,250.85 (-103.79) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 6172 | $1.14 | $80.70 | $+1887.55 | $6,997.20 | ▲ +1,887.55 after sell → book $12,066.36; vs 09:30 mark -80.70 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 223 | $22.82 | $2.95 | $+174.80 | $12,083.10 | ▲ +174.80 after sell → book $12,063.40; vs 09:30 mark -2.96 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,083.10 | ▲ close $12,063.67 vs 09:30 $12,147.06 (session +0.27) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,083.10 | ▼ 09:30 equity $12,063.37 vs yday $12,063.67 (-0.30) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 1 | $1.42 | $0.04 | $-0.14 | $12,084.49 | ▼ -0.14 after sell → book $12,063.34; vs 09:30 mark -0.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `EU` | 2 | $1.07 | $0.05 | $-0.30 | $12,086.58 | ▼ -0.30 after sell → book $12,063.29; vs 09:30 mark -0.05 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 1 | $11.75 | $0.12 | $+0.67 | $12,074.71 | ▲ +0.67 after sell → book $12,063.17; vs 09:30 mark -0.12 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,074.71 | ▲ close $12,063.24 vs 09:30 $12,063.37 (session +0.07) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,074.71 | ▼ 09:30 equity $12,062.93 vs yday $12,063.24 (-0.31) | — | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 5 | $0.96 | $0.06 | $+0.78 | $12,069.83 | ▲ +0.78 after sell → book $12,062.86; vs 09:30 mark -0.07 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ZNTL` | 1 | $4.01 | $0.04 | $-0.56 | $12,065.77 | ▼ -0.56 after sell → book $12,062.82; vs 09:30 mark -0.04 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 1 | $2.95 | $0.03 | $-0.03 | $12,062.79 | ▼ -0.03 after sell → book $12,062.79; vs 09:30 mark -0.03 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 13 | $97.43 | $2.03 | — | $10,794.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; combo leftover $1357.06; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 306 | $4.43 | $3.95 | — | $9,434.64 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-23.1; combo leftover $1357.06; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 4523 | $0.30 | $27.14 | — | $8,050.60 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-3.2; combo leftover $1357.06; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 28 | $46.85 | $2.07 | — | $6,736.73 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.0; combo leftover $1357.06; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 150 | $9.01 | $2.44 | — | $5,382.79 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-1.3; combo leftover $1357.06; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 348 | $3.89 | $4.49 | — | $4,024.58 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.5; combo leftover $1357.06; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 39 | $34.05 | $2.11 | — | $2,694.52 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+9.3; combo leftover $1357.06; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 60 | $22.44 | $2.17 | — | $1,345.95 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.1; combo leftover $1357.06; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 1 | $204.45 | $2.02 | — | $1,548.39 | — | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; combo leftover $224.33; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 10 | $21.40 | $2.05 | — | $1,760.34 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-25.2; combo leftover $224.33; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 18 | $11.81 | $2.07 | — | $1,970.94 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $224.33; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 1 | $173.90 | $1.77 | — | $2,143.07 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; combo leftover $224.33; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 2 | $106.38 | $2.02 | — | $2,353.81 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; combo leftover $224.33; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 48 | $4.61 | $2.16 | — | $2,572.93 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $224.33; owner short_news_r_h3 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,572.93 | ▲ close $12,108.91 vs 09:30 $12,062.93 (session +104.60) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,572.93 | ▲ 09:30 equity $12,142.34 vs yday $12,108.91 (+33.43) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 3 | $115.18 | $2.00 | — | $2,225.39 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; combo leftover $385.94; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 21 | $17.93 | $2.05 | — | $1,846.70 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=+0.2; combo leftover $385.94; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 4 | $93.98 | $2.00 | — | $1,468.78 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.4; combo leftover $385.94; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 8 | $43.08 | $2.01 | — | $1,122.13 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $385.94; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 167 | $2.30 | $2.49 | — | $735.54 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.0; combo leftover $385.94; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 39 | $3.11 | $1.35 | — | $855.47 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; combo leftover $122.59; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 1 | $89.10 | $0.92 | — | $943.66 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $122.59; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 3 | $38.40 | $1.18 | — | $1,057.67 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $122.59; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 5 | $20.90 | $1.08 | — | $1,161.09 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $122.59; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 4 | $27.00 | $1.11 | — | $1,267.98 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; combo leftover $122.59; owner short_news_r_h3 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,267.98 | ▲ close $12,394.50 vs 09:30 $12,142.34 (session +268.37) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,267.98 | ▲ 09:30 equity $12,439.56 vs yday $12,394.50 (+45.06) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,267.98 | ▲ close $12,558.30 vs 09:30 $12,439.56 (session +118.74) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,267.98 | ▲ 09:30 equity $12,583.08 vs yday $12,558.30 (+24.78) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `EL` | 13 | $104.00 | $2.05 | $+81.33 | $2,617.93 | ▲ +81.33 after sell → book $12,581.03; vs 09:30 mark -2.05 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `TOYO` | 306 | $4.42 | $4.01 | $-11.02 | $3,966.44 | ▼ -11.02 after sell → book $12,577.02; vs 09:30 mark -4.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DVLT` | 4523 | $0.31 | $28.35 | $-10.26 | $5,340.21 | ▼ -10.26 after sell → book $12,548.66; vs 09:30 mark -28.36 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AAP` | 28 | $43.63 | $2.09 | $-94.33 | $6,559.76 | ▼ -94.33 after sell → book $12,546.57; vs 09:30 mark -2.09 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AEG` | 150 | $9.23 | $2.48 | $+28.08 | $7,941.78 | ▲ +28.08 after sell → book $12,544.09; vs 09:30 mark -2.48 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ALVO` | 348 | $5.24 | $4.56 | $+460.75 | $9,760.74 | ▲ +460.75 after sell → book $12,539.53; vs 09:30 mark -4.56 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATAT` | 39 | $34.72 | $2.13 | $+21.90 | $11,112.70 | ▲ +21.90 after sell → book $12,537.41; vs 09:30 mark -2.12 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATHM` | 60 | $21.85 | $2.19 | $-39.76 | $12,421.50 | ▼ -39.76 after sell → book $12,535.21; vs 09:30 mark -2.20 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 1 | $212.00 | $1.99 | $-11.56 | $12,207.51 | ▼ -11.56 after sell → book $12,533.22; vs 09:30 mark -1.99 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 10 | $20.90 | $2.02 | $+0.93 | $11,996.49 | ▲ +0.93 after sell → book $12,531.20; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ABCL` | 18 | $11.00 | $2.03 | $+10.57 | $11,796.46 | ▲ +10.57 after sell → book $12,529.17; vs 09:30 mark -2.03 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 1 | $170.64 | $1.71 | $-0.22 | $11,624.11 | ▼ -0.22 after sell → book $12,527.46; vs 09:30 mark -1.71 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 2 | $105.58 | $2.00 | $-2.42 | $11,410.95 | ▼ -2.42 after sell → book $12,525.46; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 48 | $4.77 | $2.13 | $-11.97 | $11,179.86 | ▼ -11.97 after sell → book $12,523.33; vs 09:30 mark -2.13 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 7 | $175.01 | $2.01 | — | $9,952.78 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.0; combo leftover $1257.73; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 14 | $88.94 | $2.03 | — | $8,705.59 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.9; combo leftover $1257.73; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 82 | $15.28 | $2.24 | — | $7,450.39 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-0.7; combo leftover $1257.73; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 8 | $142.36 | $2.01 | — | $6,309.50 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.6; combo leftover $1257.73; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 246 | $5.10 | $3.17 | — | $5,051.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.9; combo leftover $1257.73; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 26 | $47.89 | $2.07 | — | $3,804.51 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ⚪; ret5=+14.0; combo leftover $1257.73; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 90 | $13.92 | $2.26 | — | $2,549.45 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.9; combo leftover $1257.73; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 276 | $4.54 | $3.56 | — | $1,291.47 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-14.6; combo leftover $1257.73; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 31 | $13.62 | $2.11 | — | $1,711.73 | — | news🔴; gate news=bad; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; combo leftover $430.49; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 7 | $54.51 | $2.04 | — | $2,091.26 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+15.1; combo leftover $430.49; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `INTU` | 1 | $364.35 | $2.02 | — | $2,453.59 | — | news🔴; gate news=bad; list overnight,overnight_mega; 🔵; ret5=+10.2; combo leftover $430.49; owner short_news_r_h3 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,453.59 | ▼ close $12,165.12 vs 09:30 $12,583.08 (session -332.67) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,453.59 | ▲ 09:30 equity $12,181.14 vs yday $12,165.12 (+16.02) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `FUTU` | 3 | $124.67 | $2.02 | $+24.45 | $2,825.58 | ▲ +24.45 after sell → book $12,179.12; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BEKE` | 21 | $18.14 | $2.07 | $+0.18 | $3,204.45 | ▲ +0.18 after sell → book $12,177.05; vs 09:30 mark -2.07 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BJ` | 4 | $94.60 | $2.02 | $-1.54 | $3,580.83 | ▼ -1.54 after sell → book $12,175.03; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BKE` | 8 | $44.39 | $2.03 | $+6.43 | $3,933.91 | ▲ +6.43 after sell → book $12,172.99; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `PSEC` | 167 | $2.35 | $2.53 | $+3.33 | $4,323.83 | ▲ +3.33 after sell → book $12,170.46; vs 09:30 mark -2.53 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `QTRX` | 39 | $2.83 | $1.22 | $+8.35 | $4,212.24 | ▲ +8.35 after sell → book $12,169.24; vs 09:30 mark -1.22 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 1 | $88.24 | $0.89 | $-0.94 | $4,123.12 | ▼ -0.94 after sell → book $12,168.36; vs 09:30 mark -0.88 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 3 | $38.41 | $1.16 | $-2.38 | $4,006.73 | ▼ -2.38 after sell → book $12,167.20; vs 09:30 mark -1.16 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 5 | $20.50 | $1.04 | $-0.12 | $3,903.19 | ▼ -0.12 after sell → book $12,166.16; vs 09:30 mark -1.04 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 4 | $26.00 | $1.05 | $+1.83 | $3,798.13 | ▲ +1.83 after sell → book $12,165.10; vs 09:30 mark -1.06 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 977 | $0.58 | $8.63 | — | $3,219.92 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-27.5; combo leftover $569.72; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 109 | $5.21 | $2.32 | — | $2,649.71 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ret5=+14.3; combo leftover $569.72; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `ANF` | 4 | $131.37 | $2.00 | — | $2,122.23 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.3; combo leftover $569.72; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 31 | $18.26 | $2.08 | — | $1,554.08 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.4; combo leftover $569.72; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `BOX` | 16 | $34.30 | $2.04 | — | $1,003.25 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.7; combo leftover $569.72; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `DY` | 1 | $326.91 | $1.99 | — | $674.34 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-15.2; combo leftover $569.72; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 11 | $12.22 | $1.40 | — | $807.36 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.4; combo leftover $134.87; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 26 | $5.08 | $1.42 | — | $938.02 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+17.6; combo leftover $134.87; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 1 | $132.64 | $1.35 | — | $1,069.31 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+16.5; combo leftover $134.87; owner short_news_r_h3 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,069.31 | ▲ close $12,484.37 vs 09:30 $12,181.14 (session +342.50) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,069.31 | ▼ 09:30 equity $12,474.85 vs yday $12,484.37 (-9.52) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `BBY` | 1 | $80.60 | $0.81 | — | $987.90 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.0; combo leftover $120.30; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `BILI` | 7 | $16.18 | $1.15 | — | $873.48 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-6.7; combo leftover $120.30; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 1 | $118.77 | $1.19 | — | $753.52 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.3; combo leftover $120.30; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CMBT` | 6 | $17.78 | $1.08 | — | $645.76 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.2; combo leftover $120.30; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CSIQ` | 8 | $13.41 | $1.10 | — | $537.38 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.1; combo leftover $120.30; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `HQY` | 1 | $97.16 | $0.97 | — | $439.25 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.5; combo leftover $120.30; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `TD` | 1 | $120.17 | $1.20 | — | $317.87 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; combo leftover $120.30; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 2 | $74.54 | $1.52 | — | $465.43 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=-0.1; combo leftover $158.94; owner short_news_r_h3 | — |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 2 | $55.25 | $1.13 | — | $574.80 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=+2.1; combo leftover $158.94; owner short_news_r_h3 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $574.80 | ▼ close $12,402.77 vs 09:30 $12,474.85 (session -61.90) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $574.80 | ▲ 09:30 equity $12,411.03 vs yday $12,402.77 (+8.26) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `BMO` | 7 | $172.76 | $2.03 | $-19.79 | $1,782.09 | ▼ -19.79 after sell → book $12,409.00; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BNS` | 14 | $93.30 | $2.05 | $+56.96 | $3,086.23 | ▲ +56.96 after sell → book $12,406.95; vs 09:30 mark -2.05 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 82 | $18.15 | $2.26 | $+230.84 | $4,572.27 | ▲ +230.84 after sell → book $12,404.68; vs 09:30 mark -2.27 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 8 | $132.80 | $2.03 | $-80.53 | $5,632.64 | ▼ -80.53 after sell → book $12,402.65; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EH` | 246 | $4.58 | $3.22 | $-134.32 | $6,756.09 | ▼ -134.32 after sell → book $12,399.43; vs 09:30 mark -3.22 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GFI` | 26 | $48.42 | $2.09 | $+9.62 | $8,012.93 | ▲ +9.62 after sell → book $12,397.34; vs 09:30 mark -2.09 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GRRR` | 90 | $15.66 | $2.29 | $+152.05 | $9,420.04 | ▲ +152.05 after sell → book $12,395.05; vs 09:30 mark -2.29 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SHMD` | 276 | $3.38 | $3.62 | $-328.72 | $10,349.30 | ▼ -328.72 after sell → book $12,391.44; vs 09:30 mark -3.61 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 31 | $13.90 | $2.08 | $-12.72 | $9,916.32 | ▼ -12.72 after sell → book $12,389.35; vs 09:30 mark -2.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 7 | $52.49 | $2.01 | $+10.09 | $9,546.88 | ▲ +10.09 after sell → book $12,387.34; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `INTU` | 1 | $347.82 | $1.99 | $+12.51 | $9,197.07 | ▲ +12.51 after sell → book $12,385.35; vs 09:30 mark -1.99 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 3 | $261.16 | $2.00 | — | $8,411.59 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+7.8; combo leftover $1034.67; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 68 | $15.01 | $2.19 | — | $7,388.71 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+3.7; combo leftover $1034.67; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 9 | $103.89 | $2.02 | — | $6,451.69 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.5; combo leftover $1034.67; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 266 | $3.88 | $3.43 | — | $5,416.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-8.6; combo leftover $1034.67; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 23 | $44.40 | $2.06 | — | $4,392.92 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.4; combo leftover $1034.67; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 41 | $24.69 | $2.11 | — | $3,378.51 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.8; combo leftover $1034.67; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 123 | $8.35 | $2.36 | — | $2,349.10 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.1; combo leftover $1034.67; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 27 | $37.65 | $2.07 | — | $1,330.62 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $1034.67; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 2 | $252.24 | $2.03 | — | $1,833.07 | — | news🔴; gate news=bad; list probable,yday_gainer; ⚪; ret5=+2.2; combo leftover $665.31; owner short_news_r_h3 | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 22 | $30.18 | $2.09 | — | $2,494.93 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+12.1; combo leftover $665.31; owner short_news_r_h3 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,494.93 | ▼ close $12,122.28 vs 09:30 $12,411.03 (session -240.70) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,494.93 | ▲ 09:30 equity $12,144.91 vs yday $12,122.28 (+22.63) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `SLQT` | 977 | $0.51 | $8.09 | $-88.03 | $2,985.12 | ▼ -88.03 after sell → book $12,136.83; vs 09:30 mark -8.08 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `TIGR` | 109 | $5.00 | $2.35 | $-27.55 | $3,527.77 | ▼ -27.55 after sell → book $12,134.48; vs 09:30 mark -2.35 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 4 | $148.03 | $2.02 | $+62.62 | $4,117.87 | ▲ +62.62 after sell → book $12,132.46; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 31 | $19.25 | $2.10 | $+26.50 | $4,712.52 | ▲ +26.50 after sell → book $12,130.36; vs 09:30 mark -2.10 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BOX` | 16 | $34.72 | $2.06 | $+2.62 | $5,265.98 | ▲ +2.62 after sell → book $12,128.30; vs 09:30 mark -2.06 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `DY` | 1 | $298.01 | $2.01 | $-32.91 | $5,561.98 | ▼ -32.91 after sell → book $12,126.29; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 11 | $11.10 | $1.25 | $+9.67 | $5,438.62 | ▲ +9.67 after sell → book $12,125.03; vs 09:30 mark -1.26 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 26 | $4.97 | $1.37 | $-0.06 | $5,307.90 | ▼ -0.06 after sell → book $12,123.66; vs 09:30 mark -1.37 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `NEM` | 1 | $127.45 | $1.28 | $+2.56 | $5,179.17 | ▲ +2.56 after sell → book $12,122.38; vs 09:30 mark -1.28 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,179.17 | ▲ close $12,175.59 vs 09:30 $12,144.91 (session +53.20) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,179.17 | ▼ 09:30 equity $12,111.68 vs yday $12,175.59 (-63.91) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `BBY` | 1 | $79.83 | $0.82 | $-2.40 | $5,258.18 | ▼ -2.40 after sell → book $12,110.86; vs 09:30 mark -0.82 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `BILI` | 7 | $15.97 | $1.16 | $-3.78 | $5,368.81 | ▼ -3.78 after sell → book $12,109.70; vs 09:30 mark -1.16 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CM` | 1 | $113.66 | $1.16 | $-7.46 | $5,481.31 | ▼ -7.46 after sell → book $12,108.54; vs 09:30 mark -1.16 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CMBT` | 6 | $18.28 | $1.13 | $+0.78 | $5,589.86 | ▲ +0.78 after sell → book $12,107.41; vs 09:30 mark -1.13 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CSIQ` | 8 | $12.18 | $1.02 | $-11.96 | $5,686.28 | ▼ -11.96 after sell → book $12,106.39; vs 09:30 mark -1.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `HQY` | 1 | $96.65 | $0.99 | $-2.47 | $5,781.94 | ▼ -2.47 after sell → book $12,105.40; vs 09:30 mark -0.99 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `TD` | 1 | $120.54 | $1.23 | $-2.06 | $5,901.25 | ▼ -2.06 after sell → book $12,104.17; vs 09:30 mark -1.23 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `MT` | 2 | $73.22 | $1.47 | $-0.35 | $5,753.34 | ▼ -0.35 after sell → book $12,102.70; vs 09:30 mark -1.47 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `TX` | 2 | $54.76 | $1.10 | $-1.26 | $5,642.72 | ▼ -1.26 after sell → book $12,101.60; vs 09:30 mark -1.10 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,642.72 | ▼ close $12,026.87 vs 09:30 $12,111.68 (session -74.73) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,642.72 | ▼ 09:30 equity $11,992.53 vs yday $12,026.87 (-34.34) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 3 | $246.70 | $2.02 | $-47.40 | $6,380.80 | ▼ -47.40 after sell → book $11,990.51; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBAR` | 68 | $15.01 | $2.22 | $-4.41 | $7,399.27 | ▼ -4.41 after sell → book $11,988.29; vs 09:30 mark -2.22 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ESTC` | 9 | $92.00 | $2.04 | $-111.06 | $8,225.23 | ▼ -111.06 after sell → book $11,986.26; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FINV` | 266 | $3.32 | $3.49 | $-155.88 | $9,104.86 | ▼ -155.88 after sell → book $11,982.77; vs 09:30 mark -3.49 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FRO` | 23 | $44.17 | $2.08 | $-9.43 | $10,118.70 | ▼ -9.43 after sell → book $11,980.69; vs 09:30 mark -2.08 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GAP` | 41 | $21.97 | $2.13 | $-115.77 | $11,017.33 | ▼ -115.77 after sell → book $11,978.56; vs 09:30 mark -2.13 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `HAFN` | 123 | $8.58 | $2.39 | $+23.54 | $12,070.28 | ▲ +23.54 after sell → book $11,976.17; vs 09:30 mark -2.39 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `IREN` | 27 | $35.80 | $2.09 | $-54.11 | $13,034.66 | ▼ -54.11 after sell → book $11,974.08; vs 09:30 mark -2.09 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 2 | $235.71 | $2.00 | $+29.03 | $12,561.24 | ▲ +29.03 after sell → book $11,972.08; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 22 | $26.78 | $2.06 | $+70.65 | $11,970.03 | ▲ +70.65 after sell → book $11,970.03; vs 09:30 mark -2.05 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,970.03 | ▲ close $11,970.03 vs 09:30 $11,992.53 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,970.03 | ▲ 09:30 equity $11,970.03 vs yday $11,970.03 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 125 | $10.74 | $2.37 | — | $10,624.54 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+8.5; combo leftover $1346.63; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $9,567.32 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.3; combo leftover $1346.63; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 195 | $6.90 | $2.58 | — | $8,219.24 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.8; combo leftover $1346.63; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 3 | $354.49 | $2.00 | — | $7,153.77 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.3; combo leftover $1346.63; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 60 | $22.32 | $2.17 | — | $5,812.40 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+2.4; combo leftover $1346.63; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 5 | $257.00 | $2.00 | — | $4,525.40 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-5.5; combo leftover $1346.63; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 28 | $47.60 | $2.07 | — | $3,190.52 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.2; combo leftover $1346.63; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 89 | $15.09 | $2.26 | — | $1,845.26 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+6.1; combo leftover $1346.63; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 62 | $14.85 | $2.22 | — | $2,763.73 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; combo leftover $922.63; owner short_news_r_h3 | — |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 539 | $1.71 | $7.08 | — | $3,678.35 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+11.9; combo leftover $922.63; owner short_news_r_h3 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,678.35 | ▲ close $12,465.94 vs 09:30 $11,970.03 (session +522.66) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,678.35 | ▲ 09:30 equity $12,525.20 vs yday $12,465.94 (+59.26) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 6 | $63.18 | $2.01 | — | $3,297.26 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-10.9; combo leftover $413.81; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 47 | $8.74 | $2.13 | — | $2,884.35 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.8; combo leftover $413.81; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 6 | $68.52 | $2.01 | — | $2,471.22 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.4; combo leftover $413.81; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 114 | $3.62 | $2.33 | — | $2,056.78 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.1; combo leftover $413.81; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 2 | $167.55 | $2.00 | — | $1,719.68 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; combo leftover $413.81; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 9 | $44.90 | $2.02 | — | $1,313.56 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.5; combo leftover $413.81; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 4 | $98.15 | $2.00 | — | $918.96 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.9; combo leftover $413.81; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 26 | $15.70 | $2.07 | — | $508.69 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.4; combo leftover $413.81; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 54 | $4.67 | $2.18 | — | $758.70 | — | news🔴; gate news=bad; list yday_gainer; ret5=+11.9; combo leftover $254.35; owner short_news_r_h3 | — |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 3 | $76.55 | $2.03 | — | $986.32 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; combo leftover $254.35; owner short_news_r_h3 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $986.32 | ▲ close $12,510.87 vs 09:30 $12,525.20 (session +6.44) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $986.32 | ▲ 09:30 equity $12,531.57 vs yday $12,510.87 (+20.70) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $986.32 | ▲ close $12,568.65 vs 09:30 $12,531.57 (session +37.08) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $986.32 | ▼ 09:30 equity $12,545.91 vs yday $12,568.65 (-22.74) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `AI` | 125 | $10.51 | $2.40 | $-34.14 | $2,297.67 | ▼ -34.14 after sell → book $12,543.51; vs 09:30 mark -2.40 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 3 | $366.23 | $2.02 | $+39.45 | $3,394.35 | ▲ +39.45 after sell → book $12,541.50; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CHPT` | 195 | $9.39 | $2.62 | $+480.35 | $5,222.77 | ▲ +480.35 after sell → book $12,538.87; vs 09:30 mark -2.63 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CIEN` | 3 | $341.90 | $2.02 | $-41.79 | $6,246.45 | ▼ -41.79 after sell → book $12,536.85; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CPB` | 60 | $21.67 | $2.19 | $-43.36 | $7,544.46 | ▼ -43.36 after sell → book $12,534.66; vs 09:30 mark -2.19 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FIVE` | 5 | $252.92 | $2.03 | $-24.43 | $8,807.04 | ▼ -24.43 after sell → book $12,532.64; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HPE` | 28 | $56.94 | $2.10 | $+257.35 | $10,399.26 | ▲ +257.35 after sell → book $12,530.54; vs 09:30 mark -2.10 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MEI` | 89 | $13.84 | $2.28 | $-115.79 | $11,628.74 | ▼ -115.79 after sell → book $12,528.26; vs 09:30 mark -2.28 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `SLN` | 62 | $13.60 | $2.18 | $+73.10 | $10,783.36 | ▲ +73.10 after sell → book $12,526.08; vs 09:30 mark -2.18 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 539 | $1.58 | $6.95 | $+56.04 | $9,924.79 | ▲ +56.04 after sell → book $12,519.13; vs 09:30 mark -6.95 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,924.79 | ▼ close $12,496.94 vs 09:30 $12,545.91 (session -22.19) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,924.79 | ▼ 09:30 equity $12,488.57 vs yday $12,496.94 (-8.37) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `AMBA` | 6 | $67.44 | $2.03 | $+21.52 | $10,327.40 | ▲ +21.52 after sell → book $12,486.54; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `ASAN` | 47 | $8.26 | $2.15 | $-26.84 | $10,713.47 | ▼ -26.84 after sell → book $12,484.39; vs 09:30 mark -2.15 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DOCU` | 6 | $64.60 | $2.03 | $-27.56 | $11,099.04 | ▼ -27.56 after sell → book $12,482.36; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DOMO` | 114 | $3.76 | $2.36 | $+11.84 | $11,525.32 | ▲ +11.84 after sell → book $12,480.00; vs 09:30 mark -2.36 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `GWRE` | 2 | $142.43 | $2.02 | $-54.25 | $11,808.17 | ▼ -54.25 after sell → book $12,477.98; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `IOT` | 9 | $38.23 | $2.04 | $-64.13 | $12,150.16 | ▼ -64.13 after sell → book $12,475.95; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `LULU` | 4 | $98.71 | $2.02 | $-1.78 | $12,542.97 | ▼ -1.78 after sell → book $12,473.92; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `MAMA` | 26 | $15.26 | $2.09 | $-15.60 | $12,937.65 | ▼ -15.60 after sell → book $12,471.84; vs 09:30 mark -2.08 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 54 | $4.36 | $2.15 | $+12.41 | $12,700.05 | ▲ +12.41 after sell → book $12,469.68; vs 09:30 mark -2.16 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PIPR` | 3 | $76.79 | $2.00 | $-4.74 | $12,467.68 | ▼ -4.74 after sell → book $12,467.68; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,467.68 | ▲ close $12,467.68 vs 09:30 $12,488.57 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,467.68 | ▲ 09:30 equity $12,467.68 vs yday $12,467.68 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 8 | $164.43 | $2.01 | — | $11,150.23 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,earn_react; ⚪; ret5=+4.9; combo leftover $1402.61; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 237 | $5.91 | $3.06 | — | $9,746.50 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover,ohlc_hot; ret5=+14.1; combo leftover $1402.61; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 5 | $242.17 | $2.00 | — | $8,533.65 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.1; combo leftover $1402.61; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `CPRT` | 43 | $32.01 | $2.12 | — | $7,155.10 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.4; combo leftover $1402.61; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `DSGX` | 19 | $71.71 | $2.05 | — | $5,790.56 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-9.1; combo leftover $1402.61; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `KR` | 25 | $56.02 | $2.06 | — | $4,388.00 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.2; combo leftover $1402.61; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `LPTH` | 149 | $9.37 | $2.44 | — | $2,989.43 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+1.5; combo leftover $1402.61; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `REF` | 107 | $13.10 | $2.31 | — | $1,585.42 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.9; combo leftover $1402.61; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 2 | $112.83 | $2.02 | — | $1,809.07 | — | news🔴; gate news=bad; list yday_gainer,ohlc_hot; ret5=+11.7; combo leftover $317.08; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `RWT` | 90 | $3.52 | $2.29 | — | $2,123.57 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-19.2; combo leftover $317.08; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `CRDL` | 156 | $2.03 | $2.50 | — | $2,437.75 | — | news🔴; gate news=bad; list yday_mover; ret5=-8.8; combo leftover $317.08; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BKV` | 12 | $24.97 | $2.05 | — | $2,735.34 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.8; combo leftover $317.08; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `MYGN` | 94 | $3.37 | $2.31 | — | $3,049.81 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+4.0; combo leftover $317.08; owner short_news_r_h3 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,049.81 | ▲ close $12,497.37 vs 09:30 $12,467.68 (session +58.92) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,049.81 | ▲ 09:30 equity $12,518.73 vs yday $12,497.37 (+21.36) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,049.81 | ▲ close $12,888.02 vs 09:30 $12,518.73 (session +369.29) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,049.81 | ▼ 09:30 equity $12,851.48 vs yday $12,888.02 (-36.54) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,049.81 | ▲ close $12,901.42 vs 09:30 $12,851.48 (session +49.94) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,049.81 | ▼ 09:30 equity $12,822.19 vs yday $12,901.42 (-79.23) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 8 | $140.03 | $2.03 | $-199.25 | $4,168.02 | ▼ -199.25 after sell → book $12,820.16; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `DBI` | 237 | $6.25 | $3.11 | $+74.41 | $5,646.16 | ▲ +74.41 after sell → book $12,817.05; vs 09:30 mark -3.11 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ADBE` | 5 | $253.34 | $2.03 | $+51.82 | $6,910.83 | ▲ +51.82 after sell → book $12,815.02; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CPRT` | 43 | $30.57 | $2.14 | $-66.18 | $8,223.20 | ▼ -66.18 after sell → book $12,812.88; vs 09:30 mark -2.14 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `DSGX` | 19 | $78.12 | $2.07 | $+117.67 | $9,705.41 | ▲ +117.67 after sell → book $12,810.81; vs 09:30 mark -2.07 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `KR` | 25 | $61.93 | $2.09 | $+143.60 | $11,251.58 | ▲ +143.60 after sell → book $12,808.73; vs 09:30 mark -2.08 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `LPTH` | 149 | $9.40 | $2.47 | $-0.44 | $12,649.70 | ▼ -0.44 after sell → book $12,806.25; vs 09:30 mark -2.48 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `REF` | 107 | $15.75 | $2.34 | $+278.90 | $14,332.61 | ▲ +278.90 after sell → book $12,803.91; vs 09:30 mark -2.34 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `QRVO` | 2 | $118.18 | $2.00 | $-14.71 | $14,094.26 | ▼ -14.71 after sell → book $12,801.92; vs 09:30 mark -1.99 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `RWT` | 90 | $3.98 | $2.26 | $-45.95 | $13,733.80 | ▼ -45.95 after sell → book $12,799.66; vs 09:30 mark -2.26 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `CRDL` | 156 | $1.85 | $2.46 | $+23.12 | $13,442.74 | ▲ +23.12 after sell → book $12,797.20; vs 09:30 mark -2.46 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `BKV` | 12 | $24.42 | $2.03 | $+2.52 | $13,147.67 | ▲ +2.52 after sell → book $12,795.17; vs 09:30 mark -2.03 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `MYGN` | 94 | $3.75 | $2.27 | $-40.30 | $12,792.90 | ▼ -40.30 after sell → book $12,792.90; vs 09:30 mark -2.27 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 173 | $33.14 | $2.51 | — | $7,057.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ret5=-2.9; combo leftover $5756.80; owner union_e_fresh_h3 | — |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 140 | $40.93 | $2.41 | — | $1,324.56 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $5756.80; owner union_e_fresh_h3 | — |
| 2026-09-16 09:30 ET | **SHORT** | `BBNX` | 35 | $18.61 | $2.13 | — | $1,973.78 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; ret5=-0.5; combo leftover $662.28; owner short_news_r_h3 | — |
| 2026-09-16 09:30 ET | **SHORT** | `GFR` | 96 | $6.83 | $2.32 | — | $2,627.14 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+11.2; combo leftover $662.28; owner short_news_r_h3 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,627.14 | ▲ close $12,915.32 vs 09:30 $12,822.19 (session +131.79) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,627.14 | ▲ 09:30 equity $13,289.04 vs yday $12,915.32 (+373.72) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `ALMU` | 105 | $11.21 | $2.31 | — | $1,447.78 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.0; combo leftover $1182.21; owner union_e_fresh_h3 | — |
| 2026-09-17 09:30 ET | **BUY** | `LEN` | 14 | $81.00 | $2.03 | — | $311.75 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.0; combo leftover $1182.21; owner union_e_fresh_h3 | — |
| 2026-09-17 09:30 ET | **SHORT** | `BULL` | 39 | $7.95 | $2.14 | — | $619.66 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-18.4; combo leftover $311.75; owner short_news_r_h3 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $619.66 | ▲ close $13,492.37 vs 09:30 $13,289.04 (session +209.80) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $619.66 | ▲ 09:30 equity $13,767.17 vs yday $13,492.37 (+274.80) | — | — |
| 2026-09-18 09:30 ET | **SHORT** | `FIVN` | 17 | $34.44 | $2.08 | — | $1,203.07 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $619.66; owner short_news_r_h3 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,203.07 | ▲ close $13,848.28 vs 09:30 $13,767.17 (session +83.18) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,203.07 | ▲ 09:30 equity $14,026.22 vs yday $13,848.28 (+177.94) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `FPS` | 173 | $40.03 | $2.59 | $+1186.87 | $8,125.66 | ▲ +1,186.87 after sell → book $14,023.63; vs 09:30 mark -2.59 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TCOM` | 140 | $41.00 | $2.48 | $+4.91 | $13,863.18 | ▲ +4.91 after sell → book $14,021.15; vs 09:30 mark -2.48 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `BBNX` | 35 | $22.11 | $2.10 | $-126.73 | $13,087.24 | ▼ -126.73 after sell → book $14,019.05; vs 09:30 mark -2.10 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `GFR` | 96 | $6.55 | $2.28 | $+22.28 | $12,456.16 | ▲ +22.28 after sell → book $14,016.78; vs 09:30 mark -2.27 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 424 | $8.26 | $5.66 | — | $15,952.74 | — | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $3504.19; owner short_news_r_h3 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AMD` | 6 | $583.88 | $2.14 | — | $19,453.88 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+8.5; combo leftover $3504.19; owner short_news_r_h3 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,453.88 | ▲ close $14,397.93 vs 09:30 $14,026.22 (session +388.95) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,453.88 | ▲ 09:30 equity $14,450.65 vs yday $14,397.93 (+52.72) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `BULL` | 39 | $8.28 | $2.11 | $-16.92 | $19,129.04 | ▼ -16.92 after sell → book $14,448.54; vs 09:30 mark -2.11 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SHORT** | `USFD` | 38 | $93.97 | $2.24 | — | $22,697.66 | — | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $3612.14; owner short_news_r_h3 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22,697.66 | ▼ close $14,344.62 vs 09:30 $14,450.65 (session -101.68) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22,697.66 | ▼ 09:30 equity $14,104.64 vs yday $14,344.62 (-239.98) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `ALMU` | 105 | $13.82 | $2.33 | $+269.41 | $24,146.43 | ▲ +269.41 after sell → book $14,102.30; vs 09:30 mark -2.34 | union_e_fresh_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `LEN` | 14 | $82.00 | $2.05 | $+9.92 | $25,292.38 | ▲ +9.92 after sell → book $14,100.25; vs 09:30 mark -2.05 | union_e_fresh_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `CBRL` | 95 | $47.57 | $2.27 | — | $20,770.95 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-11.2; combo leftover $4552.63; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 23 | $196.78 | $2.06 | — | $16,242.95 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $4552.63; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `GIS` | 127 | $35.74 | $2.37 | — | $11,701.60 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $4552.63; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `KBH` | 96 | $47.15 | $2.28 | — | $7,172.92 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.9; combo leftover $4552.63; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `PAYX` | 41 | $109.67 | $2.11 | — | $2,674.34 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $4552.63; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **SHORT** | `HALO` | 22 | $116.85 | $2.16 | — | $5,242.88 | — | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $2674.34; owner short_news_r_h3 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,242.88 | ▼ close $13,671.46 vs 09:30 $14,104.64 (session -415.53) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,242.88 | ▲ 09:30 equity $13,766.85 vs yday $13,671.46 (+95.39) | — | — |
| 2026-09-24 09:30 ET | **COVER** | `AMD` | 6 | $600.27 | $2.01 | $-102.49 | $1,639.26 | ▼ -102.49 after sell → book $13,764.84; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,639.26 | ▼ close $13,751.44 vs 09:30 $13,766.85 (session -13.40) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $-2,059.94 | ▼ 09:30 equity $8,899.82 vs yday $8,918.72 (-18.90) | 09:30 open · cash $-2,059.94 (unchanged overnight, no fees) · equity $8,899.82 vs prior close $8,918.72 (-18.90) | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $-2,059.94 | ▼ close $8,802.02 vs 09:30 $8,899.82 (session -97.80) | 16:00 close · cash $-2,059.94 · equity $8,802.02 vs 09:30 $8,899.82 (-97.80; session marks -97.80) · 13 name(s) marked open→close (per-name table). ABVX×32 09:30 $94.87 → close $94.87 +0.00; AEHL×294 09:30 $9.05 → close $9.36 -91.14; ANAB×60 09:30 $51.70 → close $51.70 +0.00; BAND×3 09:30 $61.83 → close $61.83 -0.00; CBRL×14 09:30 $52.39 → close $51.81 -8.12; CTAS×3 09:30 $197.68 → close $197.68 -0.00; GIS×19 09:30 $34.83 → close $34.83 +0.00; HALO×1 09:30 $115.36 → close $113.90 +1.46; KBH×14 09:30 $47.65 → close $47.65 +0.00; MLKN×162 09:30 $19.91 → close $19.91 -0.00; PAYX×6 09:30 $101.59 → close $101.59 +0.00; THO×46 09:30 $70.93 → close $70.93 +0.00; USFD×8 09:30 $93.82 → close $93.82 +0.00 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `INO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-14 | `VOR` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-14 | `ARX` | cash | leftover split 2.37 < 1 share @ 19.57 |
| 2026-08-14 | `AIRO` | cash | leftover split 2.37 < 1 share @ 11.12 |
| 2026-08-14 | `MH` | cash | leftover split 2.37 < 1 share @ 13.55 |
| 2026-08-14 | `CLBT` | cash | leftover split 2.37 < 1 share @ 10.83 |
| 2026-08-14 | `LUNR` | cash | leftover split 2.37 < 1 share @ 19.17 |
| 2026-08-14 | `NMAX` | cash | leftover split 2.37 < 1 share @ 9.89 |
| 2026-08-17 | `INO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-17 | `VOR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-17 | `BTBT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `EU` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `OWL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `APMD` | cash | leftover split 5.94 < 1 share @ 31.70 |
| 2026-08-17 | `RNW` | cash | leftover split 5.94 < 1 share @ 6.80 |
| 2026-08-18 | `BTBT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `EU` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `OWL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `VERI` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `ZNTL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `HIVE` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `KLAR` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new short short_news_r_h3 |
| 2026-08-18 | `CADL` | hard_red | hard-red S=-6.20 sit; no new short short_news_r_h3 |
| 2026-08-18 | `RNW` | hard_red | hard-red S=-6.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `VERI` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `ZNTL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `HIVE` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h3 |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h3 |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h3 |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h3 |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h3 |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h3 |
| 2026-08-19 | `KC` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h3 |
| 2026-08-19 | `KEYS` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h3 |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `LUNR` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `AAP` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-21 | `EL` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `TOYO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `DVLT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `AEG` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `ALVO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `ATAT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `ATHM` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `AEM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `WYFI` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `TEAM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `WMT` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `AQST` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `DE` | cash | leftover split 385.94 < 1 share @ 623.26 |
| 2026-08-21 | `MRNA` | cash | leftover split 122.59 < 1 share @ 133.11 |
| 2026-08-24 | `EL` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `TOYO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `DVLT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AAP` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AEG` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ALVO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ATAT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ATHM` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AEM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `WYFI` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `TEAM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `WMT` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AQST` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `FUTU` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `BEKE` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `BJ` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `BKE` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `PSEC` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `QTRX` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `AUGO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `SSRM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `ARIS` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new long union_e_fresh_h3 |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `EU` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `BMO` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-25 | `FUTU` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `BEKE` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `BJ` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `BKE` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `PSEC` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `QTRX` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `AUGO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `ARIS` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `NOG` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-26 | `BMO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `BNS` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `EH` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `GFI` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `GRRR` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `SHMD` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `AVAH` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `ARE` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `INTU` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `BE` | cash | leftover split 134.87 < 1 share @ 213.94 |
| 2026-08-26 | `CRM` | cash | leftover split 134.87 < 1 share @ 199.94 |
| 2026-08-27 | `BMO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `BNS` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `BZ` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `DKS` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `EH` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `GFI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `GRRR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `SHMD` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `AVAH` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `ARE` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `SLQT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `TIGR` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `ANF` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `BBWI` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `BOX` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `DY` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `NEM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `RY` | cash | leftover split 120.30 < 1 share @ 206.82 |
| 2026-08-28 | `SLQT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `TIGR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `ANF` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BBWI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BOX` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `DY` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `AQST` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `NEM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BBY` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `BILI` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `CM` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `CMBT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `CSIQ` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `HQY` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `TD` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `MT` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `TX` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `BBY` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `BILI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `CM` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `CMBT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `CSIQ` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `HQY` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `TD` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `MT` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `TX` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `ADSK` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `BBAR` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `ESTC` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `FINV` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `FRO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `GAP` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `HAFN` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `IREN` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `SIMO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `FIG` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new long union_e_fresh_h3 |
| 2026-09-01 | `ADSK` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `BBAR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `ESTC` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `FINV` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `FRO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `GAP` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `HAFN` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `IREN` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `SIMO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `FIG` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new long union_e_fresh_h3 |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h3 |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h3 |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h3 |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h3 |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h3 |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h3 |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new short short_news_r_h3 |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new short short_news_r_h3 |
| 2026-09-04 | `AI` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `AVGO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `CHPT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `CIEN` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `CPB` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `FIVE` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `HPE` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `MEI` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `SLN` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `AI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `AVGO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `CHPT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `CIEN` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `CPB` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `FIVE` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `HPE` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `MEI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `SLN` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `OPK` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `AMBA` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `ASAN` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `DOCU` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `DOMO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `GWRE` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `IOT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `LULU` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `MAMA` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `PIPR` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h3 |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h3 |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h3 |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `GEMI` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-09 | `AMBA` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `ASAN` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `DOCU` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `DOMO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `GWRE` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `IOT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `LULU` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `MAMA` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `GSM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `PIPR` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `ABM` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h3 |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h3 |
| 2026-09-09 | `AVO` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h3 |
| 2026-09-09 | `JMKE` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h3 |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h3 |
| 2026-09-09 | `SAIL` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h3 |
| 2026-09-09 | `TTAN` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h3 |
| 2026-09-09 | `NVS` | hard_red | hard-red S=-13.95 sit; no new short short_news_r_h3 |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h3 |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h3 |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h3 |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h3 |
| 2026-09-10 | `JMKE` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h3 |
| 2026-09-10 | `AEO` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h3 |
| 2026-09-10 | `AVAV` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h3 |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h3 |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new short short_news_r_h3 |
| 2026-09-10 | `BKV` | hard_red | hard-red S=-13.28 sit; no new short short_news_r_h3 |
| 2026-09-14 | `ORCL` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `DBI` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `ADBE` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `CPRT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `DSGX` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `KR` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `LPTH` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `REF` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `QRVO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `RWT` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `CRDL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-15 | `ORCL` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `DBI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `ADBE` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `CPRT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `DSGX` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `KR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `LPTH` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `REF` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `QRVO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `RWT` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `CRDL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `MYGN` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `FPS` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h3 |
| 2026-09-15 | `HITI` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h3 |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h3 |
| 2026-09-15 | `UROY` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h3 |
| 2026-09-15 | `TYRA` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-17 | `FPS` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-17 | `TCOM` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-17 | `GFR` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `FPS` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `TCOM` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `BBNX` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `GFR` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `ALMU` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `LEN` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `BULL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-21 | `ALMU` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `LEN` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `BULL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `FIVN` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-22 | `ALMU` | no_price | no 09:30 open — carry |
| 2026-09-22 | `LEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AMD` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-22 | `ABVX` | no_price | no 09:30 open |
| 2026-09-22 | `ANAB` | no_price | no 09:30 open |
| 2026-09-22 | `MLKN` | no_price | no 09:30 open |
| 2026-09-22 | `THO` | no_price | no 09:30 open |
| 2026-09-22 | `PAYX` | no_price | no 09:30 open |
| 2026-09-23 | `AMD` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-23 | `USFD` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `USFD` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-24 | `CBRL` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `CTAS` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `GIS` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `KBH` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `PAYX` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `HALO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `BB` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h3 |
| 2026-09-24 | `DRI` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h3 |
| 2026-09-24 | `FUL` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h3 |
| 2026-09-24 | `NEOV` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h3 |
| 2026-09-24 | `SNX` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h3 |
| 2026-09-24 | `PANW` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `VOYG` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `BMEA` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `FIVN` | 17 | 2026-09-18 @ $34.44 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+14.0; combo leftover $619.66; owner short_news_r_h3 |
| `AEHL` | 424 | 2026-09-21 @ $8.26 | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $3504.19; owner short_news_r_h3 |
| `USFD` | 38 | 2026-09-22 @ $93.97 | news🔴; gate news=bad; list flatten; ret5=-0.6; combo leftover $3612.14; owner short_news_r_h3 |
| `CBRL` | 95 | 2026-09-23 @ $47.57 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-11.2; combo leftover $4552.63; owner union_e_fresh_h3 |
| `CTAS` | 23 | 2026-09-23 @ $196.78 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $4552.63; owner union_e_fresh_h3 |
| `GIS` | 127 | 2026-09-23 @ $35.74 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $4552.63; owner union_e_fresh_h3 |
| `KBH` | 96 | 2026-09-23 @ $47.15 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.9; combo leftover $4552.63; owner union_e_fresh_h3 |
| `PAYX` | 41 | 2026-09-23 @ $109.67 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $4552.63; owner union_e_fresh_h3 |
| `HALO` | 22 | 2026-09-23 @ $116.85 | news🔴; gate news=bad; list flatten; 🔵; ⚪; ret5=+3.3; combo leftover $2674.34; owner short_news_r_h3 |
