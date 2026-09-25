# Factor mine action — `combo_sf_5050_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared short_news_r_h3/flatten_h5 w=0.5,0.5 net=priority

Cash book **-3.76%** ($9,624) · signal-only (no cash/fees) was —. Starts YES **8/30**. Fills 222 · skips 449 · realized $+800.90.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: short_news_r_h3 50%, flatten_h5 50%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: short_news_r_h3 50%, flatten_h5 50%.
- Member: short_news_r_h3 (50% · short · hold 3).
- Member: flatten_h5 (50% · long · hold 5).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $-496.20.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; combo leftover $1250.00; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+12.3; combo leftover $1250.00; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+6.2; combo leftover $1250.00; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; combo leftover $1250.00; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; combo leftover $1250.00; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; combo leftover $1250.00; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.2; combo leftover $1250.00; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+19.7; combo leftover $1250.00; owner flatten_h5 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.53 | ▲ close $10,153.12 vs 09:30 $10,000.00 (session +185.07) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 6 | $0.94 | $0.07 | — | $91.84 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.5; combo leftover $6.10; owner flatten_h5 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 4 | $1.50 | $0.07 | — | $85.77 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; combo leftover $6.10; owner flatten_h5 | — |
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 24 | $1.18 | $0.38 | — | $113.71 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $28.59; owner short_news_r_h3 | — |
| 2026-08-14 09:30 ET | **SHORT** | `LUNR` | 1 | $19.17 | $0.22 | — | $132.67 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+17.6; combo leftover $28.59; owner short_news_r_h3 | — |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 2 | $12.70 | $0.28 | — | $157.77 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.6; combo leftover $28.59; owner short_news_r_h3 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $157.77 | ▲ close $10,435.00 vs 09:30 $10,178.12 (session +257.89) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $157.77 | ▼ 09:30 equity $10,413.43 vs yday $10,435.00 (-21.57) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 2 | $4.05 | $0.09 | — | $149.59 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; combo leftover $9.86; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 1 | $8.46 | $0.09 | — | $141.04 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.4; combo leftover $9.86; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 3 | $3.24 | $0.11 | — | $131.21 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; combo leftover $9.86; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 2 | $4.81 | $0.10 | — | $121.49 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-11.4; combo leftover $9.86; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 21 | $1.15 | $0.33 | — | $145.32 | — | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; combo leftover $24.30; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 6 | $3.56 | $0.25 | — | $166.42 | — | news🔴; gate news=bad; list yday_mover; ret5=-15.6; combo leftover $24.30; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 8 | $3.01 | $0.29 | — | $190.22 | — | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; combo leftover $24.30; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `RNW` | 3 | $6.80 | $0.23 | — | $210.39 | — | news🔴; gate news=bad; list overnight; ⚪; ret5=+10.4; combo leftover $24.30; owner short_news_r_h3 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $210.39 | ▲ close $10,524.43 vs 09:30 $10,413.43 (session +112.47) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $210.39 | ▼ 09:30 equity $10,393.96 vs yday $10,524.43 (-130.47) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $210.39 | ▲ close $10,579.89 vs 09:30 $10,393.96 (session +185.94) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $210.39 | ▲ 09:30 equity $10,717.46 vs yday $10,579.89 (+137.57) | — | — |
| 2026-08-19 09:30 ET | **COVER** | `EU` | 24 | $1.07 | $0.33 | $+1.94 | $184.38 | ▲ +1.94 after sell → book $10,717.13; vs 09:30 mark -0.33 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 2 | $11.75 | $0.24 | $+1.37 | $160.64 | ▲ +1.37 after sell → book $10,716.89; vs 09:30 mark -0.24 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $160.64 | ▲ close $11,037.93 vs 09:30 $10,717.46 (session +321.04) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $160.64 | ▼ 09:30 equity $10,970.36 vs yday $11,037.93 (-67.57) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 20 | $58.64 | $2.07 | $-27.32 | $1,331.37 | ▼ -27.32 after sell → book $10,968.29; vs 09:30 mark -2.07 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 27 | $42.46 | $2.09 | $-99.20 | $2,475.69 | ▼ -99.20 after sell → book $10,966.20; vs 09:30 mark -2.09 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 24 | $53.06 | $2.08 | $+54.34 | $3,747.05 | ▲ +54.34 after sell → book $10,964.12; vs 09:30 mark -2.08 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGTX` | 25 | $51.65 | $2.09 | $+44.60 | $5,036.22 | ▲ +44.60 after sell → book $10,962.03; vs 09:30 mark -2.09 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 106 | $13.84 | $2.34 | $+222.19 | $6,500.92 | ▲ +222.19 after sell → book $10,959.69; vs 09:30 mark -2.34 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `HIMS` | 42 | $30.66 | $2.14 | $+34.39 | $7,786.50 | ▲ +34.39 after sell → book $10,957.56; vs 09:30 mark -2.13 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `INO` | 1543 | $1.30 | $20.18 | $+718.77 | $9,772.23 | ▲ +718.77 after sell → book $10,937.38; vs 09:30 mark -20.18 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 53 | $23.11 | $2.17 | $-15.98 | $10,994.89 | ▼ -15.98 after sell → book $10,935.21; vs 09:30 mark -2.17 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **COVER** | `LUNR` | 1 | $18.13 | $0.18 | $+0.64 | $10,976.57 | ▲ +0.64 after sell → book $10,935.03; vs 09:30 mark -0.18 | short_news_r_h3: dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 21 | $0.96 | $0.27 | $+3.34 | $10,956.09 | ▲ +3.34 after sell → book $10,934.76; vs 09:30 mark -0.27 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ZNTL` | 6 | $4.01 | $0.26 | $-3.24 | $10,931.74 | ▼ -3.24 after sell → book $10,934.50; vs 09:30 mark -0.26 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 8 | $2.95 | $0.26 | $-0.07 | $10,907.88 | ▼ -0.07 after sell → book $10,934.24; vs 09:30 mark -0.26 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `RNW` | 3 | $6.81 | $0.21 | $-0.48 | $10,887.23 | ▼ -0.48 after sell → book $10,934.03; vs 09:30 mark -0.21 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 33 | $20.55 | $2.09 | — | $10,206.99 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; combo leftover $680.45; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 7 | $91.01 | $2.01 | — | $9,567.91 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; combo leftover $680.45; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 32 | $20.65 | $2.09 | — | $8,905.03 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; combo leftover $680.45; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 117 | $5.77 | $2.34 | — | $8,227.60 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; combo leftover $680.45; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 34 | $19.63 | $2.09 | — | $7,558.08 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; combo leftover $680.45; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 22 | $29.63 | $2.06 | — | $6,904.17 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; combo leftover $680.45; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 388 | $1.75 | $5.01 | — | $6,220.16 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; combo leftover $680.45; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 4 | $144.54 | $2.00 | — | $5,640.00 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; combo leftover $680.45; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 3 | $204.45 | $2.04 | — | $6,251.32 | — | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; combo leftover $682.15; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 31 | $21.40 | $2.12 | — | $6,912.59 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-25.2; combo leftover $682.15; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TOYO` | 153 | $4.43 | $2.50 | — | $7,587.88 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-23.1; combo leftover $682.15; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 57 | $11.81 | $2.20 | — | $8,259.14 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $682.15; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 3 | $173.90 | $2.03 | — | $8,778.80 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; combo leftover $682.15; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AAP` | 14 | $46.85 | $2.07 | — | $9,432.63 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=+5.0; combo leftover $682.15; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 6 | $106.38 | $2.05 | — | $10,068.87 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; combo leftover $682.15; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 147 | $4.61 | $2.48 | — | $10,744.05 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $682.15; owner short_news_r_h3 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,744.05 | ▲ close $11,097.61 vs 09:30 $10,970.36 (session +200.76) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,744.05 | ▲ 09:30 equity $11,187.61 vs yday $11,097.61 (+90.00) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 6 | $0.87 | $0.09 | $-0.58 | $10,749.17 | ▼ -0.58 after sell → book $11,187.52; vs 09:30 mark -0.09 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 4 | $1.66 | $0.10 | $+0.47 | $10,755.71 | ▲ +0.47 after sell → book $11,187.42; vs 09:30 mark -0.10 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 6 | $119.43 | $2.01 | — | $10,037.12 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+21.1; combo leftover $768.26; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 44 | $17.20 | $2.12 | — | $9,278.20 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; combo leftover $768.26; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 69 | $11.13 | $2.20 | — | $8,508.03 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; combo leftover $768.26; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 311 | $2.47 | $4.01 | — | $7,735.85 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; combo leftover $768.26; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 398 | $1.93 | $5.13 | — | $6,962.57 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; combo leftover $768.26; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 12 | $59.72 | $2.03 | — | $6,243.91 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.6; combo leftover $768.26; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 582 | $1.32 | $7.51 | — | $5,468.16 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; combo leftover $768.26; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 293 | $3.11 | $3.86 | — | $6,375.53 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; combo leftover $911.36; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `MRNA` | 6 | $133.11 | $2.05 | — | $7,172.14 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=+109.5; combo leftover $911.36; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 10 | $89.10 | $2.06 | — | $8,061.07 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $911.36; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 23 | $38.40 | $2.10 | — | $8,942.17 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $911.36; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 43 | $20.90 | $2.16 | — | $9,838.71 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $911.36; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 33 | $27.00 | $2.13 | — | $10,727.57 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; combo leftover $911.36; owner short_news_r_h3 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,727.57 | ▲ close $11,295.26 vs 09:30 $11,187.61 (session +147.23) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,727.57 | ▲ 09:30 equity $11,630.58 vs yday $11,295.26 (+335.32) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 2 | $4.62 | $0.12 | $+0.94 | $10,736.70 | ▲ +0.94 after sell → book $11,630.46; vs 09:30 mark -0.12 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `TGB` | 1 | $9.26 | $0.12 | $+0.60 | $10,745.85 | ▲ +0.60 after sell → book $11,630.35; vs 09:30 mark -0.11 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 3 | $3.50 | $0.13 | $+0.54 | $10,756.21 | ▲ +0.54 after sell → book $11,630.21; vs 09:30 mark -0.14 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `HNST` | 2 | $5.05 | $0.13 | $+0.25 | $10,766.19 | ▲ +0.25 after sell → book $11,630.09; vs 09:30 mark -0.12 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,766.19 | ▼ close $11,626.12 vs 09:30 $11,630.58 (session -3.97) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,766.19 | ▼ 09:30 equity $11,526.49 vs yday $11,626.12 (-99.63) | — | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 3 | $212.00 | $2.00 | $-26.68 | $10,128.19 | ▼ -26.68 after sell → book $11,524.49; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 31 | $20.90 | $2.08 | $+11.30 | $9,478.21 | ▲ +11.30 after sell → book $11,522.41; vs 09:30 mark -2.08 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TOYO` | 153 | $4.42 | $2.45 | $-3.42 | $8,799.50 | ▼ -3.42 after sell → book $11,519.96; vs 09:30 mark -2.45 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ABCL` | 57 | $11.00 | $2.16 | $+42.09 | $8,170.34 | ▲ +42.09 after sell → book $11,517.80; vs 09:30 mark -2.16 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 3 | $170.64 | $2.00 | $+5.75 | $7,656.42 | ▲ +5.75 after sell → book $11,515.80; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AAP` | 14 | $43.63 | $2.03 | $+40.98 | $7,043.56 | ▲ +40.98 after sell → book $11,513.76; vs 09:30 mark -2.04 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 6 | $105.58 | $2.01 | $+0.75 | $6,408.08 | ▲ +0.75 after sell → book $11,511.76; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 147 | $4.77 | $2.43 | $-28.43 | $5,704.46 | ▼ -28.43 after sell → book $11,509.33; vs 09:30 mark -2.43 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 19 | $23.77 | $2.05 | — | $5,250.78 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.0; combo leftover $475.37; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 43 | $10.98 | $2.12 | — | $4,776.52 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+1.2; combo leftover $475.37; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 7 | $61.19 | $2.01 | — | $4,346.18 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+7.4; combo leftover $475.37; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 56 | $8.35 | $2.16 | — | $3,876.42 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.0; combo leftover $475.37; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 96 | $4.94 | $2.28 | — | $3,399.90 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; combo leftover $475.37; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 1 | $426.97 | $1.99 | — | $2,970.94 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.0; combo leftover $475.37; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 54 | $13.62 | $2.19 | — | $3,704.50 | — | news🔴; gate news=bad; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; combo leftover $742.73; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 13 | $54.51 | $2.07 | — | $4,411.06 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+15.1; combo leftover $742.73; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `BMO` | 4 | $175.01 | $2.04 | — | $5,109.06 | — | news🔴; gate news=bad; list earn_react; ret5=-7.0; combo leftover $742.73; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `INTU` | 2 | $364.35 | $2.04 | — | $5,835.72 | — | news🔴; gate news=bad; list overnight,overnight_mega; 🔵; ret5=+10.2; combo leftover $742.73; owner short_news_r_h3 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,835.72 | ▲ close $11,833.54 vs 09:30 $11,526.49 (session +345.16) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,835.72 | ▲ 09:30 equity $11,834.38 vs yday $11,833.54 (+0.84) | — | — |
| 2026-08-26 09:30 ET | **COVER** | `QTRX` | 293 | $2.83 | $3.78 | $+74.40 | $5,002.75 | ▲ +74.40 after sell → book $11,830.60; vs 09:30 mark -3.78 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `MRNA` | 6 | $154.20 | $2.01 | $-130.60 | $4,075.54 | ▼ -130.60 after sell → book $11,828.59; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 10 | $88.24 | $2.02 | $+4.52 | $3,191.12 | ▲ +4.52 after sell → book $11,826.57; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 23 | $38.41 | $2.06 | $-4.39 | $2,305.64 | ▼ -4.39 after sell → book $11,824.52; vs 09:30 mark -2.05 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 43 | $20.50 | $2.12 | $+12.92 | $1,422.02 | ▲ +12.92 after sell → book $11,822.40; vs 09:30 mark -2.12 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 33 | $26.00 | $2.09 | $+28.78 | $561.93 | ▲ +28.78 after sell → book $11,820.31; vs 09:30 mark -2.09 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 9 | $12.22 | $1.15 | — | $670.76 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.4; combo leftover $112.39; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 22 | $5.08 | $1.21 | — | $781.31 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+17.6; combo leftover $112.39; owner short_news_r_h3 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $781.31 | ▼ close $11,721.84 vs 09:30 $11,834.38 (session -96.11) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $781.31 | ▲ 09:30 equity $11,756.95 vs yday $11,721.84 (+35.11) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 33 | $20.93 | $2.11 | $+8.34 | $1,469.89 | ▲ +8.34 after sell → book $11,754.84; vs 09:30 mark -2.11 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 7 | $95.52 | $2.03 | $+27.53 | $2,136.50 | ▲ +27.53 after sell → book $11,752.81; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 32 | $21.31 | $2.11 | $+16.93 | $2,816.31 | ▲ +16.93 after sell → book $11,750.70; vs 09:30 mark -2.11 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 117 | $5.49 | $2.37 | $-37.47 | $3,456.27 | ▼ -37.47 after sell → book $11,748.33; vs 09:30 mark -2.37 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 34 | $21.47 | $2.11 | $+58.36 | $4,184.14 | ▲ +58.36 after sell → book $11,746.22; vs 09:30 mark -2.11 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 22 | $32.32 | $2.08 | $+55.05 | $4,893.11 | ▲ +55.05 after sell → book $11,744.15; vs 09:30 mark -2.07 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 388 | $1.91 | $5.08 | $+52.00 | $5,629.11 | ▲ +52.00 after sell → book $11,739.07; vs 09:30 mark -5.08 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 4 | $155.89 | $2.02 | $+41.38 | $6,250.64 | ▲ +41.38 after sell → book $11,737.04; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 25 | $41.44 | $2.06 | — | $5,212.58 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.1; combo leftover $1041.77; owner flatten_h5 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 72 | $14.42 | $2.21 | — | $4,172.13 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; combo leftover $1041.77; owner flatten_h5 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 400 | $2.60 | $5.16 | — | $3,126.97 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+13.0; combo leftover $1041.77; owner flatten_h5 | — |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 20 | $74.54 | $2.11 | — | $4,615.66 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=-0.1; combo leftover $1563.49; owner short_news_r_h3 | — |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 28 | $55.25 | $2.14 | — | $6,160.52 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=+2.1; combo leftover $1563.49; owner short_news_r_h3 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,160.52 | ▲ close $11,841.27 vs 09:30 $11,756.95 (session +117.91) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,160.52 | ▼ 09:30 equity $11,736.43 vs yday $11,841.27 (-104.84) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 6 | $119.19 | $2.03 | $-5.48 | $6,873.63 | ▼ -5.48 after sell → book $11,734.40; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 44 | $16.44 | $2.14 | $-37.70 | $7,594.85 | ▼ -37.70 after sell → book $11,732.26; vs 09:30 mark -2.14 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 69 | $15.43 | $2.22 | $+292.28 | $8,657.30 | ▲ +292.28 after sell → book $11,730.04; vs 09:30 mark -2.22 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 311 | $2.35 | $4.07 | $-45.41 | $9,384.08 | ▼ -45.41 after sell → book $11,725.97; vs 09:30 mark -4.07 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 398 | $2.06 | $5.21 | $+41.40 | $10,198.75 | ▲ +41.40 after sell → book $11,720.75; vs 09:30 mark -5.22 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRSP` | 12 | $58.22 | $2.05 | $-22.07 | $10,895.34 | ▼ -22.07 after sell → book $11,718.71; vs 09:30 mark -2.04 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 582 | $1.82 | $7.61 | $+275.88 | $11,946.97 | ▲ +275.88 after sell → book $11,711.09; vs 09:30 mark -7.62 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 54 | $13.90 | $2.15 | $-19.19 | $11,194.22 | ▼ -19.19 after sell → book $11,708.94; vs 09:30 mark -2.15 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 13 | $52.49 | $2.03 | $+22.16 | $10,509.82 | ▲ +22.16 after sell → book $11,706.91; vs 09:30 mark -2.03 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `BMO` | 4 | $172.76 | $2.00 | $+4.96 | $9,816.78 | ▲ +4.96 after sell → book $11,704.91; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `INTU` | 2 | $347.82 | $2.00 | $+29.03 | $9,119.14 | ▲ +29.03 after sell → book $11,702.92; vs 09:30 mark -1.99 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 11 | $252.24 | $2.13 | — | $11,891.65 | — | news🔴; gate news=bad; list probable,yday_gainer; ⚪; ret5=+2.2; combo leftover $2925.73; owner short_news_r_h3 | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 96 | $30.18 | $2.40 | — | $14,786.53 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+12.1; combo leftover $2925.73; owner short_news_r_h3 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,786.53 | ▲ close $11,815.47 vs 09:30 $11,736.43 (session +117.09) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,786.53 | ▲ 09:30 equity $11,928.22 vs yday $11,815.47 (+112.75) | — | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 9 | $11.10 | $1.03 | $+7.90 | $14,685.61 | ▲ +7.90 after sell → book $11,927.20; vs 09:30 mark -1.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 22 | $4.97 | $1.16 | $-0.06 | $14,575.00 | ▼ -0.06 after sell → book $11,926.04; vs 09:30 mark -1.16 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,575.00 | ▲ close $12,011.25 vs 09:30 $11,928.22 (session +85.21) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,575.00 | ▲ 09:30 equity $12,263.90 vs yday $12,011.25 (+252.65) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `MOS` | 19 | $23.94 | $2.07 | $-0.88 | $15,027.79 | ▼ -0.88 after sell → book $12,261.83; vs 09:30 mark -2.07 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `OCUL` | 43 | $10.42 | $2.14 | $-28.34 | $15,473.71 | ▼ -28.34 after sell → book $12,259.69; vs 09:30 mark -2.14 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `INSP` | 7 | $63.00 | $2.03 | $+8.63 | $15,912.68 | ▲ +8.63 after sell → book $12,257.66; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 56 | $8.25 | $2.18 | $-9.94 | $16,372.50 | ▼ -9.94 after sell → book $12,255.48; vs 09:30 mark -2.18 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 96 | $4.64 | $2.30 | $-33.38 | $16,815.64 | ▼ -33.38 after sell → book $12,253.18; vs 09:30 mark -2.30 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `HCA` | 1 | $418.43 | $2.01 | $-12.55 | $17,232.05 | ▼ -12.55 after sell → book $12,251.16; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **COVER** | `MT` | 20 | $73.22 | $2.05 | $+22.24 | $15,765.60 | ▲ +22.24 after sell → book $12,249.11; vs 09:30 mark -2.05 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `TX` | 28 | $54.76 | $2.07 | $+9.51 | $14,230.25 | ▲ +9.51 after sell → book $12,247.04; vs 09:30 mark -2.07 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,230.25 | ▼ close $12,217.64 vs 09:30 $12,263.90 (session -29.40) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,230.25 | ▲ 09:30 equity $12,245.46 vs yday $12,217.64 (+27.82) | — | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 11 | $235.71 | $2.02 | $+177.68 | $11,635.42 | ▲ +177.68 after sell → book $12,243.44; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 96 | $26.78 | $2.28 | $+321.73 | $9,062.26 | ▲ +321.73 after sell → book $12,241.16; vs 09:30 mark -2.28 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,062.26 | ▼ close $12,207.14 vs 09:30 $12,245.46 (session -34.02) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,062.26 | ▲ 09:30 equity $12,231.41 vs yday $12,207.14 (+24.27) | — | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 25 | $42.43 | $2.08 | $+20.60 | $10,120.92 | ▲ +20.60 after sell → book $12,229.32; vs 09:30 mark -2.09 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `CRK` | 72 | $15.45 | $2.23 | $+69.73 | $11,231.10 | ▲ +69.73 after sell → book $12,227.10; vs 09:30 mark -2.22 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 400 | $2.49 | $5.24 | $-54.40 | $12,221.86 | ▼ -54.40 after sell → book $12,221.86; vs 09:30 mark -5.24 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 23 | $52.88 | $2.06 | — | $11,003.56 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; combo leftover $1222.19; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 28 | $42.93 | $2.07 | — | $9,799.45 | — | baseline list, no extra gate; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.1; combo leftover $1222.19; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 336 | $3.63 | $4.33 | — | $8,575.43 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.8; combo leftover $1222.19; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 152 | $8.03 | $2.45 | — | $7,352.43 | — | baseline list, no extra gate; list flatten,ohlc_hot,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.5; combo leftover $1222.19; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $6,158.36 | — | baseline list, no extra gate; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.6; combo leftover $1222.19; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 205 | $14.85 | $2.79 | — | $9,199.82 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; combo leftover $3052.23; owner short_news_r_h3 | — |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 1784 | $1.71 | $23.42 | — | $12,227.05 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+11.9; combo leftover $3052.23; owner short_news_r_h3 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,227.05 | ▲ close $12,259.43 vs 09:30 $12,231.41 (session +76.70) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,227.05 | ▲ 09:30 equity $12,285.18 vs yday $12,259.43 (+25.75) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 485 | $2.52 | $6.26 | — | $10,998.59 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+5.0; combo leftover $1222.70; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 182 | $6.71 | $2.54 | — | $9,774.83 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; combo leftover $1222.70; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 643 | $1.90 | $8.29 | — | $8,544.84 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.7; combo leftover $1222.70; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 255 | $4.78 | $3.29 | — | $7,322.65 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; combo leftover $1222.70; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 108 | $11.31 | $2.31 | — | $6,098.85 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+1.2; combo leftover $1222.70; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 652 | $4.67 | $8.63 | — | $9,135.07 | — | news🔴; gate news=bad; list yday_gainer; ret5=+11.9; combo leftover $3049.43; owner short_news_r_h3 | — |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 39 | $76.55 | $2.22 | — | $12,118.30 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; combo leftover $3049.43; owner short_news_r_h3 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,118.30 | ▼ close $12,130.49 vs 09:30 $12,285.18 (session -121.15) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,118.30 | ▲ 09:30 equity $12,135.63 vs yday $12,130.49 (+5.14) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12,118.30 | ▲ close $12,294.18 vs 09:30 $12,135.63 (session +158.55) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12,118.30 | ▼ 09:30 equity $12,275.07 vs yday $12,294.18 (-19.11) | — | — |
| 2026-09-09 09:30 ET | **COVER** | `SLN` | 205 | $13.60 | $2.64 | $+250.82 | $9,327.65 | ▲ +250.82 after sell → book $12,272.42; vs 09:30 mark -2.65 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 1784 | $1.58 | $23.01 | $+185.49 | $6,485.92 | ▲ +185.49 after sell → book $12,249.41; vs 09:30 mark -23.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,485.92 | ▼ close $11,841.05 vs 09:30 $12,275.07 (session -408.36) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,485.92 | ▼ 09:30 equity $11,778.73 vs yday $11,841.05 (-62.32) | — | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 652 | $4.36 | $8.41 | $+185.08 | $3,634.79 | ▲ +185.08 after sell → book $11,770.32; vs 09:30 mark -8.41 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PIPR` | 39 | $76.79 | $2.11 | $-13.69 | $637.87 | ▼ -13.69 after sell → book $11,768.21; vs 09:30 mark -2.11 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $637.87 | ▼ close $11,559.72 vs 09:30 $11,778.73 (session -208.49) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $637.87 | ▲ 09:30 equity $11,697.39 vs yday $11,559.72 (+137.67) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `ATRC` | 23 | $53.53 | $2.08 | $+10.81 | $1,866.98 | ▲ +10.81 after sell → book $11,695.31; vs 09:30 mark -2.08 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `HRMY` | 28 | $41.30 | $2.09 | $-49.81 | $3,021.29 | ▼ -49.81 after sell → book $11,693.22; vs 09:30 mark -2.09 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CABA` | 336 | $2.77 | $4.40 | $-297.69 | $3,947.61 | ▼ -297.69 after sell → book $11,688.82; vs 09:30 mark -4.40 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `VSTM` | 152 | $7.70 | $2.48 | $-55.09 | $5,115.53 | ▼ -55.09 after sell → book $11,686.34; vs 09:30 mark -2.48 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `RVTY` | 9 | $122.40 | $2.04 | $-94.50 | $6,215.09 | ▼ -94.50 after sell → book $11,684.30; vs 09:30 mark -2.04 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 31 | $16.28 | $2.08 | — | $5,708.33 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-1.1; combo leftover $517.92; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 189 | $2.73 | $2.56 | — | $5,189.80 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-3.0; combo leftover $517.92; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 2 | $206.84 | $2.00 | — | $4,774.12 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+8.3; combo leftover $517.92; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 3 | $164.43 | $2.00 | — | $4,278.83 | — | baseline list, no extra gate; list flatten,earn_react; wish-list (live io HOLD — not a ticket); ⚪; ret5=+4.9; combo leftover $517.92; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 3 | $157.78 | $2.00 | — | $3,803.49 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+4.7; combo leftover $517.92; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 9 | $56.09 | $2.02 | — | $3,296.67 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+19.6; combo leftover $517.92; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 5 | $112.83 | $2.04 | — | $3,858.80 | — | news🔴; gate news=bad; list yday_gainer,ohlc_hot; ret5=+11.7; combo leftover $659.33; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `RWT` | 187 | $3.52 | $2.61 | — | $4,514.43 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-19.2; combo leftover $659.33; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `CRDL` | 324 | $2.03 | $4.26 | — | $5,167.89 | — | news🔴; gate news=bad; list yday_mover; ret5=-8.8; combo leftover $659.33; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BKV` | 26 | $24.97 | $2.11 | — | $5,815.00 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.8; combo leftover $659.33; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `MYGN` | 195 | $3.37 | $2.64 | — | $6,469.52 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+4.0; combo leftover $659.33; owner short_news_r_h3 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,469.52 | ▼ close $11,578.42 vs 09:30 $11,697.39 (session -79.58) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,469.52 | ▼ 09:30 equity $11,509.43 vs yday $11,578.42 (-68.99) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ALEC` | 485 | $2.15 | $6.35 | $-192.05 | $7,505.92 | ▼ -192.05 after sell → book $11,503.08; vs 09:30 mark -6.35 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BHC` | 182 | $5.93 | $2.58 | $-147.07 | $8,582.61 | ▼ -147.07 after sell → book $11,500.51; vs 09:30 mark -2.57 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BMEA` | 643 | $1.72 | $8.41 | $-135.66 | $9,676.94 | ▼ -135.66 after sell → book $11,492.09; vs 09:30 mark -8.42 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OABI` | 255 | $4.13 | $3.34 | $-172.38 | $10,726.75 | ▼ -172.38 after sell → book $11,488.75; vs 09:30 mark -3.34 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIR` | 108 | $10.73 | $2.34 | $-67.30 | $11,883.25 | ▼ -67.30 after sell → book $11,486.41; vs 09:30 mark -2.34 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,883.25 | ▼ close $11,366.68 vs 09:30 $11,509.43 (session -119.73) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,883.25 | ▲ 09:30 equity $11,386.94 vs yday $11,366.68 (+20.26) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,883.25 | ▼ close $11,279.30 vs 09:30 $11,386.94 (session -107.64) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,883.25 | ▲ 09:30 equity $11,312.31 vs yday $11,279.30 (+33.01) | — | — |
| 2026-09-16 09:30 ET | **COVER** | `QRVO` | 5 | $118.18 | $2.00 | $-30.77 | $11,290.34 | ▼ -30.77 after sell → book $11,310.30; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `RWT` | 187 | $3.98 | $2.55 | $-91.18 | $10,543.53 | ▼ -91.18 after sell → book $11,307.75; vs 09:30 mark -2.55 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `CRDL` | 324 | $1.85 | $4.18 | $+49.88 | $9,939.95 | ▲ +49.88 after sell → book $11,303.57; vs 09:30 mark -4.18 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `BKV` | 26 | $24.42 | $2.07 | $+10.13 | $9,302.96 | ▲ +10.13 after sell → book $11,301.50; vs 09:30 mark -2.07 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `MYGN` | 195 | $3.75 | $2.58 | $-79.31 | $8,569.14 | ▼ -79.31 after sell → book $11,298.93; vs 09:30 mark -2.57 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 3 | $270.89 | $2.00 | — | $7,754.47 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.0; combo leftover $1071.14; owner flatten_h5 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 13 | $77.12 | $2.03 | — | $6,749.88 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+7.2; combo leftover $1071.14; owner flatten_h5 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 74 | $14.31 | $2.21 | — | $5,688.73 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.8; combo leftover $1071.14; owner flatten_h5 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 29 | $36.46 | $2.08 | — | $4,629.31 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+2.9; combo leftover $1071.14; owner flatten_h5 | — |
| 2026-09-16 09:30 ET | **SHORT** | `BBNX` | 124 | $18.61 | $2.46 | — | $6,934.49 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; ret5=-0.5; combo leftover $2314.66; owner short_news_r_h3 | — |
| 2026-09-16 09:30 ET | **SHORT** | `GFR` | 338 | $6.83 | $4.50 | — | $9,238.53 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+11.2; combo leftover $2314.66; owner short_news_r_h3 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,238.53 | ▼ close $10,944.37 vs 09:30 $11,312.31 (session -339.28) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,238.53 | ▲ 09:30 equity $11,022.72 vs yday $10,944.37 (+78.35) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 3 | $233.85 | $2.00 | — | $8,534.98 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+11.7; combo leftover $769.88; owner flatten_h5 | — |
| 2026-09-17 09:30 ET | **BUY** | `TWST` | 5 | $151.43 | $2.00 | — | $7,775.82 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.0; combo leftover $769.88; owner flatten_h5 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 5 | $147.61 | $2.00 | — | $7,035.77 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+17.7; combo leftover $769.88; owner flatten_h5 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 75 | $10.25 | $2.21 | — | $6,264.80 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+17.1; combo leftover $769.88; owner flatten_h5 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 101 | $7.59 | $2.29 | — | $5,495.92 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+9.4; combo leftover $769.88; owner flatten_h5 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 22 | $34.93 | $2.06 | — | $4,725.40 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.6; combo leftover $769.88; owner flatten_h5 | — |
| 2026-09-17 09:30 ET | **SHORT** | `BULL` | 297 | $7.95 | $3.96 | — | $7,082.59 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-18.4; combo leftover $2362.70; owner short_news_r_h3 | — |
| 2026-09-17 09:30 ET | **SHORT** | `LEN` | 29 | $81.00 | $2.17 | — | $9,429.42 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-3.0; combo leftover $2362.70; owner short_news_r_h3 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,429.42 | ▲ close $11,224.14 vs 09:30 $11,022.72 (session +220.13) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,429.42 | ▲ 09:30 equity $11,288.02 vs yday $11,224.14 (+63.88) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AUPH` | 31 | $16.93 | $2.10 | $+15.96 | $9,952.15 | ▲ +15.96 after sell → book $11,285.92; vs 09:30 mark -2.10 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `OVID` | 189 | $2.68 | $2.60 | $-14.61 | $10,456.07 | ▼ -14.61 after sell → book $11,283.32; vs 09:30 mark -2.60 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `SANM` | 2 | $197.76 | $2.02 | $-22.17 | $10,849.57 | ▼ -22.17 after sell → book $11,281.30; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `ORCL` | 3 | $150.47 | $2.02 | $-45.90 | $11,298.96 | ▼ -45.90 after sell → book $11,279.28; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `NVT` | 3 | $152.71 | $2.02 | $-19.23 | $11,755.08 | ▼ -19.23 after sell → book $11,277.27; vs 09:30 mark -2.01 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `COHU` | 9 | $55.80 | $2.04 | $-6.66 | $12,255.24 | ▼ -6.66 after sell → book $11,275.23; vs 09:30 mark -2.04 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 59 | $34.44 | $2.17 | — | $10,221.11 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+14.0; combo leftover $2042.54; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 18 | $108.55 | $2.04 | — | $8,265.17 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+21.3; combo leftover $2042.54; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `DELL` | 3 | $593.15 | $2.00 | — | $6,483.72 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+16.1; combo leftover $2042.54; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 9 | $209.52 | $2.02 | — | $4,596.02 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.1; combo leftover $2042.54; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 9 | $219.62 | $2.02 | — | $2,617.42 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+21.5; combo leftover $2042.54; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 24 | $85.00 | $2.06 | — | $575.36 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+18.3; combo leftover $2042.54; owner flatten_h5 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $575.36 | ▼ close $10,836.29 vs 09:30 $11,288.02 (session -426.63) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $575.36 | ▲ 09:30 equity $10,923.65 vs yday $10,836.29 (+87.36) | — | — |
| 2026-09-21 09:30 ET | **COVER** | `BBNX` | 124 | $22.11 | $2.36 | $-438.83 | $-2,168.64 | ▼ -438.83 after sell → book $10,921.29; vs 09:30 mark -2.36 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `GFR` | 338 | $6.55 | $4.36 | $+85.78 | $-4,386.90 | ▲ +85.78 after sell → book $10,916.93; vs 09:30 mark -4.36 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $-4,386.90 | ▲ close $11,070.11 vs 09:30 $10,923.65 (session +153.18) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $-4,386.90 | ▼ 09:30 equity $11,045.40 vs yday $11,070.11 (-24.71) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `BULL` | 297 | $8.28 | $3.83 | $-104.32 | $-6,848.41 | ▼ -104.32 after sell → book $11,041.57; vs 09:30 mark -3.83 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $-6,848.41 | ▼ close $11,013.43 vs 09:30 $11,045.40 (session -28.14) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $-6,848.41 | ▲ 09:30 equity $11,362.41 vs yday $11,013.43 (+348.98) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `IQV` | 3 | $270.66 | $2.02 | $-4.71 | $-6,038.45 | ▼ -4.71 after sell → book $11,360.39; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `RDNT` | 13 | $73.61 | $2.05 | $-49.71 | $-5,083.56 | ▼ -49.71 after sell → book $11,358.34; vs 09:30 mark -2.05 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `AVAH` | 74 | $13.12 | $2.23 | $-92.51 | $-4,114.92 | ▼ -92.51 after sell → book $11,356.11; vs 09:30 mark -2.23 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `BLFS` | 29 | $38.04 | $2.10 | $+41.65 | $-3,013.86 | ▲ +41.65 after sell → book $11,354.01; vs 09:30 mark -2.10 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **COVER** | `LEN` | 29 | $82.00 | $2.08 | $-33.25 | $-5,393.93 | ▼ -33.25 after sell → book $11,351.93; vs 09:30 mark -2.08 | short_news_r_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $-5,393.93 | ▼ close $11,305.78 vs 09:30 $11,362.41 (session -46.16) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $-5,393.93 | ▼ 09:30 equity $11,088.71 vs yday $11,305.78 (-217.07) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `ILMN` | 3 | $253.79 | $2.02 | $+55.80 | $-4,634.58 | ▲ +55.80 after sell → book $11,086.69; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `TWST` | 5 | $157.72 | $2.02 | $+27.42 | $-3,848.01 | ▲ +27.42 after sell → book $11,084.66; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `RVTY` | 5 | $141.79 | $2.02 | $-33.13 | $-3,141.08 | ▼ -33.13 after sell → book $11,082.64; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `IOVA` | 75 | $10.39 | $2.24 | $+6.05 | $-2,364.07 | ▲ +6.05 after sell → book $11,080.40; vs 09:30 mark -2.24 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 101 | $7.38 | $2.32 | $-25.82 | $-1,621.01 | ▼ -25.82 after sell → book $11,078.08; vs 09:30 mark -2.32 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `AMN` | 22 | $33.82 | $2.08 | $-28.55 | $-879.04 | ▼ -28.55 after sell → book $11,076.01; vs 09:30 mark -2.07 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $-879.04 | ▲ close $11,084.63 vs 09:30 $11,088.71 (session +8.62) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $-496.20 | ▲ 09:30 equity $9,623.58 vs yday $9,623.58 (+0.00) | 09:30 open · cash $-496.20 (unchanged overnight, no fees) · equity $9,623.58 vs prior close $9,623.58 (+0.00) | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $-496.20 | ▲ close $9,623.58 vs 09:30 $9,623.58 (session +0.00) | 16:00 close · cash $-496.20 · equity $9,623.58 vs 09:30 $9,623.58 (+0.00; session marks +0.00) · 6 name(s) marked open→close (per-name table). DELL×2 09:30 $536.02 → close $536.02 +0.00; ECO×20 09:30 $78.22 → close $78.22 +0.00; FIVN×51 09:30 $36.66 → close $36.66 -0.00; GNRC×8 09:30 $198.05 → close $198.05 +0.00; RBRK×16 09:30 $113.80 → close $113.80 +0.00; VICR×8 09:30 $276.06 → close $276.06 -0.00 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `IREN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `TPG` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `TGTX` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `SLS` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `HIMS` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `INO` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `TNDM` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `TLN` | cash | leftover split 6.10 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 6.10 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 6.10 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 6.10 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 6.10 < 1 share @ 57.61 |
| 2026-08-14 | `MARA` | cash | leftover split 6.10 < 1 share @ 9.01 |
| 2026-08-17 | `BTSG` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `IREN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `TPG` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `TGTX` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `SLS` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `HIMS` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `INO` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `TNDM` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `LDI` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-17 | `BTBT` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-17 | `EU` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `LUNR` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `OWL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `DVN` | cash | leftover split 9.86 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 9.86 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 9.86 < 1 share @ 202.70 |
| 2026-08-17 | `ELF` | cash | leftover split 9.86 < 1 share @ 90.54 |
| 2026-08-17 | `APMD` | cash | leftover split 24.30 < 1 share @ 31.70 |
| 2026-08-18 | `BTSG` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `IREN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `TPG` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `TGTX` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `SLS` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `HIMS` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `INO` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `TNDM` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `LDI` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-18 | `BTBT` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-18 | `EU` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `LUNR` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `OWL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `TMC` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `TGB` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `DNN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `HNST` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `VERI` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `ZNTL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `HIVE` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new short short_news_r_h3 |
| 2026-08-18 | `CADL` | hard_red | hard-red S=-6.20 sit; no new short short_news_r_h3 |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-19 | `BTSG` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `IREN` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `TPG` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `TGTX` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `SLS` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `HIMS` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `INO` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `TNDM` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `LDI` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-19 | `BTBT` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-19 | `TMC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-19 | `TGB` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-19 | `DNN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-19 | `HNST` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-19 | `VERI` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `ZNTL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `HIVE` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `RNW` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `AAP` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-20 | `LDI` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-20 | `BTBT` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-20 | `TMC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-20 | `TGB` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-20 | `DNN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-20 | `HNST` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-21 | `TMC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-21 | `TGB` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-21 | `DNN` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-21 | `HNST` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-21 | `AG` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-21 | `BHP` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-21 | `CDE` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-21 | `HDSN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-21 | `IAG` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-21 | `KGC` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-21 | `NFGC` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-21 | `WPM` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-21 | `AEM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `WYFI` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `TOYO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `TEAM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `AAP` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `WMT` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `AQST` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `AG` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-24 | `BHP` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-24 | `CDE` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-24 | `HDSN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-24 | `IAG` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-24 | `KGC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-24 | `NFGC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-24 | `WPM` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-24 | `AEM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `WYFI` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `TOYO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `TEAM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AAP` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `WMT` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AQST` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AU` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-24 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-24 | `ARCT` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-24 | `AUTL` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-24 | `CRDL` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-24 | `CRSP` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-24 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 1/5 |
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
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-25 | `AG` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `BHP` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `CDE` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `HDSN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `IAG` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `KGC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `NFGC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `WPM` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `AU` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-25 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-25 | `ARCT` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-25 | `AUTL` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-25 | `CRDL` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-25 | `CRSP` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-25 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-25 | `QTRX` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `MRNA` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `AUGO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `ARIS` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `NOG` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-26 | `AG` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `BHP` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `CDE` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `HDSN` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `IAG` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `KGC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `NFGC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `WPM` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `AU` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-26 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-26 | `ARCT` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-26 | `AUTL` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-26 | `CRDL` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-26 | `CRSP` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-26 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-26 | `AVAH` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `ARE` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `BMO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `INTU` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `BE` | cash | leftover split 112.39 < 1 share @ 213.94 |
| 2026-08-26 | `NEM` | cash | leftover split 112.39 < 1 share @ 132.64 |
| 2026-08-26 | `CRM` | cash | leftover split 112.39 < 1 share @ 199.94 |
| 2026-08-27 | `AU` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `ARCT` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `AUTL` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `CRDL` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `CRSP` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `OCUL` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-27 | `INSP` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-27 | `CRMD` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-27 | `RZLT` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-27 | `HCA` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-27 | `AVAH` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `ARE` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `BMO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `OCUL` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-28 | `INSP` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-28 | `CRMD` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-28 | `RZLT` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-28 | `HCA` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-28 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `AQST` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `MT` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `TX` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `MOS` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `OCUL` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `INSP` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `CRMD` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `RZLT` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `HCA` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `RRC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-31 | `CRK` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-31 | `SLI` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-31 | `MT` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `TX` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `SIMO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `FIG` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new long flatten_h5 |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new long flatten_h5 |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new long flatten_h5 |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new long flatten_h5 |
| 2026-09-01 | `RRC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-01 | `CRK` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-01 | `SLI` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-01 | `SIMO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `FIG` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-02 | `RRC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-02 | `CRK` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-02 | `SLI` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new short short_news_r_h3 |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new short short_news_r_h3 |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-04 | `HRMY` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-04 | `VSTM` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-04 | `RVTY` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-04 | `SLN` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `ATRC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-08 | `HRMY` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-08 | `CABA` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-08 | `VSTM` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-08 | `RVTY` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-08 | `SLN` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `OPK` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `ALEC` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-08 | `BHC` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-08 | `BMEA` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-08 | `OABI` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-08 | `VIR` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-08 | `PIPR` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `GEMI` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new long flatten_h5 |
| 2026-09-08 | `SID` | hard_red | hard-red S=-11.47 sit; no new long flatten_h5 |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new long flatten_h5 |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new long flatten_h5 |
| 2026-09-09 | `ATRC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-09 | `HRMY` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-09 | `CABA` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-09 | `VSTM` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-09 | `RVTY` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-09 | `ALEC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-09 | `BHC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-09 | `BMEA` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-09 | `OABI` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-09 | `VIR` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-09 | `GSM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `PIPR` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `NVS` | hard_red | hard-red S=-13.95 sit; no new short short_news_r_h3 |
| 2026-09-09 | `PCG` | hard_red | hard-red S=-13.95 sit; no new long flatten_h5 |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new long flatten_h5 |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new long flatten_h5 |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new long flatten_h5 |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new long flatten_h5 |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new long flatten_h5 |
| 2026-09-10 | `ATRC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-10 | `HRMY` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-10 | `CABA` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-10 | `VSTM` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-10 | `RVTY` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-10 | `ALEC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-10 | `BHC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-10 | `BMEA` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-10 | `OABI` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-10 | `VIR` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new short short_news_r_h3 |
| 2026-09-10 | `BKV` | hard_red | hard-red S=-13.28 sit; no new short short_news_r_h3 |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new long flatten_h5 |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new long flatten_h5 |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new long flatten_h5 |
| 2026-09-10 | `OIS` | hard_red | hard-red S=-13.28 sit; no new long flatten_h5 |
| 2026-09-11 | `ALEC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-11 | `BHC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-11 | `BMEA` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-11 | `OABI` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-11 | `VIR` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-14 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-14 | `OVID` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-14 | `SANM` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-14 | `ORCL` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-14 | `COHU` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-14 | `QRVO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `RWT` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `CRDL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new long flatten_h5 |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new long flatten_h5 |
| 2026-09-14 | `BG` | hard_red | hard-red S=-11.00 sit; no new long flatten_h5 |
| 2026-09-15 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `OVID` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `SANM` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `ORCL` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `NVT` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `COHU` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `QRVO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `RWT` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `CRDL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `MYGN` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `TYRA` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new long flatten_h5 |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new long flatten_h5 |
| 2026-09-15 | `WAY` | hard_red | hard-red S=-3.84 sit; no new long flatten_h5 |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new long flatten_h5 |
| 2026-09-16 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-16 | `OVID` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-16 | `SANM` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-16 | `ORCL` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-16 | `NVT` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-16 | `COHU` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-17 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-17 | `OVID` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-17 | `SANM` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-17 | `ORCL` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-17 | `NVT` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-17 | `COHU` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-17 | `IQV` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-17 | `RDNT` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-17 | `AVAH` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-17 | `BLFS` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-17 | `GFR` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `IQV` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-18 | `RDNT` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-18 | `AVAH` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-18 | `BLFS` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-18 | `BBNX` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `GFR` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `ILMN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-18 | `TWST` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-18 | `RVTY` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-18 | `IOVA` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-18 | `PGEN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-18 | `AMN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-18 | `BULL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `LEN` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-21 | `IQV` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-21 | `RDNT` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-21 | `AVAH` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-21 | `BLFS` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-21 | `ILMN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-21 | `TWST` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-21 | `RVTY` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-21 | `AMN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-21 | `BULL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `LEN` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `FIVN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `RBRK` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `DELL` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `GNRC` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `VICR` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `ECO` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-22 | `IQV` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-22 | `RDNT` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-22 | `AVAH` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-22 | `BLFS` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-22 | `ILMN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-22 | `TWST` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-22 | `RVTY` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-22 | `IOVA` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-22 | `PGEN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-22 | `AMN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-22 | `LEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FIVN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `RBRK` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `DELL` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `GNRC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `VICR` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `ECO` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-23 | `ILMN` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-23 | `TWST` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-23 | `RVTY` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-23 | `IOVA` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-23 | `AMN` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-23 | `FIVN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `RBRK` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `DELL` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `GNRC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `VICR` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `ECO` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-24 | `FIVN` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `RBRK` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `DELL` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `GNRC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `VICR` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `ECO` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `AEHL` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `PANW` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `VOYG` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `BMEA` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `CHKP` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `S` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `BAH` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `FIVN` | 59 | 2026-09-18 @ $34.44 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+14.0; combo leftover $2042.54; owner flatten_h5 |
| `RBRK` | 18 | 2026-09-18 @ $108.55 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+21.3; combo leftover $2042.54; owner flatten_h5 |
| `DELL` | 3 | 2026-09-18 @ $593.15 | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+16.1; combo leftover $2042.54; owner flatten_h5 |
| `GNRC` | 9 | 2026-09-18 @ $209.52 | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.1; combo leftover $2042.54; owner flatten_h5 |
| `VICR` | 9 | 2026-09-18 @ $219.62 | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+21.5; combo leftover $2042.54; owner flatten_h5 |
| `ECO` | 24 | 2026-09-18 @ $85.00 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+18.3; combo leftover $2042.54; owner flatten_h5 |
