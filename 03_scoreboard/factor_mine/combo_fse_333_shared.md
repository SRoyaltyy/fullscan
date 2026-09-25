# Factor mine action — `combo_fse_333_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared flatten_h5/short_news_r_h3/union_e_fresh_h3 w=0.33,0.33,0.33 net=priority

Cash book **-5.13%** ($9,487) · signal-only (no cash/fees) was —. Starts YES **15/30**. Fills 379 · skips 659 · realized $+1324.13.

## How this sleeve decides (like you are 10)

Imagine 3 kids at the same 09:30 school bell sharing one $10,000 book: flatten_h5 33%, short_news_r_h3 33%, union_e_fresh_h3 33%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: flatten_h5 33%, short_news_r_h3 33%, union_e_fresh_h3 33%.
- Member: flatten_h5 (33% · long · hold 5).
- Member: short_news_r_h3 (33% · short · hold 3).
- Member: union_e_fresh_h3 (33% · long · hold 3).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $3,956.90.

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
| 2026-08-13 09:30 ET | **BUY** | `INO` | 3086 | $0.81 | $34.25 | — | $7,466.09 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+13.2; combo leftover $2500.00; owner union_e_fresh_h3 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 113 | $22.01 | $2.33 | — | $4,976.63 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+0.3; combo leftover $2500.00; owner union_e_fresh_h3 | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 11 | $59.80 | $2.02 | — | $4,316.80 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; combo leftover $710.95; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 15 | $45.98 | $2.04 | — | $3,625.07 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+12.3; combo leftover $710.95; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 14 | $50.62 | $2.03 | — | $2,914.31 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+6.2; combo leftover $710.95; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 14 | $49.70 | $2.03 | — | $2,216.48 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; combo leftover $710.95; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 60 | $11.70 | $2.17 | — | $1,512.31 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; combo leftover $710.95; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 23 | $29.74 | $2.06 | — | $826.23 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; combo leftover $710.95; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 30 | $23.33 | $2.08 | — | $124.25 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+19.7; combo leftover $710.95; owner flatten_h5 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $124.25 | ▲ close $10,400.40 vs 09:30 $10,000.00 (session +451.42) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $124.25 | ▲ 09:30 equity $10,485.91 vs yday $10,400.40 (+85.51) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 3 | $1.50 | $0.05 | — | $119.70 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; 🔵; ⚪; ret5=+9.2; combo leftover $5.18; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `EU` | 4 | $1.18 | $0.06 | — | $114.92 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $5.18; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 8 | $0.94 | $0.10 | — | $107.32 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.5; combo leftover $8.21; owner flatten_h5 | — |
| 2026-08-14 09:30 ET | **SHORT** | `OWL` | 8 | $12.70 | $1.06 | — | $207.82 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.6; combo leftover $107.32; owner short_news_r_h3 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $207.82 | ▲ close $10,953.26 vs 09:30 $10,485.91 (session +468.62) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $207.82 | ▼ 09:30 equity $10,884.17 vs yday $10,953.26 (-69.09) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 3 | $4.05 | $0.13 | — | $195.54 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; combo leftover $12.99; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 1 | $8.46 | $0.09 | — | $186.99 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.4; combo leftover $12.99; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 4 | $3.24 | $0.14 | — | $173.89 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; combo leftover $12.99; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 2 | $4.81 | $0.10 | — | $164.17 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-11.4; combo leftover $12.99; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **SHORT** | `VERI` | 28 | $1.15 | $0.43 | — | $195.94 | — | news🔴; gate news=bad; list yday_mover; ⚪; ret5=-12.2; combo leftover $32.83; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `ZNTL` | 9 | $3.56 | $0.37 | — | $227.61 | — | news🔴; gate news=bad; list yday_mover; ret5=-15.6; combo leftover $32.83; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `APMD` | 1 | $31.70 | $0.34 | — | $258.97 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+17.6; combo leftover $32.83; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `HIVE` | 10 | $3.01 | $0.35 | — | $288.72 | — | news🔴; gate news=bad; list earn_react; ⚪; ret5=-5.3; combo leftover $32.83; owner short_news_r_h3 | — |
| 2026-08-17 09:30 ET | **SHORT** | `RNW` | 4 | $6.80 | $0.30 | — | $315.62 | — | news🔴; gate news=bad; list overnight; ⚪; ret5=+10.4; combo leftover $32.83; owner short_news_r_h3 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $315.62 | ▲ close $11,133.73 vs 09:30 $10,884.17 (session +251.81) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $315.62 | ▼ 09:30 equity $11,017.72 vs yday $11,133.73 (-116.01) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 3086 | $1.14 | $40.35 | $+943.78 | $3,793.31 | ▲ +943.78 after sell → book $10,977.37; vs 09:30 mark -40.35 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 113 | $22.82 | $2.37 | $+86.83 | $6,369.60 | ▲ +86.83 after sell → book $10,975.00; vs 09:30 mark -2.37 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,369.60 | ▲ close $11,031.01 vs 09:30 $11,017.72 (session +56.01) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,369.60 | ▲ 09:30 equity $11,089.34 vs yday $11,031.01 (+58.33) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 3 | $1.42 | $0.07 | $-0.37 | $6,373.79 | ▼ -0.37 after sell → book $11,089.27; vs 09:30 mark -0.07 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `EU` | 4 | $1.07 | $0.07 | $-0.57 | $6,377.99 | ▼ -0.57 after sell → book $11,089.20; vs 09:30 mark -0.07 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OWL` | 8 | $11.75 | $0.96 | $+5.53 | $6,283.03 | ▲ +5.53 after sell → book $11,088.23; vs 09:30 mark -0.97 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,283.03 | ▲ close $11,197.76 vs 09:30 $11,089.34 (session +109.53) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,283.03 | ▼ 09:30 equity $11,157.92 vs yday $11,197.76 (-39.84) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 11 | $58.64 | $2.04 | $-16.83 | $6,926.02 | ▼ -16.83 after sell → book $11,155.87; vs 09:30 mark -2.05 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 15 | $42.46 | $2.06 | $-56.89 | $7,560.87 | ▼ -56.89 after sell → book $11,153.82; vs 09:30 mark -2.05 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 14 | $53.06 | $2.05 | $+30.03 | $8,301.66 | ▲ +30.03 after sell → book $11,151.77; vs 09:30 mark -2.05 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGTX` | 14 | $51.65 | $2.05 | $+23.22 | $9,022.70 | ▲ +23.22 after sell → book $11,149.71; vs 09:30 mark -2.06 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 60 | $13.84 | $2.19 | $+124.04 | $9,850.91 | ▲ +124.04 after sell → book $11,147.52; vs 09:30 mark -2.19 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `HIMS` | 23 | $30.66 | $2.08 | $+17.02 | $10,554.02 | ▲ +17.02 after sell → book $11,145.44; vs 09:30 mark -2.08 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 30 | $23.11 | $2.10 | $-10.78 | $11,245.22 | ▼ -10.78 after sell → book $11,143.34; vs 09:30 mark -2.10 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **COVER** | `VERI` | 28 | $0.96 | $0.35 | $+4.46 | $11,217.90 | ▲ +4.46 after sell → book $11,142.99; vs 09:30 mark -0.35 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ZNTL` | 9 | $4.01 | $0.39 | $-4.85 | $11,181.37 | ▼ -4.85 after sell → book $11,142.60; vs 09:30 mark -0.39 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `APMD` | 1 | $31.87 | $0.32 | $-0.83 | $11,149.18 | ▼ -0.83 after sell → book $11,142.28; vs 09:30 mark -0.32 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HIVE` | 10 | $2.95 | $0.33 | $-0.08 | $11,119.36 | ▼ -0.08 after sell → book $11,141.96; vs 09:30 mark -0.32 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `RNW` | 4 | $6.81 | $0.28 | $-0.63 | $11,091.83 | ▼ -0.63 after sell → book $11,141.67; vs 09:30 mark -0.29 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 104 | $4.43 | $2.30 | — | $10,628.81 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-23.1; combo leftover $462.16; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 9 | $46.85 | $2.02 | — | $10,205.14 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.0; combo leftover $462.16; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 4 | $97.43 | $2.00 | — | $9,813.42 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; combo leftover $462.16; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 1540 | $0.30 | $9.24 | — | $9,342.18 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-3.2; combo leftover $462.16; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 51 | $9.01 | $2.14 | — | $8,880.53 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-1.3; combo leftover $462.16; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 118 | $3.89 | $2.34 | — | $8,419.17 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.5; combo leftover $462.16; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 13 | $34.05 | $2.03 | — | $7,974.49 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+9.3; combo leftover $462.16; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 20 | $22.44 | $2.05 | — | $7,523.64 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.1; combo leftover $462.16; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 22 | $20.55 | $2.06 | — | $7,069.48 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; combo leftover $470.23; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 5 | $91.01 | $2.00 | — | $6,612.43 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; combo leftover $470.23; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 22 | $20.65 | $2.06 | — | $6,156.07 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; combo leftover $470.23; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 81 | $5.77 | $2.23 | — | $5,686.47 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; combo leftover $470.23; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 23 | $19.63 | $2.06 | — | $5,232.92 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; combo leftover $470.23; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 15 | $29.63 | $2.04 | — | $4,786.43 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; combo leftover $470.23; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 268 | $1.75 | $3.46 | — | $4,313.98 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; combo leftover $470.23; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 3 | $144.54 | $2.00 | — | $3,878.36 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; combo leftover $470.23; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 3 | $204.45 | $2.04 | — | $4,489.67 | — | news🔴; gate news=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; combo leftover $646.39; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WYFI` | 30 | $21.40 | $2.12 | — | $5,129.55 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-25.2; combo leftover $646.39; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 54 | $11.81 | $2.19 | — | $5,765.37 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $646.39; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 3 | $173.90 | $2.03 | — | $6,285.04 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.2; combo leftover $646.39; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 6 | $106.38 | $2.05 | — | $6,921.27 | — | news🔴; gate news=bad; list earn_react; 🔵; ret5=-1.7; combo leftover $646.39; owner short_news_r_h3 | — |
| 2026-08-20 09:30 ET | **SHORT** | `AQST` | 140 | $4.61 | $2.46 | — | $7,564.21 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $646.39; owner short_news_r_h3 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,564.21 | ▲ close $11,233.59 vs 09:30 $11,157.92 (session +146.83) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,564.21 | ▲ 09:30 equity $11,320.12 vs yday $11,233.59 (+86.53) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 8 | $0.87 | $0.11 | $-0.77 | $7,571.04 | ▼ -0.77 after sell → book $11,320.01; vs 09:30 mark -0.11 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 3 | $115.18 | $2.00 | — | $7,223.50 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; combo leftover $420.61; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 23 | $17.93 | $2.06 | — | $6,808.93 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=+0.2; combo leftover $420.61; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 4 | $93.98 | $2.00 | — | $6,431.01 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.4; combo leftover $420.61; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 9 | $43.08 | $2.02 | — | $6,041.27 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $420.61; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 182 | $2.30 | $2.54 | — | $5,620.14 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.0; combo leftover $420.61; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 3 | $119.43 | $2.00 | — | $5,259.85 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+21.1; combo leftover $401.44; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 23 | $17.20 | $2.06 | — | $4,862.19 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; combo leftover $401.44; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 36 | $11.13 | $2.10 | — | $4,459.41 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; combo leftover $401.44; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 162 | $2.47 | $2.48 | — | $4,056.80 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; combo leftover $401.44; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 207 | $1.93 | $2.67 | — | $3,654.62 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; combo leftover $401.44; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 6 | $59.72 | $2.01 | — | $3,294.29 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.6; combo leftover $401.44; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 304 | $1.32 | $3.92 | — | $2,889.09 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; combo leftover $401.44; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **SHORT** | `QTRX` | 154 | $3.11 | $2.50 | — | $3,365.53 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ret5=+9.1; combo leftover $481.51; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `MRNA` | 3 | $133.11 | $2.03 | — | $3,762.83 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=+109.5; combo leftover $481.51; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 5 | $89.10 | $2.04 | — | $4,206.29 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $481.51; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 12 | $38.40 | $2.06 | — | $4,665.03 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+15.8; combo leftover $481.51; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `ARIS` | 23 | $20.90 | $2.09 | — | $5,143.64 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+16.0; combo leftover $481.51; owner short_news_r_h3 | — |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 17 | $27.00 | $2.07 | — | $5,600.56 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.1; combo leftover $481.51; owner short_news_r_h3 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,600.56 | ▲ close $11,498.33 vs 09:30 $11,320.12 (session +218.97) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,600.56 | ▲ 09:30 equity $11,701.76 vs yday $11,498.33 (+203.43) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 3 | $4.62 | $0.17 | $+1.43 | $5,614.27 | ▲ +1.43 after sell → book $11,701.60; vs 09:30 mark -0.16 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `TGB` | 1 | $9.26 | $0.12 | $+0.60 | $5,623.42 | ▲ +0.60 after sell → book $11,701.48; vs 09:30 mark -0.12 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 4 | $3.50 | $0.17 | $+0.73 | $5,637.24 | ▲ +0.73 after sell → book $11,701.31; vs 09:30 mark -0.17 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `HNST` | 2 | $5.05 | $0.13 | $+0.25 | $5,647.22 | ▲ +0.25 after sell → book $11,701.18; vs 09:30 mark -0.13 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,647.22 | ▼ close $11,691.32 vs 09:30 $11,701.76 (session -9.86) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,647.22 | ▼ 09:30 equity $11,636.91 vs yday $11,691.32 (-54.41) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `TOYO` | 104 | $4.42 | $2.33 | $-5.67 | $6,104.57 | ▼ -5.67 after sell → book $11,634.58; vs 09:30 mark -2.33 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AAP` | 9 | $43.63 | $2.04 | $-33.03 | $6,495.20 | ▼ -33.03 after sell → book $11,632.54; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `EL` | 4 | $104.00 | $2.02 | $+22.26 | $6,909.18 | ▲ +22.26 after sell → book $11,630.52; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DVLT` | 1540 | $0.31 | $9.66 | $-3.50 | $7,376.92 | ▼ -3.50 after sell → book $11,620.86; vs 09:30 mark -9.66 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AEG` | 51 | $9.23 | $2.16 | $+6.91 | $7,845.49 | ▲ +6.91 after sell → book $11,618.70; vs 09:30 mark -2.16 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ALVO` | 118 | $5.24 | $2.37 | $+154.58 | $8,461.43 | ▲ +154.58 after sell → book $11,616.32; vs 09:30 mark -2.38 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATAT` | 13 | $34.72 | $2.05 | $+4.63 | $8,910.74 | ▲ +4.63 after sell → book $11,614.27; vs 09:30 mark -2.05 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATHM` | 20 | $21.85 | $2.07 | $-15.92 | $9,345.67 | ▼ -15.92 after sell → book $11,612.20; vs 09:30 mark -2.07 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 3 | $212.00 | $2.00 | $-26.68 | $8,707.67 | ▼ -26.68 after sell → book $11,610.20; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WYFI` | 30 | $20.90 | $2.08 | $+10.80 | $8,078.59 | ▲ +10.80 after sell → book $11,608.12; vs 09:30 mark -2.08 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ABCL` | 54 | $11.00 | $2.15 | $+39.67 | $7,482.44 | ▲ +39.67 after sell → book $11,605.97; vs 09:30 mark -2.15 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 3 | $170.64 | $2.00 | $+5.75 | $6,968.52 | ▲ +5.75 after sell → book $11,603.97; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 6 | $105.58 | $2.01 | $+0.75 | $6,333.04 | ▲ +0.75 after sell → book $11,601.97; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AQST` | 140 | $4.77 | $2.41 | $-27.27 | $5,662.83 | ▼ -27.27 after sell → book $11,599.56; vs 09:30 mark -2.41 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 1 | $175.01 | $1.75 | — | $5,486.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.0; combo leftover $235.95; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 2 | $88.94 | $1.78 | — | $5,306.40 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.9; combo leftover $235.95; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 15 | $15.28 | $2.04 | — | $5,075.16 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-0.7; combo leftover $235.95; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 1 | $142.36 | $1.43 | — | $4,931.38 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.6; combo leftover $235.95; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 46 | $5.10 | $2.13 | — | $4,694.65 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.9; combo leftover $235.95; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 4 | $47.89 | $1.93 | — | $4,501.16 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ⚪; ret5=+14.0; combo leftover $235.95; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 16 | $13.92 | $2.04 | — | $4,276.40 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.9; combo leftover $235.95; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 51 | $4.54 | $2.14 | — | $4,042.46 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-14.6; combo leftover $235.95; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 14 | $23.77 | $2.03 | — | $3,707.65 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.0; combo leftover $336.87; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 30 | $10.98 | $2.08 | — | $3,376.17 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+1.2; combo leftover $336.87; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 5 | $61.19 | $2.00 | — | $3,068.22 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+7.4; combo leftover $336.87; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 40 | $8.35 | $2.11 | — | $2,732.11 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.0; combo leftover $336.87; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 68 | $4.94 | $2.19 | — | $2,393.99 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; combo leftover $336.87; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **SHORT** | `AVAH` | 58 | $13.62 | $2.21 | — | $3,182.04 | — | news🔴; gate news=bad; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; combo leftover $798.00; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 14 | $54.51 | $2.07 | — | $3,943.10 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+15.1; combo leftover $798.00; owner short_news_r_h3 | — |
| 2026-08-25 09:30 ET | **SHORT** | `INTU` | 2 | $364.35 | $2.04 | — | $4,669.77 | — | news🔴; gate news=bad; list overnight,overnight_mega; 🔵; ret5=+10.2; combo leftover $798.00; owner short_news_r_h3 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,669.77 | ▲ close $11,763.52 vs 09:30 $11,636.91 (session +195.93) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,669.77 | ▲ 09:30 equity $11,781.86 vs yday $11,763.52 (+18.34) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `FUTU` | 3 | $124.67 | $2.02 | $+24.45 | $5,041.76 | ▲ +24.45 after sell → book $11,779.84; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BEKE` | 23 | $18.14 | $2.08 | $+0.58 | $5,456.90 | ▲ +0.58 after sell → book $11,777.76; vs 09:30 mark -2.08 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BJ` | 4 | $94.60 | $2.02 | $-1.54 | $5,833.28 | ▼ -1.54 after sell → book $11,775.74; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BKE` | 9 | $44.39 | $2.04 | $+7.74 | $6,230.75 | ▲ +7.74 after sell → book $11,773.70; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `PSEC` | 182 | $2.35 | $2.58 | $+3.99 | $6,655.88 | ▲ +3.99 after sell → book $11,771.13; vs 09:30 mark -2.57 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `QTRX` | 154 | $2.83 | $2.45 | $+38.17 | $6,217.60 | ▲ +38.17 after sell → book $11,768.67; vs 09:30 mark -2.46 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `MRNA` | 3 | $154.20 | $2.00 | $-67.30 | $5,753.00 | ▼ -67.30 after sell → book $11,766.67; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 5 | $88.24 | $2.00 | $+0.26 | $5,309.80 | ▲ +0.26 after sell → book $11,764.67; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 12 | $38.41 | $2.03 | $-4.20 | $4,846.85 | ▼ -4.20 after sell → book $11,762.64; vs 09:30 mark -2.03 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `ARIS` | 23 | $20.50 | $2.06 | $+5.05 | $4,373.29 | ▲ +5.05 after sell → book $11,760.58; vs 09:30 mark -2.06 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `NOG` | 17 | $26.00 | $2.04 | $+12.89 | $3,929.25 | ▲ +12.89 after sell → book $11,758.54; vs 09:30 mark -2.04 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 374 | $0.58 | $3.30 | — | $3,707.91 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-27.5; combo leftover $218.29; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 41 | $5.21 | $2.11 | — | $3,492.19 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ret5=+14.3; combo leftover $218.29; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `ANF` | 1 | $131.37 | $1.32 | — | $3,359.50 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.3; combo leftover $218.29; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `BBWI` | 11 | $18.26 | $2.02 | — | $3,156.62 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.4; combo leftover $218.29; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `BOX` | 6 | $34.30 | $2.01 | — | $2,948.81 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.7; combo leftover $218.29; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `HCA` | 3 | $427.50 | $2.00 | — | $1,664.31 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.1; combo leftover $1474.40; owner flatten_h5 | — |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 1 | $213.94 | $2.02 | — | $1,876.23 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; combo leftover $332.86; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `ABCL` | 27 | $12.22 | $2.10 | — | $2,204.07 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+12.4; combo leftover $332.86; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `AQST` | 65 | $5.08 | $2.21 | — | $2,532.06 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+17.6; combo leftover $332.86; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 2 | $132.64 | $2.02 | — | $2,795.31 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+16.5; combo leftover $332.86; owner short_news_r_h3 | — |
| 2026-08-26 09:30 ET | **SHORT** | `CRM` | 1 | $199.94 | $2.02 | — | $2,993.23 | — | news🔴; gate news=bad; list overnight,overnight_mega; ret5=+2.1; combo leftover $332.86; owner short_news_r_h3 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,993.23 | ▼ close $11,690.63 vs 09:30 $11,781.86 (session -44.77) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,993.23 | ▼ 09:30 equity $11,663.57 vs yday $11,690.63 (-27.06) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 22 | $20.93 | $2.08 | $+4.23 | $3,451.62 | ▲ +4.23 after sell → book $11,661.50; vs 09:30 mark -2.07 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 5 | $95.52 | $2.02 | $+18.52 | $3,927.19 | ▲ +18.52 after sell → book $11,659.47; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 22 | $21.31 | $2.08 | $+10.39 | $4,393.94 | ▲ +10.39 after sell → book $11,657.40; vs 09:30 mark -2.07 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 81 | $5.49 | $2.26 | $-27.17 | $4,836.37 | ▼ -27.17 after sell → book $11,655.14; vs 09:30 mark -2.26 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 23 | $21.47 | $2.08 | $+38.18 | $5,328.10 | ▲ +38.18 after sell → book $11,653.06; vs 09:30 mark -2.08 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 15 | $32.32 | $2.06 | $+36.26 | $5,810.85 | ▲ +36.26 after sell → book $11,651.01; vs 09:30 mark -2.05 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 268 | $1.91 | $3.51 | $+35.91 | $6,319.22 | ▲ +35.91 after sell → book $11,647.50; vs 09:30 mark -3.51 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 3 | $155.89 | $2.02 | $+30.03 | $6,784.87 | ▲ +30.03 after sell → book $11,645.48; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `BBY` | 3 | $80.60 | $2.00 | — | $6,541.07 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.0; combo leftover $282.70; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `BILI` | 17 | $16.18 | $2.04 | — | $6,263.97 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-6.7; combo leftover $282.70; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 2 | $118.77 | $2.00 | — | $6,024.43 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.3; combo leftover $282.70; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CMBT` | 15 | $17.78 | $2.04 | — | $5,755.70 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.2; combo leftover $282.70; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CSIQ` | 21 | $13.41 | $2.05 | — | $5,472.03 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.1; combo leftover $282.70; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `HQY` | 2 | $97.16 | $1.95 | — | $5,275.76 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.5; combo leftover $282.70; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `RY` | 1 | $206.82 | $1.99 | — | $5,066.95 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.2; combo leftover $282.70; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `TD` | 2 | $120.17 | $2.00 | — | $4,824.61 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; combo leftover $282.70; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 19 | $41.44 | $2.05 | — | $4,035.21 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.1; combo leftover $804.10; owner flatten_h5 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 55 | $14.42 | $2.15 | — | $3,239.95 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; combo leftover $804.10; owner flatten_h5 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 309 | $2.60 | $3.99 | — | $2,432.57 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+13.0; combo leftover $804.10; owner flatten_h5 | — |
| 2026-08-27 09:30 ET | **SHORT** | `MT` | 16 | $74.54 | $2.09 | — | $3,623.12 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=-0.1; combo leftover $1216.28; owner short_news_r_h3 | — |
| 2026-08-27 09:30 ET | **SHORT** | `TX` | 22 | $55.25 | $2.11 | — | $4,836.51 | — | news🔴; gate news=bad; list mover_buy; 🔵; ret5=+2.1; combo leftover $1216.28; owner short_news_r_h3 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,836.51 | ▲ close $11,662.70 vs 09:30 $11,663.57 (session +45.68) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,836.51 | ▼ 09:30 equity $11,631.02 vs yday $11,662.70 (-31.68) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 3 | $119.19 | $2.02 | $-4.74 | $5,192.06 | ▼ -4.74 after sell → book $11,629.00; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 23 | $16.44 | $2.08 | $-21.62 | $5,568.10 | ▼ -21.62 after sell → book $11,626.92; vs 09:30 mark -2.08 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 36 | $15.43 | $2.12 | $+150.58 | $6,121.46 | ▲ +150.58 after sell → book $11,624.80; vs 09:30 mark -2.12 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 162 | $2.35 | $2.51 | $-24.43 | $6,499.65 | ▼ -24.43 after sell → book $11,622.29; vs 09:30 mark -2.51 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 207 | $2.06 | $2.71 | $+21.53 | $6,923.35 | ▲ +21.53 after sell → book $11,619.58; vs 09:30 mark -2.71 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRSP` | 6 | $58.22 | $2.03 | $-13.04 | $7,270.64 | ▼ -13.04 after sell → book $11,617.55; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 304 | $1.82 | $3.98 | $+144.10 | $7,819.94 | ▲ +144.10 after sell → book $11,613.57; vs 09:30 mark -3.98 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMO` | 1 | $172.76 | $1.75 | $-5.75 | $7,990.95 | ▼ -5.75 after sell → book $11,611.82; vs 09:30 mark -1.75 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BNS` | 2 | $93.30 | $1.89 | $+5.04 | $8,175.66 | ▲ +5.04 after sell → book $11,609.92; vs 09:30 mark -1.90 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 15 | $18.15 | $2.06 | $+38.96 | $8,445.85 | ▲ +38.96 after sell → book $11,607.87; vs 09:30 mark -2.05 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 1 | $132.80 | $1.35 | $-12.34 | $8,577.30 | ▼ -12.34 after sell → book $11,606.52; vs 09:30 mark -1.35 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EH` | 46 | $4.58 | $2.15 | $-28.20 | $8,785.84 | ▼ -28.20 after sell → book $11,604.37; vs 09:30 mark -2.15 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GFI` | 4 | $48.42 | $1.97 | $-1.78 | $8,977.55 | ▼ -1.78 after sell → book $11,602.40; vs 09:30 mark -1.97 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GRRR` | 16 | $15.66 | $2.06 | $+23.74 | $9,226.05 | ▲ +23.74 after sell → book $11,600.34; vs 09:30 mark -2.06 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SHMD` | 51 | $3.38 | $1.90 | $-63.45 | $9,396.53 | ▼ -63.45 after sell → book $11,598.45; vs 09:30 mark -1.89 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `AVAH` | 58 | $13.90 | $2.16 | $-20.32 | $8,588.17 | ▼ -20.32 after sell → book $11,596.28; vs 09:30 mark -2.17 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 14 | $52.49 | $2.03 | $+24.18 | $7,851.28 | ▲ +24.18 after sell → book $11,594.25; vs 09:30 mark -2.03 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `INTU` | 2 | $347.82 | $2.00 | $+29.03 | $7,153.64 | ▲ +29.03 after sell → book $11,592.25; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 1 | $261.16 | $1.99 | — | $6,890.49 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+7.8; combo leftover $447.10; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 29 | $15.01 | $2.08 | — | $6,453.12 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+3.7; combo leftover $447.10; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 4 | $103.89 | $2.00 | — | $6,035.56 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.5; combo leftover $447.10; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 115 | $3.88 | $2.33 | — | $5,587.02 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-8.6; combo leftover $447.10; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 10 | $44.40 | $2.02 | — | $5,141.00 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.4; combo leftover $447.10; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 18 | $24.69 | $2.04 | — | $4,694.54 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.8; combo leftover $447.10; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 53 | $8.35 | $2.15 | — | $4,249.84 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.1; combo leftover $447.10; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 11 | $37.65 | $2.02 | — | $3,833.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $447.10; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **SHORT** | `SIMO` | 7 | $252.24 | $2.08 | — | $5,597.32 | — | news🔴; gate news=bad; list probable,yday_gainer; ⚪; ret5=+2.2; combo leftover $1916.86; owner short_news_r_h3 | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 63 | $30.18 | $2.26 | — | $7,496.40 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+12.1; combo leftover $1916.86; owner short_news_r_h3 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,496.40 | ▼ close $11,511.75 vs 09:30 $11,631.02 (session -59.51) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,496.40 | ▲ 09:30 equity $11,593.78 vs yday $11,511.75 (+82.03) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `SLQT` | 374 | $0.51 | $3.10 | $-33.71 | $7,684.04 | ▼ -33.71 after sell → book $11,590.68; vs 09:30 mark -3.10 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `TIGR` | 41 | $5.00 | $2.13 | $-12.86 | $7,886.91 | ▼ -12.86 after sell → book $11,588.54; vs 09:30 mark -2.14 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 1 | $148.03 | $1.50 | $+13.84 | $8,033.43 | ▲ +13.84 after sell → book $11,587.04; vs 09:30 mark -1.50 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 11 | $19.25 | $2.04 | $+6.82 | $8,243.14 | ▲ +6.82 after sell → book $11,585.00; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BOX` | 6 | $34.72 | $2.03 | $-1.52 | $8,449.43 | ▼ -1.52 after sell → book $11,582.97; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `BE` | 1 | $208.88 | $1.99 | $+1.05 | $8,238.56 | ▲ +1.05 after sell → book $11,580.98; vs 09:30 mark -1.99 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `ABCL` | 27 | $11.10 | $2.07 | $+26.07 | $7,936.79 | ▲ +26.07 after sell → book $11,578.90; vs 09:30 mark -2.08 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `AQST` | 65 | $4.97 | $2.19 | $+2.43 | $7,611.23 | ▲ +2.43 after sell → book $11,576.72; vs 09:30 mark -2.18 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `NEM` | 2 | $127.45 | $2.00 | $+6.36 | $7,354.33 | ▲ +6.36 after sell → book $11,574.72; vs 09:30 mark -2.00 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `CRM` | 1 | $254.39 | $1.99 | $-58.46 | $7,097.95 | ▼ -58.46 after sell → book $11,572.73; vs 09:30 mark -1.99 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,097.95 | ▲ close $11,621.52 vs 09:30 $11,593.78 (session +48.78) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,097.95 | ▲ 09:30 equity $11,767.66 vs yday $11,621.52 (+146.14) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `MOS` | 14 | $23.94 | $2.05 | $-1.70 | $7,431.06 | ▼ -1.70 after sell → book $11,765.61; vs 09:30 mark -2.05 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `OCUL` | 30 | $10.42 | $2.10 | $-20.98 | $7,741.56 | ▼ -20.98 after sell → book $11,763.51; vs 09:30 mark -2.10 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `INSP` | 5 | $63.00 | $2.02 | $+5.02 | $8,054.53 | ▲ +5.02 after sell → book $11,761.48; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 40 | $8.25 | $2.13 | $-8.24 | $8,382.40 | ▼ -8.24 after sell → book $11,759.35; vs 09:30 mark -2.13 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 68 | $4.64 | $2.22 | $-24.81 | $8,695.71 | ▼ -24.81 after sell → book $11,757.14; vs 09:30 mark -2.21 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `BBY` | 3 | $79.83 | $2.02 | $-6.33 | $8,933.18 | ▼ -6.33 after sell → book $11,755.12; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `BILI` | 17 | $15.97 | $2.06 | $-7.67 | $9,202.61 | ▼ -7.67 after sell → book $11,753.06; vs 09:30 mark -2.06 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CM` | 2 | $113.66 | $2.02 | $-14.23 | $9,427.91 | ▼ -14.23 after sell → book $11,751.04; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CMBT` | 15 | $18.28 | $2.06 | $+3.41 | $9,700.06 | ▲ +3.41 after sell → book $11,748.99; vs 09:30 mark -2.05 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CSIQ` | 21 | $12.18 | $2.07 | $-29.96 | $9,953.76 | ▼ -29.96 after sell → book $11,746.91; vs 09:30 mark -2.08 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `HQY` | 2 | $96.65 | $1.96 | $-4.93 | $10,145.10 | ▼ -4.93 after sell → book $11,744.95; vs 09:30 mark -1.96 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `RY` | 1 | $203.78 | $2.01 | $-7.05 | $10,346.87 | ▼ -7.05 after sell → book $11,742.94; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `TD` | 2 | $120.54 | $2.02 | $-3.27 | $10,585.94 | ▼ -3.27 after sell → book $11,740.93; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `MT` | 16 | $73.22 | $2.04 | $+16.99 | $9,412.38 | ▲ +16.99 after sell → book $11,738.89; vs 09:30 mark -2.04 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **COVER** | `TX` | 22 | $54.76 | $2.06 | $+6.61 | $8,205.60 | ▲ +6.61 after sell → book $11,736.83; vs 09:30 mark -2.06 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,205.60 | ▼ close $11,664.13 vs 09:30 $11,767.66 (session -72.70) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,205.60 | ▼ 09:30 equity $11,656.12 vs yday $11,664.13 (-8.01) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `HCA` | 3 | $412.46 | $2.02 | $-49.14 | $9,440.96 | ▼ -49.14 after sell → book $11,654.10; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 1 | $246.70 | $2.01 | $-18.47 | $9,685.65 | ▼ -18.47 after sell → book $11,652.08; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBAR` | 29 | $15.01 | $2.10 | $-4.17 | $10,118.84 | ▼ -4.17 after sell → book $11,649.99; vs 09:30 mark -2.09 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ESTC` | 4 | $92.00 | $2.02 | $-51.58 | $10,484.82 | ▼ -51.58 after sell → book $11,647.97; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FINV` | 115 | $3.32 | $2.36 | $-69.10 | $10,864.26 | ▼ -69.10 after sell → book $11,645.60; vs 09:30 mark -2.37 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FRO` | 10 | $44.17 | $2.04 | $-6.36 | $11,303.92 | ▼ -6.36 after sell → book $11,643.56; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GAP` | 18 | $21.97 | $2.06 | $-53.07 | $11,697.31 | ▼ -53.07 after sell → book $11,641.50; vs 09:30 mark -2.06 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `HAFN` | 53 | $8.58 | $2.17 | $+7.87 | $12,149.88 | ▲ +7.87 after sell → book $11,639.33; vs 09:30 mark -2.17 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `IREN` | 11 | $35.80 | $2.04 | $-24.42 | $12,541.59 | ▼ -24.42 after sell → book $11,637.29; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `SIMO` | 7 | $235.71 | $2.01 | $+111.62 | $10,889.60 | ▲ +111.62 after sell → book $11,635.27; vs 09:30 mark -2.02 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 63 | $26.78 | $2.18 | $+209.76 | $9,200.29 | ▲ +209.76 after sell → book $11,633.10; vs 09:30 mark -2.17 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,200.29 | ▼ close $11,606.80 vs 09:30 $11,656.12 (session -26.30) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,200.29 | ▲ 09:30 equity $11,625.62 vs yday $11,606.80 (+18.82) | — | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 19 | $42.43 | $2.07 | $+14.70 | $10,004.39 | ▲ +14.70 after sell → book $11,623.55; vs 09:30 mark -2.07 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `CRK` | 55 | $15.45 | $2.17 | $+52.32 | $10,851.96 | ▲ +52.32 after sell → book $11,621.37; vs 09:30 mark -2.18 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 309 | $2.49 | $4.05 | $-42.02 | $11,617.33 | ▼ -42.02 after sell → book $11,617.33; vs 09:30 mark -4.04 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 45 | $10.74 | $2.12 | — | $11,131.68 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+8.5; combo leftover $484.06; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 1 | $351.74 | $1.99 | — | $10,777.94 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.3; combo leftover $484.06; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 70 | $6.90 | $2.20 | — | $10,292.74 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.8; combo leftover $484.06; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 1 | $354.49 | $1.99 | — | $9,936.26 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.3; combo leftover $484.06; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 21 | $22.32 | $2.05 | — | $9,465.49 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+2.4; combo leftover $484.06; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 1 | $257.00 | $1.99 | — | $9,206.49 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-5.5; combo leftover $484.06; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 10 | $47.60 | $2.02 | — | $8,728.47 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.2; combo leftover $484.06; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 32 | $15.09 | $2.09 | — | $8,243.51 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+6.1; combo leftover $484.06; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 15 | $52.88 | $2.04 | — | $7,448.27 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; combo leftover $824.35; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 19 | $42.93 | $2.05 | — | $6,630.56 | — | baseline list, no extra gate; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.1; combo leftover $824.35; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 227 | $3.63 | $2.93 | — | $5,803.62 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.8; combo leftover $824.35; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 102 | $8.03 | $2.30 | — | $4,982.26 | — | baseline list, no extra gate; list flatten,ohlc_hot,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.5; combo leftover $824.35; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 6 | $132.45 | $2.01 | — | $4,185.55 | — | baseline list, no extra gate; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.6; combo leftover $824.35; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **SHORT** | `SLN` | 140 | $14.85 | $2.51 | — | $6,262.05 | — | news🔴; gate news=bad; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; combo leftover $2092.78; owner short_news_r_h3 | — |
| 2026-09-03 09:30 ET | **SHORT** | `OPK` | 1223 | $1.71 | $16.05 | — | $8,337.32 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ⚪; ret5=+11.9; combo leftover $2092.78; owner short_news_r_h3 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,337.32 | ▲ close $11,807.68 vs 09:30 $11,625.62 (session +236.70) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,337.32 | ▲ 09:30 equity $11,840.20 vs yday $11,807.68 (+32.52) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 5 | $63.18 | $2.00 | — | $8,019.42 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-10.9; combo leftover $347.39; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 39 | $8.74 | $2.11 | — | $7,676.45 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.8; combo leftover $347.39; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 5 | $68.52 | $2.00 | — | $7,331.85 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.4; combo leftover $347.39; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 96 | $3.62 | $2.28 | — | $6,982.53 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.1; combo leftover $347.39; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 2 | $167.55 | $2.00 | — | $6,645.43 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; combo leftover $347.39; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 7 | $44.90 | $2.01 | — | $6,329.12 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-7.5; combo leftover $347.39; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 3 | $98.15 | $2.00 | — | $6,032.67 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.9; combo leftover $347.39; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 22 | $15.70 | $2.06 | — | $5,685.22 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.4; combo leftover $347.39; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 225 | $2.52 | $2.90 | — | $5,115.31 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+5.0; combo leftover $568.52; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 84 | $6.71 | $2.24 | — | $4,549.43 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; combo leftover $568.52; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 299 | $1.90 | $3.86 | — | $3,977.47 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.7; combo leftover $568.52; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 118 | $4.78 | $2.34 | — | $3,411.09 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; combo leftover $568.52; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 50 | $11.31 | $2.14 | — | $2,843.45 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+1.2; combo leftover $568.52; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **SHORT** | `GSM` | 304 | $4.67 | $4.02 | — | $4,259.11 | — | news🔴; gate news=bad; list yday_gainer; ret5=+11.9; combo leftover $1421.73; owner short_news_r_h3 | — |
| 2026-09-04 09:30 ET | **SHORT** | `PIPR` | 18 | $76.55 | $2.10 | — | $5,634.91 | — | news🔴; gate news=bad; list mover_buy; 🔵; ⚪; ret5=+4.2; combo leftover $1421.73; owner short_news_r_h3 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,634.91 | ▼ close $11,732.17 vs 09:30 $11,840.20 (session -71.97) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,634.91 | ▲ 09:30 equity $11,748.97 vs yday $11,732.17 (+16.80) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,634.91 | ▲ close $11,818.29 vs 09:30 $11,748.97 (session +69.32) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,634.91 | ▼ 09:30 equity $11,786.61 vs yday $11,818.29 (-31.68) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `AI` | 45 | $10.51 | $2.15 | $-14.84 | $6,105.71 | ▼ -14.84 after sell → book $11,784.46; vs 09:30 mark -2.15 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 1 | $366.23 | $2.01 | $+10.48 | $6,469.93 | ▲ +10.48 after sell → book $11,782.45; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CHPT` | 70 | $9.39 | $2.22 | $+169.88 | $7,125.01 | ▲ +169.88 after sell → book $11,780.23; vs 09:30 mark -2.22 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CIEN` | 1 | $341.90 | $2.01 | $-16.60 | $7,464.89 | ▼ -16.60 after sell → book $11,778.21; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CPB` | 21 | $21.67 | $2.07 | $-17.78 | $7,917.89 | ▼ -17.78 after sell → book $11,776.14; vs 09:30 mark -2.07 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FIVE` | 1 | $252.92 | $2.01 | $-8.09 | $8,168.80 | ▼ -8.09 after sell → book $11,774.13; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HPE` | 10 | $56.94 | $2.04 | $+89.34 | $8,736.16 | ▲ +89.34 after sell → book $11,772.09; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MEI` | 32 | $13.84 | $2.11 | $-44.19 | $9,176.93 | ▼ -44.19 after sell → book $11,769.98; vs 09:30 mark -2.11 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `SLN` | 140 | $13.60 | $2.41 | $+170.08 | $7,270.52 | ▲ +170.08 after sell → book $11,767.57; vs 09:30 mark -2.41 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **COVER** | `OPK` | 1223 | $1.58 | $15.78 | $+127.16 | $5,322.40 | ▲ +127.16 after sell → book $11,751.79; vs 09:30 mark -15.78 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,322.40 | ▼ close $11,506.30 vs 09:30 $11,786.61 (session -245.49) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,322.40 | ▼ 09:30 equity $11,452.52 vs yday $11,506.30 (-53.78) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `AMBA` | 5 | $67.44 | $2.02 | $+17.27 | $5,657.58 | ▲ +17.27 after sell → book $11,450.49; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `ASAN` | 39 | $8.26 | $2.13 | $-22.95 | $5,977.59 | ▼ -22.95 after sell → book $11,448.36; vs 09:30 mark -2.13 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DOCU` | 5 | $64.60 | $2.02 | $-23.63 | $6,298.57 | ▼ -23.63 after sell → book $11,446.34; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DOMO` | 96 | $3.76 | $2.30 | $+9.34 | $6,657.22 | ▲ +9.34 after sell → book $11,444.03; vs 09:30 mark -2.31 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `GWRE` | 2 | $142.43 | $2.02 | $-54.25 | $6,940.07 | ▼ -54.25 after sell → book $11,442.02; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `IOT` | 7 | $38.23 | $2.03 | $-50.77 | $7,205.61 | ▼ -50.77 after sell → book $11,439.99; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `LULU` | 3 | $98.71 | $2.02 | $-2.34 | $7,499.72 | ▼ -2.34 after sell → book $11,437.97; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `MAMA` | 22 | $15.26 | $2.08 | $-13.81 | $7,833.37 | ▼ -13.81 after sell → book $11,435.89; vs 09:30 mark -2.08 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `GSM` | 304 | $4.36 | $3.92 | $+86.30 | $6,504.01 | ▲ +86.30 after sell → book $11,431.97; vs 09:30 mark -3.92 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **COVER** | `PIPR` | 18 | $76.79 | $2.04 | $-8.47 | $5,119.74 | ▼ -8.47 after sell → book $11,429.93; vs 09:30 mark -2.04 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,119.74 | ▼ close $11,314.68 vs 09:30 $11,452.52 (session -115.25) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,119.74 | ▲ 09:30 equity $11,389.52 vs yday $11,314.68 (+74.84) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `ATRC` | 15 | $53.53 | $2.06 | $+5.66 | $5,920.64 | ▲ +5.66 after sell → book $11,387.47; vs 09:30 mark -2.05 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `HRMY` | 19 | $41.30 | $2.07 | $-35.08 | $6,703.27 | ▼ -35.08 after sell → book $11,385.40; vs 09:30 mark -2.07 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CABA` | 227 | $2.77 | $2.98 | $-201.12 | $7,329.08 | ▼ -201.12 after sell → book $11,382.42; vs 09:30 mark -2.98 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `VSTM` | 102 | $7.70 | $2.32 | $-38.28 | $8,112.16 | ▼ -38.28 after sell → book $11,380.10; vs 09:30 mark -2.32 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `RVTY` | 6 | $122.40 | $2.03 | $-64.34 | $8,844.53 | ▼ -64.34 after sell → book $11,378.07; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 2 | $164.43 | $2.00 | — | $8,513.68 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,earn_react; ⚪; ret5=+4.9; combo leftover $368.52; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 62 | $5.91 | $2.18 | — | $8,145.08 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover,ohlc_hot; ret5=+14.1; combo leftover $368.52; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 1 | $242.17 | $1.99 | — | $7,900.92 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.1; combo leftover $368.52; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `CPRT` | 11 | $32.01 | $2.02 | — | $7,546.78 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.4; combo leftover $368.52; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `DSGX` | 5 | $71.71 | $2.00 | — | $7,186.23 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-9.1; combo leftover $368.52; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `KR` | 6 | $56.02 | $2.01 | — | $6,848.10 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.2; combo leftover $368.52; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `LPTH` | 39 | $9.37 | $2.11 | — | $6,480.56 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+1.5; combo leftover $368.52; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `REF` | 28 | $13.10 | $2.07 | — | $6,111.69 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.9; combo leftover $368.52; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 37 | $16.28 | $2.10 | — | $5,507.23 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-1.1; combo leftover $611.17; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 223 | $2.73 | $2.88 | — | $4,895.56 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-3.0; combo leftover $611.17; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 2 | $206.84 | $2.00 | — | $4,479.89 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+8.3; combo leftover $611.17; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 3 | $157.78 | $2.00 | — | $4,004.55 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+4.7; combo leftover $611.17; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 10 | $56.09 | $2.02 | — | $3,441.63 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+19.6; combo leftover $611.17; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 6 | $112.83 | $2.05 | — | $4,116.59 | — | news🔴; gate news=bad; list yday_gainer,ohlc_hot; ret5=+11.7; combo leftover $688.33; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `RWT` | 195 | $3.52 | $2.64 | — | $4,800.35 | — | news🔴; gate news=bad; list yday_mover; 🔵; ret5=-19.2; combo leftover $688.33; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `CRDL` | 339 | $2.03 | $4.46 | — | $5,484.07 | — | news🔴; gate news=bad; list yday_mover; ret5=-8.8; combo leftover $688.33; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `BKV` | 27 | $24.97 | $2.11 | — | $6,156.15 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+10.8; combo leftover $688.33; owner short_news_r_h3 | — |
| 2026-09-11 09:30 ET | **SHORT** | `MYGN` | 204 | $3.37 | $2.69 | — | $6,840.93 | — | news🔴; gate news=bad; list ohlc_hot; 🔵; ret5=+4.0; combo leftover $688.33; owner short_news_r_h3 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,840.93 | ▲ close $11,340.94 vs 09:30 $11,389.52 (session +4.18) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,840.93 | ▼ 09:30 equity $11,283.72 vs yday $11,340.94 (-57.22) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ALEC` | 225 | $2.15 | $2.95 | $-89.10 | $7,321.73 | ▼ -89.10 after sell → book $11,280.77; vs 09:30 mark -2.95 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BHC` | 84 | $5.93 | $2.27 | $-70.03 | $7,817.59 | ▼ -70.03 after sell → book $11,278.51; vs 09:30 mark -2.26 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BMEA` | 299 | $1.72 | $3.92 | $-63.09 | $8,326.46 | ▼ -63.09 after sell → book $11,274.59; vs 09:30 mark -3.92 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OABI` | 118 | $4.13 | $2.37 | $-81.42 | $8,811.42 | ▼ -81.42 after sell → book $11,272.22; vs 09:30 mark -2.37 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIR` | 50 | $10.73 | $2.16 | $-33.30 | $9,345.76 | ▼ -33.30 after sell → book $11,270.06; vs 09:30 mark -2.16 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,345.76 | ▼ close $11,246.21 vs 09:30 $11,283.72 (session -23.84) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,345.76 | ▲ 09:30 equity $11,262.35 vs yday $11,246.21 (+16.14) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,345.76 | ▼ close $11,170.60 vs 09:30 $11,262.35 (session -91.75) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,345.76 | ▲ 09:30 equity $11,186.47 vs yday $11,170.60 (+15.87) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 2 | $140.03 | $2.02 | $-52.81 | $9,623.81 | ▼ -52.81 after sell → book $11,184.46; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `DBI` | 62 | $6.25 | $2.20 | $+16.71 | $10,009.11 | ▲ +16.71 after sell → book $11,182.26; vs 09:30 mark -2.20 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ADBE` | 1 | $253.34 | $2.01 | $+7.16 | $10,260.44 | ▲ +7.16 after sell → book $11,180.25; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CPRT` | 11 | $30.57 | $2.04 | $-19.91 | $10,594.66 | ▼ -19.91 after sell → book $11,178.20; vs 09:30 mark -2.05 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `DSGX` | 5 | $78.12 | $2.02 | $+28.02 | $10,983.24 | ▲ +28.02 after sell → book $11,176.18; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `KR` | 6 | $61.93 | $2.03 | $+31.42 | $11,352.79 | ▲ +31.42 after sell → book $11,174.15; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `LPTH` | 39 | $9.40 | $2.13 | $-3.06 | $11,717.26 | ▼ -3.06 after sell → book $11,172.02; vs 09:30 mark -2.13 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `REF` | 28 | $15.75 | $2.09 | $+70.03 | $12,156.17 | ▲ +70.03 after sell → book $11,169.93; vs 09:30 mark -2.09 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `QRVO` | 6 | $118.18 | $2.01 | $-36.12 | $11,445.08 | ▼ -36.12 after sell → book $11,167.92; vs 09:30 mark -2.01 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `RWT` | 195 | $3.98 | $2.58 | $-94.91 | $10,666.41 | ▼ -94.91 after sell → book $11,165.35; vs 09:30 mark -2.57 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `CRDL` | 339 | $1.85 | $4.37 | $+52.19 | $10,034.88 | ▲ +52.19 after sell → book $11,160.97; vs 09:30 mark -4.38 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `BKV` | 27 | $24.42 | $2.07 | $+10.67 | $9,373.47 | ▲ +10.67 after sell → book $11,158.90; vs 09:30 mark -2.07 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **COVER** | `MYGN` | 204 | $3.75 | $2.63 | $-82.85 | $8,605.84 | ▼ -82.85 after sell → book $11,156.27; vs 09:30 mark -2.63 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 43 | $33.14 | $2.12 | — | $7,178.70 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ret5=-2.9; combo leftover $1434.31; owner union_e_fresh_h3 | — |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 35 | $40.93 | $2.10 | — | $5,744.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $1434.31; owner union_e_fresh_h3 | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 2 | $270.89 | $2.00 | — | $5,200.28 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.0; combo leftover $718.01; owner flatten_h5 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 9 | $77.12 | $2.02 | — | $4,504.18 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+7.2; combo leftover $718.01; owner flatten_h5 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 50 | $14.31 | $2.14 | — | $3,786.54 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.8; combo leftover $718.01; owner flatten_h5 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 19 | $36.46 | $2.05 | — | $3,091.76 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+2.9; combo leftover $718.01; owner flatten_h5 | — |
| 2026-09-16 09:30 ET | **SHORT** | `BBNX` | 83 | $18.61 | $2.31 | — | $4,634.08 | — | news🔴; gate news=bad; list probable,yday_gainer,yday_mover; ret5=-0.5; combo leftover $1545.88; owner short_news_r_h3 | — |
| 2026-09-16 09:30 ET | **SHORT** | `GFR` | 226 | $6.83 | $3.01 | — | $6,174.65 | — | news🔴; gate news=bad; list ohlc_hot; ret5=+11.2; combo leftover $1545.88; owner short_news_r_h3 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,174.65 | ▼ close $10,969.10 vs 09:30 $11,186.47 (session -169.44) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,174.65 | ▲ 09:30 equity $11,132.42 vs yday $10,969.10 (+163.32) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `LEN` | 12 | $81.00 | $2.03 | — | $5,200.63 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.0; combo leftover $1029.11; owner union_e_fresh_h3 | — |
| 2026-09-17 09:30 ET | **BUY** | `ALMU` | 91 | $11.21 | $2.26 | — | $4,178.25 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.0; combo leftover $1029.11; owner union_e_fresh_h3 | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 1 | $233.85 | $1.99 | — | $3,942.41 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+11.7; combo leftover $348.19; owner flatten_h5 | — |
| 2026-09-17 09:30 ET | **BUY** | `TWST` | 2 | $151.43 | $2.00 | — | $3,637.55 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.0; combo leftover $348.19; owner flatten_h5 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 2 | $147.61 | $2.00 | — | $3,340.34 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+17.7; combo leftover $348.19; owner flatten_h5 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 33 | $10.25 | $2.09 | — | $3,000.00 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+17.1; combo leftover $348.19; owner flatten_h5 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 45 | $7.59 | $2.12 | — | $2,656.32 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+9.4; combo leftover $348.19; owner flatten_h5 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 9 | $34.93 | $2.02 | — | $2,339.94 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.6; combo leftover $348.19; owner flatten_h5 | — |
| 2026-09-17 09:30 ET | **SHORT** | `BULL` | 294 | $7.95 | $3.92 | — | $4,673.31 | — | news🔴; gate news=bad; list yday_mover; 🔵; ⚪; ret5=-18.4; combo leftover $2339.94; owner short_news_r_h3 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,673.31 | ▲ close $11,287.70 vs 09:30 $11,132.42 (session +175.70) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,673.31 | ▲ 09:30 equity $11,343.32 vs yday $11,287.70 (+55.62) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AUPH` | 37 | $16.93 | $2.12 | $+19.83 | $5,297.60 | ▲ +19.83 after sell → book $11,341.20; vs 09:30 mark -2.12 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `OVID` | 223 | $2.68 | $2.92 | $-16.95 | $5,892.32 | ▼ -16.95 after sell → book $11,338.28; vs 09:30 mark -2.92 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `SANM` | 2 | $197.76 | $2.02 | $-22.17 | $6,285.82 | ▼ -22.17 after sell → book $11,336.26; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `NVT` | 3 | $152.71 | $2.02 | $-19.23 | $6,741.93 | ▼ -19.23 after sell → book $11,334.24; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `COHU` | 10 | $55.80 | $2.04 | $-6.96 | $7,297.89 | ▼ -6.96 after sell → book $11,332.20; vs 09:30 mark -2.04 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 11 | $108.55 | $2.02 | — | $6,101.82 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+21.3; combo leftover $1216.32; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `DELL` | 2 | $593.15 | $2.00 | — | $4,913.52 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+16.1; combo leftover $1216.32; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 5 | $209.52 | $2.00 | — | $3,863.92 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.1; combo leftover $1216.32; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 5 | $219.62 | $2.00 | — | $2,763.81 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+21.5; combo leftover $1216.32; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 14 | $85.00 | $2.03 | — | $1,571.78 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+18.3; combo leftover $1216.32; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 35 | $34.44 | $2.10 | — | $364.29 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+14.0; combo leftover $1216.32; owner flatten_h5 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $364.29 | ▼ close $11,052.73 vs 09:30 $11,343.32 (session -267.32) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $364.29 | ▲ 09:30 equity $11,150.72 vs yday $11,052.73 (+97.99) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `FPS` | 43 | $40.03 | $2.14 | $+292.01 | $2,083.43 | ▲ +292.01 after sell → book $11,148.58; vs 09:30 mark -2.14 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TCOM` | 35 | $41.00 | $2.12 | $-1.76 | $3,516.32 | ▼ -1.76 after sell → book $11,146.46; vs 09:30 mark -2.12 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `BBNX` | 83 | $22.11 | $2.24 | $-295.05 | $1,678.95 | ▼ -295.05 after sell → book $11,144.22; vs 09:30 mark -2.24 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **COVER** | `GFR` | 226 | $6.55 | $2.92 | $+57.36 | $195.73 | ▲ +57.36 after sell → book $11,141.31; vs 09:30 mark -2.91 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 1 | $13.47 | $0.14 | — | $182.13 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.6; combo leftover $19.57; owner flatten_h5 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 4 | $4.00 | $0.17 | — | $165.95 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); ret5=+58.9; combo leftover $19.57; owner flatten_h5 | — |
| 2026-09-21 09:30 ET | **SHORT** | `AEHL` | 10 | $8.26 | $0.88 | — | $247.68 | — | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $82.98; owner short_news_r_h3 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $247.68 | ▲ close $11,365.58 vs 09:30 $11,150.72 (session +225.45) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $247.68 | ▼ 09:30 equity $11,347.32 vs yday $11,365.58 (-18.26) | — | — |
| 2026-09-22 09:30 ET | **COVER** | `BULL` | 294 | $8.28 | $3.79 | $-103.27 | $-2,188.97 | ▼ -103.27 after sell → book $11,343.52; vs 09:30 mark -3.80 | short_news_r_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $-2,188.97 | ▼ close $11,317.82 vs 09:30 $11,347.32 (session -25.70) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $-2,188.97 | ▲ 09:30 equity $11,629.28 vs yday $11,317.82 (+311.46) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `IQV` | 2 | $270.66 | $2.02 | $-4.47 | $-1,649.66 | ▼ -4.47 after sell → book $11,627.26; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `RDNT` | 9 | $73.61 | $2.04 | $-35.64 | $-989.21 | ▼ -35.64 after sell → book $11,625.22; vs 09:30 mark -2.04 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `AVAH` | 50 | $13.12 | $2.16 | $-63.80 | $-335.37 | ▼ -63.80 after sell → book $11,623.06; vs 09:30 mark -2.16 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `BLFS` | 19 | $38.04 | $2.07 | $+25.91 | $385.32 | ▲ +25.91 after sell → book $11,621.00; vs 09:30 mark -2.06 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `LEN` | 12 | $82.00 | $2.05 | $+7.93 | $1,367.28 | ▲ +7.93 after sell → book $11,618.95; vs 09:30 mark -2.05 | union_e_fresh_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `ALMU` | 91 | $13.82 | $2.29 | $+232.96 | $2,622.61 | ▲ +232.96 after sell → book $11,616.66; vs 09:30 mark -2.29 | union_e_fresh_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `CBRL` | 5 | $47.57 | $2.00 | — | $2,382.75 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-11.2; combo leftover $262.26; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 1 | $196.78 | $1.97 | — | $2,184.00 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $262.26; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `GIS` | 7 | $35.74 | $2.01 | — | $1,931.81 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $262.26; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `KBH` | 5 | $47.15 | $2.00 | — | $1,694.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.9; combo leftover $262.26; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `PAYX` | 2 | $109.67 | $2.00 | — | $1,472.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $262.26; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `DXCM` | 2 | $89.50 | $1.80 | — | $1,291.92 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.3; combo leftover $210.39; owner flatten_h5 | — |
| 2026-09-23 09:30 ET | **BUY** | `A` | 1 | $166.54 | $1.67 | — | $1,123.72 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+10.3; combo leftover $210.39; owner flatten_h5 | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 1 | $116.85 | $1.17 | — | $1,005.69 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.3; combo leftover $210.39; owner flatten_h5 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 7 | $27.79 | $1.97 | — | $809.20 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; combo leftover $210.39; owner flatten_h5 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 21 | $9.81 | $2.05 | — | $601.14 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; combo leftover $210.39; owner flatten_h5 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 10 | $20.25 | $2.02 | — | $396.62 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; combo leftover $210.39; owner flatten_h5 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 10 | $20.65 | $2.02 | — | $188.10 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; combo leftover $210.39; owner flatten_h5 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $188.10 | ▼ close $11,529.12 vs 09:30 $11,629.28 (session -64.87) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $188.10 | ▼ 09:30 equity $11,395.05 vs yday $11,529.12 (-134.07) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `ILMN` | 1 | $253.79 | $2.01 | $+15.93 | $439.87 | ▲ +15.93 after sell → book $11,393.04; vs 09:30 mark -2.01 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `TWST` | 2 | $157.72 | $2.02 | $+8.57 | $753.30 | ▲ +8.57 after sell → book $11,391.02; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `RVTY` | 2 | $141.79 | $2.02 | $-15.65 | $1,034.86 | ▼ -15.65 after sell → book $11,389.00; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `IOVA` | 33 | $10.39 | $2.11 | $+0.42 | $1,375.62 | ▲ +0.42 after sell → book $11,386.90; vs 09:30 mark -2.10 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 45 | $7.38 | $2.15 | $-13.72 | $1,705.58 | ▼ -13.72 after sell → book $11,384.75; vs 09:30 mark -2.15 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `AMN` | 9 | $33.82 | $2.04 | $-14.04 | $2,007.92 | ▼ -14.04 after sell → book $11,382.71; vs 09:30 mark -2.04 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,007.92 | ▲ close $11,408.69 vs 09:30 $11,395.05 (session +25.98) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,826.98 | ▲ 09:30 equity $9,460.58 vs yday $9,458.93 (+1.65) | 09:30 open · cash $3,826.98 (unchanged overnight, no fees) · equity $9,460.58 vs prior close $9,458.93 (+1.65) | — |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 1 | $887.00 | $1.99 | — | $2,937.99 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+0.3; combo leftover $1275.66; owner union_e_fresh_h3 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 12 | $38.51 | $2.03 | — | $2,473.84 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.7; combo leftover $489.66; owner flatten_h5 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 64 | $7.65 | $2.18 | — | $1,982.06 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.2; combo leftover $489.66; owner flatten_h5 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **SHORT** | `RSKD` | 252 | $7.85 | $3.36 | — | $3,956.90 | — | news🔴; gate news=bad; list yday_gainer; 🔵; ⚪; ret5=+25.4; combo leftover $1982.06; owner short_news_r_h3 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,956.90 | ▲ close $9,486.95 vs 09:30 $9,460.58 (session +35.93) | 16:00 close · cash $3,956.90 · equity $9,486.95 vs 09:30 $9,460.58 (+26.37; session marks +35.93) · 28 name(s) marked open→close (per-name table). ADMA×14 09:30 $9.52 → close $9.52 +0.00; AEHL×28 09:30 $9.05 → close $9.36 -8.68; ARQT×5 09:30 $26.27 → close $26.27 +0.00; BAND×21 09:30 $61.83 → close $61.83 -0.00; CBRL×4 09:30 $52.39 → close $51.81 -2.32; CTAS×1 09:30 $197.68 → close $197.68 -0.00; CYPH×14 09:30 $4.00 → close $4.12 +1.61; DELL×1 09:30 $536.02 → close $536.02 +0.00; DLO×2 09:30 $13.88 → close $13.88 +0.00; DXCM×1 09:30 $87.47 → close $87.47 +0.00; ECO×12 09:30 $78.22 → close $78.22 +0.00; FIVN×30 09:30 $36.66 → close $36.66 -0.00; FTRE×6 09:30 $20.02 → close $20.02 +0.00; GIS×6 09:30 $34.83 → close $34.83 +0.00; GNRC×5 09:30 $198.05 → close $198.05 +0.00; HALO×1 09:30 $115.36 → close $113.90 -1.46; KBH×4 09:30 $47.65 → close $47.65 +0.00; MGTX×4 09:30 $11.05 → close $11.05 +0.00; MLKN×1 09:30 $19.91 → close $19.91 -0.00; OMER×6 09:30 $20.61 → close $20.08 -3.18; PAYX×2 09:30 $101.59 → close $101.59 +0.00; RBRK×9 09:30 $113.80 → close $113.80 +0.00; TDC×1 09:30 $29.46 → close $29.46 -0.00; VICR×4 09:30 $276.06 → close $276.06 -0.00; COST×1 09:30 $887.00 → close $922.76 +35.76; BLFS×12 09:30 $38.51 → close $38.49 -0.24; MRVI×64 09:30 $7.65 → close $7.60 -3.20; RSKD×252 09:30 $7.85 → close $7.78 +17.64 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `INO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-14 | `VOR` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-14 | `BTSG` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `IREN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `TPG` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `TGTX` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `SLS` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `HIMS` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `TNDM` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-14 | `LUNR` | cash | leftover split 5.18 < 1 share @ 19.17 |
| 2026-08-14 | `ARX` | cash | leftover split 5.18 < 1 share @ 19.57 |
| 2026-08-14 | `AIRO` | cash | leftover split 5.18 < 1 share @ 11.12 |
| 2026-08-14 | `MH` | cash | leftover split 5.18 < 1 share @ 13.55 |
| 2026-08-14 | `CLBT` | cash | leftover split 5.18 < 1 share @ 10.83 |
| 2026-08-14 | `NMAX` | cash | leftover split 5.18 < 1 share @ 9.89 |
| 2026-08-14 | `TLN` | cash | leftover split 8.21 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 8.21 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 8.21 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 8.21 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 8.21 < 1 share @ 57.61 |
| 2026-08-14 | `MARA` | cash | leftover split 8.21 < 1 share @ 9.01 |
| 2026-08-17 | `INO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-17 | `VOR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-17 | `BTSG` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `IREN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `TPG` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `TGTX` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `SLS` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `HIMS` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `TNDM` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `BTBT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `EU` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `LDI` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-17 | `OWL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-17 | `DVN` | cash | leftover split 12.99 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 12.99 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 12.99 < 1 share @ 202.70 |
| 2026-08-17 | `ELF` | cash | leftover split 12.99 < 1 share @ 90.54 |
| 2026-08-18 | `BTSG` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `IREN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `TPG` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `TGTX` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `SLS` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `HIMS` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `TNDM` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `BTBT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `EU` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `LDI` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-18 | `OWL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `TMC` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `TGB` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `DNN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `HNST` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `VERI` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `ZNTL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `APMD` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `HIVE` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new long flatten_h5 |
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new short short_news_r_h3 |
| 2026-08-18 | `CADL` | hard_red | hard-red S=-6.20 sit; no new short short_news_r_h3 |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `KLAR` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-19 | `BTSG` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `IREN` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `TPG` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `TGTX` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `SLS` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `HIMS` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `TNDM` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `LDI` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-19 | `TMC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-19 | `TGB` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-19 | `DNN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-19 | `HNST` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-19 | `VERI` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `ZNTL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `APMD` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `HIVE` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `RNW` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `LUNR` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `AAP` | hard_red | hard-red S=-7.20 sit; no new short short_news_r_h3 |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h3 |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h3 |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h3 |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h3 |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h3 |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h3 |
| 2026-08-19 | `KC` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h3 |
| 2026-08-19 | `KEYS` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h3 |
| 2026-08-20 | `LDI` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-20 | `TMC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-20 | `TGB` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-20 | `DNN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-20 | `HNST` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-21 | `TMC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-21 | `TGB` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-21 | `DNN` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-21 | `HNST` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-21 | `TOYO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `EL` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `DVLT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `AEG` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `ALVO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `ATAT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `ATHM` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
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
| 2026-08-21 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `TEAM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `WMT` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `AQST` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `DE` | cash | leftover split 420.61 < 1 share @ 623.26 |
| 2026-08-24 | `TOYO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AAP` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `EL` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `DVLT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AEG` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ALVO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ATAT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `ATHM` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
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
| 2026-08-24 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `TEAM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `WMT` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AQST` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `FUTU` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `BEKE` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `BJ` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `BKE` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `PSEC` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
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
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `EU` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `BMO` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new short short_news_r_h3 |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new long union_e_fresh_h3 |
| 2026-08-25 | `AG` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `BHP` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `CDE` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `HDSN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `IAG` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `KGC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `NFGC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `WPM` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `FUTU` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `BEKE` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `BJ` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `BKE` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `PSEC` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
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
| 2026-08-25 | `HCA` | cash | leftover split 336.87 < 1 share @ 426.97 |
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
| 2026-08-26 | `BMO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `BNS` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `EH` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `GFI` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `GRRR` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `SHMD` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `AVAH` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `ARE` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `INTU` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `DY` | cash | leftover split 218.29 < 1 share @ 326.91 |
| 2026-08-27 | `AU` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `ARCT` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `AUTL` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `CRDL` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `CRSP` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `BMO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `BNS` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `BZ` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `DKS` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `EH` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `GFI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `GRRR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `SHMD` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `OCUL` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-27 | `INSP` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-27 | `CRMD` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-27 | `RZLT` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-27 | `AVAH` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `ARE` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `SLQT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `TIGR` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `ANF` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `BBWI` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `BOX` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `HCA` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-27 | `BE` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `NEM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `CRM` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `OCUL` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-28 | `INSP` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-28 | `CRMD` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-28 | `RZLT` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-28 | `SLQT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `TIGR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `ANF` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BBWI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BOX` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `HCA` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-28 | `BE` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `ABCL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `AQST` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `NEM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `CRM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BBY` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `BILI` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `CM` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `CMBT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `CSIQ` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `HQY` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `RY` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `TD` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `MT` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `TX` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `MOS` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `OCUL` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `INSP` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `CRMD` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `RZLT` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `HCA` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-31 | `BBY` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `BILI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `CM` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `CMBT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `CSIQ` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `HQY` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `RY` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `TD` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-31 | `RRC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-31 | `CRK` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-31 | `SLI` | min_hold | flatten_h5: dropped but min-hold 2/5 |
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
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new long flatten_h5 |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new long flatten_h5 |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new long flatten_h5 |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new long flatten_h5 |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new long union_e_fresh_h3 |
| 2026-09-01 | `HCA` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-01 | `RRC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-01 | `CRK` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-01 | `SLI` | min_hold | flatten_h5: dropped but min-hold 3/5 |
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
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new long flatten_h5 |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new long union_e_fresh_h3 |
| 2026-09-02 | `RRC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-02 | `CRK` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-02 | `SLI` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new short short_news_r_h3 |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new short short_news_r_h3 |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h3 |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h3 |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h3 |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h3 |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h3 |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h3 |
| 2026-09-04 | `AI` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `AVGO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `CHPT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `CIEN` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `CPB` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `FIVE` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `HPE` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `MEI` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-04 | `HRMY` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-04 | `VSTM` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-04 | `RVTY` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-04 | `SLN` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `AI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `AVGO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `CHPT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `CIEN` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `CPB` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `FIVE` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `HPE` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `MEI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-08 | `ATRC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-08 | `HRMY` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-08 | `CABA` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-08 | `VSTM` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-08 | `RVTY` | min_hold | flatten_h5: dropped but min-hold 2/5 |
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
| 2026-09-08 | `ALEC` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-08 | `BHC` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-08 | `BMEA` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-08 | `OABI` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-08 | `VIR` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-08 | `PIPR` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new long flatten_h5 |
| 2026-09-08 | `SID` | hard_red | hard-red S=-11.47 sit; no new long flatten_h5 |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new long flatten_h5 |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new long flatten_h5 |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `GEMI` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new short short_news_r_h3 |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h3 |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h3 |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h3 |
| 2026-09-09 | `ATRC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-09 | `HRMY` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-09 | `CABA` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-09 | `VSTM` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-09 | `RVTY` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-09 | `AMBA` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `ASAN` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `DOCU` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `DOMO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `GWRE` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `IOT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `LULU` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `MAMA` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `ALEC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-09 | `BHC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-09 | `BMEA` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-09 | `OABI` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-09 | `VIR` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-09 | `GSM` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `PIPR` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `PCG` | hard_red | hard-red S=-13.95 sit; no new long flatten_h5 |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new long flatten_h5 |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new long flatten_h5 |
| 2026-09-09 | `UGP` | hard_red | hard-red S=-13.95 sit; no new long flatten_h5 |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new long flatten_h5 |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new long flatten_h5 |
| 2026-09-09 | `NVS` | hard_red | hard-red S=-13.95 sit; no new short short_news_r_h3 |
| 2026-09-09 | `ABM` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h3 |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h3 |
| 2026-09-09 | `AVO` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h3 |
| 2026-09-09 | `JMKE` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h3 |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h3 |
| 2026-09-09 | `SAIL` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h3 |
| 2026-09-09 | `TTAN` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h3 |
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
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new long flatten_h5 |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new long flatten_h5 |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new long flatten_h5 |
| 2026-09-10 | `OIS` | hard_red | hard-red S=-13.28 sit; no new long flatten_h5 |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new short short_news_r_h3 |
| 2026-09-10 | `BKV` | hard_red | hard-red S=-13.28 sit; no new short short_news_r_h3 |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h3 |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h3 |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h3 |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h3 |
| 2026-09-10 | `JMKE` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h3 |
| 2026-09-10 | `AEO` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h3 |
| 2026-09-10 | `AVAV` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h3 |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h3 |
| 2026-09-11 | `ALEC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-11 | `BHC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-11 | `BMEA` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-11 | `OABI` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-11 | `VIR` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-14 | `ORCL` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `DBI` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `ADBE` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `CPRT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `DSGX` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `KR` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `LPTH` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `REF` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-14 | `OVID` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-14 | `SANM` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-14 | `COHU` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-14 | `QRVO` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `RWT` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `CRDL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new long flatten_h5 |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new long flatten_h5 |
| 2026-09-14 | `BG` | hard_red | hard-red S=-11.00 sit; no new long flatten_h5 |
| 2026-09-15 | `ORCL` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `DBI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `ADBE` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `CPRT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `DSGX` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `KR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `LPTH` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `REF` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `OVID` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `SANM` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `NVT` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `COHU` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `QRVO` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `RWT` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `CRDL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `MYGN` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new long flatten_h5 |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new long flatten_h5 |
| 2026-09-15 | `WAY` | hard_red | hard-red S=-3.84 sit; no new long flatten_h5 |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new long flatten_h5 |
| 2026-09-15 | `TYRA` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new short short_news_r_h3 |
| 2026-09-15 | `FPS` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h3 |
| 2026-09-15 | `HITI` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h3 |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h3 |
| 2026-09-15 | `UROY` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h3 |
| 2026-09-16 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-16 | `OVID` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-16 | `SANM` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-16 | `NVT` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-16 | `COHU` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-17 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-17 | `OVID` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-17 | `SANM` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-17 | `NVT` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-17 | `COHU` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-17 | `FPS` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-17 | `TCOM` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-17 | `IQV` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-17 | `RDNT` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-17 | `AVAH` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-17 | `BLFS` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-17 | `GFR` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `FPS` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `TCOM` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `IQV` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-18 | `RDNT` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-18 | `AVAH` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-18 | `BLFS` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-18 | `BBNX` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `GFR` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `LEN` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `ALMU` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `ILMN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-18 | `TWST` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-18 | `RVTY` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-18 | `IOVA` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-18 | `PGEN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-18 | `AMN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-18 | `BULL` | min_hold | short_news_r_h3: dropped but min-hold 1/3 |
| 2026-09-21 | `IQV` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-21 | `RDNT` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-21 | `AVAH` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-21 | `BLFS` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-21 | `LEN` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `ALMU` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `ILMN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-21 | `TWST` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-21 | `RVTY` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-21 | `AMN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-21 | `BULL` | min_hold | short_news_r_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `RBRK` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `DELL` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `GNRC` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `VICR` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `ECO` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `FIVN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `A` | cash | leftover split 19.57 < 1 share @ 157.87 |
| 2026-09-21 | `HUM` | cash | leftover split 19.57 < 1 share @ 386.20 |
| 2026-09-21 | `DXCM` | cash | leftover split 19.57 < 1 share @ 88.83 |
| 2026-09-21 | `AMD` | cash | leftover split 82.98 < 1 share @ 583.88 |
| 2026-09-22 | `IQV` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-22 | `RDNT` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-22 | `AVAH` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-22 | `BLFS` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-22 | `LEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ALMU` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ILMN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-22 | `TWST` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-22 | `RVTY` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-22 | `IOVA` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-22 | `PGEN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-22 | `AMN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-22 | `RBRK` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `DELL` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `GNRC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `VICR` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `ECO` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `FIVN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `MGTX` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-22 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-23 | `ILMN` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-23 | `TWST` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-23 | `RVTY` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-23 | `IOVA` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-23 | `AMN` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-23 | `RBRK` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `DELL` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `GNRC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `VICR` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `ECO` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `FIVN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `MGTX` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-23 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-24 | `RBRK` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `DELL` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `GNRC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `VICR` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `ECO` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `FIVN` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `MGTX` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-24 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-24 | `CBRL` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `CTAS` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `GIS` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `KBH` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `PAYX` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `DXCM` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-24 | `A` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-24 | `HALO` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-24 | `ARQT` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-24 | `ADMA` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-24 | `FTRE` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-24 | `OMER` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `CHKP` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `S` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `BAH` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `PANW` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `VOYG` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `BMEA` | hard_red | hard-red S=-7.66 sit; no new short short_news_r_h3 |
| 2026-09-24 | `BB` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h3 |
| 2026-09-24 | `DRI` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h3 |
| 2026-09-24 | `FUL` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h3 |
| 2026-09-24 | `NEOV` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h3 |
| 2026-09-24 | `SNX` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h3 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `RBRK` | 11 | 2026-09-18 @ $108.55 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+21.3; combo leftover $1216.32; owner flatten_h5 |
| `DELL` | 2 | 2026-09-18 @ $593.15 | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+16.1; combo leftover $1216.32; owner flatten_h5 |
| `GNRC` | 5 | 2026-09-18 @ $209.52 | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.1; combo leftover $1216.32; owner flatten_h5 |
| `VICR` | 5 | 2026-09-18 @ $219.62 | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+21.5; combo leftover $1216.32; owner flatten_h5 |
| `ECO` | 14 | 2026-09-18 @ $85.00 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+18.3; combo leftover $1216.32; owner flatten_h5 |
| `FIVN` | 35 | 2026-09-18 @ $34.44 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+14.0; combo leftover $1216.32; owner flatten_h5 |
| `MGTX` | 1 | 2026-09-21 @ $13.47 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.6; combo leftover $19.57; owner flatten_h5 |
| `CYPH` | 4 | 2026-09-21 @ $4.00 | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); ret5=+58.9; combo leftover $19.57; owner flatten_h5 |
| `AEHL` | 10 | 2026-09-21 @ $8.26 | news🔴; gate news=bad; list yday_mover; ret5=+7.7; combo leftover $82.98; owner short_news_r_h3 |
| `CBRL` | 5 | 2026-09-23 @ $47.57 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-11.2; combo leftover $262.26; owner union_e_fresh_h3 |
| `CTAS` | 1 | 2026-09-23 @ $196.78 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-2.0; combo leftover $262.26; owner union_e_fresh_h3 |
| `GIS` | 7 | 2026-09-23 @ $35.74 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $262.26; owner union_e_fresh_h3 |
| `KBH` | 5 | 2026-09-23 @ $47.15 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.9; combo leftover $262.26; owner union_e_fresh_h3 |
| `PAYX` | 2 | 2026-09-23 @ $109.67 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $262.26; owner union_e_fresh_h3 |
| `DXCM` | 2 | 2026-09-23 @ $89.50 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.3; combo leftover $210.39; owner flatten_h5 |
| `A` | 1 | 2026-09-23 @ $166.54 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+10.3; combo leftover $210.39; owner flatten_h5 |
| `HALO` | 1 | 2026-09-23 @ $116.85 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.3; combo leftover $210.39; owner flatten_h5 |
| `ARQT` | 7 | 2026-09-23 @ $27.79 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; combo leftover $210.39; owner flatten_h5 |
| `ADMA` | 21 | 2026-09-23 @ $9.81 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; combo leftover $210.39; owner flatten_h5 |
| `FTRE` | 10 | 2026-09-23 @ $20.25 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; combo leftover $210.39; owner flatten_h5 |
| `OMER` | 10 | 2026-09-23 @ $20.65 | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; combo leftover $210.39; owner flatten_h5 |
