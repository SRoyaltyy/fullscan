# Factor mine action — `combo_ef_3070_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared union_e_fresh_h3/flatten_h5 w=0.3,0.7 net=priority

Cash book **-12.65%** ($8,735) · signal-only (no cash/fees) was —. Starts YES **7/30**. Fills 318 · skips 540 · realized $+600.22.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: union_e_fresh_h3 30%, flatten_h5 70%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: union_e_fresh_h3 30%, flatten_h5 70%.
- Member: union_e_fresh_h3 (30% · long · hold 3).
- Member: flatten_h5 (70% · long · hold 5).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $2,380.39.

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
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1851 | $0.81 | $20.55 | — | $8,480.14 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+13.2; combo leftover $1500.00; owner union_e_fresh_h3 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 68 | $22.01 | $2.19 | — | $6,981.27 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; ⚪; ret5=+0.3; combo leftover $1500.00; owner union_e_fresh_h3 | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 16 | $59.80 | $2.04 | — | $6,022.43 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; combo leftover $997.32; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 21 | $45.98 | $2.05 | — | $5,054.80 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+12.3; combo leftover $997.32; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 19 | $50.62 | $2.05 | — | $4,090.91 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+6.2; combo leftover $997.32; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 20 | $49.70 | $2.05 | — | $3,094.86 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; combo leftover $997.32; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 85 | $11.70 | $2.25 | — | $2,098.12 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; combo leftover $997.32; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 33 | $29.74 | $2.09 | — | $1,114.61 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; combo leftover $997.32; owner flatten_h5 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 42 | $23.33 | $2.12 | — | $132.63 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+19.7; combo leftover $997.32; owner flatten_h5 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $132.63 | ▲ close $10,253.94 vs 09:30 $10,000.00 (session +291.32) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $132.63 | ▲ 09:30 equity $10,295.29 vs yday $10,253.94 (+41.35) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 3 | $1.50 | $0.05 | — | $128.08 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten; 🔵; ⚪; ret5=+9.2; combo leftover $4.97; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `EU` | 4 | $1.18 | $0.06 | — | $123.30 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ⚪; ret5=-0.9; combo leftover $4.97; owner union_e_fresh_h3 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $114.19 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-13.5; combo leftover $17.61; owner flatten_h5 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 18 | $0.94 | $0.22 | — | $97.11 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.5; combo leftover $17.61; owner flatten_h5 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.11 | ▲ close $10,580.11 vs 09:30 $10,295.29 (session +285.25) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.11 | ▼ 09:30 equity $10,542.82 vs yday $10,580.11 (-37.29) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 2 | $4.05 | $0.09 | — | $88.92 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; combo leftover $12.14; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 1 | $8.46 | $0.09 | — | $80.37 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.4; combo leftover $12.14; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 3 | $3.24 | $0.11 | — | $70.55 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; combo leftover $12.14; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 2 | $4.81 | $0.10 | — | $60.82 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-11.4; combo leftover $12.14; owner flatten_h5 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $60.82 | ▲ close $10,686.27 vs 09:30 $10,542.82 (session +143.83) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $60.82 | ▼ 09:30 equity $10,561.40 vs yday $10,686.27 (-124.87) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1851 | $1.14 | $24.20 | $+566.08 | $2,146.76 | ▲ +566.08 after sell → book $10,537.20; vs 09:30 mark -24.20 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 68 | $22.82 | $2.22 | $+50.67 | $3,696.30 | ▲ +50.67 after sell → book $10,534.98; vs 09:30 mark -2.22 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,696.30 | ▲ close $10,606.10 vs 09:30 $10,561.40 (session +71.11) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,696.30 | ▲ 09:30 equity $10,692.43 vs yday $10,606.10 (+86.33) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 3 | $1.42 | $0.07 | $-0.37 | $3,700.49 | ▼ -0.37 after sell → book $10,692.36; vs 09:30 mark -0.07 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `EU` | 4 | $1.07 | $0.07 | $-0.57 | $3,704.70 | ▼ -0.57 after sell → book $10,692.28; vs 09:30 mark -0.08 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,704.70 | ▲ close $10,847.90 vs 09:30 $10,692.43 (session +155.63) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,704.70 | ▼ 09:30 equity $10,796.18 vs yday $10,847.90 (-51.72) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 16 | $58.64 | $2.06 | $-22.66 | $4,640.88 | ▼ -22.66 after sell → book $10,794.12; vs 09:30 mark -2.06 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 21 | $42.46 | $2.07 | $-78.05 | $5,530.47 | ▼ -78.05 after sell → book $10,792.04; vs 09:30 mark -2.08 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 19 | $53.06 | $2.07 | $+42.19 | $6,536.54 | ▲ +42.19 after sell → book $10,789.98; vs 09:30 mark -2.06 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGTX` | 20 | $51.65 | $2.07 | $+34.88 | $7,567.47 | ▲ +34.88 after sell → book $10,787.91; vs 09:30 mark -2.07 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 85 | $13.84 | $2.27 | $+177.39 | $8,741.60 | ▲ +177.39 after sell → book $10,785.64; vs 09:30 mark -2.27 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `HIMS` | 33 | $30.66 | $2.11 | $+26.16 | $9,751.27 | ▲ +26.16 after sell → book $10,783.53; vs 09:30 mark -2.11 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 42 | $23.11 | $2.14 | $-13.49 | $10,719.75 | ▼ -13.49 after sell → book $10,781.39; vs 09:30 mark -2.14 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 4 | $97.43 | $2.00 | — | $10,328.03 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; combo leftover $401.99; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `TOYO` | 90 | $4.43 | $2.26 | — | $9,927.07 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-23.1; combo leftover $401.99; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `DVLT` | 1339 | $0.30 | $8.03 | — | $9,517.34 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-3.2; combo leftover $401.99; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 8 | $46.85 | $2.01 | — | $9,140.52 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.0; combo leftover $401.99; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 44 | $9.01 | $2.12 | — | $8,741.96 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-1.3; combo leftover $401.99; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 103 | $3.89 | $2.30 | — | $8,338.99 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.5; combo leftover $401.99; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 11 | $34.05 | $2.02 | — | $7,962.42 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+9.3; combo leftover $401.99; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 17 | $22.44 | $2.04 | — | $7,578.90 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.1; combo leftover $401.99; owner union_e_fresh_h3 | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 46 | $20.55 | $2.13 | — | $6,631.47 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; combo leftover $947.36; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 10 | $91.01 | $2.02 | — | $5,719.35 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; combo leftover $947.36; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 45 | $20.65 | $2.12 | — | $4,787.98 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; combo leftover $947.36; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 164 | $5.77 | $2.48 | — | $3,839.21 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; combo leftover $947.36; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 48 | $19.63 | $2.13 | — | $2,894.84 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; combo leftover $947.36; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 31 | $29.63 | $2.08 | — | $1,974.23 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; combo leftover $947.36; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 541 | $1.75 | $6.98 | — | $1,020.50 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; combo leftover $947.36; owner flatten_h5 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 6 | $144.54 | $2.01 | — | $151.25 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; combo leftover $947.36; owner flatten_h5 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $151.25 | ▲ close $10,942.67 vs 09:30 $10,796.18 (session +206.03) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $151.25 | ▲ 09:30 equity $11,158.74 vs yday $10,942.67 (+216.07) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 1 | $11.70 | $0.14 | $+2.46 | $162.81 | ▲ +2.46 after sell → book $11,158.60; vs 09:30 mark -0.14 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 18 | $0.87 | $0.23 | $-1.71 | $178.19 | ▼ -1.71 after sell → book $11,158.37; vs 09:30 mark -0.23 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 3 | $2.30 | $0.08 | — | $171.21 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.0; combo leftover $7.64; owner union_e_fresh_h3 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $153.83 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; combo leftover $21.40; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $142.59 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; combo leftover $21.40; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 8 | $2.47 | $0.22 | — | $122.61 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; combo leftover $21.40; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 11 | $1.93 | $0.25 | — | $101.13 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; combo leftover $21.40; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 16 | $1.32 | $0.26 | — | $79.75 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; combo leftover $21.40; owner flatten_h5 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $79.75 | ▲ close $11,221.00 vs 09:30 $11,158.74 (session +63.73) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.75 | ▲ 09:30 equity $11,312.17 vs yday $11,221.00 (+91.17) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 2 | $4.62 | $0.12 | $+0.94 | $88.89 | ▲ +0.94 after sell → book $11,312.06; vs 09:30 mark -0.11 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `TGB` | 1 | $9.26 | $0.12 | $+0.60 | $98.03 | ▲ +0.60 after sell → book $11,311.94; vs 09:30 mark -0.12 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 3 | $3.50 | $0.13 | $+0.54 | $108.40 | ▲ +0.54 after sell → book $11,311.81; vs 09:30 mark -0.13 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `HNST` | 2 | $5.05 | $0.13 | $+0.25 | $118.37 | ▲ +0.25 after sell → book $11,311.68; vs 09:30 mark -0.13 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $118.37 | ▲ close $11,328.14 vs 09:30 $11,312.17 (session +16.46) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $118.37 | ▼ 09:30 equity $11,206.36 vs yday $11,328.14 (-121.78) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `EL` | 4 | $104.00 | $2.02 | $+22.26 | $532.35 | ▲ +22.26 after sell → book $11,204.34; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `TOYO` | 90 | $4.42 | $2.28 | $-5.44 | $927.86 | ▼ -5.44 after sell → book $11,202.05; vs 09:30 mark -2.29 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DVLT` | 1339 | $0.31 | $8.40 | $-3.04 | $1,334.55 | ▼ -3.04 after sell → book $11,193.65; vs 09:30 mark -8.40 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AAP` | 8 | $43.63 | $2.03 | $-29.81 | $1,681.56 | ▼ -29.81 after sell → book $11,191.62; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AEG` | 44 | $9.23 | $2.14 | $+5.42 | $2,085.54 | ▲ +5.42 after sell → book $11,189.48; vs 09:30 mark -2.14 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ALVO` | 103 | $5.24 | $2.33 | $+134.42 | $2,622.93 | ▲ +134.42 after sell → book $11,187.15; vs 09:30 mark -2.33 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATAT` | 11 | $34.72 | $2.04 | $+3.30 | $3,002.81 | ▲ +3.30 after sell → book $11,185.11; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATHM` | 17 | $21.85 | $2.06 | $-14.13 | $3,372.20 | ▼ -14.13 after sell → book $11,183.05; vs 09:30 mark -2.06 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 1 | $88.94 | $0.89 | — | $3,282.36 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.9; combo leftover $126.46; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 8 | $15.28 | $1.25 | — | $3,158.88 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-0.7; combo leftover $126.46; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 24 | $5.10 | $1.30 | — | $3,035.18 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-8.9; combo leftover $126.46; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 2 | $47.89 | $0.96 | — | $2,938.44 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ⚪; ret5=+14.0; combo leftover $126.46; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 9 | $13.92 | $1.28 | — | $2,811.88 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+5.9; combo leftover $126.46; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 27 | $4.54 | $1.31 | — | $2,687.85 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-14.6; combo leftover $126.46; owner union_e_fresh_h3 | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 18 | $23.77 | $2.04 | — | $2,257.95 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.0; combo leftover $447.98; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 40 | $10.98 | $2.11 | — | $1,816.64 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+1.2; combo leftover $447.98; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 7 | $61.19 | $2.01 | — | $1,386.30 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+7.4; combo leftover $447.98; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 53 | $8.35 | $2.15 | — | $941.60 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.0; combo leftover $447.98; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 90 | $4.94 | $2.26 | — | $494.74 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; combo leftover $447.98; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 1 | $426.97 | $1.99 | — | $65.78 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.0; combo leftover $447.98; owner flatten_h5 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $65.78 | ▲ close $11,482.02 vs 09:30 $11,206.36 (session +318.52) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $65.78 | ▼ 09:30 equity $11,334.42 vs yday $11,482.02 (-147.60) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `PSEC` | 3 | $2.35 | $0.10 | $-0.03 | $72.73 | ▼ -0.03 after sell → book $11,334.32; vs 09:30 mark -0.10 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 17 | $0.58 | $0.15 | — | $62.67 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_mover; 🔵; ret5=-27.5; combo leftover $10.39; owner union_e_fresh_h3 | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 1 | $5.21 | $0.06 | — | $57.40 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list ohlc_hot,earn_react; 🔵; ret5=+14.3; combo leftover $10.39; owner union_e_fresh_h3 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.40 | ▼ close $11,238.72 vs 09:30 $11,334.42 (session -95.39) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.40 | ▲ 09:30 equity $11,258.62 vs yday $11,238.72 (+19.90) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 46 | $20.93 | $2.15 | $+13.20 | $1,018.03 | ▲ +13.20 after sell → book $11,256.47; vs 09:30 mark -2.15 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 10 | $95.52 | $2.04 | $+41.04 | $1,971.19 | ▲ +41.04 after sell → book $11,254.43; vs 09:30 mark -2.04 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 45 | $21.31 | $2.15 | $+25.43 | $2,928.00 | ▲ +25.43 after sell → book $11,252.29; vs 09:30 mark -2.14 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 164 | $5.49 | $2.52 | $-50.92 | $3,825.84 | ▼ -50.92 after sell → book $11,249.77; vs 09:30 mark -2.52 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 48 | $21.47 | $2.15 | $+84.03 | $4,854.24 | ▲ +84.03 after sell → book $11,247.61; vs 09:30 mark -2.16 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 31 | $32.32 | $2.10 | $+79.20 | $5,854.06 | ▲ +79.20 after sell → book $11,245.51; vs 09:30 mark -2.10 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 541 | $1.91 | $7.08 | $+72.50 | $6,880.29 | ▲ +72.50 after sell → book $11,238.43; vs 09:30 mark -7.08 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 6 | $155.89 | $2.03 | $+64.06 | $7,813.61 | ▲ +64.06 after sell → book $11,236.41; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `BBY` | 3 | $80.60 | $2.00 | — | $7,569.81 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.0; combo leftover $293.01; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `BILI` | 18 | $16.18 | $2.04 | — | $7,276.52 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-6.7; combo leftover $293.01; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CM` | 2 | $118.77 | $2.00 | — | $7,036.99 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.3; combo leftover $293.01; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CMBT` | 16 | $17.78 | $2.04 | — | $6,750.47 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.2; combo leftover $293.01; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `CSIQ` | 21 | $13.41 | $2.05 | — | $6,466.81 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.1; combo leftover $293.01; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `HQY` | 3 | $97.16 | $2.00 | — | $6,173.33 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.5; combo leftover $293.01; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `RY` | 1 | $206.82 | $1.99 | — | $5,964.51 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-0.2; combo leftover $293.01; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `TD` | 2 | $120.17 | $2.00 | — | $5,722.18 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.9; combo leftover $293.01; owner union_e_fresh_h3 | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 46 | $41.44 | $2.13 | — | $3,813.81 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.1; combo leftover $1907.39; owner flatten_h5 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 132 | $14.42 | $2.39 | — | $1,907.98 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; combo leftover $1907.39; owner flatten_h5 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 730 | $2.60 | $9.42 | — | $0.57 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+13.0; combo leftover $1907.39; owner flatten_h5 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.57 | ▲ close $11,258.49 vs 09:30 $11,258.62 (session +52.14) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.57 | ▲ 09:30 equity $11,306.01 vs yday $11,258.49 (+47.52) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 1 | $16.44 | $0.19 | $-1.12 | $16.82 | ▼ -1.12 after sell → book $11,305.83; vs 09:30 mark -0.18 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 1 | $15.43 | $0.18 | $+4.01 | $32.07 | ▲ +4.01 after sell → book $11,305.65; vs 09:30 mark -0.18 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 8 | $2.35 | $0.23 | $-1.41 | $50.64 | ▼ -1.41 after sell → book $11,305.42; vs 09:30 mark -0.23 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 11 | $2.06 | $0.28 | $+0.91 | $73.02 | ▲ +0.91 after sell → book $11,305.14; vs 09:30 mark -0.28 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 16 | $1.82 | $0.36 | $+7.38 | $101.78 | ▲ +7.38 after sell → book $11,304.78; vs 09:30 mark -0.36 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `BNS` | 1 | $93.30 | $0.96 | $+2.51 | $194.12 | ▲ +2.51 after sell → book $11,303.82; vs 09:30 mark -0.96 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 8 | $18.15 | $1.50 | $+20.22 | $337.83 | ▲ +20.22 after sell → book $11,302.33; vs 09:30 mark -1.49 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EH` | 24 | $4.58 | $1.19 | $-14.97 | $446.56 | ▼ -14.97 after sell → book $11,301.13; vs 09:30 mark -1.20 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GFI` | 2 | $48.42 | $0.99 | $-0.90 | $542.40 | ▼ -0.90 after sell → book $11,300.14; vs 09:30 mark -0.99 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GRRR` | 9 | $15.66 | $1.46 | $+12.92 | $681.89 | ▲ +12.92 after sell → book $11,298.68; vs 09:30 mark -1.46 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SHMD` | 27 | $3.38 | $1.01 | $-33.78 | $772.13 | ▼ -33.78 after sell → book $11,297.67; vs 09:30 mark -1.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 6 | $15.01 | $0.92 | — | $681.15 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+3.7; combo leftover $96.52; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 24 | $3.88 | $1.00 | — | $587.03 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-8.6; combo leftover $96.52; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 2 | $44.40 | $0.89 | — | $497.34 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+0.4; combo leftover $96.52; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 3 | $24.69 | $0.75 | — | $422.52 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.8; combo leftover $96.52; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 11 | $8.35 | $0.95 | — | $329.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+5.1; combo leftover $96.52; owner union_e_fresh_h3 | — |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 2 | $37.65 | $0.76 | — | $253.67 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-4.9; combo leftover $96.52; owner union_e_fresh_h3 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $253.67 | ▼ close $11,036.76 vs 09:30 $11,306.01 (session -255.63) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $253.67 | ▲ 09:30 equity $11,112.50 vs yday $11,036.76 (+75.74) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `SLQT` | 17 | $0.51 | $0.16 | $-1.55 | $262.18 | ▼ -1.55 after sell → book $11,112.34; vs 09:30 mark -0.16 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `TIGR` | 1 | $5.00 | $0.07 | $-0.34 | $267.11 | ▼ -0.34 after sell → book $11,112.27; vs 09:30 mark -0.07 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $267.11 | ▲ close $11,136.56 vs 09:30 $11,112.50 (session +24.29) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $267.11 | ▲ 09:30 equity $11,329.12 vs yday $11,136.56 (+192.56) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `MOS` | 18 | $23.94 | $2.06 | $-1.05 | $695.96 | ▼ -1.05 after sell → book $11,327.05; vs 09:30 mark -2.07 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `OCUL` | 40 | $10.42 | $2.13 | $-26.64 | $1,110.63 | ▼ -26.64 after sell → book $11,324.92; vs 09:30 mark -2.13 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `INSP` | 7 | $63.00 | $2.03 | $+8.63 | $1,549.60 | ▲ +8.63 after sell → book $11,322.89; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 53 | $8.25 | $2.17 | $-9.62 | $1,984.68 | ▼ -9.62 after sell → book $11,320.72; vs 09:30 mark -2.17 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 90 | $4.64 | $2.28 | $-31.54 | $2,400.00 | ▼ -31.54 after sell → book $11,318.44; vs 09:30 mark -2.28 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `HCA` | 1 | $418.43 | $2.01 | $-12.55 | $2,816.41 | ▼ -12.55 after sell → book $11,316.42; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `BBY` | 3 | $79.83 | $2.02 | $-6.33 | $3,053.89 | ▼ -6.33 after sell → book $11,314.41; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `BILI` | 18 | $15.97 | $2.06 | $-7.89 | $3,339.28 | ▼ -7.89 after sell → book $11,312.34; vs 09:30 mark -2.07 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CM` | 2 | $113.66 | $2.02 | $-14.23 | $3,564.59 | ▼ -14.23 after sell → book $11,310.33; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CMBT` | 16 | $18.28 | $2.06 | $+3.90 | $3,855.01 | ▲ +3.90 after sell → book $11,308.27; vs 09:30 mark -2.06 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CSIQ` | 21 | $12.18 | $2.07 | $-29.96 | $4,108.71 | ▼ -29.96 after sell → book $11,306.19; vs 09:30 mark -2.08 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `HQY` | 3 | $96.65 | $2.02 | $-5.55 | $4,396.65 | ▼ -5.55 after sell → book $11,304.18; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `RY` | 1 | $203.78 | $2.01 | $-7.05 | $4,598.41 | ▼ -7.05 after sell → book $11,302.16; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `TD` | 2 | $120.54 | $2.02 | $-3.27 | $4,837.48 | ▼ -3.27 after sell → book $11,300.15; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,837.48 | ▼ close $11,213.33 vs 09:30 $11,329.12 (session -86.82) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,837.48 | ▼ 09:30 equity $11,154.14 vs yday $11,213.33 (-59.19) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `BBAR` | 6 | $15.01 | $0.94 | $-1.86 | $4,926.60 | ▼ -1.86 after sell → book $11,153.20; vs 09:30 mark -0.94 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FINV` | 24 | $3.32 | $0.89 | $-15.33 | $5,005.39 | ▼ -15.33 after sell → book $11,152.31; vs 09:30 mark -0.89 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FRO` | 2 | $44.17 | $0.91 | $-2.26 | $5,092.82 | ▼ -2.26 after sell → book $11,151.40; vs 09:30 mark -0.91 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GAP` | 3 | $21.97 | $0.69 | $-9.60 | $5,158.04 | ▼ -9.60 after sell → book $11,150.71; vs 09:30 mark -0.69 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `HAFN` | 11 | $8.58 | $1.00 | $+0.58 | $5,251.42 | ▲ +0.58 after sell → book $11,149.71; vs 09:30 mark -1.00 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `IREN` | 2 | $35.80 | $0.74 | $-5.20 | $5,322.27 | ▼ -5.20 after sell → book $11,148.97; vs 09:30 mark -0.74 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,322.27 | ▼ close $11,086.93 vs 09:30 $11,154.14 (session -62.04) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,322.27 | ▲ 09:30 equity $11,131.15 vs yday $11,086.93 (+44.22) | — | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 46 | $42.43 | $2.15 | $+41.26 | $7,271.90 | ▲ +41.26 after sell → book $11,129.00; vs 09:30 mark -2.15 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `CRK` | 132 | $15.45 | $2.42 | $+131.15 | $9,308.88 | ▲ +131.15 after sell → book $11,126.58; vs 09:30 mark -2.42 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 730 | $2.49 | $9.55 | $-99.27 | $11,117.02 | ▼ -99.27 after sell → book $11,117.02; vs 09:30 mark -9.56 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 38 | $10.74 | $2.10 | — | $10,706.61 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+8.5; combo leftover $416.89; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 1 | $351.74 | $1.99 | — | $10,352.88 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+3.3; combo leftover $416.89; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 60 | $6.90 | $2.17 | — | $9,936.71 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.8; combo leftover $416.89; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 1 | $354.49 | $1.99 | — | $9,580.22 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-12.3; combo leftover $416.89; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 18 | $22.32 | $2.04 | — | $9,176.42 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+2.4; combo leftover $416.89; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 1 | $257.00 | $1.99 | — | $8,917.43 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-5.5; combo leftover $416.89; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 8 | $47.60 | $2.01 | — | $8,534.61 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.2; combo leftover $416.89; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 27 | $15.09 | $2.07 | — | $8,125.11 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+6.1; combo leftover $416.89; owner union_e_fresh_h3 | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 30 | $52.88 | $2.08 | — | $6,536.63 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; combo leftover $1625.02; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 37 | $42.93 | $2.10 | — | $4,946.12 | — | baseline list, no extra gate; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.1; combo leftover $1625.02; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 447 | $3.63 | $5.77 | — | $3,317.74 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.8; combo leftover $1625.02; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 202 | $8.03 | $2.61 | — | $1,693.08 | — | baseline list, no extra gate; list flatten,ohlc_hot,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.5; combo leftover $1625.02; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 12 | $132.45 | $2.03 | — | $101.65 | — | baseline list, no extra gate; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.6; combo leftover $1625.02; owner flatten_h5 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $101.65 | ▼ close $11,080.43 vs 09:30 $11,131.15 (session -5.63) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $101.65 | ▼ 09:30 equity $11,037.26 vs yday $11,080.43 (-43.17) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 1 | $3.62 | $0.04 | — | $98.00 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-3.1; combo leftover $3.81; owner union_e_fresh_h3 | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 6 | $2.52 | $0.17 | — | $82.71 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+5.0; combo leftover $16.33; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 2 | $6.71 | $0.14 | — | $69.15 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; combo leftover $16.33; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 8 | $1.90 | $0.18 | — | $53.77 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.7; combo leftover $16.33; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 3 | $4.78 | $0.15 | — | $39.28 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; combo leftover $16.33; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 10 | $1.59 | $0.19 | — | $23.19 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; combo leftover $16.33; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 1 | $11.31 | $0.12 | — | $11.76 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+1.2; combo leftover $16.33; owner flatten_h5 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.76 | ▲ close $11,125.97 vs 09:30 $11,037.26 (session +89.68) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.76 | ▲ 09:30 equity $11,174.59 vs yday $11,125.97 (+48.62) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.76 | ▼ close $11,030.85 vs 09:30 $11,174.59 (session -143.74) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.76 | ▼ 09:30 equity $10,983.87 vs yday $11,030.85 (-46.98) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `AI` | 38 | $10.51 | $2.12 | $-13.16 | $409.02 | ▼ -13.16 after sell → book $10,981.75; vs 09:30 mark -2.12 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 1 | $366.23 | $2.01 | $+10.48 | $773.24 | ▲ +10.48 after sell → book $10,979.74; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CHPT` | 60 | $9.39 | $2.19 | $+145.04 | $1,334.45 | ▲ +145.04 after sell → book $10,977.55; vs 09:30 mark -2.19 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CIEN` | 1 | $341.90 | $2.01 | $-16.60 | $1,674.33 | ▼ -16.60 after sell → book $10,975.53; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CPB` | 18 | $21.67 | $2.06 | $-15.81 | $2,062.33 | ▼ -15.81 after sell → book $10,973.47; vs 09:30 mark -2.06 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FIVE` | 1 | $252.92 | $2.01 | $-8.09 | $2,313.24 | ▼ -8.09 after sell → book $10,971.46; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HPE` | 8 | $56.94 | $2.03 | $+70.67 | $2,766.72 | ▲ +70.67 after sell → book $10,969.42; vs 09:30 mark -2.04 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MEI` | 27 | $13.84 | $2.09 | $-37.91 | $3,138.31 | ▼ -37.91 after sell → book $10,967.33; vs 09:30 mark -2.09 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,138.31 | ▼ close $10,742.74 vs 09:30 $10,983.87 (session -224.60) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,138.31 | ▼ 09:30 equity $10,660.82 vs yday $10,742.74 (-81.92) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `DOMO` | 1 | $3.76 | $0.06 | $+0.05 | $3,142.01 | ▲ +0.05 after sell → book $10,660.76; vs 09:30 mark -0.06 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,142.01 | ▼ close $10,542.72 vs 09:30 $10,660.82 (session -118.04) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,142.01 | ▲ 09:30 equity $10,615.26 vs yday $10,542.72 (+72.54) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `ATRC` | 30 | $53.53 | $2.10 | $+15.32 | $4,745.81 | ▲ +15.32 after sell → book $10,613.16; vs 09:30 mark -2.10 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `HRMY` | 37 | $41.30 | $2.12 | $-64.53 | $6,271.79 | ▼ -64.53 after sell → book $10,611.04; vs 09:30 mark -2.12 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CABA` | 447 | $2.77 | $5.85 | $-396.04 | $7,504.12 | ▼ -396.04 after sell → book $10,605.18; vs 09:30 mark -5.86 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `VSTM` | 202 | $7.70 | $2.65 | $-71.92 | $9,056.87 | ▼ -71.92 after sell → book $10,602.53; vs 09:30 mark -2.65 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `RVTY` | 12 | $122.40 | $2.05 | $-124.67 | $10,523.63 | ▼ -124.67 after sell → book $10,600.49; vs 09:30 mark -2.04 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 2 | $164.43 | $2.00 | — | $10,192.77 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list flatten,earn_react; ⚪; ret5=+4.9; combo leftover $394.64; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 66 | $5.91 | $2.19 | — | $9,800.52 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer,yday_mover,ohlc_hot; ret5=+14.1; combo leftover $394.64; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 1 | $242.17 | $1.99 | — | $9,556.36 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-11.1; combo leftover $394.64; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `CPRT` | 12 | $32.01 | $2.03 | — | $9,170.21 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.4; combo leftover $394.64; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `DSGX` | 5 | $71.71 | $2.00 | — | $8,809.66 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-9.1; combo leftover $394.64; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `KR` | 7 | $56.02 | $2.01 | — | $8,415.51 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-2.2; combo leftover $394.64; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `LPTH` | 42 | $9.37 | $2.12 | — | $8,019.85 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=+1.5; combo leftover $394.64; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `REF` | 30 | $13.10 | $2.08 | — | $7,624.77 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-6.9; combo leftover $394.64; owner union_e_fresh_h3 | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 93 | $16.28 | $2.27 | — | $6,108.46 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-1.1; combo leftover $1524.95; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 558 | $2.73 | $7.20 | — | $4,577.92 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-3.0; combo leftover $1524.95; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 7 | $206.84 | $2.01 | — | $3,128.03 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+8.3; combo leftover $1524.95; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 9 | $157.78 | $2.02 | — | $1,705.99 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+4.7; combo leftover $1524.95; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 27 | $56.09 | $2.07 | — | $189.49 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+19.6; combo leftover $1524.95; owner flatten_h5 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $189.49 | ▲ close $10,676.60 vs 09:30 $10,615.26 (session +108.10) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $189.49 | ▼ 09:30 equity $10,399.52 vs yday $10,676.60 (-277.08) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ALEC` | 6 | $2.15 | $0.17 | $-2.56 | $202.23 | ▼ -2.56 after sell → book $10,399.36; vs 09:30 mark -0.16 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BHC` | 2 | $5.93 | $0.14 | $-1.84 | $213.94 | ▼ -1.84 after sell → book $10,399.21; vs 09:30 mark -0.15 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BMEA` | 8 | $1.72 | $0.18 | $-1.84 | $227.48 | ▼ -1.84 after sell → book $10,399.03; vs 09:30 mark -0.18 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OABI` | 3 | $4.13 | $0.15 | $-2.26 | $239.72 | ▼ -2.26 after sell → book $10,398.88; vs 09:30 mark -0.15 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OPK` | 10 | $1.59 | $0.21 | $-0.40 | $255.41 | ▼ -0.40 after sell → book $10,398.67; vs 09:30 mark -0.21 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIR` | 1 | $10.73 | $0.13 | $-0.83 | $266.01 | ▼ -0.83 after sell → book $10,398.54; vs 09:30 mark -0.13 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $266.01 | ▼ close $10,373.60 vs 09:30 $10,399.52 (session -24.94) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $266.01 | ▲ 09:30 equity $10,440.35 vs yday $10,373.60 (+66.75) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $266.01 | ▼ close $10,315.21 vs 09:30 $10,440.35 (session -125.14) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $266.01 | ▲ 09:30 equity $10,369.62 vs yday $10,315.21 (+54.41) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 2 | $140.03 | $2.02 | $-52.81 | $544.05 | ▼ -52.81 after sell → book $10,367.60; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `DBI` | 66 | $6.25 | $2.21 | $+18.04 | $954.34 | ▲ +18.04 after sell → book $10,365.39; vs 09:30 mark -2.21 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ADBE` | 1 | $253.34 | $2.01 | $+7.16 | $1,205.67 | ▲ +7.16 after sell → book $10,363.38; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CPRT` | 12 | $30.57 | $2.05 | $-21.35 | $1,570.46 | ▼ -21.35 after sell → book $10,361.33; vs 09:30 mark -2.05 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `DSGX` | 5 | $78.12 | $2.02 | $+28.02 | $1,959.04 | ▲ +28.02 after sell → book $10,359.31; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `KR` | 7 | $61.93 | $2.03 | $+37.33 | $2,390.52 | ▲ +37.33 after sell → book $10,357.28; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `LPTH` | 42 | $9.40 | $2.14 | $-2.99 | $2,783.18 | ▼ -2.99 after sell → book $10,355.14; vs 09:30 mark -2.14 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `REF` | 30 | $15.75 | $2.10 | $+75.32 | $3,253.58 | ▲ +75.32 after sell → book $10,353.04; vs 09:30 mark -2.10 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 14 | $33.14 | $2.03 | — | $2,787.59 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list yday_gainer; 🔵; ret5=-2.9; combo leftover $488.04; owner union_e_fresh_h3 | — |
| 2026-09-16 09:30 ET | **BUY** | `TCOM` | 11 | $40.93 | $2.02 | — | $2,335.34 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $488.04; owner union_e_fresh_h3 | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 2 | $270.89 | $2.00 | — | $1,791.56 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.0; combo leftover $583.83; owner flatten_h5 | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 7 | $77.12 | $2.01 | — | $1,249.71 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+7.2; combo leftover $583.83; owner flatten_h5 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 40 | $14.31 | $2.11 | — | $675.20 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.8; combo leftover $583.83; owner flatten_h5 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 16 | $36.46 | $2.04 | — | $89.80 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+2.9; combo leftover $583.83; owner flatten_h5 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $89.80 | ▲ close $10,388.99 vs 09:30 $10,369.62 (session +48.16) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $89.80 | ▲ 09:30 equity $10,616.92 vs yday $10,388.99 (+227.93) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `ALMU` | 1 | $11.21 | $0.12 | — | $78.48 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=+1.0; combo leftover $13.47; owner union_e_fresh_h3 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 1 | $10.25 | $0.11 | — | $68.12 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+17.1; combo leftover $13.08; owner flatten_h5 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 1 | $7.59 | $0.08 | — | $60.45 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+9.4; combo leftover $13.08; owner flatten_h5 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $60.45 | ▼ close $10,601.70 vs 09:30 $10,616.92 (session -14.93) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $60.45 | ▲ 09:30 equity $10,651.34 vs yday $10,601.70 (+49.64) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AUPH` | 93 | $16.93 | $2.30 | $+55.88 | $1,632.65 | ▲ +55.88 after sell → book $10,649.05; vs 09:30 mark -2.29 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `OVID` | 558 | $2.68 | $7.30 | $-42.40 | $3,120.78 | ▼ -42.40 after sell → book $10,641.74; vs 09:30 mark -7.31 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `SANM` | 7 | $197.76 | $2.03 | $-67.60 | $4,503.07 | ▼ -67.60 after sell → book $10,639.71; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `NVT` | 9 | $152.71 | $2.04 | $-49.68 | $5,875.42 | ▼ -49.68 after sell → book $10,637.67; vs 09:30 mark -2.04 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `COHU` | 27 | $55.80 | $2.09 | $-11.99 | $7,379.93 | ▼ -11.99 after sell → book $10,635.58; vs 09:30 mark -2.09 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 11 | $108.55 | $2.02 | — | $6,183.86 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+21.3; combo leftover $1229.99; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `DELL` | 2 | $593.15 | $2.00 | — | $4,995.56 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+16.1; combo leftover $1229.99; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 5 | $209.52 | $2.00 | — | $3,945.96 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.1; combo leftover $1229.99; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 5 | $219.62 | $2.00 | — | $2,845.85 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+21.5; combo leftover $1229.99; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 14 | $85.00 | $2.03 | — | $1,653.82 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+18.3; combo leftover $1229.99; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 35 | $34.44 | $2.10 | — | $446.32 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+14.0; combo leftover $1229.99; owner flatten_h5 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $446.32 | ▼ close $10,461.88 vs 09:30 $10,651.34 (session -161.55) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $446.32 | ▲ 09:30 equity $10,562.43 vs yday $10,461.88 (+100.55) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `FPS` | 14 | $40.03 | $2.05 | $+92.38 | $1,004.69 | ▲ +92.38 after sell → book $10,560.38; vs 09:30 mark -2.05 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TCOM` | 11 | $41.00 | $2.04 | $-3.30 | $1,453.65 | ▼ -3.30 after sell → book $10,558.33; vs 09:30 mark -2.05 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 1 | $157.87 | $1.58 | — | $1,294.20 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.5; combo leftover $290.73; owner flatten_h5 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 3 | $88.83 | $2.00 | — | $1,025.71 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.6; combo leftover $290.73; owner flatten_h5 | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 21 | $13.47 | $2.05 | — | $740.79 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.6; combo leftover $290.73; owner flatten_h5 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 72 | $4.00 | $2.21 | — | $450.58 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); ret5=+58.9; combo leftover $290.73; owner flatten_h5 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $450.58 | ▲ close $10,601.59 vs 09:30 $10,562.43 (session +51.09) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $450.58 | ▼ 09:30 equity $10,598.48 vs yday $10,601.59 (-3.11) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $450.58 | ▼ close $10,573.02 vs 09:30 $10,598.48 (session -25.46) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $450.58 | ▲ 09:30 equity $10,797.49 vs yday $10,573.02 (+224.47) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `IQV` | 2 | $270.66 | $2.02 | $-4.47 | $989.88 | ▼ -4.47 after sell → book $10,795.48; vs 09:30 mark -2.01 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `RDNT` | 7 | $73.61 | $2.03 | $-28.61 | $1,503.12 | ▼ -28.61 after sell → book $10,793.45; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `AVAH` | 40 | $13.12 | $2.13 | $-51.84 | $2,025.79 | ▼ -51.84 after sell → book $10,791.32; vs 09:30 mark -2.13 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `BLFS` | 16 | $38.04 | $2.06 | $+21.18 | $2,632.37 | ▲ +21.18 after sell → book $10,789.26; vs 09:30 mark -2.06 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `ALMU` | 1 | $13.82 | $0.16 | $+2.33 | $2,646.03 | ▲ +2.33 after sell → book $10,789.10; vs 09:30 mark -0.16 | union_e_fresh_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `CBRL` | 3 | $47.57 | $1.44 | — | $2,501.89 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-11.2; combo leftover $158.76; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `GIS` | 4 | $35.74 | $1.44 | — | $2,357.49 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $158.76; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `KBH` | 3 | $47.15 | $1.42 | — | $2,214.61 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.9; combo leftover $158.76; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `PAYX` | 1 | $109.67 | $1.10 | — | $2,103.84 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $158.76; owner union_e_fresh_h3 | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 3 | $116.85 | $2.00 | — | $1,751.29 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.3; combo leftover $420.77; owner flatten_h5 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 15 | $27.79 | $2.04 | — | $1,332.41 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; combo leftover $420.77; owner flatten_h5 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 42 | $9.81 | $2.12 | — | $918.27 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; combo leftover $420.77; owner flatten_h5 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 20 | $20.25 | $2.05 | — | $511.22 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; combo leftover $420.77; owner flatten_h5 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 20 | $20.65 | $2.05 | — | $96.17 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; combo leftover $420.77; owner flatten_h5 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $96.17 | ▼ close $10,721.43 vs 09:30 $10,797.49 (session -52.02) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $96.17 | ▼ 09:30 equity $10,560.91 vs yday $10,721.43 (-160.52) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `IOVA` | 1 | $10.39 | $0.13 | $-0.09 | $106.44 | ▼ -0.09 after sell → book $10,560.78; vs 09:30 mark -0.13 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 1 | $7.38 | $0.10 | $-0.39 | $113.72 | ▼ -0.39 after sell → book $10,560.69; vs 09:30 mark -0.09 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $113.72 | ▲ close $10,629.28 vs 09:30 $10,560.91 (session +68.60) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $81.23 | ▲ 09:30 equity $8,765.65 vs yday $8,759.42 (+6.23) | 09:30 open · cash $81.23 (unchanged overnight, no fees) · equity $8,765.65 vs prior close $8,759.42 (+6.23) | — |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 3 | $7.65 | $0.24 | — | $58.04 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.2; combo leftover $27.08; owner flatten_h5 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $58.04 | ▼ close $8,755.03 vs 09:30 $8,765.65 (session -10.38) | 16:00 close · cash $58.04 · equity $8,755.03 vs 09:30 $8,765.65 (-10.62; session marks -10.38) · 26 name(s) marked open→close (per-name table). A×3 09:30 $171.98 → close $172.79 +2.43; ADMA×72 09:30 $9.52 → close $9.52 +0.00; ARQT×25 09:30 $26.27 → close $26.27 +0.00; CBRL×5 09:30 $52.39 → close $51.81 -2.90; CTAS×1 09:30 $197.68 → close $197.68 -0.00; CYPH×148 09:30 $4.00 → close $4.12 +17.02; DLO×4 09:30 $13.88 → close $13.88 +0.00; DXCM×6 09:30 $87.47 → close $87.47 +0.00; ECO×3 09:30 $78.22 → close $78.22 +0.00; FIVN×9 09:30 $36.66 → close $36.66 -0.00; FTRE×35 09:30 $20.02 → close $20.02 +0.00; GIS×7 09:30 $34.83 → close $34.83 +0.00; GNRC×1 09:30 $198.05 → close $198.05 +0.00; HALO×6 09:30 $115.36 → close $113.90 -8.76; HUM×1 09:30 $380.32 → close $380.32 +0.00; KBH×5 09:30 $47.65 → close $47.65 +0.00; MGTX×44 09:30 $11.05 → close $11.05 +0.00; MKC×1 09:30 $47.82 → close $47.82 -0.00; MLKN×1 09:30 $19.91 → close $19.91 -0.00; OMER×34 09:30 $20.61 → close $20.08 -18.02; PACS×1 09:30 $41.46 → close $41.46 -0.00; PAYX×2 09:30 $101.59 → close $101.59 -0.00; RBRK×3 09:30 $113.80 → close $113.80 +0.00; TDC×2 09:30 $29.46 → close $29.46 -0.00; VICR×1 09:30 $276.06 → close $276.06 -0.00; MRVI×3 09:30 $7.65 → close $7.60 -0.15 | — |
| 2026-09-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $58.04 | ▼ 09:30 equity $8,676.70 vs yday $8,755.03 (-78.33) | 09:30 open · cash $58.04 (unchanged overnight, no fees) · equity $8,676.70 vs prior close $8,755.03 (-78.33) | — |
| 2026-09-28 09:30 ET | **SELL** | `A` | 3 | $170.00 | $2.02 | $+32.37 | $566.02 | ▲ +32.37 after sell → book $8,674.68; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-28 09:30 ET | **SELL** | `CBRL` | 5 | $52.25 | $2.02 | $+19.37 | $825.25 | ▲ +19.37 after sell → book $8,672.66; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-28 09:30 ET | **SELL** | `CTAS` | 1 | $199.51 | $2.01 | $-1.25 | $1,022.75 | ▼ -1.25 after sell → book $8,670.65; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-28 09:30 ET | **SELL** | `CYPH` | 148 | $4.03 | $2.47 | $-0.09 | $1,617.09 | ▼ -0.09 after sell → book $8,668.18; vs 09:30 mark -2.47 | flatten_h5: dropped from list after 5 sess (min 5) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-28 09:30 ET | **SELL** | `DXCM` | 6 | $86.87 | $2.03 | $-15.80 | $2,136.28 | ▼ -15.80 after sell → book $8,666.15; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-28 09:30 ET | **SELL** | `ECO` | 3 | $79.55 | $2.02 | $-20.37 | $2,372.91 | ▼ -20.37 after sell → book $8,664.13; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 6 sess (min 5) | — |
| 2026-09-28 09:30 ET | **SELL** | `FIVN` | 9 | $34.75 | $2.04 | $-1.26 | $2,683.62 | ▼ -1.26 after sell → book $8,662.09; vs 09:30 mark -2.04 | flatten_h5: dropped from list after 6 sess (min 5) | — |
| 2026-09-28 09:30 ET | **SELL** | `GIS` | 7 | $33.60 | $2.03 | $-19.02 | $2,916.79 | ▼ -19.02 after sell → book $8,660.06; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-28 09:30 ET | **SELL** | `GNRC` | 1 | $207.41 | $2.01 | $-6.12 | $3,122.19 | ▼ -6.12 after sell → book $8,658.05; vs 09:30 mark -2.01 | flatten_h5: dropped from list after 6 sess (min 5) | — |
| 2026-09-28 09:30 ET | **SELL** | `HUM` | 1 | $397.43 | $2.01 | $+7.22 | $3,517.61 | ▲ +7.22 after sell → book $8,656.04; vs 09:30 mark -2.01 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-28 09:30 ET | **SELL** | `KBH` | 5 | $47.58 | $2.02 | $-1.88 | $3,753.48 | ▼ -1.88 after sell → book $8,654.01; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-28 09:30 ET | **SELL** | `MGTX` | 44 | $10.79 | $2.14 | $-122.18 | $4,226.10 | ▼ -122.18 after sell → book $8,651.87; vs 09:30 mark -2.14 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-28 09:30 ET | **SELL** | `MLKN` | 1 | $19.88 | $0.22 | $-1.21 | $4,245.76 | ▼ -1.21 after sell → book $8,651.65; vs 09:30 mark -0.22 | union_e_fresh_h3: dropped from list after 4 sess (min 3) | — |
| 2026-09-28 09:30 ET | **SELL** | `PAYX` | 2 | $100.08 | $2.02 | $-23.19 | $4,443.90 | ▼ -23.19 after sell → book $8,649.63; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-09-28 09:30 ET | **SELL** | `RBRK` | 3 | $106.94 | $2.02 | $-8.85 | $4,762.70 | ▼ -8.85 after sell → book $8,647.61; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 6 sess (min 5) | — |
| 2026-09-28 09:30 ET | **SELL** | `VICR` | 1 | $280.00 | $2.01 | $+56.37 | $5,040.69 | ▲ +56.37 after sell → book $8,645.60; vs 09:30 mark -2.01 | flatten_h5: dropped from list after 6 sess (min 5) | — |
| 2026-09-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,040.69 | ▲ close $8,719.43 vs 09:30 $8,676.70 (session +73.83) | 16:00 close · cash $5,040.69 · equity $8,719.43 vs 09:30 $8,676.70 (+42.73; session marks +73.83) · 10 name(s) marked open→close (per-name table). ADMA×72 09:30 $9.38 → close $10.12 +53.28; ARQT×25 09:30 $26.70 → close $27.33 +15.75; DLO×4 09:30 $13.91 → close $13.82 -0.36; FTRE×35 09:30 $19.54 → close $20.06 +18.20; HALO×6 09:30 $113.34 → close $112.89 -2.70; MKC×1 09:30 $47.83 → close $48.46 +0.63; MRVI×3 09:30 $7.49 → close $7.63 +0.42; OMER×34 09:30 $19.83 → close $19.43 -13.60; PACS×1 09:30 $41.27 → close $43.06 +1.79; TDC×2 09:30 $28.34 → close $28.55 +0.42 | — |
| 2026-09-29 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,040.69 | ▼ 09:30 equity $8,699.29 vs yday $8,719.43 (-20.14) | 09:30 open · cash $5,040.69 (unchanged overnight, no fees) · equity $8,699.29 vs prior close $8,719.43 (-20.14) | — |
| 2026-09-29 09:30 ET | **SELL** | `DLO` | 4 | $13.82 | $0.58 | $-3.90 | $5,095.39 | ▼ -3.90 after sell → book $8,698.71; vs 09:30 mark -0.58 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-29 09:30 ET | **SELL** | `MKC` | 1 | $48.14 | $0.50 | $-2.43 | $5,143.02 | ▼ -2.43 after sell → book $8,698.20; vs 09:30 mark -0.51 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-29 09:30 ET | **SELL** | `PACS` | 1 | $42.75 | $0.45 | $-0.05 | $5,185.32 | ▼ -0.05 after sell → book $8,697.75; vs 09:30 mark -0.45 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-29 09:30 ET | **SELL** | `TDC` | 2 | $29.03 | $0.61 | $-2.71 | $5,242.77 | ▼ -2.71 after sell → book $8,697.14; vs 09:30 mark -0.61 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-29 09:30 ET | **BUY** | `CCL` | 12 | $24.39 | $2.03 | — | $4,948.07 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.8; combo leftover $314.57; owner union_e_fresh_h3 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `JEF` | 6 | $46.08 | $2.01 | — | $4,669.58 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-1.5; combo leftover $314.57; owner union_e_fresh_h3 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `KMX` | 5 | $60.41 | $2.00 | — | $4,365.55 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-2.3; combo leftover $314.57; owner union_e_fresh_h3 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `MTN` | 2 | $138.42 | $2.00 | — | $4,086.71 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-1.7; combo leftover $314.57; owner union_e_fresh_h3 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟡 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `UEC` | 31 | $9.91 | $2.08 | — | $3,777.42 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-8.6; combo leftover $314.57; owner union_e_fresh_h3 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `SN` | 3 | $184.05 | $2.00 | — | $3,223.27 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.7; combo leftover $629.57; owner flatten_h5 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `DT` | 10 | $57.39 | $2.02 | — | $2,647.35 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+2.6; combo leftover $629.57; owner flatten_h5 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `TOST` | 20 | $30.40 | $2.05 | — | $2,037.30 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+1.9; combo leftover $629.57; owner flatten_h5 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `SONO` | 35 | $17.76 | $2.10 | — | $1,413.61 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+9.8; combo leftover $629.57; owner flatten_h5 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-29 09:30 ET | **BUY** | `PDFS` | 12 | $50.25 | $2.03 | — | $808.58 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+6.5; combo leftover $629.57; owner flatten_h5 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `SHOO` | 13 | $45.06 | $2.03 | — | $220.77 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.8; combo leftover $629.57; owner flatten_h5 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $220.77 | ▼ close $8,632.14 vs 09:30 $8,699.29 (session -42.67) | 16:00 close · cash $220.77 · equity $8,632.14 vs 09:30 $8,699.29 (-67.15; session marks -42.67) · 17 name(s) marked open→close (per-name table). ADMA×72 09:30 $10.04 → close $9.96 -5.76; ARQT×25 09:30 $27.21 → close $26.75 -11.50; FTRE×35 09:30 $19.90 → close $19.74 -5.60; HALO×6 09:30 $112.89 → close $111.56 -7.98; MRVI×3 09:30 $7.52 → close $7.65 +0.39; OMER×34 09:30 $19.26 → close $19.25 -0.34; CCL×12 09:30 $24.39 → close $25.11 +8.64; JEF×6 09:30 $46.08 → close $46.52 +2.64; KMX×5 09:30 $60.41 → close $59.23 -5.88; MTN×2 09:30 $138.42 → close $141.29 +5.74; UEC×31 09:30 $9.91 → close $9.29 -19.22; SN×3 09:30 $184.05 → close $182.44 -4.83; DT×10 09:30 $57.39 → close $57.53 +1.40; TOST×20 09:30 $30.40 → close $30.46 +1.20; SONO×35 09:30 $17.76 → close $17.85 +3.15; PDFS×12 09:30 $50.25 → close $49.77 -5.76; SHOO×13 09:30 $45.06 → close $45.14 +1.04 | — |
| 2026-09-30 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $220.77 | ▲ 09:30 equity $8,632.14 vs yday $8,632.14 (+0.00) | 09:30 open · cash $220.77 (unchanged overnight, no fees) · equity $8,632.14 vs prior close $8,632.14 (+0.00) | — |
| 2026-09-30 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $220.77 | ▲ close $8,632.14 vs 09:30 $8,632.14 (session +0.00) | 16:00 close · cash $220.77 · equity $8,632.14 vs 09:30 $8,632.14 (+0.00; session marks +0.00) · 17 name(s) marked open→close (per-name table). ADMA×72 09:30 $9.96 → close $9.96 +0.00; ARQT×25 09:30 $26.75 → close $26.75 +0.00; CCL×12 09:30 $25.11 → close $25.11 +0.00; DT×10 09:30 $57.53 → close $57.53 +0.00; FTRE×35 09:30 $19.74 → close $19.74 +0.00; HALO×6 09:30 $111.56 → close $111.56 +0.00; JEF×6 09:30 $46.52 → close $46.52 +0.00; KMX×5 09:30 $59.23 → close $59.23 +0.00; MRVI×3 09:30 $7.65 → close $7.65 +0.00; MTN×2 09:30 $141.29 → close $141.29 +0.00; OMER×34 09:30 $19.25 → close $19.25 +0.00; PDFS×12 09:30 $49.77 → close $49.77 +0.00; SHOO×13 09:30 $45.14 → close $45.14 +0.00; SN×3 09:30 $182.44 → close $182.44 +0.00; SONO×35 09:30 $17.85 → close $17.85 +0.00; TOST×20 09:30 $30.46 → close $30.46 +0.00; UEC×31 09:30 $9.29 → close $9.29 +0.00 | — |
| 2026-10-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $220.77 | ▼ 09:30 equity $8,577.89 vs yday $8,632.14 (-54.25) | 09:30 open · cash $220.77 (unchanged overnight, no fees) · equity $8,577.89 vs prior close $8,632.14 (-54.25) | — |
| 2026-10-01 09:30 ET | **SELL** | `ADMA` | 72 | $10.07 | $2.23 | $+14.29 | $943.58 | ▲ +14.29 after sell → book $8,575.66; vs 09:30 mark -2.23 | flatten_h5: dropped from list after 6 sess (min 5) | — |
| 2026-10-01 09:30 ET | **SELL** | `ARQT` | 25 | $26.10 | $2.08 | $-46.40 | $1,594.00 | ▼ -46.40 after sell → book $8,573.58; vs 09:30 mark -2.08 | flatten_h5: dropped from list after 6 sess (min 5) | — |
| 2026-10-01 09:30 ET | **SELL** | `FTRE` | 35 | $19.88 | $2.12 | $-17.16 | $2,287.68 | ▼ -17.16 after sell → book $8,571.46; vs 09:30 mark -2.12 | flatten_h5: dropped from list after 6 sess (min 5) | — |
| 2026-10-01 09:30 ET | **SELL** | `HALO` | 6 | $109.93 | $2.03 | $-45.56 | $2,945.23 | ▼ -45.56 after sell → book $8,569.43; vs 09:30 mark -2.03 | flatten_h5: dropped from list after 6 sess (min 5) | — |
| 2026-10-01 09:30 ET | **SELL** | `OMER` | 34 | $18.87 | $2.11 | $-64.72 | $3,584.70 | ▼ -64.72 after sell → book $8,567.32; vs 09:30 mark -2.11 | flatten_h5: dropped from list after 6 sess (min 5) | — |
| 2026-10-01 09:30 ET | **BUY** | `ACN` | 1 | $215.98 | $1.99 | — | $3,366.73 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-0.1; combo leftover $268.85; owner union_e_fresh_h3 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `MKC` | 5 | $46.80 | $2.00 | — | $3,130.72 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-5.5; combo leftover $268.85; owner union_e_fresh_h3 | join🟢 sector🔴 gen🔴 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `PRGS` | 6 | $40.52 | $2.01 | — | $2,885.60 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-2.6; combo leftover $268.85; owner union_e_fresh_h3 | join🟢 sector🟢 gen🔴 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `IT` | 2 | $196.19 | $2.00 | — | $2,491.22 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.5; combo leftover $577.12; owner flatten_h5 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `KSPI` | 6 | $92.93 | $2.01 | — | $1,931.63 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+0.2; combo leftover $577.12; owner flatten_h5 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `IOT` | 14 | $38.99 | $2.03 | — | $1,383.74 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-2.4; combo leftover $577.12; owner flatten_h5 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `AVPT` | 40 | $14.27 | $2.11 | — | $810.83 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+7.3; combo leftover $577.12; owner flatten_h5 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-10-01 09:30 ET | **BUY** | `RELY` | 27 | $21.28 | $2.07 | — | $234.20 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+5.7; combo leftover $577.12; owner flatten_h5 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-10-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $234.20 | ▼ close $8,550.97 vs 09:30 $8,577.89 (session -0.13) | 16:00 close · cash $234.20 · equity $8,550.97 vs 09:30 $8,577.89 (-26.92; session marks -0.13) · 20 name(s) marked open→close (per-name table). CCL×12 09:30 $24.66 → close $25.07 +4.92; DT×10 09:30 $59.04 → close $59.26 +2.20; JEF×6 09:30 $45.41 → close $45.25 -0.96; KMX×5 09:30 $55.27 → close $55.89 +3.10; MRVI×3 09:30 $7.67 → close $7.50 -0.51; MTN×2 09:30 $138.51 → close $138.99 +0.96; PDFS×12 09:30 $51.41 → close $53.39 +23.76; SHOO×13 09:30 $44.46 → close $45.69 +15.99; SN×3 09:30 $182.44 → close $182.44 +0.00; SONO×35 09:30 $18.09 → close $17.75 -11.90; TOST×20 09:30 $29.05 → close $29.23 +3.60; UEC×31 09:30 $9.39 → close $9.36 -0.93; ACN×1 09:30 $215.98 → close $212.30 -3.68; MKC×5 09:30 $46.80 → close $44.14 -13.30; PRGS×6 09:30 $40.52 → close $36.58 -23.64; IT×2 09:30 $196.19 → close $192.80 -6.78; KSPI×6 09:30 $92.93 → close $92.02 -5.46; IOT×14 09:30 $38.99 → close $40.04 +14.70; AVPT×40 09:30 $14.27 → close $14.08 -7.60; RELY×27 09:30 $21.28 → close $21.48 +5.40 | — |
| 2026-10-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $234.20 | ▲ 09:30 equity $8,633.08 vs yday $8,550.97 (+82.11) | 09:30 open · cash $234.20 (unchanged overnight, no fees) · equity $8,633.08 vs prior close $8,550.97 (+82.11) | — |
| 2026-10-02 09:30 ET | **SELL** | `CCL` | 12 | $25.59 | $2.05 | $+10.33 | $539.23 | ▲ +10.33 after sell → book $8,631.03; vs 09:30 mark -2.05 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-10-02 09:30 ET | **SELL** | `JEF` | 6 | $45.54 | $2.03 | $-7.28 | $810.45 | ▼ -7.28 after sell → book $8,629.01; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-10-02 09:30 ET | **SELL** | `KMX` | 5 | $56.27 | $2.02 | $-24.70 | $1,089.77 | ▼ -24.70 after sell → book $8,626.98; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-10-02 09:30 ET | **SELL** | `MRVI` | 3 | $7.62 | $0.26 | $-0.59 | $1,112.37 | ▼ -0.59 after sell → book $8,626.72; vs 09:30 mark -0.26 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-10-02 09:30 ET | **SELL** | `MTN` | 2 | $140.00 | $2.02 | $-0.85 | $1,390.36 | ▼ -0.85 after sell → book $8,624.71; vs 09:30 mark -2.01 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-10-02 09:30 ET | **SELL** | `UEC` | 31 | $9.67 | $2.10 | $-11.63 | $1,688.02 | ▼ -11.63 after sell → book $8,622.60; vs 09:30 mark -2.11 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-10-02 09:30 ET | **BUY** | `NKE` | 15 | $32.55 | $2.04 | — | $1,197.69 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-2.3; combo leftover $506.41; owner union_e_fresh_h3 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `CORT` | 1 | $114.38 | $1.15 | — | $1,082.17 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-3.8; combo leftover $171.10; owner flatten_h5 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `CDNA` | 2 | $66.33 | $1.33 | — | $948.17 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.9; combo leftover $171.10; owner flatten_h5 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `WRBY` | 6 | $27.63 | $1.68 | — | $780.72 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.6; combo leftover $171.10; owner flatten_h5 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `BLFS` | 4 | $37.02 | $1.49 | — | $631.15 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-4.7; combo leftover $171.10; owner flatten_h5 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `ETON` | 3 | $52.42 | $1.58 | — | $472.30 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.6; combo leftover $171.10; owner flatten_h5 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $472.30 | ▲ close $8,631.98 vs 09:30 $8,633.08 (session +18.65) | 16:00 close · cash $472.30 · equity $8,631.98 vs 09:30 $8,633.08 (-1.10; session marks +18.65) · 20 name(s) marked open→close (per-name table). ACN×1 09:30 $211.02 → close $198.90 -12.12; AVPT×40 09:30 $14.22 → close $14.07 -6.00; DT×10 09:30 $59.45 → close $59.32 -1.30; IOT×14 09:30 $40.41 → close $41.12 +9.94; IT×2 09:30 $192.74 → close $184.80 -15.88; KSPI×6 09:30 $92.05 → close $94.05 +12.00; MKC×5 09:30 $43.52 → close $44.67 +5.75; PDFS×12 09:30 $55.24 → close $55.50 +3.12; PRGS×6 09:30 $36.88 → close $36.90 +0.12; RELY×27 09:30 $21.80 → close $21.77 -0.81; SHOO×13 09:30 $46.59 → close $45.97 -8.06; SN×3 09:30 $183.57 → close $182.57 -3.00; SONO×35 09:30 $17.88 → close $17.70 -6.30; TOST×20 09:30 $29.21 → close $29.79 +11.60; NKE×15 09:30 $32.55 → close $33.87 +19.76; CORT×1 09:30 $114.38 → close $116.23 +1.85; CDNA×2 09:30 $66.33 → close $67.15 +1.64; WRBY×6 09:30 $27.63 → close $27.03 -3.60; BLFS×4 09:30 $37.02 → close $37.30 +1.12; ETON×3 09:30 $52.42 → close $55.36 +8.82 | — |
| 2026-10-05 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $472.30 | ▼ 09:30 equity $8,625.80 vs yday $8,631.98 (-6.18) | 09:30 open · cash $472.30 (unchanged overnight, no fees) · equity $8,625.80 vs prior close $8,631.98 (-6.18) | — |
| 2026-10-05 09:30 ET | **BUY** | `DVN` | 1 | $47.64 | $0.48 | — | $424.18 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.3; combo leftover $59.04; owner flatten_h5 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `RRC` | 1 | $38.10 | $0.38 | — | $385.70 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=-1.0; combo leftover $59.04; owner flatten_h5 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `SM` | 1 | $35.27 | $0.36 | — | $350.07 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.9; combo leftover $59.04; owner flatten_h5 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `MTDR` | 1 | $53.35 | $0.54 | — | $296.18 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.8; combo leftover $59.04; owner flatten_h5 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `GPRK` | 5 | $10.87 | $0.56 | — | $241.28 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=-3.1; combo leftover $59.04; owner flatten_h5 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `OBE` | 5 | $10.26 | $0.53 | — | $189.45 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=-2.7; combo leftover $59.04; owner flatten_h5 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-10-05 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $189.45 | ▲ close $8,754.93 vs 09:30 $8,625.80 (session +131.97) | 16:00 close · cash $189.45 · equity $8,754.93 vs 09:30 $8,625.80 (+129.13; session marks +131.97) · 26 name(s) marked open→close (per-name table). ACN×1 09:30 $196.30 → close $195.05 -1.25; AVPT×40 09:30 $13.91 → close $14.66 +30.00; BLFS×4 09:30 $37.16 → close $38.61 +5.80; CDNA×2 09:30 $66.90 → close $69.85 +5.90; CORT×1 09:30 $115.59 → close $122.03 +6.44; DT×10 09:30 $59.42 → close $60.21 +7.90; ETON×3 09:30 $55.85 → close $55.62 -0.69; IOT×14 09:30 $42.00 → close $42.39 +5.46; IT×2 09:30 $185.07 → close $187.76 +5.38; KSPI×6 09:30 $94.50 → close $93.98 -3.12; MKC×5 09:30 $44.76 → close $45.54 +3.90; NKE×15 09:30 $33.82 → close $33.96 +2.10; PDFS×12 09:30 $55.71 → close $56.08 +4.44; PRGS×6 09:30 $37.00 → close $37.22 +1.29; RELY×27 09:30 $21.76 → close $22.91 +31.05; SHOO×13 09:30 $46.06 → close $45.68 -4.94; SN×3 09:30 $182.88 → close $182.69 -0.57; SONO×35 09:30 $17.52 → close $17.94 +14.70; TOST×20 09:30 $29.20 → close $30.04 +16.80; WRBY×6 09:30 $27.02 → close $26.65 -2.22; DVN×1 09:30 $47.64 → close $47.98 +0.34; RRC×1 09:30 $38.10 → close $38.67 +0.57; SM×1 09:30 $35.27 → close $35.05 -0.22; MTDR×1 09:30 $53.35 → close $53.01 -0.34; GPRK×5 09:30 $10.87 → close $11.28 +2.05; OBE×5 09:30 $10.26 → close $10.50 +1.20 | — |
| 2026-10-06 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $189.45 | ▲ 09:30 equity $8,802.74 vs yday $8,754.93 (+47.81) | 09:30 open · cash $189.45 (unchanged overnight, no fees) · equity $8,802.74 vs prior close $8,754.93 (+47.81) | — |
| 2026-10-06 09:30 ET | **SELL** | `ACN` | 1 | $195.43 | $1.98 | $-24.52 | $382.90 | ▼ -24.52 after sell → book $8,800.76; vs 09:30 mark -1.98 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-10-06 09:30 ET | **SELL** | `DT` | 10 | $60.84 | $2.04 | $+30.44 | $989.26 | ▲ +30.44 after sell → book $8,798.72; vs 09:30 mark -2.04 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-10-06 09:30 ET | **SELL** | `MKC` | 5 | $45.75 | $2.02 | $-9.28 | $1,215.99 | ▼ -9.28 after sell → book $8,796.69; vs 09:30 mark -2.03 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-10-06 09:30 ET | **SELL** | `PRGS` | 6 | $37.81 | $2.03 | $-20.32 | $1,440.80 | ▼ -20.32 after sell → book $8,794.67; vs 09:30 mark -2.02 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-10-06 09:30 ET | **SELL** | `SHOO` | 13 | $45.88 | $2.05 | $+6.58 | $2,035.19 | ▲ +6.58 after sell → book $8,792.62; vs 09:30 mark -2.05 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-10-06 09:30 ET | **SELL** | `SONO` | 35 | $18.00 | $2.12 | $+4.19 | $2,663.08 | ▲ +4.19 after sell → book $8,790.50; vs 09:30 mark -2.12 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-10-06 09:30 ET | **BUY** | `RPM` | 8 | $96.25 | $2.01 | — | $1,891.06 | — | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-4.9; combo leftover $798.92; owner union_e_fresh_h3 | join🔴 sector🟢 gen🟡 news🟡 digest🔴 ab🟢 peer🟡 heat🔴 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `NTAP` | 8 | $224.80 | $2.01 | — | $90.65 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+9.5; combo leftover $1891.06; owner flatten_h5 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-06 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $90.65 | ▼ close $8,769.11 vs 09:30 $8,802.74 (session -17.36) | 16:00 close · cash $90.65 · equity $8,769.11 vs 09:30 $8,802.74 (-33.63; session marks -17.36) · 22 name(s) marked open→close (per-name table). AVPT×40 09:30 $14.77 → close $14.59 -7.20; BLFS×4 09:30 $38.61 → close $38.61 +0.00; CDNA×2 09:30 $70.89 → close $63.79 -14.20; CORT×1 09:30 $122.20 → close $119.57 -2.63; DVN×1 09:30 $47.48 → close $48.02 +0.55; ETON×3 09:30 $56.15 → close $54.77 -4.14; GPRK×5 09:30 $11.32 → close $11.44 +0.60; IOT×14 09:30 $42.70 → close $41.74 -13.44; IT×2 09:30 $188.47 → close $184.92 -7.10; KSPI×6 09:30 $94.50 → close $93.16 -8.04; MTDR×1 09:30 $52.56 → close $52.91 +0.35; NKE×15 09:30 $33.70 → close $34.61 +13.65; OBE×5 09:30 $10.44 → close $10.62 +0.90; PDFS×12 09:30 $56.89 → close $54.44 -29.40; RELY×27 09:30 $23.12 → close $23.20 +2.16; RRC×1 09:30 $38.71 → close $40.02 +1.31; SM×1 09:30 $34.90 → close $35.10 +0.20; SN×3 09:30 $183.47 → close $184.72 +3.75; TOST×20 09:30 $30.07 → close $30.25 +3.60; WRBY×6 09:30 $26.89 → close $26.23 -3.96; RPM×8 09:30 $96.25 → close $98.31 +16.48; NTAP×8 09:30 $224.80 → close $228.45 +29.20 | — |
| 2026-10-07 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $90.65 | ▼ 09:30 equity $8,743.26 vs yday $8,769.11 (-25.85) | 09:30 open · cash $90.65 (unchanged overnight, no fees) · equity $8,743.26 vs prior close $8,769.11 (-25.85) | — |
| 2026-10-07 09:30 ET | **SELL** | `NKE` | 15 | $34.39 | $2.06 | $+23.47 | $604.45 | ▲ +23.47 after sell → book $8,741.20; vs 09:30 mark -2.06 | union_e_fresh_h3: dropped from list after 3 sess (min 3) | — |
| 2026-10-07 09:30 ET | **SELL** | `PDFS` | 12 | $52.54 | $2.05 | $+23.41 | $1,232.88 | ▲ +23.41 after sell → book $8,739.16; vs 09:30 mark -2.04 | flatten_h5: dropped from list after 6 sess (min 5) | — |
| 2026-10-07 09:30 ET | **SELL** | `SN` | 3 | $183.00 | $2.02 | $-7.17 | $1,779.86 | ▼ -7.17 after sell → book $8,737.14; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 6 sess (min 5) | — |
| 2026-10-07 09:30 ET | **SELL** | `TOST` | 20 | $30.13 | $2.07 | $-9.52 | $2,380.39 | ▼ -9.52 after sell → book $8,735.07; vs 09:30 mark -2.07 | flatten_h5: dropped from list after 6 sess (min 5) | — |
| 2026-10-07 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,380.39 | ▼ close $8,734.53 vs 09:30 $8,743.26 (session -0.54) | 16:00 close · cash $2,380.39 · equity $8,734.53 vs 09:30 $8,743.26 (-8.73; session marks -0.54) · 18 name(s) marked open→close (per-name table). AVPT×40 09:30 $14.51 → close $14.28 -9.20; BLFS×4 09:30 $38.61 → close $38.61 +0.00; CDNA×2 09:30 $61.38 → close $63.04 +3.32; CORT×1 09:30 $118.60 → close $119.60 +1.00; DVN×1 09:30 $48.40 → close $47.88 -0.52; ETON×3 09:30 $53.84 → close $54.88 +3.12; GPRK×5 09:30 $11.50 → close $11.11 -1.95; IOT×14 09:30 $41.60 → close $40.51 -15.26; IT×2 09:30 $186.51 → close $185.77 -1.48; KSPI×6 09:30 $92.70 → close $91.87 -4.98; MTDR×1 09:30 $53.44 → close $52.85 -0.59; NTAP×8 09:30 $232.00 → close $235.77 +30.16; OBE×5 09:30 $10.72 → close $10.63 -0.45; RELY×27 09:30 $22.96 → close $22.93 -0.95; RPM×8 09:30 $98.10 → close $98.33 +1.84; RRC×1 09:30 $40.20 → close $39.83 -0.37; SM×1 09:30 $35.46 → close $35.25 -0.21; WRBY×6 09:30 $25.98 → close $25.31 -4.02 | — |
| 2026-10-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,380.39 | ▲ 09:30 equity $8,734.53 vs yday $8,734.53 (-0.00) | 09:30 open · cash $2,380.39 (unchanged overnight, no fees) · equity $8,734.53 vs prior close $8,734.53 (-0.00) | — |
| 2026-10-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,380.39 | ▲ close $8,734.53 vs 09:30 $8,734.53 (session +0.00) | 16:00 close · cash $2,380.39 · equity $8,734.53 vs 09:30 $8,734.53 (-0.00; session marks +0.00) · 18 name(s) marked open→close (per-name table). AVPT×40 09:30 $14.28 → close $14.28 +0.00; BLFS×4 09:30 $38.61 → close $38.61 +0.00; CDNA×2 09:30 $63.04 → close $63.04 +0.00; CORT×1 09:30 $119.60 → close $119.60 +0.00; DVN×1 09:30 $47.88 → close $47.88 +0.00; ETON×3 09:30 $54.88 → close $54.88 +0.00; GPRK×5 09:30 $11.11 → close $11.11 +0.00; IOT×14 09:30 $40.51 → close $40.51 +0.00; IT×2 09:30 $185.77 → close $185.77 +0.00; KSPI×6 09:30 $91.87 → close $91.87 +0.00; MTDR×1 09:30 $52.85 → close $52.85 +0.00; NTAP×8 09:30 $235.77 → close $235.77 +0.00; OBE×5 09:30 $10.63 → close $10.63 +0.00; RELY×27 09:30 $22.93 → close $22.93 +0.00; RPM×8 09:30 $98.33 → close $98.33 +0.00; RRC×1 09:30 $39.83 → close $39.83 +0.00; SM×1 09:30 $35.25 → close $35.25 +0.00; WRBY×6 09:30 $25.31 → close $25.31 +0.00 | — |

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
| 2026-08-14 | `ARX` | cash | leftover split 4.97 < 1 share @ 19.57 |
| 2026-08-14 | `AIRO` | cash | leftover split 4.97 < 1 share @ 11.12 |
| 2026-08-14 | `MH` | cash | leftover split 4.97 < 1 share @ 13.55 |
| 2026-08-14 | `CLBT` | cash | leftover split 4.97 < 1 share @ 10.83 |
| 2026-08-14 | `LUNR` | cash | leftover split 4.97 < 1 share @ 19.17 |
| 2026-08-14 | `NMAX` | cash | leftover split 4.97 < 1 share @ 9.89 |
| 2026-08-14 | `TLN` | cash | leftover split 17.61 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 17.61 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 17.61 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 17.61 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 17.61 < 1 share @ 57.61 |
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
| 2026-08-17 | `MARA` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-17 | `LDI` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-17 | `DVN` | cash | leftover split 12.14 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 12.14 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 12.14 < 1 share @ 202.70 |
| 2026-08-17 | `ELF` | cash | leftover split 12.14 < 1 share @ 90.54 |
| 2026-08-18 | `BTSG` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `IREN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `TPG` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `TGTX` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `SLS` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `HIMS` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `TNDM` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `BTBT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `EU` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-18 | `MARA` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-18 | `LDI` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-18 | `TMC` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `TGB` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `DNN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `HNST` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `KLAR` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new long union_e_fresh_h3 |
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
| 2026-08-19 | `TNDM` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-19 | `MARA` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-19 | `LDI` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-19 | `TMC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-19 | `TGB` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-19 | `DNN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-19 | `HNST` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h3 |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h3 |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h3 |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h3 |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h3 |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h3 |
| 2026-08-19 | `KC` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h3 |
| 2026-08-19 | `KEYS` | hard_red | hard-red S=-7.20 sit; no new long union_e_fresh_h3 |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new long flatten_h5 |
| 2026-08-20 | `MARA` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-20 | `LDI` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-20 | `TMC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-20 | `TGB` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-20 | `DNN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-20 | `HNST` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-21 | `TMC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-21 | `TGB` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-21 | `DNN` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-21 | `HNST` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-21 | `EL` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-21 | `TOYO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
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
| 2026-08-21 | `FUTU` | cash | leftover split 7.64 < 1 share @ 115.18 |
| 2026-08-21 | `DE` | cash | leftover split 7.64 < 1 share @ 623.26 |
| 2026-08-21 | `WMT` | cash | leftover split 7.64 < 1 share @ 103.69 |
| 2026-08-21 | `BEKE` | cash | leftover split 7.64 < 1 share @ 17.93 |
| 2026-08-21 | `BJ` | cash | leftover split 7.64 < 1 share @ 93.98 |
| 2026-08-21 | `BKE` | cash | leftover split 7.64 < 1 share @ 43.08 |
| 2026-08-21 | `AU` | cash | leftover split 21.40 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 21.40 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 21.40 < 1 share @ 59.72 |
| 2026-08-24 | `EL` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `TOYO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `DVLT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-24 | `AAP` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
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
| 2026-08-24 | `PSEC` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-24 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-24 | `ARCT` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-24 | `AUTL` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-24 | `CRDL` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-24 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new long union_e_fresh_h3 |
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
| 2026-08-25 | `PSEC` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-25 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-25 | `ARCT` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-25 | `AUTL` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-25 | `CRDL` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-25 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-25 | `BMO` | cash | leftover split 126.46 < 1 share @ 175.01 |
| 2026-08-25 | `DKS` | cash | leftover split 126.46 < 1 share @ 142.36 |
| 2026-08-26 | `AG` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `BHP` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `CDE` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `HDSN` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `IAG` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `KGC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `NFGC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `WPM` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-26 | `ARCT` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-26 | `AUTL` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-26 | `CRDL` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-26 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-26 | `BNS` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `EH` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `GFI` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `GRRR` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `SHMD` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-26 | `DKS` | cash | leftover split 10.39 < 1 share @ 121.87 |
| 2026-08-26 | `ANF` | cash | leftover split 10.39 < 1 share @ 131.37 |
| 2026-08-26 | `BBWI` | cash | leftover split 10.39 < 1 share @ 18.26 |
| 2026-08-26 | `BOX` | cash | leftover split 10.39 < 1 share @ 34.30 |
| 2026-08-26 | `DY` | cash | leftover split 10.39 < 1 share @ 326.91 |
| 2026-08-27 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `ARCT` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `AUTL` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `CRDL` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `BNS` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `BZ` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `EH` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `GFI` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `GRRR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `SHMD` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-27 | `OCUL` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-27 | `INSP` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-27 | `CRMD` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-27 | `RZLT` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-27 | `HCA` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-27 | `SLQT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-27 | `TIGR` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `OCUL` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-28 | `INSP` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-28 | `CRMD` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-28 | `RZLT` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-28 | `HCA` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-28 | `SLQT` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `TIGR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-08-28 | `BBY` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `BILI` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `CM` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `CMBT` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `CSIQ` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `HQY` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `RY` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `TD` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-28 | `ADSK` | cash | leftover split 96.52 < 1 share @ 261.16 |
| 2026-08-28 | `ESTC` | cash | leftover split 96.52 < 1 share @ 103.89 |
| 2026-08-31 | `MOS` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `OCUL` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `INSP` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `CRMD` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `RZLT` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `HCA` | min_hold | flatten_h5: dropped but min-hold 4/5 |
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
| 2026-08-31 | `BBAR` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `FINV` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `FRO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `GAP` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `HAFN` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `IREN` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new long union_e_fresh_h3 |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new long flatten_h5 |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new long flatten_h5 |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new long flatten_h5 |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new long flatten_h5 |
| 2026-09-01 | `RRC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-01 | `CRK` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-01 | `SLI` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-01 | `BBAR` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `FINV` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `FRO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `GAP` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `HAFN` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `IREN` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new long union_e_fresh_h3 |
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
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h3 |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h3 |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h3 |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h3 |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h3 |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new long union_e_fresh_h3 |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
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
| 2026-09-04 | `AMBA` | cash | leftover split 3.81 < 1 share @ 63.18 |
| 2026-09-04 | `ASAN` | cash | leftover split 3.81 < 1 share @ 8.74 |
| 2026-09-04 | `DOCU` | cash | leftover split 3.81 < 1 share @ 68.52 |
| 2026-09-04 | `GWRE` | cash | leftover split 3.81 < 1 share @ 167.55 |
| 2026-09-04 | `IOT` | cash | leftover split 3.81 < 1 share @ 44.90 |
| 2026-09-04 | `LULU` | cash | leftover split 3.81 < 1 share @ 98.15 |
| 2026-09-04 | `MAMA` | cash | leftover split 3.81 < 1 share @ 15.70 |
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
| 2026-09-08 | `DOMO` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-08 | `ALEC` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-08 | `BHC` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-08 | `BMEA` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-08 | `OABI` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-08 | `OPK` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-08 | `VIR` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h3 |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h3 |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new long union_e_fresh_h3 |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new long flatten_h5 |
| 2026-09-08 | `SID` | hard_red | hard-red S=-11.47 sit; no new long flatten_h5 |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new long flatten_h5 |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new long flatten_h5 |
| 2026-09-09 | `ATRC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-09 | `HRMY` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-09 | `CABA` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-09 | `VSTM` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-09 | `RVTY` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-09 | `DOMO` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-09 | `ALEC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-09 | `BHC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-09 | `BMEA` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-09 | `OABI` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-09 | `OPK` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-09 | `VIR` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-09 | `ABM` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h3 |
| 2026-09-09 | `ASO` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h3 |
| 2026-09-09 | `AVO` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h3 |
| 2026-09-09 | `JMKE` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h3 |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h3 |
| 2026-09-09 | `SAIL` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h3 |
| 2026-09-09 | `TTAN` | hard_red | hard-red S=-13.95 sit; no new long union_e_fresh_h3 |
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
| 2026-09-10 | `OPK` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-10 | `VIR` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h3 |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h3 |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h3 |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h3 |
| 2026-09-10 | `JMKE` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h3 |
| 2026-09-10 | `AEO` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h3 |
| 2026-09-10 | `AVAV` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h3 |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new long union_e_fresh_h3 |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new long flatten_h5 |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new long flatten_h5 |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new long flatten_h5 |
| 2026-09-10 | `OIS` | hard_red | hard-red S=-13.28 sit; no new long flatten_h5 |
| 2026-09-11 | `ALEC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-11 | `BHC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-11 | `BMEA` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-11 | `OABI` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-11 | `OPK` | min_hold | flatten_h5: dropped but min-hold 4/5 |
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
| 2026-09-15 | `FPS` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h3 |
| 2026-09-15 | `HITI` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h3 |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h3 |
| 2026-09-15 | `UROY` | hard_red | hard-red S=-3.84 sit; no new long union_e_fresh_h3 |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new long flatten_h5 |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new long flatten_h5 |
| 2026-09-15 | `WAY` | hard_red | hard-red S=-3.84 sit; no new long flatten_h5 |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new long flatten_h5 |
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
| 2026-09-17 | `LEN` | cash | leftover split 13.47 < 1 share @ 81.00 |
| 2026-09-17 | `ILMN` | cash | leftover split 13.08 < 1 share @ 233.85 |
| 2026-09-17 | `TWST` | cash | leftover split 13.08 < 1 share @ 151.43 |
| 2026-09-17 | `RVTY` | cash | leftover split 13.08 < 1 share @ 147.61 |
| 2026-09-17 | `AMN` | cash | leftover split 13.08 < 1 share @ 34.93 |
| 2026-09-18 | `FPS` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `TCOM` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-18 | `IQV` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-18 | `RDNT` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-18 | `AVAH` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-18 | `BLFS` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-18 | `ALMU` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-18 | `IOVA` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-18 | `PGEN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `IQV` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-21 | `RDNT` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-21 | `AVAH` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-21 | `BLFS` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-21 | `ALMU` | min_hold | union_e_fresh_h3: dropped but min-hold 2/3 |
| 2026-09-21 | `RBRK` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `DELL` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `GNRC` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `VICR` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `ECO` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `FIVN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `HUM` | cash | leftover split 290.73 < 1 share @ 386.20 |
| 2026-09-22 | `IQV` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-22 | `RDNT` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-22 | `AVAH` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-22 | `BLFS` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-22 | `ALMU` | no_price | no 09:30 open — carry |
| 2026-09-22 | `IOVA` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-22 | `PGEN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-22 | `RBRK` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `DELL` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `GNRC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `VICR` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `ECO` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `FIVN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `A` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-22 | `DXCM` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-22 | `MGTX` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-22 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-22 | `ABVX` | no_price | no 09:30 open |
| 2026-09-22 | `ANAB` | no_price | no 09:30 open |
| 2026-09-22 | `MLKN` | no_price | no 09:30 open |
| 2026-09-22 | `THO` | no_price | no 09:30 open |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `USFD` | cash | leftover split 75.10 < 1 share @ 93.97 |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-23 | `IOVA` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-23 | `RBRK` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `DELL` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `GNRC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `VICR` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `ECO` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `FIVN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `MGTX` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-23 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-23 | `CTAS` | cash | leftover split 158.76 < 1 share @ 196.78 |
| 2026-09-24 | `RBRK` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `DELL` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `GNRC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `VICR` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `ECO` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `FIVN` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `A` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-24 | `DXCM` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-24 | `MGTX` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-24 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-24 | `CBRL` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `GIS` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `KBH` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `PAYX` | min_hold | union_e_fresh_h3: dropped but min-hold 1/3 |
| 2026-09-24 | `HALO` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-24 | `ARQT` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-24 | `ADMA` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-24 | `FTRE` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-24 | `OMER` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-24 | `BB` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h3 |
| 2026-09-24 | `DRI` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h3 |
| 2026-09-24 | `FUL` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h3 |
| 2026-09-24 | `NEOV` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h3 |
| 2026-09-24 | `SNX` | hard_red | hard-red S=-7.66 sit; no new long union_e_fresh_h3 |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `CHKP` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `S` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `BAH` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `RBRK` | 11 | 2026-09-18 @ $108.55 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+21.3; combo leftover $1229.99; owner flatten_h5 |
| `DELL` | 2 | 2026-09-18 @ $593.15 | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+16.1; combo leftover $1229.99; owner flatten_h5 |
| `GNRC` | 5 | 2026-09-18 @ $209.52 | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.1; combo leftover $1229.99; owner flatten_h5 |
| `VICR` | 5 | 2026-09-18 @ $219.62 | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+21.5; combo leftover $1229.99; owner flatten_h5 |
| `ECO` | 14 | 2026-09-18 @ $85.00 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+18.3; combo leftover $1229.99; owner flatten_h5 |
| `FIVN` | 35 | 2026-09-18 @ $34.44 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+14.0; combo leftover $1229.99; owner flatten_h5 |
| `A` | 1 | 2026-09-21 @ $157.87 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+6.5; combo leftover $290.73; owner flatten_h5 |
| `DXCM` | 3 | 2026-09-21 @ $88.83 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.6; combo leftover $290.73; owner flatten_h5 |
| `MGTX` | 21 | 2026-09-21 @ $13.47 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.6; combo leftover $290.73; owner flatten_h5 |
| `CYPH` | 72 | 2026-09-21 @ $4.00 | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); ret5=+58.9; combo leftover $290.73; owner flatten_h5 |
| `CBRL` | 3 | 2026-09-23 @ $47.57 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ret5=-11.2; combo leftover $158.76; owner union_e_fresh_h3 |
| `GIS` | 4 | 2026-09-23 @ $35.74 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-3.1; combo leftover $158.76; owner union_e_fresh_h3 |
| `KBH` | 3 | 2026-09-23 @ $47.15 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; ret5=-1.9; combo leftover $158.76; owner union_e_fresh_h3 |
| `PAYX` | 1 | 2026-09-23 @ $109.67 | union ∩ e_fresh, no 🚨; gate days_since_E_max=1,flag_E_min=0; list earn_react; 🔵; ⚪; ret5=-3.0; combo leftover $158.76; owner union_e_fresh_h3 |
| `HALO` | 3 | 2026-09-23 @ $116.85 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.3; combo leftover $420.77; owner flatten_h5 |
| `ARQT` | 15 | 2026-09-23 @ $27.79 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; combo leftover $420.77; owner flatten_h5 |
| `ADMA` | 42 | 2026-09-23 @ $9.81 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; combo leftover $420.77; owner flatten_h5 |
| `FTRE` | 20 | 2026-09-23 @ $20.25 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; combo leftover $420.77; owner flatten_h5 |
| `OMER` | 20 | 2026-09-23 @ $20.65 | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.9; combo leftover $420.77; owner flatten_h5 |
