# Factor mine action — `combo_jf_5050_shared`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **owner mix** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Combination book: each member still runs its own leak-free 09:30 `pick_day`. Shared leftover (or split cash) · one ticker one side · official 09:30 / 16:00 · owner min-hold. Does not change live `flatten_robust`.

Side **mix** · universe `combo` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · shared union_join_vol_green_h1/flatten_h5 w=0.5,0.5 net=priority

Cash book **-11.63%** ($8,837) · signal-only (no cash/fees) was —. Starts YES **2/30**. Fills 311 · skips 364 · realized $+311.24.

## How this sleeve decides (like you are 10)

Imagine 2 kids at the same 09:30 school bell sharing one $10,000 book: union_join_vol_green_h1 50%, flatten_h5 50%. This is not a new shopping list mashed together. Each kid still uses only their own leak-free 09:30 list and never peeks at today's Change%, Gap, RelVol, or the printed book. They share one leftover-cash pile. After sells, leftover is offered in claim order. A kid who cannot spend their slice leaves the unused cash for the next kid. Weights still cap each kid’s share of whatever cash is left. If two kids want the same name, the earlier claim wins (fresh-E, then heat, then other longs, then shorts). They never open a second lot in that name. Money, fills, fees, min-hold, and the hard-red sit are the same rules as every other sleeve on this board. A lot remembers which kid bought it, so that kid’s hold timer and list-drop apply. A shared mix can beat both kids because unused leftover spills — it is not the average of their Book%.

### What it looks at (inputs)

- Shopping list: each member keeps its own 09:30 list. This combo does not invent a mashed list.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees. Close marks are the official 16:00 print. A missing open is never replaced by the close.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys). Lots already held are not dumped just because S is red.
- Members and weights: union_join_vol_green_h1 50%, flatten_h5 50%.
- Member: union_join_vol_green_h1 (50% · long · hold 1).
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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $512.88.

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
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 4 | $1.50 | $0.07 | — | $91.46 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=+9.2; combo leftover $6.10; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 1 | $4.31 | $0.05 | — | $87.11 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; combo leftover $6.10; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 1 | $4.18 | $0.04 | — | $82.88 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; combo leftover $6.10; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 2 | $2.69 | $0.06 | — | $77.44 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; combo leftover $6.10; owner union_join_vol_green_h1 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $68.34 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-13.5; combo leftover $11.06; owner flatten_h5 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 11 | $0.94 | $0.14 | — | $57.90 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.5; combo leftover $11.06; owner flatten_h5 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.90 | ▲ close $10,435.44 vs 09:30 $10,178.12 (session +257.77) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.90 | ▼ 09:30 equity $10,415.13 vs yday $10,435.44 (-20.31) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 4 | $1.52 | $0.09 | $-0.08 | $63.88 | ▼ -0.08 after sell → book $10,415.04; vs 09:30 mark -0.09 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 1 | $4.60 | $0.07 | $+0.17 | $68.41 | ▲ +0.17 after sell → book $10,414.97; vs 09:30 mark -0.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 1 | $4.10 | $0.06 | $-0.19 | $72.45 | ▼ -0.19 after sell → book $10,414.91; vs 09:30 mark -0.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NCMI` | 2 | $2.80 | $0.08 | $+0.08 | $77.97 | ▲ +0.08 after sell → book $10,414.83; vs 09:30 mark -0.08 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `BORR` | 1 | $4.59 | $0.05 | — | $73.33 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ⚪; ret5=+14.8; combo leftover $7.80; owner union_join_vol_green_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 1 | $4.19 | $0.04 | — | $69.09 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ⚪; ret5=+291.8; combo leftover $7.80; owner union_join_vol_green_h1 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 2 | $4.05 | $0.09 | — | $60.91 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; combo leftover $8.64; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 1 | $8.46 | $0.09 | — | $52.36 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.4; combo leftover $8.64; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 2 | $3.24 | $0.07 | — | $45.81 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; combo leftover $8.64; owner flatten_h5 | — |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 1 | $4.81 | $0.05 | — | $40.95 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-11.4; combo leftover $8.64; owner flatten_h5 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $40.95 | ▲ close $10,524.06 vs 09:30 $10,415.13 (session +109.62) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.95 | ▼ 09:30 equity $10,391.02 vs yday $10,524.06 (-133.04) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `BORR` | 1 | $4.56 | $0.07 | $-0.15 | $45.44 | ▼ -0.15 after sell → book $10,390.95; vs 09:30 mark -0.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 1 | $3.94 | $0.06 | $-0.36 | $49.32 | ▼ -0.36 after sell → book $10,390.89; vs 09:30 mark -0.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.32 | ▲ close $10,572.21 vs 09:30 $10,391.02 (session +181.33) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.32 | ▲ 09:30 equity $10,710.51 vs yday $10,572.21 (+138.30) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $49.32 | ▲ close $11,031.48 vs 09:30 $10,710.51 (session +320.98) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.32 | ▼ 09:30 equity $10,966.08 vs yday $11,031.48 (-65.40) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 20 | $58.64 | $2.07 | $-27.32 | $1,220.05 | ▼ -27.32 after sell → book $10,964.01; vs 09:30 mark -2.07 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 27 | $42.46 | $2.09 | $-99.20 | $2,364.38 | ▼ -99.20 after sell → book $10,961.92; vs 09:30 mark -2.09 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 24 | $53.06 | $2.08 | $+54.34 | $3,635.73 | ▲ +54.34 after sell → book $10,959.83; vs 09:30 mark -2.09 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGTX` | 25 | $51.65 | $2.09 | $+44.60 | $4,924.90 | ▲ +44.60 after sell → book $10,957.75; vs 09:30 mark -2.08 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 106 | $13.84 | $2.34 | $+222.19 | $6,389.60 | ▲ +222.19 after sell → book $10,955.41; vs 09:30 mark -2.34 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `HIMS` | 42 | $30.66 | $2.14 | $+34.39 | $7,675.18 | ▲ +34.39 after sell → book $10,953.28; vs 09:30 mark -2.13 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `INO` | 1543 | $1.30 | $20.18 | $+718.77 | $9,660.91 | ▲ +718.77 after sell → book $10,933.10; vs 09:30 mark -20.18 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 53 | $23.11 | $2.17 | $-15.98 | $10,883.57 | ▼ -15.98 after sell → book $10,930.93; vs 09:30 mark -2.17 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 33 | $20.55 | $2.09 | — | $10,203.33 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; combo leftover $680.22; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 32 | $20.65 | $2.09 | — | $9,540.44 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; combo leftover $680.22; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 117 | $5.77 | $2.34 | — | $8,863.01 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; combo leftover $680.22; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 34 | $19.63 | $2.09 | — | $8,193.50 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; combo leftover $680.22; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 22 | $29.63 | $2.06 | — | $7,539.58 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; combo leftover $680.22; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 388 | $1.75 | $5.01 | — | $6,855.58 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; combo leftover $680.22; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 4 | $144.54 | $2.00 | — | $6,275.42 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; combo leftover $680.22; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 138 | $4.92 | $2.40 | — | $5,594.05 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; combo leftover $680.22; owner union_join_vol_green_h1 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 61 | $91.01 | $2.17 | — | $40.27 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; combo leftover $5594.05; owner flatten_h5 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $40.27 | ▲ close $11,153.49 vs 09:30 $10,966.08 (session +244.81) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.27 | ▲ 09:30 equity $11,469.59 vs yday $11,153.49 (+316.10) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 1 | $11.70 | $0.14 | $+2.46 | $51.83 | ▲ +2.46 after sell → book $11,469.45; vs 09:30 mark -0.14 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 11 | $0.87 | $0.15 | $-1.05 | $61.22 | ▼ -1.05 after sell → book $11,469.30; vs 09:30 mark -0.15 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 33 | $21.90 | $2.11 | $+40.35 | $781.81 | ▲ +40.35 after sell → book $11,467.19; vs 09:30 mark -2.11 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 32 | $21.75 | $2.11 | $+31.01 | $1,475.70 | ▲ +31.01 after sell → book $11,465.08; vs 09:30 mark -2.11 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 117 | $5.67 | $2.37 | $-16.41 | $2,136.72 | ▼ -16.41 after sell → book $11,462.71; vs 09:30 mark -2.37 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 34 | $21.17 | $2.11 | $+48.16 | $2,854.39 | ▲ +48.16 after sell → book $11,460.60; vs 09:30 mark -2.11 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 22 | $32.17 | $2.08 | $+51.75 | $3,560.06 | ▲ +51.75 after sell → book $11,458.53; vs 09:30 mark -2.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 388 | $1.79 | $5.08 | $+5.44 | $4,249.50 | ▲ +5.44 after sell → book $11,453.45; vs 09:30 mark -5.08 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 4 | $154.70 | $2.02 | $+36.62 | $4,866.27 | ▲ +36.62 after sell → book $11,451.42; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 138 | $5.20 | $2.44 | $+33.80 | $5,581.44 | ▲ +33.80 after sell → book $11,448.99; vs 09:30 mark -2.43 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 2 | $119.43 | $2.00 | — | $5,340.58 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; combo leftover $348.84; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 20 | $17.20 | $2.05 | — | $4,994.53 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; combo leftover $348.84; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 1 | $216.30 | $1.99 | — | $4,776.24 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; combo leftover $348.84; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 31 | $11.13 | $2.08 | — | $4,429.13 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; combo leftover $348.84; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 264 | $1.32 | $3.41 | — | $4,077.24 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; combo leftover $348.84; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 210 | $1.66 | $2.71 | — | $3,725.93 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; combo leftover $348.84; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `INDP` | 250 | $1.39 | $3.23 | — | $3,375.21 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+30.2; combo leftover $348.84; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 42 | $8.28 | $2.12 | — | $3,025.33 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; combo leftover $348.84; owner union_join_vol_green_h1 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 408 | $2.47 | $5.26 | — | $2,012.31 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; combo leftover $1008.44; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 522 | $1.93 | $6.73 | — | $998.11 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; combo leftover $1008.44; owner flatten_h5 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 16 | $59.72 | $2.04 | — | $40.55 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.6; combo leftover $1008.44; owner flatten_h5 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $40.55 | ▲ close $11,486.41 vs 09:30 $11,469.59 (session +71.04) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.55 | ▲ 09:30 equity $11,589.66 vs yday $11,486.41 (+103.25) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 2 | $4.62 | $0.12 | $+0.94 | $49.69 | ▲ +0.94 after sell → book $11,589.55; vs 09:30 mark -0.11 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `TGB` | 1 | $9.26 | $0.12 | $+0.60 | $58.83 | ▲ +0.60 after sell → book $11,589.43; vs 09:30 mark -0.12 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 2 | $3.50 | $0.10 | $+0.35 | $65.73 | ▲ +0.35 after sell → book $11,589.33; vs 09:30 mark -0.10 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `HNST` | 1 | $5.05 | $0.07 | $+0.12 | $70.71 | ▲ +0.12 after sell → book $11,589.26; vs 09:30 mark -0.07 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 2 | $120.51 | $2.02 | $-1.85 | $309.72 | ▼ -1.85 after sell → book $11,587.25; vs 09:30 mark -2.01 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 20 | $16.57 | $2.07 | $-16.72 | $639.05 | ▼ -16.72 after sell → book $11,585.18; vs 09:30 mark -2.07 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 1 | $217.03 | $2.01 | $-3.28 | $854.06 | ▼ -3.28 after sell → book $11,583.16; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 31 | $13.33 | $2.10 | $+64.01 | $1,265.19 | ▲ +64.01 after sell → book $11,581.06; vs 09:30 mark -2.10 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 264 | $1.83 | $3.46 | $+127.77 | $1,744.85 | ▲ +127.77 after sell → book $11,577.60; vs 09:30 mark -3.46 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 210 | $1.55 | $2.75 | $-28.56 | $2,067.60 | ▼ -28.56 after sell → book $11,574.85; vs 09:30 mark -2.75 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `INDP` | 250 | $1.24 | $3.28 | $-44.00 | $2,374.32 | ▼ -44.00 after sell → book $11,571.57; vs 09:30 mark -3.28 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 42 | $8.59 | $2.14 | $+8.77 | $2,732.96 | ▲ +8.77 after sell → book $11,569.43; vs 09:30 mark -2.14 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,732.96 | ▼ close $11,496.73 vs 09:30 $11,589.66 (session -72.70) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,732.96 | ▼ 09:30 equity $11,464.92 vs yday $11,496.73 (-31.81) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 104 | $1.63 | $2.01 | — | $2,561.44 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; combo leftover $170.81; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 48 | $3.55 | $1.85 | — | $2,389.19 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+27.9; combo leftover $170.81; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 26 | $6.37 | $1.73 | — | $2,221.83 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+10.9; combo leftover $170.81; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 4 | $35.05 | $1.41 | — | $2,080.22 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+19.7; combo leftover $170.81; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `ETON` | 2 | $64.55 | $1.30 | — | $1,949.82 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+4.4; combo leftover $170.81; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `WPM` | 1 | $156.51 | $1.57 | — | $1,791.74 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+17.4; combo leftover $170.81; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUZ` | 19 | $8.98 | $1.76 | — | $1,619.36 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+15.4; combo leftover $170.81; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `IAUX` | 89 | $1.90 | $1.96 | — | $1,448.30 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+16.4; combo leftover $170.81; owner union_join_vol_green_h1 | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 10 | $23.77 | $2.02 | — | $1,208.58 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.0; combo leftover $241.38; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 21 | $10.98 | $2.05 | — | $975.95 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+1.2; combo leftover $241.38; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 3 | $61.19 | $1.84 | — | $790.54 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+7.4; combo leftover $241.38; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 28 | $8.35 | $2.07 | — | $554.66 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.0; combo leftover $241.38; owner flatten_h5 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 48 | $4.94 | $2.13 | — | $315.41 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; combo leftover $241.38; owner flatten_h5 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $315.41 | ▲ close $11,787.16 vs 09:30 $11,464.92 (session +345.95) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $315.41 | ▼ 09:30 equity $11,665.89 vs yday $11,787.16 (-121.27) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 104 | $1.75 | $2.16 | $+8.83 | $495.76 | ▲ +8.83 after sell → book $11,663.72; vs 09:30 mark -2.17 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GORO` | 48 | $3.77 | $1.97 | $+6.74 | $674.75 | ▲ +6.74 after sell → book $11,661.75; vs 09:30 mark -1.97 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZURA` | 26 | $6.13 | $1.69 | $-9.67 | $832.44 | ▼ -9.67 after sell → book $11,660.06; vs 09:30 mark -1.69 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 4 | $35.70 | $1.46 | $-0.27 | $973.78 | ▼ -0.27 after sell → book $11,658.60; vs 09:30 mark -1.46 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ETON` | 2 | $63.60 | $1.30 | $-4.49 | $1,099.68 | ▼ -4.49 after sell → book $11,657.30; vs 09:30 mark -1.30 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `WPM` | 1 | $160.93 | $1.63 | $+1.22 | $1,258.98 | ▲ +1.22 after sell → book $11,655.67; vs 09:30 mark -1.63 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SUZ` | 19 | $9.03 | $1.79 | $-2.61 | $1,428.76 | ▼ -2.61 after sell → book $11,653.88; vs 09:30 mark -1.79 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `IAUX` | 89 | $1.87 | $1.96 | $-6.58 | $1,593.23 | ▼ -6.58 after sell → book $11,651.92; vs 09:30 mark -1.96 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 137 | $5.81 | $2.40 | — | $794.86 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ⚪; ret5=+117.2; combo leftover $796.61; owner union_join_vol_green_h1 | — |
| 2026-08-26 09:30 ET | **BUY** | `HCA` | 1 | $427.50 | $1.99 | — | $365.37 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.1; combo leftover $794.86; owner flatten_h5 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $365.37 | ▼ close $11,633.33 vs 09:30 $11,665.89 (session -14.20) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $365.37 | ▼ 09:30 equity $11,621.26 vs yday $11,633.33 (-12.07) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 61 | $95.52 | $2.23 | $+270.71 | $6,189.86 | ▲ +270.71 after sell → book $11,619.04; vs 09:30 mark -2.22 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 137 | $6.50 | $2.43 | $+89.70 | $7,077.92 | ▲ +89.70 after sell → book $11,616.60; vs 09:30 mark -2.44 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `DKS` | 27 | $128.73 | $2.07 | — | $3,600.14 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-32.2; combo leftover $3538.96; owner union_join_vol_green_h1 | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 28 | $41.44 | $2.07 | — | $2,437.75 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.1; combo leftover $1200.05; owner flatten_h5 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 83 | $14.42 | $2.24 | — | $1,238.65 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+7.1; combo leftover $1200.05; owner flatten_h5 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 461 | $2.60 | $5.95 | — | $34.10 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+13.0; combo leftover $1200.05; owner flatten_h5 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.10 | ▲ close $11,714.77 vs 09:30 $11,621.26 (session +110.50) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.10 | ▲ 09:30 equity $11,748.46 vs yday $11,714.77 (+33.69) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 408 | $2.35 | $5.34 | $-59.56 | $987.56 | ▼ -59.56 after sell → book $11,743.12; vs 09:30 mark -5.34 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 522 | $2.06 | $6.83 | $+54.30 | $2,056.05 | ▲ +54.30 after sell → book $11,736.29; vs 09:30 mark -6.83 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRSP` | 16 | $58.22 | $2.06 | $-28.10 | $2,985.51 | ▼ -28.10 after sell → book $11,734.23; vs 09:30 mark -2.06 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 27 | $132.80 | $2.11 | $+105.71 | $6,569.00 | ▲ +105.71 after sell → book $11,732.12; vs 09:30 mark -2.11 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 11 | $146.07 | $2.02 | — | $4,960.21 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+38.8; combo leftover $1642.25; owner union_join_vol_green_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 70 | $23.30 | $2.20 | — | $3,327.01 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+14.5; combo leftover $1642.25; owner union_join_vol_green_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 86 | $19.00 | $2.25 | — | $1,690.76 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; ret5=+7.5; combo leftover $1642.25; owner union_join_vol_green_h1 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 66 | $24.69 | $2.19 | — | $59.03 | — | combo gate; gate join=good,vol=good,last_green=True; list earn_react; ret5=+5.8; combo leftover $1642.25; owner union_join_vol_green_h1 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $59.03 | ▼ close $11,478.29 vs 09:30 $11,748.46 (session -245.17) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.03 | ▼ 09:30 equity $11,425.46 vs yday $11,478.29 (-52.83) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 11 | $148.03 | $2.05 | $+17.49 | $1,685.32 | ▲ +17.49 after sell → book $11,423.42; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NCNO` | 70 | $22.66 | $2.22 | $-49.22 | $3,269.29 | ▼ -49.22 after sell → book $11,421.19; vs 09:30 mark -2.23 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TH` | 86 | $18.12 | $2.27 | $-79.77 | $4,825.77 | ▼ -79.77 after sell → book $11,418.92; vs 09:30 mark -2.27 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GAP` | 66 | $22.98 | $2.21 | $-117.26 | $6,340.24 | ▼ -117.26 after sell → book $11,416.71; vs 09:30 mark -2.21 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,340.24 | ▲ close $11,447.07 vs 09:30 $11,425.46 (session +30.36) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,340.24 | ▲ 09:30 equity $11,577.86 vs yday $11,447.07 (+130.79) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `MOS` | 10 | $23.94 | $2.04 | $-2.36 | $6,577.60 | ▼ -2.36 after sell → book $11,575.82; vs 09:30 mark -2.04 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `OCUL` | 21 | $10.42 | $2.07 | $-15.89 | $6,794.34 | ▼ -15.89 after sell → book $11,573.74; vs 09:30 mark -2.08 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `INSP` | 3 | $63.00 | $1.92 | $+1.67 | $6,981.42 | ▲ +1.67 after sell → book $11,571.82; vs 09:30 mark -1.92 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 28 | $8.25 | $2.09 | $-6.97 | $7,210.33 | ▼ -6.97 after sell → book $11,569.73; vs 09:30 mark -2.09 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 48 | $4.64 | $2.15 | $-18.69 | $7,430.90 | ▼ -18.69 after sell → book $11,567.58; vs 09:30 mark -2.15 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,430.90 | ▼ close $11,509.23 vs 09:30 $11,577.86 (session -58.35) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,430.90 | ▼ 09:30 equity $11,473.15 vs yday $11,509.23 (-36.08) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `HCA` | 1 | $412.46 | $2.01 | $-19.05 | $7,841.34 | ▼ -19.05 after sell → book $11,471.13; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,841.34 | ▼ close $11,431.61 vs 09:30 $11,473.15 (session -39.52) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,841.34 | ▲ 09:30 equity $11,459.62 vs yday $11,431.61 (+28.01) | — | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 28 | $42.43 | $2.09 | $+23.55 | $9,027.29 | ▲ +23.55 after sell → book $11,457.53; vs 09:30 mark -2.09 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `CRK` | 83 | $15.45 | $2.26 | $+80.99 | $10,307.38 | ▲ +80.99 after sell → book $11,455.27; vs 09:30 mark -2.26 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 461 | $2.49 | $6.03 | $-62.69 | $11,449.23 | ▼ -62.69 after sell → book $11,449.23; vs 09:30 mark -6.04 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 5 | $132.45 | $2.00 | — | $10,784.98 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; combo leftover $715.58; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 42 | $16.77 | $2.12 | — | $10,078.52 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; combo leftover $715.58; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 328 | $2.18 | $4.23 | — | $9,359.25 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; combo leftover $715.58; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 29 | $23.88 | $2.08 | — | $8,664.65 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; combo leftover $715.58; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 68 | $10.42 | $2.19 | — | $7,953.90 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; combo leftover $715.58; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 370 | $1.93 | $4.77 | — | $7,235.03 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; combo leftover $715.58; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `DUOL` | 4 | $161.54 | $2.00 | — | $6,586.86 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+12.0; combo leftover $715.58; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `ALMS` | 68 | $10.38 | $2.19 | — | $5,879.17 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ret5=-56.2; combo leftover $715.58; owner union_join_vol_green_h1 | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 27 | $52.88 | $2.07 | — | $4,449.34 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; combo leftover $1469.79; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 34 | $42.93 | $2.09 | — | $2,987.63 | — | baseline list, no extra gate; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.1; combo leftover $1469.79; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 404 | $3.63 | $5.21 | — | $1,515.90 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.8; combo leftover $1469.79; owner flatten_h5 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 183 | $8.03 | $2.54 | — | $43.87 | — | baseline list, no extra gate; list flatten,ohlc_hot,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.5; combo leftover $1469.79; owner flatten_h5 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $43.87 | ▼ close $11,273.88 vs 09:30 $11,459.62 (session -141.85) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $43.87 | ▼ 09:30 equity $11,221.14 vs yday $11,273.88 (-52.74) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 5 | $130.03 | $2.02 | $-16.13 | $691.99 | ▼ -16.13 after sell → book $11,219.11; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 42 | $15.61 | $2.14 | $-52.97 | $1,345.48 | ▼ -52.97 after sell → book $11,216.98; vs 09:30 mark -2.13 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 328 | $2.16 | $4.30 | $-15.09 | $2,049.66 | ▼ -15.09 after sell → book $11,212.68; vs 09:30 mark -4.30 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 29 | $23.84 | $2.10 | $-5.33 | $2,738.92 | ▼ -5.33 after sell → book $11,210.58; vs 09:30 mark -2.10 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 68 | $10.50 | $2.22 | $+1.03 | $3,450.71 | ▲ +1.03 after sell → book $11,208.37; vs 09:30 mark -2.21 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `BMEA` | 370 | $1.90 | $4.84 | $-20.72 | $4,148.86 | ▼ -20.72 after sell → book $11,203.52; vs 09:30 mark -4.85 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DUOL` | 4 | $157.46 | $2.02 | $-20.34 | $4,776.68 | ▼ -20.34 after sell → book $11,201.50; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ALMS` | 68 | $11.23 | $2.22 | $+53.73 | $5,538.11 | ▲ +53.73 after sell → book $11,199.29; vs 09:30 mark -2.21 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 4 | $82.70 | $2.00 | — | $5,205.30 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+15.7; combo leftover $346.13; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 137 | $2.51 | $2.40 | — | $4,859.03 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+21.8; combo leftover $346.13; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 13 | $25.18 | $2.03 | — | $4,529.66 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+16.0; combo leftover $346.13; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 59 | $5.79 | $2.17 | — | $4,185.89 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+15.2; combo leftover $346.13; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `TDS` | 9 | $37.44 | $2.02 | — | $3,846.91 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+14.1; combo leftover $346.13; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `AHCO` | 54 | $6.32 | $2.15 | — | $3,503.48 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+10.7; combo leftover $346.13; owner union_join_vol_green_h1 | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 231 | $2.52 | $2.98 | — | $2,918.38 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+5.0; combo leftover $583.91; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 87 | $6.71 | $2.25 | — | $2,332.36 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; combo leftover $583.91; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 307 | $1.90 | $3.96 | — | $1,745.10 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.7; combo leftover $583.91; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 122 | $4.78 | $2.36 | — | $1,159.58 | — | baseline list, no extra gate; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; combo leftover $583.91; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 367 | $1.59 | $4.73 | — | $571.32 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; combo leftover $583.91; owner flatten_h5 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 50 | $11.31 | $2.14 | — | $3.68 | — | baseline list, no extra gate; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+1.2; combo leftover $583.91; owner flatten_h5 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.68 | ▲ close $11,312.90 vs 09:30 $11,221.14 (session +144.80) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.68 | ▼ 09:30 equity $11,309.63 vs yday $11,312.90 (-3.27) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `TARS` | 4 | $89.67 | $2.02 | $+23.86 | $360.34 | ▲ +23.86 after sell → book $11,307.61; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BRR` | 137 | $2.66 | $2.43 | $+15.72 | $722.32 | ▲ +15.72 after sell → book $11,305.17; vs 09:30 mark -2.44 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 13 | $26.44 | $2.05 | $+12.30 | $1,063.99 | ▲ +12.30 after sell → book $11,303.12; vs 09:30 mark -2.05 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 59 | $5.81 | $2.19 | $-3.17 | $1,404.60 | ▼ -3.17 after sell → book $11,300.94; vs 09:30 mark -2.18 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TDS` | 9 | $37.75 | $2.04 | $-1.26 | $1,742.31 | ▼ -1.26 after sell → book $11,298.90; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `AHCO` | 54 | $6.48 | $2.17 | $+4.32 | $2,090.06 | ▲ +4.32 after sell → book $11,296.73; vs 09:30 mark -2.17 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,090.06 | ▼ close $11,153.17 vs 09:30 $11,309.63 (session -143.56) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,090.06 | ▼ 09:30 equity $11,111.36 vs yday $11,153.17 (-41.81) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,090.06 | ▼ close $10,787.82 vs 09:30 $11,111.36 (session -323.53) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,090.06 | ▼ 09:30 equity $10,678.89 vs yday $10,787.82 (-108.93) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,090.06 | ▼ close $10,520.86 vs 09:30 $10,678.89 (session -158.03) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,090.06 | ▲ 09:30 equity $10,609.38 vs yday $10,520.86 (+88.52) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `ATRC` | 27 | $53.53 | $2.09 | $+13.39 | $3,533.27 | ▲ +13.39 after sell → book $10,607.28; vs 09:30 mark -2.10 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `HRMY` | 34 | $41.30 | $2.11 | $-59.63 | $4,935.36 | ▼ -59.63 after sell → book $10,605.17; vs 09:30 mark -2.11 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CABA` | 404 | $2.77 | $5.29 | $-357.94 | $6,049.15 | ▼ -357.94 after sell → book $10,599.88; vs 09:30 mark -5.29 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `VSTM` | 183 | $7.70 | $2.58 | $-65.51 | $7,455.67 | ▼ -65.51 after sell → book $10,597.30; vs 09:30 mark -2.58 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 19 | $23.63 | $2.05 | — | $7,004.65 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; ret5=-6.3; combo leftover $465.98; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 172 | $2.70 | $2.51 | — | $6,537.75 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+118.8; combo leftover $465.98; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 42 | $10.95 | $2.12 | — | $6,075.73 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+20.8; combo leftover $465.98; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 94 | $4.91 | $2.27 | — | $5,611.92 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+76.3; combo leftover $465.98; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 5 | $84.27 | $2.00 | — | $5,188.57 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ret5=+17.2; combo leftover $465.98; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `ASO` | 8 | $54.91 | $2.01 | — | $4,747.27 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+24.3; combo leftover $465.98; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 75 | $6.16 | $2.21 | — | $4,283.06 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+36.4; combo leftover $465.98; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `COO` | 8 | $54.66 | $2.01 | — | $3,843.76 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-22.3; combo leftover $465.98; owner union_join_vol_green_h1 | — |
| 2026-09-11 09:30 ET | **BUY** | `AUPH` | 39 | $16.28 | $2.11 | — | $3,206.74 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-1.1; combo leftover $640.63; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `OVID` | 234 | $2.73 | $3.02 | — | $2,564.90 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=-3.0; combo leftover $640.63; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 3 | $206.84 | $2.00 | — | $1,942.38 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+8.3; combo leftover $640.63; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 3 | $164.43 | $2.00 | — | $1,447.09 | — | baseline list, no extra gate; list flatten,earn_react; wish-list (live io HOLD — not a ticket); ⚪; ret5=+4.9; combo leftover $640.63; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 4 | $157.78 | $2.00 | — | $813.97 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+4.7; combo leftover $640.63; owner flatten_h5 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 11 | $56.09 | $2.02 | — | $194.95 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+19.6; combo leftover $640.63; owner flatten_h5 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $194.95 | ▼ close $10,521.70 vs 09:30 $10,609.38 (session -45.26) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $194.95 | ▼ 09:30 equity $10,431.48 vs yday $10,521.70 (-90.22) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ALEC` | 231 | $2.15 | $3.03 | $-91.48 | $688.58 | ▼ -91.48 after sell → book $10,428.45; vs 09:30 mark -3.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BHC` | 87 | $5.93 | $2.28 | $-72.39 | $1,202.21 | ▼ -72.39 after sell → book $10,426.18; vs 09:30 mark -2.27 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BMEA` | 307 | $1.72 | $4.02 | $-64.78 | $1,724.69 | ▼ -64.78 after sell → book $10,422.15; vs 09:30 mark -4.03 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OABI` | 122 | $4.13 | $2.39 | $-84.04 | $2,226.17 | ▼ -84.04 after sell → book $10,419.77; vs 09:30 mark -2.38 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OPK` | 367 | $1.59 | $4.81 | $-9.54 | $2,804.89 | ▼ -9.54 after sell → book $10,414.96; vs 09:30 mark -4.81 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIR` | 50 | $10.73 | $2.16 | $-33.30 | $3,339.23 | ▼ -33.30 after sell → book $10,412.80; vs 09:30 mark -2.16 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `TYRA` | 19 | $23.20 | $2.07 | $-12.28 | $3,777.97 | ▼ -12.28 after sell → book $10,410.74; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `INDP` | 172 | $2.80 | $2.54 | $+12.15 | $4,257.02 | ▲ +12.15 after sell → book $10,408.19; vs 09:30 mark -2.55 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `WLTH` | 42 | $10.29 | $2.14 | $-31.97 | $4,687.06 | ▼ -31.97 after sell → book $10,406.05; vs 09:30 mark -2.14 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BNC` | 94 | $5.03 | $2.30 | $+6.71 | $5,157.59 | ▲ +6.71 after sell → book $10,403.76; vs 09:30 mark -2.29 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SWKS` | 5 | $86.06 | $2.02 | $+4.92 | $5,585.86 | ▲ +4.92 after sell → book $10,401.73; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ASO` | 8 | $54.75 | $2.03 | $-5.33 | $6,021.83 | ▼ -5.33 after sell → book $10,399.70; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 75 | $6.02 | $2.24 | $-14.95 | $6,471.09 | ▼ -14.95 after sell → book $10,397.46; vs 09:30 mark -2.24 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `COO` | 8 | $54.78 | $2.03 | $-3.09 | $6,907.30 | ▼ -3.09 after sell → book $10,395.43; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,907.30 | ▼ close $10,347.17 vs 09:30 $10,431.48 (session -48.26) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,907.30 | ▲ 09:30 equity $10,375.36 vs yday $10,347.17 (+28.19) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,907.30 | ▼ close $10,303.26 vs 09:30 $10,375.36 (session -72.10) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,907.30 | ▲ 09:30 equity $10,333.98 vs yday $10,303.26 (+30.72) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 5 | $77.12 | $2.00 | — | $6,519.69 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; ret5=+7.2; combo leftover $431.71; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 73 | $5.87 | $2.21 | — | $6,088.97 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+3.1; combo leftover $431.71; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 4 | $87.40 | $2.00 | — | $5,737.37 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+3.2; combo leftover $431.71; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 15 | $27.09 | $2.04 | — | $5,328.99 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; combo leftover $431.71; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 4 | $89.38 | $2.00 | — | $4,969.46 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; combo leftover $431.71; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 18 | $23.29 | $2.04 | — | $4,548.20 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+16.1; combo leftover $431.71; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 13 | $33.14 | $2.03 | — | $4,115.35 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=-2.9; combo leftover $431.71; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `CAI` | 15 | $28.16 | $2.04 | — | $3,690.92 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ret5=+14.8; combo leftover $431.71; owner union_join_vol_green_h1 | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 4 | $270.89 | $2.00 | — | $2,605.35 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.0; combo leftover $1230.31; owner flatten_h5 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 85 | $14.31 | $2.25 | — | $1,386.76 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.8; combo leftover $1230.31; owner flatten_h5 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 33 | $36.46 | $2.09 | — | $181.49 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+2.9; combo leftover $1230.31; owner flatten_h5 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $181.49 | ▼ close $10,295.67 vs 09:30 $10,333.98 (session -15.61) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $181.49 | ▲ 09:30 equity $10,476.66 vs yday $10,295.67 (+180.99) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `RDNT` | 5 | $76.44 | $2.02 | $-7.43 | $561.66 | ▼ -7.43 after sell → book $10,474.64; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 73 | $5.58 | $2.23 | $-25.61 | $966.77 | ▼ -25.61 after sell → book $10,472.41; vs 09:30 mark -2.23 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 4 | $83.20 | $2.02 | $-20.82 | $1,297.55 | ▼ -20.82 after sell → book $10,470.39; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ADPT` | 15 | $28.23 | $2.06 | $+13.01 | $1,718.95 | ▲ +13.01 after sell → book $10,468.33; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 4 | $86.76 | $2.02 | $-14.50 | $2,063.96 | ▼ -14.50 after sell → book $10,466.31; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 18 | $24.09 | $2.06 | $+10.29 | $2,495.52 | ▲ +10.29 after sell → book $10,464.25; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `CAI` | 15 | $28.59 | $2.06 | $+2.43 | $2,922.39 | ▲ +2.43 after sell → book $10,462.19; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 1 | $147.61 | $1.48 | — | $2,773.30 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; ret5=+17.7; combo leftover $208.74; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 27 | $7.59 | $2.07 | — | $2,566.30 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; 🔵; ret5=+9.4; combo leftover $208.74; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 8 | $25.95 | $2.01 | — | $2,356.69 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; combo leftover $208.74; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 1 | $170.85 | $1.71 | — | $2,184.13 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; combo leftover $208.74; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `CIFR` | 11 | $18.04 | $2.01 | — | $1,983.73 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.1; combo leftover $208.74; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRKR` | 3 | $61.90 | $1.87 | — | $1,796.16 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; 🔵; ret5=+11.5; combo leftover $208.74; owner union_join_vol_green_h1 | — |
| 2026-09-17 09:30 ET | **BUY** | `TWST` | 3 | $151.43 | $2.00 | — | $1,339.87 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.0; combo leftover $598.72; owner flatten_h5 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 58 | $10.25 | $2.16 | — | $743.21 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); 🔵; ret5=+17.1; combo leftover $598.72; owner flatten_h5 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 17 | $34.93 | $2.04 | — | $147.36 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.6; combo leftover $598.72; owner flatten_h5 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $147.36 | ▼ close $10,426.68 vs 09:30 $10,476.66 (session -18.15) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $147.36 | ▲ 09:30 equity $10,476.21 vs yday $10,426.68 (+49.53) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AUPH` | 39 | $16.93 | $2.13 | $+21.12 | $805.50 | ▲ +21.12 after sell → book $10,474.08; vs 09:30 mark -2.13 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `OVID` | 234 | $2.68 | $3.07 | $-17.79 | $1,429.55 | ▼ -17.79 after sell → book $10,471.01; vs 09:30 mark -3.07 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `SANM` | 3 | $197.76 | $2.02 | $-31.26 | $2,020.81 | ▼ -31.26 after sell → book $10,468.99; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `ORCL` | 3 | $150.47 | $2.02 | $-45.90 | $2,470.20 | ▼ -45.90 after sell → book $10,466.97; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `NVT` | 4 | $152.71 | $2.02 | $-24.30 | $3,079.02 | ▼ -24.30 after sell → book $10,464.95; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `COHU` | 11 | $55.80 | $2.04 | $-7.26 | $3,690.78 | ▼ -7.26 after sell → book $10,462.91; vs 09:30 mark -2.04 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 13 | $39.50 | $2.05 | $+78.60 | $4,202.23 | ▲ +78.60 after sell → book $10,460.86; vs 09:30 mark -2.05 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 1 | $146.50 | $1.49 | $-4.08 | $4,347.24 | ▼ -4.08 after sell → book $10,459.37; vs 09:30 mark -1.49 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 27 | $7.98 | $2.09 | $+6.37 | $4,560.61 | ▲ +6.37 after sell → book $10,457.28; vs 09:30 mark -2.09 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ARQT` | 8 | $26.14 | $2.03 | $-2.53 | $4,767.70 | ▼ -2.53 after sell → book $10,455.25; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 1 | $182.33 | $1.85 | $+7.92 | $4,948.18 | ▲ +7.92 after sell → book $10,453.40; vs 09:30 mark -1.85 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `CIFR` | 11 | $17.80 | $2.01 | $-6.61 | $5,141.97 | ▼ -6.61 after sell → book $10,451.39; vs 09:30 mark -2.01 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BRKR` | 3 | $63.37 | $1.93 | $+0.61 | $5,330.15 | ▲ +0.61 after sell → book $10,449.46; vs 09:30 mark -1.93 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 1 | $219.62 | $1.99 | — | $5,108.54 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; combo leftover $333.13; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 3 | $85.00 | $2.00 | — | $4,851.54 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=+18.3; combo leftover $333.13; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 84 | $3.95 | $2.24 | — | $4,517.49 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; combo leftover $333.13; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 23 | $14.07 | $2.06 | — | $4,191.83 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; combo leftover $333.13; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 22 | $14.79 | $2.06 | — | $3,864.39 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; combo leftover $333.13; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 11 | $29.32 | $2.02 | — | $3,539.85 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; combo leftover $333.13; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 109 | $3.04 | $2.32 | — | $3,206.71 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; combo leftover $333.13; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `VITL` | 29 | $11.38 | $2.08 | — | $2,874.62 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+19.5; combo leftover $333.13; owner union_join_vol_green_h1 | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 6 | $108.55 | $2.01 | — | $2,221.31 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+21.3; combo leftover $718.65; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `DELL` | 1 | $593.15 | $1.99 | — | $1,626.17 | — | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+16.1; combo leftover $718.65; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 3 | $209.52 | $2.00 | — | $995.61 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.1; combo leftover $718.65; owner flatten_h5 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 20 | $34.44 | $2.05 | — | $304.76 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+14.0; combo leftover $718.65; owner flatten_h5 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $304.76 | ▼ close $10,393.87 vs 09:30 $10,476.21 (session -30.78) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $304.76 | ▲ 09:30 equity $10,514.93 vs yday $10,393.87 (+121.06) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `VICR` | 1 | $230.25 | $2.01 | $+6.62 | $532.99 | ▲ +6.62 after sell → book $10,512.91; vs 09:30 mark -2.02 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ECO` | 3 | $82.83 | $2.02 | $-10.53 | $779.47 | ▼ -10.53 after sell → book $10,510.90; vs 09:30 mark -2.01 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `EYPT` | 84 | $3.87 | $2.27 | $-11.23 | $1,102.28 | ▼ -11.23 after sell → book $10,508.63; vs 09:30 mark -2.27 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 23 | $13.90 | $2.08 | $-8.05 | $1,419.90 | ▼ -8.05 after sell → book $10,506.55; vs 09:30 mark -2.08 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 22 | $14.58 | $2.08 | $-8.75 | $1,738.58 | ▼ -8.75 after sell → book $10,504.47; vs 09:30 mark -2.08 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 11 | $29.43 | $2.04 | $-2.86 | $2,060.27 | ▼ -2.86 after sell → book $10,502.43; vs 09:30 mark -2.04 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 109 | $4.00 | $2.35 | $+100.52 | $2,493.93 | ▲ +100.52 after sell → book $10,500.09; vs 09:30 mark -2.34 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VITL` | 29 | $12.05 | $2.10 | $+15.26 | $2,841.28 | ▲ +15.26 after sell → book $10,497.99; vs 09:30 mark -2.10 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 1 | $157.87 | $1.58 | — | $2,681.83 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; ret5=+6.5; combo leftover $177.58; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 1 | $88.83 | $0.89 | — | $2,592.11 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; ret5=+7.6; combo leftover $177.58; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 19 | $9.31 | $1.83 | — | $2,413.39 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=+5.3; combo leftover $177.58; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 13 | $13.47 | $1.79 | — | $2,236.42 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; combo leftover $177.58; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 17 | $9.99 | $1.75 | — | $2,064.85 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+5.3; combo leftover $177.58; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 10 | $16.91 | $1.72 | — | $1,894.02 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+50.5; combo leftover $177.58; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `USDE` | 13 | $13.05 | $1.74 | — | $1,722.64 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+40.4; combo leftover $177.58; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `GEMI` | 30 | $5.75 | $1.82 | — | $1,548.17 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+30.3; combo leftover $177.58; owner union_join_vol_green_h1 | — |
| 2026-09-21 09:30 ET | **BUY** | `HUM` | 1 | $386.20 | $1.99 | — | $1,159.98 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=-5.8; combo leftover $387.04; owner flatten_h5 | — |
| 2026-09-21 09:30 ET | **BUY** | `PGEN` | 49 | $7.84 | $2.14 | — | $773.68 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+13.6; combo leftover $387.04; owner flatten_h5 | — |
| 2026-09-21 09:30 ET | **BUY** | `MGTX` | 28 | $13.47 | $2.07 | — | $394.45 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.6; combo leftover $387.04; owner flatten_h5 | — |
| 2026-09-21 09:30 ET | **BUY** | `CYPH` | 96 | $4.00 | $2.28 | — | $8.17 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); ret5=+58.9; combo leftover $387.04; owner flatten_h5 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.17 | ▼ close $10,468.84 vs 09:30 $10,514.93 (session -7.55) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.17 | ▲ 09:30 equity $10,479.14 vs yday $10,468.84 (+10.30) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `SBET` | 17 | $9.91 | $1.76 | $-4.87 | $174.88 | ▼ -4.87 after sell → book $10,477.38; vs 09:30 mark -1.76 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `USDE` | 13 | $12.99 | $1.75 | $-4.26 | $342.01 | ▼ -4.26 after sell → book $10,475.64; vs 09:30 mark -1.74 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `GEMI` | 30 | $6.05 | $1.93 | $+5.26 | $521.73 | ▲ +5.26 after sell → book $10,473.71; vs 09:30 mark -1.93 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `FSLY` | 1 | $28.02 | $0.28 | — | $493.43 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; ret5=+10.3; combo leftover $43.48; owner union_join_vol_green_h1 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $493.43 | ▲ close $10,498.41 vs 09:30 $10,479.14 (session +24.98) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $493.43 | ▲ 09:30 equity $10,544.06 vs yday $10,498.41 (+45.65) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `IQV` | 4 | $270.66 | $2.02 | $-4.94 | $1,574.05 | ▼ -4.94 after sell → book $10,542.04; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `AVAH` | 85 | $13.12 | $2.27 | $-105.66 | $2,686.98 | ▼ -105.66 after sell → book $10,539.77; vs 09:30 mark -2.27 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `BLFS` | 33 | $38.04 | $2.11 | $+47.94 | $3,940.19 | ▲ +47.94 after sell → book $10,537.66; vs 09:30 mark -2.11 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `BKKT` | 19 | $9.50 | $1.88 | $-0.10 | $4,118.81 | ▼ -0.10 after sell → book $10,535.78; vs 09:30 mark -1.88 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BTDR` | 13 | $12.84 | $1.73 | $-11.77 | $4,284.00 | ▼ -11.77 after sell → book $10,534.05; vs 09:30 mark -1.73 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 10 | $16.92 | $1.74 | $-3.36 | $4,451.46 | ▼ -3.36 after sell → book $10,532.31; vs 09:30 mark -1.74 | union_join_vol_green_h1: dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `FSLY` | 1 | $25.90 | $0.28 | $-2.69 | $4,477.07 | ▼ -2.69 after sell → book $10,532.02; vs 09:30 mark -0.29 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 18 | $20.65 | $2.04 | — | $4,103.33 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; combo leftover $373.09; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 23 | $15.72 | $2.06 | — | $3,739.71 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; combo leftover $373.09; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `TNGX` | 14 | $25.40 | $2.03 | — | $3,382.08 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; combo leftover $373.09; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `NMRA` | 485 | $0.77 | $5.18 | — | $3,004.42 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; combo leftover $373.09; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 8 | $41.76 | $2.01 | — | $2,668.32 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+36.4; combo leftover $373.09; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `BFLY` | 37 | $9.90 | $2.10 | — | $2,299.92 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; combo leftover $373.09; owner union_join_vol_green_h1 | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 4 | $116.85 | $2.00 | — | $1,830.52 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.3; combo leftover $574.98; owner flatten_h5 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 20 | $27.79 | $2.05 | — | $1,272.67 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; combo leftover $574.98; owner flatten_h5 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 58 | $9.81 | $2.16 | — | $701.53 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; combo leftover $574.98; owner flatten_h5 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 28 | $20.25 | $2.07 | — | $132.45 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; combo leftover $574.98; owner flatten_h5 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $132.45 | ▼ close $10,203.66 vs 09:30 $10,544.06 (session -304.65) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $132.45 | ▼ 09:30 equity $10,076.40 vs yday $10,203.66 (-127.26) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `TWST` | 3 | $157.72 | $2.02 | $+14.85 | $603.59 | ▲ +14.85 after sell → book $10,074.38; vs 09:30 mark -2.02 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `IOVA` | 58 | $10.39 | $2.18 | $+3.77 | $1,204.03 | ▲ +3.77 after sell → book $10,072.20; vs 09:30 mark -2.18 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `AMN` | 17 | $33.82 | $2.06 | $-22.97 | $1,776.91 | ▼ -22.97 after sell → book $10,070.13; vs 09:30 mark -2.07 | flatten_h5: dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 1 | $163.95 | $1.66 | $+2.84 | $1,939.20 | ▲ +2.84 after sell → book $10,068.47; vs 09:30 mark -1.66 | union_join_vol_green_h1: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 1 | $87.67 | $0.90 | $-2.95 | $2,025.97 | ▼ -2.95 after sell → book $10,067.57; vs 09:30 mark -0.90 | union_join_vol_green_h1: dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 18 | $20.52 | $2.06 | $-6.45 | $2,393.27 | ▼ -6.45 after sell → book $10,065.51; vs 09:30 mark -2.06 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 23 | $14.38 | $2.08 | $-34.96 | $2,721.93 | ▼ -34.96 after sell → book $10,063.43; vs 09:30 mark -2.08 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `TNGX` | 14 | $23.99 | $2.05 | $-23.82 | $3,055.74 | ▼ -23.82 after sell → book $10,061.38; vs 09:30 mark -2.05 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `NMRA` | 485 | $0.75 | $5.16 | $-21.01 | $3,412.38 | ▼ -21.01 after sell → book $10,056.21; vs 09:30 mark -5.17 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 8 | $36.02 | $2.03 | $-49.93 | $3,698.55 | ▼ -49.93 after sell → book $10,054.18; vs 09:30 mark -2.03 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BFLY` | 37 | $9.12 | $2.12 | $-33.08 | $4,033.87 | ▼ -33.08 after sell → book $10,052.06; vs 09:30 mark -2.12 | union_join_vol_green_h1: dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,033.87 | ▲ close $10,137.74 vs 09:30 $10,076.40 (session +85.68) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,505.95 | ▲ 09:30 equity $8,855.81 vs yday $8,855.25 (+0.56) | 09:30 open · cash $3,505.95 (unchanged overnight, no fees) · equity $8,855.81 vs prior close $8,855.25 (+0.56) | — |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 8 | $26.27 | $2.01 | — | $3,293.78 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; combo leftover $219.12; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 56 | $3.86 | $2.16 | — | $3,075.46 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; combo leftover $219.12; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TWST` | 1 | $184.00 | $1.84 | — | $2,889.61 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.3; combo leftover $219.12; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 13 | $16.21 | $2.03 | — | $2,676.86 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; combo leftover $219.12; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GRAL` | 1 | $123.50 | $1.24 | — | $2,552.12 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+56.6; combo leftover $219.12; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `QMCO` | 7 | $29.80 | $2.01 | — | $2,341.51 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+18.2; combo leftover $219.12; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CYPH` | 54 | $4.00 | $2.15 | — | $2,123.09 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+32.9; combo leftover $219.12; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CDNA` | 3 | $61.33 | $1.85 | — | $1,937.25 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+13.1; combo leftover $219.12; owner union_join_vol_green_h1 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `OMER` | 23 | $20.61 | $2.06 | — | $1,461.16 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+9.1; combo leftover $484.31; owner flatten_h5 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 12 | $38.51 | $2.03 | — | $997.01 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.7; combo leftover $484.31; owner flatten_h5 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 63 | $7.65 | $2.18 | — | $512.88 | — | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+5.2; combo leftover $484.31; owner flatten_h5 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $512.88 | ▲ close $8,837.26 vs 09:30 $8,855.81 (session +3.01) | 16:00 close · cash $512.88 · equity $8,837.26 vs 09:30 $8,855.81 (-18.55; session marks +3.01) · 27 name(s) marked open→close (per-name table). ADMA×56 09:30 $9.52 → close $9.52 +0.00; ARQT×19 09:30 $26.27 → close $26.27 +0.00; DLO×11 09:30 $13.88 → close $13.88 +0.00; EL×1 09:30 $95.37 → close $95.37 +0.00; FIVN×15 09:30 $36.66 → close $36.66 -0.00; FTRE×27 09:30 $20.02 → close $20.02 +0.00; GNRC×2 09:30 $198.05 → close $198.05 +0.00; HALO×4 09:30 $115.36 → close $113.90 -5.84; HUM×1 09:30 $380.32 → close $380.32 +0.00; MGTX×32 09:30 $11.05 → close $11.05 +0.00; MKC×3 09:30 $47.82 → close $47.82 -0.00; PACS×3 09:30 $41.46 → close $41.46 -0.00; PGEN×55 09:30 $7.70 → close $7.70 -0.00; RBRK×4 09:30 $113.80 → close $113.80 +0.00; TDC×5 09:30 $29.46 → close $29.46 -0.00; USFD×1 09:30 $93.82 → close $93.82 -0.00; WRBY×8 09:30 $26.27 → close $26.71 +3.52; ZSQR×56 09:30 $3.86 → close $3.78 -4.48; TWST×1 09:30 $184.00 → close $182.83 -1.17; SECZ×13 09:30 $16.21 → close $15.96 -3.25; GRAL×1 09:30 $123.50 → close $126.89 +3.39; QMCO×7 09:30 $29.80 → close $31.68 +13.16; CYPH×54 09:30 $4.00 → close $4.12 +6.21; CDNA×3 09:30 $61.33 → close $63.68 +7.05; OMER×23 09:30 $20.61 → close $20.08 -12.19; BLFS×12 09:30 $38.51 → close $38.49 -0.24; MRVI×63 09:30 $7.65 → close $7.60 -3.15 | — |

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
| 2026-08-14 | `BETR` | cash | leftover split 6.10 < 1 share @ 14.80 |
| 2026-08-14 | `ADUR` | cash | leftover split 6.10 < 1 share @ 16.50 |
| 2026-08-14 | `AIRO` | cash | leftover split 6.10 < 1 share @ 11.12 |
| 2026-08-14 | `QMLS` | cash | leftover split 6.10 < 1 share @ 7.29 |
| 2026-08-14 | `TLN` | cash | leftover split 11.06 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 11.06 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 11.06 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 11.06 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 11.06 < 1 share @ 57.61 |
| 2026-08-17 | `BTSG` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `IREN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `TPG` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `TGTX` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `SLS` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `HIMS` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `INO` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `TNDM` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-17 | `MARA` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-17 | `LDI` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-17 | `ABX` | cash | leftover split 7.80 < 1 share @ 9.12 |
| 2026-08-17 | `ALOY` | cash | leftover split 7.80 < 1 share @ 14.66 |
| 2026-08-17 | `MP` | cash | leftover split 7.80 < 1 share @ 58.01 |
| 2026-08-17 | `DVN` | cash | leftover split 8.64 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 8.64 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 8.64 < 1 share @ 202.70 |
| 2026-08-17 | `ELF` | cash | leftover split 8.64 < 1 share @ 90.54 |
| 2026-08-18 | `BTSG` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `IREN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `TPG` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `TGTX` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `SLS` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `HIMS` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `INO` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `TNDM` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-18 | `MARA` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-18 | `LDI` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-18 | `TMC` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `TGB` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `DNN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-18 | `HNST` | min_hold | flatten_h5: dropped but min-hold 1/5 |
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
| 2026-08-19 | `MARA` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-19 | `LDI` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-19 | `TMC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-19 | `TGB` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-19 | `DNN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-19 | `HNST` | min_hold | flatten_h5: dropped but min-hold 2/5 |
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
| 2026-08-21 | `BHP` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-24 | `BHP` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-24 | `AUTL` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-24 | `CRDL` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-24 | `CRSP` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new long union_join_vol_green_h1 |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new long flatten_h5 |
| 2026-08-25 | `BHP` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-25 | `AUTL` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-25 | `CRDL` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-25 | `CRSP` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-25 | `HCA` | cash | leftover split 241.38 < 1 share @ 426.97 |
| 2026-08-26 | `BHP` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-26 | `AUTL` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-26 | `CRDL` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-26 | `CRSP` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-27 | `AUTL` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `CRDL` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `CRSP` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-27 | `OCUL` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-27 | `INSP` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-27 | `CRMD` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-27 | `RZLT` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-27 | `HCA` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-08-28 | `OCUL` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-28 | `INSP` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-28 | `CRMD` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-28 | `RZLT` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-28 | `HCA` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-31 | `MOS` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `OCUL` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `INSP` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `CRMD` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `RZLT` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-08-31 | `HCA` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-08-31 | `RRC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-31 | `CRK` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-31 | `SLI` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new long union_join_vol_green_h1 |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new long union_join_vol_green_h1 |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new long flatten_h5 |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new long flatten_h5 |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new long flatten_h5 |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new long flatten_h5 |
| 2026-09-01 | `HCA` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-01 | `RRC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-01 | `CRK` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-01 | `SLI` | min_hold | flatten_h5: dropped but min-hold 3/5 |
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
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new long flatten_h5 |
| 2026-09-04 | `HRMY` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-04 | `VSTM` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-04 | `DELL` | cash | leftover split 346.13 < 1 share @ 513.78 |
| 2026-09-04 | `MDB` | cash | leftover split 346.13 < 1 share @ 378.34 |
| 2026-09-08 | `ATRC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-08 | `HRMY` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-08 | `CABA` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-08 | `VSTM` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-08 | `ALEC` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-08 | `BHC` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-08 | `BMEA` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-08 | `OABI` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-08 | `OPK` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-08 | `VIR` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `CRCL` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `TRMD` | hard_red | hard-red S=-11.47 sit; no new long union_join_vol_green_h1 |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new long flatten_h5 |
| 2026-09-08 | `SID` | hard_red | hard-red S=-11.47 sit; no new long flatten_h5 |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new long flatten_h5 |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new long flatten_h5 |
| 2026-09-09 | `ATRC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-09 | `HRMY` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-09 | `CABA` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-09 | `VSTM` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-09 | `ALEC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-09 | `BHC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-09 | `BMEA` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-09 | `OABI` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-09 | `OPK` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-09 | `VIR` | min_hold | flatten_h5: dropped but min-hold 2/5 |
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
| 2026-09-10 | `ALEC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-10 | `BHC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-10 | `BMEA` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-10 | `OABI` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-10 | `OPK` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-10 | `VIR` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `AMBA` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `CMPS` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `ARBE` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new long union_join_vol_green_h1 |
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
| 2026-09-14 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-14 | `OVID` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-14 | `SANM` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-14 | `ORCL` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-14 | `COHU` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `HPQ` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `INSP` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `GME` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `DHT` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `AVT` | hard_red | hard-red S=-11.00 sit; no new long union_join_vol_green_h1 |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new long flatten_h5 |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new long flatten_h5 |
| 2026-09-14 | `BG` | hard_red | hard-red S=-11.00 sit; no new long flatten_h5 |
| 2026-09-15 | `AUPH` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `OVID` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `SANM` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `ORCL` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `NVT` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `COHU` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `S` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `IOT` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `SAFX` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
| 2026-09-15 | `ECO` | hard_red | hard-red S=-3.84 sit; no new long union_join_vol_green_h1 |
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
| 2026-09-17 | `AVAH` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-17 | `BLFS` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-17 | `ILMN` | cash | leftover split 208.74 < 1 share @ 233.85 |
| 2026-09-18 | `IQV` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-18 | `AVAH` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-18 | `BLFS` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-18 | `TWST` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-18 | `IOVA` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-18 | `AMN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `IQV` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-21 | `AVAH` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-21 | `BLFS` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-21 | `TWST` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-21 | `AMN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-21 | `RBRK` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `DELL` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `GNRC` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-21 | `FIVN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-22 | `IQV` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-22 | `AVAH` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-22 | `BLFS` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-22 | `TWST` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-22 | `IOVA` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-22 | `AMN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-22 | `RBRK` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `DELL` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `GNRC` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `FIVN` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-22 | `A` | no_price | no 09:30 open — carry |
| 2026-09-22 | `DXCM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BKKT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BTDR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TJGC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `HUM` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-22 | `PGEN` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-22 | `MGTX` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-22 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-22 | `ARM` | cash | leftover split 43.48 < 1 share @ 319.41 |
| 2026-09-22 | `GRPN` | no_price | no 09:30 open |
| 2026-09-22 | `XXI` | no_price | no 09:30 open |
| 2026-09-22 | `META` | cash | leftover split 43.48 < 1 share @ 731.40 |
| 2026-09-22 | `THO` | no_price | no 09:30 open |
| 2026-09-22 | `PACS` | no_price | no 09:30 open |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `USFD` | cash | leftover split 82.24 < 1 share @ 93.97 |
| 2026-09-22 | `DLO` | no_price | no 09:30 open |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-23 | `TWST` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-23 | `IOVA` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-23 | `AMN` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-23 | `RBRK` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `DELL` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `GNRC` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `FIVN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-23 | `HUM` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-23 | `MGTX` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-23 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 2/5 |
| 2026-09-24 | `RBRK` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `DELL` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `GNRC` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `FIVN` | min_hold | flatten_h5: dropped but min-hold 4/5 |
| 2026-09-24 | `HUM` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-24 | `PGEN` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-24 | `MGTX` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-24 | `CYPH` | min_hold | flatten_h5: dropped but min-hold 3/5 |
| 2026-09-24 | `HALO` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-24 | `ARQT` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-24 | `ADMA` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-24 | `FTRE` | min_hold | flatten_h5: dropped but min-hold 1/5 |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `TLSA` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `GLND` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `FSLY` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `KVYO` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `VICR` | hard_red | hard-red S=-7.66 sit; no new long union_join_vol_green_h1 |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `CHKP` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `S` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |
| 2026-09-24 | `BAH` | hard_red | hard-red S=-7.66 sit; no new long flatten_h5 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `RBRK` | 6 | 2026-09-18 @ $108.55 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+21.3; combo leftover $718.65; owner flatten_h5 |
| `DELL` | 1 | 2026-09-18 @ $593.15 | baseline list, no extra gate; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ret5=+16.1; combo leftover $718.65; owner flatten_h5 |
| `GNRC` | 3 | 2026-09-18 @ $209.52 | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); 🔵; ret5=+14.1; combo leftover $718.65; owner flatten_h5 |
| `FIVN` | 20 | 2026-09-18 @ $34.44 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+14.0; combo leftover $718.65; owner flatten_h5 |
| `HUM` | 1 | 2026-09-21 @ $386.20 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=-5.8; combo leftover $387.04; owner flatten_h5 |
| `PGEN` | 49 | 2026-09-21 @ $7.84 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+13.6; combo leftover $387.04; owner flatten_h5 |
| `MGTX` | 28 | 2026-09-21 @ $13.47 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); ret5=+3.6; combo leftover $387.04; owner flatten_h5 |
| `CYPH` | 96 | 2026-09-21 @ $4.00 | baseline list, no extra gate; list flatten,yday_gainer,yday_mover; wish-list (live io HOLD — not a ticket); ret5=+58.9; combo leftover $387.04; owner flatten_h5 |
| `HALO` | 4 | 2026-09-23 @ $116.85 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.3; combo leftover $574.98; owner flatten_h5 |
| `ARQT` | 20 | 2026-09-23 @ $27.79 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+7.0; combo leftover $574.98; owner flatten_h5 |
| `ADMA` | 58 | 2026-09-23 @ $9.81 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.0; combo leftover $574.98; owner flatten_h5 |
| `FTRE` | 28 | 2026-09-23 @ $20.25 | baseline list, no extra gate; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+15.0; combo leftover $574.98; owner flatten_h5 |
